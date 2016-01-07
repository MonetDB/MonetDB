/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (author) M. L. Kersten
 * For documentation see website
 */

#include "monetdb_config.h"
#include "mal_module.h"
#include "mal_function.h"   /* for printFunction() */
#include "mal_namespace.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_listing.h"
#include "mal_private.h"

Module mal_scope;    /* the root of the tree */
Module scopeJump[256][256];  /* to speedup access to correct scope */

static void newSubScope(Module scope){
	int len = (MAXSCOPE)*sizeof(Module);
	scope->subscope = (Symbol *) GDKzalloc(len);
}

void
mal_module_reset(void)
{
	freeModuleList(mal_scope);
	mal_scope = NULL;
	memset((char*) scopeJump, 0, 256 * 256);
}
/*
 * Definition of a new module scope may interfere with concurrent
 * actions of multiple threads. This calls for a secure update
 * of the scope tree structure.
 * A jump table is mainted to provide a quick start in the module
 * table to find the correct one. This simple scheme safes about
 * 100ms/100K calls
 */

static void clrModuleJump(str nme, Module cur){
		if( scopeJump[(int)(*nme)][(int)(*(nme+1))]== cur)
			scopeJump[(int)(*nme)][(int)(*(nme+1))]= cur->sibling;
}

void setModuleJump(str nme, Module cur){
		cur->sibling= scopeJump[(int)(*nme)][(int)(*(nme+1))];
		scopeJump[(int)(*nme)][(int)(*(nme+1))]= cur;
}

/*
 * Module scope management
 * Upon system restart, the global scope is created. It is called "root" and
 * does not contain any symbol definitions. It merely functions as an anchor
 * point for the modules to be added later.
 */
Module newModule(Module scope, str nme){
	Module cur;

	nme = putName(nme,strlen(nme));
	assert(nme != NULL);
	cur = (Module) GDKzalloc(sizeof(ModuleRecord));
	if( cur == NULL){
		GDKerror("newModule:"MAL_MALLOC_FAIL);
	} else {
		cur->name = nme;
		cur->outer = NULL;
		cur->sibling = NULL;
		cur->subscope = NULL;
		cur->isAtomModule = FALSE;
	}
	if ( cur == NULL)
		return scope;
	newSubScope(cur);
	if( scope != NULL){
		cur->outer = scope->outer;
		scope->outer= cur;
		setModuleJump(nme,cur);
	}
	return cur;
}
/*
 * The scope can be fixed. This is used by the parser to avoid creation of
 * a string structure when possible. Subsequently we can
 * replace the module name in the instructions to become a pointer
 * to the scope directly.
 * Reading a module often calls for opening a scope level
 * if it didn't exist.
 */
Module fixModule(Module scope, str nme){
	Module s= scope;
	if( scopeJump[(int)(*nme)][(int)(*(nme+1))])
		s= scopeJump[(int)(*nme)][(int)(*(nme+1))];
	while(s != NULL){
		if( nme == s->name )
			return s;
		s= s->outer;
	}
	return newModule(scope, nme);
}
/*
 * A derived module inherits copies of all known
 * functions in the parent module. These can be
 * refined or expanded.
 */
void
deriveModule(Module scope, str nme){
	Module src= findModule(scope,nme);
	Symbol s;
	int i;
	if( src == scope) return;
	for(i=0; i<256; i++){
		s= src->subscope[i];
		while( s){
			/* copy the symbol */
			s= s->peer;
		}
	}
}
/*
 * The freeModule operation throws away a symbol without
 * concerns on it whereabouts in the scope structure.
 * This routine therefore assumes care in use.
 * The final action of the system is to remove all
 * instructions and procedures. This forcefull action
 * helps in localization of memory leakages.
 */
static void freeSubScope(Module scope)
{
	int i;

	if (scope->subscope == NULL) 
		return;
	for(i=0;i<MAXSCOPE;i++) {
		if( scope->subscope[i]){
			freeSymbolList(scope->subscope[i]);
			scope->subscope[i]= NULL;
		}
	}
	GDKfree(scope->subscope);
	scope->subscope = 0;
}
void freeModule(Module m)
{
	Symbol s;

	if (m==NULL) 
		return;
	if ((s=findSymbolInModule(m, "epilogue")) != NULL) {
		InstrPtr pci = getInstrPtr(s->def,0);
		if (pci && pci->token == COMMANDsymbol && pci->argc == 1) {
			int ret = 0;

			assert(pci->fcn != NULL);
			(*pci->fcn)(&ret);
			(void)ret;
		}
	}
	freeSubScope(m);
	clrModuleJump(m->name, m);
	if (m->help)
		GDKfree(m->help);
	GDKfree(m);
}

void freeModuleList(Module s){
	Module t;
	if (s == NULL) {
		s = mal_scope;
	}
	t=s;
	while(s){
		t= s->outer;
		s->outer= NULL;
		freeModule(s);
		s=t;
	}
}

/*
 * After filling in a structure it is added to the multi-level symbol
 * table.  We keep a skip list of similarly named function symbols.
 * This speeds up searching provided the modules adhere to the
 * structure and group the functions as well.
 */
void insertSymbol(Module scope, Symbol prg){
	InstrPtr sig;
	int t;
	Module c;

	sig = getSignature(prg);
	if(getModuleId(sig) && getModuleId(sig)!= scope->name){
		/* move the definition to the proper place */
		/* default scope is the last resort */
		c= findModule(scope,getModuleId(sig));
		if ( c )
			scope = c;
	}
	t = getSubScope(getFunctionId(sig));
	if( scope->subscope == NULL)
		newSubScope(scope);
	if(scope->subscope[t] == prg){
		/* already known, last inserted */
	 } else  {
		prg->peer= scope->subscope[t];
		scope->subscope[t] = prg;
		if( prg->peer &&
			idcmp(prg->name,prg->peer->name) == 0)
			prg->skip = prg->peer->skip;
		else
			prg->skip = prg->peer;
	}
	assert(prg != prg->peer);
}
/*
 * Removal of elements from the symbol table should be
 * done with care. For, it should be assured that
 * there are no references to the definition at the
 * moment of removal. This situation can not easily
 * checked at runtime, without tremendous overhead.
 */
void deleteSymbol(Module scope, Symbol prg){
	InstrPtr sig;
	int t;

	sig = getSignature(prg);
	if( getModuleId(sig) && getModuleId(sig)!= scope->name ){
		/* move the definition to the proper place */
		/* default scope is the last resort */
		Module c= findModule(scope,getModuleId(sig));
		if(c )
			scope = c;
	}
	t = getSubScope(getFunctionId(sig));
	if (scope->subscope[t] == prg) {
		scope->subscope[t] = scope->subscope[t]->peer;
		freeSymbol(prg);
	} else {
		Symbol nxt = scope->subscope[t];
		while (nxt->peer != NULL) {
			if (nxt->peer == prg) {
				nxt->peer = prg->peer;
				nxt->skip = prg->peer;
				freeSymbol(prg);
				return;
			}
			nxt = nxt->peer;
		}
	}
}

/*
 * Searching the scope structure.
 * Finding a scope is unrestricted. For modules we explicitly look for
 * the start of a new module scope.
 * All core modules are accessed through the jumptable.
 * The 'user' module is an alias for the scope attached
 * to the current user.
 */
Module findModule(Module scope, str name){
	Module def=scope;
	if( name==NULL) return scope;
	scope= scopeJump[(int)(*name)][(int)(*(name+1))];
	while(scope != NULL){
			if( name == scope->name )
					return scope;
			scope= scope->sibling;
	}
	/* default is always matched with current */
	if( def->name==NULL) return NULL;
	return def;
}
int isModuleDefined(Module scope, str name){
	if( name==NULL || scope==NULL) return FALSE;
	if( name == scope->name) return TRUE;
	scope= scopeJump[(int)(*name)][(int)(*(name+1))];
	while(scope != NULL){
			if( name == scope->name )
					return TRUE;
			scope= scope->sibling;
	}
	return FALSE;
}
/*
 * The routine findSymbolInModule starts at a MAL scope level and searches
 * an element amongst the peers. If it fails, it will recursively
 * inspect the outer scopes.
 *
 * In principal, external variables are subject to synchronization actions
 * to avoid concurrency conflicts. This also implies, that any parallel
 * block introduces a temporary scope.
 *
 * The variation on this routine is to dump the definition of
 * all matching definitions.
 */
Symbol findSymbolInModule(Module v, str fcn){
	Symbol s;
	if( v == NULL || fcn == NULL) return NULL;
	s= v->subscope[(int)(*fcn)];
	while(s!=NULL){
		if( idcmp(s->name,fcn)==0 ) return s;
		s= s->skip;
	}
	return NULL;
}

Symbol findSymbol(Module nspace, str mod, str fcn){
	Module m= findModule(nspace,mod);
	return findSymbolInModule(m,fcn);
}

int
findInstruction(Module scope, MalBlkPtr mb, InstrPtr pci){
	Module m;
	Symbol s;
	int i,fnd;

	for(m= findModule(scope,getModuleId(pci)); m; m= m->outer)
	if( m->name == getModuleId(pci) ) {
		s= m->subscope[(int)(getSubScope(getFunctionId(pci)))];
		for(; s; s= s->peer)
		if( getFunctionId(pci)==s->name && pci->argc == getSignature(s)->argc ){
			/* found it check argtypes */
			for( fnd=1, i = 0; i < pci->argc; i++)
				if ( getArgType(mb,pci,i) != getArgType(s->def,getSignature(s),i))
					fnd = 0;
			if( fnd)
				return 1;
		}
	}

	return 0;
}

/*
 * Some primitives to aid online help and completions.
 * Note that pattern matching is on string prefix.
 */
static int tstDuplicate(char **msg, char *s){
	int i;
	size_t len;
	len= strlen(s);
	for(i=0; msg[i]; i++)
		if( strncmp(s, msg[i], MAX(len,strlen(msg[i]))) == 0 &&
			strlen(s) == strlen(msg[i]) )
			return 1;
	return 0;
}

#define MAXHELP 500
char **getHelp(Module m, str inputpat, int completion)
{
	str pat, modnme, fcnnme = 0;
	Module m1;
	Symbol s;
	size_t len1 = 0,len2 = 0;
	int fnd=0;
	char *t, **msg, buf[1024]={0};
	int top=0, i,j,k, sig = 0, doc = 0;
	int maxhelp= MAXHELP;

#ifdef MAL_SCOPE_DEBUG
	printf("showHelp: %s",pat);
#endif
	msg= (char **) GDKmalloc( MAXHELP * sizeof(str));
	if( msg == NULL){
		GDKerror("getHelp"MAL_MALLOC_FAIL);
		return NULL;
	}
	msg[top]=0;

	if (!inputpat)
		return msg;

	pat= GDKstrdup(inputpat);
	t= strchr(pat,'\n');
	if( t) *t=0;

	t = strchr(pat,')');
	if( t) { doc++; *t=0; completion=0; }
	t= strchr(pat,'(');
	if( t) { sig++; *t=0; completion=0; }

	/* rudimentary patterns only.
	 	*.nme  nme.* nme.nme *.*
	   ignore the rest.
	*/
	modnme= pat;
	if( (fcnnme = strchr(pat,'.')) ){
		*fcnnme++ = 0;
		if( strchr(modnme,'*'))
			modnme="*";
		if( strchr(fcnnme,'*') || *fcnnme==0)
			fcnnme="*";
	} else {
		modnme="*";
		fcnnme=pat;
		if( strchr(fcnnme,'*') || *fcnnme==0)
			fcnnme="*";
	}

	if( fcnnme && *fcnnme){
		len2 = strlen(fcnnme);
	}

	len1 = (int)strlen(modnme);

	/* display module information if there is no function */
	if( fcnnme == NULL){
		for(i=0; i< MAXSCOPE; i++)
		for(j=0; j< MAXSCOPE; j++){
			m= scopeJump[i][j];
			while(m != NULL){
				if( strncmp(modnme,m->name,len1) ==0  || *modnme=='*'){
					msg[top++] = GDKstrdup(m->name);
					msg[top] =0;
					if( top == maxhelp-1) {
						msg= (char **) GDKrealloc(msg,sizeof(str)* maxhelp);
						maxhelp+= MAXHELP;
					}
				}
				m= m->sibling;
			}
		}
		GDKfree(pat);
		return msg;
	}

	/* display module.function */
	m1 = findModule(m,modnme);
	if( m1 == 0  && *modnme != '*') {
		GDKfree(pat);
		return msg;
	}

#ifdef MAL_SCOPE_DEBUG
	printf("showHelp: %s %s [" SZFMT "] %s %s\n",
			modnme,fcnnme,len2, (doc?"doc":""), (sig?"sig":""));
#endif
	for(i=0; i< MAXSCOPE; i++)
	for(k=0; k< MAXSCOPE; k++){
	  m= scopeJump[i][k];
	  while( m){
		if( strncmp(modnme,m->name,len1) && *modnme!='*' ) {
			m= m->sibling;
			continue;
		}
		for(j=0;j<MAXSCOPE;j++)
		for(s= m->subscope[j]; s; s= s->peer)
			if( strncmp(fcnnme,s->name,len2)==0 || *fcnnme=='*') {
				fnd=0;
				if( completion ) {
					snprintf(buf,sizeof(buf)," %s.%s",
						((*modnme=='*' || *modnme==0)? m->name:modnme),s->name);
					if( tstDuplicate(msg,buf+1) ) {
						continue;
					}
				} else
				if( doc) {
					char *v;

					fcnDefinition(s->def,s->def->stmt[0],buf,FALSE,buf,sizeof(buf));
					buf[0]=' ';

					v= strstr(buf,"address");
					if( v) *v=0;
					if( tstDuplicate(msg,buf+1) && s->def->help==0 ) fnd++;
					if(fnd) continue;

					msg[top++]= GDKstrdup(buf+1);
					if(v){
						*v='a';
						msg[top++]= GDKstrdup(v);
					}
					msg[top] = 0;

					if( s->def->help) {
						char *w;
						strcpy(buf+1,"comment ");
						v= buf+1+8;
						for( w= s->def->help; *w && v <buf+sizeof(buf)-2; w++)
						if( *w == '\n'){
							/*ignore */
						} else *v++ = *w;
						*v = 0;
					} else fnd = 1; /* ignore non-existing comment */
					if(v){
						*v++ ='\n';
						*v=0;
					}
				} else if( strncmp(fcnnme,s->name,strlen(fcnnme))==0 ||
							*fcnnme=='*' ) {
					fcnDefinition(s->def,s->def->stmt[0],buf,FALSE,buf,sizeof(buf));
					buf[0]=' ';
					t= strstr(buf,"address");
					if( t) *t= 0;
				}
				if( fnd == 0 && buf[1]){
					msg[top++] = GDKstrdup(buf+1);
					msg[top] = 0;
				}
				if( top >= maxhelp-3){
					msg= (char **) GDKrealloc(msg,sizeof(str)* (maxhelp+MAXHELP));
					maxhelp+= MAXHELP;
				}
			}
			m= m->sibling;
		}
	}
	GDKfree(pat);
	return msg;
}

/*
 * The second primitive of relevance is to find documentation matching
 * a keyword. Since we can not assume pcre to be everywhere, we keep
 * it simple.
 */
char **getHelpMatch(char *pat){
	char **msg, buf[1024];
	Module m;
	Symbol s;
	int top = 0, i,j,k;
	int maxhelp= MAXHELP;

	msg= (char **) GDKmalloc( maxhelp * sizeof(str));
	if( msg == NULL){
		GDKerror("getHelpMatch" MAL_MALLOC_FAIL);
		return NULL;
	}
	msg[top]=0;

	if (!pat)
		return msg;

	for(i=0; i< MAXSCOPE; i++)
	for(k=0; k< MAXSCOPE; k++){
		m= scopeJump[i][k];
		while( m){
			for(j=0;j<MAXSCOPE;j++)
			if( m->subscope[j])
				for(s= m->subscope[j]; s; s= s->peer)
				if( strstr(m->name,pat) || strstr(s->name,pat) ||
					(s->def->help && strstr(s->def->help,pat))) {
					char *v,*w;
					fcnDefinition(s->def,s->def->stmt[0],buf,FALSE,buf,sizeof(buf));
					buf[0]=' ';
					if( s->def->help ){
						v= strchr(buf,0);
						assert (v != NULL); /* fool Coverity */
						*v++ = '\\';
						*v++ = 'n';
						*v++ = '#';
						for( w= s->def->help; *w && v <buf+sizeof(buf)-3; w++)
						if( *w == '\n'){
							*v++ = '\\';
							*v++ = 'n';
							*v++ = '#';
							w++;
							if( isspace((int) *w)) {
								for(; *w && isspace((int) *w); w++);
								w--;
							}
						} else *v++ = *w;
						*v++ = '\\';
						*v++ = 'n';
						*v = 0;
					}
					msg[top++] = GDKstrdup(buf);
					msg[top] = 0;
					if( top == maxhelp-1){
						msg= (char **) GDKrealloc(msg,sizeof(str)* (maxhelp+MAXHELP));
						maxhelp+= MAXHELP;
					}
				}
			m= m->sibling;
		}
	}
	return msg;
}

void
showHelp(Module m, str txt, stream *fs){
	int i;
	char **msg = getHelp(m,txt,TRUE);
	if( msg == NULL)
		return;
	for(i=0; msg[i]; i++) {
		mnstr_printf(fs,"%s\n",msg[i]);
		GDKfree(msg[i]);
	}
	GDKfree(msg);
	if( i == 0){
		msg = getHelp(m,txt,0);
		if (msg) {
			for(i=0; msg[i]; i++) {
				mnstr_printf(fs,"%s\n",msg[i]);
				GDKfree(msg[i]);
			}
			GDKfree(msg);
		}
	}
}
/*
 * The tags file is used by the mclient frontend to
 * enable language specific word completion.
 */
void dumpHelpTable(stream *f, Module s, str text, int flag){
	str *msg;
	int j,m;

	msg= getHelp(s,text,flag);
	if( msg == NULL)
		return;
	for(m=0; msg[m]; m++ ) ;

	mnstr_printf(f,"&1 0 %d 1 %d\n",m,m);
	mnstr_printf(f,"# help # table_name\n");
	mnstr_printf(f,"# name # name\n");
	mnstr_printf(f,"# varchar # type\n");
	mnstr_printf(f,"# 0 # length\n");
	for(j=0; j<m; j++) {
		mnstr_printf(f,"[ \"%s\" ]\n",msg[j]);
		GDKfree(msg[j]);
	}
	GDKfree(msg);
}
void dumpSearchTable(stream *f, str text){
	str *msg;
	int j,m;

	msg= getHelpMatch(text);
	if( msg == NULL)
		return;
	for(m=0; msg[m]; m++ ) ;

	mnstr_printf(f,"&1 0 %d 1 %d\n",m,m);
	mnstr_printf(f,"# help # table_name\n");
	mnstr_printf(f,"# name # name\n");
	mnstr_printf(f,"# varchar # type\n");
	mnstr_printf(f,"# 0 # length\n");
	for(j=0; j<m; j++) {
		mnstr_printf(f,"[ \"%s\" ]\n",msg[j]);
		GDKfree(msg[j]);
	}
	GDKfree(msg);
}
