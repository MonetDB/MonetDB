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
	scope->subscope = (Symbol *) GDKzalloc(MAXSCOPE * sizeof(Symbol));
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

	nme = putName(nme);
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
	assert(scope->subscope);
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

