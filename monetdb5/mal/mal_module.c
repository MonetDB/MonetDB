/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (author) M. L. Kersten
 */

#include "monetdb_config.h"
#include "mal_module.h"
#include "mal_function.h"   /* for printFunction() */
#include "mal_namespace.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_listing.h"
#include "mal_private.h"

// SHOULD BE PROTECTED WITH LOCKS
/*
 * Definition of a new module may interfere with concurrent actions.
 * A jump table is mainted to provide a quick start in the module
 * table to find the correct one. 
 *
 * All modules are persistent during a server session
 */
Module moduleIndex[256][256]; 	/* to speedup access to correct scope */
Module moduleChain;				/* keep the modules in a chain as well */

static void newModuleSpace(Module scope){
	scope->space = (Symbol *) GDKzalloc(MAXSCOPE * sizeof(Symbol));
}

Module
getModuleChain(void){
	return moduleChain;
}

void
mal_module_reset(void)
{
	Module m,n;

	for( m = moduleChain, moduleChain = 0; m; ){
		n = m->next;
		freeModule(m);
		m= n;
	}
	memset((char*) moduleIndex, 0, 256 * 256 * sizeof(Module));
}

static void clrModuleIndex(str nme, Module cur){
		if( moduleIndex[(int)(*nme)][(int)(*(nme+1))]== cur)
			moduleIndex[(int)(*nme)][(int)(*(nme+1))]= cur->link;
}

static void setModuleIndex(str nme, Module cur){
		cur->link= moduleIndex[(int)(*nme)][(int)(*(nme+1))];
		moduleIndex[(int)(*nme)][(int)(*(nme+1))]= cur;
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
		cur->next = NULL;
		cur->link = NULL;
		cur->space = NULL;
		cur->isAtomModule = FALSE;
	}
	if ( cur == NULL)
		return scope;
	newModuleSpace(cur);
	// User modules are never global
	if( strcmp(nme,"user")){
		setModuleIndex(nme,cur);
		if ( moduleChain)
			cur->next = moduleChain;
		moduleChain = cur;
	}
	return cur;
}
/*
 * The scope can be fixed. This is used by the parser.
 * Reading a module often calls for creation first.
 */
Module fixModule(Module scope, str nme){
	Module s= 0;
	
	if( strcmp(nme,"user")==0)
		return scope;
	if( moduleIndex[(int)(*nme)][(int)(*(nme+1))])
		s= moduleIndex[(int)(*nme)][(int)(*(nme+1))];
	while(s != NULL){
		if( nme == s->name )
			return s;
		s= s->link;
	}
	return newModule(scope, nme);
}
/*
 * The freeModule operation throws away a symbol without
 * concerns on it whereabouts in the scope structure.
 */
static void freeSubScope(Module scope)
{
	int i;

	if (scope->space == NULL) 
		return;
	for(i=0;i<MAXSCOPE;i++) {
		if( scope->space[i]){
			freeSymbolList(scope->space[i]);
			scope->space[i]= NULL;
		}
	}
	GDKfree(scope->space);
	scope->space = 0;
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
	clrModuleIndex(m->name, m);
	if (m->help)
		GDKfree(m->help);
	GDKfree(m);
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
	t = getSymbolIndex(getFunctionId(sig));
	if( scope->space == NULL)
		newModuleSpace(scope);
	assert(scope->space);
	if(scope->space[t] == prg){
		/* already known, last inserted */
	 } else  {
		prg->peer= scope->space[t];
		scope->space[t] = prg;
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
	t = getSymbolIndex(getFunctionId(sig));
	if (scope->space[t] == prg) {
		scope->space[t] = scope->space[t]->peer;
		freeSymbol(prg);
	} else {
		Symbol nxt = scope->space[t];
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
	scope= moduleIndex[(int)(*name)][(int)(*(name+1))];
	while(scope != NULL){
			if( name == scope->name )
					return scope;
			scope= scope->link;
	}
	/* default is always matched with current */
	if( def->name==NULL) return NULL;
	return def;
}
int isModuleDefined(Module scope, str name){
	if( name==NULL || scope==NULL) return FALSE;
	if( name == scope->name) return TRUE;
	scope= moduleIndex[(int)(*name)][(int)(*(name+1))];
	while(scope != NULL){
			if( name == scope->name )
					return TRUE;
			scope= scope->link;
	}
	return FALSE;
}
/*
 * The routine findSymbolInModule starts at a MAL scope level and searches
 * an element amongst the peers. 
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
	s= v->space[(int)(*fcn)];
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

