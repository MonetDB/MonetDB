/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) M. L. Kersten
 * All symbols are collected in modules. Modules are either global
 * or private for the user. The latter are known as 'user' module functions
 * and reside within the Client record.
 */

#include "monetdb_config.h"
#include "mal_module.h"
#include "mal_function.h"   /* for printFunction() */
#include "mal_namespace.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_listing.h"
#include "mal_private.h"

/*
 * Definition of a new module may interfere with concurrent actions.
 * A jump table is mainted to provide a quick start in the module
 * table to find the correct one. 
 *
 * All modules are persistent during a server session
 */
/* #define _DEBUG_MODULE_*/

#define MODULE_HASH_SIZE 1024
Module moduleIndex[MODULE_HASH_SIZE] = { NULL };

void
listModules(stream *out, Module s)
{
	while(s){
		mnstr_printf(out,"Unexpected module %s\n",  s->name);
		s= s->link;
	}
}

// perform sanity check on duplicate occurrences as well
void
dumpModules(stream *out)
{
	int i;
	Module s,n;
	for( i = 0; i< MODULE_HASH_SIZE; i++){
		s= moduleIndex[i];
		while(s){
			mnstr_printf(out,"[%d] module %s\n", i,  s->name);
			n = s->link;
			while(n){
				if( n == s)
					mnstr_printf(out,"ASSERTION error, double occurrence of symbol in symbol table\n");
				n= n->link;
			}
			s= s->link;
		}
	}
}

/* Remove all globally known functions */
void
mal_module_reset(void)
{
	int i;
	Module m;

#ifdef _DEBUG_MODULE_
	fprintf(stderr,"#et the globale module structure \n");
#endif
	for(i = 0; i < MODULE_HASH_SIZE; i++) {
		m= moduleIndex[i];
		moduleIndex[i] = 0;
		while(m) {
			Module next = m->link;
			freeModule(m);
			m = next;
		}
	}
}

static int getModuleIndex(str name) {
	return (int) (strHash(name) % MODULE_HASH_SIZE);
}

static void clrModuleIndex(Module cur){
	int index = getModuleIndex(cur->name);
	Module prev = NULL;
	Module m = moduleIndex[index];
	while(m) {
		if (m == cur) {
			if (!prev) {
				moduleIndex[index] = m->link;
			} else {
				prev->link = m->link;
			}
			return;
		}
		prev = m;
		m = m->link;
	}
}

static void addModuleToIndex(Module cur){
	int index = getModuleIndex(cur->name);
	cur->link = moduleIndex[index];
	moduleIndex[index] = cur;
}


Module getModule(str name) {
	int index = getModuleIndex(name);
	Module m = moduleIndex[index];
	while(m) {
		//if (strcmp(name, m->name) == 0) {
		if (name == m->name) {
			return m;
		}
		m = m->link;
	}
	return NULL;
}

void getModuleList(Module** out, int* length) {
	int i;
	int moduleCount = 0;
	int currentIndex = 0;
	for(i = 0; i < MODULE_HASH_SIZE; i++) {
		Module m = moduleIndex[i];
		while(m) {
			moduleCount++;
			m = m->link;
		}
	}
	*out = GDKzalloc(moduleCount * sizeof(Module));
	if (*out == NULL) {
		return;
	}
	*length = moduleCount;

	for(i = 0; i < MODULE_HASH_SIZE; i++) {
		Module m = moduleIndex[i];
		while(m) {
			(*out)[currentIndex++] = m;
			m = m->link;
		}
	}
}

void freeModuleList(Module* list) {
	GDKfree(list);
}

/*
 * Module scope management
 * It will contain the symbol table of all globally accessible functions.
 */
Module globalModule(str nme)
{	Module cur;

	// Global modules are not named 'user'
	assert (strcmp(nme, "user"));
#ifdef _DEBUG_MODULE_
	fprintf(stderr,"#create new global module %s\n",nme);
#endif
	nme = putName(nme);
	cur = (Module) GDKzalloc(sizeof(ModuleRecord));
	if (cur == NULL)
		return NULL;
	cur->name = nme;
	cur->link = NULL;
	cur->isAtomModule = FALSE;
	cur->space = (Symbol *) GDKzalloc(MAXSCOPE * sizeof(Symbol));
	if (cur->space == NULL) {
		GDKfree(cur);
		return NULL;
	}
	addModuleToIndex(cur);
	return cur;
}

/* Every client record has a private module name 'user' 
 * for keeping around non-shared functions */
Module userModule(void){
	Module cur;

	cur = (Module) GDKzalloc(sizeof(ModuleRecord));
	if (cur == NULL)
		return NULL;
	cur->name = putName("user");
	cur->link = NULL;
	cur->space = NULL;
	cur->isAtomModule = FALSE;
	cur->space = (Symbol *) GDKzalloc(MAXSCOPE * sizeof(Symbol));
	if (cur->space == NULL) {
		GDKfree(cur);
		return NULL;
	}
	return cur;
}
/*
 * The scope can be fixed. This is used by the parser.
 * Reading a module often calls for creation first.
 */
Module fixModule(str nme) {
	Module m;

	m = getModule(nme);
	if (m) return m;
	return globalModule(nme);
}
/*
 * The freeModule operation throws away a symbol without
 * concerns on it whereabouts in the scope structure.
 */
static void freeSubScope(Module scope)
{
	int i;
	Symbol s;

	if (scope->space == NULL) 
		return;
#ifdef _DEBUG_MODULE_
	fprintf(stderr,"#freeSubScope %s \n", scope->name);
#endif
	for(i = 0; i < MAXSCOPE; i++) {
		if( scope->space[i]){
			s= scope->space[i];
			scope->space[i] = NULL;
			freeSymbolList(s);
		}
	}
	GDKfree(scope->space);
	scope->space = 0;
}

void freeModule(Module m)
{
	Symbol s;

	if (m == NULL) 
		return;
	if ((s = findSymbolInModule(m, "epilogue")) != NULL) {
		InstrPtr pci = getInstrPtr(s->def,0);
		if (pci && pci->token == COMMANDsymbol && pci->argc == 1) {
			int ret = 0;

			assert(pci->fcn != NULL);
			(*pci->fcn)(&ret);
			(void)ret;
		}
	}
#ifdef _DEBUG_MODULE_
	fprintf(stderr,"#freeModue %s \n", m->name);
#endif
	freeSubScope(m);	
	if (strcmp(m->name, "user")) {
		clrModuleIndex(m);
	}
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

	assert(scope);
	sig = getSignature(prg);
#ifdef _DEBUG_MODULE_
	fprintf(stderr,"#insertSymbol: %s.%s in %s ", getModuleId(sig), getFunctionId(sig), scope->name);
#endif
	if(getModuleId(sig) && getModuleId(sig)!= scope->name){
		/* move the definition to the proper place */
		/* default scope is the last resort */
		c= findModule(scope,getModuleId(sig));
		if ( c )
			scope = c;
#ifdef _DEBUG_MODULE_
	fprintf(stderr," found alternative module %s ", scope->name);
#endif
	}
	t = getSymbolIndex(getFunctionId(sig));
	if( scope->space == NULL) {
		scope->space = (Symbol *) GDKzalloc(MAXSCOPE * sizeof(Symbol));
		if (scope->space == NULL)
			return;
	}
	assert(scope->space);
	if (scope->space[t] == prg){
		/* already known, last inserted */
#ifdef _DEBUG_MODULE_
	fprintf(stderr," unexpected double insert  ");
#endif
	} else {
		prg->peer= scope->space[t];
		scope->space[t] = prg;
		if( prg->peer &&
			idcmp(prg->name,prg->peer->name) == 0)
			prg->skip = prg->peer->skip;
		else
			prg->skip = prg->peer;
	}
	assert(prg != prg->peer);
#ifdef _DEBUG_MODULE_
	fprintf(stderr,"\n");
#endif
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
#ifdef _DEBUG_MODULE_
	fprintf(stderr,"#delete symbol %s.%s from %s\n", getModuleId(sig), getFunctionId(sig), prg->name);
#endif
	if (getModuleId(sig) && getModuleId(sig)!= scope->name ){
		/* move the definition to the proper place */
		/* default scope is the last resort */
		Module c= findModule(scope, getModuleId(sig));
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
	Module def = scope;
	Module m;
	if (name == NULL) return scope;

#ifdef _DEBUG_MODULE_
	fprintf(stderr,"Locate module %s in scope %s\n", name,scope->name);
#endif
	m = getModule(name);
	if (m) return m;

	/* default is always matched with current */
	if (def->name == NULL) return NULL;
	return def;
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
Symbol findSymbolInModule(Module v, str fcn) {
	Symbol s;
	if (v == NULL || fcn == NULL) return NULL;
#ifdef _DEBUG_MODULE_
	fprintf(stderr,"#find symbol %s in %s\n", fcn, v->name);
#endif
	s = v->space[(int)(*fcn)];
	while (s != NULL) {
		if (idcmp(s->name,fcn)==0) return s;
		s = s->skip;
	}
	return NULL;
}

Symbol findSymbol(Module usermodule, str mod, str fcn) {
	Module m = findModule(usermodule, mod);
	return findSymbolInModule(m, fcn);
}

