/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* Author(s) M.L. Kersten
 * This module takes the statically defined modules, atoms, commands and patterns
 * and populate the internal structures.
 *
*/

#include "monetdb_config.h"
#include "mal_import.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_linker.h"		/* for loadModuleLibrary() */
#include "mal_scenario.h"
#include "mal_parser.h"
#include "mal_authorize.h"
#include "mal_private.h"

#include "mal_prelude.h"

#define MAX_MAL_MODULES 128
static int mel_modules = 0;
static str mel_module_name[MAX_MAL_MODULES] = {0};
static mel_atom *mel_module_atoms[MAX_MAL_MODULES] = {0};
static mel_func *mel_module_funcs[MAX_MAL_MODULES] = {0};

/* the MAL modules contains text to be parsed */
static int mal_modules = 0;
static str mal_module_name[MAX_MAL_MODULES] = {0};
static unsigned char *mal_module_code[MAX_MAL_MODULES] = {0};

int
mal_startup(void)
{
	/* clean up the MAL internal structures before restart */
	return 0;
}

/* all MAL related functions register themselves 
* the order in which these registrations happen is significant
* because there may be dependencies among the definitions.
* For example, you better know the atoms before you use them
*/

void
mal_module(str name, mel_atom *atoms, mel_func *funcs)
{
	assert (mel_modules < MAX_MAL_MODULES);
	mel_module_name[mel_modules] = name;
	mel_module_atoms[mel_modules] = atoms;
	mel_module_funcs[mel_modules] = funcs;
	mel_modules++;
}

void
mal_register(str name, unsigned char *code)
{
	assert (mal_modules < MAX_MAL_MODULES);
	mal_module_name[mal_modules] = name;
	mal_module_code[mal_modules] = code;
	mal_modules++;
}


static void 
initModule(Client c, char *name) 
{
	if (!getName(name))
		return;
	Module m = getModule(putName(name));
	if (m) { /* run prelude */
		Symbol s = findSymbolInModule(m, putName("prelude"));

		if (s) {
			InstrPtr pci = getInstrPtr(s->def, 0);

               		if (pci && pci->token == COMMANDsymbol && pci->argc == 1) {
                       		int ret = 0;

                       		assert(pci->fcn != NULL);
                       		(*pci->fcn)(&ret);
                       		(void)ret;
               		} else if (pci && pci->token == PATTERNsymbol) {
                       		assert(pci->fcn != NULL);
                       		(*pci->fcn)(c, NULL, NULL, NULL);
               		}
		}
	}
}

/*
 * The statically description of the MAL structures call for a translation into
 * their underlying structure.
 */
static str
addAtom( mel_atom *atoms)
{
	for(; atoms && atoms->name; atoms++) {
		int i = ATOMallocate(atoms->name);
		if (is_int_nil(i))
			throw(TYPE,"addAtom", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if (atoms->basetype) {
			int tpe = ATOMindex(atoms->basetype);
			if (tpe < 0)
				throw(TYPE,"addAtom", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			BATatoms[i] = BATatoms[tpe];
			strcpy_len(BATatoms[i].name, atoms->name, sizeof(BATatoms[i].name));
			BATatoms[i].storage = ATOMstorage(tpe);
		} else 	{ /* cannot overload void atoms */
			BATatoms[i].storage = i;
			BATatoms[i].linear = false;
		}
		if (atoms->del)
			BATatoms[i].atomDel = (void (*)(Heap *, var_t *))atoms->del;
		if (atoms->cmp) {
			BATatoms[i].atomCmp = (int (*)(const void *, const void *))atoms->cmp;
			BATatoms[i].linear = true;
		}
		if (atoms->fromstr)
			BATatoms[i].atomFromStr = (ssize_t (*)(const char *, size_t *, ptr *, bool))atoms->fromstr;
		if (atoms->tostr)
			BATatoms[i].atomToStr = (ssize_t (*)(str *, size_t *, const void *, bool))atoms->tostr;
		if (atoms->fix)
			BATatoms[i].atomFix = (gdk_return (*)(const void *))atoms->fix;
		if (atoms->unfix)
			BATatoms[i].atomUnfix = (gdk_return (*)(const void *))atoms->unfix;
		if (atoms->heap) {
			BATatoms[i].size = sizeof(var_t);
			assert_shift_width(ATOMelmshift(ATOMsize(i)), ATOMsize(i));
			BATatoms[i].atomHeap = (void (*)(Heap *, size_t))atoms->heap;
		}
		if (atoms->hash)
			BATatoms[i].atomHash = (BUN (*)(const void *))atoms->hash;
		if (atoms->length)
			BATatoms[i].atomLen = (size_t (*)(const void *))atoms->length;
		if (atoms->null) {
			const void *atmnull = ((const void *(*)(void))atoms->null)();

			BATatoms[i].atomNull = atmnull;
		}
		if (atoms->nequal)
			BATatoms[i].atomCmp = (int (*)(const void *, const void *))atoms->nequal;
		if (atoms->put)
			BATatoms[i].atomPut = (var_t (*)(Heap *, var_t *, const void *))atoms->put;
		if (atoms->storage)
			BATatoms[i].storage = (*(int (*)(void))atoms->storage)();
		if (atoms->read)
			BATatoms[i].atomRead = (void *(*)(void *, stream *, size_t))atoms->read;
		if (atoms->write)
			BATatoms[i].atomWrite = (gdk_return (*)(const void *, stream *, size_t))atoms->write;
	}
	return MAL_SUCCEED;
}

static str
makeArgument(MalBlkPtr mb, mel_arg *a, int *idx)
{
	int tpe, l;

	tpe = getAtomIndex(a->type, strlen(a->type),-1);
	if (a->isbat)
		tpe = newBatType(tpe);

	if( a->name){
		*idx = findVariableLength(mb, a->name, l = strlen(a->name));
		if( *idx != -1)
			throw(LOADER, "addFunctions", "Duplicate argument name %s", a->name);
		*idx = newVariable(mb, a->name, l, tpe);
	} else
		*idx = newTmpVariable(mb, tpe);
	return MAL_SUCCEED;
}

static str
addFunctions(mel_func *fcn){
	str msg = MAL_SUCCEED;
	mel_arg *a;
	str mod;
	int idx;
	Module c;
	Symbol s;
	MalBlkPtr mb;
	InstrPtr sig;

	for(; fcn && fcn->mod; fcn++) {
		assert(fcn->mod);
		mod = putName(fcn->mod);
		c = getModule(mod);
		if( c == NULL){
			if (globalModule(mod=putName(fcn->mod)) == NULL)
				throw(LOADER, "addFunctions", "Module %s can not be created", fcn->mod);
			c = getModule(mod);
		}

		s = newSymbol(fcn->fcn, fcn->command ? COMMANDsymbol: PATTERNsymbol );
		if ( s == NULL)
			throw(LOADER, "addFunctions", "Can not create symbol for %s.%s missing", fcn->mod, fcn->fcn);
		mb = s->def;
		if( mb == NULL)
			throw(LOADER, "addFunctions", "Can not create program block for %s.%s missing", fcn->mod, fcn->fcn);
		sig= newInstruction(mb, fcn->mod, fcn->fcn);
		sig->retc = 0;
		sig->argc = 0;
		sig->token = fcn->command?COMMANDsymbol:PATTERNsymbol;
		sig->fcn = (MALfcn)fcn->imp;
		if( fcn->unsafe)
			mb->unsafeProp = 0; 
		/* add the return variables */
		for ( a = fcn->res; a->type && a; a++){
			msg = makeArgument(mb, a, &idx);
			if( msg)
				return msg;
			sig = pushReturn(mb, sig, idx);
			if (sig == NULL)
				throw(LOADER, "addFunctions", "Failed to keep argument name %s", a->name);
		}
		/* add the arguments */
		for ( a = fcn->args; a->name && a; a++){
			msg = makeArgument(mb, a, &idx);
			if( msg)
				return msg;
			sig = pushArgument(mb, sig, idx);
			if (sig == NULL)
				throw(LOADER, "addFunctions", "Failed to keep argument name %s", a->name);
		}
		if(sig->retc == 0 || getArg(sig,0) < 0){
			sig = pushReturn(mb, sig, TYPE_void);
			if (sig == NULL)
				throw(LOADER, "addFunctions", "Failed to keep argument name %s", a->name);
		}
		pushInstruction(mb, sig);
		insertSymbol(c, s);
	}
	return msg;
}

static str
malPrelude(Client c, int listing, int embedded)
{
	int i;
	str msg = MAL_SUCCEED;

	(void) listing;
	/* Add all atom definitions */
	for(i = 0; i<mel_modules; i++) {
		if (embedded && strcmp(mel_module_name[i], "mal_mapi") == 0) /* skip mapi in the embedded version */
			continue;

		if (mel_module_atoms[i]) {
			msg = addAtom(mel_module_atoms[i]);
			if (msg)
				return msg;
		}
	}

	/* Add the signatures, where we now have access to all atoms */
	for(i = 0; i<mel_modules; i++) {
		if (mel_module_funcs[i]) {
			msg = addFunctions(mel_module_funcs[i]);
			if (msg)
				return msg;
			initModule(c, mel_module_name[i]);
		}
	}

	/* Once we have all modules loaded, we should execute their prelude function for further initialization*/
	for(i = 0; i<mal_modules; i++) {
		if (embedded && strcmp(mal_module_name[i], "mal_mapi") == 0) 
			continue;
		if ( mal_module_code[i]){
			msg = malIncludeString(c, mal_module_name[i], (str)mal_module_code[i], listing);
			if (msg)
				return msg;
		}
	}
	/* execute preludes */
	for(i = 0; i<mal_modules; i++) {
		if (strcmp(mal_module_name[i], "sql") == 0) /* skip sql should be last to startup */
			continue;
		initModule(c, mal_module_name[i]);
	}
	return MAL_SUCCEED;
}

str
malIncludeModules(Client c, char *modules[], int listing, int embedded)
{
	int i;
	str msg;
	
	for(i = 0; modules[i]; i++) {
		/* load library */
		if ((msg = loadLibrary(modules[i], listing)) != NULL)
			return msg;
	}
	/* only when the libraries are loaded the code is dynamically added, the second call this
	 * isn't done. So here the mal_modules counter is reset for this 
	 */

	/* load the mal code for these modules and execute preludes */
	if ((msg = malPrelude(c, listing, embedded)) != NULL)
		return msg;
	for(int i = 0; modules[i]; i++) {
		if (strcmp(modules[i], "sql") == 0) { /* start now */
			initModule(c, modules[i]);
			break;
		}
	}
	return MAL_SUCCEED;
}

