/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* Author(s) M.L. Kersten, N. Nes
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
static mel_init  mel_module_inits[MAX_MAL_MODULES] = {0};
static const char*mel_module_code[MAX_MAL_MODULES] = {0};

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
mal_module2(str name, mel_atom *atoms, mel_func *funcs, mel_init initfunc, const char *code)
{
	assert (mel_modules < MAX_MAL_MODULES);
	mel_module_name[mel_modules] = name;
	mel_module_atoms[mel_modules] = atoms;
	mel_module_funcs[mel_modules] = funcs;
	mel_module_inits[mel_modules] = initfunc;
	mel_module_code[mel_modules] = code;
	mel_modules++;
}

void
mal_module(str name, mel_atom *atoms, mel_func *funcs)
{
	assert (mel_modules < MAX_MAL_MODULES);
	mel_module_name[mel_modules] = name;
	mel_module_atoms[mel_modules] = atoms;
	mel_module_funcs[mel_modules] = funcs;
	mel_module_inits[mel_modules] = NULL;
	mel_module_code[mel_modules] = NULL;
	mel_modules++;
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
	for(; atoms && atoms->name[0]; atoms++) {
		int i = ATOMallocate(atoms->name);
		if (is_int_nil(i))
			throw(TYPE,"addAtom", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if (atoms->basetype[0]) {
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
	int tpe = TYPE_any;//, l;

#ifdef MEL_STR
	if (!a->type[0]) {
#else
	if (a->type == TYPE_any) {
#endif
		if (a->isbat)
			tpe = newBatType(tpe);
		if (a->nr > 0)
			setTypeIndex(tpe, a->nr);
	} else {
#ifdef MEL_STR
		tpe = getAtomIndex(a->type, strlen(a->type),-1);
#else
		tpe = a->type ;
#endif
		if (a->isbat)
			tpe = newBatType(tpe);
	}
	/*
	if (a->name){
		*idx = findVariableLength(mb, a->name, l = strlen(a->name));
		if( *idx != -1)
			throw(LOADER, "addFunctions", "Duplicate argument name %s", a->name);
		*idx = newVariable(mb, a->name, l, tpe);
	} else
	*/
		*idx = newTmpVariable(mb, tpe);
	return MAL_SUCCEED;
}

static str
addFunctions(mel_func *fcn){
	str msg = MAL_SUCCEED;
	str mod;
	int idx;
	Module c;
	Symbol s;
	MalBlkPtr mb;
	InstrPtr sig;

	for(; fcn && fcn->mod[0]; fcn++) {
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
		if (fcn->cname && fcn->cname[0])
			strcpy(mb->binding, fcn->cname);
		sig= newInstruction(mb, fcn->mod, fcn->fcn);
		sig->retc = 0;
		sig->argc = 0;
		sig->token = fcn->command?COMMANDsymbol:PATTERNsymbol;
		sig->fcn = (MALfcn)fcn->imp;
		if( fcn->unsafe)
			mb->unsafeProp = 1; 
		/* add the return variables */
		if(fcn->retc == 0){ 
			int idx = newTmpVariable(mb, TYPE_void);
			sig = pushReturn(mb, sig, idx);
			if (sig == NULL)
				throw(LOADER, "addFunctions", "Failed to create void return");
		}
		int i;
		for (i = 0; i<fcn->retc; i++ ){
			mel_arg *a = fcn->args+i;
			msg = makeArgument(mb, a, &idx);
			if( msg)
				return msg;
			sig = pushReturn(mb, sig, idx);
			if (sig == NULL)
				//throw(LOADER, "addFunctions", "Failed to keep argument name %s", a->name);
				throw(LOADER, "addFunctions", "Failed to keep argument %d", i);
			int tpe = TYPE_any;
			if (a->nr > 0) {
				if (a->isbat)
					tpe = newBatType(tpe);
                		setPolymorphic(sig, tpe, TRUE);
			}
			if (a->vargs) {
        			sig->varargs |= VARRETS;
                		setPolymorphic(sig, TYPE_any, TRUE);
			}
		}
		/* add the arguments */
		for (i = fcn->retc; i<fcn->argc; i++ ){
			mel_arg *a = fcn->args+i;
			msg = makeArgument(mb, a, &idx);
			if( msg)
				return msg;
			sig = pushArgument(mb, sig, idx);
			if (sig == NULL)
				//throw(LOADER, "addFunctions", "Failed to keep argument name %s", a->name);
				throw(LOADER, "addFunctions", "Failed to keep argument %d", i);
			int tpe = TYPE_any;
			if (a->nr > 0) {
				if (a->isbat)
					tpe = newBatType(tpe);
                		setPolymorphic(sig, tpe, TRUE);
			}
			if (a->vargs) {
        			sig->varargs |= VARARGS;
                		setPolymorphic(sig, TYPE_any, TRUE);
			}
		}
		assert(sig->retc > 0);
		pushInstruction(mb, sig);
		insertSymbol(c, s);
	}
	return msg;
}

static int
makeFuncArgument(MalBlkPtr mb, mel_func_arg *a)
{
	int tpe = TYPE_any;

	if (a->type == TYPE_any) {
		if (a->isbat)
			tpe = newBatType(tpe);
		if (a->nr > 0)
			setTypeIndex(tpe, a->nr);
	} else {
		tpe = a->type;;
		if (a->isbat)
			tpe = newBatType(tpe);
	}
	return newTmpVariable(mb, tpe);
}

int 
melFunction(bool command, char *mod, char *fcn, fptr imp, char *fname, bool unsafe, char *comment, int retc, int argc, ... )
{
	int i, idx;
	Module c;
	Symbol s;
	MalBlkPtr mb;
	InstrPtr sig;
	va_list va;

	va_start(va, argc);
	assert(mod);
	mod = putName(mod);
	c = getModule(mod);
	if (c == NULL) {
		if (globalModule(mod) == NULL)
			return MEL_ERR;
		c = getModule(mod);
	}

	s = newSymbol(fcn, command ? COMMANDsymbol:PATTERNsymbol );
	if (s == NULL)
		return MEL_ERR;
	mb = s->def;
	(void)comment;
	if (fname)
		strcpy(mb->binding, fname);
	if( mb == NULL)
		return MEL_ERR;
	sig = newInstruction(mb, mod, fcn);
	sig->retc = 0;
	sig->argc = 0;
	sig->token = command ? COMMANDsymbol:PATTERNsymbol;
	sig->fcn = (MALfcn)imp;
	if (unsafe)
		mb->unsafeProp = 1; 
	/* add the return variables */
	if(retc == 0) { 
		idx = newTmpVariable(mb, TYPE_void);
		sig = pushReturn(mb, sig, idx);
		if (sig == NULL)
			return MEL_ERR;
	}

	for (i = 0; i<retc; i++ ){
		mel_func_arg a = va_arg(va, mel_func_arg);
		idx = makeFuncArgument(mb, &a);
		sig = pushReturn(mb, sig, idx);
		if (sig == NULL)
			return MEL_ERR;
		int tpe = TYPE_any;
		if (a.nr > 0) {
			if (a.isbat)
				tpe = newBatType(tpe);
               		setPolymorphic(sig, tpe, TRUE);
		}
		if (a.vargs) {
        		sig->varargs |= VARRETS;
               		setPolymorphic(sig, TYPE_any, TRUE);
		}
	}
	/* add the arguments */
	for (i = retc; i<argc; i++ ){
		mel_func_arg a = va_arg(va, mel_func_arg);
		idx = makeFuncArgument(mb, &a);
		sig = pushArgument(mb, sig, idx);
		if (sig == NULL)
			return MEL_ERR;
		int tpe = TYPE_any;
		if (a.nr > 0) {
			if (a.isbat)
				tpe = newBatType(tpe);
                	setPolymorphic(sig, tpe, TRUE);
		}
		if (a.vargs) {
        		sig->varargs |= VARARGS;
                	setPolymorphic(sig, TYPE_any, TRUE);
		}
	}
	assert(sig->retc > 0);
	pushInstruction(mb, sig);
	insertSymbol(c, s);
	va_end(va);
	return MEL_OK;
}

static str
malPrelude(Client c, int listing, int embedded)
{
	int i;
	str msg = MAL_SUCCEED;

	(void) listing;
	/* Add all atom definitions */
	for(i = 0; i<mel_modules; i++) {
		if (mel_module_atoms[i]) {
			msg = addAtom(mel_module_atoms[i]);
			if (msg)
				return msg;
		}
	}

	/* Add the signatures, where we now have access to all atoms */
	for(i = 0; i<mel_modules; i++) {
		if (!malLibraryEnabled(mel_module_name[i]))
			continue;
		if (mel_module_funcs[i]) {
			msg = addFunctions(mel_module_funcs[i]);
			if (!msg && mel_module_code[i]) /* some modules may also have some function definitions */
				msg = malIncludeString(c, mel_module_name[i], (str)mel_module_code[i], listing, NULL);
                       	if (msg)
                               	return msg;

			/* skip sql should be last to startup and mapi in the embedded version */
			if (strcmp(mel_module_name[i], "sql") == 0 || (embedded && strcmp(mel_module_name[i], "mapi") == 0)) 
				continue;
			if (!mel_module_inits[i])
				initModule(c, mel_module_name[i]);
		}
		if (mel_module_inits[i]) {
			msg = mel_module_inits[i]();
			if (msg)
				return msg;
			/* skip sql should be last to startup and mapi in the embedded version */
			if (strcmp(mel_module_name[i], "sql") == 0 || (embedded && strcmp(mel_module_name[i], "mapi") == 0)) 
				continue;
			initModule(c, mel_module_name[i]);
		}
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
		if (!malLibraryEnabled(modules[i]))
			continue;
		if ((msg = loadLibrary(modules[i], listing)) != NULL)
			return msg;
	}
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
