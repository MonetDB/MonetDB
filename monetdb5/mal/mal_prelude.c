/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
static struct mel_module {
	char *name;
	mel_atom *atoms;
	mel_func *funcs;
	mel_init inits;
	const char *code;
} mel_module[MAX_MAL_MODULES];

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
	mel_module[mel_modules].name = name;
	mel_module[mel_modules].atoms = atoms;
	mel_module[mel_modules].funcs = funcs;
	mel_module[mel_modules].inits = initfunc;
	mel_module[mel_modules].code = code;
	mel_modules++;
}

void
mal_module(str name, mel_atom *atoms, mel_func *funcs)
{
	assert (mel_modules < MAX_MAL_MODULES);
	mel_module[mel_modules].name = name;
	mel_module[mel_modules].atoms = atoms;
	mel_module[mel_modules].funcs = funcs;
	mel_module[mel_modules].inits = NULL;
	mel_module[mel_modules].code = NULL;
	mel_modules++;
}

static char *
initModule(Client c, char *name)
{
	char *msg = MAL_SUCCEED;

	if (!getName(name))
		return msg;
	Module m = getModule(putName(name));
	if (m) { /* run prelude */
		Symbol s = findSymbolInModule(m, putName("prelude"));

		if (s) {
			InstrPtr pci = getInstrPtr(s->def, 0);

			if (pci && pci->token == COMMANDsymbol && pci->argc == 1) {
				int ret = 0;

				assert(pci->fcn != NULL);
				msg = (*pci->fcn)(&ret);
				(void)ret;
			} else if (pci && pci->token == PATTERNsymbol) {
				assert(pci->fcn != NULL);
				msg = (*pci->fcn)(c, NULL, NULL, NULL);
			}
		}
	}
	return msg;
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
			BATatoms[i].atomDel = atoms->del;
		if (atoms->cmp) {
			BATatoms[i].atomCmp = atoms->cmp;
			BATatoms[i].linear = true;
		}
		if (atoms->fromstr)
			BATatoms[i].atomFromStr = atoms->fromstr;
		if (atoms->tostr)
			BATatoms[i].atomToStr = atoms->tostr;
		if (atoms->fix)
			BATatoms[i].atomFix = atoms->fix;
		if (atoms->unfix)
			BATatoms[i].atomUnfix = atoms->unfix;
		if (atoms->heap) {
			BATatoms[i].size = sizeof(var_t);
			assert_shift_width(ATOMelmshift(ATOMsize(i)), ATOMsize(i));
			BATatoms[i].atomHeap = atoms->heap;
		}
		if (atoms->hash)
			BATatoms[i].atomHash = atoms->hash;
		if (atoms->length)
			BATatoms[i].atomLen = atoms->length;
		if (atoms->null) {
			const void *atmnull = (*atoms->null)();

			BATatoms[i].atomNull = atmnull;
		}
		if (atoms->nequal)
			BATatoms[i].atomCmp = atoms->nequal;
		if (atoms->put)
			BATatoms[i].atomPut = atoms->put;
		if (atoms->storage)
			BATatoms[i].storage = (*atoms->storage)();
		if (atoms->read)
			BATatoms[i].atomRead = atoms->read;
		if (atoms->write)
			BATatoms[i].atomWrite = atoms->write;
	}
	return MAL_SUCCEED;
}

static str
makeArgument(MalBlkPtr mb, mel_arg *a, int *idx)
{
	int tpe = TYPE_any;//, l;

	if (
#ifdef MEL_STR
		!a->type[0]
#else
		a->type == TYPE_any
#endif
		) {
		if (a->isbat)
			tpe = newBatType(tpe);
		if (a->nr > 0)
			setTypeIndex(tpe, a->nr);
	} else {
		int mask = 0;
#ifdef MEL_STR
		tpe = getAtomIndex(a->type, strlen(a->type),-1);
#else
		tpe = a->type ;
#endif
		if (a->isbat)
			tpe = newBatType(tpe) | mask;
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
	const char *mod;
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
		sig= newInstructionArgs(mb, fcn->mod, fcn->fcn, fcn->argc + (fcn->retc == 0));
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
		tpe = a->type;
		if (a->isbat)
			tpe = newBatType(tpe);
	}
	return newTmpVariable(mb, tpe);
}

int
melFunction(bool command, const char *mod, char *fcn, fptr imp, char *fname, bool unsafe, char *comment, int retc, int argc, ... )
{
	int i, idx;
	Module c;
	Symbol s;
	MalBlkPtr mb;
	InstrPtr sig;
	va_list va;

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
	if (mb == NULL) {
		freeSymbol(s);
		return MEL_ERR;
	}
	sig = newInstructionArgs(mb, mod, fcn, argc + (retc == 0));
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
		if (idx < 0 || sig == NULL) {
			freeInstruction(sig);
			freeSymbol(s);
			return MEL_ERR;
		}
	}

	va_start(va, argc);
	for (i = 0; i<retc; i++ ){
		mel_func_arg a = va_arg(va, mel_func_arg);
		idx = makeFuncArgument(mb, &a);
		sig = pushReturn(mb, sig, idx);
		if (idx < 0 || sig == NULL) {
			freeInstruction(sig);
			freeSymbol(s);
			va_end(va);
			return MEL_ERR;
		}
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
		if (idx < 0 || sig == NULL) {
			freeInstruction(sig);
			freeSymbol(s);
			va_end(va);
			return MEL_ERR;
		}
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
malPrelude(Client c, int listing, int no_mapi_server)
{
	int i;
	str msg = MAL_SUCCEED;

	(void) listing;
	/* Add all atom definitions */
	for(i = 0; i<mel_modules; i++) {
		if (mel_module[i].atoms) {
			msg = addAtom(mel_module[i].atoms);
			if (msg)
				return msg;
		}
	}

	/* Add the signatures, where we now have access to all atoms */
	for(i = 0; i<mel_modules; i++) {
		if (!malLibraryEnabled(mel_module[i].name))
			continue;
		if (mel_module[i].funcs) {
			msg = addFunctions(mel_module[i].funcs);
			if (!msg && mel_module[i].code) /* some modules may also have some function definitions */
				msg = malIncludeString(c, mel_module[i].name, (str)mel_module[i].code, listing, NULL);
			if (msg)
				return msg;

			/* skip sql should be last to startup and mapi if configured without mapi server */
			if (strcmp(mel_module[i].name, "sql") == 0 || (no_mapi_server && strcmp(mel_module[i].name, "mapi") == 0))
				continue;
			if (!mel_module[i].inits) {
				msg = initModule(c, mel_module[i].name);
				if (msg)
					return msg;
			}
		}
		if (mel_module[i].inits) {
			msg = mel_module[i].inits();
			if (msg)
				return msg;
			/* skip sql should be last to startup and mapi if configured without mapi server */
			if (strcmp(mel_module[i].name, "sql") == 0 || (no_mapi_server && strcmp(mel_module[i].name, "mapi") == 0))
				continue;
			msg = initModule(c, mel_module[i].name);
			if (msg)
				return msg;
		}
	}
	return MAL_SUCCEED;
}

str
malIncludeModules(Client c, char *modules[], int listing, int no_mapi_server)
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
	if ((msg = malPrelude(c, listing, no_mapi_server)) != NULL)
		return msg;
	for(int i = 0; modules[i]; i++) {
		if (strcmp(modules[i], "sql") == 0) { /* start now */
			initModule(c, modules[i]);
			break;
		}
	}
	return MAL_SUCCEED;
}
