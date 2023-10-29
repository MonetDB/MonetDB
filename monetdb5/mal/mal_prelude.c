/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/* Author(s) M.L. Kersten, N. Nes
 * This module takes the statically defined modules, atoms, commands and patterns
 * and populate the internal structures.
 *
 */

#include "monetdb_config.h"
#include "mal_import.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_linker.h"			/* for loadModuleLibrary() */
#include "mal_scenario.h"
#include "mal_parser.h"
#include "mal_authorize.h"
#include "mal_private.h"
#include "mutils.h"

#include "mal_prelude.h"

#define MAX_MAL_MODULES 128
static int mel_modules = 0;
static struct mel_module {
	const char *name;
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
mal_module2(const char *name, mel_atom *atoms, mel_func *funcs,
			mel_init initfunc, const char *code)
{
	assert(mel_modules < MAX_MAL_MODULES);
	mel_module[mel_modules].name = name;
	mel_module[mel_modules].atoms = atoms;
	mel_module[mel_modules].funcs = funcs;
	mel_module[mel_modules].inits = initfunc;
	mel_module[mel_modules].code = code;
	mel_modules++;
}

void
mal_module(const char *name, mel_atom *atoms, mel_func *funcs)
{
	assert(mel_modules < MAX_MAL_MODULES);
	mel_module[mel_modules].name = name;
	mel_module[mel_modules].atoms = atoms;
	mel_module[mel_modules].funcs = funcs;
	mel_module[mel_modules].inits = NULL;
	mel_module[mel_modules].code = NULL;
	mel_modules++;
}

static char *
initModule(Client c, const char *name, const char *initpasswd)
{
	char *msg = MAL_SUCCEED;

	if (!getName(name))
		return msg;
	if ((name = putName(name)) == NULL)
		throw(LOADER, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	Module m = getModule(name);
	if (m) {					/* run prelude */
		const char *prelude = putName("prelude");
		if (prelude == NULL)
			throw(LOADER, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		Symbol s = findSymbolInModule(m, prelude);

		if (s) {
			InstrPtr pci = getInstrPtr(s->def, 0);

			if (pci && pci->token == COMMANDsymbol && pci->argc == 1) {
				int ret = 0;

				assert(pci->fcn != NULL);
				msg = (*(str (*)(int *)) pci->fcn) (&ret);
				(void) ret;
			} else if (pci && pci->token == PATTERNsymbol) {
				void *mb = NULL;
				assert(pci->fcn != NULL);
				if (strcmp(name, "sql") == 0) {
					/* HACK ALERT: temporarily use sqlcontext to pass
					 * the initial password to the prelude function */
					assert(c->sqlcontext == NULL);
					c->sqlcontext = (void *) initpasswd;
					/* HACK ALERT: use mb (MalBlkPtr) to pass revision
					 * string in order to check that in the callee */
					mb = (void *) mercurial_revision();
				}
				msg = (*(str (*)(Client, MalBlkPtr, MalStkPtr, InstrPtr)) pci->
					   fcn) (c, mb, NULL, NULL);
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
addAtom(mel_atom *atoms)
{
	for (; atoms && atoms->name[0]; atoms++) {
		int i = ATOMallocate(atoms->name);
		if (is_int_nil(i))
			throw(TYPE, __func__, GDK_EXCEPTION);
		if (atoms->basetype[0]) {
			int tpe = ATOMindex(atoms->basetype);
			if (tpe < 0)
				throw(TYPE, __func__, TYPE_NOT_SUPPORTED);
			BATatoms[i] = BATatoms[tpe];
			strcpy_len(BATatoms[i].name, atoms->name, sizeof(BATatoms[i].name));
			BATatoms[i].storage = ATOMstorage(tpe);
		} else {				/* cannot overload void atoms */
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
			const void *atmnull = (*atoms->null) ();

			BATatoms[i].atomNull = atmnull;
		}
		if (atoms->nequal)
			BATatoms[i].atomCmp = atoms->nequal;
		if (atoms->put)
			BATatoms[i].atomPut = atoms->put;
		if (atoms->storage)
			BATatoms[i].storage = (*atoms->storage) ();
		if (atoms->read)
			BATatoms[i].atomRead = atoms->read;
		if (atoms->write)
			BATatoms[i].atomWrite = atoms->write;
	}
	return MAL_SUCCEED;
}

static str
makeArgument(MalBlkPtr mb, const mel_arg *a, int *idx)
{
	int tpe = TYPE_any;			//, l;

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
		tpe = getAtomIndex(a->type, strlen(a->type), -1);
#else
		tpe = a->type;
#endif
		if (a->isbat)
			tpe = newBatType(tpe) | mask;
	}
	/*
	  if (a->name) {
	  *idx = findVariableLength(mb, a->name, l = strlen(a->name));
	  if (*idx != -1)
	  throw(LOADER, __func__, "Duplicate argument name %s", a->name);
	  *idx = newVariable(mb, a->name, l, tpe);
	  } else
	*/
	*idx = newTmpVariable(mb, tpe);
	if (*idx < 0) {
		char *msg = mb->errors;
		mb->errors = NULL;
		if (msg)
			return msg;
		throw(LOADER, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

static str
addFunctions(mel_func *fcn)
{
	str msg = MAL_SUCCEED;
	const char *mod;
	int idx;
	Module c;
	Symbol s;
	MalBlkPtr mb;
	InstrPtr sig;

	for (; fcn && fcn->mod[0]; fcn++) {
		assert(fcn->mod);
		mod = putName(fcn->mod);
		if (mod == NULL)
			throw(LOADER, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		c = getModule(mod);
		if (c == NULL && (c = globalModule(mod)) == NULL)
			throw(LOADER, __func__, "Module %s can not be created", fcn->mod);

		s = newSymbol(fcn->fcn, fcn->command ? COMMANDsymbol : PATTERNsymbol);
		if (s == NULL)
			throw(LOADER, __func__,
				  "Can not create symbol for %s.%s missing", fcn->mod,
				  fcn->fcn);
		mb = s->def;
		assert(mb);				/* if this is NULL, s should have been NULL */

		if (fcn->cname && fcn->cname[0])
			strcpy_len(mb->binding, fcn->cname, sizeof(mb->binding));
		/* keep the comment around, setting the static avoids freeing
		 * the string accidentally, saving on duplicate documentation in
		 * the code. */
		mb->statichelp = mb->help = fcn->comment;

		sig = newInstructionArgs(mb, mod, putName(fcn->fcn),
								 fcn->argc + (fcn->retc == 0));
		if (sig == NULL) {
			freeSymbol(s);
			throw(LOADER, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		sig->retc = 0;
		sig->argc = 0;
		sig->token = fcn->command ? COMMANDsymbol : PATTERNsymbol;
		sig->fcn = fcn->imp;
		if (fcn->unsafe)
			mb->unsafeProp = 1;

		/* add the return variables */
		if (fcn->retc == 0) {
			int idx = newTmpVariable(mb, TYPE_void);
			if (idx < 0) {
				freeInstruction(sig);
				freeSymbol(s);
				throw(LOADER, __func__, MAL_MALLOC_FAIL);
			}
			sig = pushReturn(mb, sig, idx);
		}
		int i;
		for (i = 0; i < fcn->retc; i++) {
			const mel_arg *a = fcn->args + i;
			msg = makeArgument(mb, a, &idx);
			if (msg) {
				freeInstruction(sig);
				freeSymbol(s);
				return msg;
			}
			sig = pushReturn(mb, sig, idx);
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
		for (i = fcn->retc; i < fcn->argc; i++) {
			const mel_arg *a = fcn->args + i;
			msg = makeArgument(mb, a, &idx);
			if (msg) {
				freeInstruction(sig);
				freeSymbol(s);
				return msg;
			}
			sig = pushArgument(mb, sig, idx);
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
		if (mb->errors) {
			freeInstruction(sig);
			freeSymbol(s);
			msg = mb->errors;
			mb->errors = NULL;
			return msg;
		}
		assert(sig->retc > 0);
		pushInstruction(mb, sig);
		if (mb->errors) {
			freeSymbol(s);
			msg = mb->errors;
			mb->errors = NULL;
			return msg;
		}
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
melFunction(bool command, const char *mod, const char *fcn, MALfcn imp,
			const char *fname, bool unsafe, const char *comment, int retc,
			int argc, ...)
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
	if (c == NULL && (c = globalModule(mod)) == NULL)
		return MEL_ERR;

	s = newSymbol(fcn, command ? COMMANDsymbol : PATTERNsymbol);
	if (s == NULL)
		return MEL_ERR;
	fcn = s->name;
	mb = s->def;
	(void) comment;
	if (fname)
		strcpy_len(mb->binding, fname, sizeof(mb->binding));
	if (mb == NULL) {
		freeSymbol(s);
		return MEL_ERR;
	}
	sig = newInstructionArgs(mb, mod, fcn, argc + (retc == 0));
	if (sig == NULL) {
		freeSymbol(s);
		return MEL_ERR;
	}
	sig->retc = 0;
	sig->argc = 0;
	sig->token = command ? COMMANDsymbol : PATTERNsymbol;
	sig->fcn = imp;
	if (unsafe)
		mb->unsafeProp = 1;
	/* add the return variables */
	if (retc == 0) {
		idx = newTmpVariable(mb, TYPE_void);
		if (idx < 0) {
			freeInstruction(sig);
			freeSymbol(s);
			return MEL_ERR;
		}
		sig = pushReturn(mb, sig, idx);
	}

	va_start(va, argc);
	for (i = 0; i < retc; i++) {
		mel_func_arg a = va_arg(va, mel_func_arg);
		idx = makeFuncArgument(mb, &a);
		if (idx < 0) {
			freeInstruction(sig);
			freeSymbol(s);
			va_end(va);
			return MEL_ERR;
		}
		sig = pushReturn(mb, sig, idx);
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
	for (i = retc; i < argc; i++) {
		mel_func_arg a = va_arg(va, mel_func_arg);
		idx = makeFuncArgument(mb, &a);
		if (idx < 0) {
			freeInstruction(sig);
			freeSymbol(s);
			va_end(va);
			return MEL_ERR;
		}
		sig = pushArgument(mb, sig, idx);
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
malPrelude(Client c, int listing, int *sql, int *mapi)
{
	int i;
	str msg = MAL_SUCCEED;

	(void) listing;
	/* Add all atom definitions */
	for (i = 0; i < mel_modules; i++) {
		if (mel_module[i].atoms) {
			msg = addAtom(mel_module[i].atoms);
			if (msg)
				return msg;
		}
	}

	/* Add the signatures, where we now have access to all atoms */
	for (i = 0; i < mel_modules; i++) {
		const char *name = putName(mel_module[i].name);
		if (!malLibraryEnabled(name))
			continue;
		if (mel_module[i].funcs) {
			msg = addFunctions(mel_module[i].funcs);
			if (!msg && mel_module[i].code) /* some modules may also have some function definitions */
				msg = malIncludeString(c, name, (str) mel_module[i].code, listing, NULL);
			if (msg)
				return msg;

			/* mapi should be last, and sql last before mapi */
			if (strcmp(name, "sql") == 0) {
				*sql = i;
				continue;
			}
			if (strcmp(name, "mapi") == 0) {
				*mapi = i;
				continue;
			}
			if (!mel_module[i].inits) {
				msg = initModule(c, name, NULL);
				if (msg)
					return msg;
			}
		}
		if (mel_module[i].inits) {
			/* mapi should be last, and sql last before mapi */
			if (strcmp(name, "sql") == 0 || strcmp(name, "mapi") == 0)
				continue;
			msg = mel_module[i].inits();
			if (msg)
				return msg;
		}
	}
	return MAL_SUCCEED;
}

str
malIncludeModules(Client c, char *modules[], int listing, bool no_mapi_server,
				  const char *initpasswd)
{
	str msg;
	int sql = -1, mapi = -1;

	for (int i = 0; modules[i]; i++) {
		/* load library */
		if (!malLibraryEnabled(modules[i]))
			continue;
		if ((msg = loadLibrary(modules[i], listing)) != NULL)
			return msg;
	}
	/* load the mal code for these modules and execute preludes */
	if ((msg = malPrelude(c, listing, &sql, &mapi)) != NULL)
		return msg;
	/* mapi should be last, and sql last before mapi */
	if (sql >= 0) {
		if (mel_module[sql].inits)
			msg = mel_module[sql].inits();
		else
			msg = initModule(c, "sql", initpasswd);
		if (msg)
			return msg;
	}
	if (!no_mapi_server && mapi >= 0 && initpasswd == NULL) {
		if (mel_module[mapi].inits)
			msg = mel_module[mapi].inits();
		else
			msg = initModule(c, "mapi", NULL);
		if (msg)
			return msg;
	}
	return MAL_SUCCEED;
}
