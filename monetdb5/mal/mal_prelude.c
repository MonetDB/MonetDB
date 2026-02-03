/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
static int mel_modules = 0, mel_modules_loaded = 0;
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
	mel_modules_loaded = 0;
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
	mal_module2(name, atoms, funcs, NULL, NULL);
}

static char *
initModule(Client c, const char *name, const char *initpasswd)
{
	char *msg = MAL_SUCCEED;

	assert(getName(name) == name);
	Module m = getModule(name);
	if (m) {					/* run prelude */
		const char *prelude = putName("prelude");
		if (prelude == NULL)
			throw(LOADER, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		Symbol s = findSymbolInModule(m, prelude);

		if (s) {
			if (s && s->kind == COMMANDsymbol && s->func && s->func->argc == 1) {
				int ret = 0;

				assert(s->func != NULL);
				msg = (*(str (*)(Client, int *)) s->func->imp) (c, &ret);
				(void) ret;
			} else if (s && s->kind == PATTERNsymbol) {
				void *mb = NULL;
				assert(s->func->fcn != NULL);
				if (strcmp(name, "sql") == 0) {
					/* HACK ALERT: temporarily use sqlcontext to pass
					 * the initial password to the prelude function */
					assert(c->sqlcontext == NULL);
					c->sqlcontext = (void *) initpasswd;
					/* HACK ALERT: use mb (MalBlkPtr) to pass revision
					 * string in order to check that in the callee */
					mb = (void *) mercurial_revision();
				}
				msg = (*(str (*)(Client, MalBlkPtr, MalStkPtr, InstrPtr)) s->func->pimp) (c, mb, NULL, NULL);
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
			strtcpy(BATatoms[i].name, atoms->name, sizeof(BATatoms[i].name));
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
		if (atoms->equal)
			BATatoms[i].atomEqual = atoms->equal;
		if (atoms->fromstr)
			BATatoms[i].atomFromStr = atoms->fromstr;
		if (atoms->tostr)
			BATatoms[i].atomToStr = atoms->tostr;
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

static malType
makeMalType(mel_arg *a)
{
	malType tpe = TYPE_any;

	if (!a->type[0]) {
		a->typeid = tpe;
		if (a->isbat)
			tpe = newBatType(tpe);
		if (a->nr > 0)
			setTypeIndex(tpe, a->nr);
	} else {
		tpe = getAtomIndex(a->type, strlen(a->type), -1);
		a->typeid = tpe;
		if (a->isbat)
			tpe = newBatType(tpe);
	}
	if (a->opt == 1)
		setOptBat(tpe);
	return tpe;
}

void
setPoly(mel_func *f, malType tpe)
{
	if (isAnyExpression(tpe) || tpe == TYPE_any || getOptBat(tpe)) {
		unsigned int index = getTypeIndex(tpe);
		if (index + 1 > f->poly)
			f->poly = index + 1;
	}
}

static str
addFunctions(mel_func *fcn)
{
	str msg = MAL_SUCCEED;
	Module c;
	Symbol s;

	for (; fcn && fcn->mod; fcn++) {
		const char *mod = putName(fcn->mod);
		if (mod == NULL)
			throw(LOADER, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		c = getModule(mod);
		if (c == NULL && (c = globalModule(mod)) == NULL)
			throw(LOADER, __func__, "Module %s can not be created", mod);

		s = newSymbol(fcn->fcn, (fcn->command) ? COMMANDsymbol : PATTERNsymbol);
		if (s == NULL)
			throw(LOADER, __func__, "Can not create symbol for %s.%s missing", mod,
				  fcn->fcn);
		s->def = NULL;
		s->func = fcn;
		s->allocated = false;

		/* add the return variables */
		unsigned int i;
		for (i = 0; i < fcn->retc; i++) {
			mel_arg *a = fcn->args + i;
			malType tpe = makeMalType(a);
			if (a->nr > 0 || a->opt)
				setPoly(fcn, tpe);
			if (a->vargs) {
				fcn->vrets = true;
				setPoly(fcn, TYPE_any);
			}
			if (a->opt && fcn->command)
				throw(LOADER, __func__, "Can not have command symbol with dynamic types, ie bat vs scalar in %s.%s", mod, fcn->fcn);
		/*
			if (a->nr >= 2)
				printf("%s.%s\n", mod, fcn->fcn);
				*/
		}
		/* add the arguments */
		for (i = fcn->retc; i < fcn->argc; i++) {
			mel_arg *a = fcn->args + i;
			malType tpe = makeMalType(a);

			if (a->nr > 0  || a->opt)
				setPoly(fcn, tpe);
			if (a->vargs) {
				fcn->vargs = true;
				setPoly(fcn, TYPE_any);
			}
			if (a->opt && fcn->command)
				throw(LOADER, __func__, "Can not have command symbol with dynamic types, ie bat vs scalar in %s.%s", mod, fcn->fcn);
		/*
			if (a->nr >= 2)
				printf("%s.%s\n", mod, fcn->fcn);
				*/
		}
		insertSymbol(c, s);
	}
	return msg;
}

static void
argCopy( mel_arg *ap, mel_func_arg *a)
{
	ap->typeid = a->type;
	ap->nr = a->nr;
	ap->isbat = a->isbat;
	ap->vargs = a->vargs;
	ap->opt = a->opt;
	if (a->type != TYPE_any)
		strcpy(ap->type, BATatoms[a->type].name);
	else
		ap->type[0] = 0;
}

int
melFunction(bool command, const char *mod, const char *fcn, MALfcn imp,
			const char *fname, bool unsafe, const char *comment, int retc,
			int argc, ...)
{
	int i;
	Module c;
	Symbol s;
	mel_func *f = NULL;
	va_list va;

	assert(mod && fcn);
	mod = putName(mod);
	fcn = putName(fcn);
	c = getModule(mod);
	if (c == NULL && (c = globalModule(mod)) == NULL)
		return MEL_ERR;

	s = newSymbol(fcn, command ? COMMANDsymbol : PATTERNsymbol);
	if (s == NULL)
		return MEL_ERR;
	fcn = s->name;
	s->allocated = true;

	f = GDKmalloc(sizeof(mel_func) + argc * sizeof(mel_arg));
	if (f == NULL) {
		freeSymbol(s);
		return MEL_ERR;
	}
	mel_arg *args = (mel_arg *) (f + 1);
	*f = (mel_func) {
		.mod = mod,
		.fcn = fcn,
		.command = command,
		.unsafe = unsafe,
		.vargs = 0,
		.vrets = 0,
		.poly = 0,
		.retc = retc,
		.argc = argc,
		.args = args,
		.imp = imp,
		.comment = comment,
		.cname = fname,
	};
	s->def = NULL;
	s->func = f;

	va_start(va, argc);
	for (i = 0; i < retc; i++) {
		mel_func_arg a = va_arg(va, mel_func_arg);
		mel_arg *ap = f->args+i;
		argCopy(ap, &a);
		malType tpe = makeMalType(ap);
		if (a.nr > 0 || a.opt)
			setPoly(f, tpe);
		if (a.vargs) {
			f->vrets = true;
			setPoly(f, TYPE_any);
		}
		if (a.opt && f->command) {
			va_end(va);
			freeSymbol(s);		/* also frees f and args */
			return MEL_ERR;
		}
		/*
		if (a.nr >= 2)
			printf("%s.%s\n", f->mod, f->fcn);
			*/
	}
	/* add the arguments */
	for (i = retc; i < argc; i++) {
		mel_func_arg a = va_arg(va, mel_func_arg);
		mel_arg *ap = f->args+i;
		argCopy(ap, &a);
		malType tpe = makeMalType(ap);
		if (a.nr > 0 || a.opt)
			setPoly(f, tpe);
		if (a.vargs) {
			f->vargs = true;
			setPoly(f, TYPE_any);
		}
		if (a.opt && f->command) {
			va_end(va);
			freeSymbol(s);		/* also frees f and args */
			return MEL_ERR;
		}
		/*
		if (a.nr >= 2)
			printf("%s.%s\n", f->mod, f->fcn);
			*/
	}
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
	for (i = mel_modules_loaded; i < mel_modules; i++) {
		if (mel_module[i].atoms) {
			msg = addAtom(mel_module[i].atoms);
			if (msg)
				return msg;
		}
	}

	/* Add the signatures, where we now have access to all atoms */
	for (i = mel_modules_loaded; i < mel_modules; i++) {
		const char *name = putName(mel_module[i].name);
		if (name == NULL)
			throw(LOADER, __func__, MAL_MALLOC_FAIL);
		if (!malLibraryEnabled(name))
			continue;
		if (mel_module[i].funcs) {
			msg = addFunctions(mel_module[i].funcs);
			if (!msg && mel_module[i].code) /* some modules may also have some function definitions */
				msg = malIncludeString(c, name, (str) mel_module[i].code, listing, NULL);
			if (msg)
				return msg;

			/* mapi should be last, and sql last before mapi */
			if (name == sqlRef) {
				*sql = i;
				continue;
			}
			if (name == mapiRef) {
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
			if (name == sqlRef || name == mapiRef)
				continue;
			msg = mel_module[i].inits();
			if (msg)
				return msg;
		}
	}
	mel_modules_loaded = mel_modules;
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
			msg = initModule(c, sqlRef, initpasswd);
		if (msg)
			return msg;
	}
	if (!no_mapi_server && mapi >= 0 && initpasswd == NULL) {
		if (mel_module[mapi].inits)
			msg = mel_module[mapi].inits();
		else
			msg = initModule(c, mapiRef, NULL);
		if (msg)
			return msg;
	}
	return MAL_SUCCEED;
}
