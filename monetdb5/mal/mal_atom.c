/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * (author) M.L.Kersten
 * Every MAL command introduced in an atom module should be checked
 * to detect overloading of a predefined function.
 * Subsequently, we update the GDK atom structure.
 * The function signatures should be parameter-less, which
 * enables additional functions with the same name to appear
 * as ordinary mal operators.
 *
 * A few fields are set only once, at creation time.
 * They should be implemented with parameter-less functions.
 */
#include "monetdb_config.h"
#include "mal_instruction.h"
#include "mal_atom.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_private.h"

str
malAtomProperty(mel_func *f)
{
	const char *name;
	int tpe;
	assert(f != 0);
	name = f->fcn;
	tpe = getAtomIndex(f->mod, strlen(f->mod), TYPE_any);
	if (tpe < 0 || tpe >= GDKatomcnt || tpe >= MAXATOMS)
		return MAL_SUCCEED;
	assert(f->imp != NULL);
	switch (name[0]) {
	case 'd':
		if (idcmp("del", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomDel = (void (*)(Heap *, var_t *)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	case 'c':
		if (idcmp("cmp", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomCmp = (int (*)(const void *, const void *)) f->imp;
			BATatoms[tpe].linear = true;
			return MAL_SUCCEED;
		}
		break;
	case 'f':
		if (idcmp("fromstr", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomFromStr = (ssize_t (*)(const char *, size_t *, ptr *, bool)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	case 'h':
		if (idcmp("heap", name) == 0 && f->argc == 1) {
			/* heap function makes an atom varsized */
			BATatoms[tpe].size = sizeof(var_t);
			assert_shift_width(ATOMelmshift(ATOMsize(tpe)), ATOMsize(tpe));
			BATatoms[tpe].atomHeap = (gdk_return (*)(Heap *, size_t)) f->imp;
			return MAL_SUCCEED;
		}
		if (idcmp("hash", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomHash = (BUN (*)(const void *)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	case 'l':
		if (idcmp("length", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomLen = (size_t (*)(const void *)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	case 'n':
		if (idcmp("null", name) == 0 && f->argc == 1) {
			const void *atmnull = ((const void *(*)(void)) f->imp)();

			BATatoms[tpe].atomNull = atmnull;
			return MAL_SUCCEED;
		}
		if (idcmp("nequal", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomCmp = (int (*)(const void *, const void *)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	case 'p':
		if (idcmp("put", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomPut = (var_t (*)(BAT *, var_t *, const void *)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	case 's':
		if (idcmp("storage", name) == 0 && f->argc == 1) {
			BATatoms[tpe].storage = (*(int (*)(void)) f->imp) ();
			return MAL_SUCCEED;
		}
		break;
	case 't':
		if (idcmp("tostr", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomToStr = (ssize_t (*)(str *, size_t *, const void *, bool)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	case 'r':
		if (idcmp("read", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomRead = (void *(*)(void *, size_t *, stream *, size_t)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	case 'w':
		if (idcmp("write", name) == 0 && f->argc == 1) {
			BATatoms[tpe].atomWrite = (gdk_return (*)(const void *, stream *, size_t)) f->imp;
			return MAL_SUCCEED;
		}
		break;
	}
	return MAL_SUCCEED;
}

/*
 * Atoms are constructed incrementally in the kernel using the
 * ATOMallocate function. It takes an existing type as a base
 * to derive a new one.
 * The most tedisous work is to check the signature types of the functions
 * acceptable for the kernel.
 */

str
malAtomDefinition(const char *name, int tpe)
{
	int i;

	if (strlen(name) >= IDLENGTH) {
		throw(SYNTAX, "atomDefinition", "Atom name '%s' too long", name);
	}
	if (ATOMindex(name) >= 0) {
		return MAL_SUCCEED;
	}
	if (tpe < 0 || tpe >= GDKatomcnt) {
		throw(TYPE, "atomDefinition", "Undefined atom inheritance '%s'", name);
	}
	if (strlen(name) >= sizeof(BATatoms[0].name))
		throw(TYPE, "atomDefinition", "Atom name too long '%s'", name);

	i = ATOMallocate(name);
	if (is_int_nil(i))
		throw(TYPE, "atomDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	/* overload atom ? */
	if (tpe) {
		BATatoms[i] = BATatoms[tpe];
		strcpy_len(BATatoms[i].name, name, sizeof(BATatoms[i].name));
		BATatoms[i].storage = ATOMstorage(tpe);
	} else {					/* cannot overload void atoms */
		BATatoms[i].storage = i;
		BATatoms[i].linear = false;
	}
	return MAL_SUCCEED;
}

/*
 * User defined modules may introduce fixed sized types
 * to store information in BATs.
 */
int
malAtomSize(int size, const char *name)
{
	int i = 0;

	i = ATOMindex(name);
	BATatoms[i].storage = i;
	BATatoms[i].size = size;
	assert_shift_width(ATOMelmshift(ATOMsize(i)), ATOMsize(i));
	return i;
}

void
mal_atom_reset(void)
{
	int i;
	for (i = 0; i < GDKatomcnt; i++)
		if (BATatoms[i].atomNull) {
			// TBD
		}
}
