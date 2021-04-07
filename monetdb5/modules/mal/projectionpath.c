/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal_client.h"
#include "mal_interpreter.h"

static str
ALGprojectionpath(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	bat bid;
	bat *r = getArgReference_bat(stk, pci, 0);
	BAT *b, **joins = NULL;

	(void) mb;
	(void) cntxt;

	if(pci->argc <= 1)
		throw(MAL, "algebra.projectionpath", SQLSTATE(HY013) "INTERNAL ERROR");
	joins = (BAT**)GDKzalloc(pci->argc * sizeof(BAT*));
	if ( joins == NULL)
		throw(MAL, "algebra.projectionpath", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	for (i = pci->retc; i < pci->argc; i++) {
		bid = *getArgReference_bat(stk, pci, i);
		b = BATdescriptor(bid);
		if (b == NULL || (i + 1 < pci->argc && ATOMtype(b->ttype) != TYPE_oid && b->ttype != TYPE_msk)) {
			while (--i >= pci->retc)
				BBPunfix(joins[i - pci->retc]->batCacheid);
			GDKfree(joins);
			throw(MAL, "algebra.projectionpath", "%s", b ? SEMANTIC_TYPE_MISMATCH : INTERNAL_BAT_ACCESS);
		}
		joins[i - pci->retc] = b;
	}
	joins[pci->argc - pci->retc] = NULL;
	b = BATprojectchain(joins);
	for (i = pci->retc; i < pci->argc; i++)
		BBPunfix(joins[i - pci->retc]->batCacheid);
	GDKfree(joins);
	if ( b)
		BBPkeepref( *r = b->batCacheid);
	else
		throw(MAL, "algebra.projectionpath", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func projectionpath_init_funcs[] = {
 pattern("algebra", "projectionpath", ALGprojectionpath, false, "Routine to handle join paths.  The type analysis is rather tricky.", args(1,2, batargany("",0),batvarargany("l",0))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_projectionpath_mal)
{ mal_module("projectionpath", NULL, projectionpath_init_funcs); }
