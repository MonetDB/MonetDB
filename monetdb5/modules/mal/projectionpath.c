/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "projectionpath.h"

str
ALGprojectionpath(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, top = 0;
	bat *bid;
	bat *r = getArgReference_bat(stk, pci, 0);
	BAT *b, **joins = (BAT**)GDKzalloc(pci->argc * sizeof(BAT*)); 
	int error = 0;

	(void) mb;
	(void) cntxt;

	assert(pci->argc > 1);
	if ( joins == NULL)
		throw(MAL, "algebra.projectionpath", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	for (i = pci->retc; i < pci->argc; i++) {
		bid = getArgReference_bat(stk, pci, i);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			error = 1;
		} else {
			if (i + 1 < pci->argc && ATOMtype(b->ttype) != TYPE_oid) {
				error = 1;
			}
			else joins[top++] = b;
		}
		if (error) {
			while (top-- > 0)
				BBPunfix(joins[top]->batCacheid);
			GDKfree(joins);
			throw(MAL, "algebra.projectionpath", "%s", b ? SEMANTIC_TYPE_MISMATCH : INTERNAL_BAT_ACCESS);
		}
	}
	joins[top] = NULL;
	b = BATprojectchain(joins);
	while (top-- > 0)
		BBPunfix(joins[top]->batCacheid);
	GDKfree(joins);
	if ( b)
		BBPkeepref( *r = b->batCacheid);
	else
		throw(MAL, "algebra.projectionpath", INTERNAL_OBJ_CREATE);
	return MAL_SUCCEED;
}
