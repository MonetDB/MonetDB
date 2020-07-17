/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * M.L.Kersten
 * BAT Algebra Bitvector Extensions
 * The current primitives are focused on the use of vectors as Candidate lists
 */
#include "monetdb_config.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "bat5.h"
#include "gdk_time.h"

/*
 * BAT bitvector enhancements
 * The code to enhance the kernel.
 */

static str
MSKmask(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){

	BAT *b, *dst;
	bat *bid;
	int *ret;

	(void) mb;
	(void) cntxt;

	ret = getArgReference_bat(stk, pci, 0);
	bid = getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, "bat.mask", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	dst = VIEWcreate(b->hseqbase, b);

	*ret=  dst->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

static str
MSKumask(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){

	BAT *b, *dst;
	bat *bid;
	int *ret;

	(void) mb;
	(void) cntxt;

	ret = getArgReference_bat(stk, pci, 0);
	bid = getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, "bat.umask", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	dst = VIEWcreate(b->hseqbase, b);

	*ret=  dst->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}


#include "mel.h"
mel_func batMask_init_funcs[] = {
 pattern("mask", "mask", MSKmask, false, "", args(1,2, batargany("",1))),
 pattern("mask", "umask", MSKumask, false, "", args(1,2, batargany("",1))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batExtensions_mal)
{ mal_module("batMask", NULL, batMask_init_funcs); }
