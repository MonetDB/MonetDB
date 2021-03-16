/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
MSKmask(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b, *dst;
	bat *bid;
	int *ret;

	(void) mb;
	(void) cntxt;

	ret = getArgReference_bat(stk, pci, 0);
	bid = getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, "bat.mask", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if( !b->tkey || !b->tsorted )
		throw(SQL, "bat.mask", SQLSTATE(HY002) "Input should be unique and in ascending order");
	if (BATcount(b) == 0) {
		dst = COLnew(0, TYPE_msk, 0, TRANSIENT);
	} else {
		oid fst;
		BUN cap;
		BUN max = 0;
		if (b->tsorted) {
			fst = BUNtoid(b, 0);
			dst = COLnew(fst, TYPE_msk, BUNtoid(b, BUNlast(b) - 1) + 1 - fst, TRANSIENT);
		} else {
			fst = 0;
			dst = COLnew(0, TYPE_msk, BATcount(b), TRANSIENT);
		}
		cap = BATcapacity(b);
		if (dst) {
			memset(Tloc(dst, 0), 0, dst->theap->size);
			for (BUN p = 0; p < BATcount(b); p++) {
				oid o = BUNtoid(b, p);
				if (is_oid_nil(o)) {
					BBPunfix(b->batCacheid);
					BBPreclaim(dst);
					throw(MAL, "mask.mask", "no NULL allowed");
				}
				o -= fst;
				if (o >= cap) {
					if (BATextend(dst, o + 1) != GDK_SUCCEED) {
						BBPunfix(b->batCacheid);
						BBPreclaim(dst);
						throw(MAL, "mask.mask", GDK_EXCEPTION);
					}
					cap = BATcapacity(dst);
				}
				mskSetVal(dst, o, true);
				if (o > max)
					max = o;
			}
			BATsetcount(dst, max + 1);
			dst->tsorted = dst->trevsorted = false;
			dst->tkey = false;
			dst->tnil = false;
			dst->tnonil = true;
		}
	}
	BBPunfix(b->batCacheid);
	if (dst == NULL)
		throw(MAL, "mask.mask", GDK_EXCEPTION);

	*ret=  dst->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

static str
MSKumask(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b, *dst;
	bat *bid;
	int *ret;

	(void) mb;
	(void) cntxt;

	ret = getArgReference_bat(stk, pci, 0);
	bid = getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, "bat.umask", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	dst = BATunmask(b);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "mask.umask", GDK_EXCEPTION);
	}
	*ret=  dst->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}


#include "mel.h"
mel_func batMask_init_funcs[] = {
 pattern("mask", "mask", MSKmask, false, "", args(1,2, batarg("r", msk), batarg("b",oid))),
 pattern("mask", "umask", MSKumask, false, "", args(1,2, batarg("r", oid), batarg("b",msk))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batMask_mal)
{ mal_module("batMask", NULL, batMask_init_funcs); }
