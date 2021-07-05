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
#include "mal_exception.h"

#if 0
/*
 * String imprints.
 */
static str
PATstrimp_ndigrams(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b;
	size_t n;

	(void)cntxt;
	(void)mb;

	// return mythrow(MAL, "batcalc.striter", OPERATION_FAILED);
	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "strimps.strimpDigrams", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!STRMPndigrams(b, &n)) {
		throw(MAL, "strimps.strimpDigrams", SQLSTATE(HY002) OPERATION_FAILED);
	}

	*getArgReference_lng(stk, pci, 0) = n;

	return MAL_SUCCEED;
}

static str
PATstrimp_makehist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b, *ob;
	size_t i;
	uint64_t hist[STRIMP_HISTSIZE];
	size_t count;

	(void)cntxt;
	(void)mb;

	bid = *getArgReference_bat(stk, pci, 2);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "strimps.strimpHistogram", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!STRMPmakehistogram(b, hist, STRIMP_HISTSIZE, &count)) {
		throw(MAL, "strimps.strimpHistogram", SQLSTATE(HY002) OPERATION_FAILED);
	}

	ob = COLnew(0, TYPE_lng, STRIMP_HISTSIZE, TRANSIENT);
	if (ob == NULL) {
		throw(MAL, "strimps.strimpHistogram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i=0; i < STRIMP_HISTSIZE; i++) {
		if (BUNappend(ob, hist + i, false) != GDK_SUCCEED)
			throw(MAL, "strimps.strimpHistogram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	*getArgReference_lng(stk, pci, 0) = count;
	*getArgReference_bat(stk, pci, 1) = ob->batCacheid;

	BBPkeepref(ob->batCacheid);
	return MAL_SUCCEED;
}
#endif

static str
PATstrimpCreate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b;
	(void)cntxt;
	(void)mb;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "strimps.strimpHeader", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if(STRMPcreate(b) != GDK_SUCCEED)
		throw(MAL, "strimps.strimpHistogram", SQLSTATE(HY002) OPERATION_FAILED);

	// *getArgReference_lng(stk, pci, 0) = 0;
	return MAL_SUCCEED;
}

static str
PATstrimpFilter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;
	throw(MAL, "strimps.strimpfilter", SQLSTATE(HY002) "UNIMPLEMENTED");
}

static str
PATstrimpFilterSelect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid, sid;
	BAT *b, *s, *ob;
	str pat;

	(void)cntxt;
	(void)mb;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "strimps.strimpfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	sid = *getArgReference_bat(stk, pci, 2);
	if ((s = BATdescriptor(sid)) == NULL)
		throw(MAL, "strimps.strimpfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	assert(s->ttype == TYPE_void);


	pat = *getArgReference_str(stk, pci, 3);
	if ((ob = STRMPfilter(b, pat)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "strimps.strimpfilter", SQLSTATE(HY002) "filtering failed");
	}

	*getArgReference_bat(stk, pci, 0) = ob->batCacheid;
	BBPkeepref(ob->batCacheid);

	return MAL_SUCCEED;
}

#include "mel.h"
mel_func strimp_init_funcs[] = {
 /* String imprints */
 // pattern("bat", "strimpNDigrams", PATstrimp_ndigrams, false, "count digrams in a string bat", args(1,2,arg("",lng),batarg("b",str))),
 // pattern("bat", "strimpHistogram", PATstrimp_makehist, false, "make a histogram of all the byte pairs in a BAT", args(2,3,arg("",lng), batarg("",lng),batarg("b",str))),
 pattern("strimps", "mkstrimp", PATstrimpCreate, false, "construct the strimp a BAT", args(1,2,arg("",void),batarg("b",str))),
 pattern("strimps", "strimpfilter", PATstrimpFilter, false, "", args(1,3,arg("",bit),arg("b",str),arg("q",str))),
 pattern("strimps", "strimpfilterselect", PATstrimpFilterSelect, false, "", args(1,5,batarg("",oid),batarg("b",str),batarg("s",oid),arg("q",str),arg("a",bit))),
 pattern("strimps", "strimpfilterjoin", PATstrimpFilter, false, "", args(2,8,batarg("",oid),batarg("b",str),arg("q",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_strimps_mal)
{ mal_module("strimps", NULL, strimp_init_funcs); }
