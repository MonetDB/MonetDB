/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * author M.L.Kersten
 * BAT Iterators
 * Many low level algorithms rely on an iterator to break a
 * collection into smaller pieces. Each piece is subsequently processed
 * by a block.
 *
 * For very large BATs it may make sense to break it into chunks
 * and process them separately to solve a query. An iterator pair is
 * provided to chop a BAT into fixed size elements.
 * Each chunk is made available as a BATview.
 * It provides read-only access to an underlying BAT. Adjusting the bounds
 * is cheap, once the BATview descriptor has been constructed.
 *
 * The smallest granularity is a single BUN, which can be used
 * to realize an iterator over the individual BAT elements.
 * For larger sized chunks, the operators return a BATview.
 *
 * All iterators require storage space to administer the
 * location of the next element. The BAT iterator module uses a simple
 * lng variable, which also acts as a cursor for barrier statements.
 *
 * The larger chunks produced are currently static, i.e.
 * their size is a parameter of the call. Dynamic chunk sizes
 * are interesting for time-series query processing. (See another module)
 *
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"

/*
 * We start with the large chunk iterator.
 * The definition of the control statements require the same
 * control variables, which means that the BATview is accessible
 * to determine how far to advance when the next chunk is retrieved.
 * The number of elements in the chunk is limited by the granule
 * size.
 */
static str
ITRnewChunk(lng *res, bat *vid, bat *bid, lng *granule)
{
	BAT *b, *view;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "chop.newChunk", INTERNAL_BAT_ACCESS);
	}
	cnt = BATcount(b);
	view = VIEWcreate(b->hseqbase, b);
	if (view == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "chop.newChunk", GDK_EXCEPTION);
	}

	/*  printf("set bat chunk bound to " LLFMT " 0 - " BUNFMT "\n",
	 *granule, MIN(cnt,(BUN) *granule)); */
	VIEWbounds(b, view, 0, MIN(cnt, (BUN) * granule));
	*vid = view->batCacheid;
	BBPkeepref(view->batCacheid);
	BBPunfix(b->batCacheid);
	*res = 0;
	return MAL_SUCCEED;
}

/*
 * The nextChunk version advances the reader,
 * which also means that the view descriptor is already available.
 * The granule size may differ in each call.
 */
static str
ITRnextChunk(lng *res, bat *vid, bat *bid, lng *granule)
{
	BAT *b, *view;
	BUN i;

	if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "iterator.nextChunk", INTERNAL_BAT_ACCESS);
	}
	if ((view = BATdescriptor(*vid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "iterator.nextChunk", INTERNAL_BAT_ACCESS);
	}
	i = (BUN) (*res + BATcount(view));
	if (i >= BUNlast(b)) {
		*res = lng_nil;
		*vid = 0;
		BBPunfix(view->batCacheid);
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	/* printf("set bat chunk bound to " BUNFMT " - " BUNFMT " \n",
	   i, i+(BUN) *granule-1); */
	VIEWbounds(b, view, i, i + (BUN) * granule);
	BAThseqbase(view, is_oid_nil(b->hseqbase) ? oid_nil : b->hseqbase + i);
	BBPkeepref(*vid = view->batCacheid);
	BBPunfix(b->batCacheid);
	*res = i;
	return MAL_SUCCEED;
}

/*
 * @-
 * The BUN- and BAT-stream manipulate a long handle, i.e.
 * the destination variable. It assumes it has been set to
 * zero as part of runtime stack initialization. Subsequently,
 * it fetches a bun and returns the increment to the control
 * variable. If it returns zero the control variable has been reset
 * to zero and end of stream has been reached.
 */
static str
ITRbunIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *b;
	oid *head;
	bat *bid;
	ValPtr tail;

	(void) cntxt;
	(void) mb;
	head = getArgReference_oid(stk, pci, 0);
	tail = &stk->stk[pci->argv[1]];
	bid = getArgReference_bat(stk, pci, 2);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "iterator.nextChunk", INTERNAL_BAT_ACCESS);
	}

	if (BATcount(b) == 0) {
		*head = oid_nil;
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	*head = 0;

 	bi = bat_iterator(b);
	if (VALinit(tail, b->ttype, BUNtail(bi, *head)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "iterator.nextChunk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
ITRbunNext(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *b;
	oid *head;
	bat *bid;
	ValPtr tail;

	(void) cntxt;
	(void) mb;
	head = getArgReference_oid(stk, pci, 0);
	tail = &stk->stk[pci->argv[1]];
	bid = getArgReference_bat(stk, pci, 2);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "iterator.nextChunk", INTERNAL_BAT_ACCESS);
	}

	*head = *head + 1;
	if (*head >= BUNlast(b)) {
		*head = oid_nil;
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
 	bi = bat_iterator(b);
	if (VALinit(tail, b->ttype, BUNtail(bi, *head)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "iterator.nextChunk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str ITRnext_oid(oid *i, oid *step, oid *last){
	oid v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = oid_nil;
	return MAL_SUCCEED;
}
static str ITRnext_lng(lng *i, lng *step, lng *last){
	lng v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = lng_nil;
	return MAL_SUCCEED;
}
#ifdef HAVE_HGE
static str ITRnext_hge(hge *i, hge *step, hge *last){
	hge v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = hge_nil;
	return MAL_SUCCEED;
}
#endif
static str ITRnext_int(int *i, int *step, int *last){
	int v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = int_nil;
	return MAL_SUCCEED;
}
static str ITRnext_sht(sht *i, sht *step, sht *last){
	sht v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = int_nil;
	return MAL_SUCCEED;
}
static str ITRnext_flt(flt *i, flt *step, flt *last){
	flt v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = flt_nil;
	return MAL_SUCCEED;
}
static str ITRnext_dbl(dbl *i, dbl *step, dbl *last){
	dbl v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = dbl_nil;
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func iterator_init_funcs[] = {
 command("iterator", "new", ITRnewChunk, false, "Create an iterator with fixed granule size.\nThe result is a view.", args(2,4, arg("",lng),batargany("",2),batargany("b",2),arg("size",lng))),
 command("iterator", "next", ITRnextChunk, false, "Produce the next chunk for processing.", args(2,4, arg("",lng),batargany("",2),batargany("b",2),arg("size",lng))),
 pattern("iterator", "new", ITRbunIterator, false, "Process the buns one by one extracted from a void table.", args(2,3, arg("h",oid),argany("t",2),batargany("b",2))),
 pattern("iterator", "next", ITRbunNext, false, "Produce the next bun for processing.", args(2,3, arg("h",oid),argany("t",2),batargany("b",2))),
 command("iterator", "next", ITRnext_oid, false, "", args(1,3, arg("",oid),arg("step",oid),arg("last",oid))),
 command("iterator", "next", ITRnext_sht, false, "", args(1,3, arg("",sht),arg("step",sht),arg("last",sht))),
 command("iterator", "next", ITRnext_int, false, "", args(1,3, arg("",int),arg("step",int),arg("last",int))),
 command("iterator", "next", ITRnext_lng, false, "", args(1,3, arg("",lng),arg("step",lng),arg("last",lng))),
 command("iterator", "next", ITRnext_flt, false, "", args(1,3, arg("",flt),arg("step",flt),arg("last",flt))),
 command("iterator", "next", ITRnext_dbl, false, "Advances the iterator with a fixed value", args(1,3, arg("",dbl),arg("step",dbl),arg("last",dbl))),
#ifdef HAVE_HGE
 command("iterator", "next", ITRnext_hge, false, "", args(1,3, arg("",hge),arg("step",hge),arg("last",hge))),
#endif
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_iterator_mal)
{ mal_module("iterator", NULL, iterator_init_funcs); }
