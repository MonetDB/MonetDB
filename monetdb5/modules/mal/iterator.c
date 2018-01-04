/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "iterator.h"

/*
 * We start with the large chunk iterator.
 * The definition of the control statements require the same
 * control variables, which means that the BATview is accessible
 * to determine how far to advance when the next chunk is retrieved.
 * The number of elements in the chunk is limited by the granule
 * size.
 */
str
ITRnewChunk(lng *res, bat *vid, bat *bid, lng *granule)
{
	BAT *b, *view;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "chop.newChunk", INTERNAL_BAT_ACCESS);
	}
	cnt = BATcount(b);
	view = VIEWcreate_(b->hseqbase, b, TRUE);

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
str
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
	BAThseqbase(view, b->hseqbase == oid_nil ? oid_nil : b->hseqbase + i);
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
str
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
		throw(MAL, "iterator.nextChunk", MAL_MALLOC_FAIL);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
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
		throw(MAL, "iterator.nextChunk", MAL_MALLOC_FAIL);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str ITRnext_oid(oid *i, oid *step, oid *last){
	oid v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = oid_nil;
	return MAL_SUCCEED;
}
str ITRnext_lng(lng *i, lng *step, lng *last){
	lng v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = lng_nil;
	return MAL_SUCCEED;
}
#ifdef HAVE_HGE
str ITRnext_hge(hge *i, hge *step, hge *last){
	hge v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = hge_nil;
	return MAL_SUCCEED;
}
#endif
str ITRnext_int(int *i, int *step, int *last){
	int v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = int_nil;
	return MAL_SUCCEED;
}
str ITRnext_sht(sht *i, sht *step, sht *last){
	sht v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = int_nil;
	return MAL_SUCCEED;
}
str ITRnext_flt(flt *i, flt *step, flt *last){
	flt v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = flt_nil;
	return MAL_SUCCEED;
}
str ITRnext_dbl(dbl *i, dbl *step, dbl *last){
	dbl v = *i;
	v = v + *step;
	*i = v;
	if ( *last <= v )
		*i = dbl_nil;
	return MAL_SUCCEED;
}
