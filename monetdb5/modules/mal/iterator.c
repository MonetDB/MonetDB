/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
ITRnewChunk(lng *res, int *vid, int *bid, lng *granule)
{
	BAT *b, *view;
	BUN cnt, first;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "chop.newChunk", INTERNAL_BAT_ACCESS);
	}
	cnt = BATcount(b);
	first = BUNfirst(b);
	view = VIEWcreate_(b, b, TRUE);

	/*  printf("set bat chunk bound to " LLFMT " " BUNFMT " - " BUNFMT "\n",
	 *granule, first, MIN(cnt,(BUN) *granule)); */
	VIEWbounds(b, view, (BUN) first, first + MIN(cnt, (BUN) * granule));
	BATseqbase(view, b->hseqbase);
	*vid = view->batCacheid;
	BBPkeepref(view->batCacheid);
	BBPunfix(b->batCacheid);
	*res = first;
	return MAL_SUCCEED;
}

/*
 * The nextChunk version advances the reader,
 * which also means that the view descriptor is already available.
 * The granule size may differ in each call.
 */
str
ITRnextChunk(lng *res, int *vid, int *bid, lng *granule)
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
	BATseqbase(view, b->hseqbase == oid_nil ? oid_nil : b->hseqbase + i - BUNfirst(b));
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
	int *bid;
	ValPtr tail;

	(void) cntxt;
	(void) mb;
	head = (oid *) getArgReference(stk, pci, 0);
	tail = getArgReference(stk,pci,1);
	bid = (int *) getArgReference(stk, pci, 2);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "iterator.nextChunk", INTERNAL_BAT_ACCESS);
	}

	if (BATcount(b) == 0) {
		*head = oid_nil;
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	*head = BUNfirst(b);

 	bi = bat_iterator(b);
	VALinit(tail, b->ttype, BUNtail(bi, *(BUN*) head));
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
	head = (oid *) getArgReference(stk, pci, 0);
	tail = getArgReference(stk,pci,1);
	bid = (bat *) getArgReference(stk, pci, 2);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "iterator.nextChunk", INTERNAL_BAT_ACCESS);
	}

	*head = (BUN)*head + 1;
	if (*head >= BUNlast(b)) {
		*head = oid_nil;
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
 	bi = bat_iterator(b);
	VALinit(tail, b->ttype, BUNtail(bi, *(BUN*) head));
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
