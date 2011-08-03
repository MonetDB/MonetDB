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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f chopper
 * @v 2.0
 * @a M.L.Kersten
 * @+ BAT Iterators
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
/*
 * @-
 *
 * @include prelude.mx
 * @+ BAT Iterator Implementation
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define chopper_export extern __declspec(dllimport)
#else
#define chopper_export extern __declspec(dllexport)
#endif
#else
#define chopper_export extern
#endif

chopper_export str CHPnewChunkIterator(lng *res, int *vid, int *bid, lng *granule);
chopper_export str CHPhasMoreElements(lng *res, int *vid, int *bid, lng *granule);
chopper_export str CHPbunIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
chopper_export str CHPbunHasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
chopper_export str CHPgetHead(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
chopper_export str CHPgetTail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

/*
 * @-
 * We start with the large chunk iterator.
 * The definition of the control statements require the same
 * control variables, which means that the BATview is accessible
 * to determine how far to advance when the next chunk is retrieved.
 * The number of elements in the chunk is limited by the granule
 * size.
 */
str
CHPnewChunkIterator(lng *res, int *vid, int *bid, lng *granule)
{
	BAT *b, *view;
	BUN cnt, first;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "chop.newChunkIterator", INTERNAL_BAT_ACCESS);
	}
	cnt = BATcount(b);
	first = BUNfirst(b);
	view = VIEWcreate_(b, b, TRUE);

	/*  printf("set bat chunk bound to " LLFMT " " BUNFMT " - " BUNFMT "\n",
	 *granule, first, MIN(cnt,(BUN) *granule)); */
	VIEWbounds(b, view, (BUN) first, first + MIN(cnt, (BUN) * granule));
	BATseqbase(view, first - 1);
	*vid = view->batCacheid;
	BBPkeepref(view->batCacheid);
	BBPunfix(b->batCacheid);
	*res = first;
	return MAL_SUCCEED;
}

/*
 * @-
 * The hasMoreElements version advances the reader,
 * which also means that the view descriptor is already available.
 * The granule size may differ in each call.
 */
str
CHPhasMoreElements(lng *res, int *vid, int *bid, lng *granule)
{
	BAT *b, *view;
	BUN i;

	if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "chop.newChunkMoreElements", INTERNAL_BAT_ACCESS);
	}
	if ((view = BATdescriptor(*vid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "chop.newChunkMoreElements", INTERNAL_BAT_ACCESS);
	}
	i = (BUN) (*res + BATcount(view));
	if (i >= BUNlast(b)) {
		*res = -1;
		*vid = 0;
		BBPunfix(b->batCacheid);
		BBPunfix(view->batCacheid);
		return MAL_SUCCEED;
	}
	/* printf("set bat chunk bound to " BUNFMT " - " BUNFMT " \n",
	   i, i+(BUN) *granule-1); */
	VIEWbounds(b, view, i, i + (BUN) * granule);
	BATseqbase(view, i - 1);
	BBPunfix(b->batCacheid);
	BBPkeepref(*vid = view->batCacheid);
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
CHPbunIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *b;
	lng *cursor;
	int *bid;
	BUN yy;
	ValPtr head, tail;
	oid o;

	(void) cntxt;
	cursor = (lng *) getArgReference(stk, pci, 0);
	head = getArgReference(stk,pci,1);
	tail = getArgReference(stk,pci,2);
	bid = (int *) getArgReference(stk, pci, 3);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "chop.newChunkMoreElements", INTERNAL_BAT_ACCESS);
	}

	if (BATcount(b) == 0) {
		*cursor = -1;
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	*cursor = BUNfirst(b);

	/* get head = ... tail = ... */
	assert(0 <= *cursor && *cursor <= (lng) BUN_MAX);
	yy = (BUN)*cursor;
 	bi = bat_iterator(b);
	if( b->htype == TYPE_void){
		o= b->hseqbase;
		VALinit(head, TYPE_oid, &o);
	} else {
		VALinit(head, getArgType(mb, pci, 1), BUNhead(bi, yy));
	}
	assert(b->ttype==TYPE_bat || b->ttype == getArgType(mb,pci,2));
	VALinit(tail, b->ttype, BUNtail(bi, yy));
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
CHPbunHasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *b;
	lng *cursor;
	BUN yy;
	bat *bid;
	ValPtr head, tail;
	oid o;

	(void) cntxt;
	cursor = (lng *) getArgReference(stk, pci, 0);
	head = getArgReference(stk,pci,1);
	tail = getArgReference(stk,pci,2);
	bid = (bat *) getArgReference(stk, pci, 3);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "chop.newChunkMoreElements", INTERNAL_BAT_ACCESS);
	}

	/* get head = ... tail = ... */
	assert(0 <= *cursor && *cursor < (lng) BUN_MAX);
	yy = (BUN)*cursor + 1;
	*cursor = yy;

	if (yy >= BUNlast(b)) {
		*cursor = -1;
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
 	bi = bat_iterator(b);
	if( b->htype == TYPE_void){
		o= (oid) (yy - BUNfirst(b)+  b->hseqbase);
		VALinit(head, TYPE_oid, &o);
	} else {
		assert(b->htype == getArgType(mb,pci,1));
		VALinit(head, getArgType(mb, pci, 1), BUNhead(bi, yy));
	}
	assert(b->ttype == getArgType(mb,pci,2));
	VALinit(tail, b->ttype, BUNtail(bi, yy));
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * @-
 *
 * @-
 * The fetch operations are all pretty straight forward, provided
 * you know the underlying type. Often it is cheaper to use
 * the extended BAT iterator, because then it can re-use the
 * BAT descriptor.
 */
str
CHPgetHead(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	lng *cursor;
	int *bid;
	BUN limit;
	ValPtr head;
	oid o;

	(void) cntxt;
	cursor = (lng *) getArgReference(stk, pci, 2);
	bid = (int *) getArgReference(stk, pci, 1);
	head = getArgReference(stk,pci,0);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "chop.getHead", INTERNAL_BAT_ACCESS);
	}
	limit = BUNlast(b);
	if (*cursor < 0 || *cursor >= (lng) limit) {
		BBPunfix(b->batCacheid);
		throw(MAL, "mal.getHead", RANGE_ERROR);
	}

	/* get head = ... */
	if( getArgType(mb,pci,3) == TYPE_void){
		o = (oid)*cursor +  b->hseqbase;
		VALinit(head, TYPE_oid, &o);
	} else {
 		BATiter bi = bat_iterator(b);
		VALinit(head, getArgType(mb, pci, 3), BUNhead(bi, (BUN)*cursor));
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
CHPgetTail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
 	BATiter bi;
	BAT *b;
	lng *cursor;
	int *bid;
	BUN limit;
	ValPtr tail;

	(void) cntxt;
	cursor = (lng *) getArgReference(stk, pci, 2);
	bid = (int *) getArgReference(stk, pci, 1);
	tail = getArgReference(stk,pci,0);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "chop.getTail", INTERNAL_BAT_ACCESS);
	}
	limit = BUNlast(b);
	if (*cursor < 0 || *cursor >= (lng) limit) {
		BBPunfix(b->batCacheid);
		throw(OUTOFBNDS, "mal.getTail", RANGE_ERROR);
	}

	/* get tail = ... */
	bi = bat_iterator(b);
	VALinit(tail, getArgType(mb, pci, 3), BUNtail(bi, (BUN)*cursor));
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}
