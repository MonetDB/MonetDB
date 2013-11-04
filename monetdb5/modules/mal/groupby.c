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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) Martin Kersten
 * Multicolumn group-by support
 * The group-by support module is meant to replace and speedup the kernel grouping routines.
 * The latter was originally designed in a memory constraint setting and an exercise in
 * performing column-wise grouping incrementally. The effect is that these routines are
 * now a major performance hindrances.
 *
 * The target is to support SQL-like multicolumngroup_by operations, which are lists of
 * attributes and a group aggregate function.
 * Each group can be represented with an oid into the n-ary table.
 * Consider the query "select count(*), max(A) from R group by A, B,C." whose code
 * snippet in MAL would become something like:
 * @verbatim
 * _1:bat[:oid,:int]  := sql.bind("sys","r","a",0);
 * _2:bat[:oid,:str]  := sql.bind("sys","r","b",0);
 * _3:bat[:oid,:date]  := sql.bind("sys","r","c",0);
 * ...
 * _9 := algebra.select(_1,0,100);
 * ..
 * (grp_4:bat[:oid,:wrd], gid:bat[:oid,:oid]) := groupby.count(_9,  _1,_2 _3);
 * (grp_5:bat[:oid,:lng], gid:bat[:oid,:oid]) := groupby.max(_9,_2, _1,_2,_3);
 * @end verbatim
 *
 * All instructions have a candidate oid list.
 * The id() function merely becomes the old-fashioned oid-based group identification list.
 * This way related values can be obtained from the attribute columns. It can be the input
 * for the count() function, which saves some re-computation.
 *
 * Aside the group ids, we also provide options to return the value based aggregate table
 * to ease development of parallel plans.
 *
 * The implementation is optimized for a limited number of groups. The default is
 * to fall back on the old code sequences.
 *
 */
#include "monetdb_config.h"
#include "groupby.h"
#include "group.h"

/*
 * The implementation is based on a two-phase process. In phase 1, we estimate
 * the number of groups to deal with using column independence.
 * The grouping is performed in parallel over slices of the tables.
 * The final pieces are glued together.
 */
typedef struct{
	bat *bid;	/* input bats */
	BAT **cols;
	BUN *unique; /* number of different values */
	int last;
	BUN size;
} AGGRtask;

static AGGRtask*
GROUPcollect( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	AGGRtask *a;
	int i,j,k;
	BAT *b, *bs, *bh = NULL;
	BUN sample;

	(void) mb;
	(void) cntxt;
	a= (AGGRtask *) GDKzalloc(sizeof(*a));
	if ( a == NULL)
		return NULL;
	a->bid = (bat*) GDKzalloc(pci->argc * sizeof(bat));
	a->cols = (BAT**) GDKzalloc(pci->argc * sizeof(BAT*));
	a->unique = (BUN *) GDKzalloc(pci->argc * sizeof(BUN));
	if ( a->cols == NULL){
		GDKfree(a);
		return NULL;
	}
	for ( i= pci->retc; i< pci->argc; i++, a->last++) {
		a->bid[a->last] = *(int*) getArgReference(stk,pci,i);
		b = a->cols[a->last]= BATdescriptor(a->bid[a->last]);
		if ( a->cols[a->last] == NULL){
			for(a->last--; a->last>=0; a->last--)
				BBPreleaseref(a->cols[a->last]->batCacheid);
			return NULL;
		}
		a->size = BATcount(b);
		sample = BATcount(b) < 1000 ? BATcount(b): 1000;
		bs = BATsample( b, sample);
		if (bs) {
			bh = BATkunique(BATmirror(bs));
			a->unique[a->last] = BATcount(bh);
			if ( bh ) BBPreleaseref(bh->batCacheid);
		}
		if ( bs ) BBPreleaseref(bs->batCacheid);
	}

	/* sort the columns by decreasing unique */
	for (i = 1; i< a->last; i++)
	for( j = i+1; j<a->last; j++)
	if ( a->unique[i] < a->unique[j]){
		k =a->bid[i];
		a->bid[i] = a->bid[j];
		a->bid[j] = k;

		b= a->cols[i];
		a->cols[i] = a->cols[j];
		a->cols[j] = b;

		sample = a->unique[i];
		a->unique[i] = a->unique[j];
		a->unique[j] = sample;
	}
#ifdef _DEBUG_GROUPBY_
	for(i=0; i<a->last; i++)
		mnstr_printf(cntxt->fdout,"#group %d unique "BUNFMT "\n", i, a->unique[i]);
#endif
	return a;
}

static void
GROUPdelete(AGGRtask *a){
	for(a->last--; a->last>=0; a->last--){
		BBPreleaseref(a->cols[a->last]->batCacheid);
	}
	GDKfree(a->cols);
	GDKfree(a->unique);
	GDKfree(a);
}

// Collect the unique group identifiers for all
str
GROUPid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	AGGRtask *a;
	BAT *bn;

	a = GROUPcollect(cntxt,mb,stk,pci);
	bn =  BATnew(TYPE_void,TYPE_oid, a->size);
	if ( bn == NULL) {
		GROUPdelete(a);
		throw(MAL,"groupby.id",MAL_MALLOC_FAIL);
	}
	BATseqbase(bn,0);

	GROUPdelete(a);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
GROUPcountTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
GROUPmaxTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
GROUPminTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
GROUPavgTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
GROUPcount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	AGGRtask *a;
	BAT *bn, *bo;

	a = GROUPcollect(cntxt,mb,stk,pci);
	bo = BATnew(TYPE_void,TYPE_oid,a->size);
	bn = BATnew(TYPE_void,TYPE_wrd,a->size);
	if ( bo == NULL || bn == NULL) {
		GROUPdelete(a);
		if ( bo) BBPreleaseref(bo->batCacheid);
		if ( bn) BBPreleaseref(bn->batCacheid);
		throw(MAL,"groupby.count",MAL_MALLOC_FAIL);
	}
	BATseqbase(bn,0);

	GROUPdelete(a);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
GROUPmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	AGGRtask *a;
	BAT *bn, *bo;

	a = GROUPcollect(cntxt,mb,stk,pci);
	bo = BATnew(TYPE_void,TYPE_oid,a->size);
	bn = BATnew(TYPE_void,TYPE_wrd,a->size);
	if ( bo == NULL || bn == NULL) {
		GROUPdelete(a);
		if ( bo) BBPreleaseref(bo->batCacheid);
		if ( bn) BBPreleaseref(bn->batCacheid);
		throw(MAL,"groupby.count",MAL_MALLOC_FAIL);
	}
	BATseqbase(bn,0);

	GROUPdelete(a);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
GROUPmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	AGGRtask *a;
	BAT *bn, *bo;

	a = GROUPcollect(cntxt,mb,stk,pci);
	bo = BATnew(TYPE_void,TYPE_oid,a->size);
	bn = BATnew(TYPE_void,TYPE_wrd,a->size);
	if ( bo == NULL || bn == NULL) {
		GROUPdelete(a);
		if ( bo) BBPreleaseref(bo->batCacheid);
		if ( bn) BBPreleaseref(bn->batCacheid);
		throw(MAL,"groupby.count",MAL_MALLOC_FAIL);
	}
	BATseqbase(bn,0);

	GROUPdelete(a);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
GROUPavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	AGGRtask *a;
	BAT *bn, *bo;

	a = GROUPcollect(cntxt,mb,stk,pci);
	bo = BATnew(TYPE_void,TYPE_oid,a->size);
	bn = BATnew(TYPE_void,TYPE_wrd,a->size);
	if ( bo == NULL || bn == NULL) {
		GROUPdelete(a);
		if ( bo) BBPreleaseref(bo->batCacheid);
		if ( bn) BBPreleaseref(bn->batCacheid);
		throw(MAL,"groupby.count",MAL_MALLOC_FAIL);
	}
	BATseqbase(bn,0);

	GROUPdelete(a);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

/*
 * The groups optimizer takes a grouping sequence and attempts to
 * minimize the intermediate result.  The choice depends on a good
 * estimate of intermediate results using properties.
 */

str
GROUPmulticolumn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *grp = (bat *) getArgReference(stk, pci, 0);
	bat *ext = (bat *) getArgReference(stk, pci, 1);
	bat *hist = (bat *) getArgReference(stk, pci, 2);
	int i, j;
	bat oldgrp, oldext, oldhist;
	str msg = MAL_SUCCEED;
	BAT *b;
	BUN count = 0;
	AGGRtask *aggr;

	aggr = GROUPcollect(cntxt, mb, stk, pci);

	/* (grp,ext,hist) := group.subgroup(..) */
	/* use the old pattern to perform the incremental grouping */
	*grp = 0;
	*ext = 0;
	*hist = 0;
	msg = GRPsubgroup1(grp, ext, hist, &aggr->bid[0]);
	i = 1;
	if (msg == MAL_SUCCEED && aggr->last > 1)
		do {
			/* early break when there are as many groups as entries */
			b = BATdescriptor(*hist);
			if (b) {
				j = BATcount(b) == count;
				BBPreleaseref(*hist);
				if (j)
					break;
			}

			/* (grp,ext,hist) := group.subgroup(arg,grp,ext,hist) */
			oldgrp = *grp;
			oldext = *ext;
			oldhist = *hist;
			*grp = 0;
			*ext = 0;
			*hist = 0;
			msg = GRPsubgroup4(grp, ext, hist, &aggr->bid[i], &oldgrp, &oldext, &oldhist);
			BBPdecref(oldgrp, TRUE);
			BBPdecref(oldext, TRUE);
			BBPdecref(oldhist, TRUE);
		} while (msg == MAL_SUCCEED && ++i < aggr->last);
	GROUPdelete(aggr);
	return msg;
}
