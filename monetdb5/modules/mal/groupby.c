/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * Multicolumn group-by support
 * The group-by support module is meant to simplify code analysis and
 * speedup the kernel on multi-attribute grouping routines.
 *
 * The target is to support SQL-like multicolumngroup_by operations, which are lists of
 * attributes and a group aggregate function.
 * Each group can be represented with an oid into the n-ary table.
 * Consider the query "select count(*), max(A) from R group by A, B,C." whose code
 * snippet in MAL would become something like:
 *
 * @verbatim
 * _1:bat[:int]  := sql.bind("sys","r","a",0);
 * _2:bat[:str]  := sql.bind("sys","r","b",0);
 * _3:bat[:date]  := sql.bind("sys","r","c",0);
 * ...
 * _9 := algebra.select(_1,0,100);
 * ..
 * (grp_4:bat[:lng], gid:bat[:oid]) := groupby.count(_9,_2);
 * (grp_5:bat[:lng], gid:bat[:oid]) := groupby.max(_9,_2,_3);
 * @end verbatim
 *
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
	BAT *candidate; /* list */
	BAT **cols;
	BUN *unique; /* number of different values */
	int last;
	BUN size;
} AGGRtask;

static AGGRtask*
GROUPcollect( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	AGGRtask *a;
	int i;
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
	if ( a->cols == NULL || a->bid == NULL || a->unique == NULL){
		if(a->cols) GDKfree(a->cols);
		if(a->bid) GDKfree(a->bid);
		if(a->unique) GDKfree(a->unique);
		GDKfree(a);
		return NULL;
	}
	for ( i= pci->retc; i< pci->argc; i++, a->last++) {
		a->bid[a->last] = *getArgReference_bat(stk,pci,i);
		b = a->cols[a->last]= BATdescriptor(a->bid[a->last]);
		if ( a->cols[a->last] == NULL){
			for(a->last--; a->last>=0; a->last--)
				BBPunfix(a->cols[a->last]->batCacheid);
			GDKfree(a->cols);
			GDKfree(a->bid);
			GDKfree(a->unique);
			GDKfree(a);
			return NULL;
		}
		sample = BATcount(b) < 1000 ? BATcount(b): 1000;
		bs = BATsample( b, sample);
		if (bs) {
			bh = BATunique(b, bs);
			if (bh) {
				a->unique[a->last] = BATcount(bh);
				BBPunfix(bh->batCacheid);
			}
			BBPunfix(bs->batCacheid);
		}
		if ( b->tsorted)
			a->unique[a->last] = 1000; /* sorting helps grouping */
		a->size = BATcount(b);
	}

#ifdef _DEBUG_GROUPBY_
	for(i=0; i<a->last; i++)
		fprintf(stderr,"#group %d unique "BUNFMT "\n", i, a->unique[i]);
#endif
	return a;
}

static void 
GROUPcollectSort(AGGRtask *a, int start, int finish)
{
	int i,j,k;
	BAT *b;
	BUN sample;

	/* sort the columns by decreasing unique */
	for (i = start; i< finish; i++)
	for( j = i+1; j<finish; j++)
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
}

static void
GROUPdelete(AGGRtask *a){
	for(a->last--; a->last>=0; a->last--){
		BBPunfix(a->cols[a->last]->batCacheid);
	}
	GDKfree(a->bid);
	GDKfree(a->cols);
	GDKfree(a->unique);
	GDKfree(a);
}

/*
 * The groups optimizer takes a grouping sequence and attempts to
 * minimize the intermediate result.  The choice depends on a good
 * estimate of intermediate results using properties.
 */

str
GROUPmulticolumngroup(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *grp = getArgReference_bat(stk, pci, 0);
	bat *ext = getArgReference_bat(stk, pci, 1);
	bat *hist = getArgReference_bat(stk, pci, 2);
	int i, j;
	bat oldgrp, oldext, oldhist;
	str msg = MAL_SUCCEED;
	BAT *b;
	BUN count = 0;
	AGGRtask *aggr;

	aggr = GROUPcollect(cntxt, mb, stk, pci);
	if( aggr == NULL)
		throw(MAL,"group.multicolumn", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	GROUPcollectSort(aggr, 0, aggr->last);

	/* (grp,ext,hist) := group.group(..) */
	/* use the old pattern to perform the incremental grouping */
	*grp = 0;
	*ext = 0;
	*hist = 0;
	msg = GRPgroup1(grp, ext, hist, &aggr->bid[0]);
	i = 1;
	if (msg == MAL_SUCCEED && aggr->last > 1)
		do {
			/* early break when there are as many groups as entries */
			b = BATdescriptor(*hist);
			if (b) {
				j = BATcount(b) == count;
				BBPunfix(*hist);
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
			msg = GRPsubgroup5(grp, ext, hist, &aggr->bid[i], NULL, &oldgrp, &oldext, &oldhist);
			BBPrelease(oldgrp);
			BBPrelease(oldext);
			BBPrelease(oldhist);
		} while (msg == MAL_SUCCEED && ++i < aggr->last);
	GROUPdelete(aggr);
	return msg;
}
