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
 * A.R. van Ballegooij
 * Basic array support
 *
 * The array support library constructs the index arrays essential
 * for the Relational Algebra Model language.
 * The grid filler operation assumes that there is enough space.
 * The shift variant multiplies all elements with a constant factor.
 * It is a recurring operation for the RAM front-end and will save
 * an additional copying.
 *
 * The optimization is captured in a contraction macro.
 */

#include "monetdb_config.h"
#include "array.h"

#define new_bat(b, s, TYPE)										\
		do {													\
			(b) = BATnew(TYPE_void, TYPE_##TYPE, (BUN) (s));	\
			if (b)												\
				BATseqbase((b), 0);								\
		} while (0)
#define add_vals(b, n, TYPE)									\
		do {													\
			(b)->T->heap.free += (size_t) (n) * sizeof(TYPE);	\
			(b)->batCount += (BUN) (n);							\
			BATkey(BATmirror(b), 0);							\
			(b)->tsorted = (b)->trevsorted = 0;					\
			(b)->hrevsorted = 0;								\
		} while (0)
#define get_ptr(b,TYPE)   ((TYPE*)(Tloc(b,BUNfirst((b)))))

static int
fillgrid_int(BAT **out, int *groups, int *groupsize, int *clustersize, int *offset, int *shift)
{
	register int *ptr;
	int i = *groups;
	int n = *groupsize + *offset;
	int r = *clustersize;
	int o = *offset;
	int s = *shift;

#ifdef EXCESSIVE_DEBUGGING
	fprintf(stderr, "[grid] (%d,%d,%d,%d)", i, n, r, o);
#endif

	ptr = get_ptr(*out, int);

	while (i--) {
		register int ni = o;

		while (ni < n) {
			register int ri = r;

			while (ri--)
				(*(ptr ++)) = ni * s;
			ni++;
		}
	}

#ifdef EXCESSIVE_DEBUGGING
	fprintf(stderr, "- done\n");
#endif

	return GDK_SUCCEED;
}

static int
grid_int(BAT **out, int *groups, int *groupsize, int *clustersize, int *offset)
{
	int i = *groups;
	int n = *groupsize + *offset;
	int r = *clustersize;
	int o = *offset;
	int s = 1;

#ifdef EXCESSIVE_DEBUGGING
	fprintf(stderr, "[grid] (%d,%d,%d,%d)", i, n, r, o);
#endif

	new_bat(*out, (i * (n - o) * r), int);
	if (out == NULL) {
		GDKerror("grid: cannot create the bat (%d BUNs)\n", (i * (n - o) * r));
		return GDK_FAIL;
	}
	add_vals(*out, (i * (n - o) * r), int);
	return fillgrid_int(out, groups, groupsize, clustersize, offset, &s);
}

static int
gridShift_int(BAT **out, int *groups, int *groupsize, int *clustersize, int *offset, int *shift)
{
	int i = *groups;
	int n = *groupsize + *offset;
	int r = *clustersize;
	int o = *offset;

#ifdef EXCESSIVE_DEBUGGING
	fprintf(stderr, "[grid] (%d,%d,%d,%d)", i, n, r, o);
#endif

	new_bat(*out, (i * (n - o) * r), int);
	if (*out == 0)
		return GDK_FAIL;
	add_vals(*out, (i * (n - o) * r), int);
	return fillgrid_int(out, groups, groupsize, clustersize, offset, shift);
}

static int
fillgrid_lng(BAT **out, lng *groups, lng *groupsize, lng *clustersize, lng *offset, lng *shift)
{
	register lng *ptr;
	lng i = *groups;
	lng n = *groupsize + *offset;
	lng r = *clustersize;
	lng o = *offset;
	lng s = *shift;

#ifdef EXCESSIVE_DEBUGGING
	fprintf(stderr, "[grid] (%d,%d,%d,%d)", i, n, r, o);
#endif

	ptr = get_ptr(*out, lng);

	while (i--) {
		register lng ni = o;

		while (ni < n) {
			register lng ri = r;

			while (ri--)
				(*(ptr ++)) = ni * s;
			ni++;
		}
	}

#ifdef EXCESSIVE_DEBUGGING
	fprintf(stderr, "- done\n");
#endif

	return GDK_SUCCEED;
}

static int
grid_lng(BAT **out, lng *groups, lng *groupsize, lng *clustersize, lng *offset)
{
	lng i = *groups;
	lng n = *groupsize + *offset;
	lng r = *clustersize;
	lng o = *offset;
	lng s = 1;

#ifdef EXCESSIVE_DEBUGGING
	fprintf(stderr, "[grid] (%d,%d,%d,%d)", i, n, r, o);
#endif

	new_bat(*out, (i * (n - o) * r), lng);
	if (out == NULL) {
		GDKerror("grid: cannot create the bat (" LLFMT " BUNs)\n", (i * (n - o) * r));
		return GDK_FAIL;
	}
	add_vals(*out, (i * (n - o) * r), lng);
	return fillgrid_lng(out, groups, groupsize, clustersize, offset, &s);
}

static int
gridShift_lng(BAT **out, lng *groups, lng *groupsize, lng *clustersize, lng *offset, lng *shift)
{
	lng i = *groups;
	lng n = *groupsize + *offset;
	lng r = *clustersize;
	lng o = *offset;

#ifdef EXCESSIVE_DEBUGGING
	fprintf(stderr, "[grid] (%d,%d,%d,%d)", i, n, r, o);
#endif

	new_bat(*out, (i * (n - o) * r), lng);
	if (*out == 0)
		return GDK_FAIL;
	add_vals(*out, (i * (n - o) * r), lng);
	return fillgrid_lng(out, groups, groupsize, clustersize, offset, shift);
}

str
ARRAYgrid_int(int *ret, int *groups, int *groupsize, int *clustersize, int *offset)
{
	BAT *bn;

	if (grid_int(&bn, groups, groupsize, clustersize, offset) == GDK_FAIL)
		throw(MAL, "array.grid", MAL_MALLOC_FAIL);
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref((int)*ret);
	return MAL_SUCCEED;
}

str
ARRAYgridShift_int(int *ret, int *groups, int *groupsize, int *clustersize, int *offset, int *shift)
{
	BAT *bn;

	if (gridShift_int(&bn, groups, groupsize, clustersize, offset, shift) == GDK_FAIL)
		throw(MAL, "array.grid", MAL_MALLOC_FAIL);
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref((int)*ret);
	return MAL_SUCCEED;
}

str
ARRAYgridBAT_int(int *ret, int *bid, int *groups, int *groupsize, int *clustersize, int *offset)
{
	BAT *bn;
	int shift = 1;

	if ((bn = BATdescriptor((bat) *bid)) == NULL) {
		throw(MAL, "array.grid", RUNTIME_OBJECT_MISSING);
	}

	if (fillgrid_int(&bn, groups, groupsize, clustersize, offset, &shift) == GDK_FAIL)
		throw(MAL, "array.grid", MAL_MALLOC_FAIL);
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref((int)*ret);
	return MAL_SUCCEED;
}

str
ARRAYgridBATshift_int(int *ret, int *bid, int *groups, int *groupsize, int *clustersize, int *offset, int *shift)
{
	BAT *bn;

	if ((bn = BATdescriptor((bat) *bid)) == NULL) {
		throw(MAL, "array.grid", RUNTIME_OBJECT_MISSING);
	}
	if (fillgrid_int(&bn, groups, groupsize, clustersize, offset, shift) == GDK_FAIL)
		throw(MAL, "array.grid", MAL_MALLOC_FAIL);
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref((int)*ret);
	return MAL_SUCCEED;
}

str
ARRAYgrid_lng(lng *ret, lng *groups, lng *groupsize, lng *clustersize, lng *offset)
{
	BAT *bn;

	if (grid_lng(&bn, groups, groupsize, clustersize, offset) == GDK_FAIL)
		throw(MAL, "array.grid", MAL_MALLOC_FAIL);
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref((int)*ret);
	return MAL_SUCCEED;
}

str
ARRAYgridShift_lng(lng *ret, lng *groups, lng *groupsize, lng *clustersize, lng *offset, lng *shift)
{
	BAT *bn;

	if (gridShift_lng(&bn, groups, groupsize, clustersize, offset, shift) == GDK_FAIL)
		throw(MAL, "array.grid", MAL_MALLOC_FAIL);
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref((int)*ret);
	return MAL_SUCCEED;
}

str
ARRAYgridBAT_lng(lng *ret, lng *bid, lng *groups, lng *groupsize, lng *clustersize, lng *offset)
{
	BAT *bn;
	lng shift = 1;

	if ((bn = BATdescriptor((bat) *bid)) == NULL) {
		throw(MAL, "array.grid", RUNTIME_OBJECT_MISSING);
	}

	if (fillgrid_lng(&bn, groups, groupsize, clustersize, offset, &shift) == GDK_FAIL)
		throw(MAL, "array.grid", MAL_MALLOC_FAIL);
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref((int)*ret);
	return MAL_SUCCEED;
}

str
ARRAYgridBATshift_lng(lng *ret, lng *bid, lng *groups, lng *groupsize, lng *clustersize, lng *offset, lng *shift)
{
	BAT *bn;

	if ((bn = BATdescriptor((bat) *bid)) == NULL) {
		throw(MAL, "array.grid", RUNTIME_OBJECT_MISSING);
	}
	if (fillgrid_lng(&bn, groups, groupsize, clustersize, offset, shift) == GDK_FAIL)
		throw(MAL, "array.grid", MAL_MALLOC_FAIL);
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref((int)*ret);
	return MAL_SUCCEED;
}

#define arraymultiply(X1,X2)\
str \
ARRAYmultiply_##X1##_##X2(int *ret, int *bid, int *rid){\
	BAT *bn, *b, *r;\
	BUN p,q, s,t;\
	X2 val;\
	oid o= oid_nil;\
	BATiter bi, ri;\
	if( (b= BATdescriptor(*bid)) == NULL ){\
		 throw(MAL, "array.*", RUNTIME_OBJECT_MISSING);\
	}\
	if( (r= BATdescriptor(*rid)) == NULL ){\
		BBPreleaseref(b->batCacheid);\
		 throw(MAL, "array.*", RUNTIME_OBJECT_MISSING);\
	}\
	bn= BATnew(TYPE_void, TYPE_##X2, BATcount(b)*BATcount(r));\
	BATseqbase(bn,0);\
	bi = bat_iterator(b);\
	ri = bat_iterator(r);\
	BATloop(b,p,q){\
		BATloop(r,s,t){\
			val = (*(X1*) BUNtail(bi,p)) * (*(X1*)BUNtail(ri,s));\
			BUNfastins(bn,&o,&val);\
		}\
	}\
	bn->T->nonil = b->T->nonil & r->T->nonil;\
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret= bn->batCacheid;\
	BBPkeepref(*ret);\
	BBPreleaseref(b->batCacheid);\
	BBPreleaseref(r->batCacheid);\
	return MAL_SUCCEED;\
}

arraymultiply(sht,lng)
arraymultiply(sht,int)
arraymultiply(int,int)
arraymultiply(int,lng)
arraymultiply(lng,lng)

str
ARRAYproduct(int *ret, int *ret2, int *bid, int *rid)
{
	BAT *bn, *bm, *b, *r;
	BUN p, q, s, t;
	BATiter bi, ri;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "array.product", RUNTIME_OBJECT_MISSING);
	}
	if ((r = BATdescriptor(*rid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "array.product", RUNTIME_OBJECT_MISSING);
	}
	if (BATcount(b) > BATcount(r) || (BATcount(b) % BATcount(r)) != BATcount(b)) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(r->batCacheid);
		throw(MAL, "array.product", "Illegal argument bounds");
	}
	bn = BATnew(TYPE_void,b->ttype, BATcount(r));
	if( bn == NULL){
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(r->batCacheid);
		throw(MAL, "array.product", "Illegal argument bounds");
	}
	bm = BATnew(TYPE_void,r->ttype, BATcount(r));
	if( bm == NULL){
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(r->batCacheid);
		throw(MAL, "array.product", "Illegal argument bounds");
	}
	BATseqbase(bn,0);
	BATseqbase(bm,0);

	bi = bat_iterator(b);
	ri = bat_iterator(r);
	BATloop(r, s, t) {
		BATloop(b, p, q) {
			BUNappend(bn, BUNtail(bi,p), FALSE);
			BUNappend(bm, BUNtail(ri,s), FALSE);
			s++;
		}
		s--;
	}
	/* not sorted at best we have some fixed offset partial sorting */
	bn->hsorted = 0;
	bn->hrevsorted = 0;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	bn->T->nonil = b->T->nonil & r->T->nonil;
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;

	bm->hsorted = 0;
	bm->hrevsorted = 0;
	bm->tsorted = 0;
	bm->trevsorted = 0;
	bm->T->nonil = b->T->nonil & r->T->nonil;
	if (!(bm->batDirty&2)) bm = BATsetaccess(bm, BAT_READ); \
	*ret2 = bm->batCacheid;

	BBPkeepref(*ret);
	BBPkeepref(*ret2);
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(r->batCacheid);
	return MAL_SUCCEED;
}

str
ARRAYproject(int *ret, int *bid, int *cst)
{
	BAT *bn, *b;
	int *ptr;
	BUN i;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "array.project", RUNTIME_OBJECT_MISSING);
	}
	new_bat(bn, BATcount(b), int);
	if (bn == 0)
		throw(MAL, "array.project", MAL_MALLOC_FAIL);
	i = BATcount(b);
	add_vals(bn, i, int);
	ptr = get_ptr(bn, int);

	while (i-- > 0)
		(*(ptr ++)) = *cst;
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}


