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
 * Martin Kersten
 * Multiple association tables
 * A MAT is a convenient way to deal represent horizontal fragmented
 * tables. It combines the definitions of several, type compatible
 * BATs under a single name.
 * It is produced by the mitosis optimizer and the operations
 * are the target of the mergetable optimizer.
 *
 * The MAT is materialized when the operations
 * can not deal with the components individually,
 * or the incremental operation is not supported.
 * Normally all mat.new() operations are removed by the
 * mergetable optimizer.
 * In case a mat.new() is retained in the code, then it will
 * behave as a mat.pack();
 *
 * The primitives below are chosen to accomodate the SQL
 * front-end to produce reasonable efficient code.
 */
#include "monetdb_config.h"
#include "mat.h"
#include "group.h"

str
MATnewIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int *ret = (int*) getArgReference(stk,p,0);
	(void) cntxt;
	(void) mb; 
	if( p->argc == 1){
		*ret = 0;
	} else
		*ret= *(int*) getArgReference(stk,p,1);
	return MAL_SUCCEED;
}
str
MAThasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int *ret = (int*) getArgReference(stk,p,0);
	int i, idx = *ret;

	(void) cntxt;
	(void) mb; 
	for(i=1; i< p->argc; i++)
	if( *(int*) getArgReference(stk,p,i) == idx){
		i++;
		break;
	}
	if( i < p->argc)
		*ret= *(int*) getArgReference(stk,p,i);
	else
		*ret = 0;
	(void) mb; 
	return MAL_SUCCEED;
}
/*
 * The pack is an ordinary multi BAT insert. Oid synchronistion
 * between pieces should be ensured by the code generators.
 * The pack operation could be quite expensive, because it
 * may create a really large BAT.
 * The slice over a mat helps to avoid constructing intermediates
 * that are subsequently reduced.
 * Contrary to most operations, NIL arguments are skipped and
 * do not produce RUNTIME_OBJECT_MISSING.
 */
static str
MATpackInternal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, *ret = (int*) getArgReference(stk,p,0);
	BAT *b, *bn;
	BUN cap = 0;
	int tt = TYPE_any;
	(void) cntxt;
	(void) mb;

	for (i = 1; i < p->argc; i++) {
		int bid = stk->stk[getArg(p,i)].val.ival;
		b = BBPquickdesc(abs(bid),FALSE);
		if (b && bid < 0)
			b = BATmirror(b);
		if( b ){
			assert(BAThdense(b));
			if (tt == TYPE_any){
				tt = b->ttype;
			}
			if (!tt && tt != b->ttype)
				tt = b->ttype;
			cap += BATcount(b);
		}
	}
	if (tt == TYPE_any){
		*ret = 0;
		return MAL_SUCCEED;
	}

	bn = BATnew(TYPE_void, tt, cap);
	if (bn == NULL)
		throw(MAL, "mat.pack", MAL_MALLOC_FAIL);

	for (i = 1; i < p->argc; i++) {
		b = BATdescriptor(stk->stk[getArg(p,i)].val.ival);
		if( b ){
			if (BATcount(bn) == 0)
				BATseqbase(bn, b->H->seq);
			if (BATcount(bn) == 0)
				BATseqbase(BATmirror(bn), b->T->seq);
			BATappend(bn,b,FALSE);
			BBPunfix(b->batCacheid);
		}
	}
	assert(!bn->H->nil || !bn->H->nonil);
	assert(!bn->T->nil || !bn->T->nonil);
	BATsettrivprop(bn);
	BATderiveProps(bn,FALSE);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Enable incremental packing. The SQL front-end requires
 * fixed oid sequences.
 */
str
MATpackIncrement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int *ret = (int*) getArgReference(stk,p,0);
	int	pieces;
	BAT *b, *bb, *bn;
	size_t newsize;

	(void) cntxt;
	b = BATdescriptor( stk->stk[getArg(p,1)].val.ival);
	if ( b == NULL)
		throw(MAL, "mat.pack", RUNTIME_OBJECT_MISSING);

	if ( getArgType(mb,p,2) == TYPE_int){
		/* first step, estimate with some slack */
		pieces = stk->stk[getArg(p,2)].val.ival;
		bn = BATnew(TYPE_void, b->ttype?b->ttype:TYPE_oid, (BUN)(1.2 * BATcount(b) * pieces));
		if (bn == NULL)
			throw(MAL, "mat.pack", MAL_MALLOC_FAIL);
		/* allocate enough space for the strings */
		if ( b->T->vheap && bn->T->vheap ){
			newsize =  b->T->vheap->size * pieces;
			if (HEAPextend(bn->T->vheap, newsize, TRUE) < 0)
				throw(MAL, "mat.pack", MAL_MALLOC_FAIL);
		}
		BATseqbase(bn, b->H->seq);
		BATseqbase(BATmirror(bn), b->T->seq);
		BATappend(bn,b,FALSE);
		assert(!bn->H->nil || !bn->H->nonil);
		assert(!bn->T->nil || !bn->T->nonil);
		BBPkeepref(*ret = bn->batCacheid);
		BBPreleaseref(b->batCacheid);
	} else {
		/* remaining steps */
		bb = BATdescriptor(stk->stk[getArg(p,2)].val.ival);
		if ( bb ){
			if (BATcount(b) == 0)
				BATseqbase(b, bb->H->seq);
			if (BATcount(b) == 0)
				BATseqbase(BATmirror(b), bb->T->seq);
			BATappend(b,bb,FALSE);
		}
		assert(!b->H->nil || !b->H->nonil);
		assert(!b->T->nil || !b->T->nonil);
		BBPkeepref(*ret = b->batCacheid);
		if( bb) 
			BBPreleaseref(bb->batCacheid);
	}
	return MAL_SUCCEED;
}

static str
MATpackSliceInternal(MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, i1 = p->argc, i2 = -1, *ret = (int*) getArgReference(stk,p,0);
	BAT *b, *bn;
	BUN cap = 0, fst, lst, cnt, c;
	int ht = TYPE_any, tt = TYPE_any;

	assert(p->argc > 3);
	switch getArgType(mb,p,1) {
	case TYPE_wrd:
		fst = (BUN) *(wrd*) getArgReference(stk,p,1);
		break;
	case TYPE_lng:
		fst = (BUN) *(lng*) getArgReference(stk,p,1);
		break;
	case TYPE_int:
		fst = (BUN) *(int*) getArgReference(stk,p,1);
		break;
	default:
		throw(MAL, "mat.packSlice", "wrong type for lower bound");
	}
	switch getArgType(mb,p,2) {
	case TYPE_wrd: {
		wrd l = *(wrd*) getArgReference(stk,p,2);
		if (l == wrd_nil)
			lst = BUN_MAX; /* no upper bound */
		else
			lst = (BUN) l;
		break;
	}
	case TYPE_lng: {
		lng l = *(lng*) getArgReference(stk,p,2);
		if (l == lng_nil)
			lst = BUN_MAX; /* no upper bound */
		else
			lst = (BUN) l;
		break;
	}
	case TYPE_int: {
		int l = *(int*) getArgReference(stk,p,2);
		if (l == int_nil)
			lst = BUN_MAX; /* no upper bound */
		else
			lst = (BUN) l;
		break;
	}
	default:
		throw(MAL, "mat.packSlice", "wrong type for upper bound");
	}
	if (lst < BUN_MAX)
		lst++; /* inclusive -> exclusive upper bound */
	if (lst < fst)
		lst = fst;
	cnt = lst - fst;

	for (i = 3; i < p->argc && cap < lst; i++) {
		int bid = stk->stk[getArg(p,i)].val.ival;
		b = BBPquickdesc(abs(bid),FALSE);
		if (b && bid < 0)
			b = BATmirror(b);
		if (b == NULL)
			throw(MAL, "mat.packSlice", RUNTIME_OBJECT_MISSING);
		if (ht == TYPE_any){
			ht = b->htype;
			tt = b->ttype;
		}
		c = BATcount(b);
		if (cap <= fst) {
			/* The optimal case is when the requested slice falls completely in one BAT.
			 * In that case, we can simply return a slice (view) of that BAT.
			 * (A pitty that we have calculated the other slices as well.)
			 */
			if (lst <= cap + c) {
				b = BATdescriptor(bid);
				if( b){
					bn = BATslice(b, fst - cap, lst - cap);
					BBPunfix(b->batCacheid);
					BBPkeepref(*ret = bn->batCacheid);
				} else
					throw(MAL, "mat.packSlice", RUNTIME_OBJECT_MISSING);

				return MAL_SUCCEED;
			}
			if (fst < cap + c) {
				/* fst falls in BAT i1 == i */
				i1 = i;
				fst -= cap;
				lst -= cap;
				cap = 0;
			}
		}
		cap += c;
	}
	/* lst falls in BAT i2 == i-1 */
	i2 = i - 1;
	if (cap <= fst) /* i.e., (i1 > i2) */
		cap = 0;
	else
		cap -= fst;
	cnt = MIN(cnt, cap);

	bn = BATnew(ht, tt, cnt);
	if (bn == NULL)
		throw(MAL, "mat.packSlice", MAL_MALLOC_FAIL);
	/* must set seqbase or else BATins will not materialize column */
	if (ht == TYPE_void)
		BATseqbase(bn, 0);
	if (tt == TYPE_void)
		BATseqbase(BATmirror(bn), 0);

	for (i = i1; i <= i2; i++) {
		b = BATdescriptor(stk->stk[getArg(p,i)].val.ival);
		if (b == NULL){
			BBPreleaseref(bn->batCacheid);
			throw(MAL, "mat.packSlice", RUNTIME_OBJECT_MISSING);
		}
		c = BATcount(b);
		/* use the right oid ranges, don't change the input */
		if (i == i1 && fst > 0) {
			BAT *bb = b;
			b = BATslice(bb, fst, c);
			BBPunfix(bb->batCacheid);
		} else
		if (i == i2 && lst < c) {
			BAT *bb = b;
			b = BATslice(bb, 0, lst);
			BBPunfix(bb->batCacheid);
		}
		BATins(bn,b,FALSE);
		lst -= c;
		BBPunfix(b->batCacheid);
	}
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
MATpack2Internal(MalStkPtr stk, InstrPtr p)
{
	int i,*ret;
	BAT *b, *bn;
	BUN cap=0;

	b= BATdescriptor(stk->stk[getArg(p,1)].val.ival);
	if( b == NULL)
		throw(MAL, "mat.pack", RUNTIME_OBJECT_MISSING);
	bn = BATcopy(b, b->htype, b->ttype, TRUE);
	BBPunfix(b->batCacheid);
	if( bn == NULL)
		throw(MAL, "mat.pack", MAL_MALLOC_FAIL);

	for(i = 2; i < p->argc; i++){
		b= BATdescriptor(stk->stk[getArg(p,i)].val.ival);
		if( b == NULL){
			BBPreleaseref(bn->batCacheid);
			throw(MAL, "mat.pack", RUNTIME_OBJECT_MISSING);
		}
		cap += BATcount(b);
		BBPunfix(b->batCacheid);
	}
	bn = BATextend(bn, cap);
	if( bn == NULL)
		throw(MAL, "mat.pack", RUNTIME_OBJECT_MISSING);
	for( i = 2; i < p->argc; i++){
		b= BATdescriptor(stk->stk[getArg(p,i)].val.ival);
		if( b == NULL){
			BBPreleaseref(bn->batCacheid);
			throw(MAL, "mat.pack", RUNTIME_OBJECT_MISSING);
		}
		BATappend(bn,b,FALSE);
		BBPunfix(b->batCacheid);
	}
	ret= (int*) getArgReference(stk,p,0);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MATpack2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) mb;
	return MATpack2Internal(stk,p);
}

/*
 * the next one is specific to the centipede, where we carve out
 * a portion of table based on the value ids. They are simply glued
 * together in a void-headed bat.
 * The special case that only one partition is filled is taken separately.
*/
static str
MATpack3Internal(MalStkPtr stk, InstrPtr p)
{
	int i,*ret, nonempty=0, ref=0, type =0;
	BAT *b, *bn;
	BUN cap=0;

	for(i = 1; i < p->argc; i++){
		b= BATdescriptor(stk->stk[getArg(p,i)].val.ival);
		if( b == NULL)
			throw(MAL, "mat.pack", RUNTIME_OBJECT_MISSING);
		type= b->ttype;	/* all tail types are the same */
		cap += BATcount(b);
		nonempty += (BATcount(b) != 0);
		if ( BATcount(b)) 
			ref = i;
		BBPunfix(b->batCacheid);
	}
	if ( nonempty == 1) {
		b= BATdescriptor(stk->stk[getArg(p,ref)].val.ival);
		if( b == NULL)
			throw(MAL, "mat.pack", RUNTIME_OBJECT_MISSING);
		if ( b->htype == TYPE_void && b->hseqbase == 0 ) {
			/* steal the BAT */
			ret= (int*) getArgReference(stk,p,0);
			BBPkeepref(*ret = b->batCacheid);
			return MAL_SUCCEED;
		} 
		bn = BATcopy(b, TYPE_void, type, TRUE);
		BBPreleaseref(b->batCacheid);
	} else {
		bn = BATnew( TYPE_void, type, cap);
		for( i = 1; bn && i < p->argc; i++){
			b= BATdescriptor(stk->stk[getArg(p,i)].val.ival);
			if( b == NULL){
				BBPreleaseref(bn->batCacheid);
				throw(MAL, "mat.pack", RUNTIME_OBJECT_MISSING);
			}
			BATappend(bn,b,FALSE);
			BBPreleaseref(b->batCacheid);
		}
	}
	if( bn == NULL)
		throw(MAL, "mat.pack", MAL_MALLOC_FAIL);
	BATseqbase(bn,0);
	ret= (int*) getArgReference(stk,p,0);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MATpack3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) mb;
	return MATpack3Internal(stk,p);
}

str
MATpack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	return MATpackInternal(cntxt,mb,stk,p);
}

// merging multiple OID lists, optimized for empty bats
// Further improvement should come from multi-bat merging.
str
MATmergepack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j= 0, *ret = (int*) getArgReference(stk,p,0);
	int top=0;
	oid  **o_end, **o_src, *o, *oo, onxt;
	BAT *b, *bn, *bm, **bats;
	BUN cap = 0;

	(void)cntxt;
	(void)mb;
	bats = (BAT**) GDKzalloc(sizeof(BAT*) * p->argc);
	o_end = (oid**) GDKzalloc(sizeof(oid*) * p->argc);
	o_src = (oid**) GDKzalloc(sizeof(oid*) * p->argc);

	if ( bats ==0 || o_end == 0 || o_src == 0){
		if (bats) GDKfree(bats);
		if (o_src) GDKfree(o_src);
		if (o_end) GDKfree(o_end);
		throw(MAL,"mat.mergepack",MAL_MALLOC_FAIL);
	}
	for (i = 1; i < p->argc; i++) {
		int bid = stk->stk[getArg(p,i)].val.ival;
		b = BATdescriptor(abs(bid));
		if (b ){
			cap += BATcount(b);
			if ( BATcount(b) ){
				// pre-sort the arguments
				onxt = *(oid*) Tloc(b,BUNfirst(b));
				for( j =top; j > 0 && onxt < *o_src[j-1]; j--){
					o_src[j] = o_src[j-1];
					o_end[j] = o_end[j-1];
					bats[j] = bats[j-1];
				}
				o_src[j] = (oid*) Tloc(b,BUNfirst(b));
				o_end[j] = o_src[j] + BATcount(b);
				bats[j] = b;
				top++;
			}
		}
	}

	bn = BATnew(TYPE_void, TYPE_oid, cap);
	if (bn == NULL){
		GDKfree(bats);
		GDKfree(o_src);
		GDKfree(o_end);
		throw(MAL, "mat.pack", MAL_MALLOC_FAIL);
	}

	if ( cap == 0){
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), 0);
		BBPkeepref(*ret = bn->batCacheid);
		GDKfree(bats);
		GDKfree(o_src);
		GDKfree(o_end);
		return MAL_SUCCEED;
	}
	BATseqbase(bn, bats[0]->hseqbase);
	// UNROLL THE MULTI-BAT MERGE
	o = (oid*) Tloc(bn,BUNfirst(bn));
	while( top){
		*o++ = *o_src[0];
		o_src[0]++;
		if( o_src[0] == o_end[0]){
			// remove this one
			for(j=0; j< top; j++){
				o_src[j]= o_src[j+1];
				o_end[j]= o_end[j+1];
				bats[j] = bats[j+1];
			}
			top--;
		} else{
			// resort priority queue
			onxt= *o_src[0];
			for( j=1; j< top && onxt > *o_src[j]; j++){
				oo = o_src[j]; o_src[j]= o_src[j-1]; o_src[j-1]= oo;
				oo = o_end[j]; o_end[j]= o_end[j-1]; o_end[j-1]= oo;
				bm = bats[j]; bats[j]=bats[j-1]; bats[j-1] = bm;
			}
		}
	}
	for( i=0; i< top; i++)
		BBPunfix(bats[i]->batCacheid);
    BATsetcount(bn, (BUN) (o - (oid *) Tloc(bn, BUNfirst(bn))));
    BATseqbase(bn, 0);
	BATsettrivprop(bn);
	GDKfree(bats);
	GDKfree(o_src);
	GDKfree(o_end);
    /* properties */
    bn->trevsorted = 0;
    bn->tsorted = 1;
    bn->tkey = 1;
    bn->T->nil = 0;
    bn->T->nonil = 1;
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MATpackValues(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,*ret, type, first = 1;
	BAT *bn;

	(void) cntxt;
	type = getArgType(mb,p,first);
	bn = BATnew(TYPE_void, type, p->argc);
	if( bn == NULL)
		throw(MAL, "mat.pack", MAL_MALLOC_FAIL);

	if (ATOMvarsized(type)) {
		for(i = first; i < p->argc; i++)
			BUNappend(bn, stk->stk[getArg(p,i)].val.sval, TRUE);
	} else {
		for(i = first; i < p->argc; i++)
			BUNappend(bn, getArgReference(stk, p, i), TRUE);
	}
	ret= (int*) getArgReference(stk,p,0);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}
str
MATpackSlice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	return MATpackSliceInternal(mb,stk,p);
}


str
MATprint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt; (void) mb; (void) stk; (void) p;
	return MAL_SUCCEED;
}

str
MATdummy(int *ret, str *grp){
	(void) grp; (void) ret;
	return MAL_SUCCEED;
}
str
MATinfo(int *ret, str *grp, str *elm){
	(void) grp; (void) elm; (void) ret;
	return MAL_SUCCEED;
}

static BAT *
MATproject_any( BAT *map, BAT **bats, int len )
{
	BAT *res;
	int i;
	BUN j, cnt = BATcount(map);
	BATiter *bats_i;
	BUN *batsT;
	bte *mapT;

	res = BATnew(TYPE_void, bats[0]->ttype, cnt);
	batsT = (BUN*)GDKmalloc(sizeof(BUN) * len);
	bats_i = (BATiter*)GDKmalloc(sizeof(BATiter) * len);
	if (res == NULL || batsT == NULL || bats_i == NULL) {
		if (res)
			BBPreclaim(res);
		if (batsT)
			GDKfree(batsT);
		if (bats_i)
			GDKfree(bats_i);
		return NULL;
	}
	BATseqbase(res, map->hseqbase);
	mapT = (bte*)Tloc(map, 0);
	for (i=0; i<len; i++) {
		batsT[i] = 0;
		bats_i[i] = bat_iterator(bats[i]);
	}
	for (j=0; j<cnt; j++)
		BUNappend(res, BUNtail(bats_i[mapT[j]], batsT[mapT[j]]++), FALSE);
	GDKfree(batsT);
	GDKfree(bats_i);
	return res;
}

/* The type-specific projection operators */
static BAT *
MATproject_bte( BAT *map, BAT **bats, int len, int ttpe )
{
	BAT *res;
	int i;
	BUN j, cnt = BATcount(map);
	bte *resT, **batsT;
	bte *mapT;

	res = BATnew(TYPE_void, ttpe, cnt);
	batsT = (bte**)GDKmalloc(sizeof(bte*) * len);
	if (res == NULL || batsT == NULL) {
		if (res)
			BBPreclaim(res);
		if (batsT)
			GDKfree(batsT);
		return NULL;
	}
	BATseqbase(res, map->hseqbase);
	resT = (bte*)Tloc(res, 0);
	mapT = (bte*)Tloc(map, 0);
	for (i=0; i<len; i++)
		batsT[i] = (bte*)Tloc(bats[i], 0);
	for (j=0; j<cnt; j++)
		resT[j] = *batsT[mapT[j]]++;
	BATsetcount(res, j);
	res->hrevsorted = j <= 1;
	GDKfree(batsT);
	return res;
}

static BAT *
MATproject_sht( BAT *map, BAT **bats, int len, int ttpe )
{
	BAT *res;
	int i;
	BUN j, cnt = BATcount(map);
	sht *resT, **batsT;
	bte *mapT;

	res = BATnew(TYPE_void, ttpe, cnt);
	batsT = (sht**)GDKmalloc(sizeof(sht*) * len);
	if (res == NULL || batsT == NULL) {
		if (res)
			BBPreclaim(res);
		if (batsT)
			GDKfree(batsT);
		return NULL;
	}
	BATseqbase(res, map->hseqbase);
	resT = (sht*)Tloc(res, 0);
	mapT = (bte*)Tloc(map, 0);
	for (i=0; i<len; i++)
		batsT[i] = (sht*)Tloc(bats[i], 0);
	for (j=0; j<cnt; j++)
		resT[j] = *batsT[mapT[j]]++;
	BATsetcount(res, j);
	res->hrevsorted = j <= 1;
	GDKfree(batsT);
	return res;
}

static BAT *
MATproject_int( BAT *map, BAT **bats, int len, int ttpe )
{
	BAT *res;
	int i;
	BUN j, cnt = BATcount(map);
	int *resT, **batsT;
	bte *mapT;

	res = BATnew(TYPE_void, ttpe, cnt);
	batsT = (int**)GDKmalloc(sizeof(int*) * len);
	if (res == NULL || batsT == NULL) {
		if (res)
			BBPreclaim(res);
		if (batsT)
			GDKfree(batsT);
		return NULL;
	}
	BATseqbase(res, map->hseqbase);
	resT = (int*)Tloc(res, 0);
	mapT = (bte*)Tloc(map, 0);
	for (i=0; i<len; i++)
		batsT[i] = (int*)Tloc(bats[i], 0);
	for (j=0; j<cnt; j++)
		resT[j] = *batsT[mapT[j]]++;
	BATsetcount(res, j);
	res->hrevsorted = j <= 1;
	GDKfree(batsT);
	return res;
}

static BAT *
MATproject_lng( BAT *map, BAT **bats, int len, int ttpe )
{
	BAT *res;
	int i;
	BUN j, cnt = BATcount(map);
	lng *resT, **batsT;
	bte *mapT;

	res = BATnew(TYPE_void, ttpe, cnt);
	batsT = (lng**)GDKmalloc(sizeof(lng*) * len);
	if (res == NULL || batsT == NULL) {
		if (res)
			BBPreclaim(res);
		if (batsT)
			GDKfree(batsT);
		return NULL;
	}
	BATseqbase(res, map->hseqbase);
	resT = (lng*)Tloc(res, 0);
	mapT = (bte*)Tloc(map, 0);
	for (i=0; i<len; i++)
		batsT[i] = (lng*)Tloc(bats[i], 0);
	for (j=0; j<cnt; j++)
		resT[j] = *batsT[mapT[j]]++;
	BATsetcount(res, j);
	res->hrevsorted = j <= 1;
	GDKfree(batsT);
	return res;
}

/*
 *  Mitosis-pieces are usually slices (views) of a base table/BAT.
 *  For variable-size atoms, this means that the vheap's of all pieces are
 *  likely to be identical (full) views of the same original base-BAT vheap.
 *  If so, the result of MATproject can also simply be a view of that very
 *  original base-BAT vheap (rather than a new view with all value (re-)inserted),
 *  i.e., we only need to build the result's tail BUN heap by copying the
 *  pointers (references) into the vheap from the input BATs (pieces).
 */
static BAT *
MATproject_var( BAT *map, BAT **bats, int len )
{
	BAT *res = NULL;
	int i = 0, j = -1;
	bit shared_Tvheaps = FALSE;

	while (i < len && BATcount(bats[i]) == 0)
		i++;
	if (i < len &&
	    bats[i]->tvarsized &&
	    bats[i]->T->vheap != NULL &&
	    bats[i]->T->vheap->parentid > 0) {
		shared_Tvheaps = TRUE;
		j = i++;
	}
	while (shared_Tvheaps && i < len) {
		shared_Tvheaps &= (BATcount(bats[i]) == 0 ||
		                   ( bats[i]->ttype == bats[j]->ttype &&
		                     bats[i]->batRestricted == BAT_READ &&
		                     bats[i]->T->vheap == bats[j]->T->vheap &&
		                     bats[i]->T->width == bats[j]->T->width &&
		                     bats[i]->T->shift == bats[j]->T->shift ));
		i++;
	}
	if (shared_Tvheaps) {
		switch (bats[j]->T->width) {
		case sizeof(bte):
			res = MATproject_bte(map, bats, len, TYPE_bte);
			break;
		case sizeof(sht):
			res = MATproject_sht(map, bats, len, TYPE_sht);
			break;
		case sizeof(int):
			res = MATproject_int(map, bats, len, TYPE_int);
			break;
		case sizeof(lng):
			res = MATproject_lng(map, bats, len, TYPE_lng);
			break;
		default:
			/* can (should) not happen */
			assert(0);
			return MATproject_any( map, bats, len );
		}
		if (res != NULL) {
			res->tvarsized = 1;
			res->ttype = bats[j]->ttype;
			res->T->vheap = bats[j]->T->vheap;
			res->T->width = bats[j]->T->width;
			res->T->shift = bats[j]->T->shift;
			BBPshare(bats[j]->T->vheap->parentid);
		}
	} else
		res = MATproject_any( map, bats, len );
	return res;
}

static int
MATnonil( BAT **bats, int len)
{
	int i, nonil = 1;

	for (i=0; i<len && nonil; i++) {
		nonil &= bats[i]->T->nonil;
	}
	return nonil;
}

static BAT *
MATproject_( BAT *map, BAT **bats, int len ) 
{
	BAT *res = NULL;

	if (ATOMstorage(bats[0]->ttype) <= TYPE_void) {
		/*error*/
	} else if (ATOMvarsized(bats[0]->ttype)) {
		res = MATproject_var(map, bats, len);
	} else if (ATOMsize(bats[0]->ttype) == sizeof(bte)) {
		res = MATproject_bte(map, bats, len, bats[0]->ttype);
	} else if (ATOMsize(bats[0]->ttype) == sizeof(sht)) {
		res = MATproject_sht(map, bats, len, bats[0]->ttype);
	} else if (ATOMsize(bats[0]->ttype) == sizeof(int)) {
		res = MATproject_int(map, bats, len, bats[0]->ttype);
	} else if (ATOMsize(bats[0]->ttype) == sizeof(lng)) {
		res = MATproject_lng(map, bats, len, bats[0]->ttype);
	} else {
		res = MATproject_any(map, bats, len);
	}
	if(res){
		res->tsorted = 0;
		res->trevsorted = 0;
		res->T->nonil = MATnonil(bats, len);
	}
	return res;
}

str
MATproject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res_id = (bat*) getArgReference(stk,pci,0);
	bat map_id = *(bat*) getArgReference(stk,pci,1);
	BAT *res = NULL, *map;
	/* rest of the args are parts, (excluding result and map) */
	BAT **bats = GDKzalloc(sizeof(BAT*) * pci->argc - 2);
	BUN bcnt = 0; 
	int i, len = pci->argc-2, sorted = 1;

	(void) cntxt; (void) mb; (void) stk; 
	if( bats == NULL)
		throw(SQL, "mat.project",MAL_MALLOC_FAIL);
	map = BATdescriptor(map_id);
	if (!map)
		goto error;
	for (i=2; i<pci->argc; i++) {
		bat id = *(bat*) getArgReference(stk,pci,i);
		bats[i-2] = BATdescriptor(id);
		if (!bats[i-2])
			goto error;
		bcnt += BATcount(bats[i-2]);
		if (!bats[i-2]->T->sorted)
			sorted = 0;
	}
	assert(bcnt == BATcount(map));

	res = MATproject_(map, bats, len );
	if (sorted && res)
		BATordered(BATmirror(res));
error:
	if (map) BBPunfix(map->batCacheid);
	if (bats) {
		for (i=0; i<len && bats[i]; i++)
			BBPunfix(bats[i]->batCacheid);
		GDKfree(bats);
	}
	if (res) {
		BATsettrivprop(res);
		BBPkeepref( *res_id = res->batCacheid);
		return MAL_SUCCEED;
	}
	throw(SQL, "mat.project","Cannot access descriptor");
}

static BAT*
MATsortloop_rev( bte *map_res, BAT *i1, bte *map_i1, BUN cnt_i1, BAT *i2, bte map_i2, BUN cnt_i2) 
{
	int c;
	BUN val_i1 = BUNfirst(i1);
	BUN val_i2 = BUNfirst(i2);
	BUN end_i1 = val_i1 + cnt_i1;
	BUN end_i2 = val_i2 + cnt_i2;
	BATiter bi_i1 = bat_iterator(i1); 
	BATiter bi_i2 = bat_iterator(i2);
	int (*cmp) (const void *, const void *) = BATatoms[i1->ttype].atomCmp;
	BAT *res = BATnew(TYPE_void, i1->ttype, cnt_i1 + cnt_i2);

	BATseqbase(res, 0);
	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if ((c = cmp(BUNtail(bi_i1,val_i1),BUNtail(bi_i2,val_i2))) >= 0) {
				BUNappend(res, BUNtail(bi_i1,val_i1), FALSE);
				*map_res++ = 0;
				val_i1++;
			} else if (c < 0) {
				BUNappend(res, BUNtail(bi_i2,val_i2), FALSE);
				*map_res++ = map_i2;
				val_i2++;
			}
		}
		while ( val_i1 < end_i1 ) {
			BUNappend(res, BUNtail(bi_i1,val_i1), FALSE);
			*map_res++ = 0;
			val_i1++;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if ((c = cmp(BUNtail(bi_i1,val_i1),BUNtail(bi_i2,val_i2))) >= 0) {
				BUNappend(res, BUNtail(bi_i1,val_i1), FALSE);
				*map_res++ = *map_i1++;
				val_i1++;
			} else if (c < 0) {
				BUNappend(res, BUNtail(bi_i2,val_i2), FALSE);
				*map_res++ = map_i2;
				val_i2++;
			}
		}
		while ( val_i1 < end_i1 ) {
			BUNappend(res, BUNtail(bi_i1,val_i1), FALSE);
			*map_res++ = *map_i1++;
			val_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		BUNappend(res, BUNtail(bi_i2,val_i2), FALSE);
		*map_res++ = map_i2;
		val_i2++;
	}
	return res;
}

static BAT*
MATsortloop_( bte *map_res, BAT *i1, bte *map_i1, BUN cnt_i1, BAT *i2, bte map_i2, BUN cnt_i2) 
{
	int c;
	BUN val_i1 = BUNfirst(i1);
	BUN val_i2 = BUNfirst(i2);
	BUN end_i1 = val_i1 + cnt_i1;
	BUN end_i2 = val_i2 + cnt_i2;
	BATiter bi_i1 = bat_iterator(i1); 
	BATiter bi_i2 = bat_iterator(i2);
	int (*cmp) (const void *, const void *) = BATatoms[i1->ttype].atomCmp;
	BAT *res = BATnew(TYPE_void, i1->ttype, cnt_i1 + cnt_i2);

	BATseqbase(res, 0);
	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if ((c = cmp(BUNtail(bi_i1,val_i1),BUNtail(bi_i2,val_i2))) <= 0) {
				BUNappend(res, BUNtail(bi_i1,val_i1), FALSE);
				*map_res++ = 0;
				val_i1++;
			} else if (c > 0) {
				BUNappend(res, BUNtail(bi_i2,val_i2), FALSE);
				*map_res++ = map_i2;
				val_i2++;
			}
		}
		while ( val_i1 < end_i1 ) {
			BUNappend(res, BUNtail(bi_i1,val_i1), FALSE);
			*map_res++ = 0;
			val_i1++;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if ((c = cmp(BUNtail(bi_i1,val_i1),BUNtail(bi_i2,val_i2))) <= 0) {
				BUNappend(res, BUNtail(bi_i1,val_i1), FALSE);
				*map_res++ = *map_i1++;
				val_i1++;
			} else if (c > 0) {
				BUNappend(res, BUNtail(bi_i2,val_i2), FALSE);
				*map_res++ = map_i2;
				val_i2++;
			}
		}
		while ( val_i1 < end_i1 ) {
			BUNappend(res, BUNtail(bi_i1,val_i1), FALSE);
			*map_res++ = *map_i1++;
			val_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		BUNappend(res, BUNtail(bi_i2,val_i2), FALSE);
		*map_res++ = map_i2;
		val_i2++;
	}
	return res;
}

static BAT *
MATsort_any( BAT **map, BAT **bats, int len, BUN cnt, int rev )
{
	BAT *res = 0, *in;
	int i;
	bte *mapT;
	BUN len1, len2;
	bte *map_in = NULL;

	*map = BATnew(TYPE_void, TYPE_bte, cnt);
	BATseqbase(*map, 0);
	mapT = (bte*)Tloc(*map, 0);
	/* merge */
	/* TODO: change into a tree version */
	in = bats[0];
	len1 = BATcount(in);
	for (i=1; i<len; i++) {
		len2 = BATcount(bats[i]);
		if (rev) {
			res = MATsortloop_rev( 
				mapT+cnt-len1-len2, 
		        	in, map_in, len1, 
				bats[i], i, len2);
		} else {
			res = MATsortloop_( 
				mapT+cnt-len1-len2, 
		        	in, map_in, len1, 
				bats[i], i, len2);
		}
		if (i != 1)
			BBPunfix(in->batCacheid);
		in = res;
		map_in = mapT+cnt-len1-len2;
		len1 += len2;
	}
	BATsetcount(*map, len1);
	(*map)->hrevsorted = len1 <= 1;
	return res;
}

static int
MATsortloop_bte_rev( bte *val_res, bte *map_res, bte *val_i1, bte *map_i1, BUN cnt_i1, bte *val_i2, bte map_i2, BUN cnt_i2 ) {

	bte *end_i1 = val_i1 + cnt_i1;
	bte *end_i2 = val_i2 + cnt_i2;

	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 >= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = 0;
			} else if (*val_i1 < *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = 0;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 >= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = *map_i1++;
			} else if (*val_i1 < *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = *map_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		*val_res++ = *val_i2++;
		*map_res++ = map_i2;
	}
	return 0;
}

static int
MATsortloop_bte_( bte *val_res, bte *map_res, bte *val_i1, bte *map_i1, BUN cnt_i1, bte *val_i2, bte map_i2, BUN cnt_i2 ) {

	bte *end_i1 = val_i1 + cnt_i1;
	bte *end_i2 = val_i2 + cnt_i2;

	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 <= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = 0;
			} else if (*val_i1 > *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = 0;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 <= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = *map_i1++;
			} else if (*val_i1 > *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = *map_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		*val_res++ = *val_i2++;
		*map_res++ = map_i2;
	}
	return 0;
}
/* combined pair */
static int
MATsortloop_sht_rev( sht *val_res, bte *map_res, sht *val_i1, bte *map_i1, BUN cnt_i1, sht *val_i2, bte map_i2, BUN cnt_i2 ) {

	sht *end_i1 = val_i1 + cnt_i1;
	sht *end_i2 = val_i2 + cnt_i2;

	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 >= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = 0;
			} else if (*val_i1 < *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = 0;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 >= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = *map_i1++;
			} else if (*val_i1 < *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = *map_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		*val_res++ = *val_i2++;
		*map_res++ = map_i2;
	}
	return 0;
}

static int
MATsortloop_sht_( sht *val_res, bte *map_res, sht *val_i1, bte *map_i1, BUN cnt_i1, sht *val_i2, bte map_i2, BUN cnt_i2 ) {

	sht *end_i1 = val_i1 + cnt_i1;
	sht *end_i2 = val_i2 + cnt_i2;

	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 <= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = 0;
			} else if (*val_i1 > *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = 0;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 <= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = *map_i1++;
			} else if (*val_i1 > *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = *map_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		*val_res++ = *val_i2++;
		*map_res++ = map_i2;
	}
	return 0;
}

static int
MATsortloop_int_rev( int *val_res, bte *map_res, int *val_i1, bte *map_i1, BUN cnt_i1, int *val_i2, bte map_i2, BUN cnt_i2 ) {

	int *end_i1 = val_i1 + cnt_i1;
	int *end_i2 = val_i2 + cnt_i2;

	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 >= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = 0;
			} else if (*val_i1 < *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = 0;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 >= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = *map_i1++;
			} else if (*val_i1 < *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = *map_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		*val_res++ = *val_i2++;
		*map_res++ = map_i2;
	}
	return 0;
}

static int
MATsortloop_int_( int *val_res, bte *map_res, int *val_i1, bte *map_i1, BUN cnt_i1, int *val_i2, bte map_i2, BUN cnt_i2 ) {

	int *end_i1 = val_i1 + cnt_i1;
	int *end_i2 = val_i2 + cnt_i2;

	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 <= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = 0;
			} else if (*val_i1 > *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = 0;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 <= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = *map_i1++;
			} else if (*val_i1 > *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = *map_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		*val_res++ = *val_i2++;
		*map_res++ = map_i2;
	}
	return 0;
}

static int
MATsortloop_lng_rev( lng *val_res, bte *map_res, lng *val_i1, bte *map_i1, BUN cnt_i1, lng *val_i2, bte map_i2, BUN cnt_i2 ) {

	lng *end_i1 = val_i1 + cnt_i1;
	lng *end_i2 = val_i2 + cnt_i2;

	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 >= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = 0;
			} else if (*val_i1 < *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = 0;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 >= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = *map_i1++;
			} else if (*val_i1 < *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = *map_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		*val_res++ = *val_i2++;
		*map_res++ = map_i2;
	}
	return 0;
}

static int
MATsortloop_lng_( lng *val_res, bte *map_res, lng *val_i1, bte *map_i1, BUN cnt_i1, lng *val_i2, bte map_i2, BUN cnt_i2 ) {

	lng *end_i1 = val_i1 + cnt_i1;
	lng *end_i2 = val_i2 + cnt_i2;

	if (map_i1 == NULL) {
		/* map_i1 = 0 */
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 <= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = 0;
			} else if (*val_i1 > *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = 0;
		}
	} else {
		while ( val_i1 < end_i1 && val_i2 < end_i2) {
			if (*val_i1 <= *val_i2) {
				*val_res++ = *val_i1++;
				*map_res++ = *map_i1++;
			} else if (*val_i1 > *val_i2) {
				*val_res++ = *val_i2++;
				*map_res++ = map_i2;
			}
		}
		while ( val_i1 < end_i1 ) {
			*val_res++ = *val_i1++;
			*map_res++ = *map_i1++;
		}
	}
	while ( val_i2 < end_i2 ) {
		*val_res++ = *val_i2++;
		*map_res++ = map_i2;
	}
	return 0;
}

/* multi-bat sort primitives */
static BAT *
MATsort_lng( BAT **map, BAT **bats, int len, BUN cnt, int rev )
{
	BAT *res;
	int i;
	lng *resT, **batsT, *in;
	bte *mapT;
	BUN len1, len2;
	bte *map_in = NULL;

	res = BATnew(TYPE_void, bats[0]->ttype, cnt);
	*map = BATnew(TYPE_void, TYPE_bte, cnt);
	BATseqbase(res, 0);
	BATseqbase(*map, 0);
	resT = (lng*)Tloc(res, 0);
	mapT = (bte*)Tloc(*map, 0);
	batsT = (lng**)GDKmalloc(sizeof(lng*) * len);
	for (i=0; i<len; i++)
		batsT[i] = (lng*)Tloc(bats[i], 0);
	/* merge */
	in = batsT[0];
	len1 = BATcount(bats[0]);
	map_in = NULL;
	/* TODO: change into a tree version */
	for (i=1; i<len; i++) {
		len2 = BATcount(bats[i]);
		if (rev) {
			MATsortloop_lng_rev( resT+cnt-len1-len2, 
					mapT+cnt-len1-len2, 
				        in, map_in, len1, 
					batsT[i], i, len2);
		} else {
			MATsortloop_lng_( resT+cnt-len1-len2, 
					mapT+cnt-len1-len2, 
				        in, map_in, len1, 
					batsT[i], i, len2);
		}
		in = resT+cnt-len1-len2;
		map_in = mapT+cnt-len1-len2;
		len1 += len2;
	}
	BATsetcount(res, len1);
	BATsetcount(*map, len1);
	res->hrevsorted = len1 <= 1;
	(*map)->hrevsorted = len1 <= 1;
	GDKfree(batsT);
	return res;
}
static BAT *
MATsort_int( BAT **map, BAT **bats, int len, BUN cnt, int rev )
{
	BAT *res;
	int i;
	int *resT, **batsT, *in;
	bte *mapT;
	BUN len1, len2;
	bte *map_in = NULL;

	res = BATnew(TYPE_void, bats[0]->ttype, cnt);
	*map = BATnew(TYPE_void, TYPE_bte, cnt);
	BATseqbase(res, 0);
	BATseqbase(*map, 0);
	resT = (int*)Tloc(res, 0);
	mapT = (bte*)Tloc(*map, 0);
	batsT = (int**)GDKmalloc(sizeof(int*) * len);
	for (i=0; i<len; i++)
		batsT[i] = (int*)Tloc(bats[i], 0);
	/* merge */
	in = batsT[0];
	len1 = BATcount(bats[0]);
	map_in = NULL;
	/* TODO: change into a tree version */
	for (i=1; i<len; i++) {
		len2 = BATcount(bats[i]);
		if (rev) {
			MATsortloop_int_rev( resT+cnt-len1-len2, 
					mapT+cnt-len1-len2, 
				        in, map_in, len1, 
					batsT[i], i, len2);
		} else {
			MATsortloop_int_( resT+cnt-len1-len2, 
					mapT+cnt-len1-len2, 
				        in, map_in, len1, 
					batsT[i], i, len2);
		}
		in = resT+cnt-len1-len2;
		map_in = mapT+cnt-len1-len2;
		len1 += len2;
	}
	BATsetcount(res, len1);
	BATsetcount(*map, len1);
	res->hrevsorted = len1 <= 1;
	(*map)->hrevsorted = len1 <= 1;
	GDKfree(batsT);
	return res;
}
static BAT *
MATsort_sht( BAT **map, BAT **bats, int len, BUN cnt, int rev )
{
	BAT *res;
	int i;
	sht *resT, **batsT, *in;
	bte *mapT;
	BUN len1, len2;
	bte *map_in = NULL;

	res = BATnew(TYPE_void, bats[0]->ttype, cnt);
	*map = BATnew(TYPE_void, TYPE_bte, cnt);
	BATseqbase(res, 0);
	BATseqbase(*map, 0);
	resT = (sht*)Tloc(res, 0);
	mapT = (bte*)Tloc(*map, 0);
	batsT = (sht**)GDKmalloc(sizeof(sht*) * len);
	for (i=0; i<len; i++)
		batsT[i] = (sht*)Tloc(bats[i], 0);
	/* merge */
	in = batsT[0];
	len1 = BATcount(bats[0]);
	map_in = NULL;
	/* TODO: change into a tree version */
	for (i=1; i<len; i++) {
		len2 = BATcount(bats[i]);
		if (rev) {
			MATsortloop_sht_rev( resT+cnt-len1-len2, 
					mapT+cnt-len1-len2, 
				        in, map_in, len1, 
					batsT[i], i, len2);
		} else {
			MATsortloop_sht_( resT+cnt-len1-len2, 
					mapT+cnt-len1-len2, 
				        in, map_in, len1, 
					batsT[i], i, len2);
		}
		in = resT+cnt-len1-len2;
		map_in = mapT+cnt-len1-len2;
		len1 += len2;
	}
	BATsetcount(res, len1);
	BATsetcount(*map, len1);
	res->hrevsorted = len1 <= 1;
	(*map)->hrevsorted = len1 <= 1;
	GDKfree(batsT);
	return res;
}
static BAT *
MATsort_bte( BAT **map, BAT **bats, int len, BUN cnt, int rev )
{
	BAT *res;
	int i;
	bte *resT, **batsT, *in;
	bte *mapT;
	BUN len1, len2;
	bte *map_in = NULL;

	res = BATnew(TYPE_void, bats[0]->ttype, cnt);
	*map = BATnew(TYPE_void, TYPE_bte, cnt);
	BATseqbase(res, 0);
	BATseqbase(*map, 0);
	resT = (bte*)Tloc(res, 0);
	mapT = (bte*)Tloc(*map, 0);
	batsT = (bte**)GDKmalloc(sizeof(bte*) * len);
	for (i=0; i<len; i++)
		batsT[i] = (bte*)Tloc(bats[i], 0);
	/* merge */
	in = batsT[0];
	len1 = BATcount(bats[0]);
	map_in = NULL;
	/* TODO: change into a tree version */
	for (i=1; i<len; i++) {
		len2 = BATcount(bats[i]);
		if (rev) {
			MATsortloop_bte_rev( resT+cnt-len1-len2, 
					mapT+cnt-len1-len2, 
				        in, map_in, len1, 
					batsT[i], i, len2);
		} else {
			MATsortloop_bte_( resT+cnt-len1-len2, 
					mapT+cnt-len1-len2, 
				        in, map_in, len1, 
					batsT[i], i, len2);
		}
		in = resT+cnt-len1-len2;
		map_in = mapT+cnt-len1-len2;
		len1 += len2;
	}
	BATsetcount(res, len1);
	BATsetcount(*map, len1);
	res->hrevsorted = len1 <= 1;
	(*map)->hrevsorted = len1 <= 1;
	GDKfree(batsT);
	return res;
}

static str
MATsort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int rev)
{
	bat *res_id = (bat*) getArgReference(stk,pci,0); /* result sorted */
	bat *map_id = (bat*) getArgReference(stk,pci,1); /* result map */
	BAT *res = NULL, *map = NULL;
	/* rest of the args are sorted parts, (excluding sorted and map) */
	BAT **bats = GDKzalloc(sizeof(BAT*) * pci->argc - 2);
	BUN pcnt = 0; 
	int i, len = pci->argc-2;

	(void) cntxt; (void) mb; (void) stk; 
	if( bats == NULL)
		throw(SQL, "mat.sortTail",MAL_MALLOC_FAIL);
	for (i=2; i<pci->argc; i++) {
		bat id = *(bat*) getArgReference(stk,pci,i);
		bats[i-2] = BATdescriptor(id);
		if (!bats[i-2])
			goto error;
		pcnt += BATcount(bats[i-2]);
	}

	if (ATOMstorage(bats[0]->ttype) <= TYPE_void) {
		/*error*/
	} else if (ATOMvarsized(bats[0]->ttype)) {
		res = MATsort_any(&map, bats, len, pcnt, rev);
	} else if (ATOMsize(bats[0]->ttype) == sizeof(bte)) {
		res = MATsort_bte(&map, bats, len, pcnt, rev);
	} else if (ATOMsize(bats[0]->ttype) == sizeof(sht)) {
		res = MATsort_sht(&map, bats, len, pcnt, rev);
	} else if (ATOMsize(bats[0]->ttype) == sizeof(int)) {
		res = MATsort_int(&map, bats, len, pcnt, rev);
	} else if (ATOMsize(bats[0]->ttype) == sizeof(lng)) {
		res = MATsort_lng(&map, bats, len, pcnt, rev);
	} else {
		res = MATsort_any(&map, bats, len, pcnt, rev);
	}
	if (res) {
		res->T->nonil = MATnonil(bats, len);
		if (rev) {
			res->trevsorted = 1;
			res->tsorted = res->batCount <= 1;
		} else {
			res->tsorted = 1;
			res->trevsorted = res->batCount <= 1;
		}
	}
error:
	for (i=0; i<len && bats[i]; i++)
		BBPunfix(bats[i]->batCacheid);
	GDKfree(bats);
	if (map && res) {
		map->tsorted = 0;
		map->trevsorted = 0;
		BBPkeepref( *map_id = map->batCacheid);
		BBPkeepref( *res_id = res->batCacheid);
		return MAL_SUCCEED;
	}
	if (map) BBPunfix(map->batCacheid);
	throw(SQL, "mat.sortTail","Cannot access descriptor");
}

str
MATsortTail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return MATsort( cntxt, mb, stk, pci, 0);
}

str
MATsortReverseTail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return MATsort( cntxt, mb, stk, pci, 1);
}

