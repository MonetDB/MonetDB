/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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
	int i;
	bat *ret = getArgReference_bat(stk,p,0);
	BAT *b, *bn;
	BUN cap = 0;
	int tt = TYPE_any;
	(void) cntxt;
	(void) mb;

	for (i = 1; i < p->argc; i++) {
		bat bid = stk->stk[getArg(p,i)].val.bval;
		b = BBPquickdesc(abs(bid),FALSE);
		if( b ){
			if (tt == TYPE_any)
				tt = b->ttype;
			if (tt != b->ttype)
				throw(MAL, "mat.pack", "incompatible arguments");
			cap += BATcount(b);
		}
	}
	if (tt == TYPE_any){
		*ret = bat_nil;
		return MAL_SUCCEED;
	}

	bn = BATnew(TYPE_void, tt, cap, TRANSIENT);
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
	bat *ret = getArgReference_bat(stk,p,0);
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
		bn = BATnew(TYPE_void, b->ttype?b->ttype:TYPE_oid, (BUN)(1.2 * BATcount(b) * pieces), TRANSIENT);
		if (bn == NULL)
			throw(MAL, "mat.pack", MAL_MALLOC_FAIL);
		/* allocate enough space for the vheap, but not for strings,
		 * since BATappend does clever things for strings */
		if ( b->T->vheap && bn->T->vheap && ATOMstorage(b->ttype) != TYPE_str){
			newsize =  b->T->vheap->size * pieces;
			if (HEAPextend(bn->T->vheap, newsize, TRUE) != GDK_SUCCEED)
				throw(MAL, "mat.pack", MAL_MALLOC_FAIL);
		}
		BATseqbase(bn, b->H->seq);
		BATseqbase(BATmirror(bn), b->T->seq);
		BATappend(bn,b,FALSE);
		assert(!bn->H->nil || !bn->H->nonil);
		assert(!bn->T->nil || !bn->T->nonil);
		bn->H->align = (pieces-1);
		BBPkeepref(*ret = bn->batCacheid);
		BBPunfix(b->batCacheid);
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
		b->H->align--;
		if(b->H->align == 0)
			BATsetaccess(b, BAT_READ);
		assert(!b->H->nil || !b->H->nonil);
		assert(!b->T->nil || !b->T->nonil);
		BBPkeepref(*ret = b->batCacheid);
		if( bb) 
			BBPunfix(bb->batCacheid);
	}
	return MAL_SUCCEED;
}

str
MATpack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	return MATpackInternal(cntxt,mb,stk,p);
}

str
MATpackValues(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, type, first = 1;
	bat *ret;
	BAT *bn;

	(void) cntxt;
	type = getArgType(mb,p,first);
	bn = BATnew(TYPE_void, type, p->argc, TRANSIENT);
	if( bn == NULL)
		throw(MAL, "mat.pack", MAL_MALLOC_FAIL);

	if (ATOMvarsized(type)) {
		for(i = first; i < p->argc; i++)
			BUNappend(bn, stk->stk[getArg(p,i)].val.sval, TRUE);
	} else {
		for(i = first; i < p->argc; i++)
			BUNappend(bn, getArgReference(stk, p, i), TRUE);
	}
	BATseqbase(bn, 0);
	ret= getArgReference_bat(stk,p,0);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}
