/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal_resolve.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

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
	int rt = getArgType(mb, p, 0), unmask = 0;
	(void) cntxt;

	for (i = 1; i < p->argc; i++) {
		bat bid = stk->stk[getArg(p,i)].val.bval;
		b = BBPquickdesc(bid, false);
		if( b ){
			if (tt == TYPE_any)
				tt = b->ttype;
			if ((tt != TYPE_void && b->ttype != TYPE_void && b->ttype != TYPE_msk) && tt != b->ttype)
				throw(MAL, "mat.pack", "incompatible arguments");
			cap += BATcount(b);
		}
	}
	if (tt == TYPE_any){
		*ret = bat_nil;
		return MAL_SUCCEED;
	}

	if (tt == TYPE_msk && rt == newBatType(TYPE_oid)) {
		tt = TYPE_oid;
		unmask = 1;
	}
	bn = COLnew(0, tt, cap, TRANSIENT);
	if (bn == NULL)
		throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 1; i < p->argc; i++) {
		BAT *ob = b = BATdescriptor(stk->stk[getArg(p,i)].val.ival);
		if ((unmask && b && b->ttype == TYPE_msk) || mask_cand(b))
			b = BATunmask(b);
		if( b ){
			if (BATcount(bn) == 0) {
				BAThseqbase(bn, b->hseqbase);
				BATtseqbase(bn, b->tseqbase);
			}
			if (BATappend(bn, b, NULL, false) != GDK_SUCCEED) {
				BBPunfix(bn->batCacheid);
				BBPunfix(b->batCacheid);
				throw(MAL, "mat.pack", GDK_EXCEPTION);
			}
			BBPunfix(b->batCacheid);
		}
		if (b != ob)
			BBPunfix(ob->batCacheid);
	}
	if( !(!bn->tnil || !bn->tnonil)){
		BBPkeepref(*ret = bn->batCacheid);
		throw(MAL, "mat.pack", "INTERNAL ERROR" "bn->tnil or  bn->tnonil fails ");
	}
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Enable incremental packing. The SQL front-end requires
 * fixed oid sequences.
 */
static str
MATpackIncrement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	bat *ret = getArgReference_bat(stk,p,0);
	int	pieces;
	BAT *b, *bb, *bn;
	size_t newsize;

	(void) cntxt;
	b = BATdescriptor( stk->stk[getArg(p,1)].val.ival);
	if ( b == NULL)
		throw(MAL, "mat.pack", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if ( getArgType(mb,p,2) == TYPE_int){
		BAT *ob = b;
		/* first step, estimate with some slack */
		pieces = stk->stk[getArg(p,2)].val.ival;
		int tt = ATOMtype(b->ttype);
		if (b->ttype == TYPE_msk)
			tt = TYPE_oid;
		bn = COLnew(b->hseqbase, tt, (BUN)(1.2 * BATcount(b) * pieces), TRANSIENT);
		if (bn == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		/* allocate enough space for the vheap, but not for strings,
		 * since BATappend does clever things for strings */
		if ( b->tvheap && bn->tvheap && ATOMstorage(b->ttype) != TYPE_str){
			newsize =  b->tvheap->size * pieces;
			if (HEAPextend(bn->tvheap, newsize, true) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				BBPunfix(bn->batCacheid);
				throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		BATtseqbase(bn, b->tseqbase);
		if (b->ttype == TYPE_msk || mask_cand(b))
			b = BATunmask(b);
		if (b && BATappend(bn, b, NULL, false) != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			if (b != ob)
				BBPunfix(ob->batCacheid);
			BBPunfix(b->batCacheid);
			throw(MAL, "mat.pack", GDK_EXCEPTION);
		}
		bn->unused = (pieces-1); /* misuse "unused" field */
		*ret = bn->batCacheid;
		BATsettrivprop(bn);
		BBPretain(bn->batCacheid);
		BBPunfix(bn->batCacheid);
		if (b != ob)
			BBPunfix(ob->batCacheid);
		if (b)
			BBPunfix(b->batCacheid);
		if( !(!bn->tnil || !bn->tnonil))
			throw(MAL, "mat.packIncrement", "INTERNAL ERROR" " bn->tnil %d bn->tnonil %d", bn->tnil, bn->tnonil);
	} else {
		/* remaining steps */
		BAT *obb = bb = BATdescriptor(stk->stk[getArg(p,2)].val.ival);
		if (bb && (bb->ttype == TYPE_msk || mask_cand(bb)))
			bb = BATunmask(bb);
		if ( bb ){
			if (BATcount(b) == 0) {
				BAThseqbase(b, bb->hseqbase);
				BATtseqbase(b, bb->tseqbase);
			}
			if (BATappend(b, bb, NULL, false) != GDK_SUCCEED) {
				BBPunfix(bb->batCacheid);
				BBPunfix(b->batCacheid);
				throw(MAL, "mat.pack", GDK_EXCEPTION);
			}
			BBPunfix(bb->batCacheid);
		}
		if (bb != obb)
			BBPunfix(obb->batCacheid);
		b->unused--;
		if(b->unused == 0)
			if (BATsetaccess(b, BAT_READ) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				throw(MAL, "mat.pack", GDK_EXCEPTION);
			}
		if( !(!b->tnil || !b->tnonil)){
			BBPkeepref(*ret = b->batCacheid);
			throw(MAL, "mat.pack", "INTERNAL ERROR" " b->tnil or  b->tnonil fails ");
		}
		*ret = b->batCacheid;
		BATsettrivprop(b);
		BBPretain(b->batCacheid);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}

static str
MATpack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	return MATpackInternal(cntxt,mb,stk,p);
}

static str
MATpackValues(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, type, first = 1;
	bat *ret;
	BAT *bn;

	(void) cntxt;
	type = getArgType(mb,p,first);
	bn = COLnew(0, type, p->argc, TRANSIENT);
	if( bn == NULL)
		throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (ATOMextern(type)) {
		for(i = first; i < p->argc; i++)
			if (BUNappend(bn, stk->stk[getArg(p,i)].val.pval, false) != GDK_SUCCEED)
				goto bailout;
	} else {
		for(i = first; i < p->argc; i++)
			if (BUNappend(bn, getArgReference(stk, p, i), false) != GDK_SUCCEED)
				goto bailout;
	}
	ret= getArgReference_bat(stk,p,0);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
  bailout:
	BBPreclaim(bn);
	throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

#include "mel.h"
mel_func mat_init_funcs[] = {
 pattern("mat", "new", MATpack, false, "Define a Merge Association Table (MAT). Fall back to the pack operation\nwhen this is called ", args(1,2, batargany("",2),batvarargany("b",2))),
 pattern("bat", "pack", MATpackValues, false, "Materialize the values into a BAT. Avoiding a clash with mat.pack() in mergetable", args(1,2, batargany("",2),varargany("",2))),
 pattern("mat", "pack", MATpackValues, false, "Materialize the MAT (of values) into a BAT", args(1,2, batargany("",2),varargany("",2))),
 pattern("mat", "pack", MATpack, false, "Materialize the MAT into a BAT", args(1,2, batargany("",2),batvarargany("b",2))),
 pattern("mat", "packIncrement", MATpackIncrement, false, "Prepare incremental mat pack", args(1,3, batargany("",2),batargany("b",2),arg("pieces",int))),
 pattern("mat", "packIncrement", MATpackIncrement, false, "Prepare incremental mat pack", args(1,3, batargany("",2),batargany("b",2),batargany("c",2))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mat_mal)
{ mal_module("mat", NULL, mat_init_funcs); }
