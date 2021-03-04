/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * M.L.Kersten
 * BAT Algebra Extensions
 * The kernel libraries are unaware of the MAL runtime semantics.
 * This calls for declaring some operations in the MAL module section
 * and register them in the kernel modules explicitly.
 *
 * A good example of this borderline case are BAT creation operations,
 * which require a mapping of the type identifier to the underlying
 * implementation type.
 *
 * Another example concerns the (un)pack operations, which direct
 * access the runtime stack to (push)pull the values needed.
 */
#include "monetdb_config.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "bat5.h"
#include "gdk_time.h"

/*
 * BAT enhancements
 * The code to enhance the kernel.
 */

static str
CMDBATnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p){
	int tt;
	role_t kind = TRANSIENT;
	BUN cap = 0;
	bat *res;

	(void) cntxt;
	res = getArgReference_bat(s, p, 0);
	tt = getArgType(m, p, 1);
	if (p->argc > 2) {
		lng lcap;

		if (getArgType(m, p, 2) == TYPE_lng)
			lcap = *getArgReference_lng(s, p, 2);
		else if (getArgType(m, p, 2) == TYPE_int)
			lcap = (lng) *getArgReference_int(s, p, 2);
		else
			throw(MAL, "bat.new", ILLEGAL_ARGUMENT " Incorrect type for size");
		if (lcap < 0)
			throw(MAL, "bat.new", POSITIVE_EXPECTED);
		if (lcap > (lng) BUN_MAX)
			throw(MAL, "bat.new", ILLEGAL_ARGUMENT " Capacity too large");
		cap = (BUN) lcap;
		if( p->argc == 4 && getVarConstant(m,getArg(p,3)).val.ival)
			kind = PERSISTENT;
	}

	if (tt == TYPE_any || isaBatType(tt))
		throw(MAL, "bat.new", SEMANTIC_TYPE_ERROR);
	return (str) BKCnewBAT(res,  &tt, &cap, kind);
}

static str
CMDBATsingle(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	int * ret= getArgReference_bat(stk,pci,0);
	void *u =(void*) getArgReference(stk,pci,1);

	(void)cntxt;

	b = COLnew(0,getArgType(mb,pci,1),0, TRANSIENT);
	if( b == 0)
		throw(MAL,"bat.single", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (ATOMextern(b->ttype))
		u = (ptr) *(str *)u;
	if (BUNappend(b, u, false) != GDK_SUCCEED) {
		BBPreclaim(b);
		throw(MAL, "bat.single", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BBPkeepref(*ret = b->batCacheid);
	return MAL_SUCCEED;
}

/* If the optimizer has not determined the partition bounds we derive one here.  */
static str
CMDBATpartition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b,*bn;
	bat *ret;
	int i;
	bat bid;
	oid lval,hval=0, step;

	(void) mb;
	(void) cntxt;
	bid = *getArgReference_bat(stk, pci, pci->retc);

	if ((b = BATdescriptor(bid)) == NULL) {
		throw(MAL, "bat.partition", INTERNAL_BAT_ACCESS);
	}
	step = BATcount(b) / pci->retc + 1;

	/* create the slices slightly overshoot to make sure it all is taken*/
	for(i=0; i<pci->retc; i++){
		lval = i*step;
		hval = lval + step;
		if (i == pci->retc-1)
			hval = BATcount(b);
		bn =  BATslice(b, lval,hval);
		if (bn== NULL){
			BBPunfix(b->batCacheid);
			throw(MAL, "bat.partition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		BAThseqbase(bn, lval);
		stk->stk[getArg(pci,i)].val.bval = bn->batCacheid;
		ret= getArgReference_bat(stk,pci,i);
		BBPkeepref(*ret = bn->batCacheid);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDBATpartition2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b,*bn;
	bat *ret,bid;
	int pieces= *getArgReference_int(stk, pci, 2);
	int idx = *getArgReference_int(stk, pci, 3);
	oid lval,hval=0, step;

	(void) mb;
	(void) cntxt;
	if ( pieces <=0 )
		throw(MAL, "bat.partition", POSITIVE_EXPECTED);
	if ( idx >= pieces || idx <0 )
		throw(MAL, "bat.partition", ILLEGAL_ARGUMENT " Illegal piece index");

	bid = *getArgReference_bat(stk, pci, pci->retc);

	if ((b = BATdescriptor(bid)) == NULL) {
		throw(MAL, "bat.partition", INTERNAL_BAT_ACCESS);
	}
	step = BATcount(b) / pieces;

	lval = idx * step;
	if ( idx == pieces-1)
		hval = BATcount(b);
	else
		hval = lval+step;
	bn =  BATslice(b, lval,hval);
	BAThseqbase(bn, lval + b->hseqbase);
	BBPunfix(b->batCacheid);
	if (bn== NULL){
		throw(MAL, "bat.partition",  INTERNAL_OBJ_CREATE);
	}
	ret= getArgReference_bat(stk,pci,0);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDBATimprints(void *ret, bat *bid)
{
	BAT *b;
	gdk_return r;

	(void) ret;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.imprints", INTERNAL_BAT_ACCESS);

	r = BATimprints(b);
	BBPunfix(b->batCacheid);
	if (r != GDK_SUCCEED)
		throw(MAL, "bat.imprints", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
CMDBATimprintsize(lng *ret, bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.imprints", INTERNAL_BAT_ACCESS);

	*ret = IMPSimprintsize(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

#define append_bulk_imp_fixed_size(TPE, UNION_VAL) \
	do { \
		ValRecord *stack = stk->stk; \
		int *argv = pci->argv; \
		total = number_existing + inputs; \
		if (BATextend(b, total) != GDK_SUCCEED) { \
			BBPunfix(b->batCacheid); \
			throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
		} \
		if (!b->tsorted && !b->trevsorted) { \
			for (int i = 3, args = pci->argc; i < args; i++) { \
				TPE next = stack[argv[i]].val.UNION_VAL; \
				new_nil |= is_##TPE##_nil(next); \
				if (BUNappend(b, &next, force) != GDK_SUCCEED) { \
					BBPunfix(b->batCacheid); \
					throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
				} \
			} \
		} else { \
			bool sorted = b->tsorted, revsorted = b->trevsorted; \
			TPE prev = stack[argv[3]].val.UNION_VAL; \
			new_nil |= is_##TPE##_nil(prev); \
			if (number_existing) { \
				TPE last = *(TPE*) Tloc(b, number_existing - 1); \
				sorted &= prev >= last; \
				revsorted &= prev <= last; \
			} \
			if (BUNappend(b, &prev, force) != GDK_SUCCEED) { \
				BBPunfix(b->batCacheid); \
				throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			} \
			for (int i = 4, args = pci->argc; i < args; i++) { \
				TPE next = stack[argv[i]].val.UNION_VAL; \
				new_nil |= is_##TPE##_nil(next); \
				sorted &= next >= prev; \
				revsorted &= next <= prev; \
				if (BUNappend(b, &next, force) != GDK_SUCCEED) { \
					BBPunfix(b->batCacheid); \
					throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
				} \
			} \
			b->tsorted &= sorted; \
			b->trevsorted &= revsorted; \
		} \
	} while (0)

static str
CMDBATappend_bulk(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *r = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1);
	bit force = *getArgReference_bit(stk, pci, 2), new_nil = 0;
	BAT *b;
	BUN inputs = (BUN)(pci->argc - 3), number_existing = 0, total = 0;

	(void) cntxt;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (inputs > 0) {
		uint8_t storage = ATOMstorage(b->ttype);
		number_existing = BATcount(b);

		if (isaBatType(getArgType(mb, pci, 3))) { /* use BATappend for the bulk case */
			gdk_return rt;
			for (int i = 3, args = pci->argc; i < args; i++) {
				BAT *d = BATdescriptor(*getArgReference_bat(stk, pci, i));
				if (!d) {
					BBPunfix(b->batCacheid);
					throw(MAL, "bat.append_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
				}
				rt = BATappend(b, d, NULL, force);
				BBPunfix(d->batCacheid);
				if (rt != GDK_SUCCEED) {
					BBPunfix(b->batCacheid);
					throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		} else if (b->ttype < TYPE_str && storage == b->ttype) {
			switch (b->ttype) {
			case TYPE_bit:
			case TYPE_bte:
				append_bulk_imp_fixed_size(bte, btval);
				break;
			case TYPE_sht:
				append_bulk_imp_fixed_size(sht, shval);
				break;
			case TYPE_date:
			case TYPE_int:
				append_bulk_imp_fixed_size(int, ival);
				break;
			case TYPE_daytime:
			case TYPE_timestamp:
			case TYPE_lng:
				append_bulk_imp_fixed_size(lng, lval);
				break;
			case TYPE_oid:
				append_bulk_imp_fixed_size(oid, oval);
				break;
			case TYPE_flt:
				append_bulk_imp_fixed_size(flt, fval);
				break;
			case TYPE_dbl:
				append_bulk_imp_fixed_size(dbl, dval);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				append_bulk_imp_fixed_size(hge, hval);
				break;
#endif
			default:
				assert(0);
			}
			BATsetcount(b, total);
			if (number_existing == 0) {
				b->tnil = new_nil;
				b->tnonil = !new_nil;
			} else {
				b->tnil |= new_nil;
				b->tnonil &= ~new_nil;
			}
			b->tkey = BATcount(b) <= 1;
		} else { /* non fixed size, use the conventional way */
			total = number_existing + inputs;
			if (BATextend(b, total) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			for (int i = 3, args = pci->argc; i < args; i++) {
				ptr u = getArgReference(stk,pci,i);
				if (storage >= TYPE_str)
					u = (ptr) *(str *) u;
				if (BUNappend(b, u, force) != GDK_SUCCEED) {
					BBPunfix(b->batCacheid);
					throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		}
	}

	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * String imprints.
 */
static str
PATstrimp_ndigrams(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b;
	size_t n;

	(void)cntxt;
	(void)mb;

	// return mythrow(MAL, "batcalc.striter", OPERATION_FAILED);
	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "bat.strimpDigrams", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!GDKstrimp_ndigrams(b, &n)) {
		throw(MAL, "bat.strimpDigrams", SQLSTATE(HY002) OPERATION_FAILED);
	}

	*getArgReference_lng(stk, pci, 0) = n;

	return MAL_SUCCEED;
}

static str
PATstrimp_makehist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b, *ob;
	size_t i;
	uint64_t hist[STRIMP_HISTSIZE];
	size_t count;

	(void)cntxt;
	(void)mb;

	bid = *getArgReference_bat(stk, pci, 2);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "bat.strimpHistogram", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!GDKstrimp_make_histogram(b, hist, STRIMP_HISTSIZE, &count)) {
		throw(MAL, "bat.strimpHistogram", SQLSTATE(HY002) OPERATION_FAILED);
	}

	ob = COLnew(0, TYPE_lng, STRIMP_HISTSIZE, TRANSIENT);
	if (ob == NULL) {
		throw(MAL, "bat.strimpHistogram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i=0; i < STRIMP_HISTSIZE; i++) {
		if (BUNappend(ob, hist + i, false) != GDK_SUCCEED)
			throw(MAL, "bat.strimpHistogram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	*getArgReference_lng(stk, pci, 0) = count;
	*getArgReference_bat(stk, pci, 1) = ob->batCacheid;

	BBPkeepref(ob->batCacheid);
	return MAL_SUCCEED;
}

static str
PATstrimp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b;
	(void)cntxt;
	(void)mb;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "bat.strimpHeader", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if(GDKstrimp_create_strimp(b) != GDK_SUCCEED)
		throw(MAL, "bat.strimpHistogram", SQLSTATE(HY002) OPERATION_FAILED);

	// *getArgReference_lng(stk, pci, 0) = 0;
	return MAL_SUCCEED;
}


#include "mel.h"
mel_func batExtensions_init_funcs[] = {
 pattern("bat", "new", CMDBATnew, false, "", args(1,2, batargany("",1),argany("tt",1))),
 pattern("bat", "new", CMDBATnew, false, "", args(1,3, batargany("",1),argany("tt",1),arg("size",int))),
 pattern("bat", "new", CMDBATnew, false, "", args(1,4, batargany("",1),argany("tt",1),arg("size",lng),arg("persist",bit))),
 pattern("bat", "new", CMDBATnew, false, "", args(1,4, batargany("",1),argany("tt",1),arg("size",int),arg("persist",bit))),
 pattern("bat", "new", CMDBATnew, false, "Creates a new empty transient BAT, with tail-types as indicated.", args(1,3, batargany("",1),argany("tt",1),arg("size",lng))),
 pattern("bat", "single", CMDBATsingle, false, "Create a BAT with a single elemenet", args(1,2, batargany("",1),argany("val",1))),
 pattern("bat", "partition", CMDBATpartition, false, "Create a serie of slices over the BAT argument. The BUNs are distributed evenly.", args(1,2, batvarargany("",1),batargany("b",1))),
 pattern("bat", "partition", CMDBATpartition2, false, "Create the n-th slice over the BAT broken into several pieces.", args(1,4, batargany("",1),batargany("b",1),arg("pieces",int),arg("n",int))),
 command("bat", "imprints", CMDBATimprints, false, "", args(1,2, arg("",void),batarg("b",bte))),
 command("bat", "imprints", CMDBATimprints, false, "", args(1,2, arg("",void),batarg("b",sht))),
 command("bat", "imprints", CMDBATimprints, false, "", args(1,2, arg("",void),batarg("b",int))),
 command("bat", "imprints", CMDBATimprints, false, "", args(1,2, arg("",void),batarg("b",lng))),
 command("bat", "imprints", CMDBATimprints, false, "", args(1,2, arg("",void),batarg("b",flt))),
 command("bat", "imprints", CMDBATimprints, false, "Check for existence or create an imprint index on the BAT.", args(1,2, arg("",void),batarg("b",dbl))),
 command("bat", "imprintsize", CMDBATimprintsize, false, "", args(1,2, arg("",lng),batarg("b",bte))),
 command("bat", "imprintsize", CMDBATimprintsize, false, "", args(1,2, arg("",lng),batarg("b",sht))),
 command("bat", "imprintsize", CMDBATimprintsize, false, "", args(1,2, arg("",lng),batarg("b",int))),
 command("bat", "imprintsize", CMDBATimprintsize, false, "", args(1,2, arg("",lng),batarg("b",lng))),
 command("bat", "imprintsize", CMDBATimprintsize, false, "", args(1,2, arg("",lng),batarg("b",flt))),
 command("bat", "imprintsize", CMDBATimprintsize, false, "Return the storage size of the imprints index structure.", args(1,2, arg("",lng),batarg("b",dbl))),
#ifdef HAVE_HGE
 command("bat", "imprints", CMDBATimprints, false, "", args(0,1, batarg("b",hge))),
 command("bat", "imprintsize", CMDBATimprintsize, false, "", args(1,2, arg("",lng),batarg("b",hge))),
#endif
 pattern("bat", "appendBulk", CMDBATappend_bulk, false, "append the arguments ins to i", args(1,4, batargany("",1), batargany("i",1),arg("force",bit),varargany("ins",1))),
 pattern("bat", "appendBulk", CMDBATappend_bulk, false, "append the arguments ins to i", args(1,4, batargany("",1), batargany("i",1),arg("force",bit),batvarargany("ins",1))),

 /* String imprints */
 pattern("bat", "strimpNDigrams", PATstrimp_ndigrams, false, "count digrams in a string bat", args(1,2,arg("",lng),batarg("b",str))),
 pattern("bat", "strimpHistogram", PATstrimp_makehist, false, "make a histogram of all the byte pairs in a BAT", args(2,3,arg("",lng), batarg("",lng),batarg("b",str))),
 pattern("bat", "strimp", PATstrimp, false, "construct the strimp a BAT", args(1,2,arg("",void),batarg("b",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batExtensions_mal)
{ mal_module("batExtensions", NULL, batExtensions_init_funcs); }
