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
#include "mal_instruction.h"
#include "mal_exception.h"

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
CMDBATdup(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b, *i;
	bat *ret= getArgReference_bat(stk, pci, 0);
	int tt = getArgType(mb, pci, 1);
	bat input = *getArgReference_bat(stk, pci, 2);

	(void)cntxt;
	if ((i = BATdescriptor(input)) == NULL)
		throw(MAL, "bat.new", INTERNAL_BAT_ACCESS);
	b = COLnew(i->hseqbase, tt, BATcount(i), TRANSIENT);
	BBPunfix(i->batCacheid);
	if (b == 0)
		throw(MAL,"bat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*ret = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
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

static str
CMDBATappend_bulk(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *r = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1);
	bit force = *getArgReference_bit(stk, pci, 2);
	BAT *b;
	BUN inputs = (BUN)(pci->argc - 3), number_existing = 0, total = 0;

	(void) cntxt;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (inputs > 0) {
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
		} else {
			bool external = ATOMextern(b->ttype);
			total = number_existing + inputs;
			if (BATextend(b, total) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			for (int i = 3, args = pci->argc; i < args; i++) {
				ptr u = getArgReference(stk,pci,i);
				if (external)
					u = (ptr) *(ptr *) u;
				if (BUNappend(b, u, force) != GDK_SUCCEED) {
					BBPunfix(b->batCacheid);
					throw(MAL,"bat.append_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		}
	}

	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

#if 0
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

	if (!STRMPndigrams(b, &n)) {
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

	if (!STRMPmakehistogram(b, hist, STRIMP_HISTSIZE, &count)) {
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
#endif
static str
PATstrimpCreate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b;
	(void)cntxt;
	(void)mb;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "bat.strimpHeader", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if(STRMPcreate(b) != GDK_SUCCEED)
		throw(MAL, "bat.strimpHistogram", SQLSTATE(HY002) OPERATION_FAILED);

	// *getArgReference_lng(stk, pci, 0) = 0;
	return MAL_SUCCEED;
}

static str
PATstrimpFilter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;
	throw(MAL, "bat.strimpfilter", SQLSTATE(HY002) "UNIMPLEMENTED");
}

static str
PATstrimpFilterSelect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid, sid;
	BAT *b, *s, *ob;
	str pat;

	(void)cntxt;
	(void)mb;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "bat.strimpfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	sid = *getArgReference_bat(stk, pci, 2);
	if ((s = BATdescriptor(sid)) == NULL)
		throw(MAL, "bat.strimpfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	assert(s->ttype == TYPE_void);

	if (!STRMPcreate(b)) {
		throw(MAL, "bat.strimpfilter", SQLSTATE(HY002) OPERATION_FAILED);
	}

	pat = *getArgReference_str(stk, pci, 3);
	if ((ob = STRMPfilter(b, pat)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.strimpfilter", SQLSTATE(HY002));
	}

	*getArgReference_bat(stk, pci, 0) = ob->batCacheid;
	BBPkeepref(ob->batCacheid);

	return MAL_SUCCEED;
}

#include "mel.h"
mel_func batExtensions_init_funcs[] = {
 pattern("bat", "new", CMDBATnew, false, "", args(1,2, batargany("",1),argany("tt",1))),
 pattern("bat", "new", CMDBATnew, false, "", args(1,3, batargany("",1),argany("tt",1),arg("size",int))),
 pattern("bat", "new", CMDBATnew, false, "", args(1,4, batargany("",1),argany("tt",1),arg("size",lng),arg("persist",bit))),
 pattern("bat", "new", CMDBATnew, false, "", args(1,4, batargany("",1),argany("tt",1),arg("size",int),arg("persist",bit))),
 pattern("bat", "new", CMDBATnew, false, "Creates a new empty transient BAT, with tail-types as indicated.", args(1,3, batargany("",1),argany("tt",1),arg("size",lng))),
 pattern("bat", "new", CMDBATdup, false, "Creates a new empty transient BAT, with tail-type tt and hseqbase and size from the input bat argument.", args(1,3, batargany("",1), argany("tt",1),batargany("i",2))),
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
 // pattern("bat", "strimpNDigrams", PATstrimp_ndigrams, false, "count digrams in a string bat", args(1,2,arg("",lng),batarg("b",str))),
 // pattern("bat", "strimpHistogram", PATstrimp_makehist, false, "make a histogram of all the byte pairs in a BAT", args(2,3,arg("",lng), batarg("",lng),batarg("b",str))),
 pattern("bat", "mkstrimp", PATstrimpCreate, false, "construct the strimp a BAT", args(1,2,arg("",void),batarg("b",str))),
 pattern("bat", "strimpfilter", PATstrimpFilter, false, "", args(1,3,arg("",bit),arg("b",str),arg("q",str))),
 pattern("bat", "strimpfilterselect", PATstrimpFilterSelect, false, "", args(1,5,batarg("",oid),batarg("b",str),batarg("s",oid),arg("q",str),arg("a",bit))),
 pattern("bat", "strimpfilterjoin", PATstrimpFilter, false, "", args(2,8,batarg("",oid),batarg("b",str),arg("q",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batExtensions_mal)
{ mal_module("batExtensions", NULL, batExtensions_init_funcs); }
