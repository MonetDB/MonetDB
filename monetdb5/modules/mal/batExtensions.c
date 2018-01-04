/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "batExtensions.h"

/*
 * BAT enhancements
 * The code to enhance the kernel.
 */

str
CMDBATnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p){
	int tt, kind = TRANSIENT;
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

str
CMDBATsingle(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	int * ret= getArgReference_bat(stk,pci,0);
	void *u =(void*) getArgReference(stk,pci,1);

	(void)cntxt;

	b = COLnew(0,getArgType(mb,pci,1),0, TRANSIENT);
	if( b == 0)
		throw(MAL,"bat.single", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	if (ATOMextern(b->ttype))
		u = (ptr) *(str *)u;
	if (BUNappend(b, u, FALSE) != GDK_SUCCEED) {
		BBPreclaim(b);
		throw(MAL, "bat.single", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BBPkeepref(*ret = b->batCacheid);
	return MAL_SUCCEED;
}

/* If the optimizer has not determined the partition bounds we derive one here.  */
str
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
			throw(MAL, "bat.partition", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		BAThseqbase(bn, lval);
		stk->stk[getArg(pci,i)].val.bval = bn->batCacheid;
		ret= getArgReference_bat(stk,pci,i);
		BBPkeepref(*ret = bn->batCacheid);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}
str
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

str
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
str
CMDBATimprintsize(lng *ret, bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) 
		throw(MAL, "bat.imprints", INTERNAL_BAT_ACCESS);

	*ret = IMPSimprintsize(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}
