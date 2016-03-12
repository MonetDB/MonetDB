/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
 * @+ BAT enhancements
 * The code to enhance the kernel.
 */
str
CMDBATnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	int ht, tt;
	BUN cap = 0;
	bat *res;

	(void) cntxt;
	res = getArgReference_bat(s, p, 0);
	ht = getArgType(m, p, 1);
	tt = getArgType(m, p, 2);
	if (p->argc > 3) {
		lng lcap;

		if (getArgType(m, p, 3) == TYPE_lng)
			lcap = *getArgReference_lng(s, p, 3);
		else if (getArgType(m, p, 3) == TYPE_int)
			lcap = (lng) *getArgReference_int(s, p, 3);
		else if (getArgType(m, p, 3) == TYPE_wrd)
			lcap = (lng) *getArgReference_wrd(s, p, 3);
		else
			throw(MAL, "bat.new", ILLEGAL_ARGUMENT " Incorrect type for size");
		if (lcap < 0)
			throw(MAL, "bat.new", POSITIVE_EXPECTED);
		if (lcap > (lng) BUN_MAX)
			throw(MAL, "bat.new", ILLEGAL_ARGUMENT " Capacity too large");
		cap = (BUN) lcap;
	}

	if (ht == TYPE_any || tt == TYPE_any || isaBatType(ht) || isaBatType(tt))
		throw(MAL, "bat.new", SEMANTIC_TYPE_ERROR);
	return (str) BKCnewBAT(res, &ht, &tt, &cap, TRANSIENT);
}

str
CMDBATnew_persistent(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	int ht, tt;
	BUN cap = 0;
	bat *res;

	(void) cntxt;
	res = getArgReference_bat(s, p, 0);
	ht = getArgType(m, p, 1);
	tt = getArgType(m, p, 2);
	if (p->argc > 3) {
		lng lcap;

		if (getArgType(m, p, 3) == TYPE_lng)
			lcap = *getArgReference_lng(s, p, 3);
		else if (getArgType(m, p, 3) == TYPE_int)
			lcap = (lng) *getArgReference_int(s, p, 3);
		else if (getArgType(m, p, 3) == TYPE_wrd)
			lcap = (lng) *getArgReference_wrd(s, p, 3);
		else
			throw(MAL, "bat.new", ILLEGAL_ARGUMENT " Incorrect type for size");
		if (lcap < 0)
			throw(MAL, "bat.new", POSITIVE_EXPECTED);
		if (lcap > (lng) BUN_MAX)
			throw(MAL, "bat.new", ILLEGAL_ARGUMENT " Capacity too large");
		cap = (BUN) lcap;
	}

	if (ht == TYPE_any || tt == TYPE_any || isaBatType(ht) || isaBatType(tt))
		throw(MAL, "bat.new", SEMANTIC_TYPE_ERROR);
	return (str) BKCnewBAT(res, &ht, &tt, &cap, PERSISTENT);
}

str
CMDBATnewDerived(Client cntxt, MalBlkPtr mb, MalStkPtr s, InstrPtr p)
{
	bat bid;
	int ht, tt;
	BUN cap = 0;
	int *res;
	BAT *b;
	str msg;
	oid o;

	(void) mb;
	(void) cntxt;
	bid = *getArgReference_bat(s, p, 1);
	if ((b = BATdescriptor(bid)) == NULL) {
		throw(MAL, "bat.new", INTERNAL_BAT_ACCESS);
	}

	if (bid > 0) {
		ht = b->htype;
		tt = b->ttype;
	} else {
		tt = b->htype;
		ht = b->ttype;
	}

	if (p->argc > 2) {
		lng lcap = *getArgReference_lng(s, p, 2);
		if (lcap < 0)
			throw(MAL, "bat.new", POSITIVE_EXPECTED);
		if (lcap > (lng) BUN_MAX)
			throw(MAL, "bat.new", ILLEGAL_ARGUMENT " Capacity too large");
		cap = (BUN) lcap;
	}
	else
		cap = BATcount(b);
	o = b->hseqbase;
	BBPunfix(b->batCacheid);

	res = getArgReference_int(s, p, 0);
	msg = (str) BKCnewBAT(res, &ht, &tt, &cap, TRANSIENT);
	if (msg == MAL_SUCCEED && ht == TYPE_void) {
		b = BATdescriptor(*res);
		if ( b == NULL )
			throw(MAL, "bat.new", RUNTIME_OBJECT_MISSING);
		BATseqbase(b, o);
		BBPunfix(b->batCacheid);
	}

	return msg;
}

str
CMDBATderivedByName(bat *ret, str *nme)
{
	BAT *bn;
	int bid;

	bid = BBPindex(*nme);
	if (bid <= 0 || (bn = BATdescriptor(bid)) == 0)
		throw(MAL, "bat.new", INTERNAL_BAT_ACCESS);
	BBPincref(*ret = bn->batCacheid, TRUE);
	BBPunfix(bid);
	return MAL_SUCCEED;
}
str
CMDBATsingle(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	int * ret= getArgReference_bat(stk,pci,0);
	void *u =(void*) getArgReference(stk,pci,1);

	(void)cntxt;

	b = BATnew(TYPE_oid,getArgType(mb,pci,1),0, TRANSIENT);
	if( b == 0)
		throw(MAL,"bat.single","Could not create it");
	if (ATOMextern(b->ttype))
            	u = (ptr) *(str *)u;
	BUNappend(b, u, FALSE);
	BBPincref(*ret = b->batCacheid, TRUE);
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
	VarPtr low, hgh;
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
		low= varGetProp(mb, getArg(pci,i),PropertyIndex("hlb") );
		if (low== NULL )
			lval = i*step;
		else
			lval = low->value.val.oval;
		hgh= varGetProp(mb, getArg(pci,i),PropertyIndex("hub") );
		if (hgh== NULL )
			hval = lval + step;
		else
			hval = hgh->value.val.oval;
		if (i == pci->retc-1)
			hval = BATcount(b);
		bn =  BATslice(b, lval,hval);
		if (bn== NULL){
			BBPunfix(b->batCacheid);
			throw(MAL, "bat.partition", MAL_MALLOC_FAIL);
		}
		BATseqbase(bn, lval);
		stk->stk[getArg(pci,i)].val.bval = bn->batCacheid;
		ret= getArgReference_bat(stk,pci,i);
		BBPkeepref(*ret = bn->batCacheid);
		low= hgh;
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
	BATseqbase(bn, lval + b->hseqbase) ;
	if (bn== NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.partition",  INTERNAL_OBJ_CREATE);
	}
	ret= getArgReference_bat(stk,pci,0);
	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
CMDBATimprints(void *ret, bat *bid)
{
	BAT *b;

	(void) ret;
	if ((b = BATdescriptor(*bid)) == NULL) 
		throw(MAL, "bat.imprints", INTERNAL_BAT_ACCESS);

	BATimprints(b);
	BBPunfix(b->batCacheid);
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
