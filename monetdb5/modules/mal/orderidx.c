/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * (c) Martin Kersten, Lefteris Sidirourgos
 * Implement a parallel sort-merge MAL program generator
 */
#include "monetdb_config.h"
#include "orderidx.h"
#include "gdk.h"

#define MIN_PIECE 2	/* TODO use realistic size in production */

str
OIDXdropImplementation(Client cntxt, BAT *b)
{
	str msg = MAL_SUCCEED;
	(void) cntxt;
	(void) b;
	if ( b->torderidx){
		// drop the order index heap
	}
	return msg;
}

str
OIDXcreateImplementation(Client cntxt, int tpe, BAT *b, int pieces)
{
	int i, loopvar, arg;
	BUN cnt, step=0,o;
	MalBlkPtr smb;
	MalStkPtr newstk;
	Symbol snew;
	InstrPtr q, pack;
	char name[IDLENGTH];
	str msg= MAL_SUCCEED;

	if( BATcount(b) == 0)
		return MAL_SUCCEED;

	/* check if b is sorted, then index does not make sense */
	if (b->tsorted || b->trevsorted)
		return MAL_SUCCEED;

	/* check if b already has index */
	if (b->torderidx)
		return MAL_SUCCEED;

	if( pieces < 0 ){
		/* TODO estimate number of pieces */
		pieces = 3; // should become GDKnr_threads
	}
	if ( BATcount(b) < MIN_PIECE || BATcount(b) <= (BUN) pieces)
		pieces = 1;
#ifdef _DEBUG_OIDX_
	mnstr_printf(cntxt->fdout,"#bat.orderidx pieces %d\n",pieces);
	mnstr_printf(cntxt->fdout,"#oidx ttype %d bat %d\n", b->ttype,tpe);
#endif

	/* create a temporary MAL function to sort the BAT in parallel */
	snprintf(name, IDLENGTH, "sort%d", rand()%1000);
	snew = newFunction(putName("user", 4), putName(name, strlen(name)),
	       FUNCTIONsymbol);
	smb = snew->def;
	q = getInstrPtr(smb, 0);
	arg = newTmpVariable(smb, tpe);
	pushArgument(smb, q, arg);
	getArg(q,0) = newTmpVariable(smb, TYPE_void);

	resizeMalBlk(smb, 2*pieces+10, 2*pieces+10); // large enough
	/* create the pack instruction first, as it will hold
	 * intermediate variables */
	pack = newInstruction(0, ASSIGNsymbol);
	setModuleId(pack, putName("bat", 3));
	setFunctionId(pack, putName("orderidx", 8));
	pack->argv[0] = newTmpVariable(smb, TYPE_void);
	pack = pushArgument(smb, pack, arg);
	setVarFixed(smb, getArg(pack, 0));

	/* the costly part executed as a parallel block */
	loopvar = newTmpVariable(smb, TYPE_bit);
	q = newStmt(smb, putName("language", 8), putName("dataflow", 8));
	q->barrier = BARRIERsymbol;
	q->argv[0] = loopvar;

	cnt = BATcount(b);
	step = cnt/pieces;
	o = 0;
	for (i = 0; i < pieces; i++) {
		/* add slice instruction */
		q = newStmt(smb, putName("algebra", 7),putName("slice", 5));
		setVarType(smb, getArg(q,0), tpe);
		setVarFixed(smb, getArg(q,0));
		q = pushArgument(smb, q, arg);
		pack = pushArgument(smb, pack, getArg(q,0));
		q = pushOid(smb, q, o);
		if (i == pieces-1) {
			o = cnt;
		} else {
			o += step;
		}
		q = pushOid(smb, q, o);
		o += 1;
	}
	for (i=0; i< pieces; i++) {
		/* add sort instruction */
		q = newStmt(smb, putName("algebra",7), putName("orderidx", 8));
		setVarType(smb, getArg(q, 0), newBatType(TYPE_oid, TYPE_oid));
		setVarFixed(smb, getArg(q, 0));
		q = pushArgument(smb, q, pack->argv[2+i]);
		q = pushBit(smb, q, 0);
		q = pushBit(smb, q, 1);
		pack->argv[2+i] = getArg(q, 0);
	}
	/* finalize OID packing, check, and evaluate */
	pushInstruction(smb,pack);
	q = newAssignment(smb);
	q->barrier = EXITsymbol;
	q->argv[0] = loopvar;
	pushEndInstruction(smb);
	chkProgram(cntxt->fdout, cntxt->nspace, smb);
	//printFunction(THRdata[0], smb, 0 , 23);
	if (smb->errors) {
		msg = createException(MAL, "bat.orderidx",
		                           "Type errors in generated code");
	} else {
		/* evaluate MAL block and keep the ordered OID bat */
		newstk = prepareMALstack(smb, smb->vsize);
		newstk->up = 0;
		newstk->stk[arg].vtype= TYPE_bat;
		newstk->stk[arg].val.bval= b->batCacheid;
		BBPincref(newstk->stk[arg].val.bval, TRUE);
		msg = runMALsequence(cntxt, smb, 1, 0, newstk, 0, 0);
		freeStack(newstk);
	}
#ifdef _DEBUG_OIDX_
	printFunction(cntxt->fdout, smb, 0, LIST_MAL_ALL);
#endif
	/* get rid of temporary MAL block */
	freeSymbol(snew);
	return msg;
}

str
OIDXcreate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	str msg= MAL_SUCCEED;
	int pieces = -1;

	if (pci->argc == 3) {
		pieces = stk->stk[pci->argv[2]].val.ival;
		if (pieces < 0)
			throw(MAL,"bat.orderidx","Positive number expected");
	}

	b = BATdescriptor( *getArgReference_bat(stk, pci, 1));
	if (b == NULL)
		throw(MAL, "bat.orderidx", RUNTIME_OBJECT_MISSING);
	msg = OIDXcreateImplementation(cntxt, getArgType(mb,pci,1), b, pieces);
	BBPunfix(b->batCacheid);
	return msg;
}

str
OIDXhasorderidx(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	bit *ret = getArgReference_bit(stk,pci,0);
	bat bid = *getArgReference_bat(stk, pci, 1);

	(void) cntxt;
	(void) mb;

	b = BATdescriptor(bid);
	if (b == NULL)
		throw(MAL, "bat.hasorderidx", RUNTIME_OBJECT_MISSING);

	*ret = b->torderidx != NULL;

	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

#if 0
str
OIDXgetorderidx(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	BAT *bn;
	bat *ret = getArgReference_bat(stk,pci,0);
	bat bid = *getArgReference_bat(stk, pci, 1);

	(void) cntxt;
	(void) mb;

	b = BATdescriptor(bid);
	if (b == NULL)
		throw(MAL, "bat.getorderidx", RUNTIME_OBJECT_MISSING);

	if (b->torderidx == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.getorderidx", RUNTIME_OBJECT_MISSING);
	}

	if ((bn = BATnew(TYPE_void, TYPE_oid, BATcount(b), TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.getorderidx", MAL_MALLOC_FAIL);
	}
	memcpy(Tloc(bn, BUNfirst(bn)), b->torderidx->base, BATcount(b) * sizeof(oid));
	BATsetcount(bn, BATcount(b));
	BATseqbase(bn, 0);
	bn->tkey = 1;
	bn->tsorted = bn->trevsorted = BATcount(b) <= 1;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}
#endif

/*
 * Merge the collection of sorted OID BATs into one
 */

str
OIDXmerge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b;
	BAT **a;
	int i, j, n_ar;
	BUN m_sz;
	gdk_return rc;

	(void) cntxt;
	(void) mb;

	assert(pci->retc == 1);
	assert(pci->argc > 2);

	n_ar = pci->argc - 2;

	bid = *getArgReference_bat(stk, pci, 1);
	b = BATdescriptor(bid);
	if (b == NULL)
		throw(MAL, "bat.orderidx", RUNTIME_OBJECT_MISSING);

	assert(BAThdense(b));	/* assert void headed */
	assert(b->torderidx == NULL);

	switch (ATOMstorage(b->ttype)) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_flt:
	case TYPE_dbl:
		break;
	case TYPE_void:
	case TYPE_str:
	case TYPE_ptr:
	default:
		/* TODO: support strings, date, timestamps etc. */
		BBPunfix(bid);
		throw(MAL, "bat.orderidx", TYPE_NOT_SUPPORTED);
	}

	if ((a = (BAT **) GDKmalloc(n_ar*sizeof(BAT *))) == NULL) {
		BBPunfix(bid);
		throw(MAL, "bat.orderidx", MAL_MALLOC_FAIL);
	}
	m_sz = 0;
	for (i = 0; i < n_ar; i++) {
		a[i] = BATdescriptor(*getArgReference_bat(stk, pci, i+2));
		if (a[i] == NULL) {
			for (j = i-1; j >= 0; j--) {
				BBPunfix(a[j]->batCacheid);
			}
			GDKfree(a);
			BBPunfix(bid);
			throw(MAL, "bat.orderidx", RUNTIME_OBJECT_MISSING);
		}
		m_sz += BATcount(a[i]);
		if (BATcount(a[i]) == 0) {
			BBPunfix(a[i]->batCacheid);
			a[i] = NULL;
		}
	}
	assert(m_sz == BATcount(b));
	for (i = 0; i < n_ar; i++) {
		if (a[i] == NULL) {
			n_ar--;
			if (i < n_ar)
				a[i] = a[n_ar];
			i--;
		}
	}
	if (m_sz != BATcount(b)) {
		BBPunfix(bid);
		for (i = 0; i < n_ar; i++)
			BBPunfix(a[i]->batCacheid);
		GDKfree(a);
		throw(MAL, "bat.orderidx", "count mismatch");
	}

	rc = GDKmergeidx(b, a, n_ar);

	for (i = 0; i < n_ar; i++)
		BBPunfix(a[i]->batCacheid);
	GDKfree(a);
	BBPunfix(bid);

	if (rc != GDK_SUCCEED)
		throw(MAL, "bat.orderidx", MAL_MALLOC_FAIL);

	return MAL_SUCCEED;
}
