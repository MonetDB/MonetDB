/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (c) Martin Kersten, Lefteris Sidirourgos
 * Implement a parallel sort-merge MAL program generator
 */
#include "monetdb_config.h"
#include "orderidx.h"
#include "gdk.h"

#define MIN_PIECE	((BUN) 1000)	/* TODO use realistic size in production */

str
OIDXdropImplementation(Client cntxt, BAT *b)
{
	(void) cntxt;
	OIDXdestroy(b);
	return MAL_SUCCEED;
}

str
OIDXcreateImplementation(Client cntxt, int tpe, BAT *b, int pieces)
{
	int i, loopvar, arg;
	BUN cnt, step=0,o;
	MalBlkPtr smb;
	MalStkPtr newstk;
	Symbol snew = NULL;
	InstrPtr q, pack;
	char name[IDLENGTH];
	str msg= MAL_SUCCEED;

	if (BATcount(b) <= 1)
		return MAL_SUCCEED;

	/* check if b is sorted, then index does not make sense */
	if (b->tsorted || b->trevsorted)
		return MAL_SUCCEED;

	/* check if b already has index */
	if (b->torderidx)
		return MAL_SUCCEED;

	switch (ATOMbasetype(b->ttype)) {
	case TYPE_void:
		/* trivially supported */
		return MAL_SUCCEED;
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_flt:
	case TYPE_dbl:
		if (GDKnr_threads > 1 && BATcount(b) >= 2 * MIN_PIECE && (GDKdebug & FORCEMITOMASK) == 0)
			break;
		/* fall through */
	default:
		if (BATorderidx(b, 1) != GDK_SUCCEED)
			throw(MAL, "bat.orderidx", TYPE_NOT_SUPPORTED);
		return MAL_SUCCEED;
	}

	if( pieces <= 0 ){
		if (GDKnr_threads <= 1) {
			pieces = 1;
		} else if (GDKdebug & FORCEMITOMASK) {
			/* we want many pieces, even tiny ones */
			if (BATcount(b) < 4)
				pieces = 1;
			else if (BATcount(b) / 2 < (BUN) GDKnr_threads)
				pieces = (int) (BATcount(b) / 2);
			else
				pieces = GDKnr_threads;
		} else {
			if (BATcount(b) < 2 * MIN_PIECE)
				pieces = 1;
			else if (BATcount(b) / MIN_PIECE < (BUN) GDKnr_threads)
				pieces = (int) (BATcount(b) / MIN_PIECE);
			else
				pieces = GDKnr_threads;
		}
	} else if (BATcount(b) < (BUN) pieces || BATcount(b) < MIN_PIECE) {
		pieces = 1;
	}
#ifdef _DEBUG_OIDX_
	fprintf(stderr,"#bat.orderidx pieces %d\n",pieces);
	fprintf(stderr,"#oidx ttype %s bat %s\n", ATOMname(b->ttype),ATOMname(tpe));
#endif

	/* create a temporary MAL function to sort the BAT in parallel */
	snprintf(name, IDLENGTH, "sort%d", rand()%1000);
	snew = newFunction(putName("user"), putName(name),
	       FUNCTIONsymbol);
	if(snew == NULL) {
		msg = createException(MAL, "bat.orderidx", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto bailout;
	}
	smb = snew->def;
	q = getInstrPtr(smb, 0);
	arg = newTmpVariable(smb, tpe);
	q= pushArgument(smb, q, arg);
	getArg(q,0) = newTmpVariable(smb, TYPE_void);

	resizeMalBlk(smb, 2*pieces+10); // large enough
	/* create the pack instruction first, as it will hold
	 * intermediate variables */
	pack = newInstruction(0, putName("bat"), putName("orderidx"));
	if(pack == NULL) {
		msg = createException(MAL, "bat.orderidx", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto bailout;
	}
	pack->argv[0] = newTmpVariable(smb, TYPE_void);
	pack = pushArgument(smb, pack, arg);
	setVarFixed(smb, getArg(pack, 0));

	/* the costly part executed as a parallel block */
	loopvar = newTmpVariable(smb, TYPE_bit);
	q = newStmt(smb, putName("language"), putName("dataflow"));
	q->barrier = BARRIERsymbol;
	q->argv[0] = loopvar;

	cnt = BATcount(b);
	step = cnt/pieces;
	o = 0;
	for (i = 0; i < pieces; i++) {
		/* add slice instruction */
		q = newStmt(smb, putName("algebra"),putName("slice"));
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
		q = pushOid(smb, q, o - 1);
	}
	for (i = 0; i < pieces; i++) {
		/* add sort instruction */
		q = newStmt(smb, putName("algebra"), putName("orderidx"));
		setVarType(smb, getArg(q, 0), tpe);
		setVarFixed(smb, getArg(q, 0));
		q = pushArgument(smb, q, pack->argv[2+i]);
		q = pushBit(smb, q, 1);
		pack->argv[2+i] = getArg(q, 0);
	}
	/* finalize OID packing, check, and evaluate */
	pushInstruction(smb,pack);
	q = newAssignment(smb);
	if(q == NULL) {
		msg = createException(MAL, "bat.orderidx", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto bailout;
	}
	q->barrier = EXITsymbol;
	q->argv[0] = loopvar;
	pushEndInstruction(smb);
	chkProgram(cntxt->usermodule, smb);
	//printFunction(THRdata[0], smb, 0 , 23);
	if (smb->errors) {
		msg = createException(MAL, "bat.orderidx",
		                           "Type errors in generated code");
	} else {
		/* evaluate MAL block and keep the ordered OID bat */
		newstk = prepareMALstack(smb, smb->vsize);
		if (newstk == NULL) {
			msg = createException(MAL, "bat.orderidx", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			goto bailout;
		}
		newstk->up = 0;
		newstk->stk[arg].vtype= TYPE_bat;
		newstk->stk[arg].val.bval= b->batCacheid;
		BBPretain(newstk->stk[arg].val.bval);
		msg = runMALsequence(cntxt, smb, 1, 0, newstk, 0, 0);
		freeStack(newstk);
	}
#ifdef _DEBUG_OIDX_
	fprintFunction(stderr, smb, 0, LIST_MAL_ALL);
#endif
	/* get rid of temporary MAL block */
bailout:
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
		throw(MAL, "bat.orderidx", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
		throw(MAL, "bat.hasorderidx", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	*ret = b->torderidx != NULL;

	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

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
		throw(MAL, "bat.getorderidx", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (b->torderidx == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.getorderidx", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if ((bn = COLnew(0, TYPE_oid, BATcount(b), TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.getorderidx", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	memcpy(Tloc(bn, 0), (const oid *) b->torderidx->base + ORDERIDXOFF,
		   BATcount(b) * SIZEOF_OID);
	BATsetcount(bn, BATcount(b));
	bn->tkey = 1;
	bn->tsorted = bn->trevsorted = BATcount(b) <= 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
OIDXorderidx(bat *ret, const bat *bid, const bit *stable)
{
	BAT *b;
	gdk_return r;

	(void) ret;
	b = BATdescriptor(*bid);
	if (b == NULL)
		throw(MAL, "algebra.orderidx", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	r = BATorderidx(b, *stable);
	if (r != GDK_SUCCEED) {
		BBPunfix(*bid);
		throw(MAL, "algebra.orderidx", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	*ret = *bid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

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
		throw(MAL, "bat.orderidx", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	assert(b->torderidx == NULL);

	switch (ATOMbasetype(b->ttype)) {
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
	case TYPE_str:
		/* TODO: support strings etc. */
	case TYPE_void:
	case TYPE_ptr:
	default:
		BBPunfix(bid);
		throw(MAL, "bat.orderidx", TYPE_NOT_SUPPORTED);
	}

	if ((a = (BAT **) GDKmalloc(n_ar*sizeof(BAT *))) == NULL) {
		BBPunfix(bid);
		throw(MAL, "bat.orderidx", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			throw(MAL, "bat.orderidx", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
		throw(MAL, "bat.orderidx", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	return MAL_SUCCEED;
}
