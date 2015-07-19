/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * Implement a parallel sort-merge MAL program generator
 */
#include "monetdb_config.h"
#include "orderidx.h"
#include "gdk.h"

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

	if( pieces < 0){
		/* TODO estimate number of pieces */
		pieces = 3;
	}
#ifdef _DEBUG_OIDX_
	mnstr_printf(cntxt->fdout,"#bat.orderidx pieces %d\n",pieces);
#endif

	mnstr_printf(cntxt->fdout,"#oidx ttype %d bat %d\n", b->ttype,tpe);
	/* TODO: check if b already has index */
	/* TODO: check if b is sorted, then index does nto make sense, other action  is needed*/
	/* TODO: check if b is view and parent has index do a range select */

	// create a temporary MAL function
	snprintf(name, IDLENGTH, "sort%d", rand()%1000);
	snew = newFunction(putName("user", 4), putName(name, strlen(name)), FUNCTIONsymbol);
	smb = snew->def;
	q = getInstrPtr(smb, 0);
	arg = newTmpVariable(smb, tpe);
	pushArgument(smb, q, arg);
	getArg(q,0) = newTmpVariable(smb, TYPE_void);

	resizeMalBlk(smb, 2*pieces+10, 2*pieces+10); // large enough
	// create the pack instruction first, as it will hold intermediate variables
	pack = newInstruction(0, ASSIGNsymbol);
	setModuleId(pack, putName("bat", 3));
	setFunctionId(pack, putName("orderidx", 8));
	pack->argv[0] = newTmpVariable(smb, TYPE_void);
	pack = pushArgument(smb, pack, arg);
	setVarFixed(smb, getArg(pack, 0));

	// the costly part executed as a parallel block
	loopvar = newTmpVariable(smb, TYPE_bit);
	q = newStmt(smb, putName("language", 8), putName("dataflow", 8));
	q->barrier = BARRIERsymbol;
	q->argv[0] = loopvar;

	cnt = BATcount(b);
	step = cnt/pieces;
	o = 0;
	for (i=0; i< pieces; i++) {
		// add slice instruction
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
	}
	for (i=0; i< pieces; i++) {
		// add sort instruction
		q = newStmt(smb, putName("algebra",7), putName("orderidx", 8));
		setVarType(smb, getArg(q, 0), newBatType(TYPE_oid, TYPE_oid));
		setVarFixed(smb, getArg(q, 0));
		q = pushArgument(smb, q, pack->argv[2+i]);
		q = pushBit(smb, q, 0);
		q = pushBit(smb, q, 1);
		pack->argv[2+i] = getArg(q, 0);
	}
	// finalize, check, and evaluate
	pushInstruction(smb,pack);
	q = newAssignment(smb);
	q->barrier = EXITsymbol;
	q->argv[0] =loopvar;
	pushEndInstruction(smb);
	chkProgram(cntxt->fdout, cntxt->nspace, smb);
	if (smb->errors) {
		msg = createException(MAL, "bat.orderidx", "Type errors in generated code");
	} else {
		// evaluate MAL block
		newstk = prepareMALstack(smb, smb->vsize);
		newstk->up = 0;
		newstk->stk[arg].vtype= TYPE_bat;
		newstk->stk[arg].val.bval= b->batCacheid;
		BBPincref(newstk->stk[arg].val.bval, TRUE);
        msg = runMALsequence(cntxt, smb, 1, 0, newstk, 0, 0);
		freeStack(newstk);
	}
#ifdef _DEBUG_INDEX_
	printFunction(cntxt->fdout, smb, 0, LIST_MAL_ALL);
#endif
	// get rid of temporary MAL block
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
OIDXgetorderidx(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	bat *ret = getArgReference_bat(stk,pci,0);
	bat bid = *getArgReference_bat(stk, pci, 1);

	(void) cntxt;
	(void) mb;


	b = BATdescriptor(bid);
	if (b == NULL)
		throw(MAL, "bat.getorder", RUNTIME_OBJECT_MISSING);

	*ret = ORDERgetidx(b);

	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
OIDXmerge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *b;
	bat *aid;
	BAT **a;
	int i, j, n_ar;

	(void) cntxt;
	(void) mb;

	assert(pci->argc > 2);
	n_ar = pci->argc-2;

	bid = *getArgReference_bat(stk, pci, 1);
	b = BATdescriptor(bid);
	if (b == NULL)
		throw(MAL, "bat.orderidx", RUNTIME_OBJECT_MISSING);

	assert(BAThdense(b));	/* assert void headed */
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
		throw(MAL, "bat.orderidx", TYPE_NOT_SUPPORTED);
	}

	if ((aid = (bat *) GDKzalloc(n_ar*sizeof(bat))) == NULL ) {
		BBPunfix(bid);
		throw(MAL, "bat.orderidx", MAL_MALLOC_FAIL);
	}
	if ((a = (BAT **) GDKzalloc(n_ar*sizeof(BAT *))) == NULL) {
		BBPunfix(bid);
		GDKfree(aid);
		throw(MAL, "bat.orderidx", MAL_MALLOC_FAIL);
	}
	for (i = 0; i < n_ar; i++) {
		aid[i] = *getArgReference_bat(stk, pci, i+2);
		a[i] = BATdescriptor(aid[i]);
		if (a[i] == NULL) {
			for (j = i-1; j >= 0; j--) {
				BBPunfix(aid[j]);
			}
			GDKfree(aid);
			GDKfree(a);
			BBPunfix(bid);
			throw(MAL, "bat.orderidx", RUNTIME_OBJECT_MISSING);
		}
	}

	if (n_ar == 1) {
		/* One oid order bat, nothing to merge */
		if (ORDERkeepidx(b, a[0]) == GDK_FAIL) {
			BBPunfix(aid[0]);
			BBPunfix(bid);
			GDKfree(aid);
			GDKfree(a);
			throw(MAL,"bat.orderidx", OPERATION_FAILED);
		}
		BBPincref(aid[0], TRUE);
	} else {
		BAT *m; /* merged oid's */
		oid *mv;
		BUN m_sz;

		for (i=0, m_sz = 0; i < n_ar; i++) {
			m_sz += BATcount(a[i]);
		}
		m = BATnew(TYPE_void, TYPE_oid, m_sz, TRANSIENT);
		if (m == NULL) {
			for (i = 0; i < n_ar; i++)
				BBPunfix(aid[i]);
			BBPunfix(bid);
			GDKfree(aid);
			GDKfree(a);
			throw(MAL,"bat.orderidx", MAL_MALLOC_FAIL);
		}
		mv = (oid *) Tloc(m, BUNfirst(m));

		/* sort merge with 1 comparison per BUN */
		if (n_ar == 2) {
			oid *p0, *p1, *q0, *q1;
			p0 = (oid *) Tloc(a[0], BUNfirst(a[0]));
			q0 = (oid *) Tloc(a[0], BUNlast(a[0]));
			p1 = (oid *) Tloc(a[1], BUNfirst(a[1]));
			q1 = (oid *) Tloc(a[1], BUNlast(a[1]));

#define BINARY_MERGE(TYPE)													\
do {																		\
	TYPE *v = (TYPE *) Tloc(b, BUNfirst(b));								\
	for (; p0 < q0 && p1 < q1; ) {											\
		if (v[*p0] < v[*p1]) {												\
			*mv++ = *p0++;													\
		} else {															\
			*mv++ = *p1++;													\
		}																	\
	}																		\
	while (p0 < q0) {														\
		*mv++ = *p0++;														\
	}																		\
	while (p1 < q1) {														\
		*mv++ = *p1++;														\
	}																		\
} while(0)

			switch (ATOMstorage(b->ttype)) {
			case TYPE_bte: BINARY_MERGE(bte); break;
			case TYPE_sht: BINARY_MERGE(sht); break;
			case TYPE_int: BINARY_MERGE(int); break;
			case TYPE_lng: BINARY_MERGE(lng); break;
#ifdef HAVE_HGE
			case TYPE_hge: BINARY_MERGE(hge); break;
#endif
			case TYPE_flt: BINARY_MERGE(flt); break;
			case TYPE_dbl: BINARY_MERGE(dbl); break;
			case TYPE_void:
			case TYPE_str:
			case TYPE_ptr:
			default:
				/* TODO: support strings, date, timestamps etc. */
				throw(MAL, "bat.orderidx", TYPE_NOT_SUPPORTED);
			}

		/* use min-heap */
		} else {
			oid **p, **q, *t_oid;

			if ((p = (oid **) GDKzalloc(n_ar*sizeof(oid *))) == NULL) {
				for (i = 0; i < n_ar; i++)
					BBPunfix(aid[i]);
				BBPunfix(bid);
				BBPunfix(m->batCacheid);
				GDKfree(aid);
				GDKfree(a);
				throw(MAL,"bat.orderidx", MAL_MALLOC_FAIL);
			}
			if ((q = (oid **) GDKzalloc(n_ar*sizeof(oid *))) == NULL) {
				for (i = 0; i < n_ar; i++)
					BBPunfix(aid[i]);
				BBPunfix(bid);
				BBPunfix(m->batCacheid);
				GDKfree(aid);
				GDKfree(a);
				GDKfree(p);
				throw(MAL,"bat.orderidx", MAL_MALLOC_FAIL);
			}
			for (i = 0; i < n_ar; i++) {
				p[i] = (oid *) Tloc(a[i], BUNfirst(a[i]));
				q[i] = (oid *) Tloc(a[i], BUNlast(a[i]));
			}

#define swap(X,Y,TMP)  (TMP)=(X);(X)=(Y);(Y)=(TMP)

#define left_child(X)  (2*(X)+1)
#define right_child(X) (2*(X)+2)

#define HEAPIFY(X)															\
do {																		\
	int __cur, __min = X;													\
	do {																	\
		__cur = __min;														\
		if (left_child(__cur) < n_ar &&										\
			minhp[left_child(__cur)] < minhp[(__min)]) {					\
			__min = left_child(__cur);										\
		}																	\
		if (right_child(__cur) < n_ar &&									\
			minhp[right_child(__cur)] < minhp[(__min)]) {					\
			__min = right_child(__cur);										\
		}																	\
		if (__min != __cur) {												\
			swap(minhp[__cur], minhp[__min], t);							\
			swap(p[__cur], p[__min], t_oid);								\
			swap(q[__cur], q[__min], t_oid);								\
		}																	\
	} while (__cur != __min);												\
} while (0)

#define NWAY_MERGE(TYPE)													\
do {																		\
	TYPE *minhp, t;															\
	TYPE *v = (TYPE *) Tloc(b, BUNfirst(b));								\
	if ((minhp = (TYPE *) GDKzalloc(sizeof(TYPE)*n_ar)) == NULL) {			\
		for (i = 0; i < n_ar; i++)											\
			BBPunfix(aid[i]);												\
		BBPunfix(bid);														\
		BBPunfix(m->batCacheid);											\
		GDKfree(aid);														\
		GDKfree(a);															\
		GDKfree(p);															\
		GDKfree(q);															\
		throw(MAL,"bat.orderidx", MAL_MALLOC_FAIL);							\
	}																		\
	/* init min heap */														\
	for (i = 0; i < n_ar; i++) {											\
		minhp[i] = v[*p[i]];												\
	}																		\
	for (i = n_ar/2; i >=0 ; i--) {											\
		HEAPIFY(i);															\
	}																		\
	/* merge */																\
	while (n_ar > 1) {														\
		*mv++ = *(p[0])++;													\
		if (p[0] < q[0]) {													\
			minhp[0] = v[*p[0]];											\
		} else {															\
			swap(minhp[0], minhp[n_ar-1], t);								\
			swap(p[0], p[n_ar-1], t_oid);									\
			swap(q[0], q[n_ar-1], t_oid);									\
			n_ar--;															\
		}																	\
		HEAPIFY(0);															\
	}																		\
	while (p[0] < q[0]) {													\
		*mv++ = *(p[0])++;													\
	}																		\
	GDKfree(minhp);															\
} while (0)

			switch (ATOMstorage(b->ttype)) {
			case TYPE_bte: NWAY_MERGE(bte); break;
			case TYPE_sht: NWAY_MERGE(sht); break;
			case TYPE_int: NWAY_MERGE(int); break;
			case TYPE_lng: NWAY_MERGE(lng); break;
#ifdef HAVE_HGE
			case TYPE_hge: NWAY_MERGE(hge); break;
#endif
			case TYPE_flt: NWAY_MERGE(flt); break;
			case TYPE_dbl: NWAY_MERGE(dbl); break;
			case TYPE_void:
			case TYPE_str:
			case TYPE_ptr:
			default:
				/* TODO: support strings, date, timestamps etc. */
				throw(MAL, "bat.orderidx", TYPE_NOT_SUPPORTED);
			}
			GDKfree(p);
			GDKfree(q);
		}
		/* fix m properties */
		BATsetcount(m, m_sz);
		BATseqbase(m, b->hseqbase);
		BATseqbase(BATmirror(m), b->hseqbase);
		m->tkey = 1;
		m->T->nil = 0;
		m->T->nonil = 1;
		m->tsorted = m->trevsorted = 0;
		m->tdense = 0;
		if (ORDERkeepidx(b, m) == GDK_FAIL) {
			for (i = 0; i < n_ar; i++) {
				BBPunfix(aid[i]);
			}
			GDKfree(aid);
			GDKfree(a);
			BBPunfix(m->batCacheid);
			BBPunfix(bid);
			throw(MAL,"bat.orderidx", OPERATION_FAILED);
		}
		for (i = 0; i < n_ar; i++) {
			BBPunfix(aid[i]);
		}
		BBPincref(m->batCacheid, TRUE);
	}

	GDKfree(aid);
	GDKfree(a);
	BBPunfix(bid);
	return MAL_SUCCEED;
}
