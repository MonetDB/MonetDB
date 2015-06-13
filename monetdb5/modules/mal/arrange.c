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
#include "arrange.h"
#include "gdk.h"

str
ARNGcreate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pieces = 3;
	int i, loopvar, bid, arg;
	BUN cnt, step=0,o;
	MalBlkPtr smb;
	MalStkPtr newstk;
	Symbol snew;
	InstrPtr q, pack;
	BAT *b;
	char name[IDLENGTH];
	str msg= MAL_SUCCEED;

	if (pci->argc == 3) {
		pieces = stk->stk[pci->argv[2]].val.ival;
	} else {
		// educated guess needed on number of partitions
	}
#ifdef _DEBUG_ARNG_
	mnstr_printf(cntxt->fdout,"#bat.arrange pieces %d\n",pieces);
#endif

	if (pieces < 0)
		throw(MAL,"bat.arrange","Positive number expected");

	bid = *getArgReference_bat(stk, pci, 1);
	b = BATdescriptor(bid);
	if (b == NULL)
		throw(MAL, "bat.arrange", RUNTIME_OBJECT_MISSING);

	// create a temporary MAL function
	snprintf(name, IDLENGTH, "sort%d", rand()%1000);
	snew = newFunction(putName("user", 4), putName(name, strlen(name)), FUNCTIONsymbol);
	smb = snew->def;
	q = getInstrPtr(smb, 0);
	arg = newTmpVariable(smb, getArgType(mb,pci,1));
	pushArgument(smb, q, arg);
	getArg(q,0) = newTmpVariable(smb, TYPE_void);

	resizeMalBlk(smb, 2*pieces+10, 2*pieces+10); // large enough
	// create the pack instruction first, as it will hold intermediate variables
	pack = newInstruction(0, ASSIGNsymbol);
	setModuleId(pack, putName("bat", 3));
	setFunctionId(pack, putName("arrange", 7));
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
		setVarType(smb, getArg(q,0), getArgType(mb, pci, 1));
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
		q = newStmt(smb, putName("algebra",7), putName("arrangeorder", 9));
		setVarType(smb, getArg(q, 0), newBatType(TYPE_oid, TYPE_oid));
		setVarFixed(smb, getArg(q, 0));
		q = pushArgument(smb, q, pack->argv[2+i]);
		q = pushBit(smb, q, 0);
		q = pushBit(smb, q, 0);
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
		msg = createException(MAL, "bat.arrange", "Type errors in generated code");
	} else {
		// evaluate MAL block
		newstk = prepareMALstack(smb, smb->vsize);
		newstk->up = 0;
		VALcopy(&newstk->stk[arg], &stk->stk[getArg(pci,1)]);
		BBPincref(newstk->stk[arg].val.bval, TRUE);
        msg = runMALsequence(cntxt, smb, 1, 0, newstk, 0, 0);
		freeStack(newstk);
	}
#ifdef _DEBUG_INDEX_
	printFunction(cntxt->fdout, smb, 0, LIST_MAL_ALL);
#endif
	BBPunfix(b->batCacheid);
	// get rid of temporary MAL block
	freeSymbol(snew);
	return msg;
}


str
ARNGmerge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
		throw(MAL, "bat.arrange", RUNTIME_OBJECT_MISSING);
	/* CHECK: types */
	/* CHECK: if b is sorted call ARNGindex b with void,void */
	/* CHECK: if b is view and parent has index do a range select */

	aid = (bat *) GDKzalloc(n_ar*sizeof(bat));
	a = (BAT **) GDKzalloc(n_ar*sizeof(BAT *));
	/* CHECK: nulls*/
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
			throw(MAL, "bat.arrange", RUNTIME_OBJECT_MISSING);
		}
	}

	if (n_ar == 1) {
		/* One oid order bat, nothing to merge */
		if (ARNGindex(b, a[0]) == GDK_FAIL) {
			/* TAKE STEPS*/
		}
	} else {
		BAT *m; /* merged oid's */
		oid *mv;
		BUN m_sz;

		for (i=0, m_sz = 0; i < n_ar; i++) {
			m_sz += BATcount(a[i]);
		}
		m = BATnew(TYPE_void, TYPE_oid, m_sz, TRANSIENT);
		if (m == NULL) {
			/* CHECK: null and clean exit*/
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

			switch (ATOMbasetype(b->T->type)) {
			case TYPE_bte:
				BINARY_MERGE(bte);
				break;
			case TYPE_sht:
				BINARY_MERGE(sht);
				break;
			case TYPE_int:
				BINARY_MERGE(int);
				break;
			case TYPE_lng:
				BINARY_MERGE(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				BINARY_MERGE(hge);
				break;
#endif
			case TYPE_flt:
				BINARY_MERGE(flt);
				break;
			case TYPE_dbl:
				BINARY_MERGE(dbl);
				break;
			default:
				/* should never reach here */
				assert(0);
			}

		/* use min-heap */
		} else {
			oid **p, **q, *t_oid;

			p = (oid **) GDKzalloc(n_ar*sizeof(oid *));
			q = (oid **) GDKzalloc(n_ar*sizeof(oid *));
			/* CHECK null and clean exit */
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
	TYPE *minhp;															\
	TYPE t;																	\
	TYPE *v = (TYPE *) Tloc(b, BUNfirst(b));								\
	if ((minhp = (TYPE *) GDKzalloc(sizeof(TYPE)*n_ar)) == NULL) {			\
		/* CLEAN exit */													\
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
			swap(minhp[0], minhp[n_ar], t);									\
			swap(p[0], p[n_ar], t_oid);										\
			swap(q[0], q[n_ar], t_oid);										\
			n_ar--;															\
		}																	\
		HEAPIFY(0);															\
	}																		\
	while (p[0] < q[0]) {													\
		*mv++ = *(p[0])++;													\
	}																		\
} while (0)

			switch (ATOMbasetype(b->T->type)) {
			case TYPE_bte:
				NWAY_MERGE(bte);
				break;
			case TYPE_sht:
				NWAY_MERGE(sht);
				break;
			case TYPE_int:
				NWAY_MERGE(int);
				break;
			case TYPE_lng:
				NWAY_MERGE(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				NWAY_MERGE(hge);
				break;
#endif
			case TYPE_flt:
				NWAY_MERGE(flt);
				break;
			case TYPE_dbl:
				NWAY_MERGE(dbl);
				break;
			default:
				/* should never reach here */
				assert(0);
			}
		}
		/* fix m properties */
		if (ARNGindex(b, m) == GDK_FAIL) {
			/* CLEAN EXIT */
		}
	}

	for (i = 0; i < n_ar; i++) {
		BBPunfix(aid[i]);
	}
	GDKfree(aid);
	GDKfree(a);
	BBPunfix(bid);
	return MAL_SUCCEED;

	/* arrange merge bats creating a new :bat[:oid:oid] *o
	 * feed *o to ARNGkeep(b, o);
	 * destroy *o
	 */
}
