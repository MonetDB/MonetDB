/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * Nikos Mamoulis, Niels Nes
 * Priority queues
 *
 * This module includes functions for accessing and updating a pqueue.
 * A pqueue is an (oid,any) bat. The tail is used as a comparison key.
 * The first element of the pqueue is the smallest one in a min-pqueue
 * or the largest one in a max-pqueue.
 * Each element is larger than (smaller than) or equal to
 * its parent which is defined by (position/2) if position is odd or
 * (position-1)/2 if position is even (positions are from 0 to n-1).
 * The head of the bat is used to keep track of the object-ids which are
 * organized in the heap with respect to their values (tail column).
 *
 */

#include "pqueue.h"

/* returns the parent of a pqueue position */
static inline BUN
parent(BUN posel)
{
	if (posel % 2)				/*odd */
		return posel / 2;
	else
		return (posel - 1) / 2;
}

/* initialize pqueue */
static int
do_pqueue_init(BAT **h, BAT *b, BUN maxsize)
{
	*h = BATnew(TYPE_oid, b->ttype, maxsize);
	if (*h == NULL)
		return GDK_FAIL;
	(*h)->batDirty |= 2;
	return GDK_SUCCEED;
}

static int
pqueue_init(BAT **h, BAT *b, wrd *maxsize)
{
	return do_pqueue_init(h, b, (BUN) *maxsize);
}

#define ht_swap(tpe,cur,ins)								\
	do {													\
		oid htmp = *(oid*)BUNhloc(hi,cur);					\
		tpe ttmp = *(tpe*)BUNtloc(hi,cur);					\
		*(oid*)BUNhloc(hi,cur) = *(oid*)BUNhloc(hi,ins);	\
		*(tpe*)BUNtloc(hi,cur) = *(tpe*)BUNtloc(hi,ins);	\
		*(oid*)BUNhloc(hi,ins) = htmp;						\
		*(tpe*)BUNtloc(hi,ins) = ttmp;						\
	} while (0)

#define any_swap(cur,ins,ts)								\
	do {													\
		unsigned int i;										\
		char ch;											\
		/* only swap the locations (ie var_t's) */			\
		char *c1 = BUNtloc(hi,cur), *c2 = BUNtloc(hi,ins);	\
		oid htmp = *(oid*)BUNhloc(hi,cur);					\
		*(oid*)BUNhloc(hi,cur) = *(oid*)BUNhloc(hi,ins);	\
		*(oid*)BUNhloc(hi,ins) = htmp;						\
		for(i=0;i<ts;i++) {									\
			ch= c1[i]; c1[i]=c2[i]; c2[i]=ch;				\
		}													\
	} while (0)

#define pqueueimpl_minmax(TYPE,NAME,OPER)								\
	/*enqueue an element*/												\
	static int pqueue_enqueue_##NAME(BAT *h, oid *idx, TYPE *el)		\
	{																	\
		BATiter hi = bat_iterator(h);									\
		BUN ins,cur;													\
																		\
		BUN hbase = BUNfirst(h);										\
		BUN p, posel = BATcount(h); /*last position*/					\
																		\
		BUNins(h, (ptr)idx, (ptr)el, FALSE);							\
		ins = hbase+posel;												\
																		\
		while(posel >0) {												\
			p=parent(posel);											\
			cur = hbase+p;												\
			if (*(TYPE *)BUNtloc(hi,ins) OPER *(TYPE *)BUNtloc(hi,cur)) { \
				/* swap element with its parent */						\
				ht_swap(TYPE,cur,ins);									\
				ins = cur;												\
				posel = parent(posel);									\
			}															\
			else break;													\
		}																\
		h->hsorted = h->tsorted = FALSE;								\
		h->hrevsorted = h->trevsorted = FALSE;							\
																		\
		return GDK_SUCCEED;												\
	}																	\
																		\
	/* moves down the root element */									\
	/* used by dequeue (see below) */									\
	static int pqueue_movedowntop_##NAME(BAT *h)						\
	{																	\
		BATiter hi = bat_iterator(h);									\
		BUN swp, cur, hbase = BUNfirst(h);								\
		BUN swap, num_elems = BATcount(h);								\
		BUN posel = 0;													\
																		\
		cur = hbase;													\
																		\
		/*while posel is not a leaf and pqueue[posel].tail > any of childen*/ \
		while (posel*2+1 < num_elems) { /*there exists a left son*/		\
			if (posel*2+2< num_elems) { /*there exists a right son*/	\
				if (*(TYPE *)BUNtloc(hi,hbase+(posel*2+1)) OPER			\
					*(TYPE *)BUNtloc(hi,hbase+(posel*2+2)))				\
					swap = posel*2+1;									\
				else													\
					swap = posel*2+2;									\
			} else														\
				swap = posel*2+1;										\
																		\
			swp = hbase+swap;											\
																		\
			if (*(TYPE *)BUNtloc(hi,swp) OPER *(TYPE *)BUNtloc(hi,cur)) { \
				/*swap elements*/										\
				ht_swap(TYPE,cur,swp);									\
				cur = swp;												\
				posel = swap;											\
			} else														\
				break;													\
		}																\
																		\
		return GDK_SUCCEED;												\
	}																	\
																		\
	/* removes the root element, puts the last element as root and */	\
	/* moves it down */													\
	static int pqueue_dequeue_##NAME(BAT *h)							\
	{																	\
		BATiter hi = bat_iterator(h);									\
		BUN hbase;														\
		BUN num_elements;												\
																		\
		if (!(num_elements = BATcount(h))) {							\
			/* pqueue_dequeue: Cannot dequeue from empty queue */		\
			return GDK_FAIL;											\
		}																\
																		\
		hbase = BUNfirst(h);											\
																		\
		/* copy last element to the first position*/					\
		ht_swap(TYPE,hbase, hbase+(num_elements-1));					\
																		\
		/*delete last element*/											\
		BUNdelete(h, hbase+(num_elements-1), FALSE);					\
																		\
		pqueue_movedowntop_##NAME(h);									\
		return GDK_SUCCEED;												\
	}																	\
																		\
	/* replaces the top element with the input if it is larger */		\
	/* (smaller) and \ updates the heap */								\
	static int pqueue_topreplace_##NAME(BAT *h, oid *idx, TYPE *el)		\
	{																	\
		BATiter hi = bat_iterator(h);									\
		BUN hbase;														\
																		\
		hbase = BUNfirst(h);											\
																		\
		if (*(TYPE *)BUNtloc(hi,hbase) OPER *el) {						\
			*(oid*)BUNhloc(hi,hbase) = *idx;							\
			*(TYPE*)BUNtloc(hi,hbase) = *el;							\
			pqueue_movedowntop_##NAME(h);								\
		}																\
																		\
		return GDK_SUCCEED;												\
	}																	\
																		\
	/* TopN, based on ##NAME-pqueue */									\
																		\
	static BAT *														\
	heap2bat_##NAME(BAT *h)												\
	{																	\
		BAT *r, *n = BATnew(TYPE_oid, TYPE_oid, BATcount(h));			\
		BUN f = BUNfirst(h);											\
		oid *o = (oid*)Hloc(h, f), oo = *o;								\
		TYPE *v = (TYPE*)Tloc(h, f), ov = *v;							\
																		\
		for(f = BUNfirst(h); f < BUNlast(h); f = BUNfirst(h)) {			\
			o = (oid*)Hloc(h, f);										\
			v = (TYPE*)Tloc(h, f);										\
			if (ov != *v) {												\
				oo = *o;												\
				ov = *v;												\
			}															\
			BUNins(n, o, &oo, FALSE);									\
			pqueue_dequeue_##NAME(h);									\
		}																\
		r = BATrevert(n);												\
		return r;														\
	}																	\
																		\
	static int pqueue_topn_void##NAME(BAT **H, BAT *t, wrd *N)			\
	{																	\
		BAT *h = NULL;													\
		BATiter ti = bat_iterator(t);									\
		TYPE *v;														\
		BUN i, n = BATcount(t);											\
		oid idx = t->hseqbase;											\
																		\
		if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX && (BUN) *N < n) \
			n = (BUN) *N;												\
		if (do_pqueue_init(&h,t,n) == GDK_FAIL)							\
			return GDK_FAIL;											\
		v = (TYPE*)BUNtail(ti,BUNfirst(t));								\
																		\
		for(i=0; i<n; i++, idx++, v++) {								\
			pqueue_enqueue_##NAME(h, &idx, v);							\
		}																\
		n = BATcount(t);												\
		for(; i<n; i++, idx++, v++) {									\
			pqueue_topreplace_##NAME(h, &idx, v);						\
		}																\
		*H = heap2bat_##NAME(h);										\
		BBPunfix(h->batCacheid);										\
		return GDK_SUCCEED;												\
	}																	\
																		\
	static int pqueue_topn_##NAME(BAT **H, BAT *t, wrd *N)				\
	{																	\
		BAT *h = NULL;													\
		BATiter ti = bat_iterator(t);									\
		BUN i, n = BATcount(t);											\
		BUN p = BUNfirst(t);											\
																		\
		if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX && (BUN) *N < n) \
			n = (BUN) *N;												\
		if (do_pqueue_init(&h,t,n) == GDK_FAIL)							\
			return GDK_FAIL;											\
																		\
		for(i=0; i<n; i++, p++) {										\
			pqueue_enqueue_##NAME(h, (oid*)BUNhloc(ti,p), (TYPE*)BUNtloc(ti,p)); \
		}																\
		n = BATcount(t);												\
		for(; i<n; i++, p++) {											\
			pqueue_topreplace_##NAME(h, (oid*)BUNhloc(ti,p), (TYPE*)BUNtloc(ti,p));	\
		}																\
		*H = heap2bat_##NAME(h);										\
		BBPunfix(h->batCacheid);										\
		return GDK_SUCCEED;												\
	}																	\
																		\
	/* TopN (unique values), based on ##NAME-pqueue */					\
																		\
	static int pqueue_utopn_void##NAME(BAT **H, BAT *t, wrd *N)			\
	{																	\
		BAT *duplicates = NULL, *b;										\
		BATiter ti = bat_iterator(t);									\
		TYPE *v;														\
		BUN i, j, n, cnt = BATcount(t), p;								\
		oid idx = t->hseqbase;											\
																		\
		n = cnt;														\
		if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX)			\
			n = (BUN) *N;												\
		if (do_pqueue_init(H,t,n) == GDK_FAIL)							\
			return GDK_FAIL;											\
		duplicates = BATnew(TYPE_oid, TYPE_oid, n);						\
		if (duplicates == NULL) {										\
			BBPunfix((*H)->batCacheid);									\
			return GDK_FAIL;											\
		}																\
		v = (TYPE*)BUNtail(ti,BUNfirst(t));								\
																		\
		for(i=0, j=0; j < cnt && i<n; j++, idx++, v++) {				\
			if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {			\
				pqueue_enqueue_##NAME(*H, &idx, v);						\
				HASHdestroy(*H);										\
				BUNins(duplicates, &idx, &idx, FALSE);					\
				i++;													\
			} else {													\
				BUNins(duplicates, Hloc(*H, p), &idx, FALSE);			\
			}															\
		}																\
		for(; j<cnt; j++, idx++, v++) {									\
			if (*(TYPE *)Tloc(*H,BUNfirst(*H)) OPER##= *v) {			\
				if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {		\
					oid o_idx = *(oid*)Hloc(*H, BUNfirst(*H));			\
					BUNdelHead(duplicates, &o_idx, TRUE);				\
					pqueue_topreplace_##NAME(*H, &idx, v);				\
					HASHdestroy(*H);									\
					BUNins(duplicates, &idx, &idx, FALSE);				\
				} else {												\
					BUNins(duplicates, Hloc(*H, p), &idx, FALSE);		\
				}														\
			}															\
		}																\
		b = heap2bat_##NAME(*H);										\
		BBPunfix((*H)->batCacheid); *H = b;								\
		b = VIEWcombine(*H);											\
		BBPunfix((*H)->batCacheid); *H = b;								\
		b = BATleftjoin(*H, duplicates, BATcount(duplicates));			\
		BBPunfix((*H)->batCacheid);										\
		BBPunfix(duplicates->batCacheid);								\
		*H = BATmirror(b);												\
		return GDK_SUCCEED;												\
	}																	\
																		\
	static int pqueue_utopn_##NAME(BAT **H, BAT *t, wrd *N)				\
	{																	\
		BAT *duplicates = NULL, *b;										\
		BATiter ti = bat_iterator(t);									\
		TYPE *v;														\
		BUN i, j, n, cnt = BATcount(t), p;								\
		oid *idx;														\
																		\
		n = cnt;														\
		if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX)			\
			n = (BUN) *N;												\
		if (do_pqueue_init(H,t,n) == GDK_FAIL)							\
			return GDK_FAIL;											\
		duplicates = BATnew(TYPE_oid, TYPE_oid, n);						\
		if (duplicates == NULL) {										\
			BBPunfix((*H)->batCacheid);									\
			return GDK_FAIL;											\
		}																\
		v = (TYPE*)BUNtail(ti,BUNfirst(t));								\
		idx = (oid*)BUNhead(ti,BUNfirst(t));							\
																		\
		for(i=0, j=0; j < cnt && i<n; j++, idx++, v++) {				\
			if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {			\
				pqueue_enqueue_##NAME(*H, idx, v);						\
				HASHdestroy(*H);										\
				BUNins(duplicates, idx, idx, FALSE);					\
				i++;													\
			} else {													\
				BUNins(duplicates, Hloc(*H, p), idx, FALSE);			\
			}															\
		}																\
		for(; j<cnt; j++, idx++, v++) {									\
			if (*(TYPE *)Tloc(*H,BUNfirst(*H)) OPER##= *v) {			\
				if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {		\
					oid o_idx = *(oid*)Hloc(*H, BUNfirst(*H));			\
					BUNdelHead(duplicates, &o_idx, TRUE);				\
					pqueue_topreplace_##NAME(*H, idx, v);				\
					HASHdestroy(*H);									\
					BUNins(duplicates, idx, idx, FALSE);				\
				} else {												\
					BUNins(duplicates, Hloc(*H, p), idx, FALSE);		\
				}														\
			}															\
		}																\
		b = heap2bat_##NAME(*H);										\
		BBPunfix((*H)->batCacheid); *H = b;								\
		b = VIEWcombine(*H);											\
		BBPunfix((*H)->batCacheid); *H = b;								\
		b = BATleftjoin(*H, duplicates, BATcount(duplicates));			\
		BBPunfix((*H)->batCacheid);										\
		BBPunfix(duplicates->batCacheid);								\
		*H = BATmirror(b);												\
		return GDK_SUCCEED;												\
	}

pqueueimpl_minmax(bte, btemin, <)
pqueueimpl_minmax(bte, btemax, >)
pqueueimpl_minmax(sht, shtmin, <)
pqueueimpl_minmax(sht, shtmax, >)
pqueueimpl_minmax(int, intmin, <)
pqueueimpl_minmax(int, intmax, >)
pqueueimpl_minmax(wrd, wrdmin, <)
pqueueimpl_minmax(wrd, wrdmax, <)
pqueueimpl_minmax(oid, oidmin, >)
pqueueimpl_minmax(oid, oidmax, >)
pqueueimpl_minmax(lng, lngmin, <)
pqueueimpl_minmax(lng, lngmax, >)
pqueueimpl_minmax(flt, fltmin, <)
pqueueimpl_minmax(flt, fltmax, >)
pqueueimpl_minmax(dbl, dblmin, <)
pqueueimpl_minmax(dbl, dblmax, >)

/* The fallback case, non optimized */
/*enqueue an element*/
static int
pqueue_enqueue_anymin(BAT *h, oid *idx, ptr el, int tpe)
{
	BATiter hi = bat_iterator(h);
	BUN hbase;
	BUN ins, cur;
	BUN p, posel;
	unsigned short ts;

	hbase = BUNfirst(h);

	posel = BATcount(h);		/*last position */
	BUNins(h, (ptr) idx, (ptr) el, FALSE);
	ts = Tsize(h);
	ins = hbase + posel;

	while (posel > 0) {
		p = parent(posel);
		cur = hbase + p;
		if (atom_CMP(BUNtail(hi, ins), BUNtail(hi, cur), tpe) < 0) {
			/* swap element with its parent */
			any_swap(cur, ins, ts);
			ins = cur;
			posel = parent(posel);
		} else
			break;
	}
	h->hsorted = h->tsorted = FALSE;
	h->hrevsorted = h->trevsorted = FALSE;

	return GDK_SUCCEED;
}

/* moves down the root element */
/* used by dequeue (see below) */
static int
pqueue_movedowntop_anymin(BAT *h)
{
	BATiter hi = bat_iterator(h);
	BUN hbase;
	BUN swp, cur;
	int tpe = BATttype(h);
	BUN swap, num_elems;
	BUN posel;
	unsigned short ts = Tsize(h);

	hbase = BUNfirst(h);

	cur = hbase;
	num_elems = BATcount(h);
	posel = 0;

	/*while posel is not a leaf and pqueue[posel].tail > any of childen */
	while (posel * 2 + 1 < num_elems) {	/*there exists a left son */
		if (posel * 2 + 2 < num_elems) {	/*there exists a right son */
			if (atom_CMP(BUNtail(hi, hbase + (posel * 2 + 1)),
						 BUNtail(hi, hbase + (posel * 2 + 2)), tpe) < 0) {
				swap = posel * 2 + 1;
			} else {
				swap = posel * 2 + 2;
			}
		} else
			swap = posel * 2 + 1;

		swp = hbase + swap;

		if (atom_CMP(BUNtail(hi, swp), BUNtail(hi, cur), tpe) < 0) {
			/*swap elements */
			any_swap(cur, swp, ts);
			cur = swp;
			posel = swap;
		} else
			break;
	}
	h->hsorted = h->tsorted = FALSE;
	h->hrevsorted = h->trevsorted = FALSE;

	return GDK_SUCCEED;
}

/* removes the root element, puts the last element as root and moves it down */
static int
pqueue_dequeue_anymin(BAT *h)
{
	BATiter hi = bat_iterator(h);
	BUN hbase;
	BUN num_elements;
	unsigned short ts = Tsize(h);

	if (!(num_elements = BATcount(h))) {
		/* pqueue_dequeue: Cannot dequeue from empty queue */
		return GDK_FAIL;
	}

	hbase = BUNfirst(h);

	/* copy last element to the first position */
	any_swap(hbase, hbase + (num_elements - 1), ts);

	/*delete last element */
	BUNdelete(h, hbase + (num_elements - 1), FALSE);

	pqueue_movedowntop_anymin(h);
	return GDK_SUCCEED;
}

/* replaces the top element with the input if it is larger (smaller) and
 * updates the heap */
static int
pqueue_topreplace_anymin(BAT *h, oid *idx, ptr el, int tpe)
{
	BATiter hi = bat_iterator(h);
	BUN hbase = BUNfirst(h);

	if (atom_CMP(BUNtail(hi, hbase), el, tpe) < 0) {
		BUNinplace(h, hbase, idx, el, 0);
		*(oid *) BUNhloc(hi, hbase) = *idx;
		pqueue_movedowntop_anymin(h);
		h->hsorted = h->tsorted = FALSE;
		h->hrevsorted = h->trevsorted = FALSE;
	}

	return GDK_SUCCEED;
}

static BAT *
heap2bat_anymin(BAT *h)
{
	BATiter hi = bat_iterator(h);
	BAT *r, *n = BATnew(TYPE_oid, TYPE_oid, BATcount(h));
	BUN f = BUNfirst(h);
	oid *o = (oid *) Hloc(h, f), oo = *o;
	ptr v = (ptr) BUNtail(hi, f), ov = v;
	int tpe = h->ttype;

	for (f = BUNfirst(h); f < BUNlast(h); f = BUNfirst(h)) {
		o = (oid *) Hloc(h, f);
		v = (ptr) BUNtail(hi, f);
		if (atom_CMP(ov, v, tpe) != 0) {
			oo = *o;
			ov = v;
		}
		BUNins(n, o, &oo, FALSE);
		pqueue_dequeue_anymin(h);
	}
	r = BATrevert(n);
	return r;
}

static int
pqueue_topn_voidanymin(BAT **H, BAT *t, wrd *N)
{
	BAT *h = NULL;
	BATiter ti = bat_iterator(t);
	BUN i, n = BATcount(t);
	oid idx = t->hseqbase;
	BUN p = BUNfirst(t);
	int tpe = BATttype(t);

	if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX && (BUN) *N < n)
		n = (BUN) *N;
	if (do_pqueue_init(&h, t, n) == GDK_FAIL)
		return GDK_FAIL;

	for (i = 0; i < n; i++, idx++, p++) {
		pqueue_enqueue_anymin(h, &idx, BUNtail(ti, p), tpe);
	}
	n = BATcount(t);
	for (; i < n; i++, idx++, p++) {
		pqueue_topreplace_anymin(h, &idx, BUNtail(ti, p), tpe);
	}
	*H = heap2bat_anymin(h);
	BBPunfix(h->batCacheid);
	return GDK_SUCCEED;
}

static int
pqueue_topn_anymin(BAT **H, BAT *t, wrd *N)
{
	BAT *h = NULL;
	BATiter ti = bat_iterator(t);
	BUN i, n = BATcount(t);
	BUN p = BUNfirst(t);
	int tpe = BATttype(t);

	if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX && (BUN) *N < n)
		n = (BUN) *N;
	if (do_pqueue_init(&h, t, n) == GDK_FAIL)
		return GDK_FAIL;

	for (i = 0; i < n; i++, p++) {
		pqueue_enqueue_anymin(h, (oid *) BUNhloc(ti, p), BUNtail(ti, p), tpe);
	}
	n = BATcount(t);
	for (; i < n; i++, p++) {
		pqueue_topreplace_anymin(h, (oid *) BUNhloc(ti, p),
								 BUNtail(ti, p), tpe);
	}
	*H = heap2bat_anymin(h);
	BBPunfix(h->batCacheid);
	return GDK_SUCCEED;
}

static int
pqueue_utopn_voidanymin(BAT **H, BAT *t, wrd *N)
{
	BAT *duplicates = NULL, *b;
	BATiter hi, ti = bat_iterator(t);
	BUN i, j, n, cnt = BATcount(t), p;
	BUN q = BUNfirst(t);
	ptr v;
	oid idx = t->hseqbase;
	int tpe = BATttype(t);

	n = cnt;
	if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX)
		n = (BUN) *N;
	if (do_pqueue_init(H, t, n) == GDK_FAIL)
		return GDK_FAIL;
	duplicates = BATnew(TYPE_oid, TYPE_oid, n);
	if (duplicates == NULL) {
		BBPunfix((*H)->batCacheid);
		return GDK_FAIL;
	}
	hi = bat_iterator(*H);

	for (i = 0, j = 0; j < cnt && i < n; j++, idx++, q++) {
		v = BUNtail(ti, q);
		if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {
			pqueue_enqueue_anymin(*H, &idx, v, tpe);
			HASHdestroy(*H);
			BUNins(duplicates, &idx, &idx, FALSE);
			i++;
		} else {
			BUNins(duplicates, Hloc(*H, p), &idx, FALSE);
		}
	}
	for (; j < cnt; j++, idx++, q++) {
		v = BUNtail(ti, q);
		if (atom_CMP(BUNtail(hi, BUNfirst(*H)), v, tpe) <= 0) {
			if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {
				oid o_idx = *(oid *) Hloc(*H, BUNfirst(*H));
				BUNdelHead(duplicates, &o_idx, TRUE);
				pqueue_topreplace_anymin(*H, &idx, v, tpe);
				HASHdestroy(*H);
				BUNins(duplicates, &idx, &idx, FALSE);
			} else {
				BUNins(duplicates, Hloc(*H, p), &idx, FALSE);
			}
		}
	}
	b = heap2bat_anymin(*H);
	BBPunfix((*H)->batCacheid);
	*H = b;
	b = VIEWcombine(*H);
	BBPunfix((*H)->batCacheid);
	*H = b;
	b = BATleftjoin(*H, duplicates, BATcount(duplicates));
	BBPunfix((*H)->batCacheid);
	BBPunfix(duplicates->batCacheid);
	*H = BATmirror(b);
	return GDK_SUCCEED;
}

static int
pqueue_utopn_anymin(BAT **H, BAT *t, wrd *N)
{
	BAT *duplicates = NULL, *b;
	BATiter hi, ti = bat_iterator(t);
	BUN i, j, n, cnt = BATcount(t), p;
	BUN q = BUNfirst(t);
	oid *idx;
	ptr v;
	int tpe = BATttype(t);

	n = cnt;
	if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX)
		n = (BUN) *N;
	if (do_pqueue_init(H, t, n) == GDK_FAIL)
		return GDK_FAIL;
	duplicates = BATnew(TYPE_oid, TYPE_oid, n);
	if (duplicates == NULL) {
		BBPunfix((*H)->batCacheid);
		return GDK_FAIL;
	}
	hi = bat_iterator(*H);

	idx = (oid *) BUNhead(ti, BUNfirst(t));
	for (i = 0, j = 0; j < cnt && i < n; j++, idx++, q++) {
		v = BUNtail(ti, q);
		if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {
			pqueue_enqueue_anymin(*H, idx, v, tpe);
			HASHdestroy(*H);
			BUNins(duplicates, idx, idx, FALSE);
			i++;
		} else {
			BUNins(duplicates, Hloc(*H, p), idx, FALSE);
		}
	}
	for (; j < cnt; j++, idx++, q++) {
		v = BUNtail(ti, q);
		if (atom_CMP(BUNtail(hi, BUNfirst(*H)), v, tpe) <= 0) {
			if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {
				oid o_idx = *(oid *) Hloc(*H, BUNfirst(*H));
				BUNdelHead(duplicates, &o_idx, TRUE);
				pqueue_topreplace_anymin(*H, idx, v, tpe);
				HASHdestroy(*H);
				BUNins(duplicates, idx, idx, FALSE);
			} else {
				BUNins(duplicates, Hloc(*H, p), idx, FALSE);
			}
		}
	}
	b = heap2bat_anymin(*H);
	BBPunfix((*H)->batCacheid);
	*H = b;
	b = VIEWcombine(*H);
	BBPunfix((*H)->batCacheid);
	*H = b;
	b = BATleftjoin(*H, duplicates, BATcount(duplicates));
	BBPunfix((*H)->batCacheid);
	BBPunfix(duplicates->batCacheid);
	*H = BATmirror(b);
	return GDK_SUCCEED;
}

/*enqueue an element*/
static int
pqueue_enqueue_anymax(BAT *h, oid *idx, ptr el, int tpe)
{
	BATiter hi = bat_iterator(h);
	BUN hbase;
	BUN ins, cur;
	BUN p, posel;
	unsigned short ts;

	hbase = BUNfirst(h);

	posel = BATcount(h);		/*last position */
	BUNins(h, (ptr) idx, (ptr) el, FALSE);
	ts = Tsize(h);
	ins = hbase + posel;

	while (posel > 0) {
		p = parent(posel);
		cur = hbase + p;
		if (atom_CMP(BUNtail(hi, ins), BUNtail(hi, cur), tpe) > 0) {
			/* swap element with its parent */
			any_swap(cur, ins, ts);
			ins = cur;
			posel = parent(posel);
		} else
			break;
	}
	h->hsorted = h->tsorted = FALSE;
	h->hrevsorted = h->trevsorted = FALSE;

	return GDK_SUCCEED;
}

/* moves down the root element */
/* used by dequeue (see below) */
static int
pqueue_movedowntop_anymax(BAT *h)
{
	BATiter hi = bat_iterator(h);
	BUN hbase;
	BUN swp, cur;
	int tpe = BATttype(h);
	BUN swap, num_elems;
	BUN posel;
	unsigned short ts = Tsize(h);

	hbase = BUNfirst(h);

	cur = hbase;
	num_elems = BATcount(h);
	posel = 0;

	/*while posel is not a leaf and pqueue[posel].tail > any of childen */
	while (posel * 2 + 1 < num_elems) {	/*there exists a left son */
		if (posel * 2 + 2 < num_elems) {	/*there exists a right son */
			if (atom_CMP(BUNtail(hi, hbase + (posel * 2 + 1)),
						 BUNtail(hi, hbase + (posel * 2 + 2)), tpe) > 0) {
				swap = posel * 2 + 1;
			} else {
				swap = posel * 2 + 2;
			}
		} else
			swap = posel * 2 + 1;

		swp = hbase + swap;

		if (atom_CMP(BUNtail(hi, swp), BUNtail(hi, cur), tpe) > 0) {
			/*swap elements */
			any_swap(cur, swp, ts);
			cur = swp;
			posel = swap;
		} else
			break;
	}
	h->hsorted = h->tsorted = FALSE;
	h->hrevsorted = h->trevsorted = FALSE;

	return GDK_SUCCEED;
}

/* removes the root element, puts the last element as root and moves it down */
static int
pqueue_dequeue_anymax(BAT *h)
{
	BATiter hi = bat_iterator(h);
	BUN hbase;
	BUN num_elements;
	unsigned short ts = Tsize(h);

	if (!(num_elements = BATcount(h))) {
		/* pqueue_dequeue: Cannot dequeue from empty queue */
		return GDK_FAIL;
	}

	hbase = BUNfirst(h);

	/* copy last element to the first position */
	any_swap(hbase, hbase + (num_elements - 1), ts);

	/*delete last element */
	BUNdelete(h, hbase + (num_elements - 1), FALSE);

	pqueue_movedowntop_anymax(h);
	return GDK_SUCCEED;
}

/* replaces the top element with the input if it is larger (smaller) and
 * updates the heap */
static int
pqueue_topreplace_anymax(BAT *h, oid *idx, ptr el, int tpe)
{
	BATiter hi = bat_iterator(h);
	BUN hbase = BUNfirst(h);

	if (atom_CMP(BUNtail(hi, hbase), el, tpe) > 0) {
		BUNinplace(h, hbase, idx, el, 0);
		*(oid *) BUNhloc(hi, hbase) = *idx;
		pqueue_movedowntop_anymax(h);
		h->hsorted = h->tsorted = FALSE;
		h->hrevsorted = h->trevsorted = FALSE;
	}

	return GDK_SUCCEED;
}

static BAT *
heap2bat_anymax(BAT *h)
{
	BATiter hi = bat_iterator(h);
	BAT *r, *n = BATnew(TYPE_oid, TYPE_oid, BATcount(h));
	BUN f = BUNfirst(h);
	oid *o = (oid *) Hloc(h, f), oo = *o;
	ptr v = (ptr) BUNtail(hi, f), ov = v;
	int tpe = h->ttype;

	for (f = BUNfirst(h); f < BUNlast(h); f = BUNfirst(h)) {
		o = (oid *) Hloc(h, f);
		v = (ptr) BUNtail(hi, f);
		if (atom_CMP(ov, v, tpe) != 0) {
			oo = *o;
			ov = v;
		}
		BUNins(n, o, &oo, FALSE);
		pqueue_dequeue_anymax(h);
	}
	r = BATrevert(n);
	return r;
}

static int
pqueue_topn_voidanymax(BAT **H, BAT *t, wrd *N)
{
	BAT *h = NULL;
	BATiter ti = bat_iterator(t);
	BUN i, n = BATcount(t);
	oid idx = t->hseqbase;
	BUN p = BUNfirst(t);
	int tpe = BATttype(t);

	if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX && (BUN) *N < n)
		n = (BUN) *N;
	if (do_pqueue_init(&h, t, n) == GDK_FAIL)
		return GDK_FAIL;

	for (i = 0; i < n; i++, idx++, p++) {
		pqueue_enqueue_anymax(h, &idx, BUNtail(ti, p), tpe);
	}
	n = BATcount(t);
	for (; i < n; i++, idx++, p++) {
		pqueue_topreplace_anymax(h, &idx, BUNtail(ti, p), tpe);
	}
	*H = heap2bat_anymax(h);
	BBPunfix(h->batCacheid);
	return GDK_SUCCEED;
}

static int
pqueue_topn_anymax(BAT **H, BAT *t, wrd *N)
{
	BAT *h = NULL;
	BATiter ti = bat_iterator(t);
	BUN i, n = BATcount(t);
	BUN p = BUNfirst(t);
	int tpe = BATttype(t);

	if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX && (BUN) *N < n)
		n = (BUN) *N;
	if (do_pqueue_init(&h, t, n) == GDK_FAIL)
		return GDK_FAIL;

	for (i = 0; i < n; i++, p++) {
		pqueue_enqueue_anymax(h, (oid *) BUNhloc(ti, p), BUNtail(ti, p), tpe);
	}
	n = BATcount(t);
	for (; i < n; i++, p++) {
		pqueue_topreplace_anymax(h, (oid *) BUNhloc(ti, p),
								 BUNtail(ti, p), tpe);
	}
	*H = heap2bat_anymax(h);
	BBPunfix(h->batCacheid);
	return GDK_SUCCEED;
}

static int
pqueue_utopn_voidanymax(BAT **H, BAT *t, wrd *N)
{
	BAT *duplicates = NULL, *b;
	BATiter hi, ti = bat_iterator(t);
	BUN i, j, n, cnt = BATcount(t), p;
	BUN q = BUNfirst(t);
	ptr v;
	oid idx = t->hseqbase;
	int tpe = BATttype(t);

	n = cnt;
	if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX)
		n = (BUN) *N;
	if (do_pqueue_init(H, t, n) == GDK_FAIL)
		return GDK_FAIL;
	duplicates = BATnew(TYPE_oid, TYPE_oid, n);
	if (duplicates == NULL) {
		BBPunfix((*H)->batCacheid);
		return GDK_FAIL;
	}
	hi = bat_iterator(*H);

	for (i = 0, j = 0; j < cnt && i < n; j++, idx++, q++) {
		v = BUNtail(ti, q);
		if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {
			pqueue_enqueue_anymax(*H, &idx, v, tpe);
			HASHdestroy(*H);
			BUNins(duplicates, &idx, &idx, FALSE);
			i++;
		} else {
			BUNins(duplicates, Hloc(*H, p), &idx, FALSE);
		}
	}
	for (; j < cnt; j++, idx++, q++) {
		v = BUNtail(ti, q);
		if (atom_CMP(BUNtail(hi, BUNfirst(*H)), v, tpe) >= 0) {
			if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {
				oid o_idx = *(oid *) Hloc(*H, BUNfirst(*H));
				BUNdelHead(duplicates, &o_idx, TRUE);
				pqueue_topreplace_anymax(*H, &idx, v, tpe);
				HASHdestroy(*H);
				BUNins(duplicates, &idx, &idx, FALSE);
			} else {
				BUNins(duplicates, Hloc(*H, p), &idx, FALSE);
			}
		}
	}
	b = heap2bat_anymax(*H);
	BBPunfix((*H)->batCacheid);
	*H = b;
	b = VIEWcombine(*H);
	BBPunfix((*H)->batCacheid);
	*H = b;
	b = BATleftjoin(*H, duplicates, BATcount(duplicates));
	BBPunfix((*H)->batCacheid);
	BBPunfix(duplicates->batCacheid);
	*H = BATmirror(b);
	return GDK_SUCCEED;
}

static int
pqueue_utopn_anymax(BAT **H, BAT *t, wrd *N)
{
	BAT *duplicates = NULL, *b;
	BATiter hi, ti = bat_iterator(t);
	BUN i, j, n, cnt = BATcount(t), p;
	BUN q = BUNfirst(t);
	oid *idx;
	ptr v;
	int tpe = BATttype(t);

	n = cnt;
	if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX)
		n = (BUN) *N;
	if (do_pqueue_init(H, t, n) == GDK_FAIL)
		return GDK_FAIL;
	duplicates = BATnew(TYPE_oid, TYPE_oid, n);
	if (duplicates == NULL) {
		BBPunfix((*H)->batCacheid);
		return GDK_FAIL;
	}
	hi = bat_iterator(*H);

	idx = (oid *) BUNhead(ti, BUNfirst(t));
	for (i = 0, j = 0; j < cnt && i < n; j++, idx++, q++) {
		v = BUNtail(ti, q);
		if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {
			pqueue_enqueue_anymax(*H, idx, v, tpe);
			HASHdestroy(*H);
			BUNins(duplicates, idx, idx, FALSE);
			i++;
		} else {
			BUNins(duplicates, Hloc(*H, p), idx, FALSE);
		}
	}
	for (; j < cnt; j++, idx++, q++) {
		v = BUNtail(ti, q);
		if (atom_CMP(BUNtail(hi, BUNfirst(*H)), v, tpe) >= 0) {
			if ((p = BUNfnd(BATmirror(*H), v)) == BUN_NONE) {
				oid o_idx = *(oid *) Hloc(*H, BUNfirst(*H));
				BUNdelHead(duplicates, &o_idx, TRUE);
				pqueue_topreplace_anymax(*H, idx, v, tpe);
				HASHdestroy(*H);
				BUNins(duplicates, idx, idx, FALSE);
			} else {
				BUNins(duplicates, Hloc(*H, p), idx, FALSE);
			}
		}
	}
	b = heap2bat_anymax(*H);
	BBPunfix((*H)->batCacheid);
	*H = b;
	b = VIEWcombine(*H);
	BBPunfix((*H)->batCacheid);
	*H = b;
	b = BATleftjoin(*H, duplicates, BATcount(duplicates));
	BBPunfix((*H)->batCacheid);
	BBPunfix(duplicates->batCacheid);
	*H = BATmirror(b);
	return GDK_SUCCEED;
}

str
PQinit(int *ret, int *bid, wrd *maxsize)
{
	BAT *b, *bn;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "pqueue.init", RUNTIME_OBJECT_MISSING);
	pqueue_init(&bn, b, maxsize);
	BBPreleaseref(b->batCacheid);
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static void
PQtopn_sorted_min( BAT **bn, BAT *b, wrd _N )
{
	BUN cnt = BATcount(b), N = (BUN) _N;
	assert(_N >= 0);
	if (b->tsorted) {
		b = BATslice(b, N>=cnt?0:cnt-N, cnt);
		*bn = BATsort_rev(b);
		BBPreleaseref(b->batCacheid);	
	} else 
		*bn = BATslice(b, 0, N>=cnt?cnt:N);
}

static void
PQtopn_sorted_max( BAT **bn, BAT *b, wrd _N )
{
	BUN cnt = BATcount(b), N = (BUN) _N;
	assert(_N >= 0);
	if (b->tsorted) 
		*bn = BATslice(b, 0, N>=cnt?cnt:N);
	else {
		b = BATslice(b, N>=cnt?0:cnt-N, cnt);
		*bn = BATsort_rev(b);
		BBPreleaseref(b->batCacheid);	
	}
}

#define PQimpl1a(X,Y)												\
	str																\
	PQenqueue_##X##Y(int *ret, int *bid, oid *idx, X *el){			\
		BAT *b;														\
		if( (b= BATdescriptor(*bid)) == NULL)						\
			throw(MAL, "pqueue.enqueue", RUNTIME_OBJECT_MISSING);	\
		pqueue_enqueue_##X##Y(b,idx,el);							\
		(void) ret;													\
		return MAL_SUCCEED;											\
	}																\

#define PQimpl1b(X,Y)											\
	str															\
	PQtopreplace_##X##Y(int *ret, int *bid, oid *idx, X *el)	\
	{															\
		BAT *b;													\
		if( (b= BATdescriptor(*bid)) == NULL)					\
			throw(MAL, "pqueue.init", RUNTIME_OBJECT_MISSING);	\
		pqueue_topreplace_##X##Y(b,idx,el);						\
		(void) ret;												\
		return MAL_SUCCEED;										\
	}

#define PQimpl2(TYPE,K)													\
	str																	\
	PQmovedowntop_##TYPE##K(int *ret, int *bid)							\
	{																	\
		BAT *b;															\
		if( (b= BATdescriptor(*bid)) == NULL)							\
			throw(MAL, "pqueue.movedowntop", RUNTIME_OBJECT_MISSING);	\
		pqueue_movedowntop_##TYPE##K(b);								\
		(void) ret;														\
		return MAL_SUCCEED;												\
	}																	\
																		\
	str																	\
	PQdequeue_##TYPE##K(int *ret, int *bid)								\
	{																	\
		BAT *b;															\
		if( (b= BATdescriptor(*bid)) == NULL)							\
			throw(MAL, "pqueue.init", RUNTIME_OBJECT_MISSING);			\
		if( pqueue_dequeue_##TYPE##K(b) == GDK_FAIL)					\
			throw(MAL, "pqueue.dequeue",OPERATION_FAILED "Cannot dequeue from empty queue"); \
		(void) ret;														\
		return MAL_SUCCEED;												\
	}																	\
																		\
	str																	\
	PQtopn_##TYPE##K(int *ret, int *bid, wrd *N)						\
	{																	\
		BAT *b,*bn = NULL;												\
		if( (b= BATdescriptor(*bid)) == NULL)							\
			throw(MAL, "pqueue.topN", RUNTIME_OBJECT_MISSING);			\
		if (b->tsorted || b->trevsorted) { 	\
			PQtopn_sorted_##K(&bn, b, *N);	\
			if (bn) { 			\
				*ret= bn->batCacheid;		\
				BBPkeepref(*ret);		\
				BBPreleaseref(b->batCacheid);	\
				return MAL_SUCCEED;		\
			}				\
		} else					\
		if ((b->htype == TYPE_void ? pqueue_topn_void##TYPE##K(&bn,b,N) : pqueue_topn_##TYPE##K(&bn,b,N)) == GDK_SUCCEED && bn) { \
			*ret= bn->batCacheid;										\
			BBPkeepref(*ret);											\
			BBPreleaseref(b->batCacheid);								\
			return MAL_SUCCEED;											\
		}																\
		BBPreleaseref(b->batCacheid);									\
		throw(MAL, "pqueue.topN", MAL_MALLOC_FAIL);						\
	}																	\
	str																	\
	PQutopn_##TYPE##K(int *ret, int *bid, wrd *N)						\
	{																	\
		BAT *b,*bn = NULL;												\
		if( (b= BATdescriptor(*bid)) == NULL)							\
			throw(MAL, "pqueue.topN", RUNTIME_OBJECT_MISSING);			\
		if ((b->htype == TYPE_void ? pqueue_utopn_void##TYPE##K(&bn,b,N) : pqueue_utopn_##TYPE##K(&bn,b,N)) == GDK_SUCCEED && bn) {	\
			*ret= bn->batCacheid;										\
			BBPkeepref(*ret);											\
			BBPreleaseref(b->batCacheid);								\
			return MAL_SUCCEED;											\
		}																\
		BBPreleaseref(b->batCacheid);									\
		throw(MAL, "pqueue.topN", MAL_MALLOC_FAIL);						\
	}																	\
	str																	\
	PQtopn2_##TYPE##K(int *ret, int *aid, int *bid, wrd *N)				\
	{																	\
		BUN n, i,j, cnt;												\
		BAT *a, *b,*bn = NULL;											\
		if ((a=BATdescriptor(*aid)) == NULL || (b=BATdescriptor(*bid)) == NULL)	\
			throw(MAL, "pqueue.topN", RUNTIME_OBJECT_MISSING);			\
																		\
		cnt = n = BATcount(a);											\
		if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX && (BUN) *N < n) \
			n = (BUN) *N;												\
		bn = BATnew(TYPE_oid, TYPE_oid, n);								\
		for(i=0; i<n; ) {												\
			oid ov = * (oid *) Tloc(a, i);								\
			for (j = i; j < cnt && * (oid *) Tloc(a, j) == ov; j++)		\
				;														\
			if (j == i+1) {												\
				BUNins(bn, Hloc(a,i), &ov, FALSE);						\
			} else {													\
				BAT *s = BATslice(b, i, j), *sbn = NULL;				\
				wrd nn = n-i;											\
																		\
				if ((b->htype == TYPE_void ? pqueue_topn_void##TYPE##K(&sbn,s,&nn) : pqueue_topn_##TYPE##K(&sbn,s,&nn)) == GDK_SUCCEED && sbn) { \
					BATins(bn, sbn, FALSE);								\
					BBPunfix(sbn->batCacheid);							\
					BBPunfix(s->batCacheid);							\
				}														\
			}															\
			i = j;														\
		}																\
		if (bn) {														\
			*ret= bn->batCacheid;										\
			BBPkeepref(*ret);											\
			BBPreleaseref(b->batCacheid);								\
			BBPreleaseref(a->batCacheid);								\
			return MAL_SUCCEED;											\
		}																\
		BBPreleaseref(b->batCacheid);									\
		BBPreleaseref(a->batCacheid);									\
		throw(MAL, "pqueue.topN", MAL_MALLOC_FAIL);						\
	}																	\
																		\
	str																	\
	PQutopn2_##TYPE##K(int *ret, int *aid, int *bid, wrd *N)			\
	{																	\
		BUN n, i,j, cnt;												\
		BAT *a, *b,*bn = NULL;											\
																		\
		if ((a=BATdescriptor(*aid)) == NULL || (b=BATdescriptor(*bid)) == NULL)	\
			throw(MAL, "pqueue.topN", RUNTIME_OBJECT_MISSING);			\
		cnt = n = BATcount(a);											\
		if (*N != wrd_nil && *N >= 0 && *N <= (wrd) BUN_MAX && (BUN) *N < n) \
			n = (BUN) *N;												\
		bn = BATnew(TYPE_oid, TYPE_oid, n);								\
		for(i=0; i<n; ) {												\
			oid ov = * (oid *) Tloc(a, i);								\
			for (j = i; j < cnt && * (oid *) Tloc(a, j) == ov; j++)		\
				;														\
			if (j == i+1) {												\
				BUNins(bn, Hloc(a,i), &ov, FALSE);						\
			} else {													\
				BAT *s = BATslice(b, i, j), *sbn = NULL;				\
				wrd nn = n-i;											\
																		\
				if ((b->htype == TYPE_void ? pqueue_utopn_void##TYPE##K(&sbn,s,&nn) : pqueue_utopn_##TYPE##K(&sbn,s,&nn)) == GDK_SUCCEED && sbn) { \
					BATins(bn, sbn, FALSE);								\
					BBPunfix(sbn->batCacheid);							\
					BBPunfix(s->batCacheid);							\
				}														\
			}															\
			i = j;														\
		}																\
		if (bn) {														\
			*ret= bn->batCacheid;										\
			BBPkeepref(*ret);											\
			BBPreleaseref(b->batCacheid);								\
			BBPreleaseref(a->batCacheid);								\
			return MAL_SUCCEED;											\
		}																\
		BBPreleaseref(b->batCacheid);									\
		BBPreleaseref(a->batCacheid);									\
		throw(MAL, "pqueue.utopN", MAL_MALLOC_FAIL);					\
	}

#define PQminmax1(TYPE)							\
	PQimpl1a(TYPE,min)							\
	PQimpl1b(TYPE,min)							\
	PQimpl1a(TYPE,max)							\
	PQimpl1b(TYPE,max)

#define PQminmax2(TYPE)							\
	PQimpl2(TYPE,min)							\
	PQimpl2(TYPE,max)

#define PQminmax(TYPE)							\
 	PQminmax1(TYPE)								\
 	PQminmax2(TYPE)


PQminmax(bte)
	PQminmax(sht)
PQminmax(int)
PQminmax(oid)
PQminmax(wrd)
PQminmax(lng)
PQminmax(flt)
PQminmax(dbl)
PQimpl2(any, min)
PQimpl2(any, max)

/* int PQenqueue_anymin(BAT *h, oid *idx, ptr el, int tpe)*/
str
PQenqueue_anymin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int tpe;
	BAT *h;
	oid *idx;
	ptr el;
	(void) cntxt;

	if (p->argc != 4 || !isaBatType(getArgType(mb, p, 1)) || getArgType(mb, p, 2) != TYPE_oid)
		throw(MAL, "enqueue_min", SEMANTIC_TYPE_MISMATCH);
	tpe = getArgType(mb, p, 3);

	h = BATdescriptor(*(bat *) getArgReference(stk, p, 1));
	if (!h)
		throw(MAL, "enqueue_min", RUNTIME_OBJECT_MISSING);
	idx = (oid *) getArgReference(stk, p, 2);
	el = (ptr) getArgReference(stk, p, 3);

	pqueue_enqueue_anymin(h, idx, el, tpe);
	return MAL_SUCCEED;
}

/* int pqueue_topreplace_anymin(BAT *h, oid *idx, ptr el, int tpe) */
str
PQtopreplace_anymin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int tpe;
	BAT *h;
	oid *idx;
	ptr el;

	(void) cntxt;

	if (p->argc != 4 || !isaBatType(getArgType(mb, p, 1)) || getArgType(mb, p, 2) != TYPE_oid)
		throw(MAL, "topreplace_min", SEMANTIC_TYPE_MISMATCH);
	tpe = getArgType(mb, p, 3);

	h = BATdescriptor(*(bat *) getArgReference(stk, p, 1));
	if (!h)
		throw(MAL, "topreplace_min", RUNTIME_OBJECT_MISSING);
	idx = (oid *) getArgReference(stk, p, 2);
	el = (ptr) getArgReference(stk, p, 3);

	pqueue_topreplace_anymin(h, idx, el, tpe);
	return MAL_SUCCEED;
}

/* int PQenqueue_anymax(BAT *h, oid *idx, ptr el, int tpe)*/
str
PQenqueue_anymax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int tpe;
	BAT *h;
	oid *idx;
	ptr el;
	(void) cntxt;

	if (p->argc != 4 || !isaBatType(getArgType(mb, p, 1)) || getArgType(mb, p, 2) != TYPE_oid)
		throw(MAL, "enqueue_max", SEMANTIC_TYPE_MISMATCH);
	tpe = getArgType(mb, p, 3);

	h = BATdescriptor(*(bat *) getArgReference(stk, p, 1));
	if (!h)
		throw(MAL, "enqueue_max", RUNTIME_OBJECT_MISSING);
	idx = (oid *) getArgReference(stk, p, 2);
	el = (ptr) getArgReference(stk, p, 3);

	pqueue_enqueue_anymax(h, idx, el, tpe);
	return MAL_SUCCEED;
}

/* int pqueue_topreplace_anymax(BAT *h, oid *idx, ptr el, int tpe) */
str
PQtopreplace_anymax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int tpe;
	BAT *h;
	oid *idx;
	ptr el;

	(void) cntxt;

	if (p->argc != 4 || !isaBatType(getArgType(mb, p, 1)) || getArgType(mb, p, 2) != TYPE_oid)
		throw(MAL, "topreplace_max", SEMANTIC_TYPE_MISMATCH);
	tpe = getArgType(mb, p, 3);

	h = BATdescriptor(*(bat *) getArgReference(stk, p, 1));
	if (!h)
		throw(MAL, "topreplace_max", RUNTIME_OBJECT_MISSING);
	idx = (oid *) getArgReference(stk, p, 2);
	el = (ptr) getArgReference(stk, p, 3);

	pqueue_topreplace_anymax(h, idx, el, tpe);
	return MAL_SUCCEED;
}
