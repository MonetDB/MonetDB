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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

/* BATfirstn select the smallest n elements from the input bat b (if
 * asc(ending) is set, else the largest n elements).  Conceptually, b
 * is sorted in ascending or descending order (depending on the asc
 * argument) and then the OIDs of the first n elements are returned.
 *
 * In addition to the input BAT b, there can be a standard candidate
 * list s.  If s is specified (non-NULL), only elements in b that are
 * referred to in s are considered.
 *
 * If the third input bat g is non-NULL, then s must also be non-NULL.
 * G then specifies groups to which the elements referred to in s
 * belong (g must be aligned with s).  Conceptually, the group values
 * are sorted in ascending order together with the elements in b that
 * are referred to in s (in ascending or descending order depending on
 * asc), and the first n elements are then returned.
 *
 * If the output argument gids is NULL, only n elements are returned.
 * If the output argument gids is non-NULL, more than n elements may
 * be returned.  If there are duplicate values (if g is given, the
 * group value counts in determining duplication), all duplicates are
 * returned.
 *
 * Note that BATfirstn can be called in cascading fashion to calculate
 * the first n values of a table of multiple columns:
 *      BATfirstn(&s1, &g1, b1, NULL, NULL, n, asc, distinct);
 *      BATfirstn(&s2, &g2, b2, s1, g1, n, asc, distinct);
 *      BATfirstn(&s3, NULL, b3, s2, g2, n, asc, distinct);
 * If the input BATs b1, b2, b3 are large enough, s3 will contain the
 * OIDs of the smallest (largest) n elements in the table consisting
 * of the columns b1, b2, b3 when ordered in ascending order with b1
 * the major key.
 */

/* We use a binary heap for the implementation of the simplest form of
 * first-N.  During processing, the oids list forms a heap with the
 * root at position 0 and the children of a node at position n at
 * positions 2*n+1 and 2*n+2.  The parent node is always
 * smaller/larger (depending on the value of asc) than its children
 * (recursively).  The heapify macro creates the heap from the input
 * in-place.  We start off with a heap containing the first N elements
 * of the input, and then go over the rest of the input, replacing the
 * root of the heap with a new value if appropriate (if the new value
 * is among the first-N seen so far).  The siftup macro then restores
 * the heap property. */
#define siftup(OPER, START, GET)					\
	do {								\
	    pos = (START);						\
		childpos = (pos << 1) + 1;				\
		while (childpos < n) {					\
			/* find most extreme child */			\
			if (childpos + 1 < n &&				\
			    !(OPER(GET(oids[childpos + 1]),		\
				   GET(oids[childpos]))))		\
				childpos++;				\
			/* compare parent with most extreme child */	\
			if (OPER(GET(oids[pos]), GET(oids[childpos]))) { \
				/* exchange parent with child and */	\
				/* sift child further */		\
				item = oids[pos];			\
				oids[pos] = oids[childpos];		\
				oids[childpos] = item;			\
				pos = childpos;				\
				childpos = (pos << 1) + 1;		\
				continue;				\
			}						\
			break;						\
		}							\
	} while (0)

#define heapify(OPER, GET)				\
	do {						\
		for (i = n / 2; i > 0; i--)		\
			siftup(OPER, i - 1, GET);	\
	} while (0)

#define GETVAL(o)	vals[(o) - b->hseqbase]
#define GETVALany(o)	BUNtail(bi, (o) - b->hseqbase + BUNfirst(b))
#define LTany(a, b)	(cmp(a, b) < 0)
#define GTany(a, b)	(cmp(a, b) > 0)

#define shuffle_unique(TYPE, OPER, GET)					\
	do {								\
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b));	\
		heapify(OPER, GET);					\
		while (cand ? cand < candend : start < end) {		\
			i = cand ? *cand++ : start++ + b->hseqbase;	\
			if (OPER(GET(i), GET(oids[0]))) {		\
				oids[0] = i;				\
				siftup(OPER, 0, GET);			\
			}						\
		}							\
	} while (0)

/* This version of BATfirstn returns a list of N oids (where N is the
 * smallest among BATcount(b), BATcount(s), and n).  The oids returned
 * refer to the N smallest/largest (depending on asc) tail values of b
 * (taking the optional candidate list s into account).  If there are
 * multiple equal values to take us past N, we return a subset of those.
 */
static BAT *
BATfirstn_unique(BAT *b, BAT *s, BUN n, int asc)
{
	BAT *bn;
	BATiter bi = bat_iterator(b);
	oid *oids;
	BUN i, cnt, start, end;
	const oid *cand, *candend;
	int tpe = b->ttype;
	int (*cmp)(const void *, const void *);
	/* variables used in heapify/siftup macros */
	oid item;
	BUN pos, childpos;

	CANDINIT(b, s, start, end, cnt, cand, candend);

	if (cand) {
		if (n >= (BUN) (candend - cand)) {
			/* trivial: return the candidate list (the
			 * part that refers to b, that is) */
			return BATslice(s,
					(BUN) (cand - (const oid *) Tloc(s, 0)),
					(BUN) (candend - (const oid *) Tloc(s, 0)));
		}
	} else if (n >= cnt) {
		/* trivial: return everything */
		bn = BATnew(TYPE_void, TYPE_void, cnt, TRANSIENT);
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, cnt);
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), start + b->hseqbase);
		return bn;
	}
	if (b->tsorted || b->trevsorted) {
		/* trivial: b is sorted so we just need to return the
		 * initial or final part of it (or of the candidate
		 * list) */
		if (cand) {
			if (asc ? b->tsorted : b->trevsorted) {
				/* return copy of first relevant part
				 * of candidate list */
				i = (BUN) (cand - (const oid *) Tloc(s, 0));
				return BATslice(s, i, i + n);
			}
			/* return copy of last relevant part of
			 * candidate list */
			i = (BUN) (candend - (const oid *) Tloc(s, 0));
			return BATslice(s, i - n, i);
		}
		bn = BATnew(TYPE_void, TYPE_void, n, TRANSIENT);
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, n);
		BATseqbase(bn, 0);
		if (asc ? b->tsorted : b->trevsorted) {
			/* first n entries from b */
			BATseqbase(BATmirror(bn), start + b->hseqbase);
		} else {
			/* last n entries from b */
			BATseqbase(BATmirror(bn), start + cnt + b->hseqbase - n);
		}
		return bn;
	}

	assert(b->ttype != TYPE_void); /* tsorted above took care of this */

	bn = BATnew(TYPE_void, TYPE_oid, n, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, n);
	BATseqbase(bn, 0);
	oids = (oid *) Tloc(bn, BUNfirst(bn));
	cmp = BATatoms[b->ttype].atomCmp;
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	if (tpe != ATOMstorage(tpe) &&
	    cmp == BATatoms[ATOMstorage(b->ttype)].atomCmp) {
		/* note, this takes care of types oid and wrd */
		tpe = ATOMstorage(tpe);
	}
	if (cand) {
		for (i = 0; i < n; i++)
			oids[i] = *cand++;
	} else {
		for (i = 0; i < n; i++)
			oids[i] = start++ + b->hseqbase;
	}
	if (asc) {
		switch (tpe) {
		case TYPE_bte:
			shuffle_unique(bte, LT, GETVAL);
			break;
		case TYPE_sht:
			shuffle_unique(sht, LT, GETVAL);
			break;
		case TYPE_int:
			shuffle_unique(int, LT, GETVAL);
			break;
		case TYPE_lng:
			shuffle_unique(lng, LT, GETVAL);
			break;
		case TYPE_flt:
			shuffle_unique(flt, LT, GETVAL);
			break;
		case TYPE_dbl:
			shuffle_unique(dbl, LT, GETVAL);
			break;
		default:
			heapify(LTany, GETVALany);
			while (cand ? cand < candend : start < end) {
				i = cand ? *cand++ : start++ + b->hseqbase;
				if (LTany(GETVALany(i), GETVALany(oids[0]))) {
					oids[0] = i;
					siftup(LTany, 0, GETVALany);
				}
			}
			break;
		}
	} else {
		switch (tpe) {
		case TYPE_bte:
			shuffle_unique(bte, GT, GETVAL);
			break;
		case TYPE_sht:
			shuffle_unique(sht, GT, GETVAL);
			break;
		case TYPE_int:
			shuffle_unique(int, GT, GETVAL);
			break;
		case TYPE_lng:
			shuffle_unique(lng, GT, GETVAL);
			break;
		case TYPE_flt:
			shuffle_unique(flt, GT, GETVAL);
			break;
		case TYPE_dbl:
			shuffle_unique(dbl, GT, GETVAL);
			break;
		default:
			heapify(GTany, GETVALany);
			while (cand ? cand < candend : start < end) {
				i = cand ? *cand++ : start++ + b->hseqbase;
				if (GTany(GETVALany(i), GETVALany(oids[0]))) {
					oids[0] = i;
					siftup(GTany, 0, GETVALany);
				}
			}
			break;
		}
	}
	/* output must be sorted since it's a candidate list */
	GDKqsort(oids, NULL, NULL, (size_t) n, sizeof(oid), 0, TYPE_oid);
	bn->tsorted = 1;
	bn->trevsorted = n <= 1;
	bn->tkey = 1;
	bn->tseqbase = (bn->tdense = n <= 1) != 0 ? oids[0] : oid_nil;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	return bn;
}

#define shuffle_unique_with_groups_body(COMPARE)			\
	do {								\
		for (ci = 0, i = cand ? *cand++ - b->hseqbase : start;	\
		     i < end;						\
		     ci++, cand < candend ? (i = *cand++ - b->hseqbase) : i++) { \
			for (j = 0; j < n; j++) {			\
				if (j == top) {				\
					assert(top < n);		\
					goids[top] = gv[ci];		\
					oids[top++] = i + b->hseqbase;	\
					break;				\
				}					\
				assert(oids[j] >= b->hseqbase);		\
				assert(oids[j] - b->hseqbase < i);	\
				if (gv[ci] < goids[j] ||		\
				    (gv[ci] == goids[j] && COMPARE)) {	\
					if (top < n)			\
						top++;			\
					for (k = top - 1; k > j; k--) {	\
						oids[k] = oids[k - 1];	\
						goids[k] = goids[k - 1]; \
					}				\
					oids[j] = i + b->hseqbase;	\
					goids[j] = gv[ci];		\
					break;				\
				}					\
			}						\
		}							\
	} while (0)

#define shuffle_unique_with_groups(TYPE, OPER)				\
	do {								\
		const TYPE *v = (const TYPE *) Tloc(b, BUNfirst(b));	\
		shuffle_unique_with_groups_body(OPER(v[i], v[oids[j] - b->hseqbase])); \
	} while (0)

static BAT *
BATfirstn_unique_with_groups(BAT *b, BAT *s, BAT *g, BUN n, int asc)
{
	BAT *bn;
	BATiter bi = bat_iterator(b);
	oid *oids, *goids;
	const oid *gv;
	BUN top, i, j, k, cnt, start, end, ci;
	const oid *cand, *candend;
	int tpe = b->ttype;
	int (*cmp)(const void *, const void *);

	if (BATtdense(g)) {
		/* trivial: g determines ordering, return initial
		 * slice of s */
		return BATslice(s, 0, n);
	}

	CANDINIT(b, s, start, end, cnt, cand, candend);

	if (n > cnt)
		n = cnt;
	if (cand && n > (BUN) (candend - cand))
		n = (BUN) (candend - cand);

	if (n == 0) {
		/* candidate list might refer only to values outside
		 * of the bat and hence be effectively empty */
		bn = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
		if (bn == NULL)
			return NULL;
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), 0);
		return bn;
	}

	bn = BATnew(TYPE_void, TYPE_oid, n, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, n);
	BATseqbase(bn, 0);
	oids = (oid *) Tloc(bn, BUNfirst(bn));
	gv = (const oid *) Tloc(g, BUNfirst(g));
	goids = GDKmalloc(n * sizeof(oid));
	if (goids == NULL) {
		BBPreclaim(bn);
		return NULL;
	}

	top = 0;
	cmp = BATatoms[b->ttype].atomCmp;
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	if (tpe != ATOMstorage(tpe) &&
	    cmp == BATatoms[ATOMstorage(b->ttype)].atomCmp) {
		/* note, this takes care of types oid and wrd */
		tpe = ATOMstorage(tpe);
	}
	if (asc) {
		switch (tpe) {
		case TYPE_void:
			shuffle_unique_with_groups_body(i < oids[j] - b->hseqbase);
			break;
		case TYPE_bte:
			shuffle_unique_with_groups(bte, LT);
			break;
		case TYPE_sht:
			shuffle_unique_with_groups(sht, LT);
			break;
		case TYPE_int:
			shuffle_unique_with_groups(int, LT);
			break;
		case TYPE_lng:
			shuffle_unique_with_groups(lng, LT);
			break;
		case TYPE_flt:
			shuffle_unique_with_groups(flt, LT);
			break;
		case TYPE_dbl:
			shuffle_unique_with_groups(dbl, LT);
			break;
		default:
			shuffle_unique_with_groups_body(cmp(BUNtail(bi, i + BUNfirst(b)), BUNtail(bi, oids[j] - b->hseqbase + BUNfirst(b))) < 0);
			break;
		}
	} else {
		switch (tpe) {
		case TYPE_void:
			shuffle_unique_with_groups_body(i > oids[j] - b->hseqbase);
			break;
		case TYPE_bte:
			shuffle_unique_with_groups(bte, GT);
			break;
		case TYPE_sht:
			shuffle_unique_with_groups(sht, GT);
			break;
		case TYPE_int:
			shuffle_unique_with_groups(int, GT);
			break;
		case TYPE_lng:
			shuffle_unique_with_groups(lng, GT);
			break;
		case TYPE_flt:
			shuffle_unique_with_groups(flt, GT);
			break;
		case TYPE_dbl:
			shuffle_unique_with_groups(dbl, GT);
			break;
		default:
			shuffle_unique_with_groups_body(cmp(BUNtail(bi, i + BUNfirst(b)), BUNtail(bi, oids[j] - b->hseqbase + BUNfirst(b))) > 0);
			break;
		}
	}
	GDKfree(goids);
	/* output must be sorted since it's a candidate list */
	GDKqsort(oids, NULL, NULL, (size_t) n, sizeof(oid), 0, TYPE_oid);
	bn->tsorted = 1;
	bn->trevsorted = n <= 1;
	bn->tkey = 1;
	bn->tseqbase = (bn->tdense = n <= 1) != 0 ? oids[0] : oid_nil;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	return bn;
}

#define shuffle_grouped1_body(COMPARE, EQUAL)				\
	do {								\
		for (i = cand ? *cand++ - b->hseqbase : start;		\
		     i < end;						\
		     cand < candend ? (i = *cand++ - b->hseqbase) : i++) { \
			for (j = 0; j < n; j++) {			\
				if (j == top) {				\
					assert(top < n);		\
					groups[top].cnt = 1;		\
					groups[top++].bun = i;		\
					break;				\
				} else {				\
					assert(j < top);		\
					assert(groups[j].bun < i);	\
					if (COMPARE) {			\
						if (top < n)		\
							top++;		\
						for (k = top - 1; k > j; k--) {	\
							groups[k] = groups[k - 1]; \
						}			\
						groups[j].bun = i;	\
						groups[j].cnt = 1;	\
						break;			\
					} else if (EQUAL) {		\
						groups[j].cnt++;	\
						break;			\
					}				\
				}					\
			}						\
		}							\
	} while (0)

#define shuffle_grouped1(TYPE, OPER)					\
	do {								\
		const TYPE *v = (const TYPE *) Tloc(b, BUNfirst(b));	\
		shuffle_grouped1_body(OPER(v[i], v[groups[j].bun]),	\
				      v[i] == v[groups[j].bun]);	\
	} while (0)

#define shuffle_grouped2(TYPE)						\
	do {								\
		const TYPE *v = (const TYPE *) Tloc(b, BUNfirst(b));	\
		TYPE lastval = v[groups[top - 1].bun];			\
		for (i = cand ? *cand++ - b->hseqbase : start;		\
		     i < end;						\
		     cand < candend ? (i = *cand++ - b->hseqbase) : i++) { \
			if (asc ? v[i] > lastval : v[i] < lastval)	\
				continue;				\
			for (j = 0; j < top; j++) {			\
				if (v[i] == v[groups[j].bun]) {		\
					if (bp)				\
						*bp++ = i + b->hseqbase; \
					*gp++ = j;			\
					break;				\
				}					\
			}						\
		}							\
	} while (0)

static gdk_return
BATfirstn_grouped(BAT **topn, BAT **gids, BAT *b, BAT *s, BUN n, int asc, int distinct)
{
	BAT *bn, *gn;
	BATiter bi = bat_iterator(b);
	oid *bp, *gp;
	BUN top, i, j, k, cnt, start, end;
	const oid *cand, *candend, *oldcand;
	int tpe = b->ttype;
	int c;
	int (*cmp)(const void *, const void *);
	BUN ncnt;
	struct group {
		BUN bun;
		BUN cnt;
	} *groups;

	assert(topn);

	CANDINIT(b, s, start, end, cnt, cand, candend);

	if (n > cnt)
		n = cnt;
	if (cand && n > (BUN) (candend - cand))
		n = (BUN) (candend - cand);

	if (n == 0) {
		/* candidate list might refer only to values outside
		 * of the bat and hence be effectively empty */
		bn = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
		if (bn == NULL)
			return GDK_FAIL;
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), 0);
		if (gids) {
			gn = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
			if (gn == NULL) {
				BBPreclaim(bn);
				return GDK_FAIL;
			}
			BATseqbase(gn, 0);
			BATseqbase(BATmirror(gn), 0);
			*gids = gn;
		}
		*topn = bn;
		return GDK_SUCCEED;
	}

	top = 0;
	cmp = BATatoms[b->ttype].atomCmp;
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	if (tpe != ATOMstorage(tpe) &&
	    cmp == BATatoms[ATOMstorage(b->ttype)].atomCmp) {
		/* note, this takes care of types oid and wrd */
		tpe = ATOMstorage(tpe);
	}
	groups = GDKmalloc(sizeof(*groups) * n);
	oldcand = cand;
	if (asc) {
		switch (tpe) {
		case TYPE_void:
			shuffle_grouped1_body(i < groups[j].bun,
					      i == groups[j].bun);
			break;
		case TYPE_bte:
			shuffle_grouped1(bte, LT);
			break;
		case TYPE_sht:
			shuffle_grouped1(sht, LT);
			break;
		case TYPE_int:
			shuffle_grouped1(int, LT);
			break;
		case TYPE_lng:
			shuffle_grouped1(lng, LT);
			break;
		case TYPE_flt:
			shuffle_grouped1(flt, LT);
			break;
		case TYPE_dbl:
			shuffle_grouped1(dbl, LT);
			break;
		default:
			shuffle_grouped1_body(
				(c = cmp(BUNtail(bi, i + BUNfirst(b)),
					 BUNtail(bi, groups[j].bun))) < 0,
				c == 0);
			break;
		}
	} else {
		switch (tpe) {
		case TYPE_void:
			shuffle_grouped1_body(i > groups[j].bun,
					      i == groups[j].bun);
			break;
		case TYPE_bte:
			shuffle_grouped1(bte, GT);
			break;
		case TYPE_sht:
			shuffle_grouped1(sht, GT);
			break;
		case TYPE_int:
			shuffle_grouped1(int, GT);
			break;
		case TYPE_lng:
			shuffle_grouped1(lng, GT);
			break;
		case TYPE_flt:
			shuffle_grouped1(flt, GT);
			break;
		case TYPE_dbl:
			shuffle_grouped1(dbl, GT);
			break;
		default:
			shuffle_grouped1_body(
				(c = cmp(BUNtail(bi, i + BUNfirst(b)),
					 BUNtail(bi, groups[j].bun))) > 0,
				c == 0);
			break;
		}
	}
	cand = oldcand;
	for (i = 0, ncnt = 0; i < top && (distinct || ncnt < n); i++)
		ncnt += groups[i].cnt;
	top = i;
	assert(ncnt <= cnt);
	if (ncnt == cnt)
		bn = BATnew(TYPE_void, TYPE_void, ncnt, TRANSIENT);
	else
		bn = BATnew(TYPE_void, TYPE_oid, ncnt, TRANSIENT);
	gn = BATnew(TYPE_void, TYPE_oid, ncnt, TRANSIENT);
	if (bn == NULL || gn == NULL) {
		GDKfree(groups);
		BBPreclaim(bn);
		BBPreclaim(gn);
		return GDK_FAIL;
	}
	if (ncnt == cnt)
		bp = NULL;
	else
		bp = (oid *) Tloc(bn, BUNfirst(bn));
	gp = (oid *) Tloc(gn, BUNfirst(gn));
	switch (tpe) {
	case TYPE_void:
		for (i = cand ? *cand++ - b->hseqbase : start;
		     i < end;
		     cand < candend ? (i = *cand++ - b->hseqbase) : i++) {
			for (j = 0; j < top; j++) {
				if (i == groups[j].bun) {
					if (bp)
						*bp++ = i + b->hseqbase;
					*gp++ = j;
					break;
				}
			}
		}
		break;
	case TYPE_bte:
		shuffle_grouped2(bte);
		break;
	case TYPE_sht:
		shuffle_grouped2(sht);
		break;
	case TYPE_int:
		shuffle_grouped2(int);
		break;
	case TYPE_lng:
		shuffle_grouped2(lng);
		break;
	case TYPE_flt:
		shuffle_grouped2(flt);
		break;
	case TYPE_dbl:
		shuffle_grouped2(dbl);
		break;
	default:
		for (i = cand ? *cand++ - b->hseqbase : start;
		     i < end;
		     cand < candend ? (i = *cand++ - b->hseqbase) : i++) {
			for (j = 0; j < top; j++) {
				if (cmp(BUNtail(bi, i + BUNfirst(b)), BUNtail(bi, groups[j].bun)) == 0) {
					if (bp)
						*bp++ = i + b->hseqbase;
					*gp++ = j;
					break;
				}
			}
		}
		break;
	}
	GDKfree(groups);
	BATsetcount(bn, ncnt);
	BATseqbase(bn, 0);
	if (ncnt == cnt) {
		BATseqbase(BATmirror(bn), b->hseqbase);
	} else {
		bn->tkey = 1;
		bn->tsorted = 1;
		bn->trevsorted = ncnt <= 1;
		bn->T->nil = 0;
		bn->T->nonil = 1;
	}
	if (gids) {
		BATsetcount(gn, ncnt);
		BATseqbase(gn, 0);
		gn->tkey = ncnt == top;
		gn->tsorted = ncnt <= 1;
		gn->trevsorted = ncnt <= 1;
		gn->T->nil = 0;
		gn->T->nonil = 1;
		*gids = gn;
	} else
		BBPreclaim(gn);
	*topn = bn;
	return GDK_SUCCEED;
}

#define shuffle_grouped_with_groups1_body(COMPARE, EQUAL)		\
	do {								\
		for (ci = 0, i = cand ? *cand++ - b->hseqbase : start;	\
		     i < end;						\
		     ci++, cand < candend ? (i = *cand++ - b->hseqbase) : i++) { \
			for (j = 0; j < n; j++) {			\
				if (j == top) {				\
					assert(top < n);		\
					groups[top].grp = gv[ci];	\
					groups[top].cnt = 1;		\
					groups[top++].bun = i;		\
					break;				\
				} else {				\
					assert(j < top);		\
					assert(groups[j].bun < i);	\
					if (gv[ci] < groups[j].grp ||	\
					    (gv[ci] == groups[j].grp &&	\
					     COMPARE)) {		\
						if (top < n)		\
							top++;		\
						for (k = top - 1; k > j; k--) {	\
							groups[k] = groups[k - 1]; \
						}			\
						groups[j].bun = i;	\
						groups[j].cnt = 1;	\
						groups[j].grp = gv[ci];	\
						break;			\
					} else if (gv[ci] == groups[j].grp && \
						   EQUAL) {		\
						groups[j].cnt++;	\
						break;			\
					}				\
				}					\
			}						\
		}							\
	} while (0)

#define shuffle_grouped_with_groups1(TYPE, OPER)			\
	do {								\
		const TYPE *v = (const TYPE *) Tloc(b, BUNfirst(b));	\
		shuffle_grouped_with_groups1_body(OPER(v[i], v[groups[j].bun]),	\
						  v[i] == v[groups[j].bun]); \
	} while (0)

#define shuffle_grouped_with_groups2(TYPE)				\
	do {								\
		const TYPE *v = (const TYPE *) Tloc(b, BUNfirst(b));	\
		for (ci = 0, i = cand ? *cand++ - b->hseqbase : start;	\
		     i < end;						\
		     ci++, cand < candend ? (i = *cand++ - b->hseqbase) : i++) { \
			for (j = 0; j < top; j++) {			\
				if (gv[ci] == groups[j].grp &&		\
				    v[i] == v[groups[j].bun]) {		\
					if (bp)				\
						*bp++ = i + b->hseqbase; \
					*gp++ = j;			\
					break;				\
				}					\
			}						\
		}							\
	} while (0)

static gdk_return
BATfirstn_grouped_with_groups(BAT **topn, BAT **gids, BAT *b, BAT *s, BAT *g, BUN n, int asc, int distinct)
{
	BAT *bn, *gn;
	BATiter bi = bat_iterator(b);
	oid *bp, *gp;
	BUN top, i, j, k, cnt, start, end, ci;
	const oid *cand, *candend, *oldcand, *gv;
	int tpe = b->ttype;
	int c;
	int (*cmp)(const void *, const void *);
	BUN ncnt;
	struct group {
		BUN bun;
		BUN cnt;
		oid grp;
	} *groups;

	assert(topn);

	if (BATtdense(g)) {
		/* trivial: g determines ordering, return initial
		 * slice of s */
		bn = BATslice(s, 0, n);
		gn = gids ? BATslice(g, 0, n) : NULL;
		if (bn == NULL || (gids != NULL && gn == NULL)) {
			BBPreclaim(bn);
			BBPreclaim(gn);
			return GDK_FAIL;
		}
		*topn = bn;
		if (gids)
			*gids = gn;
		return GDK_SUCCEED;
	}

	CANDINIT(b, s, start, end, cnt, cand, candend);

	if (n > cnt)
		n = cnt;
	if (cand && n > (BUN) (candend - cand))
		n = (BUN) (candend - cand);

	if (n == 0) {
		/* candidate list might refer only to values outside
		 * of the bat and hence be effectively empty */
		bn = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
		if (bn == NULL)
			return GDK_FAIL;
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), 0);
		if (gids) {
			gn = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
			if (gn == NULL) {
				BBPreclaim(bn);
				return GDK_FAIL;
			}
			BATseqbase(gn, 0);
			BATseqbase(BATmirror(gn), 0);
			*gids = gn;
		}
		*topn = bn;
		return GDK_SUCCEED;
	}

	top = 0;
	cmp = BATatoms[b->ttype].atomCmp;
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	if (tpe != ATOMstorage(tpe) &&
	    cmp == BATatoms[ATOMstorage(b->ttype)].atomCmp) {
		/* note, this takes care of types oid and wrd */
		tpe = ATOMstorage(tpe);
	}
	groups = GDKmalloc(sizeof(*groups) * n);
	gv = (const oid *) Tloc(g, BUNfirst(g));
	oldcand = cand;
	if (asc) {
		switch (tpe) {
		case TYPE_void:
			shuffle_grouped_with_groups1_body(i < groups[j].bun,
							  i == groups[j].bun);
			break;
		case TYPE_bte:
			shuffle_grouped_with_groups1(bte, LT);
			break;
		case TYPE_sht:
			shuffle_grouped_with_groups1(sht, LT);
			break;
		case TYPE_int:
			shuffle_grouped_with_groups1(int, LT);
			break;
		case TYPE_lng:
			shuffle_grouped_with_groups1(lng, LT);
			break;
		case TYPE_flt:
			shuffle_grouped_with_groups1(flt, LT);
			break;
		case TYPE_dbl:
			shuffle_grouped_with_groups1(dbl, LT);
			break;
		default:
			shuffle_grouped_with_groups1_body(
				(c = cmp(BUNtail(bi, i + BUNfirst(b)),
					 BUNtail(bi, groups[j].bun))) < 0,
				c == 0);
			break;
		}
	} else {
		switch (tpe) {
		case TYPE_void:
			shuffle_grouped_with_groups1_body(i > groups[j].bun,
							  i == groups[j].bun);
			break;
		case TYPE_bte:
			shuffle_grouped_with_groups1(bte, GT);
			break;
		case TYPE_sht:
			shuffle_grouped_with_groups1(sht, GT);
			break;
		case TYPE_int:
			shuffle_grouped_with_groups1(int, GT);
			break;
		case TYPE_lng:
			shuffle_grouped_with_groups1(lng, GT);
			break;
		case TYPE_flt:
			shuffle_grouped_with_groups1(flt, GT);
			break;
		case TYPE_dbl:
			shuffle_grouped_with_groups1(dbl, GT);
			break;
		default:
			shuffle_grouped_with_groups1_body(
				(c = cmp(BUNtail(bi, i + BUNfirst(b)),
					 BUNtail(bi, groups[j].bun))) > 0,
				c == 0);
			break;
		}
	}
	cand = oldcand;
	for (i = 0, ncnt = 0; i < top && (distinct || ncnt < n); i++)
		ncnt += groups[i].cnt;
	top = i;
	assert(ncnt <= cnt);
	if (ncnt == cnt)
		bn = BATnew(TYPE_void, TYPE_void, ncnt, TRANSIENT);
	else
		bn = BATnew(TYPE_void, TYPE_oid, ncnt, TRANSIENT);
	gn = BATnew(TYPE_void, TYPE_oid, ncnt, TRANSIENT);
	if (bn == NULL || gn == NULL) {
		GDKfree(groups);
		BBPreclaim(bn);
		BBPreclaim(gn);
		return GDK_FAIL;
	}
	if (ncnt == cnt)
		bp = NULL;
	else
		bp = (oid *) Tloc(bn, BUNfirst(bn));
	gp = (oid *) Tloc(gn, BUNfirst(gn));
	switch (tpe) {
	case TYPE_void:
		for (ci = 0, i = cand ? *cand++ - b->hseqbase : start;
		     i < end;
		     ci++, cand < candend ? (i = *cand++ - b->hseqbase) : i++) {
			for (j = 0; j < top; j++) {
				if (gv[ci] == groups[j].grp &&
				    i == groups[j].bun) {
					if (bp)
						*bp++ = i + b->hseqbase;
					*gp++ = j;
					break;
				}
			}
		}
		break;
	case TYPE_bte:
		shuffle_grouped_with_groups2(bte);
		break;
	case TYPE_sht:
		shuffle_grouped_with_groups2(sht);
		break;
	case TYPE_int:
		shuffle_grouped_with_groups2(int);
		break;
	case TYPE_lng:
		shuffle_grouped_with_groups2(lng);
		break;
	case TYPE_flt:
		shuffle_grouped_with_groups2(flt);
		break;
	case TYPE_dbl:
		shuffle_grouped_with_groups2(dbl);
		break;
	default:
		for (ci = 0, i = cand ? *cand++ - b->hseqbase : start;
		     i < end;
		     ci++, cand < candend ? (i = *cand++ - b->hseqbase) : i++) {
			for (j = 0; j < top; j++) {
				if (gv[ci] == groups[j].grp &&
				    cmp(BUNtail(bi, i + BUNfirst(b)), BUNtail(bi, groups[j].bun)) == 0) {
					if (bp)
						*bp++ = i + b->hseqbase;
					*gp++ = j;
					break;
				}
			}
		}
		break;
	}
	GDKfree(groups);
	BATsetcount(bn, ncnt);
	BATseqbase(bn, 0);
	if (ncnt == cnt) {
		BATseqbase(BATmirror(bn), b->hseqbase);
	} else {
		bn->tkey = 1;
		bn->tsorted = 1;
		bn->trevsorted = ncnt <= 1;
		bn->T->nil = 0;
		bn->T->nonil = 1;
	}
	if (gids) {
		BATsetcount(gn, ncnt);
		BATseqbase(gn, 0);
		gn->tkey = ncnt == top;
		gn->tsorted = ncnt <= 1;
		gn->trevsorted = ncnt <= 1;
		gn->T->nil = 0;
		gn->T->nonil = 1;
		*gids = gn;
	} else
		BBPreclaim(gn);
	*topn = bn;
	return GDK_SUCCEED;
}

gdk_return
BATfirstn(BAT **topn, BAT **gids, BAT *b, BAT *s, BAT *g, BUN n, int asc, int distinct)
{
	assert(topn != NULL);
	if (b == NULL) {
		*topn = NULL;
		return GDK_SUCCEED;
	}

	/* all BATs must be dense-headed */
	assert(BAThdense(b));
	assert(s == NULL || BAThdense(s));
	assert(g == NULL || BAThdense(g));
	/* if g specified, then so must s */
	assert(g == NULL || s != NULL);
	/* g and s must be aligned (same size, same hseqbase) */
	assert(g == NULL || BATcount(s) == BATcount(g));
	assert(g == NULL || BATcount(g) == 0 || s->hseqbase == g->hseqbase);

	if (n == 0 || BATcount(b) == 0 || (s != NULL && BATcount(s) == 0)) {
		/* trivial: empty result */
		*topn = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
		if (*topn == NULL)
			return GDK_FAIL;
		BATseqbase(*topn, 0);
		BATseqbase(BATmirror(*topn), 0);
		if (gids) {
			*gids = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
			if (*gids == NULL) {
				BBPreclaim(*topn);
				return GDK_FAIL;
			}
			BATseqbase(*gids, 0);
			BATseqbase(BATmirror(*gids), 0);
		}
		return GDK_SUCCEED;
	}

	if (g == NULL) {
		if (gids == NULL && !distinct) {
			*topn = BATfirstn_unique(b, s, n, asc);
			return *topn ? GDK_SUCCEED : GDK_FAIL;
		}
		return BATfirstn_grouped(topn, gids, b, s, n, asc, distinct);
	}
	if (gids == NULL && !distinct) {
		*topn = BATfirstn_unique_with_groups(b, s, g, n, asc);
		return *topn ? GDK_SUCCEED : GDK_FAIL;
	}
	return BATfirstn_grouped_with_groups(topn, gids, b, s, g, n, asc, distinct);
}
