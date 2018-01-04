/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * If distinct is set, the result contains n complete groups of values
 * instead of just n values (or slightly more than n if gids is set
 * since then the "last" group is returned completely).
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
#define siftup(OPER, START, SWAP)					\
	do {								\
		pos = (START);						\
		childpos = (pos << 1) + 1;				\
		while (childpos < n) {					\
			/* find most extreme child */			\
			if (childpos + 1 < n &&				\
			    !(OPER(childpos + 1, childpos)))		\
				childpos++;				\
			/* compare parent with most extreme child */	\
			if (!OPER(pos, childpos)) {			\
				/* already correctly ordered */		\
				break;					\
			}						\
			/* exchange parent with child and sift child */	\
			/* further */					\
			SWAP(pos, childpos);				\
			pos = childpos;					\
			childpos = (pos << 1) + 1;			\
		}							\
	} while (0)

#define heapify(OPER, SWAP)				\
	do {						\
		for (i = n / 2; i > 0; i--)		\
			siftup(OPER, i - 1, SWAP);	\
	} while (0)

#define LTany(p1, p2)	(cmp(BUNtail(bi, oids[p1] - b->hseqbase), \
			     BUNtail(bi, oids[p2] - b->hseqbase)) < 0)
#define GTany(p1, p2)	(cmp(BUNtail(bi, oids[p1] - b->hseqbase), \
			     BUNtail(bi, oids[p2] - b->hseqbase)) > 0)
#define LTfix(p1, p2)	(vals[oids[p1] - b->hseqbase] < vals[oids[p2] - b->hseqbase])
#define GTfix(p1, p2)	(vals[oids[p1] - b->hseqbase] > vals[oids[p2] - b->hseqbase])
#define SWAP1(p1, p2)				\
	do {					\
		item = oids[p1];		\
		oids[p1] = oids[p2];		\
		oids[p2] = item;		\
	} while (0)

#define shuffle_unique(TYPE, OP)					\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		heapify(OP##fix, SWAP1);				\
		while (cand ? cand < candend : start < end) {		\
			i = cand ? *cand++ : start++ + b->hseqbase;	\
			if (OP(vals[i - b->hseqbase],			\
				 vals[oids[0] - b->hseqbase])) {	\
				oids[0] = i;				\
				siftup(OP##fix, 0, SWAP1);		\
			}						\
		}							\
	} while (0)

/* This version of BATfirstn returns a list of N oids (where N is the
 * smallest among BATcount(b), BATcount(s), and n).  The oids returned
 * refer to the N smallest/largest (depending on asc) tail values of b
 * (taking the optional candidate list s into account).  If there are
 * multiple equal values to take us past N, we return a subset of those.
 *
 * If lastp is non-NULL, it is filled in with the oid of the "last"
 * value, i.e. the value of which there may be multiple occurrences
 * that are not all included in the first N.
 */
static BAT *
BATfirstn_unique(BAT *b, BAT *s, BUN n, int asc, oid *lastp)
{
	BAT *bn;
	BATiter bi = bat_iterator(b);
	oid *restrict oids;
	BUN i, cnt, start, end;
	const oid *restrict cand, *candend;
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
			if (lastp)
				*lastp = 0;
			return BATslice(s,
					(BUN) (cand - (const oid *) Tloc(s, 0)),
					(BUN) (candend - (const oid *) Tloc(s, 0)));
		}
	} else if (n >= cnt) {
		/* trivial: return everything */
		bn = BATdense(0, start + b->hseqbase, cnt);
		if (bn == NULL)
			return NULL;
		if (lastp)
			*lastp = 0;
		return bn;
	}
	/* note, we want to do both calls */
	if (BATordered(b) | BATordered_rev(b)) {
		/* trivial: b is sorted so we just need to return the
		 * initial or final part of it (or of the candidate
		 * list) */
		if (cand) {
			if (asc ? b->tsorted : b->trevsorted) {
				/* return copy of first relevant part
				 * of candidate list */
				i = (BUN) (cand - (const oid *) Tloc(s, 0));
				if (lastp)
					*lastp = cand[n - 1];
				return BATslice(s, i, i + n);
			}
			/* return copy of last relevant part of
			 * candidate list */
			i = (BUN) (candend - (const oid *) Tloc(s, 0));
			if (lastp)
				*lastp = candend[-(ssize_t)n];
			return BATslice(s, i - n, i);
		}
		if (asc ? b->tsorted : b->trevsorted) {
			/* first n entries from b */
			bn = BATdense(0, start + b->hseqbase, n);
			if (lastp)
				*lastp = start + b->hseqbase + n - 1;
		} else {
			/* last n entries from b */
			bn = BATdense(0, start + cnt + b->hseqbase - n, n);
			if (lastp)
				*lastp = start + cnt + b->hseqbase - n;
		}
		return bn;
	}

	assert(b->ttype != TYPE_void); /* tsorted above took care of this */

	bn = COLnew(0, TYPE_oid, n, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, n);
	oids = (oid *) Tloc(bn, 0);
	cmp = ATOMcompare(b->ttype);
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	tpe = ATOMbasetype(tpe); /* takes care of oid */
	/* if the input happens to be almost sorted in ascending order
	 * (likely a common use case), it is more efficient to start
	 * off with the first n elements when doing a firstn-ascending
	 * and to start off with the last n elements when doing a
	 * firstn-descending so that most values that we look at after
	 * this will be skipped. */
	if (cand) {
		if (asc) {
			for (i = 0; i < n; i++)
				oids[i] = *cand++;
		} else {
			for (i = 0; i < n; i++)
				oids[i] = *--candend;
		}
	} else {
		if (asc) {
			for (i = 0; i < n; i++)
				oids[i] = start++ + b->hseqbase;
		} else {
			for (i = 0; i < n; i++)
				oids[i] = --end + b->hseqbase;
		}
	}
	if (asc) {
		switch (tpe) {
		case TYPE_bte:
			shuffle_unique(bte, LT);
			break;
		case TYPE_sht:
			shuffle_unique(sht, LT);
			break;
		case TYPE_int:
			shuffle_unique(int, LT);
			break;
		case TYPE_lng:
			shuffle_unique(lng, LT);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			shuffle_unique(hge, LT);
			break;
#endif
		case TYPE_flt:
			shuffle_unique(flt, LT);
			break;
		case TYPE_dbl:
			shuffle_unique(dbl, LT);
			break;
		default:
			heapify(LTany, SWAP1);
			while (cand ? cand < candend : start < end) {
				i = cand ? *cand++ : start++ + b->hseqbase;
				if (cmp(BUNtail(bi, i - b->hseqbase),
					BUNtail(bi, oids[0] - b->hseqbase)) < 0) {
					oids[0] = i;
					siftup(LTany, 0, SWAP1);
				}
			}
			break;
		}
	} else {
		switch (tpe) {
		case TYPE_bte:
			shuffle_unique(bte, GT);
			break;
		case TYPE_sht:
			shuffle_unique(sht, GT);
			break;
		case TYPE_int:
			shuffle_unique(int, GT);
			break;
		case TYPE_lng:
			shuffle_unique(lng, GT);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			shuffle_unique(hge, GT);
			break;
#endif
		case TYPE_flt:
			shuffle_unique(flt, GT);
			break;
		case TYPE_dbl:
			shuffle_unique(dbl, GT);
			break;
		default:
			heapify(GTany, SWAP1);
			while (cand ? cand < candend : start < end) {
				i = cand ? *cand++ : start++ + b->hseqbase;
				if (cmp(BUNtail(bi, i - b->hseqbase),
					BUNtail(bi, oids[0] - b->hseqbase)) > 0) {
					oids[0] = i;
					siftup(GTany, 0, SWAP1);
				}
			}
			break;
		}
	}
	if (lastp)
		*lastp = oids[0]; /* store id of largest value */
	/* output must be sorted since it's a candidate list */
	GDKqsort(oids, NULL, NULL, (size_t) n, sizeof(oid), 0, TYPE_oid);
	bn->tsorted = 1;
	bn->trevsorted = n <= 1;
	bn->tkey = 1;
	bn->tseqbase = (bn->tdense = n <= 1) != 0 ? oids[0] : oid_nil;
	bn->tnil = 0;
	bn->tnonil = 1;
	return bn;
}

#define LTfixgrp(p1, p2)						\
	(goids[p1] < goids[p2] ||					\
	 (goids[p1] == goids[p2] &&					\
	  vals[oids[p1] - b->hseqbase] < vals[oids[p2] - b->hseqbase]))
#define GTfixgrp(p1, p2)						\
	(goids[p1] < goids[p2] ||					\
	 (goids[p1] == goids[p2] &&					\
	  vals[oids[p1] - b->hseqbase] > vals[oids[p2] - b->hseqbase]))
#define LTvoidgrp(p1, p2)					\
	(goids[p1] < goids[p2] ||				\
	 (goids[p1] == goids[p2] && oids[p1] < oids[p2]))
#define GTvoidgrp(p1, p2)					\
	(goids[p1] < goids[p2] ||				\
	 (goids[p1] == goids[p2] && oids[p1] > oids[p2]))
#define LTanygrp(p1, p2)					\
	(goids[p1] < goids[p2] ||				\
	 (goids[p1] == goids[p2] &&				\
	  cmp(BUNtail(bi, oids[p1] - b->hseqbase),		\
	      BUNtail(bi, oids[p2] - b->hseqbase)) < 0))
#define GTanygrp(p1, p2)					\
	(goids[p1] < goids[p2] ||				\
	 (goids[p1] == goids[p2] &&				\
	  cmp(BUNtail(bi, oids[p1] - b->hseqbase),		\
	      BUNtail(bi, oids[p2] - b->hseqbase)) > 0))
#define SWAP2(p1, p2)				\
	do {					\
		item = oids[p1];		\
		oids[p1] = oids[p2];		\
		oids[p2] = item;		\
		item = goids[p1];		\
		goids[p1] = goids[p2];		\
		goids[p2] = item;		\
	} while (0)

#define shuffle_unique_with_groups(TYPE, OP)				\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		heapify(OP##fixgrp, SWAP2);				\
		while (cand ? cand < candend : start < end) {		\
			i = cand ? *cand++ : start++ + b->hseqbase;	\
			if (gv[ci] < goids[0] ||			\
			    (gv[ci] == goids[0] &&			\
			     OP(vals[i - b->hseqbase],			\
				vals[oids[0] - b->hseqbase]))) {	\
				oids[0] = i;				\
				goids[0] = gv[ci];			\
				siftup(OP##fixgrp, 0, SWAP2);		\
			}						\
			ci++;						\
		}							\
	} while (0)

/* This version of BATfirstn is like the one above, except that it
 * also looks at groups.  The values of the group IDs are important:
 * we return only the smallest N (i.e., not dependent on asc which
 * refers only to the values in the BAT b).
 *
 * If lastp is non-NULL, it is filled in with the oid of the "last"
 * value, i.e. the value of which there may be multiple occurrences
 * that are not all included in the first N.  If lastgp is non-NULL,
 * it is filled with the group ID (not the oid of the group ID) for
 * that same value.
 */
static BAT *
BATfirstn_unique_with_groups(BAT *b, BAT *s, BAT *g, BUN n, int asc, oid *lastp, oid *lastgp)
{
	BAT *bn;
	BATiter bi = bat_iterator(b);
	oid *restrict oids, *restrict goids;
	const oid *restrict gv;
	BUN i, cnt, start, end, ci;
	const oid *restrict cand, *candend;
	int tpe = b->ttype;
	int (*cmp)(const void *, const void *);
	/* variables used in heapify/siftup macros */
	oid item;
	BUN pos, childpos;

	CANDINIT(b, s, start, end, cnt, cand, candend);

	cnt = cand ? (BUN) (candend - cand) : end - start;
	if (n > cnt)
		n = cnt;

	if (n == 0) {
		/* candidate list might refer only to values outside
		 * of the bat and hence be effectively empty */
		if (lastp)
			*lastp = 0;
		if (lastgp)
			*lastgp = 0;
		return BATdense(0, 0, 0);
	}

	if (BATtdense(g)) {
		/* trivial: g determines ordering, return reference to
		 * initial part of b (or slice of s) */
		if (lastgp)
			*lastgp = g->tseqbase + n - 1;
		if (cand) {
			if (lastp)
				*lastp = cand[n - 1];
			bn = COLnew(0, TYPE_oid, n, TRANSIENT);
			if (bn == NULL)
				return NULL;
			memcpy(Tloc(bn, 0), cand, n * sizeof(oid));
			BATsetcount(bn, n);
			bn->tsorted = 1;
			bn->trevsorted = n <= 1;
			bn->tkey = 1;
			bn->tseqbase = (bn->tdense = n <= 1) != 0 ? cand[0] : oid_nil;
			bn->tnil = 0;
			bn->tnonil = 1;
			return bn;
		}
		if (lastp)
			*lastp = b->hseqbase + start + n - 1;
		return BATdense(0, b->hseqbase + start, n);
	}

	bn = COLnew(0, TYPE_oid, n, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, n);
	oids = (oid *) Tloc(bn, 0);
	gv = (const oid *) Tloc(g, 0);
	goids = GDKmalloc(n * sizeof(oid));
	if (goids == NULL) {
		BBPreclaim(bn);
		return NULL;
	}

	cmp = ATOMcompare(b->ttype);
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	tpe = ATOMbasetype(tpe); /* takes care of oid */
	ci = 0;
	if (cand) {
		for (i = 0; i < n; i++) {
			oids[i] = *cand++;
			goids[i] = gv[ci++];
		}
	} else {
		for (i = 0; i < n; i++) {
			oids[i] = start++ + b->hseqbase;
			goids[i] = gv[ci++];
		}
	}
	if (asc) {
		switch (tpe) {
		case TYPE_void:
			heapify(LTvoidgrp, SWAP2);
			while (cand ? cand < candend : start < end) {
				i = cand ? *cand++ : start++ + b->hseqbase;
				if (gv[ci] < goids[0] /* ||
				    (gv[ci] == goids[0] &&
				     i < oids[0]) -- always false */) {
					oids[0] = i;
					goids[0] = gv[ci];
					siftup(LTvoidgrp, 0, SWAP2);
				}
				ci++;
			}
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
#ifdef HAVE_HGE
		case TYPE_hge:
			shuffle_unique_with_groups(hge, LT);
			break;
#endif
		case TYPE_flt:
			shuffle_unique_with_groups(flt, LT);
			break;
		case TYPE_dbl:
			shuffle_unique_with_groups(dbl, LT);
			break;
		default:
			heapify(LTanygrp, SWAP2);
			while (cand ? cand < candend : start < end) {
				i = cand ? *cand++ : start++ + b->hseqbase;
				if (gv[ci] < goids[0] ||
				    (gv[ci] == goids[0] &&
				     cmp(BUNtail(bi, i - b->hseqbase),
					 BUNtail(bi, oids[0] - b->hseqbase)) < 0)) {
					oids[0] = i;
					goids[0] = gv[ci];
					siftup(LTanygrp, 0, SWAP2);
				}
				ci++;
			}
			break;
		}
	} else {
		switch (tpe) {
		case TYPE_void:
			heapify(LTvoidgrp, SWAP2);
			while (cand ? cand < candend : start < end) {
				i = cand ? *cand++ : start++ + b->hseqbase;
				if (gv[ci] < goids[0] ||
				    (gv[ci] == goids[0] /* &&
				     i > oids[0] -- always true */)) {
					oids[0] = i;
					goids[0] = gv[ci];
					siftup(LTvoidgrp, 0, SWAP2);
				}
				ci++;
			}
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
#ifdef HAVE_HGE
		case TYPE_hge:
			shuffle_unique_with_groups(hge, GT);
			break;
#endif
		case TYPE_flt:
			shuffle_unique_with_groups(flt, GT);
			break;
		case TYPE_dbl:
			shuffle_unique_with_groups(dbl, GT);
			break;
		default:
			heapify(GTanygrp, SWAP2);
			while (cand ? cand < candend : start < end) {
				i = cand ? *cand++ : start++ + b->hseqbase;
				if (gv[ci] < goids[0] ||
				    (gv[ci] == goids[0] &&
				     cmp(BUNtail(bi, i - b->hseqbase),
					 BUNtail(bi, oids[0] - b->hseqbase)) > 0)) {
					oids[0] = i;
					goids[0] = gv[ci];
					siftup(GTanygrp, 0, SWAP2);
				}
				ci++;
			}
			break;
		}
	}
	if (lastp)
		*lastp = oids[0];
	if (lastgp)
		*lastgp = goids[0];
	GDKfree(goids);
	/* output must be sorted since it's a candidate list */
	GDKqsort(oids, NULL, NULL, (size_t) n, sizeof(oid), 0, TYPE_oid);
	bn->tsorted = 1;
	bn->trevsorted = n <= 1;
	bn->tkey = 1;
	bn->tseqbase = (bn->tdense = n <= 1) != 0 ? oids[0] : oid_nil;
	bn->tnil = 0;
	bn->tnonil = 1;
	return bn;
}

static gdk_return
BATfirstn_grouped(BAT **topn, BAT **gids, BAT *b, BAT *s, BUN n, int asc, int distinct)
{
	BAT *bn, *gn, *su = NULL;
	oid last;
	gdk_return rc;

	if (distinct && !b->tkey) {
		su = s;
		s = BATunique(b, s);
		if (s == NULL)
			return GDK_FAIL;
	}
	bn = BATfirstn_unique(b, s, n, asc, &last);
	if (bn == NULL)
		return GDK_FAIL;
	if (BATcount(bn) == 0) {
		if (gids) {
			gn = BATdense(0, 0, 0);
			if (gn == NULL) {
				BBPunfix(bn->batCacheid);
				return GDK_FAIL;
			}
			*gids = gn;
		}
		*topn = bn;
		return GDK_SUCCEED;
	}
	if (!b->tkey) {
		if (distinct) {
			BAT *bn1;

			bn1 = bn;
			BBPunfix(s->batCacheid);
			rc = BATsemijoin(&bn, NULL, b, b, su, bn1, 1, BUN_NONE);
			BBPunfix(bn1->batCacheid);
			if (rc != GDK_SUCCEED)
				return GDK_FAIL;
		} else {
			BATiter bi = bat_iterator(b);
			BAT *bn1, *bn2;

			bn1 = bn;
			bn2 = BATselect(b, s, BUNtail(bi, last - b->hseqbase), NULL, 1, 0, 0);
			if (bn2 == NULL) {
				BBPunfix(bn1->batCacheid);
				return GDK_FAIL;
			}
			bn = BATmergecand(bn1, bn2);
			BBPunfix(bn1->batCacheid);
			BBPunfix(bn2->batCacheid);
			if (bn == NULL)
				return GDK_FAIL;
		}
	}
	if (gids) {
		BAT *bn1, *bn2, *bn3, *bn4;
		bn1 = BATproject(bn, b);
		if (bn1 == NULL) {
			BBPunfix(bn->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsort(NULL, &bn2, &bn3, bn1, NULL, NULL, !asc, 0);
		BBPunfix(bn1->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsort(NULL, &bn4, NULL, bn2, NULL, NULL, 0, 0);
		BBPunfix(bn2->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			BBPunfix(bn3->batCacheid);
			return GDK_FAIL;
		}
		gn = BATproject(bn4, bn3);
		BBPunfix(bn3->batCacheid);
		BBPunfix(bn4->batCacheid);
		if (gn == NULL) {
			BBPunfix(bn->batCacheid);
			return GDK_FAIL;
		}
		*gids = gn;
		assert(BATcount(gn) == BATcount(bn));
	}
	*topn = bn;
	return GDK_SUCCEED;
}

static gdk_return
BATfirstn_grouped_with_groups(BAT **topn, BAT **gids, BAT *b, BAT *s, BAT *g, BUN n, int asc, int distinct)
{
	BAT *bn, *gn;
	oid last, lastg;
	gdk_return rc;

	if (distinct) {
		BAT *bn1, *bn2, *bn3, *bn4, *bn5, *bn6, *bn7, *bn8;
		if (BATgroup(&bn1, &bn2, NULL, b, s, g, NULL, NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		bn3 = BATproject(bn2, b);
		if (bn3 == NULL) {
			BBPunfix(bn1->batCacheid);
			BBPunfix(bn2->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsemijoin(&bn4, NULL, s, bn2, NULL, NULL, 0, BUN_NONE);
		BBPunfix(bn2->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn1->batCacheid);
			return GDK_FAIL;
		}
		bn5 = BATproject(bn4, g);
		BBPunfix(bn4->batCacheid);
		if (bn5 == NULL) {
			BBPunfix(bn1->batCacheid);
			return GDK_FAIL;
		}
		bn6 = BATfirstn_unique_with_groups(bn3, NULL, bn5, n, asc, NULL, NULL);
		BBPunfix(bn3->batCacheid);
		BBPunfix(bn5->batCacheid);
		if (bn6 == NULL) {
			BBPunfix(bn1->batCacheid);
			return GDK_FAIL;
		}
		rc = BATleftjoin(&bn8, &bn7, bn1, bn6, NULL, NULL, false, BUN_NONE);
		BBPunfix(bn6->batCacheid);
		if (rc != GDK_SUCCEED)
			return GDK_FAIL;
		BBPunfix(bn7->batCacheid);
		bn = BATproject(bn8, s);
		BBPunfix(bn8->batCacheid);
		if (bn == NULL)
			return GDK_FAIL;
	} else {
		bn = BATfirstn_unique_with_groups(b, s, g, n, asc, &last, &lastg);
		if (bn == NULL)
			return GDK_FAIL;
	}
	if (BATcount(bn) == 0) {
		if (gids) {
			gn = BATdense(0, 0, 0);
			if (gn == NULL) {
				BBPunfix(bn->batCacheid);
				return GDK_FAIL;
			}
			*gids = gn;
		}
		*topn = bn;
		return GDK_SUCCEED;
	}
	if (!distinct && !b->tkey) {
		BAT *bn1, *bn2, *bn3, *bn4;
		BATiter bi = bat_iterator(b);

		bn1 = bn;
		bn2 = BATselect(g, NULL, &lastg, NULL, 1, 0, 0);
		if (bn2 == NULL) {
			BBPunfix(bn1->batCacheid);
			return GDK_FAIL;
		}
		bn3 = BATproject(bn2, s);
		BBPunfix(bn2->batCacheid);
		if (bn3 == NULL) {
			BBPunfix(bn1->batCacheid);
			return  GDK_FAIL;
		}
		bn4 = BATselect(b, bn3, BUNtail(bi, last - b->hseqbase), NULL, 1, 0, 0);
		BBPunfix(bn3->batCacheid);
		if (bn4 == NULL) {
			BBPunfix(bn1->batCacheid);
			return  GDK_FAIL;
		}
		bn = BATmergecand(bn1, bn4);
		BBPunfix(bn1->batCacheid);
		BBPunfix(bn4->batCacheid);
		if (bn == NULL)
			return GDK_FAIL;
	}
	if (gids) {
		BAT *bn1, *bn2, *bn3, *bn4, *bn5, *bn6, *bn7, *bn8;

		if (BATsemijoin(&bn1, NULL, s, bn, NULL, NULL, 0, BUN_NONE) != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			return  GDK_FAIL;
		}
		bn2 = BATproject(bn1, g);
		BBPunfix(bn1->batCacheid);
		if (bn2 == NULL) {
			BBPunfix(bn->batCacheid);
			return GDK_FAIL;
		}
		bn3 = BATproject(bn, b);
		if (bn3 == NULL) {
			BBPunfix(bn2->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsort(NULL, &bn4, &bn5, bn2, NULL, NULL, 0, 0);
		BBPunfix(bn2->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			BBPunfix(bn3->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsort(NULL, &bn6, &bn7, bn3, bn4, bn5, !asc, 0);
		BBPunfix(bn3->batCacheid);
		BBPunfix(bn4->batCacheid);
		BBPunfix(bn5->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsort(NULL, &bn8, NULL, bn6, NULL, NULL, 0, 0);
		BBPunfix(bn6->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			BBPunfix(bn7->batCacheid);
			return GDK_FAIL;
		}
		gn = BATproject(bn8, bn7);
		BBPunfix(bn7->batCacheid);
		BBPunfix(bn8->batCacheid);
		if (gn == NULL) {
			BBPunfix(bn->batCacheid);
			return GDK_FAIL;
		}
		*gids = gn;
		assert(BATcount(gn) == BATcount(bn));
	}
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

	/* if g specified, then so must s */
	assert(g == NULL || s != NULL);
	/* g and s must be aligned (same size, same hseqbase) */
	assert(g == NULL || BATcount(s) == BATcount(g));
	assert(g == NULL || BATcount(g) == 0 || s->hseqbase == g->hseqbase);

	if (n == 0 || BATcount(b) == 0 || (s != NULL && BATcount(s) == 0)) {
		/* trivial: empty result */
		*topn = BATdense(0, 0, 0);
		if (*topn == NULL)
			return GDK_FAIL;
		if (gids) {
			*gids = BATdense(0, 0, 0);
			if (*gids == NULL) {
				BBPreclaim(*topn);
				return GDK_FAIL;
			}
		}
		return GDK_SUCCEED;
	}

	if (g == NULL) {
		if (gids == NULL && !distinct) {
			*topn = BATfirstn_unique(b, s, n, asc, NULL);
			return *topn ? GDK_SUCCEED : GDK_FAIL;
		}
		return BATfirstn_grouped(topn, gids, b, s, n, asc, distinct);
	}
	if (gids == NULL && !distinct) {
		*topn = BATfirstn_unique_with_groups(b, s, g, n, asc, NULL, NULL);
		return *topn ? GDK_SUCCEED : GDK_FAIL;
	}
	return BATfirstn_grouped_with_groups(topn, gids, b, s, g, n, asc, distinct);
}
