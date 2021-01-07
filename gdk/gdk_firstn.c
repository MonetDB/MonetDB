/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

/* BATfirstn select the smallest n elements from the input bat b (if
 * asc(ending) is set, else the largest n elements).  Conceptually, b
 * is sorted in ascending or descending order (depending on the asc
 * argument) and then the OIDs of the first n elements are returned.
 * If there are NILs in the BAT, their relative ordering is set by
 * using the nilslast argument: if set, NILs come last (largest value
 * when ascending, smallest value when descending), so if there are
 * enough non-NIL values, no NILs will be returned.  If unset (false),
 * NILs come first and will be returned.
 *
 * In addition to the input BAT b, there can be a standard candidate
 * list s.  If s is specified (non-NULL), only elements in b that are
 * referred to in s are considered.
 *
 * If the third input bat g is non-NULL, then s must also be non-NULL
 * and must be aligned with g.  G then specifies groups to which the
 * elements referred to in s belong.  Conceptually, the group values
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
 *      BATfirstn(&s1, &g1, b1, NULL, NULL, n, asc, nilslast, distinct);
 *      BATfirstn(&s2, &g2, b2, s1, g1, n, asc, nilslast, distinct);
 *      BATfirstn(&s3, NULL, b3, s2, g2, n, asc, nilslast, distinct);
 * If the input BATs b1, b2, b3 are large enough, s3 will contain the
 * OIDs of the smallest (largest) n elements in the table consisting
 * of the columns b1, b2, b3 when ordered in ascending order with b1
 * the major key.
 */

/* We use a binary heap for the implementation of the simplest form of
 * first-N.  During processing, the oids list forms a heap with the
 * root at position 0 and the children of a node at position i at
 * positions 2*i+1 and 2*i+2.  The parent node is always
 * smaller/larger (depending on the value of asc) than its children
 * (recursively).  The heapify macro creates the heap from the input
 * in-place.  We start off with a heap containing the first N elements
 * of the input, and then go over the rest of the input, replacing the
 * root of the heap with a new value if appropriate (if the new value
 * is among the first-N seen so far).  The siftdown macro then
 * restores the heap property. */
#define siftdown(OPER, START, SWAP)					\
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
			siftdown(OPER, i - 1, SWAP);	\
	} while (0)

/* we inherit LT and GT from gdk_calc_private.h */

#define nLTbte(a, b)	(!is_bte_nil(b) && (is_bte_nil(a) || (a) < (b)))
#define nLTsht(a, b)	(!is_sht_nil(b) && (is_sht_nil(a) || (a) < (b)))
#define nLTint(a, b)	(!is_int_nil(b) && (is_int_nil(a) || (a) < (b)))
#define nLTlng(a, b)	(!is_lng_nil(b) && (is_lng_nil(a) || (a) < (b)))
#define nLThge(a, b)	(!is_hge_nil(b) && (is_hge_nil(a) || (a) < (b)))

#define nGTbte(a, b)	(!is_bte_nil(b) && (is_bte_nil(a) || (a) > (b)))
#define nGTsht(a, b)	(!is_sht_nil(b) && (is_sht_nil(a) || (a) > (b)))
#define nGTint(a, b)	(!is_int_nil(b) && (is_int_nil(a) || (a) > (b)))
#define nGTlng(a, b)	(!is_lng_nil(b) && (is_lng_nil(a) || (a) > (b)))
#define nGThge(a, b)	(!is_hge_nil(b) && (is_hge_nil(a) || (a) > (b)))

#define LTany(p1, p2)	(cmp(BUNtail(bi, oids[p1] - b->hseqbase),	\
			     BUNtail(bi, oids[p2] - b->hseqbase)) < 0)
#define GTany(p1, p2)	(cmp(BUNtail(bi, oids[p1] - b->hseqbase),	\
			     BUNtail(bi, oids[p2] - b->hseqbase)) > 0)

#define nLTany(p1, p2)	(cmp(BUNtail(bi, oids[p1] - b->hseqbase), nil) != 0 \
			 && (cmp(BUNtail(bi, oids[p2] - b->hseqbase), nil) == 0	\
			     || cmp(BUNtail(bi, oids[p1] - b->hseqbase), \
				    BUNtail(bi, oids[p2] - b->hseqbase)) < 0))
#define nGTany(p1, p2)	(cmp(BUNtail(bi, oids[p2] - b->hseqbase), nil) != 0 \
			 && (cmp(BUNtail(bi, oids[p1] - b->hseqbase), nil) == 0	\
			     || cmp(BUNtail(bi, oids[p1] - b->hseqbase), \
				    BUNtail(bi, oids[p2] - b->hseqbase)) > 0))

#define LTflt(a, b)	(!is_flt_nil(b) && (is_flt_nil(a) || (a) < (b)))
#define LTdbl(a, b)	(!is_dbl_nil(b) && (is_dbl_nil(a) || (a) < (b)))
#define GTflt(a, b)	(!is_flt_nil(a) && (is_flt_nil(b) || (a) > (b)))
#define GTdbl(a, b)	(!is_dbl_nil(a) && (is_dbl_nil(b) || (a) > (b)))

#define nLTflt(a, b)	(!is_flt_nil(a) && (is_flt_nil(b) || (a) < (b)))
#define nLTdbl(a, b)	(!is_dbl_nil(a) && (is_dbl_nil(b) || (a) < (b)))
#define nGTflt(a, b)	(!is_flt_nil(b) && (is_flt_nil(a) || (a) > (b)))
#define nGTdbl(a, b)	(!is_dbl_nil(b) && (is_dbl_nil(a) || (a) > (b)))

#define LTfltfix(p1, p2)	LTflt(vals[oids[p1] - b->hseqbase],	\
				      vals[oids[p2] - b->hseqbase])
#define GTfltfix(p1, p2)	GTflt(vals[oids[p1] - b->hseqbase],	\
				      vals[oids[p2] - b->hseqbase])
#define LTdblfix(p1, p2)	LTdbl(vals[oids[p1] - b->hseqbase],	\
				      vals[oids[p2] - b->hseqbase])
#define GTdblfix(p1, p2)	GTdbl(vals[oids[p1] - b->hseqbase],	\
				      vals[oids[p2] - b->hseqbase])
#define LTfix(p1, p2)		LT(vals[oids[p1] - b->hseqbase],	\
				   vals[oids[p2] - b->hseqbase])
#define GTfix(p1, p2)		GT(vals[oids[p1] - b->hseqbase],	\
				   vals[oids[p2] - b->hseqbase])

#define nLTfltfix(p1, p2)	nLTflt(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nGTfltfix(p1, p2)	nGTflt(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nLTdblfix(p1, p2)	nLTdbl(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nGTdblfix(p1, p2)	nGTdbl(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nLTbtefix(p1, p2)	nLTbte(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nGTbtefix(p1, p2)	nGTbte(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nLTshtfix(p1, p2)	nLTsht(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nGTshtfix(p1, p2)	nGTsht(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nLTintfix(p1, p2)	nLTint(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nGTintfix(p1, p2)	nGTint(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nLTlngfix(p1, p2)	nLTlng(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nGTlngfix(p1, p2)	nGTlng(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nLThgefix(p1, p2)	nLThge(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])
#define nGThgefix(p1, p2)	nGThge(vals[oids[p1] - b->hseqbase],	\
				       vals[oids[p2] - b->hseqbase])

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
		while (cnt > 0) {					\
			cnt--;						\
			i = canditer_next(&ci);				\
			if (OP(vals[i - b->hseqbase],			\
			       vals[oids[0] - b->hseqbase])) {		\
				oids[0] = i;				\
				siftdown(OP##fix, 0, SWAP1);		\
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
BATfirstn_unique(BAT *b, BAT *s, BUN n, bool asc, bool nilslast, oid *lastp, lng t0)
{
	BAT *bn;
	BATiter bi = bat_iterator(b);
	oid *restrict oids;
	BUN i, cnt;
	struct canditer ci;
	int tpe = b->ttype;
	int (*cmp)(const void *, const void *);
	const void *nil;
	/* variables used in heapify/siftdown macros */
	oid item;
	BUN pos, childpos;

	MT_thread_setalgorithm(__func__);
	cnt = canditer_init(&ci, b, s);

	if (n >= cnt) {
		/* trivial: return all candidates */
		if (lastp)
			*lastp = 0;
		bn = canditer_slice(&ci, 0, ci.ncand);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",n=" BUNFMT " -> " ALGOOPTBATFMT
			  " (trivial -- " LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	if (BATtvoid(b)) {
		/* nilslast doesn't make a difference: either all are
		 * nil, or none are */
		if (asc || is_oid_nil(b->tseqbase)) {
			/* return the first part of the candidate list
			 * or of the BAT itself */
			bn = canditer_slice(&ci, 0, n);
			if (bn && lastp)
				*lastp = BUNtoid(bn, n - 1);
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
				  ",n=" BUNFMT " -> " ALGOOPTBATFMT
				  " (initial slice -- " LLFMT " usec)\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s), n,
				  ALGOOPTBATPAR(bn), GDKusec() - t0);
			return bn;
		}
		/* return the last part of the candidate list or of
		 * the BAT itself */
		bn = canditer_slice(&ci, cnt - n, cnt);
		if (bn && lastp)
			*lastp = BUNtoid(bn, 0);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",n=" BUNFMT " -> " ALGOOPTBATFMT
			  " (final slice -- " LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}
	/* note, we want to do both calls */
	if (BATordered(b) | BATordered_rev(b)) {
		/* trivial: b is sorted so we just need to return the
		 * initial or final part of it (or of the candidate
		 * list); however, if nilslast == asc, then the nil
		 * values (if any) are in the wrong place, so we need
		 * to do a little more work */

		/* after we create the to-be-returned BAT, we set pos
		 * to the BUN in the new BAT whose value we should
		 * return through *lastp */
		if (nilslast == asc && !b->tnonil) {
			pos = SORTfndlast(b, ATOMnilptr(tpe));
			pos = canditer_search(&ci, b->hseqbase + pos, true);
			/* 0 <= pos <= cnt
			 * 0 < n < cnt
			 */
			if (b->tsorted) {
				/* [0..pos) -- nil
				 * [pos..cnt) -- non-nil <<<
				 */
				if (asc) { /* i.e. nilslast */
					/* prefer non-nil and
					 * smallest */
					if (cnt - pos < n) {
						bn = canditer_slice(&ci, cnt - n, cnt);
						pos = 0;
					} else {
						bn = canditer_slice(&ci, pos, pos + n);
						pos = n - 1;
					}
				} else { /* i.e. !asc, !nilslast */
					/* prefer nil and largest */
					if (pos < n) {
						bn = canditer_slice2(&ci, 0, pos, cnt - (n - pos), cnt);
						/* pos = pos; */
					} else {
						bn = canditer_slice(&ci, 0, n);
						pos = 0;
					}
				}
			} else { /* i.e. trevsorted */
				/* [0..pos) -- non-nil >>>
				 * [pos..cnt) -- nil
				 */
				if (asc) { /* i.e. nilslast */
					/* prefer non-nil and
					 * smallest */
					if (pos < n) {
						bn = canditer_slice(&ci, 0, n);
						/* pos = pos; */
					} else {
						bn = canditer_slice(&ci, pos - n, pos);
						pos = 0;
					}
				} else { /* i.e. !asc, !nilslast */
					/* prefer nil and largest */
					if (cnt - pos < n) {
						bn = canditer_slice2(&ci, 0, n - (cnt - pos), pos, cnt);
						pos = n - (cnt - pos) - 1;
					} else {
						bn = canditer_slice(&ci, pos, pos + n);
						pos = 0;
					}
				}
			}
		} else {
			/* either there are no nils, or they are in
			 * the appropriate position already, so we can
			 * just slice */
			if (asc ? b->tsorted : b->trevsorted) {
				/* return copy of first part of
				 * candidate list */
				bn = canditer_slice(&ci, 0, n);
				pos = n - 1;
			} else {
				/* return copy of last part of
				 * candidate list */
				bn = canditer_slice(&ci, cnt - n, cnt);
				pos = 0;
			}
		}
		if (bn && lastp)
			*lastp = BUNtoid(bn, pos);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",n=" BUNFMT " -> " ALGOOPTBATFMT
			  " (ordered -- " LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	bn = COLnew(0, TYPE_oid, n, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, n);
	oids = (oid *) Tloc(bn, 0);
	cmp = ATOMcompare(tpe);
	nil = ATOMnilptr(tpe);
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	tpe = ATOMbasetype(tpe); /* takes care of oid */
	/* if the input happens to be almost sorted in ascending order
	 * (likely a common use case), it is more efficient to start
	 * off with the first n elements when doing a firstn-ascending
	 * and to start off with the last n elements when doing a
	 * firstn-descending so that most values that we look at after
	 * this will be skipped.  However, when using a bitmask
	 * candidate list, the manipulation of the canditer structure
	 * doesn't work like this, so we still work from the
	 * beginning. */
	if (asc || ci.tpe == cand_mask) {
		for (i = 0; i < n; i++)
			oids[i] = canditer_next(&ci);
	} else {
		item = canditer_idx(&ci, cnt - n);
		ci.next = cnt - n;
		ci.add = item - ci.seq - (cnt - n);
		for (i = n; i > 0; i--)
			oids[i - 1] = canditer_next(&ci);
		canditer_reset(&ci);
	}
	cnt -= n;

	if (asc) {
		if (nilslast && !b->tnonil) {
			switch (tpe) {
			case TYPE_bte:
				shuffle_unique(bte, nLTbte);
				break;
			case TYPE_sht:
				shuffle_unique(sht, nLTsht);
				break;
			case TYPE_int:
				shuffle_unique(int, nLTint);
				break;
			case TYPE_lng:
				shuffle_unique(lng, nLTlng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				shuffle_unique(hge, nLThge);
				break;
#endif
			case TYPE_flt:
				shuffle_unique(flt, nLTflt);
				break;
			case TYPE_dbl:
				shuffle_unique(dbl, nLTdbl);
				break;
			default:
				heapify(nLTany, SWAP1);
				while (cnt > 0) {
					cnt--;
					i = canditer_next(&ci);
					if (cmp(BUNtail(bi, i - b->hseqbase), nil) != 0
					    && (cmp(BUNtail(bi, oids[0] - b->hseqbase), nil) == 0
						|| cmp(BUNtail(bi, i - b->hseqbase),
						       BUNtail(bi, oids[0] - b->hseqbase)) < 0)) {
						oids[0] = i;
						siftdown(nLTany, 0, SWAP1);
					}
				}
				break;
			}
		} else {
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
				shuffle_unique(flt, LTflt);
				break;
			case TYPE_dbl:
				shuffle_unique(dbl, LTdbl);
				break;
			default:
				heapify(LTany, SWAP1);
				while (cnt > 0) {
					cnt--;
					i = canditer_next(&ci);
					if (cmp(BUNtail(bi, i - b->hseqbase),
						BUNtail(bi, oids[0] - b->hseqbase)) < 0) {
						oids[0] = i;
						siftdown(LTany, 0, SWAP1);
					}
				}
				break;
			}
		}
	} else {
		if (nilslast || b->tnonil) {
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
				shuffle_unique(flt, GTflt);
				break;
			case TYPE_dbl:
				shuffle_unique(dbl, GTdbl);
				break;
			default:
				heapify(GTany, SWAP1);
				while (cnt > 0) {
					cnt--;
					i = canditer_next(&ci);
					if (cmp(BUNtail(bi, i - b->hseqbase),
						BUNtail(bi, oids[0] - b->hseqbase)) > 0) {
						oids[0] = i;
						siftdown(GTany, 0, SWAP1);
					}
				}
				break;
			}
		} else {
			switch (tpe) {
			case TYPE_bte:
				shuffle_unique(bte, nGTbte);
				break;
			case TYPE_sht:
				shuffle_unique(sht, nGTsht);
				break;
			case TYPE_int:
				shuffle_unique(int, nGTint);
				break;
			case TYPE_lng:
				shuffle_unique(lng, nGTlng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				shuffle_unique(hge, nGThge);
				break;
#endif
			case TYPE_flt:
				shuffle_unique(flt, nGTflt);
				break;
			case TYPE_dbl:
				shuffle_unique(dbl, nGTdbl);
				break;
			default:
				heapify(nGTany, SWAP1);
				while (cnt > 0) {
					cnt--;
					i = canditer_next(&ci);
					if (cmp(BUNtail(bi, oids[0] - b->hseqbase), nil) != 0
					    && (cmp(BUNtail(bi, i - b->hseqbase), nil) == 0
						|| cmp(BUNtail(bi, i - b->hseqbase),
						       BUNtail(bi, oids[0] - b->hseqbase)) > 0)) {
						oids[0] = i;
						siftdown(nGTany, 0, SWAP1);
					}
				}
				break;
			}
		}
	}
	if (lastp)
		*lastp = oids[0]; /* store id of largest value */
	/* output must be sorted since it's a candidate list */
	GDKqsort(oids, NULL, NULL, (size_t) n, sizeof(oid), 0, TYPE_oid, false, false);
	bn->tsorted = true;
	bn->trevsorted = n <= 1;
	bn->tkey = true;
	bn->tseqbase = n <= 1 ? oids[0] : oid_nil;
	bn->tnil = false;
	bn->tnonil = true;
	bn = virtualize(bn);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  ",n=" BUNFMT " -> " ALGOOPTBATFMT
		  " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s), n,
		  ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;
}

#define LTfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  LTfix(p1, p2)))
#define nLTbtefixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nLTbtefix(p1, p2)))
#define nLTshtfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nLTshtfix(p1, p2)))
#define nLTintfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nLTintfix(p1, p2)))
#define nLTlngfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nLTlngfix(p1, p2)))
#define nLThgefixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nLThgefix(p1, p2)))
#define LTfltfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  LTfltfix(p1, p2)))
#define LTdblfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  LTdblfix(p1, p2)))
#define nLTfltfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nLTfltfix(p1, p2)))
#define nLTdblfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nLTdblfix(p1, p2)))
#define GTfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  GTfix(p1, p2)))
#define nGTbtefixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nGTbtefix(p1, p2)))
#define nGTshtfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nGTshtfix(p1, p2)))
#define nGTintfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nGTintfix(p1, p2)))
#define nGTlngfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nGTlngfix(p1, p2)))
#define nGThgefixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nGThgefix(p1, p2)))
#define GTfltfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  GTfltfix(p1, p2)))
#define GTdblfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  GTdblfix(p1, p2)))
#define nGTfltfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nGTfltfix(p1, p2)))
#define nGTdblfixgrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nGTdblfix(p1, p2)))
#define LTvoidgrp(p1, p2)					\
	(goids[p1] < goids[p2] ||				\
	 (goids[p1] == goids[p2] && oids[p1] < oids[p2]))
#define GTvoidgrp(p1, p2)					\
	(goids[p1] < goids[p2] ||				\
	 (goids[p1] == goids[p2] && oids[p1] > oids[p2]))
#define LTanygrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  LTany(p1, p2)))
#define GTanygrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  GTany(p1, p2)))
#define nLTanygrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nLTany(p1, p2)))
#define nGTanygrp(p1, p2)			\
	(goids[p1] < goids[p2] ||		\
	 (goids[p1] == goids[p2] &&		\
	  nGTany(p1, p2)))
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
		while (cnt > 0) {					\
			cnt--;						\
			i = canditer_next(&ci);				\
			if (gv[j] < goids[0] ||				\
			    (gv[j] == goids[0] &&			\
			     OP(vals[i - b->hseqbase],			\
				vals[oids[0] - b->hseqbase]))) {	\
				oids[0] = i;				\
				goids[0] = gv[j];			\
				siftdown(OP##fixgrp, 0, SWAP2);		\
			}						\
			j++;						\
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
BATfirstn_unique_with_groups(BAT *b, BAT *s, BAT *g, BUN n, bool asc, bool nilslast, oid *lastp, oid *lastgp, lng t0)
{
	BAT *bn;
	BATiter bi = bat_iterator(b);
	oid *restrict oids, *restrict goids;
	const oid *restrict gv;
	BUN i, j, cnt;
	struct canditer ci;
	int tpe = b->ttype;
	int (*cmp)(const void *, const void *);
	const void *nil;
	/* variables used in heapify/siftdown macros */
	oid item;
	BUN pos, childpos;

	MT_thread_setalgorithm(__func__);
	cnt = canditer_init(&ci, b, s);

	if (n > cnt)
		n = cnt;

	if (n == 0) {
		/* candidate list might refer only to values outside
		 * of the bat and hence be effectively empty */
		if (lastp)
			*lastp = 0;
		if (lastgp)
			*lastgp = 0;
		bn = BATdense(0, 0, 0);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",g=" ALGOBATFMT ",n=" BUNFMT " -> " ALGOOPTBATFMT
			  " (empty -- " LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	if (BATtdense(g)) {
		/* trivial: g determines ordering, return reference to
		 * initial part of b (or slice of s) */
		if (lastgp)
			*lastgp = g->tseqbase + n - 1;
		bn = canditer_slice(&ci, 0, n);
		if (bn && lastp)
			*lastp = BUNtoid(bn, n - 1);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",g=" ALGOBATFMT ",n=" BUNFMT " -> " ALGOOPTBATFMT
			  " (dense group -- " LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
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

	cmp = ATOMcompare(tpe);
	nil = ATOMnilptr(tpe);
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	tpe = ATOMbasetype(tpe); /* takes care of oid */
	j = 0;
	for (i = 0; i < n; i++) {
		oids[i] = canditer_next(&ci);
		goids[i] = gv[j++];
	}
	cnt -= n;

	if (BATtvoid(b)) {
		/* nilslast doesn't make a difference (all nil, or no nil) */
		if (asc) {
			heapify(LTvoidgrp, SWAP2);
			while (cnt > 0) {
				cnt--;
				i = canditer_next(&ci);
				if (gv[j] < goids[0]
				    /* || (gv[j] == goids[0]
					&& i < oids[0]) -- always false */) {
					oids[0] = i;
					goids[0] = gv[j];
					siftdown(LTvoidgrp, 0, SWAP2);
				}
				j++;
			}
		} else {
			heapify(GTvoidgrp, SWAP2);
			while (cnt > 0) {
				cnt--;
				i = canditer_next(&ci);
				if (gv[j] < goids[0]
				    || (gv[j] == goids[0]
				        /* && i > oids[0] -- always true */)) {
					oids[0] = i;
					goids[0] = gv[j];
					siftdown(GTvoidgrp, 0, SWAP2);
				}
				j++;
			}
		}
	} else if (asc) {
		if (nilslast && !b->tnonil) {
			switch (tpe) {
			case TYPE_bte:
				shuffle_unique_with_groups(bte, nLTbte);
				break;
			case TYPE_sht:
				shuffle_unique_with_groups(sht, nLTsht);
				break;
			case TYPE_int:
				shuffle_unique_with_groups(int, nLTint);
				break;
			case TYPE_lng:
				shuffle_unique_with_groups(lng, nLTlng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				shuffle_unique_with_groups(hge, nLThge);
				break;
#endif
			case TYPE_flt:
				shuffle_unique_with_groups(flt, nLTflt);
				break;
			case TYPE_dbl:
				shuffle_unique_with_groups(dbl, nLTdbl);
				break;
			default:
				heapify(nLTanygrp, SWAP2);
				while (cnt > 0) {
					cnt--;
					i = canditer_next(&ci);
					if (gv[j] < goids[0]
					    || (gv[j] == goids[0]
						&& cmp(BUNtail(bi, i - b->hseqbase), nil) != 0
						&& (cmp(BUNtail(bi, oids[0] - b->hseqbase), nil) == 0
						    || cmp(BUNtail(bi, i - b->hseqbase),
							   BUNtail(bi, oids[0] - b->hseqbase)) < 0))) {
						oids[0] = i;
						goids[0] = gv[j];
						siftdown(nLTanygrp, 0, SWAP2);
					}
					j++;
				}
				break;
			}
		} else {
			switch (tpe) {
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
				shuffle_unique_with_groups(flt, LTflt);
				break;
			case TYPE_dbl:
				shuffle_unique_with_groups(dbl, LTdbl);
				break;
			default:
				heapify(LTanygrp, SWAP2);
				while (cnt > 0) {
					cnt--;
					i = canditer_next(&ci);
					if (gv[j] < goids[0] ||
					    (gv[j] == goids[0] &&
					     cmp(BUNtail(bi, i - b->hseqbase),
						 BUNtail(bi, oids[0] - b->hseqbase)) < 0)) {
						oids[0] = i;
						goids[0] = gv[j];
						siftdown(LTanygrp, 0, SWAP2);
					}
					j++;
				}
				break;
			}
		}
	} else if (nilslast || b->tnonil) {
		switch (tpe) {
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
			shuffle_unique_with_groups(flt, GTflt);
			break;
		case TYPE_dbl:
			shuffle_unique_with_groups(dbl, GTdbl);
			break;
		default:
			heapify(GTanygrp, SWAP2);
			while (cnt > 0) {
				cnt--;
				i = canditer_next(&ci);
				if (gv[j] < goids[0] ||
				    (gv[j] == goids[0] &&
				     cmp(BUNtail(bi, i - b->hseqbase),
					 BUNtail(bi, oids[0] - b->hseqbase)) > 0)) {
					oids[0] = i;
					goids[0] = gv[j];
					siftdown(GTanygrp, 0, SWAP2);
				}
				j++;
			}
			break;
		}
	} else {
		switch (tpe) {
		case TYPE_bte:
			shuffle_unique_with_groups(bte, nGTbte);
			break;
		case TYPE_sht:
			shuffle_unique_with_groups(sht, nGTsht);
			break;
		case TYPE_int:
			shuffle_unique_with_groups(int, nGTint);
			break;
		case TYPE_lng:
			shuffle_unique_with_groups(lng, nGTlng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			shuffle_unique_with_groups(hge, nGThge);
			break;
#endif
		case TYPE_flt:
			shuffle_unique_with_groups(flt, nGTflt);
			break;
		case TYPE_dbl:
			shuffle_unique_with_groups(dbl, nGTdbl);
			break;
		default:
			heapify(nGTanygrp, SWAP2);
			while (cnt > 0) {
				cnt--;
				i = canditer_next(&ci);
				if (gv[j] < goids[0]
				    || (gv[j] == goids[0]
					&& cmp(BUNtail(bi, oids[0] - b->hseqbase), nil) != 0
					&& (cmp(BUNtail(bi, i - b->hseqbase), nil) == 0
					    || cmp(BUNtail(bi, i - b->hseqbase),
						   BUNtail(bi, oids[0] - b->hseqbase)) > 0))) {
					oids[0] = i;
					goids[0] = gv[j];
					siftdown(nGTanygrp, 0, SWAP2);
				}
				j++;
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
	GDKqsort(oids, NULL, NULL, (size_t) n, sizeof(oid), 0, TYPE_oid, false, false);
	bn->tsorted = true;
	bn->trevsorted = n <= 1;
	bn->tkey = true;
	bn->tseqbase = n <= 1 ? oids[0] : oid_nil;
	bn->tnil = false;
	bn->tnonil = true;
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  ",g=" ALGOBATFMT ",n=" BUNFMT " -> " ALGOOPTBATFMT
		  " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
		  ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;
}

static gdk_return
BATfirstn_grouped(BAT **topn, BAT **gids, BAT *b, BAT *s, BUN n, bool asc, bool nilslast, bool distinct, lng t0)
{
	BAT *bn, *gn = NULL, *su = NULL;
	oid last;
	gdk_return rc;

	MT_thread_setalgorithm(__func__);
	if (distinct && !b->tkey) {
		su = s;
		s = BATunique(b, s);
		if (s == NULL)
			return GDK_FAIL;
	}
	bn = BATfirstn_unique(b, s, n, asc, nilslast, &last, t0);
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
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",n=" BUNFMT " -> " ALGOOPTBATFMT "," ALGOOPTBATFMT
			  " (empty -- " LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), n,
			  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn), GDKusec() - t0);
		return GDK_SUCCEED;
	}
	if (!b->tkey) {
		if (distinct) {
			BAT *bn1;

			bn1 = bn;
			BBPunfix(s->batCacheid);
			bn = BATintersect(b, b, su, bn1, true, false, BUN_NONE);
			BBPunfix(bn1->batCacheid);
			if (bn == NULL)
				return GDK_FAIL;
		} else {
			BATiter bi = bat_iterator(b);
			BAT *bn1, *bn2;

			bn1 = bn;
			bn2 = BATselect(b, s, BUNtail(bi, last - b->hseqbase), NULL, true, false, false);
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
		rc = BATsort(NULL, &bn2, &bn3, bn1, NULL, NULL, !asc, !asc, false);
		BBPunfix(bn1->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsort(NULL, &bn4, NULL, bn2, NULL, NULL, false, false, false);
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
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  ",n=" BUNFMT " -> " ALGOOPTBATFMT "," ALGOOPTBATFMT
		  " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s), n,
		  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn), GDKusec() - t0);
	return GDK_SUCCEED;
}

static gdk_return
BATfirstn_grouped_with_groups(BAT **topn, BAT **gids, BAT *b, BAT *s, BAT *g, BUN n, bool asc, bool nilslast, bool distinct, lng t0)
{
	BAT *bn, *gn = NULL;
	oid last, lastg;
	gdk_return rc;

	MT_thread_setalgorithm(__func__);
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
		bn4 = BATintersect(s, bn2, NULL, NULL, false, false, BUN_NONE);
		BBPunfix(bn2->batCacheid);
		if (bn4 == NULL) {
			BBPunfix(bn1->batCacheid);
			return GDK_FAIL;
		}
		bn5 = BATproject(bn4, g);
		BBPunfix(bn4->batCacheid);
		if (bn5 == NULL) {
			BBPunfix(bn1->batCacheid);
			return GDK_FAIL;
		}
		bn6 = BATfirstn_unique_with_groups(bn3, NULL, bn5, n, asc, nilslast, NULL, NULL, t0);
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
		bn = BATfirstn_unique_with_groups(b, s, g, n, asc, nilslast, &last, &lastg, t0);
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
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",g=" ALGOBATFMT ",n=" BUNFMT
			  " -> " ALGOOPTBATFMT "," ALGOOPTBATFMT
			  " (empty -- " LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
			  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn), GDKusec() - t0);
		return GDK_SUCCEED;
	}
	if (!distinct && !b->tkey) {
		BAT *bn1, *bn2, *bn3, *bn4;
		BATiter bi = bat_iterator(b);

		bn1 = bn;
		bn2 = BATselect(g, NULL, &lastg, NULL, true, false, false);
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
		bn4 = BATselect(b, bn3, BUNtail(bi, last - b->hseqbase), NULL, true, false, false);
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

		if ((bn1 = BATintersect(s, bn, NULL, NULL, false, false, BUN_NONE)) == NULL) {
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
		rc = BATsort(NULL, &bn4, &bn5, bn2, NULL, NULL, false, false, false);
		BBPunfix(bn2->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			BBPunfix(bn3->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsort(NULL, &bn6, &bn7, bn3, bn4, bn5, !asc, !asc, false);
		BBPunfix(bn3->batCacheid);
		BBPunfix(bn4->batCacheid);
		BBPunfix(bn5->batCacheid);
		if (rc != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			return GDK_FAIL;
		}
		rc = BATsort(NULL, &bn8, NULL, bn6, NULL, NULL, false, false, false);
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
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  ",g=" ALGOBATFMT ",n=" BUNFMT
		  " -> " ALGOOPTBATFMT "," ALGOOPTBATFMT
		  " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
		  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn), GDKusec() - t0);
	return GDK_SUCCEED;
}

gdk_return
BATfirstn(BAT **topn, BAT **gids, BAT *b, BAT *s, BAT *g, BUN n, bool asc, bool nilslast, bool distinct)
{
	lng t0 = 0;

	assert(topn != NULL);
	if (b == NULL) {
		*topn = NULL;
		return GDK_SUCCEED;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

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
			*topn = BATfirstn_unique(b, s, n, asc, nilslast, NULL, t0);
			return *topn ? GDK_SUCCEED : GDK_FAIL;
		}
		return BATfirstn_grouped(topn, gids, b, s, n, asc, nilslast, distinct, t0);
	}
	if (gids == NULL && !distinct) {
		*topn = BATfirstn_unique_with_groups(b, s, g, n, asc, nilslast, NULL, NULL, t0);
		return *topn ? GDK_SUCCEED : GDK_FAIL;
	}
	return BATfirstn_grouped_with_groups(topn, gids, b, s, g, n, asc, nilslast, distinct, t0);
}
