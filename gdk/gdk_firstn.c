/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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

#define nLTbte(a, b)	(!is_bte_nil(a) && (is_bte_nil(b) || (a) < (b)))
#define nLTsht(a, b)	(!is_sht_nil(a) && (is_sht_nil(b) || (a) < (b)))
#define nLTint(a, b)	(!is_int_nil(a) && (is_int_nil(b) || (a) < (b)))
#define nLTlng(a, b)	(!is_lng_nil(a) && (is_lng_nil(b) || (a) < (b)))
#define nLThge(a, b)	(!is_hge_nil(a) && (is_hge_nil(b) || (a) < (b)))

#define nGTbte(a, b)	(!is_bte_nil(b) && (is_bte_nil(a) || (a) > (b)))
#define nGTsht(a, b)	(!is_sht_nil(b) && (is_sht_nil(a) || (a) > (b)))
#define nGTint(a, b)	(!is_int_nil(b) && (is_int_nil(a) || (a) > (b)))
#define nGTlng(a, b)	(!is_lng_nil(b) && (is_lng_nil(a) || (a) > (b)))
#define nGThge(a, b)	(!is_hge_nil(b) && (is_hge_nil(a) || (a) > (b)))

#define LTany(p1, p2)	(cmp(BUNtail(*bi, oids[p1] - hseq),	\
			     BUNtail(*bi, oids[p2] - hseq)) < 0)
#define GTany(p1, p2)	(cmp(BUNtail(*bi, oids[p1] - hseq),	\
			     BUNtail(*bi, oids[p2] - hseq)) > 0)

#define nLTany(p1, p2)	(cmp(BUNtail(*bi, oids[p1] - hseq), nil) != 0 \
			 && (cmp(BUNtail(*bi, oids[p2] - hseq), nil) == 0	\
			     || cmp(BUNtail(*bi, oids[p1] - hseq), \
				    BUNtail(*bi, oids[p2] - hseq)) < 0))
#define nGTany(p1, p2)	(cmp(BUNtail(*bi, oids[p2] - hseq), nil) != 0 \
			 && (cmp(BUNtail(*bi, oids[p1] - hseq), nil) == 0	\
			     || cmp(BUNtail(*bi, oids[p1] - hseq), \
				    BUNtail(*bi, oids[p2] - hseq)) > 0))

#define LTflt(a, b)	(!is_flt_nil(b) && (is_flt_nil(a) || (a) < (b)))
#define LTdbl(a, b)	(!is_dbl_nil(b) && (is_dbl_nil(a) || (a) < (b)))
#define GTflt(a, b)	(!is_flt_nil(a) && (is_flt_nil(b) || (a) > (b)))
#define GTdbl(a, b)	(!is_dbl_nil(a) && (is_dbl_nil(b) || (a) > (b)))

#define nLTflt(a, b)	(!is_flt_nil(a) && (is_flt_nil(b) || (a) < (b)))
#define nLTdbl(a, b)	(!is_dbl_nil(a) && (is_dbl_nil(b) || (a) < (b)))
#define nGTflt(a, b)	(!is_flt_nil(b) && (is_flt_nil(a) || (a) > (b)))
#define nGTdbl(a, b)	(!is_dbl_nil(b) && (is_dbl_nil(a) || (a) > (b)))

#define LTfltfix(p1, p2)	LTflt(vals[oids[p1] - hseq],	\
				      vals[oids[p2] - hseq])
#define GTfltfix(p1, p2)	GTflt(vals[oids[p1] - hseq],	\
				      vals[oids[p2] - hseq])
#define LTdblfix(p1, p2)	LTdbl(vals[oids[p1] - hseq],	\
				      vals[oids[p2] - hseq])
#define GTdblfix(p1, p2)	GTdbl(vals[oids[p1] - hseq],	\
				      vals[oids[p2] - hseq])
#define LTfix(p1, p2)		LT(vals[oids[p1] - hseq],	\
				   vals[oids[p2] - hseq])
#define GTfix(p1, p2)		GT(vals[oids[p1] - hseq],	\
				   vals[oids[p2] - hseq])

#define nLTfltfix(p1, p2)	nLTflt(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nGTfltfix(p1, p2)	nGTflt(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nLTdblfix(p1, p2)	nLTdbl(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nGTdblfix(p1, p2)	nGTdbl(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nLTbtefix(p1, p2)	nLTbte(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nGTbtefix(p1, p2)	nGTbte(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nLTshtfix(p1, p2)	nLTsht(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nGTshtfix(p1, p2)	nGTsht(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nLTintfix(p1, p2)	nLTint(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nGTintfix(p1, p2)	nGTint(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nLTlngfix(p1, p2)	nLTlng(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nGTlngfix(p1, p2)	nGTlng(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nLThgefix(p1, p2)	nLThge(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])
#define nGThgefix(p1, p2)	nGThge(vals[oids[p1] - hseq],	\
				       vals[oids[p2] - hseq])

#define SWAP1(p1, p2)				\
	do {					\
		item = oids[p1];		\
		oids[p1] = oids[p2];		\
		oids[p2] = item;		\
	} while (0)

#define shuffle_unique(TYPE, OP)					\
	do {								\
		const TYPE *restrict vals = (const TYPE *) bi->base;	\
		heapify(OP##fix, SWAP1);				\
		TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {			\
			i = canditer_next(&ci);				\
			if (OP(vals[i - hseq],				\
			       vals[oids[0] - hseq])) {			\
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
BATfirstn_unique(BATiter *bi, BAT *s, BUN n, bool asc, bool nilslast, oid *lastp, lng t0)
{
	BAT *bn;
	oid *restrict oids;
	oid hseq = bi->b->hseqbase;
	BUN i;
	struct canditer ci;
	int tpe = bi->type;
	int (*cmp)(const void *, const void *);
	bool (*eq)(const void *, const void *);
	const void *nil;
	/* variables used in heapify/siftdown macros */
	oid item;
	BUN pos, childpos;

	MT_thread_setalgorithm(__func__);
	canditer_init(&ci, bi->b, s);

	if (n >= ci.ncand) {
		/* trivial: return all candidates */
		bn = canditer_slice(&ci, 0, ci.ncand);
		if (bn && lastp)
			*lastp = oid_nil;
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",n=" BUNFMT " -> " ALGOOPTBATFMT
			  " (trivial -- " LLFMT " usec)\n",
			  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	if (BATtvoid(bi->b)) {
		/* nilslast doesn't make a difference: either all are
		 * nil, or none are */
		if (asc || is_oid_nil(bi->tseq)) {
			/* return the first part of the candidate list
			 * or of the BAT itself */
			bn = canditer_slice(&ci, 0, n);
			if (bn && lastp)
				*lastp = BUNtoid(bn, n - 1);
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
				  ",n=" BUNFMT " -> " ALGOOPTBATFMT
				  " (initial slice -- " LLFMT " usec)\n",
				  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), n,
				  ALGOOPTBATPAR(bn), GDKusec() - t0);
			return bn;
		}
		/* return the last part of the candidate list or of
		 * the BAT itself */
		bn = canditer_slice(&ci, ci.ncand - n, ci.ncand);
		if (bn && lastp)
			*lastp = BUNtoid(bn, 0);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",n=" BUNFMT " -> " ALGOOPTBATFMT
			  " (final slice -- " LLFMT " usec)\n",
			  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}
	if (bi->sorted || bi->revsorted) {
		/* trivial: b is sorted so we just need to return the
		 * initial or final part of it (or of the candidate
		 * list); however, if nilslast == asc, then the nil
		 * values (if any) are in the wrong place, so we need
		 * to do a little more work */

		/* after we create the to-be-returned BAT, we set pos
		 * to the BUN in the new BAT whose value we should
		 * return through *lastp */
		if (nilslast == asc && !bi->nonil) {
			pos = SORTfndlast(bi->b, ATOMnilptr(tpe));
			pos = canditer_search(&ci, hseq + pos, true);
			/* 0 <= pos <= ci.ncand
			 * 0 < n < ci.ncand
			 */
			if (bi->sorted) {
				/* [0..pos) -- nil
				 * [pos..ci.ncand) -- non-nil <<<
				 */
				if (asc) { /* i.e. nilslast */
					/* prefer non-nil and
					 * smallest */
					if (ci.ncand - pos < n) {
						bn = canditer_slice(&ci, ci.ncand - n, ci.ncand);
						pos = 0;
					} else {
						bn = canditer_slice(&ci, pos, pos + n);
						pos = n - 1;
					}
				} else { /* i.e. !asc, !nilslast */
					/* prefer nil and largest */
					if (pos < n) {
						bn = canditer_slice2(&ci, 0, pos, ci.ncand - (n - pos), ci.ncand);
						/* pos = pos; */
					} else {
						bn = canditer_slice(&ci, 0, n);
						pos = 0;
					}
				}
			} else { /* i.e. trevsorted */
				/* [0..pos) -- non-nil >>>
				 * [pos..ci.ncand) -- nil
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
					if (ci.ncand - pos < n) {
						bn = canditer_slice2(&ci, 0, n - (ci.ncand - pos), pos, ci.ncand);
						pos = n - (ci.ncand - pos) - 1;
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
			if (asc ? bi->sorted : bi->revsorted) {
				/* return copy of first part of
				 * candidate list */
				bn = canditer_slice(&ci, 0, n);
				pos = n - 1;
			} else {
				/* return copy of last part of
				 * candidate list */
				bn = canditer_slice(&ci, ci.ncand - n, ci.ncand);
				pos = 0;
			}
		}
		if (bn && lastp)
			*lastp = BUNtoid(bn, pos);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",n=" BUNFMT " -> " ALGOOPTBATFMT
			  " (ordered -- " LLFMT " usec)\n",
			  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	bn = COLnew(0, TYPE_oid, n, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, n);
	oids = (oid *) Tloc(bn, 0);
	cmp = ATOMcompare(tpe);
	eq = ATOMequal(tpe);
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
		item = canditer_idx(&ci, ci.ncand - n);
		ci.next = ci.ncand - n;
		ci.add = item - ci.seq - (ci.ncand - n);
		for (i = n; i > 0; i--)
			oids[i - 1] = canditer_next(&ci);
		canditer_reset(&ci);
	}

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	if (asc) {
		if (nilslast && !bi->nonil) {
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
				TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
					i = canditer_next(&ci);
					if (!eq(BUNtail(*bi, i - hseq), nil)
					    && (eq(BUNtail(*bi, oids[0] - hseq), nil)
						|| cmp(BUNtail(*bi, i - hseq),
						       BUNtail(*bi, oids[0] - hseq)) < 0)) {
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
				TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
					i = canditer_next(&ci);
					if (cmp(BUNtail(*bi, i - hseq),
						BUNtail(*bi, oids[0] - hseq)) < 0) {
						oids[0] = i;
						siftdown(LTany, 0, SWAP1);
					}
				}
				break;
			}
		}
	} else {
		if (nilslast || bi->nonil) {
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
				TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
					i = canditer_next(&ci);
					if (cmp(BUNtail(*bi, i - hseq),
						BUNtail(*bi, oids[0] - hseq)) > 0) {
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
				TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
					i = canditer_next(&ci);
					if (!eq(BUNtail(*bi, oids[0] - hseq), nil)
					    && (eq(BUNtail(*bi, i - hseq), nil)
						|| cmp(BUNtail(*bi, i - hseq),
						       BUNtail(*bi, oids[0] - hseq)) > 0)) {
						oids[0] = i;
						siftdown(nGTany, 0, SWAP1);
					}
				}
				break;
			}
		}
	}
	TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
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
		  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), n,
		  ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;

  bailout:
	BBPreclaim(bn);
	return NULL;
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
		const TYPE *restrict vals = (const TYPE *) bi->base;	\
		heapify(OP##fixgrp, SWAP2);				\
		TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {			\
			i = canditer_next(&ci);				\
			if (gv[j] < goids[0] ||				\
			    (gv[j] == goids[0] &&			\
			     OP(vals[i - hseq],				\
				vals[oids[0] - hseq]))) {		\
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
BATfirstn_unique_with_groups(BATiter *bi, BAT *s, BAT *g, BUN n, bool asc, bool nilslast, oid *lastp, oid *lastgp, lng t0)
{
	BAT *bn;
	oid *restrict oids, *restrict goids;
	oid hseq = bi->b->hseqbase;
	const oid *restrict gv;
	BUN i, j;
	struct canditer ci;
	int tpe = bi->type;
	int (*cmp)(const void *, const void *);
	bool (*eq)(const void *, const void *);
	const void *nil;
	/* variables used in heapify/siftdown macros */
	oid item;
	BUN pos, childpos;

	MT_thread_setalgorithm(__func__);
	canditer_init(&ci, bi->b, s);

	if (n > ci.ncand)
		n = ci.ncand;

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
			  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
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
			  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	bn = COLnew(0, TYPE_oid, n, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, n);
	oids = (oid *) Tloc(bn, 0);
	gv = (const oid *) Tloc(g, 0);
	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);
	goids = ma_alloc(ta, n * sizeof(oid));
	if (goids == NULL) {
		BBPreclaim(bn);
		ma_close(&ta_state);
		return NULL;
	}

	cmp = ATOMcompare(tpe);
	eq = ATOMequal(tpe);
	nil = ATOMnilptr(tpe);
	/* if base type has same comparison function as type itself, we
	 * can use the base type */
	tpe = ATOMbasetype(tpe); /* takes care of oid */
	j = 0;
	for (i = 0; i < n; i++) {
		oids[i] = canditer_next(&ci);
		goids[i] = gv[j++];
	}

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	if (BATtvoid(bi->b)) {
		/* nilslast doesn't make a difference (all nil, or no nil) */
		if (asc) {
			heapify(LTvoidgrp, SWAP2);
			TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
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
			TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
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
		if (nilslast && !bi->nonil) {
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
				TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
					i = canditer_next(&ci);
					if (gv[j] < goids[0]
					    || (gv[j] == goids[0]
						&& !eq(BUNtail(*bi, i - hseq), nil)
						&& (eq(BUNtail(*bi, oids[0] - hseq), nil)
						    || cmp(BUNtail(*bi, i - hseq),
							   BUNtail(*bi, oids[0] - hseq)) < 0))) {
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
				TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
					i = canditer_next(&ci);
					if (gv[j] < goids[0] ||
					    (gv[j] == goids[0] &&
					     cmp(BUNtail(*bi, i - hseq),
						 BUNtail(*bi, oids[0] - hseq)) < 0)) {
						oids[0] = i;
						goids[0] = gv[j];
						siftdown(LTanygrp, 0, SWAP2);
					}
					j++;
				}
				break;
			}
		}
	} else if (nilslast || bi->nonil) {
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
			TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
				i = canditer_next(&ci);
				if (gv[j] < goids[0] ||
				    (gv[j] == goids[0] &&
				     cmp(BUNtail(*bi, i - hseq),
					 BUNtail(*bi, oids[0] - hseq)) > 0)) {
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
			TIMEOUT_LOOP(ci.ncand - n, qry_ctx) {
				i = canditer_next(&ci);
				if (gv[j] < goids[0]
				    || (gv[j] == goids[0]
					&& !eq(BUNtail(*bi, oids[0] - hseq), nil)
					&& (eq(BUNtail(*bi, i - hseq), nil)
					    || cmp(BUNtail(*bi, i - hseq),
						   BUNtail(*bi, oids[0] - hseq)) > 0))) {
					oids[0] = i;
					goids[0] = gv[j];
					siftdown(nGTanygrp, 0, SWAP2);
				}
				j++;
			}
			break;
		}
	}
	TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
	if (lastp)
		*lastp = oids[0];
	if (lastgp)
		*lastgp = goids[0];
	ma_close(&ta_state);
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
		  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
		  ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;

  bailout:
	ma_close(&ta_state);
	BBPreclaim(bn);
	return NULL;
}

static gdk_return
BATfirstn_grouped(BAT **topn, BAT **gids, BATiter *bi, BAT *s, BUN n, bool asc, bool nilslast, bool distinct, lng t0)
{
	BAT *bn, *gn = NULL, *su = NULL;
	oid last;
	gdk_return rc;

	MT_thread_setalgorithm(__func__);
	if (distinct && !bi->key) {
		su = s;
		s = BATunique(bi->b, s);
		if (s == NULL)
			return GDK_FAIL;
	}
	bn = BATfirstn_unique(bi, s, n, asc, nilslast, &last, t0);
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
			  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), n,
			  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn), GDKusec() - t0);
		return GDK_SUCCEED;
	}
	if (!bi->key) {
		if (distinct) {
			BAT *bn1;

			bn1 = bn;
			BBPunfix(s->batCacheid);
			bn = BATintersect(bi->b, bi->b, su, bn1, true, false, BUN_NONE);
			BBPunfix(bn1->batCacheid);
			if (bn == NULL)
				return GDK_FAIL;
		} else if (last != oid_nil) {
			BAT *bn1, *bn2;

			bn1 = bn;
			bn2 = BATselect(bi->b, s, BUNtail(*bi, last - bi->b->hseqbase), NULL, true, false, false, false);
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
		bn1 = BATproject(bn, bi->b);
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
		  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), n,
		  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn), GDKusec() - t0);
	return GDK_SUCCEED;
}

static gdk_return
BATfirstn_grouped_with_groups(BAT **topn, BAT **gids, BATiter *bi, BAT *s, BAT *g, BUN n, bool asc, bool nilslast, bool distinct, lng t0)
{
	BAT *bn, *gn = NULL;
	oid hseq = bi->b->hseqbase;
	oid last, lastg;
	gdk_return rc;

	MT_thread_setalgorithm(__func__);
	if (distinct) {
		BAT *bn1, *bn2, *bn3, *bn4, *bn5, *bn6, *bn7;
		if (BATgroup(&bn1, &bn2, NULL, bi->b, s, g, NULL, NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		bn3 = BATproject(bn2, bi->b);
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
		BATiter bn3i = bat_iterator(bn3);
		bn6 = BATfirstn_unique_with_groups(&bn3i, NULL, bn5, n, asc, nilslast, NULL, NULL, t0);
		bat_iterator_end(&bn3i);
		BBPunfix(bn3->batCacheid);
		BBPunfix(bn5->batCacheid);
		if (bn6 == NULL) {
			BBPunfix(bn1->batCacheid);
			return GDK_FAIL;
		}
		rc = BATleftjoin(&bn7, NULL, bn1, bn6, NULL, NULL, false, BUN_NONE);
		BBPunfix(bn6->batCacheid);
		if (rc != GDK_SUCCEED)
			return GDK_FAIL;
		bn = BATproject(bn7, s);
		BBPunfix(bn7->batCacheid);
		if (bn == NULL)
			return GDK_FAIL;
	} else {
		bn = BATfirstn_unique_with_groups(bi, s, g, n, asc, nilslast, &last, &lastg, t0);
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
			  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
			  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn), GDKusec() - t0);
		return GDK_SUCCEED;
	}
	if (!distinct && !bi->key) {
		BAT *bn1, *bn2, *bn3, *bn4;

		bn1 = bn;
		bn2 = BATselect(g, NULL, &lastg, NULL, true, false, false, false);
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
		bn4 = BATselect(bi->b, bn3, BUNtail(*bi, last - hseq), NULL, true, false, false, false);
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
		bn3 = BATproject(bn, bi->b);
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
		  ALGOBATPAR(bi->b), ALGOOPTBATPAR(s), ALGOBATPAR(g), n,
		  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn), GDKusec() - t0);
	return GDK_SUCCEED;
}

gdk_return
BATfirstn(BAT **topn, BAT **gids, BAT *b, BAT *s, BAT *g, BUN n, bool asc, bool nilslast, bool distinct)
{
	lng t0 = 0;
	gdk_return rc = GDK_SUCCEED;

	assert(topn != NULL);
	if (b == NULL) {
		*topn = NULL;
		return GDK_SUCCEED;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	(void) BATordered(b);
	(void) BATordered_rev(b);

	BATiter bi = bat_iterator(b);

	/* if g specified, then so must s */
	assert(g == NULL || s != NULL);
	/* g and s must be aligned (same size, same hseqbase) */
	assert(g == NULL || BATcount(s) == BATcount(g));
	assert(g == NULL || BATcount(g) == 0 || s->hseqbase == g->hseqbase);

	if (n == 0 || BATcount(b) == 0 || (s != NULL && BATcount(s) == 0)) {
		/* trivial: empty result */
		*topn = BATdense(0, 0, 0);
		if (*topn == NULL) {
			rc = GDK_FAIL;
		} else if (gids) {
			*gids = BATdense(0, 0, 0);
			if (*gids == NULL) {
				BBPreclaim(*topn);
				rc = GDK_FAIL;
			}
		}
	} else if (g == NULL) {
		if (gids == NULL && !distinct) {
			*topn = BATfirstn_unique(&bi, s, n, asc, nilslast, NULL, t0);
			rc = *topn ? GDK_SUCCEED : GDK_FAIL;
		} else {
			rc = BATfirstn_grouped(topn, gids, &bi, s, n, asc, nilslast, distinct, t0);
		}
	} else if (gids == NULL && !distinct) {
		*topn = BATfirstn_unique_with_groups(&bi, s, g, n, asc, nilslast, NULL, NULL, t0);
		rc = *topn ? GDK_SUCCEED : GDK_FAIL;
	} else {
		rc = BATfirstn_grouped_with_groups(topn, gids, &bi, s, g, n, asc, nilslast, distinct, t0);
	}
	bat_iterator_end(&bi);
	return rc;
}

/* Calculate the first N values for each group given in G of the bats in
 * BATS (of which there are NBATS), but only considering the candidates
 * in S.
 *
 * Conceptually, the bats in BATS are sorted per group, taking the
 * candidate list S and the values in ASC and NILSLAST into account.
 * For each group, the first N rows are then returned.
 *
 * For each bat, the sort order that is to be used is specified in the
 * array ASC.  The first N values means the smallest N values if asc is
 * set, the largest if not set.  If NILSLAST for a bat is set, nils are
 * only returned if there are not enough non-nil values; if nilslast is
 * not set, nils are returned preferentially.
 *
 * The return value is a bat with N consecutive values for each group in
 * G.  Values are nil if there are not enough values in the group, else
 * they are row ids of the first rows.
 */
BAT *
BATgroupedfirstn(BUN n, BAT *s, BAT *g, int nbats, BAT **bats, bool *asc, bool *nilslast)
{
	const char *err;
	oid min, max;
	BUN ngrp;
	struct canditer ci;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	struct batinfo {
		BATiter bi1;
		BATiter bi2;
		oid hseq;
		bool asc;
		bool nilslast;
		const void *nil;
		int (*cmp)(const void *, const void *);
	} *batinfo;

	assert(nbats > 0);

	if (n == 0 || BATcount(bats[0]) == 0) {
		return BATdense(0, 0, 0);
	}
	if (n > BATcount(bats[0]))
		n = BATcount(bats[0]);

	if ((err = BATgroupaggrinit(bats[0], g, NULL /* e */, s, &min, &max, &ngrp, &ci)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}

	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);
	batinfo = ma_alloc(ta, nbats * sizeof(struct batinfo));
	if (batinfo == NULL) {
		ma_close(&ta_state);
		return NULL;
	}

	BAT *bn = BATconstant(0, TYPE_oid, &oid_nil, ngrp * n, TRANSIENT);
	if (bn == NULL) {
		ma_close(&ta_state);
		return NULL;
	}
	/* result is unlikely to be sorted, and there may be nils if
	 * there are groups that are too small */
	bn->tsorted = bn->trevsorted = bn->batCount <= 1;
	bn->tnil = false;
	bn->tnonil = false;

	for (int i = 0; i < nbats; i++) {
		batinfo[i] = (struct batinfo) {
			.bi1 = bat_iterator(bats[i]),
			.bi2 = bat_iterator(bats[i]),
			.asc = asc ? asc[i] : false,
			.nilslast = nilslast ? nilslast[i] : true,
			.cmp = ATOMcompare(bats[i]->ttype),
			.hseq = bats[i]->hseqbase,
			.nil = ATOMnilptr(bats[i]->ttype),
		};
	}

	/* For each group we maintain a "heap" of N values inside the
	 * return bat BN.  The heap for group GRP is located in BN at
	 * BUN [grp*N..(grp+1)*N).  The first value in this heap is the
	 * "largest" (assuming all ASC bits are set) value so far. */
	oid *oids = Tloc(bn, 0);
	TIMEOUT_LOOP(ci.ncand, qry_ctx) {
		oid o = canditer_next(&ci);
		oid grp = g ? BUNtoid(g, o - g->hseqbase) : 0;
		BUN goff = grp * n;
		int comp = -1;
		/* compare new value with root of heap to see if we must
		 * keep or replace */
		if (!is_oid_nil(oids[goff])) {
			for (int i = 0; i < nbats; i++) {
				comp = batinfo[i].cmp(BUNtail(batinfo[i].bi1, o - batinfo[i].hseq),
						      BUNtail(batinfo[i].bi2, oids[goff] - batinfo[i].hseq));
				if (comp == 0)
					continue;
				if (!batinfo[i].asc)
					comp = -comp;
				if (!batinfo[i].bi1.nonil) {
					if (batinfo[i].cmp(BUNtail(batinfo[i].bi1, o - batinfo[i].hseq),
							   batinfo[i].nil) == 0) {
						if (batinfo[i].nilslast)
							comp = 1;
						else
							comp = -1;
					} else if (batinfo[i].cmp(BUNtail(batinfo[i].bi1, oids[goff] - batinfo[i].hseq),
								  batinfo[i].nil) == 0) {
						if (batinfo[i].nilslast)
							comp = -1;
						else
							comp = 1;
					}
				}
				break;
			}
		}
		/* at this point, if comp==0, the incoming value is
		 * equal to what we currently have as the last of the
		 * first-n and so we skip it; if comp<0, the incoming
		 * value is better than the worst so far, so it replaces
		 * that one, and if comp>0, the incoming value is
		 * definitely not in the first-n */
		if (comp >= 0)
			continue;
		oids[goff] = o;
		/* we replaced the root of the heap, but now we need to
		 * restore the heap property */
		BUN pos = 0;
		BUN childpos = 1;
		while (childpos < n) {
			/* find most extreme child */
			if (childpos + 1 < n) {
				if (!is_oid_nil(oids[goff + childpos])) {
					if (is_oid_nil(oids[goff + childpos + 1]))
						childpos++;
					else {
						for (int i = 0; i < nbats; i++) {
							if ((comp = batinfo[i].cmp(BUNtail(batinfo[i].bi1, oids[goff + childpos] - batinfo[i].hseq),
										   BUNtail(batinfo[i].bi2, oids[goff + childpos + 1] - batinfo[i].hseq))) == 0)
								continue;
							if (!batinfo[i].bi1.nonil) {
								if (batinfo[i].cmp(BUNtail(batinfo[i].bi1, oids[goff + childpos] - batinfo[i].hseq), batinfo[i].nil) == 0) {
									if (!batinfo[i].nilslast)
										childpos++;
									break;
								}
								if (batinfo[i].cmp(BUNtail(batinfo[i].bi1, oids[goff + childpos + 1] - batinfo[i].hseq), batinfo[i].nil) == 0) {
									if (batinfo[i].nilslast)
										childpos++;
									break;
								}
							}
							if (batinfo[i].asc ? comp < 0 : comp > 0)
								childpos++;
							break;
						}
					}
				}
			}
			/* compare parent with most extreme child */
			if (!is_oid_nil(oids[goff + childpos])) {
				for (int i = 0; i < nbats; i++) {
					if ((comp = batinfo[i].cmp(BUNtail(batinfo[i].bi1, oids[goff + pos] - batinfo[i].hseq),
								   BUNtail(batinfo[i].bi2, oids[goff + childpos] - batinfo[i].hseq))) == 0)
						continue;
					if (batinfo[i].asc ? comp > 0 : comp < 0)
						comp = 0;
					if (!batinfo[i].bi1.nonil) {
						if (batinfo[i].cmp(BUNtail(batinfo[i].bi1, oids[goff + pos] - batinfo[i].hseq), batinfo[i].nil) == 0)
							comp = !batinfo[i].nilslast;
						else if (batinfo[i].cmp(BUNtail(batinfo[i].bi1, oids[goff + childpos] - batinfo[i].hseq), batinfo[i].nil) == 0)
							comp = batinfo[i].nilslast;
					}
					break;
				}
				if (comp == 0) {
					/* already correctly ordered */
					break;
				}
			}
			oid o = oids[goff + pos];
			oids[goff + pos] = oids[goff + childpos];
			oids[goff + childpos] = o;
			pos = childpos;
			childpos = (pos << 1) + 1;
		}
	}
	for (int i = 0; i < nbats; i++) {
		bat_iterator_end(&batinfo[i].bi1);
		bat_iterator_end(&batinfo[i].bi2);
	}
	ma_close(&ta_state);
	TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
	return bn;

  bailout:
	BBPreclaim(bn);
	return NULL;
}
