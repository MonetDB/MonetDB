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

/*
 * All join variants produce some sort of join on two input BATs,
 * optionally subject to up to two candidate lists.  Only values in
 * the input BATs that are mentioned in the associated candidate list
 * (if provided) are eligible.  They all return two output BATs in the
 * first two arguments.  The join operations differ in the way in
 * which tuples from the two inputs are matched.
 *
 * The outputs consist of two aligned BATs (i.e. same length and same
 * hseqbase (0@0)) that contain the OIDs of the input BATs that match.
 * The candidate lists, if given, contain the OIDs of the associated
 * input BAT which must be considered for matching.  The input BATs
 * must have the same type.
 *
 * All functions also have a parameter nil_matches which indicates
 * whether NIL must be considered an ordinary value that can match, or
 * whether NIL must be considered to never match.
 *
 * The join functions that are provided here are:
 * BATjoin
 *	normal equi-join
 * BATleftjoin
 *	normal equi-join, but the left output is sorted
 * BATouterjoin
 *	equi-join, but the left output is sorted, and if there is no
 *	match for a value in the left input, there is still an output
 *	with NIL in the right output
 * BATsemijoin
 *	equi-join, but the left output is sorted, and if there are
 *	multiple matches, only one is returned (i.e., the left output
 *	is also key)
 * BATthetajoin
 *	theta-join: an extra operator must be provided encoded as an
 *	integer (macros JOIN_EQ, JOIN_NE, JOIN_LT, JOIN_LE, JOIN_GT,
 *	JOIN_GE); values match if the left input has the given
 *	relationship with the right input; order of the outputs is not
 *	guaranteed
 * BATbandjoin
 *	band-join: two extra input values (c1, c2) must be provided as
 *	well as Booleans (li, hi) that indicate whether the value
 *	ranges are inclusive or not; values in the left and right
 *	inputs match if right - c1 <[=] left <[=] right + c2; if c1 or
 *	c2 is NIL, there are no matches
 * BATrangejoin
 *	range-join: the right input consists of two aligned BATs,
 *	values match if the left value is between two corresponding
 *	right values; two extra Boolean parameters, li and hi,
 *	indicate whether equal values match
 *
 * In addition to these functions, there are two more functions that
 * are closely related:
 * BATintersect
 *	intersection: return a candidate list with OIDs of tuples in
 *	the left input whose value occurs in the right input
 * BATdiff
 *	difference: return a candidate list with OIDs of tuples in the
 *	left input whose value does not occur in the right input
 */

/* Perform a bunch of sanity checks on the inputs to a join. */
static gdk_return
joinparamcheck(BAT *l, BAT *r1, BAT *r2, BAT *sl, BAT *sr, const char *func)
{
	if (ATOMtype(l->ttype) != ATOMtype(r1->ttype) ||
	    (r2 && ATOMtype(l->ttype) != ATOMtype(r2->ttype))) {
		GDKerror("%s: inputs not compatible.\n", func);
		return GDK_FAIL;
	}
	if (r2 &&
	    (BATcount(r1) != BATcount(r2) || r1->hseqbase != r2->hseqbase)) {
		GDKerror("%s: right inputs not aligned.\n", func);
		return GDK_FAIL;
	}
	if ((sl && !BATiscand(sl)) || (sr && !BATiscand(sr))) {
		GDKerror("%s: argument not a candidate list.\n", func);
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

#define INCRSIZELOG	(8 + (SIZEOF_OID / 2))
#define INCRSIZE	(1 << INCRSIZELOG)

/* Create the result bats for a join, returns the absolute maximum
 * number of outputs that could possibly be generated. */
static BUN
joininitresults(BAT **r1p, BAT **r2p, BUN lcnt, BUN rcnt, bool lkey, bool rkey,
		bool semi, bool nil_on_miss, bool only_misses, bool min_one,
		BUN estimate)
{
	BAT *r1, *r2;
	BUN maxsize, size;

	/* if nil_on_miss is set, we really need a right output */
	assert(!nil_on_miss || r2p != NULL);

	lkey |= lcnt <= 1;
	rkey |= rcnt <= 1;

	*r1p = NULL;
	if (r2p)
		*r2p = NULL;
	if (lcnt == 0) {
		/* there is nothing to match */
		maxsize = 0;
	} else if (!only_misses && !nil_on_miss && rcnt == 0) {
		/* if right is empty, we have no hits, so if we don't
		 * want misses, the result is empty */
		maxsize = 0;
	} else if (rkey | semi | only_misses) {
		/* each entry left matches at most one on right, in
		 * case nil_on_miss is also set, each entry matches
		 * exactly one (see below) */
		maxsize = lcnt;
	} else if (lkey) {
		/* each entry on right is matched at most once */
		if (nil_on_miss) {
			/* one entry left could match all right, and
			 * all other entries left match nil */
			maxsize = lcnt + rcnt - 1;
		} else {
			maxsize = rcnt;
		}
	} else if (rcnt == 0) {
		/* nil_on_miss must be true due to previous checks, so
		 * all values on left miss */
		maxsize = lcnt;
	} else if (BUN_MAX / lcnt >= rcnt) {
		/* in the worst case we have a full cross product */
		maxsize = lcnt * rcnt;
	} else {
		/* a BAT cannot grow larger than BUN_MAX */
		maxsize = BUN_MAX;
	}
	size = estimate == BUN_NONE ? lcnt < rcnt ? lcnt : rcnt : estimate;
	if (size < INCRSIZE)
		size = INCRSIZE;
	if (size > maxsize)
		size = maxsize;
	if ((rkey | semi | only_misses) & nil_on_miss) {
		/* see comment above: each entry left matches exactly
		 * once */
		size = maxsize;
	}
	if (min_one && size < lcnt)
		size = lcnt;

	if (maxsize == 0) {
		r1 = BATdense(0, 0, 0);
		if (r1 == NULL) {
			return BUN_NONE;
		}
		if (r2p) {
			r2 = BATdense(0, 0, 0);
			if (r2 == NULL) {
				BBPreclaim(r1);
				return BUN_NONE;
			}
			*r2p = r2;
		}
		*r1p = r1;
		return 0;
	}

	r1 = COLnew(0, TYPE_oid, size, TRANSIENT);
	if (r1 == NULL) {
		return BUN_NONE;
	}
	r1->tnil = false;
	r1->tnonil = true;
	r1->tkey = true;
	r1->tsorted = true;
	r1->trevsorted = true;
	r1->tseqbase = 0;
	*r1p = r1;
	if (r2p) {
		r2 = COLnew(0, TYPE_oid, size, TRANSIENT);
		if (r2 == NULL) {
			BBPreclaim(r1);
			return BUN_NONE;
		}
		r2->tnil = false;
		r2->tnonil = true;
		r2->tkey = true;
		r2->tsorted = true;
		r2->trevsorted = true;
		r2->tseqbase = 0;
		*r2p = r2;
	}
	return maxsize;
}

#define VALUE(s, x)	(s##vars ?					\
			 s##vars + VarHeapVal(s##vals, (x), s##width) : \
			 s##vals ? (const char *) s##vals + ((x) * s##width) : \
			 (s##val = BUNtoid(s, (x)), (const char *) &s##val))
#define FVALUE(s, x)	((const char *) s##vals + ((x) * s##width))

#define APPEND(b, o)		(((oid *) b->theap->base)[b->batCount++] = (o))

static inline gdk_return
maybeextend(BAT *restrict r1, BAT *restrict r2,
	    BUN cnt, BUN lcur, BUN lcnt, BUN maxsize)
{
	if (BATcount(r1) + cnt > BATcapacity(r1)) {
		/* make some extra space by extrapolating how much more
		 * we need (fraction of l we've seen so far is used to
		 * estimate a new size but with a shallow slope so that
		 * a skewed join doesn't overwhelm, whilst making sure
		 * there is somewhat significant progress) */
		BUN newcap = (BUN) (lcnt / (lcnt / 4.0 + lcur * .75) * (BATcount(r1) + cnt));
		newcap = (newcap + INCRSIZE - 1) & ~(((BUN) 1 << INCRSIZELOG) - 1);
		if (newcap < cnt + BATcount(r1))
			newcap = cnt + BATcount(r1) + INCRSIZE;
		/* if close to maxsize, then just use maxsize */
		if (newcap + INCRSIZE > maxsize)
			newcap = maxsize;
		/* make sure heap.free is set properly before
		 * extending */
		BATsetcount(r1, BATcount(r1));
		if (BATextend(r1, newcap) != GDK_SUCCEED)
			return GDK_FAIL;
		if (r2) {
			BATsetcount(r2, BATcount(r2));
			if (BATextend(r2, newcap) != GDK_SUCCEED)
				return GDK_FAIL;
			assert(BATcapacity(r1) == BATcapacity(r2));
		}
	}
	return GDK_SUCCEED;
}

/* Return BATs through r1p and r2p for the case that there is no
 * match between l and r, taking all flags into consideration.
 *
 * This means, if nil_on_miss is set or only_misses is set, *r1p is a
 * copy of the left candidate list or a dense list of all "head"
 * values of l, and *r2p (if r2p is not NULL) is all nil.  If neither
 * of those flags is set, the result is two empty BATs. */
static gdk_return
nomatch(BAT **r1p, BAT **r2p, BAT *l, BAT *r, struct canditer *restrict lci,
	bool nil_on_miss, bool only_misses, const char *func, lng t0)
{
	BAT *r1, *r2 = NULL;

	MT_thread_setalgorithm(__func__);
	if (lci->ncand == 0 || !(nil_on_miss | only_misses)) {
		/* return empty BATs */
		if ((r1 = BATdense(0, 0, 0)) == NULL)
			return GDK_FAIL;
		if (r2p) {
			if ((r2 = BATdense(0, 0, 0)) == NULL) {
				BBPreclaim(r1);
				return GDK_FAIL;
			}
			*r2p = r2;
		}
	} else {
		r1 = canditer_slice(lci, 0, lci->ncand);
		if (r2p) {
			if ((r2 = BATconstant(0, TYPE_void, &oid_nil, lci->ncand, TRANSIENT)) == NULL) {
				BBPreclaim(r1);
				return GDK_FAIL;
			}
			*r2p = r2;
		}
	}
	*r1p = r1;
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ",r=" ALGOBATFMT ",sl=" ALGOOPTBATFMT
		  ",nil_on_miss=%s,only_misses=%s"
		  " - > " ALGOBATFMT "," ALGOOPTBATFMT
		  " (%s -- " LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r), ALGOOPTBATPAR(lci->s),
		  nil_on_miss ? "true" : "false",
		  only_misses ? "true" : "false",
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  func, GDKusec() - t0);
	return GDK_SUCCEED;
}

/* Implementation of join where there is a single value (possibly
 * repeated multiple times) on the left.  This means we can use a
 * point select to find matches in the right column. */
static gdk_return
selectjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r,
	   struct canditer *lci, struct canditer *rci,
	   bool nil_matches, lng t0, bool swapped, const char *reason)
{
	BATiter li = bat_iterator(l);
	const void *v;
	BAT *bn = NULL;

	assert(lci->ncand > 0);
	assert(lci->ncand == 1 || (l->tsorted && l->trevsorted));

	MT_thread_setalgorithm(__func__);
	oid o = canditer_next(lci);
	v = BUNtail(li, o - l->hseqbase);

	if (!nil_matches &&
	    (*ATOMcompare(l->ttype))(v, ATOMnilptr(l->ttype)) == 0) {
		/* NIL doesn't match anything */
		return nomatch(r1p, r2p, l, r, lci, false, false,
			       reason, t0);
	}

	bn = BATselect(r, rci->s, v, NULL, true, true, false);
	if (bn == NULL) {
		return GDK_FAIL;
	}
	if (BATcount(bn) == 0) {
		BBPunfix(bn->batCacheid);
		return nomatch(r1p, r2p, l, r, lci, false, false,
			       reason, t0);
	}
	BAT *r1 = COLnew(0, TYPE_oid, lci->ncand * BATcount(bn), TRANSIENT);
	if (r1 == NULL) {
		BBPunfix(bn->batCacheid);
		return GDK_FAIL;
	}
	BAT *r2 = NULL;
	if (r2p) {
		r2 = COLnew(0, TYPE_oid, lci->ncand * BATcount(bn), TRANSIENT);
		if (r2 == NULL) {
			BBPunfix(bn->batCacheid);
			BBPreclaim(r1);
			return GDK_FAIL;
		}
	}

	r1->tsorted = true;
	r1->trevsorted = lci->ncand == 1;
	r1->tseqbase = BATcount(bn) == 1 && lci->tpe == cand_dense ? o : oid_nil;
	r1->tkey = BATcount(bn) == 1;
	r1->tnil = false;
	r1->tnonil = true;
	if (r2) {
		r2->tsorted = lci->ncand == 1 || BATcount(bn) == 1;
		r2->trevsorted = BATcount(bn) == 1;
		r2->tseqbase = lci->ncand == 1 && BATtdense(bn) ? bn->tseqbase : oid_nil;
		r2->tkey = lci->ncand == 1;
		r2->tnil = false;
		r2->tnonil = true;
	}
	if (BATtdense(bn)) {
		oid *o1p = (oid *) Tloc(r1, 0);
		oid *o2p = r2 ? (oid *) Tloc(r2, 0) : NULL;
		oid bno = bn->tseqbase;
		BUN p, q = BATcount(bn);

		do {
			for (p = 0; p < q; p++) {
				*o1p++ = o;
			}
			if (o2p) {
				for (p = 0; p < q; p++) {
					*o2p++ = bno + p;
				}
			}
			o = canditer_next(lci);
		} while (!is_oid_nil(o));
	} else {
		oid *o1p = (oid *) Tloc(r1, 0);
		oid *o2p = r2 ? (oid *) Tloc(r2, 0) : NULL;
		const oid *bnp = (const oid *) Tloc(bn, 0);
		BUN p, q = BATcount(bn);

		do {
			for (p = 0; p < q; p++) {
				*o1p++ = o;
			}
			if (o2p) {
				for (p = 0; p < q; p++) {
					*o2p++ = bnp[p];
				}
			}
			o = canditer_next(lci);
		} while (!is_oid_nil(o));
	}
	BATsetcount(r1, lci->ncand * BATcount(bn));
	*r1p = r1;
	if (r2p) {
		BATsetcount(r2, lci->ncand * BATcount(bn));
		*r2p = r2;
	}
	BBPunfix(bn->batCacheid);
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ","
		  "r=" ALGOBATFMT ",sl=" ALGOOPTBATFMT ","
		  "sr=" ALGOOPTBATFMT ",nil_matches=%s;%s %s "
		  "-> " ALGOBATFMT "," ALGOOPTBATFMT " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  ALGOOPTBATPAR(lci->s), ALGOOPTBATPAR(rci->s),
		  nil_matches ? "true" : "false",
		  swapped ? " swapped" : "", reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);

	return GDK_SUCCEED;
}

#if SIZEOF_OID == SIZEOF_INT
#define binsearch_oid(indir, offset, vals, lo, hi, v, ordering, last) binsearch_int(indir, offset, (const int *) vals, lo, hi, (int) (v), ordering, last)
#endif
#if SIZEOF_OID == SIZEOF_LNG
#define binsearch_oid(indir, offset, vals, lo, hi, v, ordering, last) binsearch_lng(indir, offset, (const lng *) vals, lo, hi, (lng) (v), ordering, last)
#endif

/* Implementation of join where the right-hand side is dense, and if
 * there is a right candidate list, it too is dense.  In case
 * nil_on_miss is not set, we use a range select (BATselect) to find
 * the matching values in the left column and then calculate the
 * corresponding matches from the right.  If nil_on_miss is set, we
 * need to do some more work. */
static gdk_return
mergejoin_void(BAT **r1p, BAT **r2p, BAT *l, BAT *r,
	       struct canditer *restrict lci, struct canditer *restrict rci,
	       bool nil_on_miss, bool only_misses, lng t0, bool swapped,
	       const char *reason)
{
	oid lo, hi;
	BUN i;
	oid o, *o1p = NULL, *o2p = NULL;
	BAT *r1 = NULL, *r2 = NULL;
	const oid *lvals = NULL;

	/* r is dense, and if there is a candidate list, it too is
	 * dense.  This means we don't have to do any searches, we
	 * only need to compare ranges to know whether a value from l
	 * has a match in r */
	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(r->tsorted || r->trevsorted);
	assert(BATcount(l) > 0);
	assert(rci->tpe == cand_dense);
	assert(BATcount(r) > 0);

	MT_thread_setalgorithm(__func__);
	/* figure out range [lo..hi) of values in r that we need to match */
	lo = r->tseqbase;
	hi = lo + BATcount(r);
	/* restrict [lo..hi) range further using candidate list */
	if (rci->seq > r->hseqbase)
		lo += rci->seq - r->hseqbase;
	if (rci->seq + rci->ncand < r->hseqbase + BATcount(r))
		hi -= r->hseqbase + BATcount(r) - rci->seq - rci->ncand;

	/* at this point, the matchable values in r are [lo..hi) */
	if (!nil_on_miss) {
		r1 = BATselect(l, lci->s, &lo, &hi, true, false, only_misses);
		if (r1 == NULL)
			return GDK_FAIL;
		if (only_misses && !l->tnonil) {
			/* also look for NILs */
			r2 = BATselect(l, lci->s, &oid_nil, NULL, true, false, false);
			if (r2 == NULL) {
				BBPreclaim(r1);
				return GDK_FAIL;
			}
			if (BATcount(r2) > 0) {
				BAT *mg = BATmergecand(r1, r2);
				BBPunfix(r1->batCacheid);
				BBPunfix(r2->batCacheid);
				r1 = mg;
				if (r1 == NULL)
					return GDK_FAIL;
			} else {
				BBPunfix(r2->batCacheid);
			}
			r2 = NULL;
		}
		*r1p = r1;
		if (r2p == NULL)
			goto doreturn2;
		if (BATcount(r1) == 0) {
			r2 = BATdense(0, 0, 0);
			if (r2 == NULL) {
				BBPreclaim(r1);
				return GDK_FAIL;
			}
		} else if (BATtdense(r1) && BATtdense(l)) {
			r2 = BATdense(0, l->tseqbase + r1->tseqbase - l->hseqbase + r->hseqbase - r->tseqbase, BATcount(r1));
			if (r2 == NULL) {
				BBPreclaim(r1);
				return GDK_FAIL;
			}
		} else {
			r2 = COLnew(0, TYPE_oid, BATcount(r1), TRANSIENT);
			if (r2 == NULL) {
				BBPreclaim(r1);
				return GDK_FAIL;
			}
			const oid *lp = (const oid *) Tloc(l, 0);
			const oid *o1p = (const oid *) Tloc(r1, 0);
			oid *o2p = (oid *) Tloc(r2, 0);
			hi = BATcount(r1);
			if (complex_cand(l)) {
				/* this is actually generic code */
				for (o = 0; o < hi; o++)
					o2p[o] = BUNtoid(l, BUNtoid(r1, o) - l->hseqbase) - r->tseqbase + r->hseqbase;
			} else if (BATtdense(r1)) {
				lo = r1->tseqbase - l->hseqbase;
				if (r->tseqbase == r->hseqbase) {
					memcpy(o2p, lp + lo, hi * SIZEOF_OID);
				} else {
					hi += lo;
					for (o = 0; lo < hi; o++, lo++) {
						o2p[o] = lp[lo] - r->tseqbase + r->hseqbase;
					}
				}
			} else if (BATtdense(l)) {
				for (o = 0; o < hi; o++) {
					o2p[o] = o1p[o] - l->hseqbase + l->tseqbase - r->tseqbase + r->hseqbase;
				}
			} else {
				for (o = 0; o < hi; o++) {
					o2p[o] = lp[o1p[o] - l->hseqbase] - r->tseqbase + r->hseqbase;
				}
			}
			r2->tkey = l->tkey;
			r2->tsorted = l->tsorted;
			r2->trevsorted = l->trevsorted;
			r2->tnil = false;
			r2->tnonil = true;
			BATsetcount(r2, BATcount(r1));
		}
		*r2p = r2;
		goto doreturn2;
	}
	/* nil_on_miss is set, this means we must have a second output */
	assert(r2p);
	if (BATtdense(l)) {
		/* if l is dense, we can further restrict the [lo..hi)
		 * range to values in l that match with values in r */
		o = lo;
		i = lci->seq - l->hseqbase;
		if (l->tseqbase + i > lo)
			lo = l->tseqbase + i;
		i = canditer_last(lci) + 1 - l->hseqbase;
		if (l->tseqbase + i < hi)
			hi = l->tseqbase + i;
		if (lci->tpe == cand_dense) {
			/* l is dense, and so is the left candidate
			 * list (if it exists); this means we don't
			 * have to actually look at any values in l:
			 * we can just do some arithmetic; it also
			 * means that r1 will be dense, and if
			 * nil_on_miss is not set, or if all values in
			 * l match, r2 will too */
			if (hi <= lo) {
				return nomatch(r1p, r2p, l, r, lci,
					       nil_on_miss, only_misses,
					       "mergejoin_void", t0);
			}

			/* at this point, the matched values in l and
			 * r (taking candidate lists into account) are
			 * [lo..hi) which we can translate back to the
			 * respective OID values that we can store in
			 * r1 and r2; note that r1 will be dense since
			 * all values in l will match something (even
			 * if nil since nil_on_miss is set) */
			*r1p = r1 = BATdense(0, lci->seq, lci->ncand);
			if (r1 == NULL)
				return GDK_FAIL;
			if (hi - lo < lci->ncand) {
				/* we need to fill in nils in r2 for
				 * missing values */
				*r2p = r2 = COLnew(0, TYPE_oid, lci->ncand, TRANSIENT);
				if (r2 == NULL) {
					BBPreclaim(*r1p);
					return GDK_FAIL;
				}
				o2p = (oid *) Tloc(r2, 0);
				i = l->tseqbase + lci->seq - l->hseqbase;
				lo -= i;
				hi -= i;
				i += r->hseqbase - r->tseqbase;
				for (o = 0; o < lo; o++)
					*o2p++ = oid_nil;
				for (o = lo; o < hi; o++)
					*o2p++ = o + i;
				for (o = hi; o < lci->ncand; o++)
					*o2p++ = oid_nil;
				r2->tnonil = false;
				r2->tnil = true;
				/* sorted of no nils at end */
				r2->tsorted = hi == lci->ncand;
				/* reverse sorted if single non-nil at start */
				r2->trevsorted = lo == 0 && hi == 1;
				r2->tseqbase = oid_nil;
				/* (hi - lo) different OIDs in r2,
				 * plus one for nil */
				r2->tkey = hi - lo + 1 == lci->ncand;
				BATsetcount(r2, lci->ncand);
			} else {
				/* no missing values */
				*r2p = r2 = BATdense(0, r->hseqbase + lo - r->tseqbase, lci->ncand);
				if (r2 == NULL) {
					BBPreclaim(*r1p);
					return GDK_FAIL;
				}
			}
			goto doreturn;
		}
		/* l is dense, but the candidate list exists and is
		 * not dense; we can, by manipulating the range
		 * [lo..hi), just look at the candidate list values */

		/* translate lo and hi to l's OID values that now need
		 * to match */
		lo = lo - l->tseqbase + l->hseqbase;
		hi = hi - l->tseqbase + l->hseqbase;

		*r1p = r1 = COLnew(0, TYPE_oid, lci->ncand, TRANSIENT);
		*r2p = r2 = COLnew(0, TYPE_oid, lci->ncand, TRANSIENT);
		if (r1 == NULL || r2 == NULL) {
			BBPreclaim(r1);
			BBPreclaim(r2);
			return GDK_FAIL;
		}
		o1p = (oid *) Tloc(r1, 0);
		o2p = (oid *) Tloc(r2, 0);
		r2->tnil = false;
		r2->tnonil = true;
		r2->tkey = true;
		r2->tsorted = true;
		o = canditer_next(lci);
		for (i = 0; i < lci->ncand && o < lo; i++) {
			*o1p++ = o;
			*o2p++ = oid_nil;
			o = canditer_next(lci);
		}
		if (i > 0) {
			r2->tnil = true;
			r2->tnonil = false;
			r2->tkey = i == 1;
		}
		for (; i < lci->ncand && o < hi; i++) {
			*o1p++ = o;
			*o2p++ = o - l->hseqbase + l->tseqbase - r->tseqbase + r->hseqbase;
			o = canditer_next(lci);
		}
		if (i < lci->ncand) {
			r2->tkey = !r2->tnil && lci->ncand - i == 1;
			r2->tnil = true;
			r2->tnonil = false;
			r2->tsorted = false;
			for (; i < lci->ncand; i++) {
				*o1p++ = o;
				*o2p++ = oid_nil;
				o = canditer_next(lci);
			}
		}
		BATsetcount(r1, lci->ncand);
		r1->tseqbase = BATcount(r1) == 1 ? *(oid*)Tloc(r1, 0) : oid_nil;
		r1->tsorted = true;
		r1->trevsorted = BATcount(r1) <= 1;
		r1->tnil = false;
		r1->tnonil = true;
		r1->tkey = true;
		BATsetcount(r2, BATcount(r1));
		r2->tseqbase = r2->tnil || BATcount(r2) > 1 ? oid_nil : BATcount(r2) == 1 ? *(oid*)Tloc(r2, 0) : 0;
		r2->trevsorted = BATcount(r2) <= 1;
		goto doreturn;
	}
	/* l is not dense, so we need to look at the values and check
	 * whether they are in the range [lo..hi) */
	lvals = (const oid *) Tloc(l, 0);

	/* do indirection through the candidate list to look at the
	 * value */

	*r1p = r1 = COLnew(0, TYPE_oid, lci->ncand, TRANSIENT);
	*r2p = r2 = COLnew(0, TYPE_oid, lci->ncand, TRANSIENT);
	if (r1 == NULL || r2 == NULL) {
		BBPreclaim(r1);
		BBPreclaim(r2);
		return GDK_FAIL;
	}
	o1p = (oid *) Tloc(r1, 0);
	o2p = (oid *) Tloc(r2, 0);
	r2->tnil = false;
	r2->tnonil = true;
	if (complex_cand(l)) {
		for (i = 0; i < lci->ncand; i++) {
			oid c = canditer_next(lci);

			o = BUNtoid(l, c - l->hseqbase);
			*o1p++ = c;
			if (o >= lo && o < hi) {
				*o2p++ = o - r->tseqbase + r->hseqbase;
			} else {
				*o2p++ = oid_nil;
				r2->tnil = true;
				r2->tnonil = false;
			}
		}
	} else {
		for (i = 0; i < lci->ncand; i++) {
			oid c = canditer_next(lci);

			o = lvals[c - l->hseqbase];
			*o1p++ = c;
			if (o >= lo && o < hi) {
				*o2p++ = o - r->tseqbase + r->hseqbase;
			} else {
				*o2p++ = oid_nil;
				r2->tnil = true;
				r2->tnonil = false;
			}
		}
	}
	r1->tsorted = true;
	r1->trevsorted = BATcount(r1) <= 1;
	r1->tkey = true;
	r1->tseqbase = oid_nil;
	r1->tnil = false;
	r1->tnonil = true;
	BATsetcount(r1, lci->ncand);
	BATsetcount(r2, lci->ncand);
	r2->tsorted = l->tsorted || BATcount(r2) <= 1;
	r2->trevsorted = l->trevsorted || BATcount(r2) <= 1;
	r2->tkey = l->tkey || BATcount(r2) <= 1;
	r2->tseqbase = oid_nil;

  doreturn:
	if (r1->tkey)
		virtualize(r1);
	if (r2->tkey && r2->tsorted)
		virtualize(r2);
  doreturn2:
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ","
		  "r=" ALGOBATFMT ",sl=" ALGOOPTBATFMT ","
		  "sr=" ALGOOPTBATFMT ","
		  "nil_on_miss=%s,only_misses=%s;%s %s "
		  "-> " ALGOBATFMT "," ALGOOPTBATFMT " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  ALGOOPTBATPAR(lci->s), ALGOOPTBATPAR(rci->s),
		  nil_on_miss ? "true" : "false",
		  only_misses ? "true" : "false",
		  swapped ? " swapped" : "", reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);

	return GDK_SUCCEED;
}

/* Implementation of mergejoin (see below) for the special case that
 * the values are of type int, and some more conditions are met. */
static gdk_return
mergejoin_int(BAT **r1p, BAT **r2p, BAT *l, BAT *r,
	      bool nil_matches, BUN estimate, lng t0, bool swapped,
	      const char *reason)
{
	BAT *r1, *r2;
	BUN lstart, lend, lcnt;
	BUN rstart, rend;
	BUN lscan, rscan;	/* opportunistic scan window */
	BUN maxsize;
	const int *lvals, *rvals;
	int v;
	BUN nl, nr;
	oid lv;
	BUN i;

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(r->tsorted || r->trevsorted);

	MT_thread_setalgorithm(__func__);
	lstart = rstart = 0;
	lend = BATcount(l);
	lcnt = lend - lstart;
	rend = BATcount(r);
	lvals = (const int *) Tloc(l, 0);
	rvals = (const int *) Tloc(r, 0);
	assert(!r->tvarsized || !r->ttype);

	/* basic properties will be adjusted if necessary later on,
	 * they were initially set by joininitresults() */

	if (lend == 0 || rend == 0) {
		/* there are no matches */
		return nomatch(r1p, r2p, l, r,
			       &(struct canditer) {.tpe = cand_dense, .ncand = lcnt,},
			       false, false, __func__, t0);
	}

	if ((maxsize = joininitresults(r1p, r2p, BATcount(l), BATcount(r),
				       l->tkey, r->tkey, false, false,
				       false, false, estimate)) == BUN_NONE)
		return GDK_FAIL;
	r1 = *r1p;
	r2 = r2p ? *r2p : NULL;

	/* determine opportunistic scan window for l and r */
	for (nl = lend - lstart, lscan = 4; nl > 0; lscan++)
		nl >>= 1;
	for (nr = rend - rstart, rscan = 4; nr > 0; rscan++)
		nr >>= 1;

	if (!nil_matches) {
		/* skip over nils at the start of the columns */
		if (lscan < lend - lstart && is_int_nil(lvals[lstart + lscan])) {
			lstart = binsearch_int(NULL, 0, lvals, lstart + lscan,
					       lend - 1, int_nil, 1, 1);
		} else {
			while (is_int_nil(lvals[lstart]))
				lstart++;
		}
		if (rscan < rend - rstart && is_int_nil(rvals[rstart + rscan])) {
			rstart = binsearch_int(NULL, 0, rvals, rstart + rscan,
					       rend - 1, int_nil, 1, 1);
		} else {
			while (is_int_nil(rvals[rstart]))
				rstart++;
		}
	}
	/* from here on we don't have to worry about nil values */

	while (lstart < lend && rstart < rend) {
		v = rvals[rstart];

		if (lscan < lend - lstart && lvals[lstart + lscan] < v) {
			lstart = binsearch_int(NULL, 0, lvals, lstart + lscan,
					       lend - 1, v, 1, 0);
		} else {
			/* scan l for v */
			while (lstart < lend && lvals[lstart] < v)
				lstart++;
		}
		if (lstart >= lend) {
			/* nothing found */
			break;
		}

		/* Here we determine the next value in l that we are
		 * going to try to match in r.  We will also count the
		 * number of occurrences in l of that value.
		 * Afterwards, v points to the value and nl is the
		 * number of times it occurs.  Also, lstart will
		 * point to the next value to be considered (ready for
		 * the next iteration).
		 * If there are many equal values in l (more than
		 * lscan), we will use binary search to find the end
		 * of the sequence.  Obviously, we can do this only if
		 * l is actually sorted (lscan > 0). */
		nl = 1;		/* we'll match (at least) one in l */
		nr = 0;		/* maybe we won't match anything in r */
		v = lvals[lstart];
		if (l->tkey) {
			/* if l is key, there is a single value */
			lstart++;
		} else if (lscan < lend - lstart &&
			   v == lvals[lstart + lscan]) {
			/* lots of equal values: use binary search to
			 * find end */
			nl = binsearch_int(NULL, 0, lvals, lstart + lscan,
					   lend - 1, v, 1, 1);
			nl -= lstart;
			lstart += nl;
		} else {
			/* just scan */
			while (++lstart < lend && v == lvals[lstart])
				nl++;
		}
		/* lstart points one beyond the value we're
		 * going to match: ready for the next iteration. */

		/* First we find the first value in r that is at
		 * least as large as v, then we find the first
		 * value in r that is larger than v.  The difference
		 * is the number of values equal to v and is stored in
		 * nr.
		 * We will use binary search on r to find both ends of
		 * the sequence of values that are equal to v in case
		 * the position is "too far" (more than rscan
		 * away). */

		/* first find the location of the first value in r
		 * that is >= v, then find the location of the first
		 * value in r that is > v; the difference is the
		 * number of values equal to v */

		/* look ahead a little (rscan) in r to see whether
		 * we're better off doing a binary search */
		if (rscan < rend - rstart && rvals[rstart + rscan] < v) {
			/* value too far away in r: use binary
			 * search */
			rstart = binsearch_int(NULL, 0, rvals, rstart + rscan,
					       rend - 1, v, 1, 0);
		} else {
			/* scan r for v */
			while (rstart < rend && rvals[rstart] < v)
				rstart++;
		}
		if (rstart == rend) {
			/* nothing found */
			break;
		}

		/* now find the end of the sequence of equal values v */

		/* if r is key, there is zero or one match, otherwise
		 * look ahead a little (rscan) in r to see whether
		 * we're better off doing a binary search */
		if (r->tkey) {
			if (rstart < rend && v == rvals[rstart]) {
				nr = 1;
				rstart++;
			}
		} else if (rscan < rend - rstart &&
			   v == rvals[rstart + rscan]) {
			/* range too large: use binary search */
			nr = binsearch_int(NULL, 0, rvals, rstart + rscan,
					   rend - 1, v, 1, 1);
			nr -= rstart;
			rstart += nr;
		} else {
			/* scan r for end of range */
			while (rstart < rend && v == rvals[rstart]) {
				nr++;
				rstart++;
			}
		}
		/* rstart points to first value > v or end of
		 * r, and nr is the number of values in r that
		 * are equal to v */
		if (nr == 0) {
			/* no entries in r found */
			continue;
		}
		/* make space: nl values in l match nr values in r, so
		 * we need to add nl * nr values in the results */
		if (maybeextend(r1, r2, nl * nr, lstart, lend, maxsize) != GDK_SUCCEED)
			goto bailout;

		/* maintain properties */
		if (nl > 1) {
			/* value occurs multiple times in l, so entry
			 * in r will be repeated multiple times: hence
			 * r2 is not key and not dense */
			if (r2) {
				r2->tkey = false;
				r2->tseqbase = oid_nil;
			}
			/* multiple different values will be inserted
			 * in r1 (always in order), so not reverse
			 * ordered anymore */
			r1->trevsorted = false;
		}
		if (nr > 1) {
			/* value occurs multiple times in r, so entry
			 * in l will be repeated multiple times: hence
			 * r1 is not key and not dense */
			r1->tkey = false;
			r1->tseqbase = oid_nil;
			/* multiple different values will be inserted
			 * in r2 (in order), so not reverse ordered
			 * anymore */
			if (r2) {
				r2->trevsorted = false;
				if (nl > 1) {
					/* multiple values in l match
					 * multiple values in r, so an
					 * ordered sequence will be
					 * inserted multiple times in
					 * r2, so r2 is not ordered
					 * anymore */
					r2->tsorted = false;
				}
			}
		}
		if (BATcount(r1) > 0) {
			/* a new, higher value will be inserted into
			 * r1, so r1 is not reverse ordered anymore */
			r1->trevsorted = false;
			/* a new higher value will be added to r2 */
			if (r2) {
				r2->trevsorted = false;
			}
			if (BATtdense(r1) &&
			    ((oid *) r1->theap->base)[r1->batCount - 1] + 1 != l->hseqbase + lstart - nl) {
				r1->tseqbase = oid_nil;
			}
		}

		if (r2 &&
		    BATcount(r2) > 0 &&
		    BATtdense(r2) &&
		    ((oid *) r2->theap->base)[r2->batCount - 1] + 1 != r->hseqbase + rstart - nr) {
			r2->tseqbase = oid_nil;
		}

		/* insert values */
		lv = l->hseqbase + lstart - nl;
		for (i = 0; i < nl; i++) {
			BUN j;

			for (j = 0; j < nr; j++) {
				APPEND(r1, lv);
			}
			if (r2) {
				oid rv = r->hseqbase + rstart - nr;

				for (j = 0; j < nr; j++) {
					APPEND(r2, rv);
					rv++;
				}
			}
			lv++;
		}
	}
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	if (r2) {
		BATsetcount(r2, BATcount(r2));
		assert(BATcount(r1) == BATcount(r2));
	}
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2) {
			r2->tseqbase = 0;
		}
	}
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT "," "r=" ALGOBATFMT ","
		  "nil_matches=%s;%s %s "
		  "-> " ALGOBATFMT "," ALGOOPTBATFMT " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  nil_matches ? "true" : "false",
		  swapped ? " swapped" : "", reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);

	return GDK_SUCCEED;

  bailout:
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}

/* Implementation of mergejoin (see below) for the special case that
 * the values are of type lng, and some more conditions are met. */
static gdk_return
mergejoin_lng(BAT **r1p, BAT **r2p, BAT *l, BAT *r,
	      bool nil_matches, BUN estimate, lng t0, bool swapped,
	      const char *reason)
{
	BAT *r1, *r2;
	BUN lstart, lend, lcnt;
	BUN rstart, rend;
	BUN lscan, rscan;	/* opportunistic scan window */
	BUN maxsize;
	const lng *lvals, *rvals;
	lng v;
	BUN nl, nr;
	oid lv;
	BUN i;

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(r->tsorted || r->trevsorted);

	MT_thread_setalgorithm(__func__);
	lstart = rstart = 0;
	lend = BATcount(l);
	lcnt = lend - lstart;
	rend = BATcount(r);
	lvals = (const lng *) Tloc(l, 0);
	rvals = (const lng *) Tloc(r, 0);
	assert(!r->tvarsized || !r->ttype);

	/* basic properties will be adjusted if necessary later on,
	 * they were initially set by joininitresults() */

	if (lend == 0 || rend == 0) {
		/* there are no matches */
		return nomatch(r1p, r2p, l, r,
			       &(struct canditer) {.tpe = cand_dense, .ncand = lcnt,},
			       false, false, __func__, t0);
	}

	if ((maxsize = joininitresults(r1p, r2p, BATcount(l), BATcount(r),
				       l->tkey, r->tkey, false, false,
				       false, false, estimate)) == BUN_NONE)
		return GDK_FAIL;
	r1 = *r1p;
	r2 = r2p ? *r2p : NULL;

	/* determine opportunistic scan window for l and r */
	for (nl = lend - lstart, lscan = 4; nl > 0; lscan++)
		nl >>= 1;
	for (nr = rend - rstart, rscan = 4; nr > 0; rscan++)
		nr >>= 1;

	if (!nil_matches) {
		/* skip over nils at the start of the columns */
		if (lscan < lend - lstart && is_lng_nil(lvals[lstart + lscan])) {
			lstart = binsearch_lng(NULL, 0, lvals, lstart + lscan,
					       lend - 1, lng_nil, 1, 1);
		} else {
			while (is_lng_nil(lvals[lstart]))
				lstart++;
		}
		if (rscan < rend - rstart && is_lng_nil(rvals[rstart + rscan])) {
			rstart = binsearch_lng(NULL, 0, rvals, rstart + rscan,
					       rend - 1, lng_nil, 1, 1);
		} else {
			while (is_lng_nil(rvals[rstart]))
				rstart++;
		}
	}
	/* from here on we don't have to worry about nil values */

	while (lstart < lend && rstart < rend) {
		v = rvals[rstart];

		if (lscan < lend - lstart && lvals[lstart + lscan] < v) {
			lstart = binsearch_lng(NULL, 0, lvals, lstart + lscan,
					       lend - 1, v, 1, 0);
		} else {
			/* scan l for v */
			while (lstart < lend && lvals[lstart] < v)
				lstart++;
		}
		if (lstart >= lend) {
			/* nothing found */
			break;
		}

		/* Here we determine the next value in l that we are
		 * going to try to match in r.  We will also count the
		 * number of occurrences in l of that value.
		 * Afterwards, v points to the value and nl is the
		 * number of times it occurs.  Also, lstart will
		 * point to the next value to be considered (ready for
		 * the next iteration).
		 * If there are many equal values in l (more than
		 * lscan), we will use binary search to find the end
		 * of the sequence.  Obviously, we can do this only if
		 * l is actually sorted (lscan > 0). */
		nl = 1;		/* we'll match (at least) one in l */
		nr = 0;		/* maybe we won't match anything in r */
		v = lvals[lstart];
		if (l->tkey) {
			/* if l is key, there is a single value */
			lstart++;
		} else if (lscan < lend - lstart &&
			   v == lvals[lstart + lscan]) {
			/* lots of equal values: use binary search to
			 * find end */
			nl = binsearch_lng(NULL, 0, lvals, lstart + lscan,
					   lend - 1, v, 1, 1);
			nl -= lstart;
			lstart += nl;
		} else {
			/* just scan */
			while (++lstart < lend && v == lvals[lstart])
				nl++;
		}
		/* lstart points one beyond the value we're
		 * going to match: ready for the next iteration. */

		/* First we find the first value in r that is at
		 * least as large as v, then we find the first
		 * value in r that is larger than v.  The difference
		 * is the number of values equal to v and is stored in
		 * nr.
		 * We will use binary search on r to find both ends of
		 * the sequence of values that are equal to v in case
		 * the position is "too far" (more than rscan
		 * away). */

		/* first find the location of the first value in r
		 * that is >= v, then find the location of the first
		 * value in r that is > v; the difference is the
		 * number of values equal to v */

		/* look ahead a little (rscan) in r to see whether
		 * we're better off doing a binary search */
		if (rscan < rend - rstart && rvals[rstart + rscan] < v) {
			/* value too far away in r: use binary
			 * search */
			rstart = binsearch_lng(NULL, 0, rvals, rstart + rscan,
					       rend - 1, v, 1, 0);
		} else {
			/* scan r for v */
			while (rstart < rend && rvals[rstart] < v)
				rstart++;
		}
		if (rstart == rend) {
			/* nothing found */
			break;
		}

		/* now find the end of the sequence of equal values v */

		/* if r is key, there is zero or one match, otherwise
		 * look ahead a little (rscan) in r to see whether
		 * we're better off doing a binary search */
		if (r->tkey) {
			if (rstart < rend && v == rvals[rstart]) {
				nr = 1;
				rstart++;
			}
		} else if (rscan < rend - rstart &&
			   v == rvals[rstart + rscan]) {
			/* range too large: use binary search */
			nr = binsearch_lng(NULL, 0, rvals, rstart + rscan,
					   rend - 1, v, 1, 1);
			nr -= rstart;
			rstart += nr;
		} else {
			/* scan r for end of range */
			while (rstart < rend && v == rvals[rstart]) {
				nr++;
				rstart++;
			}
		}
		/* rstart points to first value > v or end of
		 * r, and nr is the number of values in r that
		 * are equal to v */
		if (nr == 0) {
			/* no entries in r found */
			continue;
		}
		/* make space: nl values in l match nr values in r, so
		 * we need to add nl * nr values in the results */
		if (maybeextend(r1, r2, nl * nr, lstart, lend, maxsize) != GDK_SUCCEED)
			goto bailout;

		/* maintain properties */
		if (nl > 1) {
			/* value occurs multiple times in l, so entry
			 * in r will be repeated multiple times: hence
			 * r2 is not key and not dense */
			if (r2) {
				r2->tkey = false;
				r2->tseqbase = oid_nil;
			}
			/* multiple different values will be inserted
			 * in r1 (always in order), so not reverse
			 * ordered anymore */
			r1->trevsorted = false;
		}
		if (nr > 1) {
			/* value occurs multiple times in r, so entry
			 * in l will be repeated multiple times: hence
			 * r1 is not key and not dense */
			r1->tkey = false;
			r1->tseqbase = oid_nil;
			/* multiple different values will be inserted
			 * in r2 (in order), so not reverse ordered
			 * anymore */
			if (r2) {
				r2->trevsorted = false;
				if (nl > 1) {
					/* multiple values in l match
					 * multiple values in r, so an
					 * ordered sequence will be
					 * inserted multiple times in
					 * r2, so r2 is not ordered
					 * anymore */
					r2->tsorted = false;
				}
			}
		}
		if (BATcount(r1) > 0) {
			/* a new, higher value will be inserted into
			 * r1, so r1 is not reverse ordered anymore */
			r1->trevsorted = false;
			/* a new higher value will be added to r2 */
			if (r2) {
				r2->trevsorted = false;
			}
			if (BATtdense(r1) &&
			    ((oid *) r1->theap->base)[r1->batCount - 1] + 1 != l->hseqbase + lstart - nl) {
				r1->tseqbase = oid_nil;
			}
		}

		if (r2 &&
		    BATcount(r2) > 0 &&
		    BATtdense(r2) &&
		    ((oid *) r2->theap->base)[r2->batCount - 1] + 1 != r->hseqbase + rstart - nr) {
			r2->tseqbase = oid_nil;
		}

		/* insert values */
		lv = l->hseqbase + lstart - nl;
		for (i = 0; i < nl; i++) {
			BUN j;

			for (j = 0; j < nr; j++) {
				APPEND(r1, lv);
			}
			if (r2) {
				oid rv = r->hseqbase + rstart - nr;

				for (j = 0; j < nr; j++) {
					APPEND(r2, rv);
					rv++;
				}
			}
			lv++;
		}
	}
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	if (r2) {
		BATsetcount(r2, BATcount(r2));
		assert(BATcount(r1) == BATcount(r2));
	}
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2) {
			r2->tseqbase = 0;
		}
	}
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT "," "r=" ALGOBATFMT ","
		  "nil_matches=%s;%s %s "
		  "-> " ALGOBATFMT "," ALGOOPTBATFMT " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  nil_matches ? "true" : "false",
		  swapped ? " swapped" : "", reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);

	return GDK_SUCCEED;

  bailout:
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}

/* Implementation of mergejoin (see below) for the special case that
 * the values are of type oid, and the right-hand side is a candidate
 * list with exception, and some more conditions are met. */
static gdk_return
mergejoin_cand(BAT **r1p, BAT **r2p, BAT *l, BAT *r,
	       bool nil_matches, BUN estimate, lng t0, bool swapped,
	       const char *reason)
{
	BAT *r1, *r2;
	BUN lstart, lend, lcnt;
	struct canditer lci, rci;
	BUN lscan;		/* opportunistic scan window */
	BUN maxsize;
	const oid *lvals;
	oid v;
	BUN nl, nr;
	oid lv;
	BUN i;

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));

	MT_thread_setalgorithm(__func__);
	lstart = 0;
	lend = BATcount(l);
	lcnt = lend - lstart;
	if (l->ttype == TYPE_void) {
		assert(!is_oid_nil(l->tseqbase));
		lcnt = canditer_init(&lci, NULL, l);
		lvals = NULL;
	} else {
		lci = (struct canditer) {.tpe = cand_dense}; /* not used */
		lvals = (const oid *) Tloc(l, 0);
		assert(lvals != NULL);
	}

	assert(complex_cand(r));
	canditer_init(&rci, NULL, r);

	/* basic properties will be adjusted if necessary later on,
	 * they were initially set by joininitresults() */

	if (lend == 0 || rci.ncand == 0) {
		/* there are no matches */
		return nomatch(r1p, r2p, l, r,
			       &(struct canditer) {.tpe = cand_dense, .ncand = lcnt,},
			       false, false, __func__, t0);
	}

	if ((maxsize = joininitresults(r1p, r2p, BATcount(l), BATcount(r),
				       l->tkey, r->tkey, false, false,
				       false, false, estimate)) == BUN_NONE)
		return GDK_FAIL;
	r1 = *r1p;
	r2 = r2p ? *r2p : NULL;

	/* determine opportunistic scan window for l and r */
	for (nl = lend - lstart, lscan = 4; nl > 0; lscan++)
		nl >>= 1;

	if (!nil_matches) {
		/* skip over nils at the start of the columns */
		if (lscan < lend - lstart && lvals && is_oid_nil(lvals[lstart + lscan])) {
			lstart = binsearch_oid(NULL, 0, lvals, lstart + lscan,
					       lend - 1, oid_nil, 1, 1);
		} else if (lvals) {
			while (is_oid_nil(lvals[lstart]))
				lstart++;
		} /* else l is candidate list: no nils */
	}
	/* from here on we don't have to worry about nil values */

	while (lstart < lend && rci.next < rci.ncand) {
		v = canditer_peek(&rci);

		if (lvals) {
			if (lscan < lend - lstart &&
			    lvals[lstart + lscan] < v) {
				lstart = binsearch_oid(NULL, 0, lvals,
						       lstart + lscan,
						       lend - 1, v, 1, 0);
			} else {
				/* scan l for v */
				while (lstart < lend && lvals[lstart] < v)
					lstart++;
			}
		} else {
			lstart = canditer_search(&lci, v, true);
			canditer_setidx(&lci, lstart);
		}
		if (lstart >= lend) {
			/* nothing found */
			break;
		}

		/* Here we determine the next value in l that we are
		 * going to try to match in r.  We will also count the
		 * number of occurrences in l of that value.
		 * Afterwards, v points to the value and nl is the
		 * number of times it occurs.  Also, lstart will
		 * point to the next value to be considered (ready for
		 * the next iteration).
		 * If there are many equal values in l (more than
		 * lscan), we will use binary search to find the end
		 * of the sequence.  Obviously, we can do this only if
		 * l is actually sorted (lscan > 0). */
		nl = 1;		/* we'll match (at least) one in l */
		nr = 0;		/* maybe we won't match anything in r */
		v = lvals ? lvals[lstart] : canditer_next(&lci);
		if (l->tkey || lvals == NULL) {
			/* if l is key, there is a single value */
			lstart++;
		} else if (lscan < lend - lstart &&
			   v == lvals[lstart + lscan]) {
			/* lots of equal values: use binary search to
			 * find end */
			nl = binsearch_oid(NULL, 0, lvals, lstart + lscan,
					   lend - 1, v, 1, 1);
			nl -= lstart;
			lstart += nl;
		} else {
			/* just scan */
			while (++lstart < lend && v == lvals[lstart])
				nl++;
		}
		/* lstart points one beyond the value we're
		 * going to match: ready for the next iteration. */

		/* First we find the first value in r that is at
		 * least as large as v, then we find the first
		 * value in r that is larger than v.  The difference
		 * is the number of values equal to v and is stored in
		 * nr.
		 * We will use binary search on r to find both ends of
		 * the sequence of values that are equal to v in case
		 * the position is "too far" (more than rscan
		 * away). */

		/* first find the location of the first value in r
		 * that is >= v, then find the location of the first
		 * value in r that is > v; the difference is the
		 * number of values equal to v */
		nr = canditer_search(&rci, v, true);
		canditer_setidx(&rci, nr);
		if (nr == rci.ncand) {
			/* nothing found */
			break;
		}

		/* now find the end of the sequence of equal values v */

		/* if r is key, there is zero or one match, otherwise
		 * look ahead a little (rscan) in r to see whether
		 * we're better off doing a binary search */
		if (canditer_peek(&rci) == v) {
			nr = 1;
			canditer_next(&rci);
		} else {
			/* rci points to first value > v or end of
			 * r, and nr is the number of values in r that
			 * are equal to v */
			/* no entries in r found */
			continue;
		}
		/* make space: nl values in l match nr values in r, so
		 * we need to add nl * nr values in the results */
		if (maybeextend(r1, r2, nl * nr, lstart, lend, maxsize) != GDK_SUCCEED)
			goto bailout;

		/* maintain properties */
		if (nl > 1) {
			/* value occurs multiple times in l, so entry
			 * in r will be repeated multiple times: hence
			 * r2 is not key and not dense */
			if (r2) {
				r2->tkey = false;
				r2->tseqbase = oid_nil;
			}
			/* multiple different values will be inserted
			 * in r1 (always in order), so not reverse
			 * ordered anymore */
			r1->trevsorted = false;
		}
		if (nr > 1) {
			/* value occurs multiple times in r, so entry
			 * in l will be repeated multiple times: hence
			 * r1 is not key and not dense */
			r1->tkey = false;
			r1->tseqbase = oid_nil;
			/* multiple different values will be inserted
			 * in r2 (in order), so not reverse ordered
			 * anymore */
			if (r2) {
				r2->trevsorted = false;
				if (nl > 1) {
					/* multiple values in l match
					 * multiple values in r, so an
					 * ordered sequence will be
					 * inserted multiple times in
					 * r2, so r2 is not ordered
					 * anymore */
					r2->tsorted = false;
				}
			}
		}
		if (BATcount(r1) > 0) {
			/* a new, higher value will be inserted into
			 * r1, so r1 is not reverse ordered anymore */
			r1->trevsorted = false;
			/* a new higher value will be added to r2 */
			if (r2) {
				r2->trevsorted = false;
			}
			if (BATtdense(r1) &&
			    ((oid *) r1->theap->base)[r1->batCount - 1] + 1 != l->hseqbase + lstart - nl) {
				r1->tseqbase = oid_nil;
			}
		}

		if (r2 &&
		    BATcount(r2) > 0 &&
		    BATtdense(r2) &&
		    ((oid *) r2->theap->base)[r2->batCount - 1] + 1 != r->hseqbase + rci.next - nr) {
			r2->tseqbase = oid_nil;
		}

		/* insert values */
		lv = l->hseqbase + lstart - nl;
		for (i = 0; i < nl; i++) {
			BUN j;

			for (j = 0; j < nr; j++) {
				APPEND(r1, lv);
			}
			if (r2) {
				oid rv = r->hseqbase + rci.next - nr;

				for (j = 0; j < nr; j++) {
					APPEND(r2, rv);
					rv++;
				}
			}
			lv++;
		}
	}
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	if (r2) {
		BATsetcount(r2, BATcount(r2));
		assert(BATcount(r1) == BATcount(r2));
	}
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2) {
			r2->tseqbase = 0;
		}
	}
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT "," "r=" ALGOBATFMT ","
		  "nil_matches=%s;%s %s "
		  "-> " ALGOBATFMT "," ALGOOPTBATFMT " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  nil_matches ? "true" : "false",
		  swapped ? " swapped" : "", reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);

	return GDK_SUCCEED;

  bailout:
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}

/* Perform a "merge" join on l and r (if both are sorted) with
 * optional candidate lists, or join using binary search on r if l is
 * not sorted.  The return BATs have already been created by the
 * caller.
 *
 * If nil_matches is set, nil values are treated as ordinary values
 * that can match; otherwise nil values never match.
 *
 * If nil_on_miss is set, a nil value is returned in r2 if there is no
 * match in r for a particular value in l (left outer join).
 *
 * If semi is set, only a single set of values in r1/r2 is returned if
 * there is a match of l in r, no matter how many matches there are in
 * r; otherwise all matches are returned.
 *
 * If max_one is set, only a single match is allowed.  This is like
 * semi, but enforces the single match.
 *
 * t0 and swapped are only for debugging (ALGOMASK set in GDKdebug).
 */
static gdk_return
mergejoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r,
	  struct canditer *restrict lci, struct canditer *restrict rci,
	  bool nil_matches, bool nil_on_miss, bool semi, bool only_misses,
	  bool not_in, bool max_one, bool min_one, BUN estimate,
	  lng t0, bool swapped,
	  const char *reason)
{
	/* [lr]scan determine how far we look ahead in l/r in order to
	 * decide whether we want to do a binary search or a scan */
	BUN lscan, rscan;
	const void *lvals, *rvals; /* the values of l/r (NULL if dense) */
	const char *lvars, *rvars; /* the indirect values (NULL if fixed size) */
	int lwidth, rwidth;	   /* width of the values */
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = ATOMcompare(l->ttype);
	const void *v;		/* points to value under consideration */
	const void *prev = NULL;
	BUN nl, nr;
	bool insert_nil;
	/* equal_order is set if we can scan both BATs in the same
	 * order, so when both are sorted or both are reverse sorted
	 * -- important to know in order to skip over values; if l is
	 * not sorted, this must be set to true and we will always do a
	 * binary search on all of r */
	bool equal_order;
	/* [lr]ordering is either 1 or -1 depending on the order of
	 * l/r: it determines the comparison function used */
	int lordering, rordering;
	oid lv;
	BUN i, j;		/* counters */
	oid lval = oid_nil, rval = oid_nil; /* temporary space to point v to */
	struct canditer llci, rrci;
	struct canditer *mlci, xlci;
	struct canditer *mrci, xrci;

	if (lci->tpe == cand_dense && lci->ncand == BATcount(l) &&
	    rci->tpe == cand_dense && rci->ncand == BATcount(r) &&
	    !nil_on_miss && !semi && !max_one && !min_one && !only_misses &&
	    !not_in &&
	    l->tsorted && r->tsorted) {
		/* special cases with far fewer options */
		if (complex_cand(r))
			return mergejoin_cand(r1p, r2p, l, r, nil_matches,
					      estimate, t0, swapped, __func__);
		switch (ATOMbasetype(l->ttype)) {
		case TYPE_int:
			return mergejoin_int(r1p, r2p, l, r, nil_matches,
					     estimate, t0, swapped, __func__);
		case TYPE_lng:
			return mergejoin_lng(r1p, r2p, l, r, nil_matches,
					     estimate, t0, swapped, __func__);
		}
	}

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(r->tsorted || r->trevsorted);

	MT_thread_setalgorithm(__func__);
	if (BATtvoid(l)) {
		/* l->ttype == TYPE_void && is_oid_nil(l->tseqbase) is
		 * handled by selectjoin */
		assert(!is_oid_nil(l->tseqbase));
		canditer_init(&llci, NULL, l);
		lvals = NULL;
	} else {
		lvals = Tloc(l, 0);			      /* non NULL */
		llci = (struct canditer) {.tpe = cand_dense}; /* not used */
	}
	rrci = (struct canditer) {.tpe = cand_dense};
	if (BATtvoid(r)) {
		if (!is_oid_nil(r->tseqbase))
			canditer_init(&rrci, NULL, r);
		rvals = NULL;
	} else {
		rvals = Tloc(r, 0);
	}
	if (l->tvarsized && l->ttype) {
		assert(r->tvarsized && r->ttype);
		lvars = l->tvheap->base;
		rvars = r->tvheap->base;
	} else {
		assert(!r->tvarsized || !r->ttype);
		lvars = rvars = NULL;
	}
	lwidth = l->twidth;
	rwidth = r->twidth;

	if (not_in && rci->ncand > 0 && !r->tnonil &&
	    ((BATtvoid(l) && l->tseqbase == oid_nil) ||
	     (BATtvoid(r) && r->tseqbase == oid_nil) ||
	     (rvals && cmp(nil, VALUE(r, (r->tsorted ? rci->seq : canditer_last(rci)) - r->hseqbase)) == 0)))
		return nomatch(r1p, r2p, l, r, lci, false, false,
			       __func__, t0);

	if (lci->ncand == 0 ||
	    rci->ncand == 0 ||
	    (!nil_matches &&
	     ((l->ttype == TYPE_void && is_oid_nil(l->tseqbase)) ||
	      (r->ttype == TYPE_void && is_oid_nil(r->tseqbase)))) ||
	    (l->ttype == TYPE_void && is_oid_nil(l->tseqbase) &&
	     (r->tnonil ||
	      (r->ttype == TYPE_void && !is_oid_nil(r->tseqbase)))) ||
	    (r->ttype == TYPE_void && is_oid_nil(r->tseqbase) &&
	     (l->tnonil ||
	      (l->ttype == TYPE_void && !is_oid_nil(l->tseqbase))))) {
		/* there are no matches */
		return nomatch(r1p, r2p, l, r, lci, nil_on_miss, only_misses,
			       __func__, t0);
	}

	BUN maxsize = joininitresults(r1p, r2p, lci->ncand, rci->ncand,
				      l->tkey, r->tkey, semi | max_one,
				      nil_on_miss, only_misses, min_one,
				      estimate);
	if (maxsize == BUN_NONE)
		return GDK_FAIL;
	BAT *r1 = *r1p;
	BAT *r2 = r2p ? *r2p : NULL;

	if (lci->tpe == cand_mask) {
		mlci = lci;
		canditer_init(&xlci, l, NULL);
		lci = &xlci;
	} else {
		mlci = NULL;
		xlci = (struct canditer) {.tpe = cand_dense}; /* not used */
	}
	if (rci->tpe == cand_mask) {
		mrci = rci;
		canditer_init(&xrci, r, NULL);
		rci = &xrci;
	} else {
		mrci = NULL;
		xrci = (struct canditer) {.tpe = cand_dense}; /* not used */
	}

	if (l->tsorted || l->trevsorted) {
		equal_order = (l->tsorted && r->tsorted) ||
			(l->trevsorted && r->trevsorted &&
			 !BATtvoid(l) && !BATtvoid(r));
		lordering = l->tsorted && (r->tsorted || !equal_order) ? 1 : -1;
		rordering = equal_order ? lordering : -lordering;
		if (!l->tnonil && !nil_matches && !nil_on_miss) {
			nl = binsearch(NULL, 0, l->ttype, lvals, lvars, lwidth, 0, BATcount(l), nil, lordering, 1);
			nl = canditer_search(lci, nl + l->hseqbase, true);
			if (l->tsorted) {
				canditer_setidx(lci, nl);
			} else if (l->trevsorted) {
				lci->ncand = nl;
			}
		}
		/* determine opportunistic scan window for l */
		lscan = 4 + ilog2(lci->ncand);
	} else {
		/* if l not sorted, we will always use binary search
		 * on r */
		assert(!BATtvoid(l)); /* void is always sorted */
		lscan = 0;
		equal_order = true;
		lordering = 1;
		rordering = r->tsorted ? 1 : -1;
	}
	/* determine opportunistic scan window for r; if l is not
	 * sorted this is only used to find range of equal values */
	rscan = 4 + ilog2(rci->ncand);

	if (!equal_order) {
		/* we go through r backwards */
		canditer_setidx(rci, rci->ncand);
	}
	/* At this point the various variables that help us through
	 * the algorithm have been set.  The table explains them.  The
	 * first two columns are the inputs, the next three columns
	 * are the variables, the final two columns indicate how the
	 * variables can be used.
	 *
	 * l/r    sl/sr | vals  cand  off | result   value being matched
	 * -------------+-----------------+----------------------------------
	 * dense  NULL  | NULL  NULL  set | i        off==nil?nil:i+off
	 * dense  dense | NULL  NULL  set | i        off==nil?nil:i+off
	 * dense  set   | NULL  set   set | cand[i]  off==nil?nil:cand[i]+off
	 * set    NULL  | set   NULL  0   | i        vals[i]
	 * set    dense | set   NULL  0   | i        vals[i]
	 * set    set   | set   set   0   | cand[i]  vals[cand[i]]
	 *
	 * If {l,r}off is lng_nil, all values in the corresponding bat
	 * are oid_nil because the bat has type VOID and the tseqbase
	 * is nil.
	 */

	/* Before we start adding values to r1 and r2, the properties
	 * are as follows:
	 * tseqbase - 0
	 * tkey - true
	 * tsorted - true
	 * trevsorted - true
	 * tnil - false
	 * tnonil - true
	 * We will modify these as we go along.
	 */
	while (lci->next < lci->ncand) {
		if (lscan == 0) {
			/* always search r completely */
			assert(equal_order);
			canditer_reset(rci);
		} else {
			/* If l is sorted (lscan > 0), we look at the
			 * next value in r to see whether we can jump
			 * over a large section of l using binary
			 * search.  We do this by looking ahead in l
			 * (lscan far, to be precise) and seeing if
			 * the value there is still too "small"
			 * (definition depends on sort order of l).
			 * If it is, we use binary search on l,
			 * otherwise we scan l for the next position
			 * with a value greater than or equal to the
			 * value in r.
			 * The next value to match in r is the first
			 * if equal_order is set, the last
			 * otherwise.
			 * When skipping over values in l, we count
			 * how many we skip in nlx.  We need this in
			 * case only_misses or nil_on_miss is set, and
			 * to properly set the dense property in the
			 * first output BAT. */
			BUN nlx = 0; /* number of non-matching values in l */

			if (equal_order) {
				if (rci->next == rci->ncand)
					v = NULL; /* no more values */
				else if (mrci) {
					oid rv = canditer_mask_next(mrci, canditer_peek(rci), true);
					v = rv == oid_nil ? NULL : VALUE(r, rv - r->hseqbase);
				} else
					v = VALUE(r, canditer_peek(rci) - r->hseqbase);
			} else {
				if (rci->next == 0)
					v = NULL; /* no more values */
				else if (mrci) {
					oid rv = canditer_mask_next(mrci, canditer_peekprev(rci), false);
					v = rv == oid_nil ? NULL : VALUE(r, rv - r->hseqbase);
				} else
					v = VALUE(r, canditer_peekprev(rci) - r->hseqbase);
			}
			/* here, v points to next value in r, or if
			 * we're at the end of r, v is NULL */
			if (v == NULL) {
				nlx = lci->ncand - lci->next;
			} else {
				if (lscan < lci->ncand - lci->next) {
					lv = canditer_idx(lci, lci->next + lscan);
					lv -= l->hseqbase;
					if (lvals) {
						if (lordering * cmp(VALUE(l, lv), v) < 0) {
							nlx = binsearch(NULL, 0, l->ttype, lvals, lvars, lwidth, lv, BATcount(l), v, lordering, 0);
							nlx = canditer_search(lci, nlx + l->hseqbase, true);
							nlx -= lci->next;
						}
					} else {
						assert(lordering == 1);
						if (canditer_idx(&llci, lv) < *(const oid *)v) {
							nlx = canditer_search(&llci, *(const oid *)v, true);
							nlx = canditer_search(lci, nlx + l->hseqbase, true);
							nlx -= lci->next;
						}
					}
					if (mlci) {
						lv = canditer_mask_next(mlci, lci->seq + lci->next + nlx, true);
						if (lv == oid_nil)
							nlx = lci->ncand - lci->next;
						else
							nlx = lv - lci->seq - lci->next;
					}
					if (lci->next + nlx == lci->ncand)
						v = NULL;
				}
			}
			if (nlx > 0) {
				if (only_misses) {
					if (maybeextend(r1, r2, nlx, lci->next, lci->ncand, maxsize) != GDK_SUCCEED)
						goto bailout;
					while (nlx > 0) {
						lv = canditer_next(lci);
						if (mlci == NULL || canditer_contains(mlci, lv))
							APPEND(r1, lv);
						nlx--;
					}
					if (r1->trevsorted && BATcount(r1) > 1)
						r1->trevsorted = false;
				} else if (nil_on_miss) {
					if (r2->tnonil) {
						r2->tnil = true;
						r2->tnonil = false;
						r2->tseqbase = oid_nil;
						r2->tsorted = false;
						r2->trevsorted = false;
						r2->tkey = false;
					}
					if (maybeextend(r1, r2, nlx, lci->next, lci->ncand, maxsize) != GDK_SUCCEED)
						goto bailout;
					while (nlx > 0) {
						lv = canditer_next(lci);
						if (mlci == NULL || canditer_contains(mlci, lv)) {
							APPEND(r1, lv);
							APPEND(r2, oid_nil);
						}
						nlx--;
					}
					if (r1->trevsorted && BATcount(r1) > 1)
						r1->trevsorted = false;
				} else {
					canditer_setidx(lci, lci->next + nlx);
				}
			}
			if (v == NULL) {
				/* we have exhausted the inputs */
				break;
			}
		}

		/* Here we determine the next value in l that we are
		 * going to try to match in r.  We will also count the
		 * number of occurrences in l of that value.
		 * Afterwards, v points to the value and nl is the
		 * number of times it occurs.  Also, lci will point to
		 * the next value to be considered (ready for the next
		 * iteration).
		 * If there are many equal values in l (more than
		 * lscan), we will use binary search to find the end
		 * of the sequence.  Obviously, we can do this only if
		 * l is actually sorted (lscan > 0). */
		nl = 1;		/* we'll match (at least) one in l */
		nr = 0;		/* maybe we won't match anything in r */
		lv = canditer_peek(lci);
		if (mlci) {
			lv = canditer_mask_next(mlci, lv, true);
			if (lv == oid_nil)
				break;
			canditer_setidx(lci, canditer_search(lci, lv, true));
		}
		v = VALUE(l, lv - l->hseqbase);
		if (l->tkey) {
			/* if l is key, there is a single value */
		} else if (lscan > 0 &&
			   lscan < lci->ncand - lci->next &&
			   cmp(v, VALUE(l, canditer_idx(lci, lci->next + lscan) - l->hseqbase)) == 0) {
			/* lots of equal values: use binary search to
			 * find end */
			assert(lvals != NULL);
			nl = binsearch(NULL, 0,
				       l->ttype, lvals, lvars,
				       lwidth, lci->next + lscan,
				       BATcount(l),
				       v, lordering, 1);
			nl = canditer_search(lci, nl + l->hseqbase, true);
			nl -= lci->next;
		} else {
			struct canditer ci = *lci; /* work on copy */
			nl = 0; /* it will be incremented again */
			do {
				canditer_next(&ci);
				nl++;
			} while (ci.next < ci.ncand &&
				 cmp(v, VALUE(l, canditer_peek(&ci) - l->hseqbase)) == 0);
		}
		/* lci->next + nl is the position for the next iteration */

		if ((!nil_matches || not_in) && !l->tnonil && cmp(v, nil) == 0) {
			if (not_in) {
				/* just skip the whole thing: nils
				 * don't cause any output */
				canditer_setidx(lci, lci->next + nl);
				continue;
			}
			/* v is nil and nils don't match anything, set
			 * to NULL to indicate nil */
			v = NULL;
		}

		/* First we find the "first" value in r that is "at
		 * least as large" as v, then we find the "first"
		 * value in r that is "larger" than v.  The difference
		 * is the number of values equal to v and is stored in
		 * nr.  The definitions of "larger" and "first" depend
		 * on the orderings of l and r.  If equal_order is
		 * set, we go through r from low to high (this
		 * includes the case that l is not sorted); otherwise
		 * we go through r from high to low.
		 * In either case, we will use binary search on r to
		 * find both ends of the sequence of values that are
		 * equal to v in case the position is "too far" (more
		 * than rscan away). */
		if (v == NULL) {
			nr = 0;	/* nils don't match anything */
		} else if (r->ttype == TYPE_void && is_oid_nil(r->tseqbase)) {
			if (is_oid_nil(*(const oid *) v)) {
				/* all values in r match */
				nr = rci->ncand;
			} else {
				/* no value in r matches */
				nr = 0;
			}
			/* in either case, we're done after this */
			canditer_setidx(rci, equal_order ? rci->ncand : 0);
		} else if (equal_order) {
			/* first find the location of the first value
			 * in r that is >= v, then find the location
			 * of the first value in r that is > v; the
			 * difference is the number of values equal
			 * v; we change rci */

			/* look ahead a little (rscan) in r to
			 * see whether we're better off doing
			 * a binary search */
			if (rvals) {
				if (rscan < rci->ncand - rci->next &&
				    rordering * cmp(v, VALUE(r, canditer_idx(rci, rci->next + rscan) - r->hseqbase)) > 0) {
					/* value too far away in r:
					 * use binary search */
					lv = binsearch(NULL, 0, r->ttype, rvals, rvars, rwidth, rci->next + rscan, BATcount(r), v, rordering, 0);
					lv = canditer_search(rci, lv + r->hseqbase, true);
					canditer_setidx(rci, lv);
				} else {
					/* scan r for v */
					while (rci->next < rci->ncand) {
						if (rordering * cmp(v, VALUE(r, canditer_peek(rci) - r->hseqbase)) <= 0)
							break;
						canditer_next(rci);
					}
				}
				if (rci->next < rci->ncand &&
				    cmp(v, VALUE(r, canditer_peek(rci) - r->hseqbase)) == 0) {
					/* if we found an equal value,
					 * look for the last equal
					 * value */
					if (r->tkey) {
						/* r is key, there can
						 * only be a single
						 * equal value */
						nr = 1;
						canditer_next(rci);
					} else if (rscan < rci->ncand - rci->next &&
						   cmp(v, VALUE(r, canditer_idx(rci, rci->next + rscan) - r->hseqbase)) == 0) {
						/* many equal values:
						 * use binary search
						 * to find the end */
						nr = binsearch(NULL, 0, r->ttype, rvals, rvars, rwidth, rci->next + rscan, BATcount(r), v, rordering, 1);
						nr = canditer_search(rci, nr + r->hseqbase, true);
						nr -= rci->next;
						canditer_setidx(rci, rci->next + nr);
					} else {
						/* scan r for end of
						 * range */
						do {
							nr++;
							canditer_next(rci);
						} while (rci->next < rci->ncand &&
							 cmp(v, VALUE(r, canditer_peek(rci) - r->hseqbase)) == 0);
					}
				}
			} else {
				assert(rordering == 1);
				rval = canditer_search(&rrci, *(const oid*)v, true) + r->hseqbase;
				lv = canditer_search(rci, rval, true);
				canditer_setidx(rci, lv);
				nr = (canditer_idx(&rrci, canditer_peek(rci) - r->hseqbase) == *(oid*)v);
				if (nr == 1)
					canditer_next(rci);
			}
			/* rci points to first value > v or end of r,
			 * and nr is the number of values in r that
			 * are equal to v */
		} else {
			/* first find the location of the first value
			 * in r that is > v, then find the location
			 * of the first value in r that is >= v; the
			 * difference is the number of values equal
			 * v; we change rci */

			/* look back from the end a little
			 * (rscan) in r to see whether we're
			 * better off doing a binary search */
			if (rvals) {
				if (rci->next > rscan &&
				    rordering * cmp(v, VALUE(r, canditer_idx(rci, rci->next - rscan) - r->hseqbase)) < 0) {
					/* value too far away
					 * in r: use binary
					 * search */
					lv = binsearch(NULL, 0, r->ttype, rvals, rvars, rwidth, 0, rci->next - rscan, v, rordering, 1);
					lv = canditer_search(rci, lv + r->hseqbase, true);
					canditer_setidx(rci, lv);
				} else {
					/* scan r for v */
					while (rci->next > 0 &&
					       rordering * cmp(v, VALUE(r, canditer_peekprev(rci) - r->hseqbase)) < 0)
						canditer_prev(rci);
				}
				if (rci->next > 0 &&
				    cmp(v, VALUE(r, canditer_peekprev(rci) - r->hseqbase)) == 0) {
					/* if we found an equal value,
					 * look for the last equal
					 * value */
					if (r->tkey) {
						/* r is key, there can only be a single equal value */
						nr = 1;
						canditer_prev(rci);
					} else if (rci->next > rscan &&
						   cmp(v, VALUE(r, canditer_idx(rci, rci->next - rscan) - r->hseqbase)) == 0) {
						/* use binary search to find the start */
						nr = binsearch(NULL, 0, r->ttype, rvals, rvars, rwidth, 0, rci->next - rscan, v, rordering, 0);
						nr = canditer_search(rci, nr + r->hseqbase, true);
						nr = rci->next - nr;
						canditer_setidx(rci, rci->next - nr);
					} else {
						/* scan r for start of range */
						do {
							canditer_prev(rci);
							nr++;
						} while (rci->next > 0 &&
							 cmp(v, VALUE(r, canditer_peekprev(rci) - r->hseqbase)) == 0);
					}
				}
			} else {
				lv = canditer_search(&rrci, *(const oid *)v, true);
				lv = canditer_search(rci, lv + r->hseqbase, true);
				nr = (canditer_idx(rci, lv) == *(const oid*)v);
				canditer_setidx(rci, lv);
			}
			/* rci points to first value > v
			 * or end of r, and nr is the number of values
			 * in r that are equal to v */
		}

		if (nr == 0) {
			/* no entries in r found */
			if (!(nil_on_miss | only_misses)) {
				if (min_one) {
					GDKerror("not enough matches");
					goto bailout;
				}
				if (lscan > 0 &&
				    (equal_order ? rci->next == rci->ncand : rci->next == 0)) {
					/* nothing more left to match
					 * in r */
					break;
				}
				canditer_setidx(lci, lci->next + nl);
				continue;
			}
			/* insert a nil to indicate a non-match */
			insert_nil = true;
			nr = 1;
			if (r2) {
				r2->tnil = true;
				r2->tnonil = false;
				r2->tsorted = false;
				r2->trevsorted = false;
				r2->tseqbase = oid_nil;
				r2->tkey = false;
			}
		} else if (nr > 1 && max_one) {
			GDKerror("more than one match");
			goto bailout;
		} else if (only_misses) {
			/* we had a match, so we're not interested */
			canditer_setidx(lci, lci->next + nl);
			continue;
		} else {
			insert_nil = false;
			if (semi) {
				/* for semi-join, only insert single
				 * value */
				nr = 1;
			}
		}
		/* make space: nl values in l match nr values in r, so
		 * we need to add nl * nr values in the results */
		if (maybeextend(r1, r2, nl * nr, lci->next, lci->ncand, maxsize) != GDK_SUCCEED)
			goto bailout;

		/* maintain properties */
		if (nl > 1) {
			if (r2) {
				/* value occurs multiple times in l,
				 * so entry in r will be repeated
				 * multiple times: hence r2 is not key
				 * and not dense */
				r2->tkey = false;
				r2->tseqbase = oid_nil;
			}
			/* multiple different values will be inserted
			 * in r1 (always in order), so not reverse
			 * ordered anymore */
			r1->trevsorted = false;
		}
		if (nr > 1) {
			/* value occurs multiple times in r, so entry
			 * in l will be repeated multiple times: hence
			 * r1 is not key and not dense */
			r1->tkey = false;
			if (r2) {
				/* multiple different values will be
				 * inserted in r2 (in order), so not
				 * reverse ordered anymore */
				r2->trevsorted = false;
				if (nl > 1) {
					/* multiple values in l match
					 * multiple values in r, so an
					 * ordered sequence will be
					 * inserted multiple times in
					 * r2, so r2 is not ordered
					 * anymore */
					r2->tsorted = false;
				}
			}
		}
		if (lscan == 0) {
			/* deduce relative positions of r matches for
			 * this and previous value in v */
			if (prev && r2) {
				/* keyness or r2 can only be assured
				 * as long as matched values are
				 * ordered */
				int ord = rordering * cmp(prev, v ? v : nil);
				if (ord < 0) {
					/* previous value in l was
					 * less than current */
					r2->trevsorted = false;
					r2->tkey &= r2->tsorted;
				} else if (ord > 0) {
					/* previous value was
					 * greater */
					r2->tsorted = false;
					r2->tkey &= r2->trevsorted;
				} else {
					/* value can be equal if
					 * intervening values in l
					 * didn't match anything; if
					 * multiple values match in r,
					 * r2 won't be sorted */
					r2->tkey = false;
					if (nr > 1) {
						r2->tsorted = false;
						r2->trevsorted = false;
					}
				}
			}
			prev = v ? v : nil;
		}
		if (BATcount(r1) > 0) {
			/* a new, higher value will be inserted into
			 * r1, so r1 is not reverse ordered anymore */
			r1->trevsorted = false;
			if (r2) {
				/* depending on whether l and r are
				 * ordered the same or not, a new
				 * higher or lower value will be added
				 * to r2 */
				if (equal_order)
					r2->trevsorted = false;
				else {
					r2->tsorted = false;
					r2->tseqbase = oid_nil;
				}
			}
		}

		/* insert values: first the left output */
		BUN nladded = 0;
		for (i = 0; i < nl; i++) {
			lv = canditer_next(lci);
			if (mlci == NULL || canditer_contains(mlci, lv)) {
				nladded++;
				for (j = 0; j < nr; j++)
					APPEND(r1, lv);
			}
		}
		nl = nladded;
		/* then the right output, various different ways of
		 * doing it */
		if (r2 == NULL) {
			/* nothing to do */
		} else if (insert_nil) {
			do {
				for (i = 0; i < nr; i++) {
					APPEND(r2, oid_nil);
				}
			} while (--nl > 0);
		} else if (equal_order) {
			struct canditer ci = *rci; /* work on copy */
			if (r2->batCount > 0 &&
			    BATtdense(r2) &&
			    ((oid *) r2->theap->base)[r2->batCount - 1] + 1 != canditer_idx(&ci, ci.next - nr))
				r2->tseqbase = oid_nil;
			do {
				canditer_setidx(&ci, ci.next - nr);
				for (i = 0; i < nr; i++) {
					APPEND(r2, canditer_next(&ci));
				}
			} while (--nl > 0);
		} else {
			if (r2->batCount > 0 &&
			    BATtdense(r2) &&
			    ((oid *) r2->theap->base)[r2->batCount - 1] + 1 != canditer_peek(rci))
				r2->tseqbase = oid_nil;
			do {
				struct canditer ci = *rci; /* work on copy */
				for (i = 0; i < nr; i++) {
					APPEND(r2, canditer_next(&ci));
				}
			} while (--nl > 0);
		}
	}
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	r1->tseqbase = oid_nil;
	if (r1->tkey)
		r1 = virtualize(r1);
	if (r2) {
		BATsetcount(r2, BATcount(r2));
		assert(BATcount(r1) == BATcount(r2));
		r2->tseqbase = oid_nil;
		if (BATcount(r2) <= 1) {
			r2->tkey = true;
			r2 = virtualize(r2);
		}
	}
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ","
		  "r=" ALGOBATFMT ",sl=" ALGOOPTBATFMT ","
		  "sr=" ALGOOPTBATFMT ","
		  "nil_on_miss=%s,semi=%s,only_misses=%s,not_in=%s;%s %s "
		  "-> " ALGOBATFMT "," ALGOOPTBATFMT " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  ALGOOPTBATPAR(lci->s), ALGOOPTBATPAR(rci->s),
		  nil_on_miss ? "true" : "false",
		  semi ? "true" : "false",
		  only_misses ? "true" : "false",
		  not_in ? "true" : "false",
		  swapped ? " swapped" : "", reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);

	return GDK_SUCCEED;

  bailout:
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}

#define HASHLOOPBODY()							\
	do {								\
		if (nr >= 1 && max_one) {				\
			GDKerror("more than one match");		\
			goto bailout;					\
		}							\
		if (maybeextend(r1, r2, 1, lci->next, lci->ncand, maxsize) != GDK_SUCCEED) \
			goto bailout;					\
		APPEND(r1, lo);						\
		if (r2)							\
			APPEND(r2, ro);					\
		nr++;							\
	} while (false)

#define EQ_int(a, b)	((a) == (b))
#define EQ_lng(a, b)	((a) == (b))
#ifdef HAVE_HGE
#define EQ_uuid(a, b)	((a).h == (b).h)
#else
#define EQ_uuid(a, b)	(memcmp((a).u, (b).u, UUID_SIZE) == 0)
#endif

#define HASHJOIN(TYPE)							\
	do {								\
		TYPE *rvals = Tloc(r, 0);				\
		TYPE *lvals = Tloc(l, 0);				\
		TYPE v;							\
		if (!hash_cand) {					\
			MT_rwlock_rdlock(&r->thashlock);		\
			locked = true;	/* in case we abandon */	\
			hsh = r->thash;	/* re-initialize inside lock */	\
		}							\
		while (lci->next < lci->ncand) {			\
			lo = canditer_next(lci);			\
			v = lvals[lo - l->hseqbase];			\
			nr = 0;						\
			if ((!nil_matches || not_in) && is_##TYPE##_nil(v)) { \
				/* no match */				\
				if (not_in) {				\
					lskipped = BATcount(r1) > 0;	\
					continue;			\
				}					\
			} else if (hash_cand) {				\
				/* private hash: no locks */		\
				for (rb = HASHget(hsh, hash_##TYPE(hsh, &v)); \
				     rb != HASHnil(hsh);		\
				     rb = HASHgetlink(hsh, rb)) {	\
					ro = canditer_idx(rci, rb);	\
					if (!EQ_##TYPE(v, rvals[ro - r->hseqbase])) \
						continue;		\
					if (only_misses) {		\
						nr++;			\
						break;			\
					}				\
					HASHLOOPBODY();			\
					if (semi && !max_one)		\
						break;			\
				}					\
			} else if (rci->tpe != cand_dense) {		\
				for (rb = HASHget(hsh, hash_##TYPE(hsh, &v)); \
				     rb != HASHnil(hsh);		\
				     rb = HASHgetlink(hsh, rb)) {	\
					if (rb >= rl && rb < rh &&	\
					    EQ_##TYPE(v, rvals[rb]) &&	\
					    canditer_contains(rci, ro = (oid) (rb - roff + rseq))) { \
						if (only_misses) {	\
							nr++;		\
							break;		\
						}			\
						HASHLOOPBODY();		\
						if (semi && !max_one)	\
							break;		\
					}				\
				}					\
			} else {					\
				for (rb = HASHget(hsh, hash_##TYPE(hsh, &v)); \
				     rb != HASHnil(hsh);		\
				     rb = HASHgetlink(hsh, rb)) {	\
					if (rb >= rl && rb < rh &&	\
					    EQ_##TYPE(v, rvals[rb])) {	\
						if (only_misses) {	\
							nr++;		\
							break;		\
						}			\
						ro = (oid) (rb - roff + rseq); \
						HASHLOOPBODY();		\
						if (semi && !max_one)	\
							break;		\
					}				\
				}					\
			}						\
			if (nr == 0) {					\
				if (only_misses) {			\
					nr = 1;				\
					if (maybeextend(r1, r2, 1, lci->next, lci->ncand, maxsize) != GDK_SUCCEED) \
						goto bailout;		\
					APPEND(r1, lo);			\
					if (lskipped)			\
						r1->tseqbase = oid_nil;	\
				} else if (nil_on_miss) {		\
					nr = 1;				\
					r2->tnil = true;		\
					r2->tnonil = false;		\
					r2->tkey = false;		\
					if (maybeextend(r1, r2, 1, lci->next, lci->ncand, maxsize) != GDK_SUCCEED) \
						goto bailout;		\
					APPEND(r1, lo);			\
					APPEND(r2, oid_nil);		\
				} else if (min_one) {			\
					GDKerror("not enough matches");	\
					goto bailout;			\
				} else {				\
					lskipped = BATcount(r1) > 0;	\
				}					\
			} else if (only_misses) {			\
				lskipped = BATcount(r1) > 0;		\
			} else {					\
				if (lskipped) {				\
					/* note, we only get here in an	\
					 * iteration *after* lskipped was \
					 * first set to true, i.e. we did \
					 * indeed skip values in l */	\
					r1->tseqbase = oid_nil;		\
				}					\
				if (nr > 1) {				\
					r1->tkey = false;		\
					r1->tseqbase = oid_nil;		\
				}					\
			}						\
			if (nr > 0 && BATcount(r1) > nr)		\
				r1->trevsorted = false;			\
		}							\
		if (!hash_cand) {					\
			locked = false;					\
			MT_rwlock_rdunlock(&r->thashlock);		\
		}							\
	} while (0)

/* Implementation of join using a hash lookup of values in the right
 * column. */
static gdk_return
hashjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r,
	 struct canditer *restrict lci, struct canditer *restrict rci,
	 bool nil_matches, bool nil_on_miss, bool semi, bool only_misses,
	 bool not_in, bool max_one, bool min_one,
	 BUN estimate, lng t0, bool swapped,
	 bool hash, bool phash, bool hash_cand,
	 const char *reason)
{
	oid lo, ro;
	BATiter ri;
	BUN rb, roff = 0;
	/* rl, rh: lower and higher bounds for BUN values in hash table */
	BUN rl, rh;
	oid rseq;
	BUN nr;
	const char *lvals;
	const char *lvars;
	int lwidth;
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = ATOMcompare(l->ttype);
	oid lval = oid_nil;	/* hold value if l is dense */
	const char *v = (const char *) &lval;
	bool lskipped = false;	/* whether we skipped values in l */
	Hash *restrict hsh = NULL;
	bool locked = false;

	assert(!BATtvoid(r));
	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));

	int t = ATOMbasetype(r->ttype);
	if (r->ttype == TYPE_void || l->ttype == TYPE_void)
		t = TYPE_void;

	lwidth = l->twidth;
	lvals = (const char *) Tloc(l, 0);
	if (l->tvarsized && l->ttype) {
		assert(r->tvarsized && r->ttype);
		lvars = l->tvheap->base;
	} else {
		assert(!r->tvarsized || !r->ttype);
		lvars = NULL;
	}
	/* offset to convert BUN to OID for value in right column */
	rseq = r->hseqbase;

	if (lci->ncand == 0 || rci->ncand== 0)
		return nomatch(r1p, r2p, l, r, lci,
			       nil_on_miss, only_misses, __func__, t0);

	BUN maxsize = joininitresults(r1p, r2p, lci->ncand, rci->ncand,
				      l->tkey, r->tkey, semi | max_one,
				      nil_on_miss, only_misses, min_one,
				      estimate);
	if (maxsize == BUN_NONE)
		return GDK_FAIL;

	BAT *r1 = *r1p;
	BAT *r2 = r2p ? *r2p : NULL;

	rl = rci->seq - r->hseqbase;
	rh = canditer_last(rci) + 1 - r->hseqbase;
	if (hash_cand) {
		/* we need to create a hash on r specific for the
		 * candidate list */
		char ext[32];
		assert(rci->s);
		MT_thread_setalgorithm(swapped ? "hashjoin using candidate hash (swapped)" : "hashjoin using candidate hash");
		TRC_DEBUG(ALGO, ALGOBATFMT ": creating "
			  "hash for candidate list " ALGOBATFMT "%s%s\n",
			  ALGOBATPAR(r), ALGOBATPAR(rci->s),
			  r->thash ? " ignoring existing hash" : "",
			  swapped ? " (swapped)" : "");
		if (snprintf(ext, sizeof(ext), "thshjn%x",
			     (unsigned) THRgettid()) >= (int) sizeof(ext))
			goto bailout;
		if ((hsh = BAThash_impl(r, rci, ext)) == NULL) {
			goto bailout;
		}
	} else if (phash) {
		/* there is a hash on the parent which we should use */
		MT_thread_setalgorithm(swapped ? "hashjoin using parent hash (swapped)" : "hashjoin using parent hash");
		BAT *b = BBPdescriptor(VIEWtparent(r));
		TRC_DEBUG(ALGO, "%s(%s): using "
			  "parent(" ALGOBATFMT ") for hash%s\n",
			  __func__,
			  BATgetId(r), ALGOBATPAR(b),
			  swapped ? " (swapped)" : "");
		hsh = b->thash;
		roff = r->tbaseoff - b->tbaseoff;
		rl += roff;
		rh += roff;
		r = b;
	} else if (hash) {
		/* there is a hash on r which we should use */
		MT_thread_setalgorithm(swapped ? "hashjoin using existing hash (swapped)" : "hashjoin using existing hash");
		hsh = r->thash;
		TRC_DEBUG(ALGO, ALGOBATFMT ": using "
			  "existing hash%s\n",
			  ALGOBATPAR(r),
			  swapped ? " (swapped)" : "");
	} else {
		/* we need to create a hash on r */
		MT_thread_setalgorithm(swapped ? "hashjoin using new hash (swapped)" : "hashjoin using new hash");
		TRC_DEBUG(ALGO, ALGOBATFMT ": creating hash%s\n",
			  ALGOBATPAR(r),
			  swapped ? " (swapped)" : "");
		if (BAThash(r) != GDK_SUCCEED)
			goto bailout;
		hsh = r->thash;
	}
	assert(hsh != NULL);

	ri = bat_iterator(r);

	if (not_in && !r->tnonil) {
		/* check whether there is a nil on the right, since if
		 * so, we should return an empty result */
		if (hash_cand) {
			for (rb = HASHget(hsh, HASHprobe(hsh, nil));
			     rb != HASHnil(hsh);
			     rb = HASHgetlink(hsh, rb)) {
				ro = canditer_idx(rci, rb);
				if ((*cmp)(nil, BUNtail(ri, ro - r->hseqbase)) == 0) {
					HEAPfree(&hsh->heaplink, true);
					HEAPfree(&hsh->heapbckt, true);
					GDKfree(hsh);
					return nomatch(r1p, r2p, l, r, lci,
						       false, false, __func__, t0);
				}
			}
		} else {
			for (rb = HASHget(hsh, HASHprobe(hsh, nil));
			     rb != HASHnil(hsh);
			     rb = HASHgetlink(hsh, rb)) {
				if (rb >= rl && rb < rh &&
				    (cmp == NULL ||
				     (*cmp)(nil, BUNtail(ri, rb)) == 0)) {
					return nomatch(r1p, r2p, l, r, lci,
						       false, false,
						       __func__, t0);
				}
			}
		}
	}

	/* basic properties will be adjusted if necessary later on,
	 * they were initially set by joininitresults() */

	if (r2) {
		r2->tkey = l->tkey;
		/* r2 is not likely to be sorted (although it is
		 * certainly possible) */
		r2->tsorted = false;
		r2->trevsorted = false;
		r2->tseqbase = oid_nil;
	}

	if (lci->tpe != cand_dense)
		r1->tseqbase = oid_nil;

	switch (t) {
	case TYPE_int:
		HASHJOIN(int);
		break;
	case TYPE_lng:
		HASHJOIN(lng);
		break;
	case TYPE_uuid:
		HASHJOIN(uuid);
		break;
	default:
		if (!hash_cand) {
			MT_rwlock_rdlock(&r->thashlock);
			locked = true;	/* in case we abandon */
			hsh = r->thash;	/* re-initialize inside lock */
		}
		while (lci->next < lci->ncand) {
			lo = canditer_next(lci);
			if (BATtvoid(l)) {
				if (BATtdense(l))
					lval = lo - l->hseqbase + l->tseqbase;
			} else {
				v = VALUE(l, lo - l->hseqbase);
			}
			nr = 0;
			if ((!nil_matches || not_in) && cmp(v, nil) == 0) {
				/* no match */
				if (not_in) {
					lskipped = BATcount(r1) > 0;
					continue;
				}
			} else if (hash_cand) {
				for (rb = HASHget(hsh, HASHprobe(hsh, v));
				     rb != HASHnil(hsh);
				     rb = HASHgetlink(hsh, rb)) {
					ro = canditer_idx(rci, rb);
					if ((*cmp)(v, BUNtail(ri, ro - r->hseqbase)) != 0)
						continue;
					if (only_misses) {
						nr++;
						break;
					}
					HASHLOOPBODY();
					if (semi && !max_one)
						break;
				}
			} else if (rci->tpe != cand_dense) {
				for (rb = HASHget(hsh, HASHprobe(hsh, v));
				     rb != HASHnil(hsh);
				     rb = HASHgetlink(hsh, rb)) {
					if (rb >= rl && rb < rh &&
					    (*(cmp))(v, BUNtail(ri, rb)) == 0 &&
					    canditer_contains(rci, ro = (oid) (rb - roff + rseq))) {
						if (only_misses) {
							nr++;
							break;
						}
						HASHLOOPBODY();
						if (semi && !max_one)
							break;
					}
				}
			} else {
				for (rb = HASHget(hsh, HASHprobe(hsh, v));
				     rb != HASHnil(hsh);
				     rb = HASHgetlink(hsh, rb)) {
					if (rb >= rl && rb < rh &&
					    (*(cmp))(v, BUNtail(ri, rb)) == 0) {
						if (only_misses) {
							nr++;
							break;
						}
						ro = (oid) (rb - roff + rseq);
						HASHLOOPBODY();
						if (semi && !max_one)
							break;
					}
				}
			}
			if (nr == 0) {
				if (only_misses) {
					nr = 1;
					if (maybeextend(r1, r2, 1, lci->next, lci->ncand, maxsize) != GDK_SUCCEED)
						goto bailout;
					APPEND(r1, lo);
					if (lskipped)
						r1->tseqbase = oid_nil;
				} else if (nil_on_miss) {
					nr = 1;
					r2->tnil = true;
					r2->tnonil = false;
					r2->tkey = false;
					if (maybeextend(r1, r2, 1, lci->next, lci->ncand, maxsize) != GDK_SUCCEED)
						goto bailout;
					APPEND(r1, lo);
					APPEND(r2, oid_nil);
				} else if (min_one) {
					GDKerror("not enough matches");
					goto bailout;
				} else {
					lskipped = BATcount(r1) > 0;
				}
			} else if (only_misses) {
				lskipped = BATcount(r1) > 0;
			} else {
				if (lskipped) {
					/* note, we only get here in an
					 * iteration *after* lskipped was
					 * first set to true, i.e. we did
					 * indeed skip values in l */
					r1->tseqbase = oid_nil;
				}
				if (nr > 1) {
					r1->tkey = false;
					r1->tseqbase = oid_nil;
				}
			}
			if (nr > 0 && BATcount(r1) > nr)
				r1->trevsorted = false;
		}
		if (!hash_cand) {
			locked = false;
			MT_rwlock_rdunlock(&r->thashlock);
		}
		break;
	}
	if (hash_cand) {
		HEAPfree(&hsh->heaplink, true);
		HEAPfree(&hsh->heapbckt, true);
		GDKfree(hsh);
	}
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	if (BATcount(r1) <= 1) {
		r1->tsorted = true;
		r1->trevsorted = true;
		r1->tkey = true;
		r1->tseqbase = 0;
	}
	if (r2) {
		BATsetcount(r2, BATcount(r2));
		assert(BATcount(r1) == BATcount(r2));
		if (BATcount(r2) <= 1) {
			r2->tsorted = true;
			r2->trevsorted = true;
			r2->tkey = true;
			r2->tseqbase = 0;
		}
	}
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2) {
			r2->tseqbase = 0;
		}
	}
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT "," "r=" ALGOBATFMT
		  ",sl=" ALGOOPTBATFMT "," "sr=" ALGOOPTBATFMT ","
		  "nil_matches=%s,nil_on_miss=%s,semi=%s,only_misses=%s,"
		  "not_in=%s,max_one=%s,min_one=%s;%s %s -> " ALGOBATFMT "," ALGOOPTBATFMT
		  " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  ALGOOPTBATPAR(lci->s), ALGOOPTBATPAR(rci->s),
		  nil_matches ? "true" : "false",
		  nil_on_miss ? "true" : "false",
		  semi ? "true" : "false",
		  only_misses ? "true" : "false",
		  not_in ? "true" : "false",
		  max_one ? "true" : "false",
		  min_one ? "true" : "false",
		  swapped ? " swapped" : "", reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);

	return GDK_SUCCEED;

  bailout:
	if (locked)
		MT_rwlock_rdunlock(&r->thashlock);
	if (hash_cand && hsh) {
		HEAPfree(&hsh->heaplink, true);
		HEAPfree(&hsh->heapbckt, true);
		GDKfree(hsh);
	}
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}

/* population count: count number of 1 bits in a value */
static inline uint32_t __attribute__((__const__))
pop(uint32_t x)
{
#if defined(__GNUC__)
	return (uint32_t) __builtin_popcount(x);
#elif defined(_MSC_VER)
	return (uint32_t) __popcnt((unsigned int) (x));
#else
	/* divide and conquer implementation (the two versions are
	 * essentially equivalent, but the first version is written a
	 * bit smarter) */
#if 1
	x -= (x >> 1) & ~0U/3 /* 0x55555555 */; /* 3-1=2; 2-1=1; 1-0=1; 0-0=0 */
	x = (x & ~0U/5) + ((x >> 2) & ~0U/5) /* 0x33333333 */;
	x = (x + (x >> 4)) & ~0UL/0x11 /* 0x0F0F0F0F */;
	x = (x + (x >> 8)) & ~0UL/0x101 /* 0x00FF00FF */;
	x = (x + (x >> 16)) & 0xFFFF /* ~0UL/0x10001 */;
#else
	x = (x & 0x55555555) + ((x >>  1) & 0x55555555);
	x = (x & 0x33333333) + ((x >>  2) & 0x33333333);
	x = (x & 0x0F0F0F0F) + ((x >>  4) & 0x0F0F0F0F);
	x = (x & 0x00FF00FF) + ((x >>  8) & 0x00FF00FF);
	x = (x & 0x0000FFFF) + ((x >> 16) & 0x0000FFFF);
#endif
	return x;
#endif
}

/* Count the number of unique values for the first half and the complete
 * set (the sample s of b) and return the two values in *cnt1 and
 * *cnt2. In case of error, both values are 0. */
static void
count_unique(BAT *b, BAT *s, BUN *cnt1, BUN *cnt2)
{
	struct canditer ci;
	BUN half;
	BUN cnt = 0;
	const void *v;
	const char *bvals;
	const char *bvars;
	oid bval;
	int bwidth;
	oid i, o;
	const char *nme;
	BUN hb;
	BATiter bi;
	int (*cmp)(const void *, const void *);
	const char *algomsg = "";
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	canditer_init(&ci, b, s);
	half = ci.ncand / 2;

	if (b->tkey || ci.ncand <= 1 || BATtdense(b)) {
		/* trivial: already unique */
		*cnt1 = half;
		*cnt2 = ci.ncand;
		return;
	}

	if ((BATordered(b) && BATordered_rev(b)) ||
	    (b->ttype == TYPE_void && is_oid_nil(b->tseqbase))) {
		/* trivial: all values are the same */
		*cnt1 = *cnt2 = 1;
		return;
	}

	assert(b->ttype != TYPE_void);

	bvals = Tloc(b, 0);
	if (b->tvarsized && b->ttype)
		bvars = b->tvheap->base;
	else
		bvars = NULL;
	bwidth = Tsize(b);
	cmp = ATOMcompare(b->ttype);
	bi = bat_iterator(b);

	*cnt1 = *cnt2 = 0;

	if (BATordered(b) || BATordered_rev(b)) {
		const void *prev = NULL;
		algomsg = "sorted";
		for (i = 0; i < ci.ncand; i++) {
			if (i == half)
				*cnt1 = cnt;
			o = canditer_next(&ci);
			v = VALUE(b, o - b->hseqbase);
			if (prev == NULL || (*cmp)(v, prev) != 0) {
				cnt++;
			}
			prev = v;
		}
		*cnt2 = cnt;
	} else if (ATOMbasetype(b->ttype) == TYPE_bte) {
		unsigned char val;
		uint32_t seen[256 / 32];

		algomsg = "byte-sized atoms";
		assert(bvars == NULL);
		memset(seen, 0, sizeof(seen));
		for (i = 0; i < ci.ncand; i++) {
			if (i == ci.ncand/ 2) {
				cnt = 0;
				for (int j = 0; j < 256 / 32; j++)
					cnt += pop(seen[j]);
				*cnt1 = cnt;
			}
			o = canditer_next(&ci);
			val = ((const unsigned char *) bvals)[o - b->hseqbase];
			if (!(seen[val >> 5] & (1U << (val & 0x1F)))) {
				seen[val >> 5] |= 1U << (val & 0x1F);
			}
		}
		cnt = 0;
		for (int j = 0; j < 256 / 32; j++)
			cnt += pop(seen[j]);
		*cnt2 = cnt;
	} else if (ATOMbasetype(b->ttype) == TYPE_sht) {
		unsigned short val;
		uint32_t *seen = NULL;

		algomsg = "short-sized atoms";
		assert(bvars == NULL);
		seen = GDKzalloc((65536 / 32) * sizeof(seen[0]));
		if (seen == NULL)
			return;
		for (i = 0; i < ci.ncand; i++) {
			if (i == half) {
				cnt = 0;
				for (int j = 0; j < 65536 / 32; j++)
					cnt += pop(seen[j]);
				*cnt1 = cnt;
			}
			o = canditer_next(&ci);
			val = ((const unsigned short *) bvals)[o - b->hseqbase];
			if (!(seen[val >> 5] & (1U << (val & 0x1F)))) {
				seen[val >> 5] |= 1U << (val & 0x1F);
			}
		}
		cnt = 0;
		for (int j = 0; j < 65536 / 32; j++)
			cnt += pop(seen[j]);
		*cnt2 = cnt;
		GDKfree(seen);
		seen = NULL;
	} else {
		BUN prb;
		BUN p;
		BUN mask;
		Hash hs = {0};

		GDKclrerr();	/* not interested in BAThash errors */
		algomsg = "new partial hash";
		nme = BBP_physical(b->batCacheid);
		mask = HASHmask(ci.ncand);
		if (mask < ((BUN) 1 << 16))
			mask = (BUN) 1 << 16;
		if ((hs.heaplink.farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0 ||
		    (hs.heapbckt.farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0 ||
		    snprintf(hs.heaplink.filename, sizeof(hs.heaplink.filename), "%s.thshunil%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs.heaplink.filename) ||
		    snprintf(hs.heapbckt.filename, sizeof(hs.heapbckt.filename), "%s.thshunib%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs.heapbckt.filename) ||
		    HASHnew(&hs, b->ttype, BUNlast(b), mask, BUN_NONE, false) != GDK_SUCCEED) {
			GDKerror("cannot allocate hash table\n");
			HEAPfree(&hs.heaplink, true);
			HEAPfree(&hs.heapbckt, true);
			return;
		}
		for (i = 0; i < ci.ncand; i++) {
			if (i == half)
				*cnt1 = cnt;
			o = canditer_next(&ci);
			v = VALUE(b, o - b->hseqbase);
			prb = HASHprobe(&hs, v);
			for (hb = HASHget(&hs, prb);
			     hb != HASHnil(&hs);
			     hb = HASHgetlink(&hs, hb)) {
				if (cmp(v, BUNtail(bi, hb)) == 0)
					break;
			}
			if (hb == HASHnil(&hs)) {
				p = o - b->hseqbase;
				cnt++;
				/* enter into hash table */
				HASHputlink(&hs, p, HASHget(&hs, prb));
				HASHput(&hs, prb, p);
			}
		}
		*cnt2 = cnt;
		HEAPfree(&hs.heaplink, true);
		HEAPfree(&hs.heapbckt, true);
	}

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " BUNFMT " " BUNFMT " (%s -- " LLFMT "usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  *cnt1, *cnt2, algomsg, GDKusec() - t0);

	return;
}

static double
guess_uniques(BAT *b, struct canditer *ci)
{
	BUN cnt1, cnt2;
	BAT *s1;

	if (b->tkey)
		return (double) ci->ncand;

	if (ci->s == NULL ||
	    (ci->tpe == cand_dense && ci->ncand == BATcount(b))) {
		const ValRecord *p = BATgetprop(b, GDK_UNIQUE_ESTIMATE);
		if (p) {
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT " use cached value\n",
				  ALGOBATPAR(b));
			return p->val.dval;
		}
		s1 = BATsample_with_seed(b, 1000, (uint64_t) GDKusec() * (uint64_t) b->batCacheid);
	} else {
		BAT *s2 = BATsample_with_seed(ci->s, 1000, (uint64_t) GDKusec() * (uint64_t) b->batCacheid);
		s1 = BATproject(s2, ci->s);
		BBPreclaim(s2);
	}
	BUN n2 = BATcount(s1);
	BUN n1 = n2 / 2;
	count_unique(b, s1, &cnt1, &cnt2);
	BBPreclaim(s1);

	double A = (double) (cnt2 - cnt1) / (n2 - n1);
	double B = cnt1 - n1 * A;

	B += A * ci->ncand;
	if (ci->s == NULL ||
	    (ci->tpe == cand_dense && ci->ncand == BATcount(b))) {
		BATsetprop(b, GDK_UNIQUE_ESTIMATE, TYPE_dbl, &B);
	}
	return B;
}

BUN
BATguess_uniques(BAT *b, struct canditer *ci)
{
	struct canditer lci;
	if (ci == NULL) {
		canditer_init(&lci, b, NULL);
		ci = &lci;
	}
	return (BUN) guess_uniques(b, ci);
}

/* estimate the cost of doing a hashjoin with a hash on r; return value
 * is the estimated cost, the last three arguments receive some extra
 * information */
static double
joincost(BAT *r, struct canditer *lci, struct canditer *rci,
	 bool *hash, bool *phash, bool *cand)
{
	bool rhash = BATcheckhash(r);
	bool prhash = false;
	bool rcand = false;
	double rcost = 1;
	bat parent;
	BAT *b;

	if (rci->nvals > 0) {
		/* if we need to do binary search on candidate
		 * list, take that into account */
		rcost += log2((double) rci->nvals);
	}
	rcost *= lci->ncand;
	if (rhash) {
		/* average chain length */
		rcost *= (double) BATcount(r) / r->thash->nheads;
	} else if ((parent = VIEWtparent(r)) != 0 &&
		   (b = BBPdescriptor(parent)) != NULL &&
		   BATcheckhash(b)) {
		rhash = prhash = true;
		/* average chain length */
		rcost *= (double) BATcount(b) / b->thash->nheads;
	} else {
		const ValRecord *prop = BATgetprop(r, GDK_NUNIQUE);
		if (prop) {
			/* we know number of unique values, assume some
			 * collisions */
			rcost *= 1.1 * ((double) BATcount(r) / prop->val.oval);
		} else {
			/* guess number of unique value and work with that */
			rcost *= 1.1 * ((double) BATcount(r) / guess_uniques(r, &(struct canditer){.tpe=cand_dense, .ncand=BATcount(r)}));
		}
#ifdef PERSISTENTHASH
		/* only count the cost of creating the hash for
		 * non-persistent bats */
		if (!(BBP_status(r->batCacheid) & BBPEXISTING) /* || r->theap->dirty */ || GDKinmemory(r->theap->farmid))
#endif
			rcost += BATcount(r) * 2.0;
	}
	if (rci->ncand != BATcount(r) && rci->tpe != cand_mask) {
		/* instead of using the hash on r (cost in rcost), we
		 * can build a new hash on r taking the candidate list
		 * into account; don't do this for masked candidate
		 * since the searching of the candidate list
		 * (canditer_idx) will kill us */
		double rccost;
		if (rhash && !prhash) {
			rccost = (double) BATcount(r) / r->thash->nheads;
		} else {
			ValPtr prop = BATgetprop(r, GDK_NUNIQUE);
			if (prop) {
				/* we know number of unique values, assume some
				 * chains */
				rccost = 1.1 * ((double) BATcount(r) / prop->val.oval);
			} else {
				/* guess number of unique value and work with that */
				rccost = 1.1 * ((double) BATcount(r) / guess_uniques(r, rci));
			}
		}
		rccost *= lci->ncand;
		rccost += rci->ncand * 2.0; /* cost of building the hash */
		if (rccost < rcost) {
			rcost = rccost;
			rcand = true;
		}
	}
	*hash = rhash;
	*phash = prhash;
	*cand = rcand;
	return rcost;
}

#define MASK_EQ		1
#define MASK_LT		2
#define MASK_GT		4
#define MASK_LE		(MASK_EQ | MASK_LT)
#define MASK_GE		(MASK_EQ | MASK_GT)
#define MASK_NE		(MASK_LT | MASK_GT)

static gdk_return
thetajoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int opcode, BUN estimate, const char *reason, lng t0)
{
	struct canditer lci, rci;
	BUN lcnt, rcnt;
	const char *lvals, *rvals;
	const char *lvars, *rvars;
	int lwidth, rwidth;
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = ATOMcompare(l->ttype);
	const void *vl, *vr;
	oid lastr = 0;		/* last value inserted into r2 */
	BUN nr;
	oid lo, ro;
	int c;
	bool lskipped = false;	/* whether we skipped values in l */
	lng loff = 0, roff = 0;
	oid lval = oid_nil, rval = oid_nil;

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert((opcode & (MASK_EQ | MASK_LT | MASK_GT)) != 0);

	lcnt = canditer_init(&lci, l, sl);
	rcnt = canditer_init(&rci, r, sr);

	lvals = BATtvoid(l) ? NULL : (const char *) Tloc(l, 0);
	rvals = BATtvoid(r) ? NULL : (const char *) Tloc(r, 0);
	if (l->tvarsized && l->ttype) {
		assert(r->tvarsized && r->ttype);
		lvars = l->tvheap->base;
		rvars = r->tvheap->base;
	} else {
		assert(!r->tvarsized || !r->ttype);
		lvars = rvars = NULL;
	}
	lwidth = l->twidth;
	rwidth = r->twidth;

	if (BATtvoid(l)) {
		if (!BATtdense(l)) {
			/* trivial: nils don't match anything */
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		}
		loff = (lng) l->tseqbase - (lng) l->hseqbase;
	}
	if (BATtvoid(r)) {
		if (!BATtdense(r)) {
			/* trivial: nils don't match anything */
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		}
		roff = (lng) r->tseqbase - (lng) r->hseqbase;
	}

	BUN maxsize = joininitresults(r1p, r2p, lcnt, rcnt, false, false,
				      false, false, false, false, estimate);
	if (maxsize == BUN_NONE)
		return GDK_FAIL;
	BAT *r1 = *r1p;
	BAT *r2 = r2p ? *r2p : NULL;

	r1->tkey = true;
	r1->tsorted = true;
	r1->trevsorted = true;
	if (r2) {
		r2->tkey = true;
		r2->tsorted = true;
		r2->trevsorted = true;
	}

	/* nested loop implementation for theta join */
	vl = &lval;
	vr = &rval;
	for (BUN li = 0; li < lci.ncand; li++) {
		lo = canditer_next(&lci);
		if (lvals)
			vl = VALUE(l, lo - l->hseqbase);
		else
			lval = (oid) ((lng) lo + loff);
		nr = 0;
		if (cmp(vl, nil) != 0) {
			canditer_reset(&rci);
			for (BUN ri = 0; ri < rci.ncand; ri++) {
				ro = canditer_next(&rci);
				if (rvals)
					vr = VALUE(r, ro - r->hseqbase);
				else
					rval = (oid) ((lng) ro + roff);
				if (cmp(vr, nil) == 0)
					continue;
				c = cmp(vl, vr);
				if (!((opcode & MASK_LT && c < 0) ||
				      (opcode & MASK_GT && c > 0) ||
				      (opcode & MASK_EQ && c == 0)))
					continue;
				if (maybeextend(r1, r2, 1, lci.next, lci.ncand, maxsize) != GDK_SUCCEED)
					goto bailout;
				if (BATcount(r1) > 0) {
					if (r2 && lastr + 1 != ro)
						r2->tseqbase = oid_nil;
					if (nr == 0) {
						r1->trevsorted = false;
						if (r2 == NULL) {
							/* nothing */
						} else if (lastr > ro) {
							r2->tsorted = false;
							r2->tkey = false;
						} else if (lastr < ro) {
							r2->trevsorted = false;
						} else {
							r2->tkey = false;
						}
					}
				}
				APPEND(r1, lo);
				if (r2) {
					APPEND(r2, ro);
				}
				lastr = ro;
				nr++;
			}
		}
		if (nr > 1) {
			r1->tkey = false;
			r1->tseqbase = oid_nil;
			if (r2) {
				r2->trevsorted = false;
			}
		} else if (nr == 0) {
			lskipped = BATcount(r1) > 0;
		} else if (lskipped) {
			r1->tseqbase = oid_nil;
		}
	}
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	if (r2) {
		BATsetcount(r2, BATcount(r2));
		assert(BATcount(r1) == BATcount(r2));
	}
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2) {
			r2->tseqbase = 0;
		}
	}
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT "," "r=" ALGOBATFMT
		  ",sl=" ALGOOPTBATFMT "," "sr=" ALGOOPTBATFMT ","
		  "opcode=%s%s%s; %s -> " ALGOBATFMT "," ALGOOPTBATFMT
		  " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  ALGOOPTBATPAR(sl), ALGOOPTBATPAR(sr),
		  opcode & MASK_LT ? "<" : "",
		  opcode & MASK_GT ? ">" : "",
		  opcode & MASK_EQ ? "=" : "",
		  reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);
	return GDK_SUCCEED;

  bailout:
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}

/* small ordered right, dense left, oid's only, do fetches */
static gdk_return
fetchjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr,
	  struct canditer *restrict lci, struct canditer *restrict rci,
	  const char *reason, lng t0)
{
	oid lo = lci->seq - l->hseqbase + l->tseqbase, hi = lo + lci->ncand;
	BUN b, e, p;
	BAT *r1, *r2 = NULL;

	MT_thread_setalgorithm(__func__);
	if (r->tsorted) {
		b = SORTfndfirst(r, &lo);
		e = SORTfndfirst(r, &hi);
	} else {
		assert(r->trevsorted);
		b = SORTfndlast(r, &hi);
		e = SORTfndlast(r, &lo);
	}
	if (b < rci->seq - r->hseqbase)
		b = rci->seq - r->hseqbase;
	if (e > rci->seq + rci->ncand - r->hseqbase)
		e = rci->seq + rci->ncand - r->hseqbase;
	if (e == b) {
		return nomatch(r1p, r2p, l, r, lci,
			       false, false, __func__, t0);
	}
	r1 = COLnew(0, TYPE_oid, e - b, TRANSIENT);
	if (r1 == NULL)
		return GDK_FAIL;
	if (r2p) {
		if ((r2 = BATdense(0, r->hseqbase + b, e - b)) == NULL) {
			BBPreclaim(r1);
			return GDK_FAIL;
		}
		*r2p = r2;
	}
	*r1p = r1;
	oid *op = (oid *) Tloc(r1, 0);
	const oid *rp = (const oid *) Tloc(r, 0);
	for (p = b; p < e; p++) {
		*op++ = rp[p] + l->hseqbase - l->tseqbase;
	}
	BATsetcount(r1, e - b);
	r1->tkey = r->tkey;
	r1->tsorted = r->tsorted || e - b <= 1;
	r1->trevsorted = r->trevsorted || e - b <= 1;
	r1->tseqbase = e == b ? 0 : e - b == 1 ? *(const oid *)Tloc(r1, 0) : oid_nil;
	TRC_DEBUG(ALGO, "%s(l=" ALGOBATFMT ","
		  "r=" ALGOBATFMT ",sl=" ALGOOPTBATFMT ","
		  "sr=" ALGOOPTBATFMT ") %s "
		  "-> (" ALGOBATFMT "," ALGOOPTBATFMT ") " LLFMT "us\n",
		  __func__,
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  ALGOOPTBATPAR(sl), ALGOOPTBATPAR(sr),
		  reason,
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);

	return GDK_SUCCEED;
}

static BAT *
bitmaskjoin(BAT *l, BAT *r,
	    struct canditer *restrict lci, struct canditer *restrict rci,
	    bool only_misses,
	    const char *reason, lng t0)
{
	BAT *r1;
	size_t nmsk = (lci->ncand + 31) / 32;
	uint32_t *mask = GDKzalloc(nmsk * sizeof(uint32_t));
	BUN cnt = 0;

	MT_thread_setalgorithm(__func__);
	if (mask == NULL)
		return NULL;

	for (BUN n = 0; n < rci->ncand; n++) {
		oid o = canditer_next(rci) - r->hseqbase;
		o = BUNtoid(r, o);
		if (is_oid_nil(o))
			continue;
		o += l->hseqbase;
		if (o < lci->seq + l->tseqbase)
			continue;
		o -= lci->seq + l->tseqbase;
		if (o >= lci->ncand)
			continue;
		if ((mask[o >> 5] & (1U << (o & 0x1F))) == 0) {
			cnt++;
			mask[o >> 5] |= 1U << (o & 0x1F);
		}
	}
	if (only_misses)
		cnt = lci->ncand - cnt;
	if (cnt == 0 || cnt == lci->ncand) {
		GDKfree(mask);
		if (cnt == 0)
			return BATdense(0, 0, 0);
		return BATdense(0, lci->seq, lci->ncand);
	}
	r1 = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	if (r1 != NULL) {
		oid *r1p = Tloc(r1, 0);

		r1->tkey = true;
		r1->tnil = false;
		r1->tnonil = true;
		r1->tsorted = true;
		r1->trevsorted = cnt <= 1;
		if (only_misses) {
			/* set the bits for unused values at the
			 * end so that we don't need special
			 * code in the loop */
			if (lci->ncand & 0x1F)
				mask[nmsk - 1] |= ~0U << (lci->ncand & 0x1F);
			for (size_t i = 0; i < nmsk; i++)
				if (mask[i] != ~0U)
					for (uint32_t j = 0; j < 32; j++)
						if ((mask[i] & (1U << j)) == 0)
							*r1p++ = i * 32 + j + lci->seq;
		} else {
			for (size_t i = 0; i < nmsk; i++)
				if (mask[i] != 0U)
					for (uint32_t j = 0; j < 32; j++)
						if ((mask[i] & (1U << j)) != 0)
							*r1p++ = i * 32 + j + lci->seq;
		}
		BATsetcount(r1, cnt);
		assert((BUN) (r1p - (oid*) Tloc(r1, 0)) == BATcount(r1));

		TRC_DEBUG(ALGO, "l=" ALGOBATFMT ","
			  "r=" ALGOBATFMT ",sl=" ALGOOPTBATFMT ","
			  "sr=" ALGOOPTBATFMT ",only_misses=%s; %s "
			  "-> " ALGOBATFMT " (" LLFMT "usec)\n",
			  ALGOBATPAR(l), ALGOBATPAR(r),
			  ALGOOPTBATPAR(lci->s), ALGOOPTBATPAR(rci->s),
			  only_misses ? "true" : "false",
			  reason,
			  ALGOBATPAR(r1),
			  GDKusec() - t0);
	}
	GDKfree(mask);
	return r1;
}

/* Make the implementation choices for various left joins.
 * nil_matches: nil is an ordinary value that can match;
 * nil_on_miss: outer join: fill in a nil value in case of no match;
 * semi: semi join: return one of potentially more than one matches;
 * only_misses: difference: list rows without match on the right;
 * not_in: for implementing NOT IN: if nil on right then there are no matches;
 * max_one: error if there is more than one match. */
static gdk_return
leftjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr,
	 bool nil_matches, bool nil_on_miss, bool semi, bool only_misses,
	 bool not_in, bool max_one, bool min_one, BUN estimate,
	 const char *func, lng t0)
{
	BUN lcnt, rcnt;
	struct canditer lci, rci;
	bool rhash, prhash, rcand;
	bat parent;
	double rcost = 0;
	gdk_return rc;

	MT_thread_setalgorithm(__func__);
	/* only_misses implies left output only */
	assert(!only_misses || r2p == NULL);
	/* if nil_on_miss is set, we really need a right output */
	assert(!nil_on_miss || r2p != NULL);
	/* if not_in is set, then so is only_misses */
	assert(!not_in || only_misses);
	*r1p = NULL;
	if (r2p)
		*r2p = NULL;

	lcnt = canditer_init(&lci, l, sl);
	rcnt = canditer_init(&rci, r, sr);

	if ((parent = VIEWtparent(l)) != 0) {
		BAT *b = BBPdescriptor(parent);
		if (l->hseqbase == b->hseqbase &&
		    BATcount(l) == BATcount(b) &&
		    ATOMtype(l->ttype) == ATOMtype(b->ttype)) {
			l = b;
		}
	}
	if ((parent = VIEWtparent(r)) != 0) {
		BAT *b = BBPdescriptor(parent);
		if (r->hseqbase == b->hseqbase &&
		    BATcount(r) == BATcount(b) &&
		    ATOMtype(r->ttype) == ATOMtype(b->ttype)) {
			r = b;
		}
	}

	if (l->ttype == TYPE_msk || mask_cand(l)) {
		if ((l = BATunmask(l)) == NULL)
			return GDK_FAIL;
	} else {
		BBPfix(l->batCacheid);
	}
	if (r->ttype == TYPE_msk || mask_cand(r)) {
		if ((r = BATunmask(r)) == NULL) {
			BBPunfix(l->batCacheid);
			return GDK_FAIL;
		}
	} else {
		BBPfix(r->batCacheid);
	}

	if (joinparamcheck(l, r, NULL, sl, sr, func) != GDK_SUCCEED) {
		rc = GDK_FAIL;
		goto doreturn;
	}

	if (lcnt == 0 || rcnt == 0) {
		TRC_DEBUG(ALGO, "%s(l=" ALGOBATFMT ","
			  "r=" ALGOBATFMT ",sl=" ALGOOPTBATFMT ","
			  "sr=" ALGOOPTBATFMT ",nil_matches=%d,"
			  "nil_on_miss=%d,semi=%d,only_misses=%d,"
			  "not_in=%d,max_one=%d,min_one=%d)\n",
			  func,
			  ALGOBATPAR(l), ALGOBATPAR(r),
			  ALGOOPTBATPAR(sl), ALGOOPTBATPAR(sr),
			  nil_matches, nil_on_miss, semi, only_misses,
			  not_in, max_one, min_one);
		rc = nomatch(r1p, r2p, l, r, &lci,
			     nil_on_miss, only_misses, func, t0);
		goto doreturn;
	}

	if (!nil_on_miss && !semi && !max_one && !min_one && !only_misses && !not_in &&
	    (lcnt == 1 || (BATordered(l) && BATordered_rev(l)) ||
	     (l->ttype == TYPE_void && is_oid_nil(l->tseqbase)))) {
		/* single value to join, use select */
		rc = selectjoin(r1p, r2p, l, r, &lci, &rci,
				nil_matches, t0, false, func);
		goto doreturn;
	} else if (BATtdense(r) && rci.tpe == cand_dense &&
		   lcnt > 0 && rcnt > 0) {
		/* use special implementation for dense right-hand side */
		rc = mergejoin_void(r1p, r2p, l, r, &lci, &rci,
				    nil_on_miss, only_misses, t0, false,
				    func);
		goto doreturn;
	} else if (BATtdense(l)
		   && lci.tpe == cand_dense
		   && rci.tpe == cand_dense
		   && !semi
		   && !max_one
		   && !min_one
		   && !nil_matches
		   && !only_misses
		   && !not_in
		   /* && (rcnt * 1024) < lcnt */
		   && (BATordered(r) || BATordered_rev(r))) {
		assert(ATOMtype(l->ttype) == TYPE_oid); /* tdense */
		rc = fetchjoin(r1p, r2p, l, r, sl, sr, &lci, &rci, func, t0);
		goto doreturn;
	} else if (BATtdense(l)
		   && lci.tpe == cand_dense
		   && r2p == NULL
		   && (semi || only_misses)
		   && !nil_on_miss
		   && !not_in
		   && !max_one
		   && !min_one) {
		*r1p = bitmaskjoin(l, r, &lci, &rci, only_misses, func, t0);
		rc = *r1p == NULL ? GDK_FAIL : GDK_SUCCEED;
		goto doreturn;
	} else if ((BATordered(r) || BATordered_rev(r))
		   && (BATordered(l)
		       || BATordered_rev(l)
		       || BATtdense(r)
		       || lcnt < 1024
		       || BATcount(r) * (Tsize(r) + (r->tvheap ? r->tvheap->size : 0) + 2 * sizeof(BUN)) > GDK_mem_maxsize / (GDKnr_threads ? GDKnr_threads : 1))) {
		rc = mergejoin(r1p, r2p, l, r, &lci, &rci,
			       nil_matches, nil_on_miss, semi, only_misses,
			       not_in, max_one, min_one, estimate, t0, false, func);
		goto doreturn;
	}
	rcost = joincost(r, &lci, &rci, &rhash, &prhash, &rcand);

	if (!nil_on_miss && !only_misses && !not_in && !max_one && !min_one) {
		/* maybe do a hash join on the swapped operands; if we
		 * do, we need to sort the output, so we take that into
		 * account as well */
		bool lhash, plhash, lcand;
		double lcost;

		lcost = joincost(l, &rci, &lci, &lhash, &plhash, &lcand);
		if (semi)
			lcost += rci.ncand; /* cost of BATunique(r) */
		/* add cost of sorting; obviously we don't know the
		 * size, so we guess that the size of the output is
		 * the same as the right input */
		lcost += rci.ncand * log((double) rci.ncand); /* sort */
		if (lcost < rcost) {
			BAT *tmp = sr;
			BAT *r1, *r2;
			if (semi) {
				sr = BATunique(r, sr);
				if (sr == NULL) {
					rc = GDK_FAIL;
					goto doreturn;
				}
				canditer_init(&rci, r, sr);
			}
			rc = hashjoin(&r2, &r1, r, l, &rci, &lci, nil_matches,
				      false, false, false, false, false, false, estimate,
				      t0, true, lhash, plhash, lcand, func);
			if (semi)
				BBPunfix(sr->batCacheid);
			if (rc != GDK_SUCCEED)
				goto doreturn;
			if (r2p == NULL) {
				BBPunfix(r2->batCacheid);
				r2 = NULL;
			}
			if (semi)
				r1->tkey = true;
			if (!VIEWtparent(r1) &&
			    r1->ttype == TYPE_oid &&
			    BBP_refs(r1->batCacheid) == 1 &&
			    (r2 == NULL ||
			     (!VIEWtparent(r2) &&
			      BBP_refs(r2->batCacheid) == 1 &&
			      r2->ttype == TYPE_oid))) {
				/* in-place sort if we can */
				if (r2) {
					GDKqsort(r1->theap->base, r2->theap->base,
						 NULL, r1->batCount, r1->twidth,
						 r2->twidth, TYPE_oid, false,
						 false);
					r2->tsorted = false;
					r2->trevsorted = false;
					r2->tseqbase = oid_nil;
					*r2p = r2;
				} else {
					GDKqsort(r1->theap->base, NULL, NULL,
						 r1->batCount, r1->twidth, 0,
						 TYPE_oid, false, false);
				}
				r1->tsorted = true;
				r1->trevsorted = false;
				*r1p = r1;
			} else {
				BAT *ob;
				rc = BATsort(&tmp, r2p ? &ob : NULL, NULL,
					     r1, NULL, NULL, false, false, false);
				BBPunfix(r1->batCacheid);
				if (rc != GDK_SUCCEED) {
					if (r2)
						BBPunfix(r2->batCacheid);
					goto doreturn;
				}
				*r1p = r1 = tmp;
				if (r2p) {
					tmp = BATproject(ob, r2);
					BBPunfix(r2->batCacheid);
					BBPunfix(ob->batCacheid);
					if (tmp == NULL) {
						BBPunfix(r1->batCacheid);
						rc = GDK_FAIL;
						goto doreturn;
					}
					*r2p = tmp;
				}
			}
			rc = GDK_SUCCEED;
			goto doreturn;
		}
	}
	rc = hashjoin(r1p, r2p, l, r, &lci, &rci,
		      nil_matches, nil_on_miss, semi, only_misses,
		      not_in, max_one, min_one, estimate, t0, false, rhash, prhash,
		      rcand, func);
  doreturn:
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	return rc;
}

/* Perform an equi-join over l and r.  Returns two new, aligned, bats
 * with the oids of matching tuples.  The result is in the same order
 * as l (i.e. r1 is sorted). */
gdk_return
BATleftjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
{
	return leftjoin(r1p, r2p, l, r, sl, sr, nil_matches,
			false, false, false, false, false, false,
			estimate, __func__,
			GDK_TRACER_TEST(M_DEBUG, ALGO) ? GDKusec() : 0);
}

/* Performs a left outer join over l and r.  Returns two new, aligned,
 * bats with the oids of matching tuples, or the oid in the first
 * output bat and nil in the second output bat if the value in l does
 * not occur in r.  The result is in the same order as l (i.e. r1 is
 * sorted). */
gdk_return
BATouterjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool match_one, BUN estimate)
{
	return leftjoin(r1p, r2p, l, r, sl, sr, nil_matches,
			true, false, false, false, match_one, match_one,
			estimate, __func__,
			GDK_TRACER_TEST(M_DEBUG, ALGO) ? GDKusec() : 0);
}

/* Perform a semi-join over l and r.  Returns one or two new, bats
 * with the oids of matching tuples.  The result is in the same order
 * as l (i.e. r1 is sorted).  If a single bat is returned, it is a
 * candidate list. */
gdk_return
BATsemijoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr,
	    bool nil_matches, bool max_one, BUN estimate)
{
	return leftjoin(r1p, r2p, l, r, sl, sr, nil_matches,
			false, true, false, false, max_one, false,
			estimate, __func__,
			GDK_TRACER_TEST(M_DEBUG, ALGO) ? GDKusec() : 0);
}

/* Return a candidate list with the list of rows in l whose value also
 * occurs in r.  This is just the left output of a semi-join. */
BAT *
BATintersect(BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool max_one,
	     BUN estimate)
{
	BAT *bn;

	if (leftjoin(&bn, NULL, l, r, sl, sr, nil_matches,
		     false, true, false, false, max_one, false,
		     estimate, __func__,
		     GDK_TRACER_TEST(M_DEBUG, ALGO) ? GDKusec() : 0) == GDK_SUCCEED)
		return virtualize(bn);
	return NULL;
}

/* Return the difference of l and r.  The result is a BAT with the
 * oids of those values in l that do not occur in r.  This is what you
 * might call an anti-semi-join.  The result is a candidate list. */
BAT *
BATdiff(BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool not_in,
	BUN estimate)
{
	BAT *bn;

	if (leftjoin(&bn, NULL, l, r, sl, sr, nil_matches,
		     false, false, true, not_in, false, false,
		     estimate, __func__,
		     GDK_TRACER_TEST(M_DEBUG, ALGO) ? GDKusec() : 0) == GDK_SUCCEED)
		return virtualize(bn);
	return NULL;
}

gdk_return
BATthetajoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int op, bool nil_matches, BUN estimate)
{
	int opcode = 0;
	lng t0 = 0;

	/* encode operator as a bit mask into opcode */
	switch (op) {
	case JOIN_EQ:
		return BATjoin(r1p, r2p, l, r, sl, sr, nil_matches, estimate);
	case JOIN_NE:
		opcode = MASK_NE;
		break;
	case JOIN_LT:
		opcode = MASK_LT;
		break;
	case JOIN_LE:
		opcode = MASK_LE;
		break;
	case JOIN_GT:
		opcode = MASK_GT;
		break;
	case JOIN_GE:
		opcode = MASK_GE;
		break;
	default:
		GDKerror("unknown operator %d.\n", op);
		return GDK_FAIL;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	*r1p = NULL;
	if (r2p) {
		*r2p = NULL;
	}
	if (joinparamcheck(l, r, NULL, sl, sr, __func__) != GDK_SUCCEED)
		return GDK_FAIL;

	return thetajoin(r1p, r2p, l, r, sl, sr, opcode, estimate,
			 __func__, t0);
}

gdk_return
BATjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
{
	struct canditer lci, rci;
	bool lhash = false, rhash = false, lcand = false;
	bool plhash = false, prhash = false, rcand = false;
	bool swap;
	bat parent;
	double rcost = 0;
	double lcost = 0;
	gdk_return rc;
	lng t0 = 0;
	BAT *r2 = NULL;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	canditer_init(&lci, l, sl);
	canditer_init(&rci, r, sr);

	if ((parent = VIEWtparent(l)) != 0) {
		BAT *b = BBPdescriptor(parent);
		if (l->hseqbase == b->hseqbase &&
		    BATcount(l) == BATcount(b))
			l = b;
	}
	if ((parent = VIEWtparent(r)) != 0) {
		BAT *b = BBPdescriptor(parent);
		if (r->hseqbase == b->hseqbase &&
		    BATcount(r) == BATcount(b))
			r = b;
	}

	if (l->ttype == TYPE_msk || mask_cand(l)) {
		if ((l = BATunmask(l)) == NULL)
			return GDK_FAIL;
	} else {
		BBPfix(l->batCacheid);
	}
	if (r->ttype == TYPE_msk || mask_cand(r)) {
		if ((r = BATunmask(r)) == NULL) {
			BBPunfix(l->batCacheid);
			return GDK_FAIL;
		}
	} else {
		BBPfix(r->batCacheid);
	}

	*r1p = NULL;
	if (r2p)
		*r2p = NULL;

	if (joinparamcheck(l, r, NULL, sl, sr, __func__) != GDK_SUCCEED) {
		rc = GDK_FAIL;
		goto doreturn;
	}

	if (lci.ncand == 0 || rci.ncand == 0) {
		TRC_DEBUG(ALGO, "BATjoin(l=" ALGOBATFMT ","
			  "r=" ALGOBATFMT ",sl=" ALGOOPTBATFMT ","
			  "sr=" ALGOOPTBATFMT ",nil_matches=%d)\n",
			  ALGOBATPAR(l), ALGOBATPAR(r),
			  ALGOOPTBATPAR(sl), ALGOOPTBATPAR(sr),
			  nil_matches);
		rc = nomatch(r1p, r2p, l, r, &lci,
			     false, false, __func__, t0);
		goto doreturn;
	}

	swap = false;

	if (lci.ncand == 1 || (BATordered(l) && BATordered_rev(l)) || (l->ttype == TYPE_void && is_oid_nil(l->tseqbase))) {
		/* single value to join, use select */
		rc = selectjoin(r1p, r2p, l, r, &lci, &rci,
				nil_matches, t0, false, __func__);
		goto doreturn;
	} else if (rci.ncand == 1 || (BATordered(r) && BATordered_rev(r)) || (r->ttype == TYPE_void && is_oid_nil(r->tseqbase))) {
		/* single value to join, use select */
		rc = selectjoin(r2p ? r2p : &r2, r1p, r, l, &rci, &lci,
				nil_matches, t0, true, __func__);
		if (rc == GDK_SUCCEED && r2p == NULL)
			BBPunfix(r2->batCacheid);
		goto doreturn;
	} else if (BATtdense(r) && rci.tpe == cand_dense) {
		/* use special implementation for dense right-hand side */
		rc = mergejoin_void(r1p, r2p, l, r, &lci, &rci,
				    false, false, t0, false, __func__);
		goto doreturn;
	} else if (BATtdense(l) && lci.tpe == cand_dense) {
		/* use special implementation for dense right-hand side */
		rc = mergejoin_void(r2p ? r2p : &r2, r1p, r, l, &rci, &lci,
				    false, false, t0, true, __func__);
		if (rc == GDK_SUCCEED && r2p == NULL)
			BBPunfix(r2->batCacheid);
		goto doreturn;
	} else if ((BATordered(l) || BATordered_rev(l)) &&
		   (BATordered(r) || BATordered_rev(r))) {
		/* both sorted */
		rc = mergejoin(r1p, r2p, l, r, &lci, &rci,
			       nil_matches, false, false, false, false, false, false,
			       estimate, t0, false, __func__);
		goto doreturn;
	}
	/* the cost of a single lookup using the hash table on l is
	 * (approximately) the average length of the hash link chain
	 * times the cost of doing a binary search on the candidate
	 * list (if one is needed), so the total cost is this
	 * multiplied by the number of times we need to do a lookup
	 * (rci.ncand) */
	lhash = BATcheckhash(l);

	lcost = joincost(l, &rci, &lci, &lhash, &plhash, &lcand);
	rcost = joincost(r, &lci, &rci, &rhash, &prhash, &rcand);

	/* if the cost of doing searches on l is lower than the cost
	 * of doing searches on r, we swap */
	swap = (lcost < rcost);

	if ((r->ttype == TYPE_void && r->tvheap != NULL) ||
	    ((BATordered(r) || BATordered_rev(r)) &&
	     (lci.ncand * (log2((double) rci.ncand) + 1) < (swap ? lcost : rcost)))) {
		/* r is sorted and it is cheaper to do multiple binary
		 * searches than it is to use a hash */
		rc = mergejoin(r1p, r2p, l, r, &lci, &rci,
			       nil_matches, false, false, false, false, false, false,
			       estimate, t0, false, __func__);
	} else if ((l->ttype == TYPE_void && l->tvheap != NULL) ||
	    ((BATordered(l) || BATordered_rev(l)) &&
	     (rci.ncand * (log2((double) lci.ncand) + 1) < (swap ? lcost : rcost)))) {
		/* l is sorted and it is cheaper to do multiple binary
		 * searches than it is to use a hash */
		rc = mergejoin(r2p ? r2p : &r2, r1p, r, l, &rci, &lci,
			       nil_matches, false, false, false, false, false, false,
			       estimate, t0, true, __func__);
		if (rc == GDK_SUCCEED && r2p == NULL)
			BBPunfix(r2->batCacheid);
	} else if (swap) {
		rc = hashjoin(r2p ? r2p : &r2, r1p, r, l, &rci, &lci,
			      nil_matches, false, false, false, false, false, false,
			      estimate, t0, true, lhash, plhash, lcand,
			      __func__);
		if (rc == GDK_SUCCEED && r2p == NULL)
			BBPunfix(r2->batCacheid);
	} else {
		rc = hashjoin(r1p, r2p, l, r, &lci, &rci,
			      nil_matches, false, false, false, false, false, false,
			      estimate, t0, false, rhash, prhash, rcand,
			      __func__);
	}
  doreturn:
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	return rc;
}

gdk_return
BATbandjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr,
	    const void *c1, const void *c2, bool li, bool hi, BUN estimate)
{
	lng t0 = 0;
	BUN lcnt, rcnt;
	struct canditer lci, rci;
	const char *lvals, *rvals;
	int lwidth, rwidth;
	int t;
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = ATOMcompare(l->ttype);
	const char *vl, *vr;
	oid lastr = 0;		/* last value inserted into r2 */
	BUN nr;
	oid lo, ro;
	bool lskipped = false;	/* whether we skipped values in l */
	BUN nils = 0;		/* needed for XXX_WITH_CHECK macros */

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	MT_thread_setalgorithm(__func__);
	*r1p = NULL;
	if (r2p) {
		*r2p = NULL;
	}
	if (joinparamcheck(l, r, NULL, sl, sr, __func__) != GDK_SUCCEED)
		return GDK_FAIL;

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));

	t = ATOMtype(l->ttype);
	t = ATOMbasetype(t);

	lcnt = canditer_init(&lci, l, sl);
	rcnt = canditer_init(&rci, r, sr);

	if (lcnt == 0 || rcnt == 0)
		return nomatch(r1p, r2p, l, r, &lci,
			       false, false, __func__, t0);

	switch (t) {
	case TYPE_bte:
		if (is_bte_nil(*(const bte *)c1) ||
		    is_bte_nil(*(const bte *)c2) ||
		    -*(const bte *)c1 > *(const bte *)c2 ||
		    ((!hi || !li) && -*(const bte *)c1 == *(const bte *)c2))
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		break;
	case TYPE_sht:
		if (is_sht_nil(*(const sht *)c1) ||
		    is_sht_nil(*(const sht *)c2) ||
		    -*(const sht *)c1 > *(const sht *)c2 ||
		    ((!hi || !li) && -*(const sht *)c1 == *(const sht *)c2))
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		break;
	case TYPE_int:
		if (is_int_nil(*(const int *)c1) ||
		    is_int_nil(*(const int *)c2) ||
		    -*(const int *)c1 > *(const int *)c2 ||
		    ((!hi || !li) && -*(const int *)c1 == *(const int *)c2))
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		break;
	case TYPE_lng:
		if (is_lng_nil(*(const lng *)c1) ||
		    is_lng_nil(*(const lng *)c2) ||
		    -*(const lng *)c1 > *(const lng *)c2 ||
		    ((!hi || !li) && -*(const lng *)c1 == *(const lng *)c2))
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (is_hge_nil(*(const hge *)c1) ||
		    is_hge_nil(*(const hge *)c2) ||
		    -*(const hge *)c1 > *(const hge *)c2 ||
		    ((!hi || !li) && -*(const hge *)c1 == *(const hge *)c2))
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		break;
#endif
	case TYPE_flt:
		if (is_flt_nil(*(const flt *)c1) ||
		    is_flt_nil(*(const flt *)c2) ||
		    -*(const flt *)c1 > *(const flt *)c2 ||
		    ((!hi || !li) && -*(const flt *)c1 == *(const flt *)c2))
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		break;
	case TYPE_dbl:
		if (is_dbl_nil(*(const dbl *)c1) ||
		    is_dbl_nil(*(const dbl *)c2) ||
		    -*(const dbl *)c1 > *(const dbl *)c2 ||
		    ((!hi || !li) && -*(const dbl *)c1 == *(const dbl *)c2))
			return nomatch(r1p, r2p, l, r, &lci,
				       false, false, __func__, t0);
		break;
	default:
		GDKerror("unsupported type\n");
		return GDK_FAIL;
	}

	BUN maxsize = joininitresults(r1p, r2p, lcnt, rcnt, false, false,
				      false, false, false, false, estimate);
	if (maxsize == BUN_NONE)
		return GDK_FAIL;
	BAT *r1 = *r1p;
	BAT *r2 = r2p ? *r2p : NULL;

	lvals = (const char *) Tloc(l, 0);
	rvals = (const char *) Tloc(r, 0);
	assert(!r->tvarsized);
	lwidth = l->twidth;
	rwidth = r->twidth;

	assert(lvals != NULL);
	assert(rvals != NULL);

	r1->tkey = true;
	r1->tsorted = true;
	r1->trevsorted = true;
	if (r2) {
		r2->tkey = true;
		r2->tsorted = true;
		r2->trevsorted = true;
	}

	/* nested loop implementation for band join */
	for (BUN li = 0; li < lcnt; li++) {
		lo = canditer_next(&lci);
		vl = FVALUE(l, lo - l->hseqbase);
		if (cmp(vl, nil) == 0)
			continue;
		nr = 0;
		canditer_reset(&rci);
		for (BUN ri = 0; ri < rcnt; ri++) {
			ro = canditer_next(&rci);
			vr = FVALUE(r, ro - r->hseqbase);
			switch (ATOMtype(l->ttype)) {
			case TYPE_bte: {
				if (is_bte_nil(*(const bte *) vr))
					continue;
				sht v1 = (sht) *(const bte *) vr, v2;
				v2 = v1;
				v1 -= *(const bte *)c1;
				if (*(const bte *)vl <= v1 &&
				    (!li || *(const bte *)vl != v1))
					continue;
				v2 += *(const bte *)c2;
				if (*(const bte *)vl >= v2 &&
				    (!hi || *(const bte *)vl != v2))
					continue;
				break;
			}
			case TYPE_sht: {
				if (is_sht_nil(*(const sht *) vr))
					continue;
				int v1 = (int) *(const sht *) vr, v2;
				v2 = v1;
				v1 -= *(const sht *)c1;
				if (*(const sht *)vl <= v1 &&
				    (!li || *(const sht *)vl != v1))
					continue;
				v2 += *(const sht *)c2;
				if (*(const sht *)vl >= v2 &&
				    (!hi || *(const sht *)vl != v2))
					continue;
				break;
			}
			case TYPE_int: {
				if (is_int_nil(*(const int *) vr))
					continue;
				lng v1 = (lng) *(const int *) vr, v2;
				v2 = v1;
				v1 -= *(const int *)c1;
				if (*(const int *)vl <= v1 &&
				    (!li || *(const int *)vl != v1))
					continue;
				v2 += *(const int *)c2;
				if (*(const int *)vl >= v2 &&
				    (!hi || *(const int *)vl != v2))
					continue;
				break;
			}
#ifdef HAVE_HGE
			case TYPE_lng: {
				if (is_lng_nil(*(const lng *) vr))
					continue;
				hge v1 = (hge) *(const lng *) vr, v2;
				v2 = v1;
				v1 -= *(const lng *)c1;
				if (*(const lng *)vl <= v1 &&
				    (!li || *(const lng *)vl != v1))
					continue;
				v2 += *(const lng *)c2;
				if (*(const lng *)vl >= v2 &&
				    (!hi || *(const lng *)vl != v2))
					continue;
				break;
			}
#else
#ifdef HAVE___INT128
			case TYPE_lng: {
				if (is_lng_nil(*(const lng *) vr))
					continue;
				__int128 v1 = (__int128) *(const lng *) vr, v2;
				v2 = v1;
				v1 -= *(const lng *)c1;
				if (*(const lng *)vl <= v1 &&
				    (!li || *(const lng *)vl != v1))
					continue;
				v2 += *(const lng *)c2;
				if (*(const lng *)vl >= v2 &&
				    (!hi || *(const lng *)vl != v2))
					continue;
				break;
			}
#else
			case TYPE_lng: {
				if (is_lng_nil(*(const lng *) vr))
					continue;
				lng v1, v2;
				bool abort_on_error = true;
				SUB_WITH_CHECK(*(const lng *)vr,
					       *(const lng *)c1,
					       lng, v1,
					       GDK_lng_max,
					       do{if(*(const lng*)c1<0)goto nolmatch;else goto lmatch1;}while(false));
				if (*(const lng *)vl <= v1 &&
				    (!li || *(const lng *)vl != v1))
					continue;
				  lmatch1:
				ADD_WITH_CHECK(*(const lng *)vr,
					       *(const lng *)c2,
					       lng, v2,
					       GDK_lng_max,
					       do{if(*(const lng*)c2>0)goto nolmatch;else goto lmatch2;}while(false));
				if (*(const lng *)vl >= v2 &&
				    (!hi || *(const lng *)vl != v2))
					continue;
				  lmatch2:
				break;
				  nolmatch:
				continue;
			}
#endif
#endif
#ifdef HAVE_HGE
			case TYPE_hge: {
				if (is_hge_nil(*(const hge *) vr))
					continue;
				hge v1, v2;
				bool abort_on_error = true;
				SUB_WITH_CHECK(*(const hge *)vr,
					       *(const hge *)c1,
					       hge, v1,
					       GDK_hge_max,
					       do{if(*(const hge*)c1<0)goto nohmatch;else goto hmatch1;}while(false));
				if (*(const hge *)vl <= v1 &&
				    (!li || *(const hge *)vl != v1))
					continue;
				  hmatch1:
				ADD_WITH_CHECK(*(const hge *)vr,
					       *(const hge *)c2,
					       hge, v2,
					       GDK_hge_max,
					       do{if(*(const hge*)c2>0)goto nohmatch;else goto hmatch2;}while(false));
				if (*(const hge *)vl >= v2 &&
				    (!hi || *(const hge *)vl != v2))
					continue;
				  hmatch2:
				break;
				  nohmatch:
				continue;
			}
#endif
			case TYPE_flt: {
				if (is_flt_nil(*(const flt *) vr))
					continue;
				dbl v1 = (dbl) *(const flt *) vr, v2;
				v2 = v1;
				v1 -= *(const flt *)c1;
				if (*(const flt *)vl <= v1 &&
				    (!li || *(const flt *)vl != v1))
					continue;
				v2 += *(const flt *)c2;
				if (*(const flt *)vl >= v2 &&
				    (!hi || *(const flt *)vl != v2))
					continue;
				break;
			}
			case TYPE_dbl: {
				if (is_dbl_nil(*(const dbl *) vr))
					continue;
				dbl v1, v2;
				bool abort_on_error = true;
				SUB_WITH_CHECK(*(const dbl *)vr,
					       *(const dbl *)c1,
					       dbl, v1,
					       GDK_dbl_max,
					       do{if(*(const dbl*)c1<0)goto nodmatch;else goto dmatch1;}while(false));
				if (*(const dbl *)vl <= v1 &&
				    (!li || *(const dbl *)vl != v1))
					continue;
				  dmatch1:
				ADD_WITH_CHECK(*(const dbl *)vr,
					       *(const dbl *)c2,
					       dbl, v2,
					       GDK_dbl_max,
					       do{if(*(const dbl*)c2>0)goto nodmatch;else goto dmatch2;}while(false));
				if (*(const dbl *)vl >= v2 &&
				    (!hi || *(const dbl *)vl != v2))
					continue;
				  dmatch2:
				break;
				  nodmatch:
				continue;
			}
			}
			if (maybeextend(r1, r2, 1, lci.next, lci.ncand, maxsize) != GDK_SUCCEED)
				goto bailout;
			if (BATcount(r1) > 0) {
				if (r2 && lastr + 1 != ro)
					r2->tseqbase = oid_nil;
				if (nr == 0) {
					r1->trevsorted = false;
					if (r2 == NULL) {
						/* nothing */
					} else if (lastr > ro) {
						r2->tsorted = false;
						r2->tkey = false;
					} else if (lastr < ro) {
						r2->trevsorted = false;
					} else {
						r2->tkey = false;
					}
				}
			}
			APPEND(r1, lo);
			if (r2) {
				APPEND(r2, ro);
			}
			lastr = ro;
			nr++;
		}
		if (nr > 1) {
			r1->tkey = false;
			r1->tseqbase = oid_nil;
			if (r2) {
				r2->trevsorted = false;
			}
		} else if (nr == 0) {
			lskipped = BATcount(r1) > 0;
		} else if (lskipped) {
			r1->tseqbase = oid_nil;
		}
	}
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	if (r2) {
		BATsetcount(r2, BATcount(r2));
		assert(BATcount(r1) == BATcount(r2));
	}
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2) {
			r2->tseqbase = 0;
		}
	}
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT "," "r=" ALGOBATFMT
		  ",sl=" ALGOOPTBATFMT "," "sr=" ALGOOPTBATFMT ","
		  " -> " ALGOBATFMT "," ALGOOPTBATFMT
		  " (" LLFMT "usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r),
		  ALGOOPTBATPAR(sl), ALGOOPTBATPAR(sr),
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  GDKusec() - t0);
	return GDK_SUCCEED;

  bailout:
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}

gdk_return
BATrangejoin(BAT **r1p, BAT **r2p, BAT *l, BAT *rl, BAT *rh,
	     BAT *sl, BAT *sr, bool li, bool hi, bool anti, bool symmetric,
	     BUN estimate)
{
	struct canditer lci, rci;
	BAT *r1 = NULL, *r2 = NULL;
	BUN maxsize;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	*r1p = NULL;
	if (r2p) {
		*r2p = NULL;
	}
	if (joinparamcheck(l, rl, rh, sl, sr, __func__) != GDK_SUCCEED)
		return GDK_FAIL;
	if (canditer_init(&lci, l, sl) == 0 ||
	    canditer_init(&rci, rl, sr) == 0 ||
	    (l->ttype == TYPE_void && is_oid_nil(l->tseqbase)) ||
	    ((rl->ttype == TYPE_void && is_oid_nil(rl->tseqbase)) &&
	     (rh->ttype == TYPE_void && is_oid_nil(rh->tseqbase)))) {
		/* trivial: empty input */
		return nomatch(r1p, r2p, l, rl, &lci, false, false,
			       __func__, t0);
	}
	if (rl->ttype == TYPE_void && is_oid_nil(rl->tseqbase)) {
		if (!anti)
			return nomatch(r1p, r2p, l, rl, &lci, false, false,
				       __func__, t0);
		return thetajoin(r1p, r2p, l, rh, sl, sr, MASK_GT, estimate,
				 __func__, t0);
	}
	if (rh->ttype == TYPE_void && is_oid_nil(rh->tseqbase)) {
		if (!anti)
			return nomatch(r1p, r2p, l, rl, &lci, false, false,
				       __func__, t0);
		return thetajoin(r1p, r2p, l, rl, sl, sr, MASK_LT, estimate,
				 __func__, t0);
	}

	if ((maxsize = joininitresults(&r1, r2p ? &r2 : NULL, sl ? BATcount(sl) : BATcount(l), sr ? BATcount(sr) : BATcount(rl), false, false, false, false, false, false, estimate)) == BUN_NONE)
		return GDK_FAIL;
	*r1p = r1;
	if (r2p) {
		*r2p = r2;
	}
	if (maxsize == 0)
		return GDK_SUCCEED;

	/* note, the rangejoin implementation is in gdk_select.c since
	 * it uses the imprints code there */
	return rangejoin(r1, r2, l, rl, rh, &lci, &rci, li, hi, anti, symmetric, maxsize);
}
