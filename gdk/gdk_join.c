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

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

/* Perform a bunch of sanity checks on the inputs to a join. */
static gdk_return
joinparamcheck(BAT *l, BAT *r, BAT *sl, BAT *sr, const char *func)
{
	if (!BAThdense(l) || !BAThdense(r)) {
		GDKerror("%s: inputs must have dense head.\n", func);
		return GDK_FAIL;
	}
	if (ATOMtype(l->ttype) != ATOMtype(r->ttype)) {
		GDKerror("%s: inputs not compatible.\n", func);
		return GDK_FAIL;
	}
	if ((sl && !BAThdense(sl)) || (sr && !BAThdense(sr))) {
		GDKerror("%s: candidate lists must have dense head.\n", func);
		return GDK_FAIL;
	}
	if ((sl && ATOMtype(sl->ttype) != TYPE_oid) ||
	    (sr && ATOMtype(sr->ttype) != TYPE_oid)) {
		GDKerror("%s: candidate lists must have OID tail.\n", func);
		return GDK_FAIL;
	}
	if ((sl && !BATtordered(sl)) ||
	    (sr && !BATtordered(sr))) {
		GDKerror("%s: candidate lists must be sorted.\n", func);
		return GDK_FAIL;
	}
	if ((sl && !BATtkey(sl)) ||
	    (sr && !BATtkey(sr))) {
		GDKerror("%s: candidate lists must be unique.\n", func);
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* Create the result bats for a join. */
static gdk_return
joininitresults(BAT **r1p, BAT **r2p, BUN size, const char *func)
{
	BAT *r1, *r2;

	r1 = BATnew(TYPE_void, TYPE_oid, size);
	r2 = BATnew(TYPE_void, TYPE_oid, size);
	if (r1 == NULL || r2 == NULL) {
		if (r1)
			BBPreclaim(r1);
		if (r2)
			BBPreclaim(r2);
		*r1p = *r2p = NULL;
		GDKerror("%s: cannot create output BATs.\n", func);
		return GDK_FAIL;
	}
	BATseqbase(r1, 0);
	BATseqbase(r2, 0);
	r1->T->nil = 0;
	r1->T->nonil = 1;
	r1->tkey = 1;
	r1->tsorted = 1;
	r1->trevsorted = 1;
	r1->tdense = 1;
	r2->T->nil = 0;
	r2->T->nonil = 1;
	r2->tkey = 1;
	r2->tsorted = 1;
	r2->trevsorted = 1;
	r2->tdense = 1;
	*r1p = r1;
	*r2p = r2;
	return GDK_SUCCEED;
}

#define VALUE(s, x)	(s##vars ? \
			 s##vars + VarHeapVal(s##vals, (x), s##width) : \
			 s##vals + ((x) * s##width))

/* Do a binary search for the first/last occurrence of v between lo and hi
 * (lo inclusive, hi not inclusive) in rvals/rvars.
 * If last is set, return the index of the first value > v; if last is
 * not set, return the index of the first value >= v.
 * If ordering is -1, the values are sorted in reverse order and hence
 * all comparisons are reversed.
 */
static BUN
binsearch(const oid *rcand, oid offset,
	  const char *rvals, const char *rvars,
	  int rwidth, BUN lo, BUN hi, const char *v,
	  int (*cmp)(const void *, const void *), int ordering, int last)
{
	BUN mid;
	int c;

	assert(ordering == 1 || ordering == -1);
	assert(lo < hi);

	--hi;			/* now hi is inclusive */
	if ((c = ordering * cmp(VALUE(r, rcand ? rcand[lo] - offset : lo), v)) > 0 ||
	    (!last && c == 0))
		return lo;
	if ((c = ordering * cmp(VALUE(r, rcand ? rcand[hi] - offset : hi), v)) < 0 ||
	    (last && c == 0))
		return hi + 1;
/* the two versions here are equivalent, the first is disabled because
 * it does more work in the inner loop */
#if 0
	/* loop invariant:
	 * last ? value@lo <= v < value@hi : value@lo < v <= value@hi */
	while (hi - lo > 1) {
		mid = (hi + lo) / 2;
		if ((c = ordering * cmp(VALUE(r, rcand ? rcand[mid] - offset : mid), v)) > 0 ||
		    (!last && c == 0))
			hi = mid;
		else
			lo = mid;
	}
#else
	if (last) {
		/* loop invariant:
		 * value@lo <= v < value@hi */
		while (hi - lo > 1) {
			mid = (hi + lo) / 2;
			if (ordering * cmp(VALUE(r, rcand ? rcand[mid] - offset : mid), v) > 0)
				hi = mid;
			else
				lo = mid;
		}
	} else {
		/* loop invariant:
		 * value@lo < v <= value@hi */
		while (hi - lo > 1) {
			mid = (hi + lo) / 2;
			if (ordering * cmp(VALUE(r, rcand ? rcand[mid] - offset : mid), v) >= 0)
				hi = mid;
			else
				lo = mid;
		}
	}
#endif
	return hi;
}

#define APPEND(b, o)		(((oid *) b->T->heap.base)[b->batFirst + b->batCount++] = (o))

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
 * If semi is set, only a single set of values in t1/r2 is returned if
 * there is a match of l in r, no matter how many matches there are in
 * r; otherwise all matches are returned.
 */
static gdk_return
mergejoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr,
	  int nil_matches, int nil_on_miss, int semi)
{
	BUN lstart, lend, lcnt;
	const oid *lcand, *lcandend;
	BUN rstart, rend, rcnt, rstartorig;
	const oid *rcand, *rcandend, *rcandorig;
	BUN lscan, rscan;
	const char *lvals, *rvals;
	const char *lvars, *rvars;
	int lwidth, rwidth;
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = BATatoms[l->ttype].atomCmp;
	const char *v, *prev = NULL;
	BUN nl, nr;
	int insert_nil;
	/* equal_order is set if we can scan both BATs in the same
	 * order, so when both are sorted or both are reverse sorted
	 * -- important to know in order to skip over values; if l is
	 * not sorted, this must be set to 1 and we will always do a
	 * binary search on all of r */
	int equal_order;
	/* [lr]ordering is either 1 or -1 depending on the order of
	 * l/r: it determines the comparison function used */
	int lordering, rordering;
	oid lv;
	BUN i;
	int lskipped = 0;	/* whether we skipped values in l */
	wrd loff = 0, roff = 0;
	oid lval = oid_nil, rval = oid_nil;

	ALGODEBUG fprintf(stderr, "#mergejoin(l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s,nil_matches=%d,"
			  "nil_on_miss=%d,semi=%d)\n",
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  sl ? BATgetId(sl) : "NULL", sl ? BATcount(sl) : 0,
			  sl && sl->tsorted ? "-sorted" : "",
			  sl && sl->trevsorted ? "-revsorted" : "",
			  sr ? BATgetId(sr) : "NULL", sr ? BATcount(sr) : 0,
			  sr && sr->tsorted ? "-sorted" : "",
			  sr && sr->trevsorted ? "-revsorted" : "",
			  nil_matches, nil_on_miss, semi);

	assert(BAThdense(l));
	assert(BAThdense(r));
	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(r->tsorted || r->trevsorted);
	assert(sl == NULL || sl->tsorted);
	assert(sr == NULL || sr->tsorted);

	CANDINIT(l, sl, lstart, lend, lcnt, lcand, lcandend);
	CANDINIT(r, sr, rstart, rend, rcnt, rcand, rcandend);
	lvals = l->ttype == TYPE_void ? NULL : (const char *) Tloc(l, BUNfirst(l));
	rvals = r->ttype == TYPE_void ? NULL : (const char *) Tloc(r, BUNfirst(r));
	if (l->tvarsized && l->ttype) {
		assert(r->tvarsized && r->ttype);
		lvars = l->T->vheap->base;
		rvars = r->T->vheap->base;
	} else {
		assert(!r->tvarsized || !r->ttype);
		lvars = rvars = NULL;
	}
	lwidth = l->T->width;
	rwidth = r->T->width;

	/* basic properties will be adjusted if necessary later on,
	 * they were initially set by joininitresults() */

	if (lstart == lend || (!nil_on_miss && rstart == rend)) {
		/* nothing to do: there are no matches */
		return GDK_SUCCEED;
	}
	if (!nil_on_miss) {
		/* if nil_on_miss is set we still have to return
		 * results */
		if (!nil_matches &&
		    ((l->ttype == TYPE_void && l->tseqbase == oid_nil) ||
		     (r->ttype == TYPE_void && r->tseqbase == oid_nil))) {
			/* nothing to do: all values in either l or r
			 * (or both) are nil, and we don't have to
			 * match nil values */
			return GDK_SUCCEED;
		}
		if ((l->ttype == TYPE_void && l->tseqbase == oid_nil &&
		     (r->T->nonil ||
		      r->ttype != TYPE_void || r->tseqbase != oid_nil)) ||
		    (r->ttype == TYPE_void && r->tseqbase == oid_nil &&
		     (l->T->nonil ||
		      l->ttype != TYPE_void || l->tseqbase != oid_nil))) {
			/* nothing to do: all values in l are nil, and
			 * we can guarantee there are no nil values in
			 * r, or vice versa */
			return GDK_SUCCEED;
		}
	}

	if (l->tsorted || l->trevsorted) {
		/* determine opportunistic scan window for l */
		for (nl = lcand ? (BUN) (lcandend - lcand) : lend - lstart,
			     lscan = 4;
		     nl > 0;
		     lscan++)
			nl >>= 1;
		equal_order = (l->tsorted && r->tsorted) ||
			(l->trevsorted && r->trevsorted &&
			 l->ttype != TYPE_void && r->ttype != TYPE_void);
		lordering = l->tsorted && (r->tsorted || !equal_order) ? 1 : -1;
		rordering = equal_order ? lordering : -lordering;
	} else {
		/* if l not sorted, we will always use binary search
		 * on r */
		assert(l->ttype != TYPE_void); /* void is always sorted */
		lscan = 0;
		equal_order = 1;
		lordering = 1;
		rordering = r->tsorted ? 1 : -1;
		/* if l not sorted, we only know for sure that r2 is
		 * key if l is, and that r1 is key if r is */
		r2->tkey = l->tkey != 0;
		r1->tkey = r->tkey != 0;
	}
	/* determine opportunistic scan window for r; if l is not
	 * sorted this is only used to find range of equal values */
	for (nl = rcand ? (BUN) (rcandend - rcand) : rend - rstart, rscan = 4;
	     nl > 0;
	     rscan++)
		nl >>= 1;

	if (l->ttype == TYPE_void) {
		if (lcand) {
			lstart = 0;
			lend = lcandend - lcand;
			lvals = (const char *) lcand;
			lcand = NULL;
			lwidth = SIZEOF_OID;
		}
		if (l->tseqbase == oid_nil)
			loff = wrd_nil;
		else
			loff = (wrd) l->tseqbase - (wrd) l->hseqbase;
	}
	if (r->ttype == TYPE_void) {
		if (rcand) {
			rstart = 0;
			rend = rcandend - rcand;
			rvals = (const char *) rcand;
			rcand = NULL;
			rwidth = SIZEOF_OID;
		}
		if (r->tseqbase == oid_nil)
			roff = wrd_nil;
		else
			roff = (wrd) r->tseqbase - (wrd) r->hseqbase;
	}
	assert(lvals != NULL || lcand == NULL);
	assert(rvals != NULL || rcand == NULL);

	rcandorig = rcand;
	rstartorig = rstart;
	while (lcand ? lcand < lcandend : lstart < lend) {
		if (!nil_on_miss && lscan > 0) {
			/* if next value in r too far away (more than
			 * lscan from current position in l), use
			 * binary search on l to skip over
			 * non-matching values, but only if l is
			 * sorted (lscan > 0) and we don't have to
			 * insert nils (outer join)
			 *
			 * next value to match in r is first if
			 * equal_order is set, last otherwise */
			if (rcand) {
				v = VALUE(r, (equal_order ? rcand[0] : rcandend[-1]) - r->hseqbase);
			} else if (rvals) {
				v = VALUE(r, equal_order ? rstart : rend - 1);
				if (roff == wrd_nil) {
					rval = oid_nil;
					v = (const char *) &rval;
				} else if (roff != 0) {
					rval = *(const oid *)v + roff;
					v = (const char *) &rval;
				}
			} else {
				if (roff == wrd_nil)
					rval = oid_nil;
				else if (equal_order)
					rval = rstart + r->tseqbase;
				else
					rval = rend - 1 + r->tseqbase;
				v = (const char *) &rval;
			}
			if (lcand) {
				if (lscan < (BUN) (lcandend - lcand) &&
				    lordering * cmp(VALUE(l, lcand[lscan] - l->hseqbase),
						    v) < 0) {
					lcand += binsearch(lcand, l->hseqbase,
							   lvals, lvars,
							   lwidth, lscan,
							   lcandend - lcand, v,
							   cmp, lordering, 0);
					if (lcand == lcandend)
						break;
					lskipped = BATcount(r1) > 0;
				}
			} else if (lvals) {
				if (lscan < lend - lstart &&
				    lordering * cmp(VALUE(l, lstart + lscan),
						    v) < 0) {
					lstart = binsearch(NULL, 0,
							   lvals, lvars,
							   lwidth,
							   lstart + lscan,
							   lend, v,
							   cmp, lordering, 0);
					if (lstart == lend)
						break;
					lskipped = BATcount(r1) > 0;
				}
			} else if (*(const oid *)v != oid_nil) {
				if (l->tseqbase == oid_nil) {
					/* there cannot be any more
					 * matches since r's next
					 * value is not nil and hence
					 * all other values in r are
					 * also not nil, and all
					 * values in l are nil */
					lstart = lend;
					break;
				}
				if (*(const oid *)v > l->tseqbase) {
					BUN olstart = lstart;
					lstart = *(const oid *)v - l->tseqbase;
					if (lstart >= lend)
						break;
					if (lstart > olstart)
						lskipped = BATcount(r1) > 0;
				}
			}
		} else if (lscan == 0) {
			/* always search r completely */
			rcand = rcandorig;
			rstart = rstartorig;
		}
		/* v is the value we're going to work with in this
		 * iteration; count number of equal values in left */
		nl = 1;		/* we'll match (at least) one in l */
		nr = 0;		/* maybe we won't match anything in r */
		if (lcand) {
			v = VALUE(l, lcand[0] - l->hseqbase);
			while (++lcand < lcandend &&
			       cmp(v, VALUE(l, lcand[0] - l->hseqbase)) == 0)
				nl++;
		} else if (lvals) {
			v = VALUE(l, lstart);
			if (loff == wrd_nil) {
				/* all values are nil */
				lval = oid_nil;
				nl = lend - lstart;
				lstart = lend;
				v = (const char *) &lval;
			} else {
				/* compare values without offset */
				while (++lstart < lend &&
				       cmp(v, VALUE(l, lstart)) == 0)
					nl++;
				/* now fix offset */
				if (loff != 0) {
					lval = *(const oid *)v + loff;
					v = (const char *) &lval;
				}
			}
		} else {
			if (loff == wrd_nil) {
				lval = oid_nil;
				nl = lend - lstart;
				lstart = lend;
			} else {
				lval = lstart + l->tseqbase;
				lstart++;
			}
			v = (const char *) &lval;
		}
		/* lcand/lstart points one beyond the value we're
		 * going to match: ready for the next */
		if (!nil_matches && cmp(v, nil) == 0) {
			/* v is nil and nils don't match anything */
			continue;
		}
		/* first we find the first value in r that is at least
		 * as large as v, then we find the first value in r
		 * that is larger than v, counting the number of
		 * values equal to v in nr */
		if (equal_order) {
			if (rcand) {
				/* first find the location of the
				 * first value in r that is >= v, then
				 * find the location of the first
				 * value in r that is > v; the
				 * difference is the number of values
				 * equal v */
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search, but if l is not
				 * sorted (lscan == 0) we'll always do
				 * a binary search */
				if (lscan == 0 ||
				    (rscan < (BUN) (rcandend - rcand) &&
				     rordering * cmp(v, VALUE(r, rcand[rscan] - r->hseqbase)) > 0)) {
					/* value too far away in r or
					 * l not sorted: use binary
					 * search */
					rcand += binsearch(rcand, r->hseqbase, rvals, rvars, rwidth, lscan == 0 ? 0 : rscan, rcandend - rcand, v, cmp, rordering, 0);
				} else {
					/* scan r for v */
					while (rcand < rcandend &&
					       rordering * cmp(v, VALUE(r, rcand[0] - r->hseqbase)) > 0)
						rcand++;
				}
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search */
				if (rscan < (BUN) (rcandend - rcand) &&
				    cmp(v, VALUE(r, rcand[rscan] - r->hseqbase)) == 0) {
					/* range too large: use binary
					 * search */
					nr = binsearch(rcand, r->hseqbase, rvals, rvars, rwidth, rscan, rcandend - rcand, v, cmp, rordering, 1);
					rcand += nr;
				} else {
					/* scan r for end of range */
					while (rcand < rcandend &&
					       cmp(v, VALUE(r, rcand[0] - r->hseqbase)) == 0) {
						nr++;
						rcand++;
					}
				}
			} else if (rvals) {
				/* first find the location of the
				 * first value in r that is >= v, then
				 * find the location of the first
				 * value in r that is > v; the
				 * difference is the number of values
				 * equal v */
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search, but if l is not
				 * sorted (lscan == 0) we'll always do
				 * a binary search */
				if (lscan == 0 ||
				    (rscan < rend - rstart &&
				     rordering * cmp(v, VALUE(r, rstart + rscan)) > 0)) {
					/* value too far away in r or
					 * l not sorted: use binary
					 * search */
					rstart = binsearch(NULL, 0, rvals, rvars, rwidth, rstart + (lscan == 0 ? 0 : rscan), rend, v, cmp, rordering, 0);
				} else {
					/* scan r for v */
					while (rstart < rend &&
					       rordering * cmp(v, VALUE(r, rstart)) > 0)
						rstart++;
				}
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search */
				if (rscan < rend - rstart &&
				    cmp(v, VALUE(r, rstart + rscan)) == 0) {
					/* range too large: use binary
					 * search */
					nr = binsearch(NULL, 0, rvals, rvars, rwidth, rstart + rscan, rend, v, cmp, rordering, 1);
					nr -= rstart;
					rstart += nr;
				} else {
					/* scan r for end of range */
					while (rstart < rend &&
					       cmp(v, VALUE(r, rstart)) == 0) {
						nr++;
						rstart++;
					}
				}
			} else {
				/* r is dense or void-nil, so we don't
				 * need to search, we know there is
				 * either zero or one match, or
				 * everything matches */
				if (r->tseqbase == oid_nil) {
					if (*(const oid *)v == oid_nil) {
						/* both sides are nil:
						 * everything matches */
						nr = rend - rstart;
						rstart = rend;
					}
				} else if (*(const oid *)v != oid_nil &&
					   *(const oid *)v >= rstart + r->tseqbase) {
					if (*(const oid *)v < rend + r->tseqbase) {
						/* within range: a
						 * single match */
						nr = 1;
						rstart = *(const oid *)v - r->tseqbase + 1;
					} else {
						/* beyond the end: no match */
						rstart = rend;
					}
				}
			}
			/* rstart or rcand points to first value > v
			 * or end of r, and nr is the number of values
			 * in r that are equal to v */
		} else {
			if (rcand) {
				/* first find the location of the
				 * first value in r that is > v, then
				 * find the location of the first
				 * value in r that is >= v; the
				 * difference is the number of values
				 * equal v */
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search, but if l is not
				 * sorted (lscan == 0) we'll always do
				 * a binary search */
				if (rscan < (BUN) (rcandend - rcand) &&
				    rordering * cmp(v, VALUE(r, rcandend[-rscan - 1] - r->hseqbase)) < 0) {
					/* value too far away in r or
					 * l not sorted: use binary
					 * search */
					rcandend = rcand + binsearch(rcand, r->hseqbase, rvals, rvars, rwidth, 0, (BUN) (rcandend - rcand) - (lscan == 0 ? 0 : rscan), v, cmp, rordering, 1);
				} else {
					/* scan r for v */
					while (rcand < rcandend &&
					       rordering * cmp(v, VALUE(r, rcandend[-1] - r->hseqbase)) < 0)
						rcandend--;
				}
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search */
				if (rscan < (BUN) (rcandend - rcand) &&
				    cmp(v, VALUE(r, rcandend[-rscan - 1] - r->hseqbase)) == 0) {
					nr = binsearch(rcand, r->hseqbase, rvals, rvars, rwidth, 0, (BUN) (rcandend - rcand) - rscan, v, cmp, rordering, 0);
					nr = (BUN) (rcandend - rcand) - nr;
					rcandend -= nr;
				} else {
					/* scan r for start of range */
					while (rcand < rcandend &&
					       cmp(v, VALUE(r, rcandend[-1] - r->hseqbase)) == 0) {
						nr++;
						rcandend--;
					}
				}
			} else if (rvals) {
				/* first find the location of the
				 * first value in r that is > v, then
				 * find the location of the first
				 * value in r that is >= v; the
				 * difference is the number of values
				 * equal v */
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search, but if l is not
				 * sorted (lscan == 0) we'll always do
				 * a binary search */
				if (rscan < rend - rstart &&
				    rordering * cmp(v, VALUE(r, rend - rscan - 1)) < 0) {
					/* value too far away in r or
					 * l not sorted: use binary
					 * search */
					rend = binsearch(NULL, 0, rvals, rvars, rwidth, rstart, rend - (lscan == 0 ? 0 : rscan), v, cmp, rordering, 1);
				} else {
					/* scan r for v */
					while (rstart < rend &&
					       rordering * cmp(v, VALUE(r, rend - 1)) < 0)
						rend--;
				}
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search */
				if (rscan < rend - rstart &&
				    cmp(v, VALUE(r, rend - rscan - 1)) == 0) {
					nr = binsearch(NULL, 0, rvals, rvars, rwidth, rstart, rend - rscan, v, cmp, rordering, 0);
					nr = rend - nr;
					rend -= nr;
				} else {
					/* scan r for start of range */
					while (rstart < rend &&
					       cmp(v, VALUE(r, rend - 1)) == 0) {
						nr++;
						rend--;
					}
				}
			} else {
				/* r is dense or void-nil, so we don't
				 * need to search, we know there is
				 * either zero or one match, or
				 * everything matches */
				if (r->tseqbase == oid_nil) {
					if (*(const oid *)v == oid_nil) {
						/* both sides are nil:
						 * everything matches */
						nr = rend - rstart;
						rend = rstart;
					}
				} else if (*(const oid *)v != oid_nil &&
					   *(const oid *)v < rend + r->tseqbase) {
					if (*(const oid *)v >= rstart + r->tseqbase) {
						/* within range: a
						 * single match */
						nr = 1;
						rend = *(const oid *)v - r->tseqbase;
					} else {
						/* before the start:
						 * no match */
						rend = rstart;
					}
				}
			}
			/* rend/rcandend now points to first value >= v
			 * or start of r */
		}
		if (nr == 0) {
			/* no entries in r found */
			if (!nil_on_miss) {
				if (lscan > 0 &&
				    (rcand ? rcand == rcandend : rstart == rend)) {
					/* nothing more left to match
					 * in r */
					break;
				}
				lskipped = BATcount(r1) > 0;
				continue;
			}
			/* insert a nil to indicate a non-match */
			insert_nil = 1;
			nr = 1;
			r2->T->nil = 1;
			r2->T->nonil = 0;
			r2->tsorted = 0;
			r2->trevsorted = 0;
			r2->tdense = 0;
		} else {
			insert_nil = 0;
			if (semi) {
				/* for semi-join, only insert single
				 * value */
				nr = 1;
			}
		}
		/* make space: nl values in l match nr values in r, so
		 * we need to add nl * nr values in the results */
		if (BATcount(r1) + nl * nr > BATcapacity(r1)) {
			/* make some extra space by extrapolating how
			 * much more we need */
			BUN newcap = BATcount(r1) + nl * nr * (lcand ? (BUN) (lcandend + 1 - lcand) : lend + 1 - lstart);
			BATsetcount(r1, BATcount(r1));
			BATsetcount(r2, BATcount(r2));
			r1 = BATextend(r1, newcap);
			r2 = BATextend(r2, newcap);
			if (r1 == NULL || r2 == NULL) {
				if (r1)
					BBPreclaim(r1);
				if (r2)
					BBPreclaim(r2);
				return GDK_FAIL;
			}
			assert(BATcapacity(r1) == BATcapacity(r2));
		}

		/* maintain properties */
		if (nl > 1) {
			/* value occurs multiple times in l, so entry
			 * in r will be repeated multiple times: hence
			 * r2 is not key and not dense */
			r2->tkey = 0;
			r2->tdense = 0;
			/* multiple different values will be inserted
			 * in r1 (always in order), so not reverse
			 * ordered anymore */
			r1->trevsorted = 0;
		}
		if (nr > 1) {
			/* value occurs multiple times in r, so entry
			 * in l will be repeated multiple times: hence
			 * r1 is not key and not dense */
			r1->tkey = 0;
			r1->tdense = 0;
			/* multiple different values will be inserted
			 * in r2 (in order), so not reverse ordered
			 * anymore */
			r2->trevsorted = 0;
			if (nl > 1) {
				/* multiple values in l match multiple
				 * values in r, so an ordered sequence
				 * will be inserted multiple times in
				 * r2, so r2 is not ordered anymore */
				r2->tsorted = 0;
			}
		}
		if (lscan == 0) {
			/* deduce relative positions of r matches for
			 * this and previous value in v */
			assert(prev != v);
			if (prev) {
				if (rordering * cmp(prev, v) < 0) {
					/* previous value in l was
					 * less than current */
					r2->trevsorted = 0;
				} else {
					r2->tsorted = 0;
				}
			}
			prev = v;
		}
		if (BATcount(r1) > 0) {
			/* a new, higher value will be inserted into
			 * r1, so r1 is not reverse ordered anymore */
			r1->trevsorted = 0;
			/* depending on whether l and r are ordered
			 * the same or not, a new higher or lower
			 * value will be added to r2 */
			if (equal_order)
				r2->trevsorted = 0;
			else {
				r2->tsorted = 0;
				r2->tdense = 0;
			}
			if (r1->tdense && lskipped)
				r1->tdense = 0;
		}

		/* insert values: various different ways of doing it */
		if (insert_nil) {
			do {
				lv = lcand ? lcand[-(ssize_t)nl] : lstart + l->hseqbase - nl;

				for (i = 0; i < nr; i++) {
					APPEND(r1, lv);
					APPEND(r2, oid_nil);
				}
			} while (--nl > 0);
		} else if (rcand && equal_order) {
			if (r2->batCount > 0 &&
			    r2->tdense &&
			    ((oid *) r2->T->heap.base)[r2->batFirst + r2->batCount - 1] + 1 != rcand[-(ssize_t)nr])
				r2->tdense = 0;
			do {
				lv = lcand ? lcand[-(ssize_t)nl] : lstart + l->hseqbase - nl;

				for (i = nr; i > 0; i--) {
					APPEND(r1, lv);
					APPEND(r2, rcand[-(ssize_t)i]);
				}
			} while (--nl > 0);
		} else if (rcand) {
			if (r2->batCount > 0 &&
			    r2->tdense &&
			    ((oid *) r2->T->heap.base)[r2->batFirst + r2->batCount - 1] + 1 != rcandend[0])
				r2->tdense = 0;
			do {
				lv = lcand ? lcand[-(ssize_t)nl] : lstart + l->hseqbase - nl;

				for (i = 0; i < nr; i++) {
					APPEND(r1, lv);
					APPEND(r2, rcandend[i]);
				}
			} while (--nl > 0);
		} else if (equal_order) {
			if (r2->batCount > 0 &&
			    r2->tdense &&
			    ((oid *) r2->T->heap.base)[r2->batFirst + r2->batCount - 1] + 1 != rstart + r->hseqbase - nr)
				r2->tdense = 0;
			do {
				lv = lcand ? lcand[-(ssize_t)nl] : lstart + l->hseqbase - nl;

				for (i = nr; i > 0; i--) {
					APPEND(r1, lv);
					APPEND(r2, rstart + r->hseqbase - i);
				}
			} while (--nl > 0);
		} else {
			if (r2->batCount > 0 &&
			    r2->tdense &&
			    ((oid *) r2->T->heap.base)[r2->batFirst + r2->batCount - 1] + 1 != rend + r->hseqbase)
				r2->tdense = 0;
			do {
				lv = lcand ? lcand[-(ssize_t)nl] : lstart + l->hseqbase - nl;

				for (i = 0; i < nr; i++) {
					APPEND(r1, lv);
					APPEND(r2, rend + r->hseqbase + i);
				}
			} while (--nl > 0);
		}
	}
	assert(BATcount(r1) == BATcount(r2));
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	BATsetcount(r2, BATcount(r2));
	if (BATcount(r1) > 0) {
		if (r1->tdense)
			r1->tseqbase = ((oid *) r1->T->heap.base)[r1->batFirst];
		if (r2->tdense)
			r2->tseqbase = ((oid *) r2->T->heap.base)[r2->batFirst];
	}
	return GDK_SUCCEED;
}

/* binary search in a candidate list, return 1 if found, 0 if not */
static inline int
binsearchcand(const oid *cand, BUN lo, BUN hi, oid v)
{
	BUN mid;

	--hi;			/* now hi is inclusive */
	if (v < cand[lo] || v > cand[hi])
		return 0;
	while (hi > lo) {
		mid = (lo + hi) / 2;
		if (cand[mid] == v)
			return 1;
		if (cand[mid] < v)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return cand[lo] == v;
}

static gdk_return
hashjoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr, int nil_matches, int nil_on_miss, int semi)
{
	BUN lstart, lend, lcnt;
	const oid *lcand = NULL, *lcandend = NULL;
	BUN rstart, rend, rcnt;
	const oid *rcand = NULL, *rcandend = NULL;
	oid lo, ro;
	BATiter ri;
	BUN rb, rb0;
	wrd rbun2oid;
	BUN nr, nrcand, newcap;
	const char *lvals;
	const char *lvars;
	int lwidth;
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = BATatoms[l->ttype].atomCmp;
	oid lval = oid_nil;	/* hold value if l has dense tail */
	const char *v = (const char *) &lval;
	int lskipped = 0;	/* whether we skipped values in l */

	ALGODEBUG fprintf(stderr, "#hashjoin(l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s,nil_matches=%d,"
			  "nil_on_miss=%d,semi=%d)\n",
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  sl ? BATgetId(sl) : "NULL", sl ? BATcount(sl) : 0,
			  sl && sl->tsorted ? "-sorted" : "",
			  sl && sl->trevsorted ? "-revsorted" : "",
			  sr ? BATgetId(sr) : "NULL", sr ? BATcount(sr) : 0,
			  sr && sr->tsorted ? "-sorted" : "",
			  sr && sr->trevsorted ? "-revsorted" : "",
			  nil_matches, nil_on_miss, semi);

	assert(BAThdense(l));
	assert(BAThdense(r));
	assert(r->ttype != TYPE_void);
	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(sl == NULL || sl->tsorted);
	assert(sr == NULL || sr->tsorted);

	CANDINIT(l, sl, lstart, lend, lcnt, lcand, lcandend);
	CANDINIT(r, sr, rstart, rend, rcnt, rcand, rcandend);
	lwidth = l->T->width;
	lvals = (const char *) Tloc(l, BUNfirst(l));
	if (l->tvarsized && l->ttype) {
		assert(r->tvarsized && r->ttype);
		lvars = l->T->vheap->base;
	} else {
		assert(!r->tvarsized || !r->ttype);
		lvars = NULL;
	}
	/* offset to convert BUN for value in right tail column to OID
	 * in right head column */
	rbun2oid = (wrd) r->hseqbase - (wrd) BUNfirst(r);

	/* basic properties will be adjusted if necessary later on,
	 * they were initially set by joininitresults() */

	/* if an input columns is key, the opposite output column will
	 * be key */
	r1->tkey = r->tkey != 0;
	r2->tkey = l->tkey != 0;
	/* r2 is not likely to be sorted (although it is certainly
	 * possible) */
	r2->tsorted = 0;
	r2->trevsorted = 0;
	r2->tdense = 0;

	if (lstart == lend || (!nil_on_miss && rstart == rend)) {
		/* nothing to do: there are no matches */
		return GDK_SUCCEED;
	}

	/* hashes work on HEAD column */
	r = BATmirror(r);
	if (BATprepareHash(r))
		goto bailout;
	ri = bat_iterator(r);
	nrcand = (BUN) (rcandend - rcand);

	if (lcand) {
		while (lcand < lcandend) {
			lo = *lcand++;
			if (l->ttype == TYPE_void) {
				if (l->tseqbase != oid_nil)
					lval = lo - l->hseqbase + l->tseqbase;
			} else {
				v = VALUE(l, lo - l->hseqbase);
			}
			if (!nil_matches && cmp(v, nil) == 0) {
				lskipped = BATcount(r1) > 0;
				continue;
			}
			nr = 0;
			if (rcand) {
				HASHloop(ri, r->H->hash, rb, v) {
					ro = (oid) rb + rbun2oid;
					if (!binsearchcand(rcand, 0, nrcand, ro))
						continue;
					if (BUNlast(r1) == BATcapacity(r1)) {
						newcap = BATgrows(r1);
						BATsetcount(r1, BATcount(r1));
						BATsetcount(r2, BATcount(r2));
						r1 = BATextend(r1, newcap);
						r2 = BATextend(r2, newcap);
						if (r1 == NULL || r2 == NULL)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
					}
					APPEND(r1, lo);
					APPEND(r2, ro);
					nr++;
					if (semi)
						break;
				}
			} else {
				HASHloop(ri, r->H->hash, rb, v) {
					rb0 = rb - BUNfirst(r); /* zero-based */
					if (rb0 < rstart || rb0 >= rend)
						continue;
					if (BUNlast(r1) == BATcapacity(r1)) {
						newcap = BATgrows(r1);
						BATsetcount(r1, BATcount(r1));
						BATsetcount(r2, BATcount(r2));
						r1 = BATextend(r1, newcap);
						r2 = BATextend(r2, newcap);
						if (r1 == NULL || r2 == NULL)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
					}
					ro = (oid) rb + rbun2oid;
					APPEND(r1, lo);
					APPEND(r2, ro);
					nr++;
					if (semi)
						break;
				}
			}
			if (nr == 0) {
				if (nil_on_miss) {
					nr = 1;
					r2->T->nil = 1;
					r2->T->nonil = 0;
					r2->tkey = 0;
					if (BUNlast(r1) == BATcapacity(r1)) {
						newcap = BATgrows(r1);
						BATsetcount(r1, BATcount(r1));
						BATsetcount(r2, BATcount(r2));
						r1 = BATextend(r1, newcap);
						r2 = BATextend(r2, newcap);
						if (r1 == NULL || r2 == NULL)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
					}
					APPEND(r1, lo);
					APPEND(r2, oid_nil);
				} else {
					lskipped = BATcount(r1) > 0;
				}
			} else {
				if (lskipped) {
					/* note, we only get here in
					 * an iteration *after*
					 * lskipped was first set to
					 * 1, i.e. we did indeed skip
					 * values in l */
					r1->tdense = 0;
				}
				if (nr > 1) {
					r1->tkey = 0;
					r1->tdense = 0;
				}
			}
			if (nr > 0 && BATcount(r1) > nr)
				r1->trevsorted = 0;
		}
	} else {
		for (lo = lstart - BUNfirst(l) + l->hseqbase; lstart < lend; lo++) {
			if (l->ttype == TYPE_void) {
				if (l->tseqbase != oid_nil)
					lval = lo - l->hseqbase + l->tseqbase;
			} else {
				v = VALUE(l, lstart);
			}
			lstart++;
			if (!nil_matches && cmp(v, nil) == 0) {
				lskipped = BATcount(r1) > 0;
				continue;
			}
			nr = 0;
			if (rcand) {
				HASHloop(ri, r->H->hash, rb, v) {
					ro = (oid) rb + rbun2oid;
					if (!binsearchcand(rcand, 0, nrcand, ro))
						continue;
					if (BUNlast(r1) == BATcapacity(r1)) {
						newcap = BATgrows(r1);
						BATsetcount(r1, BATcount(r1));
						BATsetcount(r2, BATcount(r2));
						r1 = BATextend(r1, newcap);
						r2 = BATextend(r2, newcap);
						if (r1 == NULL || r2 == NULL)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
					}
					APPEND(r1, lo);
					APPEND(r2, ro);
					nr++;
					if (semi)
						break;
				}
			} else {
				HASHloop(ri, r->H->hash, rb, v) {
					rb0 = rb - BUNfirst(r); /* zero-based */
					if (rb0 < rstart || rb0 >= rend)
						continue;
					if (BUNlast(r1) == BATcapacity(r1)) {
						newcap = BATgrows(r1);
						BATsetcount(r1, BATcount(r1));
						BATsetcount(r2, BATcount(r2));
						r1 = BATextend(r1, newcap);
						r2 = BATextend(r2, newcap);
						if (r1 == NULL || r2 == NULL)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
					}
					ro = (oid) rb + rbun2oid;
					APPEND(r1, lo);
					APPEND(r2, ro);
					nr++;
					if (semi)
						break;
				}
			}
			if (nr == 0) {
				if (nil_on_miss) {
					nr = 1;
					r2->T->nil = 1;
					r2->T->nonil = 0;
					if (BUNlast(r1) == BATcapacity(r1)) {
						newcap = BATgrows(r1);
						BATsetcount(r1, BATcount(r1));
						BATsetcount(r2, BATcount(r2));
						r1 = BATextend(r1, newcap);
						r2 = BATextend(r2, newcap);
						if (r1 == NULL || r2 == NULL)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
					}
					APPEND(r1, lo);
					APPEND(r2, oid_nil);
				} else {
					lskipped = BATcount(r1) > 0;
				}
			} else {
				if (lskipped) {
					/* note, we only get here in
					 * an iteration *after*
					 * lskipped was first set to
					 * 1, i.e. we did indeed skip
					 * values in l */
					r1->tdense = 0;
				}
				if (nr > 1) {
					r1->tkey = 0;
					r1->tdense = 0;
				}
			}
			if (nr > 0 && BATcount(r1) > nr)
				r1->trevsorted = 0;
		}
	}
	assert(BATcount(r1) == BATcount(r2));
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	BATsetcount(r2, BATcount(r2));
	if (BATcount(r1) <= 1) {
		r1->tsorted = 1;
		r1->trevsorted = 1;
		r1->tkey = 1;
		r1->tdense = 1;
		r2->tsorted = 1;
		r2->trevsorted = 1;
		r2->tkey = 1;
		r2->tdense = 1;
	}
	if (BATcount(r1) > 0) {
		if (r1->tdense)
			r1->tseqbase = ((oid *) r1->T->heap.base)[r1->batFirst];
		if (r2->tdense)
			r2->tseqbase = ((oid *) r2->T->heap.base)[r2->batFirst];
	}
	return GDK_SUCCEED;

  bailout:
	if (r1)
		BBPreclaim(r1);
	if (r2)
		BBPreclaim(r2);
	return GDK_FAIL;
}

#define MASK_EQ		1
#define MASK_LT		2
#define MASK_GT		4
#define MASK_LE		(MASK_EQ | MASK_LT)
#define MASK_GE		(MASK_EQ | MASK_GT)
#define MASK_NE		(MASK_LT | MASK_GT)

static gdk_return
thetajoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr, const char *op)
{
	BUN lstart, lend, lcnt;
	const oid *lcand = NULL, *lcandend = NULL;
	BUN rstart, rend, rcnt;
	const oid *rcand = NULL, *rcandend = NULL;
	const char *lvals, *rvals;
	const char *lvars, *rvars;
	int lwidth, rwidth;
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = BATatoms[l->ttype].atomCmp;
	const char *vl, *vr;
	const oid *p;
	oid lastr = 0;		/* last value inserted into r2 */
	BUN n, nr;
	BUN newcap;
	int opcode = 0;
	oid lo, ro;
	int c;
	int lskipped = 0;	/* whether we skipped values in l */
	wrd loff = 0, roff = 0;
	oid lval = oid_nil, rval = oid_nil;

	ALGODEBUG fprintf(stderr, "#thetajoin(l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s,op=%s)\n",
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  sl ? BATgetId(sl) : "NULL", sl ? BATcount(sl) : 0,
			  sl && sl->tsorted ? "-sorted" : "",
			  sl && sl->trevsorted ? "-revsorted" : "",
			  sr ? BATgetId(sr) : "NULL", sr ? BATcount(sr) : 0,
			  sr && sr->tsorted ? "-sorted" : "",
			  sr && sr->trevsorted ? "-revsorted" : "",
			  op);

	assert(BAThdense(l));
	assert(BAThdense(r));
	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(sl == NULL || sl->tsorted);
	assert(sr == NULL || sr->tsorted);

	/* encode operator as a bit mask into opcode */
	if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[2] == 0)) {
		/* "=" or "==" */
		opcode |= MASK_EQ;
	} else if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
		/* "!=" (equivalent to "<>") */
		opcode |= MASK_NE;
	} else 	if (op[0] == '<') {
		if (op[1] == 0) {
			/* "<" */
			opcode |= MASK_LT;
		} else if (op[1] == '=' && op[2] == 0) {
			/* "<=" */
			opcode |= MASK_LE;
		} else if (op[1] == '>' && op[2] == 0) {
			/* "<>" (equivalent to "!=") */
			opcode |= MASK_NE;
		}
	} else if (op[0] == '>') {
		if (op[1] == 0) {
			/* ">" */
			opcode |= MASK_GT;
		} else if (op[1] == '=' && op[2] == 0) {
			/* ">=" */
			opcode |= MASK_GE;
		}
	}
	if (opcode == 0) {
		GDKerror("BATthetasubjoin: unknown operator \"%s\".\n", op);
		return GDK_FAIL;
	}

	CANDINIT(l, sl, lstart, lend, lcnt, lcand, lcandend);
	CANDINIT(r, sr, rstart, rend, rcnt, rcand, rcandend);

	lvals = l->ttype == TYPE_void ? NULL : (const char *) Tloc(l, BUNfirst(l));
	rvals = r->ttype == TYPE_void ? NULL : (const char *) Tloc(r, BUNfirst(r));
	if (l->tvarsized && l->ttype) {
		assert(r->tvarsized && r->ttype);
		lvars = l->T->vheap->base;
		rvars = r->T->vheap->base;
	} else {
		assert(!r->tvarsized || !r->ttype);
		lvars = rvars = NULL;
	}
	lwidth = l->T->width;
	rwidth = r->T->width;

	if (l->ttype == TYPE_void) {
		if (l->tseqbase == oid_nil) {
			/* trivial: nils don't match anything */
			return GDK_SUCCEED;
		}
		if (lcand) {
			lstart = 0;
			lend = lcandend - lcand;
			lvals = (const char *) lcand;
			lcand = NULL;
			lwidth = SIZEOF_OID;
		}
		loff = (wrd) l->tseqbase - (wrd) l->hseqbase;
	}
	if (r->ttype == TYPE_void) {
		if (r->tseqbase == oid_nil) {
			/* trivial: nils don't match anything */
			return GDK_SUCCEED;
		}
		if (rcand) {
			rstart = 0;
			rend = rcandend - rcand;
			rvals = (const char *) rcand;
			rcand = NULL;
			rwidth = SIZEOF_OID;
		}
		roff = (wrd) r->tseqbase - (wrd) r->hseqbase;
	}
	assert(lvals != NULL || lcand == NULL);
	assert(rvals != NULL || rcand == NULL);

	r1->tkey = 1;
	r1->tsorted = 1;
	r1->trevsorted = 1;
	r2->tkey = 1;
	r2->tsorted = 1;
	r2->trevsorted = 1;

	/* nested loop implementation for theta join */
	for (;;) {
		if (lcand) {
			if (lcand == lcandend)
				break;
			lo = *lcand++;
			vl = VALUE(l, lo - l->hseqbase);
		} else {
			if (lstart == lend)
				break;
			if (lvals) {
				vl = VALUE(l, lstart);
				if (loff != 0) {
					lval = *(const oid *)vl + loff;
					vl = (const char *) &lval;
				}
			} else {
				lval = lstart + l->tseqbase;
				vl = (const char *) &lval;
			}
			lo = lstart++ + l->hseqbase;
		}
		if (cmp(vl, nil) == 0)
			continue;
		nr = 0;
		p = rcand;
		n = rstart;
		for (;;) {
			if (rcand) {
				if (p == rcandend)
					break;
				ro = *p++;
				vr = VALUE(r, ro - r->hseqbase);
			} else {
				if (n == rend)
					break;
				if (rvals) {
					vr = VALUE(r, n);
					if (roff != 0) {
						rval = *(const oid *)vr + roff;
						vr = (const char *) &rval;
					}
				} else {
					rval = n + r->tseqbase;
					vr = (const char *) &rval;
				}
				ro = n++ + r->hseqbase;
			}
			if (cmp(vr, nil) == 0)
				continue;
			c = cmp(vl, vr);
			if (!((opcode & MASK_LT && c < 0) ||
			      (opcode & MASK_GT && c > 0) ||
			      (opcode & MASK_EQ && c == 0)))
				continue;
			if (BUNlast(r1) == BATcapacity(r1)) {
				newcap = BATgrows(r1);
				BATsetcount(r1, BATcount(r1));
				BATsetcount(r2, BATcount(r2));
				r1 = BATextend(r1, newcap);
				r2 = BATextend(r2, newcap);
				if (r1 == NULL || r2 == NULL)
					goto bailout;
				assert(BATcapacity(r1) == BATcapacity(r2));
			}
			if (BATcount(r2) > 0) {
				if (lastr + 1 != ro)
					r2->tdense = 0;
				if (nr == 0) {
					r1->trevsorted = 0;
					if (lastr > ro) {
						r2->tsorted = 0;
						r2->tkey = 0;
					} else if (lastr < ro) {
						r2->trevsorted = 0;
					} else {
						r2->tkey = 0;
					}
				}
			}
			APPEND(r1, lo);
			APPEND(r2, ro);
			lastr = ro;
			nr++;
		}
		if (nr > 1) {
			r1->tkey = 0;
			r1->tdense = 0;
			r2->trevsorted = 0;
		} else if (nr == 0) {
			lskipped = BATcount(r1) > 0;
		} else if (lskipped) {
			r1->tdense = 0;
		}
	}
	assert(BATcount(r1) == BATcount(r2));
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	BATsetcount(r2, BATcount(r2));
	if (BATcount(r1) > 0) {
		if (r1->tdense)
			r1->tseqbase = ((oid *) r1->T->heap.base)[r1->batFirst];
		if (r2->tdense)
			r2->tseqbase = ((oid *) r2->T->heap.base)[r2->batFirst];
	}
	return GDK_SUCCEED;

  bailout:
	if (r1)
		BBPreclaim(r1);
	if (r2)
		BBPreclaim(r2);
	return GDK_FAIL;
}

/* Perform an equi-join over l and r.  Returns two new, aligned,
 * dense-headed bats with in the tail the oids (head column values) of
 * matching tuples.  The result is in the same order as l (i.e. r1 is
 * sorted). */
gdk_return
BATsubleftjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, BUN estimate)
{
	BAT *r1, *r2;

	*r1p = NULL;
	*r2p = NULL;
	if (joinparamcheck(l, r, sl, sr, "BATsubleftjoin") == GDK_FAIL)
		return GDK_FAIL;
	if (joininitresults(&r1, &r2, estimate != BUN_NONE ? estimate : sl ? BATcount(sl) : BATcount(l), "BATsubleftjoin") == GDK_FAIL)
		return GDK_FAIL;
	*r1p = r1;
	*r2p = r2;
	if (r->tsorted || r->trevsorted)
		return mergejoin(r1, r2, l, r, sl, sr, 0, 0, 0);
	return hashjoin(r1, r2, l, r, sl, sr, 0, 0, 0);
}

/* Performs a left outer join over l and r.  Returns two new, aligned,
 * dense-headed bats with in the tail the oids (head column values) of
 * matching tuples, or the oid in the first output bat and nil in the
 * second output bat if the value in l does not occur in r.  The
 * result is in the same order as l (i.e. r1 is sorted). */
gdk_return
BATsubouterjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, BUN estimate)
{
	BAT *r1, *r2;

	*r1p = NULL;
	*r2p = NULL;
	if (joinparamcheck(l, r, sl, sr, "BATsubouterjoin") == GDK_FAIL)
		return GDK_FAIL;
	if (joininitresults(&r1, &r2, estimate != BUN_NONE ? estimate : sl ? BATcount(sl) : BATcount(l), "BATsubouterjoin") == GDK_FAIL)
		return GDK_FAIL;
	*r1p = r1;
	*r2p = r2;
	if (r->tsorted || r->trevsorted)
		return mergejoin(r1, r2, l, r, sl, sr, 0, 1, 0);
	return hashjoin(r1, r2, l, r, sl, sr, 0, 1, 0);
}

/* Perform a semi-join over l and r.  Returns two new, aligned,
 * dense-headed bats with in the tail the oids (head column values) of
 * matching tuples.  The result is in the same order as l (i.e. r1 is
 * sorted). */
gdk_return
BATsubsemijoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, BUN estimate)
{
	BAT *r1, *r2;

	*r1p = NULL;
	*r2p = NULL;
	if (joinparamcheck(l, r, sl, sr, "BATsubsemijoin") == GDK_FAIL)
		return GDK_FAIL;
	if (joininitresults(&r1, &r2, estimate != BUN_NONE ? estimate : sl ? BATcount(sl) : BATcount(l), "BATsubsemijoin") == GDK_FAIL)
		return GDK_FAIL;
	*r1p = r1;
	*r2p = r2;
	if (r->tsorted || r->trevsorted)
		return mergejoin(r1, r2, l, r, sl, sr, 0, 0, 1);
	return hashjoin(r1, r2, l, r, sl, sr, 0, 0, 1);
}

gdk_return
BATsubthetajoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, const char *op, BUN estimate)
{
	BAT *r1, *r2;

	*r1p = NULL;
	*r2p = NULL;
	if (joinparamcheck(l, r, sl, sr, "BATsubthetajoin") == GDK_FAIL)
		return GDK_FAIL;
	if (joininitresults(&r1, &r2, estimate != BUN_NONE ? estimate : sl ? BATcount(sl) : BATcount(l), "BATsubthetajoin") == GDK_FAIL)
		return GDK_FAIL;
	*r1p = r1;
	*r2p = r2;
	return thetajoin(r1, r2, l, r, sl, sr, op);
}

gdk_return
BATsubjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, BUN estimate)
{
	BAT *r1, *r2;
	BUN lcount, rcount;
	int swap;

	*r1p = NULL;
	*r2p = NULL;
	if (joinparamcheck(l, r, sl, sr, "BATsubjoin") == GDK_FAIL)
		return GDK_FAIL;
	lcount = BATcount(l);
	if (sl)
		lcount = MIN(lcount, BATcount(sl));
	rcount = BATcount(r);
	if (sr)
		rcount = MIN(rcount, BATcount(sr));
	if (lcount == 0 || rcount == 0) {
		r1 = BATnew(TYPE_void, TYPE_void, 0);
		BATseqbase(r1, 0);
		BATseqbase(BATmirror(r1), 0);
		r2 = BATnew(TYPE_void, TYPE_void, 0);
		BATseqbase(r2, 0);
		BATseqbase(BATmirror(r2), 0);
		*r1p = r1;
		*r2p = r2;
		return GDK_SUCCEED;
	}
	if (joininitresults(&r1, &r2, estimate != BUN_NONE ? estimate : sl ? BATcount(sl) : BATcount(l), "BATsubjoin") == GDK_FAIL)
		return GDK_FAIL;
	*r1p = r1;
	*r2p = r2;
	swap = 0;
	if ((l->tsorted || l->trevsorted) && (r->tsorted || r->trevsorted)) {
		/* both sorted, don't swap */
		return mergejoin(r1, r2, l, r, sl, sr, 0, 0, 0);
	} else if (l->T->hash && r->T->hash) {
		/* both have hash, smallest on right */
		if (lcount < rcount)
			swap = 1;
	} else if (l->T->hash) {
		/* only left has hash, swap */
		swap = 1;
	} else if (r->T->hash) {
		/* only right has hash, don't swap */
		swap = 0;
	} else if (l->tsorted || l->trevsorted) {
		/* left is sorted, swap */
		return mergejoin(r2, r1, r, l, sr, sl, 0, 0, 0);
	} else if (r->tsorted || r->trevsorted) {
		/* right is sorted, don't swap */
		return mergejoin(r1, r2, l, r, sl, sr, 0, 0, 0);
	} else if (BATcount(l) < BATcount(r)) {
		/* no hashes, not sorted, create hash on smallest BAT */
		swap = 1;
	}
	if (swap) {
		return hashjoin(r2, r1, r, l, sr, sl, 0, 0, 0);
	} else {
		return hashjoin(r1, r2, l, r, sl, sr, 0, 0, 0);
	}
}

BAT *
BATproject(BAT *l, BAT *r)
{
	BAT *bn;
	const oid *o;
	const void *nil = ATOMnilptr(r->ttype);
	const void *v, *prev;
	BATiter ri, bni;
	oid lo, hi;
	BUN n;
	int (*cmp)(const void *, const void *) = BATatoms[r->ttype].atomCmp;
	int c;

	ALGODEBUG fprintf(stderr, "#BATproject(l=%s#" BUNFMT "%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s)\n",
			  BATgetId(l), BATcount(l),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "");

	assert(BAThdense(l));
	assert(BAThdense(r));
	assert(ATOMtype(l->ttype) == TYPE_oid);

	if (BATtdense(l) && BATcount(l) > 0) {
		lo = l->tseqbase;
		hi = l->tseqbase + BATcount(l);
		if (lo < r->hseqbase || hi > r->hseqbase + BATcount(r)) {
			GDKerror("BATproject: does not match always\n");
			return NULL;
		}
		bn = BATslice(r, lo - r->hseqbase, hi - r->hseqbase);
		if (bn == NULL)
			return NULL;
		return BATseqbase(bn, l->hseqbase + (lo - l->tseqbase));
	}
	if (l->ttype == TYPE_void || BATcount(l) == 0) {
		assert(BATcount(l) == 0 || l->tseqbase == oid_nil);
		bn = BATconstant(r->ttype, nil, BATcount(l));
		if (bn != NULL) {
			bn = BATseqbase(bn, l->hseqbase);
			if (bn->ttype == TYPE_void && BATcount(bn) == 0)
				BATseqbase(BATmirror(bn), 0);
		}
		return bn;
	}
	assert(l->ttype == TYPE_oid);
	bn = BATnew(TYPE_void, ATOMtype(r->ttype), BATcount(l));
	if (bn == NULL)
		return NULL;
	o = (const oid *) Tloc(l, BUNfirst(l));
	n = BUNfirst(bn);
	ri = bat_iterator(r);
	bni = bat_iterator(bn);
	/* be optimistic, we'll change this as needed */
	bn->T->nonil = 1;
	bn->T->nil = 0;
	bn->tsorted = 1;
	bn->trevsorted = 1;
	bn->tkey = 1;
	prev = NULL;
	for (lo = l->hseqbase, hi = lo + BATcount(l); lo < hi; lo++, o++, n++) {
		if (*o == oid_nil) {
			tfastins_nocheck(bn, n, nil, Tsize(bn));
			bn->T->nonil = 0;
			bn->T->nil = 1;
			bn->tsorted = 0;
			bn->trevsorted = 0;
			bn->tkey = 0;
		} else if (*o < r->hseqbase ||
			   *o >= r->hseqbase + BATcount(r)) {
			GDKerror("BATproject: does not match always\n");
			goto bunins_failed;
		} else {
			v = BUNtail(ri, *o - r->hseqbase + BUNfirst(r));
			tfastins_nocheck(bn, n, v, Tsize(bn));
			if (bn->T->nonil && cmp(v, nil) == 0) {
				bn->T->nonil = 0;
				bn->T->nil = 1;
			}
			if (prev && (bn->trevsorted | bn->tsorted | bn->tkey)) {
				c = cmp(prev, v);
				if (c < 0) {
					bn->trevsorted = 0;
					if (!bn->tsorted)
						bn->tkey = 0; /* can't be sure */
				} else if (c > 0) {
					bn->tsorted = 0;
					if (!bn->trevsorted)
						bn->tkey = 0; /* can't be sure */
				} else {
					bn->tkey = 0; /* definitely */
				}
			}
			prev = BUNtail(bni, n);
		}
	}
	assert(n == BATcount(l));
	BATsetcount(bn, n);
	BATseqbase(bn, l->hseqbase);
	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}
