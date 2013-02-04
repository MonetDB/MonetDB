#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

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
	r2->T->nil = 0;
	r2->T->nonil = 1;
	r2->tkey = 1;
	*r1p = r1;
	*r2p = r2;
	return GDK_SUCCEED;
}

#define VALUE(side, x)		(side##vars ? side##vars + VarHeapVal(side##vals, (x), shift) : side##vals + ((x) << shift))

/* Do a binary search for the first/last occurrence of v between lo and hi
 * (lo inclusive, hi not inclusive) in values.
 * If last is set, return the index of the first value > v; if last is
 * not set, return the index of the first value >= v.
 * If reverse is -1, the values are sorted in reverse order and hence
 * all comparisons are reversed.
 */
static BUN
binsearch(const oid *rcand, oid offset,
	  const char *rvals, const char *rvars,
	  int shift, BUN lo, BUN hi, const char *v,
	  int (*cmp)(const void *, const void *), int reverse, int last)
{
	BUN mid;
	int c;

	assert(reverse == 1 || reverse == -1);
	assert(lo < hi);

	--hi;			/* now hi is inclusive */
	if ((c = reverse * cmp(VALUE(r, rcand ? rcand[lo] - offset : lo), v)) > 0 ||
	    (!last && c == 0))
		return lo;
	if ((c = reverse * cmp(VALUE(r, rcand ? rcand[hi] - offset : hi), v)) < 0 ||
	    (last && c == 0))
		return hi + 1;
	while (hi - lo > 1) {
		mid = (hi + lo) / 2;
		if ((c = reverse * cmp(VALUE(r, rcand ? rcand[mid] - offset : mid), v)) > 0 ||
		    (!last && c == 0))
			hi = mid;
		else
			lo = mid;
	}
	return hi;
}

#define APPEND(b, o)		(((oid *) b->T->heap.base)[b->batFirst + b->batCount++] = (o))

/* Perform a "merge" join on l and r (must both be sorted) with
 * optional candidate lists.  The return BATs have already been
 * created by the caller.
 */
static gdk_return
mergejoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr, int nil_matches, int nil_on_miss, int semi)
{
	BUN lstart, lend, lcnt;
	const oid *lcand = NULL, *lcandend = NULL;
	BUN rstart, rend, rcnt;
	const oid *rcand = NULL, *rcandend = NULL;
	BUN lscan, rscan;
	const char *lvals, *rvals;
	const char *lvars, *rvars;
	int shift;
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = BATatoms[l->ttype].atomCmp;
	const char *v;
	BUN nl, nr;
	int insert_nil;
	int equal_order;
	int lreverse, rreverse;
	oid lv;
	BUN i;

	assert(BAThdense(l));
	assert(BAThdense(r));
	assert(l->ttype == r->ttype);
	assert(l->tsorted || l->trevsorted);
	assert(r->tsorted || r->trevsorted);
	assert(sl == NULL || sl->tsorted);
	assert(sr == NULL || sr->tsorted);

	CANDINIT(l, sl, lstart, lend, lcnt, lcand, lcandend);
	CANDINIT(r, sr, rstart, rend, rcnt, rcand, rcandend);
	lvals = (const char *) Tloc(l, BUNfirst(l));
	rvals = (const char *) Tloc(r, BUNfirst(r));
	if (l->tvarsized && l->ttype) {
		assert(r->tvarsized && r->ttype);
		lvars = l->T->vheap->base;
		rvars = r->T->vheap->base;
	} else {
		assert(!r->tvarsized || !r->ttype);
		lvars = rvars = NULL;
	}
	shift = l->T->shift;
	/* equal_order is set if we can scan both BATs in the same
	 * order, so when both are sorted or both are reverse
	 * sorted */
	equal_order = l->tsorted == r->tsorted || l->trevsorted == r->trevsorted;
	/* [lr]reverse is either 1 or -1 depending on the order of
	 * l/r: it determines the comparison function used */
	lreverse = l->tsorted ? 1 : -1;
	rreverse = r->tsorted ? 1 : -1;

	/* set basic properties, they will be adjusted if necessary
	 * later on */
	r1->tsorted = 1;
	r1->trevsorted = 1;
	r1->T->nil = 0;
	r1->T->nonil = 1;
	r2->tsorted = 1;
	r2->trevsorted = 1;
	r2->T->nil = 0;
	r2->T->nonil = 1;

	if (lstart == lend || (!nil_on_miss && rstart == rend)) {
		/* nothing to do: there are no matches */
		return GDK_SUCCEED;
	}

	/* determine opportunistic scan window for l/r */
	for (nl = lcand ? (BUN) (lcandend - lcand) : lend - lstart, lscan = 4;
	     nl > 0;
	     lscan++)
		nl >>= 1;
	for (nl = rcand ? (BUN) (rcandend - rcand) : rend - rstart, rscan = 4;
	     nl > 0;
	     rscan++)
		nl >>= 1;

	while (lcand ? lcand < lcandend : lstart < lend) {
		if (!nil_on_miss) {
			/* if next value in r too far away (more than
			 * lscan from current position in l), use
			 * binary search on l to skip over
			 * non-matching values */
			if (lcand) {
				if (lscan < (BUN) (lcandend - lcand) &&
				    lreverse * cmp(VALUE(l, *lcand - l->hseqbase + lscan),
						   (v = VALUE(r, rcand ? *rcand - r->hseqbase : rstart))) < 0)
					lcand += binsearch(lcand, l->hseqbase, lvals, lvars, shift, lscan, lcandend - lcand, v, cmp, lreverse, 0);
			} else {
				if (lscan < lend - lstart &&
				    lreverse * cmp(VALUE(l, lstart + lscan),
						   (v = VALUE(r, rcand ? *rcand - r->hseqbase : rstart))) < 0)
					lstart = binsearch(NULL, 0, lvals, lvars, shift, lstart + lscan, lend, v, cmp, lreverse, 0);
			}
		}
		/* v is the value we're going to work with in this
		 * iteration */
		v = VALUE(l, lcand ? *lcand - l->hseqbase : lstart);
		nl = 1;
		/* count number of equal values in left */
		if (lcand) {
			while (++lcand < lcandend &&
			       cmp(v, VALUE(l, *lcand - l->hseqbase)) == 0)
				nl++;
		} else {
			while (++lstart < lend &&
			       cmp(v, VALUE(l, lstart)) == 0)
				nl++;
		}
		/* lcand/lstart points one beyond the value we're
		 * going to match: ready for the next */
		if (!nil_matches && cmp(v, nil) == 0) {
			/* v is nil and nils don't match anythin */
			continue;
		}
		if (equal_order) {
			if (rcand) {
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search */
				if (rscan < (BUN) (rcandend - rcand) &&
				    rreverse * cmp(v, VALUE(r, *rcand - r->hseqbase + rscan)) > 0) {
					/* value too far away in r:
					 * use binary search */
					rcand += binsearch(rcand, r->hseqbase, rvals, rvars, shift, rscan, rcandend - rcand, v, cmp, rreverse, 0);
				} else {
					/* scan r for v */
					while (rcand < rcandend &&
					       rreverse * cmp(v, VALUE(r, *rcand - r->hseqbase)) > 0)
						rcand++;
				}
			} else {
				/* look ahead a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search */
				if (rscan < rend - rstart &&
				    rreverse * cmp(v, VALUE(r, rstart + rscan)) > 0) {
					/* value too far away in r:
					 * use binary search */
					rstart = binsearch(NULL, 0, rvals, rvars, shift, rstart + rscan, rend, v, cmp, rreverse, 0);
				} else {
					/* scan r for v */
					while (rstart < rend &&
					       rreverse * cmp(v, VALUE(r, rstart)) > 0)
						rstart++;
				}
			}
			/* rstart or rcand points to first value >= v
			 * or end of r */
		} else {
			if (rcand) {
				/* look back a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search */
				if (rscan < (BUN) (rcandend - rcand) &&
				    rreverse * cmp(v, VALUE(r, *rcandend - r->hseqbase - rscan - 1)) < 0) {
					/* value too far away in r:
					 * use binary search */
					rcandend = rcand + binsearch(rcand, r->hseqbase, rvals, rvars, shift, 0, (BUN) (rcandend - rcand) - rscan, v, cmp, rreverse, 1);
				} else {
					/* scan r for v */
					while (rcand < rcandend &&
					       rreverse * cmp(v, VALUE(r, rcandend[-1] - r->hseqbase)) < 0)
						rcandend--;
				}
			} else {
				/* look back a little (rscan) in r to
				 * see whether we're better off doing
				 * a binary search */
				if (rscan < rend - rstart &&
				    rreverse * cmp(v, VALUE(r, rend - rscan - 1)) < 0) {
					/* value too far away in r:
					 * use binary search */
					rend = binsearch(NULL, 0, rvals, rvars, shift, rstart, rend - rscan, v, cmp, rreverse, 1);
				} else {
					/* scan r for v */
					while (rstart < rend &&
					       rreverse * cmp(v, VALUE(r, rend - 1)) < 0)
						rend--;
				}
			}
			/* rend/rcandend now points to first value > v
			 * or start of r */
		}
		/* count number of entries in r that are equal to v */
		nr = 0;
		if (equal_order) {
			while ((rcand ? rcand < rcandend : rstart < rend) &&
			       cmp(v, VALUE(r, rcand ? *rcand - r->hseqbase : rstart)) == 0) {
				nr++;
				if (rcand)
					rcand++;
				else
					rstart++;
			}
		} else {
			while ((rcand ? rcand < rcandend : rstart < rend) &&
			       cmp(v, VALUE(r, rcand ? rcandend[-1] - r->hseqbase : rend - 1)) == 0) {
				nr++;
				if (rcand)
					rcandend--;
				else
					rend--;
			}
		}
		if (nr == 0) {
			/* no entries in r found */
			if (!nil_on_miss) {
				if (rcand ? rcand == rcandend : rstart == rend) {
					/* nothing more left to match
					 * in r */
					break;
				}
				continue;
			}
			/* insert a nil to indicate a non-match */
			insert_nil = 1;
			nr = 1;
			r2->T->nil = 1;
			r2->T->nonil = 0;
			r2->tsorted = 0;
			r2->trevsorted = 0;
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
			 * r2 is not key */
			r2->tkey = 0;
			/* multiple different values will be inserted
			 * in r1 (always in order), so not reverse
			 * ordered anymore */
			r1->trevsorted = 0;
		}
		if (nr > 1) {
			/* value occurs multiple times in r, so entry
			 * in l will be repeated multiple times: hence
			 * r1 is not key */
			r1->tkey = 0;
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
		if (BATcount(r1) > 0) {
			/* a new, higher value will be inserted into
			 * r1, so r1 is not reverse ordered anymore */
			r1->trevsorted = 0;
			/* depending on whether l and r are ordered
			 * the same or not, a new higher or lower
			 * value will be added to r2 */
			if (equal_order)
				r2->trevsorted = 0;
			else
				r2->tsorted = 0;
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
			do {
				lv = lcand ? lcand[-(ssize_t)nl] : lstart + l->hseqbase - nl;

				for (i = nr; i > 0; i--) {
					APPEND(r1, lv);
					APPEND(r2, rcand[-(ssize_t)i]);
				}
			} while (--nl > 0);
		} else if (rcand) {
			do {
				lv = lcand ? lcand[-(ssize_t)nl] : lstart + l->hseqbase - nl;

				for (i = 0; i < nr; i++) {
					APPEND(r1, lv);
					APPEND(r2, rcandend[i]);
				}
			} while (--nl > 0);
		} else if (equal_order) {
			do {
				lv = lcand ? lcand[-(ssize_t)nl] : lstart + l->hseqbase - nl;

				for (i = nr; i > 0; i--) {
					APPEND(r1, lv);
					APPEND(r2, rstart + r->hseqbase - i);
				}
			} while (--nl > 0);
		} else {
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
	return GDK_SUCCEED;
}

gdk_return
BATsubmergejoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr)
{
	BAT *r1, *r2;

	*r1p = NULL;
	*r2p = NULL;
	if (joinparamcheck(l, r, sl, sr, "BATsubleftjoin") == GDK_FAIL)
		return GDK_FAIL;
	if (joininitresults(&r1, &r2, sl ? BATcount(sl) : BATcount(l), "BATsubmergejoin") == GDK_FAIL)
		return GDK_FAIL;
	*r1p = r1;
	*r2p = r2;
	return mergejoin(r1, r2, l, r, sl, sr, 0, 0, 0);
}
