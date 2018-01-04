/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * In this file we have a number of functions that search a column
 * using binary search.  The column must be sorted or reverse sorted
 * (for the SORTfnd* functions), or there must be an order index (for
 * the ORDERfnd* functions).
 *
 * All functions return a BUN, i.e. an offset from the start of the
 * column.  The SORTfnd and ORDERfnd functions return BUN_NONE if the
 * value does not occur in the column.
 *
 * The ORDERfnd* functions return an offset in the order index, so to
 * get the actual position, (the OID, not the BUN), read the order
 * index at that offset.
 *
 * The *fndfirst functions return the BUN of the *first* value in the
 * column that is greater (less) than or equal to the value being
 * searched (when the column is sorted in ascending (descending)
 * order).
 *
 * The *fndlast functions return the BUN of the *first* value in the
 * column that is greater (less) than the value being searched (when
 * the column is sorted in ascending (descending) order).
 *
 * If the value to be found occurs, the following relationship holds:
 *
 * SORTfndfirst(b, v) <= SORTfnd(b, v) < SORTfndlast(b, v)
 * ORDERfndfirst(b, v) <= ORDERfnd(b, v) < ORDERfndlast(b, v)
 *
 * and the range from *fndfirst (included) up to *fndlast (not
 * included) are all values in the column that are equal to the value.
 *
 * If the value to be found does not occur, SORTfnd and ORDERfnd
 * return BUN_NONE, and the other functions return the location of the
 * next larger value, or BATcount if the value being searched for is
 * larger (smaller if reverse sorted) than any in the column.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#define VALUE(x)	(vars ?					\
			 vars + VarHeapVal(vals, (x), width) :	\
			 (const char *) vals + ((x) * width))

#define BINSEARCHFUNC(TYPE)						\
BUN									\
binsearch_##TYPE(const oid *restrict indir, oid offset,			\
		 const TYPE *restrict vals, BUN lo, BUN hi, TYPE v,	\
		 int ordering, int last)				\
{									\
	BUN mid;							\
	TYPE x;								\
									\
	assert(ordering == 1 || ordering == -1);			\
	assert(lo <= hi);						\
									\
	if (ordering > 0) {						\
		if (indir) {						\
			if (last > 0) {					\
				if ((x = vals[indir[lo] - offset]) > v) \
					return lo;			\
				if ((x = vals[indir[hi] - offset]) <= v) \
					return hi + 1;			\
									\
				/* loop invariant: */			\
				/* value@lo <= v < value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					if (vals[indir[mid] - offset] > v) \
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			} else {					\
				if ((x = vals[indir[lo] - offset]) >= v) \
					return last == 0 || x == v ? lo : BUN_NONE; \
				if ((x = vals[indir[hi] - offset]) < v) \
					return last == 0 ? hi + 1 : BUN_NONE; \
									\
				/* loop invariant: */			\
				/* value@lo < v <= value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					if (vals[indir[mid] - offset] >= v) \
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			}						\
		} else {						\
			if (last > 0) {					\
				if ((x = vals[lo]) > v)			\
					return lo;			\
				if ((x = vals[hi]) <= v)		\
					return hi + 1;			\
									\
				/* loop invariant: */			\
				/* value@lo <= v < value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					if (vals[mid] > v)		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			} else {					\
				if ((x = vals[lo]) >= v)		\
					return last == 0 || x == v ? lo : BUN_NONE; \
				if ((x = vals[hi]) < v)			\
					return last == 0 ? hi + 1 : BUN_NONE; \
									\
				/* loop invariant: */			\
				/* value@lo < v <= value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					if (vals[mid] >= v)		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			}						\
		}							\
	} else {							\
		if (indir) {						\
			if (last > 0) {					\
				if ((x = vals[indir[lo] - offset]) < v) \
					return lo;			\
				if ((x = vals[indir[hi] - offset]) >= v) \
					return hi + 1;			\
									\
				/* loop invariant: */			\
				/* value@lo >= v > value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					if (vals[indir[mid] - offset] < v) \
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			} else {					\
				if ((x = vals[indir[lo] - offset]) <= v) \
					return last == 0 || x == v ? lo : BUN_NONE; \
				if ((x = vals[indir[hi] - offset]) > v) \
					return last == 0 ? hi + 1 : BUN_NONE; \
									\
				/* loop invariant: */			\
				/* value@lo > v >= value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					if (vals[indir[mid] - offset] <= v) \
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			}						\
		} else {						\
			if (last  > 0) {				\
				if ((x = vals[lo]) < v)			\
					return lo;			\
				if ((x = vals[hi]) >= v)		\
					return hi + 1;			\
									\
				/* loop invariant: */			\
				/* value@lo >= v > value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					if (vals[mid] < v)		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			} else {					\
				if ((x = vals[lo]) <= v)		\
					return last == 0 || x == v ? lo : BUN_NONE; \
				if ((x = vals[hi]) > v)			\
					return last == 0 ? hi + 1 : BUN_NONE; \
									\
				/* loop invariant: */			\
				/* value@lo > v >= value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					if (vals[mid] <= v)		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			}						\
		}							\
	}								\
	return last >= 0 || (indir ? vals[indir[hi] - offset] : vals[hi]) == v ? hi : BUN_NONE; \
}

BINSEARCHFUNC(bte)
BINSEARCHFUNC(sht)
BINSEARCHFUNC(int)
BINSEARCHFUNC(lng)
#ifdef HAVE_HGE
BINSEARCHFUNC(hge)
#endif
BINSEARCHFUNC(flt)
BINSEARCHFUNC(dbl)

/* Do a binary search for the first/last occurrence of v between lo and hi
 * (lo inclusive, hi not inclusive) in vals/vars.
 * If last is set, return the index of the first value > v; if last is
 * not set, return the index of the first value >= v.
 * If ordering is -1, the values are sorted in reverse order and hence
 * all comparisons are reversed.
 */
BUN
binsearch(const oid *restrict indir, oid offset,
	  int type, const void *restrict vals, const char * restrict vars,
	  int width, BUN lo, BUN hi, const void *restrict v,
	  int ordering, int last)
{
	BUN mid;
	int c;
	int (*cmp)(const void *, const void *);

	assert(ordering == 1 || ordering == -1);
	assert(lo < hi);

	--hi;			/* now hi is inclusive */

	switch (ATOMbasetype(type)) {
		/* TYPE_oid is covered by TYPE_int/TYPE_lng */
	case TYPE_bte:
		return binsearch_bte(indir, offset, (const bte *) vals,
				     lo, hi, *(const bte *) v, ordering, last);
	case TYPE_sht:
		return binsearch_sht(indir, offset, (const sht *) vals,
				     lo, hi, *(const sht *) v, ordering, last);
	case TYPE_int:
		return binsearch_int(indir, offset, (const int *) vals,
				     lo, hi, *(const int *) v, ordering, last);
	case TYPE_lng:
		return binsearch_lng(indir, offset, (const lng *) vals,
				     lo, hi, *(const lng *) v, ordering, last);
#ifdef HAVE_HGE
	case TYPE_hge:
		return binsearch_hge(indir, offset, (const hge *) vals,
				     lo, hi, *(const hge *) v, ordering, last);
#endif
	case TYPE_flt:
		return binsearch_flt(indir, offset, (const flt *) vals,
				     lo, hi, *(const flt *) v, ordering, last);
	case TYPE_dbl:
		return binsearch_dbl(indir, offset, (const dbl *) vals,
				     lo, hi, *(const dbl *) v, ordering, last);
	}

	cmp = ATOMcompare(type);

	if (last > 0) {
		if ((c = ordering * cmp(VALUE(indir ? indir[lo] - offset : lo), v)) > 0)
			return lo;
		if ((c = ordering * cmp(VALUE(indir ? indir[hi] - offset : hi), v)) <= 0)
			return hi + 1;
	} else if (last == 0) {
		if ((c = ordering * cmp(VALUE(indir ? indir[lo] - offset : lo), v)) >= 0)
			return lo;
		if ((c = ordering * cmp(VALUE(indir ? indir[hi] - offset : hi), v)) < 0)
			return hi + 1;
	} else {
		if ((c = ordering * cmp(VALUE(indir ? indir[lo] - offset : lo), v)) > 0)
			return BUN_NONE;
		if (c == 0)
			return lo;
		if ((c = ordering * cmp(VALUE(indir ? indir[hi] - offset : hi), v)) < 0)
			return BUN_NONE;
		if (c == 0)
			return hi;
		if (hi - lo <= 1) {
			/* not the first, not the last, and nothing in
			 * between */
			return BUN_NONE;
		}
	}

	/* loop invariant:
	 * last ? value@lo <= v < value@hi : value@lo < v <= value@hi
	 *
	 * This version does some more work in the inner loop than the
	 * type-expanded versions (ordering and indir checks) but is
	 * slow due to the function call and the needed check for
	 * vars (in VALUE()) already, so we're beyond caring. */
	while (hi - lo > 1) {
		mid = (hi + lo) / 2;
		if ((c = ordering * cmp(VALUE(indir ? indir[mid] - offset : mid), v)) > 0 ||
		    (last <= 0 && c == 0))
			hi = mid;
		else
			lo = mid;
	}
	return last >= 0 || cmp(VALUE(indir ? indir[hi] - offset : hi), v) == 0 ? hi : BUN_NONE;
}

/* Return the BUN of any tail value in b that is equal to v; if no
 * match is found, return BUN_NONE.  b must be sorted (reverse or
 * forward). */
BUN
SORTfnd(BAT *b, const void *v)
{
	if (BATcount(b) == 0)
		return BUN_NONE;
	if (BATtdense(b)) {
		if (*(oid*)v == oid_nil ||
		    *(oid*)v < b->tseqbase ||
		    *(oid*)v >= b->tseqbase + BATcount(b))
			return BUN_NONE;
		return *(oid*)v - b->tseqbase;
	}
	if (b->ttype == TYPE_void) {
		assert(b->tseqbase == oid_nil);
		if (*(const oid *) v == oid_nil)
			return 0;
		return BUN_NONE;
	}
	return binsearch(NULL, 0, b->ttype, Tloc(b, 0),
			 b->tvheap ? b->tvheap->base : NULL, b->twidth, 0,
			 BATcount(b), v, b->tsorted ? 1 : -1, -1);
}

/* use orderidx, returns BUN on order index */
BUN
ORDERfnd(BAT *b, const void *v)
{
	assert(b->torderidx);
	if (BATcount(b) == 0)
		return BUN_NONE;
	return binsearch((oid *) b->torderidx->base + ORDERIDXOFF, 0, b->ttype,
			 Tloc(b, 0), b->tvheap ? b->tvheap->base : NULL,
			 b->twidth, 0, BATcount(b), v, 1, -1);
}

/* Return the BUN of the first (lowest numbered) tail value that is
 * equal to v; if no match is found, return the BUN of the next higher
 * value in b.  b must be sorted (reverse or forward). */
BUN
SORTfndfirst(BAT *b, const void *v)
{
	if (BATcount(b) == 0)
		return 0;
	if (BATtdense(b)) {
		if (*(oid*)v == oid_nil || *(oid*)v <= b->tseqbase)
			return 0;
		if (*(oid*)v >= b->tseqbase + BATcount(b))
			return BATcount(b);
		return *(oid*)v - b->tseqbase;
	}
	if (b->ttype == TYPE_void) {
		assert(b->tseqbase == oid_nil);
		return 0;
	}
	return binsearch(NULL, 0, b->ttype, Tloc(b, 0),
			 b->tvheap ? b->tvheap->base : NULL, b->twidth, 0,
			 BATcount(b), v, b->tsorted ? 1 : -1, 0);
}

/* use orderidx, returns BUN on order index */
BUN
ORDERfndfirst(BAT *b, const void *v)
{
	assert(b->torderidx);
	if (BATcount(b) == 0)
		return 0;
	return binsearch((oid *) b->torderidx->base + ORDERIDXOFF, 0, b->ttype,
			 Tloc(b, 0), b->tvheap ? b->tvheap->base : NULL,
			 b->twidth, 0, BATcount(b), v, 1, 0);
}

/* Return the BUN of the first (lowest numbered) tail value beyond v.
 * b must be sorted (reverse or forward). */
BUN
SORTfndlast(BAT *b, const void *v)
{
	if (BATcount(b) == 0)
		return 0;
	if (BATtdense(b)) {
		if (*(oid*)v == oid_nil || *(oid*)v <= b->tseqbase)
			return 0;
		if (*(oid*)v >= b->tseqbase + BATcount(b))
			return BATcount(b);
		return *(oid*)v - b->tseqbase;
	}
	if (b->ttype == TYPE_void) {
		assert(b->tseqbase == oid_nil);
		return BATcount(b);
	}
	return binsearch(NULL, 0, b->ttype, Tloc(b, 0),
			 b->tvheap ? b->tvheap->base : NULL, b->twidth, 0,
			 BATcount(b), v, b->tsorted ? 1 : -1, 1);
}

/* use orderidx, returns BUN on order index */
BUN
ORDERfndlast(BAT *b, const void *v)
{
	assert(b->torderidx);
	if (BATcount(b) == 0)
		return 0;
	return binsearch((oid *) b->torderidx->base + ORDERIDXOFF, 0, b->ttype,
			 Tloc(b, 0), b->tvheap ? b->tvheap->base : NULL,
			 b->twidth, 0, BATcount(b), v, 1, 1);
}
