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
 *
 * Note that the NIL value is considered smaller than all other values.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#define VALUE(x)	(vars ?					\
			 vars + VarHeapVal(vals, (x), width) :	\
			 (const char *) vals + ((x) * width))

#define bte_LT(a, b)	((a) < (b))
#define bte_LE(a, b)	((a) <= (b))
#define bte_GT(a, b)	((a) > (b))
#define bte_GE(a, b)	((a) >= (b))
#define bte_EQ(a, b)	((a) == (b))
#define sht_LT(a, b)	((a) < (b))
#define sht_LE(a, b)	((a) <= (b))
#define sht_GT(a, b)	((a) > (b))
#define sht_GE(a, b)	((a) >= (b))
#define sht_EQ(a, b)	((a) == (b))
#define int_LT(a, b)	((a) < (b))
#define int_LE(a, b)	((a) <= (b))
#define int_GT(a, b)	((a) > (b))
#define int_GE(a, b)	((a) >= (b))
#define int_EQ(a, b)	((a) == (b))
#define lng_LT(a, b)	((a) < (b))
#define lng_LE(a, b)	((a) <= (b))
#define lng_GT(a, b)	((a) > (b))
#define lng_GE(a, b)	((a) >= (b))
#define lng_EQ(a, b)	((a) == (b))
#ifdef HAVE_HGE
#define hge_LT(a, b)	((a) < (b))
#define hge_LE(a, b)	((a) <= (b))
#define hge_GT(a, b)	((a) > (b))
#define hge_GE(a, b)	((a) >= (b))
#define hge_EQ(a, b)	((a) == (b))
#endif
#define flt_LT(a, b)	(!is_flt_nil(b) && (is_flt_nil(a) || (a) < (b)))
#define flt_LE(a, b)	(is_flt_nil(a) || (!is_flt_nil(b) && (a) <= (b)))
#define flt_GT(a, b)	(!is_flt_nil(a) && (is_flt_nil(b) || (a) > (b)))
#define flt_GE(a, b)	(is_flt_nil(b) || (!is_flt_nil(a) && (a) >= (b)))
#define flt_EQ(a, b)	(is_flt_nil(a) ? is_flt_nil(b) : !is_flt_nil(b) && (a) == (b))
#define dbl_LT(a, b)	(!is_dbl_nil(b) && (is_dbl_nil(a) || (a) < (b)))
#define dbl_LE(a, b)	(is_dbl_nil(a) || (!is_dbl_nil(b) && (a) <= (b)))
#define dbl_GT(a, b)	(!is_dbl_nil(a) && (is_dbl_nil(b) || (a) > (b)))
#define dbl_GE(a, b)	(is_dbl_nil(b) || (!is_dbl_nil(a) && (a) >= (b)))
#define dbl_EQ(a, b)	(is_dbl_nil(a) ? is_dbl_nil(b) : !is_dbl_nil(b) && (a) == (b))

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
				x = vals[indir[lo] - offset];		\
				if (TYPE##_GT(x, v))			\
					return lo;			\
				x = vals[indir[hi] - offset];		\
				if (TYPE##_LE(x, v))			\
					return hi + 1;			\
									\
				/* loop invariant: */			\
				/* value@lo <= v < value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					x = vals[indir[mid] - offset];	\
					if (TYPE##_GT(x, v))		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			} else {					\
				x = vals[indir[lo] - offset];		\
				if (TYPE##_GE(x, v))			\
					return last == 0 || TYPE##_EQ(x, v) ? lo : BUN_NONE; \
				x = vals[indir[hi] - offset];		\
				if (TYPE##_LT(x, v))			\
					return last == 0 ? hi + 1 : BUN_NONE; \
									\
				/* loop invariant: */			\
				/* value@lo < v <= value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					x = vals[indir[mid] - offset];	\
					if (TYPE##_GE(x, v))		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			}						\
		} else {						\
			if (last > 0) {					\
				x = vals[lo];				\
				if (TYPE##_GT(x, v))			\
					return lo;			\
				x = vals[hi];				\
				if (TYPE##_LE(x, v))			\
					return hi + 1;			\
									\
				/* loop invariant: */			\
				/* value@lo <= v < value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					x = vals[mid];			\
					if (TYPE##_GT(x, v))		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			} else {					\
				x = vals[lo];				\
				if (TYPE##_GE(x, v))			\
					return last == 0 || TYPE##_EQ(x, v) ? lo : BUN_NONE; \
				x = vals[hi];				\
				if (TYPE##_LT(x, v))			\
					return last == 0 ? hi + 1 : BUN_NONE; \
									\
				/* loop invariant: */			\
				/* value@lo < v <= value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					x = vals[mid];			\
					if (TYPE##_GE(x, v))		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			}						\
		}							\
	} else {							\
		if (indir) {						\
			if (last > 0) {					\
				x = vals[indir[lo] - offset];		\
				if (TYPE##_LT(x, v))			\
					return lo;			\
				x = vals[indir[hi] - offset];		\
				if (TYPE##_GE(x, v))			\
					return hi + 1;			\
									\
				/* loop invariant: */			\
				/* value@lo >= v > value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					x = vals[indir[mid] - offset];	\
					if (TYPE##_LT(x, v))		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			} else {					\
				x = vals[indir[lo] - offset];		\
				if (TYPE##_LE(x, v))			\
					return last == 0 || TYPE##_EQ(x, v) ? lo : BUN_NONE; \
				x = vals[indir[hi] - offset];		\
				if (TYPE##_GT(x, v))			\
					return last == 0 ? hi + 1 : BUN_NONE; \
									\
				/* loop invariant: */			\
				/* value@lo > v >= value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					x = vals[indir[mid] - offset];	\
					if (TYPE##_LE(x, v))		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			}						\
		} else {						\
			if (last  > 0) {				\
				x = vals[lo];				\
				if (TYPE##_LT(x, v))			\
					return lo;			\
				x = vals[hi];				\
				if (TYPE##_GE(x, v))			\
					return hi + 1;			\
									\
				/* loop invariant: */			\
				/* value@lo >= v > value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					x = vals[mid];			\
					if (TYPE##_LT(x, v))		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			} else {					\
				x = vals[lo];				\
				if (TYPE##_LE(x, v))			\
					return last == 0 || TYPE##_EQ(x, v) ? lo : BUN_NONE; \
				x = vals[hi];				\
				if (TYPE##_GT(x, v))			\
					return last == 0 ? hi + 1 : BUN_NONE; \
									\
				/* loop invariant: */			\
				/* value@lo > v >= value@hi */		\
				while (hi - lo > 1) {			\
					mid = (hi + lo) / 2;		\
					x = vals[mid];			\
					if (TYPE##_LE(x, v))		\
						hi = mid;		\
					else				\
						lo = mid;		\
				}					\
			}						\
		}							\
	}								\
	return last >= 0 || (x = (indir ? vals[indir[hi] - offset] : vals[hi]), TYPE##_EQ(x, v)) ? hi : BUN_NONE; \
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
		if (is_oid_nil(*(oid*)v) ||
		    *(oid*)v < b->tseqbase ||
		    *(oid*)v >= b->tseqbase + BATcount(b))
			return BUN_NONE;
		return *(oid*)v - b->tseqbase;
	}
	if (b->ttype == TYPE_void) {
		assert(is_oid_nil(b->tseqbase));
		if (is_oid_nil(*(const oid *) v))
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
		if (is_oid_nil(*(oid*)v) || *(oid*)v <= b->tseqbase)
			return 0;
		if (*(oid*)v >= b->tseqbase + BATcount(b))
			return BATcount(b);
		return *(oid*)v - b->tseqbase;
	}
	if (b->ttype == TYPE_void) {
		assert(is_oid_nil(b->tseqbase));
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
		if (is_oid_nil(*(oid*)v) || *(oid*)v <= b->tseqbase)
			return 0;
		if (*(oid*)v >= b->tseqbase + BATcount(b))
			return BATcount(b);
		return *(oid*)v - b->tseqbase;
	}
	if (b->ttype == TYPE_void) {
		assert(is_oid_nil(b->tseqbase));
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
