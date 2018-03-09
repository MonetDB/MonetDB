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
#include <math.h>

/* grouped aggregates
 *
 * The following functions take two to four input BATs and produce a
 * single output BAT.
 *
 * The input BATs are
 * - b, a dense-headed BAT with the values to work on in the tail;
 * - g, a dense-headed BAT, aligned with b, with group ids (OID) in
 *   the tail;
 * - e, optional but recommended, a dense-headed BAT with the list of
 *   group ids in the head(!) (the tail is completely ignored);
 * - s, optional, a dense-headed bat with a list of candidate ids in
 *   the tail.
 *
 * The tail values of s refer to the head of b and g.  Only entries at
 * the specified ids are taken into account for the grouped
 * aggregates.  All other values are ignored.  s is compatible with
 * the result of BATselect().
 *
 * If e is not specified, we need to do an extra scan over g to find
 * out the range of the group ids that are used.  e is defined in such
 * a way that it can be either the extents or the histo result from
 * BATgroups().
 *
 * All functions calculate grouped aggregates.  There are as many
 * groups as there are entries in e.  If e is not specified, the
 * number of groups is equal to the difference between the maximum and
 * minimum values in g.
 *
 * If a group is empty, the result for that group is nil.
 *
 * If there is overflow during the calculation of an aggregate, the
 * whole operation fails if abort_on_error is set to non-zero,
 * otherwise the result of the group in which the overflow occurred is
 * nil.
 *
 * If skip_nils is non-zero, a nil value in b is ignored, otherwise a
 * nil in b results in a nil result for the group.
 */

/* helper function
 *
 * This function finds the minimum and maximum group id (and the
 * number of groups) and initializes the variables for candidates
 * selection.
 */
const char *
BATgroupaggrinit(BAT *b, BAT *g, BAT *e, BAT *s,
		 /* outputs: */
		 oid *minp, oid *maxp, BUN *ngrpp, BUN *startp, BUN *endp,
		 const oid **candp, const oid **candendp)
{
	oid min, max;
	BUN i, ngrp;
	const oid *restrict gids;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;

	if (b == NULL)
		return "b must exist";
	if (g) {
		if (BATcount(b) != BATcount(g) ||
		    (BATcount(b) != 0 && b->hseqbase != g->hseqbase))
			return "b and g must be aligned";
		assert(BATttype(g) == TYPE_oid);
	}
	if (g == NULL) {
		min = 0;
		max = 0;
		ngrp = 1;
	} else if (e == NULL) {
		/* we need to find out the min and max of g */
		PROPrec *prop;

		prop = BATgetprop(g, GDK_MAX_VALUE);
		if (prop) {
			min = 0; /* just assume it starts at 0 */
			max = prop->v.val.oval;
		} else {
			min = oid_nil;	/* note that oid_nil > 0! (unsigned) */
			max = 0;
			if (BATtdense(g)) {
				min = g->tseqbase;
				max = g->tseqbase + BATcount(g) - 1;
			} else if (g->tsorted) {
				gids = (const oid *) Tloc(g, 0);
				/* find first non-nil */
				for (i = 0, ngrp = BATcount(g); i < ngrp; i++, gids++) {
					if (!is_oid_nil(*gids)) {
						min = *gids;
						break;
					}
				}
				if (!is_oid_nil(min)) {
					/* found a non-nil, max must be last
					 * value (and there is one!) */
					max = * (const oid *) Tloc(g, BUNlast(g) - 1);
				}
			} else {
				/* we'll do a complete scan */
				gids = (const oid *) Tloc(g, 0);
				for (i = 0, ngrp = BATcount(g); i < ngrp; i++, gids++) {
					if (!is_oid_nil(*gids)) {
						if (*gids < min)
							min = *gids;
						if (*gids > max)
							max = *gids;
					}
				}
				/* note: max < min is possible if all groups
				 * are nil (or BATcount(g)==0) */
			}
		}
		ngrp = max < min ? 0 : max - min + 1;
	} else {
		ngrp = BATcount(e);
		min = e->hseqbase;
		max = e->hseqbase + ngrp - 1;
	}
	*minp = min;
	*maxp = max;
	*ngrpp = ngrp;

	CANDINIT(b, s, start, end, cnt, cand, candend);
	*startp = start;
	*endp = end;
	*candp = cand;
	*candendp = candend;

	return NULL;
}

/* ---------------------------------------------------------------------- */
/* sum */

static inline int
samesign(double x, double y)
{
	return (x >= 0) == (y >= 0);
}

/* Add two values, producing the sum and the remainder due to limited
 * range of floating point arithmetic.  This function depends on the
 * fact that the sum returns INFINITY in *hi of the correct sign
 * (i.e. isinf() returns TRUE) in case of overflow. */
static inline void
twosum(volatile double *hi, volatile double *lo, double x, double y)
{
	volatile double yr;

	assert(fabs(x) >= fabs(y));

	*hi = x + y;
	yr = *hi - x;
	*lo = y - yr;
}

static inline void
exchange(double *x, double *y)
{
	double t = *x;
	*x = *y;
	*y = t;
}

/* this function was adapted from https://bugs.python.org/file10357/msum4.py */
static BUN
dofsum(const void *restrict values, oid seqb, BUN start, BUN end,
       void *restrict results, BUN ngrp, int tp1, int tp2,
       const oid *restrict cand, const oid *candend, const oid *restrict gids,
       oid min, oid max, int skip_nils, int abort_on_error,
       int nil_if_empty, const char *func)
{
	struct pergroup {
		int npartials;
		int maxpartials;
		int valseen;
#ifdef INFINITES_ALLOWED
		float infs;
#else
		int infs;
#endif
		double *partials;
	} *pergroup;
	BUN listi;
	int parti;
	int i;
	BUN grp;
	double x, y;
	volatile double lo, hi;
	double twopow = pow((double) FLT_RADIX, (double) (DBL_MAX_EXP - 1));
	BUN nils = 0;
	volatile flt f;

	ALGODEBUG fprintf(stderr, "#%s: floating point summation\n", func);
	/* we only deal with the two floating point types */
	assert(tp1 == TYPE_flt || tp1 == TYPE_dbl);
	assert(tp2 == TYPE_flt || tp2 == TYPE_dbl);
	/* if input is dbl, then so it output */
	assert(tp2 == TYPE_flt || tp1 == TYPE_dbl);
	/* if no gids, then we have a single group */
	assert(ngrp == 1 || gids != NULL);
	if (gids == NULL || ngrp == 1) {
		min = max = 0;
		ngrp = 1;
		gids = NULL;
	}
	pergroup = GDKmalloc(ngrp * sizeof(*pergroup));
	if (pergroup == NULL)
		return BUN_NONE;
	for (grp = 0; grp < ngrp; grp++) {
		pergroup[grp].npartials = 0;
		pergroup[grp].valseen = 0;
		pergroup[grp].maxpartials = 2;
		pergroup[grp].infs = 0;
		pergroup[grp].partials = GDKmalloc(pergroup[grp].maxpartials * sizeof(double));
		if (pergroup[grp].partials == NULL) {
			while (grp > 0)
				GDKfree(pergroup[--grp].partials);
			GDKfree(pergroup);
			return BUN_NONE;
		}
	}
	for (;;) {
		if (cand) {
			if (cand >= candend)
				break;
			listi = *cand++ - seqb;
		} else {
			if (start >= end)
				break;
			listi = start++;
		}
		grp = gids ? gids[listi] : 0;
		if (grp < min || grp > max)
			continue;
		if (pergroup[grp].partials == NULL)
			continue;
		if (tp1 == TYPE_flt && !is_flt_nil(((const flt *) values)[listi]))
			x = ((const flt *) values)[listi];
		else if (tp1 == TYPE_dbl && !is_dbl_nil(((const dbl *) values)[listi]))
			x = ((const dbl *) values)[listi];
		else {
			/* it's a nil */
			if (!skip_nils) {
				if (tp2 == TYPE_flt)
					((flt *) results)[grp] = flt_nil;
				else
					((dbl *) results)[grp] = dbl_nil;
				GDKfree(pergroup[grp].partials);
				pergroup[grp].partials = NULL;
				if (++nils == ngrp)
					break;
			}
			continue;
		}
		pergroup[grp].valseen = 1;
#ifdef INFINITES_ALLOWED
		if (isinf(x)) {
			pergroup[grp].infs += x;
			continue;
		}
#endif
		i = 0;
		for (parti = 0; parti < pergroup[grp].npartials; parti++) {
			y = pergroup[grp].partials[parti];
			if (fabs(x) < fabs(y))
				exchange(&x, &y);
			twosum(&hi, &lo, x, y);
			if (isinf(hi)) {
				int sign = hi > 0 ? 1 : -1;
				hi = x - twopow * sign;
				x = hi - twopow * sign;
				pergroup[grp].infs += sign;
				if (fabs(x) < fabs(y))
					exchange(&x, &y);
				twosum(&hi, &lo, x, y);
			}
			if (lo != 0)
				pergroup[grp].partials[i++] = lo;
			x = hi;
		}
		if (x != 0) {
			if (i == pergroup[grp].maxpartials) {
				double *temp;
				pergroup[grp].maxpartials += pergroup[grp].maxpartials;
				temp = GDKrealloc(pergroup[grp].partials, pergroup[grp].maxpartials * sizeof(double));
				if (temp == NULL)
					goto bailout;
				pergroup[grp].partials = temp;
			}
			pergroup[grp].partials[i++] = x;
		}
		pergroup[grp].npartials = i;
	}

	for (grp = 0; grp < ngrp; grp++) {
		if (pergroup[grp].partials == NULL)
			continue;
		if (!pergroup[grp].valseen) {
			if (tp2 == TYPE_flt)
				((flt *) results)[grp] = nil_if_empty ? flt_nil : 0;
			else
				((dbl *) results)[grp] = nil_if_empty ? dbl_nil : 0;
			nils += nil_if_empty;
			GDKfree(pergroup[grp].partials);
			pergroup[grp].partials = NULL;
			continue;
		}
#ifdef INFINITES_ALLOWED
		if (isinf(pergroup[grp].infs) || isnan(pergroup[grp].infs)) {
			if (abort_on_error) {
				goto overflow;
			}
			if (tp2 == TYPE_flt)
				((flt *) results)[grp] = flt_nil;
			else
				((dbl *) results)[grp] = dbl_nil;
			nils++;
			GDKfree(pergroup[grp].partials);
			pergroup[grp].partials = NULL;
			continue;
		}
#endif

		if ((pergroup[grp].infs == 1 || pergroup[grp].infs == -1) &&
		    pergroup[grp].npartials > 0 &&
		    !samesign(pergroup[grp].infs, pergroup[grp].partials[pergroup[grp].npartials - 1])) {
			twosum(&hi, &lo, pergroup[grp].infs * twopow, pergroup[grp].partials[pergroup[grp].npartials - 1] / 2);
			if (isinf(2 * hi)) {
				y = 2 * lo;
				x = hi + y;
				x -= hi;
				if (x == y &&
				    pergroup[grp].npartials > 1 &&
				    samesign(lo, pergroup[grp].partials[pergroup[grp].npartials - 2])) {
					GDKfree(pergroup[grp].partials);
					pergroup[grp].partials = NULL;
					x = 2 * (hi + y);
					if (tp2 == TYPE_flt) {
						f = (flt) x;
						if (isinf(f) ||
						    isnan(f) ||
						    is_flt_nil(f)) {
							if (abort_on_error)
								goto overflow;
							((flt *) results)[grp] = flt_nil;
							nils++;
						} else
							((flt *) results)[grp] = f;
					} else if (is_dbl_nil(x)) {
						if (abort_on_error)
							goto overflow;
						((dbl *) results)[grp] = dbl_nil;
						nils++;
					} else
						((dbl *) results)[grp] = x;
					continue;
				}
			} else {
				if (lo) {
					if (pergroup[grp].npartials == pergroup[grp].maxpartials) {
						double *temp;
						/* we need space for one more */
						pergroup[grp].maxpartials++;
						temp = GDKrealloc(pergroup[grp].partials, pergroup[grp].maxpartials * sizeof(double));
						if (temp == NULL)
							goto bailout;
						pergroup[grp].partials = temp;
					}
					pergroup[grp].partials[pergroup[grp].npartials - 1] = 2 * lo;
					pergroup[grp].partials[pergroup[grp].npartials++] = 2 * hi;
				} else {
					pergroup[grp].partials[pergroup[grp].npartials - 1] = 2 * hi;
				}
				pergroup[grp].infs = 0;
			}
		}

		if (pergroup[grp].infs != 0)
			goto overflow;

		if (pergroup[grp].npartials == 0) {
			GDKfree(pergroup[grp].partials);
			pergroup[grp].partials = NULL;
			if (tp2 == TYPE_flt)
				((flt *) results)[grp] = 0;
			else
				((dbl *) results)[grp] = 0;
			continue;
		}

		/* accumulate into hi */
		hi = pergroup[grp].partials[--pergroup[grp].npartials];
		while (pergroup[grp].npartials > 0) {
			twosum(&hi, &lo, hi, pergroup[grp].partials[--pergroup[grp].npartials]);
			if (lo) {
				pergroup[grp].partials[pergroup[grp].npartials++] = lo;
				break;
			}
		}

		if (pergroup[grp].npartials >= 2 &&
		    samesign(pergroup[grp].partials[pergroup[grp].npartials - 1], pergroup[grp].partials[pergroup[grp].npartials - 2]) &&
		    hi + 2 * pergroup[grp].partials[pergroup[grp].npartials - 1] - hi == 2 * pergroup[grp].partials[pergroup[grp].npartials - 1]) {
			hi += 2 * pergroup[grp].partials[pergroup[grp].npartials - 1];
			pergroup[grp].partials[pergroup[grp].npartials - 1] = -pergroup[grp].partials[pergroup[grp].npartials - 1];
		}

		GDKfree(pergroup[grp].partials);
		pergroup[grp].partials = NULL;
		if (tp2 == TYPE_flt) {
			f = (flt) hi;
			if (isinf(f) || isnan(f) || is_flt_nil(f)) {
				if (abort_on_error)
					goto overflow;
				((flt *) results)[grp] = flt_nil;
				nils++;
			} else
				((flt *) results)[grp] = f;
		} else if (is_dbl_nil(hi)) {
			if (abort_on_error)
				goto overflow;
			((dbl *) results)[grp] = dbl_nil;
			nils++;
		} else
			((dbl *) results)[grp] = hi;
	}
	GDKfree(pergroup);
	return nils;

  overflow:
	GDKerror("22003!overflow in calculation.\n");
  bailout:
	for (grp = 0; grp < ngrp; grp++)
		GDKfree(pergroup[grp].partials);
	GDKfree(pergroup);
	return BUN_NONE;
}

#define AGGR_SUM(TYPE1, TYPE2)						\
	do {								\
		TYPE1 x;						\
		const TYPE1 *restrict vals = (const TYPE1 *) values;	\
		if (ngrp == 1 && cand == NULL) {			\
			/* single group, no candidate list */		\
			TYPE2 sum;					\
			ALGODEBUG fprintf(stderr,			\
					  "#%s: no candidates, no groups; " \
					  "start " BUNFMT ", end " BUNFMT \
					  ", nonil = %d\n",		\
					  func, start, end, nonil);	\
			sum = 0;					\
			if (nonil) {					\
				*seen = start < end;			\
				for (i = start; i < end && nils == 0; i++) { \
					x = vals[i];			\
					ADD_WITH_CHECK(TYPE1, x,	\
						       TYPE2, sum,	\
						       TYPE2, sum,	\
						       GDK_##TYPE2##_max, \
						       goto overflow);	\
				}					\
			} else {					\
				int seenval = 0;			\
				for (i = start; i < end && nils == 0; i++) { \
					x = vals[i];			\
					if (is_##TYPE1##_nil(x)) {	\
						if (!skip_nils) {	\
							sum = TYPE2##_nil; \
							nils = 1;	\
						}			\
					} else {			\
						ADD_WITH_CHECK(TYPE1, x, \
							       TYPE2, sum, \
							       TYPE2, sum, \
							       GDK_##TYPE2##_max, \
							       goto overflow); \
						seenval = 1;		\
					}				\
				}					\
				*seen = seenval;			\
			}						\
			if (*seen)					\
				*sums = sum;				\
		} else if (ngrp == 1) {					\
			/* single group, with candidate list */		\
			TYPE2 sum;					\
			int seenval = 0;				\
			ALGODEBUG fprintf(stderr,			\
					  "#%s: with candidates, no groups; " \
					  "start " BUNFMT ", end " BUNFMT \
					  "\n",				\
					  func, start, end);		\
			sum = 0;					\
			while (cand < candend && nils == 0) {		\
				i = *cand++ - seqb;			\
				if (i >= end)				\
					break;				\
				x = vals[i];				\
				if (is_##TYPE1##_nil(x)) {		\
					if (!skip_nils) {		\
						sum = TYPE2##_nil;	\
						nils = 1;		\
					}				\
				} else {				\
					ADD_WITH_CHECK(TYPE1, x,	\
						       TYPE2, sum,	\
						       TYPE2, sum,	\
						       GDK_##TYPE2##_max, \
						       goto overflow);	\
					seenval = 1;			\
				}					\
			}						\
			if (seenval)					\
				*sums = sum;				\
		} else if (cand == NULL) {				\
			/* multiple groups, no candidate list */	\
			ALGODEBUG fprintf(stderr,			\
					  "#%s: no candidates, with groups; " \
					  "start " BUNFMT ", end " BUNFMT \
					  "\n",				\
					  func, start, end);		\
			for (i = start; i < end; i++) {			\
				if (gids == NULL ||			\
				    (gids[i] >= min && gids[i] <= max)) { \
					gid = gids ? gids[i] - min : (oid) i; \
					x = vals[i];			\
					if (is_##TYPE1##_nil(x)) {	\
						if (!skip_nils) {	\
							sums[gid] = TYPE2##_nil; \
							nils++;		\
						}			\
					} else {			\
						if (nil_if_empty &&	\
						    !(seen[gid >> 5] & (1U << (gid & 0x1F)))) { \
							seen[gid >> 5] |= 1U << (gid & 0x1F); \
							sums[gid] = 0;	\
						}			\
						if (!is_##TYPE2##_nil(sums[gid])) { \
							ADD_WITH_CHECK(	\
								TYPE1,	\
								x,	\
								TYPE2,	\
								sums[gid], \
								TYPE2,	\
								sums[gid], \
								GDK_##TYPE2##_max, \
								goto overflow); \
						}			\
					}				\
				}					\
			}						\
		} else {						\
			/* multiple groups, with candidate list */	\
			ALGODEBUG fprintf(stderr,			\
					  "#%s: with candidates, with " \
					  "groups; start " BUNFMT ", "	\
					  "end " BUNFMT "\n",		\
					  func, start, end);		\
			while (cand < candend) {			\
				i = *cand++ - seqb;			\
				if (i >= end)				\
					break;				\
				if (gids == NULL ||			\
				    (gids[i] >= min && gids[i] <= max)) { \
					gid = gids ? gids[i] - min : (oid) i; \
					x = vals[i];			\
					if (is_##TYPE1##_nil(x)) {	\
						if (!skip_nils) {	\
							sums[gid] = TYPE2##_nil; \
							nils++;		\
						}			\
					} else {			\
						if (nil_if_empty &&	\
						    !(seen[gid >> 5] & (1U << (gid & 0x1F)))) { \
							seen[gid >> 5] |= 1U << (gid & 0x1F); \
							sums[gid] = 0;	\
						}			\
						if (!is_##TYPE2##_nil(sums[gid])) { \
							ADD_WITH_CHECK(	\
								TYPE1,	\
								x,	\
								TYPE2,	\
								sums[gid], \
								TYPE2,	\
								sums[gid], \
								GDK_##TYPE2##_max, \
								goto overflow); \
						}			\
					}				\
				}					\
			}						\
		}							\
	} while (0)

static BUN
dosum(const void *restrict values, int nonil, oid seqb, BUN start, BUN end,
      void *restrict results, BUN ngrp, int tp1, int tp2,
      const oid *restrict cand, const oid *candend, const oid *restrict gids,
      oid min, oid max, int skip_nils, int abort_on_error,
      int nil_if_empty, const char *func)
{
	BUN nils = 0;
	BUN i;
	oid gid;
	unsigned int *restrict seen = NULL; /* bitmask for groups that we've seen */

	switch (tp2) {
	case TYPE_flt:
		if (tp1 != TYPE_flt)
			goto unsupported;
		/* fall through */
	case TYPE_dbl:
		if (tp1 != TYPE_flt && tp1 != TYPE_dbl)
			goto unsupported;
		return dofsum(values, seqb, start, end, results, ngrp, tp1, tp2, cand, candend, gids, min, max, skip_nils, abort_on_error, nil_if_empty, func);
	}

	/* allocate bitmap for seen group ids */
	seen = GDKzalloc(((ngrp + 31) / 32) * sizeof(int));
	if (seen == NULL) {
		GDKerror("%s: cannot allocate enough memory\n", func);
		return BUN_NONE;
	}

	switch (tp2) {
	case TYPE_bte: {
		bte *restrict sums = (bte *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_SUM(bte, bte);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_sht: {
		sht *restrict sums = (sht *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_SUM(bte, sht);
			break;
		case TYPE_sht:
			AGGR_SUM(sht, sht);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_int: {
		int *restrict sums = (int *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_SUM(bte, int);
			break;
		case TYPE_sht:
			AGGR_SUM(sht, int);
			break;
		case TYPE_int:
			AGGR_SUM(int, int);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_lng: {
		lng *restrict sums = (lng *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_SUM(bte, lng);
			break;
		case TYPE_sht:
			AGGR_SUM(sht, lng);
			break;
		case TYPE_int:
			AGGR_SUM(int, lng);
			break;
		case TYPE_lng:
			AGGR_SUM(lng, lng);
			break;
		default:
			goto unsupported;
		}
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		hge *sums = (hge *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_SUM(bte, hge);
			break;
		case TYPE_sht:
			AGGR_SUM(sht, hge);
			break;
		case TYPE_int:
			AGGR_SUM(int, hge);
			break;
		case TYPE_lng:
			AGGR_SUM(lng, hge);
			break;
		case TYPE_hge:
			AGGR_SUM(hge, hge);
			break;
		default:
			goto unsupported;
		}
		break;
	}
#endif
	default:
		goto unsupported;
	}

	if (nils == 0 && nil_if_empty) {
		/* figure out whether there were any empty groups
		 * (that result in a nil value) */
		if (ngrp & 0x1F) {
			/* fill last slot */
			seen[ngrp >> 5] |= ~0U << (ngrp & 0x1F);
		}
		for (i = 0, ngrp = (ngrp + 31) / 32; i < ngrp; i++) {
			if (seen[i] != ~0U) {
				nils = 1;
				break;
			}
		}
	}
	GDKfree(seen);

	return nils;

  unsupported:
	GDKfree(seen);
	GDKerror("%s: type combination (sum(%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;

  overflow:
	GDKfree(seen);
	GDKerror("22003!overflow in calculation.\n");
	return BUN_NONE;
}

/* calculate group sums with optional candidates list */
BAT *
BATgroupsum(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *restrict gids;
	oid min, max;
	BUN ngrp;
	BUN nils;
	BAT *bn;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cand, &candend)) != NULL) {
		GDKerror("BATgroupsum: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupsum: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no sums, so return bat aligned with g with
		 * nil in the tail */
		return BATconstant(ngrp == 0 ? 0 : min, tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->tnonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		return BATconvert(b, s, tp, abort_on_error);
	}

	bn = BATconstant(min, tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	if (bn == NULL) {
		return NULL;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, start);

	nils = dosum(Tloc(b, 0), b->tnonil, b->hseqbase, start, end,
		     Tloc(bn, 0), ngrp, b->ttype, tp,
		     cand, candend, gids, min, max,
		     skip_nils, abort_on_error, 1, "BATgroupsum");

	if (nils < BUN_NONE) {
		BATsetcount(bn, ngrp);
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->tnil = nils != 0;
		bn->tnonil = nils == 0;
	} else {
		BBPunfix(bn->batCacheid);
		bn = NULL;
	}

	return bn;
}

gdk_return
BATsum(void *res, int tp, BAT *b, BAT *s, int skip_nils, int abort_on_error, int nil_if_empty)
{
	oid min, max;
	BUN ngrp;
	BUN nils;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	if ((err = BATgroupaggrinit(b, NULL, NULL, s, &min, &max, &ngrp,
				    &start, &end, &cand, &candend)) != NULL) {
		GDKerror("BATsum: %s\n", err);
		return GDK_FAIL;
	}
	switch (tp) {
	case TYPE_bte:
		* (bte *) res = nil_if_empty ? bte_nil : 0;
		break;
	case TYPE_sht:
		* (sht *) res = nil_if_empty ? sht_nil : 0;
		break;
	case TYPE_int:
		* (int *) res = nil_if_empty ? int_nil : 0;
		break;
	case TYPE_lng:
		* (lng *) res = nil_if_empty ? lng_nil : 0;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		* (hge *) res = nil_if_empty ? hge_nil : 0;
		break;
#endif
	case TYPE_flt:
	case TYPE_dbl:
		switch (b->ttype) {
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_int:
		case TYPE_lng:
#ifdef HAVE_HGE
		case TYPE_hge:
#endif
		{
			/* special case for summing integer types into
			 * a floating point: We calculate the average
			 * (which is done exactly), and multiply the
			 * result by the count to get the sum.  Note
			 * that if we just summed into a floating
			 * point number, we could loose too much
			 * accuracy, and if we summed into lng first,
			 * we could get unnecessary overflow. */
			dbl avg;
			BUN cnt;

			if (BATcalcavg(b, s, &avg, &cnt) != GDK_SUCCEED)
				return GDK_FAIL;
			if (cnt == 0) {
				avg = nil_if_empty ? dbl_nil : 0;
			}
			if (cnt < BATcount(b) && !skip_nils) {
				/* there were nils */
				avg = dbl_nil;
			}
			if (tp == TYPE_flt) {
				if (is_dbl_nil(avg))
					*(flt *) res = flt_nil;
				else if (cnt > 0 &&
					 GDK_flt_max / cnt < fabs(avg)) {
					if (abort_on_error) {
						GDKerror("22003!overflow in calculation.\n");
						return GDK_FAIL;
					}
					*(flt *) res = flt_nil;
				} else {
					*(flt *) res = (flt) avg * cnt;
				}
			} else {
				if (is_dbl_nil(avg)) {
					*(dbl *) res = dbl_nil;
				} else if (cnt > 0 &&
					   GDK_dbl_max / cnt < fabs(avg)) {
					if (abort_on_error) {
						GDKerror("22003!overflow in calculation.\n");
						return GDK_FAIL;
					}
					*(dbl *) res = dbl_nil;
				} else {
					*(dbl *) res = avg * cnt;
				}
			}
			return GDK_SUCCEED;
		}
		default:
			break;
		}
		if (b->ttype == TYPE_dbl)
			* (dbl *) res = nil_if_empty ? dbl_nil : 0;
		else
			* (flt *) res = nil_if_empty ? flt_nil : 0;
		break;
	default:
		GDKerror("BATsum: type combination (sum(%s)->%s) not supported.\n",
			 ATOMname(b->ttype), ATOMname(tp));
		return GDK_FAIL;
	}
	if (BATcount(b) == 0)
		return GDK_SUCCEED;
	nils = dosum(Tloc(b, 0), b->tnonil, b->hseqbase, start, end,
		     res, 1, b->ttype, tp, cand, candend, &min, min, max,
		     skip_nils, abort_on_error, nil_if_empty, "BATsum");
	return nils < BUN_NONE ? GDK_SUCCEED : GDK_FAIL;
}

/* ---------------------------------------------------------------------- */
/* product */

#define AGGR_PROD(TYPE1, TYPE2, TYPE3)					\
	do {								\
		const TYPE1 *restrict vals = (const TYPE1 *) values;	\
		assert(gidincr == 0 || gidincr == 1);			\
		gid = 0;	/* doesn't change if gidincr == 0 */	\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - seqb;			\
				if (i >= end)				\
					break;				\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			if (gids == NULL || gidincr == 0 ||		\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gidincr) {				\
					if (gids)			\
						gid = gids[i] - min;	\
					else				\
						gid = (oid) i;		\
				}					\
				if (is_##TYPE1##_nil(vals[i])) {	\
					if (!skip_nils) {		\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else {				\
					if (nil_if_empty &&		\
					    !(seen[gid >> 5] & (1U << (gid & 0x1F)))) { \
						seen[gid >> 5] |= 1U << (gid & 0x1F); \
						prods[gid] = 1;		\
					}				\
					if (!is_##TYPE2##_nil(prods[gid])) { \
						MUL4_WITH_CHECK(	\
							TYPE1, vals[i],	\
							TYPE2, prods[gid], \
							TYPE2, prods[gid], \
							GDK_##TYPE2##_max, \
							TYPE3,		\
							goto overflow);	\
					}				\
				}					\
			}						\
		}							\
	} while (0)

#ifdef HAVE_HGE
#define AGGR_PROD_HGE(TYPE)						\
	do {								\
		const TYPE *vals = (const TYPE *) values;		\
		assert(gidincr == 0 || gidincr == 1);			\
		gid = 0;	/* doesn't change if gidincr == 0 */	\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - seqb;			\
				if (i >= end)				\
					break;				\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			if (gids == NULL || gidincr == 0 ||		\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gidincr) {				\
					if (gids)			\
						gid = gids[i] - min;	\
					else				\
						gid = (oid) i;		\
				}					\
				if (nil_if_empty &&			\
				    !(seen[gid >> 5] & (1U << (gid & 0x1F)))) { \
					seen[gid >> 5] |= 1U << (gid & 0x1F); \
					prods[gid] = 1;			\
				}					\
				if (is_##TYPE##_nil(vals[i])) {		\
					if (!skip_nils) {		\
						prods[gid] = hge_nil;	\
						nils++;			\
					}				\
				} else if (!is_hge_nil(prods[gid])) {	\
					HGEMUL_CHECK(TYPE, vals[i],	\
						     hge, prods[gid],	\
						     prods[gid],	\
						     GDK_hge_max,	\
						     goto overflow);	\
				}					\
			}						\
		}							\
	} while (0)
#else
#define AGGR_PROD_LNG(TYPE)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) values;	\
		assert(gidincr == 0 || gidincr == 1);			\
		gid = 0;	/* doesn't change if gidincr == 0 */	\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - seqb;			\
				if (i >= end)				\
					break;				\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			if (gids == NULL || gidincr == 0 ||		\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gidincr) {				\
					if (gids)			\
						gid = gids[i] - min;	\
					else				\
						gid = (oid) i;		\
				}					\
				if (is_##TYPE##_nil(vals[i])) {		\
					if (!skip_nils) {		\
						prods[gid] = lng_nil;	\
						nils++;			\
					}				\
				} else {				\
					if (nil_if_empty &&		\
					    !(seen[gid >> 5] & (1U << (gid & 0x1F)))) { \
						seen[gid >> 5] |= 1U << (gid & 0x1F); \
						prods[gid] = 1;		\
					}				\
					if (!is_lng_nil(prods[gid])) {	\
						LNGMUL_CHECK(		\
							TYPE, vals[i],	\
							lng, prods[gid], \
							prods[gid],	\
							GDK_lng_max,	\
							goto overflow); \
					}				\
				}					\
			}						\
		}							\
	} while (0)
#endif

#define AGGR_PROD_FLOAT(TYPE1, TYPE2)					\
	do {								\
		const TYPE1 *restrict vals = (const TYPE1 *) values;	\
		assert(gidincr == 0 || gidincr == 1);			\
		gid = 0;	/* doesn't change if gidincr == 0 */	\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - seqb;			\
				if (i >= end)				\
					break;				\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			if (gids == NULL || gidincr == 0 ||		\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gidincr) {				\
					if (gids)			\
						gid = gids[i] - min;	\
					else				\
						gid = (oid) i;		\
				}					\
				if (is_##TYPE1##_nil(vals[i])) {	\
					if (!skip_nils) {		\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else {				\
					if (nil_if_empty &&		\
					    !(seen[gid >> 5] & (1U << (gid & 0x1F)))) { \
						seen[gid >> 5] |= 1U << (gid & 0x1F); \
						prods[gid] = 1;		\
					}				\
					if (!is_##TYPE2##_nil(prods[gid])) { \
						if (ABSOLUTE(vals[i]) > 1 && \
						    GDK_##TYPE2##_max / ABSOLUTE(vals[i]) < ABSOLUTE(prods[gid])) { \
							if (abort_on_error) \
								goto overflow; \
							prods[gid] = TYPE2##_nil; \
								nils++;	\
						} else {		\
							prods[gid] *= vals[i]; \
						}			\
					}				\
				}					\
			}						\
		}							\
	} while (0)

static BUN
doprod(const void *restrict values, oid seqb, BUN start, BUN end, void *restrict results,
       BUN ngrp, int tp1, int tp2, const oid *restrict cand, const oid *candend,
       const oid *restrict gids, int gidincr, oid min, oid max,
       int skip_nils, int abort_on_error, int nil_if_empty, const char *func)
{
	BUN nils = 0;
	BUN i;
	oid gid;
	unsigned int *restrict seen; /* bitmask for groups that we've seen */

	/* allocate bitmap for seen group ids */
	seen = GDKzalloc(((ngrp + 31) / 32) * sizeof(int));
	if (seen == NULL) {
		GDKerror("%s: cannot allocate enough memory\n", func);
		return BUN_NONE;
	}

	switch (tp2) {
	case TYPE_bte: {
		bte *restrict prods = (bte *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_PROD(bte, bte, sht);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_sht: {
		sht *restrict prods = (sht *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_PROD(bte, sht, int);
			break;
		case TYPE_sht:
			AGGR_PROD(sht, sht, int);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_int: {
		int *restrict prods = (int *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_PROD(bte, int, lng);
			break;
		case TYPE_sht:
			AGGR_PROD(sht, int, lng);
			break;
		case TYPE_int:
			AGGR_PROD(int, int, lng);
			break;
		default:
			goto unsupported;
		}
		break;
	}
#ifdef HAVE_HGE
	case TYPE_lng: {
		lng *prods = (lng *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_PROD(bte, lng, hge);
			break;
		case TYPE_sht:
			AGGR_PROD(sht, lng, hge);
			break;
		case TYPE_int:
			AGGR_PROD(int, lng, hge);
			break;
		case TYPE_lng:
			AGGR_PROD(lng, lng, hge);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_hge: {
		hge *prods = (hge *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_PROD_HGE(bte);
			break;
		case TYPE_sht:
			AGGR_PROD_HGE(sht);
			break;
		case TYPE_int:
			AGGR_PROD_HGE(int);
			break;
		case TYPE_lng:
			AGGR_PROD_HGE(lng);
			break;
		case TYPE_hge:
			AGGR_PROD_HGE(hge);
			break;
		default:
			goto unsupported;
		}
		break;
	}
#else
	case TYPE_lng: {
		lng *restrict prods = (lng *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_PROD_LNG(bte);
			break;
		case TYPE_sht:
			AGGR_PROD_LNG(sht);
			break;
		case TYPE_int:
			AGGR_PROD_LNG(int);
			break;
		case TYPE_lng:
			AGGR_PROD_LNG(lng);
			break;
		default:
			goto unsupported;
		}
		break;
	}
#endif
	case TYPE_flt: {
		flt *restrict prods = (flt *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_PROD_FLOAT(bte, flt);
			break;
		case TYPE_sht:
			AGGR_PROD_FLOAT(sht, flt);
			break;
		case TYPE_int:
			AGGR_PROD_FLOAT(int, flt);
			break;
		case TYPE_lng:
			AGGR_PROD_FLOAT(lng, flt);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			AGGR_PROD_FLOAT(hge, flt);
			break;
#endif
		case TYPE_flt:
			AGGR_PROD_FLOAT(flt, flt);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_dbl: {
		dbl *restrict prods = (dbl *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_PROD_FLOAT(bte, dbl);
			break;
		case TYPE_sht:
			AGGR_PROD_FLOAT(sht, dbl);
			break;
		case TYPE_int:
			AGGR_PROD_FLOAT(int, dbl);
			break;
		case TYPE_lng:
			AGGR_PROD_FLOAT(lng, dbl);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			AGGR_PROD_FLOAT(hge, dbl);
			break;
#endif
		case TYPE_flt:
			AGGR_PROD_FLOAT(flt, dbl);
			break;
		case TYPE_dbl:
			AGGR_PROD_FLOAT(dbl, dbl);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	default:
		goto unsupported;
	}

	if (nils == 0 && nil_if_empty) {
		/* figure out whether there were any empty groups
		 * (that result in a nil value) */
		if (ngrp & 0x1F) {
			/* fill last slot */
			seen[ngrp >> 5] |= ~0U << (ngrp & 0x1F);
		}
		for (i = 0, ngrp = (ngrp + 31) / 32; i < ngrp; i++) {
			if (seen[i] != ~0U) {
				nils = 1;
				break;
			}
		}
	}
	GDKfree(seen);

	return nils;

  unsupported:
	GDKfree(seen);
	GDKerror("%s: type combination (mul(%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;

  overflow:
	GDKfree(seen);
	GDKerror("22003!overflow in calculation.\n");
	return BUN_NONE;
}

/* calculate group products with optional candidates list */
BAT *
BATgroupprod(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *restrict gids;
	oid min, max;
	BUN ngrp;
	BUN nils;
	BAT *bn;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cand, &candend)) != NULL) {
		GDKerror("BATgroupprod: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupprod: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no products, so return bat aligned with g
		 * with nil in the tail */
		return BATconstant(ngrp == 0 ? 0 : min, tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->tnonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		return BATconvert(b, s, tp, abort_on_error);
	}

	bn = BATconstant(min, tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	if (bn == NULL) {
		return NULL;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, start);

	nils = doprod(Tloc(b, 0), b->hseqbase, start, end,
		      Tloc(bn, 0), ngrp, b->ttype, tp,
		      cand, candend, gids, 1, min, max,
		      skip_nils, abort_on_error, 1, "BATgroupprod");

	if (nils < BUN_NONE) {
		BATsetcount(bn, ngrp);
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->tnil = nils != 0;
		bn->tnonil = nils == 0;
	} else {
		BBPunfix(bn->batCacheid);
		bn = NULL;
	}

	return bn;
}

gdk_return
BATprod(void *res, int tp, BAT *b, BAT *s, int skip_nils, int abort_on_error, int nil_if_empty)
{
	oid min, max;
	BUN ngrp;
	BUN nils;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	if ((err = BATgroupaggrinit(b, NULL, NULL, s, &min, &max, &ngrp,
				    &start, &end, &cand, &candend)) != NULL) {
		GDKerror("BATprod: %s\n", err);
		return GDK_FAIL;
	}
	switch (tp) {
	case TYPE_bte:
		* (bte *) res = nil_if_empty ? bte_nil : (bte) 1;
		break;
	case TYPE_sht:
		* (sht *) res = nil_if_empty ? sht_nil : (sht) 1;
		break;
	case TYPE_int:
		* (int *) res = nil_if_empty ? int_nil : (int) 1;
		break;
	case TYPE_lng:
		* (lng *) res = nil_if_empty ? lng_nil : (lng) 1;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		* (hge *) res = nil_if_empty ? hge_nil : (hge) 1;
		break;
#endif
	case TYPE_flt:
		* (flt *) res = nil_if_empty ? flt_nil : (flt) 1;
		break;
	case TYPE_dbl:
		* (dbl *) res = nil_if_empty ? dbl_nil : (dbl) 1;
		break;
	default:
		GDKerror("BATprod: type combination (prod(%s)->%s) not supported.\n",
			 ATOMname(b->ttype), ATOMname(tp));
		return GDK_FAIL;
	}
	if (BATcount(b) == 0)
		return GDK_SUCCEED;
	nils = doprod(Tloc(b, 0), b->hseqbase, start, end, res, 1,
		      b->ttype, tp, cand, candend, &min, 0, min, max,
		      skip_nils, abort_on_error, nil_if_empty, "BATprod");
	return nils < BUN_NONE ? GDK_SUCCEED : GDK_FAIL;
}

/* ---------------------------------------------------------------------- */
/* average */

#define AVERAGE_ITER(TYPE, x, a, r, n)					\
	do {								\
		TYPE an, xn, z1;					\
		BUN z2;							\
		(n)++;							\
		/* calculate z1 = (x - a) / n, rounded down (towards */	\
		/* negative infinity), and calculate z2 = remainder */	\
		/* of the division (i.e. 0 <= z2 < n); do this */	\
		/* without causing overflow */				\
		an = (TYPE) ((a) / (SBUN) (n));				\
		xn = (TYPE) ((x) / (SBUN) (n));				\
		/* z1 will be (x - a) / n rounded towards -INF */	\
		z1 = xn - an;						\
		xn = (x) - (TYPE) (xn * (SBUN) (n));			\
		an = (a) - (TYPE) (an * (SBUN) (n));			\
		/* z2 will be remainder of above division */		\
		if (xn >= an) {						\
			z2 = (BUN) (xn - an);				\
			/* loop invariant: */				\
			/* (x - a) - z1 * n == z2 */			\
			while (z2 >= (BUN) (n)) {			\
				z2 -= (BUN) (n);			\
				z1++;					\
			}						\
		} else {						\
			z2 = (BUN) (an - xn);				\
			/* loop invariant (until we break): */		\
			/* (x - a) - z1 * n == -z2 */			\
			for (;;) {					\
				z1--;					\
				if (z2 < (BUN) (n)) {			\
					/* proper remainder */		\
					z2 = (BUN) ((n) - z2);		\
					break;				\
				}					\
				z2 -= (BUN) (n);			\
			}						\
		}							\
		(a) += z1;						\
		(r) += z2;						\
		if ((r) >= (BUN) (n)) {					\
			(r) -= (BUN) (n);				\
			(a)++;						\
		}							\
	} while (0)

#define AVERAGE_ITER_FLOAT(TYPE, x, a, n)				\
	do {								\
		(n)++;							\
		if (((a) > 0) == ((x) > 0)) {				\
			/* same sign */					\
			(a) += ((x) - (a)) / (SBUN) (n);		\
		} else {						\
			/* no overflow at the cost of an */		\
			/* extra division and slight loss of */		\
			/* precision */					\
			(a) = (a) - (a) / (SBUN) (n) + (x) / (SBUN) (n); \
		}							\
	} while (0)

#define AGGR_AVG(TYPE)							\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		TYPE *restrict avgs = GDKzalloc(ngrp * sizeof(TYPE));	\
		if (avgs == NULL)					\
			goto alloc_fail;				\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - b->hseqbase;		\
				if (i >= end)				\
					break;				\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (is_##TYPE##_nil(vals[i])) {		\
					if (!skip_nils)			\
						cnts[gid] = lng_nil;	\
				} else if (!is_lng_nil(cnts[gid])) {	\
					AVERAGE_ITER(TYPE, vals[i],	\
						     avgs[gid],		\
						     rems[gid],		\
						     cnts[gid]);	\
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] == 0 || is_lng_nil(cnts[i])) {	\
				dbls[i] = dbl_nil;			\
				cnts[i] = 0;				\
				nils++;					\
			} else {					\
				dbls[i] = avgs[i] + (dbl) rems[i] / cnts[i]; \
			}						\
		}							\
		GDKfree(avgs);						\
	} while (0)

#define AGGR_AVG_FLOAT(TYPE)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		for (i = 0; i < ngrp; i++)				\
			dbls[i] = 0;					\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - b->hseqbase;		\
				if (i >= end)				\
					break;				\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (is_##TYPE##_nil(vals[i])) {		\
					if (!skip_nils)			\
						cnts[gid] = lng_nil;	\
				} else if (!is_lng_nil(cnts[gid])) {	\
					AVERAGE_ITER_FLOAT(TYPE, vals[i], \
							   dbls[gid],	\
							   cnts[gid]);	\
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] == 0 || is_lng_nil(cnts[i])) {	\
				dbls[i] = dbl_nil;			\
				cnts[i] = 0;				\
				nils++;					\
			}						\
		}							\
	} while (0)

/* calculate group averages with optional candidates list */
gdk_return
BATgroupavg(BAT **bnp, BAT **cntsp, BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *restrict gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	BUN nils = 0;
	BUN *restrict rems = NULL;
	lng *restrict cnts = NULL;
	dbl *restrict dbls;
	BAT *bn = NULL, *cn = NULL;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_dbl);
	(void) tp;		/* compatibility (with other BATgroup*
				 * functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cand, &candend)) != NULL) {
		GDKerror("BATgroupavg: %s\n", err);
		return GDK_FAIL;
	}
	if (g == NULL) {
		GDKerror("BATgroupavg: b and g must be aligned\n");
		return GDK_FAIL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no averages, so return bat aligned with g
		 * with nil in the tail */
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &dbl_nil, ngrp, TRANSIENT);
		if (bn == NULL) {
			GDKerror("BATgroupavg: failed to create BAT\n");
			return GDK_FAIL;
		}
		if (cntsp) {
			lng zero = 0;
			if ((cn = BATconstant(ngrp == 0 ? 0 : min, TYPE_lng, &zero, ngrp, TRANSIENT)) == NULL) {
				GDKerror("BATgroupavg: failed to create BAT\n");
				BBPreclaim(bn);
				return GDK_FAIL;
			}
			*cntsp = cn;
		}
		*bnp = bn;
		return GDK_SUCCEED;
	}

	if ((!skip_nils || cntsp == NULL || b->tnonil) &&
	    (e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->tnonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		if ((bn = BATconvert(b, s, TYPE_dbl, abort_on_error)) == NULL)
			return GDK_FAIL;
		if (cntsp) {
			lng one = 1;
			if ((cn = BATconstant(ngrp == 0 ? 0 : min, TYPE_lng, &one, ngrp, TRANSIENT)) == NULL) {
				BBPreclaim(bn);
				return GDK_FAIL;
			}
			*cntsp = cn;
		}
		*bnp = bn;
		return GDK_SUCCEED;
	}

	/* allocate temporary space to do per group calculations */
	switch (b->ttype) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
		rems = GDKzalloc(ngrp * sizeof(BUN));
		if (rems == NULL)
			goto alloc_fail;
		break;
	default:
		break;
	}
	if (cntsp) {
		if ((cn = COLnew(min, TYPE_lng, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		cnts = (lng *) Tloc(cn, 0);
		memset(cnts, 0, ngrp * sizeof(lng));
	} else {
		cnts = GDKzalloc(ngrp * sizeof(lng));
		if (cnts == NULL)
			goto alloc_fail;
	}

	bn = COLnew(min, TYPE_dbl, ngrp, TRANSIENT);
	if (bn == NULL)
		goto alloc_fail;
	dbls = (dbl *) Tloc(bn, 0);

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, start);

	switch (b->ttype) {
	case TYPE_bte:
		AGGR_AVG(bte);
		break;
	case TYPE_sht:
		AGGR_AVG(sht);
		break;
	case TYPE_int:
		AGGR_AVG(int);
		break;
	case TYPE_lng:
		AGGR_AVG(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_AVG(hge);
		break;
#endif
	case TYPE_flt:
		AGGR_AVG_FLOAT(flt);
		break;
	case TYPE_dbl:
		AGGR_AVG_FLOAT(dbl);
		break;
	default:
		GDKfree(rems);
		if (cn)
			BBPreclaim(cn);
		else
			GDKfree(cnts);
		BBPunfix(bn->batCacheid);
		GDKerror("BATgroupavg: type (%s) not supported.\n",
			 ATOMname(b->ttype));
		return GDK_FAIL;
	}
	GDKfree(rems);
	if (cn == NULL)
		GDKfree(cnts);
	else {
		BATsetcount(cn, ngrp);
		cn->tkey = BATcount(cn) <= 1;
		cn->tsorted = BATcount(cn) <= 1;
		cn->trevsorted = BATcount(cn) <= 1;
		cn->tnil = 0;
		cn->tnonil = 1;
		*cntsp = cn;
	}
	BATsetcount(bn, ngrp);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	*bnp = bn;
	return GDK_SUCCEED;

  alloc_fail:
	if (bn)
		BBPunfix(bn->batCacheid);
	GDKfree(rems);
	if (cntsp) {
		BBPreclaim(*cntsp);
		*cntsp = NULL;
	} else if (cnts) {
		GDKfree(cnts);
	}
	GDKerror("BATgroupavg: cannot allocate enough memory.\n");
	return GDK_FAIL;
}

#define AVERAGE_TYPE_LNG_HGE(TYPE,lng_hge)				\
	do {								\
		TYPE x, a;						\
									\
		/* first try to calculate the sum of all values into a */ \
		/* lng/hge */						\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - b->hseqbase;		\
				if (i >= end)				\
					break;				\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			x = ((const TYPE *) src)[i];			\
			if (is_##TYPE##_nil(x))				\
				continue;				\
			ADD_WITH_CHECK(TYPE, x,				\
				       lng_hge, sum,			\
				       lng_hge, sum,			\
				       GDK_##lng_hge##_max,		\
				       goto overflow##TYPE);		\
			/* don't count value until after overflow check */ \
			n++;						\
		}							\
		/* the sum fit, so now we can calculate the average */	\
		*avg = n > 0 ? (dbl) sum / n : dbl_nil;			\
		if (0) {						\
		  overflow##TYPE:					\
			/* we get here if sum(x[0],...,x[i]) doesn't */	\
			/* fit in a lng/hge but sum(x[0],...,x[i-1]) did */ \
			/* the variable sum contains that sum */	\
			/* the rest of the calculation is done */	\
			/* according to the loop invariant described */	\
			/* in the below loop */				\
			/* note that n necessarily is > 0 (else no */	\
			/* overflow possible) */			\
			assert(n > 0);					\
			if (sum >= 0) {					\
				a = (TYPE) (sum / (lng_hge) n); /* this fits */ \
				r = (BUN) (sum % (SBUN) n);		\
			} else {					\
				sum = -sum;				\
				a = - (TYPE) (sum / (lng_hge) n); /* this fits */ \
				r = (BUN) (sum % (SBUN) n);		\
				if (r) {				\
					a--;				\
					r = n - r;			\
				}					\
			}						\
			if (cand)					\
				--cand;					\
									\
			for (;;) {					\
				/* loop invariant: */			\
				/* a + r/n == average(x[0],...,x[n]); */ \
				/* 0 <= r < n */			\
				if (cand) {				\
					if (cand == candend)		\
						break;			\
					i = *cand++ - b->hseqbase;	\
				} else {				\
					i = start++;			\
				}					\
				if (i >= end)				\
					break;				\
				x = ((const TYPE *) src)[i];		\
				if (is_##TYPE##_nil(x))			\
					continue;			\
				AVERAGE_ITER(TYPE, x, a, r, n);		\
			}						\
			*avg = a + (dbl) r / n;				\
		}							\
	} while (0)

#ifdef HAVE_HGE
#define AVERAGE_TYPE(TYPE) AVERAGE_TYPE_LNG_HGE(TYPE,hge)
#else
#define AVERAGE_TYPE(TYPE) AVERAGE_TYPE_LNG_HGE(TYPE,lng)
#endif

#define AVERAGE_FLOATTYPE(TYPE)					\
	do {							\
		double a = 0;					\
		TYPE x;						\
		for (;;) {					\
			if (cand) {				\
				if (cand == candend)		\
					break;			\
				i = *cand++ - b->hseqbase;	\
				if (i >= end)			\
					break;			\
			} else {				\
				i = start++;			\
				if (i == end)			\
					break;			\
			}					\
			x = ((const TYPE *) src)[i];		\
			if (is_##TYPE##_nil(x))			\
				continue;			\
			AVERAGE_ITER_FLOAT(TYPE, x, a, n);	\
		}						\
		*avg = n > 0 ? a : dbl_nil;			\
	} while (0)

gdk_return
BATcalcavg(BAT *b, BAT *s, dbl *avg, BUN *vals)
{
	BUN n = 0, r = 0, i = 0;
#ifdef HAVE_HGE
	hge sum = 0;
#else
	lng sum = 0;
#endif
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const void *restrict src;
	/* these two needed for ADD_WITH_CHECK macro */
	int abort_on_error = 1;
	BUN nils = 0;

	CANDINIT(b, s, start, end, cnt, cand, candend);

	src = Tloc(b, 0);

	switch (b->ttype) {
	case TYPE_bte:
		AVERAGE_TYPE(bte);
		break;
	case TYPE_sht:
		AVERAGE_TYPE(sht);
		break;
	case TYPE_int:
		AVERAGE_TYPE(int);
		break;
	case TYPE_lng:
		AVERAGE_TYPE(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AVERAGE_TYPE(hge);
		break;
#endif
	case TYPE_flt:
		AVERAGE_FLOATTYPE(flt);
		break;
	case TYPE_dbl:
		AVERAGE_FLOATTYPE(dbl);
		break;
	default:
		GDKerror("BATcalcavg: average of type %s unsupported.\n",
			 ATOMname(b->ttype));
		return GDK_FAIL;
	}
	if (vals)
		*vals = n;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* count */

#define AGGR_COUNT(TYPE)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - b->hseqbase;		\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (!is_##TYPE##_nil(vals[i])) {	\
					cnts[gid]++;			\
				}					\
			}						\
		}							\
	} while (0)

/* calculate group counts with optional candidates list */
BAT *
BATgroupcount(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *restrict gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	lng *restrict cnts;
	BAT *bn = NULL;
	int t;
	const void *nil;
	int (*atomcmp)(const void *, const void *);
	BATiter bi;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_lng);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cand, &candend)) != NULL) {
		GDKerror("BATgroupcount: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupcount: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no counts, so return bat aligned with g
		 * with zero in the tail */
		lng zero = 0;
		return BATconstant(ngrp == 0 ? 0 : min, TYPE_lng, &zero, ngrp, TRANSIENT);
	}

	bn = COLnew(min, TYPE_lng, ngrp, TRANSIENT);
	if (bn == NULL)
		return NULL;
	cnts = (lng *) Tloc(bn, 0);
	memset(cnts, 0, ngrp * sizeof(lng));

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, start);

	if (!skip_nils || b->tnonil) {
		/* if nils are nothing special, or if there are no
		 * nils, we don't need to look at the values at all */
		if (cand) {
			if (gids) {
				while (cand < candend) {
					i = *cand++ - b->hseqbase;
					if (gids[i] >= min && gids[i] <= max)
						cnts[gids[i] - min]++;
				}
			} else {
				while (cand < candend) {
					i = *cand++ - b->hseqbase;
					cnts[i] = 1;
				}
			}
		} else {
			if (gids) {
				for (i = start; i < end; i++) {
					if (gids[i] >= min && gids[i] <= max)
						cnts[gids[i] - min]++;
				}
			} else {
				for (i = start; i < end; i++)
					cnts[i] = 1;
			}
		}
	} else {
		t = b->ttype;
		nil = ATOMnilptr(t);
		atomcmp = ATOMcompare(t);
		t = ATOMbasetype(t);

		switch (t) {
		case TYPE_bte:
			AGGR_COUNT(bte);
			break;
		case TYPE_sht:
			AGGR_COUNT(sht);
			break;
		case TYPE_int:
			AGGR_COUNT(int);
			break;
		case TYPE_lng:
			AGGR_COUNT(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			AGGR_COUNT(hge);
			break;
#endif
		case TYPE_flt:
			AGGR_COUNT(flt);
			break;
		case TYPE_dbl:
			AGGR_COUNT(dbl);
			break;
		default:
			bi = bat_iterator(b);

			for (;;) {
				if (cand) {
					if (cand == candend)
						break;
					i = *cand++ - b->hseqbase;
				} else {
					i = start++;
					if (i == end)
						break;
				}
				if (gids == NULL ||
				    (gids[i] >= min && gids[i] <= max)) {
					if (gids)
						gid = gids[i] - min;
					else
						gid = (oid) i;
					if ((*atomcmp)(BUNtail(bi, i), nil) != 0) {
						cnts[gid]++;
					}
				}
			}
			break;
		}
	}
	BATsetcount(bn, ngrp);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	return bn;
}

/* calculate group sizes (number of TRUE values) with optional
 * candidates list */
BAT *
BATgroupsize(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *restrict gids;
	oid min, max;
	BUN i, ngrp;
	const bit *restrict bits;
	lng *restrict cnts;
	BAT *bn = NULL;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_lng);
	assert(b->ttype == TYPE_bit);
	/* compatibility arguments */
	(void) tp;
	(void) abort_on_error;
	(void) skip_nils;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cand, &candend)) != NULL) {
		GDKerror("BATgroupsize: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupsize: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no products, so return bat aligned with g
		 * with zero in the tail */
		lng zero = 0;
		return BATconstant(ngrp == 0 ? 0 : min, TYPE_lng, &zero, ngrp, TRANSIENT);
	}

	bn = COLnew(min, TYPE_lng, ngrp, TRANSIENT);
	if (bn == NULL)
		return NULL;
	cnts = (lng *) Tloc(bn, 0);
	memset(cnts, 0, ngrp * sizeof(lng));

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, start);

	bits = (const bit *) Tloc(b, 0);

	for (;;) {
		if (cand) {
			if (cand == candend)
				break;
			i = *cand++ - b->hseqbase;
			if (i >= end)
				break;
		} else {
			i = start++;
			if (i == end)
				break;
		}
		if (bits[i] == 1 &&
		    (gids == NULL || (gids[i] >= min && gids[i] <= max))) {
			cnts[gids ? gids[i] - min : (oid) i]++;
		}
	}
	BATsetcount(bn, ngrp);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	return bn;
}

/* ---------------------------------------------------------------------- */
/* min and max */

#define AGGR_CMP(TYPE, OP)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		if (ngrp == cnt) {					\
			/* single element groups */			\
			if (cand) {					\
				while (cand < candend) {		\
					i = *cand++ - b->hseqbase;	\
					if (i >= end)			\
						break;			\
					if (!skip_nils ||		\
					    !is_##TYPE##_nil(vals[i])) { \
						oids[i] = i + b->hseqbase; \
						nils--;			\
					}				\
				}					\
			} else {					\
				for (i = start; i < end; i++) {		\
					if (!skip_nils ||		\
					    !is_##TYPE##_nil(vals[i])) { \
						oids[i] = i + b->hseqbase; \
						nils--;			\
					}				\
				}					\
			}						\
		} else {						\
			gid = 0; /* in case gids == NULL */		\
			for (;;) {					\
				if (cand) {				\
					if (cand == candend)		\
						break;			\
					i = *cand++ - b->hseqbase;	\
					if (i >= end)			\
						break;			\
				} else {				\
					i = start++;			\
					if (i == end)			\
						break;			\
				}					\
				if (gids == NULL ||			\
				    (gids[i] >= min && gids[i] <= max)) { \
					if (gids)			\
						gid = gids[i] - min;	\
					if (!skip_nils || !is_##TYPE##_nil(vals[i])) { \
						if (is_oid_nil(oids[gid])) { \
							oids[gid] = i + b->hseqbase; \
							nils--;		\
						} else if (!is_##TYPE##_nil(vals[oids[gid] - b->hseqbase]) && \
							   (is_##TYPE##_nil(vals[i]) || \
							    OP(vals[i], vals[oids[gid] - b->hseqbase]))) \
							oids[gid] = i + b->hseqbase; \
					}				\
				}					\
			}						\
		}							\
	} while (0)

/* calculate group minimums with optional candidates list
 *
 * note that this functions returns *positions* of where the minimum
 * values occur */
static BUN
do_groupmin(oid *restrict oids, BAT *b, const oid *restrict gids, BUN ngrp,
	    oid min, oid max, BUN start, BUN end,
	    const oid *restrict cand, const oid *candend,
	    BUN cnt, int skip_nils, int gdense)
{
	oid gid;
	BUN i, nils;
	int t;
	const void *nil;
	int (*atomcmp)(const void *, const void *);
	BATiter bi;

	nils = ngrp;
	for (i = 0; i < ngrp; i++)
		oids[i] = oid_nil;
	if (cnt == 0)
		return nils;

	t = b->ttype;
	nil = ATOMnilptr(t);
	atomcmp = ATOMcompare(t);
	t = ATOMbasetype(t);
	switch (t) {
	case TYPE_bte:
		AGGR_CMP(bte, LT);
		break;
	case TYPE_sht:
		AGGR_CMP(sht, LT);
		break;
	case TYPE_int:
		AGGR_CMP(int, LT);
		break;
	case TYPE_lng:
		AGGR_CMP(lng, LT);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_CMP(hge, LT);
		break;
#endif
	case TYPE_flt:
		AGGR_CMP(flt, LT);
		break;
	case TYPE_dbl:
		AGGR_CMP(dbl, LT);
		break;
	case TYPE_void:
		if (!gdense && gids == NULL) {
			oids[0] = start + b->hseqbase;
			nils--;
			break;
		}
		/* fall through */
	default:
		assert(b->ttype != TYPE_oid);
		bi = bat_iterator(b);

		if (gdense) {
			/* single element groups */
			if (cand) {
				while (cand < candend) {
					i = *cand++ - b->hseqbase;
					if (i >= end)
						break;
					if (!skip_nils ||
					    (*atomcmp)(BUNtail(bi, i), nil) != 0) {
						oids[i] = i + b->hseqbase;
						nils--;
					}
				}
			} else {
				for (i = start; i < end; i++) {
					if (!skip_nils ||
					    (*atomcmp)(BUNtail(bi, i), nil) != 0) {
						oids[i] = i + b->hseqbase;
						nils--;
					}
				}
			}
		} else {
			gid = 0; /* in case gids == NULL */
			for (;;) {
				if (cand) {
					if (cand == candend)
						break;
					i = *cand++ - b->hseqbase;
					if (i >= end)
						break;
				} else {
					i = start++;
					if (i == end)
						break;
				}
				if (gids == NULL ||
				    (gids[i] >= min && gids[i] <= max)) {
					const void *v = BUNtail(bi, i);
					if (gids)
						gid = gids[i] - min;
					if (!skip_nils ||
					    (*atomcmp)(v, nil) != 0) {
						if (is_oid_nil(oids[gid])) {
							oids[gid] = i + b->hseqbase;
							nils--;
						} else if (t != TYPE_void) {
							const void *g = BUNtail(bi, (BUN) (oids[gid] - b->hseqbase));
							if ((*atomcmp)(g, nil) != 0 &&
							    ((*atomcmp)(v, nil) == 0 ||
							     LT((*atomcmp)(v, g), 0)))
								oids[gid] = i + b->hseqbase;
						}
					}
				}
			}
		}
		break;
	}

	return nils;
}

/* calculate group maximums with optional candidates list
 *
 * note that this functions returns *positions* of where the maximum
 * values occur */
static BUN
do_groupmax(oid *restrict oids, BAT *b, const oid *restrict gids, BUN ngrp,
	    oid min, oid max, BUN start, BUN end,
	    const oid *restrict cand, const oid *candend,
	    BUN cnt, int skip_nils, int gdense)
{
	oid gid;
	BUN i, nils;
	int t;
	const void *nil;
	int (*atomcmp)(const void *, const void *);
	BATiter bi;

	nils = ngrp;
	for (i = 0; i < ngrp; i++)
		oids[i] = oid_nil;
	if (cnt == 0)
		return nils;

	t = b->ttype;
	nil = ATOMnilptr(t);
	atomcmp = ATOMcompare(t);
	t = ATOMbasetype(t);
	switch (t) {
	case TYPE_bte:
		AGGR_CMP(bte, GT);
		break;
	case TYPE_sht:
		AGGR_CMP(sht, GT);
		break;
	case TYPE_int:
		AGGR_CMP(int, GT);
		break;
	case TYPE_lng:
		AGGR_CMP(lng, GT);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_CMP(hge, GT);
		break;
#endif
	case TYPE_flt:
		AGGR_CMP(flt, GT);
		break;
	case TYPE_dbl:
		AGGR_CMP(dbl, GT);
		break;
	case TYPE_void:
		if (!gdense && gids == NULL) {
			oids[0] = end + b->hseqbase - 1;
			nils--;
			break;
		}
		/* fall through */
	default:
		assert(b->ttype != TYPE_oid);
		bi = bat_iterator(b);

		if (gdense) {
			/* single element groups */
			if (cand) {
				while (cand < candend) {
					i = *cand++ - b->hseqbase;
					if (i >= end)
						break;
					if (!skip_nils ||
					    (*atomcmp)(BUNtail(bi, i), nil) != 0) {
						oids[i] = i + b->hseqbase;
						nils--;
					}
				}
			} else {
				for (i = start; i < end; i++) {
					if (!skip_nils ||
					    (*atomcmp)(BUNtail(bi, i), nil) != 0) {
						oids[i] = i + b->hseqbase;
						nils--;
					}
				}
			}
		} else {
			gid = 0; /* in case gids == NULL */
			for (;;) {
				if (cand) {
					if (cand == candend)
						break;
					i = *cand++ - b->hseqbase;
					if (i >= end)
						break;
				} else {
					i = start++;
					if (i == end)
						break;
				}
				if (gids == NULL ||
				    (gids[i] >= min && gids[i] <= max)) {
					const void *v = BUNtail(bi, i);
					if (gids)
						gid = gids[i] - min;
					if (!skip_nils ||
					    (*atomcmp)(v, nil) != 0) {
						if (is_oid_nil(oids[gid])) {
							oids[gid] = i + b->hseqbase;
							nils--;
						} else {
							const void *g = BUNtail(bi, (BUN) (oids[gid] - b->hseqbase));
							if (t == TYPE_void ||
							    ((*atomcmp)(g, nil) != 0 &&
							     ((*atomcmp)(v, nil) == 0 ||
							      GT((*atomcmp)(v, g), 0))))
								oids[gid] = i + b->hseqbase;
						}
					}
				}
			}
		}
		break;
	}

	return nils;
}

static BAT *
BATgroupminmax(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils,
	       int abort_on_error,
	       BUN (*minmax)(oid *restrict, BAT *, const oid *restrict, BUN,
			     oid, oid, BUN, BUN, const oid *restrict,
			     const oid *, BUN, int, int),
	       const char *name)
{
	const oid *restrict gids;
	oid min, max;
	BUN ngrp;
	oid *restrict oids;
	BAT *bn = NULL;
	BUN nils;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_oid);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if (!ATOMlinear(b->ttype)) {
		GDKerror("%s: cannot determine minimum on "
			 "non-linear type %s\n", name, ATOMname(b->ttype));
		return NULL;
	}

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cand, &candend)) != NULL) {
		GDKerror("%s: %s\n", name, err);
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no minimums, so return bat aligned with g
		 * with nil in the tail */
		return BATconstant(ngrp == 0 ? 0 : min, TYPE_oid, &oid_nil, ngrp, TRANSIENT);
	}

	bn = COLnew(min, TYPE_oid, ngrp, TRANSIENT);
	if (bn == NULL)
		return NULL;
	oids = (oid *) Tloc(bn, 0);

	if (g == NULL || BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, start);

	nils = (*minmax)(oids, b, gids, ngrp, min, max, start, end,
			 cand, candend, BATcount(b), skip_nils,
			 g && BATtdense(g));

	BATsetcount(bn, ngrp);

	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	return bn;
}

static void *
BATminmax(BAT *b, void *aggr,
	  BUN (*minmax)(oid *restrict, BAT *, const oid *restrict, BUN,
			oid, oid, BUN, BUN, const oid *restrict,
			const oid *, BUN, int, int))
{
	oid pos;
	const void *res;
	size_t s;
	BATiter bi;

	if ((VIEWtparent(b) == 0 ||
	     BATcount(b) == BATcount(BBPdescriptor(VIEWtparent(b)))) &&
	    BATcheckimprints(b)) {
		Imprints *imprints = VIEWtparent(b) ? BBPdescriptor(VIEWtparent(b))->timprints : b->timprints;
		int i;

		pos = oid_nil;
		if (minmax == do_groupmin) {
			/* find first non-empty bin */
			for (i = 0; i < imprints->bits; i++) {
				if (imprints->stats[i + 128]) {
					pos = imprints->stats[i] + b->hseqbase;
					break;
				}
			}
		} else {
			/* find last non-empty bin */
			for (i = imprints->bits - 1; i >= 0; i--) {
				if (imprints->stats[i + 128]) {
					pos = imprints->stats[i + 64] + b->hseqbase;
					break;
				}
			}
		}
	} else {
		(void) (*minmax)(&pos, b, NULL, 1, 0, 0, 0, BATcount(b),
				 NULL, NULL, BATcount(b), 1, 0);
	}
	if (is_oid_nil(pos)) {
		res = ATOMnilptr(b->ttype);
	} else {
		bi = bat_iterator(b);
		res = BUNtail(bi, pos - b->hseqbase);
	}
	if (aggr == NULL) {
		s = ATOMlen(b->ttype, res);
		aggr = GDKmalloc(s);
	} else {
		s = ATOMsize(ATOMtype(b->ttype));
	}
	if (aggr != NULL)	/* else: malloc error */
		memcpy(aggr, res, s);
	return aggr;
}

BAT *
BATgroupmin(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
	    int skip_nils, int abort_on_error)
{
	return BATgroupminmax(b, g, e, s, tp, skip_nils, abort_on_error,
			      do_groupmin, "BATgroupmin");
}

void *
BATmin(BAT *b, void *aggr)
{
	return BATminmax(b, aggr, do_groupmin);
}

BAT *
BATgroupmax(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
	    int skip_nils, int abort_on_error)
{
	return BATgroupminmax(b, g, e, s, tp, skip_nils, abort_on_error,
			      do_groupmax, "BATgroupmax");
}

void *
BATmax(BAT *b, void *aggr)
{
	return BATminmax(b, aggr, do_groupmax);
}


/* ---------------------------------------------------------------------- */
/* quantiles/median */

BAT *
BATgroupmedian(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
	       int skip_nils, int abort_on_error)
{
	return BATgroupquantile(b,g,e,s,tp,0.5,skip_nils,abort_on_error);
}

#if SIZEOF_OID == SIZEOF_INT
#define binsearch_oid(indir, offset, vals, lo, hi, v, ordering, last) binsearch_int(indir, offset, (const int *) vals, lo, hi, (int) (v), ordering, last)
#endif
#if SIZEOF_OID == SIZEOF_LNG
#define binsearch_oid(indir, offset, vals, lo, hi, v, ordering, last) binsearch_lng(indir, offset, (const lng *) vals, lo, hi, (lng) (v), ordering, last)
#endif

BAT *
BATgroupquantile(BAT *b, BAT *g, BAT *e, BAT *s, int tp, double quantile,
		 int skip_nils, int abort_on_error)
{
	int freeb = 0, freeg = 0;
	oid min, max;
	BUN ngrp;
	BUN nils = 0;
	BAT *bn = NULL;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	BAT *t1, *t2;
	BATiter bi;
	const void *v;
	const void *nil = ATOMnilptr(tp);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tp);
	const char *err;
	(void) abort_on_error;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cand, &candend)) != NULL) {
		GDKerror("BATgroupquantile: %s\n", err);
		return NULL;
	}
	assert(tp == b->ttype);
	if (!ATOMlinear(tp)) {
		GDKerror("BATgroupquantile: cannot determine quantile on "
			 "non-linear type %s\n", ATOMname(tp));
		return NULL;
	}
	if (quantile < 0 || quantile > 1) {
		GDKerror("BATgroupquantile: cannot determine quantile for "
			 "p=%f (p has to be in [0,1])\n", quantile);
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no values, thus also no quantiles,
		 * so return bat aligned with e with nil in the tail */
		return BATconstant(ngrp == 0 ? 0 : min, tp, nil, ngrp, TRANSIENT);
	}

	if (s) {
		/* there is a candidate list, replace b (and g, if
		 * given) with just the values we're interested in */
		b = BATproject(s, b);
		if (b == NULL)
			return NULL;
		freeb = 1;
		if (g) {
			g = BATproject(s, g);
			if (g == NULL)
				goto bunins_failed;
			freeg = 1;
		}
	}

	/* we want to sort b so that we can figure out the quantile, but
	 * if g is given, sort g and subsort b so that we can get the
	 * quantile for each group */
	if (g) {
		const oid *restrict grps;
		oid prev;
		BUN p, q, r;

		if (BATtdense(g)) {
			/* singleton groups, so calculating quantile is
			 * easy */
			bn = COLcopy(b, tp, 0, TRANSIENT);
			BAThseqbase(bn, g->tseqbase); /* deals with NULL */
			if (freeb)
				BBPunfix(b->batCacheid);
			if (freeg)
				BBPunfix(g->batCacheid);
			return bn;
		}
		if (BATsort(&t1, &t2, NULL, g, NULL, NULL, 0, 0) != GDK_SUCCEED)
			goto bunins_failed;
		if (freeg)
			BBPunfix(g->batCacheid);
		g = t1;
		freeg = 1;

		if (BATsort(&t1, NULL, NULL, b, t2, g, 0, 0) != GDK_SUCCEED) {
			BBPunfix(t2->batCacheid);
			goto bunins_failed;
		}
		if (freeb)
			BBPunfix(b->batCacheid);
		b = t1;
		freeb = 1;
		BBPunfix(t2->batCacheid);

		bn = COLnew(min, tp, ngrp, TRANSIENT);
		if (bn == NULL)
			goto bunins_failed;

		bi = bat_iterator(b);

		grps = (const oid *) Tloc(g, 0);
		 /* for each group (r and p are the beginning and end
		  * of the current group, respectively) */
		for (r = 0, q = BATcount(g); r < q; r = p) {
			BUN qindex;
			prev = grps[r];
			/* search for end of current group (grps is
			 * sorted so we can use binary search) */
			p = binsearch_oid(NULL, 0, grps, r, q - 1, prev, 1, 1);
			if (skip_nils && !b->tnonil) {
				/* within group, locate start of non-nils */
				r = binsearch(NULL, 0, tp, Tloc(b, 0),
					      b->tvheap ? b->tvheap->base : NULL,
					      b->twidth, r, p, nil,
					      1, 1);
			}
			if (r == p) {
				/* no non-nils found */
				v = nil;
				nils++;
			} else {
				/* round *down* to nearest integer */
				double f = (p - r - 1) * quantile;
				qindex = r + p - (BUN) (p + 0.5 - f);
				/* be a little paranoid about the index */
				assert(qindex >= r && qindex <  p);
				v = BUNtail(bi, qindex);
				if (!skip_nils && !b->tnonil)
					nils += (*atomcmp)(v, nil) == 0;
			}
			bunfastapp_nocheck(bn, BUNlast(bn), v, Tsize(bn));
		}
		nils += ngrp - BATcount(bn);
		while (BATcount(bn) < ngrp) {
			bunfastapp_nocheck(bn, BUNlast(bn), nil, Tsize(bn));
		}
		BBPunfix(g->batCacheid);
	} else {
		BUN index, r, p = BATcount(b);
		BAT *pb = NULL;
		const oid *ords;

		bn = COLnew(0, tp, 1, TRANSIENT);
		if (bn == NULL)
			goto bunins_failed;

		t1 = NULL;

		if (BATcheckorderidx(b) ||
		    (VIEWtparent(b) &&
		     (pb = BBPdescriptor(VIEWtparent(b))) != NULL &&
		     pb->theap.base == b->theap.base &&
		     BATcount(pb) == BATcount(b) &&
		     pb->hseqbase == b->hseqbase &&
		     BATcheckorderidx(pb))) {
			ords = (const oid *) (pb ? pb->torderidx->base : b->torderidx->base) + ORDERIDXOFF;
		} else {
			if (BATsort(NULL, &t1, NULL, b, NULL, g, 0, 0) != GDK_SUCCEED)
				goto bunins_failed;
			if (BATtdense(t1))
				ords = NULL;
			else
				ords = (const oid *) Tloc(t1, 0);
		}

		if (skip_nils && !b->tnonil)
			r = binsearch(ords, 0, tp, Tloc(b, 0),
				      b->tvheap ? b->tvheap->base : NULL,
				      b->twidth, 0, p,
				      nil, 1, 1);
		else
			r = 0;

		if (r == p) {
			/* no non-nil values, so quantile is nil */
			v = nil;
			nils++;
		} else {
			double f;
			bi = bat_iterator(b);
			/* round (p-r-1)*quantile *down* to nearest
			 * integer (i.e., 1.49 and 1.5 are rounded to
			 * 1, 1.51 is rounded to 2) */
			f = (p - r - 1) * quantile;
			index = r + p - (BUN) (p + 0.5 - f);
			if (ords)
				index = ords[index] - b->hseqbase;
			else
				index = index + t1->tseqbase;
			v = BUNtail(bi, index);
			nils += (*atomcmp)(v, nil) == 0;
		}
		if (t1)
			BBPunfix(t1->batCacheid);
		if (BUNappend(bn, v, FALSE) != GDK_SUCCEED)
			goto bunins_failed;
	}

	if (freeb)
		BBPunfix(b->batCacheid);

	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	return bn;

  bunins_failed:
	if (freeb)
		BBPunfix(b->batCacheid);
	if (freeg)
		BBPunfix(g->batCacheid);
	if (bn)
		BBPunfix(bn->batCacheid);
	return NULL;
}

/* ---------------------------------------------------------------------- */
/* standard deviation (both biased and non-biased) */

#define AGGR_STDEV_SINGLE(TYPE)				\
	do {						\
		TYPE x;					\
		for (i = 0; i < cnt; i++) {		\
			x = ((const TYPE *) values)[i];	\
			if (is_##TYPE##_nil(x))		\
				continue;		\
			n++;				\
			delta = (dbl) x - mean;		\
			mean += delta / n;		\
			m2 += delta * ((dbl) x - mean);	\
		}					\
	} while (0)

static dbl
calcvariance(dbl *restrict avgp, const void *restrict values, BUN cnt, int tp, int issample, const char *func)
{
	BUN n = 0, i;
	dbl mean = 0;
	dbl m2 = 0;
	dbl delta;

	assert(issample == 0 || issample == 1);

	switch (tp) {
	case TYPE_bte:
		AGGR_STDEV_SINGLE(bte);
		break;
	case TYPE_sht:
		AGGR_STDEV_SINGLE(sht);
		break;
	case TYPE_int:
		AGGR_STDEV_SINGLE(int);
		break;
	case TYPE_lng:
		AGGR_STDEV_SINGLE(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_STDEV_SINGLE(hge);
		break;
#endif
	case TYPE_flt:
		AGGR_STDEV_SINGLE(flt);
		break;
	case TYPE_dbl:
		AGGR_STDEV_SINGLE(dbl);
		break;
	default:
		GDKerror("%s: type (%s) not supported.\n",
			 func, ATOMname(tp));
		return dbl_nil;
	}
	if (n <= (BUN) issample) {
		if (avgp)
			*avgp = dbl_nil;
		return dbl_nil;
	}
	if (avgp)
		*avgp = mean;
	return m2 / (n - issample);
}

dbl
BATcalcstdev_population(dbl *avgp, BAT *b)
{
	dbl v = calcvariance(avgp, (const void *) Tloc(b, 0),
			     BATcount(b), b->ttype, 0,
			     "BATcalcstdev_population");
	return is_dbl_nil(v) ? dbl_nil : sqrt(v);
}

dbl
BATcalcstdev_sample(dbl *avgp, BAT *b)
{
	dbl v = calcvariance(avgp, (const void *) Tloc(b, 0),
			     BATcount(b), b->ttype, 1,
			     "BATcalcstdev_sample");
	return is_dbl_nil(v) ? dbl_nil : sqrt(v);
}

dbl
BATcalcvariance_population(dbl *avgp, BAT *b)
{
	return calcvariance(avgp, (const void *) Tloc(b, 0),
			    BATcount(b), b->ttype, 0,
			    "BATcalcvariance_population");
}

dbl
BATcalcvariance_sample(dbl *avgp, BAT *b)
{
	return calcvariance(avgp, (const void *) Tloc(b, 0),
			    BATcount(b), b->ttype, 1,
			    "BATcalcvariance_sample");
}

#define AGGR_STDEV(TYPE)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - b->hseqbase;		\
				if (i >= end)				\
					break;				\
			} else {					\
				i = start++;				\
				if (i == end)				\
					break;				\
			}						\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (is_##TYPE##_nil(vals[i])) {		\
					if (!skip_nils)			\
						cnts[gid] = BUN_NONE;	\
				} else if (cnts[gid] != BUN_NONE) {	\
					cnts[gid]++;			\
					delta[gid] = (dbl) vals[i] - mean[gid]; \
					mean[gid] += delta[gid] / cnts[gid]; \
					m2[gid] += delta[gid] * ((dbl) vals[i] - mean[gid]); \
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] == 0 || cnts[i] == BUN_NONE) {	\
				dbls[i] = dbl_nil;			\
				mean[i] = dbl_nil;			\
				nils++;					\
			} else if (cnts[i] == 1) {			\
				dbls[i] = issample ? dbl_nil : 0;	\
				nils2++;				\
			} else if (variance) {				\
				dbls[i] = m2[i] / (cnts[i] - issample);	\
			} else {					\
				dbls[i] = sqrt(m2[i] / (cnts[i] - issample)); \
			}						\
		}							\
	} while (0)

/* Calculate group standard deviation (population (i.e. biased) or
 * sample (i.e. non-biased)) with optional candidates list.
 *
 * Note that this helper function is prepared to return two BATs: one
 * (as return value) with the standard deviation per group, and one
 * (as return argument) with the average per group.  This isn't
 * currently used since it doesn't fit into the mold of grouped
 * aggregates. */
static BAT *
dogroupstdev(BAT **avgb, BAT *b, BAT *g, BAT *e, BAT *s, int tp,
	     int skip_nils, int issample, int variance, const char *func)
{
	const oid *restrict gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	BUN nils = 0, nils2 = 0;
	BUN *restrict cnts = NULL;
	dbl *restrict dbls, *restrict mean, *restrict delta, *restrict m2;
	BAT *bn = NULL;
	BUN start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_dbl);
	(void) tp;		/* compatibility (with other BATgroup*
				 * functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cand, &candend)) != NULL) {
		GDKerror("%s: %s\n", func, err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("%s: b and g must be aligned\n", func);
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no products, so return bat aligned with g
		 * with nil in the tail */
		return BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &dbl_nil, ngrp, TRANSIENT);
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->tnonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to zero (population) or nil (sample) */
		dbl v = issample ? dbl_nil : 0;
		return BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &v, ngrp, TRANSIENT);
	}

	delta = GDKmalloc(ngrp * sizeof(dbl));
	m2 = GDKmalloc(ngrp * sizeof(dbl));
	cnts = GDKzalloc(ngrp * sizeof(BUN));
	if (avgb) {
		if ((*avgb = COLnew(0, TYPE_dbl, ngrp, TRANSIENT)) == NULL) {
			mean = NULL;
			goto alloc_fail;
		}
		mean = (dbl *) Tloc(*avgb, 0);
	} else {
		mean = GDKmalloc(ngrp * sizeof(dbl));
	}
	if (mean == NULL || delta == NULL || m2 == NULL || cnts == NULL)
		goto alloc_fail;

	bn = COLnew(min, TYPE_dbl, ngrp, TRANSIENT);
	if (bn == NULL)
		goto alloc_fail;
	dbls = (dbl *) Tloc(bn, 0);

	for (i = 0; i < ngrp; i++) {
		mean[i] = 0;
		delta[i] = 0;
		m2[i] = 0;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, start);

	switch (b->ttype) {
	case TYPE_bte:
		AGGR_STDEV(bte);
		break;
	case TYPE_sht:
		AGGR_STDEV(sht);
		break;
	case TYPE_int:
		AGGR_STDEV(int);
		break;
	case TYPE_lng:
		AGGR_STDEV(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_STDEV(hge);
		break;
#endif
	case TYPE_flt:
		AGGR_STDEV(flt);
		break;
	case TYPE_dbl:
		AGGR_STDEV(dbl);
		break;
	default:
		if (avgb)
			BBPreclaim(*avgb);
		else
			GDKfree(mean);
		GDKfree(delta);
		GDKfree(m2);
		GDKfree(cnts);
		BBPunfix(bn->batCacheid);
		GDKerror("%s: type (%s) not supported.\n",
			 func, ATOMname(b->ttype));
		return NULL;
	}
	if (avgb) {
		BATsetcount(*avgb, ngrp);
		(*avgb)->tkey = ngrp <= 1;
		(*avgb)->tsorted = ngrp <= 1;
		(*avgb)->trevsorted = ngrp <= 1;
		(*avgb)->tnil = nils != 0;
		(*avgb)->tnonil = nils == 0;
	} else {
		GDKfree(mean);
	}
	nils += nils2;
	GDKfree(delta);
	GDKfree(m2);
	GDKfree(cnts);
	BATsetcount(bn, ngrp);
	bn->tkey = ngrp <= 1;
	bn->tsorted = ngrp <= 1;
	bn->trevsorted = ngrp <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	return bn;

  alloc_fail:
	if (avgb)
		BBPreclaim(*avgb);
	else
		GDKfree(mean);
	BBPreclaim(bn);
	GDKfree(delta);
	GDKfree(m2);
	GDKfree(cnts);
	GDKerror("%s: cannot allocate enough memory.\n", func);
	return NULL;
}

BAT *
BATgroupstdev_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
		     int skip_nils, int abort_on_error)
{
	(void) abort_on_error;
	return dogroupstdev(NULL, b, g, e, s, tp, skip_nils, 1, 0,
			    "BATgroupstdev_sample");
}

BAT *
BATgroupstdev_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
			 int skip_nils, int abort_on_error)
{
	(void) abort_on_error;
	return dogroupstdev(NULL, b, g, e, s, tp, skip_nils, 0, 0,
			    "BATgroupstdev_population");
}

BAT *
BATgroupvariance_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
		     int skip_nils, int abort_on_error)
{
	(void) abort_on_error;
	return dogroupstdev(NULL, b, g, e, s, tp, skip_nils, 1, 1,
			    "BATgroupvariance_sample");
}

BAT *
BATgroupvariance_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
			 int skip_nils, int abort_on_error)
{
	(void) abort_on_error;
	return dogroupstdev(NULL, b, g, e, s, tp, skip_nils, 0, 1,
			    "BATgroupvariance_population");
}

#define IS_A_POINTER 0
#define IS_A_BAT     1

static gdk_return
concat_strings(void *res, int what, BAT* b, int nonil, oid seqb, BUN start, BUN end, BUN ngrp, const oid *restrict cand,
			   const oid *candend, const oid *restrict gids, oid min, oid max, int skip_nils, const char *func,
			   BUN *has_nils)
{
	oid gid;
	BUN i, p, q, nils = 0;
	const oid *aux;
	const char *separator = ",";
	size_t* lengths = NULL, separator_length = strlen(separator), next_length;
	oid *lastoid = NULL;
	str* astrings = NULL, s, single_str = NULL;
	BATiter bi;
	BAT *bn = NULL;
	gdk_return rres = GDK_SUCCEED;

	(void) skip_nils;

	if (what == IS_A_BAT && (bn = COLnew(min, TYPE_str, ngrp, TRANSIENT)) == NULL) {
		rres = GDK_FAIL;
		goto finish;
	}

	bi = bat_iterator(b);

	if (ngrp == 1) {
		size_t offset = 0, single_length = 0;
		oid single_oid = 0;
		if (cand == NULL) {
			if(nonil) {
				BATloop(b,p,q) {
					s = BUNtail(bi, p);
					next_length = strlen(s);
					if(next_length) {
						single_length += next_length + separator_length;
						single_oid = p;
					}
				}
			} else {
				BATloop(b,p,q) {
					s = BUNtail(bi, p);
					if (strcmp(s, str_nil)) {
						next_length = strlen(s);
						if(next_length) {
							single_length += next_length + separator_length;
							single_oid = p;
						}
					} else {
						single_oid = BUN_NONE;
						nils = 1;
						break;
					}
				}
			}
			if(!nils) {
				if(single_length > 0) {
					if ((single_str = GDKmalloc((single_length + 1 - separator_length) * sizeof(str))) == NULL) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
					BATloop(b,p,q){
						s = BUNtail(bi, p);
						next_length = strlen(s);
						if(next_length) {
							memcpy(single_str + offset, s, next_length);
							offset += next_length;
							if(p != single_oid) {
								memcpy(single_str + offset, separator, separator_length);
								offset += separator_length;
							}
						}
					}
					single_str[offset] = '\0';
					if(what == IS_A_BAT) {
						if(BUNappend(bn, single_str, FALSE) != GDK_SUCCEED) {
							GDKerror("%s: malloc failure\n", func);
							rres = GDK_FAIL;
							goto finish;
						}
					} else {
						ValPtr pt = (ValPtr) res;
						pt->len = single_str[offset];
						if((pt->val.sval = GDKstrdup(single_str)) == NULL) {
							GDKerror("%s: malloc failure\n", func);
							rres = GDK_FAIL;
							goto finish;
						}
					}
				} else if(what == IS_A_BAT) {
					if(BUNappend(bn, "", FALSE) != GDK_SUCCEED) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
				} else {
					ValPtr pt = (ValPtr) res;
					pt->len = 0;
					if((pt->val.sval = GDKstrdup("")) == NULL) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
				}
			} else if(what == IS_A_BAT) {
				if(BUNappend(bn, str_nil, FALSE) != GDK_SUCCEED) {
					GDKerror("%s: malloc failure\n", func);
					rres = GDK_FAIL;
					goto finish;
				}
			} else {
				ValPtr pt = (ValPtr) res;
				pt->len = 0;
				if((pt->val.sval = GDKstrdup(str_nil)) == NULL) {
					GDKerror("%s: malloc failure\n", func);
					rres = GDK_FAIL;
					goto finish;
				}
			}
		} else { /* with candidate lists */
			aux = cand;
			if(nonil) {
				while (cand < candend && nils == 0) {
					i = *cand++ - seqb;
					if (i >= end)
						break;
					s = BUNtail(bi, i);
					next_length = strlen(s);
					if(next_length > 0) {
						single_length += next_length + separator_length;
						single_oid = i;
					}
				}
			} else {
				while (cand < candend && nils == 0) {
					i = *cand++ - seqb;
					if (i >= end)
						break;
					s = BUNtail(bi, i);
					if (strcmp(s, str_nil)) {
						next_length = strlen(s);
						if(next_length > 0) {
							single_length += next_length + separator_length;
							single_oid = i;
						}
					} else {
						single_oid = BUN_NONE;
						nils = 1;
						break;
					}
				}
			}
			if (!nils) {
				if(single_length > 0) {
					if ((single_str = GDKmalloc((single_length + 1 - separator_length) * sizeof(str))) == NULL) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
					cand = aux;
					while (cand < candend) {
						i = *cand++ - seqb;
						if (i >= end)
							break;
						s = BUNtail(bi, i);
						next_length = strlen(s);
						if(next_length > 0) {
							memcpy(single_str + offset, s, next_length);
							offset += next_length;
							if (i != single_oid) {
								memcpy(single_str + offset, separator, separator_length);
								offset += separator_length;
							}
						}
					}
					single_str[offset] = '\0';
					if (BUNappend(bn, single_str, FALSE) != GDK_SUCCEED) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
				} else if (BUNappend(bn, "", FALSE) != GDK_SUCCEED) {
					GDKerror("%s: malloc failure\n", func);
					rres = GDK_FAIL;
					goto finish;
				}
			} else if (BUNappend(bn, str_nil, FALSE) != GDK_SUCCEED) {
				GDKerror("%s: malloc failure\n", func);
				rres = GDK_FAIL;
				goto finish;
			}
		}
	} else {
		if((lengths = GDKzalloc(ngrp * sizeof(*lengths))) == NULL) { /* first used to calculated the total length of*/
			rres = GDK_FAIL;                                           /* each group, then the the total offset */
			goto finish;
		}
		if((lastoid = (oid*) GDKzalloc(ngrp * sizeof(oid))) == NULL) {
			rres = GDK_FAIL;
			goto finish;
		}
		if((astrings = (str*) GDKzalloc(ngrp * sizeof(str))) == NULL) {
			rres = GDK_FAIL;
			goto finish;
		}
		if (cand == NULL) {
			for (i = start; i < end; i++) {
				if (gids == NULL || (gids[i] >= min && gids[i] <= max)) {
					gid = gids ? gids[i] - min : (oid) i;
					if (lastoid[gid] != BUN_NONE) {
						s = BUNtail(bi, i);
						if (strcmp(s, str_nil)) {
							next_length = strlen(s);
							if(next_length > 0) {
								lengths[gid] += next_length + separator_length;
								lastoid[gid] = i;
							}
						} else {
							lastoid[gid] = BUN_NONE;
							nils++;
						}
					}
				}
			}
			for (i = 0; i < ngrp; i++) {
				if(lastoid[i] != BUN_NONE) {
					if(lengths[i] == 0) {
						lastoid[i] = BUN_NONE - 1;
					} else if((astrings[i] = GDKmalloc((lengths[i] + 1 - separator_length) * sizeof(str))) == NULL) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
					lengths[i] = 0;
				}
			}
			for (i = start; i < end; i++) {
				if (gids == NULL || (gids[i] >= min && gids[i] <= max)) {
					gid = gids ? gids[i] - min : (oid) i;
					if (lastoid[gid] < BUN_NONE - 1) {
						s = BUNtail(bi, i);
						next_length = strlen(s);
						if(next_length > 0) {
							memcpy(astrings[gid] + lengths[gid], s, next_length);
							lengths[gid] += next_length;
							if (i != lastoid[gid]) {
								memcpy(astrings[gid] + lengths[gid], separator, separator_length);
								lengths[gid] += separator_length;
							}
						}
					}
				}
			}
			for (i = 0; i < ngrp; i++) {
				if (lastoid[i] < BUN_NONE - 1) {
					astrings[i][lengths[i]] = '\0';
					if(BUNappend(bn, astrings[i], FALSE) != GDK_SUCCEED) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
				} else if (lastoid[i] == BUN_NONE - 1) {
					if(BUNappend(bn, "", FALSE) != GDK_SUCCEED) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
				} else if(BUNappend(bn, str_nil, FALSE) != GDK_SUCCEED) {
					GDKerror("%s: malloc failure\n", func);
					rres = GDK_FAIL;
					goto finish;
				}
			}
		} else {
			aux = cand;
			while (cand < candend) {
				i = *cand++ - seqb;
				if (i >= end)
					break;
				if (gids == NULL || (gids[i] >= min && gids[i] <= max)) {
					gid = gids ? gids[i] - min : (oid) i;
					if (lastoid[gid] != BUN_NONE) {
						s = BUNtail(bi, i);
						if (strcmp(s, str_nil)) {
							next_length = strlen(s);
							if(next_length > 0) {
								lengths[gid] += next_length;
								lastoid[gid] = i;
							}
						} else {
							lastoid[gid] = BUN_NONE;
							nils++;
						}
					}
				}
			}
			for (i = 0; i < ngrp; i++) {
				if(lastoid[i] != BUN_NONE) {
					if(lengths[i] == 0) {
						lastoid[i] = BUN_NONE - 1;
					} else if((astrings[i] = GDKmalloc((lengths[i] + 1 - separator_length) * sizeof(str))) == NULL) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
					lengths[i] = 0;
				}
			}
			cand = aux;
			while (cand < candend) {
				i = *cand++ - seqb;
				if (i >= end)
					break;
				if (gids == NULL || (gids[i] >= min && gids[i] <= max)) {
					gid = gids ? gids[i] - min : (oid) i;
					if (lastoid[gid] < BUN_NONE - 1) {
						s = BUNtail(bi, i);
						next_length = strlen(s);
						if(next_length > 0) {
							memcpy(astrings[gid] + lengths[gid], s, next_length);
							lengths[gid] += next_length;
							if (i != lastoid[gid]) {
								memcpy(astrings[gid] + lengths[gid], separator, separator_length);
								lengths[gid] += separator_length;
							}
						}
					}
				}
			}
			for (i = 0; i < ngrp; i++) {
				if (lastoid[i] < BUN_NONE - 1) {
					astrings[i][lengths[i]] = '\0';
					if(BUNappend(bn, astrings[i], FALSE) != GDK_SUCCEED) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
				} else if (lastoid[i] == BUN_NONE - 1) {
					if(BUNappend(bn, "", FALSE) != GDK_SUCCEED) {
						GDKerror("%s: malloc failure\n", func);
						rres = GDK_FAIL;
						goto finish;
					}
				} else if(BUNappend(bn, str_nil, FALSE) != GDK_SUCCEED) {
					GDKerror("%s: malloc failure\n", func);
					rres = GDK_FAIL;
					goto finish;
				}
			}
		}
	}

finish:
	if(has_nils)
		*has_nils = nils;
	if(lengths)
		GDKfree(lengths);
	if(astrings) {
		for(i = 0 ; i < ngrp ; i++) {
			if(astrings[i])
				GDKfree(astrings[i]);
		}
		GDKfree(astrings);
	}
	if(single_str)
		GDKfree(single_str);
	if(lastoid)
		GDKfree(lastoid);
	if(rres == GDK_FAIL) {
		if(bn) {
			BBPreclaim(bn);
			bn = NULL;
		} else {
			ValPtr pt = (ValPtr) res;
			if(pt->val.sval) {
				GDKfree(pt->val.sval);
				pt->val.sval = NULL;
			}
		}
	}
	if(what == IS_A_BAT) {
		*((BAT**)res) = bn;
	}

	return rres;
}

gdk_return
BATstr_group_concat(ValPtr res, BAT *b, BAT *s, int skip_nils, int abort_on_error, int nil_if_empty)
{
	oid min, max;
	BUN ngrp, start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	(void) abort_on_error;
	res->vtype = TYPE_str;

	if (BATcount(b) == 0) {
		res->len = 0;
		res->val.sval = nil_if_empty ? GDKstrdup(str_nil) : GDKstrdup("");
		if(res->val.sval == NULL) {
			GDKerror("BATstr_group_concat: malloc failure\n");
			return GDK_FAIL;
		}
		return GDK_SUCCEED;
	}
	if ((err = BATgroupaggrinit(b, NULL, NULL, s, &min, &max, &ngrp, &start, &end, &cand, &candend)) != NULL) {
		GDKerror("BATstr_group_concat: %s\n", err);
		return GDK_FAIL;
	}

	return concat_strings(res, IS_A_POINTER, b, b->tnonil, b->hseqbase, start, end, 1, cand, candend, &min, min, max,
						  skip_nils, "BATstr_group_concat", NULL);
}

BAT *
BATgroupstr_group_concat(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *restrict gids;
	BAT *bn = NULL;
	oid min, max;
	BUN ngrp, start, end;
	const oid *cand = NULL, *candend = NULL;
	const char *err;
	BUN nils = 0;
	gdk_return res;

	assert(tp == TYPE_str);
	(void) skip_nils;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end, &cand, &candend)) != NULL) {
		GDKerror("BATgroupstr_group_concat: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupstr_group_concat: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no strings to concat, so return bat aligned with g with nil in the tail */
		return BATconstant(ngrp == 0 ? 0 : min, tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	}

	if ((e == NULL || (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
		(BATtdense(g) || (g->tkey && g->tnonil))) {
		/* trivial: singleton groups, so all results are equal to the inputs (but possibly a different type) */
		return BATconvert(b, s, tp, abort_on_error);
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, start);

	res = concat_strings(&bn, IS_A_BAT, b, b->tnonil, b->hseqbase, start, end, ngrp, cand, candend, gids, min,
						  max, skip_nils, "BATgroupstr_group_concat", &nils);

	if (res == GDK_SUCCEED) {
		BATsetcount(bn, ngrp);
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->tnil = nils != 0;
		bn->tnonil = nils == 0;
	}

	return bn;
}
