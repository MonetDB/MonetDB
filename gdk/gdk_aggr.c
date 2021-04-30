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
		 oid *minp, oid *maxp, BUN *ngrpp,
		 struct canditer *ci, BUN *ncand)
{
	oid min, max;
	BUN i, ngrp;
	const oid *restrict gids;

	if (b == NULL)
		return "b must exist";
	*ncand = canditer_init(ci, b, s);
	if (g) {
		if (ci->ncand != BATcount(g) ||
		    (ci->ncand != 0 && ci->seq != g->hseqbase))
			return "b with s and g must be aligned";
		assert(BATttype(g) == TYPE_oid);
	}
	if (g == NULL) {
		min = 0;
		max = 0;
		ngrp = 1;
	} else if (e == NULL) {
		/* we need to find out the min and max of g */
		const ValRecord *prop;

		prop = BATgetprop(g, GDK_MAX_VALUE);
		if (prop) {
			assert(prop->vtype == TYPE_oid);
			min = 0; /* just assume it starts at 0 */
			max = prop->val.oval;
		} else {
			min = oid_nil;	/* note that oid_nil > 0! (unsigned) */
			max = 0;
			if (BATtdense(g)) {
				min = g->tseqbase;
				max = g->tseqbase + BATcount(g) - 1;
			} else if (g->tsorted) {
				gids = (const oid *) Tloc(g, 0);
				/* find first non-nil */
				for (i = 0, ngrp = BATcount(g); i < ngrp; i++) {
					if (!is_oid_nil(gids[i])) {
						min = gids[i];
						break;
					}
				}
				if (!is_oid_nil(min)) {
					/* found a non-nil, max must be last
					 * value (and there is one!) */
					max = gids[BUNlast(g) - 1];
				}
			} else {
				/* we'll do a complete scan */
				gids = (const oid *) Tloc(g, 0);
				for (i = 0, ngrp = BATcount(g); i < ngrp; i++) {
					if (!is_oid_nil(gids[i])) {
						if (gids[i] < min)
							min = gids[i];
						if (gids[i] > max)
							max = gids[i];
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

	return NULL;
}

/* ---------------------------------------------------------------------- */
/* sum */

static inline bool
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
BUN
dofsum(const void *restrict values, oid seqb,
       struct canditer *restrict ci, BUN ncand,
       void *restrict results, BUN ngrp, int tp1, int tp2,
       const oid *restrict gids,
       oid min, oid max, bool skip_nils, bool abort_on_error,
       bool nil_if_empty)
{
	struct pergroup {
		int npartials;
		int maxpartials;
		bool valseen;
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
		pergroup[grp].valseen = false;
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
	while (ncand > 0) {
		ncand--;
		listi = canditer_next(ci) - seqb;
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
		pergroup[grp].valseen = true;
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
	GDKerror("22003!overflow in sum aggregate.\n");
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
		if (ngrp == 1 && ci->tpe == cand_dense) {		\
			/* single group, no candidate list */		\
			TYPE2 sum;					\
			*algo = "sum: no candidates, no groups";	\
			sum = 0;					\
			if (nonil) {					\
				*seen = ncand > 0;			\
				for (i = 0; i < ncand && nils == 0; i++) { \
					x = vals[ci->seq + i - seqb];	\
					ADD_WITH_CHECK(x, sum,		\
						       TYPE2, sum,	\
						       GDK_##TYPE2##_max, \
						       goto overflow);	\
				}					\
			} else {					\
				bool seenval = false;			\
				for (i = 0; i < ncand && nils == 0; i++) { \
					x = vals[ci->seq + i - seqb];	\
					if (is_##TYPE1##_nil(x)) {	\
						if (!skip_nils) {	\
							sum = TYPE2##_nil; \
							nils = 1;	\
						}			\
					} else {			\
						ADD_WITH_CHECK(x, sum,	\
							       TYPE2, sum, \
							       GDK_##TYPE2##_max, \
							       goto overflow); \
						seenval = true;		\
					}				\
				}					\
				*seen = seenval;			\
			}						\
			if (*seen)					\
				*sums = sum;				\
		} else if (ngrp == 1) {					\
			/* single group, with candidate list */		\
			TYPE2 sum;					\
			bool seenval = false;				\
			*algo = "sum: with candidates, no groups";	\
			sum = 0;					\
			for (i = 0; i < ncand && nils == 0; i++) {	\
				x = vals[canditer_next(ci) - seqb];	\
				if (is_##TYPE1##_nil(x)) {		\
					if (!skip_nils) {		\
						sum = TYPE2##_nil;	\
						nils = 1;		\
					}				\
				} else {				\
					ADD_WITH_CHECK(x, sum,		\
						       TYPE2, sum,	\
						       GDK_##TYPE2##_max, \
						       goto overflow);	\
					seenval = true;			\
				}					\
			}						\
			if (seenval)					\
				*sums = sum;				\
		} else if (ci->tpe == cand_dense) {			\
			/* multiple groups, no candidate list */	\
			*algo = "sum: no candidates, with groups";	\
			for (i = 0; i < ncand; i++) {			\
				if (gids == NULL ||			\
				    (gids[i] >= min && gids[i] <= max)) { \
					gid = gids ? gids[i] - min : (oid) i; \
					x = vals[ci->seq + i - seqb];	\
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
								x,	\
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
			*algo = "sum: with candidates, with groups";	\
			while (ncand > 0) {				\
				ncand--;				\
				i = canditer_next(ci) - seqb;		\
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
								x,	\
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

#define AGGR_SUM_NOOVL(TYPE1, TYPE2)					\
	do {								\
		TYPE1 x;						\
		const TYPE1 *restrict vals = (const TYPE1 *) values;	\
		if (ngrp == 1 && ci->tpe == cand_dense) {		\
			/* single group, no candidate list */		\
			TYPE2 sum;					\
			sum = 0;					\
			if (nonil) {					\
				*algo = "sum: no candidates, no groups, no nils, no overflow"; \
				*seen = ncand > 0;			\
				for (i = 0; i < ncand && nils == 0; i++) { \
					sum += vals[ci->seq + i - seqb]; \
				}					\
			} else {					\
				bool seenval = false;			\
				*algo = "sum: no candidates, no groups, no overflow"; \
				for (i = 0; i < ncand && nils == 0; i++) { \
					x = vals[ci->seq + i - seqb];	\
					if (is_##TYPE1##_nil(x)) {	\
						if (!skip_nils) {	\
							sum = TYPE2##_nil; \
							nils = 1;	\
						}			\
					} else {			\
						sum += x;		\
						seenval = true;		\
					}				\
				}					\
				*seen = seenval;			\
			}						\
			if (*seen)					\
				*sums = sum;				\
		} else if (ngrp == 1) {					\
			/* single group, with candidate list */		\
			TYPE2 sum;					\
			bool seenval = false;				\
			*algo = "sum: with candidates, no groups, no overflow"; \
			sum = 0;					\
			for (i = 0; i < ncand && nils == 0; i++) {	\
				x = vals[canditer_next(ci) - seqb];	\
				if (is_##TYPE1##_nil(x)) {		\
					if (!skip_nils) {		\
						sum = TYPE2##_nil;	\
						nils = 1;		\
					}				\
				} else {				\
					sum += x;			\
					seenval = true;			\
				}					\
			}						\
			if (seenval)					\
				*sums = sum;				\
		} else if (ci->tpe == cand_dense) {			\
			/* multiple groups, no candidate list */	\
			if (nonil) {					\
				*algo = "sum: no candidates, with groups, no nils, no overflow"; \
				for (i = 0; i < ncand; i++) {		\
					if (gids == NULL ||		\
					    (gids[i] >= min && gids[i] <= max)) { \
						gid = gids ? gids[i] - min : (oid) i; \
						x = vals[ci->seq + i - seqb]; \
						if (nil_if_empty &&	\
						    !(seen[gid >> 5] & (1U << (gid & 0x1F)))) { \
							seen[gid >> 5] |= 1U << (gid & 0x1F); \
							sums[gid] = 0;	\
						}			\
						sums[gid] += x;		\
					}				\
				}					\
			} else {					\
				*algo = "sum: no candidates, with groups, no overflow"; \
				for (i = 0; i < ncand; i++) {		\
					if (gids == NULL ||		\
					    (gids[i] >= min && gids[i] <= max)) { \
						gid = gids ? gids[i] - min : (oid) i; \
						x = vals[ci->seq + i - seqb]; \
						if (is_##TYPE1##_nil(x)) { \
							if (!skip_nils) { \
								sums[gid] = TYPE2##_nil; \
								nils++;	\
							}		\
						} else {		\
							if (nil_if_empty && \
							    !(seen[gid >> 5] & (1U << (gid & 0x1F)))) { \
								seen[gid >> 5] |= 1U << (gid & 0x1F); \
								sums[gid] = 0; \
							}		\
							if (!is_##TYPE2##_nil(sums[gid])) { \
								sums[gid] += x;	\
							}		\
						}			\
					}				\
				}					\
			}						\
		} else {						\
			/* multiple groups, with candidate list */	\
			*algo = "sum: with candidates, with groups, no overflow"; \
			while (ncand > 0) {				\
				ncand--;				\
				i = canditer_next(ci) - seqb;		\
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
							sums[gid] += x;	\
						}			\
					}				\
				}					\
			}						\
		}							\
	} while (0)

static BUN
dosum(const void *restrict values, bool nonil, oid seqb,
      struct canditer *restrict ci, BUN ncand,
      void *restrict results, BUN ngrp, int tp1, int tp2,
      const oid *restrict gids,
      oid min, oid max, bool skip_nils, bool abort_on_error,
      bool nil_if_empty, const char *func, const char **algo)
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
		*algo = "sum: floating point";
		return dofsum(values, seqb, ci, ncand, results, ngrp, tp1, tp2,
			      gids, min, max, skip_nils, abort_on_error,
			      nil_if_empty);
	}

	/* allocate bitmap for seen group ids */
	seen = GDKzalloc(((ngrp + 31) / 32) * sizeof(int));
	if (seen == NULL) {
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
			if (ncand < ((BUN) 1 << ((sizeof(sht) - sizeof(bte)) << 3)))
				AGGR_SUM_NOOVL(bte, sht);
			else
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
			if (ncand < ((BUN) 1 << ((sizeof(int) - sizeof(bte)) << 3)))
				AGGR_SUM_NOOVL(bte, int);
			else
				AGGR_SUM(bte, int);
			break;
		case TYPE_sht:
			if (ncand < ((BUN) 1 << ((sizeof(int) - sizeof(sht)) << 3)))
				AGGR_SUM_NOOVL(sht, int);
			else
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
#if SIZEOF_BUN == 4
		case TYPE_bte:
			AGGR_SUM_NOOVL(bte, lng);
			break;
		case TYPE_sht:
			AGGR_SUM_NOOVL(sht, lng);
			break;
		case TYPE_int:
			AGGR_SUM_NOOVL(int, lng);
			break;
#else
		case TYPE_bte:
			if (ncand < ((BUN) 1 << ((sizeof(lng) - sizeof(bte)) << 3)))
				AGGR_SUM_NOOVL(bte, lng);
			else
				AGGR_SUM(bte, lng);
			break;
		case TYPE_sht:
			if (ncand < ((BUN) 1 << ((sizeof(lng) - sizeof(sht)) << 3)))
				AGGR_SUM_NOOVL(sht, lng);
			else
				AGGR_SUM(sht, lng);
			break;
		case TYPE_int:
			if (ncand < ((BUN) 1 << ((sizeof(lng) - sizeof(int)) << 3)))
				AGGR_SUM_NOOVL(int, lng);
			else
				AGGR_SUM(int, lng);
			break;
#endif
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
			AGGR_SUM_NOOVL(bte, hge);
			break;
		case TYPE_sht:
			AGGR_SUM_NOOVL(sht, hge);
			break;
		case TYPE_int:
			AGGR_SUM_NOOVL(int, hge);
			break;
		case TYPE_lng:
			AGGR_SUM_NOOVL(lng, hge);
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
	GDKerror("22003!overflow in sum aggregate.\n");
	return BUN_NONE;
}

/* calculate group sums with optional candidates list */
BAT *
BATgroupsum(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, bool abort_on_error)
{
	const oid *restrict gids;
	oid min, max;
	BUN ngrp;
	BUN nils;
	BAT *bn;
	struct canditer ci;
	BUN ncand;
	const char *err;
	const char *algo = NULL;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("b and g must be aligned\n");
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
		return BATconvert(b, s, tp, abort_on_error, 0, 0, 0);
	}

	bn = BATconstant(min, tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	if (bn == NULL) {
		return NULL;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, 0);

	nils = dosum(Tloc(b, 0), b->tnonil, b->hseqbase, &ci, ncand,
		     Tloc(bn, 0), ngrp, b->ttype, tp, gids, min, max,
		     skip_nils, abort_on_error, true, __func__, &algo);

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

	if (algo)
		MT_thread_setalgorithm(algo);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOOPTBATFMT ","
		  "e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
		  "; start " OIDFMT ", count " BUNFMT " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(bn),
		  ci.seq, ncand, algo ? algo : "", GDKusec() - t0);
	return bn;
}

static BUN
mskCountOnes(BAT *b, struct canditer *ci)
{
	BUN cnt = 0, ncand = ci->ncand;

	if (ci->s == NULL && mask_cand(b))
		return BATcount(b);
	if (ci->tpe == cand_dense && BATcount(b) && !mask_cand(b)) {
		const uint32_t *restrict src = Tloc(b, (ci->seq - b->hseqbase) / 32);
		int bits = (ci->seq - b->hseqbase) % 32;
		if (bits + ncand <= 32) {
			if (ncand == 32)
				return candmask_pop(src[0]);
			return candmask_pop(src[0] & (((1U << ncand) - 1) << bits));
		}
		if (bits != 0) {
			cnt = candmask_pop(src[0] & (~0U << bits));
			src++;
			ncand -= 32 - bits;
		}
		while (ncand >= 32) {
			cnt += candmask_pop(*src);
			src++;
			ncand -= 32;
		}
		if (ncand > 0)
			cnt += candmask_pop(*src & ((1U << ncand) - 1));
		return cnt;
	}
	for (BUN i = 0; i < ci->ncand; i++) {
		BUN x = canditer_next(ci) - b->hseqbase;
		cnt += mskGetVal(b, x);
	}
	return cnt;
}

gdk_return
BATsum(void *res, int tp, BAT *b, BAT *s, bool skip_nils, bool abort_on_error, bool nil_if_empty)
{
	oid min, max;
	BUN ngrp;
	BUN nils;
	struct canditer ci;
	BUN ncand;
	const char *err;
	const char *algo = NULL;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(b, NULL, NULL, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return GDK_FAIL;
	}
	if (ATOMstorage(b->ttype) == TYPE_msk || mask_cand(b)) {
		ncand = mskCountOnes(b, &ci);
		switch (tp) {
		case TYPE_bte:
			if (ncand > GDK_bte_max) {
				GDKerror("22003!overflow in sum aggregate.\n");
				return GDK_FAIL;
			}
			* (bte *) res = (bte) ncand;
			break;
		case TYPE_sht:
			if (ncand > GDK_sht_max) {
				GDKerror("22003!overflow in sum aggregate.\n");
				return GDK_FAIL;
			}
			* (sht *) res = (sht) ncand;
			break;
		case TYPE_int:
#if SIZEOF_BUN > 4
			if (ncand > GDK_int_max) {
				GDKerror("22003!overflow in sum aggregate.\n");
				return GDK_FAIL;
			}
#endif
			* (int *) res = (int) ncand;
			break;
		case TYPE_lng:
			* (lng *) res = (lng) ncand;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			* (hge *) res = (hge) ncand;
			break;
#endif
		case TYPE_flt:
			* (flt *) res = (flt) ncand;
			break;
		case TYPE_dbl:
			* (dbl *) res = (dbl) ncand;
			break;
		default:
			GDKerror("type combination (sum(%s)->%s) not supported.\n",
				 ATOMname(b->ttype), ATOMname(tp));
			return GDK_FAIL;
		}
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT "; "
			  "start " OIDFMT ", count " BUNFMT " (pop count -- " LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s),
			  ci.seq, ci.ncand, GDKusec() - t0);
		return GDK_SUCCEED;
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

			if (BATcalcavg(b, s, &avg, &cnt, 0) != GDK_SUCCEED)
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
						GDKerror("22003!overflow in sum aggregate.\n");
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
						GDKerror("22003!overflow in sum aggregate.\n");
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
		GDKerror("type combination (sum(%s)->%s) not supported.\n",
			 ATOMname(b->ttype), ATOMname(tp));
		return GDK_FAIL;
	}
	if (BATcount(b) == 0)
		return GDK_SUCCEED;
	nils = dosum(Tloc(b, 0), b->tnonil, b->hseqbase, &ci, ncand,
		     res, true, b->ttype, tp, &min, min, max,
		     skip_nils, abort_on_error, nil_if_empty, __func__, &algo);
	if (algo)
		MT_thread_setalgorithm(algo);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT "; "
		  "start " OIDFMT ", count " BUNFMT " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ci.seq, ncand, algo ? algo : "", GDKusec() - t0);
	return nils < BUN_NONE ? GDK_SUCCEED : GDK_FAIL;
}

/* ---------------------------------------------------------------------- */
/* product */

#define AGGR_PROD(TYPE1, TYPE2, TYPE3)					\
	do {								\
		const TYPE1 *restrict vals = (const TYPE1 *) values;	\
		gid = 0;	/* doesn't change if gidincr == false */ \
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(ci) - seqb;			\
			if (gids == NULL || !gidincr ||			\
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
							vals[i],	\
							prods[gid],	\
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
		gid = 0;	/* doesn't change if gidincr == false */ \
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(ci) - seqb;			\
			if (gids == NULL || !gidincr ||			\
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
					HGEMUL_CHECK(vals[i],		\
						     prods[gid],	\
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
		gid = 0;	/* doesn't change if gidincr == false */ \
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(ci) - seqb;			\
			if (gids == NULL || !gidincr ||			\
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
							vals[i],	\
							prods[gid],	\
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
		gid = 0;	/* doesn't change if gidincr == false */ \
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(ci) - seqb;			\
			if (gids == NULL || !gidincr ||			\
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
							nils++;		\
						} else {		\
							prods[gid] *= vals[i]; \
						}			\
					}				\
				}					\
			}						\
		}							\
	} while (0)

static BUN
doprod(const void *restrict values, oid seqb, struct canditer *restrict ci, BUN ncand,
       void *restrict results, BUN ngrp, int tp1, int tp2,
       const oid *restrict gids, bool gidincr, oid min, oid max,
       bool skip_nils, bool abort_on_error, bool nil_if_empty, const char *func)
{
	BUN nils = 0;
	BUN i;
	oid gid;
	unsigned int *restrict seen; /* bitmask for groups that we've seen */

	/* allocate bitmap for seen group ids */
	seen = GDKzalloc(((ngrp + 31) / 32) * sizeof(int));
	if (seen == NULL) {
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
	GDKerror("22003!overflow in product aggregate.\n");
	return BUN_NONE;
}

/* calculate group products with optional candidates list */
BAT *
BATgroupprod(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, bool abort_on_error)
{
	const oid *restrict gids;
	oid min, max;
	BUN ngrp;
	BUN nils;
	BAT *bn;
	struct canditer ci;
	BUN ncand;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("b and g must be aligned\n");
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
		return BATconvert(b, s, tp, abort_on_error, 0, 0, 0);
	}

	bn = BATconstant(min, tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	if (bn == NULL) {
		return NULL;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, 0);

	nils = doprod(Tloc(b, 0), b->hseqbase, &ci, ncand, Tloc(bn, 0), ngrp,
		      b->ttype, tp, gids, true, min, max, skip_nils,
		      abort_on_error, true, __func__);

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

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOOPTBATFMT ","
		  "e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
		  "; start " OIDFMT ", count " BUNFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(bn),
		  ci.seq, ncand, GDKusec() - t0);

	return bn;
}

gdk_return
BATprod(void *res, int tp, BAT *b, BAT *s, bool skip_nils, bool abort_on_error, bool nil_if_empty)
{
	oid min, max;
	BUN ngrp;
	BUN nils;
	struct canditer ci;
	BUN ncand;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(b, NULL, NULL, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
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
		GDKerror("type combination (prod(%s)->%s) not supported.\n",
			 ATOMname(b->ttype), ATOMname(tp));
		return GDK_FAIL;
	}
	if (BATcount(b) == 0)
		return GDK_SUCCEED;
	nils = doprod(Tloc(b, 0), b->hseqbase, &ci, ncand, res, true,
		      b->ttype, tp, &min, false, min, max,
		      skip_nils, abort_on_error, nil_if_empty, __func__);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT "; "
		  "start " OIDFMT ", count " BUNFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ci.seq, ncand, GDKusec() - t0);
	return nils < BUN_NONE ? GDK_SUCCEED : GDK_FAIL;
}

/* ---------------------------------------------------------------------- */
/* average */

#define AGGR_AVG(TYPE)							\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		TYPE *restrict avgs = GDKzalloc(ngrp * sizeof(TYPE));	\
		if (avgs == NULL)					\
			goto alloc_fail;				\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - b->hseqbase;		\
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
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - b->hseqbase;		\
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
BATgroupavg(BAT **bnp, BAT **cntsp, BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, bool abort_on_error, int scale)
{
	const oid *restrict gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	BUN nils = 0;
	lng *restrict rems = NULL;
	lng *restrict cnts = NULL;
	dbl *restrict dbls;
	BAT *bn = NULL, *cn = NULL;
	struct canditer ci;
	BUN ncand;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(tp == TYPE_dbl);
	(void) tp;		/* compatibility (with other BATgroup*
				 * functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return GDK_FAIL;
	}
	if (g == NULL) {
		GDKerror("b and g must be aligned\n");
		return GDK_FAIL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no averages, so return bat aligned with g
		 * with nil in the tail */
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &dbl_nil, ngrp, TRANSIENT);
		if (bn == NULL) {
			return GDK_FAIL;
		}
		if (cntsp) {
			lng zero = 0;
			if ((cn = BATconstant(ngrp == 0 ? 0 : min, TYPE_lng, &zero, ngrp, TRANSIENT)) == NULL) {
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
		if ((bn = BATconvert(b, s, TYPE_dbl, abort_on_error, 0, 0, 0)) == NULL)
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
		rems = GDKzalloc(ngrp * sizeof(lng));
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
		gids = (const oid *) Tloc(g, 0);

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
		GDKerror("type (%s) not supported.\n", ATOMname(b->ttype));
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
		cn->tnil = false;
		cn->tnonil = true;
		*cntsp = cn;
	}
	if (scale != 0) {
		dbl fac = pow(10.0, (double) scale);
		for (i = 0; i < ngrp; i++) {
			if (!is_dbl_nil(dbls[i]))
				dbls[i] *= fac;
		}
	}
	BATsetcount(bn, ngrp);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	*bnp = bn;
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOOPTBATFMT ","
		  "e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
		  "; start " OIDFMT ", count " BUNFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(bn),
		  ci.seq, ncand, GDKusec() - t0);
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
	return GDK_FAIL;
}

/* An exact numeric average of a bunch of values consists of three parts: the
 * average rounded down (towards minus infinity), the number of values that
 * participated in the calculation, and the remainder.  The remainder is in the
 * range 0 (inclusive) to count (not inclusive).  BATgroupavg3 calculates these
 * values for each given group.  The function below, BATgroupavg3combine,
 * combines averages calculated this way to correct, rounded or truncated
 * towards zero (depending on the symbol TRUNCATE_NUMBERS) averages. */
gdk_return
BATgroupavg3(BAT **avgp, BAT **remp, BAT **cntp, BAT *b, BAT *g, BAT *e, BAT *s, bool skip_nils)
{
	const char *err;
	oid min, max;
	BUN ngrp;
	struct canditer ci;
	BUN ncand;
	BAT *bn, *rn, *cn;
	BUN i;
	oid o;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return GDK_FAIL;
	}
	if (ncand == 0 || ngrp == 0) {
		if (ngrp == 0)
			min = 0;
		bn = BATconstant(min, b->ttype, ATOMnilptr(b->ttype),
				 ngrp, TRANSIENT);
		rn = BATconstant(min, TYPE_lng, &lng_nil, ngrp, TRANSIENT);
		cn = BATconstant(min, TYPE_lng, &(lng){0}, ngrp, TRANSIENT);
		if (bn == NULL || rn == NULL || cn == NULL) {
			BBPreclaim(bn);
			BBPreclaim(rn);
			BBPreclaim(cn);
			return GDK_FAIL;
		}
		*avgp = bn;
		*remp = rn;
		*cntp = cn;
		return GDK_SUCCEED;
	}
	ValRecord zero;
	(void) VALinit(&zero, TYPE_bte, &(bte){0});
	bn = BATconstant(min, b->ttype, VALconvert(b->ttype, &zero),
			 ngrp, TRANSIENT);
	rn = BATconstant(min, TYPE_lng, &(lng){0}, ngrp, TRANSIENT);
	cn = BATconstant(min, TYPE_lng, &(lng){0}, ngrp, TRANSIENT);
	if (bn == NULL || rn == NULL || cn == NULL) {
		BBPreclaim(bn);
		BBPreclaim(rn);
		BBPreclaim(cn);
		return GDK_FAIL;
	}
	lng *rems = Tloc(rn, 0);
	lng *cnts = Tloc(cn, 0);
	const oid *gids = g && !BATtdense(g) ? Tloc(g, 0) : NULL;
	oid gid = ngrp == 1 && gids ? gids[0] - min : 0;

	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte: {
		const bte *vals = Tloc(b, 0);
		bte *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			o = canditer_next(&ci) - b->hseqbase;
			if (ngrp > 1)
				gid = gids ? gids[o] - min : o;
			if (is_bte_nil(vals[o])) {
				if (!skip_nils) {
					avgs[gid] = bte_nil;
					rems[gid] = lng_nil;
					cnts[gid] = lng_nil;
					bn->tnil = true;
					rn->tnil = true;
					cn->tnil = true;
				}
			} else if (!is_lng_nil(cnts[gid])) {
				AVERAGE_ITER(bte, vals[o], avgs[gid], rems[gid], cnts[gid]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = bte_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_bte_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0) {
				avgs[i]++;
				rems[i] -= cnts[i];
			}
#else
			if (!is_bte_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				} else {
					if (2*rems[i] >= cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				}
			}
#endif
		}
		break;
	}
	case TYPE_sht: {
		const sht *vals = Tloc(b, 0);
		sht *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			o = canditer_next(&ci) - b->hseqbase;
			if (ngrp > 1)
				gid = gids ? gids[o] - min : o;
			if (is_sht_nil(vals[o])) {
				if (!skip_nils) {
					avgs[gid] = sht_nil;
					rems[gid] = lng_nil;
					cnts[gid] = lng_nil;
					bn->tnil = true;
					rn->tnil = true;
					cn->tnil = true;
				}
			} else if (!is_lng_nil(cnts[gid])) {
				AVERAGE_ITER(sht, vals[o], avgs[gid], rems[gid], cnts[gid]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = sht_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_sht_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0) {
				avgs[i]++;
				rems[i] -= cnts[i];
			}
#else
			if (!is_sht_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				} else {
					if (2*rems[i] >= cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				}
			}
#endif
		}
		break;
	}
	case TYPE_int: {
		const int *vals = Tloc(b, 0);
		int *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			o = canditer_next(&ci) - b->hseqbase;
			if (ngrp > 1)
				gid = gids ? gids[o] - min : o;
			if (is_int_nil(vals[o])) {
				if (!skip_nils) {
					avgs[gid] = int_nil;
					rems[gid] = lng_nil;
					cnts[gid] = lng_nil;
					bn->tnil = true;
					rn->tnil = true;
					cn->tnil = true;
				}
			} else if (!is_lng_nil(cnts[gid])) {
				AVERAGE_ITER(int, vals[o], avgs[gid], rems[gid], cnts[gid]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = int_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_int_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0) {
				avgs[i]++;
				rems[i] -= cnts[i];
			}
#else
			if (!is_int_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				} else {
					if (2*rems[i] >= cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				}
			}
#endif
		}
		break;
	}
	case TYPE_lng: {
		const lng *vals = Tloc(b, 0);
		lng *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			o = canditer_next(&ci) - b->hseqbase;
			if (ngrp > 1)
				gid = gids ? gids[o] - min : o;
			if (is_lng_nil(vals[o])) {
				if (!skip_nils) {
					avgs[gid] = lng_nil;
					rems[gid] = lng_nil;
					cnts[gid] = lng_nil;
					bn->tnil = true;
					rn->tnil = true;
					cn->tnil = true;
				}
			} else if (!is_lng_nil(cnts[gid])) {
				AVERAGE_ITER(lng, vals[o], avgs[gid], rems[gid], cnts[gid]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = lng_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_lng_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0) {
				avgs[i]++;
				rems[i] -= cnts[i];
			}
#else
			if (!is_lng_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				} else {
					if (2*rems[i] >= cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				}
			}
#endif
		}
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		const hge *vals = Tloc(b, 0);
		hge *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			o = canditer_next(&ci) - b->hseqbase;
			if (ngrp > 1)
				gid = gids ? gids[o] - min : o;
			if (is_hge_nil(vals[o])) {
				if (!skip_nils) {
					avgs[gid] = hge_nil;
					rems[gid] = lng_nil;
					cnts[gid] = lng_nil;
					bn->tnil = true;
					rn->tnil = true;
					cn->tnil = true;
				}
			} else if (!is_lng_nil(cnts[gid])) {
				AVERAGE_ITER(hge, vals[o], avgs[gid], rems[gid], cnts[gid]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = hge_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_hge_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0) {
				avgs[i]++;
				rems[i] -= cnts[i];
			}
#else
			if (!is_hge_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				} else {
					if (2*rems[i] >= cnts[i]) {
						avgs[i]++;
						rems[i] -= cnts[i];
					}
				}
			}
#endif
		}
		break;
	}
#endif
	}
	BATsetcount(bn, ngrp);
	BATsetcount(rn, ngrp);
	BATsetcount(cn, ngrp);
	bn->tnonil = !bn->tnil;
	rn->tnonil = !rn->tnil;
	cn->tnonil = !cn->tnil;
	bn->tkey = rn->tkey = cn->tkey = ngrp == 1;
	bn->tsorted = rn->tsorted = cn->tsorted = ngrp == 1;
	bn->trevsorted = rn->trevsorted = cn->trevsorted = ngrp == 1;
	*avgp = bn;
	*remp = rn;
	*cntp = cn;
	return GDK_SUCCEED;
}

#ifdef HAVE_HGE
#define BIGINT uhge
#else
#define BIGINT uint64_t
#endif
/* calculate the values (a*b)/c and (a*b)%c but without causing overflow
 *
 * this function is from https://stackoverflow.com/a/8757419, only with the
 * slight addition of returning the remainder and changing the first argument
 * and return value to BIGINT */
static inline BIGINT
multdiv(BIGINT a, uint64_t b, uint64_t c, uint64_t *rem)
{
	static BIGINT const base = ((BIGINT)1)<<(sizeof(BIGINT)*4);
//	static BIGINT const maxdiv = (base-1)*base + (base-1);
	static BIGINT const maxdiv = ~(BIGINT)0;

	// First get the easy thing
	BIGINT res = (a/c) * b + (a%c) * (b/c);
	a %= c;
	b %= c;
	// Are we done?
	if (a == 0 || b == 0) {
		*rem = 0;
		return res;
	}
	// Is it easy to compute what remain to be added?
	if (c < base) {
		*rem = (uint64_t) ((a*b)%c);
		return res + (a*b/c);
	}
	// Now 0 < a < c, 0 < b < c, c >= 1ULL
	// Normalize
	BIGINT norm = maxdiv/c;
	BIGINT cbig = c * norm;	// orig: c *= norm; and below cbig was plain c
	a *= norm;
	// split into 2 digits
	BIGINT ah = a / base, al = a % base;
	BIGINT bh = b / base, bl = b % base;
	BIGINT ch = cbig / base, cl = cbig % base;
	// compute the product
	BIGINT p0 = al*bl;
	BIGINT p1 = p0 / base + al*bh;
	p0 %= base;
	BIGINT p2 = p1 / base + ah*bh;
	p1 = (p1 % base) + ah * bl;
	p2 += p1 / base;
	p1 %= base;
	// p2 holds 2 digits, p1 and p0 one

	// first digit is easy, not null only in case of overflow
	//BIGINT q2 = p2 / cbig;
	p2 = p2 % cbig;

	// second digit, estimate
	BIGINT q1 = p2 / ch;
	// and now adjust
	BIGINT rhat = p2 % ch;
	// the loop can be unrolled, it will be executed at most twice for
	// even bases -- three times for odd one -- due to the normalisation above
	while (q1 >= base || (rhat < base && q1*cl > rhat*base+p1)) {
		q1--;
		rhat += ch;
	}
	// subtract
	p1 = ((p2 % base) * base + p1) - q1 * cl;
	p2 = (p2 / base * base + p1 / base) - q1 * ch;
	p1 = p1 % base + (p2 % base) * base;

	// now p1 hold 2 digits, p0 one and p2 is to be ignored
	BIGINT q0 = p1 / ch;
	rhat = p1 % ch;
	while (q0 >= base || (rhat < base && q0*cl > rhat*base+p0)) {
		q0--;
		rhat += ch;
	}
	// subtract
	p0 = ((p1 % base) * base + p0) - q0 * cl;
	p1 = (p1 / base * base + p0 / base) - q0 * ch;
	p0 = p0 % base + (p1 % base) * base;

	*rem = p0 / norm;
	return res + q0 + q1 * base; // + q2 *base*base
}

static inline void
combine_averages_bte(bte *avgp, lng *remp, lng *cntp,
		     bte avg1, lng rem1, lng cnt1,
		     bte avg2, lng rem2, lng cnt2)
{
	lng cnt = cnt1 + cnt2;

	if (rem2 < 0) {
		avg2--;
		rem2 += cnt2;
	}
	*cntp = cnt;
	lng v = avg1 * cnt1 + rem1 + avg2 * cnt2 + rem2;
	bte a = (bte) (v / cnt);
	v %= cnt;
	if (v < 0) {
		a--;
		v += cnt;
	}
	*avgp = a;
	*remp = v;
}

static inline void
combine_averages_sht(sht *avgp, lng *remp, lng *cntp,
		     sht avg1, lng rem1, lng cnt1,
		     sht avg2, lng rem2, lng cnt2)
{
	lng cnt = cnt1 + cnt2;

	if (rem2 < 0) {
		avg2--;
		rem2 += cnt2;
	}
	*cntp = cnt;
	lng v = avg1 * cnt1 + rem1 + avg2 * cnt2 + rem2;
	sht a = (sht) (v / cnt);
	v %= cnt;
	if (v < 0) {
		a--;
		v += cnt;
	}
	*avgp = a;
	*remp = v;
}


static inline void
combine_averages_int(int *avgp, lng *remp, lng *cntp,
		     int avg1, lng rem1, lng cnt1,
		     int avg2, lng rem2, lng cnt2)
{
	lng cnt = cnt1 + cnt2;

	if (rem2 < 0) {
		avg2--;
		rem2 += cnt2;
	}
	*cntp = cnt;
#ifdef HAVE_HGE
	hge v = avg1 * cnt1 + rem1 + avg2 * cnt2 + rem2;
	int a = (int) (v / cnt);
	v %= cnt;
	if (v < 0) {
		a--;
		v += cnt;
	}
	*avgp = a;
	*remp = (lng) v;
#else
	if (cnt1 == 0) {
		avg1 = 0;
		rem1 = 0;
	}
	lng rem = rem1 + rem2;
	lng v;
	uint64_t r;
	if (avg1 < 0) {
		avg1 = (int) multdiv((uint64_t) -avg1, cnt1, cnt, &r);
		if (r > 0) {
			avg1 = -avg1 - 1;
			r = cnt - r;
		} else {
			avg1 = -avg1;
		}
	} else {
		avg1 = (int) multdiv((uint64_t) avg1, cnt1, cnt, &r);
	}
	v = avg1;
	rem += r;
	if (avg2 < 0) {
		avg2 = (int) multdiv((uint64_t) -avg2, cnt2, cnt, &r);
		if (r > 0) {
			avg2 = -avg2 - 1;
			r = cnt - r;
		} else {
			avg2 = -avg2;
		}
	} else {
		avg2 = (int) multdiv((uint64_t) avg2, cnt2, cnt, &r);
	}
	v += avg2;
	rem += r;
	while (rem >= cnt) { /* max twice */
		v++;
		rem -= cnt;
	}
	*avgp = (int) v;
	*remp = rem;
#endif
}

static inline void
combine_averages_lng(lng *avgp, lng *remp, lng *cntp,
		     lng avg1, lng rem1, lng cnt1,
		     lng avg2, lng rem2, lng cnt2)
{
	lng cnt = cnt1 + cnt2;

	if (rem2 < 0) {
		avg2--;
		rem2 += cnt2;
	}
	*cntp = cnt;
#ifdef HAVE_HGE
	hge v = avg1 * cnt1 + rem1 + avg2 * cnt2 + rem2;
	lng a = (lng) (v / cnt);
	v %= cnt;
	if (v < 0) {
		a--;
		v += cnt;
	}
	*avgp = a;
	*remp = (lng) v;
#else
	if (cnt1 == 0) {
		avg1 = 0;
		rem1 = 0;
	}
	lng rem = rem1 + rem2;
	lng v;
	uint64_t r;
	if (avg1 < 0) {
		avg1 = (lng) multdiv((uint64_t) -avg1, cnt1, cnt, &r);
		if (r > 0) {
			avg1 = -avg1 - 1;
			r = cnt - r;
		} else {
			avg1 = -avg1;
		}
	} else {
		avg1 = (lng) multdiv((uint64_t) avg1, cnt1, cnt, &r);
	}
	v = avg1;
	rem += r;
	if (avg2 < 0) {
		avg2 = (lng) multdiv((uint64_t) -avg2, cnt2, cnt, &r);
		if (r > 0) {
			avg2 = -avg2 - 1;
			r = cnt - r;
		} else {
			avg2 = -avg2;
		}
	} else {
		avg2 = (lng) multdiv((uint64_t) avg2, cnt2, cnt, &r);
	}
	v += avg2;
	rem += r;
	while (rem >= cnt) { /* max twice */
		v++;
		rem -= cnt;
	}
	*avgp = v;
	*remp = rem;
#endif
}

#ifdef HAVE_HGE
static inline void
combine_averages_hge(hge *avgp, lng *remp, lng *cntp,
		     hge avg1, lng rem1, lng cnt1,
		     hge avg2, lng rem2, lng cnt2)
{
	if (cnt1 == 0) {
		avg1 = 0;
		rem1 = 0;
	}
	if (rem2 < 0) {
		avg2--;
		rem2 += cnt2;
	}
	lng cnt = cnt1 + cnt2;
	lng rem = rem1 + rem2;
	hge v;
	uint64_t r;

	*cntp = cnt;
	if (avg1 < 0) {
		avg1 = (hge) multdiv((uhge) -avg1, cnt1, cnt, &r);
		if (r > 0) {
			avg1 = -avg1 - 1;
			r = cnt - r;
		} else {
			avg1 = -avg1;
		}
	} else {
		avg1 = (hge) multdiv((uhge) avg1, cnt1, cnt, &r);
	}
	v = avg1;
	rem += r;
	if (avg2 < 0) {
		avg2 = (hge) multdiv((uhge) -avg2, cnt2, cnt, &r);
		if (r > 0) {
			avg2 = -avg2 - 1;
			r = cnt - r;
		} else {
			avg2 = -avg2;
		}
	} else {
		avg2 = (hge) multdiv((uhge) avg2, cnt2, cnt, &r);
	}
	v += avg2;
	rem += r;
	while (rem >= cnt) { /* max twice */
		v++;
		rem -= cnt;
	}
	*avgp = v;
	*remp = rem;
}
#endif

BAT *
BATgroupavg3combine(BAT *avg, BAT *rem, BAT *cnt, BAT *g, BAT *e, bool skip_nils)
{
	const char *err;
	oid min, max;
	BUN ngrp;
	struct canditer ci;
	BUN i, ncand;
	BAT *bn, *rn, *cn;

	if ((err = BATgroupaggrinit(avg, g, e, NULL, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	assert(ci.tpe == cand_dense);
	if (BATcount(avg) != BATcount(rem) || BATcount(avg) != BATcount(cnt)) {
		GDKerror("input bats not aligned");
		return NULL;
	}
	if (ncand == 0 || ngrp == 0) {
		return BATconstant(ngrp == 0 ? 0 : min, avg->ttype,
				   ATOMnilptr(avg->ttype), ngrp, TRANSIENT);
	}
	ValRecord zero;
	(void) VALinit(&zero, TYPE_bte, &(bte){0});
	bn = BATconstant(min, avg->ttype, VALconvert(avg->ttype, &zero),
			 ngrp, TRANSIENT);
	/* rn and cn are temporary storage of intermediates */
	rn = BATconstant(min, TYPE_lng, &(lng){0}, ngrp, TRANSIENT);
	cn = BATconstant(min, TYPE_lng, &(lng){0}, ngrp, TRANSIENT);
	if (bn == NULL || rn == NULL || cn == NULL) {
		BBPreclaim(bn);
		BBPreclaim(rn);
		BBPreclaim(cn);
		return NULL;
	}
	lng *rems = Tloc(rn, 0);
	lng *cnts = Tloc(cn, 0);
	const lng *orems = Tloc(rem, 0);
	const lng *ocnts = Tloc(cnt, 0);
	const oid *gids = g && !BATtdense(g) ? Tloc(g, 0) : NULL;
	oid gid = ngrp == 1 && gids ? gids[0] - min : 0;

	switch (ATOMbasetype(avg->ttype)) {
	case TYPE_bte: {
		const bte *vals = Tloc(avg, 0);
		bte *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			if (ngrp > 1)
				gid = gids ? gids[i] - min : i;
			if (is_bte_nil(vals[i])) {
				if (!skip_nils) {
					avgs[gid] = bte_nil;
					bn->tnil = true;
				}
			} else if (!is_bte_nil(avgs[gid])) {
				combine_averages_bte(&avgs[gid], &rems[gid],
						     &cnts[gid], avgs[gid],
						     rems[gid], cnts[gid],
						     vals[i], orems[i],
						     ocnts[i]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = bte_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_bte_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0)
				avgs[i]++;
#else
			if (!is_bte_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i])
						avgs[i]++;
				} else {
					if (2*rems[i] >= cnts[i])
						avgs[i]++;
				}
			}
#endif
		}
		break;
	}
	case TYPE_sht: {
		const sht *vals = Tloc(avg, 0);
		sht *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			if (ngrp > 1)
				gid = gids ? gids[i] - min : i;
			if (is_sht_nil(vals[i])) {
				if (!skip_nils) {
					avgs[gid] = sht_nil;
					bn->tnil = true;
				}
			} else if (!is_sht_nil(avgs[gid])) {
				combine_averages_sht(&avgs[gid], &rems[gid],
						     &cnts[gid], avgs[gid],
						     rems[gid], cnts[gid],
						     vals[i], orems[i],
						     ocnts[i]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = sht_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_sht_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0)
				avgs[i]++;
#else
			if (!is_sht_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i])
						avgs[i]++;
				} else {
					if (2*rems[i] >= cnts[i])
						avgs[i]++;
				}
			}
#endif
		}
		break;
	}
	case TYPE_int: {
		const int *vals = Tloc(avg, 0);
		int *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			if (ngrp > 1)
				gid = gids ? gids[i] - min : i;
			if (is_int_nil(vals[i])) {
				if (!skip_nils) {
					avgs[gid] = int_nil;
					bn->tnil = true;
				}
			} else if (!is_int_nil(avgs[gid])) {
				combine_averages_int(&avgs[gid], &rems[gid],
						     &cnts[gid], avgs[gid],
						     rems[gid], cnts[gid],
						     vals[i], orems[i],
						     ocnts[i]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = int_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_int_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0)
				avgs[i]++;
#else
			if (!is_int_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i])
						avgs[i]++;
				} else {
					if (2*rems[i] >= cnts[i])
						avgs[i]++;
				}
			}
#endif
		}
		break;
	}
	case TYPE_lng: {
		const lng *vals = Tloc(avg, 0);
		lng *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			if (ngrp > 1)
				gid = gids ? gids[i] - min : i;
			if (is_lng_nil(vals[i])) {
				if (!skip_nils) {
					avgs[gid] = lng_nil;
					bn->tnil = true;
				}
			} else if (!is_lng_nil(avgs[gid])) {
				combine_averages_lng(&avgs[gid], &rems[gid],
						     &cnts[gid], avgs[gid],
						     rems[gid], cnts[gid],
						     vals[i], orems[i],
						     ocnts[i]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = lng_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_lng_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0)
				avgs[i]++;
#else
			if (!is_lng_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i])
						avgs[i]++;
				} else {
					if (2*rems[i] >= cnts[i])
						avgs[i]++;
				}
			}
#endif
		}
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		const hge *vals = Tloc(avg, 0);
		hge *avgs = Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			if (ngrp > 1)
				gid = gids ? gids[i] - min : i;
			if (is_hge_nil(vals[i])) {
				if (!skip_nils) {
					avgs[gid] = hge_nil;
					bn->tnil = true;
				}
			} else if (!is_hge_nil(avgs[gid])) {
				combine_averages_hge(&avgs[gid], &rems[gid],
						     &cnts[gid], avgs[gid],
						     rems[gid], cnts[gid],
						     vals[i], orems[i],
						     ocnts[i]);
			}
		}
		for (i = 0; i < ngrp; i++) {
			if (cnts[i] == 0) {
				avgs[i] = hge_nil;
				bn->tnil = true;
			} else
#ifdef TRUNCATE_NUMBERS
			if (!is_hge_nil(avgs[i]) && rems[i] > 0 && avgs[i] < 0)
				avgs[i]++;
#else
			if (!is_hge_nil(avgs[i]) && rems[i] > 0) {
				if (avgs[i] < 0) {
					if (2*rems[i] > cnts[i])
						avgs[i]++;
				} else {
					if (2*rems[i] >= cnts[i])
						avgs[i]++;
				}
			}
#endif
		}
		break;
	}
#endif
	}
	BBPreclaim(rn);
	BBPreclaim(cn);
	BATsetcount(bn, ngrp);
	bn->tnonil = !bn->tnil;
	bn->tkey = ngrp == 1;
	bn->tsorted = ngrp == 1;
	bn->trevsorted = ngrp == 1;
	return bn;
}

#define AVERAGE_TYPE_LNG_HGE(TYPE,lng_hge)				\
	do {								\
		TYPE x, a;						\
									\
		/* first try to calculate the sum of all values into a */ \
		/* lng/hge */						\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - b->hseqbase;		\
			x = ((const TYPE *) src)[i];			\
			if (is_##TYPE##_nil(x))				\
				continue;				\
			ADD_WITH_CHECK(x, sum,				\
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
				a = (TYPE) (sum / n); /* this fits */	\
				r = (lng) (sum % n);			\
			} else {					\
				sum = -sum;				\
				a = - (TYPE) (sum / n); /* this fits */ \
				r = (lng) (sum % n);		\
				if (r) {				\
					a--;				\
					r = n - r;			\
				}					\
			}						\
			while (ncand > 0) {				\
				/* loop invariant: */			\
				/* a + r/n == average(x[0],...,x[n]); */ \
				/* 0 <= r < n */			\
				ncand--;				\
				i = canditer_next(&ci) - b->hseqbase;	\
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
		while (ncand > 0) {				\
			ncand--;				\
			i = canditer_next(&ci) - b->hseqbase;	\
			x = ((const TYPE *) src)[i];		\
			if (is_##TYPE##_nil(x))			\
				continue;			\
			AVERAGE_ITER_FLOAT(TYPE, x, a, n);	\
		}						\
		*avg = n > 0 ? a : dbl_nil;			\
	} while (0)

gdk_return
BATcalcavg(BAT *b, BAT *s, dbl *avg, BUN *vals, int scale)
{
	lng n = 0, r = 0;
	BUN i = 0;
#ifdef HAVE_HGE
	hge sum = 0;
#else
	lng sum = 0;
#endif
	struct canditer ci;
	BUN ncand;
	const void *restrict src;
	/* these two needed for ADD_WITH_CHECK macro */
	bool abort_on_error = true;
	BUN nils = 0;

	ncand = canditer_init(&ci, b, s);

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
		GDKerror("average of type %s unsupported.\n",
			 ATOMname(b->ttype));
		return GDK_FAIL;
	}
	if (scale != 0 && !is_dbl_nil(*avg))
		*avg *= pow(10.0, (double) scale);
	if (vals)
		*vals = (BUN) n;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* count */

#define AGGR_COUNT(TYPE)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - b->hseqbase;		\
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
BATgroupcount(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, bool abort_on_error)
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
	struct canditer ci;
	BUN ncand;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(tp == TYPE_lng);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("b and g must be aligned\n");
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
		gids = (const oid *) Tloc(g, 0);

	if (!skip_nils || b->tnonil) {
		/* if nils are nothing special, or if there are no
		 * nils, we don't need to look at the values at all */
		if (gids) {
			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - b->hseqbase;
				if (gids[i] >= min && gids[i] <= max)
					cnts[gids[i] - min]++;
			}
		} else {
			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - b->hseqbase;
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

			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - b->hseqbase;
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
	bn->tnil = false;
	bn->tnonil = true;
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOOPTBATFMT ","
		  "e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
		  "; start " OIDFMT ", count " BUNFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(bn),
		  ci.seq, ncand, GDKusec() - t0);
	return bn;
}

/* calculate group sizes (number of TRUE values) with optional
 * candidates list */
BAT *
BATgroupsize(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, bool abort_on_error)
{
	const oid *restrict gids;
	oid min, max;
	BUN i, ngrp;
	const bit *restrict bits;
	lng *restrict cnts;
	BAT *bn = NULL;
	struct canditer ci;
	BUN ncand;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(tp == TYPE_lng);
	assert(b->ttype == TYPE_bit);
	/* compatibility arguments */
	(void) tp;
	(void) abort_on_error;
	(void) skip_nils;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("b and g must be aligned\n");
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
		gids = (const oid *) Tloc(g, 0);

	bits = (const bit *) Tloc(b, 0);

	while (ncand > 0) {
		ncand--;
		i = canditer_next(&ci) - b->hseqbase;
		if (bits[i] == 1 &&
		    (gids == NULL || (gids[i] >= min && gids[i] <= max))) {
			cnts[gids ? gids[i] - min : (oid) i]++;
		}
	}
	BATsetcount(bn, ngrp);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = false;
	bn->tnonil = true;
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOOPTBATFMT ","
		  "e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
		  "; start " OIDFMT ", count " BUNFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(bn),
		  ci.seq, ncand, GDKusec() - t0);
	return bn;
}

/* ---------------------------------------------------------------------- */
/* min and max */

#define AGGR_CMP(TYPE, OP)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		if (ngrp == cnt) {					\
			/* single element groups */			\
			while (ncand > 0) {				\
				ncand--;				\
				i = canditer_next(ci) - b->hseqbase;	\
				if (!skip_nils ||			\
				    !is_##TYPE##_nil(vals[i])) {	\
					oids[i] = i + b->hseqbase;	\
					nils--;				\
				}					\
			}						\
		} else {						\
			gid = 0; /* in case gids == NULL */		\
			while (ncand > 0) {				\
				ncand--;				\
				i = canditer_next(ci) - b->hseqbase;	\
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
	    oid min, oid max, struct canditer *restrict ci, BUN ncand,
	    BUN cnt, bool skip_nils, bool gdense)
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
			oids[0] = canditer_next(ci);
			nils--;
			break;
		}
		/* fall through */
	default:
		assert(b->ttype != TYPE_oid);
		bi = bat_iterator(b);

		if (gdense) {
			/* single element groups */
			while (ncand > 0) {
				ncand--;
				i = canditer_next(ci) - b->hseqbase;
				if (!skip_nils ||
				    (*atomcmp)(BUNtail(bi, i), nil) != 0) {
					oids[i] = i + b->hseqbase;
					nils--;
				}
			}
		} else {
			gid = 0; /* in case gids == NULL */
			while (ncand > 0) {
				ncand--;
				i = canditer_next(ci) - b->hseqbase;
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
	    oid min, oid max, struct canditer *restrict ci, BUN ncand,
	    BUN cnt, bool skip_nils, bool gdense)
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
			oids[0] = canditer_last(ci);
			nils--;
			break;
		}
		/* fall through */
	default:
		assert(b->ttype != TYPE_oid);
		bi = bat_iterator(b);

		if (gdense) {
			/* single element groups */
			while (ncand > 0) {
				ncand--;
				i = canditer_next(ci) - b->hseqbase;
				if (!skip_nils ||
				    (*atomcmp)(BUNtail(bi, i), nil) != 0) {
					oids[i] = i + b->hseqbase;
					nils--;
				}
			}
		} else {
			gid = 0; /* in case gids == NULL */
			while (ncand > 0) {
				ncand--;
				i = canditer_next(ci) - b->hseqbase;
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
BATgroupminmax(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils,
	       bool abort_on_error,
	       BUN (*minmax)(oid *restrict, BAT *, const oid *restrict, BUN,
			     oid, oid, struct canditer *restrict, BUN,
			     BUN, bool, bool),
	       const char *name)
{
	const oid *restrict gids;
	oid min, max;
	BUN ngrp;
	oid *restrict oids;
	BAT *bn = NULL;
	BUN nils;
	struct canditer ci;
	BUN ncand;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(tp == TYPE_oid);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if (!ATOMlinear(b->ttype)) {
		GDKerror("%s: cannot determine minimum on "
			 "non-linear type %s\n", name, ATOMname(b->ttype));
		return NULL;
	}

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
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
		gids = (const oid *) Tloc(g, 0);

	nils = (*minmax)(oids, b, gids, ngrp, min, max, &ci, ncand,
			 BATcount(b), skip_nils, g && BATtdense(g));

	BATsetcount(bn, ngrp);

	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOOPTBATFMT ","
		  "e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
		  "; start " OIDFMT ", count " BUNFMT " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(bn),
		  ci.seq, ncand, name, GDKusec() - t0);
	return bn;
}

BAT *
BATgroupmin(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
	    bool skip_nils, bool abort_on_error)
{
	return BATgroupminmax(b, g, e, s, tp, skip_nils, abort_on_error,
			      do_groupmin, __func__);
}

/* return pointer to smallest non-nil value in b, or pointer to nil if
 * there is no such value (no values at all, or only nil) */
void *
BATmin_skipnil(BAT *b, void *aggr, bit skipnil)
{
	const ValRecord *prop;
	const void *res;
	size_t s;
	BATiter bi;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if (!ATOMlinear(b->ttype)) {
		/* there is no such thing as a smallest value if you
		 * can't compare values */
		GDKerror("non-linear type");
		return NULL;
	}
	if (BATcount(b) == 0) {
		res = ATOMnilptr(b->ttype);
	} else if ((prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL) {
		res = VALptr(prop);
	} else {
		oid pos;
		BAT *pb = NULL;

		if (BATcheckorderidx(b) ||
		    (/* DISABLES CODE */ (0) &&
		     VIEWtparent(b) &&
		     (pb = BBPdescriptor(VIEWtparent(b))) != NULL &&
		     pb->tbaseoff == b->tbaseoff &&
		     BATcount(pb) == BATcount(b) &&
		     pb->hseqbase == b->hseqbase &&
		     BATcheckorderidx(pb))) {
			const oid *ords = (const oid *) (pb ? pb->torderidx->base : b->torderidx->base) + ORDERIDXOFF;
			BUN r;
			if (!b->tnonil) {
				MT_thread_setalgorithm(pb ? "binsearch on parent oidx" : "binsearch on oids");
				r = binsearch(ords, 0, b->ttype, Tloc(b, 0),
					      b->tvheap ? b->tvheap->base : NULL,
					      b->twidth, 0, BATcount(b),
					      ATOMnilptr(b->ttype), 1, 1);
				if (r == 0) {
					b->tnonil = true;
					b->batDirtydesc = true;
				}
			} else {
				r = 0;
			}
			if (r == BATcount(b)) {
				/* no non-nil values */
				pos = oid_nil;
			} else {
				MT_thread_setalgorithm(pb ? "using parent oidx" : "using oids");
				pos = ords[r];
			}
		} else if ((VIEWtparent(b) == 0 ||
			    (/* DISABLES CODE */ (0) &&
			     BATcount(b) == BATcount(BBPdescriptor(VIEWtparent(b))))) &&
			   BATcheckimprints(b)) {
			Imprints *imprints = VIEWtparent(b) ? BBPdescriptor(VIEWtparent(b))->timprints : b->timprints;
			int i;


			MT_thread_setalgorithm(VIEWtparent(b) ? "using parent imprints" : "using imprints");
			pos = oid_nil;
			/* find first non-empty bin */
			for (i = 0; i < imprints->bits; i++) {
				if (imprints->stats[i + 128]) {
					pos = imprints->stats[i] + b->hseqbase;
					break;
				}
			}
		} else {
			struct canditer ci;
			BUN ncand = canditer_init(&ci, b, NULL);
			(void) do_groupmin(&pos, b, NULL, 1, 0, 0, &ci, ncand,
					   BATcount(b), skipnil, false);
		}
		if (is_oid_nil(pos)) {
			res = ATOMnilptr(b->ttype);
		} else {
			bi = bat_iterator(b);
			res = BUNtail(bi, pos - b->hseqbase);
			BATsetprop(b, GDK_MIN_VALUE, b->ttype, res);
			BATsetprop(b, GDK_MIN_POS, TYPE_oid, &(oid){pos - b->hseqbase});
		}
	}
	if (aggr == NULL) {
		s = ATOMlen(b->ttype, res);
		aggr = GDKmalloc(s);
	} else {
		s = ATOMsize(ATOMtype(b->ttype));
	}
	if (aggr != NULL)	/* else: malloc error */
		memcpy(aggr, res, s);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",skipnil=%d; (" LLFMT " usec)\n",
		  ALGOBATPAR(b), skipnil, GDKusec() - t0);
	return aggr;
}

void *
BATmin(BAT *b, void *aggr)
{
	return BATmin_skipnil(b, aggr, 1);
}

BAT *
BATgroupmax(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
	    bool skip_nils, bool abort_on_error)
{
	return BATgroupminmax(b, g, e, s, tp, skip_nils, abort_on_error,
			      do_groupmax, __func__);
}

void *
BATmax_skipnil(BAT *b, void *aggr, bit skipnil)
{
	const ValRecord *prop;
	const void *res;
	size_t s;
	BATiter bi;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if (!ATOMlinear(b->ttype)) {
		GDKerror("non-linear type");
		return NULL;
	}
	if (BATcount(b) == 0) {
		res = ATOMnilptr(b->ttype);
	} else if ((prop = BATgetprop(b, GDK_MAX_VALUE)) != NULL) {
		res = VALptr(prop);
	} else {
		oid pos;
		BAT *pb = NULL;

		if (BATcheckorderidx(b) ||
		    (/* DISABLES CODE */ (0) &&
		     VIEWtparent(b) &&
		     (pb = BBPdescriptor(VIEWtparent(b))) != NULL &&
		     pb->tbaseoff == b->tbaseoff &&
		     BATcount(pb) == BATcount(b) &&
		     pb->hseqbase == b->hseqbase &&
		     BATcheckorderidx(pb))) {
			const oid *ords = (const oid *) (pb ? pb->torderidx->base : b->torderidx->base) + ORDERIDXOFF;

			MT_thread_setalgorithm(pb ? "using parent oidx" : "using oids");
			pos = ords[BATcount(b) - 1];
			/* nils are first, ie !skipnil, check for nils */
			if (!skipnil) {
				BUN z = ords[0];

				bi = bat_iterator(b);
				res = BUNtail(bi, z - b->hseqbase);

				if (ATOMcmp(b->ttype, res, ATOMnilptr(b->ttype)) == 0)
					pos = z;
			}
		} else if ((VIEWtparent(b) == 0 ||
			    (/* DISABLES CODE */ (0) &&
			     BATcount(b) == BATcount(BBPdescriptor(VIEWtparent(b))))) &&
			   BATcheckimprints(b)) {
			Imprints *imprints = VIEWtparent(b) ? BBPdescriptor(VIEWtparent(b))->timprints : b->timprints;
			int i;

			MT_thread_setalgorithm(VIEWtparent(b) ? "using parent imprints" : "using imprints");
			pos = oid_nil;
			/* find last non-empty bin */
			for (i = imprints->bits - 1; i >= 0; i--) {
				if (imprints->stats[i + 128]) {
					pos = imprints->stats[i + 64] + b->hseqbase;
					break;
				}
			}
		} else {
			struct canditer ci;
			BUN ncand = canditer_init(&ci, b, NULL);
			(void) do_groupmax(&pos, b, NULL, 1, 0, 0, &ci, ncand,
					   BATcount(b), skipnil, false);
		}
		if (is_oid_nil(pos)) {
			res = ATOMnilptr(b->ttype);
		} else {
			bi = bat_iterator(b);
			res = BUNtail(bi, pos - b->hseqbase);
			if (b->tnonil) {
				BATsetprop(b, GDK_MAX_VALUE, b->ttype, res);
				BATsetprop(b, GDK_MAX_POS, TYPE_oid, &(oid){pos - b->hseqbase});
			}
		}
	}
	if (aggr == NULL) {
		s = ATOMlen(b->ttype, res);
		aggr = GDKmalloc(s);
	} else {
		s = ATOMsize(ATOMtype(b->ttype));
	}
	if (aggr != NULL)	/* else: malloc error */
		memcpy(aggr, res, s);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",skipnil=%d; (" LLFMT " usec)\n",
		  ALGOBATPAR(b), skipnil, GDKusec() - t0);
	return aggr;
}

void *
BATmax(BAT *b, void *aggr)
{
	return BATmax_skipnil(b, aggr, 1);
}


/* ---------------------------------------------------------------------- */
/* quantiles/median */

#if SIZEOF_OID == SIZEOF_INT
#define binsearch_oid(indir, offset, vals, lo, hi, v, ordering, last) binsearch_int(indir, offset, (const int *) vals, lo, hi, (int) (v), ordering, last)
#endif
#if SIZEOF_OID == SIZEOF_LNG
#define binsearch_oid(indir, offset, vals, lo, hi, v, ordering, last) binsearch_lng(indir, offset, (const lng *) vals, lo, hi, (lng) (v), ordering, last)
#endif

#define DO_QUANTILE_AVG(TPE)						\
	do {								\
		TPE low = *(TPE*) BUNtail(bi, r + (BUN) hi);		\
		TPE high = *(TPE*) BUNtail(bi, r + (BUN) lo);		\
		if (is_##TPE##_nil(low) || is_##TPE##_nil(high)) {	\
			val = dbl_nil;					\
			nils++;						\
		} else {						\
			val = (f - lo) * low + (lo + 1 - f) * high;	\
		}							\
	} while (0)

static BAT *
doBATgroupquantile(BAT *b, BAT *g, BAT *e, BAT *s, int tp, double quantile,
		   bool skip_nils, bool abort_on_error, bool average)
{
	BAT *origb = b;
	BAT *origg = g;
	oid min, max;
	BUN ngrp;
	BUN nils = 0;
	BAT *bn = NULL;
	struct canditer ci;
	BUN ncand;
	BAT *t1, *t2;
	BATiter bi;
	const void *v;
	const void *nil = ATOMnilptr(tp);
	const void *dnil = nil;
	dbl val;		/* only used for average */
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tp);
	const char *err;
	(void) abort_on_error;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if (average) {
		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_int:
		case TYPE_lng:
#ifdef HAVE_HGE
		case TYPE_hge:
#endif
		case TYPE_flt:
		case TYPE_dbl:
			break;
		default:
			GDKerror("incompatible type\n");
			return NULL;
		}
		dnil = &dbl_nil;
	}
	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	assert(tp == b->ttype);
	if (!ATOMlinear(tp)) {
		GDKerror("cannot determine quantile on "
			 "non-linear type %s\n", ATOMname(tp));
		return NULL;
	}
	if (quantile < 0 || quantile > 1) {
		GDKerror("cannot determine quantile for "
			 "p=%f (p has to be in [0,1])\n", quantile);
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0 || is_dbl_nil(quantile)) {
		/* trivial: no values, thus also no quantiles,
		 * so return bat aligned with e with nil in the tail
		 * The same happens for a NULL quantile */
		return BATconstant(ngrp == 0 ? 0 : min, average ? TYPE_dbl : tp, dnil, ngrp, TRANSIENT);
	}

	if (s) {
		/* there is a candidate list, replace b (and g, if
		 * given) with just the values we're interested in */
		b = BATproject(s, b);
		if (b == NULL)
			return NULL;
		if (g) {
			g = BATproject(s, g);
			if (g == NULL)
				goto bunins_failed;
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
			if (average)
				bn = BATconvert(b, NULL, TYPE_dbl, abort_on_error, 0, 0, 0);
			else
				bn = COLcopy(b, tp, false, TRANSIENT);
			BAThseqbase(bn, g->tseqbase); /* deals with NULL */
			if (b != origb)
				BBPunfix(b->batCacheid);
			if (g != origg)
				BBPunfix(g->batCacheid);
			return bn;
		}
		if (BATsort(&t1, &t2, NULL, g, NULL, NULL, false, false, false) != GDK_SUCCEED)
			goto bunins_failed;
		if (g != origg)
			BBPunfix(g->batCacheid);
		g = t1;

		if (BATsort(&t1, NULL, NULL, b, t2, g, false, false, false) != GDK_SUCCEED) {
			BBPunfix(t2->batCacheid);
			goto bunins_failed;
		}
		if (b != origb)
			BBPunfix(b->batCacheid);
		b = t1;
		BBPunfix(t2->batCacheid);

		if (average)
			bn = COLnew(min, TYPE_dbl, ngrp, TRANSIENT);
		else
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
				v = dnil;
				nils++;
			} else if (average) {
				double f = (p - r - 1) * quantile;
				double lo = floor(f);
				double hi = ceil(f);
				switch (ATOMbasetype(tp)) {
				case TYPE_bte:
					DO_QUANTILE_AVG(bte);
					break;
				case TYPE_sht:
					DO_QUANTILE_AVG(sht);
					break;
				case TYPE_int:
					DO_QUANTILE_AVG(int);
					break;
				case TYPE_lng:
					DO_QUANTILE_AVG(lng);
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					DO_QUANTILE_AVG(hge);
					break;
#endif
				case TYPE_flt:
					DO_QUANTILE_AVG(flt);
					break;
				case TYPE_dbl:
					DO_QUANTILE_AVG(dbl);
					break;
				}
				v = &val;
			} else {
				/* round *down* to nearest integer */
				double f = (p - r - 1) * quantile;
				qindex = r + p - (BUN) (p + 0.5 - f);
				/* be a little paranoid about the index */
				assert(qindex >= r && qindex <  p);
				v = BUNtail(bi, qindex);
				if (!skip_nils && !b->tnonil)
					nils += (*atomcmp)(v, dnil) == 0;
			}
			if (bunfastapp_nocheck(bn, BUNlast(bn), v, Tsize(bn)) != GDK_SUCCEED)
				goto bunins_failed;
		}
		nils += ngrp - BATcount(bn);
		while (BATcount(bn) < ngrp) {
			if (bunfastapp_nocheck(bn, BUNlast(bn), dnil, Tsize(bn)) != GDK_SUCCEED)
				goto bunins_failed;
		}
		bn->theap->dirty = true;
		BBPunfix(g->batCacheid);
	} else {
		BUN index, r, p = BATcount(b);
		BAT *pb = NULL;
		const oid *ords;

		bn = COLnew(0, average ? TYPE_dbl : tp, 1, TRANSIENT);
		if (bn == NULL)
			goto bunins_failed;

		t1 = NULL;

		if (BATcheckorderidx(b) ||
		    (/* DISABLES CODE */ (0) &&
		     VIEWtparent(b) &&
		     (pb = BBPdescriptor(VIEWtparent(b))) != NULL &&
		     pb->tbaseoff == b->tbaseoff &&
		     BATcount(pb) == BATcount(b) &&
		     pb->hseqbase == b->hseqbase &&
		     BATcheckorderidx(pb))) {
			MT_thread_setalgorithm(pb ? "using parent oidx" : "using oids");
			ords = (const oid *) (pb ? pb->torderidx->base : b->torderidx->base) + ORDERIDXOFF;
		} else {
			if (BATsort(NULL, &t1, NULL, b, NULL, g, false, false, false) != GDK_SUCCEED)
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

		bi = bat_iterator(b);
		if (r == p) {
			/* no non-nil values, so quantile is nil */
			v = dnil;
			nils++;
		} else if (average) {
			double f = (p - r - 1) * quantile;
			double lo = floor(f);
			double hi = ceil(f);
			switch (ATOMbasetype(tp)) {
			case TYPE_bte:
				DO_QUANTILE_AVG(bte);
				break;
			case TYPE_sht:
				DO_QUANTILE_AVG(sht);
				break;
			case TYPE_int:
				DO_QUANTILE_AVG(int);
				break;
			case TYPE_lng:
				DO_QUANTILE_AVG(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				DO_QUANTILE_AVG(hge);
				break;
#endif
			case TYPE_flt:
				DO_QUANTILE_AVG(flt);
				break;
			case TYPE_dbl:
				DO_QUANTILE_AVG(dbl);
				break;
			}
			v = &val;
		} else {
			double f;
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
			nils += (*atomcmp)(v, dnil) == 0;
		}
		if (t1)
			BBPunfix(t1->batCacheid);
		if (BUNappend(bn, v, false) != GDK_SUCCEED)
			goto bunins_failed;
	}

	if (b != origb)
		BBPunfix(b->batCacheid);

	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOOPTBATFMT ","
		  "e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  ",quantile=%g,average=%s -> " ALGOOPTBATFMT
		  "; start " OIDFMT ", count " BUNFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(origb), ALGOOPTBATPAR(origg), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(s), quantile, average ? "true" : "false",
		  ALGOOPTBATPAR(bn), ci.seq, ncand, GDKusec() - t0);
	return bn;

  bunins_failed:
	if (b && b != origb)
		BBPunfix(b->batCacheid);
	if (g && g != origg)
		BBPunfix(g->batCacheid);
	if (bn)
		BBPunfix(bn->batCacheid);
	return NULL;
}

BAT *
BATgroupmedian(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
	       bool skip_nils, bool abort_on_error)
{
	return doBATgroupquantile(b, g, e, s, tp, 0.5,
				  skip_nils, abort_on_error, false);
}

BAT *
BATgroupquantile(BAT *b, BAT *g, BAT *e, BAT *s, int tp, double quantile,
		 bool skip_nils, bool abort_on_error)
{
	return doBATgroupquantile(b, g, e, s, tp, quantile,
				  skip_nils, abort_on_error, false);
}

BAT *
BATgroupmedian_avg(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
		   bool skip_nils, bool abort_on_error)
{
	return doBATgroupquantile(b, g, e, s, tp, 0.5,
				  skip_nils, abort_on_error, true);
}

BAT *
BATgroupquantile_avg(BAT *b, BAT *g, BAT *e, BAT *s, int tp, double quantile,
		     bool skip_nils, bool abort_on_error)
{
	return doBATgroupquantile(b, g, e, s, tp, quantile,
				  skip_nils, abort_on_error, true);
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
			if (isinf(m2))			\
				goto overflow;		\
		}					\
	} while (0)

static dbl
calcvariance(dbl *restrict avgp, const void *restrict values, BUN cnt, int tp, bool issample)
{
	BUN n = 0, i;
	dbl mean = 0;
	dbl m2 = 0;
	dbl delta;

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
		GDKerror("type (%s) not supported.\n", ATOMname(tp));
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
  overflow:
	GDKerror("22003!overflow in calculation.\n");
	return dbl_nil;
}

dbl
BATcalcstdev_population(dbl *avgp, BAT *b)
{
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	dbl v = calcvariance(avgp, (const void *) Tloc(b, 0),
			     BATcount(b), b->ttype, false);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), GDKusec() - t0);
	return is_dbl_nil(v) ? dbl_nil : sqrt(v);
}

dbl
BATcalcstdev_sample(dbl *avgp, BAT *b)
{
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	dbl v = calcvariance(avgp, (const void *) Tloc(b, 0),
			     BATcount(b), b->ttype, true);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), GDKusec() - t0);
	return is_dbl_nil(v) ? dbl_nil : sqrt(v);
}

dbl
BATcalcvariance_population(dbl *avgp, BAT *b)
{
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	dbl v = calcvariance(avgp, (const void *) Tloc(b, 0),
			     BATcount(b), b->ttype, false);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), GDKusec() - t0);
	return v;
}

dbl
BATcalcvariance_sample(dbl *avgp, BAT *b)
{
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	dbl v = calcvariance(avgp, (const void *) Tloc(b, 0),
			     BATcount(b), b->ttype, true);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), GDKusec() - t0);
	return v;
}

#define AGGR_COVARIANCE_SINGLE(TYPE)					\
	do {								\
		TYPE x, y;						\
		for (i = 0; i < cnt; i++) {				\
			x = ((const TYPE *) v1)[i];			\
			y = ((const TYPE *) v2)[i];			\
			if (is_##TYPE##_nil(x) || is_##TYPE##_nil(y))	\
				continue;				\
			n++;						\
			delta1 = (dbl) x - mean1;			\
			mean1 += delta1 / n;				\
			delta2 = (dbl) y - mean2;			\
			mean2 += delta2 / n;				\
			m2 += delta1 * ((dbl) y - mean2);		\
			if (isinf(m2))			\
				goto overflow;		\
		}							\
	} while (0)

static dbl
calccovariance(const void *v1, const void *v2, BUN cnt, int tp, bool issample)
{
	BUN n = 0, i;
	dbl mean1 = 0, mean2 = 0, m2 = 0, delta1, delta2;

	switch (tp) {
	case TYPE_bte:
		AGGR_COVARIANCE_SINGLE(bte);
		break;
	case TYPE_sht:
		AGGR_COVARIANCE_SINGLE(sht);
		break;
	case TYPE_int:
		AGGR_COVARIANCE_SINGLE(int);
		break;
	case TYPE_lng:
		AGGR_COVARIANCE_SINGLE(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_COVARIANCE_SINGLE(hge);
		break;
#endif
	case TYPE_flt:
		AGGR_COVARIANCE_SINGLE(flt);
		break;
	case TYPE_dbl:
		AGGR_COVARIANCE_SINGLE(dbl);
		break;
	default:
		GDKerror("type (%s) not supported.\n", ATOMname(tp));
		return dbl_nil;
	}
	if (n <= (BUN) issample)
		return dbl_nil;
	return m2 / (n - issample);
  overflow:
	GDKerror("22003!overflow in calculation.\n");
	return dbl_nil;
}

dbl
BATcalccovariance_population(BAT *b1, BAT *b2)
{
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	dbl v = calccovariance(Tloc(b1, 0), Tloc(b2, 0),
			       BATcount(b1), b1->ttype, false);
	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2), GDKusec() - t0);
	return v;
}

dbl
BATcalccovariance_sample(BAT *b1, BAT *b2)
{
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	dbl v = calccovariance(Tloc(b1, 0), Tloc(b2, 0),
			      BATcount(b1), b1->ttype, true);
	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2), GDKusec() - t0);
	return v;
}

#define AGGR_CORRELATION_SINGLE(TYPE)					\
	do {								\
		TYPE x, y;						\
		for (i = 0; i < cnt; i++) {				\
			x = ((const TYPE *) v1)[i];			\
			y = ((const TYPE *) v2)[i];			\
			if (is_##TYPE##_nil(x) || is_##TYPE##_nil(y))	\
				continue;				\
			n++;						\
			delta1 = (dbl) x - mean1;			\
			mean1 += delta1 / n;				\
			delta2 = (dbl) y - mean2;			\
			mean2 += delta2 / n;				\
			aux = (dbl) y - mean2;				\
			up += delta1 * aux;				\
			down1 += delta1 * ((dbl) x - mean1);		\
			down2 += delta2 * aux;				\
			if (isinf(up) || isinf(down1) || isinf(down2))		\
				goto overflow;		\
		}							\
	} while (0)

dbl
BATcalccorrelation(BAT *b1, BAT *b2)
{
	BUN n = 0, i, cnt = BATcount(b1);
	dbl mean1 = 0, mean2 = 0, up = 0, down1 = 0, down2 = 0, delta1, delta2, aux;
	const void *v1 = (const void *) Tloc(b1, 0), *v2 = (const void *) Tloc(b2, 0);
	int tp = b1->ttype;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	switch (tp) {
	case TYPE_bte:
		AGGR_CORRELATION_SINGLE(bte);
		break;
	case TYPE_sht:
		AGGR_CORRELATION_SINGLE(sht);
		break;
	case TYPE_int:
		AGGR_CORRELATION_SINGLE(int);
		break;
	case TYPE_lng:
		AGGR_CORRELATION_SINGLE(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_CORRELATION_SINGLE(hge);
		break;
#endif
	case TYPE_flt:
		AGGR_CORRELATION_SINGLE(flt);
		break;
	case TYPE_dbl:
		AGGR_CORRELATION_SINGLE(dbl);
		break;
	default:
		GDKerror("type (%s) not supported.\n",
			 ATOMname(tp));
		return dbl_nil;
	}
	if (n != 0 && down1 != 0 && down2 != 0)
		aux = (up / n) / (sqrt(down1 / n) * sqrt(down2 / n));
	else
		aux = dbl_nil;
	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT " (" LLFMT " usec)\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2), GDKusec() - t0);
	return aux;
  overflow:
	GDKerror("22003!overflow in calculation.\n");
	return dbl_nil;
}

#define AGGR_STDEV(TYPE)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, 0);	\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - b->hseqbase;		\
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
			} else if (isinf(m2[i])) {			\
				goto overflow;		\
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
	     bool skip_nils, bool issample, bool variance, const char *func)
{
	const oid *restrict gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	BUN nils = 0, nils2 = 0;
	BUN *restrict cnts = NULL;
	dbl *restrict dbls, *restrict mean, *restrict delta, *restrict m2;
	BAT *bn = NULL, *an = NULL;
	struct canditer ci;
	BUN ncand;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(tp == TYPE_dbl);
	(void) tp;		/* compatibility (with other BATgroup*
				 * functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
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
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &dbl_nil, ngrp, TRANSIENT);
		goto doreturn;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->tnonil)) &&
	    (issample || b->tnonil)) {
		/* trivial: singleton groups, so all results are equal
		 * to zero (population) or nil (sample) */
		dbl v = issample ? dbl_nil : 0;
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &v, ngrp, TRANSIENT);
		goto doreturn;
	}

	delta = GDKmalloc(ngrp * sizeof(dbl));
	m2 = GDKmalloc(ngrp * sizeof(dbl));
	cnts = GDKzalloc(ngrp * sizeof(BUN));
	if (avgb) {
		an = COLnew(0, TYPE_dbl, ngrp, TRANSIENT);
		*avgb = an;
		if (an == NULL) {
			mean = NULL;
			goto alloc_fail;
		}
		mean = (dbl *) Tloc(an, 0);
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
		gids = (const oid *) Tloc(g, 0);

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
		if (an)
			BBPreclaim(an);
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
	if (an) {
		BATsetcount(an, ngrp);
		an->tkey = ngrp <= 1;
		an->tsorted = ngrp <= 1;
		an->trevsorted = ngrp <= 1;
		an->tnil = nils != 0;
		an->tnonil = nils == 0;
	} else {
		GDKfree(mean);
	}
	if (issample)
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
  doreturn:
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOBATFMT ",e=" ALGOOPTBATFMT
		  ",s=" ALGOOPTBATFMT
		  ",skip_nils=%s,issample=%s,variance=%s -> " ALGOOPTBATFMT
		  ",avgb=" ALGOOPTBATFMT " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOBATPAR(g), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(s),
		  skip_nils ? "true" : "false",
		  issample ? "true" : "false",
		  variance ? "true" : "false",
		  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(an),
		  func, GDKusec() - t0);
	return bn;
  overflow:
	GDKerror("22003!overflow in calculation.\n");
  alloc_fail:
	if (an)
		BBPreclaim(an);
	else
		GDKfree(mean);
	BBPreclaim(bn);
	GDKfree(delta);
	GDKfree(m2);
	GDKfree(cnts);
	return NULL;
}

BAT *
BATgroupstdev_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
		     bool skip_nils, bool abort_on_error)
{
	(void) abort_on_error;
	return dogroupstdev(NULL, b, g, e, s, tp, skip_nils, true, false,
			    __func__);
}

BAT *
BATgroupstdev_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
			 bool skip_nils, bool abort_on_error)
{
	(void) abort_on_error;
	return dogroupstdev(NULL, b, g, e, s, tp, skip_nils, false, false,
			    __func__);
}

BAT *
BATgroupvariance_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
			bool skip_nils, bool abort_on_error)
{
	(void) abort_on_error;
	return dogroupstdev(NULL, b, g, e, s, tp, skip_nils, true, true,
			    __func__);
}

BAT *
BATgroupvariance_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp,
			    bool skip_nils, bool abort_on_error)
{
	(void) abort_on_error;
	return dogroupstdev(NULL, b, g, e, s, tp, skip_nils, false, true,
			    __func__);
}

#define AGGR_COVARIANCE(TYPE)						\
	do {								\
		const TYPE *vals1 = (const TYPE *) Tloc(b1, 0);		\
		const TYPE *vals2 = (const TYPE *) Tloc(b2, 0);		\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - b1->hseqbase;		\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (is_##TYPE##_nil(vals1[i]) || is_##TYPE##_nil(vals2[i])) { \
					if (!skip_nils)			\
						cnts[gid] = BUN_NONE;	\
				} else if (cnts[gid] != BUN_NONE) {	\
					cnts[gid]++;			\
					delta1[gid] = (dbl) vals1[i] - mean1[gid]; \
					mean1[gid] += delta1[gid] / cnts[gid]; \
					delta2[gid] = (dbl) vals2[i] - mean2[gid]; \
					mean2[gid] += delta2[gid] / cnts[gid]; \
					m2[gid] += delta1[gid] * ((dbl) vals2[i] - mean2[gid]); \
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] == 0 || cnts[i] == BUN_NONE) {	\
				dbls[i] = dbl_nil;			\
				nils++;					\
			} else if (cnts[i] == 1) {			\
				dbls[i] = issample ? dbl_nil : 0;	\
				nils2++;				\
			} else if (isinf(m2[i])) {		\
				goto overflow;		\
			} else {					\
				dbls[i] = m2[i] / (cnts[i] - issample);	\
			}						\
		}							\
	} while (0)

static BAT *
dogroupcovariance(BAT *b1, BAT *b2, BAT *g, BAT *e, BAT *s, int tp,
		  bool skip_nils, bool issample, const char *func)
{
	const oid *restrict gids;
	oid gid, min, max;
	BUN i, ngrp, nils = 0, nils2 = 0, ncand;
	BUN *restrict cnts = NULL;
	dbl *restrict dbls, *restrict mean1, *restrict mean2, *restrict delta1, *restrict delta2, *restrict m2;
	BAT *bn = NULL;
	struct canditer ci;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(tp == TYPE_dbl && BATcount(b1) == BATcount(b2) && b1->ttype == b2->ttype && BATtdense(b1) == BATtdense(b2));
	(void) tp;

	if ((err = BATgroupaggrinit(b1, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s: %s\n", func, err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("%s: b1, b2 and g must be aligned\n", func);
		return NULL;
	}

	if (BATcount(b1) == 0 || ngrp == 0) {
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &dbl_nil, ngrp, TRANSIENT);
		goto doreturn;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b1) && (e->hseqbase == b1->hseqbase || e->hseqbase == b2->hseqbase))) &&
	    (BATtdense(g) || (g->tkey && g->tnonil)) &&
	    (issample || (b1->tnonil && b2->tnonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to zero (population) or nil (sample) */
		dbl v = issample ? dbl_nil : 0;
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &v, ngrp, TRANSIENT);
		goto doreturn;
	}

	delta1 = GDKmalloc(ngrp * sizeof(dbl));
	delta2 = GDKmalloc(ngrp * sizeof(dbl));
	m2 = GDKmalloc(ngrp * sizeof(dbl));
	cnts = GDKzalloc(ngrp * sizeof(BUN));
	mean1 = GDKmalloc(ngrp * sizeof(dbl));
	mean2 = GDKmalloc(ngrp * sizeof(dbl));

	if (mean1 == NULL || mean2 == NULL || delta1 == NULL || delta2 == NULL || m2 == NULL || cnts == NULL)
		goto alloc_fail;

	for (i = 0; i < ngrp; i++) {
		m2[i] = 0;
		mean1[i] = 0;
		mean2[i] = 0;
	}

	bn = COLnew(min, TYPE_dbl, ngrp, TRANSIENT);
	if (bn == NULL)
		goto alloc_fail;
	dbls = (dbl *) Tloc(bn, 0);

	if (!g || BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, 0);

	switch (b1->ttype) {
	case TYPE_bte:
		AGGR_COVARIANCE(bte);
		break;
	case TYPE_sht:
		AGGR_COVARIANCE(sht);
		break;
	case TYPE_int:
		AGGR_COVARIANCE(int);
		break;
	case TYPE_lng:
		AGGR_COVARIANCE(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_COVARIANCE(hge);
		break;
#endif
	case TYPE_flt:
		AGGR_COVARIANCE(flt);
		break;
	case TYPE_dbl:
		AGGR_COVARIANCE(dbl);
		break;
	default:
		BBPreclaim(bn);
		GDKfree(mean1);
		GDKfree(mean2);
		GDKfree(delta1);
		GDKfree(delta2);
		GDKfree(m2);
		GDKfree(cnts);
		GDKerror("%s: type (%s) not supported.\n", func, ATOMname(b1->ttype));
		return NULL;
	}
	GDKfree(mean1);
	GDKfree(mean2);

	if (issample)
		nils += nils2;
	GDKfree(delta1);
	GDKfree(delta2);
	GDKfree(m2);
	GDKfree(cnts);
	BATsetcount(bn, ngrp);
	bn->tkey = ngrp <= 1;
	bn->tsorted = ngrp <= 1;
	bn->trevsorted = ngrp <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
  doreturn:
	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT ",g=" ALGOBATFMT
		  ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  ",skip_nils=%s,issample=%s -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2), ALGOBATPAR(g),
		  ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  skip_nils ? "true" : "false",
		  issample ? "true" : "false",
		  ALGOOPTBATPAR(bn),
		  func, GDKusec() - t0);
	return bn;
  overflow:
	GDKerror("22003!overflow in calculation.\n");
  alloc_fail:
	BBPreclaim(bn);
	GDKfree(mean1);
	GDKfree(mean2);
	GDKfree(delta1);
	GDKfree(delta2);
	GDKfree(m2);
	GDKfree(cnts);
	return NULL;
}

BAT *
BATgroupcovariance_sample(BAT *b1, BAT *b2, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, bool abort_on_error)
{
	(void) abort_on_error;
	return dogroupcovariance(b1, b2, g, e, s, tp, skip_nils, true,
				 __func__);
}

BAT *
BATgroupcovariance_population(BAT *b1, BAT *b2, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, bool abort_on_error)
{
	(void) abort_on_error;
	return dogroupcovariance(b1, b2, g, e, s, tp, skip_nils, false,
				 __func__);
}

#define AGGR_CORRELATION(TYPE)						\
	do {								\
		const TYPE *vals1 = (const TYPE *) Tloc(b1, 0);		\
		const TYPE *vals2 = (const TYPE *) Tloc(b2, 0);		\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - b1->hseqbase;		\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (is_##TYPE##_nil(vals1[i]) || is_##TYPE##_nil(vals2[i])) { \
					if (!skip_nils)			\
						cnts[gid] = BUN_NONE;	\
				} else if (cnts[gid] != BUN_NONE) {	\
					cnts[gid]++;			\
					delta1[gid] = (dbl) vals1[i] - mean1[gid]; \
					mean1[gid] += delta1[gid] / cnts[gid]; \
					delta2[gid] = (dbl) vals2[i] - mean2[gid]; \
					mean2[gid] += delta2[gid] / cnts[gid]; \
					aux = (dbl) vals2[i] - mean2[gid]; \
					up[gid] += delta1[gid] * aux;	\
					down1[gid] += delta1[gid] * ((dbl) vals1[i] - mean1[gid]); \
					down2[gid] += delta2[gid] * aux; \
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] <= 1 || cnts[i] == BUN_NONE || down1[i] == 0 || down2[i] == 0) { \
				dbls[i] = dbl_nil;			\
				nils++;					\
			} else if (isinf(up[i]) || isinf(down1[i]) || isinf(down2[i])) {	\
				goto overflow;		\
			} else {					\
				dbls[i] = (up[i] / cnts[i]) / (sqrt(down1[i] / cnts[i]) * sqrt(down2[i] / cnts[i])); \
				assert(!is_dbl_nil(dbls[i]));		\
			}						\
		}							\
	} while (0)

BAT *
BATgroupcorrelation(BAT *b1, BAT *b2, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, bool abort_on_error)
{
	const oid *restrict gids;
	oid gid, min, max;
	BUN i, ngrp, nils = 0, ncand;
	BUN *restrict cnts = NULL;
	dbl *restrict dbls, *restrict mean1, *restrict mean2, *restrict delta1, *restrict delta2, *restrict up, *restrict down1, *restrict down2, aux;
	BAT *bn = NULL;
	struct canditer ci;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(tp == TYPE_dbl && BATcount(b1) == BATcount(b2) && b1->ttype == b2->ttype && BATtdense(b1) == BATtdense(b2));
	(void) tp;
	(void) abort_on_error;

	if ((err = BATgroupaggrinit(b1, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("b1, b2 and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b1) == 0 || ngrp == 0) {
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_dbl, &dbl_nil, ngrp, TRANSIENT);
		goto doreturn;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b1) && (e->hseqbase == b1->hseqbase || e->hseqbase == b2->hseqbase))) &&
	    (BATtdense(g) || (g->tkey && g->tnonil))) {
		dbl v = dbl_nil;
		bn = BATconstant(min, TYPE_dbl, &v, ngrp, TRANSIENT);
		goto doreturn;
	}

	delta1 = GDKmalloc(ngrp * sizeof(dbl));
	delta2 = GDKmalloc(ngrp * sizeof(dbl));
	up = GDKmalloc(ngrp * sizeof(dbl));
	down1 = GDKmalloc(ngrp * sizeof(dbl));
	down2 = GDKmalloc(ngrp * sizeof(dbl));
	cnts = GDKzalloc(ngrp * sizeof(BUN));
	mean1 = GDKmalloc(ngrp * sizeof(dbl));
	mean2 = GDKmalloc(ngrp * sizeof(dbl));

	if (mean1 == NULL || mean2 == NULL || delta1 == NULL || delta2 == NULL || up == NULL || down1 == NULL || down2 == NULL || cnts == NULL)
		goto alloc_fail;

	for (i = 0; i < ngrp; i++) {
		up[i] = 0;
		down1[i] = 0;
		down2[i] = 0;
		mean1[i] = 0;
		mean2[i] = 0;
	}

	bn = COLnew(min, TYPE_dbl, ngrp, TRANSIENT);
	if (bn == NULL)
		goto alloc_fail;
	dbls = (dbl *) Tloc(bn, 0);

	if (!g || BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, 0);

	switch (b1->ttype) {
	case TYPE_bte:
		AGGR_CORRELATION(bte);
		break;
	case TYPE_sht:
		AGGR_CORRELATION(sht);
		break;
	case TYPE_int:
		AGGR_CORRELATION(int);
		break;
	case TYPE_lng:
		AGGR_CORRELATION(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		AGGR_CORRELATION(hge);
		break;
#endif
	case TYPE_flt:
		AGGR_CORRELATION(flt);
		break;
	case TYPE_dbl:
		AGGR_CORRELATION(dbl);
		break;
	default:
		BBPreclaim(bn);
		GDKfree(mean1);
		GDKfree(mean2);
		GDKfree(delta1);
		GDKfree(delta2);
		GDKfree(up);
		GDKfree(down1);
		GDKfree(down2);
		GDKfree(cnts);
		GDKerror("type (%s) not supported.\n", ATOMname(b1->ttype));
		return NULL;
	}
	GDKfree(mean1);
	GDKfree(mean2);
	GDKfree(delta1);
	GDKfree(delta2);
	GDKfree(up);
	GDKfree(down1);
	GDKfree(down2);
	GDKfree(cnts);
	BATsetcount(bn, ngrp);
	bn->tkey = ngrp <= 1;
	bn->tsorted = ngrp <= 1;
	bn->trevsorted = ngrp <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
  doreturn:
	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT ",g=" ALGOBATFMT
		  ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  ",skip_nils=%s -> " ALGOOPTBATFMT
		  " (" LLFMT " usec)\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2), ALGOBATPAR(g),
		  ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  skip_nils ? "true" : "false",
		  ALGOOPTBATPAR(bn),
		  GDKusec() - t0);
	return bn;
  overflow:
	GDKerror("22003!overflow in calculation.\n");
  alloc_fail:
	BBPreclaim(bn);
	GDKfree(mean1);
	GDKfree(mean2);
	GDKfree(delta1);
	GDKfree(delta2);
	GDKfree(up);
	GDKfree(down1);
	GDKfree(down2);
	GDKfree(cnts);
	return NULL;
}
