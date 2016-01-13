/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
		 BUN *cntp, const oid **candp, const oid **candendp)
{
	oid min, max;
	BUN i, ngrp;
	const oid *restrict gids;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;

	if (b == NULL || !BAThdense(b))
		return "b must be dense-headed";
	if (g) {
		if (!BAThdense(g) || BATcount(b) != BATcount(g) ||
		    (BATcount(b) != 0 && b->hseqbase != g->hseqbase))
			return "b and g must be aligned";
		assert(BATttype(g) == TYPE_oid);
	}
	if (e != NULL && !BAThdense(e))
		return "e must be dense-headed";
	if (g == NULL) {
		min = 0;
		max = 0;
		ngrp = 1;
	} else if (e == NULL) {
		/* we need to find out the min and max of g */
		min = oid_nil;	/* note that oid_nil > 0! (unsigned) */
		max = 0;
		if (BATtdense(g)) {
			min = g->tseqbase;
			max = g->tseqbase + BATcount(g) - 1;
		} else if (g->tsorted) {
			gids = (const oid *) Tloc(g, BUNfirst(g));
			/* find first non-nil */
			for (i = 0, ngrp = BATcount(g); i < ngrp; i++, gids++) {
				if (*gids != oid_nil) {
					min = *gids;
					break;
				}
			}
			if (min != oid_nil) {
				/* found a non-nil, max must be last
				 * value (and there is one!) */
				max = * (const oid *) Tloc(g, BUNlast(g) - 1);
			}
		} else {
			/* we'll do a complete scan */
			gids = (const oid *) Tloc(g, BUNfirst(g));
			for (i = 0, ngrp = BATcount(g); i < ngrp; i++, gids++) {
				if (*gids != oid_nil) {
					if (*gids < min)
						min = *gids;
					if (*gids > max)
						max = *gids;
				}
			}
			/* note: max < min is possible if all groups
			 * are nil (or BATcount(g)==0) */
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
	*cntp = cnt;
	*candp = cand;
	*candendp = candend;

	return NULL;
}

/* ---------------------------------------------------------------------- */
/* sum */

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
					if (x == TYPE1##_nil) {		\
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
				if (x == TYPE1##_nil) {			\
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
					if (x == TYPE1##_nil) {		\
						if (!skip_nils) {	\
							sums[gid] = TYPE2##_nil; \
							nils++;	\
						}			\
					} else {			\
						if (nil_if_empty &&	\
						    !(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
							seen[gid >> 5] |= 1 << (gid & 0x1F); \
							sums[gid] = 0;	\
						}			\
						if (sums[gid] != TYPE2##_nil) { \
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
				    (gids[i] >= min && gids[i] <= max)) {	\
					gid = gids ? gids[i] - min : (oid) i; \
					x = vals[i];			\
					if (x == TYPE1##_nil) {		\
						if (!skip_nils) {	\
							sums[gid] = TYPE2##_nil; \
							nils++;		\
						}			\
					} else {			\
						if (nil_if_empty &&	\
						    !(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
							seen[gid >> 5] |= 1 << (gid & 0x1F); \
							sums[gid] = 0;	\
						}			\
						if (sums[gid] != TYPE2##_nil) { \
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
	unsigned int *restrict seen; /* bitmask for groups that we've seen */

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
#if SIZEOF_WRD == SIZEOF_INT
	case TYPE_wrd:
#endif
	case TYPE_int: {
		int *restrict sums = (int *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_SUM(bte, int);
			break;
		case TYPE_sht:
			AGGR_SUM(sht, int);
			break;
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
		case TYPE_int:
			AGGR_SUM(int, int);
			break;
		default:
			goto unsupported;
		}
		break;
	}
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
	case TYPE_lng: {
		lng *restrict sums = (lng *) results;
		switch (tp1) {
		case TYPE_bte:
			AGGR_SUM(bte, lng);
			break;
		case TYPE_sht:
			AGGR_SUM(sht, lng);
			break;
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
		case TYPE_int:
			AGGR_SUM(int, lng);
			break;
#if SIZEOF_WRD == SIZEOF_LNG
		case TYPE_wrd:
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
		switch (ATOMstorage(tp1)) {
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
	case TYPE_flt: {
		flt *restrict sums = (flt *) results;
		switch (tp1) {
		case TYPE_flt:
			AGGR_SUM(flt, flt);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_dbl: {
		dbl *restrict sums = (dbl *) results;
		switch (tp1) {
		case TYPE_flt:
			AGGR_SUM(flt, dbl);
			break;
		case TYPE_dbl:
			AGGR_SUM(dbl, dbl);
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
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
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
		bn = BATconstant(tp, ATOMnilptr(tp), ngrp, TRANSIENT);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->T->nonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		return BATconvert(b, s, tp, abort_on_error);
	}

	bn = BATconstant(tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	if (bn == NULL) {
		return NULL;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	nils = dosum(Tloc(b, BUNfirst(b)), b->T->nonil, b->hseqbase, start, end,
		     Tloc(bn, BUNfirst(bn)), ngrp, b->ttype, tp,
		     cand, candend, gids, min, max,
		     skip_nils, abort_on_error, 1, "BATgroupsum");

	if (nils < BUN_NONE) {
		BATsetcount(bn, ngrp);
		BATseqbase(bn, min);
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->T->nil = nils != 0;
		bn->T->nonil = nils == 0;
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
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	if ((err = BATgroupaggrinit(b, NULL, NULL, s, &min, &max, &ngrp,
				    &start, &end, &cnt,
				    &cand, &candend)) != NULL) {
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
#if SIZEOF_WRD == SIZEOF_INT
	case TYPE_wrd:
#endif
	case TYPE_int:
		* (int *) res = nil_if_empty ? int_nil : 0;
		break;
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
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
		case TYPE_wrd:
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
				if (avg == dbl_nil)
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
				if (avg == dbl_nil) {
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
	nils = dosum(Tloc(b, BUNfirst(b)), b->T->nonil, b->hseqbase, start, end,
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
				if (vals[i] == TYPE1##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else {				\
					if (nil_if_empty &&		\
					    !(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
						seen[gid >> 5] |= 1 << (gid & 0x1F); \
						prods[gid] = 1;		\
					}				\
					if (prods[gid] != TYPE2##_nil) { \
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
				    !(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
					seen[gid >> 5] |= 1 << (gid & 0x1F); \
					prods[gid] = 1;			\
				}					\
				if (vals[i] == TYPE##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = hge_nil;	\
						nils++;			\
					}				\
				} else if (prods[gid] != hge_nil) {	\
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
				if (vals[i] == TYPE##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = lng_nil;	\
						nils++;			\
					}				\
				} else {				\
					if (nil_if_empty &&		\
					    !(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
						seen[gid >> 5] |= 1 << (gid & 0x1F); \
						prods[gid] = 1;		\
					}				\
					if (prods[gid] != lng_nil) {	\
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
				if (vals[i] == TYPE1##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else {				\
					if (nil_if_empty && \
					    !(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
						seen[gid >> 5] |= 1 << (gid & 0x1F); \
						prods[gid] = 1;		\
					}				\
					if (prods[gid] != TYPE2##_nil) { \
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
#if SIZEOF_WRD == SIZEOF_INT
	case TYPE_wrd:
#endif
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
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
			AGGR_PROD(int, int, lng);
			break;
		default:
			goto unsupported;
		}
		break;
	}
#ifdef HAVE_HGE
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
	case TYPE_lng: {
		lng *prods = (lng *) results;
		switch (ATOMstorage(tp1)) {
		case TYPE_bte:
			AGGR_PROD(bte, lng, hge);
			break;
		case TYPE_sht:
			AGGR_PROD(sht, lng, hge);
			break;
		case TYPE_int:
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
			AGGR_PROD(int, lng, hge);
			break;
		case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
		case TYPE_wrd:
#endif
			AGGR_PROD(lng, lng, hge);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_hge: {
		hge *prods = (hge *) results;
		switch (ATOMstorage(tp1)) {
		case TYPE_bte:
			AGGR_PROD_HGE(bte);
			break;
		case TYPE_sht:
			AGGR_PROD_HGE(sht);
			break;
		case TYPE_int:
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
			AGGR_PROD_HGE(int);
			break;
		case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
		case TYPE_wrd:
#endif
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
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
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
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
			AGGR_PROD_LNG(int);
			break;
		case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
		case TYPE_wrd:
#endif
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
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
			AGGR_PROD_FLOAT(int, flt);
			break;
		case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
		case TYPE_wrd:
#endif
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
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
			AGGR_PROD_FLOAT(int, dbl);
			break;
		case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
		case TYPE_wrd:
#endif
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
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
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
		bn = BATconstant(tp, ATOMnilptr(tp), ngrp, TRANSIENT);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->T->nonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		return BATconvert(b, s, tp, abort_on_error);
	}

	bn = BATconstant(tp, ATOMnilptr(tp), ngrp, TRANSIENT);
	if (bn == NULL) {
		return NULL;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	nils = doprod(Tloc(b, BUNfirst(b)), b->hseqbase, start, end,
		      Tloc(bn, BUNfirst(bn)), ngrp, b->ttype, tp,
		      cand, candend, gids, 1, min, max,
		      skip_nils, abort_on_error, 1, "BATgroupprod");

	if (nils < BUN_NONE) {
		BATsetcount(bn, ngrp);
		BATseqbase(bn, min);
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->T->nil = nils != 0;
		bn->T->nonil = nils == 0;
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
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	if ((err = BATgroupaggrinit(b, NULL, NULL, s, &min, &max, &ngrp,
				    &start, &end, &cnt,
				    &cand, &candend)) != NULL) {
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
#if SIZEOF_WRD == SIZEOF_INT
	case TYPE_wrd:
#endif
		* (int *) res = nil_if_empty ? int_nil : (int) 1;
		break;
	case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
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
	nils = doprod(Tloc(b, BUNfirst(b)), b->hseqbase, start, end, res, 1,
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
		const TYPE *restrict vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
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
				if (vals[i] == TYPE##_nil) {		\
					if (!skip_nils)			\
						cnts[gid] = wrd_nil;	\
				} else if (cnts[gid] != wrd_nil) {	\
					AVERAGE_ITER(TYPE, vals[i],	\
						     avgs[gid],		\
						     rems[gid],		\
						     cnts[gid]);	\
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] == 0 || cnts[i] == wrd_nil) {	\
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
		const TYPE *restrict vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
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
				if (vals[i] == TYPE##_nil) {		\
					if (!skip_nils)			\
						cnts[gid] = wrd_nil;	\
				} else if (cnts[gid] != wrd_nil) {	\
					AVERAGE_ITER_FLOAT(TYPE, vals[i], \
							   dbls[gid],	\
							   cnts[gid]);	\
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] == 0 || cnts[i] == wrd_nil) {	\
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
	wrd *restrict cnts = NULL;
	dbl *restrict dbls;
	BAT *bn = NULL;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_dbl);
	(void) tp;		/* compatibility (with other BATgroup*
				 * functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
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
		bn = BATconstant(TYPE_dbl, &dbl_nil, ngrp, TRANSIENT);
		if (bn == NULL) {
			GDKerror("BATgroupavg: failed to create BAT\n");
			return GDK_FAIL;
		}
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		if (cntsp) {
			wrd zero = 0;
			if ((*cntsp = BATconstant(TYPE_wrd, &zero, ngrp, TRANSIENT)) == NULL) {
				GDKerror("BATgroupavg: failed to create BAT\n");
				BBPreclaim(bn);
				return GDK_FAIL;
			}
			BATseqbase(*cntsp, ngrp == 0 ? 0 : min);
		}
		*bnp = bn;
		return GDK_SUCCEED;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->T->nonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		if ((bn = BATconvert(b, s, TYPE_dbl, abort_on_error)) == NULL)
			return GDK_FAIL;
		if (cntsp) {
			wrd one = 1;
			if ((*cntsp = BATconstant(TYPE_wrd, &one, ngrp, TRANSIENT)) == NULL) {
				BBPreclaim(bn);
				return GDK_FAIL;
			}
			BATseqbase(*cntsp, ngrp == 0 ? 0 : min);
		}
		*bnp = bn;
		return GDK_SUCCEED;
	}

	/* allocate temporary space to do per group calculations */
	switch (b->ttype) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_wrd:
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
		if ((*cntsp = BATnew(TYPE_void, TYPE_wrd, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		cnts = (wrd *) Tloc(*cntsp, BUNfirst(*cntsp));
		memset(cnts, 0, ngrp * sizeof(wrd));
	} else {
		cnts = GDKzalloc(ngrp * sizeof(wrd));
		if (cnts == NULL)
			goto alloc_fail;
	}

	bn = BATnew(TYPE_void, TYPE_dbl, ngrp, TRANSIENT);
	if (bn == NULL)
		goto alloc_fail;
	dbls = (dbl *) Tloc(bn, BUNfirst(bn));

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	switch (b->ttype) {
	case TYPE_bte:
		AGGR_AVG(bte);
		break;
	case TYPE_sht:
		AGGR_AVG(sht);
		break;
	case TYPE_int:
#if SIZEOF_WRD == SIZEOF_INT
	case TYPE_wrd:
#endif
		AGGR_AVG(int);
		break;
	case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
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
		if (cntsp)
			BBPreclaim(*cntsp);
		else
			GDKfree(cnts);
		BBPunfix(bn->batCacheid);
		GDKerror("BATgroupavg: type (%s) not supported.\n",
			 ATOMname(b->ttype));
		return GDK_FAIL;
	}
	GDKfree(rems);
	if (cntsp == NULL)
		GDKfree(cnts);
	else {
		BATsetcount(*cntsp, ngrp);
		BATseqbase(*cntsp, min);
		(*cntsp)->tkey = BATcount(*cntsp) <= 1;
		(*cntsp)->tsorted = BATcount(*cntsp) <= 1;
		(*cntsp)->trevsorted = BATcount(*cntsp) <= 1;
		(*cntsp)->T->nil = 0;
		(*cntsp)->T->nonil = 1;
	}
	BATsetcount(bn, ngrp);
	BATseqbase(bn, min);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	*bnp = bn;
	return GDK_SUCCEED;

  alloc_fail:
	if (bn)
		BBPunfix(bn->batCacheid);
	GDKfree(rems);
	if (cntsp) {
		BBPreclaim(*cntsp);
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
			if (x == TYPE##_nil)				\
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
		*avg = (dbl) sum / n;					\
		if (0) {						\
		  overflow##TYPE:					\
			/* we get here if sum(x[0],...,x[i]) doesn't */	\
			/* fit in a lng/hge but sum(x[0],...,x[i-1]) did */ \
			/* the variable sum contains that sum */	\
			/* the rest of the calculation is done */	\
			/* according to the loop invariant described */	\
			/* in the below loop */				\
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
			for (; i < end; i++) {				\
				/* loop invariant: */			\
				/* a + r/n == average(x[0],...,x[n]); */ \
				/* 0 <= r < n (if n > 0) */		\
				/* or if n == 0: a == 0; r == 0 */	\
				if (cand) {				\
					if (i < *cand - b->H->seq)	\
						continue;		\
					assert(i == *cand - b->H->seq);	\
					if (++cand == candend)		\
						end = i + 1;		\
				}					\
				x = ((const TYPE *) src)[i];		\
				if (x == TYPE##_nil)			\
					continue;			\
				AVERAGE_ITER(TYPE, x, a, r, n);		\
			}						\
			*avg = n > 0 ? a + (dbl) r / n : dbl_nil;	\
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
			if (x == TYPE##_nil)			\
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

	src = Tloc(b, b->batFirst);

	switch (b->T->type) {
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
			 ATOMname(b->T->type));
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
		const TYPE *restrict vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
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
				if (!skip_nils || vals[i] != TYPE##_nil) { \
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
	wrd *restrict cnts;
	BAT *bn = NULL;
	int t;
	const void *nil;
	int (*atomcmp)(const void *, const void *);
	BATiter bi;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_wrd);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
		GDKerror("BATgroupcount: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupcount: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no products, so return bat aligned with g
		 * with zero in the tail */
		wrd zero = 0;
		bn = BATconstant(TYPE_wrd, &zero, ngrp, TRANSIENT);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	bn = BATnew(TYPE_void, TYPE_wrd, ngrp, TRANSIENT);
	if (bn == NULL)
		return NULL;
	cnts = (wrd *) Tloc(bn, BUNfirst(bn));
	memset(cnts, 0, ngrp * sizeof(wrd));

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	t = b->T->type;
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
				if (i >= end)
					break;
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
				if (!skip_nils ||
				    (*atomcmp)(BUNtail(bi, i + BUNfirst(b)),
					       nil) != 0) {
					cnts[gid]++;
				}
			}
		}
		break;
	}
	BATsetcount(bn, ngrp);
	BATseqbase(bn, min);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->T->nil = 0;
	bn->T->nonil = 1;
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
	wrd *restrict cnts;
	BAT *bn = NULL;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_wrd);
	assert(b->ttype == TYPE_bit);
	/* compatibility arguments */
	(void) tp;
	(void) abort_on_error;
	(void) skip_nils;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
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
		wrd zero = 0;
		bn = BATconstant(TYPE_wrd, &zero, ngrp, TRANSIENT);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	bn = BATnew(TYPE_void, TYPE_wrd, ngrp, TRANSIENT);
	if (bn == NULL)
		return NULL;
	cnts = (wrd *) Tloc(bn, BUNfirst(bn));
	memset(cnts, 0, ngrp * sizeof(wrd));

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	bits = (const bit *) Tloc(b, BUNfirst(b));

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
	BATseqbase(bn, min);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	return bn;
}

/* ---------------------------------------------------------------------- */
/* min and max */

#define AGGR_CMP(TYPE, OP)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
		if (ngrp == cnt) {					\
			/* single element groups */			\
			if (cand) {					\
				while (cand < candend) {		\
					i = *cand++ - b->hseqbase;	\
					if (i >= end)			\
						break;			\
					if (!skip_nils ||		\
					    vals[i] != TYPE##_nil) {	\
						oids[i] = i + b->hseqbase; \
						nils--;			\
					}				\
				}					\
			} else {					\
				for (i = start; i < end; i++) {		\
					if (!skip_nils ||		\
					    vals[i] != TYPE##_nil) {	\
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
					if (!skip_nils || vals[i] != TYPE##_nil) { \
						if (oids[gid] == oid_nil) { \
							oids[gid] = i + b->hseqbase; \
							nils--;		\
						} else if (vals[oids[gid] - b->hseqbase] != TYPE##_nil && \
							   (vals[i] == TYPE##_nil || \
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

	t = b->T->type;
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
		assert(b->ttype != TYPE_wrd);
		bi = bat_iterator(b);

		if (gdense) {
			/* single element groups */
			if (cand) {
				while (cand < candend) {
					i = *cand++ - b->hseqbase;
					if (i >= end)
						break;
					if (!skip_nils ||
					    (*atomcmp)(BUNtail(bi, i + BUNfirst(b)), nil) != 0) {
						oids[i] = i + b->hseqbase;
						nils--;
					}
				}
			} else {
				for (i = start; i < end; i++) {
					if (!skip_nils ||
					    (*atomcmp)(BUNtail(bi, i + BUNfirst(b)), nil) != 0) {
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
					const void *v = BUNtail(bi, i + BUNfirst(b));
					if (gids)
						gid = gids[i] - min;
					if (!skip_nils ||
					    (*atomcmp)(v, nil) != 0) {
						if (oids[gid] == oid_nil) {
							oids[gid] = i + b->hseqbase;
							nils--;
						} else if (t != TYPE_void) {
							const void *g = BUNtail(bi, (BUN) (oids[gid] - b->hseqbase) + BUNfirst(b));
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

	t = b->T->type;
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
		assert(b->ttype != TYPE_wrd);
		bi = bat_iterator(b);

		if (gdense) {
			/* single element groups */
			if (cand) {
				while (cand < candend) {
					i = *cand++ - b->hseqbase;
					if (i >= end)
						break;
					if (!skip_nils ||
					    (*atomcmp)(BUNtail(bi, i + BUNfirst(b)), nil) != 0) {
						oids[i] = i + b->hseqbase;
						nils--;
					}
				}
			} else {
				for (i = start; i < end; i++) {
					if (!skip_nils ||
					    (*atomcmp)(BUNtail(bi, i + BUNfirst(b)), nil) != 0) {
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
					const void *v = BUNtail(bi, i + BUNfirst(b));
					if (gids)
						gid = gids[i] - min;
					if (!skip_nils ||
					    (*atomcmp)(v, nil) != 0) {
						if (oids[gid] == oid_nil) {
							oids[gid] = i + b->hseqbase;
							nils--;
						} else {
							const void *g = BUNtail(bi, (BUN) (oids[gid] - b->hseqbase) + BUNfirst(b));
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
	BUN start, end, cnt;
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
				    &cnt, &cand, &candend)) != NULL) {
		GDKerror("%s: %s\n", name, err);
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no minimums, so return bat aligned with g
		 * with nil in the tail */
		bn = BATconstant(TYPE_oid, &oid_nil, ngrp, TRANSIENT);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	bn = BATnew(TYPE_void, TYPE_oid, ngrp, TRANSIENT);
	if (bn == NULL)
		return NULL;
	oids = (oid *) Tloc(bn, BUNfirst(bn));

	if (g == NULL || BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	nils = (*minmax)(oids, b, gids, ngrp, min, max, start, end,
			 cand, candend, cnt, skip_nils, g && BATtdense(g));

	BATsetcount(bn, ngrp);

	BATseqbase(bn, min);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	return bn;
}

static void *
BATminmax(BAT *b, void *aggr,
	  BUN (*minmax)(oid *restrict, BAT *, const oid *restrict, BUN,
			oid, oid, BUN, BUN, const oid *restrict,
			const oid *, BUN, int, int))
{
	oid pos;
	void *res;
	int s;
	int needdecref = 0;
	BATiter bi;

	if (!BAThdense(b))
		return NULL;
	if ((VIEWtparent(b) == 0 ||
	     BATcount(b) == BATcount(BBPdescriptor(VIEWtparent(b)))) &&
	    BATcheckimprints(b)) {
		Imprints *imprints = VIEWtparent(b) ? BBPdescriptor(-VIEWtparent(b))->T->imprints : b->T->imprints;
		pos = oid_nil;
		if (minmax == do_groupmin) {
			/* find first non-empty bin */
			for (s = 0; s < imprints->bits; s++) {
				if (imprints->stats[s + 128]) {
					pos = imprints->stats[s] + b->hseqbase;
					break;
				}
			}
		} else {
			/* find last non-empty bin */
			for (s = imprints->bits - 1; s >= 0; s--) {
				if (imprints->stats[s + 128]) {
					pos = imprints->stats[s + 64] + b->hseqbase;
					break;
				}
			}
		}
	} else {
		(void) (*minmax)(&pos, b, NULL, 1, 0, 0, 0, BATcount(b),
				 NULL, NULL, BATcount(b), 1, 0);
	}
	if (pos == oid_nil) {
		res = ATOMnilptr(b->ttype);
	} else {
		bi = bat_iterator(b);
		res = BUNtail(bi, pos + BUNfirst(b) - b->hseqbase);
	}
	if (aggr == NULL) {
		s = ATOMlen(b->ttype, res);
		aggr = GDKmalloc(s);
	} else {
		s = ATOMsize(ATOMtype(b->ttype));
	}
	if (aggr != NULL)	/* else: malloc error */
		memcpy(aggr, res, s);
	if (needdecref)
		BBPunfix(b->batCacheid);
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

BAT *
BATgroupquantile(BAT *b, BAT *g, BAT *e, BAT *s, int tp, double quantile,
		 int skip_nils, int abort_on_error)
{
	int freeb = 0, freeg = 0;
	oid min, max;
	BUN ngrp;
	BUN nils = 0;
	BAT *bn = NULL;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	BAT *t1, *t2;
	BATiter bi;
	const void *v;
	const void *nil;
	int (*atomcmp)(const void *, const void *);
	const char *err;
	(void) abort_on_error;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
		GDKerror("BATgroupquantile: %s\n", err);
		return NULL;
	}
	assert(tp == b->ttype);
	if (!ATOMlinear(b->ttype)) {
		GDKerror("BATgroupquantile: cannot determine quantile on "
			 "non-linear type %s\n", ATOMname(b->ttype));
		return NULL;
	}
	if (quantile < 0 || quantile > 1) {
		GDKerror("BATgroupquantile: cannot determine quantile for "
			 "p=%f (p has to be in [0,1])\n", quantile);
		return NULL;
	}
	assert(quantile >=0 && quantile <=1);

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no values, thus also no quantiles,
		 * so return bat aligned with e with nil in the tail */
		bn = BATconstant(tp, ATOMnilptr(tp), ngrp, TRANSIENT);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	if (s) {
		/* there is a candidate list, replace b (and g, if
		 * given) with just the values we're interested in */
		b = BATproject(s, b);
		freeb = 1;
		if (g) {
			g = BATproject(s, g);
			freeg = 1;
		}
	}

	/* we want to sort b so that we can figure out the quantile, but
	 * if g is given, sort g and subsort b so that we can get the
	 * quantile for each group */
	if (g) {
		if (BATtdense(g)) {
			/* singleton groups, so calculating quantile is
			 * easy */
			bn = COLcopy(b, b->ttype, 0, TRANSIENT);
			BATseqbase(bn, g->tseqbase);
			if (freeg)
				BBPunfix(g->batCacheid);
			return bn;
		}
		BATsort(&t1, &t2, NULL, g, NULL, NULL, 0, 0);
		if (freeg)
			BBPunfix(g->batCacheid);
		g = t1;
		freeg = 1;
	} else {
		t2 = NULL;
	}
	BATsort(&t1, NULL, NULL, b, t2, g, 0, 0);
	if (freeb)
		BBPunfix(b->batCacheid);
	b = t1;
	freeb = 1;
	if (t2)
		BBPunfix(t2->batCacheid);

	bn = BATnew(TYPE_void, b->ttype, ngrp, TRANSIENT);
	if (bn == NULL)
		return NULL;

	bi = bat_iterator(b);
	nil = ATOMnilptr(b->ttype);
	atomcmp = ATOMcompare(b->ttype);

	if (g) { /* we have to do this by group */
		const oid *restrict grps;
		oid prev;
		BUN p, q, r;

		grps = (const oid *) Tloc(g, BUNfirst(g));
		prev = grps[0];
		 /* for each group (r and p are the beginning and end
		  * of the current group, respectively) */
		for (r = 0, p = 1, q = BATcount(g); p <= q; p++) {
			assert(r < p);
			if ( p == q || grps[p] != prev) {
				BUN qindex;
				if (skip_nils) {
					while (r < p && (*atomcmp)(BUNtail(bi, BUNfirst(b) + r), nil) == 0)
						r++;
					if (r == p)
						break;
				}
				while (BATcount(bn) < prev - min) {
					bunfastapp_nocheck(bn, BUNlast(bn),
							   nil, Tsize(bn));
					nils++;
				}
				qindex = BUNfirst(b) + (BUN) (r + (p-r-1) * quantile);
				/* be a little paranoid about the index */
				assert(qindex >= (BUNfirst(b) + r ));
				assert(qindex <  (BUNfirst(b) + p));
				v = BUNtail(bi, qindex);
				bunfastapp_nocheck(bn, BUNlast(bn), v, Tsize(bn));
				nils += (*atomcmp)(v, nil) == 0;

				r = p;
				if (p < q)
					prev = grps[p];
			}
		}
		nils += ngrp - BATcount(bn);
		while (BATcount(bn) < ngrp) {
			bunfastapp_nocheck(bn, BUNlast(bn), nil, Tsize(bn));
		}
		BATseqbase(bn, min);
	} else { /* quantiles for entire BAT b, EZ */

		BUN index, r = 0, p = BATcount(b);

		if (skip_nils) {
			while (r < p && (*atomcmp)(BUNtail(bi, BUNfirst(b) + r), nil) == 0)
				r++;
		}
		index = BUNfirst(b) + (BUN) (r + (p-r-1) * quantile);
		v = BUNtail(bi, index);
		BUNappend(bn, v, FALSE);
		BATseqbase(bn, 0);
		nils += (*atomcmp)(v, nil) == 0;
	}

	if (freeb)
		BBPunfix(b->batCacheid);
	if (freeg)
		BBPunfix(g->batCacheid);

	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	return bn;

  bunins_failed:
	if (freeb)
		BBPunfix(b->batCacheid);
	if (freeg)
		BBPunfix(g->batCacheid);
	BBPunfix(bn->batCacheid);
	return NULL;
}

/* ---------------------------------------------------------------------- */
/* standard deviation (both biased and non-biased) */

#define AGGR_STDEV_SINGLE(TYPE)						\
	do {								\
		TYPE x;							\
		for (i = 0; i < cnt; i++) {				\
			x = ((const TYPE *) values)[i];			\
			if (x == TYPE##_nil)				\
				continue;				\
			n++;						\
			delta = (dbl) x - mean;				\
			mean += delta / n;				\
			m2 += delta * ((dbl) x - mean);			\
		}							\
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
#if SIZEOF_WRD == SIZEOF_INT
	case TYPE_wrd:
#endif
		AGGR_STDEV_SINGLE(int);
		break;
	case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
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
	dbl v = calcvariance(avgp, (const void *) Tloc(b, BUNfirst(b)),
			     BATcount(b), b->ttype, 0,
			     "BATcalcstdev_population");
	return v == dbl_nil ? dbl_nil : sqrt(v);
}

dbl
BATcalcstdev_sample(dbl *avgp, BAT *b)
{
	dbl v = calcvariance(avgp, (const void *) Tloc(b, BUNfirst(b)),
			     BATcount(b), b->ttype, 1,
			     "BATcalcstdev_sample");
	return v == dbl_nil ? dbl_nil : sqrt(v);
}

dbl
BATcalcvariance_population(dbl *avgp, BAT *b)
{
	return calcvariance(avgp, (const void *) Tloc(b, BUNfirst(b)),
			    BATcount(b), b->ttype, 0,
			    "BATcalcvariance_population");
}

dbl
BATcalcvariance_sample(dbl *avgp, BAT *b)
{
	return calcvariance(avgp, (const void *) Tloc(b, BUNfirst(b)),
			    BATcount(b), b->ttype, 1,
			    "BATcalcvariance_sample");
}

#define AGGR_STDEV(TYPE)						\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
		for (;;) {						\
			if (cand) {					\
				if (cand == candend)			\
					break;				\
				i = *cand++ - b->hseqbase;			\
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
				if (vals[i] == TYPE##_nil) {		\
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
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_dbl);
	(void) tp;		/* compatibility (with other BATgroup*
				 * functions) argument */

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
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
		bn = BATconstant(TYPE_dbl, &dbl_nil, ngrp, TRANSIENT);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->T->nonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to zero (population) or nil (sample) */
		dbl v = issample ? dbl_nil : 0;
		bn = BATconstant(TYPE_dbl, &v, ngrp, TRANSIENT);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	delta = GDKmalloc(ngrp * sizeof(dbl));
	m2 = GDKmalloc(ngrp * sizeof(dbl));
	cnts = GDKzalloc(ngrp * sizeof(BUN));
	if (avgb) {
		if ((*avgb = BATnew(TYPE_void, TYPE_dbl, ngrp, TRANSIENT)) == NULL) {
			mean = NULL;
			goto alloc_fail;
		}
		mean = (dbl *) Tloc(*avgb, BUNfirst(*avgb));
	} else {
		mean = GDKmalloc(ngrp * sizeof(dbl));
	}
	if (mean == NULL || delta == NULL || m2 == NULL || cnts == NULL)
		goto alloc_fail;

	bn = BATnew(TYPE_void, TYPE_dbl, ngrp, TRANSIENT);
	if (bn == NULL)
		goto alloc_fail;
	dbls = (dbl *) Tloc(bn, BUNfirst(bn));

	for (i = 0; i < ngrp; i++) {
		mean[i] = 0;
		delta[i] = 0;
		m2[i] = 0;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	switch (b->ttype) {
	case TYPE_bte:
		AGGR_STDEV(bte);
		break;
	case TYPE_sht:
		AGGR_STDEV(sht);
		break;
	case TYPE_int:
#if SIZEOF_WRD == SIZEOF_INT
	case TYPE_wrd:
#endif
		AGGR_STDEV(int);
		break;
	case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
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
		BATseqbase(*avgb, 0);
		(*avgb)->tkey = ngrp <= 1;
		(*avgb)->tsorted = ngrp <= 1;
		(*avgb)->trevsorted = ngrp <= 1;
		(*avgb)->T->nil = nils != 0;
		(*avgb)->T->nonil = nils == 0;
	} else {
		GDKfree(mean);
	}
	nils += nils2;
	GDKfree(delta);
	GDKfree(m2);
	GDKfree(cnts);
	BATsetcount(bn, ngrp);
	BATseqbase(bn, min);
	bn->tkey = ngrp <= 1;
	bn->tsorted = ngrp <= 1;
	bn->trevsorted = ngrp <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
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
