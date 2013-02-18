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
 * the result of BATsubselect().
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
BATgroupaggrinit(const BAT *b, const BAT *g, const BAT *e, const BAT *s,
		 /* outputs: */
		 oid *minp, oid *maxp, BUN *ngrpp, BUN *startp, BUN *endp,
		 BUN *cntp, const oid **candp, const oid **candendp)
{
	oid min, max;
	BUN i, ngrp;
	const oid *gids;
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

	CANDINIT(b, s);
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
		const TYPE1 *vals = (const TYPE1 *) values;		\
		if (ngrp == 1 && cand == NULL) {			\
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
							       goto overflow); \
						seenval = 1;		\
					}				\
				}					\
				*seen = seenval;			\
			}						\
			if (*seen)					\
				*sums = sum;				\
		} else if (ngrp == 1) {					\
			TYPE2 sum;					\
			int seenval = 0;				\
			ALGODEBUG fprintf(stderr,			\
					  "#%s: with candidates, no groups; " \
					  "start " BUNFMT ", end " BUNFMT \
					  "\n",				\
					  func, start, end);		\
			sum = 0;					\
			while (cand < candend) {			\
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
						       goto overflow);	\
					seenval = 1;			\
				}					\
			}						\
			if (seenval)					\
				*sums = sum;				\
		} else if (cand == NULL) {				\
			ALGODEBUG fprintf(stderr,			\
					  "#%s: no candidates, with groups; " \
					  "start " BUNFMT ", end " BUNFMT \
					  "\n",				\
					  func, start, end);		\
			for (i = start; i < end; i++) {			\
				if (gids == NULL ||			\
				    (gids[i] >= min && gids[i] <= max)) { \
					gid = gids ? gids[i] - min : (oid) i; \
					if (nil_if_empty &&		\
					    !(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
						seen[gid >> 5] |= 1 << (gid & 0x1F); \
						sums[gid] = 0;		\
					}				\
					x = vals[i];			\
					if (x == TYPE1##_nil) {		\
						if (!skip_nils) {	\
							sums[gid] = TYPE2##_nil; \
							nils++;		\
						}			\
					} else if (sums[gid] != TYPE2##_nil) { \
						ADD_WITH_CHECK(TYPE1, x, \
							       TYPE2,	\
							       sums[gid], \
							       TYPE2,	\
							       sums[gid], \
							       goto overflow); \
					}				\
				}					\
			}						\
		} else {						\
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
					if (nil_if_empty &&		\
					    !(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
						seen[gid >> 5] |= 1 << (gid & 0x1F); \
						sums[gid] = 0;		\
					}				\
					x = vals[i];			\
					if (x == TYPE1##_nil) {		\
						if (!skip_nils) {	\
							sums[gid] = TYPE2##_nil; \
							nils++;		\
						}			\
					} else if (sums[gid] != TYPE2##_nil) { \
						ADD_WITH_CHECK(TYPE1, x, \
							       TYPE2,	\
							       sums[gid], \
							       TYPE2,	\
							       sums[gid], \
							       goto overflow); \
					}				\
				}					\
			}						\
		}							\
	} while (0)

static BUN
dosum(const void *values, int nonil, oid seqb, BUN start, BUN end,
      void *results, BUN ngrp, int tp1, int tp2,
      const oid *cand, const oid *candend, const oid *gids,
      oid min, oid max, int skip_nils, int abort_on_error,
      int nil_if_empty, const char *func)
{
	BUN nils = 0;
	BUN i;
	oid gid;
	unsigned int *seen;	/* bitmask for groups that we've seen */

	/* allocate bitmap for seen group ids */
	seen = GDKzalloc(((ngrp + 31) / 32) * sizeof(int));
	if (seen == NULL) {
		GDKerror("%s: cannot allocate enough memory\n", func);
		return BUN_NONE;
	}

	switch (ATOMstorage(tp2)) {
	case TYPE_bte: {
		bte *sums = (bte *) results;
		switch (ATOMstorage(tp1)) {
		case TYPE_bte:
			AGGR_SUM(bte, bte);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_sht: {
		sht *sums = (sht *) results;
		switch (ATOMstorage(tp1)) {
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
		int *sums = (int *) results;
		switch (ATOMstorage(tp1)) {
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
		lng *sums = (lng *) results;
		switch (ATOMstorage(tp1)) {
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
	case TYPE_flt: {
		flt *sums = (flt *) results;
		switch (ATOMstorage(tp1)) {
		case TYPE_flt:
			AGGR_SUM(flt, flt);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_dbl: {
		dbl *sums = (dbl *) results;
		switch (ATOMstorage(tp1)) {
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
	const oid *gids;
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
		bn = BATconstant(tp, ATOMnilptr(tp), ngrp);
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

	bn = BATconstant(tp, ATOMnilptr(tp), ngrp);
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
	switch (ATOMstorage(tp)) {
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
	case TYPE_flt:
	case TYPE_dbl:
		switch (ATOMstorage(b->ttype)) {
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_int:
		case TYPE_lng:
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

			if (BATcalcavg(b, s, &avg, &cnt) == GDK_FAIL)
				return GDK_FAIL;
			if (cnt == 0) {
				avg = nil_if_empty ? dbl_nil : 0;
			}
			if (cnt < BATcount(b) && !skip_nils) {
				/* there were nils */
				avg = dbl_nil;
			}
			if (ATOMstorage(tp) == TYPE_flt) {
				if (avg == dbl_nil)
					*(flt *) res = flt_nil;
				else if (cnt > 0 &&
					 GDK_flt_max / cnt < ABS(avg)) {
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
					   GDK_dbl_max / cnt < ABS(avg)) {
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
		if (ATOMstorage(b->ttype) == TYPE_dbl)
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
		const TYPE1 *vals = (const TYPE1 *) values;		\
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
				if (vals[i] == TYPE1##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else if (prods[gid] != TYPE2##_nil) {	\
					MUL4_WITH_CHECK(TYPE1, vals[i],	\
							TYPE2, prods[gid], \
							TYPE2, prods[gid], \
							TYPE3,		\
							goto overflow);	\
				}					\
			}						\
		}							\
	} while (0)

#define AGGR_PROD_LNG(TYPE)						\
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
						prods[gid] = lng_nil;	\
						nils++;			\
					}				\
				} else if (prods[gid] != lng_nil) {	\
					LNGMUL_CHECK(TYPE, vals[i],	\
						     lng, prods[gid],	\
						     prods[gid],	\
						     goto overflow);	\
				}					\
			}						\
		}							\
	} while (0)

#define AGGR_PROD_FLOAT(TYPE1, TYPE2)					\
	do {								\
		const TYPE1 *vals = (const TYPE1 *) values;		\
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
				if (vals[i] == TYPE1##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else if (prods[gid] != TYPE2##_nil) {	\
					if (ABSOLUTE(vals[i]) > 1 &&	\
					    GDK_##TYPE2##_max / ABSOLUTE(vals[i]) < ABSOLUTE(prods[gid])) { \
						if (abort_on_error)	\
							goto overflow;	\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					} else {			\
						prods[gid] *= vals[i];	\
					}				\
				}					\
			}						\
		}							\
	} while (0)

static BUN
doprod(const void *values, oid seqb, BUN start, BUN end, void *results,
       BUN ngrp, int tp1, int tp2, const oid *cand, const oid *candend,
       const oid *gids, int gidincr, oid min, oid max,
       int skip_nils, int abort_on_error, int nil_if_empty, const char *func)
{
	BUN nils = 0;
	BUN i;
	oid gid;
	unsigned int *seen;	/* bitmask for groups that we've seen */

	/* allocate bitmap for seen group ids */
	seen = GDKzalloc(((ngrp + 31) / 32) * sizeof(int));
	if (seen == NULL) {
		GDKerror("%s: cannot allocate enough memory\n", func);
		return GDK_FAIL;
	}

	switch (ATOMstorage(tp2)) {
	case TYPE_bte: {
		bte *prods = (bte *) results;
		switch (ATOMstorage(tp1)) {
		case TYPE_bte:
			AGGR_PROD(bte, bte, sht);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_sht: {
		sht *prods = (sht *) results;
		switch (ATOMstorage(tp1)) {
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
		int *prods = (int *) results;
		switch (ATOMstorage(tp1)) {
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
	case TYPE_lng: {
		lng *prods = (lng *) results;
		switch (ATOMstorage(tp1)) {
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
	case TYPE_flt: {
		flt *prods = (flt *) results;
		switch (ATOMstorage(tp1)) {
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
		case TYPE_flt:
			AGGR_PROD_FLOAT(flt, flt);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_dbl: {
		dbl *prods = (dbl *) results;
		switch (ATOMstorage(tp1)) {
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
	const oid *gids;
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
		bn = BATconstant(tp, ATOMnilptr(tp), ngrp);
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

	bn = BATconstant(tp, ATOMnilptr(tp), ngrp);
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
	switch (ATOMstorage(tp)) {
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
			while (z2 >= (n)) {				\
				z2 -= (n);				\
				z1++;					\
			}						\
		} else {						\
			z2 = (BUN) (an - xn);				\
			/* loop invariant (until we break): */		\
			/* (x - a) - z1 * n == -z2 */			\
			for (;;) {					\
				z1--;					\
				if (z2 < (n)) {				\
					/* proper remainder */		\
					z2 = (n) - z2;			\
					break;				\
				}					\
				z2 -= (n);				\
			}						\
		}							\
		(a) += z1;						\
		(r) += z2;						\
		if ((r) >= (n)) {					\
			(r) -= (n);					\
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
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
		TYPE *avgs = GDKzalloc(ngrp * sizeof(TYPE));		\
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
						cnts[gid] = BUN_NONE;	\
				} else if (cnts[gid] != BUN_NONE) {	\
					AVERAGE_ITER(TYPE, vals[i],	\
						     avgs[gid],		\
						     rems[gid],		\
						     cnts[gid]);	\
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] == 0 || cnts[i] == BUN_NONE) {	\
				dbls[i] = dbl_nil;			\
				nils++;					\
			} else {					\
				dbls[i] = avgs[i] + (dbl) rems[i] / cnts[i]; \
			}						\
		}							\
		GDKfree(avgs);						\
	} while (0)

#define AGGR_AVG_FLOAT(TYPE)						\
	do {								\
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
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
						cnts[gid] = BUN_NONE;	\
				} else if (cnts[gid] != BUN_NONE) {	\
					AVERAGE_ITER_FLOAT(TYPE, vals[i], \
							   dbls[gid],	\
							   cnts[gid]);	\
				}					\
			}						\
		}							\
		for (i = 0; i < ngrp; i++) {				\
			if (cnts[i] == 0 || cnts[i] == BUN_NONE) {	\
				dbls[i] = dbl_nil;			\
				nils++;					\
			}						\
		}							\
	} while (0)

/* calculate group averages with optional candidates list */
BAT *
BATgroupavg(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	BUN nils = 0;
	BUN *rems = NULL, *cnts = NULL;
	dbl *dbls;
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
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupavg: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no products, so return bat aligned with g
		 * with nil in the tail */
		bn = BATconstant(TYPE_dbl, &dbl_nil, ngrp);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->T->nonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		return BATconvert(b, s, TYPE_dbl, abort_on_error);
	}

	/* allocate temporary space to do per group calculations */
	switch (ATOMstorage(b->ttype)) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
		rems = GDKzalloc(ngrp * sizeof(BUN));
		if (rems == NULL)
			goto alloc_fail;
		break;
	default:
		break;
	}
	cnts = GDKzalloc(ngrp * sizeof(BUN));
	if (cnts == NULL)
		goto alloc_fail;

	bn = BATnew(TYPE_void, TYPE_dbl, ngrp);
	if (bn == NULL)
		goto alloc_fail;
	dbls = (dbl *) Tloc(bn, BUNfirst(bn));

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	switch (ATOMstorage(b->ttype)) {
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
	case TYPE_flt:
		AGGR_AVG_FLOAT(flt);
		break;
	case TYPE_dbl:
		AGGR_AVG_FLOAT(dbl);
		break;
	default:
		GDKfree(rems);
		GDKfree(cnts);
		BBPunfix(bn->batCacheid);
		GDKerror("BATgroupavg: type (%s) not supported.\n",
			 ATOMname(b->ttype));
		return NULL;
	}
	GDKfree(rems);
	GDKfree(cnts);
	BATsetcount(bn, ngrp);
	BATseqbase(bn, min);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	return bn;

  alloc_fail:
	if (bn)
		BBPunfix(bn->batCacheid);
	GDKfree(rems);
	GDKfree(cnts);
	GDKerror("BATgroupavg: cannot allocate enough memory.\n");
	return NULL;
}

#define AVERAGE_TYPE(TYPE)						\
	do {								\
		TYPE x, a;						\
									\
		/* first try to calculate the sum of all values into a */ \
		/* lng */						\
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
				       lng, sum,			\
				       lng, sum,			\
				       goto overflow##TYPE);		\
			/* don't count value until after overflow check */ \
			n++;						\
		}							\
		/* the sum fit, so now we can calculate the average */	\
		*avg = (dbl) sum / n;					\
		if (0) {						\
		  overflow##TYPE:					\
			/* we get here if sum(x[0],...,x[i]) doesn't */	\
			/* fit in a lng but sum(x[0],...,x[i-1]) did */ \
			/* the variable sum contains that sum */	\
			/* the rest of the calculation is done */	\
			/* according to the loop invariant described */	\
			/* in the below loop */				\
			if (sum >= 0) {					\
				a = (TYPE) (sum / (lng) n); /* this fits */ \
				r = (BUN) (sum % (SBUN) n);		\
			} else {					\
				sum = -sum;				\
				a = - (TYPE) (sum / (lng) n); /* this fits */ \
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

int
BATcalcavg(BAT *b, BAT *s, dbl *avg, BUN *vals)
{
	BUN n = 0, r = 0, i = 0;
	lng sum = 0;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const void *src;
	/* these two needed for ADD_WITH_CHECK macro */
	int abort_on_error = 1;
	BUN nils = 0;

	CANDINIT(b, s);

	src = Tloc(b, b->U->first);

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
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
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
	const oid *gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	wrd *cnts;
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
		bn = BATconstant(TYPE_wrd, &zero, ngrp);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	bn = BATnew(TYPE_void, TYPE_wrd, ngrp);
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
	atomcmp = BATatoms[t].atomCmp;
	if (t != ATOMstorage(t) &&
	    ATOMnilptr(ATOMstorage(t)) == nil &&
	    BATatoms[ATOMstorage(t)].atomCmp == atomcmp)
		t = ATOMstorage(t);
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
	const oid *gids;
	oid min, max;
	BUN i, ngrp;
	const bit *bits;
	wrd *cnts;
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
		bn = BATconstant(TYPE_wrd, &zero, ngrp);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	bn = BATnew(TYPE_void, TYPE_wrd, ngrp);
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
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
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
					if (oids[gid] == oid_nil) {	\
						oids[gid] = i + b->hseqbase; \
						nils--;			\
					} else if (vals[oids[gid] - b->hseqbase] != TYPE##_nil && \
						   (vals[i] == TYPE##_nil || \
						    OP(vals[i], vals[oids[gid] - b->hseqbase]))) \
						oids[gid] = i + b->hseqbase; \
				}					\
			}						\
		}							\
	} while (0)

/* calculate group minimums with optional candidates list
 *
 * note that this functions returns *positions* of where the minimum
 * values occur */
BAT *
BATgroupmin(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	oid *oids;
	BAT *bn = NULL;
	BUN nils;
	int t;
	const void *nil;
	int (*atomcmp)(const void *, const void *);
	BATiter bi;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_oid);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if (!ATOMlinear(b->ttype)) {
		GDKerror("BATgroupmin: cannot determine minimum on "
			 "non-linear type %s\n", ATOMname(b->ttype));
		return NULL;
	}

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
		GDKerror("BATgroupmin: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupmin: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no products, so return bat aligned with g
		 * with nil in the tail */
		bn = BATconstant(TYPE_oid, &oid_nil, ngrp);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	bn = BATnew(TYPE_void, TYPE_oid, ngrp);
	if (bn == NULL)
		return NULL;
	oids = (oid *) Tloc(bn, BUNfirst(bn));
	nils = ngrp;
	for (i = 0; i < ngrp; i++)
		oids[i] = oid_nil;

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	t = b->T->type;
	nil = ATOMnilptr(t);
	atomcmp = BATatoms[t].atomCmp;
	if (t != ATOMstorage(t) &&
	    ATOMnilptr(ATOMstorage(t)) == nil &&
	    BATatoms[ATOMstorage(t)].atomCmp == atomcmp)
		t = ATOMstorage(t);
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
	case TYPE_oid:
		AGGR_CMP(oid, LT);
		break;
	case TYPE_lng:
		AGGR_CMP(lng, LT);
		break;
	case TYPE_flt:
		AGGR_CMP(flt, LT);
		break;
	case TYPE_dbl:
		AGGR_CMP(dbl, LT);
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
				const void *v = BUNtail(bi, i + BUNfirst(b));
				if (gids)
					gid = gids[i] - min;
				else
					gid = (oid) i;
				if (!skip_nils || (*atomcmp)(v, nil) != 0) {
					if (oids[gid] == oid_nil) {
						oids[gid] = i + b->hseqbase;
						nils--;
					} else {
						const void *g = BUNtail(bi, (BUN) (oids[gid] - b->hseqbase) + BUNfirst(b));
						if ((*atomcmp)(g, nil) != 0 &&
						   ((*atomcmp)(v, nil) == 0 ||
						    LT((*atomcmp)(v, g), 0)))
							oids[gid] = i + b->hseqbase;
					}
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
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	return bn;
}

/* calculate group maximums with optional candidates list
 *
 * note that this functions returns *positions* of where the maximum
 * values occur */
BAT *
BATgroupmax(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	oid *oids;
	BAT *bn = NULL;
	BUN nils;
	int t;
	const void *nil;
	int (*atomcmp)(const void *, const void *);
	BATiter bi;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const char *err;

	assert(tp == TYPE_oid);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if (!ATOMlinear(b->ttype)) {
		GDKerror("BATgroupmax: cannot determine maximum on "
			 "non-linear type %s\n", ATOMname(b->ttype));
		return NULL;
	}

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
				    &cnt, &cand, &candend)) != NULL) {
		GDKerror("BATgroupmax: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupmax: b and g must be aligned\n");
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no products, so return bat aligned with g
		 * with nil in the tail */
		bn = BATconstant(TYPE_oid, &oid_nil, ngrp);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	bn = BATnew(TYPE_void, TYPE_oid, ngrp);
	if (bn == NULL)
		return NULL;
	oids = (oid *) Tloc(bn, BUNfirst(bn));
	nils = ngrp;
	for (i = 0; i < ngrp; i++)
		oids[i] = oid_nil;

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	t = b->T->type;
	nil = ATOMnilptr(t);
	atomcmp = BATatoms[t].atomCmp;
	if (t != ATOMstorage(t) &&
	    ATOMnilptr(ATOMstorage(t)) == nil &&
	    BATatoms[ATOMstorage(t)].atomCmp == atomcmp)
		t = ATOMstorage(t);
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
	case TYPE_oid:
		AGGR_CMP(oid, GT);
		break;
	case TYPE_lng:
		AGGR_CMP(lng, GT);
		break;
	case TYPE_flt:
		AGGR_CMP(flt, GT);
		break;
	case TYPE_dbl:
		AGGR_CMP(dbl, GT);
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
				const void *v = BUNtail(bi, i + BUNfirst(b));
				if (gids)
					gid = gids[i] - min;
				else
					gid = (oid) i;
				if (!skip_nils || (*atomcmp)(v, nil) != 0) {
					if (oids[gid] == oid_nil) {
						oids[gid] = i + b->hseqbase;
						nils--;
					} else {
						const void *g = BUNtail(bi, (BUN) (oids[gid] - b->hseqbase) + BUNfirst(b));
						if ((*atomcmp)(g, nil) != 0 &&
						   ((*atomcmp)(v, nil) == 0 ||
						    GT((*atomcmp)(v, g), 0)))
							oids[gid] = i + b->hseqbase;
					}
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
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	return bn;
}

/* ---------------------------------------------------------------------- */
/* median */

BAT *
BATgroupmedian(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
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
		GDKerror("BATgroupmedian: %s\n", err);
		return NULL;
	}
	assert(tp == b->ttype);
	if (!ATOMlinear(b->ttype)) {
		GDKerror("BATgroupmedian: cannot determine median on "
			 "non-linear type %s\n", ATOMname(b->ttype));
		return NULL;
	}

	if (BATcount(b) == 0 || ngrp == 0) {
		/* trivial: no medians, so return bat aligned with e with
		 * nil in the tail */
		bn = BATconstant(tp, ATOMnilptr(tp), ngrp);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	if (s) {
		b = BATleftjoin(s, b, BATcount(s));
		if (b->htype != TYPE_void) {
			t1 = BATmirror(BATmark(BATmirror(b), 0));
			BBPunfix(b->batCacheid);
			b = t1;
		}
		freeb = 1;
		if (g) {
			g = BATleftjoin(s, g, BATcount(s));
			if (g->htype != TYPE_void) {
				t1 = BATmirror(BATmark(BATmirror(g), 0));
				BBPunfix(g->batCacheid);
				g = t1;
			}
			freeg = 1;
		}
	}

	if (g) {
		BATsubsort(&t1, &t2, NULL, g, NULL, NULL, 0, 0);
		if (freeg)
			BBPunfix(g->batCacheid);
		g = t1;
		freeg = 1;
	} else {
		t2 = NULL;
	}
	BATsubsort(&t1, NULL, NULL, b, t2, g, 0, 0);
	if (freeb)
		BBPunfix(b->batCacheid);
	b = t1;
	freeb = 1;
	if (t2)
		BBPunfix(t2->batCacheid);

	bn = BATnew(TYPE_void, b->ttype, ngrp);
	if (bn == NULL)
		return NULL;

	bi = bat_iterator(b);
	nil = ATOMnilptr(b->ttype);
	atomcmp = BATatoms[b->ttype].atomCmp;

	if (g) {
		const oid *grps;
		oid prev;
		BUN p, q, r;

		grps = (const oid *) Tloc(g, BUNfirst(g));
		prev = grps[0];
		for (r = 0, p = 1, q = BATcount(g); p <= q; p++) {
			if (p == q || grps[p] != prev) {
				if (skip_nils) {
					while (r < p && (*atomcmp)(BUNtail(bi, BUNfirst(b) + r), nil) == 0)
						r++;
				}
				while (BATcount(bn) < prev - min) {
					bunfastins_nocheck(bn, BUNlast(bn), 0,
							   nil, 0, Tsize(bn));
					nils++;
				}
				if (r == p) {
					bunfastins_nocheck(bn, BUNlast(bn), 0,
							   nil, 0, Tsize(bn));
					nils++;
				} else {
					v = BUNtail(bi, BUNfirst(b) + (r + p - 1) / 2);
					bunfastins_nocheck(bn, BUNlast(bn), 0,
							   v, 0, Tsize(bn));
					nils += (*atomcmp)(v, nil) == 0;
				}
				r = p;
				if (p < q)
					prev = grps[p];
			}
		}
		while (BATcount(bn) < ngrp) {
			bunfastins_nocheck(bn, BUNlast(bn), 0,
					   nil, 0, Tsize(bn));
		}
		BATseqbase(bn, min);
	} else {
		v = BUNtail(bi, BUNfirst(b) + (BATcount(b) - 1) / 2);
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
calcvariance(dbl *avgp, const void *values, BUN cnt, int tp, int issample)
{
	BUN n = 0, i;
	dbl mean = 0;
	dbl m2 = 0;
	dbl delta;

	assert(issample == 0 || issample == 1);

	switch (ATOMstorage(tp)) {
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
	case TYPE_flt:
		AGGR_STDEV_SINGLE(flt);
		break;
	case TYPE_dbl:
		AGGR_STDEV_SINGLE(dbl);
		break;
	default:
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
			     BATcount(b), b->ttype, 0);
	return v == dbl_nil ? dbl_nil : sqrt(v);
}

dbl
BATcalcstdev_sample(dbl *avgp, BAT *b)
{
	dbl v = calcvariance(avgp, (const void *) Tloc(b, BUNfirst(b)),
			     BATcount(b), b->ttype, 1);
	return v == dbl_nil ? dbl_nil : sqrt(v);
}

dbl
BATcalcvariance_population(dbl *avgp, BAT *b)
{
	return calcvariance(avgp, (const void *) Tloc(b, BUNfirst(b)),
			    BATcount(b), b->ttype, 0);
}

dbl
BATcalcvariance_sample(dbl *avgp, BAT *b)
{
	return calcvariance(avgp, (const void *) Tloc(b, BUNfirst(b)),
			    BATcount(b), b->ttype, 1);
}

#define AGGR_STDEV(TYPE)						\
	do {								\
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
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
	const oid *gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	BUN nils = 0, nils2 = 0;
	BUN *cnts = NULL;
	dbl *dbls, *mean, *delta, *m2;
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
		bn = BATconstant(TYPE_dbl, &dbl_nil, ngrp);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	if ((e == NULL ||
	     (BATcount(e) == BATcount(b) && e->hseqbase == b->hseqbase)) &&
	    (BATtdense(g) || (g->tkey && g->T->nonil))) {
		/* trivial: singleton groups, so all results are equal
		 * to zero (population) or nil (sample) */
		dbl v = issample ? dbl_nil : 0;
		bn = BATconstant(TYPE_dbl, &v, ngrp);
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		return bn;
	}

	delta = GDKmalloc(ngrp * sizeof(dbl));
	m2 = GDKmalloc(ngrp * sizeof(dbl));
	cnts = GDKzalloc(ngrp * sizeof(BUN));
	if (avgb) {
		if ((*avgb = BATnew(TYPE_void, TYPE_dbl, ngrp)) == NULL) {
			mean = NULL;
			goto alloc_fail;
		}
		mean = (dbl *) Tloc(*avgb, BUNfirst(*avgb));
	} else {
		mean = GDKmalloc(ngrp * sizeof(dbl));
	}
	if (mean == NULL || delta == NULL || m2 == NULL || cnts == NULL)
		goto alloc_fail;

	bn = BATnew(TYPE_void, TYPE_dbl, ngrp);
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

	switch (ATOMstorage(b->ttype)) {
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
	if (avgb && *avgb)
		BBPreclaim(*avgb);
	if (bn)
		BBPreclaim(bn);
	GDKfree(mean);
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
