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
 * Copyright August 2008-2012 MonetDB B.V.
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
static gdk_return
initgroupaggr(const BAT *b, const BAT *g, const BAT *e, const BAT *s,
	      /* on whose behalf we're doing this */
	      const char *func,
	      /* outputs: */
	      oid *minp, oid *maxp, BUN *ngrpp, BUN *startp, BUN *endp,
	      BUN *cntp, const oid **candp, const oid **candendp)
{
	oid min, max;
	BUN i, ngrp;
	const oid *gids;
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;

	if (b == NULL || !BAThdense(b)) {
		GDKerror("%s: b must be dense-headed\n", func);
		return GDK_FAIL;
	}
	if (g) {
		if (!BAThdense(g) || BATcount(b) != BATcount(g) ||
		    (BATcount(b) != 0 && b->hseqbase != g->hseqbase)) {
			GDKerror("%s: b and g must be aligned\n", func);
			return GDK_FAIL;
		}
		assert(BATttype(g) == TYPE_oid);
	}
	if (e != NULL && !BAThdense(e)) {
		GDKerror("%s: e must be dense-headed\n", func);
		return GDK_FAIL;
	}
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
			for (i = 0, ngrp = BATcount(g); i < ngrp; i++) {
				if (*gids != oid_nil) {
					if (*gids < min)
						min = *gids;
					if (*gids > max)
						max = *gids;
				}
				gids++;
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

	return GDK_SUCCEED;
}

#define AGGR_SUM(TYPE1, TYPE2)						\
	do {								\
		const TYPE1 *vals = (const TYPE1 *) Tloc(b, BUNfirst(b)); \
		for (i = start; i < end; i++, vals++) {			\
			if (cand) {					\
				if (i < *cand - b->hseqbase) {		\
					if (gids)			\
						gids++;			\
					continue;			\
				}					\
				assert(i == *cand - b->hseqbase);	\
				if (++cand == candend)			\
					end = i + 1;			\
			}						\
			if (gids == NULL ||				\
			    (*gids >= min && *gids <= max)) {		\
				gid = gids ? *gids - min : (oid) i;	\
				if (!(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
					seen[gid >> 5] |= 1 << (gid & 0x1F); \
					sums[gid] = 0;			\
				}					\
				if (*vals == TYPE1##_nil) {		\
					if (!skip_nils) {		\
						sums[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else if (sums[gid] != TYPE2##_nil) {	\
					ADD_WITH_CHECK(TYPE1, *vals,	\
						       TYPE2, sums[gid], \
						       TYPE2, sums[gid], \
						       goto overflow);	\
				}					\
			}						\
			if (gids)					\
				gids++;					\
		}							\
	} while (0)

/* calculate group sums with optional candidates list */
BAT *
BATgroupsum(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	BUN nils = 0;
	BAT *bn;
	unsigned int *seen;	/* bitmask for groups that we've seen */
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;

	if (initgroupaggr(b, g, e, s, "BATgroupsum", &min, &max, &ngrp,
			  &start, &end, &cnt, &cand, &candend) == GDK_FAIL)
		return NULL;
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

	/* allocate bitmap for seen group ids */
	seen = GDKzalloc(((ngrp + 31) / 32) * sizeof(int));
	if (seen == NULL) {
		GDKerror("BATgroupsum: cannot allocate enough memory\n");
		return NULL;
	}

	bn = BATnew(TYPE_void, tp, ngrp);
	if (bn == NULL) {
		GDKfree(seen);
		return NULL;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	switch (ATOMstorage(tp)) {
	case TYPE_bte: {
		bte *sums = (bte *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			sums[i] = bte_nil;
		switch (ATOMstorage(b->ttype)) {
		case TYPE_bte:
			AGGR_SUM(bte, bte);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_sht: {
		sht *sums = (sht *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			sums[i] = sht_nil;
		switch (ATOMstorage(b->ttype)) {
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
		int *sums = (int *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			sums[i] = int_nil;
		switch (ATOMstorage(b->ttype)) {
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
		lng *sums = (lng *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			sums[i] = lng_nil;
		switch (ATOMstorage(b->ttype)) {
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
		flt *sums = (flt *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			sums[i] = flt_nil;
		switch (ATOMstorage(b->ttype)) {
		case TYPE_flt:
			AGGR_SUM(flt, flt);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_dbl: {
		dbl *sums = (dbl *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			sums[i] = dbl_nil;
		switch (ATOMstorage(b->ttype)) {
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
	BATsetcount(bn, ngrp);

	if (nils == 0) {
		/* figure out whether there were any empty groups
		 * (that result in a nil value) */
		seen[ngrp >> 5] |= ~0U << (ngrp & 0x1F); /* fill last slot */
		for (i = 0, ngrp = (ngrp + 31) / 32; i < ngrp; i++) {
			if (seen[i] != ~0U) {
				nils = 1;
				break;
			}
		}
	}
	GDKfree(seen);
	BATseqbase(bn, min);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	return bn;

  unsupported:
	GDKfree(seen);
	BBPunfix(bn->batCacheid);
	GDKerror("BATgroupsum: type combination (sum(%s)->%s) not supported.\n",
		 ATOMname(b->ttype), ATOMname(tp));
	return NULL;

  overflow:
	GDKfree(seen);
	BBPunfix(bn->batCacheid);
	GDKerror("22003!overflow in calculation.\n");
	return NULL;
}

#define AGGR_PROD(TYPE1, TYPE2, TYPE3)					\
	do {								\
		const TYPE1 *vals = (const TYPE1 *) Tloc(b, BUNfirst(b)); \
		for (i = start; i < end; i++, vals++) {			\
			if (cand) {					\
				if (i < *cand - b->hseqbase) {		\
					if (gids)			\
						gids++;			\
					continue;			\
				}					\
				assert(i == *cand - b->hseqbase);	\
				if (++cand == candend)			\
					end = i + 1;			\
			}						\
			if (gids == NULL ||				\
			    (*gids >= min && *gids <= max)) {		\
				gid = gids ? *gids - min : (oid) i;	\
				if (!(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
					seen[gid >> 5] |= 1 << (gid & 0x1F); \
					prods[gid] = 1;			\
				}					\
				if (*vals == TYPE1##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else if (prods[gid] != TYPE2##_nil) {	\
					MUL4_WITH_CHECK(TYPE1, *vals,	\
							TYPE2, prods[gid], \
							TYPE2, prods[gid], \
							TYPE3,		\
							goto overflow);	\
				}					\
			}						\
			if (gids)					\
				gids++;					\
		}							\
	} while (0)

#define AGGR_PROD_LNG(TYPE)						\
	do {								\
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
		for (i = start; i < end; i++, vals++) {			\
			if (cand) {					\
				if (i < *cand - b->hseqbase) {		\
					if (gids)			\
						gids++;			\
					continue;			\
				}					\
				assert(i == *cand - b->hseqbase);	\
				if (++cand == candend)			\
					end = i + 1;			\
			}						\
			if (gids == NULL ||				\
			    (*gids >= min && *gids <= max)) {		\
				gid = gids ? *gids - min : (oid) i;	\
				if (!(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
					seen[gid >> 5] |= 1 << (gid & 0x1F); \
					prods[gid] = 1;			\
				}					\
				if (*vals == TYPE##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = lng_nil;	\
						nils++;			\
					}				\
				} else if (prods[gid] != lng_nil) {	\
					LNGMUL_CHECK(TYPE, *vals,	\
						     lng, prods[gid],	\
						     prods[gid],	\
						     goto overflow);	\
				}					\
			}						\
			if (gids)					\
				gids++;					\
		}							\
	} while (0)

#define AGGR_PROD_FLOAT(TYPE1, TYPE2)					\
	do {								\
		const TYPE1 *vals = (const TYPE1 *) Tloc(b, BUNfirst(b)); \
		for (i = start; i < end; i++, vals++) {			\
			if (cand) {					\
				if (i < *cand - b->hseqbase) {		\
					if (gids)			\
						gids++;			\
					continue;			\
				}					\
				assert(i == *cand - b->hseqbase);	\
				if (++cand == candend)			\
					end = i + 1;			\
			}						\
			if (gids == NULL ||				\
			    (*gids >= min && *gids <= max)) {		\
				gid = gids ? *gids - min : (oid) i;	\
				if (!(seen[gid >> 5] & (1 << (gid & 0x1F)))) { \
					seen[gid >> 5] |= 1 << (gid & 0x1F); \
					prods[gid] = 1;			\
				}					\
				if (*vals == TYPE1##_nil) {		\
					if (!skip_nils) {		\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					}				\
				} else if (prods[gid] != TYPE2##_nil) {	\
					if (ABSOLUTE(*vals) > 1 &&	\
					    GDK_##TYPE2##_max / ABSOLUTE(*vals) < ABSOLUTE(prods[gid])) { \
						if (abort_on_error)	\
							goto overflow;	\
						prods[gid] = TYPE2##_nil; \
						nils++;			\
					} else {			\
						prods[gid] *= *vals;	\
					}				\
				}					\
			}						\
			if (gids)					\
				gids++;					\
		}							\
	} while (0)

/* calculate group products with optional candidates list */
BAT *
BATgroupprod(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error)
{
	const oid *gids;
	oid gid;
	oid min, max;
	BUN i, ngrp;
	BUN nils = 0;
	BAT *bn;
	unsigned int *seen;	/* bitmask for groups that we've seen */
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;

	if (initgroupaggr(b, g, e, s, "BATgroupprod", &min, &max, &ngrp,
			  &start, &end, &cnt, &cand, &candend) == GDK_FAIL)
		return NULL;
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

	/* allocate bitmap for seen group ids */
	seen = GDKzalloc(((ngrp + 31) / 32) * sizeof(int));
	if (seen == NULL) {
		GDKerror("BATgroupprod: cannot allocate enough memory\n");
		return NULL;
	}

	bn = BATnew(TYPE_void, tp, ngrp);
	if (bn == NULL) {
		GDKfree(seen);
		return NULL;
	}

	if (BATtdense(g))
		gids = NULL;
	else
		gids = (const oid *) Tloc(g, BUNfirst(g) + start);

	switch (ATOMstorage(tp)) {
	case TYPE_bte: {
		bte *prods = (bte *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			prods[i] = bte_nil;
		switch (ATOMstorage(b->ttype)) {
		case TYPE_bte:
			AGGR_PROD(bte, bte, sht);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_sht: {
		sht *prods = (sht *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			prods[i] = sht_nil;
		switch (ATOMstorage(b->ttype)) {
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
		int *prods = (int *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			prods[i] = int_nil;
		switch (ATOMstorage(b->ttype)) {
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
		lng *prods = (lng *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			prods[i] = lng_nil;
		switch (ATOMstorage(b->ttype)) {
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
		flt *prods = (flt *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			prods[i] = flt_nil;
		switch (ATOMstorage(b->ttype)) {
		case TYPE_flt:
			AGGR_PROD_FLOAT(flt, flt);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_dbl: {
		dbl *prods = (dbl *) Tloc(bn, BUNfirst(bn));
		for (i = 0; i < ngrp; i++)
			prods[i] = dbl_nil;
		switch (ATOMstorage(b->ttype)) {
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
	BATsetcount(bn, ngrp);

	if (nils == 0) {
		/* figure out whether there were any empty groups
		 * (that result in a nil value) */
		seen[ngrp >> 5] |= ~0U << (ngrp & 0x1F); /* fill last slot */
		for (i = 0, ngrp = (ngrp + 31) / 32; i < ngrp; i++) {
			if (seen[i] != ~0U) {
				nils = 1;
				break;
			}
		}
	}
	GDKfree(seen);
	BATseqbase(bn, min);
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	return bn;

  unsupported:
	GDKfree(seen);
	BBPunfix(bn->batCacheid);
	GDKerror("BATgroupprod: type combination (mul(%s)->%s) not supported.\n",
		 ATOMname(b->ttype), ATOMname(tp));
	return NULL;

  overflow:
	GDKfree(seen);
	BBPunfix(bn->batCacheid);
	GDKerror("22003!overflow in calculation.\n");
	return NULL;
}

#define AGGR_AVG(TYPE)							\
	do {								\
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
		TYPE *avgs = GDKzalloc(ngrp * sizeof(TYPE));		\
		if (avgs == NULL)					\
			goto alloc_fail;				\
		for (i = start; i < end; i++, vals++) {			\
			if (cand) {					\
				if (i < *cand - b->hseqbase) {		\
					if (gids)			\
						gids++;			\
					continue;			\
				}					\
				assert(i == *cand - b->hseqbase);	\
				if (++cand == candend)			\
					end = i + 1;			\
			}						\
			if (gids == NULL ||				\
			    (*gids >= min && *gids <= max)) {		\
				gid = gids ? *gids - min : (oid) i;	\
				if (*vals == TYPE##_nil) {		\
					if (!skip_nils)			\
						cnts[gid] = BUN_NONE;	\
				} else if (cnts[gid] != BUN_NONE) {	\
					AVERAGE_ITER(TYPE, *vals,	\
						     avgs[gid],		\
						     rems[gid],		\
						     cnts[gid]);	\
				}					\
			}						\
			if (gids)					\
				gids++;					\
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
		for (i = start; i < end; i++, vals++) {			\
			if (cand) {					\
				if (i < *cand - b->hseqbase) {		\
					if (gids)			\
						gids++;			\
					continue;			\
				}					\
				assert(i == *cand - b->hseqbase);	\
				if (++cand == candend)			\
					end = i + 1;			\
			}						\
			if (gids == NULL ||				\
			    (*gids >= min && *gids <= max)) {		\
				gid = gids ? *gids - min : (oid) i;	\
				if (*vals == TYPE##_nil) {		\
					if (!skip_nils)			\
						cnts[gid] = BUN_NONE;	\
				} else if (cnts[gid] != BUN_NONE) {	\
					AVERAGE_ITER_FLOAT(TYPE, *vals, \
							   dbls[gid],	\
							   cnts[gid]);	\
				}					\
			}						\
			if (gids)					\
				gids++;					\
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

	assert(tp == TYPE_dbl);
	(void) tp;		/* compatibility (with other BATgroup*
				 * functions) argument */

	if (initgroupaggr(b, g, e, s, "BATgroupavg", &min, &max, &ngrp,
			  &start, &end, &cnt, &cand, &candend) == GDK_FAIL)
		return NULL;
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

#define AGGR_COUNT(TYPE)						\
	do {								\
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
		for (i = start; i < end; i++, vals++) {			\
			if (cand) {					\
				if (i < *cand - b->hseqbase) {		\
					if (gids)			\
						gids++;			\
					continue;			\
				}					\
				assert(i == *cand - b->hseqbase);	\
				if (++cand == candend)			\
					end = i + 1;			\
			}						\
			if (gids == NULL ||				\
			    (*gids >= min && *gids <= max)) {		\
				gid = gids ? *gids - min : (oid) i;	\
				if (!skip_nils || *vals != TYPE##_nil) { \
					cnts[gid]++;			\
				}					\
			}						\
			if (gids)					\
				gids++;					\
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

	assert(tp == TYPE_wrd);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if (initgroupaggr(b, g, e, s, "BATgroupcount", &min, &max, &ngrp,
			  &start, &end, &cnt, &cand, &candend) == GDK_FAIL)
		return NULL;
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

		for (i = start; i < end; i++) {
			if (cand) {
				if (i < *cand - b->hseqbase) {
					if (gids)
						gids++;
					continue;
				}
				assert(i == *cand - b->hseqbase);
				if (++cand == candend)
					end = i + 1;
			}
			if (gids == NULL ||
			    (*gids >= min && *gids <= max)) {
				gid = gids ? *gids - min : (oid) i;
				if (!skip_nils ||
				    (*atomcmp)(BUNtail(bi, i + BUNfirst(b)),
					       nil) != 0) {
					cnts[gid]++;
				}
			}
			if (gids)
				gids++;
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

	assert(tp == TYPE_wrd);
	assert(b->ttype == TYPE_bit);
	/* compatibility arguments */
	(void) tp;
	(void) abort_on_error;
	(void) skip_nils;

	if (initgroupaggr(b, g, e, s, "BATgroupsize", &min, &max, &ngrp,
			  &start, &end, &cnt, &cand, &candend) == GDK_FAIL)
		return NULL;
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

	for (i = start; i < end; i++, bits++) {
		if (cand) {
			if (i < *cand - b->hseqbase) {
				if (gids)
					gids++;
				continue;
			}
			assert(i == *cand - b->hseqbase);
			if (++cand == candend)
				end = i + 1;
		}
		if ((gids == NULL || (*gids >= min && *gids <= max)) &&
		    *bits == 1) {
			cnts[gids ? *gids - min : (oid) i]++;
		}
		if (gids)
			gids++;
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

#define AGGR_CMP(TYPE, OP)						\
	do {								\
		const TYPE *vals = (const TYPE *) Tloc(b, BUNfirst(b)); \
		for (i = start; i < end; i++) {				\
			if (cand) {					\
				if (i < *cand - b->hseqbase) {		\
					if (gids)			\
						gids++;			\
					continue;			\
				}					\
				assert(i == *cand - b->hseqbase);	\
				if (++cand == candend)			\
					end = i + 1;			\
			}						\
			if (gids == NULL ||				\
			    (*gids >= min && *gids <= max)) {		\
				gid = gids ? *gids - min : (oid) i;	\
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
			if (gids)					\
				gids++;					\
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

	assert(tp == TYPE_oid);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if (!ATOMlinear(b->ttype)) {
		GDKerror("BATgroupmin: cannot determine minimum on "
			 "non-linear type %s\n", ATOMname(b->ttype));
		return NULL;
	}

	if (initgroupaggr(b, g, e, s, "BATgroupmin", &min, &max, &ngrp,
			  &start, &end, &cnt, &cand, &candend) == GDK_FAIL)
		return NULL;
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

		for (i = start; i < end; i++) {
			if (cand) {
				if (i < *cand - b->hseqbase) {
					if (gids)
						gids++;
					continue;
				}
				assert(i == *cand - b->hseqbase);
				if (++cand == candend)
					end = i + 1;
			}
			if (gids == NULL ||
			    (*gids >= min && *gids <= max)) {
				const void *v = BUNtail(bi, i + BUNfirst(b));
				gid = gids ? *gids - min : (oid) i;
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
			if (gids)
				gids++;
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

	assert(tp == TYPE_oid);
	(void) tp;		/* compatibility (with other BATgroup* */
	(void) abort_on_error;	/* functions) argument */

	if (!ATOMlinear(b->ttype)) {
		GDKerror("BATgroupmax: cannot determine maximum on "
			 "non-linear type %s\n", ATOMname(b->ttype));
		return NULL;
	}

	if (initgroupaggr(b, g, e, s, "BATgroupmax", &min, &max, &ngrp,
			  &start, &end, &cnt, &cand, &candend) == GDK_FAIL)
		return NULL;
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

		for (i = start; i < end; i++) {
			if (cand) {
				if (i < *cand - b->hseqbase) {
					if (gids)
						gids++;
					continue;
				}
				assert(i == *cand - b->hseqbase);
				if (++cand == candend)
					end = i + 1;
			}
			if (gids == NULL ||
			    (*gids >= min && *gids <= max)) {
				const void *v = BUNtail(bi, i + BUNfirst(b));
				gid = gids ? *gids - min : (oid) i;
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
			if (gids)
				gids++;
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
