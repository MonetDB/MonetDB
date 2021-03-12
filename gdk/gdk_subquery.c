/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_subquery.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

#define SQLall_grp_imp(TYPE)	\
	do {			\
		const TYPE *restrict vals = (const TYPE *) Tloc(l, 0); \
		TYPE *restrict rp = (TYPE *) Tloc(res, 0); \
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - l->hseqbase;		\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (oids[gid] != (BUN_NONE - 1)) { \
					if (oids[gid] == BUN_NONE) { \
						if (!is_##TYPE##_nil(vals[i])) \
							oids[gid] = i; \
					} else { \
						if (vals[oids[gid]] != vals[i] && !is_##TYPE##_nil(vals[i])) \
							oids[gid] = BUN_NONE - 1; \
					} \
				} \
			} \
		} \
		for (i = 0; i < ngrp; i++) { /* convert the found oids in values */ \
			BUN noid = oids[i]; \
			if (noid >= (BUN_NONE - 1)) { \
				rp[i] = TYPE##_nil; \
				hasnil = 1; \
			} else { \
				rp[i] = vals[noid]; \
			} \
		} \
	} while (0)

BAT *
BATall_grp(BAT *l, BAT *g, BAT *e, BAT *s)
{
	BAT *res = NULL;
	const oid *restrict gids;
	oid gid, min, max, *restrict oids = NULL; /* The oids variable controls if we have found a nil in the group so far */
	BUN i, ngrp, ncand;
	bit hasnil = 0;
	struct canditer ci;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(l, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("l and g must be aligned\n");
		return NULL;
	}

	if (BATcount(l) == 0 || ngrp == 0) {
		const void *nilp = ATOMnilptr(l->ttype);
		if ((res = BATconstant(ngrp == 0 ? 0 : min, l->ttype, nilp, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
	} else {
		if ((res = COLnew(min, l->ttype, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		if ((oids = GDKmalloc(ngrp * sizeof(oid))) == NULL)
			goto alloc_fail;

		for (i = 0; i < ngrp; i++)
			oids[i] = BUN_NONE;

		if (!g || BATtdense(g))
			gids = NULL;
		else
			gids = (const oid *) Tloc(g, 0);

		switch (ATOMbasetype(l->ttype)) {
		case TYPE_bte:
			SQLall_grp_imp(bte);
			break;
		case TYPE_sht:
			SQLall_grp_imp(sht);
			break;
		case TYPE_int:
			SQLall_grp_imp(int);
			break;
		case TYPE_lng:
			SQLall_grp_imp(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLall_grp_imp(hge);
			break;
#endif
		case TYPE_flt:
			SQLall_grp_imp(flt);
			break;
		case TYPE_dbl:
			SQLall_grp_imp(dbl);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
			const void *restrict nilp = ATOMnilptr(l->ttype);
			BATiter li = bat_iterator(l);

			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - l->hseqbase;
				if (gids == NULL ||
					(gids[i] >= min && gids[i] <= max)) {
					if (gids)
						gid = gids[i] - min;
					else
						gid = (oid) i;
					if (oids[gid] != (BUN_NONE - 1)) {
						if (oids[gid] == BUN_NONE) {
							if (ocmp(BUNtail(li, i), nilp) != 0)
								oids[gid] = i;
						} else {
							const void *pi = BUNtail(li, oids[gid]);
							const void *pp = BUNtail(li, i);
							if (ocmp(pi, pp) != 0 && ocmp(pp, nilp) != 0)
								oids[gid] = BUN_NONE - 1;
						}
					}
				}
			}

			if (ATOMvarsized(l->ttype)) {
				for (i = 0; i < ngrp; i++) { /* convert the found oids in values */
					BUN noid = oids[i];
					void *next;
					if (noid == BUN_NONE) {
						next = (void*) nilp;
						hasnil = 1;
					} else {
						next = BUNtvar(li, noid);
					}
					if (tfastins_nocheckVAR(res, i, next, Tsize(res)) != GDK_SUCCEED)
						goto alloc_fail;
				}
			} else {
				uint8_t *restrict rcast = (uint8_t *) Tloc(res, 0);
				uint16_t width = res->twidth;
				for (i = 0; i < ngrp; i++) { /* convert the found oids in values */
					BUN noid = oids[i];
					void *next;
					if (noid == BUN_NONE) {
						next = (void*) nilp;
						hasnil = 1;
					} else {
						next = BUNtloc(li, noid);
					}
					memcpy(rcast, next, width);
					rcast += width;
				}
			}
		}
		}
		BATsetcount(res, ngrp);
		res->tnil = hasnil != 0;
		res->tnonil = hasnil == 0;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
	}

	GDKfree(oids);

	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ",g=" ALGOBATFMT
		  ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(g),
		  ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(res),
		  __func__, GDKusec() - t0);
	return res;
alloc_fail:
	BBPreclaim(res);
	GDKfree(oids);
	return NULL;
}

#define SQLnil_grp_imp(TYPE)	\
	do {								\
		const TYPE *restrict vals = (const TYPE *) Tloc(l, 0);		\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - l->hseqbase;		\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (ret[gid] != TRUE && is_##TYPE##_nil(vals[i])) \
					ret[gid] = TRUE; \
			} \
		} \
	} while (0)

BAT *
BATnil_grp(BAT *l, BAT *g, BAT *e, BAT *s)
{
	BAT *res = NULL;
	const oid *restrict gids;
	oid gid, min, max;
	BUN i, ngrp, ncand;
	struct canditer ci;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(l, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("l and g must be aligned\n");
		return NULL;
	}

	if (BATcount(l) == 0 || ngrp == 0) {
		bit F = FALSE;
		if ((res = BATconstant(ngrp == 0 ? 0 : min, TYPE_bit, &F, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;

	} else {
		bit *restrict ret;

		if ((res = COLnew(min, TYPE_bit, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		ret = (bit *) Tloc(res, 0);
		memset(ret, FALSE, ngrp * sizeof(bit));

		if (!g || BATtdense(g))
			gids = NULL;
		else
			gids = (const oid *) Tloc(g, 0);

		switch (ATOMbasetype(l->ttype)) {
		case TYPE_bte:
			SQLnil_grp_imp(bte);
			break;
		case TYPE_sht:
			SQLnil_grp_imp(sht);
			break;
		case TYPE_int:
			SQLnil_grp_imp(int);
			break;
		case TYPE_lng:
			SQLnil_grp_imp(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLnil_grp_imp(hge);
			break;
#endif
		case TYPE_flt:
			SQLnil_grp_imp(flt);
			break;
		case TYPE_dbl:
			SQLnil_grp_imp(dbl);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
			const void *restrict nilp = ATOMnilptr(l->ttype);
			BATiter li = bat_iterator(l);

			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - l->hseqbase;
				if (gids == NULL ||
					(gids[i] >= min && gids[i] <= max)) {
					if (gids)
						gid = gids[i] - min;
					else
						gid = (oid) i;
					const void *restrict lv = BUNtail(li, i);
					if (ret[gid] != TRUE && ocmp(lv, nilp) == 0)
						ret[gid] = TRUE;
				}
			}
		}
		}
		BATsetcount(res, ngrp);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = false;
		res->tnonil = true;
	}

	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ",g=" ALGOBATFMT
		  ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(g),
		  ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(res),
		  __func__, GDKusec() - t0);
	return res;
alloc_fail:
	BBPreclaim(res);
	return NULL;
}

#define SQLanyequal_or_not_grp_imp(TYPE, TEST)	\
	do {								\
		const TYPE *vals1 = (const TYPE *) Tloc(l, 0);		\
		const TYPE *vals2 = (const TYPE *) Tloc(r, 0);		\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - l->hseqbase;		\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (ret[gid] != TEST) { \
					if (is_##TYPE##_nil(vals1[i]) || is_##TYPE##_nil(vals2[i])) { \
						ret[gid] = bit_nil; \
						hasnil = 1; \
					} else if (vals1[i] == vals2[i]) { \
						ret[gid] = TEST; \
					} \
				} \
			} \
		} \
	} while (0)

BAT *
BATanyequal_grp(BAT *l, BAT *r, BAT *g, BAT *e, BAT *s)
{
	BAT *res = NULL;
	const oid *restrict gids;
	oid gid, min, max;
	BUN i, ngrp, ncand;
	bit hasnil = 0;
	struct canditer ci;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(l, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("l, r and g must be aligned\n");
		return NULL;
	}

	if (BATcount(l) == 0 || ngrp == 0) {
		bit F = FALSE;
		if ((res = BATconstant(ngrp == 0 ? 0 : min, TYPE_bit, &F, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
	} else {
		bit *restrict ret;

		if ((res = COLnew(min, TYPE_bit, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		ret = (bit *) Tloc(res, 0);
		memset(ret, FALSE, ngrp * sizeof(bit));

		if (!g || BATtdense(g))
			gids = NULL;
		else
			gids = (const oid *) Tloc(g, 0);

		switch (ATOMbasetype(l->ttype)) {
		case TYPE_bte:
			SQLanyequal_or_not_grp_imp(bte, TRUE);
			break;
		case TYPE_sht:
			SQLanyequal_or_not_grp_imp(sht, TRUE);
			break;
		case TYPE_int:
			SQLanyequal_or_not_grp_imp(int, TRUE);
			break;
		case TYPE_lng:
			SQLanyequal_or_not_grp_imp(lng, TRUE);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLanyequal_or_not_grp_imp(hge, TRUE);
			break;
#endif
		case TYPE_flt:
			SQLanyequal_or_not_grp_imp(flt, TRUE);
			break;
		case TYPE_dbl:
			SQLanyequal_or_not_grp_imp(dbl, TRUE);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
			const void *nilp = ATOMnilptr(l->ttype);
			BATiter li = bat_iterator(l), ri = bat_iterator(r);

			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - l->hseqbase;
				if (gids == NULL ||
					(gids[i] >= min && gids[i] <= max)) {
					if (gids)
						gid = gids[i] - min;
					else
						gid = (oid) i;
					if (ret[gid] != TRUE) {
						const void *lv = BUNtail(li, i);
						const void *rv = BUNtail(ri, i);
						if (ocmp(lv, nilp) == 0 || ocmp(rv, nilp) == 0) {
							ret[gid] = bit_nil;
							hasnil = 1;
						} else if (ocmp(lv, rv) == 0)
							ret[gid] = TRUE;
					}
				}
			}
		}
		}
		BATsetcount(res, ngrp);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = hasnil != 0;
		res->tnonil = hasnil == 0;
	}

	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ",r=" ALGOBATFMT ",g=" ALGOBATFMT
		  ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r), ALGOBATPAR(g),
		  ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(res),
		  __func__, GDKusec() - t0);
	return res;
alloc_fail:
	BBPreclaim(res);
	return NULL;
}

BAT *
BATallnotequal_grp(BAT *l, BAT *r, BAT *g, BAT *e, BAT *s)
{
	BAT *res = NULL;
	const oid *restrict gids;
	oid gid, min, max;
	BUN i, ngrp, ncand;
	bit hasnil = 0;
	struct canditer ci;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(l, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("l, r and g must be aligned\n");
		return NULL;
	}

	if (BATcount(l) == 0 || ngrp == 0) {
		bit T = TRUE;
		if ((res = BATconstant(ngrp == 0 ? 0 : min, TYPE_bit, &T, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
	} else {
		bit *restrict ret;

		if ((res = COLnew(min, TYPE_bit, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		ret = (bit *) Tloc(res, 0);
		memset(ret, FALSE, ngrp * sizeof(bit));

		if (!g || BATtdense(g))
			gids = NULL;
		else
			gids = (const oid *) Tloc(g, 0);

		switch (ATOMbasetype(l->ttype)) {
		case TYPE_bte:
			SQLanyequal_or_not_grp_imp(bte, FALSE);
			break;
		case TYPE_sht:
			SQLanyequal_or_not_grp_imp(sht, FALSE);
			break;
		case TYPE_int:
			SQLanyequal_or_not_grp_imp(int, FALSE);
			break;
		case TYPE_lng:
			SQLanyequal_or_not_grp_imp(lng, FALSE);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLanyequal_or_not_grp_imp(hge, FALSE);
			break;
#endif
		case TYPE_flt:
			SQLanyequal_or_not_grp_imp(flt, FALSE);
			break;
		case TYPE_dbl:
			SQLanyequal_or_not_grp_imp(dbl, FALSE);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
			const void *nilp = ATOMnilptr(l->ttype);
			BATiter li = bat_iterator(l), ri = bat_iterator(r);

			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - l->hseqbase;
				if (gids == NULL ||
					(gids[i] >= min && gids[i] <= max)) {
					if (gids)
						gid = gids[i] - min;
					else
						gid = (oid) i;
					if (ret[gid] != FALSE) {
						const void *lv = BUNtail(li, i);
						const void *rv = BUNtail(ri, i);
						if (ocmp(lv, nilp) == 0 || ocmp(rv, nilp) == 0) {
							ret[gid] = bit_nil;
							hasnil = 1;
						} else if (ocmp(lv, rv) == 0)
							ret[gid] = FALSE;
					}
				}
			}
		}
		}
		BATsetcount(res, ngrp);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = hasnil != 0;
		res->tnonil = hasnil == 0;
	}

	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ",r=" ALGOBATFMT ",g=" ALGOBATFMT
		  ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r), ALGOBATPAR(g),
		  ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(res),
		  __func__, GDKusec() - t0);
	return res;
alloc_fail:
	BBPreclaim(res);
	return NULL;
}

#define SQLanyequal_or_not_grp2_imp(TYPE, VAL1, VAL2) \
	do {								\
		const TYPE *vals1 = (const TYPE *) Tloc(l, 0);		\
		const TYPE *vals2 = (const TYPE *) Tloc(r, 0);		\
		while (ncand > 0) {					\
			ncand--;					\
			i = canditer_next(&ci) - l->hseqbase;		\
			if (gids == NULL ||				\
			    (gids[i] >= min && gids[i] <= max)) {	\
				if (gids)				\
					gid = gids[i] - min;		\
				else					\
					gid = (oid) i;			\
				if (ret[gid] != VAL1) { \
					const oid id = *(oid*)BUNtail(ii, i); \
					if (is_oid_nil(id)) { \
						ret[gid] = VAL2; \
					} else if (is_##TYPE##_nil(vals1[i]) || is_##TYPE##_nil(vals2[i])) { \
						ret[gid] = bit_nil; \
					} else if (vals1[i] == vals2[i]) { \
						ret[gid] = VAL1; \
					} \
				} \
			} \
		} \
	} while (0)

BAT *
BATanyequal_grp2(BAT *l, BAT *r, BAT *rid, BAT *g, BAT *e, BAT *s)
{
	BAT *res = NULL;
	const oid *restrict gids;
	oid gid, min, max;
	BUN i, ngrp, ncand;
	bit hasnil = 0;
	struct canditer ci;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(l, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("l, r, rid and g must be aligned\n");
		return NULL;
	}

	if (BATcount(l) == 0 || ngrp == 0) {
		bit F = FALSE;
		if ((res = BATconstant(ngrp == 0 ? 0 : min, TYPE_bit, &F, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
	} else {
		BATiter ii = bat_iterator(rid);
		bit *restrict ret;

		if ((res = COLnew(min, TYPE_bit, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		ret = (bit *) Tloc(res, 0);
		memset(ret, FALSE, ngrp * sizeof(bit));

		if (!g || BATtdense(g))
			gids = NULL;
		else
			gids = (const oid *) Tloc(g, 0);

		switch (ATOMbasetype(l->ttype)) {
		case TYPE_bte:
			SQLanyequal_or_not_grp2_imp(bte, TRUE, FALSE);
			break;
		case TYPE_sht:
			SQLanyequal_or_not_grp2_imp(sht, TRUE, FALSE);
			break;
		case TYPE_int:
			SQLanyequal_or_not_grp2_imp(int, TRUE, FALSE);
			break;
		case TYPE_lng:
			SQLanyequal_or_not_grp2_imp(lng, TRUE, FALSE);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLanyequal_or_not_grp2_imp(hge, TRUE, FALSE);
			break;
#endif
		case TYPE_flt:
			SQLanyequal_or_not_grp2_imp(flt, TRUE, FALSE);
			break;
		case TYPE_dbl:
			SQLanyequal_or_not_grp2_imp(dbl, TRUE, FALSE);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
			const void *nilp = ATOMnilptr(l->ttype);
			BATiter li = bat_iterator(l), ri = bat_iterator(r);

			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - l->hseqbase;
				if (gids == NULL ||
					(gids[i] >= min && gids[i] <= max)) {
					if (gids)
						gid = gids[i] - min;
					else
						gid = (oid) i;
					if (ret[gid] != TRUE) {
						const oid id = *(oid*)BUNtail(ii, i);
						if (is_oid_nil(id)) {
							ret[gid] = FALSE;
						} else {
							const void *lv = BUNtail(li, i);
							const void *rv = BUNtail(ri, i);
							if (ocmp(lv, nilp) == 0 || ocmp(rv, nilp) == 0) {
								ret[gid] = bit_nil;
							} else if (ocmp(lv, rv) == 0)
								ret[gid] = TRUE;
						}
					}
				}
			}
		}
		}
		for (BUN i = 0 ; i < ngrp ; i++)
			hasnil |= ret[i] == bit_nil;
		BATsetcount(res, ngrp);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = hasnil != 0;
		res->tnonil = hasnil == 0;
	}

	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ",r=" ALGOBATFMT ",rid=" ALGOBATFMT
		  ",g=" ALGOBATFMT ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r), ALGOBATPAR(rid),
		  ALGOBATPAR(g), ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(res),
		  __func__, GDKusec() - t0);
	return res;
alloc_fail:
	BBPreclaim(res);
	return NULL;
}

BAT *
BATallnotequal_grp2(BAT *l, BAT *r, BAT *rid, BAT *g, BAT *e, BAT *s)
{
	BAT *res = NULL;
	const oid *restrict gids;
	oid gid, min, max;
	BUN i, ngrp, ncand;
	bit hasnil = 0;
	struct canditer ci;
	const char *err;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((err = BATgroupaggrinit(l, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("l, r, rid and g must be aligned\n");
		return NULL;
	}

	if (BATcount(l) == 0 || ngrp == 0) {
		bit T = TRUE;
		if ((res = BATconstant(ngrp == 0 ? 0 : min, TYPE_bit, &T, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
	} else {
		BATiter ii = bat_iterator(rid);
		bit *restrict ret;

		if ((res = COLnew(min, TYPE_bit, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		ret = (bit *) Tloc(res, 0);
		memset(ret, TRUE, ngrp * sizeof(bit));

		if (!g || BATtdense(g))
			gids = NULL;
		else
			gids = (const oid *) Tloc(g, 0);

		switch (ATOMbasetype(l->ttype)) {
		case TYPE_bte:
			SQLanyequal_or_not_grp2_imp(bte, FALSE, TRUE);
			break;
		case TYPE_sht:
			SQLanyequal_or_not_grp2_imp(sht, FALSE, TRUE);
			break;
		case TYPE_int:
			SQLanyequal_or_not_grp2_imp(int, FALSE, TRUE);
			break;
		case TYPE_lng:
			SQLanyequal_or_not_grp2_imp(lng, FALSE, TRUE);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLanyequal_or_not_grp2_imp(hge, FALSE, TRUE);
			break;
#endif
		case TYPE_flt:
			SQLanyequal_or_not_grp2_imp(flt, FALSE, TRUE);
			break;
		case TYPE_dbl:
			SQLanyequal_or_not_grp2_imp(dbl, FALSE, TRUE);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
			const void *nilp = ATOMnilptr(l->ttype);
			BATiter li = bat_iterator(l), ri = bat_iterator(r);

			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - l->hseqbase;
				if (gids == NULL ||
					(gids[i] >= min && gids[i] <= max)) {
					if (gids)
						gid = gids[i] - min;
					else
						gid = (oid) i;
					if (ret[gid] != FALSE) {
						const oid id = *(oid*)BUNtail(ii, i);
						if (is_oid_nil(id)) {
							ret[gid] = TRUE;
						} else {
							const void *lv = BUNtail(li, i);
							const void *rv = BUNtail(ri, i);
							if (ocmp(lv, nilp) == 0 || ocmp(rv, nilp) == 0) {
								ret[gid] = bit_nil;
							} else if (ocmp(lv, rv) == 0)
								ret[gid] = FALSE;
						}
					}
				}
			}
		}
		}
		for (BUN i = 0 ; i < ngrp ; i++)
			hasnil |= ret[i] == bit_nil;
		BATsetcount(res, ngrp);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = hasnil != 0;
		res->tnonil = hasnil == 0;
	}

	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ",r=" ALGOBATFMT ",rid=" ALGOBATFMT
		  ",g=" ALGOBATFMT ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(l), ALGOBATPAR(r), ALGOBATPAR(rid),
		  ALGOBATPAR(g), ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(res),
		  __func__, GDKusec() - t0);
	return res;
alloc_fail:
	BBPreclaim(res);
	return NULL;
}

BAT *
BATsubexist(BAT *b, BAT *g, BAT *e, BAT *s)
{
	BAT *res = NULL;
	const oid *restrict gids;
	oid min, max;
	BUN i, ngrp, ncand;
	struct canditer ci;
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
		bit F = FALSE;
		if ((res = BATconstant(ngrp == 0 ? 0 : min, TYPE_bit, &F, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
	} else {
		bit *restrict exists;

		if ((res = COLnew(min, TYPE_bit, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		exists = (bit *) Tloc(res, 0);
		memset(exists, FALSE, ngrp * sizeof(bit));

		if (!g || BATtdense(g))
			gids = NULL;
		else
			gids = (const oid *) Tloc(g, 0);

		if (gids) {
			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - b->hseqbase;
				if (gids[i] >= min && gids[i] <= max)
					exists[gids[i] - min] = TRUE;
			}
		} else {
			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - b->hseqbase;
				exists[i] = TRUE;
			}
		}
		BATsetcount(res, ngrp);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = false;
		res->tnonil = true;
	}

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOBATFMT
		  ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOBATPAR(g),
		  ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(res),
		  __func__, GDKusec() - t0);
	return res;
alloc_fail:
	BBPreclaim(res);
	return NULL;
}

BAT *
BATsubnot_exist(BAT *b, BAT *g, BAT *e, BAT *s)
{
	BAT *res = NULL;
	const oid *restrict gids;
	oid min, max;
	BUN i, ngrp, ncand;
	struct canditer ci;
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
		bit T = TRUE;
		if ((res = BATconstant(ngrp == 0 ? 0 : min, TYPE_bit, &T, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
	} else {
		bit *restrict exists;

		if ((res = COLnew(min, TYPE_bit, ngrp, TRANSIENT)) == NULL)
			goto alloc_fail;
		exists = (bit *) Tloc(res, 0);
		memset(exists, TRUE, ngrp * sizeof(bit));

		if (!g || BATtdense(g))
			gids = NULL;
		else
			gids = (const oid *) Tloc(g, 0);

		if (gids) {
			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - b->hseqbase;
				if (gids[i] >= min && gids[i] <= max)
					exists[gids[i] - min] = FALSE;
			}
		} else {
			while (ncand > 0) {
				ncand--;
				i = canditer_next(&ci) - b->hseqbase;
				exists[i] = FALSE;
			}
		}
		BATsetcount(res, ngrp);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = false;
		res->tnonil = true;
	}

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",g=" ALGOBATFMT
		  ",e=" ALGOOPTBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOBATPAR(g),
		  ALGOOPTBATPAR(e), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(res),
		  __func__, GDKusec() - t0);
	return res;
alloc_fail:
	BBPreclaim(res);
	return NULL;
}
