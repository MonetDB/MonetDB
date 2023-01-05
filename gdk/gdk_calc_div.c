/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

/* ---------------------------------------------------------------------- */
/* division (any numeric type) */

#define DIV_3TYPE(TYPE1, TYPE2, TYPE3)					\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2)		\
{									\
	BUN nils = 0;							\
	BUN i = 0, j = 0, ncand = ci1->ncand;				\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {		\
		TIMEOUT_LOOP_IDX_DECL(k, ncand, timeoffset) {		\
			if (incr1)					\
				i = canditer_next_dense(ci1) - candoff1; \
			if (incr2)					\
				j = canditer_next_dense(ci2) - candoff2; \
			if (is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] == 0) {			\
				return BUN_NONE + 1;			\
			} else {					\
				dst[k] = (TYPE3) (lft[i] / rgt[j]);	\
				if (dst[k] < -max || dst[k] > max) {	\
					return BUN_NONE + 2;		\
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	} else {							\
		TIMEOUT_LOOP_IDX_DECL(k, ncand, timeoffset) {		\
			if (incr1)					\
				i = canditer_next(ci1) - candoff1;	\
			if (incr2)					\
				j = canditer_next(ci2) - candoff2;	\
			if (is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] == 0) {			\
				return BUN_NONE + 1;			\
			} else {					\
				dst[k] = (TYPE3) (lft[i] / rgt[j]);	\
				if (dst[k] < -max || dst[k] > max) {	\
					return BUN_NONE + 2;		\
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define DIV_3TYPE_float(TYPE1, TYPE2, TYPE3)				\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2)		\
{									\
	BUN nils = 0;							\
	BUN i = 0, j = 0, ncand = ci1->ncand;				\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {		\
		TIMEOUT_LOOP_IDX_DECL(k, ncand, timeoffset) {		\
			if (incr1)					\
				i = canditer_next_dense(ci1) - candoff1; \
			if (incr2)					\
				j = canditer_next_dense(ci2) - candoff2; \
			if (is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] == 0 ||			\
				   (ABSOLUTE(rgt[j]) < 1 &&		\
				    GDK_##TYPE3##_max * ABSOLUTE(rgt[j]) < ABSOLUTE(lft[i]))) { \
				/* only check for overflow, not for underflow */ \
				if (rgt[j] == 0)			\
					return BUN_NONE + 1;		\
				ON_OVERFLOW(TYPE1, TYPE2, "/");		\
			} else {					\
				dst[k] = (TYPE3) lft[i] / rgt[j];	\
				if (dst[k] < -max || dst[k] > max) {	\
					return BUN_NONE + 2;		\
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	} else {							\
		TIMEOUT_LOOP_IDX_DECL(k, ncand, timeoffset) {		\
			if (incr1)					\
				i = canditer_next(ci1) - candoff1;	\
			if (incr2)					\
				j = canditer_next(ci2) - candoff2;	\
			if (is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] == 0 ||			\
				   (ABSOLUTE(rgt[j]) < 1 &&		\
				    GDK_##TYPE3##_max * ABSOLUTE(rgt[j]) < ABSOLUTE(lft[i]))) { \
				/* only check for overflow, not for underflow */ \
				if (rgt[j] == 0)			\
					return BUN_NONE + 1;		\
				ON_OVERFLOW(TYPE1, TYPE2, "/");		\
			} else {					\
				dst[k] = (TYPE3) lft[i] / rgt[j];	\
				if (dst[k] < -max || dst[k] > max) {	\
					return BUN_NONE + 2;		\
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define DIV_INT_FLT_INT(TYPE1, TYPE2, TYPE3)				\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(					\
	const TYPE1 *lft, bool incr1, const TYPE2 *rgt, bool incr2,	\
	TYPE3 *restrict dst, TYPE3 max,					\
	struct canditer *restrict ci1, struct canditer *restrict ci2,	\
	oid candoff1, oid candoff2)		\
{									\
	BUN nils = 0;							\
	BUN i = 0, j = 0, ncand = ci1->ncand;				\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {		\
		TIMEOUT_LOOP_IDX_DECL(k, ncand, timeoffset) {		\
			if (incr1)					\
				i = canditer_next_dense(ci1) - candoff1; \
			if (incr2)					\
				j = canditer_next_dense(ci2) - candoff2; \
			if (is_##TYPE1##_nil(lft[i]) ||			\
			    is_##TYPE2##_nil(rgt[j])) {			\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (lft[i] == 0) {			\
				dst[k] = 0;				\
			} else if (rgt[j] == 0) {			\
				return BUN_NONE + 1;			\
			} else {					\
				double m = fabs(rgt[j]);		\
				if (m < 1 && abs##TYPE1(lft[i]) > m * max) { \
					ON_OVERFLOW(TYPE1, TYPE2, "/");	\
				} else {				\
					dst[k] = (TYPE3) rounddbl(lft[i] / (ldouble) rgt[j]); \
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	} else {							\
		TIMEOUT_LOOP_IDX_DECL(k, ncand, timeoffset) {		\
			if (incr1)					\
				i = canditer_next(ci1) - candoff1;	\
			if (incr2)					\
				j = canditer_next(ci2) - candoff2;	\
			if (is_##TYPE1##_nil(lft[i]) ||			\
			    is_##TYPE2##_nil(rgt[j])) {			\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (lft[i] == 0) {			\
				dst[k] = 0;				\
			} else if (rgt[j] == 0) {			\
				return BUN_NONE + 1;			\
			} else {					\
				double m = fabs(rgt[j]);		\
				if (m < 1 && abs##TYPE1(lft[i]) > m * max) { \
					ON_OVERFLOW(TYPE1, TYPE2, "/");	\
				} else {				\
					dst[k] = (TYPE3) rounddbl(lft[i] / (ldouble) rgt[j]); \
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

DIV_INT_FLT_INT(bte, flt, bte)
DIV_INT_FLT_INT(bte, flt, sht)
DIV_INT_FLT_INT(bte, flt, int)
DIV_INT_FLT_INT(bte, flt, lng)
DIV_INT_FLT_INT(sht, flt, bte)
DIV_INT_FLT_INT(sht, flt, sht)
DIV_INT_FLT_INT(sht, flt, int)
DIV_INT_FLT_INT(sht, flt, lng)
DIV_INT_FLT_INT(int, flt, bte)
DIV_INT_FLT_INT(int, flt, sht)
DIV_INT_FLT_INT(int, flt, int)
DIV_INT_FLT_INT(int, flt, lng)
DIV_INT_FLT_INT(lng, flt, bte)
DIV_INT_FLT_INT(lng, flt, sht)
DIV_INT_FLT_INT(lng, flt, int)
DIV_INT_FLT_INT(lng, flt, lng)
#ifdef HAVE_HGE
DIV_INT_FLT_INT(bte, flt, hge)
DIV_INT_FLT_INT(sht, flt, hge)
DIV_INT_FLT_INT(int, flt, hge)
DIV_INT_FLT_INT(lng, flt, hge)
DIV_INT_FLT_INT(hge, flt, bte)
DIV_INT_FLT_INT(hge, flt, sht)
DIV_INT_FLT_INT(hge, flt, int)
DIV_INT_FLT_INT(hge, flt, lng)
DIV_INT_FLT_INT(hge, flt, hge)
#endif

DIV_INT_FLT_INT(bte, dbl, bte)
DIV_INT_FLT_INT(bte, dbl, sht)
DIV_INT_FLT_INT(bte, dbl, int)
DIV_INT_FLT_INT(bte, dbl, lng)
DIV_INT_FLT_INT(sht, dbl, bte)
DIV_INT_FLT_INT(sht, dbl, sht)
DIV_INT_FLT_INT(sht, dbl, int)
DIV_INT_FLT_INT(sht, dbl, lng)
DIV_INT_FLT_INT(int, dbl, bte)
DIV_INT_FLT_INT(int, dbl, sht)
DIV_INT_FLT_INT(int, dbl, int)
DIV_INT_FLT_INT(int, dbl, lng)
DIV_INT_FLT_INT(lng, dbl, bte)
DIV_INT_FLT_INT(lng, dbl, sht)
DIV_INT_FLT_INT(lng, dbl, int)
DIV_INT_FLT_INT(lng, dbl, lng)
#ifdef HAVE_HGE
DIV_INT_FLT_INT(bte, dbl, hge)
DIV_INT_FLT_INT(sht, dbl, hge)
DIV_INT_FLT_INT(int, dbl, hge)
DIV_INT_FLT_INT(lng, dbl, hge)
DIV_INT_FLT_INT(hge, dbl, bte)
DIV_INT_FLT_INT(hge, dbl, sht)
DIV_INT_FLT_INT(hge, dbl, int)
DIV_INT_FLT_INT(hge, dbl, lng)
DIV_INT_FLT_INT(hge, dbl, hge)
#endif

DIV_3TYPE(bte, bte, bte)
DIV_3TYPE(bte, bte, sht)
DIV_3TYPE(bte, bte, int)
DIV_3TYPE(bte, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE(bte, bte, hge)
#endif
DIV_3TYPE(bte, bte, flt)
DIV_3TYPE(bte, bte, dbl)
DIV_3TYPE(bte, sht, bte)
DIV_3TYPE(bte, sht, sht)
DIV_3TYPE(bte, sht, int)
DIV_3TYPE(bte, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE(bte, sht, hge)
#endif
DIV_3TYPE(bte, sht, flt)
DIV_3TYPE(bte, sht, dbl)
DIV_3TYPE(bte, int, bte)
DIV_3TYPE(bte, int, sht)
DIV_3TYPE(bte, int, int)
DIV_3TYPE(bte, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE(bte, int, hge)
#endif
DIV_3TYPE(bte, int, flt)
DIV_3TYPE(bte, int, dbl)
DIV_3TYPE(bte, lng, bte)
DIV_3TYPE(bte, lng, sht)
DIV_3TYPE(bte, lng, int)
DIV_3TYPE(bte, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE(bte, lng, hge)
#endif
DIV_3TYPE(bte, lng, flt)
DIV_3TYPE(bte, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(bte, hge, bte)
DIV_3TYPE(bte, hge, sht)
DIV_3TYPE(bte, hge, int)
DIV_3TYPE(bte, hge, lng)
DIV_3TYPE(bte, hge, hge)
DIV_3TYPE(bte, hge, flt)
DIV_3TYPE(bte, hge, dbl)
#endif
DIV_3TYPE_float(bte, flt, flt)
DIV_3TYPE_float(bte, flt, dbl)
DIV_3TYPE_float(bte, dbl, dbl)
DIV_3TYPE(sht, bte, sht)
DIV_3TYPE(sht, bte, int)
DIV_3TYPE(sht, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE(sht, bte, hge)
#endif
DIV_3TYPE(sht, bte, flt)
DIV_3TYPE(sht, bte, dbl)
DIV_3TYPE(sht, sht, sht)
DIV_3TYPE(sht, sht, int)
DIV_3TYPE(sht, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE(sht, sht, hge)
#endif
DIV_3TYPE(sht, sht, flt)
DIV_3TYPE(sht, sht, dbl)
DIV_3TYPE(sht, int, sht)
DIV_3TYPE(sht, int, int)
DIV_3TYPE(sht, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE(sht, int, hge)
#endif
DIV_3TYPE(sht, int, flt)
DIV_3TYPE(sht, int, dbl)
DIV_3TYPE(sht, lng, sht)
DIV_3TYPE(sht, lng, int)
DIV_3TYPE(sht, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE(sht, lng, hge)
#endif
DIV_3TYPE(sht, lng, flt)
DIV_3TYPE(sht, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(sht, hge, sht)
DIV_3TYPE(sht, hge, int)
DIV_3TYPE(sht, hge, lng)
DIV_3TYPE(sht, hge, hge)
DIV_3TYPE(sht, hge, flt)
DIV_3TYPE(sht, hge, dbl)
#endif
DIV_3TYPE_float(sht, flt, flt)
DIV_3TYPE_float(sht, flt, dbl)
DIV_3TYPE_float(sht, dbl, dbl)
DIV_3TYPE(int, bte, int)
DIV_3TYPE(int, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE(int, bte, hge)
#endif
DIV_3TYPE(int, bte, flt)
DIV_3TYPE(int, bte, dbl)
DIV_3TYPE(int, sht, int)
DIV_3TYPE(int, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE(int, sht, hge)
#endif
DIV_3TYPE(int, sht, flt)
DIV_3TYPE(int, sht, dbl)
DIV_3TYPE(int, int, int)
DIV_3TYPE(int, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE(int, int, hge)
#endif
DIV_3TYPE(int, int, flt)
DIV_3TYPE(int, int, dbl)
DIV_3TYPE(int, lng, int)
DIV_3TYPE(int, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE(int, lng, hge)
#endif
DIV_3TYPE(int, lng, flt)
DIV_3TYPE(int, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(int, hge, int)
DIV_3TYPE(int, hge, lng)
DIV_3TYPE(int, hge, hge)
DIV_3TYPE(int, hge, flt)
DIV_3TYPE(int, hge, dbl)
#endif
DIV_3TYPE_float(int, flt, flt)
DIV_3TYPE_float(int, flt, dbl)
DIV_3TYPE_float(int, dbl, dbl)
DIV_3TYPE(lng, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE(lng, bte, hge)
#endif
DIV_3TYPE(lng, bte, flt)
DIV_3TYPE(lng, bte, dbl)
DIV_3TYPE(lng, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE(lng, sht, hge)
#endif
DIV_3TYPE(lng, sht, flt)
DIV_3TYPE(lng, sht, dbl)
DIV_3TYPE(lng, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE(lng, int, hge)
#endif
DIV_3TYPE(lng, int, flt)
DIV_3TYPE(lng, int, dbl)
DIV_3TYPE(lng, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE(lng, lng, hge)
#endif
DIV_3TYPE(lng, lng, flt)
DIV_3TYPE(lng, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(lng, hge, lng)
DIV_3TYPE(lng, hge, hge)
DIV_3TYPE(lng, hge, flt)
DIV_3TYPE(lng, hge, dbl)
#endif
DIV_3TYPE_float(lng, flt, flt)
DIV_3TYPE_float(lng, flt, dbl)
DIV_3TYPE_float(lng, dbl, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(hge, bte, hge)
DIV_3TYPE(hge, bte, flt)
DIV_3TYPE(hge, bte, dbl)
DIV_3TYPE(hge, sht, hge)
DIV_3TYPE(hge, sht, flt)
DIV_3TYPE(hge, sht, dbl)
DIV_3TYPE(hge, int, hge)
DIV_3TYPE(hge, int, flt)
DIV_3TYPE(hge, int, dbl)
DIV_3TYPE(hge, lng, hge)
DIV_3TYPE(hge, lng, flt)
DIV_3TYPE(hge, lng, dbl)
DIV_3TYPE(hge, hge, hge)
DIV_3TYPE(hge, hge, flt)
DIV_3TYPE(hge, hge, dbl)
DIV_3TYPE_float(hge, flt, flt)
DIV_3TYPE_float(hge, flt, dbl)
DIV_3TYPE_float(hge, dbl, dbl)
#endif
DIV_3TYPE(flt, bte, flt)
DIV_3TYPE(flt, bte, dbl)
DIV_3TYPE(flt, sht, flt)
DIV_3TYPE(flt, sht, dbl)
DIV_3TYPE(flt, int, flt)
DIV_3TYPE(flt, int, dbl)
DIV_3TYPE(flt, lng, flt)
DIV_3TYPE(flt, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(flt, hge, flt)
DIV_3TYPE(flt, hge, dbl)
#endif
DIV_3TYPE_float(flt, flt, flt)
DIV_3TYPE_float(flt, flt, dbl)
DIV_3TYPE_float(flt, dbl, dbl)
DIV_3TYPE(dbl, bte, dbl)
DIV_3TYPE(dbl, sht, dbl)
DIV_3TYPE(dbl, int, dbl)
DIV_3TYPE(dbl, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(dbl, hge, dbl)
#endif
DIV_3TYPE_float(dbl, flt, dbl)
DIV_3TYPE_float(dbl, dbl, dbl)

static BUN
div_typeswitchloop(const void *lft, int tp1, bool incr1,
		   const void *rgt, int tp2, bool incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1, struct canditer *restrict ci2,
		   oid candoff1, oid candoff2, const char *func)
{
	BUN nils;

	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	tp = ATOMbasetype(tp);
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_sht_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_int_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_bte_int_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_lng_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_bte_lng_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_bte_lng_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_hge_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_bte_hge_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_bte_hge_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_bte_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_hge:
				nils = div_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_bte_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_bte_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_bte_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_bte_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_bte_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_bte_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_bte_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_dbl:
				nils = div_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_int_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_lng_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_sht_lng_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_hge_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_sht_hge_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_sht_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_hge:
				nils = div_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_sht_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = div_sht_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_sht_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_sht_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_sht_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = div_sht_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_sht_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_sht_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_sht_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_dbl:
				nils = div_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_int:
				nils = div_int_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				nils = div_int_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = div_int_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_int:
				nils = div_int_lng_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_int:
				nils = div_int_hge_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_int_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_hge:
				nils = div_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_int_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = div_int_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_int_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_int_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_int_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = div_int_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_int_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_int_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_int_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_dbl:
				nils = div_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_hge:
				nils = div_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_lng_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = div_lng_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_lng_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_lng_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_lng_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = div_lng_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_lng_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_lng_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_lng_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_dbl:
				nils = div_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_hge_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_hge_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_hge_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_hge_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = div_hge_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_hge_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_hge_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_hge_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_hge:
				nils = div_hge_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = div_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_hge_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = div_hge_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = div_hge_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = div_hge_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = div_hge_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_hge:
				nils = div_hge_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
#endif
	case TYPE_flt:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_flt_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = div_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	default:
		goto unsupported;
	}

	if (nils == BUN_NONE + 1)
		GDKerror("22012!division by zero.\n");

	return nils;

  unsupported:
	GDKerror("%s: type combination (div(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcdiv(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp)
{
	return BATcalcmuldivmod(b1, b2, s1, s2, tp,
				div_typeswitchloop, __func__);
}

BAT *
BATcalcdivcst(BAT *b, const ValRecord *v, BAT *s, int tp)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, tp, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = div_typeswitchloop(bi.base, bi.type, true,
				  VALptr(v), v->vtype, false,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  b->hseqbase, 0, __func__);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		bat_iterator_end(&bi);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	/* if the input is sorted, and no zero division occurred, the
	 * result is also sorted, or reverse sorted if the constant is
	 * negative */
	ValRecord sign;

	VARcalcsign(&sign, v);
	bn->tsorted = (sign.val.btval > 0 && bi.sorted && nils == 0) ||
		(sign.val.btval < 0 && bi.revsorted && nils == 0) ||
		ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = (sign.val.btval > 0 && bi.revsorted && nils == 0) ||
		(sign.val.btval < 0 && bi.sorted && nils == 0) ||
		ci.ncand <= 1 || nils == ci.ncand;
	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bat_iterator_end(&bi);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstdiv(const ValRecord *v, BAT *b, BAT *s, int tp)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	canditer_init(&ci, b, s);
	if (ci.ncand == 0)
		return BATconstant(ci.hseq, tp, ATOMnilptr(tp),
				   ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, tp, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	nils = div_typeswitchloop(VALptr(v), v->vtype, false,
				  bi.base, bi.type, true,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  &ci,
				  0, b->hseqbase, __func__);
	bat_iterator_end(&bi);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

gdk_return
VARcalcdiv(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (div_typeswitchloop(VALptr(lft), lft->vtype, false,
			       VALptr(rgt), rgt->vtype, false,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, __func__) >= BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}
