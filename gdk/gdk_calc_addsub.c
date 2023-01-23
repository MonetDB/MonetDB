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
/* addition (any numeric type) */

#define ADD_3TYPE(TYPE1, TYPE2, TYPE3, IF)				\
static BUN								\
add_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
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
			} else {					\
				ADD##IF##_WITH_CHECK(lft[i], rgt[j],	\
						     TYPE3, dst[k],	\
						     max,		\
						     ON_OVERFLOW(TYPE1, TYPE2, "+")); \
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
			} else {					\
				ADD##IF##_WITH_CHECK(lft[i], rgt[j],	\
						     TYPE3, dst[k],	\
						     max,		\
						     ON_OVERFLOW(TYPE1, TYPE2, "+")); \
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define ADD_3TYPE_enlarge(TYPE1, TYPE2, TYPE3, IF)			\
static BUN								\
add_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2)		\
{									\
	BUN nils = 0;							\
	BUN i = 0, j = 0, ncand = ci1->ncand;				\
	const bool couldoverflow = (max < (TYPE3) GDK_##TYPE1##_max + (TYPE3) GDK_##TYPE2##_max); \
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
			} else if (couldoverflow) {			\
				ADD##IF##_WITH_CHECK(lft[i], rgt[j],	\
						     TYPE3, dst[k],	\
						     max,		\
						     ON_OVERFLOW(TYPE1, TYPE2, "+")); \
			} else {					\
				dst[k] = (TYPE3) lft[i] + rgt[j];	\
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
			} else if (couldoverflow) {			\
				ADD##IF##_WITH_CHECK(lft[i], rgt[j],	\
						     TYPE3, dst[k],	\
						     max,		\
						     ON_OVERFLOW(TYPE1, TYPE2, "+")); \
			} else {					\
				dst[k] = (TYPE3) lft[i] + rgt[j];	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

ADD_3TYPE(bte, bte, bte, I)
ADD_3TYPE_enlarge(bte, bte, sht, I)
ADD_3TYPE_enlarge(bte, bte, int, I)
ADD_3TYPE_enlarge(bte, bte, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, bte, hge, I)
#endif
ADD_3TYPE_enlarge(bte, bte, flt, F)
ADD_3TYPE_enlarge(bte, bte, dbl, F)
ADD_3TYPE(bte, sht, sht, I)
ADD_3TYPE_enlarge(bte, sht, int, I)
ADD_3TYPE_enlarge(bte, sht, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, sht, hge, I)
#endif
ADD_3TYPE_enlarge(bte, sht, flt, F)
ADD_3TYPE_enlarge(bte, sht, dbl, F)
ADD_3TYPE(bte, int, int, I)
ADD_3TYPE_enlarge(bte, int, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, int, hge, I)
#endif
ADD_3TYPE_enlarge(bte, int, flt, F)
ADD_3TYPE_enlarge(bte, int, dbl, F)
ADD_3TYPE(bte, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, lng, hge, I)
#endif
ADD_3TYPE_enlarge(bte, lng, flt, F)
ADD_3TYPE_enlarge(bte, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(bte, hge, hge, I)
ADD_3TYPE_enlarge(bte, hge, flt, F)
ADD_3TYPE_enlarge(bte, hge, dbl, F)
#endif
ADD_3TYPE(bte, flt, flt, F)
ADD_3TYPE_enlarge(bte, flt, dbl, F)
ADD_3TYPE(bte, dbl, dbl, F)
ADD_3TYPE(sht, bte, sht, I)
ADD_3TYPE_enlarge(sht, bte, int, I)
ADD_3TYPE_enlarge(sht, bte, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, bte, hge, I)
#endif
ADD_3TYPE_enlarge(sht, bte, flt, F)
ADD_3TYPE_enlarge(sht, bte, dbl, F)
ADD_3TYPE(sht, sht, sht, I)
ADD_3TYPE_enlarge(sht, sht, int, I)
ADD_3TYPE_enlarge(sht, sht, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, sht, hge, I)
#endif
ADD_3TYPE_enlarge(sht, sht, flt, F)
ADD_3TYPE_enlarge(sht, sht, dbl, F)
ADD_3TYPE(sht, int, int, I)
ADD_3TYPE_enlarge(sht, int, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, int, hge, I)
#endif
ADD_3TYPE_enlarge(sht, int, flt, F)
ADD_3TYPE_enlarge(sht, int, dbl, F)
ADD_3TYPE(sht, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, lng, hge, I)
#endif
ADD_3TYPE_enlarge(sht, lng, flt, F)
ADD_3TYPE_enlarge(sht, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(sht, hge, hge, I)
ADD_3TYPE_enlarge(sht, hge, flt, F)
ADD_3TYPE_enlarge(sht, hge, dbl, F)
#endif
ADD_3TYPE(sht, flt, flt, F)
ADD_3TYPE_enlarge(sht, flt, dbl, F)
ADD_3TYPE(sht, dbl, dbl, F)
ADD_3TYPE(int, bte, int, I)
ADD_3TYPE_enlarge(int, bte, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, bte, hge, I)
#endif
ADD_3TYPE_enlarge(int, bte, flt, F)
ADD_3TYPE_enlarge(int, bte, dbl, F)
ADD_3TYPE(int, sht, int, I)
ADD_3TYPE_enlarge(int, sht, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, sht, hge, I)
#endif
ADD_3TYPE_enlarge(int, sht, flt, F)
ADD_3TYPE_enlarge(int, sht, dbl, F)
ADD_3TYPE(int, int, int, I)
ADD_3TYPE_enlarge(int, int, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, int, hge, I)
#endif
ADD_3TYPE_enlarge(int, int, flt, F)
ADD_3TYPE_enlarge(int, int, dbl, F)
ADD_3TYPE(int, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, lng, hge, I)
#endif
ADD_3TYPE_enlarge(int, lng, flt, F)
ADD_3TYPE_enlarge(int, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(int, hge, hge, I)
ADD_3TYPE_enlarge(int, hge, flt, F)
ADD_3TYPE_enlarge(int, hge, dbl, F)
#endif
ADD_3TYPE(int, flt, flt, F)
ADD_3TYPE_enlarge(int, flt, dbl, F)
ADD_3TYPE(int, dbl, dbl, F)
ADD_3TYPE(lng, bte, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, bte, hge, I)
#endif
ADD_3TYPE_enlarge(lng, bte, flt, F)
ADD_3TYPE_enlarge(lng, bte, dbl, F)
ADD_3TYPE(lng, sht, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, sht, hge, I)
#endif
ADD_3TYPE_enlarge(lng, sht, flt, F)
ADD_3TYPE_enlarge(lng, sht, dbl, F)
ADD_3TYPE(lng, int, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, int, hge, I)
#endif
ADD_3TYPE_enlarge(lng, int, flt, F)
ADD_3TYPE_enlarge(lng, int, dbl, F)
ADD_3TYPE(lng, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, lng, hge, I)
#endif
ADD_3TYPE_enlarge(lng, lng, flt, F)
ADD_3TYPE_enlarge(lng, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(lng, hge, hge, I)
ADD_3TYPE_enlarge(lng, hge, flt, F)
ADD_3TYPE_enlarge(lng, hge, dbl, F)
#endif
ADD_3TYPE(lng, flt, flt, F)
ADD_3TYPE_enlarge(lng, flt, dbl, F)
ADD_3TYPE(lng, dbl, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(hge, bte, hge, I)
ADD_3TYPE_enlarge(hge, bte, flt, F)
ADD_3TYPE_enlarge(hge, bte, dbl, F)
ADD_3TYPE(hge, sht, hge, I)
ADD_3TYPE_enlarge(hge, sht, flt, F)
ADD_3TYPE_enlarge(hge, sht, dbl, F)
ADD_3TYPE(hge, int, hge, I)
ADD_3TYPE_enlarge(hge, int, flt, F)
ADD_3TYPE_enlarge(hge, int, dbl, F)
ADD_3TYPE(hge, lng, hge, I)
ADD_3TYPE_enlarge(hge, lng, flt, F)
ADD_3TYPE_enlarge(hge, lng, dbl, F)
ADD_3TYPE(hge, hge, hge, I)
ADD_3TYPE_enlarge(hge, hge, flt, F)
ADD_3TYPE_enlarge(hge, hge, dbl, F)
ADD_3TYPE(hge, flt, flt, F)
ADD_3TYPE_enlarge(hge, flt, dbl, F)
ADD_3TYPE(hge, dbl, dbl, F)
#endif
ADD_3TYPE(flt, bte, flt, F)
ADD_3TYPE_enlarge(flt, bte, dbl, F)
ADD_3TYPE(flt, sht, flt, F)
ADD_3TYPE_enlarge(flt, sht, dbl, F)
ADD_3TYPE(flt, int, flt, F)
ADD_3TYPE_enlarge(flt, int, dbl, F)
ADD_3TYPE(flt, lng, flt, F)
ADD_3TYPE_enlarge(flt, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(flt, hge, flt, F)
ADD_3TYPE_enlarge(flt, hge, dbl, F)
#endif
ADD_3TYPE(flt, flt, flt, F)
ADD_3TYPE_enlarge(flt, flt, dbl, F)
ADD_3TYPE(flt, dbl, dbl, F)
ADD_3TYPE(dbl, bte, dbl, F)
ADD_3TYPE(dbl, sht, dbl, F)
ADD_3TYPE(dbl, int, dbl, F)
ADD_3TYPE(dbl, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(dbl, hge, dbl, F)
#endif
ADD_3TYPE(dbl, flt, dbl, F)
ADD_3TYPE(dbl, dbl, dbl, F)

static BUN
add_typeswitchloop(const void *lft, int tp1, bool incr1,
		   const void *rgt, int tp2, bool incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1, struct canditer *restrict ci2,
		   oid candoff1, oid candoff2,
		   const char *func)
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
				nils = add_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = add_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = add_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_bte_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = add_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = add_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_bte_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = add_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_bte_int_dbl(lft, incr1, rgt, incr2,
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
				nils = add_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_bte_lng_dbl(lft, incr1, rgt, incr2,
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
			case TYPE_hge:
				nils = add_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_bte_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = add_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_bte_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = add_bte_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = add_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = add_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_sht_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = add_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = add_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_sht_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = add_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_sht_int_dbl(lft, incr1, rgt, incr2,
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
				nils = add_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_sht_lng_dbl(lft, incr1, rgt, incr2,
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
			case TYPE_hge:
				nils = add_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_sht_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = add_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_sht_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = add_sht_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = add_int_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_int_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = add_int_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_int_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = add_int_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = add_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_int_int_dbl(lft, incr1, rgt, incr2,
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
				nils = add_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_int_lng_dbl(lft, incr1, rgt, incr2,
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
			case TYPE_hge:
				nils = add_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_int_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = add_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_int_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = add_int_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = add_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_lng_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = add_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_lng_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = add_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_lng_int_dbl(lft, incr1, rgt, incr2,
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
				nils = add_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = add_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_lng_lng_dbl(lft, incr1, rgt, incr2,
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
			case TYPE_hge:
				nils = add_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_lng_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = add_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_lng_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = add_lng_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = add_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_hge_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = add_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_hge_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = add_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_hge_int_dbl(lft, incr1, rgt, incr2,
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
				nils = add_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_hge_lng_dbl(lft, incr1, rgt, incr2,
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
				nils = add_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = add_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = add_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_hge_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = add_hge_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = add_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_flt_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = add_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_flt_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = add_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_flt_int_dbl(lft, incr1, rgt, incr2,
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
				nils = add_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_flt_lng_dbl(lft, incr1, rgt, incr2,
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
				nils = add_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_flt_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = add_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = add_flt_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = add_flt_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = add_dbl_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = add_dbl_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = add_dbl_int_dbl(lft, incr1, rgt, incr2,
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
				nils = add_dbl_lng_dbl(lft, incr1, rgt, incr2,
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
				nils = add_dbl_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = add_dbl_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = add_dbl_dbl_dbl(lft, incr1, rgt, incr2,
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

	return nils;

  unsupported:
	GDKerror("%s: type combination (add(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

static BUN
addstr_loop(BAT *b1, const char *l, BAT *b2, const char *r, BAT *bn,
	    BATiter *b1i, BATiter *b2i,
	    struct canditer *restrict ci1, struct canditer *restrict ci2)
{
	BUN nils = 0, ncand = ci1->ncand;
	char *s;
	size_t slen, llen, rlen;
	oid candoff1, candoff2;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	assert(b1 != NULL || b2 != NULL); /* at least one not NULL */
	candoff1 = b1 ? b1->hseqbase : 0;
	candoff2 = b2 ? b2->hseqbase : 0;
	slen = 1024;
	s = GDKmalloc(slen);
	if (s == NULL)
		return BUN_NONE;
	TIMEOUT_LOOP_IDX_DECL(i, ncand, timeoffset) {
		oid x1 = canditer_next(ci1) - candoff1;
		oid x2 = canditer_next(ci2) - candoff2;
		if (b1)
			l = BUNtvar(*b1i, x1);
		if (b2)
			r = BUNtvar(*b2i, x2);
		if (strNil(l) || strNil(r)) {
			nils++;
			if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED)
				goto bailout;
		} else {
			llen = strlen(l);
			rlen = strlen(r);
			if (llen + rlen >= slen) {
				slen = llen + rlen + 1024;
				GDKfree(s);
				s = GDKmalloc(slen);
				if (s == NULL)
					goto bailout;
			}
			(void) stpcpy(stpcpy(s, l), r);
			if (tfastins_nocheckVAR(bn, i, s) != GDK_SUCCEED)
				goto bailout;
		}
	}
	TIMEOUT_CHECK(timeoffset,
		      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
	GDKfree(s);
	bn->theap->dirty = true;
	return nils;

  bailout:
	GDKfree(s);
	return BUN_NONE;
}

BAT *
BATcalcadd(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci1, ci2;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, tp, ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci1.ncand == 0)
		return bn;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	if (b1i.type == TYPE_str && b2i.type == TYPE_str && tp == TYPE_str) {
		nils = addstr_loop(b1, NULL, b2, NULL, bn, &b1i, &b2i, &ci1, &ci2);
	} else {
		nils = add_typeswitchloop(b1i.base, b1i.type, true,
					  b2i.base, b2i.type, true,
					  Tloc(bn, 0), tp,
					  &ci1, &ci2,
					  b1->hseqbase, b2->hseqbase, __func__);
	}

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		bat_iterator_end(&b1i);
		bat_iterator_end(&b2i);
		return NULL;
	}

	BATsetcount(bn, ci1.ncand);

	/* if both inputs are sorted the same way, and no overflow
	 * occurred, the result is also sorted */
	bn->tsorted = (b1i.sorted && b2i.sorted && nils == 0)
		|| ci1.ncand <= 1 || nils == ci1.ncand;
	bn->trevsorted = (b1i.revsorted && b2i.revsorted && nils == 0)
		|| ci1.ncand <= 1 || nils == ci1.ncand;
	bn->tkey = ci1.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcaddcst(BAT *b, const ValRecord *v, BAT *s, int tp)
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
	if (bi.type == TYPE_str && v->vtype == TYPE_str && tp == TYPE_str) {
		nils = addstr_loop(b, NULL, NULL, v->val.sval, bn, &bi, &(BATiter){0}, &ci, &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand});
	} else {
		nils = add_typeswitchloop(bi.base, bi.type, true,
					  VALptr(v), v->vtype, false,
					  Tloc(bn, 0), tp,
					  &ci,
					  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
					  b->hseqbase, 0, __func__);
	}

	if (nils == BUN_NONE) {
		bat_iterator_end(&bi);
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	/* if the input is sorted, and no overflow occurred, the result
	 * is also sorted */
	bn->tsorted = (bi.sorted && nils == 0) ||
		ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = (bi.revsorted && nils == 0) ||
		ci.ncand <= 1 || nils == ci.ncand;
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
BATcalccstadd(const ValRecord *v, BAT *b, BAT *s, int tp)
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
	if (bi.type == TYPE_str && v->vtype == TYPE_str && tp == TYPE_str) {
		nils = addstr_loop(NULL, v->val.sval, b, NULL, bn, &(BATiter){0}, &bi, &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand}, &ci);
	} else {
		nils = add_typeswitchloop(VALptr(v), v->vtype, false,
					  bi.base, bi.type, true,
					  Tloc(bn, 0), tp,
					  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
					  &ci,
					  0, b->hseqbase, __func__);
	}

	if (nils == BUN_NONE) {
		bat_iterator_end(&bi);
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	/* if the input is sorted, and no overflow occurred, the result
	 * is also sorted */
	bn->tsorted = (bi.sorted && nils == 0) ||
		ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = (bi.revsorted && nils == 0) ||
		ci.ncand <= 1 || nils == ci.ncand;
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

gdk_return
VARcalcadd(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (add_typeswitchloop(VALptr(lft), lft->vtype, false,
			       VALptr(rgt), rgt->vtype, false,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

static BAT *
BATcalcincrdecr(BAT *b, BAT *s,
		BUN (*typeswitchloop)(const void *, int, bool,
				      const void *, int, bool,
				      void *, int,
				      struct canditer *restrict,
				      struct canditer *restrict,
				      oid, oid, const char *),
		const char *func)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils= 0;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = (*typeswitchloop)(bi.base, bi.type, true,
				 &(bte){1}, TYPE_bte, false,
				 Tloc(bn, 0), bn->ttype,
				 &(struct canditer){.tpe=cand_dense, .ncand=1},
				 &ci,
				 0, b->hseqbase, func);

	if (nils == BUN_NONE) {
		bat_iterator_end(&bi);
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	/* if the input is sorted, and no overflow occurred, the result
	 * is also sorted */
	bn->tsorted = bi.sorted || ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = bi.revsorted || ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bat_iterator_end(&bi);

	TRC_DEBUG(ALGO, "%s: b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  func, ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcincr(BAT *b, BAT *s)
{
	return BATcalcincrdecr(b, s, add_typeswitchloop,
			       __func__);
}

gdk_return
VARcalcincr(ValPtr ret, const ValRecord *v)
{
	if (add_typeswitchloop(VALptr(v), v->vtype, false,
			       &(bte){1}, TYPE_bte, false,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* subtraction (any numeric type) */

#define SUB_3TYPE(TYPE1, TYPE2, TYPE3, IF)				\
static BUN								\
sub_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
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
			} else {					\
				SUB##IF##_WITH_CHECK(lft[i], rgt[j],	\
						     TYPE3, dst[k],	\
						     max,		\
						     ON_OVERFLOW(TYPE1, TYPE2, "-")); \
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
			} else {					\
				SUB##IF##_WITH_CHECK(lft[i], rgt[j],	\
						     TYPE3, dst[k],	\
						     max,		\
						     ON_OVERFLOW(TYPE1, TYPE2, "-")); \
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define SUB_3TYPE_enlarge(TYPE1, TYPE2, TYPE3, IF)			\
static BUN								\
sub_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2)		\
{									\
	BUN nils = 0;							\
	BUN i = 0, j = 0, ncand = ci1->ncand;				\
	const bool couldoverflow = (max < (TYPE3) GDK_##TYPE1##_max + (TYPE3) GDK_##TYPE2##_max); \
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
			} else if (couldoverflow) {			\
				SUB##IF##_WITH_CHECK(lft[i], rgt[j],	\
						     TYPE3, dst[k],	\
						     max,		\
						     ON_OVERFLOW(TYPE1, TYPE2, "-")); \
			} else {					\
				dst[k] = (TYPE3) lft[i] - rgt[j];	\
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
			} else if (couldoverflow) {			\
				SUB##IF##_WITH_CHECK(lft[i], rgt[j],	\
						     TYPE3, dst[k],	\
						     max,		\
						     ON_OVERFLOW(TYPE1, TYPE2, "-")); \
			} else {					\
				dst[k] = (TYPE3) lft[i] - rgt[j];	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

SUB_3TYPE(bte, bte, bte, I)
SUB_3TYPE_enlarge(bte, bte, sht, I)
SUB_3TYPE_enlarge(bte, bte, int, I)
SUB_3TYPE_enlarge(bte, bte, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, bte, hge, I)
#endif
SUB_3TYPE_enlarge(bte, bte, flt, F)
SUB_3TYPE_enlarge(bte, bte, dbl, F)
SUB_3TYPE(bte, sht, sht, I)
SUB_3TYPE_enlarge(bte, sht, int, I)
SUB_3TYPE_enlarge(bte, sht, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, sht, hge, I)
#endif
SUB_3TYPE_enlarge(bte, sht, flt, F)
SUB_3TYPE_enlarge(bte, sht, dbl, F)
SUB_3TYPE(bte, int, int, I)
SUB_3TYPE_enlarge(bte, int, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, int, hge, I)
#endif
SUB_3TYPE_enlarge(bte, int, flt, F)
SUB_3TYPE_enlarge(bte, int, dbl, F)
SUB_3TYPE(bte, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, lng, hge, I)
#endif
SUB_3TYPE_enlarge(bte, lng, flt, F)
SUB_3TYPE_enlarge(bte, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(bte, hge, hge, I)
SUB_3TYPE_enlarge(bte, hge, flt, F)
SUB_3TYPE_enlarge(bte, hge, dbl, F)
#endif
SUB_3TYPE(bte, flt, flt, F)
SUB_3TYPE_enlarge(bte, flt, dbl, F)
SUB_3TYPE(bte, dbl, dbl, F)
SUB_3TYPE(sht, bte, sht, I)
SUB_3TYPE_enlarge(sht, bte, int, I)
SUB_3TYPE_enlarge(sht, bte, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, bte, hge, I)
#endif
SUB_3TYPE_enlarge(sht, bte, flt, F)
SUB_3TYPE_enlarge(sht, bte, dbl, F)
SUB_3TYPE(sht, sht, sht, I)
SUB_3TYPE_enlarge(sht, sht, int, I)
SUB_3TYPE_enlarge(sht, sht, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, sht, hge, I)
#endif
SUB_3TYPE_enlarge(sht, sht, flt, F)
SUB_3TYPE_enlarge(sht, sht, dbl, F)
SUB_3TYPE(sht, int, int, I)
SUB_3TYPE_enlarge(sht, int, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, int, hge, I)
#endif
SUB_3TYPE_enlarge(sht, int, flt, F)
SUB_3TYPE_enlarge(sht, int, dbl, F)
SUB_3TYPE(sht, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, lng, hge, I)
#endif
SUB_3TYPE_enlarge(sht, lng, flt, F)
SUB_3TYPE_enlarge(sht, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(sht, hge, hge, I)
SUB_3TYPE_enlarge(sht, hge, flt, F)
SUB_3TYPE_enlarge(sht, hge, dbl, F)
#endif
SUB_3TYPE(sht, flt, flt, F)
SUB_3TYPE_enlarge(sht, flt, dbl, F)
SUB_3TYPE(sht, dbl, dbl, F)
SUB_3TYPE(int, bte, int, I)
SUB_3TYPE_enlarge(int, bte, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, bte, hge, I)
#endif
SUB_3TYPE_enlarge(int, bte, flt, F)
SUB_3TYPE_enlarge(int, bte, dbl, F)
SUB_3TYPE(int, sht, int, I)
SUB_3TYPE_enlarge(int, sht, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, sht, hge, I)
#endif
SUB_3TYPE_enlarge(int, sht, flt, F)
SUB_3TYPE_enlarge(int, sht, dbl, F)
SUB_3TYPE(int, int, int, I)
SUB_3TYPE_enlarge(int, int, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, int, hge, I)
#endif
SUB_3TYPE_enlarge(int, int, flt, F)
SUB_3TYPE_enlarge(int, int, dbl, F)
SUB_3TYPE(int, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, lng, hge, I)
#endif
SUB_3TYPE_enlarge(int, lng, flt, F)
SUB_3TYPE_enlarge(int, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(int, hge, hge, I)
SUB_3TYPE_enlarge(int, hge, flt, F)
SUB_3TYPE_enlarge(int, hge, dbl, F)
#endif
SUB_3TYPE(int, flt, flt, F)
SUB_3TYPE_enlarge(int, flt, dbl, F)
SUB_3TYPE(int, dbl, dbl, F)
SUB_3TYPE(lng, bte, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, bte, hge, I)
#endif
SUB_3TYPE_enlarge(lng, bte, flt, F)
SUB_3TYPE_enlarge(lng, bte, dbl, F)
SUB_3TYPE(lng, sht, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, sht, hge, I)
#endif
SUB_3TYPE_enlarge(lng, sht, flt, F)
SUB_3TYPE_enlarge(lng, sht, dbl, F)
SUB_3TYPE(lng, int, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, int, hge, I)
#endif
SUB_3TYPE_enlarge(lng, int, flt, F)
SUB_3TYPE_enlarge(lng, int, dbl, F)
SUB_3TYPE(lng, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, lng, hge, I)
#endif
SUB_3TYPE_enlarge(lng, lng, flt, F)
SUB_3TYPE_enlarge(lng, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(lng, hge, hge, I)
SUB_3TYPE_enlarge(lng, hge, flt, F)
SUB_3TYPE_enlarge(lng, hge, dbl, F)
#endif
SUB_3TYPE(lng, flt, flt, F)
SUB_3TYPE_enlarge(lng, flt, dbl, F)
SUB_3TYPE(lng, dbl, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(hge, bte, hge, I)
SUB_3TYPE_enlarge(hge, bte, flt, F)
SUB_3TYPE_enlarge(hge, bte, dbl, F)
SUB_3TYPE(hge, sht, hge, I)
SUB_3TYPE_enlarge(hge, sht, flt, F)
SUB_3TYPE_enlarge(hge, sht, dbl, F)
SUB_3TYPE(hge, int, hge, I)
SUB_3TYPE_enlarge(hge, int, flt, F)
SUB_3TYPE_enlarge(hge, int, dbl, F)
SUB_3TYPE(hge, lng, hge, I)
SUB_3TYPE_enlarge(hge, lng, flt, F)
SUB_3TYPE_enlarge(hge, lng, dbl, F)
SUB_3TYPE(hge, hge, hge, I)
SUB_3TYPE_enlarge(hge, hge, flt, F)
SUB_3TYPE_enlarge(hge, hge, dbl, F)
SUB_3TYPE(hge, flt, flt, F)
SUB_3TYPE_enlarge(hge, flt, dbl, F)
SUB_3TYPE(hge, dbl, dbl, F)
#endif
SUB_3TYPE(flt, bte, flt, F)
SUB_3TYPE_enlarge(flt, bte, dbl, F)
SUB_3TYPE(flt, sht, flt, F)
SUB_3TYPE_enlarge(flt, sht, dbl, F)
SUB_3TYPE(flt, int, flt, F)
SUB_3TYPE_enlarge(flt, int, dbl, F)
SUB_3TYPE(flt, lng, flt, F)
SUB_3TYPE_enlarge(flt, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(flt, hge, flt, F)
SUB_3TYPE_enlarge(flt, hge, dbl, F)
#endif
SUB_3TYPE(flt, flt, flt, F)
SUB_3TYPE_enlarge(flt, flt, dbl, F)
SUB_3TYPE(flt, dbl, dbl, F)
SUB_3TYPE(dbl, bte, dbl, F)
SUB_3TYPE(dbl, sht, dbl, F)
SUB_3TYPE(dbl, int, dbl, F)
SUB_3TYPE(dbl, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(dbl, hge, dbl, F)
#endif
SUB_3TYPE(dbl, flt, dbl, F)
SUB_3TYPE(dbl, dbl, dbl, F)

static BUN
sub_typeswitchloop(const void *lft, int tp1, bool incr1,
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
				nils = sub_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_sht:
				nils = sub_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = sub_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_bte_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = sub_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_bte_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_bte_int_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_bte_lng_dbl(lft, incr1, rgt, incr2,
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
			case TYPE_hge:
				nils = sub_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_bte_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_bte_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_bte_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = sub_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_sht_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_int:
				nils = sub_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_sht_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_sht_int_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_sht_lng_dbl(lft, incr1, rgt, incr2,
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
			case TYPE_hge:
				nils = sub_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_sht_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_sht_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_sht_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_int_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_int_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_int_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_int_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_int_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_lng:
				nils = sub_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_int_int_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_int_lng_dbl(lft, incr1, rgt, incr2,
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
			case TYPE_hge:
				nils = sub_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_int_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_int_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_int_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_lng_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_lng_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_lng_int_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
#endif
			case TYPE_flt:
				nils = sub_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_lng_lng_dbl(lft, incr1, rgt, incr2,
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
			case TYPE_hge:
				nils = sub_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_lng_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_lng_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_lng_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_hge_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_hge_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_hge_int_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_hge_lng_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_flt:
				nils = sub_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = sub_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_hge_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_hge_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_flt_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_flt_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_flt_int_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_flt_lng_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_flt_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2);
				break;
			case TYPE_dbl:
				nils = sub_flt_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_flt_dbl_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_dbl_bte_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_dbl_sht_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_dbl_int_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_dbl_lng_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_dbl_hge_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_dbl_flt_dbl(lft, incr1, rgt, incr2,
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
				nils = sub_dbl_dbl_dbl(lft, incr1, rgt, incr2,
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

	return nils;

  unsupported:
	GDKerror("%s: type combination (sub(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcsub(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci1, ci2;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, tp, ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci1.ncand == 0)
		return bn;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	nils = sub_typeswitchloop(b1i.base, b1i.type, true,
				  b2i.base, b2i.type, true,
				  Tloc(bn, 0), tp,
				  &ci1, &ci2,
				  b1->hseqbase, b2->hseqbase, __func__);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci1.ncand);

	bn->tsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->trevsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->tkey = ci1.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcsubcst(BAT *b, const ValRecord *v, BAT *s, int tp)
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
	nils = sub_typeswitchloop(bi.base, bi.type, true,
				  VALptr(v), v->vtype, false,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  b->hseqbase, 0, __func__);

	if (nils == BUN_NONE) {
		bat_iterator_end(&bi);
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	/* if the input is sorted, and no overflow occurred, the result
	 * is also sorted */
	bn->tsorted = (bi.sorted && nils == 0) ||
		ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = (bi.revsorted && nils == 0) ||
		ci.ncand <= 1 || nils == ci.ncand;
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
BATcalccstsub(const ValRecord *v, BAT *b, BAT *s, int tp)
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
	nils = sub_typeswitchloop(VALptr(v), v->vtype, false,
				  bi.base, bi.type, true,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  &ci,
				  0, b->hseqbase, __func__);

	if (nils == BUN_NONE) {
		bat_iterator_end(&bi);
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	/* if the input is sorted, and no overflow occurred, the result
	 * is sorted in the opposite direction (except that NILs mess
	 * things up */
	bn->tsorted = (nils == 0 && bi.revsorted) ||
		ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = (nils == 0 && bi.sorted) ||
		ci.ncand <= 1 || nils == ci.ncand;
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

gdk_return
VARcalcsub(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (sub_typeswitchloop(VALptr(lft), lft->vtype, false,
			       VALptr(rgt), rgt->vtype, false,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

BAT *
BATcalcdecr(BAT *b, BAT *s)
{
	return BATcalcincrdecr(b, s, sub_typeswitchloop,
			       __func__);
}

gdk_return
VARcalcdecr(ValPtr ret, const ValRecord *v)
{
	if (sub_typeswitchloop(VALptr(v), v->vtype, false,
			       &(bte){1}, TYPE_bte, false,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}
