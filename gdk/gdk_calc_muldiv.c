/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

/* ---------------------------------------------------------------------- */
/* multiplication (any numeric type) */

/* TYPE4 must be a type larger than both TYPE1 and TYPE2 so that
 * multiplying into it doesn't cause overflow */
#define MUL_4TYPE(TYPE1, TYPE2, TYPE3, TYPE4, IF)			\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
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
				MUL##IF##4_WITH_CHECK(lft[i], rgt[j],	\
						      TYPE3, dst[k],	\
						      max,		\
						      TYPE4,		\
						      ON_OVERFLOW(TYPE1, TYPE2, "*")); \
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
				MUL##IF##4_WITH_CHECK(lft[i], rgt[j],	\
						      TYPE3, dst[k],	\
						      max,		\
						      TYPE4,		\
						      ON_OVERFLOW(TYPE1, TYPE2, "*")); \
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define MUL_3TYPE_enlarge(TYPE1, TYPE2, TYPE3, IF)			\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
	BUN i = 0, j = 0, ncand = ci1->ncand;				\
	const bool couldoverflow = (max < (TYPE3) GDK_##TYPE1##_max * (TYPE3) GDK_##TYPE2##_max); \
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
				MUL##IF##4_WITH_CHECK(lft[i], rgt[j],	\
						      TYPE3, dst[k],	\
						      max,		\
						      TYPE3,		\
						      ON_OVERFLOW(TYPE1, TYPE2, "*")); \
			} else {					\
				dst[k] = (TYPE3) lft[i] * rgt[j];	\
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
				MUL##IF##4_WITH_CHECK(lft[i], rgt[j],	\
						      TYPE3, dst[k],	\
						      max,		\
						      TYPE3,		\
						      ON_OVERFLOW(TYPE1, TYPE2, "*")); \
			} else {					\
				dst[k] = (TYPE3) lft[i] * rgt[j];	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#ifdef HAVE_HGE

#define MUL_2TYPE_lng(TYPE1, TYPE2)	MUL_4TYPE(TYPE1, TYPE2, lng, hge, I)

#define MUL_2TYPE_hge(TYPE1, TYPE2)					\
static BUN								\
mul_##TYPE1##_##TYPE2##_hge(const TYPE1 *lft, bool incr1,		\
			    const TYPE2 *rgt, bool incr2,		\
			    hge *restrict dst, hge max,			\
			    struct canditer *restrict ci1,		\
			    struct canditer *restrict ci2,		\
			    oid candoff1, oid candoff2,			\
			    bool abort_on_error)			\
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
				dst[k] = hge_nil;			\
				nils++;					\
			} else {					\
				HGEMUL_CHECK(lft[i], rgt[j],		\
					     dst[k],			\
					     max,			\
					     ON_OVERFLOW(TYPE1, TYPE2, "*")); \
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
				dst[k] = hge_nil;			\
				nils++;					\
			} else {					\
				HGEMUL_CHECK(lft[i], rgt[j],		\
					     dst[k],			\
					     max,			\
					     ON_OVERFLOW(TYPE1, TYPE2, "*")); \
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#else

#define MUL_2TYPE_lng(TYPE1, TYPE2)					\
static BUN								\
mul_##TYPE1##_##TYPE2##_lng(const TYPE1 *lft, bool incr1,		\
			    const TYPE2 *rgt, bool incr2,		\
			    lng *restrict dst, lng max,			\
			    struct canditer *restrict ci1,		\
			    struct canditer *restrict ci2,		\
			    oid candoff1, oid candoff2,			\
			    bool abort_on_error)			\
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
				dst[k] = lng_nil;			\
				nils++;					\
			} else {					\
				LNGMUL_CHECK(lft[i], rgt[j],		\
					     dst[k],			\
					     max,			\
					     ON_OVERFLOW(TYPE1, TYPE2, "*")); \
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
				dst[k] = lng_nil;			\
				nils++;					\
			} else {					\
				LNGMUL_CHECK(lft[i], rgt[j],		\
					     dst[k],			\
					     max,			\
					     ON_OVERFLOW(TYPE1, TYPE2, "*")); \
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#endif

#define MUL_2TYPE_float(TYPE1, TYPE2, TYPE3)				\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
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
				/* only check for overflow, not for underflow */ \
				dst[k] = (TYPE3) (lft[i] * rgt[j]);	\
				if (isinf(dst[k]) || ABSOLUTE(dst[k]) > max) { \
					if (abort_on_error)		\
						ON_OVERFLOW(TYPE1, TYPE2, "*");	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
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
			} else {					\
				/* only check for overflow, not for underflow */ \
				dst[k] = (TYPE3) (lft[i] * rgt[j]);	\
				if (isinf(dst[k]) || ABSOLUTE(dst[k]) > max) { \
					if (abort_on_error)		\
						ON_OVERFLOW(TYPE1, TYPE2, "*");	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define absbte(x)	abs(x)
#define abssht(x)	abs(x)
#define absint(x)	abs(x)
#define abslng(x)	llabs(x)
#define abshge(x)	ABSOLUTE(x)

#define MUL_INT_FLT_INT(TYPE1, TYPE2, TYPE3)				\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(					\
	const TYPE1 *lft, bool incr1, const TYPE2 *rgt, bool incr2,	\
	TYPE3 *restrict dst, TYPE3 max,					\
	struct canditer *restrict ci1, struct canditer *restrict ci2,	\
	oid candoff1, oid candoff2, bool abort_on_error)		\
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
			} else if (lft[i] == 0 || rgt[j] == 0) {	\
				dst[k] = 0;				\
			} else if (max / fabs(rgt[j]) < abs##TYPE1(lft[i])) { \
				if (abort_on_error)			\
					ON_OVERFLOW(TYPE1, TYPE2, "*"); \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				double m = lft[i] * rgt[j];		\
				dst[k] = (TYPE3) rounddbl(m);		\
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
			} else if (lft[i] == 0 || rgt[j] == 0) {	\
				dst[k] = 0;				\
			} else if (max / fabs(rgt[j]) < abs##TYPE1(lft[i])) { \
				if (abort_on_error)			\
					ON_OVERFLOW(TYPE1, TYPE2, "*"); \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				double m = lft[i] * rgt[j];		\
				dst[k] = (TYPE3) rounddbl(m);		\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

MUL_INT_FLT_INT(bte, flt, bte)
MUL_INT_FLT_INT(bte, flt, sht)
MUL_INT_FLT_INT(bte, flt, int)
MUL_INT_FLT_INT(bte, flt, lng)
MUL_INT_FLT_INT(sht, flt, bte)
MUL_INT_FLT_INT(sht, flt, sht)
MUL_INT_FLT_INT(sht, flt, int)
MUL_INT_FLT_INT(sht, flt, lng)
MUL_INT_FLT_INT(int, flt, bte)
MUL_INT_FLT_INT(int, flt, sht)
MUL_INT_FLT_INT(int, flt, int)
MUL_INT_FLT_INT(int, flt, lng)
MUL_INT_FLT_INT(lng, flt, bte)
MUL_INT_FLT_INT(lng, flt, sht)
MUL_INT_FLT_INT(lng, flt, int)
MUL_INT_FLT_INT(lng, flt, lng)
#ifdef HAVE_HGE
MUL_INT_FLT_INT(bte, flt, hge)
MUL_INT_FLT_INT(sht, flt, hge)
MUL_INT_FLT_INT(int, flt, hge)
MUL_INT_FLT_INT(lng, flt, hge)
MUL_INT_FLT_INT(hge, flt, bte)
MUL_INT_FLT_INT(hge, flt, sht)
MUL_INT_FLT_INT(hge, flt, int)
MUL_INT_FLT_INT(hge, flt, lng)
MUL_INT_FLT_INT(hge, flt, hge)
#endif

MUL_INT_FLT_INT(bte, dbl, bte)
MUL_INT_FLT_INT(bte, dbl, sht)
MUL_INT_FLT_INT(bte, dbl, int)
MUL_INT_FLT_INT(bte, dbl, lng)
MUL_INT_FLT_INT(sht, dbl, bte)
MUL_INT_FLT_INT(sht, dbl, sht)
MUL_INT_FLT_INT(sht, dbl, int)
MUL_INT_FLT_INT(sht, dbl, lng)
MUL_INT_FLT_INT(int, dbl, bte)
MUL_INT_FLT_INT(int, dbl, sht)
MUL_INT_FLT_INT(int, dbl, int)
MUL_INT_FLT_INT(int, dbl, lng)
MUL_INT_FLT_INT(lng, dbl, bte)
MUL_INT_FLT_INT(lng, dbl, sht)
MUL_INT_FLT_INT(lng, dbl, int)
MUL_INT_FLT_INT(lng, dbl, lng)
#ifdef HAVE_HGE
MUL_INT_FLT_INT(bte, dbl, hge)
MUL_INT_FLT_INT(sht, dbl, hge)
MUL_INT_FLT_INT(int, dbl, hge)
MUL_INT_FLT_INT(lng, dbl, hge)
MUL_INT_FLT_INT(hge, dbl, bte)
MUL_INT_FLT_INT(hge, dbl, sht)
MUL_INT_FLT_INT(hge, dbl, int)
MUL_INT_FLT_INT(hge, dbl, lng)
MUL_INT_FLT_INT(hge, dbl, hge)
#endif

MUL_4TYPE(bte, bte, bte, sht, I)
MUL_3TYPE_enlarge(bte, bte, sht, I)
MUL_3TYPE_enlarge(bte, bte, int, I)
MUL_3TYPE_enlarge(bte, bte, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, bte, hge, I)
#endif
MUL_3TYPE_enlarge(bte, bte, flt, F)
MUL_3TYPE_enlarge(bte, bte, dbl, F)
MUL_4TYPE(bte, sht, sht, int, I)
MUL_3TYPE_enlarge(bte, sht, int, I)
MUL_3TYPE_enlarge(bte, sht, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, sht, hge, I)
#endif
MUL_3TYPE_enlarge(bte, sht, flt, F)
MUL_3TYPE_enlarge(bte, sht, dbl, F)
MUL_4TYPE(bte, int, int, lng, I)
MUL_3TYPE_enlarge(bte, int, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, int, hge, I)
#endif
MUL_3TYPE_enlarge(bte, int, flt, F)
MUL_3TYPE_enlarge(bte, int, dbl, F)
MUL_2TYPE_lng(bte, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, lng, hge, I)
#endif
MUL_3TYPE_enlarge(bte, lng, flt, F)
MUL_3TYPE_enlarge(bte, lng, dbl, F)
#ifdef HAVE_HGE
MUL_2TYPE_hge(bte, hge)
MUL_3TYPE_enlarge(bte, hge, flt, F)
MUL_3TYPE_enlarge(bte, hge, dbl, F)
#endif
MUL_2TYPE_float(bte, flt, flt)
MUL_3TYPE_enlarge(bte, flt, dbl, F)
MUL_2TYPE_float(bte, dbl, dbl)
MUL_4TYPE(sht, bte, sht, int, I)
MUL_3TYPE_enlarge(sht, bte, int, I)
MUL_3TYPE_enlarge(sht, bte, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, bte, hge, I)
#endif
MUL_3TYPE_enlarge(sht, bte, flt, F)
MUL_3TYPE_enlarge(sht, bte, dbl, F)
MUL_4TYPE(sht, sht, sht, int, I)
MUL_3TYPE_enlarge(sht, sht, int, I)
MUL_3TYPE_enlarge(sht, sht, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, sht, hge, I)
#endif
MUL_3TYPE_enlarge(sht, sht, flt, F)
MUL_3TYPE_enlarge(sht, sht, dbl, F)
MUL_4TYPE(sht, int, int, lng, I)
MUL_3TYPE_enlarge(sht, int, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, int, hge, I)
#endif
MUL_3TYPE_enlarge(sht, int, flt, F)
MUL_3TYPE_enlarge(sht, int, dbl, F)
MUL_2TYPE_lng(sht, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, lng, hge, I)
#endif
MUL_3TYPE_enlarge(sht, lng, flt, F)
MUL_3TYPE_enlarge(sht, lng, dbl, F)
#ifdef HAVE_HGE
MUL_2TYPE_hge(sht, hge)
MUL_3TYPE_enlarge(sht, hge, flt, F)
MUL_3TYPE_enlarge(sht, hge, dbl, F)
#endif
MUL_2TYPE_float(sht, flt, flt)
MUL_3TYPE_enlarge(sht, flt, dbl, F)
MUL_2TYPE_float(sht, dbl, dbl)
MUL_4TYPE(int, bte, int, lng, I)
MUL_3TYPE_enlarge(int, bte, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, bte, hge, I)
#endif
MUL_3TYPE_enlarge(int, bte, flt, F)
MUL_3TYPE_enlarge(int, bte, dbl, F)
MUL_4TYPE(int, sht, int, lng, I)
MUL_3TYPE_enlarge(int, sht, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, sht, hge, I)
#endif
MUL_3TYPE_enlarge(int, sht, flt, F)
MUL_3TYPE_enlarge(int, sht, dbl, F)
MUL_4TYPE(int, int, int, lng, I)
MUL_3TYPE_enlarge(int, int, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, int, hge, I)
#endif
MUL_3TYPE_enlarge(int, int, flt, F)
MUL_3TYPE_enlarge(int, int, dbl, F)
MUL_2TYPE_lng(int, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, lng, hge, I)
#endif
MUL_3TYPE_enlarge(int, lng, flt, F)
MUL_3TYPE_enlarge(int, lng, dbl, F)
#ifdef HAVE_HGE
MUL_2TYPE_hge(int, hge)
MUL_3TYPE_enlarge(int, hge, flt, F)
MUL_3TYPE_enlarge(int, hge, dbl, F)
#endif
MUL_2TYPE_float(int, flt, flt)
MUL_3TYPE_enlarge(int, flt, dbl, F)
MUL_2TYPE_float(int, dbl, dbl)
MUL_2TYPE_lng(lng, bte)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, bte, hge, I)
#endif
MUL_3TYPE_enlarge(lng, bte, flt, F)
MUL_3TYPE_enlarge(lng, bte, dbl, F)
MUL_2TYPE_lng(lng, sht)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, sht, hge, I)
#endif
MUL_3TYPE_enlarge(lng, sht, flt, F)
MUL_3TYPE_enlarge(lng, sht, dbl, F)
MUL_2TYPE_lng(lng, int)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, int, hge, I)
#endif
MUL_3TYPE_enlarge(lng, int, flt, F)
MUL_3TYPE_enlarge(lng, int, dbl, F)
MUL_2TYPE_lng(lng, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, lng, hge, I)
#endif
MUL_3TYPE_enlarge(lng, lng, flt, F)
MUL_3TYPE_enlarge(lng, lng, dbl, F)
#ifdef HAVE_HGE
MUL_2TYPE_hge(lng, hge)
MUL_3TYPE_enlarge(lng, hge, flt, F)
MUL_3TYPE_enlarge(lng, hge, dbl, F)
#endif
MUL_2TYPE_float(lng, flt, flt)
MUL_3TYPE_enlarge(lng, flt, dbl, F)
MUL_2TYPE_float(lng, dbl, dbl)
#ifdef HAVE_HGE
MUL_2TYPE_hge(hge, bte)
MUL_3TYPE_enlarge(hge, bte, flt, F)
MUL_3TYPE_enlarge(hge, bte, dbl, F)
MUL_2TYPE_hge(hge, sht)
MUL_3TYPE_enlarge(hge, sht, flt, F)
MUL_3TYPE_enlarge(hge, sht, dbl, F)
MUL_2TYPE_hge(hge, int)
MUL_3TYPE_enlarge(hge, int, flt, F)
MUL_3TYPE_enlarge(hge, int, dbl, F)
MUL_2TYPE_hge(hge, lng)
MUL_3TYPE_enlarge(hge, lng, flt, F)
MUL_3TYPE_enlarge(hge, lng, dbl, F)
MUL_2TYPE_hge(hge, hge)
MUL_3TYPE_enlarge(hge, hge, flt, F)
MUL_3TYPE_enlarge(hge, hge, dbl, F)
MUL_2TYPE_float(hge, flt, flt)
MUL_3TYPE_enlarge(hge, flt, dbl, F)
MUL_2TYPE_float(hge, dbl, dbl)
#endif
MUL_2TYPE_float(flt, bte, flt)
MUL_3TYPE_enlarge(flt, bte, dbl, F)
MUL_2TYPE_float(flt, sht, flt)
MUL_3TYPE_enlarge(flt, sht, dbl, F)
MUL_2TYPE_float(flt, int, flt)
MUL_3TYPE_enlarge(flt, int, dbl, F)
MUL_2TYPE_float(flt, lng, flt)
MUL_3TYPE_enlarge(flt, lng, dbl, F)
#ifdef HAVE_HGE
MUL_2TYPE_float(flt, hge, flt)
MUL_3TYPE_enlarge(flt, hge, dbl, F)
#endif
MUL_2TYPE_float(flt, flt, flt)
MUL_3TYPE_enlarge(flt, flt, dbl, F)
MUL_2TYPE_float(flt, dbl, dbl)
MUL_2TYPE_float(dbl, bte, dbl)
MUL_2TYPE_float(dbl, sht, dbl)
MUL_2TYPE_float(dbl, int, dbl)
MUL_2TYPE_float(dbl, lng, dbl)
#ifdef HAVE_HGE
MUL_2TYPE_float(dbl, hge, dbl)
#endif
MUL_2TYPE_float(dbl, flt, dbl)
MUL_2TYPE_float(dbl, dbl, dbl)

static BUN
mul_typeswitchloop(const void *lft, int tp1, bool incr1,
		   const void *rgt, int tp2, bool incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *func)
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
				nils = mul_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mul_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mul_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mul_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = mul_bte_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_bte_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_bte_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_bte_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = mul_bte_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_bte_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_bte_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_bte_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_dbl:
				nils = mul_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mul_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mul_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mul_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = mul_sht_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_sht_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_sht_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_sht_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = mul_sht_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_sht_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_sht_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_sht_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_dbl:
				nils = mul_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_int_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				nils = mul_int_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mul_int_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mul_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = mul_int_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_int_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_int_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = mul_int_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_int_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_int_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_dbl:
				nils = mul_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				nils = mul_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				nils = mul_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mul_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = mul_lng_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_lng_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_lng_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_lng_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = mul_lng_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_lng_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_lng_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_lng_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_dbl:
				nils = mul_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				nils = mul_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				nils = mul_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				nils = mul_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_bte:
				nils = mul_hge_flt_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_hge_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_hge_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_hge_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mul_hge_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = mul_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_bte:
				nils = mul_hge_dbl_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_hge_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_hge_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_hge_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mul_hge_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2,
						       candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
	GDKerror("%s: type combination mul(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

static BAT *
BATcalcmuldivmod(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp,
		 bool abort_on_error,
		 BUN (*typeswitchloop)(const void *, int, bool,
				       const void *, int, bool,
				       void *restrict, int,
				       struct canditer *restrict,
				       struct canditer *restrict,
				       oid, oid, bool, const char *),
		 const char *func)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", func);
		return NULL;
	}

	bn = COLnew(ci1.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	nils = (*typeswitchloop)(b1i.base, b1->ttype, true,
				 b2i.base, b2->ttype, true,
				 Tloc(bn, 0), tp,
				 &ci1, &ci2, b1->hseqbase, b2->hseqbase,
				 abort_on_error, func);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "%s: b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  func, ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcmul(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp, bool abort_on_error)
{
	return BATcalcmuldivmod(b1, b2, s1, s2, tp, abort_on_error,
				mul_typeswitchloop, __func__);
}

BAT *
BATcalcmulcst(BAT *b, const ValRecord *v, BAT *s, int tp, bool abort_on_error)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	ncand = canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	bool btsorted = b->tsorted, btrevsorted = b->trevsorted;
	nils = mul_typeswitchloop(bi.base, b->ttype, true,
				  VALptr(v), v->vtype, false,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  b->hseqbase, 0,
				  abort_on_error, __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted, or reverse sorted if the constant is negative */
	if (abort_on_error) {
		ValRecord sign;

		VARcalcsign(&sign, v);
		bn->tsorted = (sign.val.btval >= 0 && btsorted && nils == 0) ||
			(sign.val.btval <= 0 && btrevsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
		bn->trevsorted = (sign.val.btval >= 0 && btrevsorted && nils == 0) ||
			(sign.val.btval <= 0 && btsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
	} else {
		bn->tsorted = ncand <= 1 || nils == ncand;
		bn->trevsorted = ncand <= 1 || nils == ncand;
	}
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstmul(const ValRecord *v, BAT *b, BAT *s, int tp, bool abort_on_error)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	ncand = canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	bool btsorted = b->tsorted, btrevsorted = b->trevsorted;
	nils = mul_typeswitchloop(VALptr(v), v->vtype, false,
				  bi.base, b->ttype, true,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  0, b->hseqbase,
				  abort_on_error, __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted, or reverse sorted if the constant is negative */
	if (abort_on_error) {
		ValRecord sign;

		VARcalcsign(&sign, v);
		bn->tsorted = (sign.val.btval >= 0 && btsorted && nils == 0) ||
			(sign.val.btval <= 0 && btrevsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
		bn->trevsorted = (sign.val.btval >= 0 && btrevsorted && nils == 0) ||
			(sign.val.btval <= 0 && btsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
	} else {
		bn->tsorted = ncand <= 1 || nils == ncand;
		bn->trevsorted = ncand <= 1 || nils == ncand;
	}
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

gdk_return
VARcalcmul(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	if (mul_typeswitchloop(VALptr(lft), lft->vtype, false,
			       VALptr(rgt), rgt->vtype, false,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, abort_on_error, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* division (any numeric type) */

#define DIV_3TYPE(TYPE1, TYPE2, TYPE3)					\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
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
				if (abort_on_error)			\
					return BUN_NONE + 1;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) (lft[i] / rgt[j]);	\
				if (dst[k] < -max || dst[k] > max) {	\
					if (abort_on_error)		\
						return BUN_NONE + 2;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
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
				if (abort_on_error)			\
					return BUN_NONE + 1;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) (lft[i] / rgt[j]);	\
				if (dst[k] < -max || dst[k] > max) {	\
					if (abort_on_error)		\
						return BUN_NONE + 2;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
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
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
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
				if (abort_on_error) {			\
					if (rgt[j] == 0)		\
						return BUN_NONE + 1;	\
					ON_OVERFLOW(TYPE1, TYPE2, "/");	\
				}					\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] / rgt[j];	\
				if (dst[k] < -max || dst[k] > max) {	\
					if (abort_on_error)		\
						return BUN_NONE + 2;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
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
				if (abort_on_error) {			\
					if (rgt[j] == 0)		\
						return BUN_NONE + 1;	\
					ON_OVERFLOW(TYPE1, TYPE2, "/");	\
				}					\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] / rgt[j];	\
				if (dst[k] < -max || dst[k] > max) {	\
					if (abort_on_error)		\
						return BUN_NONE + 2;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
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
	oid candoff1, oid candoff2, bool abort_on_error)		\
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
				if (abort_on_error)			\
					return BUN_NONE + 1;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				double m = fabs(rgt[j]);		\
				if (m < 1 && abs##TYPE1(lft[i]) > m * max) { \
					if (abort_on_error)		\
						ON_OVERFLOW(TYPE1, TYPE2, "/");	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				} else {				\
					m = lft[i] / rgt[j];		\
					dst[k] = (TYPE3) rounddbl(m);	\
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
				if (abort_on_error)			\
					return BUN_NONE + 1;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				double m = fabs(rgt[j]);		\
				if (m < 1 && abs##TYPE1(lft[i]) > m * max) { \
					if (abort_on_error)		\
						ON_OVERFLOW(TYPE1, TYPE2, "/");	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				} else {				\
					m = lft[i] / rgt[j];		\
					dst[k] = (TYPE3) rounddbl(m);	\
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
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *func)
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_bte_int_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_bte_lng_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_lng_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_bte_hge_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_hge_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_bte_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_bte_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_dbl:
				nils = div_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_sht_lng_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_sht_hge_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_sht_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_sht_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_sht_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_sht_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_dbl:
				nils = div_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_int_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_int_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_int_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_int_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_int_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_int_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_int_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_dbl:
				nils = div_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_lng_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_lng_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_lng_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_lng_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_lng_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_lng_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_dbl:
				nils = div_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_hge_flt_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_hge_flt_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_hge_flt_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_hge_flt_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = div_hge_dbl_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_hge_dbl_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_hge_dbl_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_hge_dbl_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
						       ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
BATcalcdiv(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp, bool abort_on_error)
{
	return BATcalcmuldivmod(b1, b2, s1, s2, tp, abort_on_error,
				div_typeswitchloop, __func__);
}

BAT *
BATcalcdivcst(BAT *b, const ValRecord *v, BAT *s, int tp, bool abort_on_error)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	ncand = canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	bool btsorted = b->tsorted, btrevsorted = b->trevsorted;
	nils = div_typeswitchloop(bi.base, b->ttype, true,
				  VALptr(v), v->vtype, false,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  b->hseqbase, 0,
				  abort_on_error, __func__);
	bat_iterator_end(&bi);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no zero division occurred (we
	 * only know for sure if abort_on_error is set), the result is
	 * also sorted, or reverse sorted if the constant is
	 * negative */
	if (abort_on_error) {
		ValRecord sign;

		VARcalcsign(&sign, v);
		bn->tsorted = (sign.val.btval > 0 && btsorted && nils == 0) ||
			(sign.val.btval < 0 && btrevsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
		bn->trevsorted = (sign.val.btval > 0 && btrevsorted && nils == 0) ||
			(sign.val.btval < 0 && btsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
	} else {
		bn->tsorted = ncand <= 1 || nils == ncand;
		bn->trevsorted = ncand <= 1 || nils == ncand;
	}
	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstdiv(const ValRecord *v, BAT *b, BAT *s, int tp, bool abort_on_error)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	ncand = canditer_init(&ci, b, s);
	if (ncand == 0)
		return BATconstant(ci.hseq, tp, ATOMnilptr(tp),
				   ncand, TRANSIENT);

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	nils = div_typeswitchloop(VALptr(v), v->vtype, false,
				  bi.base, b->ttype, true,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  0, b->hseqbase,
				  abort_on_error, __func__);
	bat_iterator_end(&bi);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

gdk_return
VARcalcdiv(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	if (div_typeswitchloop(VALptr(lft), lft->vtype, false,
			       VALptr(rgt), rgt->vtype, false,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, abort_on_error, __func__) >= BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* modulo (any numeric type) */

#define MOD_3TYPE(TYPE1, TYPE2, TYPE3)					\
static BUN								\
mod_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst,			\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
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
				if (abort_on_error)			\
					return BUN_NONE + 1;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] % rgt[j];	\
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
				if (abort_on_error)			\
					return BUN_NONE + 1;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] % rgt[j];	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define FMOD_3TYPE(TYPE1, TYPE2, TYPE3, FUNC)				\
static BUN								\
mod_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, bool incr1,		\
				const TYPE2 *rgt, bool incr2,		\
				TYPE3 *restrict dst,			\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
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
				if (abort_on_error)			\
					return BUN_NONE + 1;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) FUNC((TYPE3) lft[i],	\
						      (TYPE3) rgt[j]);	\
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
				if (abort_on_error)			\
					return BUN_NONE + 1;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) FUNC((TYPE3) lft[i],	\
						      (TYPE3) rgt[j]);	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

MOD_3TYPE(bte, bte, bte)
MOD_3TYPE(bte, bte, sht)
MOD_3TYPE(bte, bte, int)
MOD_3TYPE(bte, bte, lng)
#ifdef HAVE_HGE
MOD_3TYPE(bte, bte, hge)
#endif
MOD_3TYPE(bte, sht, bte)
MOD_3TYPE(bte, sht, sht)
MOD_3TYPE(bte, sht, int)
MOD_3TYPE(bte, sht, lng)
#ifdef HAVE_HGE
MOD_3TYPE(bte, sht, hge)
#endif
MOD_3TYPE(bte, int, bte)
MOD_3TYPE(bte, int, sht)
MOD_3TYPE(bte, int, int)
MOD_3TYPE(bte, int, lng)
#ifdef HAVE_HGE
MOD_3TYPE(bte, int, hge)
#endif
MOD_3TYPE(bte, lng, bte)
MOD_3TYPE(bte, lng, sht)
MOD_3TYPE(bte, lng, int)
MOD_3TYPE(bte, lng, lng)
#ifdef HAVE_HGE
MOD_3TYPE(bte, lng, hge)
#endif
#ifdef HAVE_HGE
MOD_3TYPE(bte, hge, bte)
MOD_3TYPE(bte, hge, sht)
MOD_3TYPE(bte, hge, int)
MOD_3TYPE(bte, hge, lng)
MOD_3TYPE(bte, hge, hge)
#endif
MOD_3TYPE(sht, bte, bte)
MOD_3TYPE(sht, bte, sht)
MOD_3TYPE(sht, bte, int)
MOD_3TYPE(sht, bte, lng)
#ifdef HAVE_HGE
MOD_3TYPE(sht, bte, hge)
#endif
MOD_3TYPE(sht, sht, sht)
MOD_3TYPE(sht, sht, int)
MOD_3TYPE(sht, sht, lng)
#ifdef HAVE_HGE
MOD_3TYPE(sht, sht, hge)
#endif
MOD_3TYPE(sht, int, sht)
MOD_3TYPE(sht, int, int)
MOD_3TYPE(sht, int, lng)
#ifdef HAVE_HGE
MOD_3TYPE(sht, int, hge)
#endif
MOD_3TYPE(sht, lng, sht)
MOD_3TYPE(sht, lng, int)
MOD_3TYPE(sht, lng, lng)
#ifdef HAVE_HGE
MOD_3TYPE(sht, lng, hge)
#endif
#ifdef HAVE_HGE
MOD_3TYPE(sht, hge, sht)
MOD_3TYPE(sht, hge, int)
MOD_3TYPE(sht, hge, lng)
MOD_3TYPE(sht, hge, hge)
#endif
MOD_3TYPE(int, bte, bte)
MOD_3TYPE(int, bte, sht)
MOD_3TYPE(int, bte, int)
MOD_3TYPE(int, bte, lng)
#ifdef HAVE_HGE
MOD_3TYPE(int, bte, hge)
#endif
MOD_3TYPE(int, sht, sht)
MOD_3TYPE(int, sht, int)
MOD_3TYPE(int, sht, lng)
#ifdef HAVE_HGE
MOD_3TYPE(int, sht, hge)
#endif
MOD_3TYPE(int, int, int)
MOD_3TYPE(int, int, lng)
#ifdef HAVE_HGE
MOD_3TYPE(int, int, hge)
#endif
MOD_3TYPE(int, lng, int)
MOD_3TYPE(int, lng, lng)
#ifdef HAVE_HGE
MOD_3TYPE(int, lng, hge)
#endif
#ifdef HAVE_HGE
MOD_3TYPE(int, hge, int)
MOD_3TYPE(int, hge, lng)
MOD_3TYPE(int, hge, hge)
#endif
MOD_3TYPE(lng, bte, bte)
MOD_3TYPE(lng, bte, sht)
MOD_3TYPE(lng, bte, int)
MOD_3TYPE(lng, bte, lng)
#ifdef HAVE_HGE
MOD_3TYPE(lng, bte, hge)
#endif
MOD_3TYPE(lng, sht, sht)
MOD_3TYPE(lng, sht, int)
MOD_3TYPE(lng, sht, lng)
#ifdef HAVE_HGE
MOD_3TYPE(lng, sht, hge)
#endif
MOD_3TYPE(lng, int, int)
MOD_3TYPE(lng, int, lng)
#ifdef HAVE_HGE
MOD_3TYPE(lng, int, hge)
#endif
MOD_3TYPE(lng, lng, lng)
#ifdef HAVE_HGE
MOD_3TYPE(lng, lng, hge)
#endif
#ifdef HAVE_HGE
MOD_3TYPE(lng, hge, lng)
MOD_3TYPE(lng, hge, hge)
#endif
#ifdef HAVE_HGE
MOD_3TYPE(hge, bte, bte)
MOD_3TYPE(hge, bte, sht)
MOD_3TYPE(hge, bte, int)
MOD_3TYPE(hge, bte, lng)
MOD_3TYPE(hge, bte, hge)
MOD_3TYPE(hge, sht, sht)
MOD_3TYPE(hge, sht, int)
MOD_3TYPE(hge, sht, lng)
MOD_3TYPE(hge, sht, hge)
MOD_3TYPE(hge, int, int)
MOD_3TYPE(hge, int, lng)
MOD_3TYPE(hge, int, hge)
MOD_3TYPE(hge, lng, lng)
MOD_3TYPE(hge, lng, hge)
MOD_3TYPE(hge, hge, hge)
#endif

FMOD_3TYPE(bte, flt, flt, fmodf)
FMOD_3TYPE(sht, flt, flt, fmodf)
FMOD_3TYPE(int, flt, flt, fmodf)
FMOD_3TYPE(lng, flt, flt, fmodf)
#ifdef HAVE_HGE
FMOD_3TYPE(hge, flt, flt, fmodf)
#endif
FMOD_3TYPE(flt, bte, flt, fmodf)
FMOD_3TYPE(flt, sht, flt, fmodf)
FMOD_3TYPE(flt, int, flt, fmodf)
FMOD_3TYPE(flt, lng, flt, fmodf)
#ifdef HAVE_HGE
FMOD_3TYPE(flt, hge, flt, fmodf)
#endif
FMOD_3TYPE(flt, flt, flt, fmodf)
FMOD_3TYPE(bte, dbl, dbl, fmod)
FMOD_3TYPE(sht, dbl, dbl, fmod)
FMOD_3TYPE(int, dbl, dbl, fmod)
FMOD_3TYPE(lng, dbl, dbl, fmod)
#ifdef HAVE_HGE
FMOD_3TYPE(hge, dbl, dbl, fmod)
#endif
FMOD_3TYPE(flt, dbl, dbl, fmod)
FMOD_3TYPE(dbl, bte, dbl, fmod)
FMOD_3TYPE(dbl, sht, dbl, fmod)
FMOD_3TYPE(dbl, int, dbl, fmod)
FMOD_3TYPE(dbl, lng, dbl, fmod)
#ifdef HAVE_HGE
FMOD_3TYPE(dbl, hge, dbl, fmod)
#endif
FMOD_3TYPE(dbl, flt, dbl, fmod)
FMOD_3TYPE(dbl, dbl, dbl, fmod)

static BUN
mod_typeswitchloop(const void *lft, int tp1, bool incr1,
		   const void *rgt, int tp2, bool incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1, struct canditer *restrict ci2,
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *func)
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
				nils = mod_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_sht_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_int_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_bte_int_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_lng_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_bte_lng_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_lng_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_hge_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_bte_hge_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_hge_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_hge_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
			case TYPE_bte:
				nils = mod_sht_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_sht:
				nils = mod_sht_int_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_sht_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_sht:
				nils = mod_sht_lng_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_sht_lng_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_sht:
				nils = mod_sht_hge_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_sht_hge_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_hge_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
			case TYPE_bte:
				nils = mod_int_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_int_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_int_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_int_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_int_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mod_int_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_int:
				nils = mod_int_lng_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_int:
				nils = mod_int_hge_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_hge_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
			case TYPE_bte:
				nils = mod_lng_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_lng_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_lng_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_lng_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_lng_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mod_lng_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mod_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_lng:
				nils = mod_lng_hge_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
			case TYPE_bte:
				nils = mod_hge_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mod_hge_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_hge_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_hge_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_hge_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_hge_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_hge_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mod_hge_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_hge_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mod_hge_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mod_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_hge_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, candoff1, candoff2,
						       abort_on_error);
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
	GDKerror("%s: type combination (mod(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcmod(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp, bool abort_on_error)
{
	return BATcalcmuldivmod(b1, b2, s1, s2, tp, abort_on_error,
				mod_typeswitchloop, __func__);
}

BAT *
BATcalcmodcst(BAT *b, const ValRecord *v, BAT *s, int tp, bool abort_on_error)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	ncand = canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = mod_typeswitchloop(bi.base, b->ttype, true,
				  VALptr(v), v->vtype, false,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  b->hseqbase, 0,
				  abort_on_error, __func__);
	bat_iterator_end(&bi);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstmod(const ValRecord *v, BAT *b, BAT *s, int tp, bool abort_on_error)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	ncand = canditer_init(&ci, b, s);
	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = mod_typeswitchloop(VALptr(v), v->vtype, false,
				  bi.base, b->ttype, true,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  0, b->hseqbase,
				  abort_on_error, __func__);
	bat_iterator_end(&bi);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

gdk_return
VARcalcmod(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	if (mod_typeswitchloop(VALptr(lft), lft->vtype, false,
			       VALptr(rgt), rgt->vtype, false,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, abort_on_error, __func__) >= BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}
