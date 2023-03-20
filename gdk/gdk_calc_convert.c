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
/* type conversion (cast) */

/* a note on the return values from the internal conversion functions:
 *
 * the functions return the number of NIL values produced (or at
 * least, 0 if no NIL, and != 0 if there were any);
 * the return value is BUN_NONE if there was overflow and a message
 * was generated;
 * the return value is BUN_NONE + 1 if the types were not compatible;
 * the return value is BUN_NONE + 2 if inserting a value into a BAT
 * failed (only happens for conversion to str).
 */

#ifdef HAVE_HGE
static const hge scales[39] = {
	(hge) LL_CONSTANT(1),
	(hge) LL_CONSTANT(10),
	(hge) LL_CONSTANT(100),
	(hge) LL_CONSTANT(1000),
	(hge) LL_CONSTANT(10000),
	(hge) LL_CONSTANT(100000),
	(hge) LL_CONSTANT(1000000),
	(hge) LL_CONSTANT(10000000),
	(hge) LL_CONSTANT(100000000),
	(hge) LL_CONSTANT(1000000000),
	(hge) LL_CONSTANT(10000000000),
	(hge) LL_CONSTANT(100000000000),
	(hge) LL_CONSTANT(1000000000000),
	(hge) LL_CONSTANT(10000000000000),
	(hge) LL_CONSTANT(100000000000000),
	(hge) LL_CONSTANT(1000000000000000),
	(hge) LL_CONSTANT(10000000000000000),
	(hge) LL_CONSTANT(100000000000000000),
	(hge) LL_CONSTANT(1000000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000000U)
};
#else
static const lng scales[19] = {
	LL_CONSTANT(1),
	LL_CONSTANT(10),
	LL_CONSTANT(100),
	LL_CONSTANT(1000),
	LL_CONSTANT(10000),
	LL_CONSTANT(100000),
	LL_CONSTANT(1000000),
	LL_CONSTANT(10000000),
	LL_CONSTANT(100000000),
	LL_CONSTANT(1000000000),
	LL_CONSTANT(10000000000),
	LL_CONSTANT(100000000000),
	LL_CONSTANT(1000000000000),
	LL_CONSTANT(10000000000000),
	LL_CONSTANT(100000000000000),
	LL_CONSTANT(1000000000000000),
	LL_CONSTANT(10000000000000000),
	LL_CONSTANT(100000000000000000),
	LL_CONSTANT(1000000000000000000)
};
#endif

#define convertimpl_enlarge_float(TYPE1, TYPE2, MANT_DIG)		\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  struct canditer *restrict ci,			\
			  oid candoff, uint8_t scale1, bool *reduce)	\
{									\
	BUN i, nils = 0;						\
	TYPE1 v;							\
	oid x;								\
	const TYPE1 div = (TYPE1) scales[scale1];			\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	*reduce = 8 * sizeof(TYPE1) > MANT_DIG;				\
	if (ci->tpe == cand_dense) {					\
		if (div == 1) {						\
			TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {	\
				x = canditer_next_dense(ci) - candoff;	\
				v = src[x];				\
				if (is_##TYPE1##_nil(v)) {		\
					dst[i] = TYPE2##_nil;		\
					nils++;				\
				} else					\
					dst[i] = (TYPE2) v;		\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		} else {						\
			CAND_LOOP_IDX(ci, i) {				\
				x = canditer_next_dense(ci) - candoff;	\
				v = src[x];				\
				if (is_##TYPE1##_nil(v)) {		\
					dst[i] = TYPE2##_nil;		\
					nils++;				\
				} else					\
					dst[i] = (TYPE2) v / div;	\
			}						\
		}							\
	} else {							\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next(ci) - candoff;		\
			v = src[x];					\
			if (is_##TYPE1##_nil(v)) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else						\
				dst[i] = (TYPE2) v / div;		\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define CONV_OVERFLOW(TYPE1, TYPE2, value)				\
	do {								\
		GDKerror("22003!overflow in conversion of "		\
			 FMT##TYPE1 " to %s.\n", CST##TYPE1 (value),	\
			 TYPE2);					\
		return BUN_NONE;					\
	} while (0)

#define CONV_OVERFLOW_PREC(TYPE1, TYPE2, value, scale, prec)		\
	do {								\
		if (prec > 0)						\
			GDKerror("22003!overflow in conversion to "	\
				 "DECIMAL(%d,%d).\n", prec, scale);	\
		else							\
			GDKerror("22003!overflow in conversion of "	\
				 FMT##TYPE1 " to %s.\n", CST##TYPE1 (value), \
				 TYPE2);				\
		return BUN_NONE;					\
	} while (0)

#define convertimpl_oid_enlarge(TYPE1)					\
static BUN								\
convert_##TYPE1##_oid(const TYPE1 *src, oid *restrict dst,		\
		      struct canditer *restrict ci,			\
		      oid candoff, bool *reduce)			\
{									\
	BUN i, nils = 0;						\
	oid x;								\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	*reduce = false;						\
	if (ci->tpe == cand_dense) {					\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next_dense(ci) - candoff;		\
			if (is_##TYPE1##_nil(src[x])) {			\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (src[x] < 0) {			\
				CONV_OVERFLOW(TYPE1, "oid", src[i]);	\
			} else if (is_oid_nil((dst[i] = (oid) src[x]))) { \
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	} else {							\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next(ci) - candoff;		\
			if (is_##TYPE1##_nil(src[x])) {			\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (src[x] < 0) {			\
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
			} else if (is_oid_nil((dst[i] = (oid) src[x]))) { \
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define convertimpl_oid_reduce(TYPE1)					\
static BUN								\
convert_##TYPE1##_oid(const TYPE1 *src, oid *restrict dst,		\
		      struct canditer *restrict ci,			\
		      oid candoff, bool *reduce)			\
{									\
	BUN i, nils = 0;						\
	oid x;								\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	*reduce = false;						\
	if (ci->tpe == cand_dense) {					\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next_dense(ci) - candoff;		\
			if (is_##TYPE1##_nil(src[x])) {			\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (src[x] < 0 ||			\
				   src[x] > (TYPE1) GDK_oid_max) {	\
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
			} else if (is_oid_nil((dst[i] = (oid) src[x])))	{ \
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	} else {							\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next(ci) - candoff;		\
			if (is_##TYPE1##_nil(src[x])) {			\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (src[x] < 0 ||			\
				   src[x] > (TYPE1) GDK_oid_max) {	\
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
			} else if (is_oid_nil((dst[i] = (oid) src[x]))) { \
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define uint	unsigned int
#define usht	uint16_t
#define ubte	uint8_t

#ifdef TRUNCATE_NUMBERS
#define DIVIDE(v, div, TYPE)	((v) / (div))
#else
#define DIVIDE(v, div, TYPE)	((v) < 0 ? -(TYPE) (((u##TYPE) -(v) + ((u##TYPE) (div) >> 1)) / (div)) : (TYPE) (((u##TYPE) (v) + ((u##TYPE) (div) >> 1)) / (div)))
#endif

#define convertimpl(TYPE1, TYPE2)					\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *restrict src,			\
			  TYPE2 *restrict dst,				\
			  struct canditer *restrict ci,			\
			  oid candoff,					\
			  uint8_t scale1,				\
			  uint8_t scale2,				\
			  uint8_t precision,				\
			  bool *reduce)					\
{									\
	BUN i;								\
	BUN nils = 0;							\
	TYPE1 v;							\
	oid x;								\
	const TYPE1 div = (TYPE1) scales[scale1 > scale2 ? scale1 - scale2 : 0]; \
	const TYPE2 mul = (TYPE2) scales[scale2 > scale1 ? scale2 - scale1 : 0]; \
	const TYPE2 min = GDK_##TYPE2##_min / mul;			\
	const TYPE2 max = GDK_##TYPE2##_max / mul;			\
	const TYPE2 prec = (TYPE2) scales[precision] / mul;		\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	assert(div == 1 || mul == 1);					\
	assert(div >= 1 && mul >= 1);					\
									\
	*reduce = div > 1;						\
	if (ci->tpe == cand_dense) {					\
		if (div == 1 && mul == 1) {				\
			TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {	\
				x = canditer_next_dense(ci) - candoff;	\
				v = src[x];				\
				if (is_##TYPE1##_nil(v)) {		\
					dst[i] = TYPE2##_nil;		\
					nils++;				\
				} else if (v < min || v > max ||	\
					   (precision &&		\
					    (v >= prec || v <= -prec))) { \
					CONV_OVERFLOW_PREC(TYPE1, #TYPE2, v, scale2, precision); \
				} else {				\
					dst[i] = (TYPE2) v;		\
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		} else if (div == 1) {					\
			CAND_LOOP_IDX(ci, i) {				\
				x = canditer_next_dense(ci) - candoff;	\
				v = src[x];				\
				if (is_##TYPE1##_nil(v)) {		\
					dst[i] = TYPE2##_nil;		\
					nils++;				\
				} else if (v < min || v > max ||	\
					   (precision &&		\
					    (v >= prec || v <= -prec))) { \
					CONV_OVERFLOW_PREC(TYPE1, #TYPE2, src[x], scale2, precision); \
				} else {				\
					dst[i] = (TYPE2) v * mul;	\
				}					\
			}						\
		} else {						\
			/* mul == 1 */					\
			CAND_LOOP_IDX(ci, i) {				\
				x = canditer_next_dense(ci) - candoff;	\
				v = src[x];				\
				if (is_##TYPE1##_nil(v)) {		\
					dst[i] = TYPE2##_nil;		\
					nils++;				\
				} else {				\
					v = DIVIDE(v, div, TYPE1);	\
					if (v < min || v > max ||	\
					    (precision &&		\
					     (v >= prec || v <= -prec))) { \
						CONV_OVERFLOW_PREC(TYPE1, #TYPE2, src[x], scale2, precision); \
					} else {			\
						dst[i] = (TYPE2) v;	\
					}				\
				}					\
			}						\
		}							\
	} else {							\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next(ci) - candoff;		\
			v = src[x];					\
			if (is_##TYPE1##_nil(v)) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else {					\
				v = DIVIDE(v, div, TYPE1);		\
				if (v < min || v > max ||		\
				    (precision &&			\
				     (v >= prec || v <= -prec))) {	\
					CONV_OVERFLOW_PREC(TYPE1, #TYPE2, src[x], scale2, precision); \
				} else {				\
					dst[i] = (TYPE2) v * mul;	\
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

/* Special version of the above for converting from floating point.
 * The final assignment rounds the value which can still come out to
 * the NIL representation, so we need to check for that. */
#define convertimpl_reduce_float(TYPE1, TYPE2)				\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  struct canditer *restrict ci,			\
			  oid candoff, uint8_t scale2, uint8_t precision, \
			  bool *reduce)					\
{									\
	BUN i, nils = 0;						\
	oid x;								\
	TYPE1 v;							\
	const TYPE2 mul = (TYPE2) scales[scale2];			\
	const TYPE2 min = GDK_##TYPE2##_min;				\
	const TYPE2 max = GDK_##TYPE2##_max;				\
	const TYPE2 prec = (TYPE2) scales[precision];			\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	*reduce = true;							\
	if (ci->tpe == cand_dense) {					\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next_dense(ci) - candoff;		\
			v = src[x];					\
			if (is_##TYPE1##_nil(v)) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else if (v < (TYPE1) min || v > (TYPE1) max) { \
				CONV_OVERFLOW_PREC(TYPE1, #TYPE2, v, scale2, precision); \
			} else {					\
				ldouble m = (ldouble) v * mul;		\
				dst[i] = (TYPE2) rounddbl(m);		\
				if (is_##TYPE2##_nil(dst[i]) ||		\
				    (precision &&			\
				     (dst[i] >= prec ||			\
				      dst[i] <= -prec)))		\
					CONV_OVERFLOW_PREC(TYPE1, #TYPE2, v, scale2, precision); \
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	} else {							\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next(ci) - candoff;		\
			v = src[x];					\
			if (is_##TYPE1##_nil(v)) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else if (v < (TYPE1) min || v > (TYPE1) max) { \
				CONV_OVERFLOW_PREC(TYPE1, #TYPE2, v, scale2, precision); \
			} else {					\
				ldouble m = (ldouble) v * mul;		\
				dst[i] = (TYPE2) rounddbl(m);		\
				if (is_##TYPE2##_nil(dst[i]) ||		\
				    (precision &&			\
				     (dst[i] >= prec ||			\
				      dst[i] <= -prec)))		\
					CONV_OVERFLOW_PREC(TYPE1, #TYPE2, v, scale2, precision); \
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define convert2bit_impl(TYPE)						\
static BUN								\
convert_##TYPE##_bit(const TYPE *src, bit *restrict dst,		\
		     struct canditer *restrict ci,			\
		     oid candoff, bool *reduce)				\
{									\
	BUN i, nils = 0;						\
	oid x;								\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	*reduce = true;							\
	if (ci->tpe == cand_dense) {					\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next_dense(ci) - candoff;		\
			if (is_##TYPE##_nil(src[x])) {			\
				dst[i] = bit_nil;			\
				nils++;					\
			} else						\
				dst[i] = (bit) (src[x] != 0);		\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	} else {							\
		TIMEOUT_LOOP_IDX(i, ci->ncand, timeoffset) {		\
			x = canditer_next(ci) - candoff;		\
			if (is_##TYPE##_nil(src[x])) {			\
				dst[i] = bit_nil;			\
				nils++;					\
			} else						\
				dst[i] = (bit) (src[x] != 0);		\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

#define convertimpl_msk(TYPE)						\
static BUN								\
convert_##TYPE##_msk(const TYPE *src, uint32_t *restrict dst,		\
		     struct canditer *restrict ci,			\
		     oid candoff, bool *reduce)				\
{									\
	BUN cnt = ci->ncand / 32;					\
	BUN i, j;							\
	uint32_t mask;							\
	oid x;								\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	*reduce = true;							\
	if (ci->tpe == cand_dense) {					\
		TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {			\
			mask = 0;					\
			for (j = 0; j < 32; j++) {			\
				x = canditer_next_dense(ci) - candoff;	\
				mask |= (uint32_t) (!is_##TYPE##_nil(src[x]) && src[x] != 0) << j; \
			}						\
			dst[i] = mask;					\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
		cnt = ci->ncand % 32;					\
		if (cnt > 0) {						\
			mask = 0;					\
			for (j = 0; j < cnt; j++) {			\
				x = canditer_next_dense(ci) - candoff;	\
				mask |= (uint32_t) (!is_##TYPE##_nil(src[x]) && src[x] != 0) << j; \
			}						\
			dst[i] = mask;					\
		}							\
	} else {							\
		TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {			\
			mask = 0;					\
			for (j = 0; j < 32; j++) {			\
				x = canditer_next(ci) - candoff;	\
				mask |= (uint32_t) (!is_##TYPE##_nil(src[x]) && src[x] != 0) << j; \
			}						\
			dst[i] = mask;					\
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
		cnt = ci->ncand % 32;					\
		if (cnt > 0) {						\
			mask = 0;					\
			for (j = 0; j < cnt; j++) {			\
				x = canditer_next(ci) - candoff;	\
				mask |= (uint32_t) (!is_##TYPE##_nil(src[x]) && src[x] != 0) << j; \
			}						\
			dst[i] = mask;					\
		}							\
	}								\
	return 0;							\
}									\
									\
static BUN								\
convert_msk_##TYPE(const uint32_t *src, TYPE *restrict dst,		\
		   struct canditer *restrict ci,			\
		   oid candoff, bool *reduce)				\
{									\
	BUN nils = 0;							\
	BUN k;								\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
									\
	*reduce = false;						\
	if (ci->tpe == cand_dense) {					\
		uint32_t mask;						\
		BUN i = (ci->seq - candoff) / 32;			\
		BUN cnt = (ci->seq + ci->ncand - candoff) / 32;		\
		BUN first = (ci->seq - candoff) % 32;			\
		BUN rem = (ci->seq + ci->ncand - candoff) % 32;		\
		BUN j;							\
		k = 0;							\
		for (; i < cnt; i++) {					\
			mask = src[i];					\
			for (j = first; j < 32; j++) {			\
				dst[k] = (TYPE) ((mask & (1U << j)) != 0); \
				k++;					\
			}						\
			first = 0;					\
		}							\
		if (rem > first) {					\
			mask = src[i];					\
			for (j = first; j < rem; j++) {			\
				dst[k] = (TYPE) ((mask & (1U << j)) != 0); \
				k++;					\
			}						\
		}							\
	} else {							\
		TIMEOUT_LOOP_IDX(k, ci->ncand, timeoffset) {		\
			oid x = canditer_next(ci) - candoff;		\
			dst[k] = (TYPE) ((src[x / 32] & (1U << (x % 32))) != 0); \
		}							\
		TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));	\
	}								\
	return nils;							\
}

convertimpl(bte, bte)
convertimpl(bte, sht)
convertimpl(bte, int)
convertimpl_oid_enlarge(bte)
convertimpl(bte, lng)
#ifdef HAVE_HGE
convertimpl(bte, hge)
#endif
convertimpl_enlarge_float(bte, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(bte, dbl, DBL_MANT_DIG)

convertimpl(sht, bte)
convertimpl(sht, sht)
convertimpl(sht, int)
convertimpl_oid_enlarge(sht)
convertimpl(sht, lng)
#ifdef HAVE_HGE
convertimpl(sht, hge)
#endif
convertimpl_enlarge_float(sht, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(sht, dbl, DBL_MANT_DIG)

convertimpl(int, bte)
convertimpl(int, sht)
convertimpl(int, int)
convertimpl_oid_enlarge(int)
convertimpl(int, lng)
#ifdef HAVE_HGE
convertimpl(int, hge)
#endif
convertimpl_enlarge_float(int, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(int, dbl, DBL_MANT_DIG)

convertimpl(lng, bte)
convertimpl(lng, sht)
convertimpl(lng, int)
#if SIZEOF_OID == SIZEOF_LNG
convertimpl_oid_enlarge(lng)
#else
convertimpl_oid_reduce(lng)
#endif
convertimpl(lng, lng)
#ifdef HAVE_HGE
convertimpl(lng, hge)
#endif
convertimpl_enlarge_float(lng, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(lng, dbl, DBL_MANT_DIG)

#ifdef HAVE_HGE
convertimpl(hge, bte)
convertimpl(hge, sht)
convertimpl(hge, int)
convertimpl_oid_reduce(hge)
convertimpl(hge, lng)
convertimpl(hge, hge)
convertimpl_enlarge_float(hge, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(hge, dbl, DBL_MANT_DIG)
#endif

convertimpl_reduce_float(flt, bte)
convertimpl_reduce_float(flt, sht)
convertimpl_reduce_float(flt, int)
convertimpl_oid_reduce(flt)
convertimpl_reduce_float(flt, lng)
#ifdef HAVE_HGE
convertimpl_reduce_float(flt, hge)
#endif
convertimpl_enlarge_float(flt, flt, 128)
convertimpl_enlarge_float(flt, dbl, DBL_MANT_DIG)

convertimpl_reduce_float(dbl, bte)
convertimpl_reduce_float(dbl, sht)
convertimpl_reduce_float(dbl, int)
convertimpl_oid_reduce(dbl)
convertimpl_reduce_float(dbl, lng)
#ifdef HAVE_HGE
convertimpl_reduce_float(dbl, hge)
#endif
#undef rounddbl
/* no rounding here */
#define rounddbl(x)	(x)
convertimpl_reduce_float(dbl, flt)
convertimpl_enlarge_float(dbl, dbl, 128)

convert2bit_impl(bte)
convert2bit_impl(sht)
convert2bit_impl(int)
convert2bit_impl(lng)
#ifdef HAVE_HGE
convert2bit_impl(hge)
#endif
convert2bit_impl(flt)
convert2bit_impl(dbl)

convertimpl_msk(bte)
convertimpl_msk(sht)
convertimpl_msk(int)
convertimpl_msk(lng)
#ifdef HAVE_HGE
convertimpl_msk(hge)
#endif
convertimpl_msk(flt)
convertimpl_msk(dbl)

static BUN
convert_any_str(BATiter *bi, BAT *bn, struct canditer *restrict ci)
{
	int tp = bi->type;
	oid candoff = bi->b->hseqbase;
	str dst = 0;
	size_t len = 0;
	BUN nils = 0;
	BUN i;
	const void *nil = ATOMnilptr(tp);
	const void *restrict src;
	ssize_t (*atomtostr)(str *, size_t *, const void *, bool) = BATatoms[tp].atomToStr;
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tp);
	oid x;

	if (atomtostr == BATatoms[TYPE_str].atomToStr) {
		/* compatible with str, we just copy the value */
		assert(bi->type != TYPE_void);
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			src = BUNtvar(*bi, x);
			if (strNil(src))
				nils++;
			if (tfastins_nocheckVAR(bn, i, src) != GDK_SUCCEED) {
				goto bailout;
			}
		}
	} else if (bi->b->tvheap) {
		assert(bi->type != TYPE_void);
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			src = BUNtvar(*bi, x);
			if ((*atomcmp)(src, nil) == 0) {
				nils++;
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					goto bailout;
				}
			} else {
				if ((*atomtostr)(&dst, &len, src, false) < 0 ||
				    tfastins_nocheckVAR(bn, i, dst) != GDK_SUCCEED) {
					goto bailout;
				}
			}
		}
	} else if (ATOMstorage(bi->type) == TYPE_msk) {
		CAND_LOOP_IDX(ci, i) {
			const char *v;
			x = canditer_next(ci) - candoff;
			v = Tmskval(bi, x) ? "1" : "0";
			if (tfastins_nocheckVAR(bn, i, v) != GDK_SUCCEED)
				goto bailout;
		}
	} else {
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			src = BUNtloc(*bi, x);
			if ((*atomcmp)(src, nil) == 0) {
				nils++;
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED)
					goto bailout;
			} else {
				if ((*atomtostr)(&dst, &len, src, false) < 0)
					goto bailout;
				if (tfastins_nocheckVAR(bn, i, dst) != GDK_SUCCEED)
					goto bailout;
			}
		}
	}
	BATsetcount(bn, ci->ncand);
	GDKfree(dst);
	return nils;
  bailout:
	GDKfree(dst);
	return BUN_NONE + 2;
}

static BUN
convert_str_var(BATiter *bi, BAT *bn, struct canditer *restrict ci)
{
	int tp = bn->ttype;
	oid candoff = bi->b->hseqbase;
	void *dst = 0;
	size_t len = 0;
	BUN nils = 0;
	BUN i;
	const void *nil = ATOMnilptr(tp);
	const char *restrict src;
	ssize_t (*atomfromstr)(const char *, size_t *, ptr *, bool) = BATatoms[tp].atomFromStr;
	oid x;

	CAND_LOOP_IDX(ci, i) {
		x = canditer_next(ci) - candoff;
		src = BUNtvar(*bi, x);
		if (strNil(src)) {
			nils++;
			if (tfastins_nocheckVAR(bn, i, nil) != GDK_SUCCEED) {
				goto bailout;
			}
		} else {
			ssize_t l;
			if ((l = (*atomfromstr)(src, &len, &dst, false)) < 0 ||
			    l < (ssize_t) strlen(src) ||
			    tfastins_nocheckVAR(bn, i, dst) != GDK_SUCCEED) {
				goto bailout;
			}
		}
	}
	BATsetcount(bn, ci->ncand);
	GDKfree(dst);
	return nils;
  bailout:
	GDKfree(dst);
	return BUN_NONE + 2;
}

static BUN
convert_str_fix(BATiter *bi, int tp, void *restrict dst,
		struct canditer *restrict ci, oid candoff)
{
	BUN nils = 0;
	const void *nil = ATOMnilptr(tp);
	size_t len = ATOMsize(tp);
	ssize_t l;
	ssize_t (*atomfromstr)(const char *, size_t *, ptr *, bool) = BATatoms[tp].atomFromStr;
	const char *s = NULL;

	if (ATOMstorage(tp) == TYPE_msk) {
		uint32_t mask = 0;
		uint32_t *d = dst;
		int j = 0;
		CAND_LOOP(ci) {
			oid x = canditer_next(ci) - candoff;
			uint32_t v;
			s = BUNtvar(*bi, x);
			if (strcmp(s, "0") == 0)
				v = 0;
			else if (strcmp(s, "1") == 0)
				v = 1;
			else
				goto conversion_failed;
			mask |= v << j;
			if (++j == 32) {
				*d++ = mask;
				j = 0;
				mask = 0;
			}
		}
		if (j > 0)
			*d = mask;
		return 0;
	}

	CAND_LOOP(ci) {
		oid x = canditer_next(ci) - candoff;
		const char *s = BUNtvar(*bi, x);
		if (strNil(s)) {
			memcpy(dst, nil, len);
			nils++;
		} else {
			void *d = dst;
			if ((l = (*atomfromstr)(s, &len, &d, false)) < 0 ||
			    l < (ssize_t) strlen(s)) {
				goto conversion_failed;
			}
			assert(len == ATOMsize(tp));
			if (ATOMcmp(tp, dst, nil) == 0)
				nils++;
		}
		dst = (void *) ((char *) dst + len);
	}
	return nils;

  conversion_failed:
	GDKclrerr();
	size_t sz = 0;
	char *bf = NULL;

	if (s) {
		sz = escapedStrlen(s, NULL, NULL, '\'');
		bf = GDKmalloc(sz + 1);
	}
	if (bf) {
		escapedStr(bf, s, sz + 1, NULL, NULL, '\'');
		GDKerror("22018!conversion of string "
			 "'%s' to type %s failed.\n",
			 bf, ATOMname(tp));
		GDKfree(bf);
	} else {
		GDKerror("22018!conversion of string "
			 "to type %s failed.\n",
			 ATOMname(tp));
	}
	return BUN_NONE;
}

static BUN
convert_void_any(oid seq, BAT *bn,
		 struct canditer *restrict ci,
		 oid candoff, bool *reduce)
{
	BUN nils = 0;
	BUN i;
	int tp = bn->ttype;
	void *restrict dst = Tloc(bn, 0);
	ssize_t (*atomtostr)(str *, size_t *, const void *, bool) = BATatoms[TYPE_oid].atomToStr;
	char *s = NULL;
	size_t len = 0;
	oid x;

	*reduce = false;
	assert(!is_oid_nil(seq));

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			if (ci->ncand > 0) {
				x = canditer_next(ci) - candoff;
				((bit *) dst)[0] = x + seq != 0;
			}
			CAND_LOOP_IDX(ci, i) {
				((bit *) dst)[i] = 1;
			}
		} else {
			CAND_LOOP_IDX(ci, i) {
				x = canditer_next(ci) - candoff;
				if (seq + x > GDK_bte_max) {
					CONV_OVERFLOW(oid, "bte", seq + x);
				} else {
					((bte *) dst)[i] = (bte) (seq + x);
				}
			}
		}
		break;
	case TYPE_sht:
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			if (seq + x > GDK_sht_max) {
				CONV_OVERFLOW(oid, "sht", seq + x);
			} else {
				((sht *) dst)[i] = (sht) (seq + x);
			}
		}
		break;
	case TYPE_int:
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
#if SIZEOF_OID > SIZEOF_INT
			if (seq + x > GDK_int_max) {
				CONV_OVERFLOW(oid, "int", seq + x);
			} else
#endif
				((int *) dst)[i] = (int) (seq + x);
		}
		break;
	case TYPE_lng:
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			((lng *) dst)[i] = (lng) (seq + x);
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			((hge *) dst)[i] = (hge) (seq + x);
		}
		break;
#endif
	case TYPE_flt:
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			((flt *) dst)[i] = (flt) (seq + x);
		}
		break;
	case TYPE_dbl:
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			((dbl *) dst)[i] = (dbl) (seq + x);
		}
		break;
	case TYPE_str:
		CAND_LOOP_IDX(ci, i) {
			x = canditer_next(ci) - candoff;
			if ((*atomtostr)(&s, &len, &(oid){seq + x}, false) < 0)
				goto bailout;
			if (tfastins_nocheckVAR(bn, i, s) != GDK_SUCCEED)
				goto bailout;
		}
		GDKfree(s);
		s = NULL;
		break;
	default:
		return BUN_NONE + 1;
	}

	bn->theap->dirty = true;
	return nils;

  bailout:
	GDKfree(s);
	return BUN_NONE + 2;
}

static BUN
convert_typeswitchloop(const void *src, int stp, void *restrict dst, int dtp,
		       struct canditer *restrict ci,
		       oid candoff, bool *reduce,
		       uint8_t scale1, uint8_t scale2, uint8_t precision)
{
	assert(scale1 < (uint8_t) (sizeof(scales) / sizeof(scales[0])));
	assert(scale2 < (uint8_t) (sizeof(scales) / sizeof(scales[0])));
	switch (ATOMbasetype(stp)) {
	case TYPE_msk:
		switch (ATOMbasetype(dtp)) {
		/* case TYPE_msk not needed: it is done with the help
		 * of BATappend */
		case TYPE_bte:
			return convert_bte_msk(src, dst, ci, candoff,
					       reduce);
		case TYPE_sht:
			return convert_sht_msk(src, dst, ci, candoff,
					       reduce);
		case TYPE_int:
			return convert_int_msk(src, dst, ci, candoff,
					       reduce);
		case TYPE_lng:
			return convert_lng_msk(src, dst, ci, candoff,
					       reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_hge_msk(src, dst, ci, candoff,
					       reduce);
#endif
		case TYPE_flt:
			return convert_flt_msk(src, dst, ci, candoff,
					       reduce);
		case TYPE_dbl:
			return convert_dbl_msk(src, dst, ci, candoff,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_bte:
		switch (ATOMbasetype(dtp)) {
		case TYPE_msk:
			return convert_msk_bte(src, dst, ci, candoff,
					       reduce);
		case TYPE_bte:
			if (dtp == TYPE_bit)
				return convert_bte_bit(src, dst, ci,
						       candoff, reduce);
			return convert_bte_bte(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_sht:
			return convert_bte_sht(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_bte_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_bte_int(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_bte_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_bte_lng(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_bte_hge(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
#endif
		case TYPE_flt:
			return convert_bte_flt(src, dst, ci, candoff,
					       scale1,
					       reduce);
		case TYPE_dbl:
			return convert_bte_dbl(src, dst, ci, candoff,
					       scale1,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_sht:
		switch (ATOMbasetype(dtp)) {
		case TYPE_msk:
			return convert_msk_sht(src, dst, ci, candoff,
					       reduce);
		case TYPE_bte:
			if (dtp == TYPE_bit)
				return convert_sht_bit(src, dst, ci,
						       candoff, reduce);
			return convert_sht_bte(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_sht:
			return convert_sht_sht(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_sht_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_sht_int(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_sht_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_sht_lng(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_sht_hge(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
#endif
		case TYPE_flt:
			return convert_sht_flt(src, dst, ci, candoff,
					       scale1,
					       reduce);
		case TYPE_dbl:
			return convert_sht_dbl(src, dst, ci, candoff,
					       scale1,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_int:
		switch (ATOMbasetype(dtp)) {
		case TYPE_msk:
			return convert_msk_int(src, dst, ci, candoff,
					       reduce);
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_int_bit(src, dst, ci,
						       candoff, reduce);
			}
			return convert_int_bte(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_sht:
			return convert_int_sht(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_int_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_int_int(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_int_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_int_lng(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_int_hge(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
#endif
		case TYPE_flt:
			return convert_int_flt(src, dst, ci, candoff,
					       scale1,
					       reduce);
		case TYPE_dbl:
			return convert_int_dbl(src, dst, ci, candoff,
					       scale1,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_lng:
		switch (ATOMbasetype(dtp)) {
		case TYPE_msk:
			return convert_msk_lng(src, dst, ci, candoff,
					       reduce);
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_lng_bit(src, dst, ci,
						       candoff, reduce);
			}
			return convert_lng_bte(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_sht:
			return convert_lng_sht(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_lng_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_lng_int(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_lng_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_lng_lng(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_lng_hge(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
#endif
		case TYPE_flt:
			return convert_lng_flt(src, dst, ci, candoff,
					       scale1,
					       reduce);
		case TYPE_dbl:
			return convert_lng_dbl(src, dst, ci, candoff,
					       scale1,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (ATOMbasetype(dtp)) {
		case TYPE_msk:
			return convert_msk_hge(src, dst, ci, candoff,
					       reduce);
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_hge_bit(src, dst, ci,
						       candoff, reduce);
			}
			return convert_hge_bte(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_sht:
			return convert_hge_sht(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_int:
			return convert_hge_int(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_lng:
			return convert_hge_lng(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_hge:
			return convert_hge_hge(src, dst, ci, candoff,
					       scale1,
					       scale2,
					       precision, reduce);
		case TYPE_oid:
			return convert_hge_oid(src, dst, ci, candoff, reduce);
		case TYPE_flt:
			return convert_hge_flt(src, dst, ci, candoff,
					       scale1,
					       reduce);
		case TYPE_dbl:
			return convert_hge_dbl(src, dst, ci, candoff,
					       scale1,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
#endif
	case TYPE_flt:
		switch (ATOMbasetype(dtp)) {
		case TYPE_msk:
			return convert_msk_flt(src, dst, ci, candoff,
					       reduce);
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_flt_bit(src, dst, ci,
						       candoff, reduce);
			}
			return convert_flt_bte(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
		case TYPE_sht:
			return convert_flt_sht(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_flt_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_flt_int(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_flt_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_flt_lng(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_flt_hge(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
#endif
		case TYPE_flt:
			return convert_flt_flt(src, dst, ci, candoff,
					       0,
					       reduce);
		case TYPE_dbl:
			return convert_flt_dbl(src, dst, ci, candoff,
					       0,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_dbl:
		switch (ATOMbasetype(dtp)) {
		case TYPE_msk:
			return convert_msk_dbl(src, dst, ci, candoff,
					       reduce);
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_dbl_bit(src, dst, ci,
						       candoff, reduce);
			}
			return convert_dbl_bte(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
		case TYPE_sht:
			return convert_dbl_sht(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_dbl_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_dbl_int(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_dbl_oid(src, dst, ci,
						       candoff,
						       reduce);
#endif
			return convert_dbl_lng(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_dbl_hge(src, dst, ci, candoff,
					       scale2,
					       precision, reduce);
#endif
		case TYPE_flt:
			return convert_dbl_flt(src, dst, ci, candoff,
					       0, 0, reduce);
		case TYPE_dbl:
			return convert_dbl_dbl(src, dst, ci, candoff,
					       0,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	default:
		return BUN_NONE + 1;
	}
}

BAT *
BATconvert(BAT *b, BAT *s, int tp,
	   uint8_t scale1, uint8_t scale2, uint8_t precision)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils = 0;	/* in case no conversion defined */
	struct canditer ci;
	BUN cnt;
	/* set reduce to true if there are (potentially) multiple
	 * (different) source values that map to the same destination
	 * value */
	bool reduce = false;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	if (tp == TYPE_void)
		tp = TYPE_oid;

	BATiter bi = bat_iterator(b);
	cnt = BATcount(b);
	canditer_init(&ci, b, s);
	if (ci.ncand == 0 || (bi.type == TYPE_void && is_oid_nil(b->tseqbase))) {
		bat_iterator_end(&bi);
		return BATconstant(ci.hseq, tp,
				   ATOMnilptr(tp), ci.ncand, TRANSIENT);
	}

	if (cnt == ci.ncand && tp != TYPE_bit &&
	    ATOMbasetype(bi.type) == ATOMbasetype(tp) &&
	    (tp != TYPE_oid || bi.type == TYPE_oid) &&
	    scale1 == 0 && scale2 == 0 && precision == 0 &&
	    (tp != TYPE_str ||
	     BATatoms[bi.type].atomToStr == BATatoms[TYPE_str].atomToStr)) {
		bn = COLcopy(b, tp, false, TRANSIENT);
		if (bn && s)
			bn->hseqbase = s->hseqbase;
		bat_iterator_end(&bi);
		return bn;
	}
	if (ATOMstorage(tp) == TYPE_ptr) {
		GDKerror("type combination (convert(%s)->%s) "
			 "not supported.\n",
			 ATOMname(bi.type), ATOMname(tp));
		bat_iterator_end(&bi);
		return NULL;
	}
	if (ATOMstorage(tp) == TYPE_msk) {
		if (BATtdensebi(&bi)) {
			/* dense to msk is easy: all values 1, except
			 * maybe the first */
			bn = BATconstant(ci.hseq, tp, &(msk){1}, ci.ncand,
					 TRANSIENT);
			if (bn && b->tseqbase == 0)
				mskClr(bn, 0);
			bat_iterator_end(&bi);
			return bn;
		} else if (bi.type == TYPE_void) {
			/* void-nil to msk is easy: all values 0 */
			bn = BATconstant(ci.hseq, tp, &(msk){0}, ci.ncand,
					 TRANSIENT);
			bat_iterator_end(&bi);
			return bn;
		}
	}

	bn = COLnew(ci.hseq, tp, ci.ncand, TRANSIENT);
	if (bn == NULL) {
		bat_iterator_end(&bi);
		return NULL;
	}

	if (bi.type == TYPE_void)
		nils = convert_void_any(b->tseqbase, bn,
					&ci, b->hseqbase, &reduce);
	else if (tp == TYPE_str)
		nils = convert_any_str(&bi, bn, &ci);
	else if (bi.type == TYPE_str) {
		reduce = true;
		if (ATOMvarsized(tp)) {
			nils = convert_str_var(&bi, bn, &ci);
		} else {
			nils = convert_str_fix(&bi, tp, Tloc(bn, 0),
					       &ci, b->hseqbase);
		}
	} else if (ATOMstorage(bi.type) == TYPE_msk &&
		   ATOMstorage(tp) == TYPE_msk) {
		if (BATappend(bn, b, s, false) != GDK_SUCCEED)
			nils = BUN_NONE + 2;
	} else {
		nils = convert_typeswitchloop(bi.base, bi.type,
					      Tloc(bn, 0), tp,
					      &ci, b->hseqbase, &reduce,
					      scale1, scale2, precision);
	}

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		if (nils == BUN_NONE + 1) {
			GDKerror("type combination (convert(%s)->%s) "
				 "not supported.\n",
				 ATOMname(bi.type), ATOMname(tp));
		} else if (nils == BUN_NONE + 2) {
			GDKerror("could not insert value into BAT.\n");
		}
		bat_iterator_end(&bi);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	if ((bn->ttype != TYPE_str && bn->ttype != TYPE_bit && bi.type != TYPE_str) ||
	    BATcount(bn) < 2) {
		bn->tsorted = nils == 0 && bi.sorted;
		bn->trevsorted = nils == 0 && bi.revsorted;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
	}
	if (!reduce || BATcount(bn) < 2)
		bn->tkey = bi.key && nils <= 1;
	else
		bn->tkey = false;

	bat_iterator_end(&bi);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

gdk_return
VARconvert(ValPtr ret, const ValRecord *v,
	   uint8_t scale1, uint8_t scale2, uint8_t precision)
{
	ptr p;
	BUN nils = 0;
	bool reduce;

	if (ret->vtype == TYPE_msk) {
		ValRecord tmp;
		tmp.vtype = TYPE_bit;
		if (VARconvert(&tmp, v, scale1, scale2, precision) != GDK_SUCCEED)
			return GDK_FAIL;
		if (is_bte_nil(tmp.val.btval)) {
			GDKerror("22003!cannot convert nil to msk.\n");
			nils = BUN_NONE;
		}
		ret->val.mval = tmp.val.btval;
		ret->len = ATOMsize(TYPE_msk);
	} else if (v->vtype == TYPE_msk) {
		ValRecord tmp;
		tmp.vtype = TYPE_bit;
		tmp.val.btval = v->val.mval;
		if (VARconvert(ret, &tmp, scale1, scale2, precision) != GDK_SUCCEED)
			return GDK_FAIL;
	} else if (ret->vtype == TYPE_str) {
		if (v->vtype == TYPE_void ||
		    (*ATOMcompare(v->vtype))(VALptr(v),
					     ATOMnilptr(v->vtype)) == 0) {
			if (VALinit(ret, TYPE_str, str_nil) == NULL)
				return GDK_FAIL;
		} else if (BATatoms[v->vtype].atomToStr == BATatoms[TYPE_str].atomToStr) {
			if (VALinit(ret, TYPE_str, v->val.sval) == NULL)
				return GDK_FAIL;
		} else {
			ret->len = 0;
			ret->val.sval = NULL;
			if ((*BATatoms[v->vtype].atomToStr)(&ret->val.sval,
							    &ret->len,
							    VALptr(v),
							    false) < 0) {
				GDKfree(ret->val.sval);
				ret->val.sval = NULL;
				ret->len = 0;
				return GDK_FAIL;
			}
		}
	} else if (ret->vtype == TYPE_void) {
		if (ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0) {
			GDKerror("22003!cannot convert non-nil to void.\n");
			return GDK_FAIL;
		}
		ret->val.oval = oid_nil;
		ret->len = ATOMsize(TYPE_void);
	} else if (v->vtype == TYPE_void) {
		if (VALinit(ret, ret->vtype, ATOMnilptr(ret->vtype)) == NULL)
			return GDK_FAIL;
	} else if (v->vtype == TYPE_str) {
		if (strNil(v->val.sval)) {
			if (VALinit(ret, ret->vtype, ATOMnilptr(ret->vtype)) == NULL)
				return GDK_FAIL;
		} else if (ATOMstorage(ret->vtype) == TYPE_ptr) {
			nils = BUN_NONE + 1;
		} else {
			ssize_t l;
			size_t len;

			if (ATOMextern(ret->vtype)) {
				/* let atomFromStr allocate memory
				 * which we later give away to ret */
				p = NULL;
				len = 0;
			} else {
				/* use the space provided by ret */
				p = VALget(ret);
				len = ATOMsize(ret->vtype);
			}
			if ((l = (*BATatoms[ret->vtype].atomFromStr)(
				     v->val.sval, &len, &p, false)) < 0 ||
			    l < (ssize_t) strlen(v->val.sval)) {
				if (ATOMextern(ret->vtype))
					GDKfree(p);
				GDKclrerr();
				size_t sz = escapedStrlen(v->val.sval, NULL, NULL, '\'');
				char *bf = GDKmalloc(sz + 1);
				if (bf) {
					escapedStr(bf, v->val.sval, sz + 1, NULL, NULL, '\'');
					GDKerror("22018!conversion of string "
						 "'%s' to type %s failed.\n",
						 bf, ATOMname(ret->vtype));
					GDKfree(bf);
				} else {
					GDKerror("22018!conversion of string "
						 "to type %s failed.\n",
						 ATOMname(ret->vtype));
				}
				return GDK_FAIL;
			} else {
				/* now give value obtained to ret */
				assert(ATOMextern(ret->vtype) ||
				       p == VALget(ret));
				ret->len = (int) len;
				if (ATOMextern(ret->vtype))
					VALset(ret, ret->vtype, p);
			}
		}
	} else {
		nils = convert_typeswitchloop(VALptr(v), v->vtype,
					      VALget(ret), ret->vtype,
					      &(struct canditer){.tpe=cand_dense, .ncand=1},
					      0, &reduce,
					      scale1, scale2, precision);
		if (nils < BUN_NONE)
			ret->len = ATOMlen(ret->vtype, VALptr(ret));
	}
	if (nils == BUN_NONE + 1) {
		GDKerror("conversion from type %s to type %s "
			 "unsupported.\n",
			 ATOMname(v->vtype), ATOMname(ret->vtype));
		return GDK_FAIL;
	}
	return nils == BUN_NONE ? GDK_FAIL : GDK_SUCCEED;
}
