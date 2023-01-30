/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/* This file contains shared definitions for gdk_calc.c and gdk_aggr.c */

#ifndef LIBGDK
#error this file should not be included outside its source directory
#endif

/* signed version of BUN */
#if SIZEOF_BUN == SIZEOF_INT
#define SBUN	int
#else
#define SBUN	lng
#endif

#ifdef ABSOLUTE
/* Windows seems to define this somewhere */
#undef ABSOLUTE
#endif
#define ABSOLUTE(x)	((x) < 0 ? -(x) : (x))

#define LT(a, b)	((bit) ((a) < (b)))

#define GT(a, b)	((bit) ((a) > (b)))

#include "gdk_cand.h"

#ifdef HAVE___BUILTIN_ADD_OVERFLOW
#define OP_WITH_CHECK(lft, rgt, dst, op, nil, max, on_overflow)		\
	do {								\
		if (__builtin_##op##_overflow(lft, rgt, &(dst)) ||	\
		    (dst) < -(max) || (dst) > (max)) {			\
			on_overflow;					\
		}							\
	} while (0)
#endif	/* HAVE___BUILTIN_ADD_OVERFLOW */

/* dst = lft + rgt with overflow check */

/* generic version */
#define ADD_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)	\
	do {							\
		if ((rgt) < 1) {				\
			if (-(max) - (rgt) > (lft)) {		\
				on_overflow;			\
			} else {				\
				(dst) = (TYPE3) (lft) + (rgt);	\
			}					\
		} else {					\
			if ((max) - (rgt) < (lft)) {		\
				on_overflow;			\
			} else {				\
				(dst) = (TYPE3) (lft) + (rgt);	\
			}					\
		}						\
	} while (0)

#ifdef HAVE___BUILTIN_ADD_OVERFLOW
/* integer version using Gnu CC builtin function for overflow check */
#define ADDI_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)		\
	OP_WITH_CHECK(lft, rgt, dst, add, TYPE3##_nil, max, on_overflow)
#else
/* integer version using generic version */
#define ADDI_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow) \
	ADD_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)
#endif	/* HAVE___BUILTIN_ADD_OVERFLOW */

/* floating point version using generic version */
#define ADDF_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow) \
	ADD_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)

/* dst = lft - rgt with overflow check */

/* generic version */
#define SUB_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)	\
	do {							\
		if ((rgt) < 1) {				\
			if ((max) + (rgt) < (lft)) {		\
				on_overflow;			\
			} else {				\
				(dst) = (TYPE3) (lft) - (rgt);	\
			}					\
		} else {					\
			if (-(max) + (rgt) > (lft)) {		\
				on_overflow;			\
			} else {				\
				(dst) = (TYPE3) (lft) - (rgt);	\
			}					\
		}						\
	} while (0)

#ifdef HAVE___BUILTIN_ADD_OVERFLOW
/* integer version using Gnu CC builtin function for overflow check */
#define SUBI_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)		\
	OP_WITH_CHECK(lft, rgt, dst, sub, TYPE3##_nil, max, on_overflow)
#else
/* integer version using generic version */
#define SUBI_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow) \
	SUB_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)
#endif	/* HAVE___BUILTIN_ADD_OVERFLOW */

/* floating point version using generic version */
#define SUBF_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow) \
	SUB_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)

/* dst = lft * rgt with overflow check */

/* generic version */
#define MUL4_WITH_CHECK(lft, rgt, TYPE3, dst, max, TYPE4, on_overflow)	\
	do {								\
		TYPE4 c = (TYPE4) (lft) * (rgt);			\
		if (c < (TYPE4) -(max) ||				\
		    c > (TYPE4) (max)) {				\
			on_overflow;					\
		} else {						\
			(dst) = (TYPE3) c;				\
		}							\
	} while (0)

#ifdef HAVE___BUILTIN_ADD_OVERFLOW
/* integer version using Gnu CC builtin function for overflow check */
#define MULI4_WITH_CHECK(lft, rgt, TYPE3, dst, max, TYPE4, on_overflow) \
	OP_WITH_CHECK(lft, rgt, dst, mul, TYPE3##_nil, max, on_overflow)
#define LNGMUL_CHECK(lft, rgt, dst, max, on_overflow)			\
	OP_WITH_CHECK(lft, rgt, dst, mul, lng_nil, max, on_overflow)
#else
/* integer version using generic version */
#define MULI4_WITH_CHECK(lft, rgt, TYPE3, dst, max, TYPE4, on_overflow) \
	MUL4_WITH_CHECK(lft, rgt, TYPE3, dst, max, TYPE4, on_overflow)
#ifdef HAVE_HGE
#define LNGMUL_CHECK(lft, rgt, dst, max, on_overflow)			\
	MULI4_WITH_CHECK(lft, rgt, lng, dst, max, hge, on_overflow)
#elif defined(HAVE___INT128)
#define LNGMUL_CHECK(lft, rgt, dst, max, on_overflow)			\
	MULI4_WITH_CHECK(lft, rgt, lng, dst, max, __int128, on_overflow)
#elif defined(_MSC_VER) && defined(_M_AMD64) && !defined(__INTEL_COMPILER)
#include <intrin.h>
#pragma intrinsic(_mul128)
#define LNGMUL_CHECK(lft, rgt, dst, max, on_overflow)			\
	do {								\
		__int64 clo, chi;					\
		clo = _mul128((__int64) (lft), (__int64) (rgt), &chi);	\
		if ((chi == 0 && clo >= 0 && clo <= (max)) ||		\
		    (chi == -1 && clo < 0 && clo >= -(max))) {		\
			(dst) = (lng) clo;				\
		} else {						\
			on_overflow;					\
		}							\
	} while (0)
#else
#define LNGMUL_CHECK(lft, rgt, dst, max, on_overflow)			\
	do {								\
		lng a = (lft), b = (rgt);				\
		unsigned int a1, a2, b1, b2;				\
		ulng c;							\
		int sign = 1;						\
									\
		if (a < 0) {						\
			sign = -sign;					\
			a = -a;						\
		}							\
		if (b < 0) {						\
			sign = -sign;					\
			b = -b;						\
		}							\
		a1 = (unsigned int) (a >> 32);				\
		a2 = (unsigned int) a;					\
		b1 = (unsigned int) (b >> 32);				\
		b2 = (unsigned int) b;					\
		/* result = (a1*b1<<64) + (a1*b2+a2*b1<<32) + a2*b2 */	\
		if ((a1 == 0 || b1 == 0) &&				\
		    ((c = (ulng) a1 * b2 + (ulng) a2 * b1) & (~(ulng)0 << 31)) == 0 && \
		    (((c = (c << 32) + (ulng) a2 * b2) & ((ulng) 1 << 63)) == 0 && \
		     (c) <= (ulng) (max))) {				\
			(dst) = sign * (lng) c;				\
		} else {						\
			on_overflow;					\
		}							\
	} while (0)
#endif	/* HAVE_HGE */
#endif
#define MULF4_WITH_CHECK(lft, rgt, TYPE3, dst, max, TYPE4, on_overflow) \
	MUL4_WITH_CHECK(lft, rgt, TYPE3, dst, max, TYPE4, on_overflow)

#ifdef HAVE_HGE
#ifdef HAVE___BUILTIN_ADD_OVERFLOW
#define HGEMUL_CHECK(lft, rgt, dst, max, on_overflow)			\
	OP_WITH_CHECK(lft, rgt, dst, mul, hge_nil, max, on_overflow)
#else
#define HGEMUL_CHECK(lft, rgt, dst, max, on_overflow)			\
	do {								\
		hge a = (lft), b = (rgt);				\
		ulng a1, a2, b1, b2;					\
		uhge c;							\
		int sign = 1;						\
									\
		if (a < 0) {						\
			sign = -sign;					\
			a = -a;						\
		}							\
		if (b < 0) {						\
			sign = -sign;					\
			b = -b;						\
		}							\
		a1 = (ulng) (a >> 64);					\
		a2 = (ulng) a;						\
		b1 = (ulng) (b >> 64);					\
		b2 = (ulng) b;						\
		/* result = (a1*b1<<128) + ((a1*b2+a2*b1)<<64) + a2*b2 */ \
		if ((a1 == 0 || b1 == 0) &&				\
		    ((c = (uhge) a1 * b2 + (uhge) a2 * b1) & (~(uhge)0 << 63)) == 0 && \
		    (((c = (c << 64) + (uhge) a2 * b2) & ((uhge) 1 << 127)) == 0) && \
		    (c) <= (uhge) (max)) {				\
			(dst) = sign * (hge) c;				\
		} else {						\
			on_overflow;					\
		}							\
	} while (0)
#endif	/* HAVE___BUILTIN_ADD_OVERFLOW */
#endif	/* HAVE_HGE */

#define AVERAGE_ITER(TYPE, x, a, r, n)					\
	do {								\
		TYPE an, xn, z1;					\
		ulng z2;						\
		(n)++;							\
		/* calculate z1 = (x - a) / n, rounded down (towards */	\
		/* negative infinity), and calculate z2 = remainder */	\
		/* of the division (i.e. 0 <= z2 < n); do this */	\
		/* without causing overflow */				\
		an = (TYPE) ((a) / (n));				\
		xn = (TYPE) ((x) / (n));				\
		/* z1 will be (x - a) / n rounded towards -INF */	\
		z1 = xn - an;						\
		xn = (x) - (TYPE) (xn * (n));				\
		an = (a) - (TYPE) (an * (n));				\
		/* z2 will be remainder of above division */		\
		if (xn >= an) {						\
			z2 = (ulng) (xn - an);				\
			/* loop invariant: */				\
			/* (x - a) - z1 * n == z2 */			\
			while (z2 >= (ulng) (n)) {			\
				z2 -= (ulng) (n);			\
				z1++;					\
			}						\
		} else {						\
			z2 = (ulng) (an - xn);				\
			/* loop invariant (until we break): */		\
			/* (x - a) - z1 * n == -z2 */			\
			for (;;) {					\
				z1--;					\
				if (z2 < (ulng) (n)) {			\
					/* proper remainder */		\
					z2 = ((ulng) (n) - z2);		\
					break;				\
				}					\
				z2 -= (ulng) (n);			\
			}						\
		}							\
		(a) += z1;						\
		(r) += z2;						\
		if ((r) >= (n)) {					\
			(r) -= (n);					\
			(a)++;						\
		}							\
	} while (0)

#define AVERAGE_ITER_FLOAT(TYPE, x, a, n)			\
	do {							\
		(n)++;						\
		if (((a) > 0) == ((x) > 0)) {			\
			/* same sign */				\
			(a) += ((x) - (a)) / (n);		\
		} else {					\
			/* no overflow at the cost of an */	\
			/* extra division and slight loss of */	\
			/* precision */				\
			(a) = (a) - (a) / (n) + (x) / (n);	\
		}						\
	} while (0)

BUN dofsum(const void *restrict values, oid seqb,
		    struct canditer *restrict ci,
		    void *restrict results, BUN ngrp, int tp1, int tp2,
		    const oid *restrict gids,
		    oid min, oid max, bool skip_nils, bool nil_if_empty)
	__attribute__((__visibility__("hidden")));

/* format strings for the seven/eight basic types we deal with
 * these are only used in error messages */
#define FMTbte	"%d"
#define FMTsht	"%d"
#define FMTint	"%d"
#define FMTlng	LLFMT
#ifdef HAVE_HGE
#define FMThge	"%.40Lg (approx. value)"
#endif
#define FMTflt	"%.9g"
#define FMTdbl	"%.17g"
#define FMToid	OIDFMT

/* casts; only required for type hge, since there is no genuine format
 * string for it (i.e., for __int128) (yet?) */
#define CSTbte
#define CSTsht
#define CSTint
#define CSTlng
#ifdef HAVE_HGE
#define CSThge  (long double)
#endif
#define CSTflt
#define CSTdbl
#define CSToid

/* Most of the internal routines return a count of the number of NIL
 * values they produced.  They indicate an error by returning a value
 * >= BUN_NONE.  BUN_NONE means that the error was dealt with by
 * calling GDKerror (generally for overflow or conversion errors).
 * BUN_NONE+1 is returned by the DIV and MOD functions to indicate
 * division by zero.  */

/* replace BATconstant with a version that produces a void bat for
 * TYPE_oid/nil */
#define BATconstantV(HSEQ, TAILTYPE, VALUE, CNT, ROLE)			\
	((TAILTYPE) == TYPE_oid && ((CNT) == 0 || *(oid*)(VALUE) == oid_nil) \
	 ? BATconstant(HSEQ, TYPE_void, VALUE, CNT, ROLE)		\
	 : BATconstant(HSEQ, TAILTYPE, VALUE, CNT, ROLE))

#define ON_OVERFLOW1(TYPE, OP)					\
	do {							\
		GDKerror("22003!overflow in calculation "	\
			 OP "(" FMT##TYPE ").\n",		\
			 CST##TYPE src[x]);			\
		goto bailout;					\
	} while (0)

#define ON_OVERFLOW(TYPE1, TYPE2, OP)					\
	do {								\
		GDKerror("22003!overflow in calculation "		\
			 FMT##TYPE1 OP FMT##TYPE2 ".\n",		\
			 CST##TYPE1 ((TYPE1 *)lft)[i], CST##TYPE2 ((TYPE2 *)rgt)[j]); \
		return BUN_NONE;					\
	} while (0)

#define UNARY_2TYPE_FUNC(TYPE1, TYPE2, FUNC)				\
	do {								\
		const TYPE1 *restrict src = (const TYPE1 *) bi.base;	\
		TYPE2 *restrict dst = (TYPE2 *) Tloc(bn, 0);		\
		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {		\
			x = canditer_next(&ci) - bhseqbase;		\
			if (is_##TYPE1##_nil(src[x])) {			\
				nils++;					\
				dst[i] = TYPE2##_nil;			\
			} else {					\
				dst[i] = FUNC(src[x]);			\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset,				\
			      GOTO_LABEL_TIMEOUT_HANDLER(bailout));	\
	} while (0)

#define UNARY_2TYPE_FUNC_nilcheck(TYPE1, TYPE2, FUNC, on_overflow)	\
	do {								\
		const TYPE1 *restrict src = (const TYPE1 *) bi.base;	\
		TYPE2 *restrict dst = (TYPE2 *) Tloc(bn, 0);		\
		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {		\
			x = canditer_next(&ci) - bhseqbase;		\
			if (is_##TYPE1##_nil(src[x])) {			\
				nils++;					\
				dst[i] = TYPE2##_nil;			\
			} else {					\
				dst[i] = FUNC(src[x]);			\
				if (is_##TYPE2##_nil(dst[i])) {		\
					on_overflow;			\
				}					\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset,				\
			      GOTO_LABEL_TIMEOUT_HANDLER(bailout));	\
	} while (0)

#define BINARY_3TYPE_FUNC(TYPE1, TYPE2, TYPE3, FUNC)			\
	do {								\
		i = j = 0;						\
		if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {	\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next_dense(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next_dense(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				if (is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
					nils++;				\
					((TYPE3 *) dst)[k] = TYPE3##_nil; \
				} else {				\
					((TYPE3 *) dst)[k] = FUNC(v1, v2); \
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		} else {						\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				if (is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
					nils++;				\
					((TYPE3 *) dst)[k] = TYPE3##_nil; \
				} else {				\
					((TYPE3 *) dst)[k] = FUNC(v1, v2); \
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_nilcheck(TYPE1, TYPE2, TYPE3, FUNC, on_overflow) \
	do {								\
		i = j = 0;						\
		if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {	\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next_dense(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next_dense(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				if (is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
					nils++;				\
					((TYPE3 *) dst)[k] = TYPE3##_nil; \
				} else {				\
					((TYPE3 *) dst)[k] = FUNC(v1, v2); \
					if (is_##TYPE3##_nil(((TYPE3 *) dst)[k])) \
						on_overflow;		\
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		} else {						\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				if (is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
					nils++;				\
					((TYPE3 *) dst)[k] = TYPE3##_nil; \
				} else {				\
					((TYPE3 *) dst)[k] = FUNC(v1, v2); \
					if (is_##TYPE3##_nil(((TYPE3 *) dst)[k])) \
						on_overflow;		\
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		}							\
	} while (0)

/* special case for EQ and NE where we have a nil_matches flag for
 * when it is set */
#define BINARY_3TYPE_FUNC_nilmatch(TYPE1, TYPE2, TYPE3, FUNC)		\
	do {								\
		i = j = 0;						\
		if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {	\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next_dense(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next_dense(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				if (is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
					((TYPE3 *) dst)[k] = FUNC(is_##TYPE1##_nil(v1), is_##TYPE2##_nil(v2)); \
				} else {				\
					((TYPE3 *) dst)[k] = FUNC(v1, v2); \
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		} else {						\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				if (is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
					((TYPE3 *) dst)[k] = FUNC(is_##TYPE1##_nil(v1), is_##TYPE2##_nil(v2)); \
				} else {				\
					((TYPE3 *) dst)[k] = FUNC(v1, v2); \
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_nonil(TYPE1, TYPE2, TYPE3, FUNC)		\
	do {								\
		i = j = 0;						\
		if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {	\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next_dense(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next_dense(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				((TYPE3 *) dst)[k] = FUNC(v1, v2);	\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		} else {						\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				((TYPE3 *) dst)[k] = FUNC(v1, v2);	\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_nonil_nilcheck(TYPE1, TYPE2, TYPE3, FUNC, on_overflow) \
	do {								\
		i = j = 0;						\
		if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {	\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next_dense(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next_dense(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				((TYPE3 *) dst)[k] = FUNC(v1, v2);	\
				if (is_##TYPE3##_nil(((TYPE3 *) dst)[k])) \
					on_overflow;			\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		} else {						\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				((TYPE3 *) dst)[k] = FUNC(v1, v2);	\
				if (is_##TYPE3##_nil(((TYPE3 *) dst)[k])) \
					on_overflow;			\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_CHECK(TYPE1, TYPE2, TYPE3, FUNC, CHECK)	\
	do {								\
		i = j = 0;						\
		if (ci1->tpe == cand_dense && ci2->tpe == cand_dense) {	\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next_dense(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next_dense(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				if (is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
					nils++;				\
					((TYPE3 *) dst)[k] = TYPE3##_nil; \
				} else if (CHECK(v1, v2)) {		\
					GDKerror("%s: shift operand too large in " \
						 #FUNC"("FMT##TYPE1","FMT##TYPE2").\n", \
						 func,			\
						 CST##TYPE1 v1,		\
						 CST##TYPE2 v2);	\
					goto checkfail;			\
				} else {				\
					((TYPE3 *) dst)[k] = FUNC(v1, v2); \
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		} else {						\
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {	\
				if (incr1)				\
					i = canditer_next(ci1) - candoff1; \
				if (incr2)				\
					j = canditer_next(ci2) - candoff2; \
				TYPE1 v1 = ((const TYPE1 *) lft)[i];	\
				TYPE2 v2 = ((const TYPE2 *) rgt)[j];	\
				if (is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
					nils++;				\
					((TYPE3 *) dst)[k] = TYPE3##_nil; \
				} else if (CHECK(v1, v2)) {		\
					GDKerror("%s: shift operand too large in " \
						 #FUNC"("FMT##TYPE1","FMT##TYPE2").\n", \
						 func,			\
						 CST##TYPE1 v1,		\
						 CST##TYPE2 v2);	\
					goto checkfail;			\
				} else {				\
					((TYPE3 *) dst)[k] = FUNC(v1, v2); \
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE)); \
		}							\
	} while (0)

#if defined(_MSC_VER) && defined(__INTEL_COMPILER)
/* with Intel compiler on Windows, avoid using roundl and llroundl: they
 * cause a mysterious crash; long double is the same size as double
 * anyway */
typedef double ldouble;
#ifdef TRUNCATE_NUMBERS
#define rounddbl(x)	(x)
#else
#define rounddbl(x)	round(x)
#endif
#else
typedef long double ldouble;
#ifdef TRUNCATE_NUMBERS
#define rounddbl(x)	(x)
#else
#define rounddbl(x)	roundl(x)
#endif
#endif



#define absbte(x)	abs(x)
#define abssht(x)	abs(x)
#define absint(x)	abs(x)
#define abslng(x)	llabs(x)
#define abshge(x)	ABSOLUTE(x)

BAT *
BATcalcmuldivmod(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp,
		 BUN (*typeswitchloop)(const void *, int, bool,
				       const void *, int, bool,
				       void *restrict, int,
				       struct canditer *restrict,
				       struct canditer *restrict,
				       oid, oid, const char *),
		 const char *func);
