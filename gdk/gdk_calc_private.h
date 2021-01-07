/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
			if (abort_on_error)				\
				on_overflow;				\
			(dst) = nil;					\
			nils++;						\
		}							\
	} while (0)
#endif	/* HAVE___BUILTIN_ADD_OVERFLOW */

/* dst = lft + rgt with overflow check */

/* generic version */
#define ADD_WITH_CHECK(lft, rgt, TYPE3, dst, max, on_overflow)	\
	do {							\
		if ((rgt) < 1) {				\
			if (-(max) - (rgt) > (lft)) {		\
				if (abort_on_error)		\
					on_overflow;		\
				(dst) = TYPE3##_nil;		\
				nils++;				\
			} else {				\
				(dst) = (TYPE3) (lft) + (rgt);	\
			}					\
		} else {					\
			if ((max) - (rgt) < (lft)) {		\
				if (abort_on_error)		\
					on_overflow;		\
				(dst) = TYPE3##_nil;		\
				nils++;				\
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
				if (abort_on_error)		\
					on_overflow;		\
				(dst) = TYPE3##_nil;		\
				nils++;				\
			} else {				\
				(dst) = (TYPE3) (lft) - (rgt);	\
			}					\
		} else {					\
			if (-(max) + (rgt) > (lft)) {		\
				if (abort_on_error)		\
					on_overflow;		\
				(dst) = TYPE3##_nil;		\
				nils++;				\
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
			if (abort_on_error)				\
				on_overflow;				\
			(dst) = TYPE3##_nil;				\
			nils++;						\
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
#else
#if defined(HAVE__MUL128)
#include <intrin.h>
#pragma intrinsic(_mul128)
#define LNGMUL_CHECK(lft, rgt, dst, max, on_overflow)		\
	do {							\
		lng clo, chi;					\
		clo = _mul128((lng) (lft), (lng) (rgt), &chi);	\
		if ((chi == 0 && clo >= 0 && clo <= (max)) ||	\
		    (chi == -1 && clo < 0 && clo >= -(max))) {	\
			(dst) = clo;				\
		} else {					\
			if (abort_on_error)			\
				on_overflow;			\
			(dst) = lng_nil;			\
			nils++;					\
		}						\
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
			if (abort_on_error)				\
				on_overflow;				\
			(dst) = lng_nil;				\
			nils++;						\
		}							\
	} while (0)
#endif
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
			if (abort_on_error)				\
				on_overflow;				\
			(dst) = hge_nil;				\
			nils++;						\
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
		    struct canditer *restrict ci, BUN ncand,
		    void *restrict results, BUN ngrp, int tp1, int tp2,
		    const oid *restrict gids,
		    oid min, oid max, bool skip_nils, bool abort_on_error,
		    bool nil_if_empty)
	__attribute__((__visibility__("hidden")));
