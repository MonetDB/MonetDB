/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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

#ifndef HAVE_FABSF
#define fabsf ABSOLUTE
#endif
#ifndef HAVE_LLABS
#define llabs ABSOLUTE
#endif

#define LT(a, b)	((bit) ((a) < (b)))

#define GT(a, b)	((bit) ((a) > (b)))

#include "gdk_cand.h"

/* dst = lft + rgt with overflow check */
#define ADD_WITH_CHECK(TYPE1, lft, TYPE2, rgt, TYPE3, dst, max, on_overflow) \
	do {								\
		if ((rgt) < 1) {					\
			if (-(max) - (rgt) > (lft)) {			\
				if (abort_on_error)			\
					on_overflow;			\
				(dst) = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				(dst) = (TYPE3) (lft) + (rgt);		\
			}						\
		} else {						\
			if ((max) - (rgt) < (lft)) {			\
				if (abort_on_error)			\
					on_overflow;			\
				(dst) = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				(dst) = (TYPE3) (lft) + (rgt);		\
			}						\
		}							\
	} while (0)

/* dst = lft - rgt with overflow check */
#define SUB_WITH_CHECK(TYPE1, lft, TYPE2, rgt, TYPE3, dst, max, on_overflow) \
	do {								\
		if ((rgt) < 1) {					\
			if ((max) + (rgt) < (lft)) {			\
				if (abort_on_error)			\
					on_overflow;			\
				(dst) = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				(dst) = (TYPE3) (lft) - (rgt);		\
			}						\
		} else {						\
			if (-(max) + (rgt) > (lft)) {			\
				if (abort_on_error)			\
					on_overflow;			\
				(dst) = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				(dst) = (TYPE3) (lft) - (rgt);		\
			}						\
		}							\
	} while (0)

#define MUL4_WITH_CHECK(TYPE1, lft, TYPE2, rgt, TYPE3, dst, max, TYPE4, on_overflow) \
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

#define LNGMUL_CHECK(TYPE1, lft, TYPE2, rgt, dst, max, on_overflow)	\
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

#ifdef HAVE_HGE
#define HGEMUL_CHECK(TYPE1, lft, TYPE2, rgt, dst, max, on_overflow)	\
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
#endif
