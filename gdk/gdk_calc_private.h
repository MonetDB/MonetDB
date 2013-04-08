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

/* This file contains shared definitions for gdk_calc.c and gdk_aggr.c */

#ifdef HAVE_LONG_LONG
typedef unsigned long long ulng;
#else
typedef unsigned __int64 ulng;
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

#define CANDINIT(b, s)							\
	do {								\
		start = 0;						\
		end = cnt = BATcount(b);				\
		if (s) {						\
			assert(BATttype(s) == TYPE_oid);		\
			if (BATcount(s) == 0) {				\
				start = end = 0;			\
			} else {					\
				if (BATtdense(s)) {			\
					start = (s)->T->seq;		\
					end = start + BATcount(s);	\
				} else {				\
					cand = (const oid *) Tloc((s), BUNfirst(s)); \
					candend = cand + BATcount(s);	\
					start = *cand;			\
					end = candend[-1] + 1;		\
				}					\
				assert(start <= end);			\
				if (start <= (b)->H->seq)		\
					start = 0;			\
				else if (start >= (b)->H->seq + cnt)	\
					start = cnt;			\
				else					\
					start -= (b)->H->seq;		\
				if (end >= (b)->H->seq + cnt)		\
					end = cnt;			\
				else if (end <= (b)->H->seq)		\
					end = 0;			\
				else					\
					end -= (b)->H->seq;		\
			}						\
		}							\
	} while (0)

/* dst = lft + rgt with overflow check */
#define ADD_WITH_CHECK(TYPE1, lft, TYPE2, rgt, TYPE3, dst, on_overflow)	\
	do {								\
		if ((rgt) < 1) {					\
			if (GDK_##TYPE3##_min - (rgt) >= (lft)) {	\
				if (abort_on_error)			\
					on_overflow;			\
				(dst) = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				(dst) = (TYPE3) (lft) + (rgt);		\
			}						\
		} else {						\
			if (GDK_##TYPE3##_max - (rgt) < (lft)) {	\
				if (abort_on_error)			\
					on_overflow;			\
				(dst) = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				(dst) = (TYPE3) (lft) + (rgt);		\
			}						\
		}							\
	} while (0)

#define MUL4_WITH_CHECK(TYPE1, lft, TYPE2, rgt, TYPE3, dst, TYPE4, on_overflow) \
	do {								\
		TYPE4 c = (TYPE4) (lft) * (rgt);			\
		if (c <= (TYPE4) GDK_##TYPE3##_min ||			\
		    c > (TYPE4) GDK_##TYPE3##_max) {			\
			if (abort_on_error)				\
				on_overflow;				\
			(dst) = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			(dst) = (TYPE3) c;				\
		}							\
	} while (0)

#define LNGMUL_CHECK(TYPE1, lft, TYPE2, rgt, dst, on_overflow)		\
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
		/* result = (a1*b1<<64) + ((a1*b2+a2*b1)<<32) + a2*b2 */ \
		if ((a1 == 0 || b1 == 0) &&				\
		    ((c = (ulng) a1 * b2 + (ulng) a2 * b1) & (~(ulng)0 << 31)) == 0 && \
		    (((c = (c << 32) + (ulng) a2 * b2) & ((ulng) 1 << 63)) == 0)) { \
			(dst) = sign * (lng) c;				\
		} else {						\
			if (abort_on_error)				\
				on_overflow;				\
			(dst) = lng_nil;				\
			nils++;						\
		}							\
	} while (0)
