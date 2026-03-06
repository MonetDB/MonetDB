/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_time.h"
#include "mal_pipelines.h"
#include "pp_hash.h"
#include "pipeline.h"
#include "algebra.h"

#define sum(a,b) a+b
#define prod(a,b) a*b
#ifndef min
#define min(a,b) a<b?a:b
#define max(a,b) a>b?a:b
#endif

#define uuid_min(a,b) ((cmp((void*)&a,(void*)&b)<0)?a:b)
#define uuid_max(a,b) ((cmp((void*)&a,(void*)&b)>0)?a:b)

#define inet4_min(a,b) ((cmp((void*)&a,(void*)&b)<0)?a:b)
#define inet4_max(a,b) ((cmp((void*)&a,(void*)&b)>0)?a:b)

#define getArgReference_date(stk, pci, nr)      (date*)getArgReference(stk, pci, nr)
#define getArgReference_inet4(stk, pci, nr)      (inet4*)getArgReference(stk, pci, nr)
#define getArgReference_daytime(stk, pci, nr)   (daytime*)getArgReference(stk, pci, nr)
#define getArgReference_timestamp(stk, pci, nr) (timestamp*)getArgReference(stk, pci, nr)

#define aggr(T,f)												\
	if (type == TYPE_##T) {										\
		T val = *getArgReference_##T(stk, pci, 2);				\
		if (!is_##T##_nil(val) && BATcount(b)) {				\
			T *t = Tloc(b, 0);									\
			if (is_##T##_nil(t[0])) {							\
				t[0] = val;										\
			} else												\
				t[0] = f(t[0], val);							\
			MT_lock_set(&b->theaplock);							\
			b->tnil = false;									\
			b->tnonil = true;									\
			MT_lock_unset(&b->theaplock);						\
		} else if (BATcount(b) == 0) {							\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)		\
				err = createException(MAL, "lockedaggr." #f,	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);			\
		}														\
	}

#define faggr(T,f)														\
	if (type == TYPE_##T) {												\
		T val = *getArgReference_TYPE(stk, pci, 2, T);					\
		int (*cmp)(const void *v1,const void *v2) = ATOMcompare(type);	\
		if (!is_##T##_nil(val) && BATcount(b)) {						\
			T *t = Tloc(b, 0);											\
			if (is_##T##_nil(t[0])) {									\
				t[0] = val;												\
			} else														\
				t[0] = f(t[0], val);									\
			MT_lock_set(&b->theaplock);									\
			b->tnil = false;											\
			b->tnonil = true;											\
			MT_lock_unset(&b->theaplock);								\
		} else if (BATcount(b) == 0) {									\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)				\
				err = createException(MAL, "lockedaggr." #f,			\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);					\
		}																\
	}

#define vaggr(T,CT,f)														\
	if (type == TYPE_##T) {												\
		BATiter bi = bat_iterator(b);									\
		T val = *getArgReference_##T(stk, pci, 2);						\
		const void *nil = ATOMnilptr(type);								\
		int (*cmp)(const void *v1,const void *v2) = ATOMcompare(type);	\
		if (cmp(val,nil) != 0 && BATcount(b)) {							\
			CT t = BUNtvar(&bi, 0);								\
			if (cmp(t,nil) == 0) {										\
				if (BUNreplace(b, 0, val, true) != GDK_SUCCEED)			\
					err = createException(MAL, "2 lockedaggr." #f,		\
						SQLSTATE(HY013) MAL_MALLOC_FAIL);				\
			} else														\
				if (f(t, val) == val)									\
					if (BUNreplace(b, 0, val, true) != GDK_SUCCEED)		\
						err = createException(MAL, "1 lockedaggr." #f,	\
							SQLSTATE(HY013) MAL_MALLOC_FAIL);			\
			MT_lock_set(&b->theaplock);									\
			b->tnil = false;											\
			b->tnonil = true;											\
			MT_lock_unset(&b->theaplock);								\
		} else if (BATcount(b) == 0) {									\
			if (BUNappend(b, val, true) != GDK_SUCCEED)					\
				err = createException(MAL, "3 lockedaggr." #f,			\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);					\
		}																\
		bat_iterator_end(&bi);											\
	}

static str
LOCKEDAGGRsum1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (
#ifdef HAVE_HGE
			type != TYPE_hge &&
#endif
			type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "lockedaggr.sum", "Wrong input type (%d)", type);

	pipeline_lock(p);

	if (!is_bat_nil(*res)) {
		BAT *b = BATdescriptor(*res);
		if (b == NULL)
			err = createException(MAL, "lockedaggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		if (!err) {
#ifdef HAVE_HGE
			aggr(hge,sum);
#endif
			aggr(lng,sum);
			aggr(int,sum);
			aggr(sht,sum);
			aggr(bte,sum);
			aggr(flt,sum);
			aggr(dbl,sum);
			if (!err) {
				pipeline_lock2(b);
				BATnegateprops(b);
				pipeline_unlock2(b);
				BBPkeepref(b);
			} else
				BBPunfix(b->batCacheid);
		}
	} else {
			BAT *b = COLnew(0, type, 1, TRANSIENT);
			if (!b || BUNappend(b, p, true) != GDK_SUCCEED)
				err = createException(MAL, "lockedaggr.sum", "Result is not initialized");
			if (!err) {
				*res = b->batCacheid;
				BATnegateprops(b);
				BBPkeepref(b);
			} else if (b) {
				BBPunfix(b->batCacheid);
			}
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

#define paggr(T,OT,f)											\
	if (type == TYPE_##T && b->ttype == TYPE_##OT) {			\
		T val = *getArgReference_##T(stk, pci, 2);				\
		if (!is_##T##_nil(val) && BATcount(b)) {				\
			OT *t = Tloc(b, 0);									\
			if (is_##OT##_nil(t[0])) {							\
				t[0] = val;										\
			} else												\
				t[0] = f(t[0], val);							\
			MT_lock_set(&b->theaplock);							\
			b->tnil = false;									\
			b->tnonil = true;									\
			MT_lock_unset(&b->theaplock);						\
		} else if (BATcount(b) == 0) {							\
			OT ov = val;										\
			if (BUNappend(b, &ov, true) != GDK_SUCCEED)			\
				err = createException(MAL, "lockedaggr." #f,	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);			\
		}														\
	}

static str
LOCKEDAGGRprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (
#ifdef HAVE_HGE
			type != TYPE_hge &&
#endif
			type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "lockedaggr.prod", "Wrong input type (%d)", type);

	pipeline_lock(p);
	if (!is_bat_nil(*res)) {
		BAT *b = BATdescriptor(*res);
		if (b == NULL)
			err = createException(MAL, "lockedaggr.prod", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		if (!err){
			paggr(lng,lng,prod);
			paggr(int,lng,prod);
			paggr(sht,lng,prod);
			paggr(bte,lng,prod);

#ifdef HAVE_HGE
			paggr(hge,hge,prod);
			paggr(lng,hge,prod);
			paggr(int,hge,prod);
			paggr(sht,hge,prod);
			paggr(bte,hge,prod);
#endif

			paggr(flt,flt,prod);
			paggr(dbl,dbl,prod);
			if (!err) {
				pipeline_lock2(b);
				BATnegateprops(b);
				pipeline_unlock2(b);
				//BBPkeepref(*res = b->batCacheid);
				//leave writable
				BBPretain(*res = b->batCacheid);
				BBPunfix(b->batCacheid);
			} else
				BBPunfix(b->batCacheid);
		}
	} else {
			err = createException(SQL, "lockedaggr.prod", "Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

#define avg_aggr(T)														\
	if (type == TYPE_##T) {												\
		T val = *getArgReference_##T(stk, pci, pci->retc + 1);			\
		lng cnt = *getArgReference_lng(stk, pci, pci->retc + 2);		\
		if (cnt > 0 && !is_##T##_nil(val) && BATcount(b)) {				\
			T *t = Tloc(b, 0);											\
			lng *tcnt = Tloc(c, 0);										\
			if (is_##T##_nil(t[0])) {									\
				t[0] = val;												\
				tcnt[0] = cnt;											\
			} else {													\
			    dbl tt = (dbl) (tcnt[0] + cnt);							\
				t[0] = (T) ((t[0]*((dbl)tcnt[0]/tt)) + (val*((dbl)cnt/tt))); \
				tcnt[0] += cnt;											\
			}															\
			MT_lock_set(&b->theaplock);									\
			b->tnil = false;											\
			b->tnonil = true;											\
			MT_lock_unset(&b->theaplock);								\
		} else if (cnt > 0 && BATcount(b) == 0) {						\
			if (BUNappend(b, &val, true) != GDK_SUCCEED) {				\
				err = createException(MAL, "lockedaggr.avg",			\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);					\
				goto error;												\
			}															\
		}																\
	}

/* return (a * b) % c without intermediate overflow */
#ifdef HAVE_HGE
static inline lng
mulmod(hge A, lng b, lng c)
{
	lng res = 0;
	lng a = (lng) (A % c);
	while (b) {
		if (b & 1)
			res = (res + a) % c;
		a = (2 * a) % c;
		b >>= 1;
	}
	return res;
}
#else
static inline lng
mulmod(lng a, lng b, lng c)
{
	lng res = 0;
	a %= c;
	while (b) {
		if (b & 1)
			res = (res + a) % c;
		a = (2 * a) % c;
		b >>= 1;
	}
	return res;
}
#endif

#ifdef TRUNCATE_NUMBERS
#define fix_avg(T, a, r, n)								\
	do {												\
		if (!is_##T##_nil(a) && r > 0 && a < 0) {		\
			a++;										\
			r -= n;										\
		}												\
	} while (0)
#else
#define fix_avg(T, a, r, n)										\
	do {														\
		if (!is_##T##_nil(a) && r > 0 && 2*r + (a < 0) >= n) {	\
			a++;												\
			r -= n;												\
		}														\
	} while (0)
#endif
/* Inside the avg_aggr_comb macro we want to calculate
 * n = n1 + n2
 * a = (a1*n1 + r1 + a2*n2 + r2) / n
 * r = (a1*n1 + r1 + a2*n2 + r2) % n
 *
 * Note that we can't simply distribute the division over the terms but
 * need to do extra work.  What we can do is when we want to calculate
 * (a+b)/c to calculate d=(a/c + b/c) and r=(a%c + b%c) and if r too
 * small or too large (<= -abs(c) or >= abc(c)) then compensate d and r
 * to get r within the range by adding/subtracting c to/from r until r
 * is in range, and whenever we do that, add/subtract 1 to/from d.  We
 * can also do this until r is in the range 0 (inclusive) to abc(c)
 * (exclusive) to get integer division that truncates towards negative
 * infinity.
 *
 * We derive a way to calculate a*b/c and a*b%c without intermediate
 * overflow in a*b.
 *
 * Notation: x/y is integer division, x÷y is mathematically exact division.
 * x*y is integer multiplication (always mathematically exact).
 * x%y is integer remainder following the rule that x == (x/y)*y + x%y, or
 * x÷y == x/y + (x%y)÷y.
 * During the derivation we assume infinite precision.
 *
 * (a*b)÷c = (a÷c)*b
 *         = (a/c + (a%c)÷c)*b
 *         = (a/c)*b + ((a%c)*b)÷c
 *         = (a/c)*b + ((a%c)*b)/c + (((a%c)*b)%c)÷c
 *         = (a/c)*b + ((a%c)*b)/c + ((a*b)%c)÷c
 * Note that the last term is only the fraction (i.e. strictly between
 * -1 and 1), so (a*b)%c is the remainder.
 */
#ifdef HAVE___INT128
#define avg_aggr_comb(T, a1, r1, n1, a2, r2, n2)								\
	do {																		\
		if (is_##T##_nil(a2)) {													\
			if (!is_lng_nil(r2)) {												\
				a2 = a1;														\
				r2 = r1;														\
				n2 = n1;														\
			}																	\
		} else if (!is_##T##_nil(a1)) {											\
			lng N1 = is_lng_nil(n1) ? 0 : n1;									\
			lng N2 = is_lng_nil(n2) ? 0 : n2;									\
			lng n = N1 + N2;													\
			T a;																\
			lng r;																\
			if (n == 0) {														\
				a = 0;															\
				r = 0;															\
			} else {															\
				a = (T) ((a1 / n) * N1 + ((a1 % n) * (__int128) N1) / n + 		\
						 (a2 / n) * N2 + ((a2 % n) * (__int128) N2) / n +		\
						 (r1 + r2) / n);										\
				r = mulmod(a1, N1, n) + mulmod(a2, N2, n) + (r1 + r2) % n; 		\
				while (r >= n) {												\
					r -= n;														\
					a++;														\
				}																\
				while (r < 0) {													\
					r += n;														\
					a--;														\
				}																\
				fix_avg(T, a, r, n);											\
			}																	\
			a2 = a;																\
			r2 = r;																\
			n2 = n;																\
		}																		\
	} while (0)
#elif defined(_MSC_VER) && _MSC_VER >= 1920 && defined(_M_AMD64) && !defined(__INTEL_COMPILER)
#include <intrin.h>
#include <immintrin.h>
#pragma intrinsic(_mul128)
#pragma intrinsic(_div128)
#define avg_aggr_comb(T, a1, r1, n1, a2, r2, n2)			\
	do {								\
		if (is_##T##_nil(a2)) {					\
			a2 = a1;					\
			r2 = r1;					\
			n2 = n1;					\
		} else if (!is_##T##_nil(a1)) {				\
			lng N1 = is_lng_nil(n1) ? 0 : n1;		\
			lng N2 = is_lng_nil(n2) ? 0 : n2;		\
			lng n = N1 + N2;				\
			T a;						\
			lng r;						\
			if (n == 0) {					\
				a = 0;					\
				r = 0;					\
			} else {					\
				a = (T) ((a1 / n) * N1 +  (a2 / n) * N2 + (r1 + r2) / n); 	\
				__int64 xlo, xhi, rem;										\
				xlo = _mul128((__int64) (a1 % n), N1, &xhi);			\
				a += (T) _div128(xhi, xlo, (__int64) n, &rem);			\
				xlo = _mul128((__int64) (a2 % n), N2, &xhi);			\
				a += (T) _div128(xhi, xlo, (__int64) n, &rem);			\
				r = (r1 + r2) % n;						\
				xlo = _mul128(a1, N1, &xhi);					\
				xhi = _div128(xhi, xlo, n, &xlo); /* xlo is remainder */ 	\
				r += xlo;							\
				xlo = _mul128(a2, N2, &xhi);					\
				xhi = _div128(xhi, xlo, n, &xlo); /* xlo is remainder */ 	\
				r += xlo;							\
				while (r >= n) {						\
					r -= n;							\
					a++;							\
				}								\
				while (r < 0) {							\
					r += n;							\
					a--;							\
				}								\
				fix_avg(T, a, r, n);						\
			}									\
			a2 = a;									\
			r2 = r;									\
			n2 = n;									\
		}										\
	} while (0)
#else
#define avg_aggr_comb(T, a1, r1, n1, a2, r2, n2)			\
	do {								\
		if (is_##T##_nil(a2)) {					\
			a2 = a1;					\
			r2 = r1;					\
			n2 = n1;					\
		} else if (!is_##T##_nil(a1)) {				\
			lng N1 = is_lng_nil(n1) ? 0 : n1;		\
			lng N2 = is_lng_nil(n2) ? 0 : n2;		\
			lng n = N1 + N2;				\
			T a;						\
			lng r;						\
			if (n == 0) {					\
				a = 0;					\
				r = 0;					\
			} else {					\
				lng x1 = a1 % n;			\
				lng x2 = a2 % n;			\
				if ((N1 != 0 &&				\
					 (x1 > GDK_lng_max / N1 || x1 < -GDK_lng_max / N1)) || 	\
					(N2 != 0 &&					        \
					 (x2 > GDK_lng_max / N2 || x2 < -GDK_lng_max / N2))) { 	\
					err = createException(SQL, "aggr.avg",			\
						  SQLSTATE(22003) "overflow in calculation");	\
					goto error;						\
				}								\
				a = (T) ((a1 / n) * N1 + (x1 * N1) / n +			\
						 (a2 / n) * N2 + (x2 * N2) / n +		\
						 (r1 + r2) / n);				\
				r = mulmod(a1, N1, n) + mulmod(a2, N2, n) + (r1 + r2) % n; 	\
				while (r >= n) {						\
					r -= n;							\
					a++;							\
				}								\
				while (r < 0) {							\
					r += n;							\
					a--;							\
				}								\
				fix_avg(T, a, r, n);						\
			}									\
			a2 = a;									\
			r2 = r;									\
			n2 = n;									\
		}										\
	} while (0)
#endif

#define avg_aggr_acc(T)														\
	do {																	\
		T a1 = *getArgReference_##T(stk, pci, pci->retc + 1);				\
		lng r1 = *getArgReference_lng(stk, pci, pci->retc + 2);				\
		lng n1 = *getArgReference_lng(stk, pci, pci->retc + 3);				\
		T a2 = *(T*)Tloc(b, 0);								\
		lng r2 = *(lng*)Tloc(r, 0);							\
		lng n2 = *(lng*)Tloc(c, 0);							\
		avg_aggr_comb(T, a1, r1, n1, a2, r2, n2);					\
		*(T*)Tloc(b, 0) = a2;								\
		*(lng*)Tloc(r, 0) = r2;								\
		*(lng*)Tloc(c, 0) = n2;								\
	} while (0)

#define avg_aggr_float(T1, T2, a1, a2, e2, n2)						\
	do {										\
		if (is_##T2##_nil(a2)) {						\
			a2 = a1;							\
			e2 = 0;								\
			overflow += n2 != 0;							\
			n2 = !(is_##T1##_nil(a1));					\
		} else if (!is_##T1##_nil(a1)) {					\
			T2 t = a2 + a1;							\
			if (fabs(a2) >= fabs(a1))					\
				e2 += (a2 - t) + a1;					\
			else								\
				e2 += (a1 - t) + a2;					\
			a2 = t;								\
			overflow += (isinf(t) || isnan(t));				\
			n2++;								\
		}									\
	} while(0)

#define avg_aggr_float_comb(T, a1, e1, n1, a2, e2, n2)					\
	do {										\
		if (is_##T##_nil(a2)) {							\
			a2 = a1;							\
			e2 = e1;							\
			overflow += n2 != 0;							\
			overflow += (is_##T##_nil(a1)?n1!=0:0);				\
			n2 = n1;							\
		} else if (!is_##T##_nil(a1)) {						\
		    T t = a2 + a1;							\
			if (fabs(a2) >= fabs(a1))					\
				e2 += (a2 - t) + a1;					\
			else								\
				e2 += (a1 - t) + a2;					\
			a2 = t;								\
			overflow += (isinf(t) || isnan(t));				\
			e2 += e1;							\
			n2 += n1;							\
		}									\
	} while(0)

static str
LOCKEDAGGRsum_avg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bool sum)
{
	(void)cntxt;
	const char *fcn = (sum)?"lockedaggr.sum": "lockedaggr.avg";
	bat *res = getArgReference_bat(stk, pci, 0);
	bat *rcnt = getArgReference_bat(stk, pci, pci->retc - 1);
	bat *rrem = pci->retc == 3 ? getArgReference_bat(stk, pci, 1) : NULL;
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, pci->retc);
	int type = getArgType(mb, pci, pci->retc + 1);
	str err = NULL;
	BAT *b = NULL, *c = NULL, *r = NULL;

	if (
#ifdef HAVE_HGE
			type != TYPE_hge &&
#endif
			type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, fcn, "Wrong input type (%d)", type);

	pipeline_lock(p);
	if (is_bat_nil(*res)) {
		err = createException(SQL, fcn, "Result is not initialized");
		goto error;
	}
	b = BATdescriptor(*res);
	c = BATdescriptor(*rcnt);
	if (b == NULL || c == NULL) {
		err = createException(MAL, fcn, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	if (pci->retc == 3) {
		if ((r = BATdescriptor(*rrem)) == NULL) {
			err = createException(MAL, fcn, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		switch (b->ttype) {
			case TYPE_bte:
				avg_aggr_acc(bte);
				break;
			case TYPE_sht:
				avg_aggr_acc(sht);
				break;
			case TYPE_int:
				avg_aggr_acc(int);
				break;
			case TYPE_lng:
				avg_aggr_acc(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				avg_aggr_acc(hge);
				break;
#endif
			case TYPE_flt: {
				int overflow = 0;
				flt a1 = *getArgReference_flt(stk, pci, pci->retc + 1);
				flt r1 = *getArgReference_flt(stk, pci, pci->retc + 2);
				lng n1 = *getArgReference_lng(stk, pci, pci->retc + 3);
				flt a2 = *(flt*)Tloc(b, 0);
				flt r2 = *(flt*)Tloc(r, 0);
				lng n2 = *(lng*)Tloc(c, 0);
				avg_aggr_float_comb(flt, a1, r1, n1, a2, r2, n2);
				if (overflow || isinf(a1) || (!n1 && isnan(a1))) {
					err = createException(SQL, "pp aggr.avg", "Overflow in avg()");
					goto error;
				}
				*(flt*)Tloc(b, 0) = a2;
				*(flt*)Tloc(r, 0) = r2;
				*(lng*)Tloc(c, 0) = n2;
				break;
			}
			case TYPE_dbl: {
				int overflow = 0;
				dbl a1 = *getArgReference_dbl(stk, pci, pci->retc + 1);
				dbl r1 = *getArgReference_dbl(stk, pci, pci->retc + 2);
				lng n1 = *getArgReference_lng(stk, pci, pci->retc + 3);
				dbl a2 = *(dbl*)Tloc(b, 0);
				dbl r2 = *(dbl*)Tloc(r, 0);
				lng n2 = *(lng*)Tloc(c, 0);
				avg_aggr_float_comb(dbl, a1, r1, n1, a2, r2, n2);
				if (overflow || isinf(a1) || (!n1 && isnan(a1))) {
					err = createException(SQL, "pp aggr.avg", "Overflow in avg()");
					goto error;
				}
				*(dbl*)Tloc(b, 0) = a2;
				*(dbl*)Tloc(r, 0) = r2;
				*(lng*)Tloc(c, 0) = n2;
				break;
			}
		}
		pipeline_lock2(b);
		BATnegateprops(b);
		pipeline_unlock2(b);
		pipeline_lock2(r);
		BATnegateprops(r);
		pipeline_unlock2(r);
		pipeline_lock2(c);
		BATnegateprops(c);
		pipeline_unlock2(c);
		BBPkeepref(b);
		BBPkeepref(r);
		BBPkeepref(c);
	} else {
		assert(b->ttype == TYPE_dbl);
#ifdef HAVE_HGE
		avg_aggr(hge);
#endif
		avg_aggr(lng);
		avg_aggr(int);
		avg_aggr(sht);
		avg_aggr(bte);
		avg_aggr(flt);
		avg_aggr(dbl);

		pipeline_lock2(b);
		BATnegateprops(b);
		pipeline_unlock2(b);
		BBPkeepref(b);
		pipeline_lock2(c);
		BATnegateprops(c);
		pipeline_unlock2(c);
		BBPkeepref(c);
	}
	pipeline_unlock(p);
	return MAL_SUCCEED;
  error:
	pipeline_unlock(p);
	if(b) BBPunfix(b->batCacheid);
	if(c) BBPunfix(c->batCacheid);
	if(r) BBPunfix(r->batCacheid);
	return err;
}

static str
LOCKEDAGGRsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return LOCKEDAGGRsum_avg(cntxt, mb, stk, pci, true);
}

static str
LOCKEDAGGRavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return LOCKEDAGGRsum_avg(cntxt, mb, stk, pci, false);
}
#define vmin(a,b) ((cmp(a,b) < 0)?a:b)
#define vmax(a,b) ((cmp(a,b) > 0)?a:b)

static str
LOCKEDAGGRmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (
#ifdef HAVE_HGE
			type != TYPE_hge &&
#endif
		type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte && type != TYPE_bit &&
		type != TYPE_flt && type != TYPE_dbl && type != TYPE_oid &&
		type != TYPE_date && type != TYPE_daytime && type != TYPE_timestamp && type != TYPE_uuid && type != TYPE_str &&
		type != TYPE_inet4)
			return createException(SQL, "lockedaggr.min", "Wrong input type (%d)", type);

	pipeline_lock(p);
	if (!is_bat_nil(*res)) {
		BAT *b = BATdescriptor(*res);
		if (b == NULL)
			err = createException(MAL, "lockedaggr.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		if (!err) {
			aggr(date,min);
			faggr(inet4,inet4_min);
			aggr(daytime,min);
			aggr(timestamp,min);
			faggr(uuid,uuid_min);
#ifdef HAVE_HGE
			aggr(hge,min);
#endif
			aggr(lng,min);
			aggr(oid,min);
			aggr(int,min);
			aggr(sht,min);
			aggr(bte,min);
			aggr(bit,min);
			aggr(flt,min);
			aggr(dbl,min);
			vaggr(str,const char *,vmin);
			if (!err) {
				pipeline_lock2(b);
				BATnegateprops(b);
				pipeline_unlock2(b);
				//BBPkeepref(*res = b->batCacheid);
				//leave writable
				BBPretain(*res = b->batCacheid);
				BBPunfix(b->batCacheid);
			} else
				BBPunfix(b->batCacheid);
		}
	} else {
			err = createException(SQL, "lockedaggr.min", "Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LOCKEDAGGRmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (
#ifdef HAVE_HGE
			type != TYPE_hge &&
#endif
		type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte && type != TYPE_bit &&
		type != TYPE_flt && type != TYPE_dbl && type != TYPE_oid &&
		type != TYPE_date && type != TYPE_daytime && type != TYPE_timestamp && type != TYPE_uuid && type != TYPE_str &&
		type != TYPE_inet4)
			return createException(SQL, "lockedaggr.max", "Wrong input type (%d)", type);

	pipeline_lock(p);
	if (!is_bat_nil(*res)) {
		BAT *b = BATdescriptor(*res);
		if (b == NULL)
			err = createException(MAL, "lockedaggr.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		if (!err) {
			aggr(date,max);
			faggr(inet4,inet4_max);
			aggr(daytime,max);
			aggr(timestamp,max);
			faggr(uuid,uuid_max);
#ifdef HAVE_HGE
			aggr(hge,max);
#endif
			aggr(lng,max);
			aggr(oid,max);
			aggr(int,max);
			aggr(sht,max);
			aggr(bte,max);
			aggr(bit,max);
			aggr(flt,max);
			aggr(dbl,max);
			vaggr(str,const char *,vmax);
			if (!err) {
				pipeline_lock2(b);
				BATnegateprops(b);
				pipeline_unlock2(b);
				//BBPkeepref(*res = b->batCacheid);
				//leave writable
				BBPretain(*res = b->batCacheid);
				BBPunfix(b->batCacheid);
			} else
				BBPunfix(b->batCacheid);
		}
	} else {
			err = createException(SQL, "lockedaggr.max", "Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LOCKEDAGGRnull(Client ctx, bat *result, const ptr *h, const bit *hadnull)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*h;
	str err = MAL_SUCCEED;

	pipeline_lock(p);
	if (!is_bat_nil(*result)) {
		BAT *b = BATdescriptor(*result);
		if (b == NULL)
			err = createException(MAL, "lockedaggr.null", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		if (!err) {
			assert(BATcount(b) == 1);
			bit *n = Tloc(b, 0); /* b must have been initialised to bit_nil */
			if (is_bit_nil(n[0]) || (n[0] ==false && !is_bit_nil(*hadnull))) {
				n[0] = *hadnull;
			}
			if (!is_bit_nil(*hadnull)) {
				pipeline_lock2(b);
				b->tnil = false;
				b->tnonil = true;
				pipeline_unlock2(b);
			}
			//BBPkeepref(*res = b->batCacheid);
			//leave writable
			BBPretain(*result = b->batCacheid);
			BBPunfix(b->batCacheid);
		}
	} else {
		err = createException(SQL, "lockedaggr.null", "Result is not initialized");
	}
	pipeline_unlock(p);
	return err;
}

static str
LALGprojection(Client ctx, bat *result, const ptr *h, const bat *lid, const bat *rid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	pipeline_lock(p);
	res = ALGprojection(ctx, result, lid, rid);
	pipeline_unlock(p);
	return res;
}

#define unique_(Type, BaseType, INIT_ALLOCATOR, INIT_ITER, NEW_VAL, HASH_VAL, VAL_NOT_EQUAL, VAL_ASSIGN, ITER_NEXT, NEXTK) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		INIT_ITER; \
		Type *vals = h->vals; \
		INIT_ALLOCATOR; \
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool new = 0, fnd = 0; \
			for(; !fnd; ) { \
				NEW_VAL; \
				gid hv = HASH_VAL&h->mask, k = hv; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(gid l=1;g && VAL_NOT_EQUAL; l++) { \
					NEXTK; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					VAL_ASSIGN; \
					new = 1; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slot--; \
						slots++; \
						new = 0; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			if (new) \
				gp[r++] = b->hseqbase + i; \
		} \
		ITER_NEXT; \
	}

#define unique(Type) \
	unique_(Type,  \
			Type, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(bp[i]), \
			vals[g] != bp[i], \
			vals[g] = bp[i], \
			, \
			nextk \
		)

#define funique(Type, BaseType) \
	unique_(Type, \
			BaseType, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(*(((BaseType*)bp)+i)), \
			(!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]), \
			vals[g] = bp[i], \
			, \
			nextk \
		)

#define cunique(Type, BaseType) \
	unique_(Type, \
			BaseType, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(*(((BaseType*)bp)+i)), \
			(!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && h->cmp(vals+g, bp+i) != 0), \
			vals[g] = bp[i], \
			, \
			nextk \
		)

#define aunique_(Type,CType) \
	unique_(Type, \
			Type, \
			allocator *ma = h->allocators[p->wid], \
			BATiter bi = bat_iterator(b), \
			CType bpi = BUNtvar(&bi, i), \
			(gid)h->hsh(bpi), \
			(h->cmp(vals[g], bpi) != 0), \
			vals[g] = ma_strdup(ma, bpi), \
			bat_iterator_end(&bi), \
			nextk \
		)

#define aunique(Type,CType) \
	unique_(Type, \
			Type, \
			, \
			BATiter bi = bat_iterator(b), \
			CType bpi = BUNtvar(&bi, i), \
			(gid)h->hsh(bpi), \
			(h->cmp(vals[g], bpi) != 0), \
			vals[g] = (Type)bpi, \
			bat_iterator_end(&bi), \
			nextk \
		)

static str
LALGunique(Client ctx, bat *rid, bat *uid, const ptr *H, bat *bid, bat *sid)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H;
	assert(!is_bat_nil(*uid));
	str err = NULL;
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;
	bool local_storage = false;

	BAT *u = BATdescriptor(*uid);
	BAT *b = BATdescriptor(*bid);
	if (u == NULL || b == NULL) {
		err = createException(MAL, "pp algebra.unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	hash_table *h = (hash_table*)u->tsink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if (ATOMvarsized(u->ttype) /*&& !VIEWvtparent(b)*/) {
		local_storage = true;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		pipeline_lock(p);
		if (!h->allocators) {
			h->allocators = (allocator**)GDKzalloc(p->p->nr_workers*sizeof(allocator*));
			if (!h->allocators) {
				pipeline_unlock(p);
				err = createException(MAL, "pp algebra.(group )unique", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			} else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			char name[8];
			snprintf(name, sizeof(name), "pp%d", p->wid);
			h->allocators[p->wid] = create_allocator(name, false);
			if (!h->allocators[p->wid]) {
				err = createException(MAL, "pp algebra.(group )unique", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
		}
	} else if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		BATswap_heaps(u, b, p);
	} else if (ATOMvarsized(u->ttype) && u->tvheap->parentid != b->tvheap->parentid) {
		int i = 0;
		for(i = 0; i < h->pinned_nr; i++) {
			if (h->pinned[i] == b->tvheap)
				break;
		}
		if (i == h->pinned_nr) {
			HEAPincref(b->tvheap);
			BBPfix(b->tvheap->parentid);
			h->pinned[h->pinned_nr++] = b->tvheap;
			assert(h->pinned_nr < 1024);
		}
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	} else {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	}
	if (h) {
		BUN cnt = BATcount(b);

		ATOMIC_BASE_TYPE expected = 0;
		BUN r = 0;

		BAT *g = COLnew(0, TYPE_oid, cnt, TRANSIENT);
		if (g == NULL) {
			err = createException(MAL, "pp algebra.unique", MAL_MALLOC_FAIL);
			goto error;
		}
		if (cnt && !err) {
			ht_activate(h);
			/* probably need bat resize and create hash */
			int tt = b->ttype;
			oid *gp = Tloc(g, 0);

			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
			unique(bit)
			unique(bte)
			unique(sht)
			unique(int)
			unique(date)
			cunique(inet4, int)
			unique(lng)
			unique(daytime)
			unique(timestamp)
#ifdef HAVE_HGE
			unique(hge)
#endif
			funique(flt, int)
			funique(dbl, lng)
#ifdef HAVE_HGE
			cunique(uuid, hge)
#endif
			if (local_storage) {
				aunique_(str,const char *)
			} else {
				aunique(str,const char *)
			}
			h->processed += cnt;
			ht_deactivate(h);
			TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp algebra.unique", RUNTIME_QRY_TIMEOUT));
		}
		if (err) {
			BBPunfix(g->batCacheid);
			goto error;
		}
		BATsetcount(g, r);
		pipeline_lock2(g);
		BATnegateprops(g);
		pipeline_unlock2(g);
		/* props */
		*uid = u->batCacheid;
		*rid = g->batCacheid;
		BBPkeepref(u);
		BBPkeepref(g);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
  error:
	if (u) BBPunfix(u->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	return err;
}

#define gunique_(Type, BaseType, INIT_ALLOCATOR, INIT_ITER, NEW_VAL, HASH_VAL, VAL_NOT_EQUAL, VAL_ASSIGN, ITER_NEXT, NEXTK) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		INIT_ITER; \
		Type *vals = h->vals; \
		INIT_ALLOCATOR; \
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool new = 0, fnd = 0; \
			for(; !fnd;) { \
				NEW_VAL; \
				gid hv = (gid)combine(gi[i], HASH_VAL, prime)&h->mask, k = hv; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(gid l=1; g && (pgids[g] != gi[i] || VAL_NOT_EQUAL); l++) { \
					NEXTK; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							pgids = h->pgids; \
							prime = hash_prime_nr[h->bits-5]; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					VAL_ASSIGN; \
					pgids[g] = gi[i]; \
					new = 1; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slot--; \
						slots++; \
						new = 0; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			if (new) \
				gp[r++] = b->hseqbase + i; \
		} \
		ITER_NEXT; \
	}

#define gunique(Type) \
	gunique_(Type,  \
			Type, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(bp[i]), \
			vals[g] != bp[i], \
			vals[g] = bp[i], \
			, \
			nextk \
		)

#define gfunique(Type, BaseType) \
	gunique_(Type, \
			BaseType, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(*(((BaseType*)bp)+i)), \
			(!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]), \
			vals[g] = bp[i], \
			, \
			nextk \
		)

#define gcunique(Type, BaseType) \
	gunique_(Type, \
			BaseType, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(*(((BaseType*)bp)+i)), \
			(!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && h->cmp(vals+g, bp+i) != 0), \
			vals[g] = bp[i], \
			, \
			nextk \
		)

#define gaunique_(Type,CType) \
	gunique_(Type, \
			Type, \
			allocator *ma = h->allocators[p->wid], \
			BATiter bi = bat_iterator(b), \
			CType bpi = BUNtvar(&bi, i), \
			(gid)h->hsh(bpi), \
			(h->cmp(vals[g], bpi) != 0), \
			vals[g] = ma_strdup(ma, bpi), \
			bat_iterator_end(&bi), \
			nextk \
		)

#define gaunique(Type,CType) \
	gunique_(Type, \
			Type, \
			, \
			BATiter bi = bat_iterator(b), \
			CType bpi = BUNtvar(&bi, i), \
			(gid)h->hsh(bpi), \
			(h->cmp(vals[g], bpi) != 0), \
			vals[g] = (Type)bpi, \
			bat_iterator_end(&bi), \
			nextk \
		)

static str
LALGgroup_unique(Client ctx, bat *rid, bat *uid, const ptr *H, bat *bid, bat *sid, bat *Gid)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H;
	assert(!is_bat_nil(*uid));
	str err = NULL;
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;
	bool local_storage = false;

	BAT *u = BATdescriptor(*uid);
	BAT *G = BATdescriptor(*Gid);
	BAT *b = BATdescriptor(*bid);
	if (u == NULL || G == NULL || b == NULL) {
		err = createException(MAL, "pp algebra.(group_)unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	hash_table *h = (hash_table*)u->tsink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if (ATOMvarsized(u->ttype) /*&& !VIEWvtparent(b)*/) {
		local_storage = true;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		pipeline_lock(p);
		if (!h->allocators) {
			h->allocators = (allocator**)GDKzalloc(p->p->nr_workers*sizeof(allocator*));
			if (!h->allocators) {
				pipeline_unlock(p);
				err = createException(MAL, "pp algebra.(group )unique", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			} else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			char name[8];
			snprintf(name, sizeof(name), "pp%d", p->wid);
			h->allocators[p->wid] = create_allocator(name, false);
			if (!h->allocators[p->wid]) {
				err = createException(MAL, "pp algebra.(group )unique", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
		}
	} else if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		BATswap_heaps(u, b, p);
	} else if (ATOMvarsized(u->ttype) && u->tvheap->parentid != b->tvheap->parentid) {
		int i = 0;
		for(i = 0; i < h->pinned_nr; i++) {
			if (h->pinned[i] == b->tvheap)
				break;
		}
		if (i == h->pinned_nr) {
			HEAPincref(b->tvheap);
			BBPfix(b->tvheap->parentid);
			h->pinned[h->pinned_nr++] = b->tvheap;
			assert(h->pinned_nr < 1024);
		}
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	} else {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	}
	if (h) {
		BUN cnt = BATcount(b);

		BUN r = 0;

		BAT *ng = COLnew(0, TYPE_oid, cnt, TRANSIENT);
		if (ng == NULL) {
			err = createException(MAL, "pp algebra.(group_)unique", MAL_MALLOC_FAIL);
			goto error;
		}
		if (cnt && !err) {
			ht_activate(h);
			ATOMIC_BASE_TYPE expected = 0;
			/* probably need bat resize and create hash */
			int tt = b->ttype;
			oid *gp = Tloc(ng, 0);
			gid *gi = Tloc(G, 0);
			gid *pgids = h->pgids;
			int prime = hash_prime_nr[h->bits-5];

			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
			gunique(bit)
			gunique(bte)
			gunique(sht)
			gunique(int)
			gunique(date)
			gcunique(inet4, int)
			gunique(lng)
			gunique(daytime)
			gunique(timestamp)
#ifdef HAVE_HGE
			gunique(hge)
#endif
			gfunique(flt, int)
			gfunique(dbl, lng)
#ifdef HAVE_HGE
			gcunique(uuid, hge)
#endif
			if (local_storage) {
				gaunique_(str,const char *)
			} else {
				gaunique(str,const char *)
			}
			ht_deactivate(h);
			TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp algebra.(group_)unique", RUNTIME_QRY_TIMEOUT));
		}
		if (err) {
			BBPunfix(ng->batCacheid);
			goto error;
		}
		BATsetcount(ng, r);
		pipeline_lock2(ng);
		BATnegateprops(ng);
		pipeline_unlock2(ng);
		/* props */
		*uid = u->batCacheid;
		*rid = ng->batCacheid;
		BBPkeepref(u);
		BBPkeepref(ng);
	}
	BBPunfix(G->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
  error:
	if (u) BBPunfix(u->batCacheid);
	if (G) BBPunfix(G->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	return err;
}

#define group_(Type, BaseType, INIT_ALLOCATOR, INIT_ITER, NEW_VAL, HASH_VAL, VAL_NOT_EQUAL, VAL_ASSIGN, ITER_NEXT, NEXTK) \
		int slots = 0; \
		gid slot = 0; \
		INIT_ITER; \
		Type *vals = h->vals; \
		INIT_ALLOCATOR; \
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			gid g = 0; \
			for(; !fnd; ) { \
				NEW_VAL; \
				gid hv = HASH_VAL&h->mask, k = hv; \
				g = ATOMIC_GET(h->gids+k); \
				for(gid l=1; g && VAL_NOT_EQUAL; l++) { \
					NEXTK; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					VAL_ASSIGN; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slot--; \
						slots++; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		ITER_NEXT;

#define group(Type) \
	if (tt == TYPE_##Type) { \
		group_(Type, \
			Type, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(bp[i]), \
			vals[g] != bp[i], \
			vals[g] = bp[i], \
			, \
			nextk \
		) \
	}

#define vgroup() \
	if (tt == TYPE_void) { \
		if (!BATtdense(b)) { \
			assert(cnt); \
			int slots = 0; \
			gid slot = 0; \
			oid bpi = b->tseqbase; \
			oid *vals = h->vals; \
			\
			bool fnd = 0; \
			gid g = 0; \
			for(; !fnd; ) { \
				gid k = (gid)_hash_oid(oid_nil)&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && vals[g] != bpi;) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slot--; \
						slots++; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			for(BUN i = 0; i<cnt; i++) { \
				gp[i] = g-1; \
			} \
		} else { \
			assert(BATtdense(b)); \
			group_(oid, \
				oid, \
				, \
				oid bp = b->tseqbase, \
				oid bpi = bp+i, \
				(gid)_hash_oid(bpi), \
				vals[g] != bpi, \
				vals[g] = bpi, \
				, \
				nextk \
			) \
		} \
	}

#define fgroup(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		group_(Type, \
			BaseType, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(*(((BaseType*)bp)+i)), \
			(!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]), \
			vals[g] = bp[i], \
			, \
			nextk \
		) \
	}

#define agroup_(Type,P) \
	if (ATOMstorage(tt) == TYPE_str) { \
		group_(Type, \
			Type, \
			allocator *ma = h->allocators[p->wid], \
			BATiter bi = bat_iterator(b), \
			Type bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)), \
			(gid)str_hsh(bpi), \
			(h->cmp(vals[g], bpi) != 0), \
			vals[g] = ma_strdup(ma, bpi), \
			bat_iterator_end(&bi), \
			nextk \
		) \
	} else { \
		group_(Type, \
			Type, \
			allocator *ma = h->allocators[p->wid], \
			BATiter bi = bat_iterator(b), \
			void *bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)), \
			(gid)h->hsh(bpi), \
			(h->cmp(vals[g], bpi) != 0), \
			vals[g] = ma_copy(ma, bpi, h->len(bpi)), \
			bat_iterator_end(&bi), \
			nextk \
		) \
	} \

#define agroup(Type) \
	if (ATOMvarsized(tt)) { \
		group_(Type, \
			Type, \
			, \
			BATiter bi = bat_iterator(b), \
			Type bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)), \
			(gid)h->hsh(bpi), \
			(h->cmp(vals[g], bpi) != 0), \
			vals[g] = bpi, \
			bat_iterator_end(&bi), \
			nextk \
		) \
	}

#define afgroup() \
	    assert(h->hsh && h->cmp); \
		int w = b->twidth; \
		group_(char *, \
			char *, \
			, \
			char *ivals = Tloc(b, 0), \
			, \
			(gid)h->hsh(ivals+(i*w)), \
			(h->cmp(vals+(g*w), ivals+(i*w)) != 0), \
			memcpy(vals+(g*w), ivals+(i*w), w), \
			, \
			nextk \
		) \

static str
LALGgroup(Client ctx, bat *rid, bat *uid, const ptr *H, bat *bid/*, bat *sid*/)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H;
	/* private or not */
	bool private = (!*uid || is_bat_nil(*uid)), local_storage = false;
	str err = NULL;
	BAT *u = NULL, *b = NULL;

	b = BATdescriptor(*bid);
	if (!b)
		return createException(MAL, "pp group.group", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (private) { /* TODO ... create but how big ??? */
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		if (!u) {
			err = createException(MAL, "pp group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		u->tsink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, NULL);
		if (u->tsink == NULL) {
			err = createException(MAL, "pp group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		u->tprivate_bat = 1;
	} else {
		u = BATdescriptor(*uid);
		if (!u) {
			err = createException(MAL, "pp group.group", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}
	private = u->tprivate_bat;

	//assert(is_bat_nil(*sid)); /* no cands jet */
	//(void)sid;

	hash_table *h = (hash_table*)u->tsink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if ((ATOMvarsized(u->ttype) && !VIEWvtparent(b)) ||
	    (ATOMvarsized(u->ttype) && BATcount(b) && u->tvheap->parentid != u->batCacheid && u->tvheap->parentid != b->tvheap->parentid) ||
		(ATOMvarsized(u->ttype) && u->twidth != b->twidth)) {
		local_storage = true;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		pipeline_lock(p);
		if (!h->allocators) {
			h->allocators = (allocator**)GDKzalloc(p->p->nr_workers*sizeof(allocator*));
			if (!h->allocators) {
				pipeline_unlock(p);
				err = createException(MAL, "pp group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			} else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			char name[8];
			snprintf(name, sizeof(name), "pp%d", p->wid);
			h->allocators[p->wid] = create_allocator(name, false);
			if (!h->allocators[p->wid]) {
				err = createException(MAL, "pp group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
		}
	} else if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		BATswap_heaps(u, b, p);
	} else {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	}
	if (h) {
		BUN cnt = BATcount(b);

		ATOMIC_BASE_TYPE expected = 0;
		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
		if (g == NULL) {
			err = createException(MAL, "pp group.group", MAL_MALLOC_FAIL);
			goto error;
		}
		if (cnt && !err) {
			ht_activate(h);
			int tt = b->ttype;
			oid *gp = Tloc(g, 0);

			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
			vgroup()
			else group(bit)
			else group(bte)
			else group(sht)
			else group(int)
			else group(date)
			else group(lng)
			else group(oid)
			else group(daytime)
			else group(timestamp)
#ifdef HAVE_HGE
			else group(hge)
#endif
			else fgroup(flt, int)
			else fgroup(dbl, lng)
			else if (ATOMvarsized(tt)) {
				if (local_storage) {
					agroup_(str, p)
				} else {
					agroup(str)
				}
			} else {
				afgroup()
			}
			ht_deactivate(h);
			TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp group.group", RUNTIME_QRY_TIMEOUT));
		}
		if (err || p->p->status) {
			BBPunfix(g->batCacheid);
			/* We don't want to overwrite existing error message.
			 * p->p->status doesn't carry much info. yet.
			 */
			if (!err)
				err = createException(MAL, "pp group.group", "pipeline execution error");
			goto error;
		}
		BATsetcount(g, cnt);
		pipeline_lock2(g);
		BATnegateprops(g);
		/* props */
		gid last = ATOMIC_GET(&h->last);
		/* pass max id */
		g->tmaxval = last;
		pipeline_unlock2(g);
		*uid = u->batCacheid;
		*rid = g->batCacheid;
		BBPkeepref(u);
		BBPkeepref(g);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
  error:
	BBPreclaim(b);
	BBPreclaim(u);
	return err;
}

#define derive_(Type, BaseType, INIT_ALLOCATOR, INIT_ITER, NEW_VAL, HASH_VAL, VAL_NOT_EQUAL, VAL_ASSIGN, ITER_NEXT, NEXTK) \
		int slots = 0; \
		gid slot = 0; \
		INIT_ITER; \
		Type *vals = h->vals; \
		INIT_ALLOCATOR; \
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			gid g = 0; \
			for(; !fnd; ) { \
				NEW_VAL; \
				gid hv = (gid)combine(gi[i], HASH_VAL, prime)&h->mask, k = hv; \
				g = ATOMIC_GET(h->gids+k); \
				for(gid l=1; g && (pgids[g] != gi[i] || VAL_NOT_EQUAL); l++) { \
					NEXTK; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); { \
							vals = h->vals; \
							pgids = h->pgids; \
							prime = hash_prime_nr[h->bits-5]; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					VAL_ASSIGN; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slot--; \
						slots++; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		ITER_NEXT;

#define derive(Type) \
	if (tt == TYPE_##Type) { \
		derive_(Type, \
			Type, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(bp[i]), \
			vals[g] != bp[i], \
			vals[g] = bp[i], \
			, \
			nextk \
		) \
	}

#define vderive() \
	if (tt == TYPE_void) { \
		assert(BATtdense(b)); \
		derive_(oid, \
			oid, \
			, \
			oid bp = b->tseqbase, \
			oid bpi = bp+i, \
			(gid)_hash_oid(bpi), \
			vals[g] != bpi, \
			vals[g] = bpi, \
			, \
			nextk \
		) \
	}

#define fderive(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		derive_(Type, \
			BaseType, \
			, \
		    Type *bp = Tloc(b, 0), \
			, \
			(gid)_hash_##Type(*(((BaseType*)bp)+i)), \
			(!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]), \
			vals[g] = bp[i], \
			, \
			nextk \
		) \
	}

#define aderive_(Type, P) \
	if (ATOMstorage(tt) == TYPE_str) { \
		derive_(Type, \
			Type, \
			allocator *ma = h->allocators[P->wid], \
			BATiter bi = bat_iterator(b), \
			Type bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)), \
			(gid)str_hsh(bpi), \
			(vals[g] && h->cmp(vals[g], bpi) != 0), \
			vals[g] = ma_strdup(ma, bpi), \
			bat_iterator_end(&bi), \
			nextk \
		) \
	} else { \
		derive_(Type, \
			Type, \
			allocator *ma = h->allocators[P->wid], \
			BATiter bi = bat_iterator(b), \
			void *bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)), \
			(gid)h->hsh(bpi), \
			(vals[g] && h->cmp(vals[g], bpi) != 0), \
			vals[g] = ma_copy(ma, bpi, h->len(bpi)), \
			bat_iterator_end(&bi), \
			nextk \
		) \
	} \

#define aderive(Type) \
	if (ATOMvarsized(tt)) { \
		derive_(Type, \
			Type, \
			, \
			BATiter bi = bat_iterator(b), \
			Type bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)), \
			(gid)h->hsh(bpi), \
			(vals[g] && h->cmp(vals[g], bpi) != 0), \
			vals[g] = bpi, \
			bat_iterator_end(&bi), \
			nextk \
		) \
	}


static str
LALGderive(Client ctx, bat *rid, bat *uid, const ptr *H, bat *Gid, bat *Ph, bat *bid /*, bat *sid*/)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H;
	bool private = (!*uid || is_bat_nil(*uid)), local_storage = false;
	str err = NULL;
	BAT *u = NULL;

	BAT *b = BATdescriptor(*bid);
	BAT *G = BATdescriptor(*Gid);
	if (b == NULL || G == NULL) {
		err = createException(MAL, "pp group.group(derive)", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (private) { /* TODO ... create but how big ??? */
		BAT *H = BATdescriptor(*Ph);
		if (!H) {
			err = createException(MAL, "pp group.group(derive)", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		if (!u) {
			BBPunfix(H->batCacheid);
			err = createException(MAL, "pp group.group(derive)", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		/* Lookup parent hash */
		u->tsink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, (hash_table*)H->tsink);
		if (u->tsink == NULL) {
			BBPunfix(H->batCacheid);
			err = createException(MAL, "pp group.group(derive)", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		u->tprivate_bat = 1;
		BBPunfix(H->batCacheid);
	} else {
		u = BATdescriptor(*uid);
		if (!u) {
			err = createException(MAL, "pp group.group(derive)", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}
	private = u->tprivate_bat;
	//assert(is_bat_nil(*sid)); /* no cands jet */
	//(void)sid;

	hash_table *h = (hash_table*)u->tsink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if ((ATOMvarsized(u->ttype) && !VIEWvtparent(b)) ||
	    (ATOMvarsized(u->ttype) && BATcount(b) && u->tvheap->parentid != u->batCacheid && u->tvheap->parentid != b->tvheap->parentid) ||
		(ATOMvarsized(u->ttype) && u->twidth != b->twidth)) {
		local_storage = true;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		pipeline_lock(p);
		if (!h->allocators) {
			h->allocators = (allocator**)GDKzalloc(p->p->nr_workers*sizeof(allocator*));
			if (!h->allocators) {
				pipeline_unlock(p);
				err = createException(MAL, "pp group.group(derive)", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			} else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			char name[8];
			snprintf(name, sizeof(name), "pp%d", p->wid);
			h->allocators[p->wid] = create_allocator(name, false);
			if (!h->allocators[p->wid]) {
				err = createException(MAL, "pp group.group(derive)", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
		}
	} else if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		BATswap_heaps(u, b, p);
	} else {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	}
	if (h) {
		BUN cnt = BATcount(b);

		assert(cnt == BATcount(G));
		ATOMIC_BASE_TYPE expected = 0;
		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
		if (g == NULL) {
			err = createException(MAL, "pp group.group(derive)", MAL_MALLOC_FAIL);
			goto error;
		}
		if (cnt && !err) {
			ht_activate(h);
			int tt = b->ttype;
			oid *gp = Tloc(g, 0);
			gid *gi = Tloc(G, 0);
			gid *pgids = h->pgids;
			int prime = hash_prime_nr[h->bits-5];

			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
			vderive()
			else derive(bit)
			else derive(bte)
			else derive(sht)
			else derive(int)
			else derive(date)
			else derive(lng)
			else derive(oid)
			else derive(daytime)
			else derive(timestamp)
#ifdef HAVE_HGE
			else derive(hge)
#endif
			else fderive(flt, int)
			else fderive(dbl, lng)
			else if (ATOMvarsized(tt)) {
				if (local_storage) {
					aderive_(str,p)
				} else {
					aderive(str)
				}
			} else {
				err = createException(MAL, "pp group.derive", "Type (%s) not handled yet\n", ATOMname(tt));
			}
			ht_deactivate(h);
			TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp group.group(derive)", RUNTIME_QRY_TIMEOUT));
		}
		if (err || p->p->status) {
			BBPunfix(g->batCacheid);
			/* We don't want to overwrite existing error message.
			 * p->p->status doesn't carry much info. yet.
			 */
			if (!err)
				err = createException(MAL, "pp group.group(derive)", "pipeline execution error");
			goto error;
		}
		BATsetcount(g, cnt);
		pipeline_lock2(g);
		BATnegateprops(g);
		/* props */
		gid last = ATOMIC_GET(&h->last);
		/* pass max id */
		g->tmaxval = last;
		pipeline_unlock2(g);
		*uid = u->batCacheid;
		*rid = g->batCacheid;
		BBPkeepref(u);
		BBPkeepref(g);
	}
	BBPunfix(b->batCacheid);
	BBPunfix(G->batCacheid);
	return MAL_SUCCEED;
  error:
	BBPreclaim(u);
	BBPreclaim(b);
	BBPreclaim(G);
	return err;
}

#define projectconst(Type) \
	do { \
		Type v = *getArgReference_##Type(stk, pci, r); \
		Type *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			oid gi = g->tseqbase; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				o[gi + i] = v; \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				o[gp[i]] = v; \
			} \
		} \
	} while(0)

#define aprojectconst(Type,w,Toff) \
	do { \
		if ((ATOMvarsized(tt) || ATOMstorage(tt) == TYPE_##Type) && r->twidth == w) { \
			Toff v = *getArgReference_##Type(stk, pci, r); \
			Toff *o = Tloc(r, 0); \
			if (g->ttype == TYPE_void) { \
				oid gi = g->tseqbase; \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[gi + i] = v; \
			} else { \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[gp[i]] = v; \
			} \
		} \
	} while(0)

/* runs locked ie resizes should work */
#define aprojectconst_(Type) \
	do { \
		if ((ATOMvarsized(tt) || ATOMstorage(tt) == TYPE_##Type)) { \
			BATiter bi = bat_iterator(b); \
			int ins = 0; \
			if (g->ttype == TYPE_void) { \
				oid gi = g->tseqbase; \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
					int w = r->twidth; \
					if(w == 1) { \
						uint8_t *o = Tloc(r, 0); \
						ins = (o[gi + i] == 0); \
					} else if (w == 2) { \
						uint16_t *o = Tloc(r, 0); \
						ins = (o[gi + i] == 0); \
					} else if (w == 4) { \
						uint32_t *o = Tloc(r, 0); \
						ins = (o[gi + i] == 0); \
					} else { \
						var_t *o = Tloc(r, 0); \
						ins = (o[gi + i] == 0); \
					} \
					if (ins && tfastins_nocheckVAR( r, gi + i, BUNtvar(&bi, i)) != GDK_SUCCEED) { \
						err = createException(MAL, "pp algebra.projection", MAL_MALLOC_FAIL);\
						goto error; \
					} \
					if (err) \
					TIMEOUT_LOOP_BREAK; \
				} \
			} else { \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
					int w = r->twidth; \
					if(w == 1) { \
						uint8_t *o = Tloc(r, 0); \
						ins = (o[gp[i]] == 0); \
					} else if (w == 2) { \
						uint16_t *o = Tloc(r, 0); \
						ins = (o[gp[i]] == 0); \
					} else if (w == 4) { \
						uint32_t *o = Tloc(r, 0); \
						ins = (o[gp[i]] == 0); \
					} else { \
						var_t *o = Tloc(r, 0); \
						ins = (o[gp[i]] == 0); \
					} \
					if (ins && tfastins_nocheckVAR( r, gp[i], BUNtvar(&bi, i)) != GDK_SUCCEED) { \
						err = createException(MAL, "pp algebra.projection", MAL_MALLOC_FAIL);\
						goto error; \
					} \
					if (err) \
					TIMEOUT_LOOP_BREAK; \
				} \
			} \
			bat_iterator_end(&bi); \
		} \
	} while(0)

/* inout := algebra.project(groupid, val, PTR)  */
/* this (possibly) overwrites the values, therefor for expensive (var) types we
 * only write offsets (ie use the heap from the parent) */
static str
//LALGconstant(bat *rid, bat *gid, void *val, const ptr *H)
LALGconstant(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)ctx;

	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *gid = getArgReference_bat(stk, pci, 1);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 3); /* last arg should move to first argument .. */

	BAT *g = NULL, *r = NULL;
	str err = NULL;
	bool private = true, locked = false;

	g = BATdescriptor(*gid);
	if (g == NULL) {
		err = createException(MAL, "pp algebra.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
//	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err = createException(MAL, "pp algebra.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
//	}

	int tt = getArgType(mb, pci, 2);
	if (!tt) tt = TYPE_oid;

	oid max = BATcount(g)?g->tmaxval:0;
	/* probably need bat resize and create hash */
	private = (!r || r->tprivate_bat);

	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

#if 0
	if (!r) {
		r = COLnew(0, tt, max, TRANSIENT);
		if (r == NULL) {
			err = createException(MAL, "pp algebra.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		assert(private);
		r->tprivate_bat = 1;
	}
#endif

	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp algebra.project", MAL_MALLOC_FAIL);
			goto error;
		}
	}

	/* only do the expensive memset for strings, to know when there are already
	 * values at certain locations to avoid duplicate writes */
	BUN cnt = BATcount(r);
	if (ATOMvarsized(r->ttype) && cnt < max)
		memset(Tloc(r, cnt), 0, r->twidth*(max-cnt));

	cnt = BATcount(g);
	if (cnt) {
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		void *val = getArgReference(stk, pci, 2);
		BAT *v = BATconstant(0, tt, val, cnt, TRANSIENT);
		if (v == NULL) {
			err = createException(MAL, "pp algebra.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		if (BATupdate(r, g, v, false) != GDK_SUCCEED) {
			err = createException(MAL, "pp algebra.project", SQLSTATE(HY002) OPERATION_FAILED);
			goto error;
		}

		if (!err)
			TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp algebra.project", RUNTIME_QRY_TIMEOUT));
	}
	if (err || p->p->status) {
		if (!err)
			err = createException(MAL, "pp algebra.project", "pipeline execution error");
		goto error;
	}

	if (!private)
		pipeline_lock2(r);
	if (cnt && BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	//*rid = r->batCacheid;
	//BBPkeepref(r);
	//leave writable
	BBPretain(*rid = r->batCacheid);
	BBPunfix(r->batCacheid);

	if (locked)
		pipeline_unlock1(r);

	BBPunfix(g->batCacheid);
	return MAL_SUCCEED;
  error:
	if (locked)
		pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

#define project(Type) \
	if (ATOMstorage(tt) == TYPE_##Type) { \
		Type *v = Tloc(b, 0); \
		Type *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			oid gi = g->tseqbase; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[gi + i] = v[i]; \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[gp[i]] = v[i]; \
		} \
	}

#define vproject() \
	if (tt == TYPE_void) { \
		oid vi = b->tseqbase; \
		oid *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			oid gi = g->tseqbase; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[gi + i] = vi + i; \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[gp[i]] = vi + i; \
		} \
	}

#define aproject(Type,w,Toff) \
	if ((ATOMvarsized(tt) || ATOMstorage(tt) == TYPE_##Type) && b->twidth == w) { \
		assert(b->twidth == r->twidth);\
		Toff *v = Tloc(b, 0); \
		Toff *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			oid gi = g->tseqbase; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[gi + i] = v[i]; \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[gp[i]] = v[i]; \
		} \
	}

/* runs locked ie resizes should work */
#define aproject_(Type) \
	if ((ATOMvarsized(tt) || ATOMstorage(tt) == TYPE_##Type)) { \
		BATiter bi = bat_iterator(b); \
		int ins = 0; \
		if (g->ttype == TYPE_void) { \
			oid gi = g->tseqbase; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				int w = r->twidth; \
				if(w == 1) { \
					uint8_t *o = Tloc(r, 0); \
					ins = (o[gi + i] == 0); \
				} else if (w == 2) { \
					uint16_t *o = Tloc(r, 0); \
					ins = (o[gi + i] == 0); \
				} else if (w == 4) { \
					uint32_t *o = Tloc(r, 0); \
					ins = (o[gi + i] == 0); \
				} else { \
					var_t *o = Tloc(r, 0); \
					ins = (o[gi + i] == 0); \
				} \
				if (ins && tfastins_nocheckVAR( r, gi + i, BUNtvar(&bi, i)) != GDK_SUCCEED) { \
					err = createException(MAL, "pp algebra.projection", MAL_MALLOC_FAIL);\
					goto error; \
				} \
				if (err) \
					TIMEOUT_LOOP_BREAK; \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				int w = r->twidth; \
				if(w == 1) { \
					uint8_t *o = Tloc(r, 0); \
					ins = (o[gp[i]] == 0); \
				} else if (w == 2) { \
					uint16_t *o = Tloc(r, 0); \
					ins = (o[gp[i]] == 0); \
				} else if (w == 4) { \
					uint32_t *o = Tloc(r, 0); \
					ins = (o[gp[i]] == 0); \
				} else { \
					var_t *o = Tloc(r, 0); \
					ins = (o[gp[i]] == 0); \
				} \
				if (ins && tfastins_nocheckVAR( r, gp[i], BUNtvar(&bi, i)) != GDK_SUCCEED) { \
					err = createException(MAL, "pp algebra.projection", MAL_MALLOC_FAIL);\
					goto error; \
				} \
				if (err) \
					TIMEOUT_LOOP_BREAK; \
			} \
		} \
		bat_iterator_end(&bi); \
	}

/* result := algebra.projection(groupid, input, PTR)  */
/* this (possibly) overwrites the values, therefor for expensive (var) types we
 * only write offsets (ie use the heap from the parent) */
static str
LALGproject(Client ctx, bat *rid, bat *gid, bat *bid, const ptr *H)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = NULL, *b = NULL, *r = NULL;
	str err = NULL;
	bool private = true, local_storage = false, locked = false;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err = createException(MAL, "pp algebra.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err = createException(MAL, "pp algebra.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}

	int tt = b->ttype;
	oid max = BATcount(g)?g->tmaxval:0;
	/* probably need bat resize and create hash */
	private = (!r || r->tprivate_bat);

	if (b && (b->ttype == TYPE_msk || mask_cand(b))) {
		/* todo check all pp functions on msk/mask_cand (next too TYPE_void) */
		BAT *t = BATunmask(b);
		if (!t) {
			err = createException(MAL, "pp algebra.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		BBPreclaim(b);
		b = t;
		tt = b->ttype;
	}
	if (!tt && BATtdense(b))
		tt = TYPE_oid;
	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

	assert(b->hseqbase == g->hseqbase);
	if (r && BATcount(b)) {
		MT_lock_set(&r->theaplock);
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b, true)) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			err = createException(MAL, "pp algebra.projection", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		} else if (ATOMvarsized(r->ttype) && r->tvheap->parentid == r->batCacheid && (BATcount(r) ||
				(!VIEWvtparent(b) || BBP_desc(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && r->tvheap->parentid != r->batCacheid &&
				r->tvheap->parentid != b->tvheap->parentid) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			if (unshare_varsized_heap(r) != GDK_SUCCEED) {
				err = createException(MAL, "pp algebra.projection", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			BATswap_heaps(r, b, p);
			assert(r->twidth == b->twidth);
		} else {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
		}
	} else if (!r) {
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(tt) && VIEWvtparent(b) && BBP_desc(VIEWvtparent(b))->batRestricted == BAT_READ) {
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, tt, max, TRANSIENT, b->twidth);
			if (r == NULL) {
				err = createException(MAL, "pp algebra.projection", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			local_storage = true;
			r = COLnew2(0, tt, max, TRANSIENT, b->twidth);
			if (r == NULL) {
				err = createException(MAL, "pp algebra.projection", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			if (r->tvheap && r->tvheap->base == NULL &&
				ATOMheap(r->ttype, r->tvheap, r->batCapacity) != GDK_SUCCEED) {
				err = createException(MAL, "pp algebra.projection", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
		}
		assert(private);
		r->tprivate_bat = 1;
	}

	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp algebra.projection", MAL_MALLOC_FAIL);
			goto error;
		}
	}

	/* get max id from gid */
	if (ATOMvarsized(r->ttype) && cnt < max)
		memset(Tloc(r, cnt), 0, r->twidth*(max-cnt));
	cnt = BATcount(b);
	if (!tt)
		r->tseqbase = b->tseqbase;
	if (tt && cnt) {
		oid *gp = Tloc(g, 0);
		tt = b->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		vproject()
			project(bte)
			project(sht)
			project(int)
			project(inet4)
			project(lng)
#ifdef HAVE_HGE
			project(hge)
#endif
			project(inet6)
			project(flt)
			project(dbl)
			if (local_storage) {
				if (!private)
					pipeline_lock2(r);
				if (BATcount(r) < max)
					BATsetcount(r, max);
				if (!private)
					pipeline_unlock2(r);
				aproject_(str)
			} else {
				aproject(str,1,uint8_t)
					aproject(str,2,uint16_t)
					aproject(str,4,uint32_t)
					aproject(str,8,var_t)
			}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp algebra.projection", RUNTIME_QRY_TIMEOUT));
	}
	if (err)
		goto error;

	if (!private)
		pipeline_lock2(r);
	if (cnt && BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	*rid = r->batCacheid;
	BBPkeepref(r);

	if (locked)
		pipeline_unlock1(r);

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	return MAL_SUCCEED;
  error:
	if (locked)
		pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

static str
LALGcountstar(Client ctx, bat *rid, bat *gid, const ptr *H, bat *pid)
{
	(void)ctx;
	//Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	(void)H;
	BAT *r = NULL, *g = NULL;
	str err = NULL;
	bool private = true, locked = false;

	if ((g = BATdescriptor(*gid)) == NULL) {
		err = createException(MAL, "pp aggr.count(star)", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err = createException(MAL, "pp aggr.count(star)", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}
	private = (!r || r->tprivate_bat);

	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

	BAT *pg = BATdescriptor(*pid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.count(star)", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg)?pg->tmaxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		r = COLnew(0, TYPE_lng, max, TRANSIENT);
		if (r == NULL) {
			err = createException(MAL, "pp aggr.count(star)", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		r->tprivate_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.count(star)", MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (cnt < max)
		memset(Tloc(r, cnt), 0, sizeof(lng)*(max-cnt));

	cnt = BATcount(g);

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	oid *v = Tloc(g, 0);
	lng *o = Tloc(r, 0);
	if (!BATtdense(g)) {
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx)
			o[v[i]]++;
	} else {
		oid b = g->tseqbase;
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx)
			o[b+i]++;
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.count(star)", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(g->batCacheid);
	if (!private)
		pipeline_lock2(r);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	*rid = r->batCacheid;
	BBPkeepref(r);

	if (!private)
		pipeline_unlock1(r);
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

#define gcount(Type) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[v[i]]+= (!is_##Type##_nil(in[i])); \
	}

#define gfcount(Type) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				o[v[i]]+= cmp(in+i, &Type##_nil) != 0; \
	}

#define gacount(Type) \
	if (tt == TYPE_##Type) { \
			BATiter bi = bat_iterator(b); \
			const void *nil = ATOMnilptr(tt); \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				o[v[i]]+= cmp(BUNtvar(&bi, i), nil)!=0; \
			} \
			bat_iterator_end(&bi); \
	}

static str
LALGcount(Client ctx, bat *rid, bat *gid, bat *bid, bit *nonil, const ptr *H, bat *pid)
{
	//Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	if (!(*nonil))
		return LALGcountstar(ctx, rid, gid, H, pid);

	/* use bid to check for null values */
	BAT *g = NULL, *b = NULL, *r = NULL;
	str err = NULL;
	bool private = true, locked = false;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err =  createException(MAL, "pp aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err =  createException(MAL, "pp aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}
	private = (!r || r->tprivate_bat);

	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

	BAT *pg = BATdescriptor(*pid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg)?pg->tmaxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		r = COLnew(0, TYPE_lng, max, TRANSIENT);
		if (r == NULL) {
			err = createException(MAL, "pp aggr.count", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		r->tprivate_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.count", MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (cnt < max)
		memset(Tloc(r, cnt), 0, sizeof(lng)*(max-cnt));

	cnt = BATcount(g);

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	oid *v = Tloc(g, 0);
	lng *o = Tloc(r, 0);
	if (BATtdense(b) && b->tnonil) {
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx)
			o[v[i]]++;
	} else { /* per type */
		int tt = b->ttype;

		gcount(bit);
		gcount(bte);
		gcount(sht);
		gcount(int);
		gcount(date);
		gfcount(inet4);
		gcount(lng);
		gcount(daytime);
		gcount(timestamp);
#ifdef HAVE_HGE
		gcount(hge);
#endif
		gfcount(uuid);
		gcount(flt);
		gcount(dbl);
		gacount(str);
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.count", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (!private)
		pipeline_lock2(r);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	*rid = r->batCacheid;
	BBPkeepref(r);

	if (!private)
		pipeline_unlock1(r);
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

/* TODO do we need to split out the nil check, ie for when we know there are no nils */
#define gsum(OutType, InType) \
	if (tt == TYPE_##InType && ot == TYPE_##OutType) { \
			InType *in = Tloc(b, 0); \
			OutType *o = Tloc(r, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				if (!is_##InType##_nil(in[i])) { \
					if (is_##OutType##_nil(o[grp[i]])) \
						o[grp[i]] = in[i]; \
					else \
						o[grp[i]] += in[i]; \
				} \
	}

static str
//LALGsum(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
LALGsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *gid = getArgReference_bat(stk, pci, 1);
	bat *bid = getArgReference_bat(stk, pci, 2);
	//Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 3); /* last arg should move to first argument .. */
	bat *pid = getArgReference_bat(stk, pci, 4);
	BAT *b = NULL, *g = NULL, *r = NULL;
	str err = NULL;
	bool private = true, locked = false;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err =  createException(MAL, "pp aggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err =  createException(MAL, "pp aggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}
	private = (!r || r->tprivate_bat);

	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

	BAT *pg = BATdescriptor(*pid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg)?pg->tmaxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		int tt = getBatType(getArgType(mb, pci, 0));
		r = COLnew(b->hseqbase, tt, max+1, TRANSIENT);
		if (r == NULL) {
			err = createException(MAL, "pp aggr.sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		r->tprivate_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.sum", MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (cnt < max) {
		char *d = Tloc(r, 0);
		const char *nil = ATOMnilptr(r->ttype);
		for (BUN i=cnt; i<max; i++)
			memcpy(d+(i<<r->tshift), nil, r->twidth);
	}

	cnt = BATcount(g);
	int tt = b->ttype, ot = r->ttype;

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	oid *grp = Tloc(g, 0);

	gsum(bte,bte);
	gsum(sht,bte);
	gsum(sht,sht);
	gsum(int,bte);
	gsum(int,sht);
	gsum(int,int);
	gsum(lng,bte);
	gsum(lng,sht);
	gsum(lng,int);
	gsum(lng,lng);
#ifdef HAVE_HGE
	gsum(hge,bte);
	gsum(hge,sht);
	gsum(hge,int);
	gsum(hge,lng);
	gsum(hge,hge);
#endif
	gsum(flt,flt);
	gsum(dbl,flt);
	gsum(dbl,dbl);
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.sum", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (!private)
		pipeline_lock2(r);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	*rid = r->batCacheid;
	BBPkeepref(r);
	if (!private)
		pipeline_unlock1(r);
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

#define gprod(OutType, InType) \
	if (tt == TYPE_##InType && ot == TYPE_##OutType) { \
			InType *in = Tloc(b, 0); \
			OutType *o = Tloc(r, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				if (!is_##InType##_nil(in[i])) { \
					if (is_##OutType##_nil(o[grp[i]])) \
						o[grp[i]] = in[i]; \
					else \
						o[grp[i]] *= in[i]; \
				} \
	}

static str
//LALGprod(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
LALGprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *gid = getArgReference_bat(stk, pci, 1);
	bat *bid = getArgReference_bat(stk, pci, 2);
	//Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 3); /* last arg should move to first argument .. */
	bat *pid = getArgReference_bat(stk, pci, 4);
	BAT *b = NULL, *g = NULL, *r = NULL;
	str err = NULL;
	bool private = true, locked = false;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err = createException(MAL, "pp aggr.prod", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err = createException(MAL, "pp aggr.prod", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}
	private = (!r || r->tprivate_bat);

	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

	BAT *pg = BATdescriptor(*pid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.prod", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg)?pg->tmaxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		int tt = getBatType(getArgType(mb, pci, 0));
		r = COLnew(b->hseqbase, tt, max, TRANSIENT);
		if (r == NULL) {
			err = createException(MAL, "pp aggr.prod", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		r->tprivate_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.prod", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (cnt < max) {
		char *d = Tloc(r, 0);
		const char *nil = ATOMnilptr(r->ttype);
		for (BUN i=cnt; i<max; i++)
			memcpy(d+(i*r->twidth), nil, r->twidth);
	}

	cnt = BATcount(g);
	int tt = b->ttype, ot = r->ttype;

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	oid *grp = Tloc(g, 0);

	gprod(lng,bte);
	gprod(lng,sht);
	gprod(lng,int);
	gprod(lng,lng);
#ifdef HAVE_HGE
	gprod(hge,bte);
	gprod(hge,sht);
	gprod(hge,int);
	gprod(hge,lng);
	gprod(hge,hge);
#endif
	gprod(flt,flt);
	gprod(dbl,dbl);
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.prod", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (!private)
		pipeline_lock2(r);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	*rid = r->batCacheid;
	BBPkeepref(r);

	if (!private)
		pipeline_unlock1(r);
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

static str
// for ints
//avg_core(bat *rid, bat *rrem, bat *rcnt, bat *gid, bat *bid, ptr *H, bat *pid)
//avg_combine(bat *rid, bat *rrem, bat *rcnt, bat *gid, bat *bid, bat *rem, bat *cnt, ptr *H, bat *pid)
// for floats
//fsum_core(bat *rsum, bat *rcom, bat *rcnt, bat *gid, bat *val, ptr *H, bat *pid);
//fsum_combine(bat *rsum, bat *rcom, bat *rcnt, bat *gid, bat *sum, bat *com, bat *cnt, ptr *H, bat *pid);
LALGavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *rrem = getArgReference_bat(stk, pci, 1);
	bat *rcid = getArgReference_bat(stk, pci, 2);
	bat *gid = getArgReference_bat(stk, pci, 3);
	bat *bid = getArgReference_bat(stk, pci, 4);
	bat *rem = pci->argc == 9 ? getArgReference_bat(stk, pci, 5) : NULL;
	bat *cid = pci->argc == 9 ? getArgReference_bat(stk, pci, 6) : NULL;
	Pipeline *p = (Pipeline *) *getArgReference_ptr(stk, pci, pci->argc - 2);
	bat *pgid = getArgReference_bat(stk, pci, pci->argc - 1);
	BAT *b = BATdescriptor(*bid);
	BAT *g = BATdescriptor(*gid);
	BAT *c = cid ? BATdescriptor(*cid) : NULL;
	BAT *r = rem ? BATdescriptor(*rem) : NULL;
	BAT *bn = (*rid && !is_bat_nil(*rid)) ? BATdescriptor(*rid) : NULL;
	BAT *cn = bn ? BATdescriptor(*rcid) : NULL;
	BAT *rn = bn ? BATdescriptor(*rrem) : NULL;
	bool private = (bn == NULL || bn->tprivate_bat), locked = false;
	str err = NULL;

	if (!private) {
		pipeline_lock1(bn);
		locked = true;
	}

	if (b == NULL ||
	    g == NULL ||
	    (cid && c == NULL) ||
	    (rem && r == NULL) ||
	    ((*rid && !is_bat_nil(*rid)) && bn == NULL) ||
	    (bn && cn == NULL) ||
	    ((bn && rrem) && rn == NULL)) {
		err = createException(MAL, "pp aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	BAT *pg = BATdescriptor(*pgid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg) ? pg->tmaxval : 0;
	BBPunfix(pg->batCacheid);

	if ((b->ttype == TYPE_flt || b->ttype == TYPE_dbl) && pci->argc == 7) {
		/* float avg core */
		if (!bn) {
			bn = COLnew(0, TYPE_dbl, max, TRANSIENT);
			cn = COLnew(0, TYPE_lng, max, TRANSIENT);
			rn = COLnew(0, TYPE_dbl, max, TRANSIENT);
			if (bn == NULL || cn == NULL || rn == NULL) {
				err = createException(MAL, "pp aggr.avg", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			bn->tprivate_bat = 1;
			cn->tprivate_bat = 1;
			rn->tprivate_bat = 1;
		}
		if (bn->batCount < max &&
			(BATextend(bn, max) != GDK_SUCCEED ||
			 BATextend(cn, max) != GDK_SUCCEED ||
			 BATextend(rn, max) != GDK_SUCCEED)) {
			err = createException(MAL, "pp aggr.avg", GDK_EXCEPTION);
			goto error;
		}
		lng *rcnts = Tloc(cn, 0);
		lng *rerrs = Tloc(rn, 0);
		dbl *rvals = Tloc(bn, 0);
		oid *grps = Tloc(g, 0);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		int overflow = 0;
		switch (ATOMbasetype(b->ttype)) {
			case TYPE_flt: {
				flt *vals = Tloc(b, 0);
				for (oid i = bn->batCount; i < max; i++) {
					rvals[i] = dbl_nil;
					rcnts[i] = 0;
					rerrs[i] = 0;
				}
				TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
					avg_aggr_float(flt, dbl, vals[i], rvals[grps[i]], rerrs[grps[i]], rcnts[grps[i]]);
				}
				break;
			}
			case TYPE_dbl: {
				dbl *vals = Tloc(b, 0);
				for (oid i = bn->batCount; i < max; i++) {
					rvals[i] = dbl_nil;
					rcnts[i] = 0;
					rerrs[i] = 0;
				}
				TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
					avg_aggr_float(dbl, dbl, vals[i], rvals[grps[i]], rerrs[grps[i]], rcnts[grps[i]]);
				}
				break;
			}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.avg", RUNTIME_QRY_TIMEOUT));
		if (overflow)
			err = createException(SQL, "pp aggr.avg", "Overflow in avg()");
		if (err) goto error;

		pipeline_lock2(bn);
		BATnegateprops(bn);
		pipeline_unlock2(bn);
		pipeline_lock2(rn);
		BATnegateprops(rn);
		pipeline_unlock2(rn);
		pipeline_lock2(cn);
		BATnegateprops(cn);
		pipeline_unlock2(cn);
		if (BATcount(bn) < max) {
			BATsetcount(bn, max);
			BATsetcount(rn, max);
			BATsetcount(cn, max);
		}
	} else if ((b->ttype == TYPE_flt || b->ttype == TYPE_dbl) && pci->argc == 9) {
		/* float avg combine */
		if (bn->batCount < max &&
			(BATextend(bn, max) != GDK_SUCCEED ||
			 BATextend(cn, max) != GDK_SUCCEED ||
			 BATextend(rn, max) != GDK_SUCCEED)) {
			err = createException(MAL, "pp aggr.avg", GDK_EXCEPTION);
			goto error;
		}
		dbl *vals = Tloc(b, 0);
		lng *cnts = Tloc(c, 0);
		dbl *errs = Tloc(r, 0);
		lng *rcnts = Tloc(cn, 0);
		lng *rerrs = Tloc(rn, 0);
		dbl *rvals = Tloc(bn, 0);
		oid *grps = Tloc(g, 0);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		int overflow = 0;
		assert(b->ttype == TYPE_dbl);
		for (oid i = bn->batCount; i < max; i++) {
			rvals[i] = dbl_nil;
			rcnts[i] = 0;
			rerrs[i] = 0;
		}
		TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
			avg_aggr_float_comb(dbl, vals[i], errs[i], cnts[i], rvals[grps[i]], rerrs[grps[i]], rcnts[grps[i]]);
		}
		if (overflow)
			err = createException(SQL, "pp aggr.avg", "Overflow in avg()");
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.avg", RUNTIME_QRY_TIMEOUT));
		if (err) goto error;

		pipeline_lock2(bn);
		BATnegateprops(bn);
		pipeline_unlock2(bn);
		pipeline_lock2(rn);
		BATnegateprops(rn);
		pipeline_unlock2(rn);
		pipeline_lock2(cn);
		BATnegateprops(cn);
		pipeline_unlock2(cn);
		if (BATcount(bn) < max) {
			BATsetcount(bn, max);
			BATsetcount(rn, max);
			BATsetcount(cn, max);
		}
	} else if (pci->retc == 3 && pci->argc == 7) {
		if (BATgroupavg3(&bn, &rn, &cn, b, g, NULL, NULL, true, bn != NULL) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.avg", GDK_EXCEPTION);
			goto error;
		}
		pipeline_lock2(bn);
		BATnegateprops(bn);
		pipeline_unlock2(bn);
		pipeline_lock2(rn);
		BATnegateprops(rn);
		pipeline_unlock2(rn);
		pipeline_lock2(cn);
		BATnegateprops(cn);
		pipeline_unlock2(cn);
	} else if (pci->retc == 3 && pci->argc == 9) {
		if (bn->batCount < max &&
			(BATextend(bn, max) != GDK_SUCCEED ||
			 BATextend(cn, max) != GDK_SUCCEED ||
			 BATextend(rn, max) != GDK_SUCCEED)) {
			err = createException(MAL, "pp aggr.avg", GDK_EXCEPTION);
			goto error;
		}
		lng *cnts = Tloc(c, 0);
		lng *rems = Tloc(r, 0);
		lng *rcnts = Tloc(cn, 0);
		lng *rrems = Tloc(rn, 0);
		oid *grps = Tloc(g, 0);
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte: {
			bte *vals = Tloc(b, 0);
			bte *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = bte_nil;
				rcnts[i] = 0;
				rrems[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_comb(bte, vals[i], rems[i], cnts[i],
							  rvals[grps[i]], rrems[grps[i]], rcnts[grps[i]]);
			}
			break;
		}
		case TYPE_sht: {
			sht *vals = Tloc(b, 0);
			sht *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = sht_nil;
				rcnts[i] = 0;
				rrems[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_comb(sht, vals[i], rems[i], cnts[i],
							  rvals[grps[i]], rrems[grps[i]], rcnts[grps[i]]);
			}
			break;
		}
		case TYPE_int: {
			int *vals = Tloc(b, 0);
			int *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = int_nil;
				rcnts[i] = 0;
				rrems[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_comb(int, vals[i], rems[i], cnts[i],
							  rvals[grps[i]], rrems[grps[i]], rcnts[grps[i]]);
			}
			break;
		}
		case TYPE_lng: {
			lng *vals = Tloc(b, 0);
			lng *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = lng_nil;
				rcnts[i] = 0;
				rrems[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_comb(lng, vals[i], rems[i], cnts[i],
							  rvals[grps[i]], rrems[grps[i]], rcnts[grps[i]]);
			}
			break;
		}
#ifdef HAVE_HGE
		case TYPE_hge: {
			hge *vals = Tloc(b, 0);
			hge *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = hge_nil;
				rcnts[i] = 0;
				rrems[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_comb(hge, vals[i], rems[i], cnts[i],
							  rvals[grps[i]], rrems[grps[i]], rcnts[grps[i]]);
			}
			break;
		}
#endif
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.avg", RUNTIME_QRY_TIMEOUT));
		if (err) goto error;

		pipeline_lock2(bn);
		BATnegateprops(bn);
		pipeline_unlock2(bn);
		pipeline_lock2(rn);
		BATnegateprops(rn);
		pipeline_unlock2(rn);
		pipeline_lock2(cn);
		BATnegateprops(cn);
		pipeline_unlock2(cn);
		if (BATcount(bn) < max) {
			BATsetcount(bn, max);
			BATsetcount(rn, max);
			BATsetcount(cn, max);
		}
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (c)
		BBPunfix(c->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn) {
		*rid = bn->batCacheid;
		BBPkeepref(bn);
	}
	if (cn) {
		*rcid = cn->batCacheid;
		BBPkeepref(cn);
	}
	if (rn) {
		*rrem = rn->batCacheid;
		BBPkeepref(rn);
	}

	if (!private)
		pipeline_unlock1(bn);
	else
		bn->tprivate_bat = true; /* in case it's a new one, set the bit */

	(void)p;

	(void)cntxt;
	(void)mb;
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(bn);
	if (b) BBPunfix(b->batCacheid);
	if (g) BBPunfix(g->batCacheid);
	if (c) BBPunfix(c->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	if (bn) BBPunfix(bn->batCacheid);
	if (cn) BBPunfix(cn->batCacheid);
	if (rn) BBPunfix(rn->batCacheid);
	return err;
}

static str
//compute_avg(bat *ravg, bat *avg, bat *rem, bat *cnt)
compute_avg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *ravg = getArgReference_bat(stk, pci, 0);
	bat *avg = getArgReference_bat(stk, pci, 1);
	bat *rem = getArgReference_bat(stk, pci, 2);
	bat *cnt = getArgReference_bat(stk, pci, 3);
	str err = NULL;

	BAT *a = BATdescriptor(*avg);
	BAT *r = BATdescriptor(*rem);
	BAT *c = BATdescriptor(*cnt);

	if (!a || !r || !c)
		err = createException(MAL, "pp aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!err) {
		BUN n = BATcount(a);
		assert(n == BATcount(r) && n == BATcount(c));
		BAT *res = COLnew(0, TYPE_dbl, n, TRANSIENT);

		if (!res) {
			err = createException(MAL, "pp aggr.avg", MAL_MALLOC_FAIL);
			goto bail;
		}

		lng *cr = Tloc(r, 0), *cc = Tloc(c, 0);
		dbl *cres = Tloc(res, 0);

		if (a->ttype == TYPE_bte) {
			bte *ca = Tloc(a, 0);
			for(BUN i = 0; i<n; i++)
				cres[i] = cc[i] ? ((dbl)ca[i]) + ((dbl)cr[i])/cc[i] : dbl_nil;
		} else if (a->ttype == TYPE_sht) {
			sht *ca = Tloc(a, 0);
			for(BUN i = 0; i<n; i++)
				cres[i] = cc[i] ? ((dbl)ca[i]) + ((dbl)cr[i])/cc[i] : dbl_nil;
		} else if (a->ttype == TYPE_int) {
			int *ca = Tloc(a, 0);
			for(BUN i = 0; i<n; i++)
				cres[i] = cc[i] ? ((dbl)ca[i]) + ((dbl)cr[i])/cc[i] : dbl_nil;
		} else if (a->ttype == TYPE_lng) {
			lng *ca = Tloc(a, 0);
			for(BUN i = 0; i<n; i++)
				cres[i] = cc[i] ? ((dbl)ca[i]) + ((dbl)cr[i])/cc[i] : dbl_nil;
		}
#ifdef HAVE_HGE
		else if (a->ttype == TYPE_hge) {
			lng *ca = Tloc(a, 0);
			for(BUN i = 0; i<n; i++)
				cres[i] = cc[i] ? ((dbl)ca[i]) + ((dbl)cr[i])/cc[i] : dbl_nil;
		}
#endif
		else if (a->ttype == TYPE_dbl) {
			dbl *ca = Tloc(a, 0);
			dbl *ce = (dbl*)cr;
			for(BUN i = 0; i<n; i++)
				cres[i] = cc[i] ? (ca[i] + ce[i])/cc[i] : dbl_nil;
		}
		else {
			err = createException(MAL, "pp aggr.avg", "Only supports numerical input types");
			BBPreclaim(res);
			goto bail;
		}
		BATsetcount(res, n);
		BATnegateprops(res);
		*ravg = res->batCacheid;
		BBPkeepref(res);
	}
bail:
	BBPreclaim(a);
	BBPreclaim(r);
	BBPreclaim(c);
	return err;
}

static str
LALGsum_float(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *rrem = getArgReference_bat(stk, pci, 1);
	bat *rcid = getArgReference_bat(stk, pci, 2);
	bat *gid = getArgReference_bat(stk, pci, 3);
	bat *bid = getArgReference_bat(stk, pci, 4);
	bat *rem = pci->argc == 9 ? getArgReference_bat(stk, pci, 5) : NULL;
	bat *cid = pci->argc == 9 ? getArgReference_bat(stk, pci, 6) : NULL;
	Pipeline *p = (Pipeline *) *getArgReference_ptr(stk, pci, pci->argc - 2);
	bat *pgid = getArgReference_bat(stk, pci, pci->argc - 1);
	BAT *b = BATdescriptor(*bid);
	BAT *g = BATdescriptor(*gid);
	BAT *c = cid ? BATdescriptor(*cid) : NULL;
	BAT *r = rem ? BATdescriptor(*rem) : NULL;
	BAT *bn = (*rid && !is_bat_nil(*rid)) ? BATdescriptor(*rid) : NULL;
	BAT *cn = bn ? BATdescriptor(*rcid) : NULL;
	BAT *rn = bn ? BATdescriptor(*rrem) : NULL;
	bool private = (bn == NULL || bn->tprivate_bat), locked = false;
	str err = NULL;

	if (!private) {
		pipeline_lock1(bn);
		locked = true;
	}

	if (b == NULL ||
	    g == NULL ||
	    (cid && c == NULL) ||
	    (rem && r == NULL) ||
	    ((*rid && !is_bat_nil(*rid)) && bn == NULL) ||
	    (bn && cn == NULL) ||
	    ((bn && rrem) && rn == NULL)) {
		err = createException(MAL, "pp aggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	BAT *pg = BATdescriptor(*pgid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg) ? pg->tmaxval : 0;
	BBPunfix(pg->batCacheid);

	if ((b->ttype == TYPE_flt || b->ttype == TYPE_dbl) && pci->argc == 7) {
		/* float sum core */
		if (!bn) {
			bn = COLnew(0, b->ttype, max, TRANSIENT);
			cn = COLnew(0, TYPE_lng, max, TRANSIENT);
			rn = COLnew(0, b->ttype, max, TRANSIENT);
			if (bn == NULL || cn == NULL || rn == NULL) {
				err = createException(MAL, "pp aggr.sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			bn->tprivate_bat = 1;
			cn->tprivate_bat = 1;
			rn->tprivate_bat = 1;
		}
		if (bn->batCount < max &&
			(BATextend(bn, max) != GDK_SUCCEED ||
			 BATextend(cn, max) != GDK_SUCCEED ||
			 BATextend(rn, max) != GDK_SUCCEED)) {
			err = createException(MAL, "pp aggr.sum", GDK_EXCEPTION);
			goto error;
		}
		lng *rcnts = Tloc(cn, 0);
		oid *grps = Tloc(g, 0);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		int overflow = 0;
		if (ATOMbasetype(b->ttype) == TYPE_flt) {
			flt *vals = Tloc(b, 0);
			flt *rerrs = Tloc(rn, 0);
			flt *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = flt_nil;
				rcnts[i] = 0;
				rerrs[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_float(flt, flt, vals[i], rvals[grps[i]], rerrs[grps[i]], rcnts[grps[i]]);
			}
		} else {
			dbl *vals = Tloc(b, 0);
			dbl *rerrs = Tloc(rn, 0);
			dbl *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = dbl_nil;
				rcnts[i] = 0;
				rerrs[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_float(dbl, dbl, vals[i], rvals[grps[i]], rerrs[grps[i]], rcnts[grps[i]]);
			}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.sum", RUNTIME_QRY_TIMEOUT));
		if (overflow)
			err = createException(SQL, "pp aggr.sum", "Overflow in sum()");
		if (err) goto error;

		pipeline_lock2(bn);
		BATnegateprops(bn);
		pipeline_unlock2(bn);
		pipeline_lock2(rn);
		BATnegateprops(rn);
		pipeline_unlock2(rn);
		pipeline_lock2(cn);
		BATnegateprops(cn);
		pipeline_unlock2(cn);
		if (BATcount(bn) < max) {
			BATsetcount(bn, max);
			BATsetcount(rn, max);
			BATsetcount(cn, max);
		}
	} else if ((b->ttype == TYPE_flt || b->ttype == TYPE_dbl) && pci->argc == 9) {
		/* float avg combine */
		if (bn->batCount < max &&
			(BATextend(bn, max) != GDK_SUCCEED ||
			 BATextend(cn, max) != GDK_SUCCEED ||
			 BATextend(rn, max) != GDK_SUCCEED)) {
			err = createException(MAL, "pp aggr.avg", GDK_EXCEPTION);
			goto error;
		}
		lng *cnts = Tloc(c, 0);
		lng *rcnts = Tloc(cn, 0);
		oid *grps = Tloc(g, 0);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		int overflow = 0;
		if (ATOMbasetype(b->ttype) == TYPE_flt) {
			flt *vals = Tloc(b, 0);
			flt *errs = Tloc(r, 0);
			flt *rerrs = Tloc(rn, 0);
			flt *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = flt_nil;
				rcnts[i] = 0;
				rerrs[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_float_comb(flt, vals[i], errs[i], cnts[i], rvals[grps[i]], rerrs[grps[i]], rcnts[grps[i]]);
			}
		} else {
			dbl *vals = Tloc(b, 0);
			dbl *errs = Tloc(r, 0);
			dbl *rerrs = Tloc(rn, 0);
			dbl *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = dbl_nil;
				rcnts[i] = 0;
				rerrs[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
				avg_aggr_float_comb(dbl, vals[i], errs[i], cnts[i], rvals[grps[i]], rerrs[grps[i]], rcnts[grps[i]]);
			}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.avg", RUNTIME_QRY_TIMEOUT));
		if (overflow)
			err = createException(SQL, "pp aggr.sum", "Overflow in sum()");
		if (err) goto error;

		pipeline_lock2(bn);
		BATnegateprops(bn);
		pipeline_unlock2(bn);
		pipeline_lock2(rn);
		BATnegateprops(rn);
		pipeline_unlock2(rn);
		pipeline_lock2(cn);
		BATnegateprops(cn);
		pipeline_unlock2(cn);
		if (BATcount(bn) < max) {
			BATsetcount(bn, max);
			BATsetcount(rn, max);
			BATsetcount(cn, max);
		}
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (c)
		BBPunfix(c->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn) {
		*rid = bn->batCacheid;
		BBPkeepref(bn);
	}
	if (cn) {
		*rcid = cn->batCacheid;
		BBPkeepref(cn);
	}
	if (rn) {
		*rrem = rn->batCacheid;
		BBPkeepref(rn);
	}

	if (!private)
		pipeline_unlock1(bn);
	else
		bn->tprivate_bat = true; /* in case it's a new one, set the bit */

	(void)p;

	(void)cntxt;
	(void)mb;
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(bn);
	if (b) BBPunfix(b->batCacheid);
	if (g) BBPunfix(g->batCacheid);
	if (c) BBPunfix(c->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	if (bn) BBPunfix(bn->batCacheid);
	if (cn) BBPunfix(cn->batCacheid);
	if (rn) BBPunfix(rn->batCacheid);
	return err;
}

static str
//compute_sum(bat *rsum, bat *sum, bat *com, bat *cnt)
compute_sum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *rsum = getArgReference_bat(stk, pci, 0);
	bat *sum = getArgReference_bat(stk, pci, 1);
	bat *rem = getArgReference_bat(stk, pci, 2);
	bat *cnt = getArgReference_bat(stk, pci, 3);
	str err = NULL;

	BAT *a = BATdescriptor(*sum);
	BAT *r = BATdescriptor(*rem);
	BAT *c = BATdescriptor(*cnt);

	if (!a || !r || !c)
		err = createException(MAL, "pp aggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!err) {
		BUN n = BATcount(a);
		assert(n == BATcount(r) && n == BATcount(c));
		BAT *res = COLnew(0, TYPE_dbl, n, TRANSIENT);

		if (!res) {
			err = createException(MAL, "pp aggr.sum", MAL_MALLOC_FAIL);
			goto bail;
		}

		dbl *cres = Tloc(res, 0);

		if (a->ttype == TYPE_dbl) {
			dbl *ca = Tloc(a, 0);
			dbl *ce = Tloc(r, 0);
			lng *cc = Tloc(c, 0);
			for(BUN i = 0; i<n; i++)
				cres[i] = cc[i] ? (ca[i] + ce[i]) : dbl_nil;
		} else {
			err = createException(MAL, "pp aggr.sum", "Only supports decimal input types");
			BBPreclaim(res);
			goto bail;
		}
		BATsetcount(res, n);
		BATnegateprops(res);
		*rsum = res->batCacheid;
		BBPkeepref(res);
	}
bail:
	BBPreclaim(a);
	BBPreclaim(r);
	BBPreclaim(c);
	return err;
}


/* TODO handle nil based on argument 'skipnil' */
#define gfunc(Type, f) \
	if (tt == TYPE_##Type) { \
		Type *in = Tloc(b, 0); \
		Type *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				if (is_##Type##_nil(o[i])) \
					o[i] = in[i]; \
				else if (!is_##Type##_nil(in[i])) \
					o[i] = f(o[i], in[i]); \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				if (is_##Type##_nil(o[grp[i]])) \
					o[grp[i]] = in[i]; \
				else if (!is_##Type##_nil(in[i])) \
					o[grp[i]] = f(o[grp[i]], in[i]); \
		} \
	}

#define gfunc2(Type, f) \
	if (tt == TYPE_##Type) { \
		int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
		Type *in = Tloc(b, 0); \
		Type *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				if (is_##Type##_nil(o[i])) \
					o[i] = in[i]; \
				else if (!is_##Type##_nil(in[i])) \
					o[i] = f(o[i], in[i]); \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
				if (is_##Type##_nil(o[grp[i]])) \
					o[grp[i]] = in[i]; \
				else if (!is_##Type##_nil(in[i])) \
					o[grp[i]] = f(o[grp[i]], in[i]); \
		} \
	}

/* from now on assume shared heap */
#define vamin(cmp, o, opos, op, in, i, bp, nil) \
	if (!o[opos] || \
		(cmp(bp+in[i], nil) != 0 && \
		 cmp(op+o[opos], nil) != 0 && \
		 cmp(op+o[opos], bp+in[i]) > 0)) \
		o[opos] = in[i];
#define vamax(cmp, o, opos, op, in, i, bp, nil) \
	if (!o[opos] || \
		(cmp(bp+in[i], nil) != 0 && \
		 cmp(op+o[opos], nil) != 0 && \
		 cmp(op+o[opos], bp+in[i]) < 0) ) \
		o[opos] = in[i];

#define gafunc(f) \
	if (ATOMextern(tt) && ATOMvarsized(tt)) { \
			BATiter bi = bat_iterator(b); \
			BATiter ri = bat_iterator(r); \
			char *bp = bi.vh->base; \
			char *op = ri.vh->base; \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			const char *nil = ATOMnilptr(r->ttype); \
			if (b->twidth == 1) { \
				bp += GDK_VAROFFSET; \
				op += GDK_VAROFFSET; \
				uint8_t *in = Tloc(b, 0); \
				uint8_t *o = Tloc(r, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 2) { \
				bp += GDK_VAROFFSET; \
				op += GDK_VAROFFSET; \
				uint16_t *in = Tloc(b, 0); \
				uint16_t *o = Tloc(r, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 4) { \
				uint32_t *in = Tloc(b, 0); \
				uint32_t *o = Tloc(r, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 8) { \
				var_t *in = Tloc(b, 0); \
				var_t *o = Tloc(r, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} \
			bat_iterator_end(&bi); \
			bat_iterator_end(&ri); \
	}

/* private (changing) heap */
#define vamin_(cmp, opos, in, i, bp, nil) \
	if (!getoffset(r->theap->base, opos, r->twidth) || \
			(cmp(bp+in[i], nil) != 0 && \
			 cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), nil) != 0 && \
			 cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), bp+in[i]) > 0)) \
		if (tfastins_nocheckVAR( r, opos, bp+in[i]) != GDK_SUCCEED) { \
			err = createException(MAL, "pp aggr.min", MAL_MALLOC_FAIL);\
			goto error; \
		}

#define vamax_(cmp, opos, in, i, bp, nil) \
	if (!getoffset(r->theap->base, opos, r->twidth) || \
			(cmp(bp+in[i], nil) != 0 && \
			 cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), nil) != 0 && \
			 cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), bp+in[i]) < 0)) \
		if (tfastins_nocheckVAR( r, opos, bp+in[i]) != GDK_SUCCEED) { \
			err = createException(MAL, "pp aggr.max", MAL_MALLOC_FAIL);\
			goto error; \
		}

static inline size_t
getoffset(const void *b, BUN p, int w)
{
	switch (w) {
		case 1:
			return (size_t) ((const uint8_t *) b)[p];
		case 2:
			return (size_t) ((const uint16_t *) b)[p];
#if SIZEOF_VAR_T == 8
		case 4:
			return (size_t) ((const uint32_t *) b)[p];
#endif
		default:
			return (size_t) ((const var_t *) b)[p];
	}
}

#define gafunc_(f) \
	if (ATOMextern(tt) && ATOMvarsized(tt)) { \
			BATiter bi = bat_iterator(b); \
			BATiter ri = bat_iterator(r); \
			char *bp = bi.vh->base; \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			const char *nil = ATOMnilptr(r->ttype); \
			if (b->twidth == 1) { \
				bp += GDK_VAROFFSET; \
				uint8_t *in = Tloc(b, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 2) { \
				bp += GDK_VAROFFSET; \
				uint16_t *in = Tloc(b, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 4) { \
				uint32_t *in = Tloc(b, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 8) { \
				var_t *in = Tloc(b, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} \
			bat_iterator_end(&bi); \
			bat_iterator_end(&ri); \
	}

static str
LALGmin(Client ctx, bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = NULL, *b = NULL, *r = NULL;
	str err = NULL;
	bool private = true, local_storage = false, locked = false;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err = createException(MAL, "pp aggr.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err = createException(MAL, "pp aggr.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}

	int tt = b->ttype;
	private = (!r || r->tprivate_bat);
	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

	BAT *pg = BATdescriptor(*pid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg)?pg->tmaxval:0;
	BBPunfix(pg->batCacheid);

	if (r && BATcount(b)) {
		MT_lock_set(&r->theaplock);
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b, true)) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			err = createException(MAL, "pp aggr.min", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		} else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_desc(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
		}
	} else if (!r) {
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(b->ttype) && VIEWvtparent(b) && BBP_desc(VIEWvtparent(b))->batRestricted == BAT_READ) {
			uint16_t width = b->twidth;
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, b->ttype, max, TRANSIENT, width);
			if (r == NULL) {
				err = createException(MAL, "pp aggr.min", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			BATswap_heaps(r, b, p);
		} else {
			local_storage = true;
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, tt, max, TRANSIENT, b->twidth);
			if (r == NULL) {
				err = createException(MAL, "pp aggr.min", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			if (r->tvheap && r->tvheap->base == NULL &&
				ATOMheap(r->ttype, r->tvheap, r->batCapacity) != GDK_SUCCEED) {
				err = createException(MAL, "pp aggr.min", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
		}
		r->tprivate_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.min", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (cnt < max) {
		if (ATOMextern(r->ttype)) {
			memset(Tloc(r, cnt), 0, r->twidth*(max-cnt));
		} else {
			char *d = Tloc(r, 0);
			const char *nil = ATOMnilptr(r->ttype);
			for (BUN i=cnt; i<max; i++)
				memcpy(d+(i*r->twidth), nil, r->twidth);
		}
	}
	assert(b->twidth == r->twidth || local_storage || !BATcount(b));

	cnt = BATcount(g);

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	oid *grp = Tloc(g, 0);

	assert(BATcount(b) == BATcount(g));
	gfunc(bit,min);
	gfunc(bte,min);
	gfunc(sht,min);
	gfunc(int,min);
	gfunc(date,min);
	gfunc2(inet4,inet4_min);
	gfunc(lng,min);
	gfunc(daytime,min);
	gfunc(timestamp,min);
#ifdef HAVE_HGE
	gfunc(hge,min);
#endif
	gfunc2(uuid,uuid_min);
	gfunc(flt,min);
	gfunc(dbl,min);
	if (local_storage) {
		gafunc_(vamin);
	} else {
		gafunc(vamin);
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.min", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (!private)
		pipeline_lock2(r);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	*rid = r->batCacheid;
	BBPkeepref(r);

	if (!private)
		pipeline_unlock1(r);
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

static str
LALGmax(Client ctx, bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = NULL, *b = NULL, *r = NULL;
	str err = NULL;
	bool private = true, local_storage = false, locked = false;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err = createException(MAL, "pp aggr.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err = createException(MAL, "pp aggr.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}

	int tt = b->ttype;
	private = (!r || r->tprivate_bat);
	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

	BAT *pg = BATdescriptor(*pid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg)?pg->tmaxval:0;
	BBPunfix(pg->batCacheid);

	if (r && BATcount(b)) {
		MT_lock_set(&r->theaplock);
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b, true)) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			err = createException(MAL, "pp aggr.max", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		} else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_desc(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
		}
	} else if (!r) {
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(b->ttype) && VIEWvtparent(b) && BBP_desc(VIEWvtparent(b))->batRestricted == BAT_READ) {
			uint16_t width = b->twidth;
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, b->ttype, max, TRANSIENT, width);
			if (r == NULL) {
				err = createException(MAL, "pp aggr.max", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			local_storage = true;
			r = COLnew2(0, tt, max, TRANSIENT, b->twidth);
			if (r == NULL) {
				err = createException(MAL, "pp aggr.max", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			if (r->tvheap && r->tvheap->base == NULL &&
				ATOMheap(r->ttype, r->tvheap, r->batCapacity) != GDK_SUCCEED)
				err = "ERROR";
		}
		r->tprivate_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.max", MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (cnt < max) {
		if (ATOMextern(r->ttype)) {
			memset(Tloc(r, cnt), 0, r->twidth*(max-cnt));
		} else {
			char *d = Tloc(r, 0);
			const char *nil = ATOMnilptr(r->ttype);
			for (BUN i=cnt; i<max; i++)
				memcpy(d+(i*r->twidth), nil, r->twidth);
		}
	}
	assert(b->twidth == r->twidth || local_storage || !BATcount(b));

	cnt = BATcount(g);

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	oid *grp = Tloc(g, 0);

	gfunc(bit,max);
	gfunc(bte,max);
	gfunc(sht,max);
	gfunc(int,max);
	gfunc(date,max);
	gfunc2(inet4,inet4_max);
	gfunc(lng,max);
	gfunc(daytime,max);
	gfunc(timestamp,max);
#ifdef HAVE_HGE
	gfunc(hge,max);
#endif
	gfunc2(uuid,uuid_max);
	gfunc(flt,max);
	gfunc(dbl,max);
	if (local_storage) {
		gafunc_(vamax);
	} else {
		gafunc(vamax);
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.max", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (!private)
		pipeline_lock2(r);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	*rid = r->batCacheid;
	BBPkeepref(r);

	if (!private)
		pipeline_unlock1(r);
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

#define gNull(Type) \
	do { \
		Type *restrict in = (Type *)bi.base; \
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			o[grp[i]] = o[grp[i]] == true? o[grp[i]] : is_##Type##_nil(in[i]);\
		} \
	} while (0)
static str
LALGnull(Client ctx, bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	(void)ctx;
	(void) H; /* last arg should move to first argument .. */
	BAT *g = NULL, *b = NULL, *r = NULL;
	str err = NULL;
	bool private = true, locked = false;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err = createException(MAL, "pp aggr.null", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err = createException(MAL, "pp aggr.null", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}

	private = (!r || r->tprivate_bat);
	if (!private) {
		pipeline_lock1(r);
		locked = true;
	}

	BAT *pg = BATdescriptor(*pid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.null", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg)?pg->tmaxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		r = COLnew2(0, TYPE_bit, max, TRANSIENT, b->twidth);
		if (r == NULL) {
			err = createException(MAL, "pp aggr.null", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		r->tprivate_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.null", MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (cnt < max) {
		char *d = Tloc(r, 0);
		const char *nil = ATOMnilptr(r->ttype);
		for (BUN i=cnt; i<max; i++)
			memcpy(d+(i*r->twidth), nil, r->twidth);
	}

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	cnt = BATcount(g);
	BATiter bi = bat_iterator(b);
	oid *grp = Tloc(g, 0);
	bit *o = Tloc(r, 0);
	switch(ATOMbasetype(bi.type)){
		case TYPE_bte:
			gNull(bte);
			break;
		case TYPE_sht:
			gNull(sht);
			break;
		case TYPE_int:
			gNull(int);
			break;
		case TYPE_lng:
			gNull(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			gNull(hge);
			break;
#endif
		case TYPE_flt:
			gNull(flt);
			break;
		case TYPE_dbl:
			gNull(dbl);
			break;
		default: {
			 int (*ocmp) (const void *, const void *) = ATOMcompare(bi.type);
			 const void *restrict nilp = ATOMnilptr(bi.type);

			 TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
				 if (o[grp[i]] != true) {
					 const void *restrict c = BUNtail(&bi, i);
					 o[grp[i]] = (ocmp(nilp, c) == 0);
				 }
			 }
		}
	}
	bat_iterator_end(&bi);

	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.null", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (!private)
		pipeline_lock2(r);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	if (!private)
		pipeline_unlock2(r);
	*rid = r->batCacheid;
	BBPkeepref(r);

	if (!private)
		pipeline_unlock1(r);
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(r);
	if (g) BBPunfix(g->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

static str
LALGcnull(Client ctx, bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	(void)ctx;
	(void) H; /* last arg should move to first argument .. */
	BAT *g = NULL, *b = NULL, *r = NULL;
	str err = NULL;
	bool locked = false;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err = createException(MAL, "pp aggr.cnull", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	if (is_bat_nil(*rid) || (r = BATdescriptor(*rid)) == NULL) {
		err = createException(MAL, "pp aggr.cnull", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	pipeline_lock1(r);
	locked = true;

	BAT *pg = BATdescriptor(*pid);
	if (pg == NULL) {
		err = createException(MAL, "pp aggr.cnull", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	oid max = BATcount(pg)?pg->tmaxval:0;
	BBPunfix(pg->batCacheid);

	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.cnull", MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (cnt < max) {
		char *d = Tloc(r, 0);
		const char *nil = ATOMnilptr(r->ttype);
		for (BUN i=cnt; i<max; i++)
			memcpy(d+(i*r->twidth), nil, r->twidth);
	}

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	cnt = BATcount(g);
	BATiter bi = bat_iterator(b);
	oid *grp = Tloc(g, 0);
	bit *o = Tloc(r, 0);
	/* for the combine-phase, we only have TYPE_bit to process */
	assert(b->ttype == TYPE_bit);
	bit *restrict in = (bit *)bi.base;
	TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
		if (is_bit_nil(o[grp[i]]) || (o[grp[i]] == false && !is_bit_nil(in[i]))) {
			o[grp[i]] = in[i];
		}
	}
	bat_iterator_end(&bi);
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.cnull", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);

	pipeline_lock2(r);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	pipeline_unlock2(r);

	*rid = r->batCacheid;
	BBPkeepref(r);

	pipeline_unlock1(r);
	return MAL_SUCCEED;
  error:
	if (locked) pipeline_unlock1(r);
	BBPreclaim(g);
	BBPreclaim(b);
	BBPreclaim(r);
	return err;
}

/* return value position groupcount*p/100 */
#define qfunc(Type, p, f) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
			Type *o = Tloc(r, 0); \
		    BUN s = 0; \
		    oid cur = grp[0]; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				if (grp[i] != cur) { \
					BUN pos = ((i-s-1)*p/100); \
					o[grp[s]] = in[pos]; \
					s = i; \
				} \
			} \
			if (s != cnt) { \
				BUN pos = ((cnt-s-1)*p/100); \
				o[grp[s]] = in[pos]; \
			} \
	}

static str
LALGquantile(Client ctx, bat *rid, bat *gid, bat *bid, bte *perc)
{
	(void)ctx;
	BAT *g = NULL, *b = NULL, *r = NULL;
	str err = NULL;
	bte p = *perc;

	if (p < 0 || p > 100) {
		err = createException(MAL, "pp aggr.quantile", SQLSTATE(HY002) "percentage out of range %d", p);
		goto error;
	}

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (g == NULL || b == NULL) {
		err = createException(MAL, "pp aggr.quantile", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	int tt = b->ttype;
	if (tt != TYPE_bte && tt != TYPE_sht && tt != TYPE_int && tt != TYPE_lng &&
#ifdef HAVE_HGE
		tt != TYPE_hge &&
#endif
		tt != TYPE_flt && tt != TYPE_dbl) {
		err = createException(MAL, "pp aggr.quantile", SQLSTATE(HY002) "incompatible type");
		goto error;
	}

	if (!is_bat_nil(*rid)) {
		if ((r = BATdescriptor(*rid)) == NULL) {
			err = createException(MAL, "pp aggr.quantile", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}

	/* ToDo make sure ordering set this */
	oid max = BATcount(g)?g->tmaxval:0;

	if (!r) {
		r = COLnew(0, tt, max, TRANSIENT);
		if (r == NULL) {
			err = createException(MAL, "pp aggr.max", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
	}
	if (BATcapacity(r) < max) {
		if (BATextend(r, max) != GDK_SUCCEED) {
			err = createException(MAL, "pp aggr.max", MAL_MALLOC_FAIL);
			goto error;
		}
	}

	BUN cnt = BATcount(g);

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	oid *grp = Tloc(g, 0);

	qfunc(bte,p,max);
	qfunc(sht,p,max);
	qfunc(int,p,max);
	qfunc(lng,p,max);
#ifdef HAVE_HGE
	qfunc(hge,p,max);
#endif
	qfunc(flt,p,max);
	qfunc(dbl,p,max);
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.quantile", RUNTIME_QRY_TIMEOUT));
	if (err) {
		goto error;
	}

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	if (BATcount(r) < max)
		BATsetcount(r, max);
	BATnegateprops(r);
	*rid = r->batCacheid;
	BBPkeepref(r);

	return MAL_SUCCEED;
  error:
	if (g) BBPunfix(g->batCacheid);
	if (b) BBPunfix(b->batCacheid);
	if (r) BBPunfix(r->batCacheid);
	return err;
}

static str
ALGcountCND_nil(Client ctx, lng *result, const bat *bid, const bat *cnd, const bit *ignore_nils)
{
	(void)ctx;
	BAT *b = NULL, *s = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "iaggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (cnd && !is_bat_nil(*cnd) && (s = BATdescriptor(*cnd)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "iaggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	lng result1 = 0;
	if (b->ttype == TYPE_msk || mask_cand(b)) {
		BATsum(&result1, TYPE_lng, b, s, *ignore_nils, false, false);
	} else if (*ignore_nils) {
		result1 = (lng) BATcount_no_nil(b, s);
	} else {
		struct canditer ci;
		canditer_init(&ci, b, s);
		result1 = (lng) ci.ncand;
	}
	if (is_lng_nil(*result))
		*result = result1;
	else if (!is_lng_nil(result1))
		*result += result1;
	if (s)
		BBPunfix(s->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGcount_nil(Client ctx, lng *result, const bat *bid, const bit *ignore_nils)
{
	return ALGcountCND_nil(ctx, result, bid, NULL, ignore_nils);
}

static str
ALGcountCND_bat(Client ctx, lng *result, const bat *bid, const bat *cnd)
{
	return ALGcountCND_nil(ctx, result, bid, cnd, &(bit){0});
}

static str
ALGcount_bat(Client ctx, lng *result, const bat *bid)
{
	return ALGcountCND_nil(ctx, result, bid, NULL, &(bit){0});
}

static str
ALGcountCND_no_nil(Client ctx, lng *result, const bat *bid, const bat *cnd)
{
	return ALGcountCND_nil(ctx, result, bid, cnd, &(bit){1});
}

static str
ALGcount_no_nil(Client ctx, lng *result, const bat *bid)
{
	return ALGcountCND_nil(ctx, result, bid, NULL, &(bit){1});
}

static str
ALGminany_skipnil(Client ctx, ptr result, const bat *bid, const bit *skipnil)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "iaggr.min", "atom '%s' cannot be ordered linearly", ATOMname(b->ttype));
	} else {
		allocator *ma = ctx->curprg->def->ma;
		if (ATOMextern(b->ttype)) {
			const void *nil = ATOMnilptr(b->ttype);
			int (*cmp)(const void *v1,const void *v2) = ATOMcompare(b->ttype);

			p = BATmin_skipnil(ma, b, NULL, *skipnil, false);
			if (cmp(*(ptr*)result, nil) == 0 || (cmp(p, nil) != 0 && cmp(p, *(ptr*)result) < 0))
				* (ptr *) result = p;
		} else {
			p = BATmin_skipnil(ma, b, result, *skipnil, true);
			if (p != result )
				msg = createException(MAL, "iaggr.min", SQLSTATE(HY002) "INTERNAL ERROR");
		}
		if (msg == MAL_SUCCEED && p == NULL)
			msg = createException(MAL, "iaggr.min", GDK_EXCEPTION);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGminany(Client ctx, ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGminany_skipnil(ctx, result, bid, &skipnil);
}

static str
ALGmaxany_skipnil(Client ctx, ptr result, const bat *bid, const bit *skipnil)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "iaggr.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "iaggr.max", "atom '%s' cannot be ordered linearly", ATOMname(b->ttype));
	} else {
		allocator *ma = ctx->curprg->def->ma;
		if (ATOMextern(b->ttype)) {
			const void *nil = ATOMnilptr(b->ttype);
			int (*cmp)(const void *v1,const void *v2) = ATOMcompare(b->ttype);

			p = BATmax_skipnil(ma, b, NULL, *skipnil, false);
			if (cmp(*(ptr*)result, nil) == 0 || (cmp(p, nil) != 0 && cmp(p, *(ptr*)result) > 0))
				* (ptr *) result = p;
		} else {
			p = BATmax_skipnil(ma, b, result, *skipnil, true);
			if (p != result )
				msg = createException(MAL, "iaggr.max", SQLSTATE(HY002) "INTERNAL ERROR");
		}
		if ( msg == MAL_SUCCEED && p == NULL)
			msg = createException(MAL, "iaggr.max", GDK_EXCEPTION);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGmaxany(Client ctx, ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGmaxany_skipnil(ctx, result, bid, &skipnil);
}

#define ALGnull_impl(TPE) \
	do {		\
		TPE *restrict bp = (TPE*)bi.base;	\
		TIMEOUT_LOOP_IDX_DECL(q, o, qry_ctx) { \
			if (is_##TPE##_nil(bp[q])) { \
				hasnull = TRUE; \
				break; \
			} \
		} \
	} while (0)

static str
ALGnull(Client ctx, bit *result, const bat *bid)
{
	(void)ctx;
	BAT *b = NULL;
	bit hasnull = false;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL) {
		BBPreclaim(b);
		throw(MAL, "iaggr.null", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (*result != true) {
		if (BATcount(b) > 0) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

			BUN o = BATcount(b);
			BATiter bi = bat_iterator(b);
			switch (ATOMbasetype(bi.type)) {
				case TYPE_bte:
					ALGnull_impl(bte);
					break;
				case TYPE_sht:
					ALGnull_impl(sht);
					break;
				case TYPE_int:
					ALGnull_impl(int);
					break;
				case TYPE_lng:
					ALGnull_impl(lng);
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					ALGnull_impl(hge);
					break;
#endif
				case TYPE_flt:
					ALGnull_impl(flt);
					break;
				case TYPE_dbl:
					ALGnull_impl(dbl);
					break;
				default: {
					 int (*ocmp) (const void *, const void *) = ATOMcompare(bi.type);
					 const void *restrict nilp = ATOMnilptr(bi.type);

					 TIMEOUT_LOOP_IDX_DECL(q, o, qry_ctx) {
						 const void *restrict c = BUNtail(&bi, q);
						 if (ocmp(nilp, c) == 0) {
							 hasnull = true;
							 break;
						 }
					 }
				}
			}
			bat_iterator_end(&bi);
			*result = hasnull;

			TIMEOUT_CHECK(qry_ctx, msg = createException(SQL, "pp aggr.null", RUNTIME_QRY_TIMEOUT));
		}
	} /* else: (*result == null): we've found a NULL before, nothing to do */

	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGfsum_skipnil_flt(Client ctx, flt *result, flt *rcom, lng *rcnt, const bat *bid, const bit *skipnil)
{
	(void)ctx;
	BAT *b;
	str err = MAL_SUCCEED;

	(void)skipnil;
	if (result == NULL || rcom == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "iaggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	assert(*skipnil == TRUE);
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	if (b->ttype != TYPE_flt) {
		err = createException(MAL, "iaggr.sum", "Kahan/Neumaier summation can only be done with floating point numbers");
	} else if (BATcount(b) == 0) {
		/* nothing needed */
	} else {
		flt *v = Tloc(b, 0);
		flt r = *result, c = *rcom;
		lng cc = *rcnt;
		if (is_flt_nil(r)) {
			r = 0;
			c = 0;
			cc = 0;
		}
		TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
			flt val = v[i];
			if (!is_flt_nil(val)) {
				flt t = r + val;
				if (fabs(r) >= fabs(val))
					c += (r - t) + val;
				else
					c += (val - t) + r;
				r = t;
				cc++;
			}
		}
		*result = r;
		*rcom = c;
		*rcnt = cc;
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.sum", RUNTIME_QRY_TIMEOUT));
	BBPunfix(b->batCacheid);
	return err;
}

static str
ALGfsum_flt(Client ctx, flt *result, flt *com, lng *cnt, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGfsum_skipnil_flt(ctx, result, com, cnt, bid, &skipnil);
}

static str
ALGfsum_skipnil(Client ctx, dbl *result, dbl *rcom, lng *rcnt, const bat *bid, const bit *skipnil)
{
	(void)ctx;
	BAT *b;
	str err = MAL_SUCCEED;

	(void)skipnil;
	if (result == NULL || rcom == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "iaggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	assert(*skipnil == TRUE);
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	if (b->ttype != TYPE_flt && b->ttype != TYPE_dbl) {
		err = createException(MAL, "iaggr.sum", "Kahan/Neumaier summation can only be done with floating point numbers");
	} else if (BATcount(b) == 0) {
		/* nothing needed */
	} else if (b->ttype == TYPE_flt) {
		flt *v = Tloc(b, 0);
		dbl r = *result, c = *rcom;
		lng cc = *rcnt;
		if (is_dbl_nil(r)) {
			r = 0;
			c = 0;
			cc = 0;
		}
		TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
			flt val = v[i];
			if (!is_flt_nil(val)) {
				dbl t = r + val;
				if (fabs(r) >= fabs(val))
					c += (r - t) + val;
				else
					c += (val - t) + r;
				r = t;
				cc++;
			}
		}
		*result = r;
		*rcom = c;
		*rcnt = cc;
	} else if (b->ttype == TYPE_dbl) {
		dbl *v = Tloc(b, 0), r = *result, c = *rcom;
		lng cc = *rcnt;
		if (is_dbl_nil(r)) {
			r = 0;
			c = 0;
			cc = 0;
		}
		TIMEOUT_LOOP_IDX_DECL(i, b->batCount, qry_ctx) {
			dbl val = v[i];
			if (!is_dbl_nil(val)) {
				dbl t = r + val;
				if (fabs(r) >= fabs(val))
					c += (r - t) + val;
				else
					c += (val - t) + r;
				r = t;
				cc++;
			}
		}
		*result = r;
		*rcom = c;
		*rcnt = cc;
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp aggr.sum", RUNTIME_QRY_TIMEOUT));
	BBPunfix(b->batCacheid);
	return err;
}

static str
ALGfsum(Client ctx, dbl *result, dbl *com, lng *cnt, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGfsum_skipnil(ctx, result, com, cnt, bid, &skipnil);
}

#include "mel.h"
static mel_func pp_algebra_init_funcs[] = {
 pattern("lockedaggr", "sum", LOCKEDAGGRsum1, true, "sum values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "prod", LOCKEDAGGRprod, true, "product of all values, using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 2))),
 pattern("lockedaggr", "avg", LOCKEDAGGRavg, true, "avg values into bat (bat has value, update), using the bat lock", args(2,5, sharedbatargany("", 1), sharedbatarg("rcnt", lng), arg("pipeline", ptr), argany("val", 1), arg("cnt", lng))),
 pattern("lockedaggr", "avg", LOCKEDAGGRavg, true, "avg values into bat (bat has value, update), using the bat lock", args(3,7, sharedbatargany("", 1), sharedbatarg("rremainder", lng), sharedbatarg("rcnt", lng), arg("pipeline", ptr), argany("val", 1), arg("remainder", lng), arg("cnt", lng))),
 pattern("lockedaggr", "sum", LOCKEDAGGRsum, true, "Kahan/neumaier summation, using the bat lock", args(3,7, sharedbatarg("rsum", flt), sharedbatarg("rcom", flt), sharedbatarg("rcnt", lng), arg("pipeline", ptr), arg("sum", flt), arg("com", flt), arg("cnt", lng))),
 pattern("lockedaggr", "sum", LOCKEDAGGRsum, true, "Kahan/neumaier summation, using the bat lock", args(3,7, sharedbatarg("rsum", dbl), sharedbatarg("rcom", dbl), sharedbatarg("rcnt", lng), arg("pipeline", ptr), arg("sum", dbl), arg("com", dbl), arg("cnt", lng))),
 pattern("lockedaggr", "avg", LOCKEDAGGRavg, true, "Kahan/neumaier summation, using the bat lock", args(3,7, sharedbatarg("ravg", dbl), sharedbatarg("rcem", dbl), sharedbatarg("rcnt", lng), arg("pipeline", ptr), arg("val", dbl), arg("com", dbl), arg("cnt", lng))),
 pattern("lockedaggr", "min", LOCKEDAGGRmin, true, "min values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "max", LOCKEDAGGRmax, true, "max values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 command("lockedaggr", "null", LOCKEDAGGRnull, true, "Returns true or false if the input contains a NULL or not, nil if the input is empty..", args(1,3, sharedbatarg("",bit),arg("pipeline", ptr),arg("hadnull",bit))),
 command("lockedalgebra", "projection", LALGprojection, false, "Project left input onto right input.", args(1,4, batargany("",1), arg("pipeline", ptr), batarg("left",oid),batargany("right",1))),

 command("algebra", "unique", LALGunique, false, "Unique rows.", args(2,5, batarg("gid", oid), batargany("",1), arg("pipeline", ptr), batargany("b",1), batarg("s",oid))),
 command("algebra", "unique", LALGgroup_unique, false, "Unique per group rows.", args(2,6, batarg("ngid", oid), batargany("",1), arg("pipeline", ptr), batargany("b",1), batarg("s",oid), batarg("gid",oid))),
 command("group", "group", LALGgroup, false, "Group input.", args(2,4, batarg("gid", oid), batargany("sink",1), arg("pipeline", ptr), batargany("b",1))),
 command("group", "group", LALGderive, false, "Sub Group input.", args(2,6, batarg("gid", oid), batargany("sink",1), arg("pipeline", ptr), batarg("pgid", oid), batargany("phash", 2), batargany("b",1))),
 pattern("algebra", "project", LALGconstant, false, "Project a single value", args(1,4, batargany("",1), batarg("gid", oid), argany("val",1), arg("pipeline", ptr))),
 command("algebra", "projection", LALGproject, false, "Project.", args(1,4, batargany("",1), batarg("gid", oid), batargany("b",1), arg("pipeline", ptr))),
 command("aggr", "count", LALGcount, false, "Count per group.", args(1,6, batarg("",lng), batarg("gid", oid), batargany("", 1), arg("nonil", bit), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "count", LALGcountstar, false, "count per group.", args(1,4, batarg("",lng), batarg("gid", oid), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "sum", LALGsum, false, "sum per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 /* sum core */
 pattern("aggr", "sum", LALGsum_float, false, "Kahan/Neumaier summation per group.", args(3,7, batarg("rsum", flt), batarg("rcom", flt), batarg("rcnt", lng), batarg("gid", oid), batarg("val", flt), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "sum", LALGsum_float, false, "Kahan/Neumaier summation per group.", args(3,7, batarg("rsum", dbl), batarg("rcom", dbl), batarg("rcnt", lng), batarg("gid", oid), batarg("val", dbl), arg("pipeline", ptr), batarg("pid", oid))),
 /* sum combine */
 pattern("aggr", "sum", LALGsum_float, false, "Kahan/Neumaier summation per group.", args(3,9, batarg("rsum", flt), batarg("rcom", flt), batarg("rcnt", lng), batarg("gid", oid), batarg("sum", flt), batarg("com", flt), batarg("cnt", lng), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "sum", LALGsum_float, false, "Kahan/Neumaier summation per group.", args(3,9, batarg("rsum", dbl), batarg("rcom", dbl), batarg("rcnt", lng), batarg("gid", oid), batarg("sum", dbl), batarg("com", dbl), batarg("cnt", lng), arg("pipeline", ptr), batarg("pid", oid))),
 /* sum finish */
 pattern("aggr", "compute_sum", compute_sum, false, "compute Kahan/Neumaier summation.", args(1,3, batarg("rsum",flt), batarg("sum", flt), batarg("com", flt))),
 pattern("aggr", "compute_sum", compute_sum, false, "compute Kahan/Neumaier summation.", args(1,3, batarg("rsum",dbl), batarg("sum", dbl), batarg("com", dbl))),
 pattern("aggr", "prod", LALGprod, false, "product per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 /* avg core */
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(3,7, batarg("ravg", dbl), batarg("rerror", dbl), batarg("rcnt", lng), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 /* avg combine */
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(3,9, batarg("ravg", dbl), batarg("rerror", dbl), batarg("rcnt", lng), batarg("gid", oid), batargany("", 1), batarg("error", dbl), batarg("cnt", lng), arg("pipeline", ptr), batarg("pid", oid))),
 /* avg integers/decimals core */
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(3,7, batargany("ravg",1), batarg("rremainder", lng), batarg("rcnt", lng), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 /* avg integers/decimals combine */
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(3,9, batargany("ravg",1), batarg("rremainder", lng), batarg("rcnt", lng), batarg("gid", oid), batargany("", 1), batarg("remainder", lng), batarg("cnt", lng), arg("pipeline", ptr), batarg("pid", oid))),
 /* avg integer and doubles finish */
 pattern("aggr", "compute_avg", compute_avg, false, "compute avg from integer avg + rest/count.", args(1,4, batarg("ravg",dbl), batargany("avg", 1), batarg("remainder", lng), batarg("cnt", lng))),
 pattern("aggr", "compute_avg", compute_avg, false, "compute avg from floating point (sum + error)/count.", args(1,4, batarg("ravg",dbl), batarg("avg", dbl), batarg("error", dbl), batarg("cnt", lng))),
 command("aggr", "min", LALGmin, false, "Min per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "max", LALGmax, false, "Max per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "null", LALGnull, false, "has-null per group per partition.", args(1,5, batarg("",bit), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "cnull", LALGcnull, false, "has-null per group all partition combined.", args(1,5, batarg("",bit), batarg("gid", oid), batarg("", bit), arg("pipeline", ptr), batarg("pid", oid))),

 /* Incremental aggregates */
 command("iaggr", "count", ALGcount_bat, false, "Return the current size (in number of elements) in a BAT.", args(1,2, arg("",lng), batargany("b",0))),
 command("iaggr", "count", ALGcount_nil, false, "Return the number of elements currently in a BAT ignores\nBUNs with nil-tail iff ignore_nils==TRUE.", args(1,3, arg("",lng), batargany("b",0),arg("ignore_nils",bit))),
 command("iaggr", "count_no_nil", ALGcount_no_nil, false, "Return the number of elements currently\nin a BAT ignoring BUNs with nil-tail", args(1,2, arg("",lng), batargany("b",2))),
 command("iaggr", "count", ALGcountCND_bat, false, "Return the current size (in number of elements) in a BAT.", args(1,3, arg("",lng), batargany("b",0),batarg("cnd",oid))),
 command("iaggr", "count", ALGcountCND_nil, false, "Return the number of elements currently in a BAT ignores\nBUNs with nil-tail iff ignore_nils==TRUE.", args(1,4, arg("",lng), batargany("b",0),batarg("cnd",oid),arg("ignore_nils",bit))),
 command("iaggr", "count_no_nil", ALGcountCND_no_nil, false, "Return the number of elements currently\nin a BAT ignoring BUNs with nil-tail", args(1,3, arg("",lng), batargany("b",2),batarg("cnd",oid))),
 command("iaggr", "min", ALGminany, false, "Return the lowest tail value or nil.", args(1,2, argany("",2), batargany("b",2))),
 command("iaggr", "min", ALGminany_skipnil, false, "Return the lowest tail value or nil.", args(1,3, argany("",2), batargany("b",2),arg("skipnil",bit))),
 command("iaggr", "max", ALGmaxany, false, "Return the highest tail value or nil.", args(1,2, argany("",2), batargany("b",2))),
 command("iaggr", "max", ALGmaxany_skipnil, false, "Return the highest tail value or nil.", args(1,3, argany("",2), batargany("b",2),arg("skipnil",bit))),
 command("iaggr", "null", ALGnull, false, "Returns true or false if the input contains a NULL or not, nil if the input is empty..", args(1,2, arg("",bit), batargany("b",1))),
 command("aggr", "sum", ALGfsum_flt, false, "Return the Kahan/Neumaier summation.", args(3,4, arg("rsum", flt), arg("rcom", flt), arg("rcnt", lng), batarg("b", flt))),
 command("aggr", "sum", ALGfsum_skipnil_flt, false, "Return the Kahan/Neumaier summation or nil.", args(3,5, arg("rsum", flt), arg("rcom", flt), arg("rcnt", lng), batarg("b", flt), arg("skipnil",bit))),
 command("aggr", "sum", ALGfsum, false, "Return the Kahan/Neumaier summation.", args(3,4, arg("rsum", dbl), arg("rcom", dbl), arg("rcnt", lng), batarg("b", flt))),
 command("aggr", "sum", ALGfsum_skipnil, false, "Return the Kahan/Neumaier summation or nil.", args(3,5, arg("rsum", dbl), arg("rcom", dbl), arg("rcnt", lng), batarg("b", flt), arg("skipnil",bit))),
 command("aggr", "sum", ALGfsum, false, "Return the Kahan/Neumaier summation.", args(3,4, arg("rsum", dbl), arg("rcom", dbl), arg("rcnt", lng), batarg("b", dbl))),
 command("aggr", "sum", ALGfsum_skipnil, false, "Return the Kahan/Neumaier summation or nil.", args(3,5, arg("rsum", dbl), arg("rcom", dbl), arg("rcnt", lng), batarg("b", dbl), arg("skipnil",bit))),
 command("aggr", "avg", ALGfsum, false, "Return the Kahan/Neumaier summation.", args(3,4, arg("rsum", dbl), arg("rcom", dbl), arg("rcnt", lng), batarg("b", flt))),
 command("aggr", "avg", ALGfsum_skipnil, false, "Return the Kahan/Neumaier summation or nil.", args(3,5, arg("rsum", dbl), arg("rcom", dbl), arg("rcnt", lng), batarg("b", flt), arg("skipnil",bit))),
 command("aggr", "avg", ALGfsum, false, "Return the Kahan/Neumaier summation.", args(3,4, arg("rsum", dbl), arg("rcom", dbl), arg("rcnt", lng), batarg("b", dbl))),
 command("aggr", "avg", ALGfsum_skipnil, false, "Return the Kahan/Neumaier summation or nil.", args(3,5, arg("rsum", dbl), arg("rcom", dbl), arg("rcnt", lng), batarg("b", dbl), arg("skipnil",bit))),
 command("aggr", "ord_quantile", LALGquantile, false, "Return the p-th's quantile per group, where p is between 0 and 100", args(1,4, batargany("quantile", 1), batarg("gid", oid), batargany("i", 1), arg("p", bte))),
 //command("aggr", "quantile", LALGquantile, false, "Return the p-th's quantile per group, where p is between 0 and 100", args(1,5, batargany("quantile", 1), batarg("gid", oid), batargany("i", 1), arg("pipeline", ptr), batarg("pid", oid))),
 /* in combine we return the pth row (or avg) */
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pp_algebra", NULL, pp_algebra_init_funcs); }
