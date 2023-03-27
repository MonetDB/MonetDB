/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#define min(a,b) a<b?a:b
#define max(a,b) a>b?a:b

#define uuid_min(a,b) ((cmp((void*)&a,(void*)&b)<0)?a:b)
#define uuid_max(a,b) ((cmp((void*)&a,(void*)&b)>0)?a:b)

#define getArgReference_date(stk, pci, nr)      (date*)getArgReference(stk, pci, nr)
#define getArgReference_daytime(stk, pci, nr)   (daytime*)getArgReference(stk, pci, nr)
#define getArgReference_timestamp(stk, pci, nr) (timestamp*)getArgReference(stk, pci, nr)

#define aggr(T,f)										\
	if (type == TYPE_##T) {									\
		T val = *getArgReference_##T(stk, pci, 2);					\
		if (!is_##T##_nil(val) && BATcount(b)) {					\
			T *t = Tloc(b, 0);							\
			if (is_##T##_nil(t[0])) {						\
				t[0] = val;							\
			} else									\
				t[0] = f(t[0], val);						\
			MT_lock_set(&b->theaplock);						\
			b->tnil = false;							\
			b->tnonil = true;							\
			MT_lock_unset(&b->theaplock);						\
		} else if (BATcount(b) == 0) {							\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)				\
				err = createException(SQL, "lockedaggr." #f,			\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);			\
		}										\
	}

#define faggr(T,f)										\
	if (type == TYPE_##T) {									\
		T val = *getArgReference_TYPE(stk, pci, 2, T);					\
		int (*cmp)(const void *v1,const void *v2) = ATOMcompare(type);			\
		if (!is_##T##_nil(val) && BATcount(b)) {					\
			T *t = Tloc(b, 0);							\
			if (is_##T##_nil(t[0])) {						\
				t[0] = val;							\
			} else									\
				t[0] = f(t[0], val);						\
			MT_lock_set(&b->theaplock);						\
			b->tnil = false;							\
			b->tnonil = true;							\
			MT_lock_unset(&b->theaplock);						\
		} else if (BATcount(b) == 0) {							\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)				\
				err = createException(SQL, "lockedaggr." #f,			\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);			\
		}										\
	}

#define vaggr(T,f)										\
	if (type == TYPE_##T) {									\
		BATiter bi = bat_iterator(b);							\
		T val = *getArgReference_##T(stk, pci, 2);					\
		const void *nil = ATOMnilptr(type);						\
		int (*cmp)(const void *v1,const void *v2) = ATOMcompare(type);			\
		if (cmp(val,nil) != 0 && BATcount(b)) {						\
			T t = BUNtvar(bi, 0);							\
			if (cmp(t,nil) == 0) {							\
				if (BUNreplace(b, 0, val, true) != GDK_SUCCEED)			\
					err = createException(SQL, "2 lockedaggr." #f,		\
						SQLSTATE(HY013) MAL_MALLOC_FAIL);		\
			} else									\
				if (f(t, val) == val)						\
					if (BUNreplace(b, 0, val, true) != GDK_SUCCEED)		\
						err = createException(SQL, "1 lockedaggr." #f,	\
							SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
			MT_lock_set(&b->theaplock);						\
			b->tnil = false;							\
			b->tnonil = true;							\
			MT_lock_unset(&b->theaplock);						\
		} else if (BATcount(b) == 0) {							\
			if (BUNappend(b, val, true) != GDK_SUCCEED)				\
				err = createException(SQL, "3 lockedaggr." #f,			\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);			\
		}										\
		bat_iterator_end(&bi);								\
	}

static str
LOCKEDAGGRsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
	if (*res) {
		BAT *b = BATdescriptor(*res);

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
	} else {
			err = createException(SQL, "lockedaggr.sum", "Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

#define paggr(T,OT,f)								\
	if (type == TYPE_##T && b->ttype == TYPE_##OT) {			\
		T val = *getArgReference_##T(stk, pci, 2);			\
		if (!is_##T##_nil(val) && BATcount(b)) {			\
			OT *t = Tloc(b, 0);					\
			if (is_##OT##_nil(t[0])) {				\
				t[0] = val;					\
			} else							\
				t[0] = f(t[0], val);				\
			MT_lock_set(&b->theaplock);				\
			b->tnil = false;					\
			b->tnonil = true;					\
			MT_lock_unset(&b->theaplock);				\
		} else if (BATcount(b) == 0) {					\
			OT ov = val;						\
			if (BUNappend(b, &ov, true) != GDK_SUCCEED)		\
				err = createException(SQL, "lockedaggr." #f,	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}								\
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
	if (*res) {
		BAT *b = BATdescriptor(*res);

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
			BBPkeepref(b);
		} else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "lockedaggr.prod", "Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

#define avg_aggr(T)									\
	if (type == TYPE_##T) {								\
		T val = *getArgReference_##T(stk, pci, pci->retc + 1);			\
		lng cnt = *getArgReference_lng(stk, pci, pci->retc + 2);		\
		if (cnt > 0 && !is_##T##_nil(val) && BATcount(b)) {			\
			T *t = Tloc(b, 0);						\
			lng *tcnt = Tloc(c, 0);						\
			if (is_##T##_nil(t[0])) {					\
				t[0] = val;						\
				tcnt[0] = cnt;						\
			} else {							\
			    dbl tt = (tcnt[0] + cnt);					\
				t[0] = (t[0]*((dbl)tcnt[0]/tt)) + (val*((dbl)cnt/tt));	\
				tcnt[0] += cnt;						\
			}								\
			MT_lock_set(&b->theaplock);					\
			b->tnil = false;						\
			b->tnonil = true;						\
			MT_lock_unset(&b->theaplock);					\
		} else if (cnt > 0 && BATcount(b) == 0) {				\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)			\
				err = createException(SQL, "lockedaggr.avg",		\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);		\
		}									\
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
#define fix_avg(T, a, r, n)						\
	do {								\
		if (!is_##T##_nil(a) && r > 0 && a < 0) {		\
			a++;						\
			r -= n;						\
		}							\
	} while (0)
#else
#define fix_avg(T, a, r, n)						\
	do {								\
		if (!is_##T##_nil(a) && r > 0 && 2*r + (a < 0) >= n) {	\
			a++;						\
			r -= n;						\
		}							\
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
#define avg_aggr_comb(T, a1, r1, n1, a2, r2, n2)							\
	do {												\
		if (is_##T##_nil(a2)) {									\
			if (!is_lng_nil(r2)) {								\
				a2 = a1;								\
				r2 = r1;								\
				n2 = n1;								\
			}										\
		} else if (!is_##T##_nil(a1)) {								\
			lng N1 = is_lng_nil(n1) ? 0 : n1;						\
			lng N2 = is_lng_nil(n2) ? 0 : n2;						\
			lng n = N1 + N2;								\
			T a;										\
			lng r;										\
			if (n == 0) {									\
				a = 0;									\
				r = 0;									\
			} else {									\
				a = (T) ((a1 / n) * N1 + ((a1 % n) * (__int128) N1) / n + 		\
						 (a2 / n) * N2 + ((a2 % n) * (__int128) N2) / n + 	\
						 (r1 + r2) / n);					\
				r = mulmod(a1, N1, n) + mulmod(a2, N2, n) + (r1 + r2) % n; 		\
				while (r >= n) {							\
					r -= n;								\
					a++;								\
				}									\
				while (r < 0) {								\
					r += n;								\
					a--;								\
				}									\
				fix_avg(T, a, r, n);							\
			}										\
			a2 = a;										\
			r2 = r;										\
			n2 = n;										\
		}											\
	} while (0)
#elif defined(_MSC_VER) && _MSC_VER >= 1920 && defined(_M_AMD64) && !defined(__INTEL_COMPILER)
#include <intrin.h>
#include <immintrin.h>
#pragma intrinsic(_mul128)
#pragma intrinsic(_div128)
#define avg_aggr_comb(T, a1, r1, n1, a2, r2, n2)						\
	do {											\
		if (is_##T##_nil(a2)) {								\
			a2 = a1;								\
			r2 = r1;								\
			n2 = n1;								\
		} else if (!is_##T##_nil(a1)) {							\
			lng N1 = is_lng_nil(n1) ? 0 : n1;					\
			lng N2 = is_lng_nil(n2) ? 0 : n2;					\
			lng n = N1 + N2;							\
			T a;									\
			lng r;									\
			if (n == 0) {								\
				a = 0;								\
				r = 0;								\
			} else {								\
				a = (T) ((a1 / n) * N1 +  (a2 / n) * N2 + (r1 + r2) / n); 	\
				__int64 xlo, xhi;						\
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
#define avg_aggr_comb(T, a1, r1, n1, a2, r2, n2)						\
	do {											\
		if (is_##T##_nil(a2)) {								\
			a2 = a1;								\
			r2 = r1;								\
			n2 = n1;								\
		} else if (!is_##T##_nil(a1)) {							\
			lng N1 = is_lng_nil(n1) ? 0 : n1;					\
			lng N2 = is_lng_nil(n2) ? 0 : n2;					\
			lng n = N1 + N2;							\
			T a;									\
			lng r;									\
			if (n == 0) {								\
				a = 0;								\
				r = 0;								\
			} else {								\
				lng x1 = a1 % n;						\
				lng x2 = a2 % n;						\
				if ((N1 != 0 &&							\
					 (x1 > GDK_lng_max / N1 || x1 < -GDK_lng_max / N1)) || 	\
					(N2 != 0 &&						\
					 (x2 > GDK_lng_max / N2 || x2 < -GDK_lng_max / N2))) { 	\
					BBPunfix(b->batCacheid);				\
					BBPunfix(c->batCacheid);				\
					BBPunfix(r->batCacheid);				\
					throw(SQL, "aggr.avg",					\
						  SQLSTATE(22003) "overflow in calculation");	\
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

#define avg_aggr_acc(T)										\
	do {											\
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

static str
LOCKEDAGGRavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat *rcnt = getArgReference_bat(stk, pci, pci->retc - 1);
	bat *rrem = pci->retc == 3 ? getArgReference_bat(stk, pci, 1) : NULL;
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, pci->retc);
	int type = getArgType(mb, pci, pci->retc + 1);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "lockedaggr.avg", "Wrong input type (%d)", type);

	pipeline_lock(p);
	if (*res) {
		BAT *b = BATdescriptor(*res);
		BAT *c = BATdescriptor(*rcnt);

		if (pci->retc == 3) {
			BAT *r = BATdescriptor(*rrem);
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
			avg_aggr(hge);
			avg_aggr(lng);
			avg_aggr(int);
			avg_aggr(sht);
			avg_aggr(bte);
			avg_aggr(flt);
			avg_aggr(dbl);
			if (!err) {
				pipeline_lock2(b);
				BATnegateprops(b);
				pipeline_unlock2(b);
				BBPkeepref(b);
				pipeline_lock2(c);
				BATnegateprops(c);
				pipeline_unlock2(c);
				BBPkeepref(c);
			} else {
				BBPunfix(b->batCacheid);
				BBPunfix(c->batCacheid);
			}
		}
	} else {
			err = createException(SQL, "lockedaggr.avg", "Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	return MAL_SUCCEED;
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

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte && type != TYPE_bit &&
		type != TYPE_flt && type != TYPE_dbl &&
		type != TYPE_date && type != TYPE_daytime && type != TYPE_timestamp && type != TYPE_uuid && type != TYPE_str)
			return createException(SQL, "lockedaggr.min", "Wrong input type (%d)", type);

	pipeline_lock(p);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(date,min);
		aggr(daytime,min);
		aggr(timestamp,min);
		faggr(uuid,uuid_min);
		aggr(hge,min);
		aggr(lng,min);
		aggr(int,min);
		aggr(sht,min);
		aggr(bte,min);
		aggr(bit,min);
		aggr(flt,min);
		aggr(dbl,min);
		vaggr(str,vmin);
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

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte && type != TYPE_bit &&
		type != TYPE_flt && type != TYPE_dbl &&
		type != TYPE_date && type != TYPE_daytime && type != TYPE_timestamp && type != TYPE_uuid && type != TYPE_str)
			return createException(SQL, "lockedaggr.max", "Wrong input type (%d)", type);

	pipeline_lock(p);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(date,max);
		aggr(daytime,max);
		aggr(timestamp,max);
		faggr(uuid,uuid_max);
		aggr(hge,max);
		aggr(lng,max);
		aggr(int,max);
		aggr(sht,max);
		aggr(bte,max);
		aggr(bit,max);
		aggr(flt,max);
		aggr(dbl,max);
		vaggr(str,vmax);
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
LALGprojection(bat *result, const ptr *h, const bat *lid, const bat *rid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	pipeline_lock(p);
	res = ALGprojection(result, lid, rid);
	pipeline_unlock(p);
	return res;
}

/* TODO unique: rehash iff too many probes need to be done, in the linear chain */
#define unique(Type) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)_hash_##Type(bp[i])&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && vals[k] != bp[i];) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define funique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && vals[k] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define cunique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && h->cmp(vals+k, bp+i) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

/* todo handle all any types */
#define aunique(Type) \
	if (tt == TYPE_##Type) { \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				Type bpi = BUNtvar(bi, i); \
				gid k = (gid)h->hsh(bpi)&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (h->cmp(vals[k], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bpi; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
		bat_iterator_end(&bi); \
	}

static str
LALGunique(bat *rid, bat *uid, const ptr *H, bat *bid, bat *sid)
{
	Pipeline *p = (Pipeline*)*H;
	assert(*uid && !is_bat_nil(*uid));
	BAT *u = BATdescriptor(*uid);
	BAT *b = BATdescriptor(*bid);
	int err = 0;
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;
	lng timeoffset = 0;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		BATswap_heaps(u, b, p);
	} else {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	}
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);

		BAT *g = COLnew(0, TYPE_oid, cnt, TRANSIENT);

		/* probably need bat resize and create hash */
		int tt = b->ttype;
		oid *gp = Tloc(g, 0);
		BUN r = 0;

		if (cnt && !err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			unique(bit)
			unique(bte)
			unique(sht)
			unique(int)
			unique(date)
			unique(lng)
			unique(daytime)
			unique(timestamp)
			unique(hge)
			funique(flt, int)
			funique(dbl, lng)
			cunique(uuid, hge)
			aunique(str)
		}
		if (!err) {
			BBPunfix(b->batCacheid);
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
	}
	/* FIXME: the name 'group.unique' doesn't match the mel definition */
	TIMEOUT_CHECK(timeoffset, throw(MAL, "group.unique", GDK_EXCEPTION));
	if (err)
		throw(MAL, "group.unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

/* TODO gunique: rehash iff too many probes need to be done, in the linear chain */
#define gunique(Type) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)combine(p[i], _hash_##Type(bp[i]))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || vals[k] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					pgids[k] = p[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define gfunique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)combine(p[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && vals[k] != bp[i]));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					pgids[k] = p[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define gcunique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)combine(p[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && h->cmp(vals+k, bp+i) != 0));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					pgids[k] = p[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

/* todo handle all any types */
#define gaunique(Type) \
	if (tt == TYPE_##Type) { \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				Type bpi = BUNtvar(bi, i); \
				gid k = (gid)combine(p[i], h->hsh(bpi))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || h->cmp(vals[k], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bpi; \
					pgids[k] = p[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
		bat_iterator_end(&bi); \
	}

static str
LALGgroup_unique(bat *rid, bat *uid, const ptr *H, bat *bid, bat *sid, bat *Gid)
{
	Pipeline *p = (Pipeline*)*H;
	assert(*uid && !is_bat_nil(*uid));
	BAT *u = BATdescriptor(*uid);
	BAT *G = BATdescriptor(*Gid);
	BAT *b = BATdescriptor(*bid);
	int err = 0;
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;
	lng timeoffset = 0;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		BATswap_heaps(u, b, p);
	} else {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	}
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);

		BAT *ng = COLnew(0, TYPE_oid, cnt, TRANSIENT);

		/* probably need bat resize and create hash */
		int tt = b->ttype;
		oid *gp = Tloc(ng, 0);
		gid *p = Tloc(G, 0);
		gid *pgids = h->pgids;
		BUN r = 0;

		if (cnt && !err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			gunique(bit)
			gunique(bte)
			gunique(sht)
			gunique(int)
			gunique(date)
			gunique(lng)
			gunique(daytime)
			gunique(timestamp)
			gunique(hge)
			gfunique(flt, int)
			gfunique(dbl, lng)
			gcunique(uuid, hge)
			gaunique(str)
		}
		if (!err) {
			BBPunfix(G->batCacheid);
			BBPunfix(b->batCacheid);
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
	}
	/* FIXME: the name 'group.unique' doesn't match the mel definition */
	TIMEOUT_CHECK(timeoffset, throw(MAL, "group.unique", GDK_EXCEPTION));
	if (err)
		throw(MAL, "group.unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define PRE_CLAIM 256
#define group(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			gid k = (gid)_hash_##Type(bp[i])&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && vals[g] != bp[i];) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
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
			gid k = (gid)_hash_oid(oid_nil)&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && vals[g] != bpi;) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			for(BUN i = 0; i<cnt; i++, bpi++) { \
				gp[i] = g-1; \
			} \
		} else { \
			assert(BATtdense(b)); \
			int slots = 0; \
			gid slot = 0; \
			oid bpi = b->tseqbase; \
			oid *vals = h->vals; \
			\
			for(BUN i = 0; i<cnt; i++, bpi++) { \
				bool fnd = 0; \
				gid k = (gid)_hash_oid(bpi)&h->mask; \
				gid g = 0; \
				\
				for(; !fnd; ) { \
					g = ATOMIC_GET(h->gids+k); \
					for(;g && vals[g] != bpi;) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = private?1:PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
							if (((slot*100)/70) >= (gid)h->size) \
								hash_rehash(h, p, err); \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = bpi; \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
							continue; \
					} \
					fnd = 1; \
				} \
				gp[i] = g-1; \
			} \
		} \
	}

#define fgroup(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define agroup(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)h->hsh(bpi)&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (vals[g] && h->cmp(vals[g], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}

#define agroup_(Type,P) \
	if (ATOMstorage(tt) == TYPE_str) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		mallocator *ma = h->allocators[P->wid]; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)str_hsh(bpi)&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (vals[g] && h->cmp(vals[g], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = ma_strdup(ma, bpi); \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	} else \
	if (ATOMvarsized(tt)) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		mallocator *ma = h->allocators[P->wid]; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			void *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)h->hsh(bpi)&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (vals[g] && h->cmp(vals[g], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = ma_copy(ma, bpi, h->len(bpi)); \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}

static str
LALGgroup(bat *rid, bat *uid, const ptr *H, bat *bid/*, bat *sid*/)
{
	Pipeline *p = (Pipeline*)*H;
	/* private or not */
	bool private = (!*uid || is_bat_nil(*uid)), local_storage = false;
	int err = 0;
	BAT *u, *b = NULL;
	lng timeoffset = 0;

   	b = BATdescriptor(*bid);
	if (!b)
		return createException(SQL, "group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (private && *uid && is_bat_nil(*uid)) { /* TODO ... create but how big ??? */
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		u->T.sink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, NULL);
		u->T.private_bat = 1;
	} else {
		u = BATdescriptor(*uid);
	}
	if (!u) {
		BBPunfix(*bid);
		return createException(SQL, "group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	private = u->T.private_bat;

	//assert(is_bat_nil(*sid)); /* no cands jet */
	//(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if (ATOMvarsized(u->ttype) && !VIEWvtparent(b)) {
		local_storage = true;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		pipeline_lock(p);
		if (!h->allocators) {
			h->allocators = (mallocator**)GDKzalloc(p->p->nr_workers*sizeof(mallocator*));
			if (!h->allocators)
				err = 1;
			else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			h->allocators[p->wid] = ma_create();
			if (!h->allocators[p->wid])
				err = 1;
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
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);
		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
		int tt = b->ttype;
		oid *gp = Tloc(g, 0);

		if (cnt && !err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			vgroup()
			group(bit)
			group(bte)
			group(sht)
			group(int)
			group(date)
			group(lng)
			group(oid)
			group(daytime)
			group(timestamp)
			group(hge)
			fgroup(flt, int)
			fgroup(dbl, lng)
			if (local_storage) {
				agroup_(str, p)
			} else {
				agroup(str)
			}
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BATsetcount(g, cnt);
			pipeline_lock2(g);
			BATnegateprops(g);
			pipeline_unlock2(g);
			/* props */
			gid last = ATOMIC_GET(&h->last);
			/* pass max id */
			g->T.maxval = last;
			g->tkey = FALSE;
			*uid = u->batCacheid;
			*rid = g->batCacheid;
			BBPkeepref(u);
			BBPkeepref(g);
		}
	}
	TIMEOUT_CHECK(timeoffset, throw(MAL, "group.group", GDK_EXCEPTION));
	if (err || p->p->status)
		throw(MAL, "group.group", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define derive(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			gid k = (gid)combine(gi[i], _hash_##Type(bp[i]))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || vals[g] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define vderive() \
	if (tt == TYPE_void) { \
		int slots = 0; \
		gid slot = 0; \
		oid bpi = b->tseqbase; \
		oid *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			gid k = (gid)combine(gi[i], _hash_oid(bpi))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || vals[g] != bpi);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define fderive(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			gid k = (gid)combine(gi[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define aderive(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)combine(gi[i], h->hsh(bpi))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/100) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}

#define aderive_(Type, P) \
	if (ATOMstorage(tt) == TYPE_str) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		mallocator *ma = h->allocators[P->wid]; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)combine(gi[i], str_hsh(bpi))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/100) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = ma_strdup(ma, bpi); \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	} else \
	if (ATOMvarsized(tt)) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		mallocator *ma = h->allocators[P->wid]; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)combine(gi[i], h->hsh(bpi))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = ma_copy(ma, bpi, h->len(bpi)); \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}


static str
LALGderive(bat *rid, bat *uid, const ptr *H, bat *Gid, bat *Ph, bat *bid /*, bat *sid*/)
{
	Pipeline *p = (Pipeline*)*H;
	bool private = (!*uid || is_bat_nil(*uid)), local_storage = false;
	int err = 0;
	BAT *u, *b = BATdescriptor(*bid);
	BAT *G = BATdescriptor(*Gid);
	lng timeoffset = 0;

	if (!b || !G) {
		if (b)
			BBPunfix(*bid);
		return createException(SQL, "group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (private && *uid && is_bat_nil(*uid)) { /* TODO ... create but how big ??? */
		BAT *H = BATdescriptor(*Ph);
		if (!H) {
			BBPunfix(*bid);
			BBPunfix(*Gid);
			return createException(SQL, "group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		/* Lookup parent hash */
		u->T.sink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, (hash_table*)H->T.sink);
		u->T.private_bat = 1;
		BBPunfix(*Ph);
	} else {
		u = BATdescriptor(*uid);
	}
	if (!u) {
		BBPunfix(*Gid);
		BBPunfix(*bid);
		return createException(SQL, "group.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	private = u->T.private_bat;
	//assert(is_bat_nil(*sid)); /* no cands jet */
	//(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if (ATOMvarsized(u->ttype) && !VIEWvtparent(b)) {
		local_storage = true;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		pipeline_lock(p);
		if (!h->allocators) {
			h->allocators = (mallocator**)GDKzalloc(p->p->nr_workers*sizeof(mallocator*));
			if (!h->allocators)
				err = 1;
			else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			h->allocators[p->wid] = ma_create();
			if (!h->allocators[p->wid])
				err = 1;
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
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);
		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
		int tt = b->ttype;
		oid *gp = Tloc(g, 0);
		gid *gi = Tloc(G, 0);
		gid *pgids = h->pgids;

		if (cnt && !err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			vderive()
			derive(bit)
			derive(bte)
			derive(sht)
			derive(int)
			derive(date)
			derive(lng)
			derive(oid)
			derive(daytime)
			derive(timestamp)
			derive(hge)
			fderive(flt, int)
			fderive(dbl, lng)
			if (local_storage) {
				aderive_(str,p)
			} else {
				aderive(str)
			}
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(G->batCacheid);
			BATsetcount(g, cnt);
			pipeline_lock2(g);
			BATnegateprops(g);
			pipeline_unlock2(g);
			/* props */
			gid last = ATOMIC_GET(&h->last);
			/* pass max id */
			g->T.maxval = last;
			g->tkey = FALSE;
			*uid = u->batCacheid;
			*rid = g->batCacheid;
			BBPkeepref(u);
			BBPkeepref(g);
		}
	}
	/* FIXME: the name 'group.derive' doesn't match the mel definition */
	TIMEOUT_CHECK(timeoffset, throw(MAL, "group.derive", GDK_EXCEPTION));
	if (err || p->p->status)
		throw(MAL, "group.derive", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define project(Type) \
	if (ATOMstorage(tt) == TYPE_##Type) { \
		Type *v = Tloc(b, 0); \
		Type *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			oid gi = g->tseqbase; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				o[gi + i] = v[i]; \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				o[gp[i]] = v[i]; \
		} \
	}

#define vproject() \
	if (tt == TYPE_void) { \
		oid vi = b->tseqbase; \
		oid *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			oid gi = g->tseqbase; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				o[gi + i] = vi + i; \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				o[gp[i]] = vi + i; \
		} \
	}

#define aproject(Type,w,Toff) \
	if ((ATOMvarsized(tt) || ATOMstorage(tt) == TYPE_##Type) && b->twidth == w) { \
		Toff *v = Tloc(b, 0); \
		Toff *o = Tloc(r, 0); \
		if (g->ttype == TYPE_void) { \
			oid gi = g->tseqbase; \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				o[gi + i] = v[i]; \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
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
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
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
				if (ins && tfastins_nocheckVAR( r, gi + i, BUNtvar(bi, i)) != GDK_SUCCEED) \
					err = 1; \
				if (w < r->twidth) { \
					BUN sz = BATcapacity(r); \
					memset(Tloc(r, max), 0, r->twidth*(sz-max)); \
				} \
				if (err) \
					TIMEOUT_LOOP_BREAK; \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
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
				if (ins && tfastins_nocheckVAR( r, gp[i], BUNtvar(bi, i)) != GDK_SUCCEED) \
					err = 1; \
				if (w < r->twidth) { \
					BUN sz = BATcapacity(r); \
					memset(Tloc(r, max), 0, r->twidth*(sz-max)); \
				} \
				if (err) \
					TIMEOUT_LOOP_BREAK; \
			} \
		} \
		bat_iterator_end(&bi); \
	}

/* result := algebra.projections(groupid, input)  */
/* this (possibly) overwrites the values, therefor for expensive (var) types we only write offsets (ie use the heap from
 * the parent) */
static str
LALGproject(bat *rid, bat *gid, bat *bid, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g, *b, *r = NULL;
	int err = 0;
	lng timeoffset = 0;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	int tt = b->ttype;
	oid max = BATcount(g)?g->T.maxval:0;
	/* probably need bat resize and create hash */
	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat), local_storage = false;

	if (!tt)
		tt = TYPE_oid;
	if (!private)
		pipeline_lock1(r);
	if (r && BATcount(b)) {
		MT_lock_set(&r->theaplock);
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b, true)) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			err = 1;
		} else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_cache(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			assert(r->tvheap->parentid == r->batCacheid);
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
		if (ATOMvarsized(tt) && VIEWvtparent(b) && BBP_cache(VIEWvtparent(b))->batRestricted == BAT_READ) {
			uint16_t width = b->twidth;
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, tt, max, TRANSIENT, width);
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			local_storage = true;
			r = COLnew2(0, tt, max, TRANSIENT, b->twidth);
			if (r->tvheap && r->tvheap->base == NULL &&
				ATOMheap(r->ttype, r->tvheap, r->batCapacity) != GDK_SUCCEED)
				err = 1;
		}
		assert(private);
		r->T.private_bat = 1;
	}

	BUN cnt = 0;
	if (!err) {
		cnt = BATcount(r);
		if (BATcapacity(r) < max) {
			BUN sz = max*2;
			if (BATextend(r, sz) != GDK_SUCCEED)
				err = 1;
		}
	}

	/* get max id from gid */
	if (!err) {
		if (cnt < max)
			memset(Tloc(r, cnt), 0, r->twidth*(max-cnt));

		cnt = BATcount(b);

		int err = 0;
		oid *gp = Tloc(g, 0);

		tt = b->ttype;
		if (!err && cnt) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			vproject()
			project(bte)
			project(sht)
			project(int)
			project(lng)
			project(hge)
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
		}
		if (!err) {
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
		}
	}
	if (!private)
		pipeline_unlock1(r);
	/* FIXME: the name 'aggr.project' doesn't match the mel definition */
	TIMEOUT_CHECK(timeoffset, throw(MAL, "aggr.project", GDK_EXCEPTION));
	if (err)
		throw(MAL, "aggr.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

static str
LALGcountstar(bat *rid, bat *gid, const ptr *H, bat *pid)
{
	//Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	(void)H;
	BAT *g = BATdescriptor(*gid);
	BAT *r = NULL;
	int err = 0;
	lng timeoffset = 0;

	if (*rid)
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		pipeline_lock1(r);
		//pipeline_lock(p);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		r = COLnew(0, TYPE_lng, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		//printf("count extend %ld\n", sz);
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max)
		memset(Tloc(r, cnt), 0, sizeof(lng)*(max-cnt));

	if (!err) {
		BUN cnt = BATcount(g);

		int err = 0;

		if (!err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			oid *v = Tloc(g, 0);
			lng *o = Tloc(r, 0);
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset)
				o[v[i]]++;
		}
		if (!err) {
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
		}
	}
	if (!private)
		pipeline_unlock1(r);
		//pipeline_unlock(p);
	TIMEOUT_CHECK(timeoffset, throw(MAL, "aggr.count", GDK_EXCEPTION));
	if (err)
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define gcount(Type) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				o[v[i]]+= (!is_##Type##_nil(in[i])); \
	}

#define gfcount(Type) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				o[v[i]]+= cmp(in+i, &Type##_nil) != 0; \
	}

#define gacount(Type) \
	if (tt == TYPE_##Type) { \
			BATiter bi = bat_iterator(b); \
			const void *nil = ATOMnilptr(tt); \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
				Type bpi = BUNtvar(bi, i); \
				o[v[i]]+= cmp(bpi, nil)!=0; \
			} \
			bat_iterator_end(&bi); \
	}

static str
LALGcount(bat *rid, bat *gid, bat *bid, bit *nonil, const ptr *H, bat *pid)
{
	//Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	if (!(*nonil))
		return LALGcountstar(rid, gid, H, pid);

	/* use bid to check for null values */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0;
	lng timeoffset = 0;

	if (!g || !b) {
		if (g)
			BBPunfix(*gid);
		if (b)
			BBPunfix(*bid);
		return createException(SQL, "aggr.count", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if (*rid && !is_bat_nil(*rid)) {
		r = BATdescriptor(*rid);
		if (!r) {
			BBPunfix(*gid);
			BBPunfix(*bid);
			return createException(SQL, "aggr.count", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bool private = (!r || r->T.private_bat);

	if (!private)
		pipeline_lock1(r);
		//pipeline_lock(p);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		r = COLnew(0, TYPE_lng, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		//printf("count extend %ld\n", sz);
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max)
		memset(Tloc(r, cnt), 0, sizeof(lng)*(max-cnt));

	if (!err) {
		BUN cnt = BATcount(g);

		int err = 0;

		if (!err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			oid *v = Tloc(g, 0);
			lng *o = Tloc(r, 0);
			if (b->tnonil) {
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset)
					o[v[i]]++;
			} else { /* per type */
				int tt = b->ttype;

				gcount(bit);
				gcount(bte);
				gcount(sht);
				gcount(int);
				gcount(date);
				gcount(lng);
				gcount(daytime);
				gcount(timestamp);
				gcount(hge);
				gfcount(uuid);
				gcount(flt);
				gcount(dbl);
				gacount(str);
			}
		}
		if (!err) {
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
		}
	}
	if (!private)
		pipeline_unlock1(r);
		//pipeline_unlock(p);
	TIMEOUT_CHECK(timeoffset, throw(MAL, "aggr.count", GDK_EXCEPTION));
	if (err)
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

/* TODO do we need to split out the nil check, ie for when we know there are no nils */
#define gsum(OutType, InType) \
	if (tt == TYPE_##InType && ot == TYPE_##OutType) { \
			InType *in = Tloc(b, 0); \
			OutType *o = Tloc(r, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
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
	BAT *b, *g, *r = NULL;
	int err = 0;
	lng timeoffset = 0;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		pipeline_lock1(r);
		//pipeline_lock(p);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		int tt = getBatType(getArgType(mb, pci, 0));
		r = COLnew(b->hseqbase, tt, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		//printf("sum extend %ld\n", sz);
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max) {
		char *d = Tloc(r, 0);
		const char *nil = ATOMnilptr(r->ttype);
		for (BUN i=cnt; i<max; i++)
			memcpy(d+(i<<r->tshift), nil, r->twidth);
	}

	if (!err) {
		BUN cnt = BATcount(g);
		int tt = b->ttype, ot = r->ttype;

		if (!err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
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
			gsum(hge,bte);
			gsum(hge,sht);
			gsum(hge,int);
			gsum(hge,lng);
			gsum(hge,hge);
			gsum(flt,flt);
			gsum(dbl,flt);
			gsum(dbl,dbl);
		}
		if (!err) {
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
		}
	}
	if (!private)
		pipeline_unlock1(r);
		//pipeline_unlock(p);
	TIMEOUT_CHECK(timeoffset, throw(MAL, "aggr.sum", GDK_EXCEPTION));
	if (err)
		throw(MAL, "aggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define gprod(OutType, InType) \
	if (tt == TYPE_##InType && ot == TYPE_##OutType) { \
			InType *in = Tloc(b, 0); \
			OutType *o = Tloc(r, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
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
	BAT *b, *g, *r = NULL;
	int err = 0;
	lng timeoffset = 0;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		pipeline_lock1(r);
		//pipeline_lock(p);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		int tt = getBatType(getArgType(mb, pci, 0));
		r = COLnew(b->hseqbase, tt, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max) {
		char *d = Tloc(r, 0);
		const char *nil = ATOMnilptr(r->ttype);
		for (BUN i=cnt; i<max; i++)
			memcpy(d+(i*r->twidth), nil, r->twidth);
	}

	if (!err) {
		BUN cnt = BATcount(g);
		int tt = b->ttype, ot = r->ttype;

		if (!err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
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
		}
		if (!err) {
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
		}
	}
	if (!private)
		pipeline_unlock1(r);
		//pipeline_unlock(p);
	TIMEOUT_CHECK(timeoffset, throw(MAL, "aggr.prod", GDK_EXCEPTION));
	if (err)
		throw(MAL, "aggr.prod", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

static str
//LALGavg(bat *rid, [bat *rremainer,] bat *rcnt, bat *gid, bat *bid, const ptr *H, bat *pid)
//LALGavg(bat *rid, [bat *rremainer,] bat *rcnt, bat *gid, bat *bid, [bat *remainder,] bat *cnt, const ptr *H, bat *pid)
LALGavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *rrem = pci->retc == 3 ? getArgReference_bat(stk, pci, 1) : NULL;
	bat *rcid = getArgReference_bat(stk, pci, pci->retc - 1);
	bat *gid = getArgReference_bat(stk, pci, pci->retc);
	bat *bid = getArgReference_bat(stk, pci, pci->retc + 1);
	bat *rem = pci->argc == 9 ? getArgReference_bat(stk, pci, pci->retc + 2) : NULL;
	bat *cid = pci->argc - pci->retc > 4 ? getArgReference_bat(stk, pci, pci->argc - 3) : NULL;
	Pipeline *p = (Pipeline *) *getArgReference_ptr(stk, pci, pci->argc - 2);
	bat *pgid = getArgReference_bat(stk, pci, pci->argc - 1);
	BAT *b = BATdescriptor(*bid);
	BAT *g = BATdescriptor(*gid);
	BAT *c = cid ? BATdescriptor(*cid) : NULL;
	BAT *r = rem ? BATdescriptor(*rem) : NULL;
	BAT *bn = (*rid && !is_bat_nil(*rid)) ? BATdescriptor(*rid) : NULL;
	BAT *cn = bn ? BATdescriptor(*rcid) : NULL;
	BAT *rn = bn && rrem ? BATdescriptor(*rrem) : NULL;
	bool private = bn == NULL || bn->T.private_bat;
	lng timeoffset = 0;

	if (!private)
		pipeline_lock1(bn);

	BAT *pg = BATdescriptor(*pgid);
	oid max = BATcount(pg) ? pg->T.maxval : 0;
	BBPunfix(pg->batCacheid);

	if (pci->retc == 2 && (pci->argc == 6 || pci->argc == 7)) {
		if (BATgroupavg2(&bn, &cn, b, g, NULL, NULL, TYPE_dbl, max, true, 0) != GDK_SUCCEED) {
			if (!private)
				pipeline_unlock1(bn);
			throw(MAL, "aggr.avg", GDK_EXCEPTION);
		}
		pipeline_lock2(bn);
		BATnegateprops(bn);
		pipeline_unlock2(bn);
	} else if (pci->retc == 3 && pci->argc == 7) {
		if (BATgroupavg3(&bn, &rn, &cn, b, g, NULL, NULL, true, bn != NULL) != GDK_SUCCEED) {
			if (!private)
				pipeline_unlock1(bn);
			throw(MAL, "aggr.avg", GDK_EXCEPTION);
		}
		pipeline_lock2(bn);
		BATnegateprops(bn);
		pipeline_unlock2(bn);
	} else if (pci->retc == 3 && pci->argc == 9) {
		if (bn->batCount < max &&
			(BATextend(bn, max) != GDK_SUCCEED ||
			 BATextend(cn, max) != GDK_SUCCEED ||
			 BATextend(rn, max) != GDK_SUCCEED)) {
			if (!private)
				pipeline_unlock1(bn);
			throw(MAL, "aggr.avg", GDK_EXCEPTION);
		}
		lng *cnts = Tloc(c, 0);
		lng *rems = Tloc(r, 0);
		lng *rcnts = Tloc(cn, 0);
		lng *rrems = Tloc(rn, 0);
		oid *grps = Tloc(g, 0);
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		if (qry_ctx != NULL) {
			timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
		}
		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte: {
			bte *vals = Tloc(b, 0);
			bte *rvals = Tloc(bn, 0);
			for (oid i = bn->batCount; i < max; i++) {
				rvals[i] = bte_nil;
				rcnts[i] = 0;
				rrems[i] = 0;
			}
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, timeoffset) {
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
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, timeoffset) {
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
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, timeoffset) {
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
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, timeoffset) {
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
			TIMEOUT_LOOP_IDX_DECL(i, b->batCount, timeoffset) {
				avg_aggr_comb(hge, vals[i], rems[i], cnts[i],
							  rvals[grps[i]], rrems[grps[i]], rcnts[grps[i]]);
			}
			break;
		}
#endif
		}
		TIMEOUT_CHECK(timeoffset, do{BBPunfix(bn->batCacheid);BBPunfix(rn->batCacheid);BBPunfix(cn->batCacheid);throw(MAL, "aggr.prod", GDK_EXCEPTION);}while(0));
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
		bn->T.private_bat = true; /* in case it's a new one, set the bit */

	(void)p;
	(void)max;

	(void)cntxt;
	(void)mb;
	return MAL_SUCCEED;
}

/* TODO handle nil based on argument 'skipnil' */
#define gfunc(Type, f) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
			Type *o = Tloc(r, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				if (is_##Type##_nil(o[grp[i]])) \
					o[grp[i]] = in[i]; \
				else if (!is_##Type##_nil(in[i])) \
					o[grp[i]] = f(o[grp[i]], in[i]); \
	}

#define gfunc2(Type, f) \
	if (tt == TYPE_##Type) { \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			Type *in = Tloc(b, 0); \
			Type *o = Tloc(r, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
				if (is_##Type##_nil(o[grp[i]])) \
					o[grp[i]] = in[i]; \
				else if (!is_##Type##_nil(in[i])) \
					o[grp[i]] = f(o[grp[i]], in[i]); \
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
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 2) { \
				bp += GDK_VAROFFSET; \
				op += GDK_VAROFFSET; \
				uint16_t *in = Tloc(b, 0); \
				uint16_t *o = Tloc(r, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 4) { \
				uint32_t *in = Tloc(b, 0); \
				uint32_t *o = Tloc(r, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 8) { \
				var_t *in = Tloc(b, 0); \
				var_t *o = Tloc(r, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
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
		if (tfastins_nocheckVAR( r, opos, bp+in[i]) != GDK_SUCCEED) \
			err = 1; \

#define vamax_(cmp, opos, in, i, bp, nil) \
	if (!getoffset(r->theap->base, opos, r->twidth) || \
            (cmp(bp+in[i], nil) != 0 && \
             cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), nil) != 0 && \
             cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), bp+in[i]) < 0)) \
		if (tfastins_nocheckVAR( r, opos, bp+in[i]) != GDK_SUCCEED) \
			err = 1; \

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
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 2) { \
				bp += GDK_VAROFFSET; \
				uint16_t *in = Tloc(b, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 4) { \
				uint32_t *in = Tloc(b, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 8) { \
				var_t *in = Tloc(b, 0); \
				TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} \
			bat_iterator_end(&bi); \
			bat_iterator_end(&ri); \
	}

static str
LALGmin(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0, tt = b->ttype;
	lng timeoffset = 0;

	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat), local_storage = false;

	if (!private)
		pipeline_lock1(r);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (r && BATcount(b)) {
		MT_lock_set(&r->theaplock);
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b, true)) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			err = 1;
		} else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_cache(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			assert(r->tvheap->parentid == r->batCacheid);
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
		if (ATOMvarsized(b->ttype) && VIEWvtparent(b) && BBP_cache(VIEWvtparent(b))->batRestricted == BAT_READ) {
			uint16_t width = b->twidth;
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, b->ttype, max, TRANSIENT, width);
			BATswap_heaps(r, b, p);
		} else {
			local_storage = true;
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, tt, max, TRANSIENT, b->twidth);
			if (r->tvheap && r->tvheap->base == NULL &&
				ATOMheap(r->ttype, r->tvheap, r->batCapacity) != GDK_SUCCEED)
				err = 1;
		}
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
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

	if (!err) {
		BUN cnt = BATcount(g);

		if (!err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			oid *grp = Tloc(g, 0);

			gfunc(bit,min);
			gfunc(bte,min);
			gfunc(sht,min);
			gfunc(int,min);
			gfunc(date,min);
			gfunc(lng,min);
			gfunc(daytime,min);
			gfunc(timestamp,min);
			gfunc(hge,min);
			gfunc2(uuid,uuid_min);
			gfunc(flt,min);
			gfunc(dbl,min);
			if (local_storage) {
				gafunc_(vamin);
			} else {
				gafunc(vamin);
			}
		}
		if (!err) {
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
		}
	}
	if (!private)
		pipeline_unlock1(r);
	TIMEOUT_CHECK(timeoffset, throw(MAL, "aggr.min", GDK_EXCEPTION));
	if (err)
		throw(MAL, "aggr.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

static str
LALGmax(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0, tt = b->ttype;
	lng timeoffset = 0;

	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat), local_storage = false;

	if (!private)
		pipeline_lock1(r);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (r && BATcount(b)) {
		MT_lock_set(&r->theaplock);
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b, true)) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			err = 1;
		} else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_cache(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			assert(r->tvheap->parentid == r->batCacheid);
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
		if (ATOMvarsized(b->ttype) && VIEWvtparent(b) && BBP_cache(VIEWvtparent(b))->batRestricted == BAT_READ) {
			uint16_t width = b->twidth;
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, b->ttype, max, TRANSIENT, width);
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			local_storage = true;
			r = COLnew2(0, tt, max, TRANSIENT, b->twidth);
			if (r->tvheap && r->tvheap->base == NULL &&
				ATOMheap(r->ttype, r->tvheap, r->batCapacity) != GDK_SUCCEED)
				err = 1;
		}
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
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

	if (!err) {
		BUN cnt = BATcount(g);

		if (!err) {
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
			oid *grp = Tloc(g, 0);

			gfunc(bit,max);
			gfunc(bte,max);
			gfunc(sht,max);
			gfunc(int,max);
			gfunc(date,max);
			gfunc(lng,max);
			gfunc(daytime,max);
			gfunc(timestamp,max);
			gfunc(hge,max);
			gfunc2(uuid,uuid_max);
			gfunc(flt,max);
			gfunc(dbl,max);
			if (local_storage) {
				gafunc_(vamax);
			} else {
				gafunc(vamax);
			}
		}
		if (!err) {
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
		}
	}
	if (!private)
		pipeline_unlock1(r);
	TIMEOUT_CHECK(timeoffset, throw(MAL, "aggr.max", GDK_EXCEPTION));
	if (err)
		throw(MAL, "aggr.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

static str
ALGcountCND_nil(lng *result, const bat *bid, const bat *cnd, const bit *ignore_nils)
{
	BAT *b, *s = NULL;

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
ALGcount_nil(lng *result, const bat *bid, const bit *ignore_nils)
{
	return ALGcountCND_nil(result, bid, NULL, ignore_nils);
}

static str
ALGcountCND_bat(lng *result, const bat *bid, const bat *cnd)
{
	return ALGcountCND_nil(result, bid, cnd, &(bit){0});
}

static str
ALGcount_bat(lng *result, const bat *bid)
{
	return ALGcountCND_nil(result, bid, NULL, &(bit){0});
}

static str
ALGcountCND_no_nil(lng *result, const bat *bid, const bat *cnd)
{
	return ALGcountCND_nil(result, bid, cnd, &(bit){1});
}

static str
ALGcount_no_nil(lng *result, const bat *bid)
{
	return ALGcountCND_nil(result, bid, NULL, &(bit){1});
}

static str
ALGminany_skipnil(ptr result, const bat *bid, const bit *skipnil)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "iaggr.min",
					"atom '%s' cannot be ordered linearly",
					ATOMname(b->ttype));
	} else {
		if (ATOMextern(b->ttype)) {
			const void *nil = ATOMnilptr(b->ttype);
			int (*cmp)(const void *v1,const void *v2) = ATOMcompare(b->ttype);

			p = BATmin_skipnil(b, NULL, *skipnil, false);
			if (cmp(*(ptr*)result, nil) == 0 || (cmp(p, nil) != 0 && cmp(p, *(ptr*)result) < 0))
				* (ptr *) result = p;
			else
				GDKfree(p);
		} else {
			p = BATmin_skipnil(b, result, *skipnil, true);
			if ( p != result )
				msg = createException(MAL, "iaggr.min", SQLSTATE(HY002) "INTERNAL ERROR");
		}
		if (msg == MAL_SUCCEED && p == NULL)
			msg = createException(MAL, "iaggr.min", GDK_EXCEPTION);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGminany(ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGminany_skipnil(result, bid, &skipnil);
}

static str
ALGmaxany_skipnil(ptr result, const bat *bid, const bit *skipnil)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "iaggr.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "iaggr.max",
					"atom '%s' cannot be ordered linearly",
					ATOMname(b->ttype));
	} else {
		if (ATOMextern(b->ttype)) {
			const void *nil = ATOMnilptr(b->ttype);
			int (*cmp)(const void *v1,const void *v2) = ATOMcompare(b->ttype);

			p = BATmax_skipnil(b, NULL, *skipnil, false);
			if (cmp(*(ptr*)result, nil) == 0 || (cmp(p, nil) != 0 && cmp(p, *(ptr*)result) > 0))
				* (ptr *) result = p;
			else
				GDKfree(p);
		} else {
			p = BATmax_skipnil(b, result, *skipnil, true);
			if ( p != result )
				msg = createException(MAL, "iaggr.max", SQLSTATE(HY002) "INTERNAL ERROR");
		}
		if ( msg == MAL_SUCCEED && p == NULL)
			msg = createException(MAL, "iaggr.max", GDK_EXCEPTION);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGmaxany(ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGmaxany_skipnil(result, bid, &skipnil);
}

#include "mel.h"
static mel_func pp_algebra_init_funcs[] = {
 pattern("lockedaggr", "sum", LOCKEDAGGRsum, true, "sum values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "prod", LOCKEDAGGRprod, true, "product of all values, using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 2))),
 pattern("lockedaggr", "avg", LOCKEDAGGRavg, true, "avg values into bat (bat has value, update), using the bat lock", args(2,5, sharedbatargany("", 1), sharedbatarg("rcnt", lng), arg("pipeline", ptr), argany("val", 1), arg("cnt", lng))),
 pattern("lockedaggr", "avg", LOCKEDAGGRavg, true, "avg values into bat (bat has value, update), using the bat lock", args(3,7, sharedbatargany("", 1), sharedbatarg("rremainder", lng), sharedbatarg("rcnt", lng), arg("pipeline", ptr), argany("val", 1), arg("remainder", lng), arg("cnt", lng))),
 pattern("lockedaggr", "min", LOCKEDAGGRmin, true, "min values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "max", LOCKEDAGGRmax, true, "max values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 command("lockedalgebra", "projection", LALGprojection, false, "Project left input onto right input.", args(1,4, batargany("",3), arg("pipeline", ptr), batarg("left",oid),batargany("right",3))),
 command("algebra", "unique", LALGunique, false, "Unique rows.", args(2,5, batarg("gid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",3), batarg("s",oid))),
 command("algebra", "unique", LALGgroup_unique, false, "Unique per group rows.", args(2,6, batarg("ngid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",3), batarg("s",oid), batarg("gid",oid))),
 command("group", "group", LALGgroup, false, "Group input.", args(2,4, batarg("gid", oid), batargany("sink",3), arg("pipeline", ptr), batargany("b",4))),
 command("group", "group", LALGderive, false, "Sub Group input.", args(2,6, batarg("gid", oid), batargany("sink",3), arg("pipeline", ptr), batarg("pgid", oid), batargany("phash", 5), batargany("b",3))),
 command("algebra", "projection", LALGproject, false, "Project.", args(1,4, batargany("",1), batarg("gid", oid), batargany("b",1), arg("pipeline", ptr))),
 command("aggr", "count", LALGcount, false, "Count per group.", args(1,6, batarg("",lng), batarg("gid", oid), batargany("", 1), arg("nonil", bit), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "count", LALGcountstar, false, "count per group.", args(1,4, batarg("",lng), batarg("gid", oid), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "sum", LALGsum, false, "sum per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "prod", LALGprod, false, "product per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(2,6, batarg("ravg", dbl), batarg("rcnt", lng), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(3,7, batargany("ravg",1), batarg("rremainder", lng), batarg("rcnt", lng), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(2,7, batarg("ravg", dbl), batarg("rcnt", lng), batarg("gid", oid), batargany("", 1), batarg("cnt", lng), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(3,9, batargany("ravg",1), batarg("rremainder", lng), batarg("rcnt", lng), batarg("gid", oid), batargany("", 1), batarg("remainder", lng), batarg("cnt", lng), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "min", LALGmin, false, "Min per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "max", LALGmax, false, "Max per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),

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
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pp_algebra", NULL, pp_algebra_init_funcs); }

