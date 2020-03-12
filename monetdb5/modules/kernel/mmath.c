/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * N.J. Nes, M. Kersten
 * 07/01/1996
 * The math module
 * This module contains the math commands. The implementation is very simply,
 * the c math library functions are called. See for documentation the
 * ANSI-C/POSIX manuals of the equaly named functions.
 *
 * NOTE: the operand itself is being modified, rather than that we produce
 * a new BAT. This to save the expensive copying.
 */
#include "monetdb_config.h"
#include "mmath.h"
#include <fenv.h>
#include "mmath_private.h"
#ifndef FE_INVALID
#define FE_INVALID			0
#endif
#ifndef FE_DIVBYZERO
#define FE_DIVBYZERO		0
#endif
#ifndef FE_OVERFLOW
#define FE_OVERFLOW			0
#endif

#define cot(x)				(1 / tan(x))
#define radians(x)			((x) * 3.14159265358979323846 / 180.0)
#define degrees(x)			((x) * 180.0 / 3.14159265358979323846)

double
logbs(double x, double base)
{
	return log(x) / log(base);
}

float
logbsf(float x, float base)
{
	return logf(x) / logf(base);
}

#define unopbaseM5(NAME, FUNC, TYPE)								\
str																	\
MATHunary##NAME##TYPE(TYPE *res , const TYPE *a)					\
{																	\
	if (is_##TYPE##_nil(*a)) {										\
		*res = TYPE##_nil;											\
	} else {														\
		double a1 = *a, r;											\
		int e = 0, ex = 0;											\
		errno = 0;													\
		feclearexcept(FE_ALL_EXCEPT);								\
		r = FUNC(a1);												\
		if ((e = errno) != 0 ||										\
			(ex = fetestexcept(FE_INVALID | FE_DIVBYZERO |			\
							   FE_OVERFLOW)) != 0) {				\
			const char *err;										\
			if (e) {												\
				err = GDKstrerror(e, (char[128]){0}, 128);			\
			} else if (ex & FE_DIVBYZERO)							\
				err = "Divide by zero";								\
			else if (ex & FE_OVERFLOW)								\
				err = "Overflow";									\
			else													\
				err = "Invalid result";								\
			throw(MAL, "mmath." #FUNC, "Math exception: %s", err);	\
		}															\
		*res = (TYPE) r;											\
	}																\
	return MAL_SUCCEED;												\
}

#define unopM5(NAME, FUNC)						\
	unopbaseM5(NAME, FUNC, dbl)					\
	unopbaseM5(NAME, FUNC, flt)

#define binopbaseM5(NAME, FUNC, TYPE)								\
str																	\
MATHbinary##NAME##TYPE(TYPE *res, const TYPE *a, const TYPE *b)		\
{																	\
	if (is_##TYPE##_nil(*a) || is_##TYPE##_nil(*b)) {				\
		*res = TYPE##_nil;											\
	} else {														\
		double r1, a1 = *a, b1 = *b;								\
		int e = 0, ex = 0;											\
		errno = 0;													\
		feclearexcept(FE_ALL_EXCEPT);								\
		r1 = FUNC(a1, b1);											\
		if ((e = errno) != 0 ||										\
			(ex = fetestexcept(FE_INVALID | FE_DIVBYZERO |			\
							   FE_OVERFLOW)) != 0) {				\
			const char *err;										\
			if (e) {												\
				err = GDKstrerror(e, (char[128]){0}, 128);			\
			} else if (ex & FE_DIVBYZERO)							\
				err = "Divide by zero";								\
			else if (ex & FE_OVERFLOW)								\
				err = "Overflow";									\
			else													\
				err = "Invalid result";								\
			throw(MAL, "mmath." #FUNC, "Math exception: %s", err);	\
		}															\
		*res= (TYPE) r1;											\
	}																\
	return MAL_SUCCEED;												\
}

#define unopM5NOT(NAME, FUNC)									\
str																\
MATHunary##NAME##dbl(dbl *res , const dbl *a)					\
{																\
	(void)res;	\
	(void)a;	\
	throw(MAL, "mmath." #FUNC, SQLSTATE(0A000) PROGRAM_NYI);	\
}																\
str																\
MATHunary##NAME##flt(flt *res , const flt *a)					\
{																\
	(void)res;	\
	(void)a;	\
	throw(MAL, "mmath." #FUNC, SQLSTATE(0A000) PROGRAM_NYI);	\
}

#define binopM5(NAME, FUNC)						\
  binopbaseM5(NAME, FUNC, dbl)					\
  binopbaseM5(NAME, FUNC, flt)

#define roundM5(TYPE)											\
str																\
MATHbinary_ROUND##TYPE(TYPE *res, const TYPE *x, const int *y)	\
{																\
	if (is_##TYPE##_nil(*x) || is_int_nil(*y)) {				\
		*res = TYPE##_nil;										\
	} else {													\
		dbl factor = pow(10,*y), integral;						\
		dbl tmp = *y > 0 ? modf(*x, &integral) : *x;			\
																\
		tmp *= factor;											\
		if (tmp >= 0)											\
			tmp = floor(tmp + 0.5);								\
		else													\
			tmp = ceil(tmp - 0.5);								\
		tmp /= factor;											\
																\
		if (*y > 0)												\
			tmp += integral;									\
																\
		*res = (TYPE) tmp;										\
	}															\
	return MAL_SUCCEED;											\
}


unopM5(_ACOS,acos)
unopM5(_ASIN,asin)
unopM5(_ATAN,atan)
binopM5(_ATAN2,atan2)
unopM5(_COS,cos)
unopM5(_SIN,sin)
unopM5(_TAN,tan)
unopM5(_COT,cot)
unopM5(_RADIANS,radians)
unopM5(_DEGREES,degrees)

unopM5(_COSH,cosh)
unopM5(_SINH,sinh)
unopM5(_TANH,tanh)

unopM5(_EXP,exp)
unopM5(_LOG,log)
unopM5(_LOG10,log10)
unopM5(_LOG2,log2)

binopM5(_LOG,logbs)

binopM5(_POW,pow)
unopM5(_SQRT,sqrt)
#ifdef HAVE_CBRT
unopM5(_CBRT,cbrt)
#else
unopM5NOT(_CBRT,cbrt)
#endif

unopM5(_CEIL,ceil)
unopM5(_FLOOR,floor)

str
MATHunary_FABSdbl(dbl *res , const dbl *a)
{
	*res = is_dbl_nil(*a) ? dbl_nil : fabs(*a);
	return MAL_SUCCEED;
}

roundM5(dbl)
roundM5(flt)

str
MATHunary_ISNAN(bit *res, const dbl *a)
{
	if (is_dbl_nil(*a)) {
		*res = bit_nil;
	} else {
		*res = isnan(*a) != 0;
	}
	return MAL_SUCCEED;
}

str
MATHunary_ISINF(int *res, const dbl *a)
{
	if (is_dbl_nil(*a)) {
		*res = int_nil;
	} else {
		if (isinf(*a)) {
			*res = (*a < 0.0) ? -1 : 1;
		} else {
			*res = 0;
		}
	}
	return MAL_SUCCEED;
}

str
MATHunary_FINITE(bit *res, const dbl *a)
{
	if (is_dbl_nil(*a)) {
		*res = bit_nil;
	} else {
		*res = isfinite(*a) != 0;
	}
	return MAL_SUCCEED;
}

#include "xoshiro256starstar.h"

/* global pseudo random generator state */
static random_state_engine mmath_rse;
/* serialize access to state */
static MT_Lock mmath_rse_lock = MT_LOCK_INITIALIZER("mmath_rse_lock");

str
MATHprelude(void *ret)
{
	(void) ret;
	init_random_state_engine(mmath_rse, (uint64_t) GDKusec());
	return MAL_SUCCEED;
}

str
MATHrandint(int *res)
{
#ifdef STATIC_CODE_ANALYSIS
	*res = 0;
#else
	MT_lock_set(&mmath_rse_lock);
	*res = (int) (next(mmath_rse) >> 33);
	MT_lock_unset(&mmath_rse_lock);
#endif
	return MAL_SUCCEED;
}

str
MATHrandintarg(int *res, const int *dummy)
{
	(void) dummy;
#ifdef STATIC_CODE_ANALYSIS
	*res = 0;
#else
	MT_lock_set(&mmath_rse_lock);
	*res = (int) (next(mmath_rse) >> 33);
	MT_lock_unset(&mmath_rse_lock);
#endif
	return MAL_SUCCEED;
}

str
MATHsrandint(void *ret, const int *seed)
{
	(void) ret;
	MT_lock_set(&mmath_rse_lock);
	init_random_state_engine(mmath_rse, (uint64_t) *seed);
	MT_lock_unset(&mmath_rse_lock);
	return MAL_SUCCEED;
}

str
MATHsqlrandint(int *res, const int *seed)
{
#ifdef STATIC_CODE_ANALYSIS
	(void) seed;
	*res = 0;
#else
	MT_lock_set(&mmath_rse_lock);
	init_random_state_engine(mmath_rse, (uint64_t) *seed);
	*res = (int) (next(mmath_rse) >> 33);
	MT_lock_unset(&mmath_rse_lock);
#endif
	return MAL_SUCCEED;
}

str
MATHpi(dbl *pi)
{
	*pi = 3.14159265358979323846;
	return MAL_SUCCEED;
}
