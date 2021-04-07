/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal.h"
#include "mal_exception.h"
#include <fenv.h>
#include "mmath_private.h"

double
cot(double x)
{
	return 1.0 / tan(x);
}

float
cotf(float x)
{
	return 1.0f / tanf(x);
}

double
radians(double x)
{
	return x * (M_PI / 180.0);
}

float
radiansf(float x)
{
	return x * (M_PIF / 180.0f);
}

double
degrees(double x)
{
	return x * (180.0 / M_PI);
}

float
degreesf(float x)
{
	return x * (180.0f / M_PIF);
}

double
logbs(double base, double x)
{
	if (base == 1) {
		feraiseexcept(FE_DIVBYZERO);
		return INFINITY;
	}
	return log(x) / log(base);
}

float
logbsf(float base, float x)
{
	if (base == 1) {
		feraiseexcept(FE_DIVBYZERO);
		return INFINITY;
	}
	return logf(x) / logf(base);
}

#define unopbaseM5(NAME, FUNC, TYPE)								\
static str															\
MATHunary##NAME##TYPE(TYPE *res, const TYPE *a)						\
{																	\
	if (is_##TYPE##_nil(*a)) {										\
		*res = TYPE##_nil;											\
	} else {														\
		int e = 0, ex = 0;											\
		errno = 0;													\
		feclearexcept(FE_ALL_EXCEPT);								\
		*res = FUNC(*a);											\
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
	}																\
	return MAL_SUCCEED;												\
}

#define unopM5(NAME, FUNC)						\
	unopbaseM5(NAME, FUNC, dbl)					\
	unopbaseM5(NAME, FUNC##f, flt)

#define binopbaseM5(NAME, FUNC, TYPE)								\
static str															\
MATHbinary##NAME##TYPE(TYPE *res, const TYPE *a, const TYPE *b)		\
{																	\
	if (is_##TYPE##_nil(*a) || is_##TYPE##_nil(*b)) {				\
		*res = TYPE##_nil;											\
	} else {														\
		int e = 0, ex = 0;											\
		errno = 0;													\
		feclearexcept(FE_ALL_EXCEPT);								\
		*res = FUNC(*a, *b);										\
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
	}																\
	return MAL_SUCCEED;												\
}

#define binopM5(NAME, FUNC)						\
  binopbaseM5(NAME, FUNC, dbl)					\
  binopbaseM5(NAME, FUNC##f, flt)

#define roundM5(TYPE)											\
static str														\
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
unopM5(_SQRT,sqrt)
unopM5(_CBRT,cbrt)
unopM5(_CEIL,ceil)
unopM5(_FLOOR,floor)

binopM5(_ATAN2,atan2)
binopM5(_POW,pow)
binopM5(_LOG,logbs)

static str
MATHunary_FABSdbl(dbl *res , const dbl *a)
{
	*res = is_dbl_nil(*a) ? dbl_nil : fabs(*a);
	return MAL_SUCCEED;
}

roundM5(dbl)
roundM5(flt)

static str
MATHunary_ISNAN(bit *res, const dbl *a)
{
	if (is_dbl_nil(*a)) {
		*res = bit_nil;
	} else {
		*res = isnan(*a) != 0;
	}
	return MAL_SUCCEED;
}

static str
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

static str
MATHunary_FINITE(bit *res, const dbl *a)
{
	if (is_dbl_nil(*a)) {
		*res = bit_nil;
	} else {
		*res = isfinite(*a) != 0;
	}
	return MAL_SUCCEED;
}

/* global pseudo random generator state */
random_state_engine mmath_rse;
/* serialize access to state */
MT_Lock mmath_rse_lock = MT_LOCK_INITIALIZER(mmath_rse_lock);

static str
MATHprelude(void *ret)
{
	(void) ret;
	init_random_state_engine(mmath_rse, (uint64_t) GDKusec());
	return MAL_SUCCEED;
}

static str
MATHrandint(int *res)
{
#ifdef __COVERITY__
	*res = 0;
#else
	MT_lock_set(&mmath_rse_lock);
	*res = (int) (next(mmath_rse) >> 33);
	MT_lock_unset(&mmath_rse_lock);
#endif
	return MAL_SUCCEED;
}

static str
MATHrandintarg(int *res, const int *dummy)
{
	(void) dummy;
#ifdef __COVERITY__
	*res = 0;
#else
	MT_lock_set(&mmath_rse_lock);
	*res = (int) (next(mmath_rse) >> 33);
	MT_lock_unset(&mmath_rse_lock);
#endif
	return MAL_SUCCEED;
}

static str
MATHsrandint(void *ret, const int *seed)
{
	(void) ret;
	MT_lock_set(&mmath_rse_lock);
	init_random_state_engine(mmath_rse, (uint64_t) *seed);
	MT_lock_unset(&mmath_rse_lock);
	return MAL_SUCCEED;
}

static str
MATHsqlrandint(int *res, const int *seed)
{
#ifdef __COVERITY__
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

static str
MATHpi(dbl *pi)
{
	*pi = M_PI;
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func mmath_init_funcs[] = {
 command("mmath", "acos", MATHunary_ACOSflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "acos", MATHunary_ACOSdbl, false, "The acos(x) function calculates the arc cosine of x, that is the \nvalue whose cosine is x. The value is returned in radians and is \nmathematically defined to be between 0 and PI (inclusive).", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "asin", MATHunary_ASINflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "asin", MATHunary_ASINdbl, false, "The asin(x) function calculates the arc sine of x, that is the value \nwhose sine is x. The value is returned in radians and is mathematically \ndefined to be between -PI/20 and -PI/2 (inclusive).", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "atan", MATHunary_ATANflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "atan", MATHunary_ATANdbl, false, "The atan(x) function calculates the arc tangent of x, that is the value \nwhose tangent is x. The value is returned in radians and is mathematically \ndefined to be between -PI/2 and PI/2 (inclusive).", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "atan2", MATHbinary_ATAN2flt, false, "", args(1,3, arg("",flt),arg("x",flt),arg("y",flt))),
 command("mmath", "atan2", MATHbinary_ATAN2dbl, false, "The atan2(x,y) function calculates the arc tangent of the two \nvariables x and y.  It is similar to calculating the arc\ntangent of y / x, except that the signs of both arguments are \nused to determine the quadrant of the result.  The value is \nreturned in radians and is mathematically defined to be between \n-PI/2 and PI/2 (inclusive).", args(1,3, arg("",dbl),arg("x",dbl),arg("y",dbl))),
 command("mmath", "cos", MATHunary_COSflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "cos", MATHunary_COSdbl, false, "The cos(x) function returns the cosine of x, where x is given in \nradians. The return value is between -1 and 1.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "sin", MATHunary_SINflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "sin", MATHunary_SINdbl, false, "The sin(x) function returns the cosine of x, where x is given in \nradians. The return value is between -1 and 1.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "tan", MATHunary_TANflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "tan", MATHunary_TANdbl, false, "The tan(x) function returns the tangent of x,\nwhere x is given in radians", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "cot", MATHunary_COTflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "cot", MATHunary_COTdbl, false, "The cot(x) function returns the Cotangent of x,\nwhere x is given in radians", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "cosh", MATHunary_COSHflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "cosh", MATHunary_COSHdbl, false, "The cosh() function  returns the hyperbolic cosine of x, which is \ndefined mathematically as (exp(x) + exp(-x)) / 2.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "sinh", MATHunary_SINHflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "sinh", MATHunary_SINHdbl, false, "The sinh() function  returns  the  hyperbolic sine of x, which \nis defined mathematically as (exp(x) - exp(-x)) / 2.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "tanh", MATHunary_TANHflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "tanh", MATHunary_TANHdbl, false, "The tanh() function returns the hyperbolic tangent of x, which is \ndefined mathematically as sinh(x) / cosh(x).", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "radians", MATHunary_RADIANSflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "radians", MATHunary_RADIANSdbl, false, "The radians() function converts degrees into radians", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "degrees", MATHunary_DEGREESflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "degrees", MATHunary_DEGREESdbl, false, "The degrees() function converts radians into degrees", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "exp", MATHunary_EXPflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "exp", MATHunary_EXPdbl, false, "The exp(x) function returns the value of e (the base of \nnatural logarithms) raised to the power of x.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "log", MATHunary_LOGflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "log", MATHunary_LOGdbl, false, "The log(x) function returns the natural logarithm of x.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "log2arg", MATHbinary_LOGflt, false, "The log(x) function returns the logarithm of x in the given base.", args(1,3, arg("",flt),arg("x",flt),arg("base",flt))),
 command("mmath", "log2arg", MATHbinary_LOGdbl, false, "The log(x) function returns the logarithm of x in the given base.", args(1,3, arg("",dbl),arg("x",dbl),arg("base",dbl))),
 command("mmath", "log10", MATHunary_LOG10flt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "log10", MATHunary_LOG10dbl, false, "The log10(x) function returns the base-10 logarithm of x.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "log2", MATHunary_LOG2flt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "log2", MATHunary_LOG2dbl, false, "The log2(x) function returns the base-2 logarithm of x.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "pow", MATHbinary_POWflt, false, "", args(1,3, arg("",flt),arg("x",flt),arg("y",flt))),
 command("mmath", "pow", MATHbinary_POWdbl, false, "The pow(x,y) function  returns the value of x raised to the power of y.", args(1,3, arg("",dbl),arg("x",dbl),arg("y",dbl))),
 command("mmath", "sqrt", MATHunary_SQRTflt, false, "", args(1,2, arg("",flt),arg("y",flt))),
 command("mmath", "sqrt", MATHunary_SQRTdbl, false, "The sqrt(x) function returns the non-negative root of x.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "cbrt", MATHunary_CBRTflt, false, "", args(1,2, arg("",flt),arg("y",flt))),
 command("mmath", "cbrt", MATHunary_CBRTdbl, false, "The cbrt(x) function returns the cube root of x.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "ceil", MATHunary_CEILflt, false, "", args(1,2, arg("",flt),arg("y",flt))),
 command("mmath", "ceil", MATHunary_CEILdbl, false, "The ceil(x) function rounds x upwards to the nearest integer.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "fabs", MATHunary_FABSdbl, false, "The fabs(x) function  returns  the  absolute value of the \nfloating-point number x.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "floor", MATHunary_FLOORflt, false, "", args(1,2, arg("",flt),arg("y",flt))),
 command("mmath", "floor", MATHunary_FLOORdbl, false, "The floor(x) function rounds x downwards to the nearest integer.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "round", MATHbinary_ROUNDflt, false, "", args(1,3, arg("",flt),arg("x",flt),arg("y",int))),
 command("mmath", "round", MATHbinary_ROUNDdbl, false, "The round(n, m) returns n rounded to m places to the right \nof the decimal point; if m is omitted, to 0 places. m can be \nnegative to round off digits left of the decimal point. \nm must be an integer.", args(1,3, arg("",dbl),arg("x",dbl),arg("y",int))),
 command("mmath", "isnan", MATHunary_ISNAN, false, "The isnan(x) function returns true if x is 'not-a-number' \n(NaN), and false otherwise.", args(1,2, arg("",bit),arg("d",dbl))),
 command("mmath", "isinf", MATHunary_ISINF, false, "The isinf(x) function returns -1 if x represents negative \ninfinity, 1 if x represents positive infinity, and 0 otherwise.", args(1,2, arg("",int),arg("d",dbl))),
 command("mmath", "finite", MATHunary_FINITE, false, "The finite(x) function returns true if x is neither infinite \nnor a 'not-a-number' (NaN) value, and false otherwise.", args(1,2, arg("",bit),arg("d",dbl))),
 command("mmath", "rand", MATHrandint, true, "return a random number", args(1,1, arg("",int))),
 command("mmath", "rand", MATHrandintarg, true, "return a random number", args(1,2, arg("",int),arg("v",int))),
 command("mmath", "srand", MATHsrandint, false, "initialize the rand() function with a seed", args(1,2, arg("",void),arg("seed",int))),
 command("mmath", "sqlrand", MATHsqlrandint, false, "initialize the rand() function with a seed and call rand()", args(1,2, arg("",int),arg("seed",int))),
 command("mmath", "pi", MATHpi, false, "return an important mathematical value", args(1,1, arg("",dbl))),
 command("mmath", "prelude", MATHprelude, false, "initilize mmath module", args(1,1, arg("",void))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mmath_mal)
{ mal_module("mmath", NULL, mmath_init_funcs); }
