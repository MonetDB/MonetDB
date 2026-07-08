/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * N.J. Nes, M. Kersten
 * 07/01/1996
 * The math module
 * This module contains the math commands. The implementation is very simply,
 * the c math library functions are called. See for documentation the
 * ANSI-C/POSIX manuals of the equally named functions.
 *
 * NOTE: the operand itself is being modified, rather than that we produce
 * a new BAT. This to save the expensive copying.
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "mal_client.h"
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

static str
MATHunary_dbl(Client ctx, dbl *res, const dbl *a, double (*func)(double),
			  const char *funcname)
{
	(void) ctx;
	if (is_dbl_nil(*a)) {
		*res = dbl_nil;
	} else {
		int e = 0, ex = 0;
		errno = 0;
		feclearexcept(FE_ALL_EXCEPT);
		*res = (*func)(*a);
		e = errno;
		ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);
		if (e != 0 || ex != 0) {
			const char *err;
			const char *sqlstate;
			char buf[128];
			if (ex & FE_DIVBYZERO) {
				err = "Divide by zero";
				sqlstate = SQLSTATE(22012);
			} else if (ex & FE_OVERFLOW ||
					   (e == ERANGE && isinf(*res))) {
				err = "Overflow";
				sqlstate = SQLSTATE(22003);
			} else if (e == EDOM) {
				err = "Invalid argument";
				if (strncmp(funcname, "mmath.log", 9) == 0)
					sqlstate = SQLSTATE(2201E);
				else
					sqlstate = SQLSTATE(22003);
			} else if (e) {
				err = GDKstrerror(e, buf, sizeof(buf));
				sqlstate = "";
			} else {
				err = "Invalid result";
				sqlstate = SQLSTATE(22023);
			}
			throw(MAL, funcname, "%sMath exception: %s", sqlstate, err);
		}
	}
	return MAL_SUCCEED;
}

static str
MATHunary_flt(Client ctx, flt *res, const flt *a, float (*func)(float),
			  const char *funcname)
{
	(void) ctx;
	if (is_flt_nil(*a)) {
		*res = flt_nil;
	} else {
		int e = 0, ex = 0;
		errno = 0;
		feclearexcept(FE_ALL_EXCEPT);
		*res = (*func)(*a);
		e = errno;
		ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);
		if (e != 0 || ex != 0) {
			const char *err;
			const char *sqlstate;
			char buf[128];
			if (ex & FE_DIVBYZERO) {
				err = "Divide by zero";
				sqlstate = SQLSTATE(22012);
			} else if (ex & FE_OVERFLOW ||
					   (e == ERANGE && isinf(*res))) {
				err = "Overflow";
				sqlstate = SQLSTATE(22003);
			} else if (e == EDOM) {
				err = "Invalid argument";
				if (strncmp(funcname, "mmath.log", 9) == 0)
					sqlstate = SQLSTATE(2201E);
				else
					sqlstate = SQLSTATE(22003);
			} else if (e) {
				err = GDKstrerror(e, buf, sizeof(buf));
				sqlstate = "";
			} else {
				err = "Invalid result";
				sqlstate = SQLSTATE(22023);
			}
			throw(MAL, funcname, "%sMath exception: %s", sqlstate, err);
		}
	}
	return MAL_SUCCEED;
}

static str
MATHbinary_dbl(Client ctx, dbl *res, const dbl *a, const dbl *b,
			   double (*func)(double, double), const char *funcname)
{
	(void) ctx;
	if (is_dbl_nil(*a) || is_dbl_nil(*b)) {
		*res = dbl_nil;
	} else {
		int e = 0, ex = 0;
		errno = 0;
		feclearexcept(FE_ALL_EXCEPT);
		*res = (*func)(*a, *b);
		e = errno;
		ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);
		if (e != 0 || ex != 0) {
			const char *err;
			const char *sqlstate;
			char buf[128];
			if (ex & FE_DIVBYZERO) {
				err = "Divide by zero";
				sqlstate = SQLSTATE(22012);
			} else if (ex & FE_OVERFLOW ||
					   (e == ERANGE && isinf(*res))) {
				err = "Overflow";
				sqlstate = SQLSTATE(22003);
			} else if (e == EDOM) {
				err = "Invalid argument";
				if (strncmp(funcname, "mmath.log", 9) == 0)
					sqlstate = SQLSTATE(2201E);
				else if (strcmp(funcname, "mmath.pow") == 0)
					sqlstate = SQLSTATE(2201F);
				else
					sqlstate = SQLSTATE(22003);
			} else if (e) {
				err = GDKstrerror(e, buf, sizeof(buf));
				sqlstate = "";
			} else {
				err = "Invalid result";
				sqlstate = SQLSTATE(22023);
			}
			throw(MAL, funcname, "%sMath exception: %s", sqlstate, err);
		}
	}
	return MAL_SUCCEED;
}

static str
MATHbinary_flt(Client ctx, flt *res, const flt *a, const flt *b,
			   float (*func)(float, float), const char *funcname)
{
	(void) ctx;
	if (is_flt_nil(*a) || is_flt_nil(*b)) {
		*res = flt_nil;
	} else {
		int e = 0, ex = 0;
		errno = 0;
		feclearexcept(FE_ALL_EXCEPT);
		*res = (*func)(*a, *b);
		e = errno;
		ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);
		if (e != 0 || ex != 0) {
			const char *err;
			const char *sqlstate;
			char buf[128];
			if (ex & FE_DIVBYZERO) {
				err = "Divide by zero";
				sqlstate = SQLSTATE(22012);
			} else if (ex & FE_OVERFLOW ||
					   (e == ERANGE && isinf(*res))) {
				err = "Overflow";
				sqlstate = SQLSTATE(22003);
			} else if (e == EDOM) {
				err = "Invalid argument";
				if (strncmp(funcname, "mmath.log", 9) == 0)
					sqlstate = SQLSTATE(2201E);
				else if (strcmp(funcname, "mmath.pow") == 0)
					sqlstate = SQLSTATE(2201F);
				else
					sqlstate = SQLSTATE(22003);
			} else if (e) {
				err = GDKstrerror(e, buf, sizeof(buf));
				sqlstate = "";
			} else {
				err = "Invalid result";
				sqlstate = SQLSTATE(22023);
			}
			throw(MAL, funcname, "%sMath exception: %s", sqlstate, err);
		}
	}
	return MAL_SUCCEED;
}

static str
MATHunary_ACOSdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, acos, "mmath.acos");
}

static str
MATHunary_ACOSflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, acosf, "mmath.acos");
}

static str
MATHunary_ASINdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, asin, "mmath.asin");
}

static str
MATHunary_ASINflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, asinf, "mmath.asin");
}

static str
MATHunary_ATANdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, atan, "mmath.atan");
}

static str
MATHunary_ATANflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, atanf, "mmath.atan");
}

static str
MATHunary_COSdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, cos, "mmath.cos");
}

static str
MATHunary_COSflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, cosf, "mmath.cos");
}

static str
MATHunary_SINdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, sin, "mmath.sin");
}

static str
MATHunary_SINflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, sinf, "mmath.sin");
}

static str
MATHunary_TANdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, tan, "mmath.tan");
}

static str
MATHunary_TANflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, tanf, "mmath.tan");
}

static str
MATHunary_COTdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, cot, "mmath.cot");
}

static str
MATHunary_COTflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, cotf, "mmath.cot");
}

static str
MATHunary_RADIANSdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, radians, "mmath.radians");
}

static str
MATHunary_RADIANSflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, radiansf, "mmath.radians");
}

static str
MATHunary_DEGREESdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, degrees, "mmath.degrees");
}

static str
MATHunary_DEGREESflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, degreesf, "mmath.degrees");
}

static str
MATHunary_COSHdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, cosh, "mmath.cosh");
}

static str
MATHunary_COSHflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, coshf, "mmath.cosh");
}

static str
MATHunary_SINHdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, sinh, "mmath.sinh");
}

static str
MATHunary_SINHflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, sinhf, "mmath.sinh");
}

static str
MATHunary_TANHdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, tanh, "mmath.tanh");
}

static str
MATHunary_TANHflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, tanhf, "mmath.tanh");
}

static str
MATHunary_EXPdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, exp, "mmath.exp");
}

static str
MATHunary_EXPflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, expf, "mmath.exp");
}

static str
MATHunary_LOGdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, log, "mmath.log");
}

static str
MATHunary_LOGflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, logf, "mmath.log");
}

static str
MATHunary_LOG10dbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, log10, "mmath.log10");
}

static str
MATHunary_LOG10flt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, log10f, "mmath.log10");
}

static str
MATHunary_LOG2dbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, log2, "mmath.log2");
}

static str
MATHunary_LOG2flt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, log2f, "mmath.log2");
}

static str
MATHunary_SQRTdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, sqrt, "mmath.sqrt");
}

static str
MATHunary_SQRTflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, sqrtf, "mmath.sqrt");
}

static str
MATHunary_CBRTdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, cbrt, "mmath.cbrt");
}

static str
MATHunary_CBRTflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, cbrtf, "mmath.cbrt");
}

static str
MATHunary_CEILdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, ceil, "mmath.ceil");
}

static str
MATHunary_CEILflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, ceilf, "mmath.ceil");
}

static str
MATHunary_FLOORdbl(Client ctx, dbl *res, const dbl *a)
{
	return MATHunary_dbl(ctx, res, a, floor, "mmath.floor");
}

static str
MATHunary_FLOORflt(Client ctx, flt *res, const flt *a)
{
	return MATHunary_flt(ctx, res, a, floorf, "mmath.floor");
}

static str
MATHbinary_ATAN2dbl(Client ctx, dbl *res, const dbl *a, const dbl *b)
{
	return MATHbinary_dbl(ctx, res, a, b, atan2, "mmath.atan2");
}

static str
MATHbinary_ATAN2flt(Client ctx, flt *res, const flt *a, const flt *b)
{
	return MATHbinary_flt(ctx, res, a, b, atan2f, "mmath.atan2");
}

static str
MATHbinary_POWdbl(Client ctx, dbl *res, const dbl *a, const dbl *b)
{
	return MATHbinary_dbl(ctx, res, a, b, pow, "mmath.pow");
}

static str
MATHbinary_POWflt(Client ctx, flt *res, const flt *a, const flt *b)
{
	return MATHbinary_flt(ctx, res, a, b, powf, "mmath.pow");
}

static str
MATHbinary_LOGdbl(Client ctx, dbl *res, const dbl *a, const dbl *b)
{
	return MATHbinary_dbl(ctx, res, a, b, logbs, "mmath.logbs");
}

static str
MATHbinary_LOGflt(Client ctx, flt *res, const flt *a, const flt *b)
{
	return MATHbinary_flt(ctx, res, a, b, logbsf, "mmath.logbs");
}

static str
MATHbinary_NEXTAFTERdbl(Client ctx, dbl *res, const dbl *a, const dbl *b)
{
	return MATHbinary_dbl(ctx, res, a, b, nextafter, "mmath.nextafter");
}

static str
MATHbinary_NEXTAFTERflt(Client ctx, flt *res, const flt *a, const flt *b)
{
	return MATHbinary_flt(ctx, res, a, b, nextafterf, "mmath.nextafter");
}


static str
MATHunary_FABSdbl(Client ctx, dbl *res, const dbl *a)
{
	(void) ctx;
	*res = is_dbl_nil(*a) ? dbl_nil : fabs(*a);
	return MAL_SUCCEED;
}

static str
MATHbinary_ROUNDdbl(Client ctx, dbl *res, const dbl *x, const int *y)
{
	(void) ctx;
	if (is_dbl_nil(*x) || is_int_nil(*y)) {
		*res = dbl_nil;
	} else {
		dbl factor = pow(10, *y), integral;
		dbl tmp = *y > 0 ? modf(*x, &integral) : *x;

		tmp *= factor;
		if (tmp >= 0)
			tmp = floor(tmp + 0.5);
		else
			tmp = ceil(tmp - 0.5);
		tmp /= factor;

		if (*y > 0)
			tmp += integral;

		*res = (dbl) tmp;
	}
	return MAL_SUCCEED;
}

static str
MATHbinary_ROUNDflt(Client ctx, flt *res, const flt *x, const int *y)
{
	(void) ctx;
	if (is_flt_nil(*x) || is_int_nil(*y)) {
		*res = flt_nil;
	} else {
		dbl factor = pow(10, *y), integral;
		dbl tmp = *y > 0 ? modf(*x, &integral) : *x;

		tmp *= factor;
		if (tmp >= 0)
			tmp = floor(tmp + 0.5);
		else
			tmp = ceil(tmp - 0.5);
		tmp /= factor;

		if (*y > 0)
			tmp += integral;

		*res = (flt) tmp;
	}
	return MAL_SUCCEED;
}

static str
MATHunary_ISNAN(Client ctx, bit *res, const dbl *a)
{
	(void) ctx;
	if (is_dbl_nil(*a)) {
		*res = bit_nil;
	} else {
		*res = isnan(*a) != 0;
	}
	return MAL_SUCCEED;
}

static str
MATHunary_ISINF(Client ctx, int *res, const dbl *a)
{
	(void) ctx;
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
MATHunary_FINITE(Client ctx, bit *res, const dbl *a)
{
	(void) ctx;
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
MATHprelude(void)
{
	init_random_state_engine(mmath_rse, (uint64_t) GDKusec());
	return MAL_SUCCEED;
}

static str
MATHrandint(Client ctx, int *res)
{
	(void) ctx;
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
MATHrandintarg(Client ctx, int *res, const int *dummy)
{
	(void) ctx;
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
MATHsrandint(Client ctx, void *ret, const int *seed)
{
	(void) ctx;
	(void) ret;
	MT_lock_set(&mmath_rse_lock);
	init_random_state_engine(mmath_rse, (uint64_t) * seed);
	MT_lock_unset(&mmath_rse_lock);
	return MAL_SUCCEED;
}

static str
MATHsqlrandint(Client ctx, int *res, const int *seed)
{
	(void) ctx;
#ifdef __COVERITY__
	(void) seed;
	*res = 0;
#else
	MT_lock_set(&mmath_rse_lock);
	init_random_state_engine(mmath_rse, (uint64_t) * seed);
	*res = (int) (next(mmath_rse) >> 33);
	MT_lock_unset(&mmath_rse_lock);
#endif
	return MAL_SUCCEED;
}

static str
MATHpi(Client ctx, dbl *pi)
{
	(void) ctx;
	*pi = M_PI;
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func mmath_init_funcs[] = {
 command("mmath", "acos", MATHunary_ACOSflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "acos", MATHunary_ACOSdbl, false, "The acos(x) function calculates the arc cosine of x, that is the value whose cosine is x. The value is returned in radians and is mathematically defined to be between 0 and PI (inclusive).", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "asin", MATHunary_ASINflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "asin", MATHunary_ASINdbl, false, "The asin(x) function calculates the arc sine of x, that is the value whose sine is x. The value is returned in radians and is mathematically defined to be between -PI/20 and -PI/2 (inclusive).", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "atan", MATHunary_ATANflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "atan", MATHunary_ATANdbl, false, "The atan(x) function calculates the arc tangent of x, that is the value whose tangent is x. The value is returned in radians and is mathematically defined to be between -PI/2 and PI/2 (inclusive).", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "atan2", MATHbinary_ATAN2flt, false, "", args(1,3, arg("",flt),arg("x",flt),arg("y",flt))),
 command("mmath", "atan2", MATHbinary_ATAN2dbl, false, "The atan2(x,y) function calculates the arc tangent of the two variables x and y. It is similar to calculating the arc tangent of y / x, except that the signs of both arguments are used to determine the quadrant of the result. The value is returned in radians and is mathematically defined to be between -PI/2 and PI/2 (inclusive).", args(1,3, arg("",dbl),arg("x",dbl),arg("y",dbl))),
 command("mmath", "cos", MATHunary_COSflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "cos", MATHunary_COSdbl, false, "The cos(x) function returns the cosine of x, where x is given in radians. The return value is between -1 and 1.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "sin", MATHunary_SINflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "sin", MATHunary_SINdbl, false, "The sin(x) function returns the cosine of x, where x is given in radians. The return value is between -1 and 1.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "tan", MATHunary_TANflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "tan", MATHunary_TANdbl, false, "The tan(x) function returns the tangent of x, where x is given in radians", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "cot", MATHunary_COTflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "cot", MATHunary_COTdbl, false, "The cot(x) function returns the Cotangent of x, where x is given in radians", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "cosh", MATHunary_COSHflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "cosh", MATHunary_COSHdbl, false, "The cosh() function returns the hyperbolic cosine of x, which is defined mathematically as (exp(x) + exp(-x)) / 2.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "sinh", MATHunary_SINHflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "sinh", MATHunary_SINHdbl, false, "The sinh() function returns the hyperbolic sine of x, which is defined mathematically as (exp(x) - exp(-x)) / 2.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "tanh", MATHunary_TANHflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "tanh", MATHunary_TANHdbl, false, "The tanh() function returns the hyperbolic tangent of x, which is defined mathematically as sinh(x) / cosh(x).", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "radians", MATHunary_RADIANSflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "radians", MATHunary_RADIANSdbl, false, "The radians() function converts degrees into radians", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "degrees", MATHunary_DEGREESflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "degrees", MATHunary_DEGREESdbl, false, "The degrees() function converts radians into degrees", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "exp", MATHunary_EXPflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "exp", MATHunary_EXPdbl, false, "The exp(x) function returns the value of e (the base of natural logarithms) raised to the power of x.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "log", MATHunary_LOGflt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "log", MATHunary_LOGdbl, false, "The log(x) function returns the natural logarithm of x.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "log2arg", MATHbinary_LOGflt, false, "", args(1,3, arg("",flt),arg("x",flt),arg("base",flt))),
 command("mmath", "log2arg", MATHbinary_LOGdbl, false, "The log(base,x) function returns the logarithm of x in the given base.", args(1,3, arg("",dbl),arg("x",dbl),arg("base",dbl))),
 command("mmath", "log10", MATHunary_LOG10flt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "log10", MATHunary_LOG10dbl, false, "The log10(x) function returns the base-10 logarithm of x.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "log2", MATHunary_LOG2flt, false, "", args(1,2, arg("",flt),arg("x",flt))),
 command("mmath", "log2", MATHunary_LOG2dbl, false, "The log2(x) function returns the base-2 logarithm of x.", args(1,2, arg("",dbl),arg("x",dbl))),
 command("mmath", "pow", MATHbinary_POWflt, false, "", args(1,3, arg("",flt),arg("x",flt),arg("y",flt))),
 command("mmath", "pow", MATHbinary_POWdbl, false, "The pow(x,y) function returns the value of x raised to the power of y.", args(1,3, arg("",dbl),arg("x",dbl),arg("y",dbl))),
 command("mmath", "nextafter", MATHbinary_NEXTAFTERflt, false, "The nextafter(x,y) function returns the next representable floating-point value of x in the direction of y.", args(1,3, arg("",flt),arg("x",flt),arg("y",flt))),
 command("mmath", "nextafter", MATHbinary_NEXTAFTERdbl, false, "The nextafter(x,y) function returns the next representable floating-point value of x in the direction of y.", args(1,3, arg("",dbl),arg("x",dbl),arg("y",dbl))),
 command("mmath", "sqrt", MATHunary_SQRTflt, false, "", args(1,2, arg("",flt),arg("y",flt))),
 command("mmath", "sqrt", MATHunary_SQRTdbl, false, "The sqrt(x) function returns the non-negative root of x.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "cbrt", MATHunary_CBRTflt, false, "", args(1,2, arg("",flt),arg("y",flt))),
 command("mmath", "cbrt", MATHunary_CBRTdbl, false, "The cbrt(x) function returns the cube root of x.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "ceil", MATHunary_CEILflt, false, "", args(1,2, arg("",flt),arg("y",flt))),
 command("mmath", "ceil", MATHunary_CEILdbl, false, "The ceil(x) function rounds x upwards to the nearest integer.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "fabs", MATHunary_FABSdbl, false, "The fabs(x) function returns the absolute value of the floating-point number x.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "floor", MATHunary_FLOORflt, false, "", args(1,2, arg("",flt),arg("y",flt))),
 command("mmath", "floor", MATHunary_FLOORdbl, false, "The floor(x) function rounds x downwards to the nearest integer.", args(1,2, arg("",dbl),arg("y",dbl))),
 command("mmath", "round", MATHbinary_ROUNDflt, false, "", args(1,3, arg("",flt),arg("x",flt),arg("y",int))),
 command("mmath", "round", MATHbinary_ROUNDdbl, false, "The round(n, m) returns n rounded to m places to the right of the decimal point; if m is omitted, to 0 places. m can be negative to round off digits left of the decimal point. m must be an integer.", args(1,3, arg("",dbl),arg("x",dbl),arg("y",int))),
 command("mmath", "isnan", MATHunary_ISNAN, false, "The isnan(x) function returns true if x is 'not-a-number' (NaN), and false otherwise.", args(1,2, arg("",bit),arg("d",dbl))),
 command("mmath", "isinf", MATHunary_ISINF, false, "The isinf(x) function returns -1 if x represents negative infinity, 1 if x represents positive infinity, and 0 otherwise.", args(1,2, arg("",int),arg("d",dbl))),
 command("mmath", "finite", MATHunary_FINITE, false, "The finite(x) function returns true if x is neither infinite nor a 'not-a-number' (NaN) value, and false otherwise.", args(1,2, arg("",bit),arg("d",dbl))),
 command("mmath", "rand", MATHrandint, true, "return a random number", args(1,1, arg("",int))),
 command("mmath", "rand", MATHrandintarg, true, "return a random number", args(1,2, arg("",int),arg("v",int))),
 command("mmath", "srand", MATHsrandint, false, "initialize the rand() function with a seed", args(1,2, arg("",void),arg("seed",int))),
 command("mmath", "sqlrand", MATHsqlrandint, false, "initialize the rand() function with a seed and call rand()", args(1,2, arg("",int),arg("seed",int))),
 command("mmath", "pi", MATHpi, false, "return an important mathematical value", args(1,1, arg("",dbl))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mmath_mal)
{ mal_module2("mmath", NULL, mmath_init_funcs, MATHprelude, NULL); }
