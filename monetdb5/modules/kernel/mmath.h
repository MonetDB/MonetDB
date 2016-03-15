/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * The constants defined in math.h are defined in const.mx
 */
#ifndef __MMATH_H__
#define __MMATH_H__
#include "mal.h"
#include "mal_exception.h"
#include <math.h>

#ifdef WIN32
# include <float.h>
#if _MSC_VER <= 1600
/* Windows spells these differently */
# define isnan(x)	_isnan(x)
#endif
# define finite(x)	_finite(x)
/* NOTE: HAVE_FPCLASS assumed... */
# define fpclass(x)	_fpclass(x)
# define FP_NINF		_FPCLASS_NINF
# define FP_PINF		_FPCLASS_PINF
#else /* !_MSC_VER */
# ifdef HAVE_IEEEFP_H
#  include <ieeefp.h>
# endif
#endif

#if defined(HAVE_FPCLASSIFY) || defined(fpclassify)
/* C99 interface: fpclassify */
# define MNisinf(x)		(fpclassify(x) == FP_INFINITE)
# define MNisnan(x)		(fpclassify(x) == FP_NAN)
# define MNfinite(x)	(!MNisinf(x) && !MNisnan(x))
#else
# define MNisnan(x)		isnan(x)
# define MNfinite(x)	finite(x)
# ifdef HAVE_ISINF
#  define MNisinf(x)	isinf(x)
# else
static inline int
MNisinf(double x)
{
#ifdef HAVE_FPCLASS
	int cl = fpclass(x);

	return ((cl == FP_NINF) || (cl == FP_PINF));
#else
	(void)x;
	return 0;		/* XXX not correct if infinite */
#endif
}
# endif
#endif /* HAVE_FPCLASSIFY */

extern double sqrt(double x);
extern double sin(double x);
extern double cos(double x);
extern double fabs(double x);

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mmath_export extern __declspec(dllimport)
#else
#define mmath_export extern __declspec(dllexport)
#endif
#else
#define mmath_export extern
#endif

#define unopbaseM5_export(X1,X2)\
mmath_export str MATHunary##X1##X2(X2 *res, const X2 *a);

#define unopM5_export(X1)\
  unopbaseM5_export(X1,dbl)\
  unopbaseM5_export(X1,flt)

#define binopbaseM5_export(X1,X2,X3)\
mmath_export str MATHbinary##X1##X2(X2 *res, const X2 *a, const X3 *b);

#define binopM5_export(X1)\
  binopbaseM5_export(X1,dbl,dbl)\
  binopbaseM5_export(X1,flt,flt)

unopM5_export(_ACOS)
unopM5_export(_ASIN)
unopM5_export(_ATAN)
binopM5_export(_ATAN2)
unopM5_export(_COS)
unopM5_export(_SIN)
unopM5_export(_TAN)
unopM5_export(_COT)

unopM5_export(_COSH)
unopM5_export(_SINH)
unopM5_export(_TANH)
unopM5_export(_RADIANS)
unopM5_export(_DEGREES)

unopM5_export(_EXP)
unopM5_export(_LOG)
unopM5_export(_LOG10)

binopM5_export(_POW)
unopM5_export(_SQRT)

unopM5_export(_CEIL)
unopbaseM5_export(_FABS,dbl)
unopM5_export(_FLOOR)
binopbaseM5_export(_ROUND,dbl,int)
binopbaseM5_export(_ROUND,flt,int)

mmath_export str MATHunary_ISNAN(bit *res, const dbl *a);
mmath_export str MATHunary_ISINF(int *res, const dbl *a);
mmath_export str MATHunary_FINITE(bit *res, const dbl *a);
mmath_export str MATHrandint(int *res);
mmath_export str MATHrandintarg(int *res, const int *dummy);
mmath_export str MATHsrandint(void *ret, const int *seed);
mmath_export str MATHsqlrandint(int *res, const int *seed);
mmath_export str MATHpi(dbl *pi);
#endif /* __MMATH_H__ */
