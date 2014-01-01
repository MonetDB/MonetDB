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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
*/
/*
 * The constants defined in math.h are defined in const.mx
 */
#ifndef __MMATH_H__
#define __MMATH_H__
#include "mal.h"
#include "mal_exception.h"
#include <math.h>

extern double sqrt(double x);
extern double cbrt(double x);
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
mmath_export str MATHunary##X1##X2(X2 *res , X2 *a );

#define unopM5_export(X1)\
  unopbaseM5_export(X1,dbl)\
  unopbaseM5_export(X1,flt)

#define binopbaseM5_export(X1,X2,X3)\
mmath_export str MATHbinary##X1##X2(X2 *res, X2 *a, X3 *b );

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

mmath_export str MATHunary_ISNAN(bit *res, dbl *a);
mmath_export str MATHunary_ISINF(int *res, dbl *a);
mmath_export str MATHunary_FINITE(bit *res, dbl *a);
mmath_export str MATHrandint(int *res);
mmath_export str MATHsrandint(int *seed);
mmath_export str MATHsqlrandint(int *res, int *seed);
mmath_export str MATHpi(dbl *pi);
#endif /* __MMATH_H__ */
