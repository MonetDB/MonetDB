/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef __MMATH_H__
#define __MMATH_H__
#include "mal.h"
#include "mal_exception.h"

#define unopbaseM5_export(X1,X2)\
mal_export str MATHunary##X1##X2(X2 *res, const X2 *a);

#define unopM5_export(X1)\
  unopbaseM5_export(X1,dbl)\
  unopbaseM5_export(X1,flt)

#define binopbaseM5_export(X1,X2,X3)\
mal_export str MATHbinary##X1##X2(X2 *res, const X2 *a, const X3 *b);

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
unopM5_export(_CBRT)

unopM5_export(_CEIL)
unopbaseM5_export(_FABS,dbl)
unopM5_export(_FLOOR)
binopbaseM5_export(_ROUND,dbl,int)
binopbaseM5_export(_ROUND,flt,int)

mal_export str MATHunary_ISNAN(bit *res, const dbl *a);
mal_export str MATHunary_ISINF(int *res, const dbl *a);
mal_export str MATHunary_FINITE(bit *res, const dbl *a);
mal_export str MATHrandint(int *res);
mal_export str MATHrandintarg(int *res, const int *dummy);
mal_export str MATHsrandint(void *ret, const int *seed);
mal_export str MATHsqlrandint(int *res, const int *seed);
mal_export str MATHpi(dbl *pi);
#endif /* __MMATH_H__ */
