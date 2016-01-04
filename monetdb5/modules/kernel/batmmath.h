/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _BATMATH_H
#define _BATMATH_H
#include "gdk.h"
#include "math.h"
#include "mal_exception.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define batmmath_export extern __declspec(dllimport)
#else
#define batmmath_export extern __declspec(dllexport)
#endif
#else
#define batmmath_export extern
#endif

#define radians(x)       ((x) * 3.14159265358979323846 / 180.0)
#define degrees(x)       ((x) * 180.0 / 3.14159265358979323846)
#define radiansf(x)      ((flt) radians(x))
#define degreesf(x)      ((flt) degrees(x))


#define scienceDef(X1)\
batmmath_export str CMDscience_bat_dbl_##X1(bat *ret, const bat *bid);\
batmmath_export str CMDscience_bat_flt_##X1(bat *ret, const bat *bid);

scienceDef(asin)
scienceDef(acos)
scienceDef(atan)
scienceDef(cos)
scienceDef(sin)
scienceDef(tan)
scienceDef(cosh)
scienceDef(sinh)
scienceDef(tanh)
scienceDef(radians)
scienceDef(degrees)
scienceDef(exp)
scienceDef(log)
scienceDef(log10)
scienceDef(sqrt)
scienceDef(ceil)
scienceDef(fabs)
scienceDef(floor)

batmmath_export str CMDscience_bat_cst_atan2_dbl(bat *ret, const bat *bid, const dbl *d);
batmmath_export str CMDscience_bat_cst_atan2_flt(bat *ret, const bat *bid, const flt *d);
batmmath_export str CMDscience_bat_cst_pow_dbl(bat *ret, const bat *bid, const dbl *d);
batmmath_export str CMDscience_bat_cst_pow_flt(bat *ret, const bat *bid, const flt *d);
#endif  /* _BATMATH_H */
