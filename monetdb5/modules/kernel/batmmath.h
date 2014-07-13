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
batmmath_export str CMDscience_bat_dbl_##X1(int *ret, int *bid);\
batmmath_export str CMDscience_bat_flt_##X1(int *ret, int *bid);

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

batmmath_export str CMDscience_bat_cst_atan2_dbl(int *ret, int *bid, dbl *d);
batmmath_export str CMDscience_bat_cst_atan2_flt(int *ret, int *bid, flt *d);
batmmath_export str CMDscience_bat_cst_pow_dbl(int *ret, int *bid, dbl *d);
batmmath_export str CMDscience_bat_cst_pow_flt(int *ret, int *bid, flt *d);
#endif  /* _BATMATH_H */
