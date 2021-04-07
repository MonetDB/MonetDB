/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "gdk.h"
#include "xoshiro256starstar.h"

/* global pseudo random generator state */
extern random_state_engine mmath_rse
__attribute__((__visibility__("hidden")));
/* serialize access to state */
extern MT_Lock mmath_rse_lock
__attribute__((__visibility__("hidden")));

/* cotangent value */
extern double cot(double);
extern float cotf(float);

/* degress to radians conversion */
extern double radians(double);
extern float radiansf(float);

/* radians to degress conversion */
extern double degrees(double);
extern float degreesf(float);

/* return the logarigthm of the first argument with the second
 * argument as base */
extern double logbs(double, double);
extern float logbsf(float, float);

/* backup definitions of some useful constants */
#ifndef FE_INVALID
#define FE_INVALID			0
#endif
#ifndef FE_DIVBYZERO
#define FE_DIVBYZERO		0
#endif
#ifndef FE_OVERFLOW
#define FE_OVERFLOW			0
#endif
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif
#ifndef M_PIF
#define M_PIF 3.14159265358979323846f
#endif
