/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

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
