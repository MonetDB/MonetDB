/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @+ Experimentation Gimmicks
 * @- Data Generation
 */
#ifndef _MBM_H_
#define _MBM_H_
#include <mal.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mb_export extern __declspec(dllimport)
#else
#define mb_export extern __declspec(dllexport)
#endif
#else
#define mb_export extern
#endif

mb_export str MBMrandom(bat *ret, oid *base, lng *size, int *domain);
mb_export str MBMrandom_seed(bat *ret, oid *base, lng *size, int *domain, const int *seed);
mb_export str MBMuniform(bat *ret, oid *base, lng *size, int *domain);
mb_export str MBMnormal(bat *ret, oid *base, lng *size, int *domain, int *stddev, int *mean);
mb_export str MBMmix(bat *ret, bat *batid);
mb_export str MBMskewed(bat *ret, oid *base, lng *size, int *domain, int *skew);

#endif /* _MBM_H_ */
