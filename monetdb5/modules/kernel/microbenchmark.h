/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @+ Experimentation Gimmicks
 * @- Data Generation
 */
#ifndef _MBM_H_
#define _MBM_H_
#include <mal.h>

mal_export str MBMrandom(bat *ret, oid *base, lng *size, int *domain);
mal_export str MBMrandom_seed(bat *ret, oid *base, lng *size, int *domain, const int *seed);
mal_export str MBMuniform(bat *ret, oid *base, lng *size, int *domain);
mal_export str MBMnormal(bat *ret, oid *base, lng *size, int *domain, int *stddev, int *mean);
mal_export str MBMmix(bat *ret, bat *batid);
mal_export str MBMskewed(bat *ret, oid *base, lng *size, int *domain, int *skew);

#endif /* _MBM_H_ */
