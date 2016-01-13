/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _GROUP_H_
#define _GROUP_H_
#include "gdk.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define group_export extern __declspec(dllimport)
#else
#define group_export extern __declspec(dllexport)
#endif
#else
#define group_export extern
#endif

group_export str GRPsubgroup1(bat *ngid, bat *next, bat *nhis,
							  const bat *bid);
group_export str GRPsubgroup2(bat *ngid, bat *next, bat *nhis,
							  const bat *bid, const bat *gid);
group_export str GRPsubgroup4(bat *ngid, bat *next, bat *nhis,
							  const bat *bid, const bat *gid,
							  const bat *eid, const bat *hid);

#endif /* _GROUP_H_ */
