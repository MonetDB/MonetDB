/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _GROUP_H_
#define _GROUP_H_
#include "gdk.h"

mal_export str GRPgroup1(bat *ngid, bat *next, bat *nhis, const bat *bid);
mal_export str GRPgroup2(bat *ngid, bat *next, bat *nhis,
						 const bat *bid, const bat *sid);
mal_export str GRPgroup3(bat *ngid, bat *next, const bat *bid);
mal_export str GRPgroup4(bat *ngid, bat *next,
						 const bat *bid, const bat *sid);
mal_export str GRPsubgroup2(bat *ngid, bat *next, bat *nhis,
							const bat *bid, const bat *gid);
mal_export str GRPsubgroup3(bat *ngid, bat *next, bat *nhis,
							const bat *bid, const bat *sid,
							const bat *gid);
mal_export str GRPsubgroup4(bat *ngid, bat *next, bat *nhis,
							const bat *bid, const bat *gid,
							const bat *eid, const bat *hid);
mal_export str GRPsubgroup5(bat *ngid, bat *next, bat *nhis,
							const bat *bid, const bat *sid,
							const bat *gid, const bat *eid, const bat *hid);
mal_export str GRPsubgroup6(bat *ngid, bat *next,
							const bat *bid, const bat *gid);
mal_export str GRPsubgroup7(bat *ngid, bat *next,
							const bat *bid, const bat *sid,
							const bat *gid);
mal_export str GRPsubgroup8(bat *ngid, bat *next,
							const bat *bid, const bat *gid,
							const bat *eid, const bat *hid);
mal_export str GRPsubgroup9(bat *ngid, bat *next,
							const bat *bid, const bat *sid,
							const bat *gid, const bat *eid, const bat *hid);

#endif /* _GROUP_H_ */
