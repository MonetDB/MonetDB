/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _GROUP_H_
#define _GROUP_H_
#include "gdk.h"

mal_export str GRPgroup1(bat *ngid, bat *next, bat *nhis, const bat *bid);
mal_export str GRPsubgroup5(bat *ngid, bat *next, bat *nhis,
							const bat *bid, const bat *sid,
							const bat *gid, const bat *eid, const bat *hid);

extern str GRPsubgroup2(bat *ngid, bat *next, bat *nhis, const bat *bid,
						const bat *gid);
extern str GRPgroup3(bat *ngid, bat *next, const bat *bid);
#endif /* _GROUP_H_ */
