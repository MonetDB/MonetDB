/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _GROUP_H_
#define _GROUP_H_
#include "gdk.h"
#include "mal_client.h"

mal_export str GRPgroup1(Client ctx, bat *ngid, bat *next, bat *nhis, const bat *bid);
mal_export str GRPsubgroup5(Client ctx, bat *ngid, bat *next, bat *nhis,
							const bat *bid, const bat *sid,
							const bat *gid, const bat *eid, const bat *hid);

extern str GRPsubgroup2(Client ctx, bat *ngid, bat *next, bat *nhis, const bat *bid,
						const bat *gid)
	__attribute__((__visibility__("hidden")));
extern str GRPgroup3(Client ctx, bat *ngid, bat *next, const bat *bid)
	__attribute__((__visibility__("hidden")));
#endif /* _GROUP_H_ */
