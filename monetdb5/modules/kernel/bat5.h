/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _BAT_H_
#define _BAT_H_

#include "mal.h"
#include "gdk.h"
#include "mal_client.h"

mal_export str BKCnewBAT(bat *res, const int *tt, const BUN *cap, role_t role);
mal_export str BKCmirror(Client ctx, bat *ret, const bat *bid);
#endif /*_BAT_H_*/
