/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _BAT_H_
#define _BAT_H_

#include "mal.h"
#include "gdk.h"

mal_export str BKCnewBAT(bat *res, const int *tt, const BUN *cap, role_t role);
mal_export str BKCmirror(bat *ret, const bat *bid);
#endif /*_BAT_H_*/
