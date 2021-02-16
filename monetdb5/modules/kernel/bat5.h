/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _BAT_H_
#define _BAT_H_

#include "mal.h"
#include "gdk.h"

mal_export str BKCnewBAT(bat *res, const int *tt, const BUN *cap, role_t role);
mal_export str BKCmirror(bat *ret, const bat *bid);
mal_export str BKCsetPersistent(void *r, const bat *bid);
mal_export str BKCsetName(void *r, const bat *bid, const char * const *s);
mal_export str BKCshrinkBAT(bat *ret, const bat *bid, const bat *did);
mal_export str BKCreuseBAT(bat *ret, const bat *bid, const bat *did);
#endif /*_BAT_H_*/
