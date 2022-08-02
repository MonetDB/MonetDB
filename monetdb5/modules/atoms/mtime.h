/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _MONETTIME_H_
#define _MONETTIME_H_

#include "mal.h"

mal_export str MTIMEanalyticalrangebounds(BAT *r, BAT *b, BAT *p, BAT *l,
										  const void* restrict bound,
										  int tp1, int tp2, bool preceding,
										  lng first_half);

#endif /* _MONETTIME_H_ */
