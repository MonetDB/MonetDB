/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _PP_MAT_H_
#define _PP_MAT_H_

typedef struct mat_t {
	Sink s;
	int nr;
	BAT **bat;
} mat_t;

extern BAT * pack_mat(BAT *b);

#endif /*_PP_MAT_H_*/
