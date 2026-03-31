/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PP_MAT_H_
#define _PP_MAT_H_

typedef struct mat_t {
	Sink s;
	int nr;
	int nr_parts;
	int *part;
	int *subpart;
	BUN slicesize;
	BAT **bat;
	MT_RWLock rwlock;	/* needed for save resizing */
} mat_t;

extern BAT * pack_mat(BAT *b);

#endif /*_PP_MAT_H_*/
