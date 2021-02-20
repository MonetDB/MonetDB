/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _GDK_STRIMPS_H_
#define _GDK_STRIMPS_H_

#include <stdint.h>

#define HISTSIZE 64

typedef struct {
	uint64_t counts[HISTSIZE];
	char foo;
} Histogram;

typedef struct {
	Histogram* hist;
} Strimp;

gdk_export gdk_return GDKstrimp_ndigrams(BAT *b, size_t *n);

#endif /* _GDK_STRIMPS_H_ */
