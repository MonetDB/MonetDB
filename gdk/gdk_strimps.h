/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _GDK_STRIMPS_H_
#define _GDK_STRIMPS_H_

#include <stdint.h>


#define STRIMP_VERSION (uint64_t)2
#define STRIMP_HISTSIZE 256*256
#define STRIMP_HEADER_SIZE 64
#define STRIMP_PAIRS (STRIMP_HEADER_SIZE - 1)
#define STRIMP_CREATION_THRESHOLD 5000

typedef struct {
	uint8_t *pbytes;
	uint8_t psize;
} CharPair;

typedef struct {
	size_t pos;
	size_t lim;
	const char *s;
} PairIterator;

typedef struct {
	CharPair *p;
	uint64_t cnt;
} PairHistogramElem;


#endif /* _GDK_STRIMPS_H_ */
