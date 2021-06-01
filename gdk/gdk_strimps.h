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


#define STRIMP_VERSION (uint64_t)1
#define STRIMP_HISTSIZE 256*256
#define STRIMP_HEADER_SIZE 64

typedef struct {
	uint8_t *pbytes;
	uint8_t psize;
} CharPair;

typedef struct {
	size_t pos;
	size_t lim;
	str s;
} PairIterator;

typedef struct {
	CharPair *p;
	uint64_t cnt;
} PairHistogramElem;


/* typedef struct { */
/* 	uint8_t *sizes; */
/* 	uint8_t *pairs; */
/* } StrimpHeader; */

// gdk_export gdk_return STRMPndigrams(BAT *b, size_t *n); // Remove?
// gdk_export gdk_return STRMPmakehistogramBP(BAT *b, uint64_t *hist, size_t hist_size, size_t *nbins); // make static
// gdk_export gdk_return STRMP_make_header(StrimpHeader *h, uint64_t *hist, size_t hist_size); // make static
// gdk_export gdk_return STRMP_make_header(BAT *b);
gdk_export gdk_return STRMPcreate(BAT *b);
gdk_export BAT *STRMPfilter(BAT *b, char *q);
#endif /* _GDK_STRIMPS_H_ */
