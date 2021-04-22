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
/* Count the occurences of pairs of bytes. This is a compromise between
 * just handling ASCII and full UTF-8 support.
 */
#define STRIMP_HISTSIZE 256*256
#define STRIMP_HEADER_SIZE 64
#define STRIMP_OFFSET 1 + STRIMP_HEADER_SIZE*sizeof(DataPair)/sizeof(uint64_t) /* version + header */


typedef uint16_t DataPair;
typedef struct {
	// TODO: find a better name for this
	DataPair bytepairs[STRIMP_HEADER_SIZE];
} StrimpHeader;

// gdk_export gdk_return STRMPndigrams(BAT *b, size_t *n); // Remove?
// gdk_export gdk_return STRMPmakehistogramBP(BAT *b, uint64_t *hist, size_t hist_size, size_t *nbins); // make static
// gdk_export gdk_return STRMP_make_header(StrimpHeader *h, uint64_t *hist, size_t hist_size); // make static
// gdk_export gdk_return STRMP_make_header(BAT *b);
gdk_export gdk_return STRMPcreate(BAT *b);
gdk_export BAT *STRMPfilter(BAT *b, char *q);
#endif /* _GDK_STRIMPS_H_ */
