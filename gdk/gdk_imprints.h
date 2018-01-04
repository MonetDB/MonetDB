/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef GDK_IMPS_H
#define GDK_IMPS_H

/*
 * the cache dictionary struct
 */
typedef struct {
	unsigned int cnt:24,   /* cnt of pages <= IMPS_MAX_CNT */
		     repeat:1, /* repeat flag                 */
		     flags:7;  /* reserved flags for future   */
} cchdc_t;

/* hard bounds */
#define IMPS_MAX_CNT	((1 << 24) - 1)		/* 24 one bits */
#define IMPS_PAGE	64

/* auxiliary macros */
#define IMPSsetBit(B, X, Y)	((X) | ((uint##B##_t) 1 << (Y)))
#define IMPSunsetBit(B, X, Y)	((X) & ~((uint##B##_t) 1 << (Y)))
#define IMPSisSet(B, X, Y)	(((X) & ((uint##B##_t) 1 << (Y))) != 0)

#endif /* GDK_IMPS_H */
