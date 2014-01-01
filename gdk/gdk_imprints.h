/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
#define IMPS_MAX_CNT 16777215
#define IMPS_PAGE 64

/* auxiliary macros */
#define IMPScnt(X)           ((X) & 16777215) /* 24 bits for cnt   */
#define IMPSsetcnt(X,Y)      ((X) = (Y))      /* no overflow check */
#define IMPScnt_inc(X)       ((X)++)          /* no overflow check */
#define IMPSrepeat(X)        (((X) & 16777216)?1:0)
#define IMPSsetrepeat(X)     ((X) |= 16777216)
#define IMPSunsetrepeat(X)   ((X) &= (~16777216))
#define IMPSsetBit(B,X,Y)      ((((uint##B##_t)1)<<(Y))|(X))
#define IMPSunsetBit(B,X,Y)    ((~(((uint##B##_t)1)<<(Y)))&(X))
#define IMPSisSet(B,X,Y)       (((((uint##B##_t)1)<<Y)&X)?1:0)
#define IMPSmod2(X,Y)        ((X)&((Y)-1))

#endif /* GDK_IMPS_H */
