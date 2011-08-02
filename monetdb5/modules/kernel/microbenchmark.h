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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @+ Experimentation Gimmicks
 * @- Data Generation
 */
#ifndef _MBM_H_
#define _MBM_H_
#include <mal.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mb_export extern __declspec(dllimport)
#else
#define mb_export extern __declspec(dllexport)
#endif
#else
#define mb_export extern
#endif

mb_export str MBMrandom(int *ret, oid *base, int *size, int *domain);
mb_export str MBMuniform(int *ret, oid *base, int *size, int *domain);
mb_export str MBMnormal(int *ret, oid *base, int *size, int *domain, int *stddev, int *mean);
mb_export str MBMmix(int *ret, int *batid);
mb_export str MBMskewed(int *ret, oid *base, int *size, int *domain, int *skew);

#endif /* _MBM_H_ */
