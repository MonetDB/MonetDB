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
 * @a Lefteris Sidirourgos
 * @d 30/08/2011
 * @+ The sampling facilities
 */

#ifndef _SAMPLE_H_
#define _SAMPLE_H_

/* #define _DEBUG_SAMPLE_ */

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define sample_export extern __declspec(dllimport)
#else
#define sample_export extern __declspec(dllexport)
#endif
#else
#define sample_export extern
#endif

#define DRAND ((double)rand()/(double)RAND_MAX)

sample_export str
SAMPLEuniform(bat *r, bat *b, ptr s);

#endif
