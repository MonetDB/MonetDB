/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

sample_export str
SAMPLEuniform(bat *r, bat *b, wrd *s);

sample_export str
SAMPLEuniform_dbl(bat *r, bat *b, dbl *p);

#endif
