/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 */

#ifndef _JSON_H_
#define _JSON_H_

/* #define _DEBUG_JSON_ */

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define jsonutil_export extern __declspec(dllimport)
#else
#define jsonutil_export extern __declspec(dllexport)
#endif
#else
#define jsonutil_export extern
#endif

#include "json.h"

jsonutil_export str
JSONresultSet(json *res,bat *u, bat *rev, bat *js);

#endif
