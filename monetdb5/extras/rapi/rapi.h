/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * H. Muehleisen, M. Kersten
 * The R interface
 */
#ifndef _RAPI_LIB_
#define _RAPI_LIB_

#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#ifdef WIN32
#ifndef LIBRAPI
#define rapi_export extern __declspec(dllimport)
#else
#define rapi_export extern __declspec(dllexport)
#endif
#else
#define rapi_export extern
#endif

#define RAPI_MAX_TUPLES 2147483647L

rapi_export void* RAPIloopback(void *query);

#if 0
char* rtypename(int rtypeid);
#endif

#endif /* _RAPI_LIB_ */
