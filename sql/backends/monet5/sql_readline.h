/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef SQL_READLINETOOLS_H_INCLUDED
#define SQL_READLINETOOLS_H_INCLUDED

#ifdef WIN32
#ifndef LIBSQL
#define sql5_export extern __declspec(dllimport)
#else
#define sql5_export extern __declspec(dllexport)
#endif
#else
#define sql5_export extern
#endif

#include "mal_client.h"
sql5_export int SQLreadConsole(Client cntxt);

#endif /* SQL_READLINETOOLS_H_INCLUDED */
