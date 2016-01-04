/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _EMBEDDEDCLIENT_H_
#define _EMBEDDEDCLIENT_H_

/* avoid using "#ifdef WIN32" so that this file does not need our config.h */
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#ifndef LIBEMBEDDEDSQL5
#define embeddedclient_export extern __declspec(dllimport)
#else
#define embeddedclient_export extern __declspec(dllexport)
#endif
#else
#define embeddedclient_export extern
#endif

#include <stdio.h>
#include <stream.h>
#include <mapi.h>

#ifdef __cplusplus
extern "C" {
#endif

embeddedclient_export Mapi monetdb_sql(char *dbpath);
embeddedclient_export Mapi embedded_sql(opt *set, int len);

#ifdef __cplusplus
}
#endif

#endif /* _EMBEDDEDCLIENT_H_ */
