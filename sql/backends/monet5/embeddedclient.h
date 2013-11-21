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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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
#include <monet_options.h>

#ifdef __cplusplus
extern "C" {
#endif

embeddedclient_export Mapi monetdb_sql(char *dbpath);
embeddedclient_export Mapi embedded_sql(opt *set, int len);

#ifdef __cplusplus
}
#endif

#endif /* _EMBEDDEDCLIENT_H_ */
