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

/*
 * @-
 * The key action is to break an url into its constituents.
 * Parsing is done for each individual request, because this way we
 * secure concurrent use from different threads.
 */
#ifndef URL_H
#define URL_H

#include <gdk.h>
#include <ctype.h>

typedef str url;

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define url_export extern __declspec(dllimport)
#else
#define url_export extern __declspec(dllexport)
#endif
#else
#define url_export extern
#endif

url_export str escape_str(str *retval, str s);
url_export str unescape_str(str *retval, str s);
url_export str URLnoop(str *url, str *val);
url_export str URLnew(str *url, str *val);
url_export str URLgetAnchor(str *retval, str *val);
url_export str URLgetBasename(str *retval, str *t);
url_export str URLgetContent(str *retval, str *Str1);
url_export str URLgetContext(str *retval, str *val);
url_export str URLgetDomain(str *retval, str *tv);
url_export str URLgetExtension(str *retval, str *tv);
url_export str URLgetFile(str *retval, str *tv);
url_export str URLgetHost(str *retval, str *tv);
url_export str URLgetPort(str *retval, str *tv);
url_export str URLgetProtocol(str *retval, str *tv);
url_export str URLgetQuery(str *retval, str *tv);
url_export str URLgetUser(str *retval, str *tv);
url_export str URLgetRobotURL(str *retval, str *tv);
url_export str URLisaURL(bit *retval, str *tv);
url_export str URLnew4(str *url, str *protocol, str *server,
		int *port, str *file);
url_export str URLnew3(str *url, str *protocol, str *server, str *file);
url_export int URLfromString(str src, int *len, str *url);
url_export int URLtoString(str *s, int *len, str src);

#endif /* URL_H */
