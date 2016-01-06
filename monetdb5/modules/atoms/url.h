/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
url_export str URLnoop(url *u, url *val);
url_export str URLnew(url *u, str *val);
url_export str URLgetAnchor(str *retval, url *val);
url_export str URLgetBasename(str *retval, url *t);
url_export str URLgetContent(str *retval, url *Str1);
url_export str URLgetContext(str *retval, url *val);
url_export str URLgetDomain(str *retval, url *tv);
url_export str URLgetExtension(str *retval, url *tv);
url_export str URLgetFile(str *retval, url *tv);
url_export str URLgetHost(str *retval, url *tv);
url_export str URLgetPort(str *retval, url *tv);
url_export str URLgetProtocol(str *retval, url *tv);
url_export str URLgetQuery(str *retval, url *tv);
url_export str URLgetUser(str *retval, url *tv);
url_export str URLgetRobotURL(str *retval, url *tv);
url_export str URLisaURL(bit *retval, url *tv);
url_export str URLnew4(url *u, str *protocol, str *server,
		int *port, str *file);
url_export str URLnew3(url *u, str *protocol, str *server, str *file);
url_export int URLfromString(str src, int *len, str *u);
url_export int URLtoString(str *s, int *len, str src);

#endif /* URL_H */
