/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * @-
 * The key action is to break an url into its constituents.
 * Parsing is done for each individual request, because this way we
 * secure concurrent use from different threads.
 */
#ifndef URL_H
#define URL_H

#include "gdk.h"
#include <ctype.h>

typedef str url;

mal_export str escape_str(str *retval, str s);
mal_export str unescape_str(str *retval, str s);
mal_export str URLnoop(url *u, url *val);
mal_export str URLnew(url *u, str *val);
mal_export str URLgetAnchor(str *retval, url *val);
mal_export str URLgetBasename(str *retval, url *t);
mal_export str URLgetContent(str *retval, url *Str1);
mal_export str URLgetContext(str *retval, url *val);
mal_export str URLgetDomain(str *retval, url *tv);
mal_export str URLgetExtension(str *retval, url *tv);
mal_export str URLgetFile(str *retval, url *tv);
mal_export str URLgetHost(str *retval, url *tv);
mal_export str URLgetPort(str *retval, url *tv);
mal_export str URLgetProtocol(str *retval, url *tv);
mal_export str URLgetQuery(str *retval, url *tv);
mal_export str URLgetUser(str *retval, url *tv);
mal_export str URLgetRobotURL(str *retval, url *tv);
mal_export str URLisaURL(bit *retval, url *tv);
mal_export str URLnew4(url *u, str *protocol, str *server,
		int *port, str *file);
mal_export str URLnew3(url *u, str *protocol, str *server, str *file);
mal_export ssize_t URLfromString(const char *src, size_t *len, str *u);
mal_export ssize_t URLtoString(str *s, size_t *len, const char *src);

#endif /* URL_H */
