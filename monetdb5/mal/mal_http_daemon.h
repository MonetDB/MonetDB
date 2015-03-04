/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _MAL_HTTP_DAEMON_H_
#define _MAL_HTTP_DAEMON_H_
#include "mal.h"

typedef int (*http_request_handler)
    (const char *url, const char *method, char **page, const char *);

mal_export void register_http_handler(http_request_handler handler);

#endif
