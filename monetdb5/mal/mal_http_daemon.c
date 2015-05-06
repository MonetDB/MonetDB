/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* (author) A. de Rijke
 * For documentation see website
 */

#include "monetdb_config.h"

#include "mal.h"
#include "mal_exception.h"
#include "mal_private.h"
#include "mal_http_daemon.h"

/* dummy noop functions to implement the exported API, if these had been
 * defined to return a str, we could have informed the caller no
 * implementation was available */

void
register_http_handler(http_request_handler handler)
{
	(void)handler;
}

void
startHttpdaemon(void)
{
}

void
stopHttpdaemon(void)
{
}
