/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"

#include <openssl/ssl.h>

stream *
openssl_stream(const char *hostname, SSL *ssl)
{
	(void)ssl;
	mnstr_set_open_error(hostname, 0, "not implemented yet");
	return NULL;
}
