/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"

struct mapi_recv_upload {
	stream *from_client; // set to NULL after sending MAPI_PROMPT3
	stream *to_client; // set to NULL when client sends empty
};




stream*
mapi_request_upload(const char *filename, bool binary, stream *from, stream *to)
{
	(void)from; (void)to; (void)binary;
	mnstr_set_open_error(filename, 0, "ON CLIENT not supported yet");
	return NULL;
}

