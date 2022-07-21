/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mel.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#include "copy.h"
#include "rel_copy.h"



struct copy_destructor {
	Sink sink;
	stream *stream_to_destroy;
};

#define COPY_SINK 42

static void
defer_close(void *sink)
{
	struct copy_destructor *destr = (struct copy_destructor*)sink;
	assert(destr->sink.type == COPY_SINK);
	if (destr->stream_to_destroy) {
		mnstr_close(destr->stream_to_destroy);
	GDKfree(destr);
	}
}

str
COPYdefer_close_stream(bat *container, Stream *s)
{
	str msg = MAL_SUCCEED;
	stream *st = *s;
	BAT *b = NULL;
	struct copy_destructor *destr = GDKmalloc(sizeof *destr);

	destr->sink.type = COPY_SINK;
	destr->sink.destroy = defer_close;
	destr->stream_to_destroy = st;

	b = COLnew(0, TYPE_bit, 0, TRANSIENT);
	if (!destr || !b)
		bailout("copy.defer_close",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	b->T.sink = &destr->sink;

	*container = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
end:
	GDKfree(destr);
	if (b)
		BBPunfix(b->batCacheid);
	return msg;
}


str
COPYrequest_upload(Stream *upload, str *filename, bit *binary)
{
	Client cntxt = getClientContext();
	backend *be = cntxt->sqlcontext;
	mvc *mvc = be->mvc;
	*upload = mapi_request_upload(*filename, *binary, mvc->scanner.rs, mvc->scanner.ws);
	if (*upload == NULL)
		throw(IO, "streams.request_upload", "%s", mnstr_peek_error(NULL));
	return MAL_SUCCEED;
}

str
COPYfrom_stdin(Stream *s, bit *stoponemptyline, lng *lines)
{
	(void)s;
	(void)stoponemptyline;
	(void)lines;
	throw(MAL, "copy.from_stdin", "FROM STDIN not implemented yet");
}
