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
#include "mapi_prompt.h"


static void
discard(stream *s)
{
	static char bitbucket[8192];
	while (1) {
		ssize_t nread = mnstr_read(s, bitbucket, 1, sizeof(bitbucket));
		if (nread <= 0)
			return;
		assert(1);
	}
}

struct mapi_recv_upload {
	stream *from_client; // set to NULL after sending MAPI_PROMPT3
	stream *to_client; // set to NULL when client sends empty
};

static ssize_t
recv_upload_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	struct mapi_recv_upload *state = s->stream_data.p;

	if (state->from_client == NULL) {
		assert(s->eof);
		return 0;
	}

	ssize_t nread = mnstr_read(state->from_client, buf, elmsize, cnt);
	if (nread != 0 || state->from_client->eof)
		return nread;

	// before returning the 0 we send the prompt and make another attempt.
	if (
			mnstr_write(state->to_client, PROMPT2, strlen(PROMPT2), 1) != 1
		||	mnstr_flush(state->to_client, MNSTR_FLUSH_ALL) < 0
	) {
		mnstr_set_error(s, mnstr_errnr(state->to_client), "%s", mnstr_peek_error(state->to_client));
		return -1;
	}

	// if it succeeds, return that to the client.
	// if it's still a block boundary, return that to the client.
	// if there's an error, return that to the client.
	nread = mnstr_read(state->from_client, buf, elmsize, cnt);
	if (nread > 0)
		return nread;
	if (nread == 0) {
		s->eof = true;
		state->from_client = NULL;
		return nread;
	} else {
		mnstr_set_error(s, mnstr_errnr(state->from_client), "%s", mnstr_peek_error(state->from_client));
		return -1;
	}
}

static void
recv_upload_close(stream *s)
{
	struct mapi_recv_upload *state = s->stream_data.p;

	stream *from = state->from_client;
	if (from)
		discard(from);

	stream *to = state->to_client;
	mnstr_write(to, PROMPT3, strlen(PROMPT3), 1);
	mnstr_flush(to, MNSTR_FLUSH_ALL);
}

static void
recv_upload_destroy(stream *s)
{
	struct mapi_recv_upload *state = s->stream_data.p;
	free(state);
	destroy_stream(s);
}


stream*
mapi_request_upload(const char *filename, bool binary, bstream *bs, stream *ws)
{
	const char *msg = NULL;
	stream *s = NULL;
	struct mapi_recv_upload *state = NULL;
	ssize_t nwritten;

	while (!bs->eof)
		bstream_next(bs);
	stream *rs = bs->s;
	assert(isa_block_stream(ws));
	assert(isa_block_stream(rs));

	if (binary)
		nwritten = mnstr_printf(ws, "%srb %s\n", PROMPT3, filename);
	else
		nwritten = mnstr_printf(ws, "%sr 0 %s\n", PROMPT3, filename);
	if (nwritten <= 0) {
		msg = mnstr_peek_error(ws);
		goto end;
	}
	if (mnstr_flush(ws, MNSTR_FLUSH_ALL) < 0) {
		msg = mnstr_peek_error(ws);
		goto end;
	}

	char buf[256];
	if (mnstr_readline(rs, buf, sizeof(buf)) != 1 || buf[0] != '\n') {
		msg = buf;
		discard(rs);
		goto end;
	}

	// Client accepted the request
	state = malloc(sizeof(*state));
	if (!state) {
		msg = "malloc failed";
		goto end;
	}
	s = create_stream("ONCLIENT");
	if (!s) {
		free(state);			/* no chance to free through destroy function */
		msg = mnstr_peek_error(NULL);
		goto end;
	}
	state->from_client = rs;
	state->to_client = ws;
	s->stream_data.p = state;
	s->binary= binary;
	s->read = recv_upload_read;
	s->close = recv_upload_close;
	s->destroy = recv_upload_destroy;
end:
	if (msg) {
		mnstr_destroy(s);
		mnstr_set_open_error(filename, 0, "ON CLIENT: %s", msg);
		return NULL;
	} else {
		return s;
	}
}
