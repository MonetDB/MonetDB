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

#include "mapi_prompt.h"
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


static lng
skip_lines(lng nlines, struct scan_state *scan)
{
	lng orig_nlines = nlines;

	int quote_char = scan->quote_char;
	int line_sep = scan->line_sep;
	bool escape_enabled = scan->escape_enabled;

	scan->quote_char = 0;
	scan->line_sep = '\n';
	scan->escape_enabled = false;

	while (scan->pos < scan-> end && nlines > 0) {
		if (find_end_of_line(scan)) {
			assert(scan->pos[0] == '\n');
			scan->pos++;
			nlines --;
		}
	}

	scan->quote_char = quote_char;
	scan->line_sep = line_sep;
	scan->escape_enabled = escape_enabled;

	return orig_nlines - nlines;
}

struct from_stdin_state {
	bstream *bs;
	stream *ws;
	bool stop_on_empty;
	lng offset_left;
	lng lines_left;
	bool at_start_of_line;
	struct scan_state scan_state;
};

static ssize_t
from_stdin_read(void *restrict private, void *restrict buf, size_t elmsize, size_t cnt)
{
	assert(elmsize == 1);
	if (elmsize != 1) {
		return -1;
	}

	struct from_stdin_state *state = private;
	bstream *bs = state->bs;
	if (bs == NULL || cnt == 0)
		return 0;

	char *orig_buf = buf;
	size_t orig_cnt = cnt;
	while (cnt > 0 && state->lines_left > 0 && (bs->pos < bs->len || !bs->eof)) {
		if (bs->pos == bs->len) {
			ssize_t nread = bstream_next(bs);
			if (nread > 0)
				continue;
			if (nread == 0) {
				if (
						mnstr_write(state->ws, PROMPT2, strlen(PROMPT2), 1) != 1
					||	mnstr_flush(state->ws, MNSTR_FLUSH_ALL) < 0
				) {
					return -1;
				}
				bs->eof = false;
				nread = bstream_next(bs);
			}
			if (nread < 0) {
				state->bs = NULL;
				state->ws = NULL;
				return -1;
			}
			// Maybe we reached eof, check again
			continue;
		}
		assert(bs->pos < bs->len);

		// There is some data to be copied to buf.
		// However, we have to be careful not to copy beyond the end of the data.
		struct scan_state *scan = &state->scan_state;
		scan->start = (unsigned char*) &bs->buf[bs->pos];
		size_t n = bs->len - bs->pos;
		if (n > cnt)
			n = cnt;
		scan->end = scan->start + n;
		scan->pos = scan->start;
		if (state->offset_left > 0) {
			lng skipped = skip_lines(state->offset_left, scan);
			state->offset_left -= skipped;
			bs->pos += scan->pos - scan->start;
			continue;
		}
		bool omit_last_char = false;
		while (scan->pos < scan->end && state->lines_left > 0) {
			if (state->stop_on_empty && state->at_start_of_line && *scan->pos == '\n') {
				// Found it. Stop. Consume the empty line but do not copy it.
				state->lines_left = 0;
				scan->pos++;
				omit_last_char = true;
				break;
			}
			if (find_end_of_line(scan)) {
				// pos advanced to the next newline. Include it.
				scan->pos++;
				state->at_start_of_line = true;
				state->lines_left--;
			} else {
				// pos advanced beyond end of buffer.
				// pos started before the end so we must have consumed at least 1
				// character:
				state->at_start_of_line = false;
			}
		}

		// Copy to buf and adjust bookkeeping
		size_t scanned = scan->pos - scan->start;
		size_t to_copy = scanned - omit_last_char;
		assert((char*)buf >= orig_buf); (void)orig_buf;
		assert((char*)buf + to_copy <= orig_buf + orig_cnt);
		memcpy(buf, scan->start, to_copy);
		buf = (char*)buf + to_copy;
		bs->pos += scanned;
		assert(to_copy <= cnt);
		cnt -= to_copy;
	}

	size_t nread = orig_cnt - cnt;
	if (nread == 0) {
		state->bs = NULL;
		state->ws = NULL;
	}

	return (ssize_t)nread;
}

static void
from_stdin_close(void *private)
{
	struct from_stdin_state *state = private;

	state->bs = NULL;
	state->ws = NULL;
}

static void
from_stdin_destroy(void *private)
{
	GDKfree(private);
}

str
COPYfrom_stdin(Stream *s, lng *offset, lng *lines, bit *stoponemptyline, str *linesep_arg, str *quote_arg, bit *escape)
{
	(void)s;
	(void)stoponemptyline;
	(void)lines;
	Client cntxt = getClientContext();
	backend *be = cntxt->sqlcontext;
	mvc *mvc = be->mvc;

	struct from_stdin_state *state = GDKmalloc(sizeof(*state));
	if (!state)
		throw(MAL, "copy.from_stdin", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	bool backslash_escapes = *escape;
	*state = (struct from_stdin_state) {
		.bs = mvc->scanner.rs,
		.ws = mvc->scanner.ws,
		.stop_on_empty = *stoponemptyline,
		.offset_left = *offset,
		.lines_left = *lines,
		.at_start_of_line = true,
		.scan_state = {
			.quote_char = get_sep_char(*quote_arg, backslash_escapes),
			.line_sep = get_sep_char(*linesep_arg, backslash_escapes),
			.escape_enabled = backslash_escapes,
			.quoted = false,
			.escape_pending = false,
		}
	};

	*s = callback_stream(state, from_stdin_read, NULL, from_stdin_close, from_stdin_destroy, "FROM STDIN");
	if (!*s) {
		GDKfree(state);
		throw(MAL, "copy.from_stdin", "%s", mnstr_peek_error(NULL));
	}

	return MAL_SUCCEED;
}
