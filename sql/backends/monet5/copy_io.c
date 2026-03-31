/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_pipelines.h"
#include "mel.h"
#include "pipeline.h"

#include "sql.h"
#include "mapi_prompt.h"
#include "copy.h"
#include "rel_copy.h"

str
COPYrequest_upload(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	Stream *upload = getArgReference(stk, pci, 0);
	str filename = *getArgReference_str(stk, pci, 1);
	int on_client = *getArgReference_int(stk, pci, 2);
	backend *be = cntxt->sqlcontext;
	mvc *mvc = be->mvc;
	*upload = mapi_request_upload(filename, on_client>1, mvc->scanner.rs, mvc->scanner.ws);
	if (*upload == NULL)
		throw(IO, "streams.request_upload", "%s", mnstr_peek_error(NULL));
	if (on_client > 1) {
		str msg = wrap_onclient_compression((stream**)upload, "sql.copy_from", on_client, false);
		if (msg != NULL) {
			close_stream(*upload);
			return msg;
		}
	}
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
	// While want to read more
	while (cnt > 0 && state->lines_left > 0) {
		// Replenish buffer if needed
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
			if (nread == 0) {
				// Client says EOF.. should we leaf bs->eof as true or clear it?
				state->bs = NULL;
				state->ws = NULL;
				break;
			}
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
			if (scan->line_sep_len == 1 && find_end_of_line(scan)) {
				// pos advanced to the next newline. Include it.
				scan->pos++;
				state->at_start_of_line = true;
				state->lines_left--;
			} else if (scan->line_sep_len != 1 && find_end_of_lineN(scan)) {
				// pos advanced to the next newline. Include it.
				scan->pos += scan->line_sep_len;
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
COPYfrom_stdin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	Stream *s = (Stream*)getArgReference(stk, pci, 0);
	lng offset = *getArgReference_lng(stk, pci, 1);
	lng lines = *getArgReference_lng(stk, pci, 2);
	bit stoponemptyline = *getArgReference_bit(stk, pci, 3);
	str linesep_arg = *getArgReference_str(stk, pci, 4);
	str quote_arg = *getArgReference_str(stk, pci, 5);
	bit escape = *getArgReference_bit(stk, pci, 6);

	backend *be = cntxt->sqlcontext;
	mvc *mvc = be->mvc;

	struct from_stdin_state *state = GDKmalloc(sizeof(*state));
	if (!state)
		throw(MAL, "copy.from_stdin", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	bool backslash_escapes = escape;
	int quote_char = check_sep(quote_arg, backslash_escapes);
	int line_sep = check_sep(linesep_arg, backslash_escapes);
	*state = (struct from_stdin_state) {
		.bs = mvc->scanner.rs,
		.ws = mvc->scanner.ws,
		.stop_on_empty = stoponemptyline,
		.offset_left = offset,
		.lines_left = lines,
		.at_start_of_line = true,
		.scan_state = {
			.quote_char = quote_char==1 ?quote_arg[0]:0,
			.line_sep = line_sep==1 ?linesep_arg[0]:0,
			.line_sep_str = (unsigned char*)linesep_arg,
			.line_sep_len = (int)strlen(linesep_arg),
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
