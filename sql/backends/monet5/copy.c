/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "streams.h"
#include "gdk.h"
#include "mel.h"
#include "mal_exception.h"
#include "mal_pipelines.h"
#include "pipeline.h"
#include "mal_interpreter.h"
#include "tablet.h"

#include "copy.h"
#include "rel_copy.h"

#define COPY_SINK 42

static void
bufferstream_destroy( bufferstream *bs )
{
	if (!bs)
		return ;
	for (int i = 0; i<bs->nr_bufs; i++)
		GDKfree(bs->buf[i]);
	GDKfree(bs->sz);
	GDKfree(bs->len);
	GDKfree(bs->pos);
	GDKfree(bs->jmp);
	GDKfree(bs->buf);
	GDKfree(bs->seq);
	GDKfree(bs);
}

static bufferstream *
bufferstream_create( stream *s, size_t sz, int readers )
{
	if (s) {
		bufferstream *bs = ZNEW(bufferstream);
		if (!bs)
			return NULL;

		bs->sz = ZNEW_ARRAY(size_t, readers);
		bs->len = ZNEW_ARRAY(size_t, readers);
		bs->pos = ZNEW_ARRAY(size_t, readers);
		bs->jmp = ZNEW_ARRAY(size_t, readers);
		bs->buf = ZNEW_ARRAY(unsigned char*, readers);
		bs->seq = ZNEW_ARRAY(BUN, readers);

		if (!bs->sz || !bs->len || !bs->pos || !bs->jmp || !bs->buf || !bs->seq) {
			bufferstream_destroy(bs);
			return NULL;
		}

		bs->sz[0] = sz-2;
		bs->buf[0] = 0;
		bs->nr_bufs = readers;
		bs->cur_buf = 0;
		bs->s = s;
		for(int i = 0; i<readers; i++)
			bs->seq[i] = i;
		return bs;
	}
	return NULL;
}

static ssize_t
bufferstream_read( bufferstream *bs, int cur)
{
	int ocur = bs->cur_buf;
	assert(bs->eof || !bs->buf[0] || cur == (ocur+1)%bs->nr_bufs);
	if (bs->eof || mnstr_eof(bs->s)) {
	    if (bs->pos[ocur] == bs->len[ocur])
			bs->eof = 1;
		return 0;
	}
	if (!bs->buf[cur]) {
		bs->sz[cur] = bs->sz[0];
		bs->buf[cur] = (unsigned char*)GDKmalloc(bs->sz[0] + 2 + 2);
		bs->buf[cur][0] = 0;
		bs->len[cur] = 0;
		/* add nil add end */
		bs->buf[cur][bs->sz[0]+2] = '\200';
		bs->buf[cur][bs->sz[0]+3] = 0;
	}
	bs->cur_buf = cur;

	if (bs->pos[cur] && bs->pos[cur] == bs->len[cur])
		bs->pos[cur] = bs->len[cur] = bs->jmp[cur] = 0;
	if ((bs->pos[ocur] < bs->len[ocur]) || bs->jmp[ocur]) {
		BUN p = bs->jmp[ocur] ? bs->jmp[ocur] : bs->pos[ocur];
		memcpy(bs->buf[cur], bs->buf[ocur]+p, bs->len[ocur] - p + 1);
		bs->len[cur] = bs->len[ocur] - p;
		if (ocur != cur)
			bs->len[ocur] = bs->pos[ocur];
	}
	bs->pos[cur] = bs->jmp[cur] = 0;

	/* out of space ? */
	if (!bs->pos[cur] && bs->len[cur] == bs->sz[cur]) {
		bs->sz[cur] *= 2;
		//bs->sz[cur] += 2;
		bs->buf[cur] = (unsigned char*)GDKrealloc(bs->buf[cur], bs->sz[cur] + 2 + 2);
		/* add nil add end */
		bs->buf[cur][bs->sz[0]+2] = '\200';
		bs->buf[cur][bs->sz[0]+3] = 0;
	}

	/* read new */
	ssize_t rd = 0;
	ssize_t sz = 0;
	if (!mnstr_eof(bs->s)) {
		sz = bs->sz[cur] - bs->len[cur];
		rd = mnstr_read( bs->s, bs->buf[cur]+bs->len[cur], 1, sz);
	}
	if (rd < 0)
		return -1;
	if (rd < sz) {
		if (mnstr_eof(bs->s))
			bs->eof = 1;
	}
	bs->len[cur] += rd;
	/* extra EOS */
	bs->buf[cur][bs->len[cur]] = 0;
	assert(bs->len[cur] - bs->pos[cur] <= bs->sz[cur]);
	return rd;
}

static inline bool
bufferstream_jump(bufferstream *bs, unsigned char tsep, int cb)
{
	unsigned char *b = bs->buf[cb]+bs->pos[cb];
	unsigned char *e = bs->buf[cb]+bs->len[cb];

	if (bs->eof || e == b)
		return true;
	while(*e!=tsep && e > b)
		e--;
	if (e == b)
		return false;
	e++;
	bs->jmp[cb] = e - bs->buf[cb];
	return e > b;
}

static inline bool
bufferstream_jumpN(bufferstream *bs, unsigned char *sep, int len, int cb)
{
	unsigned char *b = bs->buf[cb]+bs->pos[cb];
	unsigned char *e = bs->buf[cb]+bs->len[cb];

	if (bs->eof || e == b)
		return true;
	while((*e!=sep[0] || strncmp((char*)e, (char*)sep, len) != 0) && e > b)
		e--;
	if (e == b)
		return false;
	e+=len;
	bs->jmp[cb] = e - bs->buf[cb];
	return e > b;
}

static void
reader_destroy(reader *r)
{
	assert(r->sink.type == COPY_SINK);
	if (r->s) {
		mnstr_close(r->s);
		mnstr_destroy(r->s);
	}
	if (r->bs)
		bufferstream_destroy(r->bs);
	if (r->line_count)
		GDKfree(r->line_count);
	GDKfree(r);
}

static int
reader_done(reader *r, int wid, int nr_workers, bool redo)
{
	(void)wid;
	(void)nr_workers;
	(void)redo;
	return r->done;
}

static reader *
reader_new(stream *s, BUN offset, BUN maxcount, BUN sz, str col_sep_str, str line_sep_str, str quote_str, str null_repr, bool escape_enabled, bool best_effort)
{
	reader *r = (reader*)GDKzalloc(sizeof(reader));
	r->sink.destroy = (sink_destroy)&reader_destroy;
	r->sink.done = (sink_done)&reader_done;
	r->sink.type = COPY_SINK;
	r->s = s;
	r->offset = offset;
	r->maxcount = maxcount;
	r->linecount = 0;
	r->sz = sz<1024?sz+2:sz;
	r->seqnr = ATOMIC_VAR_INIT(0);
	r->offset_seqnr = ATOMIC_VAR_INIT(0);
	r->jump_seqnr = ATOMIC_VAR_INIT(0);
	r->done = false;
	r->error = false;
	r->col_sep_str = (unsigned char*)col_sep_str;
	r->col_sep_len = (int)strlen(col_sep_str);
	r->line_sep_str = (unsigned char*)line_sep_str;
	r->line_sep_len = (int)strlen(line_sep_str);
	r->quote_str = (unsigned char*)quote_str;
	r->null_repr = (unsigned char*)null_repr;
	r->null_repr_len = (int)strlen(null_repr);
	r->escape_enabled = escape_enabled;
	r->best_effort = best_effort;
	return r;
}

static lng
COPYskiplines(reader *r, int wid)
{
	lng toskip = r->offset, nr = 0;
	unsigned char *start, *pos, *end;

	if (!toskip)
		return 0;

	start = r->bs->buf[wid];
	pos = start + r->bs->pos[wid];
	end = start + r->bs->len[wid];
	while (toskip && pos < end) {
		/* TODO allow more line seperators */
		unsigned char *p = memchr(pos, '\n', end - pos);
		if (!p) {
			// discard everything but do not decrement toskip
			pos = end;
			break;
		}
		pos = p + 1;
		toskip--;
		nr++;
	}

	if (pos > start)
		r->bs->pos[wid] = pos - start;
	return nr;
}

int
check_sep(str sep, bool backslash_escapes)
{
	if (strNil(sep) || strlen(sep) == 0)
		return 0;

	for(; *sep; sep++) {
		int c = *sep;
		switch (c) {
			case '\\':
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
			case 'a':
			case 'b':
			case 'c':
			case 'd':
			case 'e':
			case 'f':
			case 'x':
			case 'u':
			case 'U':
			case 'n':
			case 'r':
			case 't':
				if (backslash_escapes)
					return -1;
				break;
			default:
				break;
		}
	}
	return 1;
}

static const unsigned char *
find_end_of_lines1nn(struct scan_state *st, BUN *newlines_count, BUN maxcount )
	/* single char line end and no escape and no quote */
{
	int line_sep = st->line_sep;
	unsigned char *end = st->end;
	// these are updated
	unsigned char *pos = st->pos;

	BUN newline_count = 0;
	const unsigned char *latest_pos = NULL;
	if (maxcount) {
		for (; pos < end; pos++) {
			if (*pos == line_sep) {
				*pos++ = 0;
				newline_count++;
				latest_pos = pos;
				if (newline_count == maxcount)
					break;
			}
		}
	}
	st->pos = pos;
	*newlines_count = newline_count;
	return latest_pos;
}

static const unsigned char *
find_end_of_lines1(struct scan_state *st, BUN *newlines_count, BUN maxcount )
{
	bool found = true;
	int quote_char = st->quote_char;
	int line_sep = st->line_sep;
	bool escape_enabled = st->escape_enabled;
	unsigned char *end = st->end;
	// these are updated
	unsigned char *pos = st->pos;
	bool quoted = st->quoted;
	bool escape_pending = st->escape_pending;

	BUN newline_count = 0;
	const unsigned char *latest_pos = NULL;
	while (found && pos < end && newline_count < maxcount) {
		found = false;
		for (; pos < end; pos++) {
			if (escape_pending) {
				escape_pending = false;
				continue;
			}
			if (escape_enabled && *pos == '\\') {
				escape_pending = true;
				continue;
			}
			bool is_quote = (quote_char != 0 && *pos == quote_char);
			quoted ^= is_quote;
			if (!quoted && *pos == line_sep) {
				found = true;
				break;
			}
		}
		if (found) {
			*pos = 0;
			pos++;
			newline_count++;
			latest_pos = pos;
		}
	}
	st->pos = pos;
	st->quoted = quoted;
	st->escape_pending = escape_pending;
	*newlines_count = newline_count;
	return latest_pos;
}

static const unsigned char *
find_end_of_linesN(struct scan_state *st, BUN *newlines_count, BUN maxcount )
{
	bool found = true;
	int quote_char = st->quote_char;
	unsigned char ls0 = st->line_sep_str[0];
	char *line_sep = (char*)st->line_sep_str;
	int line_sep_len = st->line_sep_len;
	bool escape_enabled = st->escape_enabled;
	unsigned char *end = st->end;
	// these are updated
	unsigned char *pos = st->pos;
	bool quoted = st->quoted;
	bool escape_pending = st->escape_pending;

	BUN newline_count = 0;
	const unsigned char *latest_pos = NULL;
	while (found && pos < end && newline_count < maxcount) {
		found = false;
		for (; pos < end; pos++) {
			if (escape_pending) {
				escape_pending = false;
				continue;
			}
			if (escape_enabled && *pos == '\\') {
				escape_pending = true;
				continue;
			}
			bool is_quote = (quote_char != 0 && *pos == quote_char);
			quoted ^= is_quote;
			if (!quoted && *pos == ls0 && strncmp((char*)pos, line_sep, line_sep_len) == 0) {
				found = true;
				break;
			}
		}
		if (found) {
			*pos = 0;
			pos += line_sep_len;
			newline_count++;
			latest_pos = pos;
		}
	}
	st->pos = pos;
	st->quoted = quoted;
	st->escape_pending = escape_pending;
	*newlines_count = newline_count;
	return latest_pos;
}

static str
COPYfixlines(lng *ret_linecount, BUN *e, reader *r, int wid, struct error_handling *errors)
{
	str msg = MAL_SUCCEED;
	struct scan_state state = {
		.quote_char = r->quote_char,
		.line_sep = r->line_sep,
		.line_sep_str = r->line_sep_str,
		.line_sep_len = r->line_sep_len,
		.escape_enabled = r->escape_enabled,
		.quoted = false,
		.escape_pending = false,
	};

	unsigned char *data;
	BUN size, start;
	BUN newline_count = 0;

	data = r->bs->buf[wid];
	start = r->bs->pos[wid];
	size = r->bs->jmp[wid] ? r->bs->jmp[wid] : r->bs->len[wid];

	state.start = data;
	state.pos = data + start;
	state.end = data + size;

	if (state.pos == state.end) {
		*ret_linecount = 0;
		*e = state.end - data;
		return MAL_SUCCEED;
	}

	// Scan for unquoted newlines. Determine both the total count and the
	// position of the last occurrence.
	const unsigned char *latest_pos = NULL;
	if (state.line_sep_len == 1 && !state.quote_char && !state.escape_enabled)
		latest_pos = find_end_of_lines1nn(&state, &newline_count, r->maxcount);
	else if (state.line_sep_len == 1)
		latest_pos = find_end_of_lines1(&state, &newline_count, r->maxcount);
	else
		latest_pos = find_end_of_linesN(&state, &newline_count, r->maxcount);

	if (newline_count == r->maxcount) {
		/* We have all the rows we need. The rest is no longer needed */
		*ret_linecount = newline_count;
		*e = state.pos - state.start;
		return MAL_SUCCEED;
	}

	if (state.pos == state.end && latest_pos < state.pos && r->bs->eof) {
		gdk_return proceed;
		if (state.quoted)
			proceed = copy_report_error(errors, newline_count, -1, "unterminated quoted string");
		else
			proceed = copy_report_error(errors, newline_count, -1, "unterminated line at end of file");
		*ret_linecount = newline_count;
		if (proceed == GDK_FAIL)
			bailout("copy.fixlines", "%s", copy_error_message(errors));
		*e = latest_pos - state.start;
		return MAL_SUCCEED;
	}

	if (newline_count) {
		/* we handled some lines */
		*ret_linecount = newline_count;
		*e = latest_pos - state.start;
		return MAL_SUCCEED;
	}

	if (state.pos == state.end && !newline_count && !r->bs->eof) {
		*ret_linecount = 0;
		return msg;
	}
	*ret_linecount = 0;
end:
	return msg;
}

static void
sleep_ns( int ns)
{
#ifdef HAVE_NANOSLEEP
        struct timespec ts;

        ts.tv_sec = (time_t) 0;
        ts.tv_nsec = ns;
        while (nanosleep(&ts, &ts) == -1 && errno == EINTR)
                ;
#else
        struct timeval tv;

        tv.tv_sec = 0;
        tv.tv_usec = ((ns+999)/1000);
        (void) select(0, NULL, NULL, NULL, &tv);
#endif
}

static str
COPYsplitlines(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	str msg = MAL_SUCCEED;
	BAT *block_bat = NULL;
	BAT **return_bats = NULL;
	int **return_indices = NULL;
	int ncols = pci->retc;
	struct error_handling errors;
	struct scan_state state = {
		// most fields are initialized below
		.quoted = false,
		.escape_pending = false,
	};

	assert(pci->argc == pci->retc + 3);
	bat block_bat_id = *getArgReference_bat(stk, pci, pci->retc + 0);
	bat rows_bat_id = *getArgReference_bat(stk, pci, pci->retc + 1);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, pci->retc + 2);
	reader *r = NULL;

	copy_init_error_handling(&errors, cntxt, 0, -1, NULL, rows_bat_id);

	if ((block_bat = BATdescriptor(block_bat_id)) == NULL)
		bailout("copy.splitlines", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	r = (reader*)block_bat->tsink;
	errors.r = r;

	while(!r->bs) {
		pipeline_lock(p);
		if (!r->bs) {
			r->line_count = GDKzalloc(sizeof(lng) * p->p->nr_workers);
			r->bs = bufferstream_create(r->s, r->sz, p->p->nr_workers);
			/* TODO iff mallocs failed set error in r, such that all threads fail properly */
		}
		pipeline_unlock(p);
	}

	state.line_sep = r->line_sep;
	state.line_sep_len = r->line_sep_len;
	state.line_sep_str = r->line_sep_str;
	state.col_sep = r->col_sep;
	state.col_sep_len = r->col_sep_len;
	state.col_sep_str = r->col_sep_str;
	state.quote_char = r->quote_char;
	state.escape_enabled = r->escape_enabled;

	/* busy wait, ie take turn using counter */
	if ((r->done) || r->error)
		p->p->master_counter = (p->p->nr_workers*-2);

	while (!r->done && !r->error && ATOMIC_GET(&r->seqnr) != r->bs->seq[p->wid])
		sleep_ns(1);

	if (!r->done)
		p->seqnr = (int)r->bs->seq[p->wid];
	else
		p->seqnr = -2;
	if (!r->done && !r->error) {
		if (bufferstream_read(r->bs, p->wid) < 0) {
			bailout("copy.splitlines", SQLSTATE(42000) "unterminated line at end of file");
		}
	}

	if (!r->done && !r->error && r->can_jump) {
		if ((r->line_sep_len == 1 && !bufferstream_jump(r->bs, r->line_sep, p->wid)) ||
		    (r->line_sep_len != 1 && !bufferstream_jumpN(r->bs, r->line_sep_str, r->line_sep_len, p->wid))) {
			r->error = true;
			bailout("copy.splitlines", SQLSTATE(42000) "unterminated line at end of file while jumping");
		}
		ATOMIC_INC(&r->seqnr);
	}

	/* TODO: handle skipping with jumping */
	if (!r->error && !r->done && r->offset) {
		lng skipped = COPYskiplines(r, p->wid);
		r->offset -= skipped;
		//r->linecount += skipped; ??
	}

	lng line_count = 0;
	if (!r->error && !r->done) {
		state.start = r->bs->buf[p->wid];
		state.pos = state.start + r->bs->pos[p->wid];
		state.end = state.start + r->bs->len[p->wid];

		BUN e = 0;
		r->line_count[p->wid] = r->linecount;
		errors.starting_row = r->line_count[p->wid];
		msg = COPYfixlines(&line_count, &e, r, p->wid, &errors);
		if (msg) {
			r->error = true;
			return msg;
		}
		if (r->can_jump) {
			while (!r->done && !r->error && ATOMIC_GET(&r->jump_seqnr) != r->bs->seq[p->wid])
				sleep_ns(1);
		}
		r->linecount += line_count;
		state.end = state.start + e;
		r->bs->pos[p->wid] = e;
		r->maxcount -= line_count;
		if (r->maxcount == 0)
			r->done = 1;
		if (r->can_jump)
			ATOMIC_INC(&r->jump_seqnr);
	}
	r->bs->seq[p->wid] += p->p->nr_workers;
	if (r->best_effort) {
		errors.rows = BATdescriptor(errors.rows_batid);
		errors.rows->tseqbase = 0;
		BATsetcount(errors.rows, line_count);
	}

	if (!r->can_jump)
		ATOMIC_INC(&r->seqnr);

	if (r->bs->eof)
		r->done = r->bs->eof;

	return_bats = GDKzalloc(ncols * sizeof(*return_bats));
	return_indices = GDKzalloc(ncols * sizeof(*return_indices));
	if (!return_bats || !return_indices)
		bailout("copy.splitlines",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (int i = 0; i < ncols; i++) {
		BAT *b = COLnew(0, TYPE_int, line_count, TRANSIENT);
		if (b == NULL)
			bailout("copy.splitlines", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return_bats[i] = b;
		return_indices[i] = Tloc(b, 0);
	}

	if (r->col_sep_len > 1)
		msg = scan_fieldsN(&errors, &state, r->null_repr, r->null_repr_len, ncols, line_count, return_indices);
	else if (r->can_jump)
		msg = scan_fields1(&errors, &state, r->null_repr, r->null_repr_len, ncols, line_count, return_indices);
	else
		msg = scan_fields(&errors, &state, r->null_repr, r->null_repr_len, ncols, line_count, return_indices);

end:
	copy_destroy_error_handling(&errors);
	if (block_bat)
		BBPunfix(block_bat->batCacheid);
	if (return_bats) {
		for (int i = 0; i < ncols; i++) {
			if (return_bats[i]) {
				BAT *b = return_bats[i];
				bat id = b->batCacheid;
				if (msg == MAL_SUCCEED) {
					BATsetcount(b, line_count);
					b->tkey = false;
					b->tnil = false;
					b->tnonil = false;
					b->tsorted = false;
					b->trevsorted = false;
					BBPkeepref(b);
					*getArgReference_bat(stk, pci, i) = id;
				} else {
					BBPunfix(id);
				}
			}
		}
		GDKfree(return_bats);
	}
	GDKfree(return_indices);
	if (msg && r)
		r->error = true;
	return msg;
}

static str
COPYrows(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *res = getArgReference_bat(stk, pci, 0);
	BAT *b = COLnew( 0, TYPE_void, 0, TRANSIENT);
	if (!b)
		throw(SQL, "copy.rows",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BATsetcount(b, 0);
	*res = (b->batCacheid);
	BBPkeepref(b);
	return MAL_SUCCEED;
}

static const char fwftsep[2] = {STREAM_FWF_FIELD_SEP, '\0'};
static const char fwfrsep[2] = {STREAM_FWF_RECORD_SEP, '\0'};

static str
COPYnew(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	bat *res = getArgReference_bat(stk, pci, 0);
	stream *s = *(stream**)getArgReference(stk, pci, pci->retc);
	BUN offset = *getArgReference_lng(stk, pci, pci->retc + 1);
	BUN maxcount = *getArgReference_lng(stk, pci, pci->retc + 2);
	BUN sz = *getArgReference_lng(stk, pci, pci->retc + 3);
	str col_sep_str = *getArgReference_str(stk, pci, pci->retc + 4);
	str line_sep_str = *getArgReference_str(stk, pci, pci->retc + 5);
	str quote_str = *getArgReference_str(stk, pci, pci->retc + 6);
	str null_repr = *getArgReference_str(stk, pci, pci->retc + 7);
	bit escape_enabled = *getArgReference_bit(stk, pci, pci->retc + 8);
	str fixed_widths = *getArgReference_str(stk, pci, pci->retc + 9);
	bit best_effort = *getArgReference_bit(stk, pci, pci->retc + 10);

	if (best_effort)
		COPYrejects_create(cntxt);

	if (!strNil(fixed_widths)) {
		size_t ncol = 0, current_width_entry = 0, i;
		size_t *widths;
		const char* val_start = fixed_widths;
		size_t width_len = strlen(fixed_widths);
		stream *ns;

		for (i = 0; i < width_len; i++) {
			if (fixed_widths[i] == STREAM_FWF_FIELD_SEP) {
				ncol++;
			}
		}
		widths = malloc(sizeof(size_t) * ncol); /* use malloc as the widths are eaten by stream_fwf_create outside gdk */
		if (!widths) {
			//close_stream(s);
			throw(MAL, "sql.copy_from", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		for (i = 0; i < width_len; i++) {
			if (fixed_widths[i] == STREAM_FWF_FIELD_SEP) {
				widths[current_width_entry++] = (size_t) strtoll(val_start, NULL, 10);
				val_start = fixed_widths + i + 1;
			}
		}
		/* overwrite other delimiters to the ones the FWF stream uses */
		col_sep_str = (char*)fwftsep;
		line_sep_str = (char*)fwfrsep;
		quote_str = (char*)str_nil;

		ns = stream_fwf_create(s, ncol, widths, STREAM_FWF_FILLER);
		if (ns == NULL || mnstr_errnr(ns) != MNSTR_NO__ERROR) {
			msg = createException(IO, "sql.copy_from", SQLSTATE(42000) "%s", mnstr_peek_error(NULL));
			//close_stream(s);
			free(widths);
			return msg;
		}
		s = ns;
	}

	int line_sep = check_sep(line_sep_str, escape_enabled);
	if (line_sep <= 0) // 0 is not ok
		bailout("copy.new", SQLSTATE(42000) "invalid line separator");
	int col_sep = check_sep(col_sep_str, escape_enabled);
	if (col_sep <= 0) // 0 is not ok
		bailout("copy.new", SQLSTATE(42000) "invalid column separator");
	int quote_char = check_sep(quote_str, escape_enabled);
	if (quote_char < 0 || strlen(quote_str) > 1) // 0 is ok
		bailout("copy.new", SQLSTATE(42000) "invalid quote character");

	line_sep = (line_sep==1)?line_sep_str[0]:0;
	col_sep = (col_sep==1)?col_sep_str[0]:0;
	quote_char = (quote_char==1)?quote_str[0]:0;

	if (strNil(null_repr))
		null_repr = NULL;

	(void)mb; (void)cntxt;
	BAT *b = COLnew(0, TYPE_bte, 1024, TRANSIENT);
	if (!b)
		throw(SQL, "copy.new",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	b->tsink = (Sink*)reader_new(s, offset, maxcount, sz, col_sep_str, line_sep_str, quote_str, null_repr, escape_enabled, best_effort);
	if (!b->tsink) {
		BBPreclaim(b);
		throw(SQL, "copy.new",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	reader *r = (reader*)b->tsink;
	r->col_sep = col_sep;
	r->line_sep = line_sep;
	r->quote_char = quote_char;
	*res = (b->batCacheid);
	r->can_jump = (!escape_enabled && !quote_char && r->col_sep_len == 1 && r->offset == 0);
	BBPkeepref(b);
end:
	return msg;
}

static mel_func copy_init_funcs[] = {
 pattern("copy", "request_upload", COPYrequest_upload, true, "request MAPI file upload",
	args(1, 3,
		arg("", streams),
		arg("filename", str), arg("on_client", int)
 )),
 pattern("copy", "from_stdin", COPYfrom_stdin, true, "read FROM STDIN",
 	args(1, 7,
		arg("", streams),
		arg("offset", lng), arg("lines", lng),
		arg("stoponemptyline", bit),
		arg("linesep", str), arg("quote", str), arg("escape", bit)
 )),
 pattern("copy", "new", COPYnew, true, "Create resource for shared reading from stream",
	args(1, 12,
		batarg("", bte),
		arg("s", stream), arg("offset", lng), arg("count", lng), arg("sz", lng),
		arg("col_sep", str), arg("line_sep", str), arg("quote", str), arg("null_repr", str), arg("escape", bit),
		arg("fixed_widths", str), arg("best_effort", bit)
 )),
 pattern("copy", "rows", COPYrows, true, "For best effort we use a selection vector too produce the correctly formated rows",
	args(1, 1,
		batarg("", oid)
 )),
 pattern("copy", "splitlines", COPYsplitlines, false, "Find the fields of the individual columns",
	args(1, 4,
	batvararg("", int),
	batarg("block", bte), batarg("rows", oid), arg("pipeline", ptr)
 )),
 pattern("copy", "parse_generic", COPYparse_generic, false, "Parse using GDK's atomFromStr", args(1, 8,
	batargany("", 1),
	batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), argany("type", 1), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 pattern("copy", "parse_float", COPYparse_float, false, "Parse using GDK's atomFromStr, handle optional seperator and skip char", args(1, 10,
	batargany("", 1),
	batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), argany("type", 1), batarg("rows", oid), arg("colno", int), arg("colname", str), arg("sep", str), arg("skip", str)
 )),
 pattern("copy", "parse_string", COPYparse_string, false, "Parse as a string with given max width", args(1, 8,
	batarg("", str),
	batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("maxlen", int), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 pattern("copy", "parse_decimal", COPYparse_decimal_bte, false, "Parse as a decimal", args(1, 12,
	 batarg("", bte),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", bte), batarg("rows", oid), arg("colno", int), arg("colname", str), arg("dec_sep", str), arg("dec_skip", str)
 )),
 pattern("copy", "parse_decimal", COPYparse_decimal_sht, false, "Parse as a decimal", args(1, 12,
	 batarg("", sht),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", sht), batarg("rows", oid), arg("colno", int), arg("colname", str), arg("dec_sep", str), arg("dec_skip", str)
 )),
 pattern("copy", "parse_decimal", COPYparse_decimal_int, false, "Parse as a decimal", args(1, 12,
	 batarg("", int),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", int), batarg("rows", oid), arg("colno", int), arg("colname", str), arg("dec_sep", str), arg("dec_skip", str)
 )),
 pattern("copy", "parse_decimal", COPYparse_decimal_lng, false, "Parse as a decimal", args(1, 12,
	 batarg("", lng),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", lng), batarg("rows", oid), arg("colno", int), arg("colname", str), arg("dec_sep", str), arg("dec_skip", str)
 )),
 #ifdef HAVE_HGE
 pattern("copy", "parse_decimal", COPYparse_decimal_hge, false, "Parse as a decimal", args(1, 12,
	 batarg("", hge),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", hge), batarg("rows", oid), arg("colno", int), arg("colname", str), arg("dec_sep", str), arg("dec_skip", str)
 )),
#endif

 pattern("copy", "parse_integer", COPYparse_integer_bte, false, "Parse as an integer", args(1, 8,
	 batarg("", bte),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("type", bte), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 pattern("copy", "parse_integer", COPYparse_integer_sht, false, "Parse as an integer", args(1, 8,
	 batarg("", sht),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("type", sht), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 pattern("copy", "parse_integer", COPYparse_integer_int, false, "Parse as an integer", args(1, 8,
	 batarg("", int),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("type", int), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 pattern("copy", "parse_integer", COPYparse_integer_lng, false, "Parse as an integer", args(1, 8,
	 batarg("", lng),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("type", lng), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 #ifdef HAVE_HGE
 pattern("copy", "parse_integer", COPYparse_integer_hge, false, "Parse as an integer", args(1, 8,
	 batarg("", hge),
	 batarg("block", bte), arg("pipeline", ptr), batarg("offsets", int), arg("type", hge), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
#endif

 pattern("copy", "scale", COPYscale_bte, false, "scale by a power of 10", args(1, 6,
	batarg("", bte),
	batarg("values", bte), arg("scale", int), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 pattern("copy", "scale", COPYscale_sht, false, "scale by a power of 10", args(1, 6,
	batarg("", sht),
	batarg("values", sht), arg("scale", int), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 pattern("copy", "scale", COPYscale_int, false, "scale by a power of 10", args(1, 6,
	batarg("", int),
	batarg("values", int), arg("scale", int), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 pattern("copy", "scale", COPYscale_lng, false, "scale by a power of 10", args(1, 6,
	batarg("", lng),
	batarg("values", lng), arg("scale", int), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
 #ifdef HAVE_HGE
 pattern("copy", "scale", COPYscale_hge, false, "scale by a power of 10", args(1, 6,
	batarg("", hge),
	batarg("values", hge), arg("scale", int), batarg("rows", oid), arg("colno", int), arg("colname", str)
 )),
#endif


 command("copy", "set_blocksize", COPYset_blocksize, false, "set the COPY block size", args(1, 2,
	arg("", int),
	arg("blocksize", int)
 )),
 command("copy", "get_blocksize", COPYget_blocksize, false, "get the COPY block size", args(1, 1,
	arg("", int)
 )),

 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_copy_mal)
{
	mal_module("copy", NULL, copy_init_funcs);
}
