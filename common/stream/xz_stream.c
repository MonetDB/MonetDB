/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* streams working on a lzma/xz-compressed disk file */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"
#include "pump.h"


#ifdef HAVE_LIBLZMA
#define XZBUFSIZ 64*1024
typedef struct xz_state {
	FILE *fp;
	lzma_stream strm;
	uint8_t buf[XZBUFSIZ];
} xz_state;


/* Keep calling lzma_code until the whole input buffer has been consumed
 * and all necessary output has been written.
 *
 * If action is LZMA_RUN, iteration ends when lzma_code() returns LZMA_OK
 * while the output buffer is not empty. The output buffers is not flushed
 * because we expect further invocations.
 *
 * If action is something else, for example LZMA_FINISH, iteration ends
 * when lzma_code() returns LZMA_STREAM_END and the out buffer is flushed
 * afterward.
 *
 * Returns > 0 on succes, 0 on error.
 */
static pump_result
pumper(void *state, pump_action action)
{
	stream *s = (stream*) state;
	xz_state *xz = (xz_state*) s->stream_data.p;

	lzma_action a;
	switch (action) {
	case PUMP_WORK:
		a = LZMA_RUN;
		break;
	case PUMP_FLUSH_DATA:
		a = LZMA_SYNC_FLUSH;
		break;
	case PUMP_FLUSH_ALL:
		a = LZMA_FULL_FLUSH;
		break;
	case PUMP_FINISH:
		a = LZMA_FINISH;
		break;
	}

	lzma_ret ret = lzma_code(&xz->strm, a);

	switch (ret) {
		case LZMA_OK:
			return PUMP_OK;
		case LZMA_STREAM_END:
			return PUMP_END;
		default:
			return PUMP_ERROR;
	}
}


static ssize_t
ship_in(void *state, char *start, size_t count)
{
	stream *s = (stream*) state;
	xz_state *xz =  (xz_state*) s->stream_data.p;

	size_t nread = fread(start, 1, count, xz->fp);
	if (nread == 0 && ferror(xz->fp))
		return -1;
	else
		return (ssize_t)nread;
}

static ssize_t
ship_out(void *state, char *start, size_t count)
{
	stream *s = (stream*) state;
	xz_state *xz =  (xz_state*) s->stream_data.p;

	size_t nwritten = fwrite(start, 1, count, xz->fp);
	if (nwritten == 0 && ferror(xz->fp))
		return -1;
	else
		return (ssize_t)nwritten;
}


static pump_result
pump_out(stream *s, pump_action action)
{
	xz_state *xz = (xz_state *) s->stream_data.p;

	pump_buffer loc_buffer = { .start = (char*)&xz->buf, .count = XZBUFSIZ };
	pump_buffer_location loc_win_in = { .start = (char**)&xz->strm.next_in, .count = &xz->strm.avail_in };
	pump_buffer_location loc_win_out = { .start = (char**)&xz->strm.next_out, .count = &xz->strm.avail_out };
	return generic_pump_out(s, action, loc_buffer, loc_win_in, loc_win_out, pumper, ship_out);
}

static ssize_t
pump_in(stream *s)
{
	xz_state *xz = (xz_state *) s->stream_data.p;


	pump_buffer loc_buffer = { .start = (char*)&xz->buf, .count = XZBUFSIZ };
	pump_buffer_location loc_win_in = { .start = (char**)&xz->strm.next_in, .count = &xz->strm.avail_in };
	pump_buffer_location loc_win_out = { .start = (char**)&xz->strm.next_out, .count = &xz->strm.avail_out };

	uint8_t *before = xz->strm.next_out;
	pump_result ret = generic_pump_in(s, loc_buffer, loc_win_in, loc_win_out, pumper, ship_in);
	uint8_t *after = xz->strm.next_out;
	if (ret == PUMP_ERROR)
		return -1;
	return after - before;
}

static ssize_t
stream_xzread(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	xz_state *xz = s->stream_data.p;
	size_t size = elmsize * cnt;

	if (xz == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}

	xz->strm.next_out = (uint8_t*) buf;
	xz->strm.avail_out = size;
	ssize_t nread = pump_in(s);
	if (nread < 0)
		return -1;
	else
		return nread / (ssize_t) elmsize;
}

static ssize_t
stream_xzwrite(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	xz_state *xz = s->stream_data.p;
	size_t size = elmsize * cnt;

	if (size == 0)
		return cnt;

	if (xz == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}

	xz->strm.next_in = buf;
	xz->strm.avail_in = size;

	if (pump_out(s, PUMP_WORK) == PUMP_OK)
		return (ssize_t) (size / elmsize);
	else {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
}

static void
stream_xzclose(stream *s)
{
	xz_state *xz = s->stream_data.p;

	if (xz) {
		if (!s->readonly) {
			xz->strm.next_in = NULL;
			xz->strm.avail_in = 0;
			if (pump_out(s, PUMP_FINISH) == PUMP_END) {
				fflush(xz->fp);
			} else {
				s->errnr = MNSTR_WRITE_ERROR;
			}
		}
		if (xz->fp)
			fclose(xz->fp);
		lzma_end(&xz->strm);
		free(xz);
	}
	s->stream_data.p = NULL;
}

static int
stream_xzflush(stream *s)
{
	xz_state *xz = s->stream_data.p;

	if (s->readonly)
		return 0;
	if (xz == NULL)
		return -1;

	xz->strm.next_in = NULL;
	xz->strm.avail_in = 0;
	if (pump_out(s, PUMP_FLUSH_ALL) == PUMP_OK) {
		fflush(xz->fp);
	} else {
		s->errnr = MNSTR_WRITE_ERROR;
	}

	return 0;
}

static stream *
open_xzstream(const char *restrict filename, const char *restrict flags)
{
	stream *s;
	xz_state *xz;
	uint32_t preset = 0;
	char fl[3];

	if ((xz = calloc(1, sizeof(struct xz_state))) == NULL)
		return NULL;
	if (((flags[0] == 'r' &&
	      lzma_stream_decoder(&xz->strm, UINT64_MAX, LZMA_CONCATENATED) != LZMA_OK)) ||
	    (flags[0] == 'w' &&
	     lzma_easy_encoder(&xz->strm, preset, LZMA_CHECK_CRC64) != LZMA_OK)) {
		free(xz);
		return NULL;
	}
	if ((s = create_stream(filename)) == NULL) {
		free(xz);
		return NULL;
	}
	fl[0] = flags[0];	/* 'r' or 'w' */
	fl[1] = 'b';		/* always binary */
	fl[2] = '\0';
#ifdef HAVE__WFOPEN
	{
		wchar_t *wfname = utf8towchar(filename);
		wchar_t *wflags = utf8towchar(fl);
		if (wfname != NULL)
			xz->fp = _wfopen(wfname, wflags);
		else
			xz->fp = NULL;
		if (wfname)
			free(wfname);
		if (wflags)
			free(wflags);
	}
#else
	{
		char *fname = cvfilename(filename);
		if (fname) {
			xz->fp = fopen(fname, fl);
			free(fname);
		} else
			xz->fp = NULL;
	}
#endif
	if (xz->fp == NULL) {
		destroy_stream(s);
		free(xz);
		return NULL;
	}
	s->read = stream_xzread;
	s->write = stream_xzwrite;
	s->close = stream_xzclose;
	s->flush = stream_xzflush;
	s->stream_data.p = (void *) xz;
	if (flags[0] == 'r') {
		// input stream -> our buffer -> lzma_state -> caller buffer
		xz->strm.next_in = xz->buf;
		xz->strm.avail_in = 0;
	} else {
		assert(flags[0] == 'w');
		// caller buffer -> lzma_state -> our buffer -> output stream
		xz->strm.next_out = xz->buf;
		xz->strm.avail_out = XZBUFSIZ;
	}
	return s;
}

stream *
open_xzrstream(const char *filename)
{
	stream *s;

	if ((s = open_xzstream(filename, "rb")) == NULL)
		return NULL;
	s->binary = true;
	return s;
}

stream *
open_xzwstream(const char *restrict filename, const char *restrict mode)
{
	stream *s;

	if ((s = open_xzstream(filename, mode)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = true;
	return s;
}

stream *
open_xzrastream(const char *filename)
{
	stream *s = open_xzstream(filename, "r");
	return create_text_stream(s);
}

stream *
open_xzwastream(const char *restrict filename, const char *restrict mode)
{
	stream *s;

	if ((s = open_xzstream(filename, mode)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = false;
	return s;
}
#else
stream *open_xzrstream(const char *filename)
{
	return NULL;
}

stream *open_xzwstream(const char *filename, const char *mode)
{
	return NULL;
}

stream *open_xzrastream(const char *filename)
{
	return NULL;
}

stream *open_xzwastream(const char *filename, const char *mode)
{
	return NULL;
}

#endif

