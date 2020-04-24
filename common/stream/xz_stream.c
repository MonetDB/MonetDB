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
static int
pump_out(stream *s, lzma_action action)
{
	xz_state *xz = (xz_state *) s->stream_data.p;

	while (1) {
		// Make sure there is room in the output buffer
		if (xz->strm.avail_out == 0) {
			size_t nwritten = fwrite(xz->buf, 1, XZBUFSIZ, xz->fp);
			if (nwritten != XZBUFSIZ) {
				return 0;
			}
			xz->strm.next_out = xz->buf;
			xz->strm.avail_out = XZBUFSIZ;
		}

		lzma_ret ret = lzma_code(&xz->strm, action);
		if (ret != LZMA_OK && ret != LZMA_STREAM_END) {
			// Some kind of error.
			return 0;
		}

		if (xz->strm.avail_in > 0) {
			// Definitely not done yet. Flush the buffer and encode
			// some more.
			continue;
		}

		// Whether we are already done or not depends on the mode.
		if (action == LZMA_RUN) {
			assert(ret == LZMA_OK);
			// More input will follow so we can leave the output buffer
			// for later.
			return 1;
		}

		// We need to flush all data out of the encoder and out of our
		// buffer.
		if (ret == LZMA_OK) {
			// encoder requests another iteration
			continue;
		}

		// That was it.  Flush the remainder of the buffer and exit.
		size_t amount = xz->strm.next_out - xz->buf;
		if (amount > 0) {
			size_t nwritten = fwrite(xz->buf, 1, amount, xz->fp);
			if (nwritten != amount) {
				return 0;
			}
		}
		xz->strm.next_out = xz->buf;
		xz->strm.avail_out = XZBUFSIZ;
		return 1;
	}
}


/* Keep working lzma_code until the output buffer is full or the input stream
 * is exhausted.
 *
 * We're moving data from the input stream to the input buffer to the
 * lzma internal state to the output buffer. If the input stream is exhausted
 * we still have to flush the input buffer and especially the internal state.
 *
 * The most trickly situation is if the output buffer goes full while this
 * flushing takes place. We have to remember until next time that the internal
 * state is not fully flushed. We do so by setting next_int to NULL after we
 * have received LZMA_STREAM_END to signal we're fully done.
 *
 * Return the number of bytes stored into the output buffer, or -1 on error.
 */
static ssize_t
pump_in(stream *s)
{
	xz_state *xz = (xz_state *) s->stream_data.p;
	uint8_t *orig_out = xz->strm.next_out;

	if (xz->strm.next_in == NULL) {
		// Signals we're fully done with no data lingering in the internal
		// state.
		assert(xz->fp == NULL);
		return 0;
	}

	while (1) {
		// If the output buffer is full, return immediately.
		if (xz->strm.avail_out == 0)
			return xz->strm.next_out - orig_out;

		// Refill the buffer if necessary and possible.
		if (xz->strm.avail_in == 0 && xz->fp != NULL) {
			size_t nread = fread(xz->buf, 1, XZBUFSIZ, xz->fp);
			if (nread == 0) {
				if (feof(xz->fp)) {
					fclose(xz->fp);
					xz->fp = NULL;
				} else {
					s->errnr = MNSTR_READ_ERROR;
					return -1;
				}
			}
			xz->strm.next_in = xz->buf;
			xz->strm.avail_in = nread;
		}

		// If we have no input stream, we're flushing.
		lzma_action action = xz->fp != NULL ? LZMA_RUN : LZMA_FINISH;
		lzma_ret ret = lzma_code(&xz->strm, action);
		switch (ret) {
			case LZMA_OK:
				// time for another round of output checking
				// input filling
				continue;
			case LZMA_STREAM_END:
				// fully done.
				if (xz->fp != NULL)
					fclose(xz->fp);
				assert(xz->strm.avail_in == 0);
				xz->strm.next_in = NULL; // to indicate we have seen LZMA_STREAM_END
				return xz->strm.next_out - orig_out;
			default:
				s->errnr = MNSTR_READ_ERROR;
				return -1;
		}
	}
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

	if (pump_out(s, LZMA_RUN))
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
			if (pump_out(s, LZMA_FINISH)) {
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
	if (pump_out(s, LZMA_FULL_BARRIER)) {
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

