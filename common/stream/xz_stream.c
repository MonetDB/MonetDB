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
typedef struct xz_stream {
	FILE *fp;
	lzma_stream strm;
	size_t todo;
	uint8_t buf[XZBUFSIZ];
} xz_stream;

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
pump_out(xz_stream *xz, lzma_action action)
{
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


static ssize_t
stream_xzread(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	xz_stream *xz = s->stream_data.p;
	size_t size = elmsize * cnt, origsize = size, ressize = 0;
	uint8_t *outbuf = buf;
	lzma_action action = LZMA_RUN;

	if (xz == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}

	xz->strm.next_in = xz->buf;
	xz->strm.avail_in = xz->todo;
	xz->strm.next_out = outbuf;
	xz->strm.avail_out = size;
	while (size && (xz->strm.avail_in || !feof(xz->fp))) {
		lzma_ret ret;
		size_t sz = (size > XZBUFSIZ) ? XZBUFSIZ : size;

		if (xz->strm.avail_in == 0 &&
		    (xz->strm.avail_in = fread(xz->buf, 1, sz, xz->fp)) == 0) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
		xz->strm.next_in = xz->buf;
		if (feof(xz->fp))
			action = LZMA_FINISH;
		ret = lzma_code(&xz->strm, action);
		if (xz->strm.avail_out == 0 || ret == LZMA_STREAM_END) {
			origsize -= xz->strm.avail_out;	/* remaining space */
			xz->todo = xz->strm.avail_in;
			if (xz->todo > 0)
				memmove(xz->buf, xz->strm.next_in, xz->todo);
			ressize = origsize;
			break;
		}
		if (ret != LZMA_OK) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
	}
	if (ressize) {
		/* when in text mode, convert \r\n line endings to
		 * \n */
		if (!s->binary) {
			char *p1, *p2, *pe;

			p1 = buf;
			pe = p1 + ressize;
			while (p1 < pe && *p1 != '\r')
				p1++;
			p2 = p1;
			while (p1 < pe) {
				if (*p1 == '\r' && p1[1] == '\n')
					ressize--;
				else
					*p2++ = *p1;
				p1++;
			}
		}
		return (ssize_t) (ressize / elmsize);
	}
	return 0;
}

static ssize_t
stream_xzwrite(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	xz_stream *xz = s->stream_data.p;
	size_t size = elmsize * cnt;

	if (size == 0)
		return cnt;

	if (xz == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}

	xz->strm.next_in = buf;
	xz->strm.avail_in = size;

	if (pump_out(xz, LZMA_RUN))
		return (ssize_t) (size / elmsize);
	else {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
}

static void
stream_xzclose(stream *s)
{
	xz_stream *xz = s->stream_data.p;

	if (xz) {
		if (!s->readonly) {
			xz->strm.next_in = NULL;
			xz->strm.avail_in = 0;
			if (pump_out(xz, LZMA_FINISH)) {
				fflush(xz->fp);
			} else {
				s->errnr = MNSTR_WRITE_ERROR;
			}
		}
		fclose(xz->fp);
		lzma_end(&xz->strm);
		free(xz);
	}
	s->stream_data.p = NULL;
}

static int
stream_xzflush(stream *s)
{
	xz_stream *xz = s->stream_data.p;

	if (s->readonly)
		return 0;
	if (xz == NULL)
		return -1;

	xz->strm.next_in = NULL;
	xz->strm.avail_in = 0;
	if (pump_out(xz, LZMA_FULL_BARRIER)) {
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
	xz_stream *xz;
	uint32_t preset = 0;
	char fl[3];

	if ((xz = calloc(1, sizeof(struct xz_stream))) == NULL)
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
	xz->strm.next_out = xz->buf;
	xz->strm.avail_out = XZBUFSIZ;
	if (flags[0] == 'r' && flags[1] != 'b') {
		char buf[UTF8BOMLENGTH];
		if (stream_xzread(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
			s->isutf8 = true;
		} else {
			lzma_end(&xz->strm);
			if (lzma_stream_decoder(&xz->strm, UINT64_MAX, LZMA_CONCATENATED) != LZMA_OK
				|| fseek (xz->fp, 0L, SEEK_SET) < 0) {
				fclose(xz->fp);
				free(xz);
				destroy_stream(s);
				return NULL;
			}
			xz->todo = 0;
		}
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
	stream *s;

	if ((s = open_xzstream(filename, "r")) == NULL)
		return NULL;
	s->binary = false;
	return s;
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

