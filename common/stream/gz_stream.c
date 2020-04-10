/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* streams working on a gzip-compressed disk file */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


#ifdef HAVE_LIBZ
#if ZLIB_VERNUM < 0x1290
typedef size_t z_size_t;

/* simplistic version for ancient systems (CentOS 6, Ubuntu Trusty) */
static z_size_t
gzfread(void *buf, z_size_t size, z_size_t nitems, gzFile file)
{
	unsigned sz = nitems * size > (size_t) 1 << 30 ? 1 << 30 : (unsigned) (nitems * size);
	int len;

	len = gzread(file, buf, sz);
	if (len == -1)
		return 0;
	return (z_size_t) len / size;
}

static z_size_t
gzfwrite(const void *buf, z_size_t size, z_size_t nitems, gzFile file)
{
	z_size_t sz = nitems * size;

	while (sz > 0) {
		unsigned len = sz > ((z_size_t) 1 << 30) ? 1 << 30 : (unsigned) sz;
		int wlen;

		wlen = gzwrite(file, buf, len);
		if (wlen <= 0)
			return 0;
		buf = (const void *) ((const char *) buf + wlen);
		sz -= (z_size_t) wlen;
	}
	return nitems;
}
#endif

static ssize_t
stream_gzread(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	gzFile fp = (gzFile) s->stream_data.p;
	z_size_t size;

	if (fp == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}

	if (elmsize == 0 || cnt == 0)
		return 0;

	size = gzfread(buf, elmsize, cnt, fp);
	/* when in text mode, convert \r\n line endings to \n */
	if (!s->binary) {
		char *p1, *p2, *pe;

		p1 = buf;
		pe = p1 + size;
		while (p1 < pe && *p1 != '\r')
			p1++;
		p2 = p1;
		while (p1 < pe) {
			if (*p1 == '\r' && p1[1] == '\n')
				size--;
			else
				*p2++ = *p1;
			p1++;
		}
	}

	return size == 0 ? -1 : (ssize_t) size;
}

static ssize_t
stream_gzwrite(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	gzFile fp = (gzFile) s->stream_data.p;
	z_size_t size;

	if (fp == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}

	if (elmsize == 0 || cnt == 0)
		return 0;

	size = gzfwrite(buf, elmsize, cnt, fp);
	return size == 0 ? -1 : (ssize_t) size;
}

static int
stream_gzflush(stream *s)
{
	if (s->stream_data.p == NULL)
		return -1;
	if (!s->readonly &&
	    gzflush((gzFile) s->stream_data.p, Z_SYNC_FLUSH) != Z_OK)
		return -1;
	return 0;
}

static void
stream_gzclose(stream *s)
{
	stream_gzflush(s);
	if (s->stream_data.p)
		gzclose((gzFile) s->stream_data.p);
	s->stream_data.p = NULL;
}

static stream *
open_gzstream(const char *restrict filename, const char *restrict flags)
{
	stream *s;
	gzFile fp;

	if ((s = create_stream(filename)) == NULL)
		return NULL;
#ifdef HAVE__WFOPEN
	{
		wchar_t *wfname = utf8towchar(filename);
		if (wfname != NULL) {
			fp = gzopen_w(wfname, flags);
			free(wfname);
		} else
			fp = NULL;
	}
#else
	{
		char *fname = cvfilename(filename);
		if (fname) {
			fp = gzopen(fname, flags);
			free(fname);
		} else
			fp = NULL;
	}
#endif
	if (fp == NULL) {
		destroy_stream(s);
		return NULL;
	}
	s->read = stream_gzread;
	s->write = stream_gzwrite;
	s->close = stream_gzclose;
	s->flush = stream_gzflush;
	s->stream_data.p = (void *) fp;
	if (flags[0] == 'r' && flags[1] != 'b') {
		char buf[UTF8BOMLENGTH];
		if (gzread(fp, buf, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
			s->isutf8 = true;
		} else {
			gzrewind(fp);
		}
	}
	return s;
}

stream *
open_gzrstream(const char *filename)
{
	stream *s;

	if ((s = open_gzstream(filename, "rb")) == NULL)
		return NULL;
	s->binary = true;
	return s;
}

stream *
open_gzwstream(const char *restrict filename, const char *restrict mode)
{
	stream *s;

	if ((s = open_gzstream(filename, mode)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = true;
	return s;
}

stream *
open_gzrastream(const char *filename)
{
	stream *s;

	if ((s = open_gzstream(filename, "r")) == NULL)
		return NULL;
	s->binary = false;
	return s;
}

stream *
open_gzwastream(const char *restrict filename, const char *restrict mode)
{
	stream *s;

	if ((s = open_gzstream(filename, mode)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = false;
	return s;
}
#else

stream *
open_gzrstream(const char *filename)
{
	return NULL;
}

stream *
open_gzwstream(const char *filename, const char *mode)
{
	return NULL;
}

stream *
open_gzrastream(const char *filename)
{
	return NULL;
}

stream *
open_gzwastream(const char *filename, const char *mode)
{
	return NULL;
}

#endif

