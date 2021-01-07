/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"




/* ------------------------------------------------------------------ */

bstream *
bstream_create(stream *s, size_t size)
{
	bstream *b;

	if (s == NULL)
		return NULL;
	if ((b = malloc(sizeof(*b))) == NULL)
		return NULL;
	*b = (bstream) {
		.mode = size,
		.s = s,
		.eof = false,
	};
	if (size == 0)
		size = BUFSIZ;
	b->buf = malloc(size + 1 + 1);
	if (b->buf == NULL) {
		free(b);
		return NULL;
	}
	b->size = size;
	return b;
}

ssize_t
bstream_read(bstream *s, size_t size)
{
	ssize_t rd, rd1 = 0;

	if (s == NULL)
		return -1;

	if (s->eof)
		return 0;

	assert(s->buf != NULL);

	if (s->pos > 0) {
		if (s->pos < s->len) {
			/* move all data and end of string marker */
			memmove(s->buf, s->buf + s->pos, s->len - s->pos + 1);
			s->len -= s->pos;
		} else
			s->len = 0;
		s->pos = 0;
	}

	if (s->len == s->size) {
		size_t sz = size > 8192 ? 8192 : size;
		char tmpbuf[8192];

		/* before we realloc more space, see if there is a need */
		if ((rd1 = s->s->read(s->s, tmpbuf, 1, sz)) == 0) {
			s->eof = true;
			return 0;
		}
		if (rd1 < 0)
			return rd1;
		char *p;
		size_t ns = s->size + size;
		if ((p = realloc(s->buf, ns + 1)) == NULL) {
			return -1;
		}
		s->size = ns;
		s->buf = p;
		memcpy(s->buf + s->len, tmpbuf, rd1);
		s->len += rd1;
		size -= rd1;
		if (size == 0)
			return rd1;
	}

	if (s->len + size > s->size)
		size = s->size - s->len;

	rd = s->s->read(s->s, s->buf + s->len, 1, size);

	if (rd < 0)
		return rd;

	if (rd == 0) {
		s->eof = true;
		return rd1;
	}
	s->len += (size_t) rd;
	s->buf[s->len] = 0;	/* fill in the spare with EOS */
	return rd + rd1;
}

#ifdef _POSIX2_LINE_MAX
#define STREAM_LINE_MAX _POSIX2_LINE_MAX
#else
#define STREAM_LINE_MAX 2048
#endif

static ssize_t
bstream_readline(bstream *s)
{
	size_t size = STREAM_LINE_MAX;
	size_t rd;

	if (s->eof)
		return 0;

	if (s->pos > 0 && s->len + size >= s->size) {
		if (s->pos < s->len) {
			/* move all data and end of string marker */
			memmove(s->buf, s->buf + s->pos, s->len - s->pos + 1);
			s->len -= s->pos;
		} else
			s->len = 0;
		s->pos = 0;
	}

	assert(s->buf != NULL);
	if (s->len == s->size) {
		char *p;
		size_t ns = s->size + size + 8192;
		if ((p = realloc(s->buf, ns + 1)) == NULL) {
			return -1;
		}
		s->size = ns;
		s->buf = p;
	}

	if (size > s->size - s->len)
		size = s->size - s->len;

	if (fgets(s->buf + s->len, (int) size, s->s->stream_data.p) == NULL)
		return -1;

	rd = strlen(s->buf + s->len);

	if (rd == 0) {
		s->eof = true;
		return 0;
	}
	s->len += rd;
	s->buf[s->len] = 0;	/* fill in the spare with EOS */
	return (ssize_t) rd;
}


ssize_t
bstream_next(bstream *s)
{
	if (s == NULL)
		return -1;
	if (s->mode > 0) {
		return bstream_read(s, s->mode);
	} else if (s->s->read == file_read) {
		return bstream_readline(s);
	} else {
		size_t sz = 0;
		ssize_t rd;

		while ((rd = bstream_read(s, 1)) == 1 &&
		       s->buf[s->pos + sz] != '\n') {
			sz++;	/* sz += rd, but rd == 1 */
		}
		if (rd < 0)
			return rd;
		return (ssize_t) sz;
	}
}

void
bstream_destroy(bstream *s)
{
	if (s) {
		if (s->s) {
			s->s->close(s->s);
			s->s->destroy(s->s);
		}
		if (s->buf)
			free(s->buf);
		free(s);
	}
}

