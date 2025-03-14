/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


void
buffer_init(buffer *restrict b, char *restrict buf, size_t size)
{
	if (b == NULL || buf == NULL)
		return;
	*b = (buffer) {
		.pos = 0,
		.buf = buf,
		.len = size,
	};
}

buffer *
buffer_create(size_t size)
{
	buffer *b;

	if ((b = malloc(sizeof(*b))) == NULL)
		return NULL;
	*b = (buffer) {
		.buf = malloc(size),
		.len = size,
	};
	if (b->buf == NULL) {
		free(b);
		return NULL;
	}
	return b;
}

char *
buffer_get_buf(buffer *b)
{
	char *r;

	if (b == NULL)
		return NULL;
	if (b->pos == b->len) {
		if ((r = realloc(b->buf, b->len + 1)) == NULL) {
			/* keep b->buf in tact */
			return NULL;
		}
		b->buf = r;
	}
	r = b->buf;
	r[b->pos] = '\0';
	b->buf = malloc(b->len);
	if (b->buf == NULL) {
		/* restore b->buf */
		b->buf = r;
		return NULL;
	}
	b->pos = 0;
	return r;
}

void
buffer_destroy(buffer *b)
{
	if (b == NULL)
		return;
	if (b->buf)
		free(b->buf);
	free(b);
}

buffer *
mnstr_get_buffer(stream *s)
{
	if (s == NULL)
		return NULL;
	buffer *b = (buffer *) s->stream_data.p;
	if (b == NULL)
		return NULL;
	if (!s->readonly) {
		/* switching from write mode to read mode */
		s->readonly = true;
		b->len = b->pos;
	}
	b->pos = 0;
	return b;
}

static ssize_t
buffer_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (b == NULL)
		return -1;
	if (size != 0) {
		if (size + b->pos > b->len)
			size = b->len - b->pos;
		memcpy(buf, b->buf + b->pos, size);
		b->pos += size;
		return (ssize_t) (size / elmsize);
	}
	s->eof |= b->pos == b->len;
	return 0;
}

static ssize_t
buffer_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (b == NULL) {
		mnstr_set_error(s, MNSTR_WRITE_ERROR, "buffer already deallocated");
		return -1;
	}
	if (b->pos + size > b->len) {
		char *p;
		size_t ns = b->pos + size + 8192;

		if ((p = realloc(b->buf, ns)) == NULL) {
			mnstr_set_error(s, MNSTR_WRITE_ERROR, "buffer reallocation failed");
			return -1;
		}
		b->buf = p;
		b->len = ns;
	}
	memcpy(b->buf + b->pos, buf, size);
	b->pos += size;
	return (ssize_t) cnt;
}

static void
buffer_close(stream *s)
{
	(void) s;
}

static int
buffer_flush(stream *s, mnstr_flush_level flush_level)
{
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (b == NULL)
		return -1;
	b->pos = 0;
	(void) flush_level;
	return 0;
}

static void
buffer_destroy_stream(stream *s)
{
	buffer_destroy(s->stream_data.p);
	destroy_stream(s);
}

stream *
buffer_rastream(buffer *restrict b, const char *restrict name)
{
	stream *s;

	if (b == NULL || name == NULL) {
		mnstr_set_open_error(name, 0, "no buffer or no name");
		return NULL;
	}
#ifdef STREAM_DEBUG
	fprintf(stderr, "buffer_rastream %s\n", name);
#endif
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->binary = false;
	s->read = buffer_read;
	s->write = buffer_write;
	s->close = buffer_close;
	s->flush = buffer_flush;
	s->stream_data.p = (void *) b;
	return s;
}

stream *
buffer_wastream(buffer *restrict b, const char *restrict name)
{
	stream *s;

	if (name == NULL) {
		mnstr_set_open_error(name, 0, "no name");
		return NULL;
	}
#ifdef STREAM_DEBUG
	fprintf(stderr, "buffer_wastream %s\n", name);
#endif
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = false;
	s->read = buffer_read;
	s->write = buffer_write;
	s->close = buffer_close;
	s->flush = buffer_flush;
	if (b == NULL) {
		b = buffer_create(1 << 20);
		if (b == NULL) {
			destroy_stream(s);
			return NULL;
		}
		s->destroy = buffer_destroy_stream;
	}
	s->stream_data.p = (void *) b;
	return s;
}
