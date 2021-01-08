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
/* callback stream
 *
 * read-only stream which calls a user-provided callback function in
 * order to get more data to be returned to the reader */

struct cbstream {
	void *private;
	void (*destroy)(void *);
	void (*close)(void *);
	ssize_t (*read)(void *, void *, size_t, size_t);
	ssize_t (*write)(void *, const void *, size_t, size_t);
};

static void
cb_destroy(stream *s)
{
	struct cbstream *cb = s->stream_data.p;

	if (cb->destroy)
		cb->destroy(cb->private);
	free(cb);
	s->stream_data.p = NULL;
	destroy_stream(s);
}

static void
cb_close(stream *s)
{
	struct cbstream *cb = s->stream_data.p;

	if (cb->close)
		cb->close(cb->private);
}

static ssize_t
cb_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	struct cbstream *cb = s->stream_data.p;

	return cb->read(cb->private, buf, elmsize, cnt);
}

static ssize_t
cb_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	struct cbstream *cb = s->stream_data.p;

	return cb->write(cb->private, buf, elmsize, cnt);
}

stream *
callback_stream(void *restrict private,
		ssize_t (*read)(void *restrict private, void *restrict buf, size_t elmsize, size_t cnt),
		ssize_t (*write)(void *restrict private, const void *restrict buf, size_t elmsize, size_t cnt),
		void (*close)(void *private),
		void (*destroy)(void *private),
		const char *restrict name)
{
	stream *s;
	struct cbstream *cb;

	if ((write && read) || (!write && !read))
		return NULL;

	s = create_stream(name);
	if (s == NULL)
		return NULL;
	cb = malloc(sizeof(struct cbstream));
	if (cb == NULL) {
		destroy_stream(s);
		mnstr_set_open_error(name, errno, NULL);
		return NULL;
	}
	*cb = (struct cbstream) {
		.private = private,
		.destroy = destroy,
		.read = read,
		.write = write,
		.close = close,
	};
	s->readonly = (read != NULL);
	s->stream_data.p = cb;
	s->read = read ? cb_read : NULL;
	s->write = write ? cb_write : NULL;
	s->destroy = cb_destroy;
	s->close = cb_close;
	return s;
}
