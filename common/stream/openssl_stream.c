/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

static ssize_t ostream_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt);
static ssize_t ostream_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt);
static void ostream_close(stream *s);
static int ostream_flush(stream *s, mnstr_flush_level flush_level);

stream *
openssl_rstream(const char *host_colon_port, BIO *bio)
{
	stream *s = openssl_wstream(host_colon_port, bio);
	if (s != NULL)
		s->readonly = true;
	return s;
}
stream *
openssl_wstream(const char *host_colon_port, BIO *bio)
{
	assert(bio);

	stream *s = create_stream(host_colon_port);
	if (s == NULL)
		return NULL;

	s->stream_data.p = bio;
	s->readonly = false;
	s->binary = true;
	s->read = ostream_read;
	s->write = ostream_write;
	s->close = ostream_close;
	s->flush = ostream_flush;

	return s;
}

static ssize_t
ostream_error(stream *s, mnstr_error_kind kind)
{
	unsigned long err = ERR_get_error();
	const char *msg = ERR_reason_error_string(err);
	mnstr_set_error(s, kind, "%s", msg);
	return -1;
}


ssize_t
ostream_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	BIO *bio = (BIO*)s->stream_data.p;

	char *start = (char*)buf;
	size_t size = elmsize * cnt;
	if (size == 0)
		return 0;

	// iterate in order to read a complete number of items
	size_t pos = 0;
	do {
		size_t nread;
		if (!BIO_read_ex(bio, start + pos, size - pos, &nread))
			return ostream_error(s, MNSTR_READ_ERROR);
		if (nread == 0) {
			s->eof = 0;
			break;
		}
		pos += nread;

		// adjust pos to the smallest multiple of elmsize.
		// example 1: size=4 pos=7 (-7)%4=1, newsize=8
		size_t delta = (-pos) % size;
		if (size - pos > delta)
			size = pos + delta;
	} while (pos < size);

	return (ssize_t) (pos / elmsize);
}

ssize_t
ostream_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	BIO *bio = (BIO*)s->stream_data.p;

	char *start = (char*)buf;
	size_t size = elmsize * cnt;
	size_t pos = 0;
	while (pos < size) {
		size_t nwritten;
		if (!BIO_write_ex(bio, start + pos, size - pos, &nwritten))
			return ostream_error(s, MNSTR_WRITE_ERROR);
		if (nwritten == 0 && !BIO_should_retry(bio))
			break;
		pos += nwritten;
	}

	return (ssize_t) (pos / elmsize);
}

void
ostream_close(stream *s)
{
	BIO *bio = (BIO*) s->stream_data.p;
	BIO_free(bio);
	s->stream_data.p = NULL;
}

int
ostream_flush(stream *s, mnstr_flush_level flush_level)
{
	(void)s;
	(void)flush_level;
	mnstr_set_error(s, MNSTR_WRITE_ERROR, "flush not implemented");
	return -1;
}

