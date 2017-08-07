/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 *  Niels Nes
 *  A simple interface to IO streams
 * All file IO is tunneled through the stream library, which guarantees
 * cross-platform capabilities.  Several protocols are provided, e.g. it
 * can be used to open 'non compressed, gzipped, bzip2ed' data files.  It
 * encapsulates the corresponding library managed in common/stream.
 */

#include "monetdb_config.h"
#include "streams.h"
#include "mal_exception.h"

str mnstr_open_rstreamwrap(Stream *S, str *filename)
{
	stream *s;

	if ((s = open_rstream(*filename)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		throw(IO, "streams.open", "could not open file '%s': %s",
				*filename, strerror(errnr));
	} else {
		*(stream**)S = s;
	}

	return MAL_SUCCEED;
}
str mnstr_open_wstreamwrap(Stream *S, str *filename)
{
	stream *s;

	if ((s = open_wstream(*filename)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		throw(IO, "streams.open", "could not open file '%s': %s",
				*filename, strerror(errnr));
	} else {
		*(stream**)S = s;
	}

	return MAL_SUCCEED;
}

str mnstr_open_rastreamwrap(Stream *S, str *filename)
{
	stream *s;

	if ((s = open_rastream(*filename)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		throw(IO, "streams.open", "could not open file '%s': %s",
				*filename, strerror(errnr));
	} else {
		*(stream**)S = s;
	}

	return MAL_SUCCEED;
}

str mnstr_open_wastreamwrap(Stream *S, str *filename)
{
	stream *s;

	if ((s = open_wastream(*filename)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		throw(IO, "streams.open", "could not open file '%s': %s",
				*filename, strerror(errnr));
	} else {
		*(stream**)S = s;
	}

	return MAL_SUCCEED;
}

str
mnstr_write_stringwrap(void *ret, Stream *S, str *data)
{
	stream *s = *(stream **)S;
	(void)ret;

	if (mnstr_write(s, *data, 1, strlen(*data)) < 0)
		throw(IO, "streams.writeStr", "failed to write string");

	return MAL_SUCCEED;
}

str
mnstr_writeIntwrap(void *ret, Stream *S, int *data)
{
	stream *s = *(stream **)S;
	(void)ret;

	if (!mnstr_writeInt(s, *data))
		throw(IO, "streams.writeInt", "failed to write int");

	return MAL_SUCCEED;
}

str
mnstr_readIntwrap(int *ret, Stream *S)
{
	stream *s = *(stream **)S;

	if (mnstr_readInt(s, ret) != 1)
		throw(IO, "streams.readInt", "failed to read int");

	return MAL_SUCCEED;
}

#define CHUNK (64 * 1024)
str
mnstr_read_stringwrap(str *res, Stream *S)
{
	stream *s = *(stream **)S;
	ssize_t len = 0;
	size_t size = CHUNK + 1;
	char *buf = GDKmalloc(size), *start = buf, *tmp;

	if( buf == NULL)
		throw(MAL,"mnstr_read_stringwrap", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	while ((len = mnstr_read(s, start, 1, CHUNK)) > 0) {
		size += len;
		tmp = GDKrealloc(buf, size);
		if (tmp == NULL) {
			GDKfree(buf);
			throw(MAL,"mnstr_read_stringwrap", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		buf = tmp;
		start = buf + size - CHUNK - 1;

		*start = '\0';
	}
	if (len < 0)
		throw(IO, "streams.readStr", "failed to read string");
	start += len;
	*start = '\0';
	*res = buf;

	return MAL_SUCCEED;
}

str
mnstr_flush_streamwrap(void *ret, Stream *S)
{
	stream *s = *(stream **)S;
	(void)ret;

	if (mnstr_flush(s))
		throw(IO, "streams.flush", "failed to flush stream");

	return MAL_SUCCEED;
}

str
mnstr_close_streamwrap(void *ret, Stream *S)
{
	(void)ret;

	close_stream(*(stream **)S);

	return MAL_SUCCEED;
}

str
open_block_streamwrap(Stream *S, Stream *is)
{
	if ((*(stream **)S = block_stream(*(stream **)is)) == NULL)
		throw(IO, "bstreams.open", "failed to open block stream");

	return MAL_SUCCEED;
}

str
bstream_create_wrapwrap(Bstream *Bs, Stream *S, int *bufsize)
{
	if ((*(bstream **)Bs = bstream_create(*(stream **)S, (size_t)*bufsize)) == NULL)
		throw(IO, "bstreams.create", "failed to create block stream");

	return MAL_SUCCEED;
}

str
bstream_destroy_wrapwrap(void *ret, Bstream *BS)
{
	(void)ret;

	bstream_destroy(*(bstream **)BS);

	return MAL_SUCCEED;
}

str
bstream_read_wrapwrap(int *res, Bstream *BS, int *size)
{
	*res = (int)bstream_read(*(bstream **)BS, (size_t)*size);

	return MAL_SUCCEED;
}
