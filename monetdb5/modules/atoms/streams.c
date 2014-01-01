/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
mnstr_stream_rstreamwrap(Stream *sout, Stream *sin)
{
	*(stream**)sout = mnstr_rstream(*(stream**)sin);
	return MAL_SUCCEED;
}

str
mnstr_stream_wstreamwrap(Stream *sout, Stream *sin)
{
	*(stream**)sout = mnstr_wstream(*(stream**)sin);
	return MAL_SUCCEED;
}

str
mnstr_socket_rstreamwrap(Stream *S, int *socket, str *name)
{
	stream *s;

	if ((s = socket_rstream(*socket, *name)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		throw(IO, "streams.open", "could not open socket: %s",
				strerror(errnr));
	} else {
		*(stream**)S = s;
	}

	return MAL_SUCCEED;
}
str
mnstr_socket_wstreamwrap(Stream *S, int *socket, str *name)
{
	stream *s;

	if ((s = socket_wstream(*socket, *name)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		throw(IO, "streams.open", "could not open socket: %s",
				strerror(errnr));
	} else {
		*(stream**)S = s;
	}

	return MAL_SUCCEED;
}
str
mnstr_socket_rastreamwrap(Stream *S, int *socket, str *name)
{
	stream *s;

	if ((s = socket_rastream(*socket, *name)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		throw(IO, "streams.open", "could not open socket: %s",
				strerror(errnr));
	} else {
		*(stream**)S = s;
	}

	return MAL_SUCCEED;
}
str
mnstr_socket_wastreamwrap(Stream *S, int *socket, str *name)
{
	stream *s;

	if ((s = socket_wastream(*socket, *name)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		throw(IO, "streams.open", "could not open socket: %s",
				strerror(errnr));
	} else {
		*(stream**)S = s;
	}

	return MAL_SUCCEED;
}

str
mnstr_write_stringwrap(int *ret, Stream *S, str *data)
{
	stream *s = *(stream **)S;
	(void)ret;

	if (mnstr_write(s, *data, 1, strlen(*data)) < 0)
		throw(IO, "streams.writeStr", "failed to write string");

	return MAL_SUCCEED;
}

str
mnstr_writeIntwrap(int *ret, Stream *S, int *data)
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

	if (!mnstr_readInt(s, ret))
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
	char *buf = GDKmalloc(size), *start = buf;

	while ((len = mnstr_read(s, start, 1, CHUNK)) > 0) {
		size += len;
		buf = GDKrealloc(buf, size);
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
mnstr_flush_streamwrap(int *ret, Stream *S)
{
	stream *s = *(stream **)S;
	(void)ret;

	if (mnstr_flush(s))
		throw(IO, "streams.flush", "failed to flush stream");

	return MAL_SUCCEED;
}

str
mnstr_close_streamwrap(int *ret, Stream *S)
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
bstream_destroy_wrapwrap(int *ret, Bstream *BS)
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
