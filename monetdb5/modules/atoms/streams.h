/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _STREAMS_H_
#define _STREAMS_H_

#include "mal.h"
#include <stdio.h>
#include "stream_socket.h"

typedef ptr Stream;
typedef ptr Bstream;

mal_export int mnstr_write_string(Stream *S, str data);
mal_export int mnstr_writeInt_wrap(Stream *S, int *data);
mal_export int mnstr_readInt_wrap(int *data, Stream *S);
mal_export int mnstr_read_string(str *res, Stream *S);
mal_export int mnstr_flush_stream(Stream *S);
mal_export int mnstr_close_stream(Stream *S);
mal_export int open_block_stream(Stream *S, Stream *is);
mal_export int bstream_create_wrap(Bstream *BS, Stream *S, int *bufsize);
mal_export int bstream_destroy_wrap(Bstream *BS);
mal_export int bstream_read_wrap(int *res, Bstream *BS, int *size);
mal_export str mnstr_write_stringwrap(void *ret, Stream *S, str *data);
mal_export str mnstr_writeIntwrap(void *ret, Stream *S, int *data);
mal_export str mnstr_readIntwrap(int *ret, Stream *S);
mal_export str mnstr_read_stringwrap(str *res, Stream *s);
mal_export str mnstr_flush_streamwrap(void *ret, Stream *s);
mal_export str mnstr_close_streamwrap(void *ret, Stream *s);
mal_export str open_block_streamwrap(Stream *S, Stream *is);
mal_export str bstream_create_wrapwrap(Bstream *Bs, Stream *S, int *bufsize);
mal_export str bstream_destroy_wrapwrap(void *ret, Bstream *BS);
mal_export str bstream_read_wrapwrap(int *res, Bstream *BS, int *size);

mal_export str mnstr_open_rstreamwrap(Stream *S, str *filename);
mal_export str mnstr_open_wstreamwrap(Stream *S, str *filename);
mal_export str mnstr_open_rastreamwrap(Stream *S, str *filename);
mal_export str mnstr_open_wastreamwrap(Stream *S, str *filename);

mal_export str mnstr_stream_rstreamwrap(Stream *sout, Stream *sin);
mal_export str mnstr_stream_wstreamwrap(Stream *sout, Stream *sin);

mal_export str mnstr_socket_rstreamwrap(Stream *S, int *socket, str *name);
mal_export str mnstr_socket_wstreamwrap(Stream *S, int *socket, str *name);
mal_export str mnstr_socket_rastreamwrap(Stream *S, int *socket, str *name);
mal_export str mnstr_socket_wastreamwrap(Stream *S, int *socket, str *name);
#endif /*_STREAMS_H_*/
