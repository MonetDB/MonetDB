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
#ifndef _STREAMS_H_
#define _STREAMS_H_

#include "monetdb_config.h"
#include <mal.h>
#include <stdio.h>
#include <stream_socket.h>

typedef ptr Stream;
typedef ptr Bstream;

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define streams_export extern __declspec(dllimport)
#else
#define streams_export extern __declspec(dllexport)
#endif
#else
#define streams_export extern
#endif

streams_export int mnstr_write_string(Stream *S, str data);
streams_export int mnstr_writeInt_wrap(Stream *S, int *data);
streams_export int mnstr_readInt_wrap(int *data, Stream *S);
streams_export int mnstr_read_string(str *res, Stream *S);
streams_export int mnstr_flush_stream(Stream *S);
streams_export int mnstr_close_stream(Stream *S);
streams_export int open_block_stream(Stream *S, Stream *is);
streams_export int bstream_create_wrap(Bstream *BS, Stream *S, int *bufsize);
streams_export int bstream_destroy_wrap(Bstream *BS);
streams_export int bstream_read_wrap(int *res, Bstream *BS, int *size);
streams_export str mnstr_write_stringwrap(int *ret, Stream *S, str *data);
streams_export str mnstr_writeIntwrap(int *ret, Stream *S, int *data);
streams_export str mnstr_readIntwrap(int *ret, Stream *S);
streams_export str mnstr_read_stringwrap(str *res, Stream *s);
streams_export str mnstr_flush_streamwrap(int *ret, Stream *s);
streams_export str mnstr_close_streamwrap(int *ret, Stream *s);
streams_export str open_block_streamwrap(Stream *S, Stream *is);
streams_export str bstream_create_wrapwrap(Bstream *Bs, Stream *S, int *bufsize);
streams_export str bstream_destroy_wrapwrap(int *ret, Bstream *BS);
streams_export str bstream_read_wrapwrap(int *res, Bstream *BS, int *size);
streams_export str mnstr_readIntwrap(int *ret, Stream *S);
streams_export str mnstr_read_stringwrap(str *res, Stream *s);

streams_export str mnstr_open_rstreamwrap(Stream *S, str *filename);
streams_export str mnstr_open_wstreamwrap(Stream *S, str *filename);
streams_export str mnstr_open_rastreamwrap(Stream *S, str *filename);
streams_export str mnstr_open_wastreamwrap(Stream *S, str *filename);

streams_export str mnstr_stream_rstreamwrap(Stream *sout, Stream *sin);
streams_export str mnstr_stream_wstreamwrap(Stream *sout, Stream *sin);

streams_export str mnstr_socket_rstreamwrap(Stream *S, int *socket, str *name);
streams_export str mnstr_socket_wstreamwrap(Stream *S, int *socket, str *name);
streams_export str mnstr_socket_rastreamwrap(Stream *S, int *socket, str *name);
streams_export str mnstr_socket_wastreamwrap(Stream *S, int *socket, str *name);
#endif /*_STREAMS_H_*/
