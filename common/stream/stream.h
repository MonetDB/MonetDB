/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _STREAM_H_
#define _STREAM_H_

/*
 * File: stream.h
 * Auteur: Niels J. Nes
 * Date: 09-01-2001
 *
 * Version 0.1: start
 *
 * This is the general interface to input/output. Each stream will
 * contains some stream info (for now only byteorder). This is
 * required for proper conversion on different byte order platforms.
 */

#include <unistd.h>
#include <ctype.h>
#include <stdio.h>

#include <stdlib.h>
#include <signal.h>
#include <limits.h>

/* avoid using "#ifdef WIN32" so that this file does not need our config.h */
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
# ifndef LIBSTREAM
#  define stream_export extern __declspec(dllimport)
# else
#  define stream_export extern __declspec(dllexport)
# endif
# ifndef SIZEOF_LNG
typedef __int64 lng;
# endif
#else
# define stream_export extern
# ifndef SIZEOF_LNG
typedef long long lng;
# endif
#endif
#ifndef HAVE_HGE
# ifdef HAVE___INT128
#  define HAVE_HGE 1
typedef __int128 hge;
# else
#  ifdef HAVE___INT128_T
#   define HAVE_HGE 1
typedef __int128_t hge;
#  endif
# endif
#endif


/* Defines to help the compiler check printf-style format arguments.
 * These defines are also in our config.h, but we repeat them here so
 * that we don't need that for this file.*/
#if !defined(__GNUC__) || __GNUC__ < 2 || (__GNUC__ == 2 && __GNUC_MINOR__ < 5)
/* This feature is available in gcc versions 2.5 and later.  */
# ifndef __attribute__
#  define __attribute__(Spec) /* empty */
# endif
#else
/* The __-protected variants of `format' and `printf' attributes are
 * accepted by gcc versions 2.6.4 (effectively 2.7) and later.  */
# if !defined(__format__) && (__GNUC__ < 2 || (__GNUC__ == 2 && __GNUC_MINOR__ < 7))
#  define __format__ format
#  define __printf__ printf
# endif
#endif
#if !defined(_MSC_VER) && !defined(_In_z_)
# define _In_z_
# define _Printf_format_string_
#endif

#define EOT 4

#define ST_ASCII  0
#define ST_BIN 1

#define ST_READ  0
#define ST_WRITE 1

typedef struct stream stream;

/* some os specific initialization */
stream_export int mnstr_init(void);

/* all mnstr_readX/mnstr_writeX return
 *  0 on error
 * !0 on success
 */
stream_export int mnstr_readBte(stream *s, signed char *val);
stream_export int mnstr_writeBte(stream *s, signed char val);
stream_export int mnstr_readSht(stream *s, short *val);
stream_export int mnstr_writeSht(stream *s, short val);
stream_export int mnstr_readInt(stream *s, int *val);
stream_export int mnstr_writeInt(stream *s, int val);
stream_export int mnstr_readLng(stream *s, lng *val);
stream_export int mnstr_writeLng(stream *s, lng val);
#ifdef HAVE_HGE
stream_export int mnstr_readHge(stream *s, hge *val);
stream_export int mnstr_writeHge(stream *s, hge val);
#endif

stream_export int mnstr_readBteArray(stream *s, signed char *val, size_t cnt);
stream_export int mnstr_writeBteArray(stream *s, const signed char *val, size_t cnt);
stream_export int mnstr_readShtArray(stream *s, short *val, size_t cnt);
stream_export int mnstr_writeShtArray(stream *s, const short *val, size_t cnt);
stream_export int mnstr_readIntArray(stream *s, int *val, size_t cnt);
stream_export int mnstr_writeIntArray(stream *s, const int *val, size_t cnt);
stream_export int mnstr_readLngArray(stream *s, lng *val, size_t cnt);
stream_export int mnstr_writeLngArray(stream *s, const lng *val, size_t cnt);
#ifdef HAVE_HGE
stream_export int mnstr_readHgeArray(stream *s, hge *val, size_t cnt);
stream_export int mnstr_writeHgeArray(stream *s, const hge *val, size_t cnt);
#endif
stream_export int mnstr_printf(stream *s, _In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 2, 3)));
stream_export ssize_t mnstr_read(stream *s, void *buf, size_t elmsize, size_t cnt);
stream_export ssize_t mnstr_readline(stream *s, void *buf, size_t maxcnt);
stream_export ssize_t mnstr_write(stream *s, const void *buf, size_t elmsize, size_t cnt);
stream_export void mnstr_close(stream *s);
stream_export void mnstr_destroy(stream *s);
stream_export char *mnstr_error(stream *s);
stream_export int mnstr_flush(stream *s);
stream_export int mnstr_fsync(stream *s);
stream_export int mnstr_fgetpos(stream *s, lng *p);
stream_export int mnstr_fsetpos(stream *s, lng p);
stream_export char *mnstr_name(stream *s);
stream_export int mnstr_errnr(stream *s);
stream_export void mnstr_clearerr(stream *s);
stream_export int mnstr_type(stream *s);
stream_export int mnstr_byteorder(stream *s);
stream_export void mnstr_set_byteorder(stream *s, char bigendian);
stream_export stream *mnstr_rstream(stream *s);
stream_export stream *mnstr_wstream(stream *s);
stream_export void mnstr_settimeout(stream *s, unsigned int ms, int (*func)(void));

stream_export stream *open_rstream(const char *filename);
stream_export stream *open_wstream(const char *filename);

/* append to stream */
stream_export stream *append_wstream(const char *filename);

/* open in ascii stream in read mode */
stream_export stream *open_rastream(const char *filename);

/* open in ascii stream in write mode*/
stream_export stream *open_wastream(const char *filename);

/* append to ascii stream */
stream_export stream *append_wastream(const char *filename);

stream_export void close_stream(stream *s);

stream_export stream *open_urlstream(const char *url);

stream_export stream *udp_rastream(const char *hostname, int port, const char *name);
stream_export stream *udp_wastream(const char *hostname, int port, const char *name);

stream_export stream *file_rstream(FILE *fp, const char *name);
stream_export stream *file_wstream(FILE *fp, const char *name);
stream_export stream *file_rastream(FILE *fp, const char *name);
stream_export stream *file_wastream(FILE *fp, const char *name);

stream_export FILE *getFile(stream *s);
stream_export size_t getFileSize(stream *s);

stream_export stream *iconv_rstream(stream *ss, const char *charset, const char *name);
stream_export stream *iconv_wstream(stream *ss, const char *charset, const char *name);

typedef struct buffer {
	char *buf;
	size_t pos;
	size_t len;
} buffer;

stream_export void buffer_init(buffer *b, char *buf, size_t size);
stream_export buffer *buffer_create(size_t size);
stream_export char *buffer_get_buf(buffer *b);
stream_export void buffer_destroy(buffer *b);

stream_export stream *buffer_rastream(buffer *b, const char *name);
stream_export stream *buffer_wastream(buffer *b, const char *name);
stream_export buffer *mnstr_get_buffer(stream *s);

/* note, the size is fixed to 8K, you cannot simply change it to any
 * value */
#define BLOCK (8 * 1024 - 2)

/* Block stream is a stream which sends data in blocks of a known size
 * (BLOCK size or dynamically changed using CHANGE_BLOCK_SIZE msg).
 *
 * A block is written once more than BLOCK size data has been written
 * using the write commands or when the flush command is sent.
 *
 * All full blocks together with a single not full block form a major
 * block. Major blocks can be used to synchronize the communication.
 * Example server sends some reply, ie a major block consisting of
 * various minor blocks. The header of the major block can contain
 * special info which the client can interpret.
 *
 * Each read attempt tries to return the number of bytes. Once a lower
 * number of bytes can be read the end of the major block is
 * found. The next read will then start with a new major block.
 */
stream_export stream *wbstream(stream *s, size_t buflen);
stream_export stream *block_stream(stream *s);
stream_export int isa_block_stream(stream *s);
/* read block of data including the end of block marker */
stream_export ssize_t mnstr_read_block(stream *s, void *buf, size_t elmsize, size_t cnt);

typedef struct bstream {
	stream *s;
	char *buf;
	size_t size;		/* size of buf */
	size_t pos;		/* the data cursor (ie read uptil pos) */
	size_t len;		/* len of the data (could < size but usually == size) */
	int eof;
	int mode;		/* 0 line mode else size for block mode */
} bstream;

stream_export bstream *bstream_create(stream *rs, size_t chunk_size);
stream_export void bstream_destroy(bstream *s);
stream_export ssize_t bstream_read(bstream *s, size_t size);
stream_export ssize_t bstream_next(bstream *s);

typedef enum mnstr_errors {
	MNSTR_NO__ERROR = 0,
	MNSTR_OPEN_ERROR,
	MNSTR_READ_ERROR,
	MNSTR_WRITE_ERROR,
	MNSTR_TIMEOUT
} mnstr_errors;

/* Callback stream is a read-only stream where the read function is
 * provided by the caller.  close and destroy are also provided.  The
 * private pointer is passed on to the callback functions when they
 * are invoked. */
stream_export stream *callback_stream(
	void *private,
	ssize_t (*read) (void *private, void *buf, size_t elmsize, size_t cnt),
	void (*close) (void *private),
	void (*destroy) (void *private),
	const char *name);

#endif /*_STREAM_H_*/
