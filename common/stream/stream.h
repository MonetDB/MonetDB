/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#else
# define stream_export extern
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
#  define __attribute__(Spec)	/* empty */
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

/* fwf gets turned into a csv with these parameters */
#define STREAM_FWF_FIELD_SEP '|'
#define STREAM_FWF_ESCAPE '\\'
#define STREAM_FWF_RECORD_SEP '\n'
#define STREAM_FWF_FILLER ' '

typedef struct stream stream;

/* some os specific initialization */
stream_export int mnstr_init(void);

/* all mnstr_readX/mnstr_writeX return
 *  0 on error
 * !0 on success
 */
stream_export int mnstr_readBte(stream *restrict s, int8_t *restrict val);
stream_export int mnstr_readChr(stream *restrict s, char *restrict val);
stream_export int mnstr_writeChr(stream *s, char val);

stream_export int mnstr_writeBte(stream *s, int8_t val);
stream_export int mnstr_readSht(stream *restrict s, int16_t *restrict val);
stream_export int mnstr_writeSht(stream *s, int16_t val);
stream_export int mnstr_readInt(stream *restrict s, int *restrict val);
stream_export int mnstr_writeInt(stream *s, int val);
stream_export int mnstr_readLng(stream *restrict s, int64_t *restrict val);
stream_export int mnstr_writeLng(stream *s, int64_t val);


stream_export int mnstr_writeFlt(stream *s, float val);
stream_export int mnstr_writeDbl(stream *s, double val);

#ifdef HAVE_HGE
stream_export int mnstr_readHge(stream *restrict s, hge *restrict val);
stream_export int mnstr_writeHge(stream *s, hge val);
#endif

stream_export int mnstr_readBteArray(stream *restrict s, int8_t *restrict val, size_t cnt);
stream_export int mnstr_writeBteArray(stream *restrict s, const int8_t *restrict val, size_t cnt);
stream_export int mnstr_writeStr(stream *restrict s, const char *restrict val);
stream_export int mnstr_readStr(stream *restrict s, char *restrict val);

stream_export int mnstr_readShtArray(stream *restrict s, int16_t *restrict val, size_t cnt);
stream_export int mnstr_writeShtArray(stream *restrict s, const int16_t *restrict val, size_t cnt);
stream_export int mnstr_readIntArray(stream *restrict s, int *restrict val, size_t cnt);
stream_export int mnstr_writeIntArray(stream *restrict s, const int *restrict val, size_t cnt);
stream_export int mnstr_readLngArray(stream *restrict s, int64_t *restrict val, size_t cnt);
stream_export int mnstr_writeLngArray(stream *restrict s, const int64_t *restrict val, size_t cnt);
#ifdef HAVE_HGE
stream_export int mnstr_readHgeArray(stream *restrict s, hge *restrict val, size_t cnt);
stream_export int mnstr_writeHgeArray(stream *restrict s, const hge *restrict val, size_t cnt);
#endif
stream_export int mnstr_printf(stream *restrict s, _In_z_ _Printf_format_string_ const char *restrict format, ...)
	__attribute__((__format__(__printf__, 2, 3)));
stream_export ssize_t mnstr_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt);
stream_export ssize_t mnstr_readline(stream *restrict s, void *restrict buf, size_t maxcnt);
stream_export ssize_t mnstr_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt);
stream_export void mnstr_close(stream *s);
stream_export void mnstr_destroy(stream *s);
stream_export char *mnstr_error(stream *s);
stream_export int mnstr_flush(stream *s);
stream_export int mnstr_fsync(stream *s);
stream_export int mnstr_fgetpos(stream *restrict s, fpos_t *restrict p);
stream_export int mnstr_fsetpos(stream *restrict s, fpos_t *restrict p);
stream_export char *mnstr_name(stream *s);
stream_export int mnstr_errnr(stream *s);
stream_export void mnstr_clearerr(stream *s);
stream_export int mnstr_type(stream *s);
stream_export int mnstr_byteorder(stream *s);
stream_export void mnstr_set_byteorder(stream *s, char bigendian);
stream_export void mnstr_settimeout(stream *s, unsigned int ms, int (*func)(void));
stream_export int mnstr_isalive(stream *s);

stream_export stream *open_rstream(const char *filename);
stream_export stream *open_wstream(const char *filename);

/* open in ascii stream in read mode */
stream_export stream *open_rastream(const char *filename);

/* open in ascii stream in write mode*/
stream_export stream *open_wastream(const char *filename);

stream_export void close_stream(stream *s);

stream_export stream *open_urlstream(const char *url);

stream_export stream *file_rstream(FILE *restrict fp, const char *restrict name);
stream_export stream *file_wstream(FILE *restrict fp, const char *restrict name);
stream_export stream *file_rastream(FILE *restrict fp, const char *restrict name);
stream_export stream *file_wastream(FILE *restrict fp, const char *restrict name);

stream_export FILE *getFile(stream *s);
stream_export int getFileNo(stream *s);	/* fileno(getFile(s)) */
stream_export size_t getFileSize(stream *s);

stream_export stream *iconv_rstream(stream *restrict ss, const char *restrict charset, const char *restrict name);
stream_export stream *iconv_wstream(stream *restrict ss, const char *restrict charset, const char *restrict name);

typedef struct buffer {
	char *buf;
	size_t pos;
	size_t len;
} buffer;

stream_export void buffer_init(buffer *restrict b, char *restrict buf, size_t size);
stream_export buffer *buffer_create(size_t size);
stream_export char *buffer_get_buf(buffer *b);
stream_export void buffer_destroy(buffer *b);

stream_export stream *buffer_rastream(buffer *restrict b, const char *restrict name);
stream_export stream *buffer_wastream(buffer *restrict b, const char *restrict name);
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
stream_export stream *block_stream(stream *s);
stream_export int isa_block_stream(stream *s);
stream_export int isa_fixed_block_stream(stream *s);
stream_export stream *bs_stream(stream *s);
stream_export stream *bs_stealstream(stream *s);


typedef enum {
	PROTOCOL_AUTO = 0,
	PROTOCOL_9 = 1,
	PROTOCOL_10 = 2
} protocol_version;

typedef enum {
	COMPRESSION_NONE = 0,
	COMPRESSION_SNAPPY = 1,
	COMPRESSION_LZ4 = 2,
	COMPRESSION_AUTO = 255
} compression_method;

typedef enum {
	COLUMN_COMPRESSION_NONE = 0,
	COLUMN_COMPRESSION_AUTO = 255
} column_compression;

stream_export stream *block_stream2(stream *s, size_t bufsiz, compression_method comp, column_compression colcomp);
stream_export void *bs2_stealbuf(stream *ss);
stream_export int bs2_resizebuf(stream *ss, size_t bufsiz);
stream_export void bs2_resetbuf(stream *ss);
stream_export buffer bs2_buffer(stream *s);
stream_export column_compression bs2_colcomp(stream *ss);
stream_export void bs2_setpos(stream *ss, size_t pos);


/* read block of data including the end of block marker */
stream_export ssize_t mnstr_read_block(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt);

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
	void *restrict priv,
	ssize_t (*read)(void *restrict priv, void *restrict buf, size_t elmsize, size_t cnt),
	void (*close)(void *priv),
	void (*destroy)(void *priv),
	const char *restrict name);

stream_export stream *stream_blackhole_create(void);

stream_export stream *stream_fwf_create(stream *restrict s, size_t num_fields, size_t *restrict widths, char filler);

#endif /*_STREAM_H_*/
