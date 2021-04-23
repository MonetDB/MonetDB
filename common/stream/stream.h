/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#ifndef __GNUC__
/* This feature is available in gcc versions 2.5 and later.  */
# ifndef __attribute__
#  define __attribute__(Spec)	/* empty */
# endif
#endif
#if !defined(_MSC_VER) && !defined(_In_z_)
# define _In_z_
# define _Printf_format_string_
#endif

#define EOT 4

/* fwf gets turned into a csv with these parameters */
#define STREAM_FWF_FIELD_SEP '|'
#define STREAM_FWF_ESCAPE '\\'
#define STREAM_FWF_RECORD_SEP '\n'
#define STREAM_FWF_FILLER ' '

typedef struct stream stream;

/* some os specific initialization */
stream_export int mnstr_init(void);


typedef enum mnstr_error_kind {
	MNSTR_NO__ERROR = 0,
	MNSTR_OPEN_ERROR,
	MNSTR_READ_ERROR,
	MNSTR_WRITE_ERROR,
	MNSTR_TIMEOUT,
	MNSTR_UNEXPECTED_EOF,
} mnstr_error_kind;

typedef enum mnstr_flush_level {
	MNSTR_FLUSH_DATA = 1, // write all data
	MNSTR_FLUSH_ALL = 2,  // write all data and also reset compression state
} mnstr_flush_level;

stream_export const char *mnstr_version(void);

stream_export char *mnstr_error(const stream *s);
stream_export const char* mnstr_peek_error(const stream *s);
stream_export mnstr_error_kind mnstr_errnr(const stream *s);
stream_export const char *mnstr_error_kind_name(mnstr_error_kind k);
stream_export void mnstr_clearerr(stream *s);

/* all mnstr_readX/mnstr_writeX return
 *  0 on error
 * !0 on success
 */
stream_export int mnstr_readBte(stream *restrict s, int8_t *restrict val); // unused
stream_export int mnstr_readChr(stream *restrict s, char *restrict val); // used once in gdk_logger.c
stream_export int mnstr_writeChr(stream *s, char val); // used once in gdk_logger and mclient.c
stream_export int mnstr_writeBte(stream *s, int8_t val); // used in sql_result.c/mapi10

stream_export int mnstr_readSht(stream *restrict s, int16_t *restrict val); // unused
stream_export int mnstr_writeSht(stream *s, int16_t val); // used in sql_result.c/mapi10
stream_export int mnstr_readInt(stream *restrict s, int *restrict val); // used in gdk
stream_export int mnstr_writeInt(stream *s, int val); // used in gdk
stream_export int mnstr_readLng(stream *restrict s, int64_t *restrict val); // used in gdk_logger.c
stream_export int mnstr_writeLng(stream *s, int64_t val); // used in gdk_logger.c, sql_result.c/mapi10


stream_export int mnstr_writeFlt(stream *s, float val); // sql_result.c/mapi10
stream_export int mnstr_writeDbl(stream *s, double val); // sql_result.c/mapi10

#ifdef HAVE_HGE
stream_export int mnstr_readHge(stream *restrict s, hge *restrict val); // unused
stream_export int mnstr_writeHge(stream *s, hge val); // sql_result.c/mapi10
#endif

stream_export int mnstr_readBteArray(stream *restrict s, int8_t *restrict val, size_t cnt); // unused
stream_export int mnstr_writeBteArray(stream *restrict s, const int8_t *restrict val, size_t cnt);
stream_export int mnstr_writeStr(stream *restrict s, const char *restrict val); // sql_result.c/mapi10
stream_export int mnstr_readStr(stream *restrict s, char *restrict val); // unused

stream_export int mnstr_readShtArray(stream *restrict s, int16_t *restrict val, size_t cnt); // unused
stream_export int mnstr_writeShtArray(stream *restrict s, const int16_t *restrict val, size_t cnt); //unused
stream_export int mnstr_readIntArray(stream *restrict s, int *restrict val, size_t cnt); // used once in geom.c
stream_export int mnstr_writeIntArray(stream *restrict s, const int *restrict val, size_t cnt); // used once in geom.c
stream_export int mnstr_readLngArray(stream *restrict s, int64_t *restrict val, size_t cnt); // unused
stream_export int mnstr_writeLngArray(stream *restrict s, const int64_t *restrict val, size_t cnt); // unused
#ifdef HAVE_HGE
stream_export int mnstr_readHgeArray(stream *restrict s, hge *restrict val, size_t cnt); // unused
stream_export int mnstr_writeHgeArray(stream *restrict s, const hge *restrict val, size_t cnt); // unused
#endif
stream_export int mnstr_printf(stream *restrict s, _In_z_ _Printf_format_string_ const char *restrict format, ...) // USED all over
	__attribute__((__format__(__printf__, 2, 3)));
stream_export ssize_t mnstr_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt); // USED all over
stream_export ssize_t mnstr_readline(stream *restrict s, void *restrict buf, size_t maxcnt); // used in mclient, sql.c/mvc_export_table_wrap, bat_logger.c
stream_export ssize_t mnstr_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt); // USED all over
stream_export void mnstr_close(stream *s);
stream_export void mnstr_destroy(stream *s);
stream_export int mnstr_flush(stream *s, mnstr_flush_level flush_level); // used all over
stream_export int mnstr_fsync(stream *s); // used in gdk_logger.c, wlc.c and store.c
stream_export int mnstr_fgetpos(stream *restrict s, fpos_t *restrict p); // unused
stream_export int mnstr_fsetpos(stream *restrict s, fpos_t *restrict p); // unused
stream_export char *mnstr_name(const stream *s); // used when wrapping in mclient.c
stream_export bool mnstr_isbinary(const stream *s); // unused
stream_export bool mnstr_get_swapbytes(const stream *s); // sql_result.c/mapi10
stream_export void mnstr_set_bigendian(stream *s, bool bigendian); // used in mapi.c and mal_session.c
stream_export void mnstr_settimeout(stream *s, unsigned int ms, bool (*func)(void *), void *data); // used in mapi.c and mal_session.c
stream_export int mnstr_isalive(const stream *s); // used once in mal_interpreter.c

stream_export stream *open_rstream(const char *filename); // used in mclient.c, gdk_logger.c, store.c, snapshot.c
stream_export stream *open_wstream(const char *filename); // used in gdk_logger.c and store.c

/* open in ascii stream in read mode */
stream_export stream *open_rastream(const char *filename); // used 13 times

/* open in ascii stream in write mode*/
stream_export stream *open_wastream(const char *filename); // used in mclient.c, mapi.c, mal_io.c, wlc.c, sql.c, wlr.c

stream_export void close_stream(stream *s);

stream_export stream *open_urlstream(const char *url); // mclient.c, future copy from remote

stream_export stream *file_rstream(FILE *restrict fp, bool binary, const char *restrict name); // unused
stream_export stream *file_wstream(FILE *restrict fp, bool binary, const char *restrict name); // unused
stream_export stream *stdin_rastream(void);
stream_export stream *stdout_wastream(void);
stream_export stream *stderr_wastream(void);

stream_export stream *xz_stream(stream *inner, int preset);
stream_export stream *gz_stream(stream *inner, int preset);
stream_export stream *bz2_stream(stream *inner, int preset);
stream_export stream *lz4_stream(stream *inner, int preset);
stream_export stream *compressed_stream(stream *inner, int preset);

stream_export FILE *getFile(stream *s); // gdk_logger.c progress messages
stream_export int getFileNo(stream *s);	/* fileno(getFile(s)) */ // mclient.c, gdk_logger.c progress messages
stream_export size_t getFileSize(stream *s); // mal_import.c, sql_scenario.c, wlr.c, store.c, bat_logger.c

stream_export stream *iconv_rstream(stream *restrict ss, const char *restrict charset, const char *restrict name); // mclient.c stdin
stream_export stream *iconv_wstream(stream *restrict ss, const char *restrict charset, const char *restrict name); // mclient.c stdout

typedef struct buffer {
	char *buf;
	size_t pos;
	size_t len;
} buffer;

stream_export void buffer_init(buffer *restrict b, char *restrict buf, size_t size); // used in many places
stream_export buffer *buffer_create(size_t size); // used in sql_gencode.c and store.c/snapshot
stream_export char *buffer_get_buf(buffer *b);
stream_export void buffer_destroy(buffer *b);

stream_export stream *buffer_rastream(buffer *restrict b, const char *restrict name); // used in many places
stream_export stream *buffer_wastream(buffer *restrict b, const char *restrict name); // sql_gencode.c
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
stream_export stream *block_stream(stream *s); // mapi.c, mal_mapi.c, client.c, merovingian
stream_export bool isa_block_stream(const stream *s); // mapi.c, mal_client.c, remote.c, sql_scenario.c/sqlReader, sql_scan.c
stream_export stream *bs_stream(stream *s); // unused
stream_export void set_prompting(stream *block_stream, const char *prompt, stream *prompt_stream);


typedef enum {
	PROTOCOL_AUTO = 0, // unused
	PROTOCOL_9 = 1, // mal_mapi.c, mal_client.c;
	PROTOCOL_COLUMNAR = 3 // sql_result.c
} protocol_version;

typedef enum {
	COMPRESSION_NONE = 0, // mal_mapi.c
	COMPRESSION_SNAPPY = 1, // mcrypt.c, mal_mapi.c
	COMPRESSION_LZ4 = 2, // same
	COMPRESSION_AUTO = 255 // never used
} compression_method;

stream_export stream *block_stream2(stream *s, size_t bufsiz, compression_method comp); // mal_mapi.c
stream_export int bs2_resizebuf(stream *ss, size_t bufsiz); // sql_result.c
stream_export buffer bs2_buffer(stream *s); // sql_result.c
stream_export void bs2_setpos(stream *ss, size_t pos); // sql_result.c


/* read block of data including the end of block marker */
stream_export ssize_t mnstr_read_block(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt);
// used in mapi.c ,mal_mapi,c, merovingian/{client.c,controlrunner.c,control.c}

typedef struct bstream {
	stream *s;
	char *buf;
	size_t size;		/* size of buf */
	size_t pos;		/* the data cursor (ie read until pos) */
	size_t len;		/* len of the data (<= size) */
	size_t mode;		/* 0 line mode else size for block mode */
	bool eof;
} bstream;

stream_export bstream *bstream_create(stream *rs, size_t chunk_size); // used all over
stream_export void bstream_destroy(bstream *s); // all over
stream_export ssize_t bstream_read(bstream *s, size_t size); // tablet.c, tokenizer.c
stream_export ssize_t bstream_next(bstream *s); // all over

/* Callback stream is a stream where the read and write functions are
 * provided by the caller.  close and destroy are also provided.  The
 * private pointer is passed on to the callback functions when they
 * are invoked. */
stream_export stream *callback_stream(
	void *restrict priv,
	ssize_t (*read)(void *restrict priv, void *restrict buf, size_t elmsize, size_t cnt),
	ssize_t (*write)(void *restrict priv, const void *restrict buf, size_t elmsize, size_t cnt),
	void (*close)(void *priv),
	void (*destroy)(void *priv),
	const char *restrict name); // used in mclient.c, for readline

stream_export stream *stream_blackhole_create(void); // never used

stream_export stream *stream_fwf_create(stream *restrict s, size_t num_fields, size_t *restrict widths, char filler); // sql.c


stream_export stream *create_text_stream(stream *s);

#endif /*_STREAM_H_*/
