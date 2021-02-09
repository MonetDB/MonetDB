/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

// All this used to be at the top of stream.c. Much of it is probably
// only used in a single file, we'll prune it down later on.


/* #define STREAM_DEBUG 1  */
/* #define BSTREAM_DEBUG 1 */

#include "stream_socket.h"
#include "matomic.h"

#include <string.h>
#include <stddef.h>

#ifdef HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
# include <sys/stat.h>
#endif
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#ifdef HAVE_NETDB_H
# include <netinet/in_systm.h>
# include <netinet/in.h>
# include <netinet/ip.h>
# include <netinet/tcp.h>
# include <netdb.h>
#endif
#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#ifdef NATIVE_WIN32
#include <io.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_LIBZ
#include <zlib.h>
#endif
#ifdef HAVE_LIBBZ2
#include <bzlib.h>
#endif
#ifdef HAVE_LIBLZMA
#include <lzma.h>
#endif
#ifdef HAVE_SNAPPY
#include <snappy-c.h>
#endif
#ifdef HAVE_LIBLZ4
#include <lz4.h>
#include <lz4frame.h>
#endif

#ifdef HAVE_ICONV
#include <iconv.h>
#ifdef HAVE_NL_LANGINFO
#include <langinfo.h>
#endif
#endif

#ifndef SHUT_RD
#define SHUT_RD		0
#define SHUT_WR		1
#define SHUT_RDWR	2
#endif

#ifndef EWOULDBLOCK
#define EWOULDBLOCK	EAGAIN
#endif

#ifndef EINTR
#define EINTR		EAGAIN
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET	(-1)
#endif

#ifdef NATIVE_WIN32
#define pclose _pclose
#define fileno(fd) _fileno(fd)
#endif

#define UTF8BOM		"\xEF\xBB\xBF"	/* UTF-8 encoding of Unicode BOM */
#define UTF8BOMLENGTH	3		/* length of above */

#ifdef _MSC_VER
/* use intrinsic functions on Windows */
#define short_int_SWAP(s)	((int16_t) _byteswap_ushort((uint16_t) (s)))
/* on Windows, long is the same size as int */
#define normal_int_SWAP(i)	((int) _byteswap_ulong((unsigned long) (i)))
#define long_int_SWAP(l)	((int64_t) _byteswap_uint64((unsigned __int64) (l)))
#else
#define short_int_SWAP(s)				\
	((int16_t) (((0x00ff & (uint16_t) (s)) << 8) |	\
		  ((0xff00 & (uint16_t) (s)) >> 8)))

#define normal_int_SWAP(i)						\
	((int) (((((unsigned) 0xff <<  0) & (unsigned) (i)) << 24) |	\
		((((unsigned) 0xff <<  8) & (unsigned) (i)) <<  8) |	\
		((((unsigned) 0xff << 16) & (unsigned) (i)) >>  8) |	\
		((((unsigned) 0xff << 24) & (unsigned) (i)) >> 24)))

#define long_int_SWAP(l)						\
	((int64_t) (((((uint64_t) 0xff <<  0) & (uint64_t) (l)) << 56) | \
		((((uint64_t) 0xff <<  8) & (uint64_t) (l)) << 40) |	\
		((((uint64_t) 0xff << 16) & (uint64_t) (l)) << 24) |	\
		((((uint64_t) 0xff << 24) & (uint64_t) (l)) <<  8) |	\
		((((uint64_t) 0xff << 32) & (uint64_t) (l)) >>  8) |	\
		((((uint64_t) 0xff << 40) & (uint64_t) (l)) >> 24) |	\
		((((uint64_t) 0xff << 48) & (uint64_t) (l)) >> 40) |	\
		((((uint64_t) 0xff << 56) & (uint64_t) (l)) >> 56)))
#endif

#ifdef HAVE_HGE
#define huge_int_SWAP(h)					\
	((hge) (((((uhge) 0xff <<   0) & (uhge) (h)) << 120) |	\
		((((uhge) 0xff <<   8) & (uhge) (h)) << 104) |	\
		((((uhge) 0xff <<  16) & (uhge) (h)) <<  88) |	\
		((((uhge) 0xff <<  24) & (uhge) (h)) <<  72) |	\
		((((uhge) 0xff <<  32) & (uhge) (h)) <<  56) |	\
		((((uhge) 0xff <<  40) & (uhge) (h)) <<  40) |	\
		((((uhge) 0xff <<  48) & (uhge) (h)) <<  24) |	\
		((((uhge) 0xff <<  56) & (uhge) (h)) <<   8) |	\
		((((uhge) 0xff <<  64) & (uhge) (h)) >>   8) |	\
		((((uhge) 0xff <<  72) & (uhge) (h)) >>  24) |	\
		((((uhge) 0xff <<  80) & (uhge) (h)) >>  40) |	\
		((((uhge) 0xff <<  88) & (uhge) (h)) >>  56) |	\
		((((uhge) 0xff <<  96) & (uhge) (h)) >>  72) |	\
		((((uhge) 0xff << 104) & (uhge) (h)) >>  88) |	\
		((((uhge) 0xff << 112) & (uhge) (h)) >> 104) |	\
		((((uhge) 0xff << 120) & (uhge) (h)) >> 120)))
#endif


struct stream {
	char *name;		/* name of the stream */
	struct stream *inner; /* if this stream is a wrapper around another one */
	bool swapbytes;		/* whether to swap bytes */
	bool readonly;		/* only reading or only writing */
	bool isutf8;		/* known to be UTF-8 due to BOM */
	bool binary;		/* text/binary */
	unsigned int timeout;	/* timeout in ms */
	bool (*timeout_func)(void *); /* callback function: NULL/true -> return */
	void *timeout_data;	/* data for the above */
	union {
		void *p;
		int i;
		SOCKET s;
	} stream_data;
	ssize_t (*read)(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt);
	ssize_t (*write)(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt);
	void (*close)(stream *s);
	void (*clrerr)(stream *s);
	void (*destroy)(stream *s);
	int (*flush)(stream *s, mnstr_flush_level flush_level);
	int (*fsync)(stream *s);
	int (*fgetpos)(stream *restrict s, fpos_t *restrict p);
	int (*fsetpos)(stream *restrict s, fpos_t *restrict p);
	void (*update_timeout)(stream *s);
	int (*isalive)(const stream *s);
	mnstr_error_kind errkind;
	char errmsg[1024]; // avoid allocation on error. We don't have THAT many streams..
};

#ifdef __CYGWIN__
#define __visibility__(a)
#endif

void mnstr_va_set_error(stream *s, mnstr_error_kind kind, const char *fmt, va_list ap)
	__attribute__((__visibility__("hidden")));

void mnstr_set_error(stream *s, mnstr_error_kind kind, const char *fmt, ...)
	__attribute__((__format__(__printf__, 3, 4)))
	__attribute__((__visibility__("hidden")));

void mnstr_set_error_errno(stream *s, mnstr_error_kind kind, const char *fmt, ...)
	__attribute__((__format__(__printf__, 3, 4)))
	__attribute__((__visibility__("hidden")));

void mnstr_copy_error(stream *dst, stream *src)
	__attribute__((__visibility__("hidden")));

void mnstr_set_open_error(const char *name, int errnr, const char *fmt, ...)
	__attribute__((__format__(__printf__, 3, 4)))
	__attribute__((__visibility__("hidden")));


/* used to be static: */

stream *create_stream(const char *name)
	__attribute__((__visibility__("hidden")));

stream *create_wrapper_stream(const char *name, stream *inner)
	__attribute__((__visibility__("hidden")));
void destroy_stream(stream *s)
	__attribute__((__visibility__("hidden")));
stream *open_stream(const char *restrict filename, const char *restrict flags)
	__attribute__((__visibility__("hidden")));

stream *file_stream(const char *name)
	__attribute__((__visibility__("hidden")));

int file_remove(const char *filename)
	__attribute__((__visibility__("hidden")));

/* implementation detail of stdio_stream.c which must be public because
 * for example bstream() special cases on it to provide a fast path for file
 * i/o.
 */
ssize_t file_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
	__attribute__((__visibility__("hidden")));


stream *open_gzrstream(const char *filename)
	__attribute__((__visibility__("hidden")));
stream *open_gzwstream(const char *restrict filename, const char *restrict mode)
	__attribute__((__visibility__("hidden")));
stream *open_gzrastream(const char *filename)
	__attribute__((__visibility__("hidden")));
stream *open_gzwastream(const char *restrict filename, const char *restrict mode)
	__attribute__((__visibility__("hidden")));


stream *open_bzrstream(const char *filename)
	__attribute__((__visibility__("hidden")));
stream *open_bzwstream(const char *restrict filename, const char *restrict mode)
	__attribute__((__visibility__("hidden")));
stream *open_bzrastream(const char *filename)
	__attribute__((__visibility__("hidden")));
stream *open_bzwastream(const char *restrict filename, const char *restrict mode)
	__attribute__((__visibility__("hidden")));

stream *open_xzrstream(const char *filename)
	__attribute__((__visibility__("hidden")));
stream *open_xzwstream(const char *restrict filename, const char *restrict mode)
	__attribute__((__visibility__("hidden")));
stream *open_xzrastream(const char *filename)
	__attribute__((__visibility__("hidden")));
stream *open_xzwastream(const char *restrict filename, const char *restrict mode)
	__attribute__((__visibility__("hidden")));

stream *open_lz4rstream(const char *filename)
	__attribute__((__visibility__("hidden")));
stream *open_lz4wstream(const char *restrict filename, const char *restrict mode)
	__attribute__((__visibility__("hidden")));
stream *open_lz4rastream(const char *filename)
	__attribute__((__visibility__("hidden")));
stream *open_lz4wastream(const char *restrict filename, const char *restrict mode)
	__attribute__((__visibility__("hidden")));


/* Shared between bs2.c, bs.c and stream.c.
 * Have to look into this but I think it should all be in bs.c only, and
 * bs2.c should be dropped.*/
typedef struct bs bs;
struct bs {
	unsigned nr;		/* how far we got in buf */
	unsigned itotal;	/* amount available in current read block */
	size_t blks;		/* read/writen blocks (possibly partial) */
	size_t bytes;		/* read/writen bytes */
	const char *prompt;	/* on eof, first try to send this then try again */
	stream *pstream;	/* stream to send prompts on */
	char buf[BLOCK];	/* the buffered data (minus the size of
				 * size-short */
};
ssize_t bs_read(stream *restrict ss, void *restrict buf, size_t elmsize, size_t cnt)
	__attribute__((__visibility__("hidden")));
ssize_t bs_write(stream *restrict ss, const void *restrict buf, size_t elmsize, size_t cnt)
	__attribute__((__visibility__("hidden")));
void bs_clrerr(stream *s)
	__attribute__((__visibility__("hidden")));
void bs_destroy(stream *ss)
	__attribute__((__visibility__("hidden")));

ssize_t bs2_read(stream *restrict ss, void *restrict buf, size_t elmsize, size_t cnt)
	__attribute__((__visibility__("hidden")));
ssize_t bs2_write(stream *restrict ss, const void *restrict buf, size_t elmsize, size_t cnt)
	__attribute__((__visibility__("hidden")));


#ifdef _MSC_VER
/* windows specific */
ssize_t console_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt);
ssize_t console_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt);
stream *win_console_in_stream(const char *name);
stream *win_console_out_stream(const char *name);
#endif
