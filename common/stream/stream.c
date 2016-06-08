/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/* stream
 * ======
 * Niels Nes
 * An simple interface to streams
 *
 * Processing files, streams, and sockets is quite different on Linux
 * and Windows platforms. To improve portability between both, we advise
 * to replace the stdio actions with the stream functionality provided
 * here.
 *
 * This interface can also be used to open 'non compressed, gzipped,
 * bz2zipped' data files and sockets. Using this interface one could
 * easily switch between the various underlying storage types.
 *
 * buffered streams
 * ----------------
 *
 * The bstream (or buffered_stream) can be used for efficient reading of
 * a stream. Reading can be done in large chunks and access can be done
 * in smaller bits, by directly accessing the underlying buffer.
 *
 * Beware that a flush on a buffered stream emits an empty block to
 * synchronize with the other side, telling it has reached the end of
 * the sequence and can close its descriptors.
 *
 * bstream functions
 * -----------------
 *
 * The bstream_create gets a read stream (rs) as input and the initial
 * chunk size and creates a buffered stream from this. A spare byte is
 * kept at the end of the buffer.  The bstream_read will at least read
 * the next 'size' bytes. If the not read data (aka pos < len) together
 * with the new data will not fit in the current buffer it is resized.
 * The spare byte is kept.
 *
 * tee streams
 * -----------
 *
 * A tee stream is a write stream that duplicates all output to two
 * write streams of the same type (asc/bin).
 */


#include "monetdb_config.h"
#include "stream.h"
#include "stream_socket.h"

#include <string.h>
#include <stdio.h>		/* NULL, printf etc. */
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <stdarg.h>		/* va_alist.. */
#include <assert.h>

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

#ifdef HAVE_ICONV
#ifdef HAVE_ICONV_H
#include <iconv.h>
#endif
#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif
#endif

#ifndef SHUT_RD
#define SHUT_RD		0
#define SHUT_WR		1
#define SHUT_RDWR	2
#endif

#ifndef EWOULDBLOCK
#define EWOULDBLOCK EAGAIN
#endif

#ifndef EINTR
#define EINTR 	EAGAIN
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET (-1)
#endif

#ifdef NATIVE_WIN32
#define pclose _pclose
#endif

#define UTF8BOM		"\xEF\xBB\xBF" /* UTF-8 encoding of Unicode BOM */
#define UTF8BOMLENGTH	3	       /* length of above */

#ifdef _MSC_VER
/* use intrinsic functions on Windows */
#define short_int_SWAP(s)	((short) _byteswap_ushort((unsigned short) (s)))
/* on Windows, long is the same size as int */
#define normal_int_SWAP(s)	((int) _byteswap_ulong((unsigned long) (s)))
#define long_long_SWAP(l)	((lng) _byteswap_uint64((unsigned __int64) (s)))
#else
#define short_int_SWAP(s) ((short)(((0x00ff&(s))<<8) | ((0xff00&(s))>>8)))

#define normal_int_SWAP(i) (((0x000000ff&(i))<<24) | ((0x0000ff00&(i))<<8) | \
			    ((0x00ff0000&(i))>>8)  | ((0xff000000&(i))>>24))
#define long_long_SWAP(l) \
		((((lng)normal_int_SWAP(l))<<32) |\
		 (0xffffffff&normal_int_SWAP(l>>32)))
#endif

#ifdef HAVE_HGE
#define huge_int_SWAP(h) \
		((((hge)long_long_SWAP(h))<<64) |\
		 (0xffffffffffffffff&long_long_SWAP(h>>64)))
#endif


struct stream {
	short byteorder;
	char access;		/* read/write */
	char isutf8;		/* known to be UTF-8 due to BOM */
	short type;		/* ascii/binary */
	char *name;
	unsigned int timeout;	   /* timeout in ms */
	int (*timeout_func)(void); /* callback function: NULL/true -> return */
	union {
		void *p;
		int i;
		SOCKET s;
	} stream_data;
	int errnr;
	ssize_t (*read) (stream *s, void *buf, size_t elmsize, size_t cnt);
	ssize_t (*write) (stream *s, const void *buf, size_t elmsize, size_t cnt);
	void (*close) (stream *s);
	void (*clrerr) (stream *s);
	char *(*error) (stream *s);
	void (*destroy) (stream *s);
	int (*flush) (stream *s);
	int (*fsync) (stream *s);
	int (*fgetpos) (stream *s, lng *p);
	int (*fsetpos) (stream *s, lng p);
	void (*update_timeout) (stream *s);
	int (*isalive) (stream *s);
};

int
mnstr_init(void)
{
	static int inited = 0;

	if (inited)
		return 0;

#ifdef NATIVE_WIN32
	{
		WSADATA w;

		if (WSAStartup(0x0101, &w) != 0)
			return -1;
	}
#endif
	inited = 1;
	return 0;
}

/* #define STREAM_DEBUG 1  */
/* #define BSTREAM_DEBUG 1 */

#ifdef HAVE__WFOPEN
/* convert a string from UTF-8 to wide characters; the return value is
 * freshly allocated */
static wchar_t *
utf8towchar(const char *s)
{
	wchar_t *ws;
	size_t i = 0;
	size_t j = 0;

	ws = malloc((strlen(s) + 1) * sizeof(wchar_t));
	if (ws == NULL)
		return NULL;
	while (s[j]) {
		if ((s[j] & 0x80) == 0) {
			ws[i++] = s[j++];
		} else if ((s[j] & 0xC0) == 0x80) {
			free(ws);
			return NULL;
		} else if ((s[j] & 0xE0) == 0xC0) {
			ws[i] = (s[j++] & 0x1F) << 6;
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i++] |= s[j++] & 0x3F;
		} else if ((s[j] & 0xF0) == 0xE0) {
			ws[i] = (s[j++] & 0x0F) << 12;
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i] |= (s[j++] & 0x3F) << 6;
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i++] |= s[j++] & 0x3F;
		} else if ((s[j] & 0xF8) == 0xF0) {
#if SIZEOF_WCHAR_T == 2
			ws[i] = (s[j++] & 0x07) << 8;
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i] |= (s[j++] & 0x3F) << 2;
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i] |= (s[j] & 0x30) >> 4;
			ws[i] -= 0x0040;
			ws[i++] |= 0xD800;
			ws[i] = 0xDC00 | ((s[j++] & 0x0F) << 6);
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i++] |= s[j++] & 0x3F;
#else
			ws[i] = (s[j++] & 0x07) << 18;
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i] |= (s[j++] & 0x3F) << 12;
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i] |= (s[j++] & 0x3F) << 6;
			if ((s[j] & 0xC0) != 0x80) {
				free(ws);
				return NULL;
			}
			ws[i++] |= s[j++] & 0x3F;
#endif
		} else {
			free(ws);
			return NULL;
		}
	}
	ws[i] = L'\0';
	return ws;
}
#else
static char *
cvfilename(const char *filename)
{
#if defined(HAVE_NL_LANGINFO) && defined(HAVE_ICONV)
	char *code_set = nl_langinfo(CODESET);

	if (code_set != NULL && strcmp(code_set, "UTF-8") != 0) {
		iconv_t cd = iconv_open("UTF-8", code_set);

		if (cd != (iconv_t) -1) {
			size_t len = strlen(filename);
			size_t size = 4 * len;
			ICONV_CONST char *from = (ICONV_CONST char *) filename;
			char *r = malloc(size + 1);
			char *p = r;

			if (r) {
				if (iconv(cd, &from, &len, &p, &size) != (size_t) -1) {
					iconv_close(cd);
					*p = 0;
					return r;
				}
				free(r);
			}
			iconv_close(cd);
		}
	}
#endif
	/* couldn't use iconv for whatever reason; alternative is to
	 * use utf8towchar above to convert to a wide character string
	 * (wcs) and convert that to the locale-specific encoding
	 * using wcstombs or wcsrtombs (but preferably only if the
	 * locale's encoding is not UTF-8) */
	return strdup(filename);
}
#endif

/* Read at most cnt elements of size elmsize from the stream.  Returns
 * the number of elements actually read or < 0 on failure. */
ssize_t
mnstr_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	if (s == NULL || buf == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "read %s " SZFMT " " SZFMT "\n", s->name ? s->name : "<unnamed>", elmsize, cnt);
#endif
	assert(s->access == ST_READ);
	if (s->errnr)
		return -1;
	return (*s->read) (s, buf, elmsize, cnt);
}

/* Read one line (seperated by \n) of at most maxcnt-1 characters from
 * the stream.  Returns the number of characters actually read,
 * includes the trailing \n; terminated by a NULL byte. */
ssize_t
mnstr_readline(stream *s, void *buf, size_t maxcnt)
{
	char *b = buf, *start = buf;

	if (s == NULL || buf == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "readline %s " SZFMT "\n", s->name ? s->name : "<unnamed>", maxcnt);
#endif
	assert(s->access == ST_READ);
	if (s->errnr)
		return -1;
	if (maxcnt == 0)
		return 0;
	if (maxcnt == 1) {
		*start = 0;
		return 0;
	}
	for (;;) {
		switch ((*s->read)(s, start, 1, 1)) {
		case 1:
			/* successfully read a character,
			 * check whether it is the line
			 * separator and whether we have space
			 * left for more */
			if (*start++ == '\n' || --maxcnt == 1) {
				*start = 0;
#if 0
				if (s->type == ST_ASCII &&
				    start[-1] == '\n' &&
				    start > b + 1 &&
				    start[-2] == '\r') {
					/* convert CR-LF to just LF */
					start[-2] = start[-1];
					start--;
				}
#endif
				return (ssize_t) (start - b);
			}
			break;
		case -1:
			/* error: if we didn't read anything yet,
			 * return the error, otherwise return what we
			 * have */
			if (start == b)
				return -1;
			/* fall through */
		case 0:
			/* end of file: return what we have */
			*start = 0;
			return (ssize_t) (start - b);
		}
	}
}

/* Write cnt elements of size elmsize to the stream.  Returns the
 * number of elements actually written.  If elmsize or cnt equals zero,
 * returns cnt. */
ssize_t
mnstr_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	if (s == NULL || buf == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "write %s " SZFMT " " SZFMT "\n", s->name ? s->name : "<unnamed>", elmsize, cnt);
#endif
	assert(s->access == ST_WRITE);
	if (s->errnr)
		return -1;
	return (*s->write) (s, buf, elmsize, cnt);
}

void
mnstr_settimeout(stream *s, unsigned int ms, int (*func)(void))
{
	if (s) {
		s->timeout = ms;
		s->timeout_func = func;
		if (s->update_timeout)
			(*s->update_timeout)(s);
	}
}

void
mnstr_close(stream *s)
{
	if (s) {
#ifdef STREAM_DEBUG
		fprintf(stderr, "close %s\n", s->name ? s->name : "<unnamed>");
#endif
		(*s->close) (s);
	}
}

void
mnstr_destroy(stream *s)
{
	if (s) {
#ifdef STREAM_DEBUG
		fprintf(stderr, "destroy %s\n", s->name ? s->name : "<unnamed>");
#endif
		(*s->destroy) (s);
	}
}

char *
mnstr_error(stream *s)
{
	if (s == NULL)
		return "Connection terminated";
	return (*s->error) (s);
}

/* flush buffer, return 0 on success, non-zero on failure */
int
mnstr_flush(stream *s)
{
	if (s == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "flush %s\n", s->name ? s->name : "<unnamed>");
#endif
	assert(s->access == ST_WRITE);
	if (s->errnr)
		return -1;
	if (s->flush)
		return (*s->flush) (s);
	return 0;
}

/* sync file to disk, return 0 on success, non-zero on failure */
int
mnstr_fsync(stream *s)
{
	if (s == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "fsync %s (%d)\n", s->name ? s->name : "<unnamed>", s->errnr);
#endif
	assert(s->access == ST_WRITE);
	if (s->errnr)
		return -1;
	if (s->fsync)
		return (*s->fsync) (s);
	return 0;
}

int
mnstr_fgetpos(stream *s, lng *p)
{
	if (s == NULL || p == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "fgetpos %s\n", s->name ? s->name : "<unnamed>");
#endif
	if (s->errnr)
		return -1;
	if (s->fgetpos)
		return (*s->fgetpos) (s, p);
	return 0;
}

int
mnstr_fsetpos(stream *s, lng p)
{
	if (s == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "fsetpos %s\n", s->name ? s->name : "<unnamed>");
#endif
	if (s->errnr)
		return -1;
	if (s->fsetpos)
		return (*s->fsetpos) (s, p);
	return 0;
}

int
mnstr_isalive(stream *s)
{
	if (s == NULL)
		return 0;
	if (s->errnr)
		return -1;
	if (s->isalive)
		return (*s->isalive)(s);
	return 1;
}

char *
mnstr_name(stream *s)
{
	if (s == NULL)
		return "connection terminated";
	return s->name;
}

int
mnstr_errnr(stream *s)
{
	if (s == NULL)
		return MNSTR_READ_ERROR;
	return s->errnr;
}

void
mnstr_clearerr(stream *s)
{
	if (s != NULL) {
		s->errnr = MNSTR_NO__ERROR;
		if (s->clrerr)
			(*s->clrerr) (s);
	}
}

int
mnstr_type(stream *s)
{
	if (s == NULL)
		return 0;
	return s->type;
}

int
mnstr_byteorder(stream *s)
{
	if (s == NULL)
		return 0;
	return s->byteorder;
}

void
mnstr_set_byteorder(stream *s, char bigendian)
{
	if (s == NULL)
		return;
#ifdef STREAM_DEBUG
	fprintf(stderr, "mnstr_set_byteorder %s\n", s->name ? s->name : "<unnamed>");
#endif
	assert(s->access == ST_READ);
	s->type = ST_BIN;
#ifdef WORDS_BIGENDIAN
	s->byteorder = bigendian ? 1234 : 3412;
#else
	s->byteorder = bigendian ? 3412 : 1234;
#endif
}


void
close_stream(stream *s)
{
	if (s) {
		s->close(s);
		s->destroy(s);
	}
}

stream *
mnstr_rstream(stream *s)
{
	if (s == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "mnstr_rstream %s\n", s->name ? s->name : "<unnamed>");
#endif
	assert(s->access == ST_READ);
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR)
		s->read(s, (void *) &s->byteorder, sizeof(s->byteorder), 1);
	return s;
}

stream *
mnstr_wstream(stream *s)
{
	if (s == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "mnstr_wstream %s\n", s->name ? s->name : "<unnamed>");
#endif
	assert(s->access == ST_WRITE);
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR)
		s->write(s, (void *) &s->byteorder, sizeof(s->byteorder), 1);
	return s;
}

#define EXT_LEN 4
static const char *
get_extention(const char *file)
{
	char *ext_start;

	return (ext_start = strrchr(file, '.')) != NULL ? ext_start + 1 : "";
}

static void
destroy(stream *s)
{
	if (s->name)
		free(s->name);
	free(s);
}

static char *
error(stream *s)
{
	char buf[128];

	switch (s->errnr) {
	case MNSTR_OPEN_ERROR:
		snprintf(buf, sizeof(buf), "error could not open file %.100s\n", 
			 s->name);
		return strdup(buf);
	case MNSTR_READ_ERROR:
		snprintf(buf, sizeof(buf), "error reading file %.100s\n",
			 s->name);
		return strdup(buf);
	case MNSTR_WRITE_ERROR:
		snprintf(buf, sizeof(buf), "error writing file %.100s\n",
			 s->name);
		return strdup(buf);
	case MNSTR_TIMEOUT:
		snprintf(buf, sizeof(buf), "timeout on %.100s\n", s->name);
		return strdup(buf);
	}
	return strdup("Unknown error");
}

static stream *
create_stream(const char *name)
{
	stream *s;

	if (name == NULL)
		return NULL;
	if ((s = (stream *) malloc(sizeof(*s))) == NULL)
		return NULL;
	s->byteorder = 1234;
	s->access = ST_READ;
	s->isutf8 = 0;		/* not known for sure */
	s->type = ST_ASCII;
	s->name = strdup(name);
	s->stream_data.p = NULL;
	s->errnr = MNSTR_NO__ERROR;
	s->read = NULL;
	s->write = NULL;
	s->close = NULL;
	s->clrerr = NULL;
	s->error = error;
	s->destroy = destroy;
	s->flush = NULL;
	s->fsync = NULL;
	s->fgetpos = NULL;
	s->fsetpos = NULL;
	s->timeout = 0;
	s->timeout_func = NULL;
	s->update_timeout = NULL;
	s->isalive = NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "create_stream %s -> " PTRFMT "\n", name ? name : "<unnamed>", PTRFMTCAST s);
#endif
	return s;
}

/* ------------------------------------------------------------------ */
/* streams working on a disk file */

static ssize_t
file_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	FILE *fp = (FILE *) s->stream_data.p;
	size_t rc = 0;

	if (fp == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}

	if (elmsize && cnt && !feof(fp)) {
		if (ferror(fp) ||
		    ((rc = fread(buf, elmsize, cnt, fp)) == 0 &&
		     ferror(fp))) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
	}
	return (ssize_t) rc;
}

static ssize_t
file_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}

	if (elmsize && cnt) {
		size_t rc = fwrite(buf, elmsize, cnt, fp);

		if (ferror(fp)) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		return (ssize_t) rc;
	}
	return (ssize_t) cnt;
}

static void
file_close(stream *s)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL)
		return;
	if (fp != stdin && fp != stdout && fp != stderr) {
		if (s->name && *s->name == '|')
			pclose(fp);
		else
			fclose(fp);
	} else if (s->access == ST_WRITE)
		fflush(fp);
	s->stream_data.p = NULL;
}

static void
file_clrerr(stream *s)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp)
		clearerr(fp);
}

static int
file_flush(stream *s)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL || (s->access == ST_WRITE && fflush(fp) < 0)) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	return 0;
}

static int
file_fsync(stream *s)
{

	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL ||
	    (s->access == ST_WRITE
#ifdef NATIVE_WIN32
	     && _commit(_fileno(fp)) < 0
#else
#ifdef HAVE_FDATASYNC
	     && fdatasync(fileno(fp)) < 0
#else
#ifdef HAVE_FSYNC
	     && fsync(fileno(fp)) < 0
#endif
#endif
#endif
		    )) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	return 0;
}

static int
file_fgetpos(stream *s, lng *p)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL || p == NULL)
		return -1;
#ifdef WIN32
	*p = (lng) _ftelli64(fp);	/* returns __int64 */
#else
#ifdef HAVE_FSEEKO
	*p = (lng) ftello(fp);	/* returns off_t */
#else
	*p = (lng) ftell(fp);	/* returns long */
#endif
#endif
	return *p < 0 ? -1 : 0;
}

static int
file_fsetpos(stream *s, lng p)
{
	int res = 0;
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL)
		return -1;
#ifdef WIN32
	res = _fseeki64(fp, (__int64) p, SEEK_SET);
#else
#ifdef HAVE_FSEEKO
	res = fseeko(fp, (off_t) p, SEEK_SET);
#else
	res = fseek(fp, (long) p, SEEK_SET);
#endif
#endif
	return res;
}

#ifdef NATIVE_WIN32
#define fileno(fd) _fileno(fd)
#endif

size_t
getFileSize(stream *s)
{
       if (s->read == file_read) {
               struct stat stb;

               if (fstat(fileno((FILE *) s->stream_data.p), &stb) == 0)
		       return (size_t) stb.st_size;
	       /* we shouldn't get here... */
       }
       return 0;               /* unknown */
}

static stream *
open_stream(const char *filename, const char *flags)
{
	stream *s;
	FILE *fp;
	lng pos;
	char buf[UTF8BOMLENGTH + 1];

	if ((s = create_stream(filename)) == NULL)
		return NULL;
#ifdef HAVE__WFOPEN
	{
		wchar_t *wfname = utf8towchar(filename);
		wchar_t *wflags = utf8towchar(flags);
		if (wfname != NULL && wflags != NULL)
			fp = _wfopen(wfname, wflags);
		else
			fp = NULL;
		if (wfname)
			free(wfname);
		if (wflags)
			free(wflags);
	}
#else
	{
		char *fname = cvfilename(filename);
		if (fname) {
			fp = fopen(fname, flags);
			free(fname);
		} else
			fp = NULL;
	}
#endif
	if (fp == NULL) {
		destroy(s);
		return NULL;
	}
	s->read = file_read;
	s->write = file_write;
	s->close = file_close;
	s->clrerr = file_clrerr;
	s->flush = file_flush;
	s->fsync = file_fsync;
	s->fgetpos = file_fgetpos;
	s->fsetpos = file_fsetpos;
	s->stream_data.p = (void *) fp;
	/* if a text file is opened for reading, and it starts with
	 * the UTF-8 encoding of the Unicode Byte Order Mark, skip the
	 * mark, and mark the stream as being a UTF-8 stream */
	if (flags[0] == 'r' &&
	    flags[1] != 'b' &&
	    file_fgetpos(s, &pos) == 0) {
		if (file_read(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0)
			s->isutf8 = 1;
		else if (file_fsetpos(s, pos) < 0) {
			/* unlikely: we couldn't seek the file back */
			fclose(fp);
			destroy(s);
			return NULL;
		}
	}
	return s;
}

/* ------------------------------------------------------------------ */
/* streams working on a gzip-compressed disk file */

#ifdef HAVE_LIBZ
static ssize_t
stream_gzread(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	gzFile fp = (gzFile) s->stream_data.p;
	int size = (int) (elmsize * cnt);
	int err = 0;

	if (fp == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}

	if (size && !gzeof(fp)) {
		size = gzread(fp, buf, size);
		if (gzerror(fp, &err) != NULL && err < 0) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
#ifdef WIN32
		/* on Windows when in text mode, convert \r\n line
		 * endings to \n */
		if (s->type == ST_ASCII) {
			char *p1, *p2, *pe;

			p1 = buf;
			pe = p1 + size;
			while (p1 < pe && *p1 != '\r')
				p1++;
			p2 = p1;
			while (p1 < pe) {
				if (*p1 == '\r' && p1[1] == '\n')
					size--;
				else
					*p2++ = *p1;
				p1++;
			}
		}
#endif
		return (ssize_t) (size / elmsize);
	}
	return 0;
}

static ssize_t
stream_gzwrite(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	gzFile fp = (gzFile) s->stream_data.p;
	int size = (int) (elmsize * cnt);
	int err = 0;

	if (fp == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}

	if (size) {
		size = gzwrite(fp, buf, size);
		if (gzerror(fp, &err) != NULL && err < 0) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		return (ssize_t) (size / elmsize);
	}
	return (ssize_t) cnt;
}

static void
stream_gzclose(stream *s)
{
	if (s->stream_data.p)
		gzclose((gzFile) s->stream_data.p);
	s->stream_data.p = NULL;
}

static int
stream_gzflush(stream *s)
{
	if (s->stream_data.p == NULL)
		return -1;
	if (s->access == ST_WRITE &&
	    gzflush((gzFile) s->stream_data.p, Z_SYNC_FLUSH) != Z_OK)
		return -1;
	return 0;
}

static stream *
open_gzstream(const char *filename, const char *flags)
{
	stream *s;
	gzFile fp;

	if ((s = create_stream(filename)) == NULL)
		return NULL;
#ifdef HAVE__WFOPEN
	{
		wchar_t *wfname = utf8towchar(filename);
		if (wfname != NULL) {
			fp = gzopen_w(wfname, flags);
			free(wfname);
		} else
			fp = NULL;
	}
#else
	{
		char *fname = cvfilename(filename);
		if (fname) {
			fp = gzopen(fname, flags);
			free(fname);
		} else
			fp = NULL;
	}
#endif
	if (fp == NULL) {
		destroy(s);
		return NULL;
	}
	s->read = stream_gzread;
	s->write = stream_gzwrite;
	s->close = stream_gzclose;
	s->flush = stream_gzflush;
	s->stream_data.p = (void *) fp;
	if (flags[0] == 'r' && flags[1] != 'b') {
		char buf[UTF8BOMLENGTH];
		if (gzread(fp, buf, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
			s->isutf8 = 1;
		} else {
			gzrewind(fp);
		}
	}
	return s;
}

static stream *
open_gzrstream(const char *filename)
{
	stream *s;

	if ((s = open_gzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    gzread((gzFile) s->stream_data.p, (void *) &s->byteorder, sizeof(s->byteorder)) < (int) sizeof(s->byteorder)) {
		stream_gzclose(s);
		destroy(s);
		return NULL;
	}
	return s;
}

static stream *
open_gzwstream(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_gzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    gzwrite((gzFile) s->stream_data.p, (void *) &s->byteorder, sizeof(s->byteorder)) < (int) sizeof(s->byteorder)) {
		stream_gzclose(s);
		destroy(s);
		return NULL;
	}
	return s;
}

static stream *
open_gzrastream(const char *filename)
{
	stream *s;

	if ((s = open_gzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_ASCII;
	return s;
}

static stream *
open_gzwastream(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_gzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}
#else
#define open_gzrstream(filename)	NULL
#define open_gzwstream(filename, mode)	NULL
#define open_gzrastream(filename)	NULL
#define open_gzwastream(filename, mode)	NULL
#endif

/* ------------------------------------------------------------------ */
/* streams working on a bzip2-compressed disk file */

#ifdef HAVE_LIBBZ2
struct bz {
	BZFILE *b;
	FILE *f;
};

static void
stream_bzclose(stream *s)
{
	int err = BZ_OK;

	if (s->stream_data.p) {
		if (s->access == ST_READ)
			BZ2_bzReadClose(&err, ((struct bz *) s->stream_data.p)->b);
		else
			BZ2_bzWriteClose(&err, ((struct bz *) s->stream_data.p)->b, 0, NULL, NULL);
		fclose(((struct bz *) s->stream_data.p)->f);
		free(s->stream_data.p);
	}
	s->stream_data.p = NULL;
}

static ssize_t
stream_bzread(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	int size = (int) (elmsize * cnt);
	int err;
	void *punused;
	int nunused;
	char unused[BZ_MAX_UNUSED];
	struct bz *bzp = s->stream_data.p;

	if (bzp == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}
	if (size == 0)
		return 0;
	size = BZ2_bzRead(&err, bzp->b, buf, size);
	if (err == BZ_STREAM_END) {
		/* end of stream, but not necessarily end of file: get
		 * unused bits, close stream, and open again with the
		 * saved unused bits */
		BZ2_bzReadGetUnused(&err, bzp->b, &punused, &nunused);
		if (err == BZ_OK && (nunused > 0 || !feof(bzp->f))) {
			if (nunused > 0)
				memcpy(unused, punused, nunused);
			BZ2_bzReadClose(&err, bzp->b);
			bzp->b = BZ2_bzReadOpen(&err, bzp->f, 0, 0, unused, nunused);
		} else {
			stream_bzclose(s);
		}
	}
	if (err != BZ_OK) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}
#ifdef WIN32
	/* on Windows when in text mode, convert \r\n line endings to
	 * \n */
	if (s->type == ST_ASCII) {
		char *p1, *p2, *pe;

		p1 = buf;
		pe = p1 + size;
		while (p1 < pe && *p1 != '\r')
			p1++;
		p2 = p1;
		while (p1 < pe) {
			if (*p1 == '\r' && p1[1] == '\n')
				size--;
			else
				*p2++ = *p1;
			p1++;
		}
	}
#endif
	return size / elmsize;
}

static ssize_t
stream_bzwrite(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	int size = (int) (elmsize * cnt);
	int err;
	struct bz *bzp = s->stream_data.p;

	if (bzp == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	if (size) {
		BZ2_bzWrite(&err, bzp->b, (void *) buf, size);
		if (err != BZ_OK) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		return cnt;
	}
	return 0;
}

static stream *
open_bzstream(const char *filename, const char *flags)
{
	stream *s;
	int err;
	struct bz *bzp;

	if ((bzp = malloc(sizeof(struct bz))) == NULL)
		return NULL;
	if ((s = create_stream(filename)) == NULL) {
		free(bzp);
		return NULL;
	}
#ifdef HAVE__WFOPEN
	{
		wchar_t *wfname = utf8towchar(filename);
		wchar_t *wflags = utf8towchar(flags);
		if (wfname != NULL && wflags != NULL)
			bzp->f = _wfopen(wfname, wflags);
		else
			bzp->f = NULL;
		if (wfname)
			free(wfname);
		if (wflags)
			free(wflags);
	}
#else
	{
		char *fname = cvfilename(filename);
		if (fname) {
			bzp->f = fopen(fname, flags);
			free(fname);
		} else
			bzp->f = NULL;
	}
#endif
	if (bzp->f == NULL) {
		destroy(s);
		free(bzp);
		return NULL;
	}
	s->read = stream_bzread;
	s->write = stream_bzwrite;
	s->close = stream_bzclose;
	s->flush = NULL;
	s->stream_data.p = (void *) bzp;
	if (flags[0] == 'r' && flags[1] != 'b') {
		s->access = ST_READ;
		bzp->b = BZ2_bzReadOpen(&err, bzp->f, 0, 0, NULL, 0);
		if (err == BZ_STREAM_END) {
			BZ2_bzReadClose(&err, bzp->b);
			bzp->b = NULL;
		} else {
			char buf[UTF8BOMLENGTH];

			if (stream_bzread(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
			    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
				s->isutf8 = 1;
			} else if (s->stream_data.p) {
				bzp = s->stream_data.p;
				BZ2_bzReadClose(&err, bzp->b);
				rewind(bzp->f);
				bzp->b = BZ2_bzReadOpen(&err, bzp->f, 0, 0, NULL, 0);
			}
		}
	} else if (flags[0] == 'r') {
		bzp->b = BZ2_bzReadOpen(&err, bzp->f, 0, 0, NULL, 0);
		s->access = ST_READ;
	} else {
		bzp->b = BZ2_bzWriteOpen(&err, bzp->f, 9, 0, 30);
		s->access = ST_WRITE;
	}
	if (err != BZ_OK) {
		stream_bzclose(s);
		destroy(s);
		return NULL;
	}
	return s;
}

static stream *
open_bzrstream(const char *filename)
{
	stream *s;

	if ((s = open_bzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    stream_bzread(s, (void *) &s->byteorder, sizeof(s->byteorder), 1) != 1) {
		stream_bzclose(s);
		destroy(s);
		return NULL;
	}
	return s;
}

static stream *
open_bzwstream(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_bzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    stream_bzwrite(s, (void *) &s->byteorder, sizeof(s->byteorder), 1) != 1) {
		stream_bzclose(s);
		destroy(s);
		return NULL;
	}
	return s;
}

static stream *
open_bzrastream(const char *filename)
{
	stream *s;

	if ((s = open_bzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_ASCII;
	return s;
}

static stream *
open_bzwastream(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_bzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}
#else
#define open_bzrstream(filename)	NULL
#define open_bzwstream(filename, mode)	NULL
#define open_bzrastream(filename)	NULL
#define open_bzwastream(filename, mode)	NULL
#endif

/* ------------------------------------------------------------------ */
/* streams working on a lzma-compressed disk file */

#ifdef HAVE_LIBLZMA
#define XZBUFSIZ 64*1024
typedef struct xz_stream {
	FILE *fp;
	lzma_stream strm;
	int  todo;
	uint8_t buf[XZBUFSIZ]; 
} xz_stream;

static ssize_t
stream_xzread(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	xz_stream *xz = s->stream_data.p;
	size_t size = elmsize * cnt, origsize = size, ressize = 0;
	uint8_t *outbuf = buf;
	lzma_action action = LZMA_RUN;

	if (xz == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}

	xz->strm.next_in = xz->buf;
	xz->strm.avail_in = xz->todo;
	xz->strm.next_out = outbuf;
	xz->strm.avail_out = size;
	while (size && (xz->strm.avail_in || !feof(xz->fp))) {
		lzma_ret ret;
		size_t sz = (size>XZBUFSIZ) ? XZBUFSIZ : size;

		if (!xz->strm.avail_in &&
		    (xz->strm.avail_in = fread(xz->buf, 1, sz, xz->fp)) == 0) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
		xz->strm.next_in = xz->buf;
		if (feof(xz->fp))
			action = LZMA_FINISH;
		ret = lzma_code(&xz->strm, action);
		if (xz->strm.avail_out == 0 || ret == LZMA_STREAM_END) {
			origsize -= xz->strm.avail_out; /* remaining space */
			xz->todo = xz->strm.avail_in;
			if (xz->todo > 0)
				memmove(xz->buf, xz->strm.next_in, xz->todo);
			outbuf[origsize] = 0; /* add EOS */
			ressize = origsize;
			break;
		}
		if (ret != LZMA_OK) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
	}
	if (ressize) {
#ifdef WIN32
		/* on Windows when in text mode, convert \r\n line
		 * endings to \n */
		if (s->type == ST_ASCII) {
			char *p1, *p2, *pe;

			p1 = buf;
			pe = p1 + ressize;
			while (p1 < pe && *p1 != '\r')
				p1++;
			p2 = p1;
			while (p1 < pe) {
				if (*p1 == '\r' && p1[1] == '\n')
					ressize--;
				else
					*p2++ = *p1;
				p1++;
			}
		}
#endif
		return (ssize_t) (ressize / elmsize);
	}
	return 0;
}

static ssize_t
stream_xzwrite(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	xz_stream *xz = s->stream_data.p;
	size_t size = elmsize * cnt;
	lzma_action action = LZMA_RUN;

	if (xz == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}

	xz->strm.next_in = buf;
	xz->strm.avail_in = size;
	xz->strm.next_out = xz->buf;
	xz->strm.avail_out = XZBUFSIZ;

	size = 0;
	while (xz->strm.avail_in) {
		size_t sz = 0, isz = xz->strm.avail_in;

		lzma_ret ret = lzma_code(&xz->strm, action);
		if (xz->strm.avail_out == 0 || ret != LZMA_OK) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		sz = XZBUFSIZ - xz->strm.avail_out;
		if (fwrite(xz->buf, 1, sz, xz->fp) != sz) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		assert(xz->strm.avail_in == 0);
		size += isz;
		xz->strm.next_out = xz->buf;
		xz->strm.avail_out = XZBUFSIZ;
	}
	if (size) 
		return (ssize_t) (size / elmsize);
	return (ssize_t) cnt;
}

static void
stream_xzclose(stream *s)
{
	xz_stream *xz = s->stream_data.p;

	if (xz) {
		if (s->access == ST_WRITE) {
			lzma_ret ret = lzma_code(&xz->strm, LZMA_FINISH);

			if (xz->strm.avail_out && ret == LZMA_STREAM_END) {
				size_t sz = XZBUFSIZ - xz->strm.avail_out;
				if (fwrite(xz->buf, 1, sz, xz->fp) != sz) 
					s->errnr = MNSTR_WRITE_ERROR;
			}
		}
		fflush(xz->fp);
		fclose(xz->fp);
		lzma_end(&xz->strm);
		free(xz);
	}
	s->stream_data.p = NULL;
}

static int
stream_xzflush(stream *s)
{
	xz_stream *xz = s->stream_data.p;

	if (xz == NULL)
		return -1;
	if (s->access == ST_WRITE && fflush(xz->fp)) 
		return -1;
	return 0;
}

static stream *
open_xzstream(const char *filename, const char *flags)
{
	stream *s;
	xz_stream *xz;
	uint32_t preset = 0;

	if ((xz = malloc(sizeof(struct xz_stream))) == NULL)
		return NULL;
	if (xz)
		memset(xz, 0, sizeof(xz_stream));
	if (((flags[0] == 'r' && 
	      lzma_stream_decoder(&xz->strm, UINT64_MAX, LZMA_CONCATENATED) != LZMA_OK)) ||
	     (flags[0] == 'w' &&
	      lzma_easy_encoder(&xz->strm, preset, LZMA_CHECK_CRC64) != LZMA_OK)) {
		free(xz);
		return NULL;
	}
	if ((s = create_stream(filename)) == NULL) {
		free(xz);
		return NULL;
	}
#ifdef HAVE__WFOPEN
	{_
		wchar_t *wfname = utf8towchar(filename);
		wchar_t *wflags = utf8towchar(flags);
		if (wfname != NULL) 
			xz->fp = _wfopen(wfname, wflags);
		} else
			xz->fp = NULL;
		if (wfname)
			free(wfname);
		if (wflags)
			free(wflags);
	}
#else
	{
		char *fname = cvfilename(filename);
		if (fname) {
			xz->fp = fopen(fname, flags);
			free(fname);
		} else
			xz->fp = NULL;
	}
#endif
	if (xz->fp == NULL) {
		destroy(s);
		free(xz);
		return NULL;
	}
	s->read = stream_xzread;
	s->write = stream_xzwrite;
	s->close = stream_xzclose;
	s->flush = stream_xzflush;
	s->stream_data.p = (void *) xz;
	if (flags[0] == 'r' && flags[1] != 'b') {
		char buf[UTF8BOMLENGTH];
		if (stream_xzread(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
			s->isutf8 = 1;
		} else {
			rewind(xz->fp);
		}
	}
	return s;
}

static stream *
open_xzrstream(const char *filename)
{
	stream *s;

	if ((s = open_xzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    stream_xzread(s, (void *) &s->byteorder, sizeof(s->byteorder), 1) < 1) {
		stream_xzclose(s);
		destroy(s);
		return NULL;
	}
	return s;
}

static stream *
open_xzwstream(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_xzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    stream_xzwrite(s, (void *) &s->byteorder, sizeof(s->byteorder), 1) < 1) {
		stream_xzclose(s);
		destroy(s);
		return NULL;
	}
	return s;
}

static stream *
open_xzrastream(const char *filename)
{
	stream *s;

	if ((s = open_xzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_ASCII;
	return s;
}

static stream *
open_xzwastream(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_xzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}
#else
#define open_xzrstream(filename)	NULL
#define open_xzwstream(filename, mode)	NULL
#define open_xzrastream(filename)	NULL
#define open_xzwastream(filename, mode)	NULL
#endif

/* ------------------------------------------------------------------ */
/* streams working on a disk file, compressed or not */

stream *
open_rstream(const char *filename)
{
	stream *s;
	const char *ext;

	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_rstream %s\n", filename);
#endif
	ext = get_extention(filename);

	if (strcmp(ext, "gz") == 0)
		return open_gzrstream(filename);
	if (strcmp(ext, "bz2") == 0)
		return open_bzrstream(filename);
	if (strcmp(ext, "xz") == 0)
		return open_xzrstream(filename);

	if ((s = open_stream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR) {
		FILE *fp = s->stream_data.p;
		if (fread(&s->byteorder, sizeof(s->byteorder), 1, fp) < 1 ||
		    ferror(fp)) {
			fclose(fp);
			destroy(s);
			return NULL;
		}
	}
	return s;
}

static stream *
open_wstream_(const char *filename, char *mode)
{
	stream *s;
	const char *ext;

	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_wstream %s\n", filename);
#endif
	ext = get_extention(filename);

	if (strcmp(ext, "gz") == 0)
		return open_gzwstream(filename, mode);
	if (strcmp(ext, "bz2") == 0)
		return open_bzwstream(filename, mode);
	if (strcmp(ext, "xz") == 0)
		return open_xzwstream(filename, mode);

	if ((s = open_stream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR) {
		FILE *fp = s->stream_data.p;
		if (fwrite(&s->byteorder, sizeof(s->byteorder), 1, fp) < 1) {
			fclose(fp);
			destroy(s);
			return NULL;
		}
	}
	return s;
}

stream *
open_wstream(const char *filename)
{
	return open_wstream_(filename, "wb");
}

stream *
append_wstream(const char *filename)
{
	return open_wstream_(filename, "ab");
}

stream *
open_rastream(const char *filename)
{
	stream *s;
	const char *ext;

	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_rastream %s\n", filename);
#endif
	ext = get_extention(filename);

	if (strcmp(ext, "gz") == 0)
		return open_gzrastream(filename);
	if (strcmp(ext, "bz2") == 0)
		return open_bzrastream(filename);
	if (strcmp(ext, "xz") == 0)
		return open_xzrastream(filename);

	if ((s = open_stream(filename, "r")) == NULL)
		return NULL;
	s->type = ST_ASCII;
	return s;
}

static stream *
open_wastream_(const char *filename, char *mode)
{
	stream *s;
	const char *ext;

	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_wastream %s\n", filename);
#endif
	ext = get_extention(filename);

	if (strcmp(ext, "gz") == 0)
		return open_gzwastream(filename, mode);
	if (strcmp(ext, "bz2") == 0)
		return open_bzwastream(filename, mode);
	if (strcmp(ext, "xz") == 0)
		return open_xzwastream(filename, mode);

	if ((s = open_stream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}

stream *
open_wastream(const char *filename)
{
	return open_wastream_(filename, "w");
}

stream *
append_wastream(const char *filename)
{
	return open_wastream_(filename, "a");
}

/* ------------------------------------------------------------------ */
/* streams working on a remote file using cURL */

#ifdef HAVE_CURL
#include <curl/curl.h>

#ifdef USE_CURL_MULTI
static CURLM *multi_handle;
#endif

struct curl_data {
	CURL *handle;
	char *buffer;		/* buffer to store incoming data */
	size_t maxsize;		/* size of allocated buffer */
	size_t usesize;		/* end of used data */
	size_t offset;		/* start of unread data */
	int running;		/* whether still transferring */
#ifdef USE_CURL_MULTI
	CURLMcode result;	/* result of transfer (if !running) */
	struct curl_data *next;	/* linked list (curl_handles) */
#endif
};
#ifdef USE_CURL_MULTI
static struct curl_data *curl_handles;
#endif

#define BLOCK_CURL	(1 << 16)

/* this function is called by libcurl when there is data for us */
static size_t
write_callback(char *buffer, size_t size, size_t nitems, void *userp)
{
	stream *s = (stream *) userp;
	struct curl_data *c = (struct curl_data *) s->stream_data.p;

	size *= nitems;
	if (size == 0)		/* unlikely */
		return 0;
	/* allocate a buffer if we don't have one yet */
	if (c->buffer == NULL) {
		/* BLOCK_CURL had better be a power of 2! */
		c->maxsize = (size + BLOCK_CURL - 1) & ~(BLOCK_CURL - 1);
		if ((c->buffer = malloc(c->maxsize)) == NULL)
			return 0;
		c->usesize = 0;
		c->offset = 0;
	}
#ifndef USE_CURL_MULTI
	/* move data if we don't have enough space */
	if (c->maxsize - c->usesize < size && c->offset > 0) {
		memmove(c->buffer, c->buffer + c->offset, c->usesize - c->offset);
		c->usesize -= c->offset;
		c->offset = 0;
	}
#endif
	/* allocate more buffer space if we still don't have enough space */
	if (c->maxsize - c->usesize < size) {
		char *b;
		size_t maxsize;

		maxsize = (c->usesize + size + BLOCK_CURL - 1) & ~(BLOCK_CURL - 1);
		b = realloc(c->buffer, c->maxsize);
		if (b == NULL)
			return 0; /* indicate failure to library */
		c->buffer = b;
		c->maxsize = maxsize;
	}
	/* finally, store the data we received */
	memcpy(c->buffer + c->usesize, buffer, size);
	c->usesize += size;
	return size;
}

static void
curl_destroy(stream *s)
{
	struct curl_data *c;
#ifdef USE_CURL_MULTI
	struct curl_data **cp;
#endif

	if ((c = (struct curl_data *) s->stream_data.p) != NULL) {
		s->stream_data.p = NULL;
#ifdef USE_CURL_MULTI
		/* lock access to curl_handles */
		cp = &curl_handles;
		while (*cp && *cp != c)
			cp = &(*cp)->next;
		if (*cp)
			*cp = c->next;
		/* unlock access to curl_handles */
#endif
		if (c->handle) {
#ifdef USE_CURL_MULTI
			curl_multi_remove_handle(mult_handle, c->handle);
#endif
			curl_easy_cleanup(c->handle);
		}
		if (c->buffer)
			free(c->buffer);
		free(c);
	}
	destroy(s);
}

static ssize_t
curl_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	struct curl_data *c = (struct curl_data *) s->stream_data.p;
	size_t size = cnt * elmsize;

	if (c == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}

	if (size == 0)
		return 0;
	if (c->usesize - c->offset >= elmsize || !c->running) {
		/* there is at least one element's worth of data
		 * available, or we have reached the end: return as
		 * much as we have, but no more than requested */
		if (size > c->usesize - c->offset) {
			cnt = (c->usesize - c->offset) / elmsize;
			size = cnt * elmsize;
		}
		memcpy(buf, c->buffer + c->offset, size);
		c->offset += size;
		if (c->offset == c->usesize)
			c->usesize = c->offset = 0;
		return (ssize_t) cnt;
	}
	/* not enough data, we must wait until we get some */
#ifndef USE_CURL_MULTI
	return 0;
#endif
}

static ssize_t
curl_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	(void) s;
	(void) buf;
	(void) elmsize;
	(void) cnt;
	assert(0);
	return -1;
}

static void
curl_close(stream *s)
{
	(void) s;
}

stream *
open_urlstream(const char *url)
{
	stream *s;
	struct curl_data *c;
#ifdef USE_CURL_MULTI
	CURLMsg *msg;
#endif

	if ((c = malloc(sizeof(*c))) == NULL)
		return NULL;
	c->handle = NULL;
	c->buffer = NULL;
	c->maxsize = c->usesize = c->offset = 0;
	c->running = 1;
	if ((s = create_stream(url)) == NULL) {
		free(c);
		return NULL;
	}
#ifdef USE_CURL_MULTI
	/* lock access to curl_handles */
	c->next = curl_handles;
	curl_handles = c;
	/* unlock access to curl_handles */
#endif
	s->read = curl_read;
	s->write = curl_write;
	s->close = curl_close;
	s->destroy = curl_destroy;
	if ((c->handle = curl_easy_init()) == NULL) {
		free(c);
		destroy(s);
		return NULL;
	}
	s->stream_data.p = (void *) c;
	curl_easy_setopt(c->handle, CURLOPT_URL, s->name);
	curl_easy_setopt(c->handle, CURLOPT_WRITEDATA, s);
	curl_easy_setopt(c->handle, CURLOPT_VERBOSE, 0);
	curl_easy_setopt(c->handle, CURLOPT_NOSIGNAL, 1);
	curl_easy_setopt(c->handle, CURLOPT_WRITEFUNCTION, write_callback);
#ifdef USE_CURL_MULTI
	if (multi_handle == NULL)
		multi_handle = curl_multi_init();
	curl_multi_add_handle(multi_handle, c->handle);
	while (curl_multi_perform(multi_handle, NULL) == CURLM_CALL_MULTI_PERFORM)
		;
	while ((msg = curl_multi_info_read(multi_handle, NULL)) != NULL) {
		struct curl_data *p;
		/* lock access to curl_handles */
		for (p = curl_handles; p; p = p->next) {
			if (p->handle == msg->easy_handle) {
				switch (msg->msg) {
				case CURLMSG_DONE:
					p->running = 0;
					p->result = msg->data.result;
					curl_multi_remove_handle(multi_handle, p->handle);
					curl_easy_cleanup(p->handle);
					p->handle = NULL;
					break;
				default:
					break;
				}
				break;
			}
		}
		/* unlock access to curl_handles */
	}
#else
	if (curl_easy_perform(c->handle) != CURLE_OK) {
		curl_destroy(s);
		return NULL;
	}
	curl_easy_cleanup(c->handle);
	c->handle = NULL;
	c->running = 0;
#endif
	return s;
}

#else
stream *
open_urlstream(const char *url)
{
	if (url != NULL && strncmp(url, "file://", sizeof("file://") - 1) == 0) {
		url += sizeof("file://") - 1;
#ifdef _MSC_VER
		/* file:///C:/... -- remove third / as well */
		if (url[0] == '/' && url[2] == ':')
			url++;
#endif
		return open_rastream(url);
	}
	return NULL;
}
#endif /* HAVE_CURL */

/* ------------------------------------------------------------------ */
/* streams working on a socket */

static ssize_t
socket_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	ssize_t nr = 0, res = 0, size = (ssize_t) (elmsize * cnt);

	if (s->errnr)
		return -1;

	if (size == 0 || elmsize == 0)
		return (ssize_t) cnt;

	errno = 0;
	while (res < size &&
	       (
#ifdef NATIVE_WIN32
		/* send works on int, make sure the argument fits */
		((nr = send(s->stream_data.s, (void *) ((char *) buf + res),
			    (int) min(size - res, 1 << 16), 0)) > 0)
#else
		((nr = write(s->stream_data.s, ((const char *) buf + res),
			     size - res)) > 0)
#endif
		|| (nr < 0 &&	/* syscall failed */
		    s->timeout > 0 && /* potentially timeout */
#ifdef _MSC_VER
		    WSAGetLastError() == WSAEWOULDBLOCK &&
#else
		    (errno == EAGAIN
#if EAGAIN != EWOULDBLOCK
		     || errno == EWOULDBLOCK
#endif
			    ) && /* it was! */
#endif
		    s->timeout_func != NULL && /* callback function exists */
		    !(*s->timeout_func)())     /* callback says don't stop */
		|| (nr < 0 &&
#ifdef _MSC_VER
		    WSAGetLastError() == WSAEINTR
#else
		    errno == EINTR
#endif
			)) /* interrupted */
		) {
		errno = 0;
#ifdef _MSC_VER
		WSASetLastError(0);
#endif
		if (nr > 0)
			res += nr;
	}
	if ((size_t) res >= elmsize)
		return (ssize_t) (res / elmsize);
	if (nr < 0) {
		if (s->timeout > 0 &&
#ifdef _MSC_VER
		    WSAGetLastError() == WSAEWOULDBLOCK
#else
		    (errno == EAGAIN
#if EAGAIN != EWOULDBLOCK
		     || errno == EWOULDBLOCK
#endif
			    )
#endif
			)
			s->errnr = MNSTR_TIMEOUT;
		else
			s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	return 0;
}

static ssize_t
socket_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	ssize_t nr = 0, size = (ssize_t) (elmsize * cnt);

	if (s->errnr)
		return -1;
	if (size == 0)
		return 0;

#ifdef _MSC_VER
	/* recv only takes an int parameter, and read does not accept
	 * sockets */
	if (size > INT_MAX)
		size = elmsize * (INT_MAX / elmsize);
#endif
	for (;;) {
		if (s->timeout) {
			struct timeval tv;
			fd_set fds;
			int ret;

			errno = 0;
#ifdef _MSC_VER
			WSASetLastError(0);
#endif
			FD_ZERO(&fds);
			FD_SET(s->stream_data.s, &fds);
			tv.tv_sec = s->timeout / 1000;
			tv.tv_usec = (s->timeout % 1000) * 1000;
			ret = select(
#ifdef _MSC_VER
				0, /* ignored on Windows */
#else
				s->stream_data.s + 1,
#endif
				&fds, NULL, NULL, &tv);
			if (s->timeout_func && (*s->timeout_func)()) {
				s->errnr = MNSTR_TIMEOUT;
				return -1;
			}
			if (ret == SOCKET_ERROR) {
				s->errnr = MNSTR_READ_ERROR;
				return -1;
			}
			if (ret == 0)
				continue;
			assert(ret == 1);
			assert(FD_ISSET(s->stream_data.s, &fds));
		}
#ifdef _MSC_VER
		nr = recv(s->stream_data.s, buf, (int) size, 0);
		if (nr == SOCKET_ERROR) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
#else
		nr = read(s->stream_data.s, buf, size);
		if (nr == -1) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
#endif
		break;
	}
	if (nr == 0)
		return 0;	/* end of file */
	while (elmsize > 1 && nr % elmsize != 0) {
		/* if elmsize > 1, we really expect that "the other
		 * side" wrote complete items in a single system call,
		 * so we expect to at least receive complete items,
		 * and hence we continue reading until we did in fact
		 * receive an integral number of complete items,
		 * ignoring any timeouts (but not real errors)
		 * (note that recursion is limited since we don't
		 * propagate the element size to the recursive
		 * call) */
		ssize_t n;
		n = socket_read(s, (char *) buf + nr, 1, (size_t) (size - nr));
		if (n < 0) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
		if (n == 0)	/* unexpected end of file */
			break;
		nr += n;
	}
	return (ssize_t) (nr / elmsize);
}

static void
socket_close(stream *s)
{
	SOCKET fd = s->stream_data.s;

	if (fd != INVALID_SOCKET) {
		/* Related read/write (in/out, from/to) streams
		 * share a single socket which is not dup'ed (anymore)
		 * as Windows' dup doesn't work on sockets;
		 * hence, only one of the streams must/may close that
		 * socket; we choose to let the read socket do the
		 * job, since in mapi.mx it may happen that the read
		 * stream is closed before the write stream was even
		 * created.
		 */
		if (s->access == ST_READ) {
#ifdef HAVE_SHUTDOWN
			shutdown(fd, SHUT_RDWR);
#endif
			closesocket(fd);
		}
	}
	s->stream_data.s = INVALID_SOCKET;
}

static void
socket_update_timeout(stream *s)
{
	SOCKET fd = s->stream_data.s;
	struct timeval tv;

	if (fd == INVALID_SOCKET)
		return;
	tv.tv_sec = s->timeout / 1000;
	tv.tv_usec = (s->timeout % 1000) * 1000;
	/* cast to char * for Windows, no harm on "normal" systems */
	if (s->access == ST_WRITE)
		(void) setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *) &tv, (socklen_t) sizeof(tv));
}

#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif

static int
socket_isalive(stream *s)
{
	SOCKET fd = s->stream_data.s;
	char buffer[32];
	fd_set fds;
	struct timeval t;

	t.tv_sec = 0;
	t.tv_usec = 0;
	FD_ZERO(&fds);
	FD_SET(fd, &fds);
	return select(
#ifdef _MSC_VER
		0,		/* ignored on Windows */
#else
		fd + 1,
#endif
		&fds, NULL, NULL, &t) <= 0 ||
		recv(fd, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT) != 0;
}

static stream *
socket_open(SOCKET sock, const char *name)
{
	stream *s;
	int domain = 0;

	if (sock == INVALID_SOCKET)
		return NULL;
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->read = socket_read;
	s->write = socket_write;
	s->close = socket_close;
	s->stream_data.s = sock;
	s->update_timeout = socket_update_timeout;
	s->isalive = socket_isalive;

	errno = 0;
#ifdef _MSC_VER
	WSASetLastError(0);
#endif
#if defined(SO_DOMAIN)
	{
		socklen_t len = (socklen_t) sizeof(domain);
		if (getsockopt(sock, SOL_SOCKET, SO_DOMAIN, (void *) &domain, &len) == SOCKET_ERROR)
			domain = AF_INET; /* give it a value if call fails */
	}
#endif
#if defined(SO_KEEPALIVE) && !defined(WIN32)
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
		int opt = 1;
		(void) setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *) &opt, sizeof(opt));
	}
#endif
#if defined(IPTOS_THROUGHPUT) && !defined(WIN32)
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
		int tos = IPTOS_THROUGHPUT;

		(void) setsockopt(sock, IPPROTO_IP, IP_TOS, (void *) &tos, sizeof(tos));
	}
#endif
#ifdef TCP_NODELAY
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
		int nodelay = 1;

		(void) setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (void *) &nodelay, sizeof(nodelay));
	}
#endif
#ifdef HAVE_FCNTL
	{
		int fl = fcntl(sock, F_GETFL);

		fl &= ~O_NONBLOCK;
		if (fcntl(sock, F_SETFL, fl) < 0) {
			s->errnr = MNSTR_OPEN_ERROR;
			return s;
		}
	}
#endif

	return s;
}

stream *
socket_rstream(SOCKET sock, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	fprintf(stderr, "socket_rstream " SSZFMT " %s\n", (ssize_t) sock, name);
#endif
	if ((s = socket_open(sock, name)) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    socket_read(s, (void *) &s->byteorder, sizeof(s->byteorder), 1) < 1) {
		socket_close(s);
		s->errnr = MNSTR_OPEN_ERROR;
	}
	return s;
}

stream *
socket_wstream(SOCKET sock, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	fprintf(stderr, "socket_wstream " SSZFMT " %s\n", (ssize_t) sock, name);
#endif
	if ((s = socket_open(sock, name)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    socket_write(s, (void *) &s->byteorder, sizeof(s->byteorder), 1) < 1) {
		socket_close(s);
		s->errnr = MNSTR_OPEN_ERROR;
	}
	return s;
}

stream *
socket_rastream(SOCKET sock, const char *name)
{
	stream *s = NULL;

#ifdef STREAM_DEBUG
	fprintf(stderr, "socket_rastream " SSZFMT " %s\n", (ssize_t) sock, name);
#endif
	if ((s = socket_open(sock, name)) != NULL)
		s->type = ST_ASCII;
	return s;
}

stream *
socket_wastream(SOCKET sock, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	fprintf(stderr, "socket_wastream " SSZFMT " %s\n", (ssize_t) sock, name);
#endif
	if ((s = socket_open(sock, name)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}

/* ------------------------------------------------------------------ */
/* streams working on a UDP socket */

typedef struct udp_stream {
	SOCKET s;
	struct sockaddr_in addr;
} udp_stream;

static ssize_t
udp_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	ssize_t res = 0, size = (ssize_t) (elmsize * cnt);
	udp_stream *udp;
	int addrlen;

	udp = s->stream_data.p;
	if (s->errnr || udp == NULL)
		return -1;

	if (size == 0 || elmsize == 0)
		return (ssize_t) cnt;
	addrlen = sizeof(udp->addr);
	errno = 0;
#ifdef _MSC_VER
	WSASetLastError(0);
#endif
	if ((res = sendto(udp->s, buf,
#ifdef NATIVE_WIN32
			  (int)	/* on Windows, the length is an int... */
#endif
			  size, 0, (struct sockaddr *) &udp->addr, addrlen)) < 0) {
		s->errnr = MNSTR_WRITE_ERROR;
		return res;
	}
	if (res > 0)
		return (ssize_t) (res / elmsize);
	return 0;
}

static ssize_t
udp_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	ssize_t res = 0, size = (ssize_t) (elmsize * cnt);
	struct sockaddr_in from;
	socklen_t fromlen = sizeof(struct sockaddr_in);
	udp_stream *udp;

	udp = s->stream_data.p;
	if (s->errnr || udp == NULL)
		return -1;

	if (size == 0)
		return 0;
	errno = 0;
#ifdef _MSC_VER
	WSASetLastError(0);
#endif
	if ((res = recvfrom(udp->s, buf,
#ifdef NATIVE_WIN32
			    (int)	/* on Windows, the length is an int... */
#endif
			    size, 0, (struct sockaddr *) &from, &fromlen)) < 0) {
		s->errnr = MNSTR_READ_ERROR;
		return res;
	}
	if (res > 0)
		return (ssize_t) (res / elmsize);
	return 0;
}

static void
udp_close(stream *s)
{
	udp_stream *udp = s->stream_data.p;

	if (udp) {
#ifdef HAVE_SHUTDOWN
		shutdown(udp->s, SHUT_RDWR);
#endif
		closesocket(udp->s);
	}
}

static void
udp_destroy(stream *s)
{
	if (s->stream_data.p)
		free(s->stream_data.p);
	destroy(s);
}

static stream *
udp_create(const char *name)
{
	stream *s;
	udp_stream *udp = NULL;

	if ((s = create_stream(name)) == NULL)
		return NULL;
	if ((udp = (udp_stream *) malloc(sizeof(udp_stream))) == NULL) {
		destroy(s);
		return NULL;
	}
	s->read = udp_read;
	s->write = udp_write;
	s->close = udp_close;
	s->destroy = udp_destroy;
	s->stream_data.p = udp;

	errno = 0;
#ifdef _MSC_VER
	WSASetLastError(0);
#endif
	return s;
}

static int
udp_socket(udp_stream * udp, const char *hostname, int port, int write)
{
#ifdef HAVE_GETADDRINFO
	struct addrinfo hints, *res, *rp;
	char sport[32];

	snprintf(sport, sizeof(sport), "%d", port);
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; /* IPv4 or IPv6 */
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_flags = AI_PASSIVE;
	hints.ai_protocol = IPPROTO_UDP;
	if (getaddrinfo(hostname, sport, &hints, &res))
		return -1;
	memset(&udp->addr, 0, sizeof(udp->addr));
	for (rp = res; rp; rp = rp->ai_next) {
		udp->s = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (udp->s == INVALID_SOCKET)
			continue;
		if (!write &&
		    bind(udp->s, rp->ai_addr, 
#ifdef _MSC_VER
			 (int)	/* Windows got the interface wrong... */
#endif
			 rp->ai_addrlen) == SOCKET_ERROR) {
			closesocket(udp->s);
			continue;
		}
		memcpy(&udp->addr, rp->ai_addr, rp->ai_addrlen);
		freeaddrinfo(res);
		return 0;
	}
	freeaddrinfo(res);
	return -1;
#else
	struct sockaddr *serv;
	socklen_t servsize;
	struct hostent *hp;

	hp = gethostbyname(hostname);
	if (hp == NULL)
		return -1;

	memset(&udp->addr, 0, sizeof(udp->addr));
	if (write)
		memcpy(&udp->addr.sin_addr, hp->h_addr_list[0], hp->h_length);
	else
		udp->addr.sin_addr.s_addr = INADDR_ANY;
	udp->addr.sin_family = hp->h_addrtype;
	udp->addr.sin_port = htons((unsigned short) (port & 0xFFFF));
	serv = (struct sockaddr *) &udp->addr;
	servsize = (socklen_t) sizeof(udp->addr);
	udp->s = socket(serv->sa_family, SOCK_DGRAM, IPPROTO_UDP);
	if (udp->s == INVALID_SOCKET)
		return -1;
	if (!write && bind(udp->s, serv, servsize) == SOCKET_ERROR)
		return -1;
	return 0;
#endif
}

stream *
udp_rastream(const char *hostname, int port, const char *name)
{
	stream *s;

	if (hostname == NULL || name == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "udp_rawastream %s %s\n", hostname, name);
#endif
	s = udp_create(name);
	if (s == NULL)
		return NULL;
	if (udp_socket(s->stream_data.p, hostname, port, 0) < 0) {
		udp_destroy(s);
		return NULL;
	}
	s->type = ST_ASCII;
	return s;
}

stream *
udp_wastream(const char *hostname, int port, const char *name)
{
	stream *s;

	if (hostname == NULL || name == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "udp_wastream %s %s\n", hostname, name);
#endif
	s = udp_create(name);
	if (s == NULL)
		return NULL;
	if (udp_socket(s->stream_data.p, hostname, port, 1) < 0) {
		udp_destroy(s);
		return NULL;
	}
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}

/* ------------------------------------------------------------------ */
/* streams working on an open file pointer */

#ifdef _MSC_VER
/* special case code for reading from/writing to a Windows cmd window */

struct console {
	HANDLE h;
	DWORD len;
	DWORD rd;
	unsigned char i;
	WCHAR wbuf[8192];
};

static ssize_t
console_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	struct console *c = s->stream_data.p;
	size_t n = elmsize * cnt;
	unsigned char *p = buf;

	if (c == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}
	if (n == 0)
		return 0;
	if (c->rd == c->len) {
		if (!ReadConsoleW(c->h, c->wbuf, 8192, &c->len, NULL)) {
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}
		c->rd = 0;
		if (c->len > 0 && c->wbuf[0] == 26) { /* control-Z */
			c->len = 0;
			return 0;
		}
		if (c->len > 0 && c->wbuf[0] == 0xFEFF)
			c->rd++; /* skip BOM */
	}
	while (n > 0 && c->rd < c->len) {
		if (c->wbuf[c->rd] == L'\r') {
			/* skip CR */
			c->rd++;
		} else if (c->wbuf[c->rd] <= 0x7F) {
			/* old-fashioned ASCII */
			*p++ = (unsigned char) c->wbuf[c->rd++];
			n--;
		} else if (c->wbuf[c->rd] <= 0x7FF) {
			if (c->i == 0) {
				*p++ = 0xC0 | (c->wbuf[c->rd] >> 6);
				c->i = 1;
				n--;
			}
			if (c->i == 1 && n > 0) {
				*p++ = 0x80 | (c->wbuf[c->rd++] & 0x3F);
				c->i = 0;
				n--;
			}
		} else if ((c->wbuf[c->rd] & 0xFC00) == 0xD800) {
			/* high surrogate */
			/* Unicode code points U+10000 and
			 * higher cannot be represented in two
			 * bytes in UTF-16.  Instead they are
			 * represented in four bytes using so
			 * called high and low surrogates.
			 * 00000000000uuuuuxxxxyyyyyyzzzzzz
			 * 110110wwwwxxxxyy 110111yyyyzzzzzz
			 * -> 11110uuu 10uuxxxx 10yyyyyy 10zzzzzz
			 * where uuuuu = wwww + 1 */
			if (c->i == 0) {
				*p++ = 0xF0 | (((c->wbuf[c->rd] & 0x03C0) + 0x0040) >> 8);
				c->i = 1;
				n--;
			}
			if (c->i == 1 && n > 0) {
				*p++ = 0x80 | ((((c->wbuf[c->rd] & 0x03FC) + 0x0040) >> 2) & 0x3F);
				c->i = 2;
				n--;
			}
			if (c->i == 2 && n > 0) {
				*p = 0x80 | ((c->wbuf[c->rd++] & 0x0003) << 4);
				c->i = 3;
			}
		} else if ((c->wbuf[c->rd] & 0xFC00) == 0xDC00) {
			/* low surrogate */
			if (c->i == 3) {
				*p++ |= (c->wbuf[c->rd] & 0x03C0) >> 6;
				c->i = 4;
				n--;
			}
			if (c->i == 4 && n > 0) {
				*p++ = 0x80 | (c->wbuf[c->rd++] & 0x3F);
				c->i = 0;
				n--;
			}
		} else {
			if (c->i == 0) {
				*p++ = 0xE0 | (c->wbuf[c->rd] >> 12);
				c->i = 1;
				n--;
			}
			if (c->i == 1 && n > 0) {
				*p++ = 0x80 | ((c->wbuf[c->rd] >> 6) & 0x3F);
				c->i = 2;
				n--;
			}
			if (c->i == 2 && n > 0) {
				*p++ = 0x80 | (c->wbuf[c->rd++] & 0x3F);
				c->i = 0;
				n--;
			}
		}
	}
	return (ssize_t) ((p - (unsigned char *) buf) / elmsize);
}

static ssize_t
console_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	struct console *c = s->stream_data.p;
	size_t n = elmsize * cnt;
	const unsigned char *p = buf;

	if (c == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	if (n == 0)
		return 0;

	c->len = 0;
	while (n > 0) {
		if (c->len >= 8191) {
			if (!WriteConsoleW(c->h, c->wbuf, c->len, &c->rd, NULL)) {
				s->errnr = MNSTR_WRITE_ERROR;
				return -1;
			}
			c->len = 0;
		}
		if ((*p & 0x80) == 0) {
			if (*p == '\n')
				c->wbuf[c->len++] = L'\r';
			c->wbuf[c->len++] = *p++;
			n--;
		} else if ((*p & 0xE0) == 0xC0 &&
			   n >= 2 &&
			   (p[1] & 0xC0) == 0x80) {
			c->wbuf[c->len++] = ((p[0] & 0x1F) << 6) |
				(p[1] & 0x3F);
			p += 2;
			n -= 2;
		} else if ((*p & 0xF0) == 0xE0 &&
			   n >= 3 &&
			   (p[1] & 0xC0) == 0x80 &&
			   (p[2] & 0xC0) == 0x80) {
			c->wbuf[c->len++] = ((p[0] & 0x0F) << 12) |
				((p[1] & 0x3F) << 6) |
				(p[2] & 0x3F);
			p += 3;
			n -= 3;
		} else if ((*p & 0xF8) == 0xF0 &&
			   n >= 4 &&
			   (p[1] & 0xC0) == 0x80 &&
			   (p[2] & 0xC0) == 0x80 &&
			   (p[3] & 0xC0) == 0x80) {
			c->wbuf[c->len++] = 0xD800 |
				((((p[0] & 0x07) << 8) | ((p[1] & 0x3F) << 2)) - 0x0040) |
				((p[2] & 0x30) >> 4);
			c->wbuf[c->len++] = 0xDC00 | ((p[2] & 0x0F) << 6) |
				(p[3] & 0x3F);
			p += 4;
			n -= 4;
		} else {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
	}
	if (c->len > 0) {
		if (!WriteConsoleW(c->h, c->wbuf, c->len, &c->rd, NULL)) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		c->len = 0;
	}
	return (ssize_t) ((p - (const unsigned char *) buf) / elmsize);
}

static void
console_destroy(stream *s)
{
	if (s->stream_data.p)
		free(s->stream_data.p);
	destroy(s);
}
#endif

static stream *
file_stream(const char *name)
{
	stream *s;

	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->read = file_read;
	s->write = file_write;
	s->close = file_close;
	s->flush = file_flush;
	s->fsync = file_fsync;
	s->fgetpos = file_fgetpos;
	s->fsetpos = file_fsetpos;
	return s;
}

stream *
file_rstream(FILE *fp, const char *name)
{
	stream *s;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_rstream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->type = ST_BIN;

	if (s->errnr == MNSTR_NO__ERROR &&
	    (fread((void *) &s->byteorder, sizeof(s->byteorder), 1, fp) < 1 ||
	     ferror(fp))) {
		fclose(fp);
		destroy(s);
		return NULL;
	}
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
file_wstream(FILE *fp, const char *name)
{
	stream *s;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_wstream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;

	if (s->errnr == MNSTR_NO__ERROR &&
	    (fwrite((void *) &s->byteorder, sizeof(s->byteorder), 1, fp) < 1 ||
	     ferror(fp))) {
		fclose(fp);
		destroy(s);
		return NULL;
	}
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
file_rastream(FILE *fp, const char *name)
{
	stream *s;
	lng pos;
	char buf[UTF8BOMLENGTH + 1];
	struct stat stb;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_rastream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->type = ST_ASCII;
	s->stream_data.p = (void *) fp;
	if (fstat(fileno(fp), &stb) == 0 && S_ISREG(stb.st_mode) && file_fgetpos(s, &pos) == 0) {
		if (file_read(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
			s->isutf8 = 1;
			return s;
		}
		file_fsetpos(s, pos);
	}
#ifdef _MSC_VER
	if (_fileno(fp) == 0 && _isatty(0)) {
		struct console *c = malloc(sizeof(struct console));
		s->stream_data.p = c;
		c->h = GetStdHandle(STD_INPUT_HANDLE);
		c->i = 0;
		c->len = 0;
		c->rd = 0;
		s->read = console_read;
		s->write = NULL;
		s->destroy = console_destroy;
		s->close = NULL;
		s->flush = NULL;
		s->fsync = NULL;
		s->fgetpos = NULL;
		s->fsetpos = NULL;
		s->isutf8 = 1;
		return s;
	}
#endif
	return s;
}

stream *
file_wastream(FILE *fp, const char *name)
{
	stream *s;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_wastream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
#ifdef _MSC_VER
	if ((_fileno(fp) == 1 || _fileno(fp) == 2) && _isatty(_fileno(fp))) {
		struct console *c = malloc(sizeof(struct console));
		s->stream_data.p = c;
		c->h = GetStdHandle(STD_OUTPUT_HANDLE);
		c->i = 0;
		c->len = 0;
		c->rd = 0;
		s->read = NULL;
		s->write = console_write;
		s->destroy = console_destroy;
		s->close = NULL;
		s->flush = NULL;
		s->fsync = NULL;
		s->fgetpos = NULL;
		s->fsetpos = NULL;
		s->isutf8 = 1;
		return s;
	}
#endif
	s->stream_data.p = (void *) fp;
	return s;
}

/* ------------------------------------------------------------------ */
/* streams working on a substream, converting character sets using iconv */

#ifdef HAVE_ICONV

struct icstream {
	iconv_t cd;
	stream *s;
	char buffer[BUFSIZ];
	size_t buflen;
	int eof;
};

static ssize_t
ic_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;
	ICONV_CONST char *inbuf = (ICONV_CONST char *) buf;
	size_t inbytesleft = elmsize * cnt;
	char *bf = NULL;

	if (ic == NULL)
		goto bailout;

	/* if unconverted data from a previous call remains, add it to
	 * the start of the new data, using temporary space */
	if (ic->buflen > 0) {
		bf = malloc(ic->buflen + inbytesleft);
		if (bf == NULL) {
			/* cannot allocate memory */
			goto bailout;
		}
		memcpy(bf, ic->buffer, ic->buflen);
		memcpy(bf + ic->buflen, buf, inbytesleft);
		buf = bf;
		inbytesleft += ic->buflen;
		ic->buflen = 0;
	}
	while (inbytesleft > 0) {
		char *outbuf = ic->buffer;
		size_t outbytesleft = sizeof(ic->buffer);

		if (iconv(ic->cd, &inbuf, &inbytesleft, &outbuf, &outbytesleft) == (size_t) -1) {
			switch (errno) {
			case EILSEQ:
				/* invalid multibyte sequence encountered */
				goto bailout;
			case EINVAL:
				/* incomplete multibyte sequence
				 * encountered flush what has been
				 * converted */
				if (outbytesleft < sizeof(ic->buffer) &&
				    mnstr_write(ic->s, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft) < 0) {
					goto bailout;
				}
				/* remember what hasn't been converted */
				if (inbytesleft > sizeof(ic->buffer)) {
					/* ridiculously long multibyte
					 * sequence, so return
					 * error */
					goto bailout;
				}
				memcpy(ic->buffer, inbuf, inbytesleft);
				ic->buflen = inbytesleft;
				if (bf)
					free(bf);
				return (ssize_t) cnt;
			case E2BIG:
				/* not enough space in output buffer */
				break;
			default:
				/* cannot happen (according to manual) */
				goto bailout;
			}
		}
		if (mnstr_write(ic->s, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft) < 0) {
			goto bailout;
		}
	}
	if (bf)
		free(bf);
	return (ssize_t) cnt;

  bailout:
	s->errnr = MNSTR_WRITE_ERROR;
	if (bf)
		free(bf);
	return -1;
}

static ssize_t
ic_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;
	ICONV_CONST char *inbuf;
	size_t inbytesleft;
	char *outbuf;
	size_t outbytesleft;

	if (ic == NULL) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}
	inbuf = ic->buffer;
	inbytesleft = ic->buflen;
	outbuf = (char *) buf;
	outbytesleft = elmsize * cnt;
	if (outbytesleft == 0)
		return 0;
	while (outbytesleft > 0 && !ic->eof) {
		if (ic->buflen == sizeof(ic->buffer)) {
			/* ridiculously long multibyte sequence, return error */
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}

		switch (mnstr_read(ic->s, ic->buffer + ic->buflen, 1, 1)) {
		case 1:
			/* expected: read one byte */
			ic->buflen++;
			inbytesleft++;
			break;
		case 0:
			/* end of file */
			ic->eof = 1;
			if (ic->buflen > 0) {
				/* incomplete input */
				s->errnr = MNSTR_READ_ERROR;
				return -1;
			}
			if (iconv(ic->cd, NULL, NULL, &outbuf, &outbytesleft) == (size_t) -1) {
				/* some error occurred */
				s->errnr = MNSTR_READ_ERROR;
				return -1;
			}
			goto exit_func; /* double break */
		default:
			/* error */
			s->errnr = ic->s->errnr;
			return -1;
		}
		if (iconv(ic->cd, &inbuf, &inbytesleft, &outbuf, &outbytesleft) == (size_t) -1) {
			switch (errno) {
			case EILSEQ:
				/* invalid multibyte sequence encountered */
				s->errnr = MNSTR_READ_ERROR;
				return -1;
			case EINVAL:
				/* incomplete multibyte sequence encountered */
				break;
			case E2BIG:
				/* not enough space in output buffer,
				 * return what we have, saving what's in
				 * the buffer */
				goto exit_func;
			default:
				/* cannot happen (according to manual) */
				s->errnr = MNSTR_READ_ERROR;
				return -1;
			}
		}
		if (inbytesleft == 0) {
			/* converted complete buffer */
			inbuf = ic->buffer;
			ic->buflen = 0;
		}
	}
      exit_func:
	if (inbuf > ic->buffer)
		memmove(ic->buffer, inbuf, inbytesleft);
	ic->buflen = inbytesleft;
	if (outbytesleft == elmsize * cnt) {
		/* if we're returning data, we must pass on EOF on the
		 * next call (i.e. keep ic->eof set), otherwise we
		 * must clear it so that the next call will cause the
		 * underlying stream to be read again */
		ic->eof = 0;
	}
	return (ssize_t) ((elmsize * cnt - outbytesleft) / elmsize);
}

static int
ic_flush(stream *s)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;
	char *outbuf;
	size_t outbytesleft;

	if (ic == NULL)
		return -1;
	outbuf = ic->buffer;
	outbytesleft = sizeof(ic->buffer);
	/* if unconverted data from a previous call remains, it was an
	 * incomplete multibyte sequence, so an error */
	if (ic->buflen > 0 ||
	    iconv(ic->cd, NULL, NULL, &outbuf, &outbytesleft) == (size_t) -1 ||
	    (outbytesleft < sizeof(ic->buffer) &&
	     mnstr_write(ic->s, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft) < 0)) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	return mnstr_flush(ic->s);
}

static void
ic_close(stream *s)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;

	if (ic) {
		ic_flush(s);
		iconv_close(ic->cd);
		mnstr_close(ic->s);
		free(s->stream_data.p);
		s->stream_data.p = NULL;
	}
}

static void
ic_update_timeout(stream *s)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;

	if (ic && ic->s) {
		ic->s->timeout = s->timeout;
		ic->s->timeout_func = s->timeout_func;
		if (ic->s->update_timeout)
			(*ic->s->update_timeout)(ic->s);
	}
}

static int
ic_isalive(stream *s)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;

	if (ic && ic->s) {
		if (ic->s->isalive)
			return (*ic->s->isalive)(ic->s);
		return 1;
	}
	return 0;
}

static void
ic_clrerr(stream *s)
{
	if (s->stream_data.p)
		mnstr_clearerr(((struct icstream *) s->stream_data.p)->s);
}

static stream *
ic_open(iconv_t cd, stream *ss, const char *name)
{
	stream *s;
	struct icstream *ic;

	if (ss->isutf8)
		return ss;
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->read = ic_read;
	s->write = ic_write;
	s->close = ic_close;
	s->clrerr = ic_clrerr;
	s->flush = ic_flush;
	s->update_timeout = ic_update_timeout;
	s->isalive = ic_isalive;
	s->stream_data.p = malloc(sizeof(struct icstream));
	if (s->stream_data.p == NULL) {
		mnstr_destroy(s);
		return NULL;
	}
	ic = (struct icstream *) s->stream_data.p;
	ic->cd = cd;
	ic->s = ss;
	ic->buflen = 0;
	ic->eof = 0;
	return s;
}

stream *
iconv_rstream(stream *ss, const char *charset, const char *name)
{
	stream *s;
	iconv_t cd;

	if (ss == NULL || charset == NULL || name == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "iconv_rstream %s %s\n", charset, name);
#endif
	if (ss->isutf8)
		return ss;
	cd = iconv_open("utf-8", charset);
	if (cd == (iconv_t) - 1)
		return NULL;
	s = ic_open(cd, ss, name);
	if (s == NULL) {
		iconv_close(cd);
		return NULL;
	}
	s->access = ST_READ;
	s->isutf8 = 1;
	return s;
}

stream *
iconv_wstream(stream *ss, const char *charset, const char *name)
{
	stream *s;
	iconv_t cd;

	if (ss == NULL || charset == NULL || name == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "iconv_wstream %s %s\n", charset, name);
#endif
	if (ss->isutf8)
		return ss;
	cd = iconv_open(charset, "utf-8");
	if (cd == (iconv_t) - 1)
		return NULL;
	s = ic_open(cd, ss, name);
	if (s == NULL) {
		iconv_close(cd);
		return NULL;
	}
	s->access = ST_WRITE;
	return s;
}

#else
stream *
iconv_rstream(stream *ss, const char *charset, const char *name)
{
	if (ss == NULL || charset == NULL || name == NULL)
		return NULL;
	if (ss->isutf8 ||
	    strcmp(charset, "utf-8") == 0 ||
	    strcmp(charset, "UTF-8") == 0 ||
	    strcmp(charset, "UTF8") == 0)
		return ss;

	return NULL;
}

stream *
iconv_wstream(stream *ss, const char *charset, const char *name)
{
	if (ss == NULL || charset == NULL || name == NULL)
		return NULL;
	if (ss->isutf8 ||
	    strcmp(charset, "utf-8") == 0 ||
	    strcmp(charset, "UTF-8") == 0 ||
	    strcmp(charset, "UTF8") == 0)
		return ss;

	return NULL;
}
#endif /* HAVE_ICONV */

/* ------------------------------------------------------------------ */

void
buffer_init(buffer *b, char *buf, size_t size)
{
	if (b == NULL || buf == NULL)
		return;
	b->pos = 0;
	b->buf = buf;
	b->len = size;
}

buffer *
buffer_create(size_t size)
{
	buffer *b;

	if ((b = malloc(sizeof(*b))) == NULL)
		return NULL;
	b->pos = 0;
	b->buf = malloc(size);
	if (b->buf == NULL) {
		free(b);
		return NULL;
	}
	b->len = size;
	return b;
}

char *
buffer_get_buf(buffer *b)
{
	char *r;

	if (b == NULL)
		return NULL;
	if (b->pos == b->len && (b->buf = realloc(b->buf, b->len + 1)) == NULL)
		return NULL;
	r = b->buf;
	r[b->pos] = '\0';
	b->buf = malloc(b->len);
	b->len = b->buf ? b->len : 0;
	b->pos = 0;
	return r;
}

void
buffer_destroy(buffer *b)
{
	if (b == NULL)
		return;
	if (b->buf)
		free(b->buf);
	free(b);
}

buffer *
mnstr_get_buffer(stream *s)
{
	if (s == NULL)
		return NULL;
	return (buffer *) s->stream_data.p;
}

static ssize_t
buffer_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (size && b && b->pos + size <= b->len) {
		memcpy(buf, b->buf + b->pos, size);
		b->pos += size;
		return (ssize_t) (size / elmsize);
	}
	return 0;
}

static ssize_t
buffer_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (b == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	if (b->pos + size > b->len) {
		size_t ns = b->len;

		while (b->pos + size > ns)
			ns *= 2;
		if ((b->buf = realloc(b->buf, ns)) == NULL) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		b->len = ns;
	}
	memcpy(b->buf + b->pos, buf, size);
	b->pos += size;
	return (ssize_t) cnt;
}

static void
buffer_close(stream *s)
{
	(void) s;
}

static int
buffer_flush(stream *s)
{
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (b == NULL)
		return -1;
	b->pos = 0;
	return 0;
}

stream *
buffer_rastream(buffer *b, const char *name)
{
	stream *s;

	if (b == NULL || name == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "buffer_rastream %s\n", name);
#endif
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->type = ST_ASCII;
	s->read = buffer_read;
	s->write = buffer_write;
	s->close = buffer_close;
	s->flush = buffer_flush;
	s->stream_data.p = (void *) b;
	return s;
}

stream *
buffer_wastream(buffer *b, const char *name)
{
	stream *s;

	if (b == NULL || name == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "buffer_wastream %s\n", name);
#endif
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	s->read = buffer_read;
	s->write = buffer_write;
	s->close = buffer_close;
	s->flush = buffer_flush;
	s->stream_data.p = (void *) b;
	return s;
}

/* ------------------------------------------------------------------ */

/* A buffered stream consists of a sequence of blocks.  Each block
 * consists of a count followed by the data in the block.  A flush is
 * indicated by an empty block (i.e. just a count of 0).
 */
typedef struct bs {
	stream *s;		/* underlying stream */
	unsigned nr;		/* how far we got in buf */
	unsigned itotal;	/* amount available in current read block */
	size_t blks;		/* read/writen blocks (possibly partial) */
	size_t bytes;		/* read/writen bytes */
	char buf[BLOCK];	/* the buffered data (minus the size of
				 * size-short */
} bs;

static bs *
bs_create(stream *s)
{
	/* should be a binary stream */
	bs *ns;

	if ((ns = malloc(sizeof(*ns))) == NULL)
		return NULL;
	ns->s = s;
	ns->nr = 0;
	ns->itotal = 0;
	ns->blks = 0;
	ns->bytes = 0;
	return ns;
}

/* Collect data until the internal buffer is filled, then write the
 * filled buffer to the underlying stream.
 * Struct field usage:
 * s - the underlying stream;
 * buf - the buffer in which data is collected;
 * nr - how much of buf is already filled (if nr == sizeof(buf) the
 *      data is written to the underlying stream, so upon entry nr <
 *      sizeof(buf));
 * itotal - unused.
 */
static ssize_t
bs_write(stream *ss, const void *buf, size_t elmsize, size_t cnt)
{
	bs *s;
	size_t todo = cnt * elmsize;
	short blksize;

	s = (bs *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(ss->access == ST_WRITE);
	assert(s->nr < sizeof(s->buf));
	while (todo > 0) {
		size_t n = sizeof(s->buf) - s->nr;

		if (todo < n)
			n = todo;
		memcpy(s->buf + s->nr, buf, n);
		s->nr += (unsigned) n;
		todo -= n;
		buf = ((const char *) buf + n);
		if (s->nr == sizeof(s->buf)) {
			/* block is full, write it to the stream */
#ifdef BSTREAM_DEBUG
			{
				unsigned i;

				fprintf(stderr, "W %s %u \"", ss->name, s->nr);
				for (i = 0; i < s->nr; i++)
					if (' ' <= s->buf[i] && s->buf[i] < 127)
						putc(s->buf[i], stderr);
					else
						fprintf(stderr, "\\%03o", s->buf[i]);
				fprintf(stderr, "\"\n");
			}
#endif
			/* since the block is at max BLOCK (8K) - 2 size we can
			 * store it in a two byte integer */
			blksize = (short) s->nr;
			s->bytes += s->nr;
			/* the last bit tells whether a flush is in there, it's not
			 * at this moment, so shift it to the left */
			blksize <<= 1;
#ifdef WORDS_BIGENDIAN
			blksize = short_int_SWAP(blksize);
#endif
			if (!mnstr_writeSht(s->s, blksize) || s->s->write(s->s, s->buf, 1, s->nr) != (ssize_t) s->nr) {
				ss->errnr = MNSTR_WRITE_ERROR;
				return -1;
			}
			s->blks++;
			s->nr = 0;
		}
	}
	return (ssize_t) cnt;
}

/* If the internal buffer is partially filled, write it to the
 * underlying stream.  Then in any case write an empty buffer to the
 * underlying stream to indicate to the receiver that the data was
 * flushed.
 */
static int
bs_flush(stream *ss)
{
	short blksize;
	bs *s;

	s = (bs *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(ss->access == ST_WRITE);
	assert(s->nr < sizeof(s->buf));
	if (ss->access == ST_WRITE) {
		/* flush the rest of buffer (if s->nr > 0), then set the
		 * last bit to 1 to to indicate user-instigated flush */
#ifdef BSTREAM_DEBUG
		if (s->nr > 0) {
			unsigned i;

			fprintf(stderr, "W %s %u \"", ss->name, s->nr);
			for (i = 0; i < s->nr; i++)
				if (' ' <= s->buf[i] && s->buf[i] < 127)
					putc(s->buf[i], stderr);
				else
					fprintf(stderr, "\\%03o", s->buf[i]);
			fprintf(stderr, "\"\n");
			fprintf(stderr, "W %s 0\n", ss->name);
		}
#endif
		blksize = (short) (s->nr << 1);
		s->bytes += s->nr;
		/* indicate that this is the last buffer of a block by
		 * setting the low-order bit */
		blksize |= (short) 1;
		/* allways flush (even empty blocks) needed for the protocol) */
#ifdef WORDS_BIGENDIAN
		blksize = short_int_SWAP(blksize);
#endif
		if ((!mnstr_writeSht(s->s, blksize) ||
		     (s->nr > 0 &&
		      s->s->write(s->s, s->buf, 1, s->nr) != (ssize_t) s->nr))) {
			ss->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		s->blks++;
		s->nr = 0;
	}
	return 0;
}

/* Read buffered data and return the number of items read.  At the
 * flush boundary we will return 0 to indicate the end of a block.
 *
 * Structure field usage:
 * s - the underlying stream;
 * buf - not used;
 * itotal - the amount of data in the current block that hasn't yet
 *          been read;
 * nr - indicates whether the flush marker has to be returned.
 */
static ssize_t
bs_read(stream *ss, void *buf, size_t elmsize, size_t cnt)
{
	bs *s;
	size_t todo = cnt * elmsize;
	size_t n;

	s = (bs *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(ss->access == ST_READ);
	assert(s->nr <= 1);

	if (s->itotal == 0) {
		short blksize = 0;

		if (s->nr) {
			/* We read the closing block but hadn't
			 * returned that yet. Return it now, and note
			 * that we did by setting s->nr to 0. */
			assert(s->nr == 1);
			s->nr = 0;
			return 0;
		}

		assert(s->nr == 0);

		/* There is nothing more to read in the current block,
		 * so read the count for the next block */
		switch (mnstr_readSht(s->s, &blksize)) {
		case -1:
			ss->errnr = s->s->errnr;
			return -1;
		case 0:
			return 0;
		case 1:
			break;
		}
		if (blksize < 0) {
			ss->errnr = MNSTR_READ_ERROR;
			return -1;
		}
#ifdef BSTREAM_DEBUG
		fprintf(stderr, "RC size: %d, final: %s\n", blksize >> 1, blksize & 1 ? "true" : "false");
		fprintf(stderr, "RC %s %d\n", ss->name, blksize);
#endif
		s->itotal = (unsigned) (blksize >> 1);	/* amount readable */
		/* store whether this was the last block or not */
		s->nr = blksize & 1;
		s->bytes += s->itotal;
		s->blks++;
	}

	/* Fill the caller's buffer. */
	cnt = 0;		/* count how much we put into the buffer */
	while (todo > 0) {
		/* there is more data waiting in the current block, so
		 * read it */
		n = todo < s->itotal ? todo : s->itotal;
		while (n > 0) {
			ssize_t m = s->s->read(s->s, buf, 1, n);

			if (m <= 0) {
				ss->errnr = s->s->errnr;
				return -1;
			}
#ifdef BSTREAM_DEBUG
			{
				ssize_t i;

				fprintf(stderr, "RD %s %zd \"", ss->name, m);
				for (i = 0; i < m; i++)
					if (' ' <= ((char *) buf)[i] && ((char *) buf)[i] < 127)
						putc(((char *) buf)[i], stderr);
					else
						fprintf(stderr, "\\%03o", ((char *) buf)[i]);
				fprintf(stderr, "\"\n");
			}
#endif
			buf = (void *) ((char *) buf + m);
			cnt += m;
			n -= m;
			s->itotal -= (int) m;
			todo -= m;
		}

		if (s->itotal == 0) {
			short blksize = 0;

			/* The current block has been completely read,
			 * so read the count for the next block, only
			 * if the previous was not the last one */
			if (s->nr)
				break;
			switch (mnstr_readSht(s->s, &blksize)) {
			case -1:
				ss->errnr = s->s->errnr;
				return -1;
			case 0:
				return 0;
			case 1:
				break;
			}
			if (blksize < 0) {
				ss->errnr = MNSTR_READ_ERROR;
				return -1;
			}
#ifdef BSTREAM_DEBUG
			fprintf(stderr, "RC size: %d, final: %s\n", blksize >> 1, blksize & 1 ? "true" : "false");
			fprintf(stderr, "RC %s %d\n", ss->name, s->nr);
			fprintf(stderr, "RC %s %d\n", ss->name, blksize);
#endif
			s->itotal = (unsigned) (blksize >> 1);	/* amount readable */
			/* store whether this was the last block or not */
			s->nr = blksize & 1;
			s->bytes += s->itotal;
			s->blks++;
		}
	}
	/* if we got an empty block with the end-of-sequence marker
	 * set (low-order bit) we must only return an empty read once,
	 * so we must squash the flag that we still have to return an
	 * empty read */
	if (todo > 0 && cnt == 0)
		s->nr = 0;
	return (ssize_t) (elmsize > 0 ? cnt / elmsize : 0);
}

static void
bs_update_timeout(stream *ss)
{
	bs *s;

	if ((s = ss->stream_data.p) != NULL && s->s) {
		s->s->timeout = ss->timeout;
		s->s->timeout_func = ss->timeout_func;
		if (s->s->update_timeout)
			(*s->s->update_timeout)(s->s);
	}
}

static int
bs_isalive(stream *ss)
{
	struct bs *s;

	if ((s = ss->stream_data.p) != NULL && s->s) {
		if (s->s->isalive)
			return (*s->s->isalive)(s->s);
		return 1;
	}
	return 0;
}

static void
bs_close(stream *ss)
{
	bs *s;

	s = (bs *) ss->stream_data.p;
	assert(s);
	if (s == NULL)
		return;
	assert(s->s);
	if (s->s)
		s->s->close(s->s);
}

static void
bs_destroy(stream *ss)
{
	bs *s;

	s = (bs *) ss->stream_data.p;
	assert(s);
	if (s) {
		assert(s->s);
		if (s->s)
			s->s->destroy(s->s);
		free(s);
	}
	destroy(ss);
}

static void
bs_clrerr(stream *s)
{
	if (s->stream_data.p)
		mnstr_clearerr(((bs *) s->stream_data.p)->s);
}

stream *
block_stream(stream *s)
{
	stream *ns;
	bs *b;

	if (s == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "block_stream %s\n", s->name ? s->name : "<unnamed>");
#endif
	if ((ns = create_stream(s->name)) == NULL)
		return NULL;
	if ((b = bs_create(s)) == NULL) {
		destroy(ns);
		return NULL;
	}
	/* blocksizes have a fixed little endian byteorder */
#ifdef WORDS_BIGENDIAN
	s->byteorder = 3412;	/* simply != 1234 */
#endif
	ns->type = s->type;
	ns->access = s->access;
	ns->close = bs_close;
	ns->clrerr = bs_clrerr;
	ns->destroy = bs_destroy;
	ns->flush = bs_flush;
	ns->read = bs_read;
	ns->write = bs_write;
	ns->update_timeout = bs_update_timeout;
	ns->isalive = bs_isalive;
	ns->stream_data.p = (void *) b;

	return ns;
}

int
isa_block_stream(stream *s)
{
	assert(s != NULL);
	return s && (s->read == bs_read || s->write == bs_write);
}

/* ------------------------------------------------------------------ */

ssize_t
mnstr_read_block(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	ssize_t len = 0;
	char x = 0;

	if (s == NULL || buf == NULL)
		return -1;
	assert(s->read == bs_read || s->write == bs_write);
	if ((len = mnstr_read(s, buf, elmsize, cnt)) < 0 ||
	    mnstr_read(s, &x, 0, 0) < 0	/* read prompt */ ||
	    x > 0)
		return -1;
	return len;
}

int
mnstr_readBte(stream *s, signed char *val)
{
	if (s == NULL || val == NULL)
		return -1;
	return (int) s->read(s, (void *) val, sizeof(*val), 1);
}

int
mnstr_writeBte(stream *s, signed char val)
{
	if (s == NULL || s->errnr)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), 1) == 1;
}

int
mnstr_readSht(stream *s, short *val)
{
	if (s == NULL || val == NULL)
		return 0;
	switch (s->read(s, (void *) val, sizeof(*val), 1)) {
	case 1:
		if (s->byteorder != 1234)
			*val = short_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeSht(stream *s, short val)
{
	if (s == NULL || s->errnr)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), 1) == 1;
}

int
mnstr_readInt(stream *s, int *val)
{
	if (s == NULL || val == NULL)
		return 0;

	switch (s->read(s, (void *) val, sizeof(*val), 1)) {
	case 1:
		if (s->byteorder != 1234)
			*val = normal_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeInt(stream *s, int val)
{
	if (s == NULL || s->errnr)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), (size_t) 1) == 1;
}

int
mnstr_readLng(stream *s, lng *val)
{
	if (s == NULL || val == NULL)
		return 0;

	switch (s->read(s, (void *) val, sizeof(*val), 1)) {
	case 1:
		if (s->byteorder != 1234)
			*val = long_long_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeLng(stream *s, lng val)
{
	if (s == NULL || s->errnr)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), (size_t) 1) == 1;
}

#ifdef HAVE_HGE
int
mnstr_readHge(stream *s, hge *val)
{
	switch (s->read(s, (void *) val, sizeof(*val), 1)) {
	case 1:
		if (s->byteorder != 1234)
			*val = huge_int_SWAP(*val);
		return 1;
	case 0:
		/* consider EOF an error */
		s->errnr = MNSTR_READ_ERROR;
		/* fall through */
	default:
		/* read failed */
		return 0;
	}
}

int
mnstr_writeHge(stream *s, hge val)
{
	if (!s || s->errnr)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), (size_t) 1) == 1;
}
#endif

int
mnstr_readBteArray(stream *s, signed char *val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;

	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}

	return 1;
}

int
mnstr_writeBteArray(stream *s, const signed char *val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readShtArray(stream *s, short *val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;

	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}

	if (s->byteorder != 1234) {
		size_t i;
		for (i = 0; i < cnt; i++, val++)
			*val = short_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeShtArray(stream *s, const short *val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readIntArray(stream *s, int *val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;

	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}

	if (s->byteorder != 1234) {
		size_t i;
		for (i = 0; i < cnt; i++, val++)
			*val = normal_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeIntArray(stream *s, const int *val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readLngArray(stream *s, lng *val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;

	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}

	if (s->byteorder != 1234) {
		size_t i;
		for (i = 0; i < cnt; i++, val++)
			*val = long_long_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeLngArray(stream *s, const lng *val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

#ifdef HAVE_HGE
int
mnstr_readHgeArray(stream *s, hge *val, size_t cnt)
{
	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
		s->errnr = MNSTR_READ_ERROR;
		return 0;
	}

	if (s->byteorder != 1234) {
		size_t i;
		for (i = 0; i < cnt; i++, val++)
			*val = huge_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeHgeArray(stream *s, const hge *val, size_t cnt)
{
	if (!s || s->errnr)
		return 0;
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}
#endif

int
mnstr_printf(stream *s, const char *format, ...)
{
	char buf[512], *bf = buf;
	int i = 0;
	size_t bfsz = sizeof(buf);
	va_list ap;

	if (s == NULL || s->errnr)
		return -1;

	va_start(ap, format);
	i = vsnprintf(bf, bfsz, format, ap);
	va_end (ap);
	while (i < 0 || (size_t) i >= bfsz) {
		if (i >= 0)	/* glibc 2.1 */
			bfsz = (size_t) i + 1;	/* precisely what is needed */
		else		/* glibc 2.0 */
			bfsz *= 2;	/* twice the old size */
		if (bf != buf)
			free(bf);
		bf = malloc(bfsz);
		if (bf == NULL) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		va_start(ap, format);
		i = vsnprintf(bf, bfsz, format, ap);
		va_end (ap);
	}
	s->write(s, (void *) bf, (size_t) i, (size_t) 1);
	if (bf != buf)
		free(bf);
	return s->errnr ? -1 : i;
}


/* ------------------------------------------------------------------ */

bstream *
bstream_create(stream *s, size_t size)
{
	bstream *b;

	if (s == NULL || size >= (1 << 30))
		return NULL;
	if ((b = malloc(sizeof(*b))) == NULL)
		return NULL;
	b->mode = (int) size;
	if (size == 0)
		size = BUFSIZ;
	b->s = s;
	b->buf = malloc(size + 1 + 1);
	if (b->buf == NULL) {
		free(b);
		return NULL;
	}
	b->size = size;
	b->pos = 0;
	b->len = 0;
	b->eof = 0;
	return b;
}

ssize_t
bstream_read(bstream *s, size_t size)
{
	ssize_t rd;

	if (s == NULL)
		return -1;

	if (s->eof)
		return 0;

	if (s->pos > 0) {
		if (s->pos < s->len) {
			/* move all data and end of string marker */
			memmove(s->buf, s->buf + s->pos, s->len - s->pos + 1);
			s->len -= s->pos;
		} else
			s->len = 0;
		s->pos = 0;
	}

	assert(s->buf != NULL);
	if (s->len == s->size && (s->buf = realloc(s->buf, (s->size <<= 1) + 1)) == NULL) {
		s->size = 0;
		s->len = 0;
		s->pos = 0;
		return -1;
	}

	if (size > s->size - s->len)
		size = s->size - s->len;

	rd = s->s->read(s->s, s->buf + s->len, 1, size);

	if (rd < 0)
		return rd;

	if (rd == 0) {
		s->eof = 1;
		return 0;
	}
	s->len += rd;
	s->buf[s->len] = 0;	/* fill in the spare with EOS */
	return rd;
}

#ifdef _POSIX2_LINE_MAX
#define STREAM_LINE_MAX _POSIX2_LINE_MAX
#else
#define STREAM_LINE_MAX 2048
#endif

static ssize_t
bstream_readline(bstream *s)
{
	size_t size = STREAM_LINE_MAX;
	size_t rd;

	if (s->eof)
		return 0;

	if (s->pos > 0 && s->len + size >= s->size) {
		if (s->pos < s->len) {
			/* move all data and end of string marker */
			memmove(s->buf, s->buf + s->pos, s->len - s->pos + 1);
			s->len -= s->pos;
		} else
			s->len = 0;
		s->pos = 0;
	}

	assert(s->buf != NULL);
	if (s->len == s->size && (s->buf = realloc(s->buf, (s->size <<= 1) + 1)) == NULL) {
		s->size = 0;
		s->len = 0;
		s->pos = 0;
		return -1;
	}

	if (size > s->size - s->len)
		size = s->size - s->len;

	if (fgets(s->buf + s->len, (int) size, s->s->stream_data.p) == NULL)
		return -1;

	rd = strlen(s->buf + s->len);

	if (rd == 0) {
		s->eof = 1;
		return 0;
	}
	s->len += rd;
	s->buf[s->len] = 0;	/* fill in the spare with EOS */
	return (ssize_t) rd;
}


ssize_t
bstream_next(bstream *s)
{
	if (s == NULL)
		return -1;
	if (s->mode) {
		return bstream_read(s, s->mode);
	} else if (s->s->read == file_read) {
		return bstream_readline(s);
	} else {
		ssize_t sz = 0, rd;

		while ((rd = bstream_read(s, 1)) == 1 &&
		       s->buf[s->pos + sz] != '\n') {
			sz += rd;
		}
		if (rd < 0)
			return rd;
		return sz;
	}
}

void
bstream_destroy(bstream *s)
{
	if (s) {
		if (s->s) {
			s->s->close(s->s);
			s->s->destroy(s->s);
		}
		if (s->buf)
			free(s->buf);
		free(s);
	}
}

/* ------------------------------------------------------------------ */

/*
 * Buffered write stream batches subsequent write requests in order to
 * optimize write bandwidth
 */
typedef struct {
	stream *s;
	size_t len, pos;
	char buf[FLEXIBLE_ARRAY_MEMBER]; /* NOTE: buf extends beyond array for wbs->len bytes */
} wbs_stream;

static int
wbs_flush(stream *s)
{
	wbs_stream *wbs;
	size_t len;

	wbs = (wbs_stream *) s->stream_data.p;
	if (wbs == NULL)
		return -1;
	len = wbs->pos;
	wbs->pos = 0;
	if (wbs->s == NULL ||
	    (*wbs->s->write) (wbs->s, wbs->buf, 1, len) != (ssize_t) len)
		return -1;
	if (wbs->s->flush)
		return (*wbs->s->flush) (wbs->s);
	return 0;
}

static ssize_t
wbs_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	wbs_stream *wbs;
	size_t nbytes, reqsize = cnt * elmsize, todo = reqsize;

	wbs = (wbs_stream *) s->stream_data.p;
	if (wbs == NULL)
		return -1;
	while (todo > 0) {
		int flush = 1;
		nbytes = wbs->len - wbs->pos;
		if (nbytes > todo) {
			nbytes = todo;
			flush = 0;
		}
		memcpy(wbs->buf + wbs->pos, buf, nbytes);
		todo -= nbytes;
		buf = (((const char *) buf) + nbytes);
		wbs->pos += nbytes;
		if (flush && wbs_flush(s) < 0)
			return -1;
	}
	return (ssize_t) cnt;
}

static void
wbs_close(stream *s)
{
	wbs_stream *wbs = (wbs_stream *) s->stream_data.p;

	wbs_flush(s);
	if (wbs && wbs->s)
		(*wbs->s->close) (wbs->s);
}

static void
wbs_destroy(stream *s)
{
	wbs_stream *wbs = (wbs_stream *) s->stream_data.p;

	if (wbs) {
		if (wbs->s)
			(*wbs->s->destroy) (wbs->s);
		free(wbs);
	}
	destroy(s);
}

static void
wbs_update_timeout(stream *s)
{
	wbs_stream *wbs = (wbs_stream *) s->stream_data.p;

	if (wbs && wbs->s) {
		wbs->s->timeout = s->timeout;
		wbs->s->timeout_func = s->timeout_func;
		if (wbs->s->update_timeout)
			(*wbs->s->update_timeout)(wbs->s);
	}
}

static int
wbs_isalive(stream *s)
{
	wbs_stream *wbs = (wbs_stream *) s->stream_data.p;

	if (wbs && wbs->s) {
		if (wbs->s->isalive)
			return (*wbs->s->isalive)(wbs->s);
		return 1;
	}
	return 0;
}

static void
wbs_clrerr(stream *s)
{
	if (s && s->stream_data.p)
		mnstr_clearerr(((wbs_stream *) s->stream_data.p)->s);
}

stream *
wbstream(stream *s, size_t buflen)
{
	stream *ns;
	wbs_stream *wbs;

	if (s == NULL)
		return NULL;
	ns = create_stream(s->name);
	if (ns == NULL)
		return NULL;
	wbs = (wbs_stream *) malloc(offsetof(wbs_stream, buf) + buflen);
	if (wbs == NULL) {
		destroy(ns);
		return NULL;
	}
	ns->type = s->type;
	ns->access = s->access;
	ns->close = wbs_close;
	ns->clrerr = wbs_clrerr;
	ns->destroy = wbs_destroy;
	ns->flush = wbs_flush;
	ns->update_timeout = wbs_update_timeout;
	ns->isalive = wbs_isalive;
	ns->write = wbs_write;
	ns->stream_data.p = (void *) wbs;
	wbs->s = s;
	wbs->pos = 0;
	wbs->len = buflen;
	return ns;
}

/* ------------------------------------------------------------------ */

struct cbstream {
	void *private;
	void (*destroy)(void *);
	void (*close)(void *);
	ssize_t (*read)(void *, void *, size_t, size_t);
};

static void
cb_destroy(stream *s)
{
	struct cbstream *cb = s->stream_data.p;

	if (cb->destroy)
		(*cb->destroy)(cb->private);
	destroy(s);
}

static void
cb_close(stream *s)
{
	struct cbstream *cb = s->stream_data.p;

	if (cb->close)
		(*cb->close)(cb->private);
}

static ssize_t
cb_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	struct cbstream *cb = s->stream_data.p;

	return (*cb->read)(cb->private, buf, elmsize, cnt);
}

stream *
callback_stream(void *private,
		ssize_t (*read) (void *private, void *buf, size_t elmsize, size_t cnt),
		void (*close) (void *private),
		void (*destroy) (void *private),
		const char *name)
{
	stream *s;
	struct cbstream *cb;

	s = create_stream(name);
	if (s == NULL)
		return NULL;
	cb = malloc(sizeof(struct cbstream));
	if (cb == NULL) {
		destroy(s);
		return NULL;
	}
	cb->private = private;
	cb->destroy = destroy;
	cb->read = read;
	cb->close = close;
	s->stream_data.p = cb;
	s->read = cb_read;
	s->destroy = cb_destroy;
	s->close = cb_close;
	return s;
}

/* Front-ends may wish to have more control over the designated file
 * activity. For this they need access to the file descriptor or even
 * duplicate it. (e.g. tablet loader) */
FILE *
getFile(stream *s)
{
#ifdef _MSC_VER
	if (s->read == console_read)
		return stdin;
	if (s->write == console_write)
		return stdout;
#endif
	if (s->read != file_read)
		return NULL;
	return (FILE *) s->stream_data.p;
}

static ssize_t
stream_blackhole_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	s = (stream*)s;
	buf = (const void*) buf;
	elmsize = (size_t) elmsize;
	return (ssize_t) cnt;
}

static void
stream_blackhole_close(stream *s)
{
	s = (stream*)s;
	// no resources to close
}

stream * stream_blackhole_create (void)
{
	stream *s;
	if ((s = create_stream("blackhole")) == NULL) {
		return NULL;
	}

	s->read = NULL;
	s->write = stream_blackhole_write;
	s->close = stream_blackhole_close;
	s->flush = NULL;
	s->access = ST_WRITE;
	return s;
}


/* fixed-width format streams */
#define STREAM_FWF_NAME "fwf_ftw"

typedef struct {
	stream *s;
	// config
	size_t num_fields;
	size_t *widths;
	char filler;
	// state
	size_t line_len;
	char* in_buf;
	char* out_buf;
	size_t out_buf_start;
	size_t out_buf_remaining;
} stream_fwf_data;


static ssize_t
stream_fwf_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	stream_fwf_data *fsd;
	size_t to_write = cnt;
	size_t buf_written = 0;
	if (strcmp(s->name, STREAM_FWF_NAME) != 0 || elmsize != 1) {
		return -1;
	}
	fsd = (stream_fwf_data*) s->stream_data.p;

	while (to_write > 0) {
		// input conversion
		if (fsd->out_buf_remaining == 0) { // need to convert next line
			size_t field_idx, in_buf_pos = 0, out_buf_pos = 0;
			ssize_t actually_read = fsd->s->read(fsd->s, fsd->in_buf, 1, fsd->line_len);
			if (actually_read < (ssize_t) fsd->line_len) { // incomplete last line
				if (actually_read < 0) {
					return actually_read; // this is an error
				}
				return buf_written; // skip last line
			}
			for (field_idx = 0; field_idx < fsd->num_fields; field_idx++) {
				char *val_start, *val_end;
				val_start = fsd->in_buf + in_buf_pos;
				in_buf_pos += fsd->widths[field_idx];
				val_end = fsd->in_buf + in_buf_pos - 1;
				while (*val_start == fsd->filler) val_start++;
				while (*val_end == fsd->filler) val_end--;
				while (val_start <= val_end) {
					if (*val_start == STREAM_FWF_FIELD_SEP) {
						fsd->out_buf[out_buf_pos++] = STREAM_FWF_ESCAPE;
					}
					fsd->out_buf[out_buf_pos++] = *val_start++;
				}
				fsd->out_buf[out_buf_pos++] = STREAM_FWF_FIELD_SEP;
			}
			fsd->out_buf[out_buf_pos++] = STREAM_FWF_RECORD_SEP;
			fsd->out_buf_remaining = out_buf_pos;
			fsd->out_buf_start = 0;
		}

		// now we know something is in output_buf so deliver it
		if (fsd->out_buf_remaining <= to_write) {
			memcpy((char*)buf + buf_written, fsd->out_buf + fsd->out_buf_start, fsd->out_buf_remaining);
			to_write -= fsd->out_buf_remaining;
			buf_written += fsd->out_buf_remaining;
			fsd->out_buf_remaining = 0;
		} else {
			memcpy((char*) buf + buf_written, fsd->out_buf + fsd->out_buf_start, to_write);
			fsd->out_buf_start += to_write;
			fsd->out_buf_remaining -= to_write;
			buf_written += to_write;
			to_write = 0;
		}
	}
	return buf_written;
}


static void
stream_fwf_close(stream *s)
{
	if (strcmp(s->name, STREAM_FWF_NAME) == 0) {
		stream_fwf_data *fsd = (stream_fwf_data*) s->stream_data.p;
		mnstr_close(fsd->s);
		free(fsd->widths);
		free(fsd->in_buf);
		free(fsd->out_buf);
		free(fsd);
	}
	// FIXME destroy(s);
}

stream*
stream_fwf_create (stream *s, size_t num_fields, size_t *widths, char filler)
{
	stream *ns;
	stream_fwf_data *fsd = malloc(sizeof(stream_fwf_data));
	size_t i, out_buf_len;
	if (!fsd) {
		return NULL;
	}
	fsd->s = s;
	fsd->num_fields = num_fields;
	fsd->widths = widths;
	fsd->filler = filler;
	fsd->line_len = 1; // newline
	for (i = 0; i < num_fields; i++) {
		fsd->line_len += widths[i];
	}
	fsd->in_buf = malloc(fsd->line_len);
	if (!fsd->in_buf) {
		free(fsd);
		return NULL;
	}
	out_buf_len = fsd->line_len * 3;
	fsd->out_buf = malloc(out_buf_len);
	if (!fsd->out_buf) {
		free(fsd->in_buf);
		free(fsd);
		return NULL;
	}
	fsd->out_buf_remaining = 0;

	if ((ns = create_stream(STREAM_FWF_NAME)) == NULL) {
		free(fsd->in_buf);
		free(fsd->out_buf);
		free(fsd);
		return NULL;
	}
	ns->read = stream_fwf_read;
	ns->close = stream_fwf_close;
	ns->write = NULL;
	ns->flush = NULL;
	ns->access = ST_READ;
	ns->stream_data.p = fsd;
	return ns;
}

