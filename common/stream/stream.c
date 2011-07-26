/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
#include <errno.h>
#include <stdarg.h>		/* va_alist.. */
#include <assert.h>

#ifdef HAVE_NETDB_H
# include <sys/types.h>
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

#define short_int_SWAP(s) ((short)(((0x00ff&(s))<<8) | ((0xff00&(s))>>8)))

#define normal_int_SWAP(i) (((0x000000ff&(i))<<24) | ((0x0000ff00&(i))<<8) | \
	               ((0x00ff0000&(i))>>8)  | ((0xff000000&(i))>>24))

#define long_long_SWAP(l) \
		((((lng)normal_int_SWAP(l))<<32) |\
		 (0xffffffff&normal_int_SWAP(l>>32)))


struct stream {
	short byteorder;
	short access;		/* read/write */
	short type;		/* ascii/binary */
	char *name;
	union {
		void *p;
		int i;
		SOCKET s;
	} stream_data;
	int errnr;
	ssize_t (*read) (stream *s, void *buf, size_t elmsize, size_t cnt);
	ssize_t (*readline) (stream *s, void *buf, size_t maxcnt);
	ssize_t (*write) (stream *s, const void *buf, size_t elmsize, size_t cnt);
	void (*close) (stream *s);
	char *(*error) (stream *s);
	void (*destroy) (stream *s);
	int (*flush) (stream *s);
	int (*fsync) (stream *s);
	int (*fgetpos) (stream *s, lng *p);
	int (*fsetpos) (stream *s, lng p);
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

/* Read at most cnt elements of size elmsize from the stream.  Returns
   the number of elements actually read. */
ssize_t
mnstr_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
#ifdef STREAM_DEBUG
	printf("read %s " SZFMT " " SZFMT "\n", s->name ? s->name : "<unnamed>", elmsize, cnt);
#endif
	assert(s->access == ST_READ);
	if (s->errnr)
		return s->errnr;
	return (*s->read) (s, buf, elmsize, cnt);
}

/* Read one line (seperated by \n) of atmost maxcnt characters from the stream. 
   Returns the number of characters actually read, includes the trailing \n */
ssize_t
mnstr_readline(stream *s, void *buf, size_t maxcnt)
{
#ifdef STREAM_DEBUG
	printf("readline %s " SZFMT "\n", s->name ? s->name : "<unnamed>", maxcnt);
#endif
	assert(s->access == ST_READ);
	if (s->errnr)
		return s->errnr;
	if (!s->readline) {
		size_t len = 0;
		char *b = buf, *start = buf;
		while ((*s->read) (s, start, 1, 1) > 0 && len < maxcnt) {
			if (*start++ == '\n')
				break;
		}
		if (s->errnr)
			return s->errnr;
		return (ssize_t) (start - b);
	} else
		return (*s->readline) (s, buf, maxcnt);
}

/* Write cnt elements of size elmsize to the stream.  Returns the
   number of elements actually written.  If elmsize or cnt equals zero,
   returns cnt. */
ssize_t
mnstr_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
#ifdef STREAM_DEBUG
	printf("write %s " SZFMT " " SZFMT "\n", s->name ? s->name : "<unnamed>", elmsize, cnt);
#endif
	assert(s->access == ST_WRITE);
	if (s->errnr)
		return s->errnr;
	return (*s->write) (s, buf, elmsize, cnt);
}

void
mnstr_close(stream *s)
{
#ifdef STREAM_DEBUG
	printf("close %s\n", s->name ? s->name : "<unnamed>");
#endif
	if (s)
		(*s->close) (s);
}

void
mnstr_destroy(stream *s)
{
#ifdef STREAM_DEBUG
	printf("destroy %s\n", s->name ? s->name : "<unnamed>");
#endif
	if (s)
		(*s->destroy) (s);
}

char *
mnstr_error(stream *s)
{
	if (s == 0)
		return "Connection terminated";
	return (*s->error) (s);
}

int
mnstr_flush(stream *s)
{
	if (!s)
		return -1;
#ifdef STREAM_DEBUG
	printf("flush %s\n", s->name ? s->name : "<unnamed>");
#endif
	assert(s->access == ST_WRITE);
	if (s->errnr)
		return s->errnr;
	if (s->flush)
		return (*s->flush) (s);
	return 0;
}

int
mnstr_fsync(stream *s)
{
#ifdef STREAM_DEBUG
	printf("fsync %s (%d)\n", s->name ? s->name : "<unnamed>", s->errnr);
#endif
	if (!s)
		return -1;
	assert(s->access == ST_WRITE);
	if (s->errnr)
		return s->errnr;
	if (s->fsync)
		return (*s->fsync) (s);
	return 0;
}

int
mnstr_fgetpos(stream *s, lng *p)
{
#ifdef STREAM_DEBUG
	printf("fgetpos %s\n", s->name ? s->name : "<unnamed>");
#endif
	if (!s)
		return -1;
	assert(s->access == ST_WRITE);
	if (s->errnr)
		return s->errnr;
	if (s->fgetpos)
		return (*s->fgetpos) (s, p);
	return 0;
}

int
mnstr_fsetpos(stream *s, lng p)
{
#ifdef STREAM_DEBUG
	printf("fsetpos %s\n", s->name ? s->name : "<unnamed>");
#endif
	if (!s)
		return -1;
	if (s->errnr)
		return s->errnr;
	if (s->fsetpos)
		return (*s->fsetpos) (s, p);
	return 0;
}


char *
mnstr_name(stream *s)
{
	if (s == 0)
		return "connection terminated";
	return s->name;
}

int
mnstr_errnr(stream *s)
{
	if (s == 0)
		return MNSTR_READ_ERROR;
	return s->errnr;
}

void
mnstr_clearerr(stream *s)
{
	if (s != NULL)
		s->errnr = MNSTR_NO__ERROR;
}

int
mnstr_type(stream *s)
{
	if (s == 0)
		return 0;
	return s->type;
}

int
mnstr_byteorder(stream *s)
{
	if (s == 0)
		return 0;
	return s->byteorder;
}

void
mnstr_set_byteorder(stream *s, char bigendian)
{
#ifdef STREAM_DEBUG
	printf("mnstr_set_byteorder %s\n", s->name ? s->name : "<unnamed>");
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
	s->close(s);
	s->destroy(s);
}

stream *
mnstr_rstream(stream *s)
{
#ifdef STREAM_DEBUG
	printf("mnstr_rstream %s\n", s->name ? s->name : "<unnamed>");
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
#ifdef STREAM_DEBUG
	printf("mnstr_wstream %s\n", s->name ? s->name : "<unnamed>");
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
	free(s->name);
	free(s);
}

static char *
error(stream *s)
{
	char buf[BUFSIZ];

	switch (s->errnr) {
	case MNSTR_OPEN_ERROR:
		snprintf(buf, BUFSIZ, "error could not open file %s\n", s->name);
		return strdup(buf);
	case MNSTR_READ_ERROR:
		snprintf(buf, BUFSIZ, "error reading file %s\n", s->name);
		return strdup(buf);
	case MNSTR_WRITE_ERROR:
		snprintf(buf, BUFSIZ, "error writing file %s\n", s->name);
		return strdup(buf);
	}
	return strdup("Unknown error");
}

static stream *
create_stream(const char *name)
{
	stream *s;

	if ((s = (stream *) malloc(sizeof(*s))) == NULL)
		return NULL;
	s->byteorder = 1234;
	s->access = ST_READ;
	s->type = ST_ASCII;
	s->name = strdup(name);
	s->stream_data.p = NULL;
	s->errnr = MNSTR_NO__ERROR;
	s->stream_data.p = NULL;
	s->read = NULL;
	s->readline = NULL;
	s->write = NULL;
	s->close = NULL;
	s->error = error;
	s->destroy = destroy;
	s->flush = NULL;
	s->fsync = NULL;
	s->fgetpos = NULL;
	s->fsetpos = NULL;
#ifdef STREAM_DEBUG
	printf("create_stream %s -> " PTRFMT "\n", name ? name : "<unnamed>", PTRFMTCAST s);
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

	if (!feof(fp)) {
		rc = fread(buf, elmsize, cnt, fp);
		if (ferror(fp))
			s->errnr = MNSTR_READ_ERROR;
	}
	return (ssize_t) rc;
}

static ssize_t
file_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	if (elmsize && cnt) {
		size_t rc = fwrite(buf, elmsize, cnt, (FILE *) s->stream_data.p);

		if (rc < cnt)
			s->errnr = MNSTR_WRITE_ERROR;
		return (ssize_t) rc;
	}
	return (ssize_t) cnt;
}

static void
file_close(stream *s)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (!fp)
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

static int
file_flush(stream *s)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (s->access == ST_WRITE && fflush(fp) < 0) {
		s->errnr = MNSTR_WRITE_ERROR;
		return 1;
	}
	return 0;
}

static int
file_fsync(stream *s)
{

	FILE *fp = (FILE *) s->stream_data.p;

	if (s->access == ST_WRITE
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
	    ) {
		s->errnr = MNSTR_WRITE_ERROR;
		return 1;
	}
	return 0;
}

static int
file_fgetpos(stream *s, lng *p)
{
	FILE *fp = (FILE *) s->stream_data.p;

#if defined(NATIVE_WIN32) && _MSC_VER >= 1400	/* Visual Studio 2005 */
	*p = (lng) _ftelli64(fp);	/* returns __int64 */
#else
#ifdef HAVE_FSEEKO
	*p = (lng) ftello(fp);	/* returns off_t */
#else
	*p = (lng) ftell(fp);	/* returns long */
#endif
#endif
	return 0;
}

static int
file_fsetpos(stream *s, lng p)
{
	int res = 0;
	FILE *fp = (FILE *) s->stream_data.p;

#if defined(NATIVE_WIN32) && _MSC_VER >= 1400	/* Visual Studio 2005 */
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

/* Front-ends may wish to have more control over the designated file
 * activity. For this they need access to the file descriptor or even
 * duplicate it. (e.g. tablet loader) */
FILE *
getFile(stream *s)
{
	if (s->read != file_read)
		return NULL;
	return (FILE *) s->stream_data.p;
}

static stream *
open_stream(const char *filename, const char *flags)
{
	stream *s;
	FILE *fp;

	if ((s = create_stream(filename)) == NULL)
		return NULL;
	if ((fp = fopen(filename, flags)) == NULL)
		s->errnr = MNSTR_OPEN_ERROR;
	s->read = file_read;
	s->readline = NULL;
	s->write = file_write;
	s->close = file_close;
	s->flush = file_flush;
	s->fsync = file_fsync;
	s->fgetpos = file_fgetpos;
	s->fsetpos = file_fsetpos;
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
dupFileStream(stream *s)
{
	stream *ns;
	if (s->read != file_read)
		return NULL;
	if (s->access == ST_WRITE)
		ns = open_stream(s->name, "w");
	else
		ns = open_stream(s->name, "r");
	return ns;
}

/* ------------------------------------------------------------------ */
/* streams working on a gzip-compressed disk file */

#ifdef HAVE_LIBZ
static ssize_t
stream_gzread(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	gzFile *fp = (gzFile *) s->stream_data.p;
	int size = (int) (elmsize * cnt);

	if (!gzeof(fp)) {
		size = gzread(fp, buf, size);
		if (size)
			return (ssize_t) (size / elmsize);
	}
	return 0;
}

static ssize_t
stream_gzwrite(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	int size = (int) (elmsize * cnt);

	if (size) {
		size = gzwrite((gzFile *) s->stream_data.p, buf, size);
		return (ssize_t) (size / elmsize);
	}
	return (ssize_t) cnt;
}

static void
stream_gzclose(stream *s)
{
	if (s->stream_data.p)
		gzclose((gzFile *) s->stream_data.p);
	s->stream_data.p = NULL;
}

static int
stream_gzflush(stream *s)
{
	if (s->access == ST_WRITE)
		gzflush((gzFile *) s->stream_data.p, Z_SYNC_FLUSH);
	return 0;
}

static stream *
open_gzstream(const char *filename, const char *flags)
{
	stream *s;
	gzFile *fp;

	if ((s = create_stream(filename)) == NULL)
		return NULL;
	if ((fp = gzopen(filename, flags)) == NULL)
		s->errnr = MNSTR_OPEN_ERROR;
	s->read = stream_gzread;
	s->write = stream_gzwrite;
	s->close = stream_gzclose;
	s->flush = stream_gzflush;
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
open_gzrstream(const char *filename)
{
	stream *s;

	if ((s = open_gzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    gzread((gzFile *) s->stream_data.p, (void *) &s->byteorder, sizeof(s->byteorder)) < (int) sizeof(s->byteorder)) {
		stream_gzclose(s);
		s->errnr = MNSTR_OPEN_ERROR;
	}
	return s;
}

static stream *
open_gzwstream_(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_gzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR)
		gzwrite((gzFile *) s->stream_data.p, (void *) &s->byteorder, sizeof(s->byteorder));
	return s;
}

stream *
open_gzwstream(const char *filename)
{
	return open_gzwstream_(filename, "wb");
}

stream *
open_gzrastream(const char *filename)
{
	stream *s;

	if ((s = open_gzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_ASCII;
	return s;
}

static stream *
open_gzwastream_(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_gzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}

stream *
open_gzwastream(const char *filename)
{
	return open_gzwastream_(filename, "wb");
}
#else
stream *open_gzrstream(const char *filename) {
	(void) filename;
	return NULL;
}
stream *open_gzwstream(const char *filename) {
	(void) filename;
	return NULL;
}
stream *open_gzrastream(const char *filename) {
	(void) filename;
	return NULL;
}
stream *open_gzwastream(const char *filename) {
	(void) filename;
	return NULL;
}
#endif

/* ------------------------------------------------------------------ */
/* streams working on a bzip2-compressed disk file */

#ifdef HAVE_LIBBZ2
static ssize_t
stream_bzread(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	int size = (int) (elmsize * cnt);

	size = BZ2_bzread((BZFILE *) s->stream_data.p, buf, size);
	if (size)
		return size / elmsize;
	return 0;
}

static ssize_t
stream_bzwrite(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	int size = (int) (elmsize * cnt);

	if (size) {
		size = BZ2_bzwrite((BZFILE *) s->stream_data.p, (void *) buf, size);
		return size / elmsize;
	}
	return cnt;
}

static void
stream_bzclose(stream *s)
{
	if (s->stream_data.p)
		BZ2_bzclose((BZFILE *) s->stream_data.p);
	s->stream_data.p = NULL;
}

static int
stream_bzflush(stream *s)
{
	if (s->access == ST_WRITE)
		BZ2_bzflush((BZFILE *) s->stream_data.p);
	return 0;
}

static stream *
open_bzstream(const char *filename, const char *flags)
{
	stream *s;
	BZFILE *fp;

	if ((s = create_stream(filename)) == NULL)
		return NULL;
	if ((fp = BZ2_bzopen(filename, flags)) == NULL)
		s->errnr = MNSTR_OPEN_ERROR;
	s->read = stream_bzread;
	s->write = stream_bzwrite;
	s->close = stream_bzclose;
	s->flush = stream_bzflush;
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
open_bzrstream(const char *filename)
{
	stream *s;

	if ((s = open_bzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    BZ2_bzread((BZFILE *) s->stream_data.p, (void *) &s->byteorder, sizeof(s->byteorder)) < (int) sizeof(s->byteorder)) {
		stream_bzclose(s);
		s->errnr = MNSTR_OPEN_ERROR;
	}
	return s;
}

static stream *
open_bzwstream_(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_bzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR)
		BZ2_bzwrite((BZFILE *) s->stream_data.p, (void *) &s->byteorder, sizeof(s->byteorder));
	return s;
}

stream *
open_bzwstream(const char *filename)
{
	return open_bzwstream_(filename, "wb");
}

stream *
open_bzrastream(const char *filename)
{
	stream *s;

	if ((s = open_bzstream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_ASCII;
	return s;
}

static stream *
open_bzwastream_(const char *filename, const char *mode)
{
	stream *s;

	if ((s = open_bzstream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}

stream *
open_bzwastream(const char *filename)
{
	return open_bzwastream_(filename, "wb");
}
#else
stream *open_bzrstream(const char *filename) {
	(void) filename;
	return NULL;
}
stream *open_bzwstream(const char *filename) {
	(void) filename;
	return NULL;
}
stream *open_bzrastream(const char *filename) {
	(void) filename;
	return NULL;
}
stream *open_bzwastream(const char *filename) {
	(void) filename;
	return NULL;
}
#endif

/* ------------------------------------------------------------------ */
/* streams working on a disk file, compressed or not */

stream *
open_rstream(const char *filename)
{
	stream *s;
	const char *ext;

#ifdef STREAM_DEBUG
	printf("open_rstream %s\n", filename);
#endif
	ext = get_extention(filename);

	if (strcmp(ext, "gz") == 0) {
#ifdef HAVE_LIBZ
		return open_gzrstream(filename);
#else
		if ((s = create_stream(filename)) != NULL)
			s->errnr = MNSTR_OPEN_ERROR;
		return s;
#endif
	}
	if (strcmp(ext, "bz2") == 0) {
#ifdef HAVE_LIBBZ2
		return open_bzrstream(filename);
#else
		if ((s = create_stream(filename)) != NULL)
			s->errnr = MNSTR_OPEN_ERROR;
		return s;
#endif
	}
	if ((s = open_stream(filename, "rb")) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR) {
		if (fread((void *) &s->byteorder, sizeof(s->byteorder), 1, (FILE *) s->stream_data.p) < 1 ||
		    ferror((FILE *) s->stream_data.p)) {
			fclose((FILE *) s->stream_data.p);
			s->stream_data.p = NULL;
			s->errnr = MNSTR_OPEN_ERROR;
		}
	}
	return s;
}

static stream *
open_wstream_(const char *filename, char *mode)
{
	stream *s;
	const char *ext;

#ifdef STREAM_DEBUG
	printf("open_wstream %s\n", filename);
#endif
	ext = get_extention(filename);

	if (strcmp(ext, "gz") == 0) {
#ifdef HAVE_LIBZ
		return open_gzwstream_(filename, mode);
#else
		if ((s = create_stream(filename)) != NULL)
			s->errnr = MNSTR_OPEN_ERROR;
		return s;
#endif
	}
	if (strcmp(ext, "bz2") == 0) {
#ifdef HAVE_LIBBZ2
		return open_bzwstream_(filename, mode);
#else
		if ((s = create_stream(filename)) != NULL)
			s->errnr = MNSTR_OPEN_ERROR;
		return s;
#endif
	}
	if ((s = open_stream(filename, mode)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR &&
	    fwrite((void *) &s->byteorder, sizeof(s->byteorder), 1, (FILE *) s->stream_data.p) < 1)
		s->errnr = MNSTR_OPEN_ERROR;
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

#ifdef STREAM_DEBUG
	printf("open_rastream %s\n", filename);
#endif
	ext = get_extention(filename);

	if (strcmp(ext, "gz") == 0) {
#ifdef HAVE_LIBZ
		return open_gzrastream(filename);
#else
		if ((s = create_stream(filename)) != NULL)
			s->errnr = MNSTR_OPEN_ERROR;
		return s;
#endif
	}
	if (strcmp(ext, "bz2") == 0) {
#ifdef HAVE_LIBBZ2
		return open_bzrastream(filename);
#else
		if ((s = create_stream(filename)) != NULL)
			s->errnr = MNSTR_OPEN_ERROR;
		return s;
#endif
	}
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

#ifdef STREAM_DEBUG
	printf("open_wastream %s\n", filename);
#endif
	ext = get_extention(filename);

	if (strcmp(ext, "gz") == 0) {
#ifdef HAVE_LIBZ
		return open_gzwastream_(filename, mode);
#else
		if ((s = create_stream(filename)) != NULL)
			s->errnr = MNSTR_OPEN_ERROR;
		return s;
#endif
	}
	if (strcmp(ext, "bz2") == 0) {
#ifdef HAVE_LIBBZ2
		return open_bzwastream_(filename, mode);
#else
		if ((s = create_stream(filename)) != NULL)
			s->errnr = MNSTR_OPEN_ERROR;
		return s;
#endif
	}
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

/* this function is called by libcurl when there is data for us */
static size_t
write_callback(char *buffer, size_t size, size_t nitems, void *userp)
{
	stream *s = (stream *) userp;
	struct curl_data *c = (struct curl_data *) s->stream_data.p;

	size *= nitems;
	/* allocate a buffer if we don't have one yet */
	if (c->buffer == NULL && size != 0) {
		/* BLOCK had better be a power of 2! */
		c->maxsize = (size + BLOCK - 1) & ~(BLOCK - 1);
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
		c->maxsize = (c->usesize + size + BLOCK - 1) & ~(BLOCK - 1);
		c->buffer = realloc(c->buffer, c->usesize + size);
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
	size_t size;

	if (c->usesize - c->offset >= elmsize || !c->running) {
		/* there is at least one element's worth of data
		   available, or we have reached the end: return as
		   much as we have, but no more than requested */
		if (cnt * elmsize > c->usesize - c->offset)
			cnt = (c->usesize - c->offset) / elmsize;
		size = cnt * elmsize;
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
	s->stream_data.p = (void *) c;
	if ((c->handle = curl_easy_init()) == NULL) {
		destroy(s);
		return NULL;
	}
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
	if (curl_easy_perform(c->handle) != CURLE_OK)
		s->errnr = MNSTR_OPEN_ERROR;
	curl_easy_cleanup(c->handle);
	c->handle = NULL;
	c->running = 0;
#endif
	return s;
}

#else
stream *open_urlstream(const char *url) {
	(void) url;
	return NULL;
}
#endif /* HAVE_CURL */

/* ------------------------------------------------------------------ */
/* streams working on a socket */

static ssize_t
socket_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	ssize_t nr = 0, res = 0, size = (ssize_t) (elmsize * cnt);

	if (!s || s->errnr)
		return -1;

	if (size == 0 || elmsize == 0)
		return (ssize_t) cnt;

	errno = 0;
	while (res < size &&
	       (
#ifdef NATIVE_WIN32
		/* send works on int, make sure the argument fits */
		((nr = send(s->stream_data.s, (void *) ((char *) buf + res), (int) min(size - res, 1 << 16), 0)) > 0)
#else
		((nr = write(s->stream_data.s, ((const char *) buf + res), size - res)) > 0)
#endif
		|| errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
		) {
		errno = 0;
		if (nr > 0)
			res += nr;
	}
	if (nr < 0) {
		s->errnr = MNSTR_WRITE_ERROR;
		return nr;
	}
	if (res > 0)
		return (ssize_t) (res / elmsize);
	s->errnr = MNSTR_WRITE_ERROR;
	return -1;
}

static ssize_t
socket_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	ssize_t nr = 0, res = 0, size = (ssize_t) (elmsize * cnt);

	if (!s || s->errnr)
		return (-1);

	errno = 0;
	while (res < size &&
	       (
#ifdef NATIVE_WIN32
		/* recv works on int, make sure the argument fits */
		((nr = recv(s->stream_data.s, (void *) ((char *) buf + res), (int) min(size - res, 1 << 16), 0)) > 0)
#else
		((nr = read(s->stream_data.s, (void *) ((char *) buf + res), size - res)) > 0)
#endif
		|| errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
		) {
		errno = 0;
		if (nr > 0)
			res += nr;
	}
	if (nr < 0) {
		s->errnr = MNSTR_READ_ERROR;
		return nr;
	}
	return (ssize_t) (res / elmsize);
}

/* Read one line (seperated by \n) of at most maxcnt characters from the 
   stream. Returns the number of characters actually read, includes the
   trailing \n. */
static ssize_t
socket_readline(stream *s, void *buf, size_t maxcnt)
{
	char *b = buf, *start = buf, *end = start + maxcnt;

	while (socket_read(s, start, 1, 1) > 0 && start < end) {
		if (*start++ == '\n')
			break;
	}
	if (s->errnr)
		return s->errnr;
	return (ssize_t) (start - b);
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

static stream *
socket_open(SOCKET sock, const char *name)
{
	stream *s;
	int domain = 0;

	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->read = socket_read;
	s->readline = socket_readline;
	s->write = socket_write;
	s->close = socket_close;
	s->flush = NULL;
	s->stream_data.s = sock;

	errno = 0;
#if defined(SO_DOMAIN)
	{
		socklen_t len = (socklen_t) sizeof(domain);
		getsockopt(sock, SOL_SOCKET, SO_DOMAIN, (void *) &domain, &len);
	}
#endif
#if defined(SO_KEEPALIVE) && !defined(WIN32)
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
		int opt = 0;
		setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *) &opt, sizeof(opt));
	}
#endif
#if defined(IPTOS_THROUGHPUT) && !defined(WIN32)
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
		int tos = IPTOS_THROUGHPUT;

		setsockopt(sock, IPPROTO_IP, IP_TOS, (void *) &tos, sizeof(tos));
	}
#endif
#ifdef TCP_NODELAY
	if (domain != PF_UNIX) {	/* not on UNIX sockets */
		int nodelay = 1;

		setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (void *) &nodelay, sizeof(nodelay));
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
	printf("socket_rstream " SSZFMT " %s\n", (ssize_t) sock, name);
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
	printf("socket_wstream " SSZFMT " %s\n", (ssize_t) sock, name);
#endif
	if ((s = socket_open(sock, name)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (s->errnr == MNSTR_NO__ERROR)
		socket_write(s, (void *) &s->byteorder, sizeof(s->byteorder), 1);
	return s;
}

stream *
socket_rastream(SOCKET sock, const char *name)
{
	stream *s = NULL;

#ifdef STREAM_DEBUG
	printf("socket_rastream " SSZFMT " %s\n", (ssize_t) sock, name);
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
	printf("socket_wastream " SSZFMT " %s\n", (ssize_t) sock, name);
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

	if (!s || s->errnr)
		return -1;

	if (size == 0 || elmsize == 0)
		return (ssize_t) cnt;
	udp = s->stream_data.p;
	addrlen = sizeof(udp->addr);
	errno = 0;
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

	if (!s || s->errnr)
		return (-1);

	udp = s->stream_data.p;
	errno = 0;
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
#ifdef HAVE_SHUTDOWN
	shutdown(udp->s, SHUT_RDWR);
#endif
	closesocket(udp->s);
}

static void
udp_destroy(stream *udp)
{
	if (udp->stream_data.p)
		free(udp->stream_data.p);
	free(udp);
}

static stream *
udp_create(const char *name)
{
	stream *s;
	udp_stream *udp = NULL;

	if ((s = create_stream(name)) == NULL)
		return NULL;
	if ((udp = (udp_stream *) malloc(sizeof(udp_stream))) == NULL) {
		free(s);
		return NULL;
	}
	s->readline = NULL;
	s->read = udp_read;
	s->write = udp_write;
	s->close = udp_close;
	s->flush = NULL;
	s->destroy = udp_destroy;
	s->stream_data.p = udp;

	errno = 0;
	return s;
}

int
udp_socket(udp_stream * udp, char *hostname, int port, int write)
{
	struct sockaddr *serv;
	socklen_t servsize;
	struct hostent *hp;

	hp = gethostbyname(hostname);
	if (hp == NULL)
		return 0;

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
		return 0;
	if (!write && bind(udp->s, serv, servsize) < 0)
		return 0;
	return 1;
}

stream *
udp_rastream(char *hostname, int port, const char *name)
{
	stream *s = udp_create(name);

#ifdef STREAM_DEBUG
	printf("udp_rawastream %s %s\n", hostname, name);
#endif
	if (!s)
		return NULL;
	if (!udp_socket(s->stream_data.p, hostname, port, 0)) {
		udp_destroy(s);
		return NULL;
	}
	s->type = ST_ASCII;
	return s;
}

stream *
udp_wastream(char *hostname, int port, const char *name)
{
	stream *s = udp_create(name);

#ifdef STREAM_DEBUG
	printf("udp_wastream %s %s\n", hostname, name);
#endif
	if (!s)
		return NULL;
	if (!udp_socket(s->stream_data.p, hostname, port, 1)) {
		udp_destroy(s);
		return NULL;
	}
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	return s;
}

/* ------------------------------------------------------------------ */
/* streams working on an open file pointer */

static stream *
file_stream(const char *name)
{
	stream *s;

	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->read = file_read;
	s->readline = NULL;
	s->write = file_write;
	s->close = file_close;
	s->flush = file_flush;
	s->fgetpos = file_fgetpos;
	s->fsetpos = file_fsetpos;
	return s;
}

stream *
file_rstream(FILE *fp, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	printf("file_rstream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->type = ST_BIN;
	if (fp == NULL)
		s->errnr = MNSTR_OPEN_ERROR;
	s->stream_data.p = (void *) fp;

	if (s->errnr == MNSTR_NO__ERROR) {
		if (fread((void *) &s->byteorder, sizeof(s->byteorder), 1, fp) < 1 ||
		    ferror(fp)) {
			fclose(fp);
			s->stream_data.p = NULL;
			s->errnr = MNSTR_OPEN_ERROR;
		}
	}
	return s;
}

stream *
file_wstream(FILE *fp, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	printf("file_wstream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_BIN;
	if (fp == NULL)
		s->errnr = MNSTR_OPEN_ERROR;
	s->stream_data.p = (void *) fp;

	if (s->errnr == MNSTR_NO__ERROR) {
		if (fwrite((void *) &s->byteorder, sizeof(s->byteorder), 1, fp) < 1 ||
		    ferror(fp))
			s->errnr = MNSTR_OPEN_ERROR;
	}
	return s;
}

stream *
file_rastream(FILE *fp, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	printf("file_rastream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->type = ST_ASCII;
	if (fp == NULL)
		s->errnr = MNSTR_OPEN_ERROR;
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
file_wastream(FILE *fp, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	printf("file_wastream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->access = ST_WRITE;
	s->type = ST_ASCII;
	if (fp == NULL)
		s->errnr = MNSTR_OPEN_ERROR;
	s->stream_data.p = (void *) fp;
	return s;
}

/* ------------------------------------------------------------------ */
/* streams working on a substream, converting character sets using iconv */

#ifdef HAVE_ICONV

#ifdef HAVE_ICONV_H
#include <iconv.h>
#endif

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

	/* if unconverted data from a previous call remains, add it to
	   the start of the new data, using temporary space */
	if (ic->buflen > 0) {
		char *bf = alloca(ic->buflen + inbytesleft);

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
				s->errnr = MNSTR_WRITE_ERROR;
				return -1;
			case EINVAL:
				/* incomplete multibyte sequence encountered */
				/* flush what has been converted */
				if (outbytesleft < sizeof(ic->buffer))
					mnstr_write(ic->s, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft);
				/* remember what hasn't been converted */
				if (inbytesleft > sizeof(ic->buffer)) {
					/* ridiculously long multibyte sequence, so return error */
					s->errnr = MNSTR_WRITE_ERROR;
					return -1;
				}
				memcpy(ic->buffer, inbuf, inbytesleft);
				ic->buflen = inbytesleft;
				return (ssize_t) cnt;
			case E2BIG:
				/* not enough space in output buffer */
				break;
			default:
				/* cannot happen (according to manual) */
				s->errnr = MNSTR_WRITE_ERROR;
				return -1;
			}
		}
		mnstr_write(ic->s, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft);
	}
	return (ssize_t) cnt;
}

static ssize_t
ic_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;
	ICONV_CONST char *inbuf = ic->buffer;
	size_t inbytesleft = ic->buflen;
	char *outbuf = (char *) buf;
	size_t outbytesleft = elmsize * cnt;

	while (outbytesleft > 0 && !ic->eof) {
		if (ic->buflen == sizeof(ic->buffer)) {
			/* ridiculously long multibyte sequence, return error */
			s->errnr = MNSTR_READ_ERROR;
			return -1;
		}

		switch (mnstr_read(ic->s, ic->buffer +ic->buflen, 1, 1)) {
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
			iconv(ic->cd, NULL, NULL, &outbuf, &outbytesleft);
			goto exit_func;
		default:
			/* error */
			s->errnr = MNSTR_READ_ERROR;
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
				   return what we have, saving what's in
				   the buffer */
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
		   next call (i.e. keep ic->eof set), otherwise we
		   must clear it so that the next call will cause the
		   underlying stream to be read again */
		ic->eof = 0;
	}
	return (ssize_t) ((elmsize * cnt - outbytesleft) / elmsize);
}

static int
ic_flush(stream *s)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;
	char *outbuf = ic->buffer;
	size_t outbytesleft = sizeof(ic->buffer);

	/* if unconverted data from a previous call remains, it was an
	   incomplete multibyte sequence, so an error */
	if (ic->buflen > 0) {
		s->errnr = MNSTR_WRITE_ERROR;
		return 1;
	}
	iconv(ic->cd, NULL, NULL, &outbuf, &outbytesleft);
	if (outbytesleft < sizeof(ic->buffer))
		mnstr_write(ic->s, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft);
	return mnstr_flush(ic->s);
}

static void
ic_close(stream *s)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;

	if (ic == NULL)
		return;
	ic_flush(s);
	mnstr_close(ic->s);
	free(s->stream_data.p);
	s->stream_data.p = NULL;
}

static stream *
ic_open(iconv_t cd, stream *ss, const char *name)
{
	stream *s;
	struct icstream *ic;

	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->read = ic_read;
	s->write = ic_write;
	s->close = ic_close;
	s->flush = ic_flush;
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

#ifdef STREAM_DEBUG
	printf("iconv_rstream %s %s\n", charset, name);
#endif
	cd = iconv_open("utf-8", charset);
	if (cd == (iconv_t) - 1)
		return NULL;
	s = ic_open(cd, ss, name);
	s->access = ST_READ;
	return s;
}

stream *
iconv_wstream(stream *ss, const char *charset, const char *name)
{
	stream *s;
	iconv_t cd;

#ifdef STREAM_DEBUG
	printf("iconv_wstream %s %s\n", charset, name);
#endif
	cd = iconv_open(charset, "utf-8");
	if (cd == (iconv_t) - 1)
		return NULL;
	s = ic_open(cd, ss, name);
	s->access = ST_WRITE;
	return s;
}

#else
stream *iconv_rstream(stream *ss, const char *charset, const char *name) {
	(void) name;

	if (strcmp(charset, "utf-8") == 0 ||
			strcmp(charset, "UTF-8") == 0 ||
			strcmp(charset, "UTF8") == 0)
		return ss;
	
	return NULL;
}
stream *iconv_wstream(stream *ss, const char *charset, const char *name) {
	(void) name;

	if (strcmp(charset, "utf-8") == 0 ||
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
	return (buffer *) s->stream_data.p;
}

static ssize_t
buffer_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	buffer *b = (buffer *) s->stream_data.p;

	assert(b);
	if (b->pos + size <= b->len) {
		memcpy(buf, b->buf + b->pos, size);
		b->pos += size;
		return (ssize_t) (size / elmsize);
	} else {
		s->errnr = MNSTR_READ_ERROR;
		return 0;
	}
}

static ssize_t
buffer_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	buffer *b = (buffer *) s->stream_data.p;

	assert(b);
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
	buffer *b = (buffer *) s->stream_data.p;

	assert(b);
	b->pos = 0;
	return 0;
}

stream *
buffer_rastream(buffer *b, const char *name)
{
	stream *s;

#ifdef STREAM_DEBUG
	printf("buffer_rastream %s\n", name);
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

#ifdef STREAM_DEBUG
	printf("buffer_wastream %s\n", name);
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
   consists of a count followed by the data in the block.  A flush is
   indicated by an empty block (i.e. just a count of 0).
 */
typedef struct bs {
	stream *s;		/* underlying stream */
	unsigned nr;		/* how far we got in buf */
	unsigned itotal;	/* amount available in current read block */
	size_t blks;		/* read/writen blocks (possibly partial) */
	size_t bytes;		/* read/writen bytes */
	char buf[BLOCK];	/* the buffered data (minus the size of
				   size-short */
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
   filled buffer to the underlying stream.
   Struct field usage:
   s - the underlying stream;
   buf - the buffer in which data is collected;
   nr - how much of buf is already filled (if nr == sizeof(buf) the
        data is written to the underlying stream, so upon entry nr <
        sizeof(buf));
   itotal - unused.
 */
static ssize_t
bs_write(stream *ss, const void *buf, size_t elmsize, size_t cnt)
{
	bs *s = (bs *) ss->stream_data.p;
	size_t todo = cnt * elmsize;
	short blksize;

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

				printf("W %s %u \"", ss->name, s->nr);
				for (i = 0; i < s->nr; i++)
					if (' ' <= s->buf[i] && s->buf[i] < 127)
						putchar(s->buf[i]);
					else
						printf("\\%03o", s->buf[i]);
				printf("\"\n");
			}
#endif
			/* since the block is at max BLOCK (8K) - 2 size we can
			   store it in a two byte integer */
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
   underlying stream.  Then in any case write an empty buffer to the
   underlying stream to indicate to the receiver that the data was
   flushed.
 */
static int
bs_flush(stream *ss)
{
	short blksize;
	bs *s = (bs *) ss->stream_data.p;

	assert(ss->access == ST_WRITE);
	assert(s->nr < sizeof(s->buf));
	if (ss->access == ST_WRITE) {
		/* flush the rest of buffer (if s->nr > 0), then set the
		 * last bit to 1 to to indicate user-instigated flush */
#ifdef BSTREAM_DEBUG
		if (s->nr > 0) {
			unsigned i;

			printf("W %s %u \"", ss->name, s->nr);
			for (i = 0; i < s->nr; i++)
				if (' ' <= s->buf[i] && s->buf[i] < 127)
					putchar(s->buf[i]);
				else
					printf("\\%03o", s->buf[i]);
			printf("\"\n");
			printf("W %s 0\n", ss->name);
		}
#endif
		blksize = (short) (s->nr << 1);
		s->bytes += s->nr;
		/* indicate that this is the last buffer of a block by
		   setting the low-order bit */
		blksize |= (short) 1;
		/* allways flush (even empty blocks) needed for the protocol) */
#ifdef WORDS_BIGENDIAN
		blksize = short_int_SWAP(blksize);
#endif
		if ((!mnstr_writeSht(s->s, blksize) || s->s->write(s->s, s->buf, 1, s->nr) != (ssize_t) s->nr)) {
			ss->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		s->blks++;
		s->nr = 0;
	}
	return 0;
}

/* Read buffered data and return the number of items read.  At the
   flush boundary we will return 0 to indicate the end of a block.

   Structure field usage:
   s - the underlying stream;
   buf - not used;
   itotal - the amount of data in the current block that hasn't yet
            been read;
   nr - indicates whether the flush marker has to be returned.
 */
static ssize_t
bs_read(stream *ss, void *buf, size_t elmsize, size_t cnt)
{
	bs *s = (bs *) ss->stream_data.p;
	size_t todo = cnt * elmsize;
	size_t n;

	assert(ss->access == ST_READ);
	assert(s->nr <= 1);

	if (s->itotal == 0) {
		short blksize = 0;

		if (s->nr) {
			/* We read the closing block but hadn't returned that yet.
			   Return it now, and note that we did by setting s->nr to
			   0.
			 */
			assert(s->nr == 1);
			s->nr = 0;
			return 0;
		}

		assert(s->nr == 0);

		/* There is nothing more to read in the current block,
		   so read the count for the next block */
		if (!mnstr_readSht(s->s, &blksize) || blksize < 0) {
			ss->errnr = MNSTR_READ_ERROR;
			return -1;
		}
#ifdef BSTREAM_DEBUG
		printf("RC size: %d, final: %s\n", blksize >> 1, blksize & 1 ? "true" : "false");
		printf("RC %s %d\n", ss->name, blksize);
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
		/* there is more data waiting in the current block,
		   so read it */
		n = todo < s->itotal ? todo : s->itotal;
		while (n > 0) {
			ssize_t m = s->s->read(s->s, buf, 1, n);

			if (m <= 0) {
				ss->errnr = MNSTR_READ_ERROR;
				return -1;
			}
#ifdef BSTREAM_DEBUG
			{
				ssize_t i;

				printf("RD %s %zd \"", ss->name, m);
				for (i = 0; i < m; i++)
					if (' ' <= ((char *) buf)[i] && ((char *) buf)[i] < 127)
						putchar(((char *) buf)[i]);
					else
						printf("\\%03o", ((char *) buf)[i]);
				printf("\"\n");
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
			   so read the count for the next block, only if the
			   previous was not the last one */
			if (s->nr) {
				break;
			} else if (!mnstr_readSht(s->s, &blksize) || blksize < 0) {
				ss->errnr = MNSTR_READ_ERROR;
				return -1;
			}
#ifdef BSTREAM_DEBUG
			printf("RC size: %d, final: %s\n", blksize >> 1, blksize & 1 ? "true" : "false");
			printf("RC %s %d\n", ss->name, s->nr);
			printf("RC %s %d\n", ss->name, blksize);
#endif
			s->itotal = (unsigned) (blksize >> 1);	/* amount readable */
			/* store whether this was the last block or not */
			s->nr = blksize & 1;
			s->bytes += s->itotal;
			s->blks++;
		}
	}
	/* if we got an empty block with the end-of-sequence marker
	   set (low-order bit) we must only return an empty read once, so
	   we must squash the flag that we still have to return an empty
	   read */
	if (todo > 0 && cnt == 0)
		s->nr = 0;
	return (ssize_t) (cnt / elmsize);
}

/* Read the next bit of a block.  If this was the last bit of the
   current block, set the value pointed to by last to 1, otherwise set
   it to 0. */
ssize_t
bs_read_next(stream *ss, void *buf, size_t nbytes, int *last)
{
	ssize_t n;
	bs *s = (bs *) ss->stream_data.p;

	n = bs_read(ss, buf, 1, nbytes);
	if (n < 0) {
		if (last)
			*last = 1;
		return -1;
	}
	if (last)
		*last = s->itotal == 0;
	if (s->itotal == 0) {
		/* we don't want to get an empty buffer at the next read */
		s->nr = 0;
	}
	return n;
}

static void
bs_close(stream *ss)
{
	bs *s = (bs *) ss->stream_data.p;

	assert(s);
	assert(s->s);
	s->s->close(s->s);
}

static void
bs_destroy(stream *ss)
{
	bs *s = (bs *) ss->stream_data.p;

	assert(s);
	assert(s->s);
	s->s->destroy(s->s);
	free(s);
	destroy(ss);
}

stream *
block_stream(stream *s)
{
	stream *ns;
	bs *b;

#ifdef STREAM_DEBUG
	printf("block_stream %s\n", s->name ? s->name : "<unnamed>");
#endif
	if ((ns = create_stream(s->name)) == NULL)
		return NULL;
	if ((b = bs_create(s)) == NULL)
		ns->errnr = MNSTR_OPEN_ERROR;
	/* blocksizes have a fixed little endian byteorder */
#ifdef WORDS_BIGENDIAN
	s->byteorder = 3412;	/* simply != 1234 */
#endif
	ns->type = s->type;
	ns->access = s->access;
	ns->read = bs_read;
	ns->write = bs_write;
	ns->close = bs_close;
	ns->flush = bs_flush;
	ns->destroy = bs_destroy;
	ns->stream_data.p = (void *) b;

	return ns;
}

int
isa_block_stream(stream *s)
{
	assert(s != NULL);
	return s->read == bs_read || s->write == bs_write;
}

/* ------------------------------------------------------------------ */

/* A tee stream just duplicates the output of a stream to to streams */

typedef struct {
	stream *orig, *log;
} tee_stream;

static ssize_t
ts_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	tee_stream *ts = (tee_stream *) s->stream_data.p;
	ssize_t reorig = ts->orig->write(ts->orig, buf, elmsize, cnt);
	ssize_t relog = ts->log->write(ts->log, buf, elmsize, cnt);
	return (reorig < relog) ? reorig : relog;
}

static int
ts_flush(stream *s)
{
	tee_stream *ts = (tee_stream *) s->stream_data.p;
	int reorig = ts->orig->flush(ts->orig);
	int relog = ts->log->flush(ts->log);
	return (reorig < relog) ? reorig : relog;
}

static void
ts_close(stream *s)
{
	tee_stream *ts = (tee_stream *) s->stream_data.p;
	ts->orig->close(ts->orig);
	ts->log->close(ts->log);
}

static void
ts_destroy(stream *s)
{
	tee_stream *ts = (tee_stream *) s->stream_data.p;
	ts->orig->destroy(ts->orig);
	ts->log->destroy(ts->log);
	free(ts);
	destroy(s);
}

void
detach_teestream(stream *s)
{
	tee_stream *ts = (tee_stream *) s->stream_data.p;
	ts->log->close(ts->log);
	ts->log->destroy(ts->log);
	free(ts);
	destroy(s);
}

stream *
attach_teestream(stream *orig, stream *log)
{
	tee_stream *ts;
	stream *ns;

	/* we require two write streams of the same type */
	if (orig == NULL || log == NULL || orig->access != ST_WRITE || log->access != ST_WRITE)
		return NULL;
	ts = (tee_stream *) malloc(sizeof(tee_stream));

	if (ts == NULL)
		return NULL;
	ts->orig = orig;
	ts->log = log;

#ifdef STREAM_DEBUG
	printf("tee_stream %s %s\n", orig->name ? orig->name : "<unnamed>", log->name ? log->name : "<unnamed>");
#endif
	if ((ns = create_stream(orig->name)) == NULL) {
		free(ts);
		return NULL;
	}
	/* blocksizes have a fixed little endian byteorder */
#ifdef WORDS_BIGENDIAN
	ns->byteorder = 3412;	/* simply != 1234 */
#endif
	ns->type = orig->type;
	ns->access = orig->access;
	ns->write = ts_write;
	ns->close = ts_close;
	ns->flush = ts_flush;
	ns->destroy = ts_destroy;
	ns->stream_data.p = (void *) ts;

	return ns;
}


/* ------------------------------------------------------------------ */

ssize_t
mnstr_read_block(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	ssize_t len = 0;
	char x = 0;

	assert(s->read == bs_read || s->write == bs_write);
	len = mnstr_read(s, buf, elmsize, cnt);
	mnstr_read(s, &x, 0, 0);	/* read prompt */
	if (x > 0)
		return -1;
	return len;
}

int
mnstr_readBte(stream *s, signed char *val)
{
	switch (s->read(s, (void *) val, sizeof(*val), 1)) {
	case 1:
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
mnstr_writeBte(stream *s, signed char val)
{
	if (!s || s->errnr)
		return (0);
	return s->write(s, (void *) &val, sizeof(val), 1) == 1;
}

int
mnstr_readSht(stream *s, short *val)
{
	switch (s->read(s, (void *) val, sizeof(*val), 1)) {
	case 1:
		if (s->byteorder != 1234)
			*val = short_int_SWAP(*val);
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
mnstr_writeSht(stream *s, short val)
{
	if (!s || s->errnr)
		return (0);
	return s->write(s, (void *) &val, sizeof(val), 1) == 1;
}

int
mnstr_readInt(stream *s, int *val)
{
	switch (s->read(s, (void *) val, sizeof(*val), 1)) {
	case 1:
		if (s->byteorder != 1234)
			*val = normal_int_SWAP(*val);
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
mnstr_writeInt(stream *s, int val)
{
	if (!s || s->errnr)
		return (0);
	return s->write(s, (void *) &val, sizeof(val), (size_t) 1) == 1;
}

int
mnstr_readLng(stream *s, lng *val)
{
	switch (s->read(s, (void *) val, sizeof(*val), 1)) {
	case 1:
		if (s->byteorder != 1234)
			*val = long_long_SWAP(*val);
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
mnstr_writeLng(stream *s, lng val)
{
	if (!s || s->errnr)
		return (0);
	return s->write(s, (void *) &val, sizeof(val), (size_t) 1) == 1;
}


int
mnstr_readBteArray(stream *s, signed char *val, size_t cnt)
{
	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
		s->errnr = MNSTR_READ_ERROR;
		return 0;
	}

	return 1;
}

int
mnstr_writeBteArray(stream *s, const signed char *val, size_t cnt)
{
	if (!s || s->errnr)
		return (0);
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readShtArray(stream *s, short *val, size_t cnt)
{
	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
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
	if (!s || s->errnr)
		return (0);
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readIntArray(stream *s, int *val, size_t cnt)
{
	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
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
	if (!s || s->errnr)
		return (0);
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readLngArray(stream *s, lng *val, size_t cnt)
{
	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
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
	if (!s || s->errnr)
		return (0);
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_printf(stream *s, const char *format, ...)
{
	char buf[BUFSIZ], *bf = buf;
	int i = 0;
	size_t bfsz = BUFSIZ;
	va_list ap;
	if (!s || s->errnr)
		return (-1);

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

	if ((b = malloc(sizeof(*b))) == NULL)
		return NULL;
	b->mode = (int) size;	/* 64bit: should check that size isn't too large and fits in an int */
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

	if (s->eof)
		return 0;

	if (s->pos > 0) {
		if (s->pos < s->len)
			/* move all data and end of string marker */
			memmove(s->buf, s->buf + s->pos, s->len - s->pos + 1);
		s->len -= s->pos;
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
		if (s->pos < s->len)
			/* move all data and end of string marker */
			memmove(s->buf, s->buf + s->pos, s->len - s->pos + 1);
		s->len -= s->pos;
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
	if (s->mode) {
		return bstream_read(s, s->mode);
	} else if (s->s->read == file_read) {
		return bstream_readline(s);
	} else {
		ssize_t sz = 0, rd;

		while ((rd = bstream_read(s, 1)) == 1 && s->buf[s->pos + sz] != '\n') {
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
	s->s->close(s->s);
	s->s->destroy(s->s);
	free(s->buf);
	free(s);
}

/* ------------------------------------------------------------------ */

/*
 * Buffered write stream batches subsequent write requests in order to optimize write bandwidth
 */
typedef struct {
	stream *s;
	size_t len, pos;
	char buf[1];		/* NOTE: buf extends beyond array for wbs->len bytes */
} wbs_stream;

static int
wbs_flush(stream *s)
{
	wbs_stream *wbs = (wbs_stream *) s->stream_data.p;
	size_t len = wbs->pos;
	wbs->pos = 0;
	if ((*wbs->s->write) (wbs->s, wbs->buf, 1, len) != (ssize_t) len)
		return -1;
	if (wbs->s->flush)
		return (*wbs->s->flush) (wbs->s);
	return 0;
}

static ssize_t
wbs_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	wbs_stream *wbs = (wbs_stream *) s->stream_data.p;
	size_t nbytes, reqsize = cnt * elmsize, todo = reqsize;
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
	(*wbs->s->close) (wbs->s);
}

static void
wbs_destroy(stream *s)
{
	wbs_stream *wbs = (wbs_stream *) s->stream_data.p;
	(*wbs->s->destroy) (wbs->s);
	free(wbs);
	destroy(s);
}

stream *
wbstream(stream *s, size_t buflen)
{
	stream *ns = create_stream(s->name);
	if (ns) {
		wbs_stream *wbs = (wbs_stream *) malloc(sizeof(wbs_stream) + buflen - 1);
		if (wbs) {
			ns->type = s->type;
			ns->access = s->access;
			ns->write = wbs_write;
			ns->flush = wbs_flush;
			ns->close = wbs_close;
			ns->destroy = wbs_destroy;
			ns->stream_data.p = (void *) wbs;
			wbs->s = s;
			wbs->pos = 0;
			wbs->len = buflen;
		} else {
			mnstr_destroy(ns);
			ns = NULL;
		}
	}
	return ns;
}

/* ------------------------------------------------------------------ */

#ifdef HAVE_PIPE
static ssize_t
pipe_write(stream *s, const void *buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	ssize_t sz;

	if (s->errnr)
		return -1;

	if (size == 0 || elmsize == 0)
		return (ssize_t) cnt;
	sz = write(s->stream_data.i, buf,
#ifdef NATIVE_WIN32
		   (unsigned int)	/* on Windows, the length is an unsigned int... */
#endif
		   size);
	if (sz > 0)
		return (ssize_t) (sz / elmsize);
	if (sz < 0)
		s->errnr = MNSTR_WRITE_ERROR;
	return sz;
}

static ssize_t
pipe_read(stream *s, void *buf, size_t elmsize, size_t cnt)
{
	ssize_t nr = 0;
	size_t res = 0, size = elmsize * cnt;

	if (s->errnr)
		return -1;

	while (res < size && (nr = read(s->stream_data.i, (void *) ((char *) buf + res),
#ifdef NATIVE_WIN32
					(unsigned int)	/* on Windows, the length is an unsigned int... */
#endif
					(size - res))) > 0) {
		res += nr;
	}
	if (nr < 0) {
		s->errnr = MNSTR_READ_ERROR;
		return nr;
	}
	if (res)
		return (ssize_t) (res / elmsize);
	return 0;
}

static void
pipe_close(stream *s)
{
	if (s->stream_data.i >= 0)
		close(s->stream_data.i);
	s->stream_data.i = -1;
}

int
rendezvous_streams(stream **in, stream **out, const char *name)
{
	stream *sin, *sout;
	int pipes[2];

	if ((sin = create_stream(name)) == NULL)
		return 0;
	if ((sout = create_stream(name)) == NULL) {
		destroy(sin);
		return 0;
	}
	if (pipe(pipes) != 0) {
		destroy(sin);
		destroy(sout);
		return 0;
	}
	sin->access = ST_READ;
	sin->close = pipe_close;
	sin->read = pipe_read;
	sin->stream_data.i = pipes[0];
	sin->type = ST_BIN;
	sout->access = ST_WRITE;
	sout->close = pipe_close;
	sout->stream_data.i = pipes[1];
	sout->type = ST_BIN;
	sout->write = pipe_write;
	*in = sin;
	*out = sout;
	return 1;
}
#else
int
rendezvous_streams(stream **in, stream **out, const char *name)
{
	(void) in;
	(void) out;
	(void) name;
	return 0;
}
#endif /* HAVE_PIPE */
