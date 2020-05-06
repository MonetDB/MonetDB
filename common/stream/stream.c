/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
 * write streams of the same type (txt/bin).
 */

/* Generic stream handling code such as init and close */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"

int
mnstr_init(void)
{
	static ATOMIC_FLAG inited = ATOMIC_FLAG_INIT;

	if (ATOMIC_TAS(&inited))
		return 0;

#ifdef NATIVE_WIN32
	{
		WSADATA w;

		if (WSAStartup(0x0101, &w) != 0)
			return -1;
	}
#endif
	return 0;
}


/* Read at most cnt elements of size elmsize from the stream.  Returns
 * the number of elements actually read or < 0 on failure. */
ssize_t
mnstr_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	if (s == NULL || buf == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "read %s %zu %zu\n",
		s->name ? s->name : "<unnamed>", elmsize, cnt);
#endif
	assert(s->readonly);
	if (s->errnr)
		return -1;
	return s->read(s, buf, elmsize, cnt);
}


/* Write cnt elements of size elmsize to the stream.  Returns the
 * number of elements actually written.  If elmsize or cnt equals zero,
 * returns cnt. */
ssize_t
mnstr_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	if (s == NULL || buf == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "write %s %zu %zu\n",
		s->name ? s->name : "<unnamed>", elmsize, cnt);
#endif
	assert(!s->readonly);
	if (s->errnr)
		return -1;
	return s->write(s, buf, elmsize, cnt);
}


/* Read one line (seperated by \n) of at most maxcnt-1 characters from
 * the stream.  Returns the number of characters actually read,
 * includes the trailing \n; terminated by a NULL byte. */
ssize_t
mnstr_readline(stream *restrict s, void *restrict buf, size_t maxcnt)
{
	char *b = buf, *start = buf;

	if (s == NULL || buf == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "readline %s %zu\n",
		s->name ? s->name : "<unnamed>", maxcnt);
#endif
	assert(s->readonly);
	if (s->errnr)
		return -1;
	if (maxcnt == 0)
		return 0;
	if (maxcnt == 1) {
		*start = 0;
		return 0;
	}
	for (;;) {
		switch (s->read(s, start, 1, 1)) {
		case 1:
			/* successfully read a character,
			 * check whether it is the line
			 * separator and whether we have space
			 * left for more */
			if (*start++ == '\n' || --maxcnt == 1) {
				*start = 0;
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


void
mnstr_settimeout(stream *s, unsigned int ms, bool (*func)(void))
{
	if (s) {
		s->timeout = ms;
		s->timeout_func = func;
		if (s->update_timeout)
			s->update_timeout(s);
	}
}


void
mnstr_close(stream *s)
{
	if (s) {
#ifdef STREAM_DEBUG
		fprintf(stderr, "close %s\n", s->name ? s->name : "<unnamed>");
#endif
		s->close(s);
	}
}


void
mnstr_destroy(stream *s)
{
	if (s) {
#ifdef STREAM_DEBUG
		fprintf(stderr, "destroy %s\n",
			s->name ? s->name : "<unnamed>");
#endif
		s->destroy(s);
	}
}


char *
mnstr_error(const stream *s)
{
	if (s == NULL)
		return "Connection terminated";
	return s->error(s);
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
	assert(!s->readonly);
	if (s->errnr)
		return -1;
	if (s->flush)
		return s->flush(s);
	return 0;
}


/* sync file to disk, return 0 on success, non-zero on failure */
int
mnstr_fsync(stream *s)
{
	if (s == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "fsync %s (%d)\n",
		s->name ? s->name : "<unnamed>", s->errnr);
#endif
	assert(!s->readonly);
	if (s->errnr)
		return -1;
	if (s->fsync)
		return s->fsync(s);
	return 0;
}


int
mnstr_fgetpos(stream *restrict s, fpos_t *restrict p)
{
	if (s == NULL || p == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "fgetpos %s\n", s->name ? s->name : "<unnamed>");
#endif
	if (s->errnr)
		return -1;
	if (s->fgetpos)
		return s->fgetpos(s, p);
	return 0;
}


int
mnstr_fsetpos(stream *restrict s, fpos_t *restrict p)
{
	if (s == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "fsetpos %s\n", s->name ? s->name : "<unnamed>");
#endif
	if (s->errnr)
		return -1;
	if (s->fsetpos)
		return s->fsetpos(s, p);
	return 0;
}


int
mnstr_isalive(const stream *s)
{
	if (s == NULL)
		return 0;
	if (s->errnr)
		return -1;
	if (s->isalive)
		return s->isalive(s);
	return 1;
}


char *
mnstr_name(const stream *s)
{
	if (s == NULL)
		return "connection terminated";
	return s->name;
}


int
mnstr_errnr(const stream *s)
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
			s->clrerr(s);
	}
}


bool
mnstr_isbinary(const stream *s)
{
	if (s == NULL)
		return false;
	return s->binary;
}


bool
mnstr_get_swapbytes(const stream *s)
{
	if (s == NULL)
		return 0;
	return s->swapbytes;
}


/* set stream to big-endian/little-endian byte order; the default is
 * native byte order */
void
mnstr_set_bigendian(stream *s, bool bigendian)
{
	if (s == NULL)
		return;
#ifdef STREAM_DEBUG
	fprintf(stderr, "mnstr_set_bigendian %s %s\n",
		s->name ? s->name : "<unnamed>",
		swapbytes ? "true" : "false");
#endif
	assert(s->readonly);
	s->binary = true;
#ifdef WORDS_BIGENDIAN
	s->swapbytes = !bigendian;
#else
	s->swapbytes = bigendian;
#endif
}


void
close_stream(stream *s)
{
	if (s) {
		if (s->close)
			s->close(s);
		if (s->destroy)
			s->destroy(s);
	}
}


void
destroy_stream(stream *s)
{
	if (s->name)
		free(s->name);
	free(s);
}


static char *
error(const stream *s)
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


stream *
create_stream(const char *name)
{
	stream *s;

	if (name == NULL)
		return NULL;
	if ((s = (stream *) malloc(sizeof(*s))) == NULL)
		return NULL;
	*s = (stream) {
		.swapbytes = false,
		.readonly = true,
		.isutf8 = false,	/* not known for sure */
		.binary = false,
		.name = strdup(name),
		.errnr = MNSTR_NO__ERROR,
		.error = error,
		.destroy = destroy_stream,
	};
	if(s->name == NULL) {
		free(s);
		return NULL;
	}
#ifdef STREAM_DEBUG
	fprintf(stderr, "create_stream %s -> %p\n",
		name ? name : "<unnamed>", s);
#endif
	return s;
}


static ssize_t
wrapper_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	return s->inner->read(s->inner, buf, elmsize, cnt);
}


static ssize_t
wrapper_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	return s->inner->write(s->inner, buf, elmsize, cnt);
}


static void
wrapper_close(stream *s)
{
	s->inner->close(s->inner);
}


static void
wrapper_clrerr(stream *s)
{
	s->inner->clrerr(s->inner);
}


static char *
wrapper_error(const stream *s)
{
	return s->inner->error(s->inner);
}


static void
wrapper_destroy(stream *s)
{
	s->inner->destroy(s->inner);
	destroy_stream(s);
}


static int
wrapper_flush(stream *s)
{
	return s->inner->flush(s->inner);
}


static int
wrapper_fsync(stream *s)
{
	return s->inner->fsync(s->inner);
}


static int
wrapper_fgetpos(stream *s, fpos_t *restrict p)
{
	return s->inner->fgetpos(s->inner, p);
}


static int
wrapper_fsetpos(stream *s, fpos_t *restrict p)
{
	return s->inner->fsetpos(s->inner, p);
}


static void
wrapper_update_timeout(stream *s)
{
	s->inner->timeout = s->timeout;
	s->inner->timeout_func = s->timeout_func;
	s->inner->update_timeout(s->inner);
}


static int
wrapper_isalive(const stream *s)
{
	return s->inner->isalive(s->inner);
}


stream *
create_wrapper_stream(const char *name, stream *inner)
{
	if (inner == NULL)
		return NULL;
	if (name == NULL)
		name = inner->name;
	stream *s = create_stream(name);
	if (s == NULL)
		return NULL;


	s->swapbytes = inner->swapbytes;
	s->readonly = inner->readonly;
	s->isutf8 = inner->isutf8;
	s->binary = inner->binary;
	s->timeout = inner->timeout;
	s->inner = inner;

	s->read = inner->read == NULL ? NULL : wrapper_read;
	s->write = inner->write == NULL ? NULL : wrapper_write;
	s->close = inner->close == NULL ? NULL : wrapper_close;
	s->clrerr = inner->clrerr == NULL ? NULL : wrapper_clrerr;
	s->error = inner->error == NULL ? NULL : wrapper_error;
	s->destroy = inner->destroy == NULL ? NULL : wrapper_destroy;
	s->flush = inner->flush == NULL ? NULL : wrapper_flush;
	s->fsync = inner->fsync == NULL ? NULL : wrapper_fsync;
	s->fgetpos = inner->fgetpos == NULL ? NULL : wrapper_fgetpos;
	s->fsetpos = inner->fsetpos == NULL ? NULL : wrapper_fsetpos;
	s->isalive = inner->isalive == NULL ? NULL : wrapper_isalive;
	s->update_timeout = inner->update_timeout == NULL ? NULL : wrapper_update_timeout;

	return s;
}

/* ------------------------------------------------------------------ */
/* streams working on a disk file, compressed or not */

stream *
open_rstream(const char *filename)
{
	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_rstream %s\n", filename);
#endif

	stream *s = open_stream(filename, "rb");
	if (s == NULL)
		return NULL;

	stream *c = compressed_stream(s, 0);
	if (c == NULL)
		mnstr_close(s);

	return c;
}

stream *
open_wstream(const char *filename)
{
	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_wstream %s\n", filename);
#endif

	stream *s = open_stream(filename, "wb");
	if (s == NULL)
		return NULL;

	stream *c = compressed_stream(s, 0);
	if (c == NULL)
		mnstr_close(s);

	return c;
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
	ext = get_extension(filename);

	if (strcmp(ext, "gz") == 0)
		return open_gzrastream(filename);
	if (strcmp(ext, "bz2") == 0)
		return open_bzrastream(filename);
	if (strcmp(ext, "xz") == 0)
		return open_xzrastream(filename);
	if (strcmp(ext, "lz4") == 0)
		return open_lz4rastream(filename);

	if ((s = open_stream(filename, "r")) == NULL)
		return NULL;
	s->binary = false;
	return s;
}

stream *
open_wastream(const char *filename)
{
	stream *s;
	const char *ext;

	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_wastream %s\n", filename);
#endif
	ext = get_extension(filename);

	if (strcmp(ext, "gz") == 0)
		return open_gzwastream(filename, "w");
	if (strcmp(ext, "bz2") == 0)
		return open_bzwastream(filename, "w");
	if (strcmp(ext, "xz") == 0)
		return open_xzwastream(filename, "w");
	if (strcmp(ext, "lz4") == 0)
		return open_lz4wastream(filename, "w");

	if ((s = open_stream(filename, "w")) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = false;
	return s;
}


/* put here because it depends on both bs_read AND bs2_read */
bool
isa_block_stream(const stream *s)
{
	assert(s != NULL);
	return s &&
		((s->read == bs_read ||
		  s->write == bs_write) ||
		 (s->read == bs2_read ||
		  s->write == bs2_write));
}


/* Put here because I need to think very carefully about this
 * mnstr_read(,, 0, 0). What would that mean?
 */
ssize_t
mnstr_read_block(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	ssize_t len = 0;
	char x = 0;

	if (s == NULL || buf == NULL)
		return -1;
	assert(s->read == bs_read || s->write == bs_write);
	if ((len = mnstr_read(s, buf, elmsize, cnt)) < 0 ||
	    mnstr_read(s, &x, 0, 0) < 0 /* read prompt */  ||
	    x > 0)
		return -1;
	return len;
}

