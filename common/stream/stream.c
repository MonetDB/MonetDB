/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include <stdio.h>


#ifdef HAVE_PTHREAD_H
#include <pthread.h>
#endif

struct tl_error_buf {
	char msg[1024];
};

static int tl_error_init(void);
static struct tl_error_buf *get_tl_error_buf(void);

#ifdef HAVE_PTHREAD_H

static pthread_key_t tl_error_key;

static int
tl_error_init(void)
{
	if (pthread_key_create(&tl_error_key, free) != 0)
		return -1;
	return 0;
}

static struct tl_error_buf*
get_tl_error_buf(void)
{
	struct tl_error_buf *p = pthread_getspecific(tl_error_key);
	if (p == NULL) {
		p = malloc(sizeof(*p));
		if (p == NULL)
			return NULL;
		*p = (struct tl_error_buf) { .msg = {0} };
		pthread_setspecific(tl_error_key, p);
		struct tl_error_buf *second_attempt = pthread_getspecific(tl_error_key);
		assert(p == second_attempt /* maybe mnstr_init has not been called? */);
		(void) second_attempt; // suppress warning if asserts disabled
	}
	return p;
}

#elif defined(WIN32)

static DWORD tl_error_key = 0;

static int
tl_error_init(void)
{
	DWORD key = TlsAlloc();
	if (key == TLS_OUT_OF_INDEXES)
		return -1;
	else {
		tl_error_key = key;
		return 0;
	}
}

static struct tl_error_buf*
get_tl_error_buf(void)
{
	struct tl_error_buf *p = TlsGetValue(tl_error_key);

	if (p == NULL) {
		if (GetLastError() != ERROR_SUCCESS)
			return NULL; // something went terribly wrong

		// otherwise, initialize
		p = malloc(sizeof(*p));
		if (p == NULL)
			return NULL;
		*p = (struct tl_error_buf) { .msg = 0 };
		if (!TlsSetValue(tl_error_key, p)) {
			free(p);
			return NULL;
		}

		struct tl_error_buf *second_attempt = TlsGetValue(tl_error_key);
		assert(p == second_attempt /* maybe mnstr_init has not been called? */);
		(void) second_attempt; // suppress warning if asserts disabled
	}
	return p;
}

#else

#error "no pthreads and no Windows, don't know what to do"

#endif

static const char *mnstr_error_kind_description(mnstr_error_kind kind);

int
mnstr_init(void)
{
	static ATOMIC_FLAG inited = ATOMIC_FLAG_INIT;

	if (ATOMIC_TAS(&inited))
		return 0;

	if (tl_error_init()< 0)
		return -1;

#ifdef NATIVE_WIN32
	WSADATA w;
	if (WSAStartup(0x0101, &w) != 0)
		return -1;
#endif

	return 0;
}

const char *
mnstr_version(void)
{
	return STREAM_VERSION;
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
	if (s->errkind != MNSTR_NO__ERROR)
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
	if (s->errkind != MNSTR_NO__ERROR)
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
	if (s->errkind != MNSTR_NO__ERROR)
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
mnstr_settimeout(stream *s, unsigned int ms, bool (*func)(void *), void *data)
{
	if (s) {
		s->timeout = ms;
		s->timeout_func = func;
		s->timeout_data = data;
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

void
mnstr_va_set_error(stream *s, mnstr_error_kind kind, const char *fmt, va_list ap)
{
	if (s == NULL)
		return;

	s->errkind = kind;

	if (kind == MNSTR_NO__ERROR) {
		s->errmsg[0] = '\0';
		return;
	}

	char *start = &s->errmsg[0];
	char *end = start + sizeof(s->errmsg);
	if (s->name != NULL)
		start += snprintf(start, end - start, "stream %s: ", s->name);

	if (start >= end - 1)
		return;

	if (fmt == NULL)
		fmt = mnstr_error_kind_description(kind);

	// Complicated pointer dance in order to shut up 'might be a candidate
	// for gnu_printf format attribute' warning from gcc.
	// It's really eager to trace where the vsnprintf ends up, we need
	// the ? : to throw it off its scent.
	// Similarly, the parentheses around the 1 serve to suppress a Clang
	// warning about dead code (the atoi).
	void *f1 = (1) ? (void*)&vsnprintf : (void*)&atoi;
	int (*f)(char *str, size_t size, const char *format, va_list ap) = f1;
	f(start, end - start, fmt, ap);
}

void
mnstr_set_error(stream *s, mnstr_error_kind kind, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	mnstr_va_set_error(s, kind, fmt, ap);
	va_end(ap);
}

static size_t my_strerror_r(int error_nr, char *buf, size_t len);

void
mnstr_set_error_errno(stream *s, mnstr_error_kind kind, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	mnstr_va_set_error(s, kind, fmt, ap);
	va_end(ap);

	/* append as much as fits of the system error message */
	char *start = &s->errmsg[0] + strlen(s->errmsg);
	char *end = &s->errmsg[0] + sizeof(s->errmsg);
	if (end - start >= 3) {
		start = stpcpy(start, ": ");
		start += my_strerror_r(errno, start, end - start);
	}
}


void
mnstr_set_open_error(const char *name, int errnr, const char *fmt, ...)
{
	va_list ap;

	struct tl_error_buf *buf = get_tl_error_buf();
	if (buf == NULL)
		return; // hopeless

	if (errnr == 0 && fmt == NULL) {
		buf->msg[0] = '\0';
		return;
	}

	char *start = &buf->msg[0];
	char *end = start + sizeof(buf->msg);

	if (name != NULL)
		start += snprintf(start, end - start, "when opening %s: ", name);
	if (start >= end - 1)
		return;

	if (fmt != NULL) {
		va_start(ap, fmt);
		start += vsnprintf(start, end - start, fmt, ap);
		va_end(ap);
	}
	if (start >= end - 1)
		return;

	if (errnr != 0 && end - start >= 3) {
		start = stpcpy(start, ": ");
		start += my_strerror_r(errno, start, end - start);
	}
	if (start >= end - 1)
		return;
}

static size_t
my_strerror_r(int error_nr, char *buf, size_t buflen)
{
	// Three cases:
	// 1. no strerror_r
	// 2. gnu strerror_r (returns char* and does not always fill buffer)
	// 3. xsi strerror_r (returns int and always fills the buffer)
	char *to_move;
#ifndef HAVE_STRERROR_R
	// Hope for the best
	to_move = strerror(error_nr);
#elif !defined(_GNU_SOURCE) || !_GNU_SOURCE
	// standard strerror_r always writes to buf
	int result_code = strerror_r(error_nr, buf, buflen);
	if (result_code == 0)
		to_move = NULL;
	else
		to_move = "<failed to retrieve error message>";
#else
	// gnu strerror_r sometimes only returns static string, needs copy
	to_move = strerror_r(error_nr, buf, buflen);
#endif
	if (to_move != NULL) {
		// move to buffer
		size_t size = strlen(to_move) + 1;
		assert(size <= buflen);
		// strerror_r may have return a pointer to/into the buffer
		memmove(buf, to_move, size);
		return size - 1;
	} else {
		return strlen(buf);
	}
}



void mnstr_copy_error(stream *dst, stream *src)
{
	dst->errkind = src->errkind;
	memcpy(dst->errmsg, src->errmsg, sizeof(dst->errmsg));
}

char *
mnstr_error(const stream *s)
{
	const char *msg = mnstr_peek_error(s);
	if (msg != NULL)
		return strdup(msg);
	else
		return NULL;
}

const char*
mnstr_peek_error(const stream *s)
{
	if (s == NULL) {
		struct tl_error_buf *b = get_tl_error_buf();
		if (b != NULL)
			return b->msg;
		else
			return "unknown error";
	}

	if (s->errkind == MNSTR_NO__ERROR)
		return "no error";

	if (s->errmsg[0] != '\0')
		return s->errmsg;

	return mnstr_error_kind_description(s->errkind);
}

static const char *
mnstr_error_kind_description(mnstr_error_kind kind)
{
	switch (kind) {
	case MNSTR_NO__ERROR:
		/* unreachable */
		assert(0);
		return NULL;
	case MNSTR_OPEN_ERROR:
		return "error could not open";
	case MNSTR_READ_ERROR:
		return "error reading";
	case MNSTR_WRITE_ERROR:
		return "error writing";
	case MNSTR_TIMEOUT:
		return "timeout";
	case MNSTR_UNEXPECTED_EOF:
		return "timeout";
	}

	return "Unknown error";
}

/* flush buffer, return 0 on success, non-zero on failure */
int
mnstr_flush(stream *s, mnstr_flush_level flush_level)
{
	if (s == NULL)
		return -1;
#ifdef STREAM_DEBUG
	fprintf(stderr, "flush %s\n", s->name ? s->name : "<unnamed>");
#endif
	assert(!s->readonly);
	if (s->errkind != MNSTR_NO__ERROR)
		return -1;
	if (s->flush)
		return s->flush(s, flush_level);
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
	if (s->errkind != MNSTR_NO__ERROR)
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
	if (s->errkind != MNSTR_NO__ERROR)
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
	if (s->errkind != MNSTR_NO__ERROR)
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
	if (s->errkind != MNSTR_NO__ERROR)
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


mnstr_error_kind
mnstr_errnr(const stream *s)
{
	if (s == NULL)
		return MNSTR_READ_ERROR;
	return s->errkind;
}

const char *
mnstr_error_kind_name(mnstr_error_kind k)
{
	switch (k) {
	case MNSTR_NO__ERROR:
		return "MNSTR_NO__ERROR";
	case MNSTR_OPEN_ERROR:
		return "MNSTR_OPEN_ERROR";
	case MNSTR_READ_ERROR:
		return "MNSTR_READ_ERROR";
	case MNSTR_WRITE_ERROR:
		return "MNSTR_WRITE_ERROR";
	case MNSTR_TIMEOUT:
		return "MNSTR_TIMEOUT";
	default:
		return "<UNKNOWN_ERROR>";
	}

}
void
mnstr_clearerr(stream *s)
{
	if (s != NULL) {
		s->errkind = MNSTR_NO__ERROR;
		s->errmsg[0] = '\0';
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


stream *
create_stream(const char *name)
{
	stream *s;

	if (name == NULL) {
		mnstr_set_open_error(NULL, 0, "internal error: name not set");
		return NULL;
	}
	if ((s = (stream *) malloc(sizeof(*s))) == NULL) {
		mnstr_set_open_error(name, errno, "malloc failed");
		return NULL;
	}
	*s = (stream) {
		.swapbytes = false,
		.readonly = true,
		.isutf8 = false,	/* not known for sure */
		.binary = false,
		.name = strdup(name),
		.errkind = MNSTR_NO__ERROR,
		.errmsg = {0},
		.destroy = destroy_stream,
	};
	if(s->name == NULL) {
		free(s);
		mnstr_set_open_error(name, errno, "malloc failed");
		return NULL;
	}
#ifdef STREAM_DEBUG
	fprintf(stderr, "create_stream %s -> %p\n",
		name ? name : "<unnamed>", s);
#endif
	mnstr_set_open_error(NULL, 0, NULL); // clear the error
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


static void
wrapper_destroy(stream *s)
{
	s->inner->destroy(s->inner);
	destroy_stream(s);
}


static int
wrapper_flush(stream *s, mnstr_flush_level flush_level)
{
	return s->inner->flush(s->inner, flush_level);
}


static int
wrapper_fsync(stream *s)
{
	return s->inner->fsync(s->inner);
}


static int
wrapper_fgetpos(stream *restrict s, fpos_t *restrict p)
{
	return s->inner->fgetpos(s->inner, p);
}


static int
wrapper_fsetpos(stream *restrict s, fpos_t *restrict p)
{
	return s->inner->fsetpos(s->inner, p);
}


static void
wrapper_update_timeout(stream *s)
{
	s->inner->timeout = s->timeout;
	s->inner->timeout_func = s->timeout_func;
	s->inner->timeout_data = s->timeout_data;
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
	s->destroy = wrapper_destroy;
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
		close_stream(s);

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
	if (c == NULL) {
		close_stream(s);
		file_remove(filename);
	}

	return c;
}

stream *
open_rastream(const char *filename)
{
	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_rastream %s\n", filename);
#endif
	stream *s = open_rstream(filename);
	if (s == NULL)
		return NULL;

	stream *t = create_text_stream(s);
	if (t == NULL)
		close_stream(s);

	return t;
}

stream *
open_wastream(const char *filename)
{
	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_wastream %s\n", filename);
#endif
	stream *s = open_wstream(filename);
	if (s == NULL)
		return NULL;

	stream *t = create_text_stream(s);
	if (t == NULL) {
		close_stream(s);
		file_remove(filename);
	}

	return t;
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
