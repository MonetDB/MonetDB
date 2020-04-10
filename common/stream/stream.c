/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
	ext = get_extension(filename);

	if (strcmp(ext, "gz") == 0)
		return open_gzrstream(filename);
	if (strcmp(ext, "bz2") == 0)
		return open_bzrstream(filename);
	if (strcmp(ext, "xz") == 0)
		return open_xzrstream(filename);
	if (strcmp(ext, "lz4") == 0)
		return open_lz4rstream(filename);

	if ((s = open_stream(filename, "rb")) == NULL)
		return NULL;
	s->binary = true;
	return s;
}

stream *
open_wstream(const char *filename)
{
	stream *s;
	const char *ext;

	if (filename == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "open_wstream %s\n", filename);
#endif
	ext = get_extension(filename);

	if (strcmp(ext, "gz") == 0)
		return open_gzwstream(filename, "wb");
	if (strcmp(ext, "bz2") == 0)
		return open_bzwstream(filename, "wb");
	if (strcmp(ext, "xz") == 0)
		return open_xzwstream(filename, "wb");
	if (strcmp(ext, "lz4") == 0)
		return open_lz4wstream(filename, "wb");

	if ((s = open_stream(filename, "wb")) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = true;
	return s;
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
