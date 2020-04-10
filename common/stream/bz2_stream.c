/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* streams working on a bzip2-compressed disk file */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


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
		if (s->readonly)
			BZ2_bzReadClose(&err, ((struct bz *) s->stream_data.p)->b);
		else
			BZ2_bzWriteClose(&err, ((struct bz *) s->stream_data.p)->b, 0, NULL, NULL);
		fclose(((struct bz *) s->stream_data.p)->f);
		free(s->stream_data.p);
	}
	s->stream_data.p = NULL;
}

static ssize_t
stream_bzread(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
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
	size = (size_t) BZ2_bzRead(&err, bzp->b, buf, size > ((size_t) 1 << 30) ? 1 << 30 : (int) size);
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
	/* when in text mode, convert \r\n line endings to \n */
	if (!s->binary) {
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
	return (ssize_t) (size / elmsize);
}

static ssize_t
stream_bzwrite(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	int err;
	struct bz *bzp = s->stream_data.p;

	if (bzp == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	if (size == 0)
		return 0;
	while (size > 0) {
		int sz = size > (1 << 30) ? 1 << 30 : (int) size;
		BZ2_bzWrite(&err, bzp->b, (void *) buf, sz);
		if (err != BZ_OK) {
			stream_bzclose(s);
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		size -= (size_t) sz;
	}
	return (ssize_t) cnt;
}

static stream *
open_bzstream(const char *restrict filename, const char *restrict flags)
{
	stream *s;
	int err;
	struct bz *bzp;
	char fl[3];

	if ((bzp = malloc(sizeof(struct bz))) == NULL)
		return NULL;
	if ((s = create_stream(filename)) == NULL) {
		free(bzp);
		return NULL;
	}
	*bzp = (struct bz) {0};
	fl[0] = flags[0];	/* 'r' or 'w' */
	fl[1] = 'b';		/* always binary */
	fl[2] = '\0';
#ifdef HAVE__WFOPEN
	{
		wchar_t *wfname = utf8towchar(filename);
		wchar_t *wflags = utf8towchar(fl);
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
			bzp->f = fopen(fname, fl);
			free(fname);
		} else
			bzp->f = NULL;
	}
#endif
	if (bzp->f == NULL) {
		destroy_stream(s);
		free(bzp);
		return NULL;
	}
	s->read = stream_bzread;
	s->write = stream_bzwrite;
	s->close = stream_bzclose;
	s->flush = NULL;
	s->stream_data.p = (void *) bzp;
	if (flags[0] == 'r' && flags[1] != 'b') {
		s->readonly = true;
		bzp->b = BZ2_bzReadOpen(&err, bzp->f, 0, 0, NULL, 0);
		if (err == BZ_STREAM_END) {
			BZ2_bzReadClose(&err, bzp->b);
			bzp->b = NULL;
		} else {
			char buf[UTF8BOMLENGTH];

			if (stream_bzread(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
			    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
				s->isutf8 = true;
			} else if (s->stream_data.p) {
				bzp = s->stream_data.p;
				BZ2_bzReadClose(&err, bzp->b);
				rewind(bzp->f);
				bzp->b = BZ2_bzReadOpen(&err, bzp->f, 0, 0, NULL, 0);
			}
		}
	} else if (flags[0] == 'r') {
		bzp->b = BZ2_bzReadOpen(&err, bzp->f, 0, 0, NULL, 0);
		s->readonly = true;
	} else {
		bzp->b = BZ2_bzWriteOpen(&err, bzp->f, 9, 0, 30);
		s->readonly = false;
	}
	if (err != BZ_OK) {
		stream_bzclose(s);
		destroy_stream(s);
		return NULL;
	}
	return s;
}

stream *
open_bzrstream(const char *filename)
{
	stream *s;

	if ((s = open_bzstream(filename, "rb")) == NULL)
		return NULL;
	s->binary = true;
	return s;
}

stream *
open_bzwstream(const char *restrict filename, const char *restrict mode)
{
	stream *s;

	if ((s = open_bzstream(filename, mode)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = true;
	return s;
}

stream *
open_bzrastream(const char *filename)
{
	stream *s;

	if ((s = open_bzstream(filename, "r")) == NULL)
		return NULL;
	s->binary = false;
	return s;
}

stream *
open_bzwastream(const char *restrict filename, const char *restrict mode)
{
	stream *s;

	if ((s = open_bzstream(filename, mode)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = false;
	return s;
}
#else

stream *open_bzrstream(const char *filename)
{
	return NULL;
}

stream *open_bzwstream(const char *filename, const char *mode)
{
	return NULL;
}

stream *open_bzrastream(const char *filename)
{
	return NULL;
}

stream *open_bzwastream(const char *filename, const char *mode)
{
	return NULL;
}


#endif

