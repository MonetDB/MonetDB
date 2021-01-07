/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* Generic stream handling code such as init and close */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


/* ------------------------------------------------------------------ */
/* streams working on a substream, converting character sets using iconv */

#ifdef HAVE_ICONV

struct icstream {
	iconv_t cd;
	char buffer[BUFSIZ];
	size_t buflen;
	bool eof;
};

static ssize_t
ic_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;
	ICONV_CONST char *inbuf;
	size_t inbytesleft = elmsize * cnt;
	char *bf = NULL;

	if (ic == NULL) {
		mnstr_set_error(s, MNSTR_WRITE_ERROR, "stream already ended");
		goto bailout;
	}

	/* if unconverted data from a previous call remains, add it to
	 * the start of the new data, using temporary space */
	if (ic->buflen > 0) {
		bf = malloc(ic->buflen + inbytesleft);
		if (bf == NULL) {
			/* cannot allocate memory */
			mnstr_set_error(s, MNSTR_WRITE_ERROR, "out of memory");
			goto bailout;
		}
		memcpy(bf, ic->buffer, ic->buflen);
		memcpy(bf + ic->buflen, buf, inbytesleft);
		buf = bf;
		inbytesleft += ic->buflen;
		ic->buflen = 0;
	}
	inbuf = (ICONV_CONST char *) buf;
	while (inbytesleft > 0) {
		char *outbuf = ic->buffer;
		size_t outbytesleft = sizeof(ic->buffer);

		if (iconv(ic->cd, &inbuf, &inbytesleft, &outbuf, &outbytesleft) == (size_t) -1) {
			switch (errno) {
			case EILSEQ:
				/* invalid multibyte sequence encountered */
				mnstr_set_error(s, MNSTR_WRITE_ERROR, "invalid multibyte sequence");
				goto bailout;
			case EINVAL:
				/* incomplete multibyte sequence
				 * encountered flush what has been
				 * converted */
				if (outbytesleft < sizeof(ic->buffer) &&
				    mnstr_write(s->inner, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft) < 0) {
					mnstr_set_error(s, MNSTR_WRITE_ERROR, "incomplete multibyte sequence");
					goto bailout;
				}
				/* remember what hasn't been converted */
				if (inbytesleft > sizeof(ic->buffer)) {
					/* ridiculously long multibyte
					 * sequence, so return
					 * error */
					mnstr_set_error(s, MNSTR_WRITE_ERROR, "multibyte sequence too long");
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
				mnstr_set_error(s, MNSTR_WRITE_ERROR, "iconv internal error %d", errno);
				goto bailout;
			}
		}
		if (mnstr_write(s->inner, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft) < 0) {
			mnstr_copy_error(s, s->inner);
			goto bailout;
		}
	}
	if (bf)
		free(bf);
	return (ssize_t) cnt;

	bailout:
	assert(s->errkind != MNSTR_NO__ERROR);
	if (bf)
		free(bf);
	return -1;
}

static ssize_t
ic_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;
	ICONV_CONST char *inbuf;
	size_t inbytesleft;
	char *outbuf;
	size_t outbytesleft;

	if (ic == NULL) {
		mnstr_set_error(s, MNSTR_READ_ERROR, "stream already ended");
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
			mnstr_set_error(s, MNSTR_READ_ERROR, "multibyte sequence too long");
			return -1;
		}

		switch (mnstr_read(s->inner, ic->buffer + ic->buflen, 1, 1)) {
		case 1:
			/* expected: read one byte */
			ic->buflen++;
			inbytesleft++;
			break;
		case 0:
			/* end of file */
			ic->eof = true;
			if (ic->buflen > 0) {
				/* incomplete input */
				mnstr_set_error(s, MNSTR_READ_ERROR, "incomplete input");
				return -1;
			}
			if (iconv(ic->cd, NULL, NULL, &outbuf, &outbytesleft) == (size_t) -1) {
				/* some error occurred */
				mnstr_set_error(s, MNSTR_READ_ERROR, "unspecified iconv error occurred");
				return -1;
			}
			goto exit_func;	/* double break */
		default:
			/* error */
			mnstr_copy_error(s, s->inner);
			return -1;
		}
		if (iconv(ic->cd, &inbuf, &inbytesleft, &outbuf, &outbytesleft) == (size_t) -1) {
			switch (errno) {
			case EILSEQ:
				mnstr_set_error(s, MNSTR_READ_ERROR, "invalid multibyte sequence");
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
				mnstr_set_error(s, MNSTR_READ_ERROR, "inconv stream: internal error");
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
		ic->eof = false;
	}
	return (ssize_t) ((elmsize * cnt - outbytesleft) / elmsize);
}

static int
ic_flush(stream *s, mnstr_flush_level flush_level)
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
	     mnstr_write(s->inner, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft) < 0)) {
		mnstr_copy_error(s, s->inner);
		return -1;
	}
	return mnstr_flush(s->inner, flush_level);
}

static void
ic_close(stream *s)
{
	struct icstream *ic = (struct icstream *) s->stream_data.p;

	if (ic) {
		if (!s->readonly)
			ic_flush(s, MNSTR_FLUSH_DATA);
		iconv_close(ic->cd);
		close_stream(s->inner);
		s->inner = NULL;
		free(s->stream_data.p);
		s->stream_data.p = NULL;
	}
}

static void
ic_destroy(stream *s)
{
	ic_close(s);
	mnstr_destroy(s->inner);
	destroy_stream(s);
}

static stream *
ic_open(iconv_t cd, stream *restrict ss, const char *restrict name)
{
	stream *s;
	struct icstream *ic;

	if (ss->isutf8)
		return ss;
	if ((s = create_wrapper_stream(name, ss)) == NULL)
		return NULL;
	s->read = ic_read;
	s->write = ic_write;
	s->close = ic_close;
	s->destroy = ic_destroy;
	s->flush = ic_flush;
	ic = malloc(sizeof(struct icstream));
	if (ic == NULL) {
		mnstr_destroy(s);
		mnstr_set_open_error(s->name, errno, NULL);
		return NULL;
	}
	s->stream_data.p = ic;
	*ic = (struct icstream) {
		.cd = cd,
		.buflen = 0,
		.eof = false,
	};
	return s;
}

stream *
iconv_rstream(stream *restrict ss, const char *restrict charset, const char *restrict name)
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
	if (cd == (iconv_t) -1) {
		mnstr_set_open_error(name, errno, "iconv_open");
		return NULL;
	}
	s = ic_open(cd, ss, name);
	if (s == NULL) {
		iconv_close(cd);
		return NULL;
	}
	s->readonly = true;
	s->isutf8 = true;
	return s;
}

stream *
iconv_wstream(stream *restrict ss, const char *restrict charset, const char *restrict name)
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
	if (cd == (iconv_t) -1) {
		mnstr_set_open_error(name, errno, "iconv_open");
		return NULL;
	}
	s = ic_open(cd, ss, name);
	if (s == NULL) {
		iconv_close(cd);
		return NULL;
	}
	s->readonly = false;
	return s;
}

#else
stream *
iconv_rstream(stream *restrict ss, const char *restrict charset, const char *restrict name)
{
	if (ss == NULL || charset == NULL || name == NULL)
		return NULL;
	if (ss->isutf8 ||
	    strcmp(charset, "utf-8") == 0 ||
	    strcmp(charset, "UTF-8") == 0 ||
	    strcmp(charset, "UTF8") == 0)
		return ss;

	mnstr_set_open_error(name, 0, "ICONV support has been left out of this MonetDB");
	return NULL;
}

stream *
iconv_wstream(stream *restrict ss, const char *restrict charset, const char *restrict name)
{
	if (ss == NULL || charset == NULL || name == NULL)
		return NULL;
	if (ss->isutf8 ||
	    strcmp(charset, "utf-8") == 0 ||
	    strcmp(charset, "UTF-8") == 0 ||
	    strcmp(charset, "UTF8") == 0)
		return ss;

	mnstr_set_open_error(name, 0, "ICONV support has been left out of this MonetDB");
	return NULL;
}
#endif /* HAVE_ICONV */
