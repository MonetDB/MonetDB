/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* streams working on a disk file */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


/* ------------------------------------------------------------------ */
/* streams working on a disk file */


#ifdef _MSC_VER
/* special case code for reading from/writing to a Windows console and
 * for reading from a Windows pipe
 *
 * For reading from and writing to the console we can use a wide
 * character interface which means that we are independent of the code
 * page being used.  We can translate the wide characters (which are
 * Unicode code points) easily to UTF-8.
 *
 * Both for reading from the console and from a pipe, we avoid hanging
 * (waiting for input) in the read function.  Instead, we only call
 * the read function when we know there is input available.  This is
 * to prevent a deadlock situation, especially for reading from pipes,
 * when another thread were to also interact with pipes (as happend in
 * the scipy Python module as used in the sql/backends/monet5/pyapi05
 * test). */

struct console {
	HANDLE h;
	DWORD len;
	DWORD rd;
	unsigned char i;
	uint32_t ch;
	WCHAR wbuf[8192];
};

static ssize_t
console_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	struct console *c = s->stream_data.p;
	size_t n = elmsize * cnt;
	unsigned char *p = buf;

	if (c == NULL) {
		mnstr_set_error(s, MNSTR_READ_ERROR, "closed");
		return -1;
	}
	if (n == 0)
		return 0;
	if (c->rd == c->len) {
		while (WaitForSingleObject(c->h, INFINITE) == WAIT_TIMEOUT)
			;
		if (!ReadConsoleW(c->h, c->wbuf, 8192, &c->len, NULL)) {
			mnstr_set_error(s, MNSTR_READ_ERROR, "console read failed");
			return -1;
		}
		c->rd = 0;
		if (c->len > 0 && c->wbuf[0] == 26) {	/* control-Z */
			c->len = 0;
			return 0;
		}
		if (c->len > 0 && c->wbuf[0] == 0xFEFF)
			c->rd++;	/* skip BOM */
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
console_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	struct console *c = s->stream_data.p;
	size_t n = elmsize * cnt;
	const unsigned char *p = buf;
	uint32_t ch;
	int x;

	if (c == NULL) {
		mnstr_set_error(s, MNSTR_READ_ERROR, "closed");
		return -1;
	}
	if (n == 0)
		return 0;

	c->len = 0;
	if (c->i > 0) {
		while (c->i > 0 && n > 0) {
			if ((*p & 0xC0) != 0x80) {
				mnstr_set_error(s, MNSTR_READ_ERROR, NULL);
				return -1;
			}
			c->ch <<= 6;
			c->ch |= *p & 0x3F;
			p++;
			n--;
			c->i--;
		}
		if (c->i > 0) {
			;
		} else if (c->ch > 0x10FFFF || (c->ch & 0xFFFFF800) == 0xD800) {
			mnstr_set_error(s, MNSTR_READ_ERROR, NULL);
			return -1;
		} else if (c->ch > 0xFFFF) {
			c->wbuf[c->len++] = 0xD800 | ((c->ch >> 10) - (1 << 6));
			c->wbuf[c->len++] = 0xDC00 | (c->ch & 0x03FF);
		} else {
			c->wbuf[c->len++] = c->ch;
		}
	}
	while (n > 0) {
		if (c->len >= 8191) {
			if (!WriteConsoleW(c->h, c->wbuf, c->len, &c->rd, NULL)) {
				mnstr_set_error(s, MNSTR_READ_ERROR, NULL);
				return -1;
			}
			c->len = 0;
		}
		if ((*p & 0x80) == 0) {
			if (*p == '\n')
				c->wbuf[c->len++] = L'\r';
			c->wbuf[c->len++] = *p++;
			n--;
			x = 0;
			continue;
		} else if ((*p & 0xE0) == 0xC0) {
			x = 1;
			ch = *p & 0x1F;
		} else if ((*p & 0xF0) == 0xE0) {
			x = 2;
			ch = *p & 0x0F;
		} else if ((*p & 0xF8) == 0xF0) {
			x = 3;
			ch = *p & 0x07;
		} else {
			mnstr_set_error(s, MNSTR_READ_ERROR, NULL);
			return -1;
		}
		p++;
		n--;
		while (x > 0 && n > 0) {
			if ((*p & 0xC0) != 0x80) {
				mnstr_set_error(s, MNSTR_READ_ERROR, NULL);
				return -1;
			}
			ch <<= 6;
			ch |= *p & 0x3F;
			p++;
			n--;
			x--;
		}
		if (x > 0) {
			c->ch = ch;
			c->i = x;
		} else if (ch > 0x10FFFF || (ch & 0xFFFFF800) == 0xD800) {
			mnstr_set_error(s, MNSTR_READ_ERROR, NULL);
			return -1;
		} else if (ch > 0xFFFF) {
			c->wbuf[c->len++] = 0xD800 | ((ch >> 10) - (1 << 6));
			c->wbuf[c->len++] = 0xDC00 | (ch & 0x03FF);
		} else {
			c->wbuf[c->len++] = ch;
		}
	}
	if (c->len > 0) {
		if (!WriteConsoleW(c->h, c->wbuf, c->len, &c->rd, NULL)) {
			mnstr_set_error(s, MNSTR_READ_ERROR, NULL);
			return -1;
		}
		c->len = 0;
	}
	return (ssize_t) ((p - (const unsigned char *) buf) / elmsize);
}

static ssize_t
pipe_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	HANDLE h = s->stream_data.p;
	size_t n;
	unsigned char *p;
	DWORD nread;

	if (h == NULL) {
				mnstr_set_error(s, MNSTR_READ_ERROR, "closed");
		return -1;
	}
	if (elmsize == 0 || cnt == 0)
		return 0;
  tailrecurse:
	n = elmsize * cnt;
	p = buf;

	for (;;) {
		DWORD ret = PeekNamedPipe(h, NULL, 0, NULL, &nread, NULL);
		if (ret == 0) {
			if (GetLastError() == ERROR_BROKEN_PIPE)
				return 0;
			mnstr_set_error(s, MNSTR_READ_ERROR, "PeekNamedPipe failed");
			return -1;
		}
		if (nread > 0)
			break;
		Sleep(100);
	}
	if ((size_t) nread < n)
		n = (size_t) nread;
	if (!ReadFile(h, buf, (DWORD) n, &nread, NULL)) {
		mnstr_set_error(s, MNSTR_READ_ERROR, "ReadFile failed");
		return -1;
	}
	/* when in text mode, convert \r\n line endings to \n */
	if (!s->binary && nread > 0) {
		char *p1, *p2, *pe;

		p1 = buf;
		pe = p1 + nread;
		while (p1 < pe && *p1 != '\r')
			p1++;
		p2 = p1;
		while (p1 < pe) {
			if (*p1 == '\r' /*&& p1[1] == '\n'*/)
				nread--;
			else
				*p2++ = *p1;
			p1++;
		}
		if (nread == 0) {
			/* try again after removing \r and ending up
			 * with nothing */
			goto tailrecurse;
		}
	}
	return nread / elmsize;
}

static void
console_destroy(stream *s)
{
	if (s->stream_data.p)
		free(s->stream_data.p);
	destroy_stream(s);
}
#endif




/* should be static but isn't because there are some other parts of the
 * library that have a specific fast path for stdio streams.
 */
ssize_t
file_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	FILE *fp = (FILE *) s->stream_data.p;
	size_t rc = 0;

	if (fp == NULL) {
		mnstr_set_error(s, MNSTR_READ_ERROR, "file ended");
		return -1;
	}

	if (elmsize && cnt && !feof(fp)) {
		if (ferror(fp) ||
		    ((rc = fread(buf, elmsize, cnt, fp)) == 0 && ferror(fp))) {
			mnstr_set_error_errno(s, MNSTR_READ_ERROR, "read error");
			return -1;
		}
	}
	return (ssize_t) rc;
}


static ssize_t
file_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL) {
		mnstr_set_error(s, MNSTR_WRITE_ERROR, "file ended");
		return -1;
	}

	if (elmsize && cnt) {
		size_t rc = fwrite(buf, elmsize, cnt, fp);

		if (ferror(fp)) {
			mnstr_set_error_errno(s, MNSTR_WRITE_ERROR, "write error");
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
	} else if (!s->readonly)
		fflush(fp);
	s->stream_data.p = NULL;
}


static void
file_destroy(stream *s)
{
	file_close(s);
	destroy_stream(s);
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

	if (fp == NULL || (!s->readonly && fflush(fp) < 0)) {
			mnstr_set_error_errno(s, MNSTR_WRITE_ERROR, "flush error");
		return -1;
	}
	return 0;
}


static int
file_fsync(stream *s)
{

	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL ||
	    (!s->readonly
#ifdef NATIVE_WIN32
	     && _commit(fileno(fp)) < 0
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
		mnstr_set_error(s, MNSTR_WRITE_ERROR, "fsync failed");
		return -1;
	}
	return 0;
}


static int
file_fgetpos(stream *restrict s, fpos_t *restrict p)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL || p == NULL)
		return -1;
	return fgetpos(fp, p) ? -1 : 0;
}


static int
file_fsetpos(stream *restrict s, fpos_t *restrict p)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL || p == NULL)
		return -1;
	return fsetpos(fp, p) ? -1 : 0;
}

/* This is used in the file opening functions, in a code sequence that is
 * duplicated between open_stream and
 *     open_gzstream open_bzstream open_xzstream open_lz4stream
 * Eventually these will all just use open_stream.
 *
 * Currently in misc.c
 */
#ifdef HAVE__WFOPEN
wchar_t *utf8towchar(const char *src);
#else
char *cvfilename(const char *filename);
#endif

#ifdef HAVE__WFOPEN
/* convert a string from UTF-8 to wide characters; the return value is
 * freshly allocated */
wchar_t *
utf8towchar(const char *src)
{
	wchar_t *dest;
	size_t i = 0;
	size_t j = 0;
	uint32_t c;

	/* count how many wchar_t's we need, while also checking for
	 * correctness of the input */
	while (src[j]) {
		i++;
		if ((src[j+0] & 0x80) == 0) {
			j += 1;
		} else if ((src[j+0] & 0xE0) == 0xC0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+0] & 0x1E) != 0) {
			j += 2;
		} else if ((src[j+0] & 0xF0) == 0xE0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && ((src[j+0] & 0x0F) != 0
			       || (src[j+1] & 0x20) != 0)) {
			j += 3;
		} else if ((src[j+0] & 0xF8) == 0xF0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && (src[j+3] & 0xC0) == 0x80) {
			c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
			if (c < 0x10000
			    || c > 0x10FFFF
			    || (c & 0x1FF800) == 0x00D800)
				return NULL;
#if SIZEOF_WCHAR_T == 2
			i++;
#endif
			j += 4;
		} else {
			return NULL;
		}
	}
	dest = malloc((i + 1) * sizeof(wchar_t));
	if (dest == NULL)
		return NULL;
	/* go through the source string again, this time we can skip
	 * the correctness tests */
	i = j = 0;
	while (src[j]) {
		if ((src[j+0] & 0x80) == 0) {
			dest[i++] = src[j+0];
			j += 1;
		} else if ((src[j+0] & 0xE0) == 0xC0) {
			dest[i++] = (src[j+0] & 0x1F) << 6
				| (src[j+1] & 0x3F);
			j += 2;
		} else if ((src[j+0] & 0xF0) == 0xE0) {
			dest[i++] = (src[j+0] & 0x0F) << 12
				| (src[j+1] & 0x3F) << 6
				| (src[j+2] & 0x3F);
			j += 3;
		} else if ((src[j+0] & 0xF8) == 0xF0) {
			c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
#if SIZEOF_WCHAR_T == 2
			dest[i++] = 0xD800 | ((c - 0x10000) >> 10);
			dest[i++] = 0xDE00 | (c & 0x3FF);
#else
			dest[i++] = c;
#endif
			j += 4;
		}
	}
	dest[i] = 0;
	return dest;
}
#else
char *
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


stream *
open_stream(const char *restrict filename, const char *restrict flags)
{
	stream *s;
	FILE *fp;
	fpos_t pos;
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
		mnstr_set_open_error(filename, errno, "open failed");
		destroy_stream(s);
		return NULL;
	}
	s->readonly = flags[0] == 'r';
	s->binary = flags[1] == 'b';
	s->read = file_read;
	s->write = file_write;
	s->close = file_close;
	s->destroy = file_destroy;
	s->clrerr = file_clrerr;
	s->flush = file_flush;
	s->fsync = file_fsync;
	s->fgetpos = file_fgetpos;
	s->fsetpos = file_fsetpos;
	s->stream_data.p = (void *) fp;
	/* if a text file is opened for reading, and it starts with
	 * the UTF-8 encoding of the Unicode Byte Order Mark, skip the
	 * mark, and mark the stream as being a UTF-8 stream */
	if (flags[0] == 'r' && flags[1] != 'b' && fgetpos(fp, &pos) == 0) {
		if (file_read(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0)
			s->isutf8 = true;
		else if (fsetpos(fp, &pos) != 0) {
			/* unlikely: we couldn't seek the file back */
			fclose(fp);
			destroy_stream(s);
			return NULL;
		}
	}
	return s;
}

static stream *
file_stream(const char *name)
{
	stream *s;

	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->read = file_read;
	s->write = file_write;
	s->close = file_close;
	s->destroy = file_destroy;
	s->flush = file_flush;
	s->fsync = file_fsync;
	s->fgetpos = file_fgetpos;
	s->fsetpos = file_fsetpos;
	return s;
}

stream *
file_rstream(FILE *restrict fp, const char *restrict name)
{
	stream *s;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_rstream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->binary = true;
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
file_wstream(FILE *restrict fp, const char *restrict name)
{
	stream *s;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_wstream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = true;
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
file_rastream(FILE *restrict fp, const char *restrict name)
{
	stream *s;
	fpos_t pos;
	char buf[UTF8BOMLENGTH + 1];
	struct stat stb;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_rastream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->binary = false;
	s->stream_data.p = (void *) fp;
	if (fstat(fileno(fp), &stb) == 0 &&
	    S_ISREG(stb.st_mode) &&
	    fgetpos(fp, &pos) == 0) {
		if (file_read(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
			s->isutf8 = true;
			return s;
		}
		if (fsetpos(fp, &pos) != 0) {
			/* unlikely: we couldn't seek the file back */
			destroy_stream(s);
			return NULL;
		}
	}
#ifdef _MSC_VER
	if (fp == stdin) {
		HANDLE h = GetStdHandle(STD_INPUT_HANDLE);

		switch (GetFileType(h)) {
		case FILE_TYPE_PIPE:
			s->stream_data.p = h;
			s->read = pipe_read;
			s->write = NULL;
			s->destroy = destroy_stream;
			s->close = NULL;
			s->flush = NULL;
			s->fsync = NULL;
			s->fgetpos = NULL;
			s->fsetpos = NULL;
			break;
		case FILE_TYPE_CHAR: {
			struct console *c = malloc(sizeof(struct console));
			if (c == NULL) {
				destroy_stream(s);
				return NULL;
			}
			s->stream_data.p = c;
			*c = (struct console) {
				.h = h,
			};
			s->read = console_read;
			s->write = NULL;
			s->destroy = console_destroy;
			s->close = NULL;
			s->flush = NULL;
			s->fsync = NULL;
			s->fgetpos = NULL;
			s->fsetpos = NULL;
			s->isutf8 = true;
			break;
		}
		}
	}
#endif
	return s;
}

stream *
file_wastream(FILE *restrict fp, const char *restrict name)
{
	stream *s;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_wastream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = false;
#ifdef _MSC_VER
	if ((fileno(fp) == 1 || fileno(fp) == 2) && isatty(fileno(fp))) {
		struct console *c = malloc(sizeof(struct console));
		if (c == NULL) {
			destroy_stream(s);
			return NULL;
		}
		s->stream_data.p = c;
		*c = (struct console) {
			.h = GetStdHandle(STD_OUTPUT_HANDLE),
		};
		s->read = NULL;
		s->write = console_write;
		s->destroy = console_destroy;
		s->close = NULL;
		s->flush = NULL;
		s->fsync = NULL;
		s->fgetpos = NULL;
		s->fsetpos = NULL;
		s->isutf8 = true;
		return s;
	}
#endif
	s->stream_data.p = (void *) fp;
	return s;
}

/* some lower-level access functions */
FILE *
getFile(stream *s)
{
	for (; s != NULL; s = s->inner) {
#ifdef _MSC_VER
		if (s->read == console_read)
			return stdin;
		if (s->write == console_write)
			return stdout;
#endif
		if (s->read == file_read)
			return (FILE *) s->stream_data.p;
	}

	return NULL;
}

int
getFileNo(stream *s)
{
	FILE *f;

	f = getFile(s);
	if (f == NULL)
		return -1;
	return fileno(f);
}

size_t
getFileSize(stream *s)
{
	struct stat stb;
	int fd = getFileNo(s);

	if (fd >= 0 && fstat(fd, &stb) == 0)
		return (size_t) stb.st_size;
	return 0;		/* unknown */
}

