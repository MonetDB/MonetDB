/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* streams working on a disk file */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


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


void
mnstr_set_error_lasterror(stream *s, mnstr_error_kind kind, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	mnstr_va_set_error(s, kind, fmt, ap);
	va_end(ap);

	/* append as much as fits of the system error message */
	char *start = &s->errmsg[0] + strlen(s->errmsg);
	char *end = &s->errmsg[0] + sizeof(s->errmsg);
	if (end - start >= 3) {
		DWORD last_error = GetLastError();
		start += snprintf(start, end - start, ": [%ld] ", (long)last_error);
		FormatMessage(
			/* flags */ FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
			/* lpSource, we don't provide our own format string */ NULL,
			/* message id */ last_error,
			/* language */MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
			/* result buffer */ start,
			/* buffer size */ (DWORD)(end - start - 1),
			/* no arguments */ NULL
		);
	}
}


ssize_t
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

ssize_t
console_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	struct console *c = s->stream_data.p;
	size_t n = elmsize * cnt;
	const unsigned char *p = buf;
	uint32_t ch;
	int x;

	if (c == NULL) {
		mnstr_set_error(s, MNSTR_WRITE_ERROR, "closed");
		return -1;
	}
	if (n == 0)
		return 0;

	c->len = 0;
	if (c->i > 0) {
		while (c->i > 0 && n > 0) {
			if ((*p & 0xC0) != 0x80) {
				mnstr_set_error(s, MNSTR_WRITE_ERROR, "encoding error %d", __LINE__);
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
			mnstr_set_error(s, MNSTR_WRITE_ERROR, "encoding error %d", __LINE__);
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
				mnstr_set_error_lasterror(s, MNSTR_WRITE_ERROR, NULL);
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
			mnstr_set_error(s, MNSTR_WRITE_ERROR, "encoding error %d", __LINE__);
			return -1;
		}
		p++;
		n--;
		while (x > 0 && n > 0) {
			if ((*p & 0xC0) != 0x80) {
				mnstr_set_error(s, MNSTR_WRITE_ERROR, "encoding error %d", __LINE__);
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
			mnstr_set_error(s, MNSTR_WRITE_ERROR, "encoding error %d", __LINE__);
			return -1;
		} else if (ch > 0xFFFF) {
			c->wbuf[c->len++] = 0xD800 | ((ch >> 10) - (1 << 6));
			c->wbuf[c->len++] = 0xDC00 | (ch & 0x03FF);
		} else {
			c->wbuf[c->len++] = ch;
		}
	}
	if (c->len > 0) {
		if (WriteConsoleW(c->h, c->wbuf, c->len, &c->rd, NULL) == 0) {
			// char errbuf[4096];
			DWORD last_error = GetLastError();
			mnstr_set_error_lasterror(s, MNSTR_WRITE_ERROR, NULL);
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


stream *
win_console_in_stream(const char *name)
{
	HANDLE h = GetStdHandle(STD_INPUT_HANDLE);
	struct console *c = NULL;
	stream *s;

	switch (GetFileType(h)) {
	case FILE_TYPE_PIPE:
		s = create_stream(name);
		if (s == NULL) {
			mnstr_set_error_errno(NULL, MNSTR_OPEN_ERROR, NULL);
			return NULL;
		}
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
	case FILE_TYPE_CHAR:
		s = create_stream(name);
		if (s == NULL) {
			mnstr_set_error_errno(NULL, MNSTR_OPEN_ERROR, NULL);
			return NULL;
		}
		c = malloc(sizeof(struct console));
		if (c == NULL) {
			destroy_stream(s);
			mnstr_set_error_errno(NULL, MNSTR_OPEN_ERROR, NULL);
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
	default:
		s = file_rstream(stdin, false, name);
		break;
	}
	s->readonly = true;
	return s;
}

stream *
win_console_out_stream(const char *name)
{
	stream *s = create_stream(name);
	if (s == NULL) {
		mnstr_set_error_errno(NULL, MNSTR_OPEN_ERROR, NULL);
		return NULL;
	}
	s->readonly = false;

	struct console *c = malloc(sizeof(struct console));
	if (c == NULL) {
		mnstr_set_error_errno(NULL, MNSTR_OPEN_ERROR, NULL);
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
