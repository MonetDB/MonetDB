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


/* ------------------------------------------------------------------ */
/* streams working on a disk file */


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
file_flush(stream *s, mnstr_flush_level flush_level)
{
	FILE *fp = (FILE *) s->stream_data.p;

	if (fp == NULL || (!s->readonly && fflush(fp) < 0)) {
			mnstr_set_error_errno(s, MNSTR_WRITE_ERROR, "flush error");
		return -1;
	}
	(void) flush_level;
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

/* convert a string from UTF-8 to wide characters; the return value is
 * freshly allocated */
#ifdef NATIVE_WIN32
static wchar_t *
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


stream *
open_stream(const char *restrict filename, const char *restrict flags)
{
	stream *s;
	FILE *fp;
	fpos_t pos;
	char buf[UTF8BOMLENGTH + 1];

	if ((s = create_stream(filename)) == NULL)
		return NULL;
#ifdef NATIVE_WIN32
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

stream *
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
file_rstream(FILE *restrict fp, bool binary, const char *restrict name)
{
	stream *s;

	if (fp == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "file_rstream %s\n", name);
#endif
	if ((s = file_stream(name)) == NULL)
		return NULL;
	s->binary = binary;
	s->stream_data.p = (void *) fp;
	return s;
}

stream *
file_wstream(FILE *restrict fp, bool binary, const char *restrict name)
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
	s->binary = binary;
	s->stream_data.p = (void *) fp;
	return s;
}


stream *
stdin_rastream(void)
{
	const char *name = "<stdin>";
	// Make an attempt to skip a BOM marker.
	// It would be nice to integrate this with with the BOM removal code
	// in text_stream.c but that is complicated. In text_stream,
	struct stat stb;
	if (fstat(fileno(stdin), &stb) == 0 && (stb.st_mode & S_IFMT) == S_IFREG) {
		fpos_t pos;
		if (fgetpos(stdin, &pos) == 0) {
			char bytes[UTF8BOMLENGTH];
			size_t nread = fread(bytes, 1, UTF8BOMLENGTH, stdin);
			if (nread != 3 || memcmp(bytes, UTF8BOM, UTF8BOMLENGTH) != 0) {
				// not a BOM, rewind
				if (nread > 0 && fsetpos(stdin, &pos) != 0) {
					// oops, bytes have been read but we can't rewind
					mnstr_set_error_errno(NULL, MNSTR_OPEN_ERROR, "while rewinding after checking for byte order mark");
					return NULL;
				}
			}
		}
	}

#ifdef _MSC_VER
	return win_console_in_stream(name);
#else
	return file_rstream(stdin, false, name);
#endif
}

stream *
stdout_wastream(void)
{
	const char *name = "<stdout>";
#ifdef _MSC_VER
	if (isatty(fileno(stdout)))
		return win_console_out_stream(name);
#endif
	return file_wstream(stdout, false, name);
}

stream *
stderr_wastream(void)
{
	const char *name = "<stderr>";
#ifdef _MSC_VER
	if (isatty(fileno(stderr)))
		return win_console_out_stream(name);
#endif
	return file_wstream(stderr, false, name);
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

int
file_remove(const char *filename)
{
	int rc = -1;

#ifdef NATIVE_WIN32
	wchar_t *wfname = utf8towchar(filename);
	if (wfname != NULL) {
		rc = _wremove(wfname);
		free(wfname);
	}
#else
	char *fname = cvfilename(filename);
	if (fname) {
		rc = remove(fname);
		free(fname);
	}
#endif
	return rc;
}
