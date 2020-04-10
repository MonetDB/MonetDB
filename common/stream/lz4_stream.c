/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* streams working on a lz4-compressed disk file */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


#ifdef HAVE_LIBLZ4
#define LZ4DECOMPBUFSIZ 128*1024
typedef struct lz4_stream {
	FILE *fp;
	size_t total_processing;
	size_t ring_buffer_size;
	void* ring_buffer;
	union {
		LZ4F_compressionContext_t comp_context;
		LZ4F_decompressionContext_t dec_context;
	} context;
} lz4_stream;

static ssize_t
stream_lz4read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	lz4_stream *lz4 = s->stream_data.p;
	size_t size = elmsize * cnt, total_read = 0, total_decompressed, ret, remaining_to_decompress;

	if (lz4 == NULL || size <= 0) {
		s->errnr = MNSTR_READ_ERROR;
		return -1;
	}

	while (total_read < size) {
		if (lz4->total_processing == lz4->ring_buffer_size) {
			if(feof(lz4->fp)) {
				break;
			} else {
				lz4->ring_buffer_size = fread(lz4->ring_buffer, 1, LZ4_COMPRESSBOUND(LZ4DECOMPBUFSIZ), lz4->fp);
				if (lz4->ring_buffer_size == 0 || ferror(lz4->fp)) {
					s->errnr = MNSTR_READ_ERROR;
					return -1;
				}
				lz4->total_processing = 0;
			}
		}

		remaining_to_decompress = size - total_read;
		total_decompressed = lz4->ring_buffer_size - lz4->total_processing;
		ret = LZ4F_decompress(lz4->context.dec_context, (char*)buf + total_read, &remaining_to_decompress,
				      (char*)lz4->ring_buffer + lz4->total_processing, &total_decompressed, NULL);
		if(LZ4F_isError(ret)) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}

		lz4->total_processing += total_decompressed;
		total_read += remaining_to_decompress;
	}

	/* when in text mode, convert \r\n line endings to \n */
	if (!s->binary) {
		char *p1, *p2, *pe;

		p1 = buf;
		pe = p1 + total_read;
		while (p1 < pe && *p1 != '\r')
			p1++;
		p2 = p1;
		while (p1 < pe) {
			if (*p1 == '\r' && p1[1] == '\n')
				total_read--;
			else
				*p2++ = *p1;
			p1++;
		}
	}
	return (ssize_t) (total_read / elmsize);
}

static ssize_t
stream_lz4write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	lz4_stream *lz4 = s->stream_data.p;
	size_t ret, size = elmsize * cnt, total_written = 0, next_batch, next_attempt, available, real_written;

	if (lz4 == NULL || size > LZ4_MAX_INPUT_SIZE || size <= 0) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}

	while (total_written < size) {
		next_batch = size - total_written;
		available = lz4->ring_buffer_size - lz4->total_processing;
		do {
			next_attempt = LZ4F_compressBound(next_batch, NULL); /* lz4->ring_buffer must be at least 65548 bytes */
			if(next_attempt > available) {
				next_batch >>= 1;
			} else {
				break;
			}
			if(next_batch == 0)
				break;
		} while(1);
		assert(next_batch > 0);

		ret = LZ4F_compressUpdate(lz4->context.comp_context, ((char*)lz4->ring_buffer) + lz4->total_processing,
					  available, ((char*)buf) + total_written, next_batch, NULL);
		if(LZ4F_isError(ret)) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		} else {
			lz4->total_processing += ret;
		}

		if(lz4->total_processing == lz4->ring_buffer_size) {
			real_written = fwrite((void *)lz4->ring_buffer, 1, lz4->total_processing, lz4->fp);
			if (real_written == 0) {
				s->errnr = MNSTR_WRITE_ERROR;
				return -1;
			}
			lz4->total_processing = 0;
		}
		total_written += next_batch;
	}

	return (ssize_t) (total_written / elmsize);
}

static void
stream_lz4close(stream *s)
{
	lz4_stream *lz4 = s->stream_data.p;

	if (lz4) {
		if (!s->readonly) {
			size_t ret, real_written;

			if (lz4->total_processing > 0 && lz4->total_processing < lz4->ring_buffer_size) { /* compress remaining */
				real_written = fwrite(lz4->ring_buffer, 1, lz4->total_processing, lz4->fp);
				if (real_written == 0) {
					s->errnr = MNSTR_WRITE_ERROR;
					return ;
				}
				lz4->total_processing = 0;
			} /* finish compression */
			ret = LZ4F_compressEnd(lz4->context.comp_context, lz4->ring_buffer, lz4->ring_buffer_size, NULL);
			if(LZ4F_isError(ret)) {
				s->errnr = MNSTR_WRITE_ERROR;
				return ;
			}
			assert(ret < LZ4DECOMPBUFSIZ);
			lz4->total_processing = ret;

			real_written = fwrite(lz4->ring_buffer, 1, lz4->total_processing, lz4->fp);
			if (real_written == 0) {
				s->errnr = MNSTR_WRITE_ERROR;
				return ;
			}
			lz4->total_processing = 0;

			fflush(lz4->fp);
		}
		if(!s->readonly) {
			(void) LZ4F_freeCompressionContext(lz4->context.comp_context);
		} else {
			(void) LZ4F_freeDecompressionContext(lz4->context.dec_context);
		}
		fclose(lz4->fp);
		free(lz4->ring_buffer);
		free(lz4);
	}
	s->stream_data.p = NULL;
}

static int
stream_lz4flush(stream *s)
{
	lz4_stream *lz4 = s->stream_data.p;
	size_t real_written, ret;

	if (lz4 == NULL)
		return -1;
	if (!s->readonly) {
		if (lz4->total_processing > 0 && lz4->total_processing < lz4->ring_buffer_size) { /* compress remaining */
			real_written = fwrite(lz4->ring_buffer, 1, lz4->total_processing, lz4->fp);
			if (real_written == 0) {
				s->errnr = MNSTR_WRITE_ERROR;
				return -1;
			}
			lz4->total_processing = 0;
		}
		ret = LZ4F_flush(lz4->context.comp_context, lz4->ring_buffer, lz4->ring_buffer_size, NULL); /* flush it */
		if(LZ4F_isError(ret)) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		lz4->total_processing = ret;
		real_written = fwrite(lz4->ring_buffer, 1, lz4->total_processing, lz4->fp);
		if (real_written == 0) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		lz4->total_processing = 0;

		if(fflush(lz4->fp))
			return -1;
	}
	return 0;
}

static stream *
open_lz4stream(const char *restrict filename, const char *restrict flags)
{
	stream *s;
	lz4_stream *lz4;
	LZ4F_errorCode_t error_code;
	char fl[3];
	size_t buffer_size = (flags[0] == 'r') ? LZ4_COMPRESSBOUND(LZ4DECOMPBUFSIZ) : LZ4DECOMPBUFSIZ;

	if ((lz4 = malloc(sizeof(struct lz4_stream))) == NULL)
		return NULL;
	*lz4 = (struct lz4_stream) {
		.ring_buffer = malloc(buffer_size),
		.total_processing = (flags[0] == 'r') ? buffer_size : 0,
		.ring_buffer_size = buffer_size,
	};
	if (lz4->ring_buffer == NULL) {
		free(lz4);
		return NULL;
	}

	if(flags[0] == 'w') {
		error_code = LZ4F_createCompressionContext(&(lz4->context.comp_context), LZ4F_VERSION);
	} else {
		error_code = LZ4F_createDecompressionContext(&(lz4->context.dec_context), LZ4F_VERSION);
	}
	if(LZ4F_isError(error_code)) {
		free(lz4->ring_buffer);
		free(lz4);
		return NULL;
	}

	if ((s = create_stream(filename)) == NULL) {
		if(flags[0] == 'w') {
			(void) LZ4F_freeCompressionContext(lz4->context.comp_context);
		} else {
			(void) LZ4F_freeDecompressionContext(lz4->context.dec_context);
		}
		free(lz4->ring_buffer);
		free(lz4);
		return NULL;
	}
	fl[0] = flags[0];	/* 'r' or 'w' */
	fl[1] = 'b';		/* always binary */
	fl[2] = '\0';
#ifdef HAVE__WFOPEN
	{
		wchar_t *wfname = utf8towchar(filename);
		wchar_t *wflags = utf8towchar(fl);
		if (wfname != NULL)
			lz4->fp = _wfopen(wfname, wflags);
		else
			lz4->fp = NULL;
		if (wfname)
			free(wfname);
		if (wflags)
			free(wflags);
	}
#else
	{
		char *fname = cvfilename(filename);
		if (fname) {
			lz4->fp = fopen(fname, fl);
			free(fname);
		} else
			lz4->fp = NULL;
	}
#endif
	if (lz4->fp == NULL) {
		destroy_stream(s);
		if(flags[0] == 'w') {
			(void) LZ4F_freeCompressionContext(lz4->context.comp_context);
		} else {
			(void) LZ4F_freeDecompressionContext(lz4->context.dec_context);
		}
		free(lz4->ring_buffer);
		free(lz4);
		return NULL;
	}
	s->read = stream_lz4read;
	s->write = stream_lz4write;
	s->close = stream_lz4close;
	s->flush = stream_lz4flush;
	s->stream_data.p = (void *) lz4;

	if(flags[0] == 'w') { /* start compression by writting the headers */
		size_t nwritten = LZ4F_compressBegin(lz4->context.comp_context, lz4->ring_buffer, lz4->ring_buffer_size, NULL);
		assert(nwritten < LZ4DECOMPBUFSIZ);
		if(LZ4F_isError(nwritten)) {
			(void) LZ4F_freeCompressionContext(lz4->context.comp_context);
			free(lz4->ring_buffer);
			free(lz4);
			return NULL;
		} else {
			lz4->total_processing += nwritten;
		}
	} else if (flags[0] == 'r' && flags[1] != 'b') { /* check for utf-8 encoding */
		char buf[UTF8BOMLENGTH];
		if (stream_lz4read(s, buf, 1, UTF8BOMLENGTH) == UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0) {
			s->isutf8 = true;
		} else {
			rewind(lz4->fp);
			lz4->total_processing = buffer_size;
			lz4->ring_buffer_size = buffer_size;
			LZ4F_resetDecompressionContext(lz4->context.dec_context);
			mnstr_clearerr(s);
		}
	}
	return s;
}

stream *
open_lz4rstream(const char *filename)
{
	stream *s;

	if ((s = open_lz4stream(filename, "rb")) == NULL)
		return NULL;
	s->binary = true;
	return s;
}

stream *
open_lz4wstream(const char *restrict filename, const char *restrict mode)
{
	stream *s;

	if ((s = open_lz4stream(filename, mode)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = true;
	return s;
}

stream *
open_lz4rastream(const char *filename)
{
	stream *s;

	if ((s = open_lz4stream(filename, "r")) == NULL)
		return NULL;
	s->binary = false;
	return s;
}

stream *
open_lz4wastream(const char *restrict filename, const char *restrict mode)
{
	stream *s;

	if ((s = open_lz4stream(filename, mode)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = false;
	return s;
}
#else

stream *open_lz4rstream(const char *filename)
{
	return NULL;
}

stream *open_lz4wstream(const char *restrict filename, const char *restrict mode)
{
	return NULL;
}

stream *open_lz4rastream(const char *filename)
{
	return NULL;
}

stream *open_lz4wastream(const char *restrict filename, const char *restrict mode)
{
	return NULL;
}



#endif

