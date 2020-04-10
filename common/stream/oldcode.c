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

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


/* ------------------------------------------------------------------ */

void
buffer_init(buffer *restrict b, char *restrict buf, size_t size)
{
	if (b == NULL || buf == NULL)
		return;
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
	*b = (buffer) {
		.buf = malloc(size),
		.len = size,
	};
	if (b->buf == NULL) {
		free(b);
		return NULL;
	}
	return b;
}

char *
buffer_get_buf(buffer *b)
{
	char *r;

	if (b == NULL)
		return NULL;
	if (b->pos == b->len) {
		if ((r = realloc(b->buf, b->len + 1)) == NULL) {
			/* keep b->buf in tact */
			return NULL;
		}
		b->buf = r;
	}
	r = b->buf;
	r[b->pos] = '\0';
	b->buf = malloc(b->len);
	if (b->buf == NULL) {
		free(b);
		free(r);
		return NULL;
	}
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
	if (s == NULL)
		return NULL;
	return (buffer *) s->stream_data.p;
}

static ssize_t
buffer_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (size && b && b->pos + size <= b->len) {
		memcpy(buf, b->buf + b->pos, size);
		b->pos += size;
		return (ssize_t) (size / elmsize);
	}
	return 0;
}

static ssize_t
buffer_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	size_t size = elmsize * cnt;
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (b == NULL) {
		s->errnr = MNSTR_WRITE_ERROR;
		return -1;
	}
	if (b->pos + size > b->len) {
		char *p;
		size_t ns = b->pos + size + 8192;

		if ((p = realloc(b->buf, ns)) == NULL) {
			s->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		b->buf = p;
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
	buffer *b;

	b = (buffer *) s->stream_data.p;
	assert(b);
	if (b == NULL)
		return -1;
	b->pos = 0;
	return 0;
}

stream *
buffer_rastream(buffer *restrict b, const char *restrict name)
{
	stream *s;

	if (b == NULL || name == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "buffer_rastream %s\n", name);
#endif
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->binary = false;
	s->read = buffer_read;
	s->write = buffer_write;
	s->close = buffer_close;
	s->flush = buffer_flush;
	s->stream_data.p = (void *) b;
	return s;
}

stream *
buffer_wastream(buffer *restrict b, const char *restrict name)
{
	stream *s;

	if (b == NULL || name == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "buffer_wastream %s\n", name);
#endif
	if ((s = create_stream(name)) == NULL)
		return NULL;
	s->readonly = false;
	s->binary = false;
	s->read = buffer_read;
	s->write = buffer_write;
	s->close = buffer_close;
	s->flush = buffer_flush;
	s->stream_data.p = (void *) b;
	return s;
}



/* ------------------------------------------------------------------ */

/* A buffered stream consists of a sequence of blocks.  Each block
 * consists of a count followed by the data in the block.  A flush is
 * indicated by an empty block (i.e. just a count of 0).
 */
typedef struct bs {
	stream *s;		/* underlying stream */
	unsigned nr;		/* how far we got in buf */
	unsigned itotal;	/* amount available in current read block */
	size_t blks;		/* read/writen blocks (possibly partial) */
	size_t bytes;		/* read/writen bytes */
	char buf[BLOCK];	/* the buffered data (minus the size of
				 * size-short */
} bs;

static bs *
bs_create(stream *s)
{
	/* should be a binary stream */
	bs *ns;

	if ((ns = malloc(sizeof(*ns))) == NULL)
		return NULL;
	*ns = (bs) {
		.s = s,
	};
	return ns;
}

/* Collect data until the internal buffer is filled, then write the
 * filled buffer to the underlying stream.
 * Struct field usage:
 * s - the underlying stream;
 * buf - the buffer in which data is collected;
 * nr - how much of buf is already filled (if nr == sizeof(buf) the
 *      data is written to the underlying stream, so upon entry nr <
 *      sizeof(buf));
 * itotal - unused.
 */
static ssize_t
bs_write(stream *restrict ss, const void *restrict buf, size_t elmsize, size_t cnt)
{
	bs *s;
	size_t todo = cnt * elmsize;
	uint16_t blksize;

	s = (bs *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(!ss->readonly);
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

				fprintf(stderr, "W %s %u \"", ss->name, s->nr);
				for (i = 0; i < s->nr; i++)
					if (' ' <= s->buf[i] && s->buf[i] < 127)
						putc(s->buf[i], stderr);
					else
						fprintf(stderr, "\\%03o", (unsigned char) s->buf[i]);
				fprintf(stderr, "\"\n");
			}
#endif
			/* since the block is at max BLOCK (8K) - 2 size we can
			 * store it in a two byte integer */
			blksize = (uint16_t) s->nr;
			s->bytes += s->nr;
			/* the last bit tells whether a flush is in
			 * there, it's not at this moment, so shift it
			 * to the left */
			blksize <<= 1;
			if (!mnstr_writeSht(s->s, (int16_t) blksize) ||
			    s->s->write(s->s, s->buf, 1, s->nr) != (ssize_t) s->nr) {
				ss->errnr = MNSTR_WRITE_ERROR;
				s->nr = 0; /* data is lost due to error */
				return -1;
			}
			s->blks++;
			s->nr = 0;
		}
	}
	return (ssize_t) cnt;
}

/* If the internal buffer is partially filled, write it to the
 * underlying stream.  Then in any case write an empty buffer to the
 * underlying stream to indicate to the receiver that the data was
 * flushed.
 */
static int
bs_flush(stream *ss)
{
	uint16_t blksize;
	bs *s;

	s = (bs *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(!ss->readonly);
	assert(s->nr < sizeof(s->buf));
	if (!ss->readonly) {
		/* flush the rest of buffer (if s->nr > 0), then set the
		 * last bit to 1 to to indicate user-instigated flush */
#ifdef BSTREAM_DEBUG
		if (s->nr > 0) {
			unsigned i;

			fprintf(stderr, "W %s %u \"", ss->name, s->nr);
			for (i = 0; i < s->nr; i++)
				if (' ' <= s->buf[i] && s->buf[i] < 127)
					putc(s->buf[i], stderr);
				else
					fprintf(stderr, "\\%03o", (unsigned char) s->buf[i]);
			fprintf(stderr, "\"\n");
			fprintf(stderr, "W %s 0\n", ss->name);
		}
#endif
		blksize = (uint16_t) (s->nr << 1);
		s->bytes += s->nr;
		/* indicate that this is the last buffer of a block by
		 * setting the low-order bit */
		blksize |= 1;
		/* allways flush (even empty blocks) needed for the protocol) */
		if ((!mnstr_writeSht(s->s, (int16_t) blksize) ||
		     (s->nr > 0 &&
		      s->s->write(s->s, s->buf, 1, s->nr) != (ssize_t) s->nr))) {
			ss->errnr = MNSTR_WRITE_ERROR;
			s->nr = 0; /* data is lost due to error */
			return -1;
		}
		s->blks++;
		s->nr = 0;
	}
	return 0;
}

/* Read buffered data and return the number of items read.  At the
 * flush boundary we will return 0 to indicate the end of a block.
 *
 * Structure field usage:
 * s - the underlying stream;
 * buf - not used;
 * itotal - the amount of data in the current block that hasn't yet
 *          been read;
 * nr - indicates whether the flush marker has to be returned.
 */
static ssize_t
bs_read(stream *restrict ss, void *restrict buf, size_t elmsize, size_t cnt)
{
	bs *s;
	size_t todo = cnt * elmsize;
	size_t n;

	s = (bs *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(ss->readonly);
	assert(s->nr <= 1);

	if (s->itotal == 0) {
		int16_t blksize = 0;

		if (s->nr) {
			/* We read the closing block but hadn't
			 * returned that yet. Return it now, and note
			 * that we did by setting s->nr to 0. */
			assert(s->nr == 1);
			s->nr = 0;
			return 0;
		}

		assert(s->nr == 0);

		/* There is nothing more to read in the current block,
		 * so read the count for the next block */
		switch (mnstr_readSht(s->s, &blksize)) {
		case -1:
			ss->errnr = s->s->errnr;
			return -1;
		case 0:
			return 0;
		case 1:
			break;
		}
		if ((uint16_t) blksize > (BLOCK << 1 | 1)) {
			ss->errnr = MNSTR_READ_ERROR;
			return -1;
		}
#ifdef BSTREAM_DEBUG
		fprintf(stderr, "RC size: %u, final: %s\n", (uint16_t) blksize >> 1, (uint16_t) blksize & 1 ? "true" : "false");
		fprintf(stderr, "RC %s %u\n", ss->name, (uint16_t) blksize);
#endif
		s->itotal = (uint16_t) blksize >> 1;	/* amount readable */
		/* store whether this was the last block or not */
		s->nr = (uint16_t) blksize & 1;
		s->bytes += s->itotal;
		s->blks++;
	}

	/* Fill the caller's buffer. */
	cnt = 0;		/* count how much we put into the buffer */
	while (todo > 0) {
		/* there is more data waiting in the current block, so
		 * read it */
		n = todo < s->itotal ? todo : s->itotal;
		while (n > 0) {
			ssize_t m = s->s->read(s->s, buf, 1, n);

			if (m <= 0) {
				ss->errnr = s->s->errnr;
				return -1;
			}
#ifdef BSTREAM_DEBUG
			{
				ssize_t i;

				fprintf(stderr, "RD %s %zd \"", ss->name, m);
				for (i = 0; i < m; i++)
					if (' ' <= ((char *) buf)[i] &&
					    ((char *) buf)[i] < 127)
						putc(((char *) buf)[i], stderr);
					else
						fprintf(stderr, "\\%03o", ((unsigned char *) buf)[i]);
				fprintf(stderr, "\"\n");
			}
#endif
			buf = (void *) ((char *) buf + m);
			cnt += (size_t) m;
			n -= (size_t) m;
			s->itotal -= (unsigned) m;
			todo -= (size_t) m;
		}

		if (s->itotal == 0) {
			int16_t blksize = 0;

			/* The current block has been completely read,
			 * so read the count for the next block, only
			 * if the previous was not the last one */
			if (s->nr)
				break;
			switch (mnstr_readSht(s->s, &blksize)) {
			case -1:
				ss->errnr = s->s->errnr;
				return -1;
			case 0:
				return 0;
			case 1:
				break;
			}
			if ((uint16_t) blksize > (BLOCK << 1 | 1)) {
				ss->errnr = MNSTR_READ_ERROR;
				return -1;
			}
#ifdef BSTREAM_DEBUG
			fprintf(stderr, "RC size: %d, final: %s\n", (uint16_t) blksize >> 1, (uint16_t) blksize & 1 ? "true" : "false");
			fprintf(stderr, "RC %s %d\n", ss->name, s->nr);
			fprintf(stderr, "RC %s %d\n", ss->name, blksize);
#endif
			s->itotal = (uint16_t) blksize >> 1;	/* amount readable */
			/* store whether this was the last block or not */
			s->nr = (uint16_t) blksize & 1;
			s->bytes += s->itotal;
			s->blks++;
		}
	}
	/* if we got an empty block with the end-of-sequence marker
	 * set (low-order bit) we must only return an empty read once,
	 * so we must squash the flag that we still have to return an
	 * empty read */
	if (todo > 0 && cnt == 0)
		s->nr = 0;
	return (ssize_t) (elmsize > 0 ? cnt / elmsize : 0);
}

static void
bs_update_timeout(stream *ss)
{
	bs *s;

	if ((s = ss->stream_data.p) != NULL && s->s) {
		s->s->timeout = ss->timeout;
		s->s->timeout_func = ss->timeout_func;
		if (s->s->update_timeout)
			s->s->update_timeout(s->s);
	}
}

static int
bs_isalive(const stream *ss)
{
	struct bs *s;

	if ((s = ss->stream_data.p) != NULL && s->s) {
		if (s->s->isalive)
			return s->s->isalive(s->s);
		return 1;
	}
	return 0;
}

static void
bs_close(stream *ss)
{
	bs *s;

	s = (bs *) ss->stream_data.p;
	assert(s);
	if (s == NULL)
		return;
	if (!ss->readonly && s->nr > 0)
		bs_flush(ss);
	if (s->s)
		s->s->close(s->s);
}

static void
bs_destroy(stream *ss)
{
	bs *s;

	s = (bs *) ss->stream_data.p;
	assert(s);
	if (s) {
		if (s->s)
			s->s->destroy(s->s);
		free(s);
	}
	destroy_stream(ss);
}

static void
bs_clrerr(stream *s)
{
	if (s->stream_data.p)
		mnstr_clearerr(((bs *) s->stream_data.p)->s);
}

stream *
bs_stream(stream *s)
{
	assert(isa_block_stream(s));
	return ((bs *) s->stream_data.p)->s;
}

stream *
block_stream(stream *s)
{
	stream *ns;
	bs *b;

	if (s == NULL)
		return NULL;
#ifdef STREAM_DEBUG
	fprintf(stderr, "block_stream %s\n", s->name ? s->name : "<unnamed>");
#endif
	if ((ns = create_stream(s->name)) == NULL)
		return NULL;
	if ((b = bs_create(s)) == NULL) {
		destroy_stream(ns);
		return NULL;
	}
	/* blocksizes have a fixed little endian byteorder */
#ifdef WORDS_BIGENDIAN
	s->swapbytes = true;
#endif
	ns->binary = s->binary;
	ns->readonly = s->readonly;
	ns->close = bs_close;
	ns->clrerr = bs_clrerr;
	ns->destroy = bs_destroy;
	ns->flush = bs_flush;
	ns->read = bs_read;
	ns->write = bs_write;
	ns->update_timeout = bs_update_timeout;
	ns->isalive = bs_isalive;
	ns->stream_data.p = (void *) b;

	return ns;
}

typedef struct bs2 {
	stream *s;		/* underlying stream */
	size_t nr;		/* how far we got in buf */
	size_t itotal;		/* amount available in current read block */
	size_t bufsiz;
	size_t readpos;
	compression_method comp;
	char *compbuf;
	size_t compbufsiz;
	char *buf;
} bs2;


static ssize_t
compress_stream_data(bs2 *s)
{
	assert(s->comp != COMPRESSION_NONE);
	if (s->comp == COMPRESSION_SNAPPY) {
#ifdef HAVE_LIBSNAPPY
		size_t compressed_length = s->compbufsiz;
		snappy_status ret;
		if ((ret = snappy_compress(s->buf, s->nr, s->compbuf, &compressed_length)) != SNAPPY_OK) {
			s->s->errnr = (int) ret;
			return -1;
		}
		return compressed_length;
#else
		assert(0);
		return -1;
#endif
	} else if (s->comp == COMPRESSION_LZ4) {
#ifdef HAVE_LIBLZ4
		int compressed_length = (int) s->compbufsiz;
		if ((compressed_length = LZ4_compress_fast(s->buf, s->compbuf, s->nr, compressed_length, 1)) == 0) {
			s->s->errnr = -1;
			return -1;
		}
		return compressed_length;
#else
		assert(0);
		return -1;
#endif
	}
	return -1;
}


static ssize_t
decompress_stream_data(bs2 *s)
{
	assert(s->comp != COMPRESSION_NONE);
	if (s->comp == COMPRESSION_SNAPPY) {
#ifdef HAVE_LIBSNAPPY
		snappy_status ret;
		size_t uncompressed_length = s->bufsiz;
		if ((ret = snappy_uncompress(s->compbuf, s->itotal, s->buf, &uncompressed_length)) != SNAPPY_OK) {
			s->s->errnr = (int) ret;
			return -1;
		}
		return (ssize_t) uncompressed_length;
#else
		assert(0);
		return -1;
#endif
	} else if (s->comp == COMPRESSION_LZ4) {
#ifdef HAVE_LIBLZ4
		int uncompressed_length = (int) s->bufsiz;
		if ((uncompressed_length = LZ4_decompress_safe(s->compbuf, s->buf, s->itotal, uncompressed_length)) <= 0) {
			s->s->errnr = uncompressed_length;
			return -1;
		}
		return uncompressed_length;
#else
		assert(0);
		return -1;
#endif
	}
	return -1;
}

static ssize_t
compression_size_bound(bs2 *s)
{
	if (s->comp == COMPRESSION_NONE) {
		return 0;
	} else if (s->comp == COMPRESSION_SNAPPY) {
#ifndef HAVE_LIBSNAPPY
		return -1;
#else
		return snappy_max_compressed_length(s->bufsiz);
#endif
	} else if (s->comp == COMPRESSION_LZ4) {
#ifndef HAVE_LIBLZ4
		return -1;
#else
		return LZ4_compressBound(s->bufsiz);
#endif
	}
	return -1;
}

static bs2 *
bs2_create(stream *s, size_t bufsiz, compression_method comp)
{
	/* should be a binary stream */
	bs2 *ns;
	ssize_t compress_bound = 0;

	if ((ns = malloc(sizeof(*ns))) == NULL)
		return NULL;
	*ns = (bs2) {
		.buf = malloc(bufsiz),
		.s = s,
		.bufsiz = bufsiz,
		.comp = comp,
	};
	if (ns->buf == NULL) {
		free(ns);
		return NULL;
	}

	compress_bound = compression_size_bound(ns);
	if (compress_bound > 0) {
		ns->compbufsiz = (size_t) compress_bound;
		ns->compbuf = malloc(ns->compbufsiz);
		if (!ns->compbuf) {
			free(ns->buf);
			free(ns);
			return NULL;
		}
	} else if (compress_bound < 0) {
		free(ns->buf);
		free(ns);
		return NULL;
	}
	return ns;
}

/* Collect data until the internal buffer is filled, then write the
 * filled buffer to the underlying stream.
 * Struct field usage:
 * s - the underlying stream;
 * buf - the buffer in which data is collected;
 * nr - how much of buf is already filled (if nr == sizeof(buf) the
 *      data is written to the underlying stream, so upon entry nr <
 *      sizeof(buf));
 * itotal - unused.
 */
static ssize_t
bs2_write(stream *restrict ss, const void *restrict buf, size_t elmsize, size_t cnt)
{
	bs2 *s;
	size_t todo = cnt * elmsize;
	int64_t blksize;
	char *writebuf;
	size_t writelen;

	s = (bs2 *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(!ss->readonly);
	assert(s->nr < s->bufsiz);
	while (todo > 0) {
		size_t n = s->bufsiz - s->nr;

		if (todo < n)
			n = todo;
		memcpy(s->buf + s->nr, buf, n);
		s->nr += n;
		todo -= n;
		buf = ((const char *) buf + n);
		/* block is full, write it to the stream */
		if (s->nr == s->bufsiz) {

#ifdef BSTREAM_DEBUG
			{
				size_t i;

				fprintf(stderr, "W %s %zu \"", ss->name, s->nr);
				for (i = 0; i < s->nr; i++)
					if (' ' <= s->buf[i] && s->buf[i] < 127)
						putc(s->buf[i], stderr);
					else
						fprintf(stderr, "\\%03o", (unsigned char) s->buf[i]);
				fprintf(stderr, "\"\n");
			}
#endif

			writelen = s->nr;
			blksize = (int64_t) s->nr;
			writebuf = s->buf;

			if (s->comp != COMPRESSION_NONE) {
				ssize_t compressed_length = compress_stream_data(s);
				if (compressed_length < 0) {
					return -1;
				}
				writebuf = s->compbuf;
				blksize = (int64_t) compressed_length;
				writelen = (size_t) compressed_length;
			}


			/* the last bit tells whether a flush is in there, it's not
			 * at this moment, so shift it to the left */
			blksize <<= 1;
			if (!mnstr_writeLng(s->s, blksize) ||
			    s->s->write(s->s, writebuf, 1, writelen) != (ssize_t) writelen) {
				ss->errnr = MNSTR_WRITE_ERROR;
				return -1;
			}
			s->nr = 0;
		}
	}
	return (ssize_t) cnt;
}

/* If the internal buffer is partially filled, write it to the
 * underlying stream.  Then in any case write an empty buffer to the
 * underlying stream to indicate to the receiver that the data was
 * flushed.
 */
static int
bs2_flush(stream *ss)
{
	int64_t blksize;
	bs2 *s;
	char *writebuf;
	size_t writelen;

	s = (bs2 *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(!ss->readonly);
	assert(s->nr < s->bufsiz);
	if (!ss->readonly) {
		/* flush the rest of buffer (if s->nr > 0), then set the
		 * last bit to 1 to to indicate user-instigated flush */
#ifdef BSTREAM_DEBUG
		if (s->nr > 0) {
			size_t i;

			fprintf(stderr, "W %s %zu \"", ss->name, s->nr);
			for (i = 0; i < s->nr; i++)
				if (' ' <= s->buf[i] && s->buf[i] < 127)
					putc(s->buf[i], stderr);
				else
					fprintf(stderr, "\\%03o", (unsigned char) s->buf[i]);
			fprintf(stderr, "\"\n");
			fprintf(stderr, "W %s 0\n", ss->name);
		}
#endif

		writelen = s->nr;
		blksize = (int64_t) s->nr;
		writebuf = s->buf;

		if (s->nr > 0 && s->comp != COMPRESSION_NONE) {
			ssize_t compressed_length = compress_stream_data(s);
			if (compressed_length < 0) {
				return -1;
			}
			writebuf = s->compbuf;
			blksize = (int64_t) compressed_length;
			writelen = (size_t) compressed_length;
		}

		/* indicate that this is the last buffer of a block by
		 * setting the low-order bit */
		blksize <<= 1;
		blksize |= 1;
		/* always flush (even empty blocks) needed for the protocol) */

		if ((!mnstr_writeLng(s->s, blksize) ||
		     (s->nr > 0 &&
		      s->s->write(s->s, writebuf, 1, writelen) != (ssize_t) writelen))) {
			ss->errnr = MNSTR_WRITE_ERROR;
			return -1;
		}
		s->nr = 0;
	}
	return 0;
}

/* Read buffered data and return the number of items read.  At the
 * flush boundary we will return 0 to indicate the end of a block.
 *
 * Structure field usage:
 * s - the underlying stream;
 * buf - not used;
 * itotal - the amount of data in the current block that hasn't yet
 *          been read;
 * nr - indicates whether the flush marker has to be returned.
 */
static ssize_t
bs2_read(stream *restrict ss, void *restrict buf, size_t elmsize, size_t cnt)
{
	bs2 *s;
	size_t todo = cnt * elmsize;
	size_t n;

	s = (bs2 *) ss->stream_data.p;
	if (s == NULL)
		return -1;
	assert(ss->readonly);
	assert(s->nr <= 1);

	if (s->itotal == 0) {
		int64_t blksize = 0;

		if (s->nr) {
			/* We read the closing block but hadn't
			 * returned that yet. Return it now, and note
			 * that we did by setting s->nr to 0. */
			assert(s->nr == 1);
			s->nr = 0;
			return 0;
		}

		assert(s->nr == 0);

		/* There is nothing more to read in the current block,
		 * so read the count for the next block */
		switch (mnstr_readLng(s->s, &blksize)) {
		case -1:
			ss->errnr = s->s->errnr;
			return -1;
		case 0:
			return 0;
		case 1:
			break;
		}
		if (blksize < 0) {
			ss->errnr = MNSTR_READ_ERROR;
			return -1;
		}
#ifdef BSTREAM_DEBUG
		fprintf(stderr, "R1 '%s' length: %lld, final: %s\n", ss->name, blksize >> 1, blksize & 1 ? "true" : "false");
#endif
		s->itotal = (size_t) (blksize >> 1);	/* amount readable */
		/* store whether this was the last block or not */
		s->nr = blksize & 1;

		if (s->itotal > 0) {
			/* read everything into the comp buf */
			ssize_t uncompressed_length = (ssize_t) s->bufsiz;
			size_t m = 0;
			char *buf = s->buf;

			if (s->comp != COMPRESSION_NONE) {
				buf = s->compbuf;
			}

			while (m < s->itotal) {
				ssize_t bytes_read = 0;
				bytes_read = s->s->read(s->s, buf + m, 1, s->itotal - m);
				if (bytes_read <= 0) {
					ss->errnr = s->s->errnr;
					return -1;
				}
				m += (size_t) bytes_read;
			}
			if (s->comp != COMPRESSION_NONE) {
				uncompressed_length = decompress_stream_data(s);
				if (uncompressed_length < 0) {
					ss->errnr = (int) uncompressed_length;
					return -1;
				}
			} else {
				uncompressed_length = (ssize_t) m;
			}
			s->itotal = (size_t) uncompressed_length;
			s->readpos = 0;
		}
	}

	/* Fill the caller's buffer. */
	cnt = 0;		/* count how much we put into the buffer */
	while (todo > 0) {
		/* there is more data waiting in the current block, so
		 * read it */
		n = todo < s->itotal ? todo : s->itotal;

		memcpy(buf, s->buf + s->readpos, n);
		buf = (void *) ((char *) buf + n);
		cnt += n;
		todo -= n;
		s->readpos += n;
		s->itotal -= n;

		if (s->itotal == 0) {
			int64_t blksize = 0;

			/* The current block has been completely read,
			 * so read the count for the next block, only
			 * if the previous was not the last one */
			if (s->nr)
				break;
			switch (mnstr_readLng(s->s, &blksize)) {
			case -1:
				ss->errnr = s->s->errnr;
				return -1;
			case 0:
				return 0;
			case 1:
				break;
			}
			if (blksize < 0) {
				ss->errnr = MNSTR_READ_ERROR;
				return -1;
			}
#ifdef BSTREAM_DEBUG
			fprintf(stderr, "R3 '%s' length: %lld, final: %s\n", ss->name, blksize >> 1, blksize & 1 ? "true" : "false");
#endif


			s->itotal = (size_t) (blksize >> 1);	/* amount readable */
			/* store whether this was the last block or not */
			s->nr = blksize & 1;

			if (s->itotal > 0) {
				/* read everything into the comp buf */
				ssize_t uncompressed_length = (ssize_t) s->bufsiz;
				size_t m = 0;
				char *buf = s->buf;

				if (s->comp != COMPRESSION_NONE) {
					buf = s->compbuf;
				}

				while (m < s->itotal) {
					ssize_t bytes_read = 0;
					bytes_read = s->s->read(s->s, buf + m, 1, s->itotal - m);
					if (bytes_read <= 0) {
						ss->errnr = s->s->errnr;
						return -1;
					}
					m += (size_t) bytes_read;
				}
				if (s->comp != COMPRESSION_NONE) {
					uncompressed_length = decompress_stream_data(s);
					if (uncompressed_length < 0) {
						ss->errnr = (int) uncompressed_length;
						return -1;
					}
				} else {
					uncompressed_length = (ssize_t) m;
				}
				s->itotal = (size_t) uncompressed_length;
				s->readpos = 0;
			}
		}
	}
	/* if we got an empty block with the end-of-sequence marker
	 * set (low-order bit) we must only return an empty read once,
	 * so we must squash the flag that we still have to return an
	 * empty read */
	if (todo > 0 && cnt == 0)
		s->nr = 0;
	return (ssize_t) (elmsize > 0 ? cnt / elmsize : 0);
}



static void
bs2_resetbuf(stream *ss)
{
	bs2 *s = (bs2 *) ss->stream_data.p;
	assert(ss->read == bs2_read);
	s->itotal = 0;
	s->nr = 0;
	s->readpos = 0;
}

int
bs2_resizebuf(stream *ss, size_t bufsiz)
{
	ssize_t compress_bound;
	bs2 *s = (bs2 *) ss->stream_data.p;
	assert(ss->read == bs2_read);

	if (s->buf)
		free(s->buf);
	if (s->compbuf)
		free(s->compbuf);

	s->bufsiz = 0;
	s->buf = NULL;
	s->compbuf = NULL;

	if ((s->buf = malloc(bufsiz)) == NULL) {
		return -1;
	}
	s->bufsiz = bufsiz;
	compress_bound = compression_size_bound(s);
	if (compress_bound > 0) {
		s->compbufsiz = (size_t) compress_bound;
		s->compbuf = malloc(s->compbufsiz);
		if (!s->compbuf) {
			free(s->buf);
			s->buf = NULL;
			return -1;
		}
	}
	bs2_resetbuf(ss);
	return 0;
}

buffer
bs2_buffer(stream *ss)
{
	bs2 *s = (bs2 *) ss->stream_data.p;
	buffer b;
	assert(ss->read == bs2_read);
	b.buf = s->buf;
	b.pos = s->nr;
	b.len = s->itotal;
	return b;
}

void
bs2_setpos(stream *ss, size_t pos)
{
	bs2 *s = (bs2 *) ss->stream_data.p;
	assert(pos < s->bufsiz);
	s->nr = pos;
}

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

static void
bs2_close(stream *ss)
{
	bs2 *s;

	s = (bs2 *) ss->stream_data.p;
	assert(s);
	if (s == NULL)
		return;
	if (!ss->readonly && s->nr > 0)
		bs2_flush(ss);
	assert(s->s);
	if (s->s)
		s->s->close(s->s);
}

static void
bs2_destroy(stream *ss)
{
	bs2 *s;

	s = (bs2 *) ss->stream_data.p;
	assert(s);
	if (s) {
		assert(s->s);
		if (s->s)
			s->s->destroy(s->s);
		if (s->buf)
			free(s->buf);
		if (s->compbuf)
			free(s->compbuf);
		free(s);
	}
	destroy_stream(ss);
}

static void
bs2_update_timeout(stream *ss)
{
	bs2 *s;

	if ((s = ss->stream_data.p) != NULL && s->s) {
		s->s->timeout = ss->timeout;
		s->s->timeout_func = ss->timeout_func;
		if (s->s->update_timeout)
			s->s->update_timeout(s->s);
	}
}

static int
bs2_isalive(const stream *ss)
{
	struct bs2 *s;

	if ((s = ss->stream_data.p) != NULL && s->s) {
		if (s->s->isalive)
			return s->s->isalive(s->s);
		return 1;
	}
	return 0;
}

stream *
block_stream2(stream *s, size_t bufsiz, compression_method comp)
{
	stream *ns;
	stream *os = NULL;
	bs2 *b;

	if (s == NULL)
		return NULL;
	if (s->read == bs_read || s->write == bs_write) {
		/* if passed in a block_stream instance, extract the
		 * underlying stream */
		os = s;
		s = ((bs *) s->stream_data.p)->s;
	}

#ifdef STREAM_DEBUG
	fprintf(stderr, "block_stream2 %s\n", s->name ? s->name : "<unnamed>");
#endif
	if ((ns = create_stream(s->name)) == NULL)
		return NULL;
	if ((b = bs2_create(s, bufsiz, comp)) == NULL) {
		destroy_stream(ns);
		return NULL;
	}
	/* blocksizes have a fixed little endian byteorder */
#ifdef WORDS_BIGENDIAN
	s->swapbytes = true;
#endif
	ns->binary = s->binary;
	ns->readonly = s->readonly;
	ns->close = bs2_close;
	ns->clrerr = bs_clrerr;
	ns->destroy = bs2_destroy;
	ns->flush = bs2_flush;
	ns->read = bs2_read;
	ns->write = bs2_write;
	ns->update_timeout = bs2_update_timeout;
	ns->isalive = bs2_isalive;
	ns->stream_data.p = (void *) b;

	if (os != NULL) {
		/* we extracted the underlying stream, destroy the old
		 * shell */
		((bs *) os->stream_data.p)->s = NULL;
		bs_destroy(os);
	}

	return ns;
}


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


int
mnstr_readChr(stream *restrict s, char *restrict val)
{
	if (s == NULL || val == NULL)
		return -1;
	return (int) s->read(s, (void *) val, sizeof(*val), 1);
}

int
mnstr_writeChr(stream *s, char val)
{
	if (s == NULL || s->errnr)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), 1) == 1;
}

int
mnstr_readBte(stream *restrict s, int8_t *restrict val)
{
	if (s == NULL || val == NULL)
		return -1;
	return (int) s->read(s, (void *) val, sizeof(*val), 1);
}

int
mnstr_writeBte(stream *s, int8_t val)
{
	if (s == NULL || s->errnr)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), 1) == 1;
}

int
mnstr_readSht(stream *restrict s, int16_t *restrict val)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	switch (s->read(s, val, sizeof(*val), 1)) {
	case 1:
		if (s->swapbytes)
			*val = short_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeSht(stream *s, int16_t val)
{
	if (s == NULL || s->errnr)
		return 0;
	assert(s->binary);
	if (s->swapbytes)
		val = short_int_SWAP(val);
	return s->write(s, &val, sizeof(val), 1) == 1;
}

int
mnstr_readInt(stream *restrict s, int *restrict val)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	switch (s->read(s, val, sizeof(*val), 1)) {
	case 1:
		if (s->swapbytes)
			*val = normal_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeInt(stream *s, int val)
{
	if (s == NULL || s->errnr)
		return 0;
	assert(s->binary);
	if (s->swapbytes)
		val = normal_int_SWAP(val);
	return s->write(s, &val, sizeof(val), 1) == 1;
}

int
mnstr_writeStr(stream *restrict s, const char *restrict val)
{
	if (s == NULL || s->errnr)
		return 0;
	return s->write(s, (void *) val, strlen(val), (size_t) 1) == 1;
}

int
mnstr_readStr(stream *restrict s, char *restrict val)
{
	if (s == NULL || s->errnr)
		return 0;
	do {
		if (mnstr_readChr(s, val) != 1) {
			return -1;
		}
		val++;
	} while (*(val - 1) != '\0');
	return 1;
}


int
mnstr_readLng(stream *restrict s, int64_t *restrict val)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	switch (s->read(s, val, sizeof(*val), 1)) {
	case 1:
		if (s->swapbytes)
			*val = long_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeLng(stream *s, int64_t val)
{
	if (s == NULL || s->errnr)
		return 0;
	assert(s->binary);
	if (s->swapbytes)
		val = long_int_SWAP(val);
	return s->write(s, &val, sizeof(val), 1) == 1;
}

int
mnstr_writeFlt(stream *s, float val)
{
	if (s == NULL || s->errnr)
		return 0;
	assert(s->binary);
	return s->write(s, &val, sizeof(val), 1) == 1;
}

int
mnstr_writeDbl(stream *s, double val)
{
	if (s == NULL || s->errnr)
		return 0;
	assert(s->binary);
	return s->write(s, &val, sizeof(val), 1) == 1;
}


#ifdef HAVE_HGE
int
mnstr_readHge(stream *restrict s, hge *restrict val)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	switch (s->read(s, val, sizeof(*val), 1)) {
	case 1:
		if (s->swapbytes)
			*val = huge_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeHge(stream *s, hge val)
{
	if (s == NULL || s->errnr)
		return 0;
	assert(s->binary);
	if (s->swapbytes)
		val = huge_int_SWAP(val);
	return s->write(s, &val, sizeof(val), 1) == 1;
}
#endif

int
mnstr_readBteArray(stream *restrict s, int8_t *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;

	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}

	return 1;
}

int
mnstr_writeBteArray(stream *restrict s, const int8_t *restrict val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readShtArray(stream *restrict s, int16_t *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	if (s->read(s, val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++, val++)
			*val = short_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeShtArray(stream *restrict s, const int16_t *restrict val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	assert(s->binary);
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++)
			if (!mnstr_writeSht(s, val[i]))
				return 0;
		return 1;
	}
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readIntArray(stream *restrict s, int *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	if (s->read(s, val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++, val++)
			*val = normal_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeIntArray(stream *restrict s, const int *restrict val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	assert(s->binary);
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++)
			if (!mnstr_writeInt(s, val[i]))
				return 0;
		return 1;
	}
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readLngArray(stream *restrict s, int64_t *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	if (s->read(s, val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++, val++)
			*val = long_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeLngArray(stream *restrict s, const int64_t *restrict val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	assert(s->binary);
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++)
			if (!mnstr_writeLng(s, val[i]))
				return 0;
		return 1;
	}
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

#ifdef HAVE_HGE
int
mnstr_readHgeArray(stream *restrict s, hge *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	if (s->read(s, val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errnr == MNSTR_NO__ERROR)
			s->errnr = MNSTR_READ_ERROR;
		return 0;
	}
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++, val++)
			*val = huge_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeHgeArray(stream *restrict s, const hge *restrict val, size_t cnt)
{
	if (s == NULL || s->errnr || val == NULL)
		return 0;
	assert(s->binary);
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++)
			if (!mnstr_writeHge(s, val[i]))
				return 0;
		return 1;
	}
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}
#endif

int
mnstr_printf(stream *restrict s, const char *restrict format, ...)
{
	char buf[512], *bf = buf;
	int i = 0;
	size_t bfsz = sizeof(buf);
	va_list ap;

	if (s == NULL || s->errnr)
		return -1;

	va_start(ap, format);
	i = vsnprintf(bf, bfsz, format, ap);
	va_end(ap);
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
		va_end(ap);
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

	if (s == NULL)
		return NULL;
	if ((b = malloc(sizeof(*b))) == NULL)
		return NULL;
	*b = (bstream) {
		.mode = size,
		.s = s,
		.eof = false,
	};
	if (size == 0)
		size = BUFSIZ;
	b->buf = malloc(size + 1 + 1);
	if (b->buf == NULL) {
		free(b);
		return NULL;
	}
	b->size = size;
	return b;
}

ssize_t
bstream_read(bstream *s, size_t size)
{
	ssize_t rd, rd1 = 0;

	if (s == NULL)
		return -1;

	if (s->eof)
		return 0;

	assert(s->buf != NULL);

	if (s->pos > 0) {
		if (s->pos < s->len) {
			/* move all data and end of string marker */
			memmove(s->buf, s->buf + s->pos, s->len - s->pos + 1);
			s->len -= s->pos;
		} else
			s->len = 0;
		s->pos = 0;
	}

	if (s->len == s->size) {
		size_t sz = size > 8192 ? 8192 : size;
		char tmpbuf[8192];

		/* before we realloc more space, see if there is a need */
		if ((rd1 = s->s->read(s->s, tmpbuf, 1, sz)) == 0) {
			s->eof = true;
			return 0;
		}
		if (rd1 < 0)
			return rd1;
		char *p;
		size_t ns = s->size + size;
		if ((p = realloc(s->buf, ns + 1)) == NULL) {
			return -1;
		}
		s->size = ns;
		s->buf = p;
		memcpy(s->buf + s->len, tmpbuf, rd1);
		s->len += rd1;
		size -= rd1;
		if (size == 0)
			return rd1;
	}

	if (s->len + size > s->size)
		size = s->size - s->len;

	rd = s->s->read(s->s, s->buf + s->len, 1, size);

	if (rd < 0)
		return rd;

	if (rd == 0) {
		s->eof = true;
		return rd1;
	}
	s->len += (size_t) rd;
	s->buf[s->len] = 0;	/* fill in the spare with EOS */
	return rd + rd1;
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
		if (s->pos < s->len) {
			/* move all data and end of string marker */
			memmove(s->buf, s->buf + s->pos, s->len - s->pos + 1);
			s->len -= s->pos;
		} else
			s->len = 0;
		s->pos = 0;
	}

	assert(s->buf != NULL);
	if (s->len == s->size) {
		char *p;
		size_t ns = s->size + size + 8192;
		if ((p = realloc(s->buf, ns + 1)) == NULL) {
			return -1;
		}
		s->size = ns;
		s->buf = p;
	}

	if (size > s->size - s->len)
		size = s->size - s->len;

	if (fgets(s->buf + s->len, (int) size, s->s->stream_data.p) == NULL)
		return -1;

	rd = strlen(s->buf + s->len);

	if (rd == 0) {
		s->eof = true;
		return 0;
	}
	s->len += rd;
	s->buf[s->len] = 0;	/* fill in the spare with EOS */
	return (ssize_t) rd;
}


ssize_t
bstream_next(bstream *s)
{
	if (s == NULL)
		return -1;
	if (s->mode > 0) {
		return bstream_read(s, s->mode);
	} else if (s->s->read == file_read) {
		return bstream_readline(s);
	} else {
		size_t sz = 0;
		ssize_t rd;

		while ((rd = bstream_read(s, 1)) == 1 &&
		       s->buf[s->pos + sz] != '\n') {
			sz++;	/* sz += rd, but rd == 1 */
		}
		if (rd < 0)
			return rd;
		return (ssize_t) sz;
	}
}

void
bstream_destroy(bstream *s)
{
	if (s) {
		if (s->s) {
			s->s->close(s->s);
			s->s->destroy(s->s);
		}
		if (s->buf)
			free(s->buf);
		free(s);
	}
}

/* ------------------------------------------------------------------ */
/* callback stream
 *
 * read-only stream which calls a user-provided callback function in
 * order to get more data to be returned to the reader */

struct cbstream {
	void *private;
	void (*destroy)(void *);
	void (*close)(void *);
	ssize_t (*read)(void *, void *, size_t, size_t);
};

static void
cb_destroy(stream *s)
{
	struct cbstream *cb = s->stream_data.p;

	if (cb->destroy)
		cb->destroy(cb->private);
	free(cb);
	s->stream_data.p = NULL;
	destroy_stream(s);
}

static void
cb_close(stream *s)
{
	struct cbstream *cb = s->stream_data.p;

	if (cb->close)
		cb->close(cb->private);
}

static ssize_t
cb_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	struct cbstream *cb = s->stream_data.p;

	return cb->read(cb->private, buf, elmsize, cnt);
}

stream *
callback_stream(void *restrict private,
		ssize_t (*read)(void *restrict private, void *restrict buf, size_t elmsize, size_t cnt),
		void (*close)(void *private),
		void (*destroy)(void *private),
		const char *restrict name)
{
	stream *s;
	struct cbstream *cb;

	s = create_stream(name);
	if (s == NULL)
		return NULL;
	cb = malloc(sizeof(struct cbstream));
	if (cb == NULL) {
		destroy_stream(s);
		return NULL;
	}
	*cb = (struct cbstream) {
		.private = private,
		.destroy = destroy,
		.read = read,
		.close = close,
	};
	s->stream_data.p = cb;
	s->read = cb_read;
	s->destroy = cb_destroy;
	s->close = cb_close;
	return s;
}

static ssize_t
stream_blackhole_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	(void) s;
	(void) buf;
	(void) elmsize;
	return (ssize_t) cnt;
}

static void
stream_blackhole_close(stream *s)
{
	(void) s;
	/* no resources to close */
}

stream *
stream_blackhole_create(void)
{
	stream *s;
	if ((s = create_stream("blackhole")) == NULL) {
		return NULL;
	}

	s->read = NULL;
	s->write = stream_blackhole_write;
	s->close = stream_blackhole_close;
	s->flush = NULL;
	s->readonly = false;
	return s;
}


/* fixed-width format streams */
#define STREAM_FWF_NAME "fwf_ftw"

typedef struct {
	stream *s;
	bool eof;
	/* config */
	size_t num_fields;
	size_t *widths;
	char filler;
	/* state */
	size_t line_len;
	char *in_buf;
	char *out_buf;
	size_t out_buf_start;
	size_t out_buf_remaining;
} stream_fwf_data;


static ssize_t
stream_fwf_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	stream_fwf_data *fsd;
	size_t to_write = cnt;
	size_t buf_written = 0;
	char nl_buf;

	fsd = (stream_fwf_data *) s->stream_data.p;
	if (fsd == NULL || elmsize != 1) {
		return -1;
	}
	if (fsd->eof)
		return 0;

	while (to_write > 0) {
		/* input conversion */
		if (fsd->out_buf_remaining == 0) {	/* need to convert next line */
			size_t field_idx, in_buf_pos = 0, out_buf_pos = 0;
			ssize_t actually_read = fsd->s->read(fsd->s, fsd->in_buf, 1, fsd->line_len);
			if (actually_read < (ssize_t) fsd->line_len) {	/* incomplete last line */
				if (actually_read < 0) {
					return actually_read;	/* this is an error */
				}
				fsd->eof = true;
				return (ssize_t) buf_written;	/* skip last line */
			}
			/* consume to next newline */
			while (fsd->s->read(fsd->s, &nl_buf, 1, 1) == 1 &&
			       nl_buf != '\n')
				;

			for (field_idx = 0; field_idx < fsd->num_fields; field_idx++) {
				char *val_start, *val_end;
				val_start = fsd->in_buf + in_buf_pos;
				in_buf_pos += fsd->widths[field_idx];
				val_end = fsd->in_buf + in_buf_pos - 1;
				while (*val_start == fsd->filler)
					val_start++;
				while (*val_end == fsd->filler)
					val_end--;
				while (val_start <= val_end) {
					if (*val_start == STREAM_FWF_FIELD_SEP) {
						fsd->out_buf[out_buf_pos++] = STREAM_FWF_ESCAPE;
					}
					fsd->out_buf[out_buf_pos++] = *val_start++;
				}
				fsd->out_buf[out_buf_pos++] = STREAM_FWF_FIELD_SEP;
			}
			fsd->out_buf[out_buf_pos++] = STREAM_FWF_RECORD_SEP;
			fsd->out_buf_remaining = out_buf_pos;
			fsd->out_buf_start = 0;
		}
		/* now we know something is in output_buf so deliver it */
		if (fsd->out_buf_remaining <= to_write) {
			memcpy((char *) buf + buf_written, fsd->out_buf + fsd->out_buf_start, fsd->out_buf_remaining);
			to_write -= fsd->out_buf_remaining;
			buf_written += fsd->out_buf_remaining;
			fsd->out_buf_remaining = 0;
		} else {
			memcpy((char *) buf + buf_written, fsd->out_buf + fsd->out_buf_start, to_write);
			fsd->out_buf_start += to_write;
			fsd->out_buf_remaining -= to_write;
			buf_written += to_write;
			to_write = 0;
		}
	}
	return (ssize_t) buf_written;
}


static void
stream_fwf_close(stream *s)
{
	stream_fwf_data *fsd = (stream_fwf_data *) s->stream_data.p;

	if (fsd != NULL) {
		stream_fwf_data *fsd = (stream_fwf_data *) s->stream_data.p;
		close_stream(fsd->s);
		free(fsd->widths);
		free(fsd->in_buf);
		free(fsd->out_buf);
		free(fsd);
		s->stream_data.p = NULL;
	}
}

static void
stream_fwf_destroy(stream *s)
{
	stream_fwf_close(s);
	destroy_stream(s);
}

stream *
stream_fwf_create(stream *restrict s, size_t num_fields, size_t *restrict widths, char filler)
{
	stream *ns;
	stream_fwf_data *fsd = malloc(sizeof(stream_fwf_data));

	if (fsd == NULL) {
		return NULL;
	}
	*fsd = (stream_fwf_data) {
		.s = s,
		.num_fields = num_fields,
		.widths = widths,
		.filler = filler,
		.line_len = 0,
		.eof = false,
	};
	for (size_t i = 0; i < num_fields; i++) {
		fsd->line_len += widths[i];
	}
	fsd->in_buf = malloc(fsd->line_len);
	if (fsd->in_buf == NULL) {
		close_stream(fsd->s);
		free(fsd);
		return NULL;
	}
	fsd->out_buf = malloc(fsd->line_len * 3);
	if (fsd->out_buf == NULL) {
		close_stream(fsd->s);
		free(fsd->in_buf);
		free(fsd);
		return NULL;
	}
	if ((ns = create_stream(STREAM_FWF_NAME)) == NULL) {
		close_stream(fsd->s);
		free(fsd->in_buf);
		free(fsd->out_buf);
		free(fsd);
		return NULL;
	}
	ns->read = stream_fwf_read;
	ns->close = stream_fwf_close;
	ns->destroy = stream_fwf_destroy;
	ns->write = NULL;
	ns->flush = NULL;
	ns->readonly = true;
	ns->stream_data.p = fsd;
	return ns;
}
