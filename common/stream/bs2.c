/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"


/* ------------------------------------------------------------------ */
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
#ifdef HAVE_SNAPPY
		size_t compressed_length = s->compbufsiz;
		snappy_status ret;
		if ((ret = snappy_compress(s->buf, s->nr, s->compbuf, &compressed_length)) != SNAPPY_OK) {
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
		assert(s->nr < INT_MAX);
		if ((compressed_length = LZ4_compress_fast(s->buf, s->compbuf, (int)s->nr, compressed_length, 1)) == 0) {
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
#ifdef HAVE_SNAPPY
		snappy_status ret;
		size_t uncompressed_length = s->bufsiz;
		if ((ret = snappy_uncompress(s->compbuf, s->itotal, s->buf, &uncompressed_length)) != SNAPPY_OK) {
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
		assert(s->itotal < INT_MAX);
		if ((uncompressed_length = LZ4_decompress_safe(s->compbuf, s->buf, (int)s->itotal, uncompressed_length)) <= 0) {
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
#ifndef HAVE_SNAPPY
		return -1;
#else
		return snappy_max_compressed_length(s->bufsiz);
#endif
	} else if (s->comp == COMPRESSION_LZ4) {
#ifndef HAVE_LIBLZ4
		return -1;
#else
		assert(s->bufsiz < INT_MAX);
		return LZ4_compressBound((int)s->bufsiz);
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
ssize_t
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
				mnstr_copy_error(ss, s->s);
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
bs2_flush(stream *ss, mnstr_flush_level flush_level)
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
			mnstr_copy_error(ss, s->s);
			return -1;
		}
		s->nr = 0;
		// shouldn't we flush s->s too?
		(void) flush_level;
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
ssize_t
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
			mnstr_copy_error(ss, s->s);
			return -1;
		case 0:
			return 0;
		case 1:
			break;
		}
		if (blksize < 0) {
			mnstr_set_error(ss, MNSTR_READ_ERROR, "invalid block size %" PRId64 "", blksize);
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
					mnstr_copy_error(ss, s->s);
					return -1;
				}
				m += (size_t) bytes_read;
			}
			if (s->comp != COMPRESSION_NONE) {
				uncompressed_length = decompress_stream_data(s);
				if (uncompressed_length < 0) {
					if (s->s->errkind != MNSTR_NO__ERROR)
						mnstr_copy_error(ss, s->s);
					else
						mnstr_set_error(ss, MNSTR_READ_ERROR, "uncompress failed with code %d", (int) uncompressed_length);
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
				mnstr_copy_error(ss, s->s);
				return -1;
			case 0:
				return 0;
			case 1:
				break;
			}
			if (blksize < 0) {
				mnstr_set_error(ss, MNSTR_READ_ERROR, "invalid block size %" PRId64 "", blksize);
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
						mnstr_copy_error(ss, s->s);
						return -1;
					}
					m += (size_t) bytes_read;
				}
				if (s->comp != COMPRESSION_NONE) {
					uncompressed_length = decompress_stream_data(s);
					if (uncompressed_length < 0) {
						if (s->s->errkind != MNSTR_NO__ERROR)
							mnstr_copy_error(ss, s->s);
						else
							mnstr_set_error(ss, MNSTR_READ_ERROR, "uncompress failed with code %d", (int) uncompressed_length);
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




static void
bs2_close(stream *ss)
{
	bs2 *s;

	s = (bs2 *) ss->stream_data.p;
	assert(s);
	if (s == NULL)
		return;
	if (!ss->readonly && s->nr > 0)
		bs2_flush(ss, MNSTR_FLUSH_DATA);
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
		s->s->timeout_data = ss->timeout_data;
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
		s = s->inner;
	}

#ifdef STREAM_DEBUG
	fprintf(stderr, "block_stream2 %s\n", s->name ? s->name : "<unnamed>");
#endif
	if ((ns = create_wrapper_stream(NULL, s)) == NULL)
		return NULL;
	if ((b = bs2_create(s, bufsiz, comp)) == NULL) {
		destroy_stream(ns);
		mnstr_set_open_error(s->name, 0, "bs2_create failed");
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
		os->inner = NULL;
		bs_destroy(os);
	}

	return ns;
}
