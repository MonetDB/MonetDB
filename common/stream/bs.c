/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"

/* ------------------------------------------------------------------ */

/* A buffered stream consists of a sequence of blocks.  Each block
 * consists of a count followed by the data in the block.  A flush is
 * indicated by an empty block (i.e. just a count of 0).
 */

static bs *
bs_create(void)
{
	/* should be a binary stream */
	bs *ns;

	if ((ns = malloc(sizeof(*ns))) == NULL)
		return NULL;
	*ns = (bs) {0};
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	InitializeCriticalSection(&ns->lock);
#else
	pthread_mutex_init(&ns->lock, NULL);
#endif
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
		s->nr += (uint16_t) n;
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
			if (!mnstr_writeSht(ss->inner, (int16_t) blksize) ||
			    ss->inner->write(ss->inner, s->buf, 1, s->nr) != (ssize_t) s->nr
			) {
				if (mnstr_errnr(ss->inner) != MNSTR_NO__ERROR) {
					mnstr_copy_error(ss, ss->inner);
				} else {
					mnstr_set_error(ss, MNSTR_WRITE_ERROR, "connection closed unexpectedly");
				}

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
bs_flush(stream *ss, mnstr_flush_level flush_level)
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
		/* always flush (even empty blocks) needed for the protocol) */
		if ((!mnstr_writeSht(ss->inner, (int16_t) blksize) ||
		     (s->nr > 0 &&
		      ss->inner->write(ss->inner, s->buf, 1, s->nr) != (ssize_t) s->nr))) {
			mnstr_copy_error(ss, ss->inner);
			s->nr = 0; /* data is lost due to error */
			return -1;
		}
		// shouldn't we flush ss->inner too?
		(void) flush_level;
		s->blks++;
		s->nr = 0;
	}
	return 0;
}

static int
bs_putoob(stream *ss, char val)
{
	bs *s = (bs *) ss->stream_data.p;
	if (s == NULL || ss->inner->putoob == NULL)
		return -1;
	if (!ss->readonly && s->nr > 0) {
		uint16_t blksize = (uint16_t) (s->nr << 1);
		s->bytes += s->nr;
		if (!mnstr_writeSht(ss->inner, (int16_t) blksize) ||
			ss->inner->write(ss->inner, s->buf, 1, s->nr) != (ssize_t) s->nr) {
			mnstr_copy_error(ss, ss->inner);
			s->nr = 0; /* data is lost due to error */
			return -1;
		}
		s->blks++;
		s->nr = 0;
	}
	return ss->inner->putoob(ss->inner, val);
}

static int
bs_getoob(stream *ss)
{
	int oobval;
	bs *s = (bs *) ss->stream_data.p;
	if (s == NULL || ss->inner->getoob == NULL)
		return 0;
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	EnterCriticalSection(&s->lock);
#else
	pthread_mutex_lock(&s->lock);
#endif
	if (s->seenoob) {
		s->seenoob = false;
		oobval = s->oobval;
	} else if (ss->readonly && s->itotal == 0)
		oobval = ss->inner->getoob(ss->inner);
	else
		oobval = 0;
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	LeaveCriticalSection(&s->lock);
#else
	pthread_mutex_unlock(&s->lock);
#endif
	return oobval;
}

/* Read buffered data and return the number of items read.  At the
 * flush boundary we will return 0 to indicate the end of a block,
 * unless prompt and pstream are set. In that case, only return 0
 * after the prompt has been written to pstream and another read
 * attempt immediately returns a block boundary.
 *
 * Structure field usage:
 * s - the underlying stream;
 * buf - not used;
 * itotal - the amount of data in the current block that hasn't yet
 *          been read;
 * nr - indicates whether the flush marker has to be returned.
 */
ssize_t
bs_read(stream *restrict ss, void *restrict buf, size_t elmsize, size_t cnt)
{
	bs *s = (bs *) ss->stream_data.p;
	size_t todo = cnt * elmsize;
	size_t n;

	assert(ss->readonly);
	if (!ss->readonly)
		return -1;
	if (s == NULL)
		return -1;

	cnt = 0;					/* #bytes copied to caller */
	if (s->itotal > 0) {
		n = s->itotal - s->nr;	/* amount readable in current block */
		if (n > todo)
			n = todo;
		memcpy(buf, s->buf + s->nr, n);
		buf = (void *) ((char *) buf + n);
		s->nr += (uint16_t) n;
		todo -= n;
		cnt += n;
		if (s->nr == s->itotal)
			s->itotal = s->nr = 0;
	}
	while (todo > 0 && !s->seenflush) {
		uint16_t blksize;
		assert(s->itotal == 0);
		switch (mnstr_readSht(ss->inner, (int16_t *) &blksize)) {
		case -1:
			mnstr_copy_error(ss, ss->inner);
			return -1;
		case 0:
			ss->eof |= ss->inner->eof;
			return 0;
		case 1:
			break;
		}
		if (blksize == 0xFFFF) {
			/* client interrupt */
			s->seenoob = true;
			ss->inner->read(ss->inner, &s->oobval, 1, 1);
			return 0;
		}
		if (blksize > (BLOCK << 1 | 1)) {
			mnstr_set_error(ss, MNSTR_READ_ERROR, "invalid block size %"PRIu16,
							blksize);
			return -1;
		}
		assert(s->nr == 0);
		s->seenflush = blksize & 1;
		s->itotal = blksize >> 1;
		ssize_t m;
		n = 0;
		do {
			m = ss->inner->read(ss->inner, s->buf + n, 1, s->itotal - n);
			if (m < 0) {
				mnstr_copy_error(ss, ss->inner);
				return -1;
			}
			n += (size_t) m;
		} while (n < s->itotal);
		if (n > todo)
			n = todo;
		memcpy(buf, s->buf, n);
		buf = (void *) ((char *) buf + n);
		s->nr += (uint16_t) n;
		todo -= n;
		cnt += n;
		if (s->nr == s->itotal)
			s->itotal = s->nr = 0;
	}
	if (s->seenflush && cnt == 0) {
		s->seenflush = false;	/* ready for next time */
	}
	return elmsize == 0 ? 0 : (ssize_t) (cnt / elmsize);
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
		bs_flush(ss, MNSTR_FLUSH_DATA);
	mnstr_close(ss->inner);
}

void
bs_destroy(stream *ss)
{
	bs *s;

	s = (bs *) ss->stream_data.p;
	assert(s);
	if (s) {
		if (ss->inner)
			ss->inner->destroy(ss->inner);
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
		DeleteCriticalSection(&s->lock);
#else
		pthread_mutex_destroy(&s->lock);
#endif
		free(s);
	}
	destroy_stream(ss);
}

void
bs_clrerr(stream *s)
{
	if (s->stream_data.p)
		mnstr_clearerr(s->inner);
}

stream *
bs_stream(stream *s)
{
	assert(isa_block_stream(s));
	return s->inner;
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
	if ((ns = create_wrapper_stream(NULL, s)) == NULL)
		return NULL;
	if ((b = bs_create()) == NULL) {
		destroy_stream(ns);
		mnstr_set_open_error(s->name, 0, "bs_create failed");
		return NULL;
	}
	/* blocksizes have a fixed little endian byteorder */
#ifdef WORDS_BIGENDIAN
	s->swapbytes = true;
#endif

	ns->flush = bs_flush;
	ns->read = bs_read;
	ns->write = bs_write;
	ns->close = bs_close;
	ns->destroy = bs_destroy;
	ns->stream_data.p = (void *) b;
	if (ns->putoob)
		ns->putoob = bs_putoob;
	if (ns->getoob)
		ns->getoob = bs_getoob;

	return ns;
}
