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

/* A buffered stream consists of a sequence of blocks.  Each block
 * consists of a count followed by the data in the block.  A flush is
 * indicated by an empty block (i.e. just a count of 0).
 */

static ssize_t bs_read_internal(stream *restrict ss, void *restrict buf, size_t elmsize, size_t cnt);

static bs *
bs_create(void)
{
	/* should be a binary stream */
	bs *ns;

	if ((ns = malloc(sizeof(*ns))) == NULL)
		return NULL;
	*ns = (bs) {0};
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
			if (!mnstr_writeSht(ss->inner, (int16_t) blksize) ||
			    ss->inner->write(ss->inner, s->buf, 1, s->nr) != (ssize_t) s->nr) {
				mnstr_copy_error(ss, ss->inner);
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
	ssize_t ret = bs_read_internal(ss, buf, elmsize, cnt);
	if (ret != 0)
		return ret;

	bs *b = (bs*) ss-> stream_data.p;
	if (b->prompt == NULL || b->pstream == NULL)
		return 0;

	// before returning the 0 we send the prompt and make another attempt.
	if (mnstr_write(b->pstream, b->prompt, strlen(b->prompt), 1) != 1)
		return -1;
	if (mnstr_flush(b->pstream, MNSTR_FLUSH_DATA) < 0)
		return -1;

	// if it succeeds, return that to the client.
	// if it's still a block boundary, return that to the client.
	// if there's an error, return that to the client.
	return bs_read_internal(ss, buf, elmsize, cnt);
}

static ssize_t
bs_read_internal(stream *restrict ss, void *restrict buf, size_t elmsize, size_t cnt)
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
		switch (mnstr_readSht(ss->inner, &blksize)) {
		case -1:
			mnstr_copy_error(ss, ss->inner);
			return -1;
		case 0:
			return 0;
		case 1:
			break;
		}
		if ((uint16_t) blksize > (BLOCK << 1 | 1)) {
			mnstr_set_error(ss, MNSTR_READ_ERROR, "invalid block size %d", blksize);
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
			ssize_t m = ss->inner->read(ss->inner, buf, 1, n);

			if (m <= 0) {
				mnstr_copy_error(ss, ss->inner);
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
			switch (mnstr_readSht(ss->inner, &blksize)) {
			case -1:
				mnstr_copy_error(ss, ss->inner);
				return -1;
			case 0:
				return 0;
			case 1:
				break;
			}
			if ((uint16_t) blksize > (BLOCK << 1 | 1)) {
				mnstr_set_error(ss, MNSTR_READ_ERROR, "invalid block size %d", blksize);
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

	return ns;
}

void
set_prompting(stream *block_stream, const char *prompt, stream *prompt_stream)
{
	if (isa_block_stream(block_stream)) {
		bs *bs = block_stream->stream_data.p;
		bs->prompt = prompt;
		bs->pstream = prompt_stream;
	}
}
