/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* streams working on a lzma/xz-compressed disk file */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"
#include "pump.h"


#ifdef HAVE_LIBBZ2

struct inner_state {
	bz_stream strm;
	int (*work)(bz_stream *strm, int flush);
	int (*end)(bz_stream *strm);
	void (*reset)(inner_state_t *inner_state);
	bool eof_reached;
	char buf[64*1024];
};


static pump_buffer
get_src_win(inner_state_t *inner_state)
{
	return (pump_buffer) {
		.start = (void*) inner_state->strm.next_in,
		.count = inner_state->strm.avail_in,
	};
}

static void
set_src_win(inner_state_t *inner_state, pump_buffer buf)
{
	assert(buf.count < UINT_MAX);
	inner_state->strm.next_in = buf.start;
	inner_state->strm.avail_in = (unsigned int)buf.count;
}

static pump_buffer
get_dst_win(inner_state_t *inner_state)
{
	return (pump_buffer) {
		.start = inner_state->strm.next_out,
		.count = inner_state->strm.avail_out,
	};
}

static void
set_dst_win(inner_state_t *inner_state, pump_buffer buf)
{
	assert(buf.count < UINT_MAX);
	inner_state->strm.next_out = buf.start;
	inner_state->strm.avail_out = (unsigned int)buf.count;
}

static pump_buffer
get_buffer(inner_state_t *inner_state)
{
	return (pump_buffer) {
		.start = (char*)inner_state->buf,
		.count = sizeof(inner_state->buf),
	};
}

static pump_result
work(inner_state_t *inner_state, pump_action action)
{
	if (inner_state->eof_reached)
		return PUMP_END;

	int a;
	switch (action) {
	case PUMP_NO_FLUSH:
		a = BZ_RUN;
		break;
	case PUMP_FLUSH_DATA:
		a = BZ_FLUSH;
		break;
	case PUMP_FLUSH_ALL:
		a = BZ_FLUSH;
		break;
	case PUMP_FINISH:
		a = BZ_FINISH;
		break;
	default:
		assert(0 /* unknown action */);
		return PUMP_ERROR;
	}

	int ret = inner_state->work(&inner_state->strm, a);

	switch (ret) {
		case BZ_OK:
		case BZ_RUN_OK:
			if (a == PUMP_NO_FLUSH)
				return PUMP_OK;
			else
				/* when flushing or finishing, done */
				return PUMP_END;
		case BZ_FLUSH_OK:
		case BZ_FINISH_OK:
			/* flushing and finishing is not yet done */
			return PUMP_OK;
		case BZ_STREAM_END:
			if (action == PUMP_NO_FLUSH && inner_state->reset != NULL) {
				// attempt to read concatenated additional bz2 stream
				inner_state->reset(inner_state);
				return PUMP_OK;
			}
			inner_state->eof_reached = true;
			return PUMP_END;
		default:
			return PUMP_ERROR;
	}
}

static void
finalizer(inner_state_t *inner_state)
{
	inner_state->end(&inner_state->strm);
	free(inner_state);
}

static const char*
bz2_get_error(inner_state_t *inner_state)
{
	int dummy;
	return BZ2_bzerror(&inner_state->strm, &dummy);
}

static int
BZ2_bzDecompress_wrapper(bz_stream *strm, int a)
{
	(void)a;
	return BZ2_bzDecompress(strm);
}

static void
bz2_decompress_reset(inner_state_t *inner_state)
{
	pump_buffer src = get_src_win(inner_state);
	pump_buffer dst = get_dst_win(inner_state);
	BZ2_bzDecompressEnd(&inner_state->strm);
	BZ2_bzDecompressInit(&inner_state->strm, 0, 0);
	set_src_win(inner_state, src);
	set_dst_win(inner_state, dst);
}

stream *
bz2_stream(stream *inner, int level)
{
	inner_state_t *bz = calloc(1, sizeof(inner_state_t));
	pump_state *state = calloc(1, sizeof(pump_state));
	if (bz == NULL || state == NULL) {
		free(bz);
		free(state);
		mnstr_set_open_error(inner->name, errno, "couldn't initialize bz2 stream");
		return NULL;
	}

	state->inner_state = bz;
	state->get_src_win = get_src_win;
	state->set_src_win = set_src_win;
	state->get_dst_win = get_dst_win;
	state->set_dst_win = set_dst_win;
	state->get_buffer = get_buffer;
	state->worker = work;
	state->get_error = bz2_get_error;
	state->finalizer = finalizer;

	int ret;
	if (inner->readonly) {
		bz->work = BZ2_bzDecompress_wrapper;
		bz->end = BZ2_bzDecompressEnd;
		bz->reset = bz2_decompress_reset;
		ret = BZ2_bzDecompressInit(&bz->strm, 0, 0);
	} else {
		bz->work = BZ2_bzCompress;
		bz->end = BZ2_bzCompressEnd;
		if (level == 0)
			level = 6;
		ret = BZ2_bzCompressInit(&bz->strm, level, 0, 0);
	}

	if (ret != BZ_OK) {
		free(bz);
		free(state);
		mnstr_set_open_error(inner->name, 0, "failed to initialize bz2: code %d", ret);
		return NULL;
	}

	stream *s = pump_stream(inner, state);

	if (s == NULL) {
		bz->end(&bz->strm);
		free(bz);
		free(state);
		return NULL;
	}

	return s;
}

static stream *
open_bzstream(const char *restrict filename, const char *restrict flags)
{
	stream *inner;
	int preset = 6;

	inner = open_stream(filename, flags);
	if (inner == NULL)
		return NULL;

	return bz2_stream(inner, preset);
}

stream *
open_bzrstream(const char *filename)
{
	stream *s = open_bzstream(filename, "rb");
	if (s == NULL)
		return NULL;

	assert(s->readonly == true);
	assert(s->binary == true);
	return s;
}

stream *
open_bzwstream(const char *restrict filename, const char *restrict mode)
{
	stream *s = open_bzstream(filename, mode);
	if (s == NULL)
		return NULL;

	assert(s->readonly == false);
	assert(s->binary == true);
	return s;
}

stream *
open_bzrastream(const char *filename)
{
	stream *s = open_bzstream(filename, "r");
	s = create_text_stream(s);
	if (s == NULL)
		return NULL;

	assert(s->readonly == true);
	assert(s->binary == false);
	return s;
}

stream *
open_bzwastream(const char *restrict filename, const char *restrict mode)
{
	stream *s = open_bzstream(filename, mode);
	s = create_text_stream(s);
	if (s == NULL)
		return NULL;
	assert(s->readonly == false);
	assert(s->binary == false);
	return s;
}
#else

stream *
bz2_stream(stream *inner, int preset)
{
	(void) inner;
	(void) preset;
	mnstr_set_open_error(inner->name, 0, "BZIP2 support has been left out of this MonetDB");
	return NULL;
}

stream *
open_bzrstream(const char *filename)
{
	mnstr_set_open_error(filename, 0, "BZIP2 support has been left out of this MonetDB");
	return NULL;
}

stream *
open_bzwstream(const char *restrict filename, const char *restrict mode)
{
	(void) mode;
	mnstr_set_open_error(filename, 0, "BZIP2 support has been left out of this MonetDB");
	return NULL;
}

stream *
open_bzrastream(const char *filename)
{
	mnstr_set_open_error(filename, 0, "BZIP2 support has been left out of this MonetDB");
	return NULL;
}

stream *
open_bzwastream(const char *restrict filename, const char *restrict mode)
{
	(void) mode;
	mnstr_set_open_error(filename, 0, "BZIP2 support has been left out of this MonetDB");
	return NULL;
}

#endif
