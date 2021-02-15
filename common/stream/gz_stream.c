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


#ifdef HAVE_LIBZ

struct inner_state {
	z_stream strm;
	int (*indeflate)(z_streamp strm, int flush);
	int (*indeflateEnd)(z_streamp strm);
	void (*reset)(inner_state_t *inner_state);
	Bytef buf[64*1024];
	bool prev_was_stream_end;
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
	inner_state->strm.next_in = (Bytef*)buf.start;
	inner_state->strm.avail_in = (uInt)buf.count;
}

static pump_buffer
get_dst_win(inner_state_t *inner_state)
{
	return (pump_buffer) {
		.start = (char*)inner_state->strm.next_out,
		.count = inner_state->strm.avail_out,
	};
}

static void
set_dst_win(inner_state_t *inner_state, pump_buffer buf)
{
	assert(buf.count < UINT_MAX);
	inner_state->strm.next_out = (Bytef*)buf.start;
	inner_state->strm.avail_out = (uInt)buf.count;
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
	int a;
	switch (action) {
	case PUMP_NO_FLUSH:
		a = Z_NO_FLUSH;
		break;
	case PUMP_FLUSH_DATA:
		a = Z_SYNC_FLUSH;
		break;
	case PUMP_FLUSH_ALL:
		a = Z_FULL_FLUSH;
		break;
	case PUMP_FINISH:
		a = Z_FINISH;
		break;
	default:
		assert(0 /* unknown action */);
		return PUMP_ERROR;
	}

	if (inner_state->strm.next_in == NULL && inner_state->prev_was_stream_end) {
		// on the previous Z_STREAM_END we attempted to continue in case there
		// was a concatenated additional zstream but that is not the case.
		return PUMP_END;
	}

	int ret = inner_state->indeflate(&inner_state->strm, a);

	inner_state->prev_was_stream_end = false;
	switch (ret) {
		case Z_OK:
			return PUMP_OK;
		case Z_STREAM_END:
			inner_state->prev_was_stream_end = true;
			if (action == PUMP_NO_FLUSH && inner_state->reset != NULL) {
				// zlib returns end, but maybe the input consists of multiple
				// gzipped files.
				inner_state->reset(inner_state);
				return PUMP_OK;
			} else {
				// no more incoming data
				return PUMP_END;
			}
		default:
			return PUMP_ERROR;
	}
}

static void
finalizer(inner_state_t *inner_state)
{
	inner_state->indeflateEnd(&inner_state->strm);
	free(inner_state);
}


static const char*
get_error(inner_state_t *inner_state)
{
	return inner_state->strm.msg;
}

static void
inflate_reset(inner_state_t *inner_state)
{
	pump_buffer src = get_src_win(inner_state);
	pump_buffer dst = get_dst_win(inner_state);
	inflateReset(&inner_state->strm);
	set_src_win(inner_state, src);
	set_dst_win(inner_state, dst);
}

stream *
gz_stream(stream *inner, int level)
{
	inner_state_t *gz = calloc(1, sizeof(inner_state_t));
	pump_state *state = calloc(1, sizeof(pump_state));
	if (gz == NULL || state == NULL) {
		free(gz);
		free(state);
		mnstr_set_open_error(inner->name, errno, "couldn't initialize gz stream");
		return NULL;
	}

	state->inner_state = gz;
	state->get_src_win = get_src_win;
	state->set_src_win = set_src_win;
	state->get_dst_win = get_dst_win;
	state->set_dst_win = set_dst_win;
	state->get_buffer = get_buffer;
	state->worker = work;
	state->get_error = get_error;
	state->finalizer = finalizer;

	int ret;
	if (inner->readonly) {
		gz->indeflate = inflate;
		gz->indeflateEnd = inflateEnd;
		gz->reset = inflate_reset;
		gz->strm.next_in = gz->buf;
		gz->strm.avail_in = 0;
		gz->strm.next_in = NULL;
		gz->strm.avail_in = 0;
		ret = inflateInit2(&gz->strm, 15 | 32); // 15 = allow all window sizes, 32 = accept gzip and zlib headers
	} else {
		gz->indeflate = deflate;
		gz->indeflateEnd = deflateEnd;
		gz->strm.next_out = gz->buf;
		gz->strm.avail_out = sizeof(gz->buf);
		if (level == 0)
			level = 6;
		ret = deflateInit2(&gz->strm, level, Z_DEFLATED, 15 | 16, 8, Z_DEFAULT_STRATEGY);
	}

	if (ret != Z_OK) {
		free(gz);
		free(state);
		mnstr_set_open_error(inner->name, 0, "failed to initialize gz stream: code %d", (int)ret);
		return NULL;
	}

	stream *s = pump_stream(inner, state);

	if (s == NULL) {
		gz->indeflateEnd(&gz->strm);
		free(gz);
		free(state);
		return NULL;
	}

	return s;
}

static stream *
open_gzstream(const char *restrict filename, const char *restrict flags)
{
	stream *inner;

	inner = open_stream(filename, flags);
	if (inner == NULL)
		return NULL;

	return gz_stream(inner, 0);
}

stream *
open_gzrstream(const char *filename)
{
	stream *s = open_gzstream(filename, "rb");
	if (s == NULL)
		return NULL;

	assert(s->readonly == true);
	assert(s->binary == true);
	return s;
}

stream *
open_gzwstream(const char *restrict filename, const char *restrict mode)
{
	stream *s = open_gzstream(filename, mode);
	if (s == NULL)
		return NULL;

	assert(s->readonly == false);
	assert(s->binary == true);
	return s;
}

stream *
open_gzrastream(const char *filename)
{
	stream *s = open_gzstream(filename, "r");
	s = create_text_stream(s);
	if (s == NULL)
		return NULL;

	assert(s->readonly == true);
	assert(s->binary == false);
	return s;
}

stream *
open_gzwastream(const char *restrict filename, const char *restrict mode)
{
	stream *s = open_gzstream(filename, mode);
	s = create_text_stream(s);
	if (s == NULL)
		return NULL;
	assert(s->readonly == false);
	assert(s->binary == false);
	return s;
}
#else

stream *
gz_stream(stream *inner, int preset)
{
	(void) inner;
	(void) preset;
	mnstr_set_open_error(inner->name, 0, "GZ support has been left out of this MonetDB");
	return NULL;
}
stream *
open_gzrstream(const char *filename)
{
	mnstr_set_open_error(filename, 0, "GZ support has been left out of this MonetDB");
	return NULL;
}

stream *
open_gzwstream(const char *restrict filename, const char *restrict mode)
{
	(void) mode;
	mnstr_set_open_error(filename, 0, "GZ support has been left out of this MonetDB");
	return NULL;
}

stream *
open_gzrastream(const char *filename)
{
	mnstr_set_open_error(filename, 0, "GZ support has been left out of this MonetDB");
	return NULL;
}

stream *
open_gzwastream(const char *restrict filename, const char *restrict mode)
{
	(void) mode;
	mnstr_set_open_error(filename, 0, "GZ support has been left out of this MonetDB");
	return NULL;
}

#endif
