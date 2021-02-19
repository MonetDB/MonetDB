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


#ifdef HAVE_LIBLZ4

#define READ_CHUNK (1024)
#define WRITE_CHUNK (1024)

struct inner_state {
	pump_buffer src_win;
	pump_buffer dst_win;
	pump_buffer buffer;
	union {
		LZ4F_cctx *c;
		LZ4F_dctx *d;
	} ctx;
	LZ4F_preferences_t compression_prefs;
	LZ4F_errorCode_t error_code;
	bool finished;
};

static pump_buffer
get_src_win(inner_state_t *inner_state)
{
	return inner_state->src_win;
}

static void
set_src_win(inner_state_t *inner_state, pump_buffer buf)
{
	inner_state->src_win = buf;
}

static pump_buffer
get_dst_win(inner_state_t *inner_state)
{
	return inner_state->dst_win;
}

static void
set_dst_win(inner_state_t *inner_state, pump_buffer buf)
{
	inner_state->dst_win = buf;
}

static pump_buffer
get_buffer(inner_state_t *inner_state)
{
	return inner_state->buffer;
}

static pump_result
decomp(inner_state_t *inner_state, pump_action action)
{
	LZ4F_errorCode_t ret;

	if (inner_state->src_win.count == 0 && action == PUMP_FINISH)
		inner_state->finished = true;
	if (inner_state->finished)
		return PUMP_END;

	LZ4F_decompressOptions_t opts = {0};
	size_t nsrc = inner_state->src_win.count; // amount available
	size_t ndst = inner_state->dst_win.count; // space available
	ret = LZ4F_decompress(
		inner_state->ctx.d,
		inner_state->dst_win.start, &ndst,
		inner_state->src_win.start, &nsrc,
		&opts);
	// Now nsrc has become the amount consumed, ndst the amount produced!
	inner_state->src_win.start += nsrc;
	inner_state->src_win.count -= nsrc;
	inner_state->dst_win.start += ndst;
	inner_state->dst_win.count -= ndst;

	if (LZ4F_isError(ret)) {
		inner_state->error_code = ret;
		return PUMP_ERROR;
	}
	return PUMP_OK;
}

static void
decomp_end(inner_state_t *inner_state)
{
	LZ4F_freeDecompressionContext(inner_state->ctx.d);
	free(inner_state->buffer.start);
	free(inner_state);
}


static pump_result
compr(inner_state_t *inner_state, pump_action action)
{
	LZ4F_compressOptions_t opts = {0};
	size_t consumed;
	LZ4F_errorCode_t produced;
	pump_result intended_result;

	if (inner_state->finished)
		return PUMP_END;

	size_t chunk = inner_state->src_win.count;
	if (chunk > WRITE_CHUNK)
		chunk = WRITE_CHUNK;

	switch (action) {

		case PUMP_NO_FLUSH:
			produced = LZ4F_compressUpdate(
				inner_state->ctx.c,
				inner_state->dst_win.start,
				inner_state->dst_win.count,
				inner_state->src_win.start,
				chunk,
				&opts);
			consumed = chunk;
			intended_result = PUMP_OK;
			break;

		case PUMP_FLUSH_ALL:
		case PUMP_FLUSH_DATA:
			// FLUSH_ALL not supported yet, just flush the data
			produced = LZ4F_flush(
				inner_state->ctx.c,
				inner_state->dst_win.start,
				inner_state->dst_win.count,
				&opts);
			consumed = 0;
			intended_result = PUMP_END;
			break;

		case PUMP_FINISH:
			produced = LZ4F_compressEnd(
				inner_state->ctx.c,
				inner_state->dst_win.start,
				inner_state->dst_win.count,
				&opts);
			consumed = 0;
			inner_state->finished = true;
			intended_result = PUMP_END;
			break;

		default:
			assert(0); // shut up, compiler!
			return PUMP_ERROR;
	}

	if (LZ4F_isError(produced)) {
		inner_state->error_code = produced;
		return PUMP_ERROR;
	}

	inner_state->src_win.start += consumed;
	inner_state->src_win.count -= consumed;
	inner_state->dst_win.start += produced;
	inner_state->dst_win.count -= produced;

	return intended_result;
}

static void
compr_end(inner_state_t *inner_state)
{
	LZ4F_freeCompressionContext(inner_state->ctx.c);
	free(inner_state->buffer.start);
	free(inner_state);
}

static const char*
get_error(inner_state_t *inner_state)
{
	return LZ4F_getErrorName(inner_state->error_code);
}

static stream *
setup_decompression(stream *inner, pump_state *state)
{
	inner_state_t *inner_state = state->inner_state;
	void *buf = malloc(READ_CHUNK);
	if (buf == NULL)
		return NULL;

	inner_state->buffer = (pump_buffer) { .start = buf, .count = READ_CHUNK };
	inner_state->src_win = inner_state->buffer;
	inner_state->src_win.count = 0;

	LZ4F_errorCode_t ret = LZ4F_createDecompressionContext(
		&inner_state->ctx.d, LZ4F_VERSION);
	if (LZ4F_isError(ret)) {
		free(buf);
		mnstr_set_open_error(inner->name, 0, "failed to initialize lz4: %s", LZ4F_getErrorName(ret));
		return NULL;
	}

	state->worker = decomp;
	state->finalizer = decomp_end;

	stream *s = pump_stream(inner, state);
	if (s == NULL) {
		free(buf);
		return NULL;
	}

	return s;
}

static stream *
setup_compression(stream *inner, pump_state *state, int level)
{
	inner_state_t *inner_state = state->inner_state;
	LZ4F_errorCode_t ret;

	// When pumping data into the compressor, the output buffer must be
	// sufficiently large to hold all output caused by the current input. We
	// will restrict our writes to be at most WRITE_CHUCK large and allocate
	// a buffer that can accomodate even the worst case amount of output
	// caused by input of that size.

	// The required size depends on the preferences so we set those first.
	memset(&inner_state->compression_prefs, 0, sizeof(inner_state->compression_prefs));
	inner_state->compression_prefs.compressionLevel = level;

	// Set up a buffer that can hold the largest output block plus the initial
	// header frame.
	size_t bound = LZ4F_compressBound(WRITE_CHUNK, &inner_state->compression_prefs);
	size_t bufsize = bound + LZ4F_HEADER_SIZE_MAX;
	char *buffer = malloc(bufsize);
	if (buffer == NULL)
		return NULL;
	inner_state->buffer = (pump_buffer) { .start = buffer, .count = bufsize };
	inner_state->dst_win = inner_state->buffer;
	state->elbow_room = bound;

	ret = LZ4F_createCompressionContext(&inner_state->ctx.c, LZ4F_VERSION);
	if (LZ4F_isError(ret)) {
		free(buffer);
		return NULL;
	}

	// Write the header frame.
	size_t nwritten = LZ4F_compressBegin(
		inner_state->ctx.c,
		inner_state->dst_win.start,
		inner_state->dst_win.count,
		&inner_state->compression_prefs
	);
	if (LZ4F_isError(nwritten)) {
		LZ4F_freeCompressionContext(inner_state->ctx.c);
		free(buffer);
		mnstr_set_open_error(inner->name, 0, "failed to initialize lz4: %s", LZ4F_getErrorName(ret));
		return NULL;
	}
	inner_state->dst_win.start += nwritten;
	inner_state->dst_win.count -= nwritten;

	state->worker = compr;
	state->finalizer = compr_end;

	stream *s = pump_stream(inner, state);
	if (s == NULL) {
		free(buffer);
		return NULL;
	}

	return s;
}

stream *
lz4_stream(stream *inner, int level)
{
	inner_state_t *inner_state = calloc(1, sizeof(inner_state_t));
	pump_state *state = calloc(1, sizeof(pump_state));
	if (inner_state == NULL || state == NULL) {
		free(inner_state);
		free(state);
		mnstr_set_open_error(inner->name, errno, "couldn't initialize lz4 stream");
		return NULL;
	}

	state->inner_state = inner_state;
	state->get_src_win = get_src_win;
	state->set_src_win = set_src_win;
	state->get_dst_win = get_dst_win;
	state->set_dst_win = set_dst_win;
	state->get_buffer = get_buffer;
	state->get_error = get_error;

	stream *s;
	if (inner->readonly)
		s = setup_decompression(inner, state);
	else
		s = setup_compression(inner, state, level);

	if (s == NULL) {
		free(inner_state);
		free(state);
		return NULL;
	}

	return s;
}

static stream *
open_lz4stream(const char *restrict filename, const char *restrict flags)
{
	stream *inner;
	int preset = 6;

	inner = open_stream(filename, flags);
	if (inner == NULL)
		return NULL;

	return lz4_stream(inner, preset);
}

stream *
open_lz4rstream(const char *filename)
{
	stream *s = open_lz4stream(filename, "rb");
	if (s == NULL)
		return NULL;

	assert(s->readonly == true);
	assert(s->binary == true);
	return s;
}

stream *
open_lz4wstream(const char *restrict filename, const char *restrict mode)
{
	stream *s = open_lz4stream(filename, mode);
	if (s == NULL)
		return NULL;

	assert(s->readonly == false);
	assert(s->binary == true);
	return s;
}

stream *
open_lz4rastream(const char *filename)
{
	stream *s = open_lz4stream(filename, "r");
	s = create_text_stream(s);
	if (s == NULL)
		return NULL;

	assert(s->readonly == true);
	assert(s->binary == false);
	return s;
}

stream *
open_lz4wastream(const char *restrict filename, const char *restrict mode)
{
	stream *s = open_lz4stream(filename, mode);
	s = create_text_stream(s);
	if (s == NULL)
		return NULL;
	assert(s->readonly == false);
	assert(s->binary == false);
	return s;
}
#else

stream *
lz4_stream(stream *inner, int preset)
{
	(void) inner;
	(void) preset;
	mnstr_set_open_error(inner->name, 0, "LZ4 support has been left out of this MonetDB");
	return NULL;
}

stream *
open_lz4rstream(const char *filename)
{
	mnstr_set_open_error(filename, 0, "LZ4 support has been left out of this MonetDB");
	return NULL;
}

stream *
open_lz4wstream(const char *restrict filename, const char *restrict mode)
{
	(void) mode;
	mnstr_set_open_error(filename, 0, "LZ4 support has been left out of this MonetDB");
	return NULL;
}

stream *
open_lz4rastream(const char *filename)
{
	mnstr_set_open_error(filename, 0, "LZ4 support has been left out of this MonetDB");
	return NULL;
}

stream *
open_lz4wastream(const char *restrict filename, const char *restrict mode)
{
	(void) mode;
	mnstr_set_open_error(filename, 0, "LZ4 support has been left out of this MonetDB");
	return NULL;
}

#endif
