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


#ifdef HAVE_LIBLZMA

struct inner_state {
	lzma_stream strm;
	uint8_t buf[64*1024];
	lzma_ret error_code;
};

static pump_buffer
xz_get_src_win(inner_state_t *xz)
{
	return (pump_buffer) {
		.start = (char*) xz->strm.next_in,
		.count = xz->strm.avail_in,
	};
}

static void
xz_set_src_win(inner_state_t *xz, pump_buffer buf)
{
	xz->strm.next_in = (const uint8_t*)buf.start;
	xz->strm.avail_in = buf.count;
}

static pump_buffer
xz_get_dst_win(inner_state_t *xz)
{
	return (pump_buffer) {
		.start = (char*) xz->strm.next_out,
		.count = xz->strm.avail_out,
	};
}

static void
xz_set_dst_win(inner_state_t *xz, pump_buffer buf)
{
	xz->strm.next_out = (uint8_t*)buf.start;
	xz->strm.avail_out = buf.count;
}

static pump_buffer
xz_get_buffer(inner_state_t *xz)
{
	return (pump_buffer) {
		.start = (char*)xz->buf,
		.count = sizeof(xz->buf),
	};
}

static pump_result
xz_work(inner_state_t *xz, pump_action action)
{
	lzma_action a;
	switch (action) {
	case PUMP_NO_FLUSH:
		a = LZMA_RUN;
		break;
	case PUMP_FLUSH_DATA:
		a = LZMA_SYNC_FLUSH;
		break;
	case PUMP_FLUSH_ALL:
		a = LZMA_FULL_FLUSH;
		break;
	case PUMP_FINISH:
		a = LZMA_FINISH;
		break;
	default:
		assert(0 /* unknown action */);
		return PUMP_ERROR;
	}

	lzma_ret ret = lzma_code(&xz->strm, a);
	xz->error_code = ret;

	switch (ret) {
		case LZMA_OK:
			return PUMP_OK;
		case LZMA_STREAM_END:
			return PUMP_END;
		default:
			return PUMP_ERROR;
	}
}

static void
xz_finalizer(inner_state_t *xz)
{
	lzma_end(&xz->strm);
	free(xz);
}

static const char *
xz_get_error(inner_state_t *xz)
{
	static const char *msgs[] = {
		"LZMA_OK",
		"LZMA_STREAM_END",
		"LZMA_NO_CHECK",
		"LZMA_UNSUPPORTED_CHECK",
		"LZMA_GET_CHECK",
		"LZMA_MEM_ERROR",
		"LZMA_MEMLIMIT_ERROR",
		"LZMA_FORMAT_ERROR",
		"LZMA_OPTIONS_ERROR",
		"LZMA_DATA_ERROR",
		"LZMA_BUF_ERROR",
		"LZMA_PROG_ERROR"
	};

	if (xz->error_code <= LZMA_PROG_ERROR)
		return msgs[xz->error_code];
	else
		return "unknown LZMA error code";
}




stream *
xz_stream(stream *inner, int preset)
{
	inner_state_t *xz = calloc(1, sizeof(inner_state_t));
	pump_state *state = calloc(1, sizeof(pump_state));
	if (xz == NULL || state == NULL) {
		free(xz);
		free(state);
		mnstr_set_open_error(inner->name, errno, "couldn't initialize xz stream");
		return NULL;
	}

	state->inner_state = xz;
	state->get_src_win = xz_get_src_win;
	state->set_src_win = xz_set_src_win;
	state->get_dst_win = xz_get_dst_win;
	state->set_dst_win = xz_set_dst_win;
	state->get_buffer = xz_get_buffer;
	state->worker = xz_work;
	state->finalizer = xz_finalizer;
	state->get_error = xz_get_error;

	lzma_ret ret;
	if (inner->readonly) {
		ret = lzma_stream_decoder(&xz->strm, UINT64_MAX, LZMA_CONCATENATED);
	} else {
		ret = lzma_easy_encoder(&xz->strm, preset, LZMA_CHECK_CRC64);
	}

	stream *s = pump_stream(inner, state);

	if (ret != LZMA_OK || s == NULL) {
		lzma_end(&xz->strm);
		free(xz);
		free(state);
		return NULL;
	}

	return s;
}

static stream *
open_xzstream(const char *restrict filename, const char *restrict flags)
{
	stream *inner;
	int preset = 0;

	inner = open_stream(filename, flags);
	if (inner == NULL)
		return NULL;

	return xz_stream(inner, preset);
}

stream *
open_xzrstream(const char *filename)
{
	stream *s = open_xzstream(filename, "rb");
	if (s == NULL)
		return NULL;

	assert(s->readonly == true);
	assert(s->binary == true);
	return s;
}

stream *
open_xzwstream(const char *restrict filename, const char *restrict mode)
{
	stream *s = open_xzstream(filename, mode);
	if (s == NULL)
		return NULL;

	assert(s->readonly == false);
	assert(s->binary == true);
	return s;
}

stream *
open_xzrastream(const char *filename)
{
	stream *s = open_xzstream(filename, "r");
	s = create_text_stream(s);
	if (s == NULL)
		return NULL;

	assert(s->readonly == true);
	assert(s->binary == false);
	return s;
}

stream *
open_xzwastream(const char *restrict filename, const char *restrict mode)
{
	stream *s = open_xzstream(filename, mode);
	s = create_text_stream(s);
	if (s == NULL)
		return NULL;
	assert(s->readonly == false);
	assert(s->binary == false);
	return s;
}
#else

stream *
xz_stream(stream *inner, int preset)
{
	(void) inner;
	(void) preset;
	mnstr_set_open_error(inner->name, 0, "XZ/LZMA support has been left out of this MonetDB");
	return NULL;
}
stream *
open_xzrstream(const char *filename)
{
	mnstr_set_open_error(filename, 0, "XZ/LZMA support has been left out of this MonetDB");
	return NULL;
}

stream *
open_xzwstream(const char *restrict filename, const char *restrict mode)
{
	(void) mode;
	mnstr_set_open_error(filename, 0, "XZ/LZMA support has been left out of this MonetDB");
	return NULL;
}

stream *
open_xzrastream(const char *filename)
{
	mnstr_set_open_error(filename, 0, "XZ/LZMA support has been left out of this MonetDB");
	return NULL;
}

stream *
open_xzwastream(const char *restrict filename, const char *restrict mode)
{
	(void) mode;
	mnstr_set_open_error(filename, 0, "XZ/LZMA support has been left out of this MonetDB");
	return NULL;
}

#endif
