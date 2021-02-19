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

#include <assert.h>

static pump_result pump_in(stream *s);
static pump_result pump_out(stream *s, pump_action action);

static ssize_t pump_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt);
static ssize_t pump_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt);
static int pump_flush(stream *s, mnstr_flush_level flush_level);
static void pump_close(stream *s);
static void pump_destroy(stream *s);


stream *
pump_stream(stream *inner, pump_state *state)
{
	assert(inner);
	assert(state);
	assert(state->set_src_win != NULL);
	assert(state->get_dst_win != NULL);
	assert(state->set_dst_win != NULL);
	assert(state->get_buffer != NULL);
	assert(state->worker != NULL);
	assert(state->get_error != NULL);
	assert(state->finalizer != NULL);

	inner_state_t *inner_state = state->inner_state;

	stream *s = create_wrapper_stream(NULL, inner);
	if (s == NULL)
		return NULL;

	pump_buffer buf = state->get_buffer(inner_state);
	if (s->readonly) {
		// Read from inner stream to src buffer through pumper to outbufs.
		// This means the src window starts empty
		buf.count = 0;
		state->set_src_win(inner_state, buf);
	} else {
		// from inbufs through pumper to dst buffer to inner stream.
		// This means the out window is our whole buffer.
		// Check for NULL in case caller has already initialized it
		// and written something
		if (state->get_dst_win(inner_state).start == NULL)
			state->set_dst_win(inner_state, buf);
	}

	s->stream_data.p = (void*) state;
	s->read = pump_read;
	s->write = pump_write;
	s->flush = pump_flush;
	s->close = pump_close;
	s->destroy = pump_destroy;
	return s;
}


static ssize_t
pump_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	pump_state *state = (pump_state*) s->stream_data.p;
	inner_state_t *inner_state = state->inner_state;
	size_t size = elmsize * cnt;

	state->set_dst_win(inner_state, (pump_buffer){ .start = buf, .count = size});
	pump_result ret = pump_in(s);
	if (ret == PUMP_ERROR) {
		const char *msg = state->get_error(inner_state);
		if (msg != NULL)
			msg = "processing failed without further error indication";
		mnstr_set_error(s, MNSTR_READ_ERROR, "%s", msg);
		return -1;
	}

	char *free_space = state->get_dst_win(inner_state).start;
	ssize_t nread = free_space - (char*) buf;

	return nread / (ssize_t) elmsize;
}


static ssize_t
pump_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	pump_state *state = (pump_state*) s->stream_data.p;
	inner_state_t *inner_state = state->inner_state;
	size_t size = elmsize * cnt;

	if (size == 0)
		return cnt;

	state->set_src_win(inner_state, (pump_buffer){ .start = (void*)buf, .count = size });
	pump_result ret = pump_out(s, PUMP_NO_FLUSH);
	if (ret == PUMP_ERROR) {
		const char *msg = state->get_error(inner_state);
		if (msg != NULL)
			msg = "processing failed without further error indication";
		mnstr_set_error(s, MNSTR_READ_ERROR, "%s", msg);
		return -1;
	}
	ssize_t nwritten = state->get_src_win(inner_state).start - (char*)buf;
	return nwritten / (ssize_t) elmsize;
}


static int pump_flush(stream *s, mnstr_flush_level flush_level)
{
	pump_state *state = (pump_state*) s->stream_data.p;
	inner_state_t *inner_state = state->inner_state;
	pump_action action;

	switch (flush_level) {
		case MNSTR_FLUSH_DATA:
			action = PUMP_FLUSH_DATA;
			break;
		case MNSTR_FLUSH_ALL:
			action = PUMP_FLUSH_ALL;
			break;
		default:
			assert(0 /* unknown flush_level */);
			action = PUMP_FLUSH_DATA;
			break;
	}

	state->set_src_win(inner_state, (pump_buffer){ .start = NULL, .count = 0 });
	ssize_t nwritten = pump_out(s, action);
	if (nwritten < 0)
		return -1;
	else
		return mnstr_flush(s->inner, flush_level);
}


static void
pump_close(stream *s)
{
	pump_state *state = (pump_state*) s->stream_data.p;
	inner_state_t *inner_state = state->inner_state;

	if (!s->readonly) {
		state->set_src_win(inner_state, (pump_buffer){ .start = NULL, .count = 0 });
		pump_out(s, PUMP_FINISH);
	}
	mnstr_close(s->inner);
}


static void
pump_destroy(stream *s)
{
	pump_state *state = (pump_state*) s->stream_data.p;
	inner_state_t *inner_state = state->inner_state;

	state->finalizer(inner_state);
	free(state);
	mnstr_destroy(s->inner);
	destroy_stream(s);
}

static pump_result
pump_in(stream *s)
{
	pump_state *state = (pump_state *) s->stream_data.p;
	inner_state_t *inner_state = state->inner_state;

	char *before = state->get_dst_win(inner_state).start;
	(void) before; // nice while in the debugger

	pump_buffer buffer = state->get_buffer(inner_state);
	while (1) {
		pump_buffer dst = state->get_dst_win(inner_state);
		pump_buffer src = state->get_src_win(inner_state);

		if (dst.count == 0)
			// Output buffer is full, we're done.
			return PUMP_OK;

		// Handle input, if possible and necessary
		if (src.start != NULL && src.count == 0) {
			// start != NULL means we haven't encountered EOF yet

			ssize_t nread = mnstr_read(s->inner, buffer.start, 1, buffer.count);

			if (nread < 0)
				// Error. Return directly, discarding any data lingering
				// in the internal state.
				return PUMP_ERROR;
			if (nread == 0)
				// Set to NULL so we'll remember next time.
				// Maybe there is some data in the internal state we don't
				// return immediately.
				src = (pump_buffer){.start=NULL, .count=0};
			else
				// All good
				src = (pump_buffer) { .start = buffer.start, .count = nread};

			state->set_src_win(inner_state, src);
		}

		pump_action action = (src.start != NULL) ? PUMP_NO_FLUSH : PUMP_FINISH;

		// Try to make some progress
		assert(dst.count > 0);
		assert(src.count > 0 || action == PUMP_FINISH);
		pump_result ret = state->worker(inner_state, action);
		if (ret == PUMP_ERROR)
			return PUMP_ERROR;

		if (ret == PUMP_END)
			// If you say so
			return PUMP_END;

		// If we get here we made some progress so we're ready for a new iteration.
	}
}


static pump_result
pump_out(stream *s, pump_action action)
{
	pump_state *state = (pump_state *) s->stream_data.p;
	inner_state_t *inner_state = state->inner_state;

	void *before = state->get_src_win(inner_state).start;
	(void) before; // nice while in the debugger

	pump_buffer buffer = state->get_buffer(inner_state);

	while (1) {
		pump_buffer dst = state->get_dst_win(inner_state);
		pump_buffer src = state->get_src_win(inner_state);

		// Make sure there is room in the output buffer
		assert(state->elbow_room <= buffer.count);
		if (dst.count == 0 || dst.count < state->elbow_room) {
			size_t amount = dst.start - buffer.start;
			ssize_t nwritten = mnstr_write(s->inner, buffer.start, 1, amount);
			if (nwritten != (ssize_t)amount)
				return PUMP_ERROR;
			dst = buffer;
			state->set_dst_win(inner_state, dst); // reset output window
		}

		// Try to make progress
		pump_result ret = state->worker(inner_state, action);
		if (ret == PUMP_ERROR)
			return PUMP_ERROR;

		// src and dst have been invalidated by the call to worker
		dst = state->get_dst_win(inner_state);
		src = state->get_src_win(inner_state);

		// There was no error but if input is still available, we definitely
		// need another round
		if (src.count > 0)
			continue;

		// Though the input data has been consumed, some of it might still
		// linger in the internal state.
		if (action == PUMP_NO_FLUSH) {
			// Let it linger, we'll combine it with the next batch
			assert(ret == PUMP_OK); // worker would never PUMP_END, would it?
			return PUMP_OK;
		}

		// We are flushing or finishing or whatever.
		// We may need to do more iterations to fully flush the internal state.
		// Is there any internal state left?
		if (ret == PUMP_OK)
			// yes, there is
			continue;

		// All internal state has been drained.
		// Now drain the output buffer
		assert(ret == PUMP_END);
		size_t amount = dst.start - buffer.start;
		if (amount > 0) {
			ssize_t nwritten = mnstr_write(s->inner, buffer.start, 1, amount);
			if (nwritten != (ssize_t)amount)
				return PUMP_ERROR;
		}
		state->set_dst_win(inner_state, buffer); // reset output window
		return PUMP_END;
	}


}
