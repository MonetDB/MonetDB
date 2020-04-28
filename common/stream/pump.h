/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* streams working on a lzma/xz-compressed disk file */

#include "monetdb_config.h"

/* The logic of zlib's inflate and various other compression libraries
 * is always very similar. At the heart there is a work function which is
 * called with an input buffer, an output buffer and some internal state.
 * The function tries to move bits out of the input buffer through the
 * internal state to the output buffer until the input buffer is empty
 * or the output buffer is full.
 *
 * It has an action parameter which controls flushing: sometimes, for example
 * when closing the file, you want to flush the internal state out instead
 * of hanging onto it waiting for more compression opportunities. If that
 * parameter is set, the protocol is that you keep making room in the output
 * buffer and keep offering *exactly the same* input buffer until the
 * worker indicates all content has been flushed.
 *
 * All compression libraries we use have such an API but of course the
 * parameter types etc are always different.
 * This function encapsulates the logic of copying a whole buffer
 * out to a stream in a type-agnostic way.
 */

/* Helper functions used to access the state in a generic way */

typedef enum {
	PUMP_OK,
	PUMP_END,
	PUMP_ERROR,
} pump_result;

typedef enum {
	PUMP_WORK,
	PUMP_FLUSH_DATA,
	PUMP_FLUSH_ALL,
	PUMP_FINISH,
} pump_action;

typedef pump_result (*pump_worker)(void *state, pump_action action);
typedef ssize_t (*pump_io)(void *state, char *data, size_t len);

typedef struct {
	char *start;
	size_t count;
} pump_buffer;

typedef struct {
	char **start;
	size_t *count;
} pump_buffer_location;

// These helper functions help make sure we don't accidentally
// write `buf.start == 0` where we meant `*buf.start == 0`.

static inline char *
start(pump_buffer_location b)
{
	return *b.start;
}

static inline size_t
count(pump_buffer_location b)
{
	return *b.count;
}

static inline void
set_count(pump_buffer_location b, size_t count)
{
	*b.count = count;
}

static inline void
set_start(pump_buffer_location b, char *start)
{
	*b.start = start;
}

static inline void
reset_buffer(pump_buffer_location b, pump_buffer buf)
{
	*b.start = buf.start;
	*b.count = buf.count;
}

static inline pump_result
generic_pump_out(
	void *state,
	pump_action action,
	pump_buffer buffer,
	pump_buffer_location window_in,
	pump_buffer_location window_out,
	pump_worker worker,
	pump_io ship_out)
{
	while (1) {
		// Make sure there is room in the output buffer
		if (count(window_out) == 0) {
			size_t amount = start(window_out) - buffer.start;
			ssize_t nwritten = ship_out(state, buffer.start, amount);
			if (nwritten != (ssize_t)amount)
				return PUMP_ERROR;
			reset_buffer(window_out, buffer);
		}

		// Try to make progress
		pump_result ret = worker(state, action);
		if (ret == PUMP_ERROR)
			return PUMP_ERROR;

		// There was no error but if input is still available, we definitely
		// need another round
		if (count(window_in) > 0)
			continue;

		// Though the input data has been consumed, some may still linger
		// in the internal state.
		if (action == PUMP_WORK) {
			// Let it linger, we'll combine it with the next batch
			assert(ret == PUMP_OK); // worker would never PUMP_END
			return PUMP_OK;
		}

		// We are flushing or finishing or whatever.
		// We may need to keep iterating to flush the internal state.
		// Is there any internal state left?
		if (ret == PUMP_OK)
			// yes, there is
			continue;

		// All internal state has been drained.
		// Now drain the output buffer
		assert(ret == PUMP_END);
		size_t amount = start(window_out) - buffer.start;
		if (amount > 0) {
			ssize_t nwritten = ship_out(state, buffer.start, amount);
			if (nwritten != (ssize_t)amount)
				return PUMP_ERROR;
		}
		reset_buffer(window_out, buffer);
		return PUMP_END;
	}
}

/* Similar to generic_pump_out, but for reading.
*
* In every iteration, fill the input buffer if empty, and let the worker try to
* make progress. Stop iterating if the output buffer is full, when ship_in fails
* or when it does not fill the whole buffer. The latter case is important when
* we're reading for example from a socket. It's more important to return the
* data that has come in so far than to wait until the output buffer is
* completely full.
*
* Returns PUMP_END if the input is exhausted and no data lingers in the internal
* state, PUMP_ERROR when the input has a failure or PUMP_OK otherwise.
 */
static inline pump_result
generic_pump_in(
	void *state,
	pump_buffer buffer,
	pump_buffer_location window_in,
	pump_buffer_location window_out,
	pump_worker worker,
	pump_io ship_in)
{
	char *orig_out = start(window_out); // nice for debugging
	(void)orig_out;
	while (1) {
		if (count(window_out) == 0)
			// Output buffer is sufficiently full.
			return PUMP_OK;

		// Handle input, if possible and necessary
		if (start(window_in) != NULL && count(window_in) == 0) {
			// start != NULL means we haven't encountered EOF yet
			ssize_t nread = ship_in(state, buffer.start, buffer.count);
			if (nread < 0)
				// Error. Return directly, discarding any data lingering
				// in the internal state.
				return PUMP_ERROR;
			if (nread == 0)
				// Set to NULL so we'll remember next time.
				// Maybe there is some data in the internal state we don't
				// return immediately.
				set_start(window_in, NULL);
			else
				// All good
				set_start(window_in, buffer.start);
			set_count(window_in, (ssize_t)nread);
		}

		// Try to make some progress
		pump_action action = (start(window_in) != NULL) ? PUMP_WORK : PUMP_FINISH;
		assert(count(window_out) > 0);
		assert(count(window_in) > 0 || action == PUMP_FINISH);
		pump_result ret = worker(state, action);
		if (ret == PUMP_ERROR)
			return PUMP_ERROR;

		if (ret == PUMP_END)
			// If you say so
			return PUMP_END;

		// If we get here we made some progress so we're ready for a new iteration.
	}
}

