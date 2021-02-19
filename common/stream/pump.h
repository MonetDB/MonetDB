/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
 */


typedef enum {
	PUMP_OK,
	PUMP_END,
	PUMP_ERROR,
} pump_result;


typedef enum {
	PUMP_NO_FLUSH,
	PUMP_FLUSH_DATA,
	PUMP_FLUSH_ALL,
	PUMP_FINISH,
} pump_action;


typedef struct {
	char *start;
	size_t count;
} pump_buffer;

// To be defined by the user
typedef struct inner_state inner_state_t;

typedef pump_buffer (*buf_getter)(inner_state_t *inner_state);
typedef void (*buf_setter)(inner_state_t *inner_state, pump_buffer buf);

typedef pump_result (*pump_worker)(inner_state_t *inner_state, pump_action action);

typedef struct pump_state {
	inner_state_t *inner_state;
	buf_getter get_src_win;
	buf_setter set_src_win;
	buf_getter get_dst_win;
	buf_setter set_dst_win;
	buf_getter get_buffer;
	pump_worker worker;
	void (*finalizer)(inner_state_t *inner_state);
	const char *(*get_error)(inner_state_t *inner_state);
	size_t elbow_room;
} pump_state;

stream *pump_stream(stream *inner, pump_state *state);
