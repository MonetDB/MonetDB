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
#include "pump.h"

/* When reading, text streams convert \r\n to \n regardless of operating system,
 * and they drop the leading UTF-8 BOM marker if found.
 * When writing on Windows, \n is translated back to \r\n.
 *
 * Currently, skipping the BOM happens when opening, not on the first read action.
 */

#define UTF8BOM		"\xEF\xBB\xBF"	/* UTF-8 encoding of Unicode BOM */
#define UTF8BOMLENGTH	3	/* length of above */

#define BUFFER_SIZE (65536)
struct inner_state {
	pump_buffer src_win;
	pump_buffer dst_win;
	pump_buffer putback_win;
	char putback_buf[UTF8BOMLENGTH];
	bool crlf_pending;
	char buffer[BUFFER_SIZE];
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
	return (pump_buffer) { .start = inner_state->buffer, .count = BUFFER_SIZE };
}

inline static void
put_byte(inner_state_t *ist, char byte)
{
	*ist->dst_win.start++ = byte;
	assert(ist->dst_win.count > 0);
	ist->dst_win.count--;
}

inline static char
take_byte(inner_state_t *ist)
{
	ist->src_win.count--;
	return *ist->src_win.start++;
}

static pump_result
text_pump_in(inner_state_t *ist, pump_action action)
{
	bool crlf_pending = ist->crlf_pending;

	while (ist->src_win.count > 0 && ist->dst_win.count > 0) {
		char c = take_byte(ist);
		switch (c) {
			case '\r':
				if (crlf_pending) {
					// put the previous one, which is clearly not followed by an \n
					put_byte(ist, '\r');
				}
				crlf_pending = true;
				continue;
			case '\n':
				put_byte(ist, c);
				crlf_pending = false;
				continue;
			default:
				if (crlf_pending) {
					put_byte(ist, '\r');
					crlf_pending = false;
					// if dst_win.count was 1, there is no room for another put_byte().
					if (ist->dst_win.count > 0) {
						put_byte(ist, c);
					} else {
						// no room anymore for char c, put it back!
						ist->src_win.start--;
						ist->src_win.count++;
					}
				} else {
					put_byte(ist, c);
				}
				continue;
		}
	}

	ist->crlf_pending = crlf_pending;

	if (action == PUMP_FINISH) {
		if (ist->src_win.count > 0)
			// More work to do
			return PUMP_OK;
		if (!ist->crlf_pending)
			// Completely done
			return PUMP_END;
		if (ist->dst_win.count > 0) {
			put_byte(ist, '\r');
			ist->crlf_pending = false; // not strictly necessary
			// Now we're completely done
			return PUMP_END;
		} else
			// Come back another time to flush the pending CR
			return PUMP_OK;
	} else
		// There is no error and we are not finishing so clearly we
		// must return PUMP_OK
		return PUMP_OK;
}


static pump_result
text_pump_in_with_putback(inner_state_t *ist, pump_action action)
{
	if (ist->putback_win.count > 0) {
		pump_buffer tmp = ist->src_win;
		ist->src_win = ist->putback_win;
		pump_result ret = text_pump_in(ist, PUMP_NO_FLUSH);
		ist->putback_win = ist->src_win;
		ist->src_win = tmp;
		if (ret == PUMP_ERROR)
			return PUMP_ERROR;
	}
	return text_pump_in(ist, action);
}


static pump_result
text_pump_out(inner_state_t *ist, pump_action action)
{
	size_t src_count = ist->src_win.count;
	size_t dst_count = ist->dst_win.count;
	size_t ncopy = src_count < dst_count ? src_count : dst_count;

	memcpy(ist->dst_win.start, ist->src_win.start, ncopy);
	ist->dst_win.start += ncopy;
	ist->dst_win.count -= ncopy;
	ist->src_win.start += ncopy;
	ist->src_win.count -= ncopy;

	if (ist->src_win.count > 0)
		// definitely not done
		return PUMP_OK;
	if (action == PUMP_NO_FLUSH)
		// never return PUMP_END
		return PUMP_OK;
	if (ist->crlf_pending)
		// src win empty but cr still pending so not done
		return PUMP_OK;
	// src win empty and no cr pending and flush or finish requested
	return PUMP_END;
}


static pump_result
text_pump_out_crlf(inner_state_t *ist, pump_action action)
{
	if (ist->crlf_pending && ist->dst_win.count > 0) {
		put_byte(ist, '\n');
		ist->crlf_pending = false;
	}

	while (ist->src_win.count > 0 && ist->dst_win.count > 0) {
		char c = take_byte(ist);
		if (c != '\n') {
			put_byte(ist, c);
			continue;
		}
		put_byte(ist, '\r');
		if (ist->dst_win.count > 0)
			put_byte(ist, '\n');
		else {
			ist->crlf_pending = true;
			break;
		}
	}

	if (ist->src_win.count > 0)
		// definitely not done
		return PUMP_OK;
	if (action == PUMP_NO_FLUSH)
		// never return PUMP_END
		return PUMP_OK;
	if (ist->crlf_pending)
		// src win empty but cr still pending so not done
		return PUMP_OK;
	// src win empty and no cr pending and flush or finish requested
	return PUMP_END;
}


static void
text_end(inner_state_t *s)
{
	free(s);
}


static const char*
get_error(inner_state_t *s)
{
	(void)s;
	return "line ending conversion failure";
}

static ssize_t
skip_bom(stream *s)
{
	pump_state *state = (pump_state*) s->stream_data.p;
	stream *inner = s->inner;
	inner_state_t *ist = state->inner_state;

	ssize_t nread = mnstr_read(inner, ist->putback_buf, 1, UTF8BOMLENGTH);
	if (nread < 0) {
		mnstr_copy_error(s, inner);
		return nread;
	}

	if (nread == UTF8BOMLENGTH &&  memcmp(ist->putback_buf, UTF8BOM, nread) == 0) {
		// Bingo! Skip it!
		s->isutf8 = true;
		return 3;
	}

	// We have consumed some bytes that have to be unconsumed.
	// skip_bom left them in the putback_buf.
	ist->putback_win.start = ist->putback_buf;
	ist->putback_win.count = nread;

	return 0;
}


stream *
create_text_stream(stream *inner)
{
	inner_state_t *inner_state = calloc(1, sizeof(inner_state_t));
	if (inner_state == NULL) {
		mnstr_set_open_error(inner->name, errno, NULL);
		return NULL;
	}

	pump_state *state = calloc(1, sizeof(pump_state));
	if (inner_state == NULL || state == NULL) {
		free(inner_state);
		mnstr_set_open_error(inner->name, errno, NULL);
		return NULL;
	}

	state->inner_state = inner_state;
	state->get_src_win = get_src_win;
	state->set_src_win = set_src_win;
	state->get_dst_win = get_dst_win;
	state->set_dst_win = set_dst_win;
	state->get_buffer = get_buffer;
	state->finalizer = text_end;
	state->get_error = get_error;

	inner_state->putback_win.start = inner_state->putback_buf;
	inner_state->putback_win.count = 0;
	if (inner->readonly) {
		inner_state->src_win.start = inner_state->buffer;
		inner_state->src_win.count = 0;
		state->worker = text_pump_in_with_putback;
	} else {
		inner_state->dst_win.start = inner_state->buffer;
		inner_state->dst_win.count = BUFFER_SIZE;
#ifdef _MSC_VER
		state->worker = text_pump_out_crlf;
		(void) text_pump_out;
#else
		state->worker = text_pump_out;
		(void) text_pump_out_crlf;
#endif
	}

	stream *s = pump_stream(inner, state);
	if (s == NULL) {
		free(inner_state);
		free(state);
		return NULL;
	}

	s->binary = false;

	if (s->readonly)
		if (skip_bom(s) < 0) {
			free(inner_state);
			free(state);
			char *err = mnstr_error(s);
			mnstr_set_open_error(inner->name, 0, "while looking for a byte order mark: %s", err);
			free(err);
			destroy_stream(s);
			return NULL;
		}

	return s;
}
