/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/* return display width of Unicode codepoint c */
extern int charwidth(uint32_t c);

/* decode UTF-8 string byte by byte into *state and *codep, returns
*  state; UTF-8 sequence is complete (and value is in *codep) when state
*  is UTF8_ACCEPT, incorrect when state is UTF8_REJECT, and incomplete
*  for any other value of state */

/* this function and the table are copyright Bjoern Hoehrmann per the
 * below notice.  The layout was changed. */

// Copyright (c) 2008-2009 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

extern const uint8_t utf8d[364];
#define UTF8_ACCEPT 0
#define UTF8_REJECT 12

static inline uint32_t
decode(uint32_t *state, uint32_t *codep, uint32_t byte)
{
	uint32_t type = utf8d[byte];

	*codep = (*state != UTF8_ACCEPT) ?
		(byte & 0x3fu) | (*codep << 6) :
		(0xff >> type) & (byte);

	*state = utf8d[256 + *state + type];
	return *state;
}
/* end copyright Bjoern Hoehrmann */

/* return in *c the codepoint of the next character in string s, return
 * a pointer to the start of the following character */
static inline char *
nextchar(const char *s, uint32_t *c)
{
	uint32_t codepoint = 0, state = 0;
	while (*s) {
		switch (decode(&state, &codepoint, (uint8_t) *s++)) {
		case UTF8_ACCEPT:
			*c = codepoint;
			return (char *) s;
		case UTF8_REJECT:
			*c = 0;
			return NULL;
		default:
			break;
		}
	}
	*c = 0;
	return NULL;
}
