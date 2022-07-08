/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "mal_exception.h"

#include "copy.h"
#include "rel_copy.h"

static int
scan_octal_escape(const char **err_msg, unsigned char **rr, unsigned char **ww, unsigned char *end, int c)
{
	unsigned char *r = *rr + 2;
	if (r >= end || *r < '0' || *r > '7')
		goto end;
	c = 8 * c + *r - '0';
	r++;
	if (r >= end || *r < '0' || *r > '7')
		goto end;
	c = 8 * c + *r - '0';
	r++;
end:
	if (c > 0xFF) {
		*err_msg = "octal escape out of range";
		return -1;
	}
	if (c == 0) {
		*err_msg = "\\000 is not a valid octal escape";
		return -1;
	}
	*rr = r;
	*(*ww)++ = c;
	return 0;
}

static int
one_hex_digit(const char **err_msg, unsigned char **rr, unsigned char *end)
{
	if (*rr >= end) {
		// this only occurs if for example the buffer ends in '\\' 'x'.
		*err_msg = "incomplete hex sequence";
		return -1;
	}
	unsigned int d = **rr;
	unsigned int d0 = d - '0';
	unsigned int da = d - 'a';
	unsigned int dA = d - 'A';
	int v0 = ( d0 < 10 ? d0 + 1 : 0);
	int va = (da < 6 ? da + 11 : 0);
	int vA = (dA < 6 ? dA + 11 : 0);
	int v =  v0 | va | vA;

	// If it's not a hex digit, v will be 0, otherwise it will be 1 too high
	if (v > 0) {
		*rr += 1;
		return v - 1;
	} else {
		*err_msg = "incomplete hex sequence";
		return -1;
	}
}

static int
scan_hex_escape(const char **err_msg, unsigned char **rr, unsigned char **ww, unsigned char *end)
{
	int acc;
	*rr += 2;
	int d = one_hex_digit(err_msg, rr, end);
	if (d < 0)
		return d;
	acc = d;
	d = one_hex_digit(err_msg, rr, end);
	if (d < 0)
		return d;
	acc = 16 * acc + d;

	assert(acc >= 0);
	assert(acc < 0xFF);
	if (acc == 0) {
		*err_msg = "\\x00 is not a valid hex escape";
		return -1;
	}
	// rr has already been updated by one_hex_digit
	*(*ww)++ = acc;
	return 0;
}

static int
utf8cont(unsigned int n, int shift)
{
	n = n >> shift;
	n = n & 0x3F; // 0x3F == 0b0011_1111
	n = n | 0x80; // 0x80 == 0b1000_0000
	return n;
}
static int
scan_unicode_escape(const char **err_msg, unsigned char **rr, unsigned char **ww, unsigned char *end, int digits)
{
	unsigned int acc = 0;
	*rr += 2;
	if (end - *rr < digits) {
		*err_msg = "incomplete unicode hex sequence";
		return -1;
	}
	for (int i = 0; i < digits; i++) {
		int d = one_hex_digit(err_msg, rr, end);
		if (d < 0)
			return d;
		acc = 16 * acc + d;
	}
	if (acc == 0) {
		if (digits == 8)
			*err_msg = "\\U00000000 is not a valid unicode escape";
		else
			*err_msg = "\\u0000 is not a valid unicode escape";
		return -1;
	}
	else if (acc <      0x80) {
		*(*ww)++ = acc;
	} else if (acc <  0x0800) {
		*(*ww)++ = (acc >> 6) | 0xC0; //    0xC0 == 0b1100_0000
		*(*ww)++ = utf8cont(acc, 0);
	} else if (acc <  0xD800) {
		*(*ww)++ = (acc >> 12) | 0xE0; //   0xE0 == 0b1110_0000
		*(*ww)++ = utf8cont(acc, 6);
		*(*ww)++ = utf8cont(acc, 0);
	} else if (acc <  0xE000) {
		*err_msg = "invalid unicode escape, it denotes a surrogate halve";
		return -1;
	} else if (acc < 0x10000) {
		*(*ww)++ = (acc >> 12) | 0xE0; //   0xE0 == 0b1110_0000
		*(*ww)++ = utf8cont(acc, 6);
		*(*ww)++ = utf8cont(acc, 0);
	} else if (acc < 0x110000) {
		*(*ww)++ = (acc >> 18) | 0xF0; //   0xF0 == 0b1111_0000
		*(*ww)++ = utf8cont(acc, 12);
		*(*ww)++ = utf8cont(acc, 6);
		*(*ww)++ = utf8cont(acc, 0);
	}
	return 0;
}

static const char *
scan_backslash_escape(struct scan_state *state, unsigned char **ww)
{
	if (state->end - state->pos < 2) {
		return "incomplete backslash escape sequence";
	}
	assert(state->pos[0] == '\\');
	unsigned char c;
	int n;
	const char *err_msg = NULL;
	switch (state->pos[1]) {
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
			n = scan_octal_escape(&err_msg, &state->pos, ww, state->end, state->pos[1] - '0');
			return n < 0 ? err_msg : NULL;
		case 'x':
			n = scan_hex_escape(&err_msg, &state->pos, ww, state->end);
			return n < 0 ? err_msg : NULL;
		case 'u':
			n = scan_unicode_escape(&err_msg, &state->pos, ww, state->end, 4);
			return n < 0 ? err_msg : NULL;
		case 'U':
			n = scan_unicode_escape(&err_msg, &state->pos, ww, state->end, 8);
			return n < 0 ? err_msg : NULL;
		case 'a':
			c = '\a';
			break;
		case 'b':
			c = '\b';
			break;
		case 'f':
			c = '\f';
			break;
		case 'n':
			c = '\n';
			break;
		case 'r':
			c = '\r';
			break;
		case 't':
			c = '\t';
			break;
		default:
			c = state->pos[1];
	}
	state->pos += 2;
	*(*ww)++ = c;
	return 0;
}

// Scan the text pointed to by 'start', replacing quote pairs with single
// quote instances. Do not process backslash escapes but keep in mind that
// when those are enabled, quote characters can be backslash escaped.
// End at the first nondoubled nonquoted quote character and replace it with
// a \0 character. Return the number of bytes scanned, or < 0 on error.
// Never scan past 'end'. Reaching 'end' is considered an error.
// Writes the number of bytes written, excluding the '\0', to '*nwritten'.
static const char *
scan_quoted(struct scan_state *state)
{
	if (state->end - state->pos < 2) {
		return "incomplete quoted text at end";
	}
	unsigned char *last = state->end - 1;
	unsigned char *w = state->pos;

	// skip the opening quote
	assert(state->pos[0] == state->quote_char);
	state->pos++;

	while (state->pos <= last) {
		assert(w <= state->pos);
		if (state->pos[0] == '\0') {
			return "NUL character not allowed in textual data";
		}
		if (state->pos[0] == state->quote_char) {
			if (state->pos < last && state->pos[1] == state->quote_char) {
				// doubled quote, write only one
				*w++ = state->quote_char;
				state->pos += 2;
				continue;
			} else {
				// end quote found
				*w = '\0';
				state->pos++;
				return NULL;
			}
		} else if (state->escape_enabled && state->pos[0] == '\\') {
			const char *err_msg = scan_backslash_escape(state, &w);
			if (err_msg)
				return err_msg;
			continue;
		} else {
			// Some other character
			*w++ = *state->pos++;
			continue;
		}
		assert(0 /* unreachable */);
	}
	return "incomplete quoted text";
}

// Scan the text pointed to by 'start', looking for occurrences of 'col_sep'
// or 'line_sep'. If found, replace it with a '\0' and return succesfully. .
// Reaching 'end' is also considered an error.
// If 'backslash_escapes' is set, backslashes suppress the special .
// Return the number of bytes scanned, including the separator.
// On succesful return, write the separator found to '*sep_found'.
static const char *
scan_unquoted_no_escapes(struct scan_state *state, unsigned char *sep_found)
{
	// is there a col_sep anywhere?
	unsigned char *pcol = memchr(state->pos, state->col_sep, state->end - state->pos);
	unsigned char *pline;
	if (pcol) {
		// there is a col_sep. is there a line_sep before thatstate->?
		pline = memchr(state->pos, state->line_sep, pcol - state->pos);
	} else {
		// there is no col_sep, is there a line_sep anywhere?
		pline = memchr(state->pos, state->line_sep, state->end - state->pos);
	}
	// pline is either earlier than pcol or there was no pcol.
	unsigned char *sep = pline ? pline : pcol;
	if (sep) {
		if (memchr(state->pos, '\0', sep - state->pos) != NULL) {
			return "NUL character not allowed in textual data";
		}
		*sep_found = *sep;
		*sep = 0;
		state->pos = sep + 1;
		return NULL;
	} else {
		return "no column- or line separator found";
	}
}

static const char *
scan_unquoted_with_escapes(struct scan_state *state, unsigned char *sep_found)
{
	unsigned char *w = state->pos;
	while (state->pos < state->end) {
		if (*state->pos == '\0') {
			return "NUL character not allowed in textual data";
		}
		if (*state->pos == state->col_sep || *state->pos == state->line_sep) {
			*sep_found = *state->pos++;
			*w = 0;
			return NULL;
		}
		if (*state->pos == '\\') {
			const char *err_msg = scan_backslash_escape(state, &w);
			if (err_msg)
				return err_msg;
		} else {
			*w++ = *state->pos++;
		}
	}
	return "no column- or line separator found";
}

// Scan the text pointed to by 'start' up to the first occurrence of either
// the column or the line separator, but never beyond 'end'.
// Reaching 'end' is considered an error.
// Remove quoting and process backslash escapes. Place a '\0' at the end of
// the field, overwriting the separator or end quote.
// Return the (strictly positive) number of bytes scanned including the
// separator, or < 0 on error. Write the separator found to '*sep_found'.
static const char*
scan_field(struct scan_state *state, unsigned char *sep_found)
{
	assert(state->pos < state->end);

	if (state->quote_char && state->pos[0] == state->quote_char) {
		const char *err_msg = NULL;
		(void)scan_quoted;
		err_msg = scan_quoted(state);
		if (err_msg)
			return err_msg;
		// It's safe to access state->pos[0] because scan_quoted would have
		// given an error if we'd reached 'end'.
		if (state->pos[0] == state->col_sep || state->pos[0] == state->line_sep) {
			*sep_found = *state->pos++;
		} else {
			return "end quote must be followed by separator";
		}
	} else {
		const char *err_msg = NULL;
		if (state->escape_enabled) {
			err_msg = scan_unquoted_with_escapes(state, sep_found);
		} else {
			err_msg = scan_unquoted_no_escapes(state, sep_found);
		}
		if (err_msg)
			return err_msg;
	}

	return NULL;
}

// Scan the memory 'start' .. 'end' for fields, unquoting and unescaping.
// Place the index of each field in the appropriate index array.
// 'ncols' is the number of columns. 'columns' is a pointer to an array of
// size 'ncols' of pointers to the index arrays, each of which has room
// for 'nrows' field indices.
// Verify that column separators and row separators occur at the appropriate
// moment. Verify that exactly the right amount of data is offered.
// If a field contains the null_repr, replace its index with int_nil.
// Note: we must do the NULL check BEFORE processing quotes and backslashes!
str
scan_fields(
	struct error_handling *errors, struct scan_state *state,
	char *null_repr, int ncols, int nrows, int **columns)
{
	int row = 0;
	int col = 0;
	size_t null_repr_len = null_repr ? strlen(null_repr) : 0;
	while (state->pos < state->end && row < nrows) {
		unsigned char sep = 0;

		bool is_null = (
			null_repr
			&& state->pos + null_repr_len < state->end
			&& (state->pos[null_repr_len] == state->col_sep || state->pos[null_repr_len] == state->line_sep)
			&& strncasecmp((char*)state->pos, null_repr, null_repr_len) == 0
		);

		int field;
		const char *err_msg = NULL;
		if (is_null) {
			field = int_nil;
			sep = state->pos[null_repr_len];
			state->pos += null_repr_len + 1;
		} else {
			field = state->pos - state->start;
			err_msg = scan_field(state, &sep);
		}
		if (err_msg) {
			copy_report_error(errors, row, col, "%s", err_msg);
			throw(MAL, "copy.splitlines", "%s", copy_error_message(errors));
		}
		bool last_col = col == ncols - 1;
		bool ok;
		if (!last_col) {
			if (sep == state->col_sep) {
				ok = true;
			} else if (sep == state->line_sep) {
				copy_report_error(errors, row, -1, "too few fields, expected %d but found %d", ncols, col + 1);
				ok = false;
			} else {
				// it must have found a separator, doesn't it?
				throw(MAL, "copy.splitlines", "internal error: found %d while col sep is %d and line sep is %d", sep, state->col_sep, state->line_sep);
			}
		} else {
			if (sep == state->line_sep) {
				ok = true;
			} else if (sep == state->col_sep) {
				// Special case: col_sep followed by line_sep as in TPC-H.
				// So the column separator is really a column terminator.
				// n bytes have been consumed so p[n-1] is 0.
				assert(state->pos > state->start && state->pos[-1] == '\0');
				if (state->pos < state->end  && state->pos[0] == state->line_sep) {
					state->pos++;
					ok = true;
				} else {
					copy_report_error(errors, row, -1, "too many fields, expected %d but found more", ncols);
					ok = false;
				}
			}
		}
		if (!ok) {
			throw(MAL, "copy.splitlines", "%s", copy_error_message(errors));
		}
		columns[col][row] = field;
		if (last_col) {
			row += 1;
			col = 0;
		} else {
			col += 1;
		}
	}

	assert(state->pos == state->end || row == nrows);

	if (state->pos < state->end) {
		throw(MAL, "copy.splitlines", "leftover data at end of buffer");
	}
	if (row < nrows) {
		throw(MAL, "copy.splitlines", "not enough rows found in buffer");
	}

	return MAL_SUCCEED;
}

