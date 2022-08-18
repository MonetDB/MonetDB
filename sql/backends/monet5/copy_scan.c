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

static const char *
scan_octal_escape(struct scan_state *state, unsigned char **ww, int acc)
{
	state->pos += 2;

	if (state->pos >= state->end || state->pos[0] < '0' || state->pos[0] > '7')
		goto end;
	acc = 8 * acc + *state->pos++ - '0';
	if (state->pos >= state->end || state->pos[0] < '0' || state->pos[0] > '7')
		goto end;
	acc = 8 * acc + *state->pos++ - '0';
end:
	if (acc > 0xFF) {
		state->escape_pending = false; // pos has already advanced beyond the backslash
		return "octal escape out of range";
	}
	if (acc == 0) {
		state->escape_pending = false; // pos has already advanced beyond the backslash
		return "\\000 is not a valid octal escape";
	}

	*(*ww)++ = acc;
	return NULL;
}

static int
one_hex_digit(const char **err_msg, struct scan_state *state)
{
	if (state->pos >= state->end) {
		// this only occurs if for example the buffer ends in '\\' 'x'.
		*err_msg = "incomplete hex sequence";
		return -1;
	}
	unsigned int d = state->pos[0];
	unsigned int d0 = d - '0';
	unsigned int da = d - 'a';
	unsigned int dA = d - 'A';
	int v0 = ( d0 < 10 ? d0 + 1 : 0);
	int va = (da < 6 ? da + 11 : 0);
	int vA = (dA < 6 ? dA + 11 : 0);
	int v =  v0 | va | vA;

	// If it's not a hex digit, v will be 0, otherwise it will be 1 too high
	if (v > 0) {
		state->pos++;
		return v - 1;
	} else {
		*err_msg = "incomplete hex sequence";
		return -1;
	}
}

static const char *
scan_hex_escape(struct scan_state *state, unsigned char **ww)
{
	const char *err_msg = NULL;
	int acc;
	state->pos += 2;
	int d = one_hex_digit(&err_msg, state);
	assert((d < 0) == (err_msg != NULL));
	if (err_msg) {
		state->escape_pending = false; // pos has already advanced beyond the backslash
		return err_msg;
	}
	acc = d;
	d = one_hex_digit(&err_msg, state);
	assert((d < 0) == (err_msg != NULL));
	if (err_msg) {
		state->escape_pending = false; // pos has already advanced beyond the backslash
		return err_msg;
	}
	acc = 16 * acc + d;

	assert(acc >= 0);
	assert(acc < 0xFF);
	if (acc == 0) {
		state->escape_pending = false; // pos has already advanced beyond the backslash
		return "\\x00 is not a valid hex escape";
	}

	*(*ww)++ = acc;
	return NULL;
}

static int
utf8cont(unsigned int n, int shift)
{
	n = n >> shift;
	n = n & 0x3F; // 0x3F == 0b0011_1111
	n = n | 0x80; // 0x80 == 0b1000_0000
	return n;
}

static const char *
scan_unicode_escape(struct scan_state *state, unsigned char **ww, int digits)
{
	unsigned int acc = 0;
	state->pos += 2;
	if (state->end - state->pos < digits) {
		return "incomplete unicode hex sequence";
	}
	for (int i = 0; i < digits; i++) {
		const char *err_msg = NULL;
		int d = one_hex_digit(&err_msg, state);
		assert((d < 0) == (err_msg != NULL));
		if (err_msg) {
			state->escape_pending = false; // pos has already advanced beyond the backslash
			return err_msg;
		}
		acc = 16 * acc + d;
	}
	if (acc == 0) {
		state->escape_pending = false; // pos has already advanced beyond the backslash
		if (digits == 8)
			return "\\U00000000 is not a valid unicode escape";
		else
			return "\\u0000 is not a valid unicode escape";
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
		state->escape_pending = false; // pos has already advanced beyond the backslash
		return "invalid unicode escape, it denotes a surrogate halve";
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
	return NULL;
}

static const char *
scan_backslash_escape(struct scan_state *state, unsigned char **ww)
{
	if (state->end - state->pos < 2) {
		return "incomplete backslash escape sequence";
	}
	assert(state->pos[0] == '\\');
	unsigned char c;
	switch (state->pos[1]) {
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
			return scan_octal_escape(state, ww, state->pos[1] - '0');
		case 'x':
			return scan_hex_escape(state, ww);
		case 'u':
			return scan_unicode_escape(state, ww, 4);
		case 'U':
			return scan_unicode_escape(state, ww, 8);
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
	return NULL;
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
	state->quoted = true;

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
				state->quoted = false;
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
	assert(state->quoted == false);
	assert(state->escape_pending == false);

	if (state->quote_char && state->pos[0] == state->quote_char) {
		const char *err_msg = NULL;
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

static gdk_return
check_row_end(
	struct error_handling *errors, struct scan_state *state,
	int row, int col, int ncols, unsigned char sep)
{
	if (col == ncols - 1) {
		// Last column. Expect LINE_SEP or COL_SEP LINE_SEP
		if (sep == state->line_sep) {
			return GDK_SUCCEED;
		} else if (sep == state->col_sep) {
			assert(state->pos > state->start && state->pos[-1] == '\0');
			if (state->pos < state->end && state->pos[0] == state->line_sep) {
				state->pos++;
				return GDK_SUCCEED;
			} else {
				copy_report_error(errors, row, -1, "too many fields, expected %d but found more", ncols);
				return GDK_FAIL;
			}
		}
	} else {
		// Not the last column. Expect COL_SEP
		if (sep == state->col_sep) {
			return GDK_SUCCEED;
		} else if (sep == state->line_sep) {
			copy_report_error(errors, row, -1, "too few fields, expected %d but found %d", ncols, col + 1);
			// If we're in BEST EFFORT mode we're going to try to scan to the
			// end of the line. But in fact we're already there and have replaced
			// it with a \0, so the scan would actually skip the next line instead
			// of the rest of the current line.
			state->pos--;
			state->pos[0] = state->line_sep;
			return GDK_FAIL;
		} else {
			copy_report_error(errors, row, col, "internal error: found %d while col_sep is %d and line_sep is %d", sep, state->col_sep, state->line_sep);
			return GDK_FAIL;
		}
	}
	assert(0 && "unreachable");
	return GDK_FAIL;
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
		int field_offset;
		bool ok;

		bool field_is_null = (
			null_repr
			&& state->pos + null_repr_len < state->end
			&& (state->pos[null_repr_len] == state->col_sep || state->pos[null_repr_len] == state->line_sep)
			&& strncasecmp((char*)state->pos, null_repr, null_repr_len) == 0
		);

		if (field_is_null) {
			field_offset = int_nil;
			sep = state->pos[null_repr_len];
			state->pos += null_repr_len + 1;
			ok = true;
		} else {
			field_offset = state->pos - state->start;
			const char *err_msg = scan_field(state, &sep);
			if (err_msg == NULL) {
				ok = true;
			} else {
				copy_report_error(errors, row, col, "%s", err_msg);
				ok = false;
			}
		}

		if (ok && check_row_end(errors, state, row, col, ncols, sep) == GDK_FAIL) {
			ok = false;
		}

		if (ok) {
			// The happy path.  Store the field and advance row and col.
			columns[col][row] = field_offset;
			if (col < ncols - 1) {
				col += 1;
			} else {
				row += 1;
				col = 0;
			}
			continue;
		}

		// An error has occurred. BEST EFFORT determines if we want to stop right now
		const char *err = copy_check_too_many_errors(errors, "copy.splitlines");
		if (err) {
			// Bail out now
			throw(MAL, "copy.splitlines", "%s", copy_error_message(errors));
		}

		// We must be in BEST EFFORT mode.
		// Set all fields to nil and advance to the next line
		for (int i = 0; i < ncols; i++)
			columns[i][row] = int_nil;
		col = 0;
		row += 1;
		if (find_end_of_line(state))
			state->pos++;
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

