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
scan_octal_escape(const char **err_msg, const unsigned char **rr, unsigned char **ww, unsigned char *end, int c)
{
	const unsigned char *r = *rr + 2;
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
one_hex_digit(const char **err_msg, const unsigned char **rr, unsigned char *end)
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
scan_hex_escape(const char **err_msg, const unsigned char **rr, unsigned char **ww, unsigned char *end)
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
scan_unicode_escape(const char **err_msg, const unsigned char **rr, unsigned char **ww, unsigned char *end, int digits)
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

static int
scan_backslash_escape(const char **err_msg, unsigned const char **rr, unsigned char **ww, unsigned char *end)
{
	const unsigned char *r = *rr;
	if (end - r < 2) {
		*err_msg = "incomplete backslash escape sequence";
		return -50;
	}
	assert(r[0] == '\\');
	unsigned char c;
	switch (r[1]) {
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
			return scan_octal_escape(err_msg, rr, ww, end, r[1] - '0');
		case 'x':
			return scan_hex_escape(err_msg, rr, ww, end);
		case 'u':
			return scan_unicode_escape(err_msg, rr, ww, end, 4);
		case 'U':
			return scan_unicode_escape(err_msg, rr, ww, end, 8);
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
			c = r[1];
	}
	*rr += 2;
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
static int
scan_quoted(const char **err_msg, unsigned char *start, unsigned char *end, int quote, bool backslash_escapes, int *nwritten)
{
	if (end - start < 2) {
		*err_msg = "incomplete quoted text at end";
		return -30;
	}
	unsigned char *last = end - 1;
	const unsigned char *r = start + 1;
	unsigned char *w = start;

	while (r <= last) {
		assert(w <= r);
		if (*r == '\0') {
			*err_msg = "NUL character not allowed in textual data";
			return -31;
		}
		if (*r == quote) {
			if (r < last && r[1] == quote) {
				// doubled quote, write only one
				*w++ = quote;
				r += 2;
				continue;
			} else {
				// end quote found
				*w = '\0';
				*nwritten = w - start;
				return r - start + 1;
			}
		} else if (backslash_escapes && *r == '\\') {
			int ret = scan_backslash_escape(err_msg, &r, &w, end);
			if (ret < 0)
				return ret;
			continue;
		} else {
			// Some other character
			*w++ = *r++;
			continue;
		}
		assert(0 /* unreachable */);
	}
	*err_msg = "incomplete quoted text";
	return -32;
}

// Scan the text pointed to by 'start', looking for occurrences of 'col_sep'
// or 'line_sep'. If found, replace it with a '\0' and return succesfully. .
// Reaching 'end' is also considered an error.
// If 'backslash_escapes' is set, backslashes suppress the special .
// Return the number of bytes scanned, including the separator.
// On succesful return, write the separator found to '*sep_found'.
static int
scan_unquoted(const char **err_msg, unsigned char *start, unsigned char *end, int col_sep, int line_sep, bool backslash_escapes, unsigned char *sep_found)
{
	char *sep;

	if (!backslash_escapes) {
		// is there a col_sep anywhere?
		char *pcol = memchr(start, col_sep, end - start);
		char *pline;
		if (pcol) {
			// there is a col_sep. is there a line_sep before that?
			pline = memchr(start, line_sep, pcol - (char*)start);
		} else {
			// there is no col_sep, is there a line_sep anywhere?
			pline = memchr(start, line_sep, end - start);
		}
		// pline is either earlier than pcol or there was no pcol.
		sep = pline ? pline : pcol;
		if (sep) {
			if (memchr(start, '\0', sep - (char*)start) != NULL) {
				*err_msg = "NUL character not allowed in textual data";
				return -40;
			}
			*sep_found = *sep;
			*sep = 0;
			return sep - (char*)start + 1;
		} else {
			*err_msg = "no column- or line separator found";
			return -41;
		}
	}

	// go over it character by character and convert backslash escapes
	unsigned const char *r = start;
	unsigned char *w = start;
	while (r < end) {
		if (*r == '\0') {
			*err_msg = "NUL character not allowed in textual data";
			return -42;
		}
		if (*r == col_sep || *r == line_sep) {
			*sep_found = *r;
			*w = 0;
			return r - start + 1;
		}
		if (*r == '\\') {
			int ret = scan_backslash_escape(err_msg, &r, &w, end);
			if (ret < 0)
				return ret;
		} else {
			*w++ = *r++;
		}
	}
	// no sep found is an error
	*err_msg = "no column- or line separator found";
	return -43;
}

// Scan the text pointed to by 'start' up to the first occurrence of either
// the column or the line separator, but never beyond 'end'.
// Reaching 'end' is considered an error.
// Remove quoting and process backslash escapes. Place a '\0' at the end of
// the field, overwriting the separator or end quote.
// Return the (strictly positive) number of bytes scanned including the
// separator, or < 0 on error. Write the separator found to '*sep_found'.
static int
scan_field(const char **err_msg, unsigned char *start, unsigned char *end, int col_sep, int line_sep, int quote, bool backslash_escapes, unsigned char *sep_found)
{
	assert(start < end);

	int nread;
	int nwritten;
	if (quote && *start == quote) {

		nread = scan_quoted(err_msg, start, end, quote, backslash_escapes, &nwritten);
		if (nread < 0)
			return nread;
		// scan_quoted errors out if it reaches 'end' so we know 'start[n]' exists
		if (start[nread] == col_sep || start[nread] == line_sep) {
			*sep_found = start[nread];
			// nread should include the separator
			nread += 1;
		} else {
			*err_msg = "end quote must be followed by separator";
			return -11;
		}
	} else {
		nread = scan_unquoted(err_msg, start, end, col_sep, line_sep, backslash_escapes, sep_found);
		if (nread < 0)
			return nread;
		// with scan_unquoted, nread includes the separator, now replaced with '\0'
		nwritten = nread - 1;
	}

	assert(nread > 0); // the separator, if nothing else
	return nread;
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
	struct error_handling *errors,
	char *data_start, int skip_amount, char *data_end,
	int col_sep, int line_sep, int quote, bool backslash_escapes, char *null_repr,
	int ncols, int nrows, int **columns)
{
	unsigned char *p = (unsigned char*)&data_start[skip_amount];
	unsigned char *end = (unsigned char*)data_end;
	int row = 0;
	int col = 0;
	const char *err_msg = NULL;
	size_t null_repr_len = null_repr ? strlen(null_repr) : 0;
	while (p < end && row < nrows) {
		unsigned char sep = 0;
		int n;

		bool is_null = (
			null_repr
			&& p + null_repr_len < end
			&& (p[null_repr_len] == col_sep || p[null_repr_len] == line_sep)
			&& strncasecmp((char*)p, null_repr, null_repr_len) == 0
		);

		if (is_null) {
			sep = p[null_repr_len];
			n = null_repr_len + 1;
		} else {
			n = scan_field(&err_msg, p, end, col_sep, line_sep, quote, backslash_escapes, &sep);
		}
		assert(n != 0);
		assert((n < 0) == (err_msg != NULL));
		if (n < 0) {
			copy_report_error(errors, row, col, "%s", err_msg);
			throw(MAL, "copy.splitlines", "%s", copy_error_message(errors));
		}
		bool last_col = col == ncols - 1;
		bool ok;
		if (!last_col) {
			if (sep == col_sep) {
				ok = true;
			} else if (sep == line_sep) {
				copy_report_error(errors, row, -1, "too few fields, expected %d but found %d", ncols, col + 1);
				ok = false;
			} else {
				// if scan_field returned >=0 it must have found a separator, doesn't it?
				throw(MAL, "copy.splitlines", "internal error: found %d while col sep is %d and line sep is %d", sep, col_sep, line_sep);
			}
		} else {
			if (sep == line_sep) {
				ok = true;
			} else if (sep == col_sep) {
				// Special case: col_sep followed by line_sep as in TPC-H.
				// So the column separator is really a column terminator.
				// n bytes have been consumed so p[n-1] is 0.
				assert(p[n - 1] == '\0');
				if (p + n < end && p[n] == line_sep) {
					n += 1; // skip the col_sep
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
		int field = is_null ? int_nil : ((char*)p - data_start);
		columns[col][row] = field;
		p += n;
		if (last_col) {
			row += 1;
			col = 0;
		} else {
			col += 1;
		}
	}

	assert(p == end || row == nrows);

	if (p < end) {
		throw(MAL, "copy.splitlines", "leftover data at end of buffer");
	}
	if (row < nrows) {
		throw(MAL, "copy.splitlines", "not enough rows found in buffer");
	}

	return MAL_SUCCEED;
}

