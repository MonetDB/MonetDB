/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal_interpreter.h"

#include "copy.h"
#include "rel_copy.h"

static int
scan_octal_escape(unsigned char **rr, unsigned char **ww, unsigned char *end, int c)
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
	if (c > 0xFF)
		return -1;
	*rr = r;
	*(*ww)++ = c;
	return 0;
}

static int
one_hex_digit(unsigned char **rr, unsigned char *end)
{
	if (*rr >= end)
		return -1;
	unsigned int d = **rr;
	unsigned int d0 = d - '0';
	unsigned int da = d - 'a';
	unsigned int dA = d - 'A';
	int v0 = ( d0 < 10 ? d0 + 1 : 0);
	int va = (da < 6 ? da + 11 : 0);
	int vA = (dA < 6 ? dA + 11 : 0);
	int v =  v0 | va | vA;

	// If there was a match, v is one too high, otherwise it's 0.
	*rr += v > 0;
	return v - 1;
}

static int
scan_hex_escape(unsigned char **rr, unsigned char **ww, unsigned char *end)
{
	int acc;
	*rr += 2;
	int d = one_hex_digit(rr, end);
	if (d < 0)
		return d;
	acc = d;
	d = one_hex_digit(rr, end);
	if (d >= 0) {
		acc = 16 * acc + d;
	}
	if (acc > 0xFF)
		return -1;
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
scan_unicode_escape(unsigned char **rr, unsigned char **ww, unsigned char *end, int digits)
{
	unsigned int acc = 0;
	*rr += 2;
	if (end - *rr < digits)
		return -1;
	for (int i = 0; i < digits; i++) {
		int d = one_hex_digit(rr, end);
		if (d < 0)
			return d;
		acc = 16 * acc + d;
	}
	if (acc == 0)
		return -1;
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
		return -1; // reserved for utf16 surrogate halves
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
scan_backslash_escape(unsigned char **rr, unsigned char **ww, unsigned char *end)
{
	unsigned char *r = *rr;
	if (end - r < 2)
		return -50;
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
			return scan_octal_escape(rr, ww, end, r[1] - '0');
		case 'x':
			return scan_hex_escape(rr, ww, end);
		case 'u':
			return scan_unicode_escape(rr, ww, end, 4);
		case 'U':
			return scan_unicode_escape(rr, ww, end, 8);
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
scan_quoted(unsigned char *start, unsigned char *end, int quote, bool backslash_escapes, int *nwritten)
{
	if (start == end)
		return -30;
	unsigned char *last = end - 1;
	unsigned char *r = start + 1;
	unsigned char *w = start;

	while (r <= last) {
		assert(w <= r);
		if (*r == '\0')
			return -31;
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
			int ret = scan_backslash_escape(&r, &w, end);
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
	return -32;
}

// Scan the text pointed to by 'start', looking for occurrences of 'col_sep'
// or 'line_sep'. If found, replace it with a '\0' and return succesfully. .
// Reaching 'end' is also considered an error.
// If 'backslash_escapes' is set, backslashes suppress the special .
// Return the number of bytes scanned, including the separator.
// On succesful return, write the separator found to '*sep_found'.
static int
scan_unquoted(unsigned char *start, unsigned char *end, int col_sep, int line_sep, bool backslash_escapes, unsigned char *sep_found)
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
			*sep_found = *sep;
			*sep = 0;
			return sep - (char*)start + 1;
		} else {
			return -40;
		}
	}

	// go over it character by character and convert backslash escapes
	unsigned char *r = start;
	unsigned char *w = start;
	while (r < end) {
		if (*r == col_sep || *r == line_sep) {
			*sep_found = *r;
			*w = 0;
			return r - start + 1;
		}
		if (*r == '\\') {
			int ret = scan_backslash_escape(&r, &w, end);
			if (ret < 0)
				return ret;
		} else {
			*r++ = *w++;
		}
	}
	// no sep found is an error
	return -40;
}

// Scan the text pointed to by 'start' up to the first occurrence of either
// the column or the line separator, but never beyond 'end'.
// Reaching 'end' is considered an error.
// Remove quoting and process backslash escapes. Place a '\0' at the end of
// the field, overwriting the separator or end quote.
// Return the number of bytes scanned including the separator, or < 0 on
// error. Write the separator found to '*sep_found'.
static int
scan_field(unsigned char *start, unsigned char *end, int col_sep, int line_sep, int quote, bool backslash_escapes, unsigned char *sep_found)
{
	if (start == end)
		return -10;

	int nread;
	int nwritten;
	if (quote && *start == quote) {

		nread = scan_quoted(start, end, quote, backslash_escapes, &nwritten);
		if (nread < 0)
			return nread;
		// scan_quoted errors out if it reaches 'end' so we know 'start[n]' exists
		if (start[nread] == col_sep || start[nread] == line_sep) {
			*sep_found = start[nread];
			// nread should include the separator
			nread += 1;
		} else {
			// end quote must be followed by separator
			return -11;
		}
	} else {
		nread = scan_unquoted(start, end, col_sep, line_sep, backslash_escapes, sep_found);
		if (nread < 0)
			return nread;
		// with scan_unquoted, nread includes the separator, now replaced with '\0'
		nwritten = nread - 1;
	}

	if (backslash_escapes) {
		(void)nwritten;
		(void)start;
	}

	return nread;
}

// Scan the memory 'start' .. 'end' for fields, unquoting and unescaping.
// Place the index of each field in the appropriate index array.
// 'ncols' is the number of columns. 'columns' is a pointer to an array of
// size 'ncols' of pointers to the index arrays, each of which has room
// for 'nrows' field indices.
// Verify that column separators and row separators occur at the appropriate
// moment. Verify that exactly the right amount of data is offered.
// Return 0 on succes, < 0 on error.
// If a field contains the null_repr, replace its index with int_nil.
int
scan_fields(
	char *data_start, int skip_amount, char *data_end,
	int col_sep, int line_sep, int quote, bool backslash_escapes, char *null_repr,
	int ncols, int nrows, int **columns)
{
	if (ncols < 0 || nrows < 0)
		return -1;

	unsigned char *p = (unsigned char*)&data_start[skip_amount];
	unsigned char *end = (unsigned char*)data_end;
	int row = 0;
	int col = 0;
	while (p < end && row < nrows) {
		unsigned char sep = 0;
		int n = scan_field(p, end, col_sep, line_sep, quote, backslash_escapes, &sep);
		if (n < 0) {
			printf("ERROR row=%d col=%d\n", row, col);
			return n;
		}
		bool last_col = col == ncols - 1;
		bool ok;
		if (!last_col) {
			// must be col_sep
			ok = (sep == col_sep);
		} else {
			// either col_sep or col_sep followed by line_sep
			if (sep == line_sep) {
				ok = true;
			} else if (sep == col_sep && p + n < end && p[n] == line_sep) {
				n += 1;
				ok = true;
			} else {
				ok = false;
			}
		}
		if (!ok)
			return -2;
		bool is_null = (null_repr && strcasecmp((char*)p, null_repr) == 0);
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

	if (p < end) {
		// leftover data
		return -3;
	}
	if (row < nrows || col != 0) {
		// too few rows
		return -4;
	}

	return 0;
}

