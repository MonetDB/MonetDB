/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "streams.h"
#include "mel.h"
#include "mal.h"
#include "mal_errors.h"
#include "mal_client.h"
// #include "mal_instruction.h"
#include "mal_exception.h"
#include "mal_interpreter.h"


#define bailout(f, ...) do { \
		msg = createException(SQL, f,  __VA_ARGS__); \
		goto end; \
	} while (0)


static str
COPYread(lng *ret_nread, Stream *stream_arg, lng *block_size_arg, bat *block_bat_arg)
{
	str msg = MAL_SUCCEED;
	stream *s = *stream_arg;
	lng block_size = *block_size_arg;
	bat b = *block_bat_arg;
	BAT *bat = NULL;
	lng nread;

	bat = BATdescriptor(b);
	if (bat->batRole != TRANSIENT) {
		bailout("copy.read", SQLSTATE(42000) "can only read into transient BAT");
	}
	BATclear(bat, true);

	if (BATcapacity(bat) < (BUN)block_size && BATextend(bat, block_size) != GDK_SUCCEED) {
		bailout("copy.read", "%s", GDK_EXCEPTION);
	}

	nread = mnstr_read(s, Tloc(bat, 0), 1, block_size);
	if (nread < 0) {
		bailout("copy.read", SQLSTATE(42000) "%s", mnstr_peek_error(s));
	}

	BATsetcount(bat, nread);
	*ret_nread = nread;
end:
	if (bat != NULL)
		BBPunfix(bat->batCacheid);
	return msg;
}

static int
get_sep_char(str sep, bool backslash_escapes)
{
	if (strNil(sep))
		return 0;
	int c = *sep;

	if (strlen(sep) != 1)
		return -1;

	switch (c) {
		case '\\':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
		case 'a':
		case 'b':
		case 'c':
		case 'd':
		case 'e':
		case 'f':
		case 'x':
		case 'u':
		case 'U':
		case 'n':
		case 'r':
		case 't':
			if (backslash_escapes)
				return -1;
			break;
		default:
			break;
	}
	return c;
}

static str
COPYfixlines(lng *ret_linecount, lng *ret_bytesmoved, bat *left_block, lng *left_skip_amount, bat *right_block, str *linesep_arg, str *quote_arg, bit *escape)
{
	str msg = MAL_SUCCEED;
	int linesep, quote;
	bool backslash_escapes;
	BAT *left = NULL, *right = NULL;
	int start, left_size, right_size;
	char *left_data, *right_data;
	int newline_count;
	int latest_newline;
	bool escape_pending;
	bool quoted;
	int borrow;

	backslash_escapes = *escape;
	linesep = get_sep_char(*linesep_arg, backslash_escapes);
	if (linesep <= 0) // 0 not ok
		bailout("copy.fixlines", SQLSTATE(42000) "invalid line separator");
	quote = get_sep_char(*quote_arg, backslash_escapes);
	if (quote < 0) // 0 is ok
		bailout("copy.fixlines", SQLSTATE(42000) "invalid quote character");
	if (linesep == quote)
		bailout("copy.fixlines", SQLSTATE(42000) "line separator and quote character cannot be the same");

	if (is_bat_nil(*left_block) || is_bat_nil(*right_block) || is_lng_nil(*left_skip_amount) || is_bit_nil(*escape))
		bailout("copy.fixlines", "arguments must not be nil");

	if ((left = BATdescriptor(*left_block)) == NULL || (right = BATdescriptor(*right_block)) == NULL)
		bailout("copy.fixlines", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (BATcount(left) > (BUN)INT_MAX || BATcount(right) > (BUN)INT_MAX)
		bailout("copy.fixlines", SQLSTATE(42000) "block size too large");
	if (BATcount(left) < (BUN)*left_skip_amount || *left_skip_amount > (lng)INT_MAX)
		bailout("copy.fixlines", SQLSTATE(42000) "skip amount out of bounds");

	// Scan 'left' for unquoted newlines. Determine both the count and the position
	// of the last occurrence.
	start = (int)*left_skip_amount;
	left_size = (int)BATcount(left);
	escape_pending = false;
	quoted = false;
	left_data = Tloc(left, 0);
	if (start < left_size) {
		newline_count = 0;
		latest_newline = start - 1;
		for (int i = start; i < left_size; i++) {
			if (escape_pending) {
				escape_pending = false;
				continue;
			}
			if (backslash_escapes && left_data[i] == '\\') {
				escape_pending = true;
				continue;
			}
			bool is_quote = left_data[i] == quote;
			quoted ^= is_quote;
			if (!quoted && left_data[i] == linesep) {
				latest_newline = i;
				newline_count++;
			}
		}
	} else {
		// start == left_size means left block is empty, nothing to do
		*ret_linecount = 0;
		*ret_bytesmoved = 0;
		msg = MAL_SUCCEED;
		goto end;
	}

	if (!escape_pending && !quoted && latest_newline == left_size - 1) {
		// Block ends in a newline, nothing more to do
		*ret_linecount = newline_count;
		*ret_bytesmoved = 0;
		msg = MAL_SUCCEED;
		goto end;
	}

	// We have to borrow some data from the next block to complete the final line
	right_size = BATcount(right);
	borrow = -1;
	right_data = Tloc(right, 0);
	for (int i = 0; i < right_size; i++) {
		if (escape_pending) {
			escape_pending = false;
			continue;
		}
		if (backslash_escapes && right_data[i] == '\\') {
			escape_pending = true;
			continue;
		}
		bool is_quote = right_data[i] == quote;
		quoted ^= is_quote;
		if (!quoted && right_data[i] == linesep) {
			borrow = i + 1;
			break;
		}
	}
	if (borrow == -1)
		bailout("copy.fixlines", SQLSTATE(42000) "line too long");

	if (BATextend(left, left_size + borrow) != GDK_SUCCEED) {
		bailout("copy.fixlines", GDK_EXCEPTION);
	}

	memcpy(Tloc(left, left_size), right_data, borrow);
	BATsetcount(left, left_size + borrow);

	assert(*(char*)Tloc(left, BATcount(left) - 1) == linesep);

	*ret_linecount = newline_count + 1;
	*ret_bytesmoved = borrow;
	msg = MAL_SUCCEED;

end:
	if (left != NULL)
		BBPunfix(left->batCacheid);
	if (right != NULL)
		BBPunfix(right->batCacheid);
	return msg;
}

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
static int
scan_fields(
	char *start, int len,
	int col_sep, int line_sep, int quote, bool backslash_escapes, char *null_repr,
	int ncols, int nrows, int **columns)
{
	if (ncols < 0 || nrows < 0)
		return -1;

	unsigned char *p = (unsigned char*)start;
	unsigned char *end = p + len;
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
		int expected_sep = last_col ? line_sep : col_sep;
		if (sep != expected_sep)
			return -2;
		bool is_null = (null_repr && strcasecmp((char*)p, null_repr) == 0);
		int field = is_null ? int_nil : ((char*)p - start);
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

static str
COPYsplitlines(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	str msg = MAL_SUCCEED;
	BAT *block_bat = NULL;
	BAT **return_bats = NULL;
	int **return_indices = NULL;
	int ncols = pci->retc;
	int line_sep, col_sep, quote;
	char *data;
	int len;

	bat block_bat_id = *getArgReference_bat(stk, pci, pci->retc + 0);
	int skip_amount = *getArgReference_int(stk, pci, pci->retc + 1);
	lng line_count = *getArgReference_lng(stk, pci, pci->retc + 2);
	str col_sep_str = *getArgReference_str(stk, pci, pci->retc + 3);
	str line_sep_str = *getArgReference_str(stk, pci, pci->retc + 4);
	str quote_str = *getArgReference_str(stk, pci, pci->retc + 5);
	str null_repr = *getArgReference_str(stk, pci, pci->retc + 6);
	bool backslash_escapes = *getArgReference_bit(stk, pci, pci->retc + 7);

	line_sep = get_sep_char(line_sep_str, backslash_escapes);
	if (line_sep <= 0) // 0 not ok
		bailout("copy.splitlines", SQLSTATE(42000) "invalid line separator");
	col_sep = get_sep_char(col_sep_str, backslash_escapes);
	if (col_sep <= 0) // 0 is not ok
		bailout("copy.splitlines", SQLSTATE(42000) "invalid column separator");
	quote = get_sep_char(quote_str, backslash_escapes);
	if (quote < 0) // 0 is ok
		bailout("copy.splitlines", SQLSTATE(42000) "invalid quote character");

	if (strNil(null_repr))
		null_repr = NULL;

	if ((block_bat = BATdescriptor(block_bat_id)) == NULL)
		bailout("copy.splitlines", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	assert(skip_amount >= 0);
	assert( (line_count == 0) == (skip_amount == (int)BATcount(block_bat)) );

	return_bats = GDKzalloc(ncols * sizeof(*return_bats));
	return_indices = GDKzalloc(ncols * sizeof(*return_indices));
	if (!return_bats || !return_indices)
		bailout("copy.splitlines",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (int i = 0; i < ncols; i++) {
		BAT *b = COLnew(0, TYPE_int, line_count, TRANSIENT);
		if (b == NULL)
			bailout("copy.splitlines", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return_bats[i] = b;
		return_indices[i] = Tloc(b, 0);
	}

	data = Tloc(block_bat, skip_amount);
	len = (int)BATcount(block_bat) - skip_amount;
	int ret;
	ret = scan_fields(
		data, len,
		col_sep, line_sep, quote, backslash_escapes, null_repr,
		ncols, line_count,
		return_indices);
	if (ret < 0)
		bailout("copy.splitlines", "field splitting failed");

end:
	if (block_bat)
		BBPunfix(block_bat->batCacheid);
	if (return_bats) {
		for (int i = 0; i < ncols; i++) {
			if (return_bats[i]) {
				BAT *b = return_bats[i];
				bat id = b->batCacheid;
				if (msg == MAL_SUCCEED) {
					BATcount(b) = line_count;
					BBPkeepref(id);
					*getArgReference_bat(stk, pci, i) = id;
				}
				else {
					BBPunfix(id);
				}
			}
		}
		GDKfree(return_bats);
	}
	GDKfree(return_indices);
	return msg;
}

static str
COPYparse_generic(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;
	BAT *ret = NULL;
	BAT *block = BATdescriptor(*getArgReference_bat(stk, pci, 1));
	BAT *indices = BATdescriptor(*getArgReference_bat(stk, pci, 2));
	int tpe = getArgGDKType(mb, pci, 3);
	int n;

	if (block == NULL || indices == NULL)
		bailout("copy.parse_generic", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(indices);

	ret = COLnew(0, tpe, n, TRANSIENT);
	if (!ret)
		bailout("copy.parse_generic",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	const void *nil_ptr; nil_ptr = ATOMnilptr(tpe);
	size_t nil_len; nil_len = ATOMlen(tpe, ATOMnilptr(tpe));

	void *buffer; buffer = NULL;
	size_t buffer_len; buffer_len = 0;
	for (int i = 0; i < n; i++) {
		int offset = *(int*)Tloc(indices, i);
		const char *src = Tloc(block, offset);
		const void *p;
		ssize_t len;
		if (is_int_nil(offset)) {
			p = nil_ptr;
			len = nil_len;
		} else {
			len = BATatoms[tpe].atomFromStr(src, &buffer_len, &buffer, false);
			p = buffer;
			if (len < 0)
				bailout("copy.parse_generic", SQLSTATE(42000)"Conversion failed for value '%s'", src);
		}
		if (bunfastapp(ret, p) != GDK_SUCCEED)
			bailout("copy.parse_generic", GDK_EXCEPTION);
	}
	GDKfree(buffer);
	BATsetcount(ret, n);
end:
	if (ret) {
		if (msg == MAL_SUCCEED) {
			*getArgReference_bat(stk, pci, 0) = ret->batCacheid;
			BBPkeepref(ret->batCacheid);
		}
		else
			BBPunfix(ret->batCacheid);
	}
	if (block)
		BBPunfix(block->batCacheid);
	if (indices)
		BBPunfix(indices->batCacheid);
	return msg;
}

static mel_func copy_init_funcs[] = {
 command("copy", "read", COPYread, true, "Clear the BAT and read 'block_size' bytes into it from 's'",
	args(1, 4,
		arg("",lng),
		arg("stream", streams), arg("block_size", lng), batarg("block", bte)
 )),
 command("copy", "fixlines", COPYfixlines, true, "Copy bytes from 'right' to 'left' to complete the final line of 'left'. Return left line count and bytes copied",
	args(2, 8,
	arg("linecount", lng), arg("bytesmoved", int),
	batarg("left",bte), arg("left_skip", int), batarg("right", bte), arg("linesep", str), arg("quote", str), arg("escape", bit)
 )),
 pattern("copy", "splitlines", COPYsplitlines, false, "Find the fields of the individual columns", args(1, 9,
	batvararg("", int),
	batarg("block", bte), arg("skip", int), arg("linecount", lng), arg("col_sep", str), arg("line_sep", str), arg("quote", str), arg("null_repr", str), arg("escape", bit)
 )),

 pattern("copy", "parse_generic", COPYparse_generic, false, "Parse as an integer", args(1, 4,
     batargany("", 1),
	 batarg("block", bte), batarg("offsets", int), argany("type", 1)
 )),


 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_copy_mal)
{
	mal_module("copy", NULL, copy_init_funcs);
}
