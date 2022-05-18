/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "streams.h"
#include "mel.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#include "copy.h"
#include "rel_copy.h"

// #define BLOCK_DEBUG

static str
COPYread(Stream *stream_out_arg, Stream *stream_in_arg, lng *block_size_arg, bat *block_bat_arg)
{
	str msg = MAL_SUCCEED;
	stream *s = *stream_in_arg;
	lng block_size = *block_size_arg;
	bat b = *block_bat_arg;
	BUN start;
	BUN newcap;
	BAT *bat = NULL;
	lng nread;

	if (s == NULL)
		goto end;

	bat = BATdescriptor(b);
	if (!bat)
		bailout("copy.read", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (bat->batRole != TRANSIENT) {
		bailout("copy.read", SQLSTATE(42000) "can only read into transient BAT");
	}

	newcap = BATcount(bat) + block_size;
	if (BATcapacity(bat) < newcap && BATextend(bat, newcap) != GDK_SUCCEED) {
		bailout("copy.read", "%s", GDK_EXCEPTION);
	}

	start = BATcount(bat);
	nread = mnstr_read(s, Tloc(bat, start), 1, block_size);
	if (nread < 0) {
		bailout("copy.read", SQLSTATE(42000) "%s", mnstr_peek_error(s));
	}

	BATsetcount(bat, start + nread);
	bat->batInserted = 0;
	// it would be very surprising if the bytes we just read were ordered or unique!
	bat->tkey = false;
	bat->tnonil = false;
	bat->tnil = false;
	bat->tsorted = false;
	bat->trevsorted = false;

	if (nread == 0){
		mnstr_close(s);
		s = NULL;
	}
#ifdef BLOCK_DEBUG
	dump_block("just read", bat);
#endif
end:
	if (msg == MAL_SUCCEED) {
		*stream_out_arg = s;
	}
	if (bat != NULL)
		BBPunfix(bat->batCacheid);
	return msg;
}

static str
COPYskiplines(lng *toskip_out, bat *block_bat, lng *toskip_in)
{
	str msg = MAL_SUCCEED;
	lng toskip = *toskip_in;
	BAT *block = NULL;
	char *start, *pos, *end;

	if (!toskip)
		goto end;

	block = BATdescriptor(*block_bat);
	if (!block)
		bailout("copy.skiplines", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	start = Tloc(block, 0);
	pos = start;
	end = Tloc(block, BATcount(block));
	while (toskip && pos < end) {
		char *p = memchr(pos, '\n', end - pos);
		if (!p) {
			// discard everything but do not decrement toskip
			pos = end;
			break;
		}
		pos = p + 1;
		toskip--;
	}

	if (pos > start) {
		size_t n = end - pos;
		memmove(start, pos, n);
		BATsetcount(block, n);
	}

	end:
	*toskip_out = toskip;
	if (block) {
		BBPunfix(block->batCacheid);
	}
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
COPYfixlines(
	  bat *ret_left, bat *ret_right, lng *ret_linecount,
	  bat *left_block, bat *right_block, str *linesep_arg, str *quote_arg, bit *escape)
{
	str msg = MAL_SUCCEED;
	int linesep, quote;
	bool backslash_escapes;
	BAT *left = NULL, *right = NULL;
	BAT *new_left = NULL, *new_right = NULL;
	int start, left_size, right_size;
	char *left_data, *right_data;
	int newline_count = 0;
	int latest_newline;
	bool escape_pending;
	bool quoted;
	int borrow = 0;

	backslash_escapes = *escape;
	linesep = get_sep_char(*linesep_arg, backslash_escapes);
	if (linesep <= 0) // 0 not ok
		bailout("copy.fixlines", SQLSTATE(42000) "invalid line separator");
	quote = get_sep_char(*quote_arg, backslash_escapes);
	if (quote < 0) // 0 is ok
		bailout("copy.fixlines", SQLSTATE(42000) "invalid quote character");
	if (linesep == quote)
		bailout("copy.fixlines", SQLSTATE(42000) "line separator and quote character cannot be the same");

	if (is_bat_nil(*left_block) || is_bat_nil(*right_block) || is_bit_nil(*escape))
		bailout("copy.fixlines", "arguments must not be nil");

	if ((left = BATdescriptor(*left_block)) == NULL || (right = BATdescriptor(*right_block)) == NULL)
		bailout("copy.fixlines", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (BATcount(left) > (BUN)INT_MAX || BATcount(right) > (BUN)INT_MAX)
		bailout("copy.fixlines", SQLSTATE(42000) "block size too large");

	if (BATcount(left) < left->batInserted)
		bailout("copy.fixlines", SQLSTATE(42000) "skip amount out of bounds");
	if (right->batInserted != 0)
		bailout("copy.fixlines", SQLSTATE(42000) "right hand skip amount expected to be zero, not " BUNFMT, right->batInserted);
	if (left->hseqbase != 0 || right->hseqbase != 0)
		bailout("copy.fixlines", SQLSTATE(42000) "hseqbases must be 0");

#ifdef BLOCK_DEBUG
	dump_block("fixlines incoming left", left);
	dump_block("fixlines incoming right", right);
#endif

	// Scan 'left' for unquoted newlines. Determine both the count and the position
	// of the last occurrence.
	start = (int)left->batInserted;
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
		new_left = left;
		new_right = right;
		*ret_linecount = 0;
		msg = MAL_SUCCEED;
		goto end;
	}

	if (!escape_pending && !quoted && latest_newline == left_size - 1) {
		// Block ends in a newline, nothing more to do
		new_left = left;
		new_right = right;
		*ret_linecount = newline_count;
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

	if (borrow >= 0) {
		// Move some bytes from 'right' to 'left'
		if (BATextend(left, (BUN)left_size + (BUN)borrow) != GDK_SUCCEED) {
			bailout("copy.fixlines", GDK_EXCEPTION);
		}
		memcpy(Tloc(left, left_size), right_data, borrow);
		BATsetcount(left, (BUN)left_size + (BUN)borrow);
		right->batInserted = borrow;
		new_left = left;
		new_right = right;
		*ret_linecount = newline_count + 1;
	} else {
		// the last line of 'left' is so long it extends all the way through 'right'
		// into the next block. Our invariant is that 'new_left' must start and end
		// on line boundaries and 'new_right' must start on a line boundary.
		// The best way to do so is by appending all of 'right' to 'left' and
		// returning the resulting jumbo block as 'new_right', with an empty
		// block as 'new_left'.
		BUN new_size = (BUN)left_size + (BUN)right_size;
		if (new_size >= MAX_LINE_LENGTH)
			bailout("copy.fixlines", SQLSTATE(42000) "line too long: " BUNFMT ", limit set to " BUNFMT, new_size, (BUN)MAX_LINE_LENGTH);
		if (BATextend(left, new_size) != GDK_SUCCEED) {
			bailout("copy.fixlines", GDK_EXCEPTION);
		}
		memcpy(Tloc(left, left_size), right_data, right_size);
		BATsetcount(left, (BUN)left_size + (BUN)right_size);
		BATsetcount(right, 0);
		assert(right->batInserted == 0);
		// notice how 'left' and 'right' cross over:
		new_left = right;
		new_right = left;
		*ret_linecount = 0;
	}
	msg = MAL_SUCCEED;

end:

#ifdef BLOCK_DEBUG
	fprintf(stderr, "fixlines returning %ld lines\n", *ret_linecount);
	if (new_left)
		dump_block("fixlines outgoing new_left", new_left);
	if (new_right)
		dump_block("fixlines outgoing new_right", new_right);
	if (left || right)
		fprintf(stderr, "\n");
#endif

	if (left != NULL)
		BBPunfix(left->batCacheid);
	if (right != NULL)
		BBPunfix(right->batCacheid);
	// new_left and new_right are aliases of left and right, but not necessarily in that order.
	if (new_left != NULL) {
		*ret_left = new_left->batCacheid;
		BBPretain(new_left->batCacheid);
	}
	if (new_right != NULL) {
		*ret_right = new_right->batCacheid;
		BBPretain(new_right->batCacheid);
	}
	return msg;
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

	assert(pci->argc == pci->retc + 7);
	bat block_bat_id = *getArgReference_bat(stk, pci, pci->retc + 0);
	lng line_count = *getArgReference_lng(stk, pci, pci->retc + 1);
	str col_sep_str = *getArgReference_str(stk, pci, pci->retc + 2);
	str line_sep_str = *getArgReference_str(stk, pci, pci->retc + 3);
	str quote_str = *getArgReference_str(stk, pci, pci->retc + 4);
	str null_repr = *getArgReference_str(stk, pci, pci->retc + 5);
	bool backslash_escapes = *getArgReference_bit(stk, pci, pci->retc + 6);


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

	assert( (line_count == 0) == (BATcount(block_bat) == block_bat->batInserted) );

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

#ifdef BLOCK_DEBUG
	dump_block("splitlines", block_bat);
#endif
	int ret;
	ret = scan_fields(
		Tloc(block_bat, 0), (int)block_bat->batInserted, Tloc(block_bat, BATcount(block_bat)),
		col_sep, line_sep, quote, backslash_escapes, null_repr,
		ncols, line_count,
		return_indices);
	if (ret < 0)
		bailout("copy.splitlines", "field splitting failed: %d", ret);

end:
	if (block_bat)
		BBPunfix(block_bat->batCacheid);
	if (return_bats) {
		for (int i = 0; i < ncols; i++) {
			if (return_bats[i]) {
				BAT *b = return_bats[i];
				bat id = b->batCacheid;
				if (msg == MAL_SUCCEED) {
					BATsetcount(b, line_count);
					b->tkey = false;
					b->tnil = false;
					b->tnonil = false;
					b->tsorted = false;
					b->trevsorted = false;
					BBPkeepref(b);
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
COPYtrackrowids(lng *start, bat *newrows_out_id, bat *newrows_in_id, lng *count, oid *offset, bat *pos_bat_id)
{
	str msg = MAL_SUCCEED;
	BAT *newrows = NULL;
	BAT *pos = NULL;

	newrows = BATdescriptor(*newrows_in_id);
	if (!newrows)
		bailout("copy.trackrowids", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	*start = BATcount(newrows);
	*newrows_out_id = *newrows_in_id;
	BBPretain(*newrows_out_id);

	if (*count == 0)
		goto end;

	if (is_bat_nil(*pos_bat_id)) {
		if (BATextend(newrows, *start + *count) != GDK_SUCCEED)
			bailout("copy.trackrowid", GDK_EXCEPTION);
		oid *p = (oid*)Tloc(newrows, *start);
		for (int i = 0; i < *count; i++) {
			*p++ = *offset + i;
		}
		BATsetcount(newrows, *start + *count);
	} else {
		pos = BATdescriptor(*pos_bat_id);
		if (!pos)
			bailout("copy.trackrowids", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		if (BATcount(pos) != (BUN)*count)
			bailout("copy.trackrowids", SQLSTATE(42000) "expected " BUNFMT " row ids, got %" PRId64, *count, BATcount(pos));
		if (BUNappendmulti(newrows, Tloc(pos, 0), (BUN)*count, false) != GDK_SUCCEED)
			bailout("copy.trackrowids", GDK_EXCEPTION);
	}
	newrows->trevsorted = false;
	newrows->tseqbase = oid_nil;
end:
	if (newrows)
		BBPunfix(*newrows_in_id);
	if (pos)
		BBPunfix(*pos_bat_id);
	return msg;
}



static mel_func copy_init_funcs[] = {
 command("copy", "read", COPYread, true, "Read 'block_size' bytes into 'block' from 's'",
	args(1, 4,
		arg("",streams),
		arg("stream", streams), arg("block_size", lng), batarg("block", bte)
 )),
 command("copy", "skiplines", COPYskiplines, true, "Skip the first N lines in the buffer", args(1, 3,
	arg("", lng),
	batarg("block", bte),arg("toskip", lng)
 )),
 command("copy", "fixlines", COPYfixlines, true, "Copy bytes from 'right' to 'left' to complete the final line of 'left'. Return left line count and bytes copied",
	args(3, 8,
	batarg("new_left", bte), batarg("new_right", bte), arg("linecount", lng),
	batarg("left", bte), batarg("right", bte), arg("linesep", str), arg("quote", str), arg("escape", bit),
 )),
 pattern("copy", "splitlines", COPYsplitlines, false, "Find the fields of the individual columns", args(1, 8,
	batvararg("", int),
	batarg("block", bte), arg("linecount", lng), arg("col_sep", str), arg("line_sep", str), arg("quote", str), arg("null_repr", str), arg("escape", bit)
 )),

 command("copy", "trackrowids", COPYtrackrowids, true, "keep track of newly claimed rows", args(2, 6,
	arg("", lng), batarg("", oid),
	batarg("newrows", oid), arg("count", lng), arg("offset",oid), batarg("pos",oid)
 )),

 pattern("copy", "parse_generic", COPYparse_generic, false, "Parse using GDK's atomFromStr", args(1, 4,
	batargany("", 1),
	batarg("block", bte), batarg("offsets", int), argany("type", 1)
 )),

 command("copy", "parse_decimal", COPYparse_decimal_bte, false, "Parse as a decimal", args(1, 6,
	 batarg("", bte),
	 batarg("block", bte), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", bte)
 )),
 command("copy", "parse_decimal", COPYparse_decimal_sht, false, "Parse as a decimal", args(1, 6,
	 batarg("", sht),
	 batarg("block", bte), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", sht)
 )),
 command("copy", "parse_decimal", COPYparse_decimal_int, false, "Parse as a decimal", args(1, 6,
	 batarg("", int),
	 batarg("block", bte), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", int)
 )),
 command("copy", "parse_decimal", COPYparse_decimal_lng, false, "Parse as a decimal", args(1, 6,
	 batarg("", lng),
	 batarg("block", bte), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", lng)
 )),
 #ifdef HAVE_HGE
 command("copy", "parse_decimal", COPYparse_decimal_hge, false, "Parse as a decimal", args(1, 6,
	 batarg("", hge),
	 batarg("block", bte), batarg("offsets", int), arg("digits", int), arg("scale", int), arg("type", hge)
 )),
#endif

 command("copy", "parse_integer", COPYparse_integer_bte, false, "Parse as an integer", args(1, 4,
	 batarg("", bte),
	 batarg("block", bte), batarg("offsets", int), arg("type", bte)
 )),
 command("copy", "parse_integer", COPYparse_integer_sht, false, "Parse as an integer", args(1, 4,
	 batarg("", sht),
	 batarg("block", bte), batarg("offsets", int), arg("type", sht)
 )),
 command("copy", "parse_integer", COPYparse_integer_int, false, "Parse as an integer", args(1, 4,
	 batarg("", int),
	 batarg("block", bte), batarg("offsets", int), arg("type", int)
 )),
 command("copy", "parse_integer", COPYparse_integer_lng, false, "Parse as an integer", args(1, 4,
	 batarg("", lng),
	 batarg("block", bte), batarg("offsets", int), arg("type", lng)
 )),
 #ifdef HAVE_HGE
 command("copy", "parse_integer", COPYparse_integer_hge, false, "Parse as an integer", args(1, 4,
	 batarg("", hge),
	 batarg("block", bte), batarg("offsets", int), arg("type", hge)
 )),
#endif

 command("copy", "set_blocksize", COPYset_blocksize, true, "set the COPY block size", args(1, 2,
	arg("", int),
	arg("blocksize", int)
 )),
 command("copy", "get_blocksize", COPYget_blocksize, true, "get the COPY block size", args(1, 1,
	arg("", int)
 )),

 command("copy", "set_parallel", COPYset_parallel, true, "switch between different COPY INTO implementations", args(1, 2,
	arg("", bit), arg("level", int)
 )),
 command("copy", "get_parallel", COPYget_parallel, true, "get the id of the current COPY INTO implementation", args(1, 1,
	arg("", int)
 )),


 // for testing
 command("copy", "str2buf", COPYstr2buf, false, "turn str into bat[:bte]", args(1, 2,
	batarg("buf", bte),
	arg("", str),
 )),
 command("copy", "buf2str", COPYbuf2str, false, "turn bat[:bte] into str", args(1, 2,
	arg("str", str),
	batarg("", bte),
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
