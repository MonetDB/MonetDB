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
#include "mal.h"
#include "mal_errors.h"
#include "mal_client.h"
// #include "mal_instruction.h"
#include "mal_exception.h"
// #include "mal_interpreter.h"


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


static str
COPYfixlines(lng *ret_linecount, lng *ret_bytesmoved, bat *left_block, lng *left_skip_amount, bat *right_block, str *linesep_arg, str *quote_arg)
{
	str msg = MAL_SUCCEED;
	char linesep, quote;
	BAT *left = NULL, *right = NULL;
	int start, left_size, right_size;
	char *left_data, *right_data;
	int newline_count;
	int latest_newline;
	bool quoted;
	int borrow;


	if (strNil(*linesep_arg) || strlen(*linesep_arg) != 1 || strNil(*quote_arg) || strlen(*quote_arg) != 1)
		bailout("copy.fixlines", SQLSTATE(42000) "unsupported separator");
	linesep = **linesep_arg;
	quote = **quote_arg;

	if (is_bat_nil(*left_block) || is_bat_nil(*right_block) || is_lng_nil(*left_skip_amount))
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
	quoted = false;
	left_data = Tloc(left, 0);
	if (start < left_size) {
		newline_count = 0;
		latest_newline = start - 1;
		for (int i = start; i < left_size; i++) {
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

	if (latest_newline == left_size - 1) {
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


static str
COPYsplitlines(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;

	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;

	bailout("copy.splitlines", SQLSTATE(42000) "banana");
end:
	return msg;
}

static str
COPYparse_append(int *ret, int *mvc, str *s, str *t, str *c, oid *position, bat *positions, bat *block, bat *fields)
{
	str msg = MAL_SUCCEED;

	(void)ret;
	(void)mvc;
	(void)s;
	(void)t;
	(void)c;
	(void)position;
	(void)positions;
	(void)block;
	(void)fields;

	bailout("copy.parse_append", SQLSTATE(42000) "banana");
end:
	return msg;
}


#include "mel.h"
static mel_func copy_init_funcs[] = {
 command("copy", "read", COPYread, true, "Clear the BAT and read 'block_size' bytes into it from 's'",
	args(1, 4,
		arg("",lng),
		arg("stream", streams), arg("block_size", int), batarg("block", bte)
 )),
 command("copy", "fixlines", COPYfixlines, true, "Copy bytes from 'right' to 'left' to complete the final line of 'left'. Return left line count and bytes copied",
	args(2, 7,
	arg("linecount", lng), arg("bytesmoved", int),
	batarg("left",bte), arg("left_skip", int), batarg("right", bte), arg("linesep", str), arg("quote", str),
 )),
 pattern("copy", "splitlines", COPYsplitlines, false, "Find the fields of the individual columns", args(1,8,
	batvararg("", int),
	batarg("block", bte), arg("skip", int), arg("col_sep", str), arg("line_sep", str), arg("quote", str), arg("null_repr", str), arg("escape", bit)
 )),

 command("copy", "parse_append", COPYparse_append, true, "parse the fields and append them to the column",
 args(1, 9,
	arg("", int),
	arg("mvc", int), arg("s", str), arg("t", str), arg("c", str), arg ("position", oid), batarg("positions", oid), batarg("block", bte), batarg("fields", int)
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
