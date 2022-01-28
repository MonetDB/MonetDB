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
		msg = createException(SQL, f, SQLSTATE(42000) __VA_ARGS__); \
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
		bailout("copy.read", "can only read into transient BAT");
	}
	BATclear(bat, true);

	if (BATcapacity(bat) < (BUN)block_size && BATextend(bat, block_size) != GDK_SUCCEED) {
		bailout("copy.read", "%s", GDK_EXCEPTION);
	}

	nread = mnstr_read(s, Tloc(bat, 0), 1, block_size);
	if (nread < 0) {
		bailout("copy.read", "%s", mnstr_peek_error(s));
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

	(void)ret_linecount;
	(void)ret_bytesmoved;
	(void)left_block;
	(void)left_skip_amount;
	(void)right_block;
	(void)linesep_arg;
	(void)quote_arg;

	bailout("copy.fixlines", "banana");
end:
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

	bailout("copy.splitlines", "banana");
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

	bailout("copy.parse_append", "banana");
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
