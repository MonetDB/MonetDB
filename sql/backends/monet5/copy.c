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
// #include "mal_client.h"
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


#include "mel.h"
static mel_func copy_init_funcs[] = {
 command("copy", "read", COPYread, true, "Clear the BAT and read 'block_size' bytes into it from 's'",
	args(1, 4,
		arg("",lng),
		arg("stream", streams), arg("block_size", lng), batarg("block", bte)
 )),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_json_mal)
{ mal_module("copy", NULL, copy_init_funcs); }
