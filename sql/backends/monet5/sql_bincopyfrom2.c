/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * Implementation of COPY BINARY INTO
 */

#include "monetdb_config.h"
#include "mapi_prompt.h"
#include "sql.h"
#include "mal_backend.h"
#include "mal_interpreter.h"
#include "copybinary.h"

#define bailout(...) do { \
		msg = createException(MAL, "sql.importColumn", SQLSTATE(42000) __VA_ARGS__); \
		goto end; \
	} while (0)


static struct type_rec {
	char *method;
	int gdk_type;
	str (*loader)(BAT *bat, stream *s);
} type_recs[] = {
	{
		.method = "sht",
		.gdk_type = TYPE_sht,
	},
};


static struct type_rec*
find_type_rec(str name)
{
	struct type_rec *end = (struct type_rec*)((char *)type_recs + sizeof(type_recs));
	for (struct type_rec *t = &type_recs[0]; t < end; t++)
		if (strcmp(t->method, name) == 0)
			return t;
	return NULL;
}


static str
load_column(struct type_rec *rec, BAT *bat, stream *s, int *eof_reached)
{
	str msg = MAL_SUCCEED;
	(void)rec;
	(void)bat;
	(void)s;
	(void)eof_reached;

	*eof_reached = 0;
	bailout("load_column not implemented yet");

	end:
		return msg;
}


static str
start_mapi_file_upload(backend *be, str path, stream **s)
{
	str msg = MAL_SUCCEED;
	*s = NULL;

	stream *ws = be->mvc->scanner.ws;
	bstream *bs = be->mvc->scanner.rs;
	stream *rs = bs->s;
	assert(isa_block_stream(ws));
	assert(isa_block_stream(rs));

	mnstr_write(ws, PROMPT3, sizeof(PROMPT3)-1, 1);
	mnstr_printf(ws, "rb %s\n", path);
	mnstr_flush(ws, MNSTR_FLUSH_DATA);
	while (!bs->eof)
		bstream_next(bs);
	char buf[80];
	if (mnstr_readline(rs, buf, sizeof(buf)) > 1) {
		msg = createException(IO, "sql.attach", "%s", buf);
		goto end;
	}
	set_prompting(rs, PROMPT2, ws);

	*s = rs;
end:
	return msg;
}


static str
finish_mapi_file_upload(backend *be, bool eof_reached)
{
	str msg = MAL_SUCCEED;
	stream *ws = be->mvc->scanner.ws;
	bstream *bs = be->mvc->scanner.rs;
	stream *rs = bs->s;
	assert(isa_block_stream(ws));
	assert(isa_block_stream(rs));

	set_prompting(rs, NULL, NULL);
	if (!eof_reached) {
		// Probably due to an error. Read until message boundary.
		char buf[8190];
		while (1) {
			ssize_t nread = mnstr_read(rs, buf, 1, sizeof(buf));
			if (nread > 0)
				continue;
			if (nread < 0)
				msg = createException(
					SQL, "mvc_bin_import_table_wrap",
					SQLSTATE(42000) "while syncing read stream: %s", mnstr_peek_error(rs));
			break;
		}
	}
	mnstr_write(ws, PROMPT3, sizeof(PROMPT3)-1, 1);
	mnstr_flush(ws, MNSTR_FLUSH_DATA);

	return msg;
}



/* Import a single file into a new BAT.
 */
static str
importColumn(backend *be, bat *ret, lng *retcnt, const str *method, const str *path, const int *onclient, const lng *nrows)
{
	// In this function we create the BAT and open the file, and tidy
	// up when things go wrong. The actual work happens in load_column().

	// These are managed by the end: block.
	str msg = MAL_SUCCEED;
	BAT *bat = NULL;
	stream *stream_to_close = NULL;
	int eof_reached = -1; // 1 = read to the end; 0 = stopped reading early; -1 = unset, a bug.

	// This one is not managed by the end: block
	stream *s;

	// Set safe values
	*ret = 0;
	*retcnt = 0;

	// Figure out how to load it
	struct type_rec *rec = find_type_rec(*method);
	if (rec == NULL)
		bailout("COPY BINARY FROM not implemented for '%s'", *method);

	// Create the BAT
	bat = COLnew(0, rec->gdk_type, *nrows, TRANSIENT);
	if (bat == NULL)
		bailout("%s", GDK_EXCEPTION);

	// Open the input stream
	if (onclient) {
		s = NULL;
		msg = start_mapi_file_upload(be, *path, &s);
		if (msg != MAL_SUCCEED)
			goto end;
	} else {
		s = stream_to_close = open_rstream(*path);
		if (s == NULL)
			bailout("Couldn't open '%s' on server: %s", *path, mnstr_peek_error(NULL));
	}

	// Do the work
	msg = load_column(rec, bat, s, &eof_reached);
	if (eof_reached != 0 && eof_reached != 1)
		bailout("internal error in sql.importColumn: eof_reached not set (%s)", *method);

	// Fall through into the end block which will clean things up
end:
	if (onclient) {
		str msg1 = finish_mapi_file_upload(be, eof_reached == 1);
		if (msg == MAL_SUCCEED)
			msg = msg1;
	}

	if (stream_to_close)
		close_stream(stream_to_close);

	// Manage the return values and `bat`.
	if (msg == MAL_SUCCEED) {
		BBPkeepref(bat->batCacheid); // should I call this?
		*ret = bat->batCacheid;
		*retcnt = BATcount(bat);
	} else {
		if (bat != NULL) {
			BBPunfix(bat->batCacheid);
			bat = NULL;
		}
		*ret = 0;
		*retcnt = 0;
	}

	return msg;
}


str
mvc_bin_import_column_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)importColumn;
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;

	assert(pci->retc == 2);
	assert(pci->argc == 6);
	bat* ret = getArgReference_bat(stk, pci, 0);
	lng* retcnt = getArgReference_lng(stk, pci, 1);
	str *method = getArgReference_str(stk, pci, 2);
	str *path = getArgReference_str(stk, pci, 3);
	int *onclient = getArgReference_int(stk, pci, 4);
	lng *nrows = getArgReference_lng(stk, pci, 5);

	backend *be = cntxt->sqlcontext;

	return importColumn(be, ret, retcnt, method, path, onclient, nrows);
}
