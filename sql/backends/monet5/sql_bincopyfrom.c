/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * Implementation of COPY BINARY INTO
 */

#include "monetdb_config.h"
#include "mapi_prompt.h"
#include "sql.h"
#include "mal_backend.h"
#include "mal_interpreter.h"
#include "sql_bincopyconvert.h"
#include "copybinary.h"
#include "copybinary_support.h"


static str
load_trivial(BAT *bat, stream *s, BUN rows_estimate, int *eof_seen)
{
	str msg = MAL_SUCCEED;
	int tt = BATttype(bat);
	const size_t asz = (size_t) ATOMsize(tt);
	const size_t chunk_size = 1<<20;

	bool eof = false;
	while (!eof) {
		assert(chunk_size % asz == 0);
		size_t n;
		if (rows_estimate > 0) {
			// Set n to estimate+1 so it will read once, get n - 1 and know it's at EOF.
			// Otherwise, it will read n, get n, then enlarge the heap, read again,
			// and only then know it's at eof.
			n = rows_estimate + 1;
			rows_estimate = 0;
		} else {
			n = chunk_size / asz;
		}

		// First make some room
		BUN validCount = bat->batCount;
		BUN newCount = validCount + n;
		if (BATextend(bat, newCount) != GDK_SUCCEED)
			bailout("load_trivial: %s", GDK_EXCEPTION);

		// Read into the newly allocated space
		char *start = Tloc(bat, validCount);
		char *cur = start;
		char *end = Tloc(bat, newCount);
		while (cur < end) {
			ssize_t nread = mnstr_read(s, cur, 1, end - cur);
			if (nread < 0)
				bailout("load_trivial: %s", mnstr_peek_error(s));
			if (nread == 0) {
				eof = true;
				size_t tail = (cur - start) % asz;
				if (tail != 0) {
					bailout("load_trivial: final item incomplete: %d bytes instead of %d", (int) tail, (int) asz);
				}
				end = cur;
			}
			cur += (size_t) nread;
		}
		if (msg != NULL)
			goto end;
		BUN actualCount = validCount + (end - start) / asz;
		BATsetcount(bat, actualCount);
	}

	BATsetcount(bat, bat->batCount);
	bat->tseqbase = oid_nil;
	bat->tnonil = bat->batCount == 0;
	bat->tnil = false;
	if (bat->batCount <= 1) {
		bat->tsorted = true;
		bat->trevsorted = true;
		bat->tkey = true;
	} else {
		bat->tsorted = false;
		bat->trevsorted = false;
		bat->tkey = false;
	}

end:
	*eof_seen = (int)eof;
	return msg;
}

static str
load_fixed_width(BAT *bat, stream *s, bool byteswap, bincopy_decoder_t convert, size_t record_size, int *eof_reached)
{
	str msg = MAL_SUCCEED;
	bstream *bs = NULL;

	if (record_size == 0) {
		int tt = BATttype(bat);
		record_size = (size_t) ATOMsize(tt);
	}

	size_t chunk_size = 1<<20;
	assert(record_size > 0);
	chunk_size -= chunk_size % record_size;

	bs = bstream_create(s, chunk_size);
	if (bs == NULL) {
		msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto end;
	}

	while (1) {
		ssize_t nread = bstream_next(bs);
		if (nread < 0)
			bailout("%s", mnstr_peek_error(s));
		if (nread == 0)
			break;

		size_t n = (bs->len - bs->pos) / record_size;
		size_t extent = n * record_size;
		BUN count = BATcount(bat);
		BUN newCount = count + n;
		if (BATextend(bat, newCount) != GDK_SUCCEED)
			bailout("%s", GDK_EXCEPTION);

		msg = convert(Tloc(bat, count), &bs->buf[bs->pos], n, byteswap);
		if (msg != MAL_SUCCEED)
			goto end;
		BATsetcount(bat, newCount);
		bs->pos += extent;
	}

	bat->tseqbase = oid_nil;
	bat->tnonil = bat->batCount == 0;
	bat->tnil = false;
	if (bat->batCount <= 1) {
		bat->tsorted = true;
		bat->trevsorted = true;
		bat->tkey = true;
	} else {
		bat->tsorted = false;
		bat->trevsorted = false;
		bat->tkey = false;
	}

end:
	*eof_reached = 0;
	if (bs != NULL) {
		*eof_reached = (int)bs->eof;
		bs->s = NULL;
		bstream_destroy(bs);
	}
	return msg;
}


static str
load_column(struct type_rec *rec, const char *name, BAT *bat, stream *s, bool byteswap, BUN rows_estimate, int *eof_reached)
{
	str msg = MAL_SUCCEED;
	BUN orig_count, new_count;
	BUN rows_added;

	orig_count = BATcount(bat);

	// cannot have loader AND converter
	assert(rec->decoder == NULL || rec->loader == NULL);

	// loaders cannot be trivial
	assert( rec->loader == NULL || !rec->trivial_if_no_byteswap);

	if (rec->loader) {
		msg = rec->loader(bat, s, eof_reached);
	} else if (rec->decoder == NULL || (rec->trivial_if_no_byteswap && !byteswap)) {
		// load the bytes directly into the bat, as-is
		msg = load_trivial(bat, s, rows_estimate, eof_reached);
	} else {
		// load the bytes into an intermediate buffer and use the converter to
		// move them to the BAT
		msg = load_fixed_width(bat, s, byteswap, rec->decoder, rec->record_size, eof_reached);
	}

	new_count = BATcount(bat);
	rows_added = new_count - orig_count;

	if (msg == MAL_SUCCEED && rows_estimate != 0 && rows_estimate != rows_added)
		bailout(
			"inconsistent row count in %s: expected "BUNFMT", got "BUNFMT,
			name,
			rows_estimate, rows_added);

	end:
		return msg;
}

/* Import a single file into a new BAT.
 */
static str
importColumn(backend *be, bat *ret, BUN *retcnt, str method, bool byteswap, str path, int onclient,  BUN nrows)
{
	// In this function we create the BAT and open the file, and tidy
	// up when things go wrong. The actual work happens in load_column().

	// These are managed by the end: block.
	str msg = MAL_SUCCEED;
	int gdk_type;
	BAT *bat = NULL;
	stream *stream_to_close = NULL;
	int eof_reached = -1; // 1 = read to the end; 0 = stopped reading early; -1 = unset, a bug.

	// This one is not managed by the end: block
	stream *s;

	// Set safe values
	*ret = 0;
	*retcnt = 0;

	// Figure out what kind of data we have
	struct type_rec *rec = find_type_rec(method);
	if (rec == NULL)
		bailout("COPY BINARY FROM not implemented for '%s'", method);

	// Create the BAT
	gdk_type = ATOMindex(rec->gdk_type);
	if (gdk_type < 0)
		bailout("cannot load %s as %s: unknown atom type %s", path, method, rec->gdk_type);
	bat = COLnew(0, gdk_type, nrows, PERSISTENT);
	if (bat == NULL)
		bailout("%s", GDK_EXCEPTION);

	// Open the input stream
	if (onclient) {
		s = stream_to_close = mapi_request_upload(path, true, be->mvc->scanner.rs, be->mvc->scanner.ws);
	} else {
		s = stream_to_close = open_rstream(path);
	}
	if (!s) {
		msg = mnstr_error(NULL);
		goto end;
	}

	// Do the work
	msg = load_column(rec, path, bat, s, byteswap, nrows, &eof_reached);
	if (eof_reached != 0 && eof_reached != 1) {
		if (msg)
			bailout("internal error in sql.importColumn: eof_reached not set (%s). Earlier error: %s", method, msg);
		else
			bailout("internal error in sql.importColumn: eof_reached not set (%s)", method);
	}

	// Fall through into the end block which will clean things up
end:
	if (stream_to_close)
		close_stream(stream_to_close);

	// Manage the return values and `bat`.
	if (msg == MAL_SUCCEED) {
		BBPkeepref(bat);
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
	// Entry point for sql.importColumn.
	// Does the argument/return handling, the work is done by importColumn.
	(void)mb;

	assert(pci->retc == 2);
	bat *ret = getArgReference_bat(stk, pci, 0);
	BUN *retcnt = getArgReference_oid(stk, pci, 1);

	assert(pci->argc == 7);
	str method = *getArgReference_str(stk, pci, 2);
	bit byteswap = *getArgReference_bit(stk, pci, 3);
	str path = *getArgReference_str(stk, pci, 4);
	int onclient = *getArgReference_int(stk, pci, 5);
	BUN nrows = *getArgReference_oid(stk, pci, 6);

	backend *be = cntxt->sqlcontext;

	return importColumn(be, ret, retcnt, method, byteswap, path, onclient, nrows);
}

str
mvc_bin_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	// At some point we should remove all traces of importTable.
	// Until then, an error message.
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;

	return createException(MAL, "mvc_bin_import_table_wrap", "MAL operator sql.importTable should have been replaced with sql.importColumn");
}
