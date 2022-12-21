/*
 * SPDX-License-Identifier: MPL-2.0
 *
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
#include "gdk.h"
#include "sql.h"
#include "mal_backend.h"
#include "mal_interpreter.h"
#include "sql_bincopyconvert.h"
#include "copybinary.h"
#include "copybinary_support.h"


static str
load_trivial(BAT *bat, stream *s, BUN rows_estimate, int *eof_seen)
{
	const char *mal_operator = "sql.importColumn";
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
load_fixed_width(BAT *bat, stream *s, int width, bool byteswap, bincopy_decoder_t convert, size_t record_size, int *eof_reached)
{
	const char *mal_operator = "sql.importColumn";
	str msg = MAL_SUCCEED;
	bstream *bs = NULL;

	if (record_size == 0) {
		int tt = BATttype(bat);
		record_size = (size_t) ATOMsize(tt);
	}

	// Read whole number of records
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

		msg = convert(Tloc(bat, count), &bs->buf[bs->pos], n, width, byteswap);
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
load_column(type_record_t *rec, const char *name, BAT *bat, stream *s, int width, bool byteswap, BUN rows_estimate, int *eof_reached)
{
	const char *mal_operator = "sql.importColumn";
	BUN orig_count, new_count;
	str msg = MAL_SUCCEED;
	BUN rows_added;

	bincopy_loader_t loader = rec->loader;
	bincopy_decoder_t decoder = rec->decoder;
	bool trivial = rec->decoder_trivial;

	// sanity check
	assert( (loader != NULL) + (decoder != NULL) + trivial == 1); (void)trivial;

	if (rec->trivial_if_no_byteswap && !byteswap)
		decoder = NULL;

	orig_count = BATcount(bat);

	if (loader) {
		msg = loader(bat, s, eof_reached, width, byteswap);
	} else if (decoder) {
		msg = load_fixed_width(bat, s, width, byteswap, rec->decoder, rec->record_size, eof_reached);
	} else {
		// load the bytes directly into the bat, as-is
		msg = load_trivial(bat, s, rows_estimate, eof_reached);
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
import_column(backend *be, bat *ret, BUN *retcnt, str method, int width, bool byteswap, str path, int onclient,  BUN nrows)
{
	// In this function we create the BAT and open the file, and tidy
	// up when things go wrong. The actual work happens in load_column().

	const str mal_operator = "sql.importColumn";

	// These are managed by the end: block.
	str msg = MAL_SUCCEED;
	int gdk_type;
	BAT *bat = NULL;
	int eof_reached = -1; // 1 = read to the end; 0 = stopped reading early; -1 = unset, a bug.
	stream *s = NULL;

	// Set safe values
	*ret = 0;
	*retcnt = 0;

	// Figure out what kind of data we have
	type_record_t *rec = find_type_rec(method);
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
		s = mapi_request_upload(path, true, be->mvc->scanner.rs, be->mvc->scanner.ws);
	} else {
		s = open_rstream(path);
	}
	if (!s) {
		bailout("%s", mnstr_peek_error(NULL));
	}

	// Do the work
	msg = load_column(rec, path, bat, s, width, byteswap, nrows, &eof_reached);
	if (eof_reached != 0 && eof_reached != 1) {
		if (msg)
			bailout("internal error in sql.importColumn: eof_reached not set (%s). Earlier error: %s", method, msg);
		else
			bailout("internal error in sql.importColumn: eof_reached not set (%s)", method);
	}

	// Fall through into the end block which will clean things up
end:
	if (s)
		close_stream(s);

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

	assert(pci->argc == 8);
	str method = *getArgReference_str(stk, pci, 2);
	int width = *getArgReference_int(stk, pci, 3);
	bit byteswap = *getArgReference_bit(stk, pci, 4);
	str path = *getArgReference_str(stk, pci, 5);
	int onclient = *getArgReference_int(stk, pci, 6);
	BUN nrows = *getArgReference_oid(stk, pci, 7);

	backend *be = cntxt->sqlcontext;

	return import_column(be, ret, retcnt, method, width, byteswap, path, onclient, nrows);
}



static str
write_out(const char *start, const char *end, stream *s)
{
	const char *mal_operator = "sql.export_bin_column";
	str msg = MAL_SUCCEED;

	const char *p = start;
	while (p < end) {
		ssize_t nwritten = mnstr_write(s, p, 1, end - p);
		if (nwritten < 0)
			bailout("%s", mnstr_peek_error(s));
		if (nwritten == 0)
			bailout("Unexpected EOF on %s", mnstr_name(s));
		p += nwritten;
	}
end:
	return msg;
}

static str
dump_trivial(BAT *b, stream *s)
{
	assert(!ATOMvarsized(BATttype(b)));

	return write_out(Tloc(b, 0), Tloc(b, BATcount(b)), s);
}

static str
dump_fixed_width(BAT *b, stream *s, bool byteswap, bincopy_encoder_t encoder, size_t record_size)
{
	const char *mal_operator = "sql.export_bin_column";
	str msg = MAL_SUCCEED;
	char *buffer = NULL;

	if (record_size == 0) {
		int tt = BATttype(b);
		record_size = (size_t) ATOMsize(tt);
	}
	size_t buffer_size = 1024 * 1024;
	BUN batch_size = buffer_size / record_size;
	if (batch_size > BATcount(b))
		batch_size = BATcount(b);
	buffer_size = batch_size * record_size;
	buffer = GDKmalloc(buffer_size);
	if (buffer == NULL)
		bailout(MAL_MALLOC_FAIL);

	BUN n;
	for (BUN pos = 0; pos < BATcount(b); pos += n) {
		n = BATcount(b) - pos;
		if (n > batch_size)
			n = batch_size;
		msg = encoder(buffer, Tloc(b, pos), n, 0, byteswap);
		if (msg != MAL_SUCCEED)
			goto end;
		msg = write_out(buffer, buffer + n * record_size, s);
		if (msg != MAL_SUCCEED)
			goto end;
	}

end:
	GDKfree(buffer);
	return msg;
}

static str
dump_column(const struct type_record_t *rec, BAT *b, bool byteswap, stream *s)
{
	str msg = MAL_SUCCEED;

	bincopy_dumper_t dumper = rec->dumper;
	bincopy_encoder_t encoder = rec->encoder;
	bool trivial = rec->encoder_trivial;

	// sanity check
	assert( (dumper != NULL) + (encoder != NULL) + trivial == 1); (void)trivial;

	if (rec->trivial_if_no_byteswap && !byteswap)
		encoder = NULL;

	if (dumper) {
		msg = rec->dumper(b, s, byteswap);
	} else if (encoder) {
		msg = dump_fixed_width(b, s, byteswap, rec->encoder, rec->record_size);
	} else {
		msg = dump_trivial(b, s);
	}

	return msg;
}


static str
export_column(backend *be, BAT *b, bool byteswap, str filename, bool onclient)
{
	const char *mal_operator = "sql.export_bin_column";
	str msg = MAL_SUCCEED;
	stream *s = NULL;

	// Figure out what kind of data we have
	int tpe = BATttype(b);
	const char *gdk_name = ATOMname(tpe);
	type_record_t *rec = find_type_rec(gdk_name);
	if (rec == NULL)
		bailout("COPY INTO BINARY not implemented for '%s'", gdk_name);

	if (onclient) {
		(void)be;
		s = mapi_request_download(filename, true, be->mvc->scanner.rs, be->mvc->scanner.ws);
	} else {
		s = open_wstream(filename);
	}
	if (!s) {
		bailout("%s", mnstr_peek_error(NULL));
	}

	msg = dump_column(rec, b, byteswap, s);

	if (s && msg == MAL_SUCCEED) {
		if (mnstr_flush(s, MNSTR_FLUSH_DATA) != 0) {
			bailout("%s", mnstr_peek_error(s));
		}
	}

end:
	close_stream(s);
	return msg;
}

str
mvc_bin_export_column_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const char *mal_operator = "sql.export_bin_column";
	str msg = MAL_SUCCEED;
	BAT *b = NULL;
	backend *be = cntxt->sqlcontext;
	assert(pci->retc == 1);
	assert(pci->argc == 5);

	lng *ret = getArgReference_lng(stk, pci, 0);
	// arg 1 handled below
	bool byteswap = *getArgReference_bit(stk, pci, 2);
	str filename = *getArgReference_str(stk, pci, 3);
	bool onclient = (bool) *getArgReference_int(stk, pci, 4);

	// Usually we are called with a BAT argument but if the user types
	// something like
	//
	//    COPY (SELECT 42 AS num, 'banana' AS word) INTO BINARY ...
	//
	// it will be a single value instead.
	// To avoid having to handle separate cases everywhere we simply
	// stuff the value into a temporary BAT
	int arg_type = getArgType(mb, pci, 1);
	if (isaBatType(arg_type)) {
		bat id = *getArgReference_bat(stk, pci, 1);
		b = BATdescriptor(id);
	} else {
		void *value = getArgReference(stk, pci, 1);
		b = COLnew(0, arg_type, 1, TRANSIENT);
		if (!b)
			bailout("%s", GDK_EXCEPTION);
		if (BUNappend(b, value, true) != GDK_SUCCEED)
			bailout("%s", GDK_EXCEPTION);
	}

	msg = export_column(be, b, byteswap, filename, onclient);
	if (msg == MAL_SUCCEED)
		*ret = BATcount(b);

end:
	BBPreclaim(b);
	return msg;
}
