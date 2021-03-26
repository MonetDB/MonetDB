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
#include "copybinary_support.h"

#define bailout(...) do { \
		msg = createException(MAL, "sql.importColumn", SQLSTATE(42000) __VA_ARGS__); \
		goto end; \
	} while (0)


static str
BATattach_as_bytes(BAT *bat, stream *s, bool byteswap, BUN rows_estimate, str (*fixup)(void*,void*,bool), int *eof_seen)
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
			bailout("BATattach_as_bytes: %s", GDK_EXCEPTION);

		// Read into the newly allocated space
		char *start = Tloc(bat, validCount);
		char *cur = start;
		char *end = Tloc(bat, newCount);
		while (cur < end) {
			ssize_t nread = mnstr_read(s, cur, 1, end - cur);
			if (nread < 0)
				bailout("BATattach_as_bytes: %s", mnstr_peek_error(s));
			if (nread == 0) {
				eof = true;
				size_t tail = (cur - start) % asz;
				if (tail != 0) {
					bailout("BATattach_as_bytes: final item incomplete: %d bytes instead of %d", (int) tail, (int) asz);
				}
				end = cur;
			}
			cur += (size_t) nread;
		}
		msg = fixup(start, end, byteswap);
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
convert_bte(void *start, void *end, bool byteswap)
{
	(void)start;
	(void)end;
	(void)byteswap;

	return MAL_SUCCEED;
}

static str
convert_bit(void *start, void *end, bool byteswap)
{
	(void)byteswap;
	unsigned char *e = end;
	for (unsigned char *p = start; p < e; p++) {
		int b = *p;
		if (b > 1)
			throw(SQL, "convert_bit", SQLSTATE(22003) "invalid boolean byte value: %d", b);
	}
	return MAL_SUCCEED;
}

static str
convert_sht(void *start, void *end, bool byteswap)
{
	if (byteswap)
		for (sht *p = start; p < (sht*)end; p++)
			copy_binary_convert16(p);

	return MAL_SUCCEED;
}

static str
convert_int(void *start, void *end, bool byteswap)
{
	if (byteswap)
		for (int *p = start; p < (int*)end; p++)
			copy_binary_convert32(p);

	return MAL_SUCCEED;
}

static str
convert_lng(void *start, void *end, bool byteswap)
{
	if (byteswap)
		for (lng *p = start; p < (lng*)end; p++)
			copy_binary_convert64(p);

	return MAL_SUCCEED;
}

#ifdef HAVE_HGE
static str
convert_hge(void *start, void *end, bool byteswap)
{
	if (byteswap)
		for (hge *p = start; p < (hge*)end; p++)
			copy_binary_convert128(p);

	return MAL_SUCCEED;
}
#endif

static str
convert_uuid(void *start, void *end, bool byteswap)
{
	(void)byteswap;
	size_t nbytes = (char*)end - (char*)start;
	(void)nbytes; assert(nbytes % 16 == 0);

	return MAL_SUCCEED;
}

static str
convert_flt(void *start, void *end, bool byteswap)
{
	// Slightly dodgy pointer conversions here
	assert(sizeof(uint32_t) == sizeof(flt));
	assert(sizeof(struct { char dummy; uint32_t ui; }) >= sizeof(struct { char dummy; flt f; }));

	if (byteswap)
		for (uint32_t *p = start; (void*)p < end; p++)
			copy_binary_convert32(p);

	return MAL_SUCCEED;
}

static str
convert_dbl(void *start, void *end, bool byteswap)
{
	// Slightly dodgy pointer conversions here
	assert(sizeof(uint64_t) == sizeof(dbl));
	assert(sizeof(struct { char dummy; uint64_t ui; }) >= sizeof(struct { char dummy; dbl f; }));


	if (byteswap)
		for (uint64_t *p = start; (void*)p < end; p++)
			copy_binary_convert64(p);

	return MAL_SUCCEED;
}

static str
BATattach_fixed_width(BAT *bat, stream *s, bool byteswap, str (*convert)(void*,void*,void*,void*,bool), size_t record_size, int *eof_reached)
{
	str msg = MAL_SUCCEED;
	bstream *bs = NULL;

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

		msg = convert(
			Tloc(bat, count), Tloc(bat, newCount),
			&bs->buf[bs->pos], &bs->buf[bs->pos + extent],
			byteswap);
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
convert_date(void *dst_start, void *dst_end, void *src_start, void *src_end, bool byteswap)
{
	date *dst = (date*)dst_start;
	date *dst_e = (date*)dst_end;
	copy_binary_date *src = (copy_binary_date*)src_start;
	copy_binary_date *src_e = (copy_binary_date*)src_end;
	(void)dst_e; assert(dst_e - dst == src_e - src);

	for (; src < src_e; src++) {
		if (byteswap)
			copy_binary_convert_date(src);
		date value = date_create(src->year, src->month, src->day);
		*dst++ = value;
	}

	return MAL_SUCCEED;
}

static str
convert_time(void *dst_start, void *dst_end, void *src_start, void *src_end, bool byteswap)
{
	(void)byteswap;
	daytime *dst = (daytime*)dst_start;
	daytime *dst_e = (daytime*)dst_end;
	copy_binary_time *src = (copy_binary_time*)src_start;
	copy_binary_time *src_e = (copy_binary_time*)src_end;
	(void)dst_e; assert(dst_e - dst == src_e - src);

	for (; src < src_e; src++) {
		if (byteswap)
			copy_binary_convert_time(src);
		daytime value = daytime_create(src->hours, src->minutes, src->seconds, src->ms);
		*dst++ = value;
	}

	return MAL_SUCCEED;
}

static str
convert_timestamp(void *dst_start, void *dst_end, void *src_start, void *src_end, bool byteswap)
{
	(void)byteswap;
	timestamp *dst = (timestamp*)dst_start;
	timestamp *dst_e = (timestamp*)dst_end;
	copy_binary_timestamp *src = (copy_binary_timestamp*)src_start;
	copy_binary_timestamp *src_e = (copy_binary_timestamp*)src_end;
	(void)dst_e; assert(dst_e - dst == src_e - src);

	for (; src < src_e; src++) {
		if (byteswap)
			copy_binary_convert_timestamp(src);
		date dt = date_create(src->date.year, src->date.month, src->date.day);
		daytime tm = daytime_create(src->time.hours, src->time.minutes, src->time.seconds, src->time.ms);
		timestamp value = timestamp_create(dt, tm);
		*dst++ = value;
	}

	return MAL_SUCCEED;
}


static void
convert_line_endings(char *text)
{
	// Read- and write positions.
	// We always have w <= r, or it wouldn't be safe.
	const char *r = text;
	char *w = text;
	while (*r) {
		if (r[0] == '\r' && r[1] == '\n')
			r++;
		*w++ = *r++;
	}
	*w = '\0';
}

static str
append_text(BAT *bat, char *start, char *end)
{
	(void)bat;

	char *cr = memchr(start, '\r', end - start);
	if (cr)
		convert_line_endings(cr);

	if (BUNappend(bat, start, false) != GDK_SUCCEED)
		return createException(SQL, "sql.importColumn", GDK_EXCEPTION);

	return MAL_SUCCEED;
}

// Load items from the stream and put them in the BAT.
// Because it's text read from a binary stream, we replace \r\n with \n.
// We don't have to validate the utf-8 structure because BUNappend does that for us.
static str
load_zero_terminated_text(BAT *bat, stream *s, int *eof_reached)
{
	str msg = MAL_SUCCEED;
	bstream *bs = NULL;

	bs = bstream_create(s, 1 << 20);
	if (bs == NULL) {
		msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto end;
	}

	// In the outer loop we refill the buffer until the stream ends.
	// In the inner loop we look for complete \0-terminated strings.
	while (1) {
		ssize_t nread = bstream_next(bs);
		if (nread < 0)
			bailout("%s", mnstr_peek_error(s));
		if (nread == 0)
			break;

		char *buf_start = &bs->buf[bs->pos];
		char *buf_end = &bs->buf[bs->len];
		char *start, *end;
		for (start = buf_start; (end = memchr(start, '\0', buf_end - start)) != NULL; start = end + 1) {
			msg = append_text(bat, start, end);
			if (msg != NULL)
				goto end;
		}
		bs->pos = start - buf_start;
	}

	// It's an error to have date left after falling out of the outer loop
	if (bs->pos < bs->len)
		bailout("unterminated string at end");

end:
	*eof_reached = 0;
	if (bs != NULL) {
		*eof_reached = (int)bs->eof;
		bs->s = NULL;
		bstream_destroy(bs);
	}
	return msg;
}


// Dispatcher table for imports. We dispatch on a string value instead of for
// example the underlying gdktype so we have freedom to some day implement for
// example both zero-terminated strings and newline-terminated strings.
//
// An entry must fill one field of the following three: 'loader',
// 'convert_fixed_width', or 'convert_in_place'.

// A 'loader' has complete freedom. It is handed a BAT and a stream and it can
// then do whatever it wants. We use it to read strings and json and other
// variable-width data.
//
// If an entry has has 'convert_in_place' this means the external and internal
// forms have the same size and are probably identical. In this case, the data
// is loaded directly into the bat heap and then the 'convert_in_place' function
// is called once for the whole block to perform any necessary tweaking of the data.
// We use this for example for the integer types, on little-endian platforms no
// tweaking is necessary and on big-endian platforms we byteswap the data.
//
// Finally, if an entry has 'convert_fixed_width' it means the internal and
// external forms are both fixed width but different in size. The data is loaded into
// intermediate buffers first and the conversion function copies the data from
// an array of incoming data in the buffer to an array of internal
// representations in the BAT.
//
// A note about the function signatures: we use start/end pointers instead of
// start/size pairs because this way there can be no confusion about whether
// the given size is a byte count or an item count.
static struct type_rec {
	char *method;
	char *gdk_type;
	str (*loader)(BAT *bat, stream *s, int *eof_reached);
	str (*convert_fixed_width)(void *dst_start, void *dst_end, void *src_start, void *src_end, bool byteswap);
	size_t record_size;
	str (*convert_in_place)(void *start, void *end, bool byteswap);
} type_recs[] = {
	{ "bit", "bit", .convert_in_place=convert_bit, },
	{ "bte", "bte", .convert_in_place=convert_bte, },
	{ "sht", "sht", .convert_in_place=convert_sht, },
	{ "int", "int", .convert_in_place=convert_int, },
	{ "lng", "lng", .convert_in_place=convert_lng, },
	{ "flt", "flt", .convert_in_place=convert_flt, },
	{ "dbl", "dbl", .convert_in_place=convert_dbl, },
	//
#ifdef HAVE_HGE
	{ "hge", "hge", .convert_in_place=convert_hge, },
#endif
	//
	{ "str", "str", .loader=load_zero_terminated_text },
	{ "url", "url", .loader=load_zero_terminated_text },
	{ "json", "json", .loader=load_zero_terminated_text },
	{ "uuid", "uuid", .convert_in_place=convert_uuid, },
	//
	{ "date", "date", .convert_fixed_width=convert_date, .record_size=sizeof(copy_binary_date), },
	{ "daytime", "daytime", .convert_fixed_width=convert_time, .record_size=sizeof(copy_binary_time), },
	{ "timestamp", "timestamp", .convert_fixed_width=convert_timestamp, .record_size=sizeof(copy_binary_timestamp), },
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
load_column(struct type_rec *rec, const char *name, BAT *bat, stream *s, bool byteswap, BUN rows_estimate, int *eof_reached)
{
	str msg = MAL_SUCCEED;
	BUN orig_count, new_count;
	BUN rows_added;

	orig_count = BATcount(bat);

	if (rec->loader != NULL) {
		msg = rec->loader(bat, s, eof_reached);
	} else if (rec->convert_in_place != NULL) {
		msg = BATattach_as_bytes(bat, s, byteswap, rows_estimate, rec->convert_in_place, eof_reached);
	} else if (rec->convert_fixed_width != NULL) {
		msg = BATattach_fixed_width(bat, s, byteswap, rec->convert_fixed_width, rec->record_size, eof_reached);
	} else {
		*eof_reached = 0;
		bailout("invalid loader configuration for '%s'", rec->method);
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
		msg = createException(IO, "sql.importColumn", "Error %s", buf);
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
					IO, "sql.importColumn",
					"while syncing read stream: %s", mnstr_peek_error(rs));
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
importColumn(backend *be, bat *ret, BUN *retcnt, str method, bool byteswap, str path, int onclient,  BUN nrows)
{
	// In this function we create the BAT and open the file, and tidy
	// up when things go wrong. The actual work happens in load_column().

	// These are managed by the end: block.
	str msg = MAL_SUCCEED;
	int gdk_type;
	BAT *bat = NULL;
	stream *stream_to_close = NULL;
	bool do_finish_mapi = false;
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
		s = NULL;
		do_finish_mapi = true;
		msg = start_mapi_file_upload(be, path, &s);
		if (msg != MAL_SUCCEED)
			goto end;
	} else {
		s = stream_to_close = open_rstream(path);
		if (s == NULL)
			bailout("Couldn't open '%s' on server: %s", path, mnstr_peek_error(NULL));
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
	if (do_finish_mapi) {
		str msg1 = finish_mapi_file_upload(be, eof_reached == 1);
		if (msg == MAL_SUCCEED)
			msg = msg1;
	}

	if (stream_to_close)
		close_stream(stream_to_close);

	// Manage the return values and `bat`.
	if (msg == MAL_SUCCEED) {
		BBPkeepref(bat->batCacheid);
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
