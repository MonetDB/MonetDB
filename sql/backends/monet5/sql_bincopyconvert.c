/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "copybinary.h"
#include "copybinary_support.h"
#include "sql_bincopyconvert.h"
#include "sql.h"
#include "gdk.h"
#include "mal_backend.h"
#include "mal_interpreter.h"

static str
validate_bit(void *dst_, void *src_, size_t count, bool byteswap)
{
	(void)byteswap;
	unsigned char *dst = dst_;
	const unsigned char *src = src_;

	for (size_t i = 0; i < count; i++) {
		if (*src > 1)
			throw(SQL, "convert_bit", SQLSTATE(22003) "invalid boolean byte value: %d", *src);
		*dst++ = *src++;
	}
	return MAL_SUCCEED;
}

static str
byteswap_sht(void *dst_, void *src_, size_t count, bool byteswap)
{
	assert(byteswap); (void)byteswap; // otherwise, why call us?
	sht *dst = dst_;
	const sht *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap16(*src++);
	return MAL_SUCCEED;
}

static str
byteswap_int(void *dst_, void *src_, size_t count, bool byteswap)
{
	assert(byteswap); (void)byteswap; // otherwise, why call us?
	int *dst = dst_;
	const int *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap32(*src++);
	return MAL_SUCCEED;
}

static str
byteswap_lng(void *dst_, void *src_, size_t count, bool byteswap)
{
	assert(byteswap); (void)byteswap; // otherwise, why call us?
	lng *dst = dst_;
	const lng *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap64(*src++);
	return MAL_SUCCEED;
}

#ifdef HAVE_HGE
static str
byteswap_hge(void *dst_, void *src_, size_t count, bool byteswap)
{
	assert(byteswap); (void)byteswap; // otherwise, why call us?
	hge *dst = dst_;
	const hge *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap128(*src++);
	return MAL_SUCCEED;
}
#endif

static str
byteswap_flt(void *dst_, void *src_, size_t count, bool byteswap)
{
	// Verify that size and alignment requirements of flt do not exceed int
	assert(sizeof(uint32_t) == sizeof(flt));
	assert(sizeof(struct { char dummy; uint32_t ui; }) >= sizeof(struct { char dummy; flt f; }));

	assert(byteswap); (void)byteswap; // otherwise, why call us?
	int *dst = dst_;
	const int *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap32(*src++);
	return MAL_SUCCEED;
}

static str
byteswap_dbl(void *dst_, void *src_, size_t count, bool byteswap)
{
	// Verify that size and alignment requirements of dbl do not exceed lng
	assert(sizeof(uint64_t) == sizeof(dbl));
	assert(sizeof(struct { char dummy; uint64_t ui; }) >= sizeof(struct { char dummy; dbl f; }));

	assert(byteswap); (void)byteswap; // otherwise, why call us?
	lng *dst = dst_;
	const lng *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap64(*src++);
	return MAL_SUCCEED;
}


static str
decode_date(void *dst_, void *src_, size_t count, bool byteswap)
{
	date *dst = dst_;
	copy_binary_date *src = src_;

	for (size_t i = 0; i < count; i++) {
		copy_binary_date incoming;
		if (!byteswap)
			incoming = *src++;
		else
			incoming = copy_binary_byteswap_date(*src++);
		date value = date_create(incoming.year, incoming.month, incoming.day);
		*dst++ = value;
	}
	return MAL_SUCCEED;
}

static str
encode_date(void *dst_, void *src_, size_t count, bool byteswap)
{
	copy_binary_date *dst = dst_;
	date *src = src_;
	for (size_t i = 0; i < count; i++) {
		date dt = *src++;
		int16_t year = date_year(dt);
		if (byteswap)
			year = copy_binary_byteswap16(year);
		*dst++ = (copy_binary_date){
			.day = date_day(dt),
			.month = date_month(dt),
			.year = year,
		};
	}
	return MAL_SUCCEED;
}

static str
decode_time(void *dst_, void *src_, size_t count, bool byteswap)
{
	daytime *dst = dst_;
	copy_binary_time *src = src_;

	for (size_t i = 0; i < count; i++) {
		copy_binary_time incoming;
		if (!byteswap)
			incoming = *src++;
		else
			incoming = copy_binary_byteswap_time(*src++);
		daytime value = daytime_create(incoming.hours, incoming.minutes, incoming.seconds, incoming.ms);
		*dst++ = value;
	}
	return MAL_SUCCEED;
}

static str
encode_time(void *dst_, void *src_, size_t count, bool byteswap)
{
	copy_binary_time *dst = dst_;
	daytime *src = src_;
	for (size_t i = 0; i < count; i++) {
		daytime tm = *src++;
		uint32_t ms = daytime_usec(tm);
		if (byteswap)
			ms = copy_binary_byteswap32(ms);
		*dst++ = (copy_binary_time){
			.ms = ms,
			.seconds = daytime_sec(tm),
			.minutes = daytime_min(tm),
			.hours = daytime_hour(tm),
		};
	}
	return MAL_SUCCEED;
}

static str
decode_timestamp(void *dst_, void *src_, size_t count, bool byteswap)
{
	timestamp *dst = dst_;
	copy_binary_timestamp *src = src_;

	for (size_t i = 0; i < count; i++) {
		copy_binary_timestamp incoming;
		if (!byteswap)
			incoming = *src++;
		else
			incoming = copy_binary_byteswap_timestamp(*src++);
		date dt = date_create(incoming.date.year, incoming.date.month, incoming.date.day);
		daytime tm = daytime_create(incoming.time.hours, incoming.time.minutes, incoming.time.seconds, incoming.time.ms);
		timestamp value = timestamp_create(dt, tm);
		*dst++ = value;
	}
	return MAL_SUCCEED;
}


static str
encode_timestamp(void *dst_, void *src_, size_t count, bool byteswap)
{
	copy_binary_timestamp *dst = dst_;
	timestamp *src = src_;
	for (size_t i = 0; i < count; i++) {
		timestamp value = *src++;
		date dt = timestamp_date(value);
		daytime tm = timestamp_daytime(value);
		int16_t year = date_year(dt);
		uint32_t ms = daytime_usec(tm);
		if (byteswap) {
			ms = copy_binary_byteswap32(ms);
			year = copy_binary_byteswap16(year);
		}
		*dst++ = (copy_binary_timestamp) {
			.time = {
				.ms = ms,
				.seconds = daytime_sec(tm),
				.minutes = daytime_min(tm),
				.hours = daytime_hour(tm),
			},
			.date = {
				.day = date_day(dt),
				.month = date_month(dt),
				.year = year,
			},
		};
	}
	return MAL_SUCCEED;
}


static str
convert_and_validate_utf8(char *text)
{
	unsigned char *r = (unsigned char*)text;
	unsigned char *w = r;

	if (*r == 0x80 && *(r+1) == 0) {
		// Technically a utf-8 violation, but we treat it as the NULL marker
		// GDK does so as well so we can just pass it on.
		// load_zero_terminated_text() below contains an assert to ensure
		// this remains the case.
		return MAL_SUCCEED;
	}

	while (*r != 0) {
		unsigned char c = *w++ = *r++;

		if (c == '\r' && *r == '\n') {
			w--;
			continue;
		}
		if ((c & 0x80) == 0x00) // 1xxx_xxxx: standalone byte
			continue;
		if ((c & 0xF8) == 0xF0) // 1111_0xxx
			goto expect3;
		if ((c & 0xF0) == 0xE0) // 1110_xxxx
			goto expect2;
		if ((c & 0xE0) == 0xC0) // 110x_xxxx
			goto expect1;
		goto bad_utf8;

expect3:
		if (((*w++ = *r++) & 0x80) != 0x80)
			goto bad_utf8;
expect2:
		if (((*w++ = *r++) & 0x80) != 0x80)
			goto bad_utf8;
expect1:
		if (((*w++ = *r++) & 0x80) != 0x80)
			goto bad_utf8;

	}
	*w = '\0';
	return MAL_SUCCEED;

bad_utf8:
	return createException(SQL, "BATattach_stream", SQLSTATE(42000) "malformed utf-8 byte sequence");
}

// Load items from the stream and put them in the BAT.
// Because it's text read from a binary stream, we replace \r\n with \n.
static str
load_zero_terminated_text(BAT *bat, stream *s, int *eof_reached, bool byteswap)
{
	(void)byteswap;
	const char *mal_operator = "sql.importColumn";
	str msg = MAL_SUCCEED;
	bstream *bs = NULL;
	int tpe = BATttype(bat);
	void *buffer = NULL;
	size_t buffer_len = 0;

	// convert_and_validate_utf8() above counts on the following property to hold:
	assert(strNil((const char[2]){ 0x80, 0 }));

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
			char *value;
			msg = convert_and_validate_utf8(start);
			if (msg != NULL)
					goto end;
			if (tpe == TYPE_str) {
					value = start;
			} else {
					ssize_t n = ATOMfromstr(tpe, &buffer, &buffer_len, start, false);
					if (n <= 0) {
							msg = createException(SQL, "sql.importColumn", GDK_EXCEPTION);
							goto end;
					}
					value = buffer;
			}
			if (BUNappend(bat, value, false) != GDK_SUCCEED) {
					msg = createException(SQL, "sql.importColumn", GDK_EXCEPTION);
					goto end;
			}
		}
		bs->pos = start - buf_start;
	}

	// It's an error to have date left after falling out of the outer loop
	if (bs->pos < bs->len)
		bailout("unterminated string at end");

end:
	*eof_reached = 0;
	GDKfree(buffer);
	if (bs != NULL) {
		*eof_reached = (int)bs->eof;
		bs->s = NULL;
		bstream_destroy(bs);
	}
	return msg;
}

static str
dump_zero_terminated_text(BAT *bat, stream *s, bool byteswap)
{
	(void)byteswap;
	const char *mal_operator = "sql.export_bin_column";
	str msg = MAL_SUCCEED;
	int tpe = BATttype(bat);
	assert(ATOMstorage(tpe) == TYPE_str);
	assert(mnstr_isbinary(s));


	BUN end = BATcount(bat);
	BATiter bi = bat_iterator(bat);
	for (BUN p = 0; p < end; p++) {
		const char *v = BUNtvar(bi, p);
		if (mnstr_writeStr(s, v) < 0 || mnstr_writeBte(s, 0) < 0) {
			bailout("%s", mnstr_peek_error(s));
		}
	}

end:
	bat_iterator_end(&bi);
	return msg;
}


static struct type_record_t type_recs[] = {

	// no conversion, no byteswapping
	{ "bte", "bte", .encoder_trivial=true, .decoder_trivial=true},
	{ "uuid", "uuid", .encoder_trivial=true, .decoder_trivial=true},

	// no conversion and no byteswapping but we must do range checking on loading
	{ "bit", "bit", .trivial_if_no_byteswap=false, .decoder=validate_bit, .encoder_trivial=true},

	// vanilla integer types
	{ "sht", "sht", .trivial_if_no_byteswap=true, .decoder=byteswap_sht, .encoder=byteswap_sht},
	{ "int", "int", .trivial_if_no_byteswap=true, .decoder=byteswap_int, .encoder=byteswap_int},
	{ "lng", "lng", .trivial_if_no_byteswap=true, .decoder=byteswap_lng, .encoder=byteswap_lng},
	{ "flt", "flt", .trivial_if_no_byteswap=true, .decoder=byteswap_flt, .encoder=byteswap_flt},
	{ "dbl", "dbl", .trivial_if_no_byteswap=true, .decoder=byteswap_dbl, .encoder=byteswap_dbl},
#ifdef HAVE_HGE
	{ "hge", "hge", .trivial_if_no_byteswap=true, .decoder=byteswap_hge, .encoder=byteswap_hge},
#endif

	// \0-terminated text records
	{ "str", "str", .loader=load_zero_terminated_text, .dumper=dump_zero_terminated_text },
	{ "url", "url", .loader=load_zero_terminated_text, .dumper=dump_zero_terminated_text },
	{ "json", "json", .loader=load_zero_terminated_text, .dumper=dump_zero_terminated_text },

	// temporal types have record size different from the underlying gdk type
	{ "date", "date", .decoder=decode_date, .encoder=encode_date, .record_size=sizeof(copy_binary_date), },
	{ "daytime", "daytime", .decoder=decode_time, .encoder=encode_time, .record_size=sizeof(copy_binary_time), },
	{ "timestamp", "timestamp", .decoder=decode_timestamp, .encoder=encode_timestamp, .record_size=sizeof(copy_binary_timestamp), },
};


type_record_t*
find_type_rec(const char *name)
{
	struct type_record_t *end = (struct type_record_t*)((char *)type_recs + sizeof(type_recs));
	for (struct type_record_t *t = &type_recs[0]; t < end; t++)
		if (strcmp(t->method, name) == 0)
			return t;
	return NULL;
}
