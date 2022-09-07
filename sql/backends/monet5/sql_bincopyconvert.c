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
convert_bit(void *dst_, void *src_, size_t count, bool byteswap)
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
convert_sht(void *dst_, void *src_, size_t count, bool byteswap)
{
	assert(byteswap); // otherwise, why call us?
	sht *dst = dst_;
	const sht *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap16(*src++);
	return MAL_SUCCEED;
}

static str
convert_int(void *dst_, void *src_, size_t count, bool byteswap)
{
	assert(byteswap); // otherwise, why call us?
	int *dst = dst_;
	const int *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap32(*src++);
	return MAL_SUCCEED;
}

static str
convert_lng(void *dst_, void *src_, size_t count, bool byteswap)
{
	assert(byteswap); // otherwise, why call us?
	lng *dst = dst_;
	const lng *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap64(*src++);
	return MAL_SUCCEED;
}

#ifdef HAVE_HGE
static str
convert_hge(void *dst_, void *src_, size_t count, bool byteswap)
{
	assert(byteswap); // otherwise, why call us?
	hge *dst = dst_;
	const hge *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap128(*src++);
	return MAL_SUCCEED;
}
#endif

static str
convert_flt(void *dst_, void *src_, size_t count, bool byteswap)
{
	// Verify that size and alignment requirements of flt do not exceed int
	assert(sizeof(uint32_t) == sizeof(flt));
	assert(sizeof(struct { char dummy; uint32_t ui; }) >= sizeof(struct { char dummy; flt f; }));

	assert(byteswap); // otherwise, why call us?
	int *dst = dst_;
	const int *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap32(*src++);
	return MAL_SUCCEED;
}

static str
convert_dbl(void *dst_, void *src_, size_t count, bool byteswap)
{
	// Verify that size and alignment requirements of dbl do not exceed lng
	assert(sizeof(uint64_t) == sizeof(dbl));
	assert(sizeof(struct { char dummy; uint64_t ui; }) >= sizeof(struct { char dummy; dbl f; }));

	assert(byteswap); // otherwise, why call us?
	lng *dst = dst_;
	const lng *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap64(*src++);
	return MAL_SUCCEED;
}


static str
convert_date(void *dst_, void *src_, size_t count, bool byteswap)
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
convert_time(void *dst_, void *src_, size_t count, bool byteswap)
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
convert_timestamp(void *dst_, void *src_, size_t count, bool byteswap)
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
convert_and_validate(char *text)
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

static str
append_text(BAT *bat, char *start)
{
	str msg = convert_and_validate(start);
	if (msg != MAL_SUCCEED)
		return msg;

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

	// convert_and_validate() above counts on the following property to hold:
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
			msg = append_text(bat, start);
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









static struct type_rec type_recs[] = {
	// no conversion, no byteswapping
	{ "bte", "bte", .decoder=NULL, },
	{ "uuid", "uuid", .decoder=NULL, },
	// no conversion and no byteswapping but we must do range checking
	{ "bit", "bit", .trivial_if_no_byteswap=false, .decoder=convert_bit, },
	//
	{ "sht", "sht", .trivial_if_no_byteswap=true, .decoder=convert_sht, },
	{ "int", "int", .trivial_if_no_byteswap=true, .decoder=convert_int, },
	{ "lng", "lng", .trivial_if_no_byteswap=true, .decoder=convert_lng, },
	{ "flt", "flt", .trivial_if_no_byteswap=true, .decoder=convert_flt, },
	{ "dbl", "dbl", .trivial_if_no_byteswap=true, .decoder=convert_dbl, },
#ifdef HAVE_HGE
	{ "hge", "hge", .trivial_if_no_byteswap=true, .decoder=convert_hge, },
#endif
	//
	{ "str", "str", .loader=load_zero_terminated_text },
	{ "url", "url", .loader=load_zero_terminated_text },
	{ "json", "json", .loader=load_zero_terminated_text },
	//
	{ "date", "date", .decoder=convert_date, .record_size=sizeof(copy_binary_date), },
	{ "daytime", "daytime", .decoder=convert_time, .record_size=sizeof(copy_binary_time), },
	{ "timestamp", "timestamp", .decoder=convert_timestamp, .record_size=sizeof(copy_binary_timestamp), },
};


struct type_rec*
find_type_rec(str name)
{
	struct type_rec *end = (struct type_rec*)((char *)type_recs + sizeof(type_recs));
	for (struct type_rec *t = &type_recs[0]; t < end; t++)
		if (strcmp(t->method, name) == 0)
			return t;
	return NULL;
}
