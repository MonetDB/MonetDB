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
	//
	{ "uuid", "uuid", .convert_in_place=convert_uuid, },
	{ "date", "date", .convert_fixed_width=convert_date, .record_size=sizeof(copy_binary_date), },
	{ "daytime", "daytime", .convert_fixed_width=convert_time, .record_size=sizeof(copy_binary_time), },
	{ "timestamp", "timestamp", .convert_fixed_width=convert_timestamp, .record_size=sizeof(copy_binary_timestamp), },
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
