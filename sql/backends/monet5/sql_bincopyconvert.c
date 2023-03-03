/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "copybinary.h"
#include "copybinary_support.h"
#include "sql_bincopyconvert.h"
#include "sql.h"
#include "gdk.h"
#include "mal_backend.h"
#include "mal_interpreter.h"
#include "mstring.h"


#define bailout(...) do { \
		msg = createException(MAL, mal_operator, SQLSTATE(42000) __VA_ARGS__); \
		goto end; \
	} while (0)


static str
validate_bit(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;
	(void)byteswap;
	bit *dst = dst_;
	const unsigned char *src = src_;

	for (size_t i = 0; i < count; i++) {
		if (*src > 1 && *src != 0x80)
			throw(SQL, "convert_bit", SQLSTATE(22003) "invalid boolean byte value: %d", *src);
		*dst++ = (bit)*src++;
	}
	return MAL_SUCCEED;
}

// width is only nonzero for DECIMAL types. For plain integer types it is 0.
#define VALIDATE_DECIMAL(TYP) do { \
		if (width) { \
			TYP m = 1; \
			for (int i = 0; i < width; i++) \
				m *= 10; \
			dst = dst_; \
			for (size_t i = 0; i < count; i++) { \
				if (dst[i] >= m || dst[i] <= -m) \
					throw(SQL, "convert", SQLSTATE(22003) "decimal out of range"); \
			} \
		} \
    } while (0)


static str
byteswap_sht(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	assert(byteswap); (void)byteswap; // otherwise, why call us?
	sht *dst = dst_;
	const sht *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap16(*src++);

	VALIDATE_DECIMAL(sht);

	return MAL_SUCCEED;
}

static str
byteswap_int(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	assert(byteswap); (void)byteswap; // otherwise, why call us?
	int *dst = dst_;
	const int *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap32(*src++);

	VALIDATE_DECIMAL(int);

	return MAL_SUCCEED;
}

static str
byteswap_lng(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	assert(byteswap); (void)byteswap; // otherwise, why call us?
	lng *dst = dst_;
	const lng *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap64(*src++);

	VALIDATE_DECIMAL(lng);

	return MAL_SUCCEED;
}

#ifdef HAVE_HGE
static str
byteswap_hge(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	assert(byteswap); (void)byteswap; // otherwise, why call us?
	hge *dst = dst_;
	const hge *src = src_;
	for (size_t i = 0; i < count; i++)
		*dst++ = copy_binary_byteswap128(*src++);

	VALIDATE_DECIMAL(hge);

	return MAL_SUCCEED;
}
#endif

static str
byteswap_flt(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;

	// Verify that size and alignment requirements of flt do not exceed int.
	// This is important because we use the int32 byteswap to byteswap the floats.
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
byteswap_dbl(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;

	// Verify that size and alignment requirements of dbl do not exceed lng
	// This is important because we use the int64 byteswap to byteswap the doubles.
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
decode_date(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;

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
encode_date(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;
	copy_binary_date *dst = dst_;
	date *src = src_;
	for (size_t i = 0; i < count; i++) {
		date dt = *src++;
		if (is_date_nil(dt)) {
			*dst++ = (copy_binary_date){
				.day = 0xFF,
				.month = 0xFF,
				.year = -1,
			};
			continue;
		}
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
decode_time(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;

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
encode_time(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;
	copy_binary_time *dst = dst_;
	daytime *src = src_;
	for (size_t i = 0; i < count; i++) {
		daytime tm = *src++;
		if (is_daytime_nil(tm)) {
			*dst++ = (copy_binary_time){
				.ms = 0xFFFFFFFF,
				.seconds = 0xFF,
				.minutes = 0xFF,
				.hours = 0xFF,
				.padding = 0xFF,
			};
			continue;
		}
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
decode_timestamp(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;

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
encode_timestamp(void *dst_, void *src_, size_t count, int width, bool byteswap)
{
	(void)width;

	copy_binary_timestamp *dst = dst_;
	timestamp *src = src_;
	for (size_t i = 0; i < count; i++) {
		timestamp value = *src++;
		if (is_timestamp_nil(value)) {
			*dst++ = (copy_binary_timestamp) {
				.time = {
					.ms = 0xFFFFFFFF,
					.seconds = 0xFF,
					.minutes = 0xFF,
					.hours = 0xFF,
					.padding = 0xFF,
				},
				.date = {
					.day = 0xFF,
					.month = 0xFF,
					.year = -1,
				}
			};
			continue;
		}
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

// Load NUL-terminated items from the stream and put them in the BAT.
static str
load_zero_terminated_text(BAT *bat, stream *s, int *eof_reached, int width, bool byteswap)
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
			if (!checkUTF8(start)) {
				msg = createException(SQL, "load_zero_terminated_text", SQLSTATE(42000) "malformed utf-8 byte sequence");
				goto end;
			}
			if (tpe == TYPE_str) {
					if (width > 0) {
						int w = UTF8_strwidth(start);
						if (w > width) {
							msg = createException(SQL, "sql.importColumn", "string too wide for column");
							goto end;
						}
					}
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
dump_zero_terminated_text(BAT *bat, stream *s, BUN start, BUN length, bool byteswap)
{
	(void)byteswap;
	const char *mal_operator = "sql.export_bin_column";
	str msg = MAL_SUCCEED;
	int tpe = BATttype(bat);
	assert(ATOMstorage(tpe) == TYPE_str); (void)tpe;
	assert(mnstr_isbinary(s));


	BUN end = start + length;
	assert(end <= BATcount(bat));
	BATiter bi = bat_iterator(bat);
	for (BUN p = start; p < end; p++) {
		const char *v = BUNtvar(bi, p);
		if (mnstr_writeStr(s, v) < 0 || mnstr_writeBte(s, 0) < 0) {
			bailout("%s", mnstr_peek_error(s));
		}
	}

end:
	bat_iterator_end(&bi);
	return msg;
}

// Some streams, in particular the mapi upload stream, sometimes read fewer
// bytes than requested. This function wraps the read in a loop to force it to
// read the whole block
static ssize_t
read_exact(stream *s, void *buffer, size_t length)
{
	char *p = buffer;

	while (length > 0) {
		ssize_t nread = mnstr_read(s, p, 1, length);
		if (nread < 0) {
			return nread;
		} else if (nread == 0) {
			break;
		} else {
			p += nread;
			length -= nread;
		}
	}

	return p - (char*)buffer;
}

// Read BLOBs.  Every blob is preceded by a 64bit header word indicating its length.
// NULLs are indicated by length==-1
static str
load_blob(BAT *bat, stream *s, int *eof_reached, int width, bool byteswap)
{
	(void)width;
	const char *mal_operator = "sql.importColumn";
	str msg = MAL_SUCCEED;
	const blob *nil_value = ATOMnilptr(TYPE_blob);
	blob *buffer = NULL;
	size_t buffer_size = 0;
	union {
		uint64_t length;
		char bytes[8];
	} header;

	*eof_reached = 0;

	while (1) {
		const blob *value;
		// Read the header
		ssize_t nread = read_exact(s, header.bytes, 8);
		if (nread < 0) {
			bailout("%s", mnstr_peek_error(s));
		} else if (nread == 0) {
			*eof_reached = 1;
			break;
		} else if (nread < 8) {
			bailout("incomplete blob at end of file");
		}
		if (byteswap) {
			copy_binary_convert64(&header.length);
		}

		if (header.length == ~(uint64_t)0) {
			value = nil_value;
		} else {
			size_t length;
			size_t needed;

			if (header.length >= VAR_MAX) {
				bailout("blob too long");
			}
			length = (size_t) header.length;

			// Reallocate the buffer
			needed = sizeof(blob) + length;
			if (buffer_size < needed) {
				// do not use GDKrealloc, no need to copy the old contents
				GDKfree(buffer);
				size_t allocate = needed;
				allocate += allocate / 16;   // add a little margin
#ifdef _MSC_VER
#pragma warning(suppress:4146)
#endif
				allocate += ((-allocate) % 0x100000);   // round up to nearest MiB
				assert(allocate >= needed);
				buffer = GDKmalloc(allocate);
				if (!buffer) {
					msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto end;
				}
				buffer_size = allocate;
			}

			// Fill the buffer
			buffer->nitems = length;
			if (length > 0) {
				nread = read_exact(s, buffer->data, length);
				if (nread < 0) {
					bailout("%s", mnstr_peek_error(s));
				} else if ((size_t)nread < length) {
					bailout("Incomplete blob at end of file");
				}
			}

			value = buffer;
		}

		if (BUNappend(bat, value, false) != GDK_SUCCEED) {
				msg = createException(SQL, mal_operator, GDK_EXCEPTION);
				goto end;
		}
	}

end:
	GDKfree(buffer);
	return msg;
}

static str
dump_blob(BAT *bat, stream *s, BUN start, BUN length, bool byteswap)
{
	const char *mal_operator = "sql.export_bin_column";
	str msg = MAL_SUCCEED;
	int tpe = BATttype(bat);
	assert(ATOMstorage(tpe) == TYPE_blob); (void)tpe;
	assert(mnstr_isbinary(s));

	BUN end = start + length;
	assert(end <= BATcount(bat));
	BATiter bi = bat_iterator(bat);
	uint64_t nil_header = ~(uint64_t)0;
	for (BUN p = start; p < end; p++) {
		const blob *b = BUNtvar(bi, p);
		uint64_t header = is_blob_nil(b) ? nil_header : (uint64_t)b->nitems;
		if (byteswap)
			copy_binary_convert64(&header);
		if (mnstr_write(s, &header, 8, 1) != 1) {
			bailout("%s", mnstr_peek_error(s));
		}
		if (!is_blob_nil(b) && mnstr_write(s, b->data,b->nitems, 1) != 1) {
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

	{ "blob", "blob", .loader=load_blob, .dumper=dump_blob },

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

bool
can_dump_binary_column(const type_record_t *rec)
{
	return rec->encoder_trivial || rec->dumper || rec->encoder;
}
