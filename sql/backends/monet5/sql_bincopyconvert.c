/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
validate_bit(void *dst_, size_t count, int width, const char *filename)
{
	(void)width;
	const unsigned char *data = dst_;

	for (size_t i = 0; i < count; i++) {
		if (data[i] > 1 && data[i] != 0x80)
			throw(SQL, "convert_bit", SQLSTATE(22003) "invalid boolean byte value %d in %s", data[i], filename);
	}
	return MAL_SUCCEED;
}

// width is only nonzero for DECIMAL types. For plain integer types it is 0.
#define VALIDATE_DECIMAL(TYP,NIL_VALUE) do { \
		if (width) { \
			TYP m = 1; \
			for (int i = 0; i < width; i++) \
				m *= 10; \
			TYP *dst = dst_; \
			for (size_t i = 0; i < count; i++) { \
				if (dst[i] == NIL_VALUE) \
					continue; \
				if (dst[i] >= m || dst[i] <= -m) \
					throw(SQL, "convert", SQLSTATE(22003) "decimal out of range in %s", filename); \
			} \
		} \
    } while (0)


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
validate_sht(void *dst_, size_t count, int width, const char *filename)
{
	VALIDATE_DECIMAL(sht, sht_nil);
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
validate_int(void *dst_, size_t count, int width, const char *filename)
{
	VALIDATE_DECIMAL(int, int_nil);
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

static str
validate_lng(void *dst_, size_t count, int width, const char *filename)
{
	VALIDATE_DECIMAL(lng, lng_nil);
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

static str
validate_hge(void *dst_, size_t count, int width, const char *filename)
{
	VALIDATE_DECIMAL(hge, hge_nil);
	return MAL_SUCCEED;
}
#endif

static str
byteswap_flt(void *dst_, void *src_, size_t count, bool byteswap)
{
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
byteswap_dbl(void *dst_, void *src_, size_t count, bool byteswap)
{
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

void init_insert_state(struct insert_state *st, allocator *ma, BAT *bat, int width) {
	*st = (struct insert_state) {
		.ma = ma,
		.bat = bat,
		.width = width,
		.scratch = NULL,
		.schratch_len = 0,
		.resume = 0,
	};
	for (size_t i = 0; i < sizeof(st->singlechar)/sizeof(st->singlechar[0]); i++) {
		st->singlechar[i] = BUN_NONE;
	}
};

void release_insert_state(struct insert_state *st) {
	// No longer needed because we use the .ma allocator which is managed by
	// our caller: GDKfree(st->scratch);
	(void)st;
}

static str
reinsert(struct insert_state *st, BUN bun)
{
	if (bun >= BATcount(st->bat))
		throw(SQL, "insert_nul_terminated_values", SQLSTATE(42000) "invalid repeat bun " BUNFMT, bun);
	if (BATcount(st->bat) == BATcapacity(st->bat)) {
		if (BATextend(st->bat, BATgrows(st->bat)) != GDK_SUCCEED)
				throw(SQL, "insert_nul_terminated_values", GDK_EXCEPTION);
	}
	void *src = Tloc(st->bat, bun);
	void *dst = Tloc(st->bat, BATcount(st->bat));
	switch (st->bat->twidth) {
		case 1:
			*(uint8_t*)dst = *(uint8_t*)src;
			break;
		case 2:
			*(uint16_t*)dst = *(uint16_t*)src;
			break;
		case 4:
			*(uint32_t*)dst = *(uint32_t*)src;
			break;
		case 8:
			*(uint64_t*)dst = *(uint64_t*)src;
			break;
		default:
			MT_UNREACHABLE();
	}
	st->bat->batCount++;
	return MAL_SUCCEED;
}

static str
insert_non_nil(struct insert_state *st, const char *item)
{
	int tpe = BATttype(st->bat);
	const void *value;

	if (!checkUTF8(item)) {
		throw(SQL, "insert_nul_terminated_values", SQLSTATE(42000) "malformed utf-8 byte sequence");
	}
	if (tpe == TYPE_str) {
		if (st->width > 0 && UTF8_strlen(item) > st->width)
			throw(SQL, "insert_nul_terminated_values", "string too wide for column");
		value = item;
	} else {
		ssize_t n = ATOMfromstr(st->ma, tpe, &st->scratch, &st->schratch_len, item, false);
		if (n <= 0)
			throw(SQL, "insert_nul_terminated_values", GDK_EXCEPTION);
		value = st->scratch;
	}

	// By now 'value' has been set
	if (bunfastapp(st->bat, value) != GDK_SUCCEED) {
		throw(SQL, "insert_nul_terminated_values", GDK_EXCEPTION);
	}

	return MAL_SUCCEED;
}

// Can be used to insert a string that consists of a single ascii
// character, or nil (ch==0x80), or the empty string (ch==0)
static str
insert_single_char(struct insert_state *st, int ch)
{
	BUN reuse = st->singlechar[ch];
	if (reuse != BUN_NONE) {
		str msg = reinsert(st, reuse);
		if (msg != MAL_SUCCEED)
			return msg;
	} else {
		char value[] = {ch, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		if (bunfastapp(st->bat, (char*)value) != GDK_SUCCEED)
			throw(SQL, "insert_nul_terminated_values", GDK_EXCEPTION);
	}
	// Prefer to remember the latest occurrence so we can use short backrefs
	st->singlechar[ch] = st->bat->batCount - 1;
	return MAL_SUCCEED;
}

str
insert_nul_terminated_values(struct insert_state *st, const char *data, size_t total_len, size_t *consumed)
{
	assert(st->resume < total_len);

	// We use unsigned char to make the backref computations easier
	const unsigned char *start = (const unsigned char*)data;
	const unsigned char *limit = start + total_len;
	const unsigned char *current = start;   // start of the current item
	const unsigned char *resume = current + st->resume; // where to start looking for NUL

	while (current < limit) {
		// Recognize the item and determine where it ends.
		// If we reach 'limit' we'll goto end without updating 'current'.
		const unsigned char *pos = current;
		const unsigned char first = *pos++;
		str msg;
		if ((first & 0xC0) != 0x80) {
			// Not a nil, not a backref.
			if (first == 0 || (pos < limit && *pos == 0)) {
				// We have an extra efficient code path for empty-
				// and single character strings.
				msg = insert_single_char(st, first);
				// Skip NUL if we haven't already
				pos += (first != 0);
			} else {
				//  Find out how long it is.
				pos = memchr(resume, '\0', limit - resume);
				if (pos == NULL) {
					// the end of the string is not yet in our buffer
					resume = limit;
					goto end;
				}
				pos++; // include the NUL terminator
				msg = insert_non_nil(st, (char*)current);
			}
			if (msg != MAL_SUCCEED)
				return msg;
		} else if (first > 0x80) {
			// 0x81 .. 0xBF, a short back ref
			assert(first <= 0xBF);
			BUN delta = first - 0x80;
			msg = reinsert(st, BATcount(st->bat) - delta);
			if (msg != MAL_SUCCEED)
				return msg;
		} else {
			// 0x80 so it's either a nil or a long backref
			assert(first == 0x80);
			if (pos == limit) {
				// can't tell the difference yet
				resume = current;
				goto end;
			}
			unsigned char follower = *pos++;
			if (follower == '\0') {
				// it's a nil
				str msg = insert_single_char(st, 0x80);
				if (msg != MAL_SUCCEED)
					return msg;
			} else {
				// it's a long backref
				BUN delta = follower & 0x7F;
				unsigned int shift = 0;
				while (follower > 0x7F) {
					if (pos == limit) {
						// incomplete
						resume = current;
						goto end;
					}
					if (shift > 8 * sizeof(BUN) - 14) {
						// the payload is 7 bits wide, if we increase shift
						// by 7 it's going to overflow.
						// TODO maybe we need to set a stricter limit?
						throw(SQL, "insert_nul_terminated_values", SQLSTATE(42000)"invalid backref in binary data at %ld", (long)BATcount(st->bat));
					}
					shift += 7;
					follower = *pos++;
					BUN payload = follower & 0x7F;
					delta = delta | (payload << shift);
				}
				msg = reinsert(st, BATcount(st->bat) - delta);
				if (msg != MAL_SUCCEED)
					return msg;
			}
		}

		// Prepare for the next iteration
		current = pos;
		resume = current;
	}

end:
	*consumed = current - start;
	st->resume = resume - current;
	return MAL_SUCCEED;
}

// #define DEBUG_PRINTFS

static str
load_zero_terminated_text(BAT *bat, stream *s, int *eof_reached, int width, bool byteswap)
{
	str msg;
	static const char mal_operator[] = "sql.export_bin_column";
	const size_t min_read = 8190;
	allocator *ma = MT_thread_getallocator();
	allocator_state ma_state = ma_open(ma);
	struct insert_state state;
	bstream *bs = NULL;

	(void)byteswap; // not applicable to strings
	init_insert_state(&state, ma, bat, width);
	bs = bstream_create(s, 1<<20);
	if (bs == NULL) {
		msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto end;
	}

	while (1) {
		size_t in_use = bs->len - bs->pos;
		size_t free = bs->size - in_use;
		size_t to_read = free >= min_read ? free : min_read;
		#ifdef DEBUG_PRINTFS
		fprintf(stderr, "# before read %zu: %zu..%zu/%zu = ", to_read, bs->pos, bs->len, bs->size);
		for (size_t i = bs->pos; i < bs->len; i++)
			fprintf(stderr, " %02X", (unsigned char) bs->buf[i]);
		fprintf(stderr, "\n");
		#endif
		ssize_t nread = bstream_read(bs, to_read);
		if (nread < 0)
			bailout("%s", mnstr_peek_error(s));
		else if (nread == 0) {
			assert(bs->eof);
			break;
		}
		#ifdef DEBUG_PRINTFS
		fprintf(stderr, "# after read %zu: %zu..%zu/%zu = ", nread, bs->pos, bs->len, bs->size);
		for (size_t i = bs->pos; i < bs->len; i++)
			fprintf(stderr, " %02X", (unsigned char) bs->buf[i]);
		fprintf(stderr, "\n");
		#endif
		size_t consumed;
		msg = insert_nul_terminated_values(&state, &bs->buf[bs->pos], bs->len - bs->pos, &consumed);
		#ifdef DEBUG_PRINTFS
		fprintf(stderr, "# consumed %zu, left_over=%zu, batcount=%zu\n", consumed, state.left_over, BATcount(bat));
		#endif
		if (msg != MAL_SUCCEED)
			goto end;
		bs->pos += consumed;
	}

	if (bs->pos < bs->len)
		bailout("unterminated string at end");

	// We've been incrementing bat->batCount directly but there is some
	// bookkeeping that must be maintained as well
	BATsetcount(bat, bat->batCount);

	msg = MAL_SUCCEED;
end:
	release_insert_state(&state);
	ma_close(ma, &ma_state);
	if (bs != NULL) {
		*eof_reached = bs->eof;
		bs->s = NULL;
		bstream_destroy(bs);
	}
	return msg;
}

bool
is_nul_terminated_text(type_record_t *rec)
{
	return rec->loader == load_zero_terminated_text;
}



static str
dump_zero_terminated_text(BAT *bat, stream *s, BUN start, BUN length, bool byteswap)
{
	(void)byteswap;
	static const char mal_operator[] = "sql.export_bin_column";
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
	static const char mal_operator[] = "sql.importColumn";
	str msg = MAL_SUCCEED;
	const blob *nil_value = ATOMnilptr(TYPE_blob);
	blob *buffer = NULL;
	size_t buffer_size = 0;
	union {
		uint64_t length;
		char bytes[8];
	} header;

	*eof_reached = 0;

	/* we know nothing about the ordering of the input data */
	bat->tsorted = false;
	bat->trevsorted = false;
	bat->tkey = false;
	/* keep tno* properties: if they're set they remain valid when
	 * appending */
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
			bat->tnonil = false;
			bat->tnil = true;
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
				allocate += ((~allocate + 1) % 0x100000);   // round up to nearest MiB
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

		if (bunfastapp(bat, value) != GDK_SUCCEED) {
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
	static const char mal_operator[] = "sql.export_bin_column";
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
	{ "bit", "bit", .encoder_trivial=true, .decoder_trivial=true, .validate=validate_bit },

	// vanilla integer types
	{ "sht", "sht", .trivial_if_no_byteswap=true, .decoder=byteswap_sht, .encoder=byteswap_sht, .validate=validate_sht },
	{ "int", "int", .trivial_if_no_byteswap=true, .decoder=byteswap_int, .encoder=byteswap_int, .validate=validate_int },
	{ "lng", "lng", .trivial_if_no_byteswap=true, .decoder=byteswap_lng, .encoder=byteswap_lng, .validate=validate_lng },
	{ "flt", "flt", .trivial_if_no_byteswap=true, .decoder=byteswap_flt, .encoder=byteswap_flt},
	{ "dbl", "dbl", .trivial_if_no_byteswap=true, .decoder=byteswap_dbl, .encoder=byteswap_dbl},
#ifdef HAVE_HGE
	{ "hge", "hge", .trivial_if_no_byteswap=true, .decoder=byteswap_hge, .encoder=byteswap_hge, .validate=validate_hge },
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
can_dump_binary_column(type_record_t *rec)
{
	return rec->encoder_trivial || rec->dumper || rec->encoder;
}
