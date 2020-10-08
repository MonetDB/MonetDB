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

static str
BATattach_str(bstream *in, BAT *bn)
{
	// we have three jobs:
	// 1. scan for the '\0' that terminates a str value.
	// 2. validate the utf-8 encoding.
	// 3. convert \r\n to \n.
	//
	// It might be possible to combine all this into a single scan
	// but it's simpler to first scan for \0 and then when the full string
	// is available, do a validation/conversion scan.

	// The outer loop iterates over input blocks
	while (1) {
		ssize_t nread = bstream_next(in);
		if (nread < 0) {
			return createException(SQL, "BATattach_stream", SQLSTATE(42000) "%s", mnstr_peek_error(in->s));
		}
		if (nread == 0)
			break;

		// the middle loop looks for complete strings
		char *end;
		while ((end = memchr(&in->buf[in->pos], '\0', in->len - in->pos)) != NULL) {
			unsigned char *r = (unsigned char*) &in->buf[in->pos];
			unsigned char *w = (unsigned char*) &in->buf[in->pos];
			const char *s;

			if (*r == 0x80 && *(r+1) == 0) {
				// technically a utf-8 violation but we treat it as the NULL marker
				s = str_nil;
			} else {
				s = &in->buf[in->pos]; // to be validated first

				// the inner loop validates them and converts line endings.
				unsigned int u = 0; // utf-8 state
				while (1) {
					if (u > 0) {
						// must be an utf-8 continuation byte.
						if ((*r & 0xC0) == 0x80)    // 10xx xxxx
							u--;
						else
							goto bad_utf8;
					} else if ((*r & 0xF8) == 0xF0) // 1111_0xxx
						u = 3;
					else if ((*r & 0xF0) == 0xE0)   // 1110_xxxx
						u = 2;
					else if ((*r & 0xE0) == 0xC0)   // 110x xxxx
						u = 1;
					else if ((*r & 0xC0) == 0x80)   // 10xx xxxx
						goto bad_utf8;
					else if (*r == '\r' && *(r+1) == '\n') // convert!
						r++;
					else if (*r == '\0') { // guaranteed to happen.
						*w = '\0';
						break;
					}
					*w++ = *r++;
				}
			}

			if (BUNappend(bn, s, false) != GDK_SUCCEED)
				return createException(SQL, "BATattach_stream", GDK_EXCEPTION);

			in->pos = end - in->buf + 1;
		}
		// If we fall out of the middle loop, part of the buffer may be unconsumed.
		// This is fine, we'll pick it up on the next iteration.
	}

	// It's an error to  have data left after falling out of the outer loop.
	if (in->pos < in->len)
		return createException(SQL, "BATattach_str", SQLSTATE(42000) "unterminated string at end");

	return MAL_SUCCEED;
bad_utf8:
	return createException(SQL, "BATattach_stream", SQLSTATE(42000) "malformed utf-8 byte sequence");
}


static str
BATattach_fixed_width(bstream *in, BAT *bn, str (*converter)(void*, size_t, char*, char*), void *cookie, size_t size_external)
{
	int internal_type = BATttype(bn);
	int storage_type = ATOMstorage(internal_type);
	size_t internal_size = ATOMsize(storage_type);
	while (1) {
		ssize_t nread = bstream_next(in);
		if (nread < 0) {
			return createException(SQL, "BATattach_fixed_width", SQLSTATE(42000) "%s", mnstr_peek_error(in->s));
		}
		if (nread == 0)
			break;

		size_t n = (in->len - in->pos) / size_external;
		BUN count = bn->batCount;
		BUN newCount = count + n;
		if (BATextend(bn, newCount) != GDK_SUCCEED)
			return createException(SQL, "BATattach_fixed_width", GDK_EXCEPTION);

		// future work: parallellize this..
		for (size_t i = 0; i < n; i++) {
			char *external = in->buf + in->pos + i * size_external;
			char *internal = Tloc(bn, count + i);
			str msg = converter(cookie, internal_size, external, internal);
			if (msg != MAL_SUCCEED)
				return msg;
		}

		// All conversions succeeded. Update the bookkeeping.
		bn->batCount += n;
		in->pos += n * size_external;
	}

	// It's an error to  have data left after falling out of the loop.
	if (in->pos < in->len)
		return createException(SQL, "BATattach_str", SQLSTATE(42000) "incomplete value at end");

	BATsetcount(bn, bn->batCount);
	bn->tseqbase = oid_nil;
	bn->tnonil = bn->batCount == 0;
	bn->tnil = false;
	if (bn->batCount <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
	}
	return MAL_SUCCEED;
}

static str
BATattach_as_bytes(int tt, stream *s, BAT *bn, bool *eof_seen)
{
	size_t asz = (size_t) ATOMsize(tt);
	size_t chunk_size = 1<<20;

	bool eof = false;
	while (!eof) {
		assert(chunk_size % asz == 0);
		size_t n = chunk_size / asz;

		// First make some room
		BUN validCount = bn->batCount;
		BUN newCount = validCount + n;
		if (BATextend(bn, newCount) != GDK_SUCCEED)
			return createException(SQL, "BATattach_stream", GDK_EXCEPTION);

		// Read into the newly allocated space
		char *start = Tloc(bn, validCount);
		char *cur = start;
		char *end = Tloc(bn, newCount);
		while (cur < end) {
			ssize_t nread = mnstr_read(s, cur, 1, end - cur);
			if (nread < 0)
				return createException(SQL, "BATattach_stream", SQLSTATE(42000) "%s", mnstr_peek_error(s));
			if (nread == 0) {
				size_t tail = (cur - start) % asz;
				if (tail != 0) {
					return createException(SQL, "BATattach_stream", SQLSTATE(42000) "final item incomplete: %d bytes instead of %d",
						(int) tail, (int) asz);
				}
				eof = true;
				if (eof_seen != NULL)
					*eof_seen = true;
				end = cur;
			}
			cur += (size_t) nread;
		}
		bn->batCount += (cur - start) / asz;
	}

	BATsetcount(bn, bn->batCount);
	bn->tseqbase = oid_nil;
	bn->tnonil = bn->batCount == 0;
	bn->tnil = false;
	if (bn->batCount <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
	}
	return MAL_SUCCEED;
}

static str
convert_timestamp(void *cookie, size_t internal_size, char *external, char *internal)
{
	copy_binary_timestamp *src = (copy_binary_timestamp*) external;
	timestamp *dst = (timestamp*) internal;
	(void)internal_size; assert(internal_size == sizeof(*dst));

	COPY_BINARY_CONVERT_TIMESTAMP_ENDIAN(*src);

	date dt = date_create(src->date.year, src->date.month, src->date.day);
	daytime tm = daytime_create(src->time.hours, src->time.minutes, src->time.seconds, src->time.ms);
	timestamp value = timestamp_create(dt, tm);

	(void)cookie;
	*dst = value;

	return MAL_SUCCEED;
}

static str
convert_date(void *cookie, size_t internal_size, char *external, char *internal)
{
	copy_binary_date *src = (copy_binary_date*) external;
	date *dst = (date*) internal;
	(void)internal_size; assert(internal_size == sizeof(*dst));

	COPY_BINARY_CONVERT_DATE_ENDIAN(*src);

	date value = date_create(src->year, src->month, src->day);

	(void)cookie;
	*dst = value;

	return MAL_SUCCEED;
}

static str
convert_time(void *cookie, size_t internal_size, char *external, char *internal)
{
	copy_binary_time *src = (copy_binary_time*) external;
	timestamp *dst = (timestamp*) internal;
	(void)internal_size; assert(internal_size == sizeof(*dst));

	COPY_BINARY_CONVERT_TIME_ENDIAN(*src);

	daytime value = daytime_create(src->hours, src->minutes, src->seconds, src->ms);

	(void)cookie;
	*dst = value;

	return MAL_SUCCEED;
}

static str
BATattach_stream(BAT **result, const char *colname, int tt, stream *s, BUN size, bool *eof_seen)
{
	str msg = MAL_SUCCEED;
	BAT *bn = NULL;
	bstream *in = NULL;

	bn = COLnew(0, tt, size, TRANSIENT);
	if (bn == NULL) {
		msg = createException(SQL, "BATattach_stream", GDK_EXCEPTION);
		goto end;
	}

	switch (tt) {
		case TYPE_bit:
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_bat:
		case TYPE_int:
		case TYPE_oid:
		case TYPE_flt:
		case TYPE_dbl:
		case TYPE_lng:
#ifdef HAVE_HGE
		case TYPE_hge:
#endif
			msg = BATattach_as_bytes(tt, s, bn, eof_seen);
			break;

		case TYPE_str:
			in = bstream_create(s, 1 << 20);
			if (in == NULL) {
				msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto end;
			}
			msg = BATattach_str(in, bn);
			if (eof_seen != NULL)
				*eof_seen = in->eof;
			break;

		case TYPE_date:
			in = bstream_create(s, 1 << 20);
			if (in == NULL) {
				msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto end;
			}
			msg = BATattach_fixed_width(in, bn, convert_date, NULL, sizeof(copy_binary_date));
			if (eof_seen != NULL)
				*eof_seen = in->eof;
			break;

		case TYPE_daytime:
			in = bstream_create(s, 1 << 20);
			if (in == NULL) {
				msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto end;
			}
			msg = BATattach_fixed_width(in, bn, convert_time, NULL, sizeof(copy_binary_time));
			if (eof_seen != NULL)
				*eof_seen = in->eof;
			break;

		case TYPE_timestamp:
			in = bstream_create(s, 1 << 20);
			if (in == NULL) {
				msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto end;
			}
			msg = BATattach_fixed_width(in, bn, convert_timestamp, NULL, sizeof(copy_binary_timestamp));
			if (eof_seen != NULL)
				*eof_seen = in->eof;
			break;

		default:
			msg = createException(SQL, "sql",
				SQLSTATE(42000) "Type of column %s not supported for BINARY COPY",
				colname);
			goto end;
	}

end:
	if (in != NULL){
		in->s = NULL;
		bstream_destroy(in);
	}
	if (msg == NULL) {
		*result = bn;
		return NULL;
	} else {
		*result = NULL;
		if (bn != NULL)
			BBPreclaim(bn);
		return msg;
	}
}


static str
importColumn(bat *ret, lng *retcnt, const str *method, const str *path, const int *onclient, const lng *nrows)
{
	(void)ret;
	(void)retcnt;
	(void)method;
	(void)path;
	(void)onclient;
	(void)nrows;

	return createException(MAL, "sql.importColumn", SQLSTATE(42000) "COPY BINARY FROM not implemented for type '%s': path=%s onclient=%d nrows=%ld",
		*method, *path, *onclient, *nrows);
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

	return importColumn(ret, retcnt, method, path, onclient, nrows);
}


/* str mvc_bin_import_table_wrap(.., str *sname, str *tname, str *fname..);
 * binary attachment only works for simple binary types.
 * Non-simple types require each line to contain a valid ascii representation
 * of the text terminate by a new-line. These strings are passed to the corresponding
 * atom conversion routines to fill the column.
 */
str
mvc_bin_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	BUN cnt = 0;
	sql_column *cnt_col = NULL;
	bool init = false;
	int i;
	const char *sname = *getArgReference_str(stk, pci, 0 + pci->retc);
	const char *tname = *getArgReference_str(stk, pci, 1 + pci->retc);
	int onclient = *getArgReference_int(stk, pci, 2 + pci->retc);
	sql_schema *s;
	sql_table *t;
	node *n;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((s = mvc_bind_schema(m, sname)) == NULL)
		throw(SQL, "sql.import_table", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (!t)
		throw(SQL, "sql", SQLSTATE(42S02) "Table missing %s", tname);
	if (list_length(t->columns.set) != (pci->argc - (3 + pci->retc)))
		throw(SQL, "sql", SQLSTATE(42000) "Not enough columns found in input file");
	if (2 * pci->retc + 3 != pci->argc)
		throw(SQL, "sql", SQLSTATE(42000) "Not enough output values");

	if (onclient && !cntxt->filetrans) {
		throw(MAL, "sql.copy_from", "cannot transfer files from client");
	}

	backend *be = cntxt->sqlcontext;

	for (i = 0; i < pci->retc; i++)
		*getArgReference_bat(stk, pci, i) = 0;

	for (i = pci->retc + 3, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
		sql_column *col = n->data;
		BAT *c = NULL;
		int tpe = col->type.type->localtype;
		const char *fname = *getArgReference_str(stk, pci, i);

		/* handle the various cases */
		if (strNil(fname)) {
			// no filename for this column, skip for now because we potentially don't know the count yet
			continue;
		}
		if (ATOMvarsized(tpe) && tpe != TYPE_str) {
			msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", *getArgReference_str(stk, pci, i));
			goto bailout;
		}

		if (tpe <= TYPE_str || tpe == TYPE_date || tpe == TYPE_daytime || tpe == TYPE_timestamp) {
			if (onclient) {
				stream *ws = be->mvc->scanner.ws;
				mnstr_write(ws, PROMPT3, sizeof(PROMPT3)-1, 1);
				mnstr_printf(ws, "rb %s\n", fname);
				msg = MAL_SUCCEED;
				mnstr_flush(ws, MNSTR_FLUSH_DATA);
				while (!be->mvc->scanner.rs->eof)
					bstream_next(be->mvc->scanner.rs);
				stream *rs = be->mvc->scanner.rs->s;
				char buf[80];
				if (mnstr_readline(rs, buf, sizeof(buf)) > 1) {
					msg = createException(IO, "sql.attach", "%s", buf);
					goto bailout;
				}
				assert(isa_block_stream(rs));
				assert(isa_block_stream(ws));
				bool eof = false;
				set_prompting(rs, PROMPT2, ws);
				msg = BATattach_stream(&c, col->base.name, col->type.type->localtype, rs, cnt, &eof);
				set_prompting(rs, NULL, NULL);
				if (!eof) {
					// Didn't read everything, probably due to an error.
					// Read until message boundary.
					char buf[8190];
					while (1) {
						ssize_t nread = mnstr_read(rs, buf, 1, sizeof(buf));
						if (nread > 0)
							continue;
						if (nread < 0) {
							// do not overwrite existing error message
							if (msg == NULL)
								msg = createException(
									SQL, "mvc_bin_import_table_wrap",
									SQLSTATE(42000) "while syncing read stream: %s", mnstr_peek_error(rs));
						}
						break;
					}
				}
				mnstr_write(ws, PROMPT3, sizeof(PROMPT3)-1, 1);
				mnstr_flush(ws, MNSTR_FLUSH_DATA);
				if (msg != NULL)
					goto bailout;
			} else {
				stream *s = open_rstream(fname);
				if (s != NULL) {
					msg = BATattach_stream(&c, col->base.name, tpe, s, 0, NULL);
					close_stream(s);
				}
				else
					msg = createException(
						SQL, "mvc_bin_import_table_wrap",
						SQLSTATE(42000) "Failed to attach file %s: %s",
						fname, mnstr_peek_error(NULL));
			}
			if (c == NULL) {
				if (msg == NULL)
					msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", fname);
				goto bailout;
			}
			if (BATsetaccess(c, BAT_READ) != GDK_SUCCEED) {
				BBPreclaim(c);
				msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to set internal access while attaching file %s", fname);
				goto bailout;
			}
		} else {
			msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", fname);
			goto bailout;
		}
		if (init && cnt != BATcount(c)) {
			BUN this_cnt = BATcount(c);
			BBPunfix(c->batCacheid);
			msg = createException(SQL, "sql",
				SQLSTATE(42000) "Binary files for table '%s' have inconsistent counts: "
				"%s has %zu rows, %s has %zu", tname,
				cnt_col->base.name, (size_t)cnt,
				col->base.name, (size_t)this_cnt);
			goto bailout;
		}
		cnt = BATcount(c);
		cnt_col = col;
		init = true;
		*getArgReference_bat(stk, pci, i - (3 + pci->retc)) = c->batCacheid;
		BBPkeepref(c->batCacheid);
	}
	if (init) {
		for (i = pci->retc + 3, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
			// now that we know the BAT count, we can fill in the columns for which no parameters were passed
			sql_column *col = n->data;
			BAT *c = NULL;
			int tpe = col->type.type->localtype;

			const char *fname = *getArgReference_str(stk, pci, i);
			if (strNil(fname)) {
				// fill the new BAT with NULL values
				c = BATconstant(0, tpe, ATOMnilptr(tpe), cnt, TRANSIENT);
				if (c == NULL) {
					msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				*getArgReference_bat(stk, pci, i - (3 + pci->retc)) = c->batCacheid;
				BBPkeepref(c->batCacheid);
			}
		}
	}
	return MAL_SUCCEED;
  bailout:
	for (i = 0; i < pci->retc; i++) {
		bat bid;
		if ((bid = *getArgReference_bat(stk, pci, i)) != 0) {
			BBPrelease(bid);
			*getArgReference_bat(stk, pci, i) = 0;
		}
	}
	return msg;
}
