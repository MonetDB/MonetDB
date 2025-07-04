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

/*
 * author N.J. Nes
 */

#include "monetdb_config.h"
#include "sql_result.h"
#include "str.h"
#include "tablet.h"
#include "gdk_time.h"
#include "bat/res_table.h"
#include "bat/bat_storage.h"
#include "rel_exp.h"
#include "sql_bincopyconvert.h"

#ifndef HAVE_LLABS
#define llabs(x)	((x) < 0 ? -(x) : (x))
#endif

#ifdef _MSC_VER
/* use intrinsic functions on Windows */
#define short_int_SWAP(s)	((short) _byteswap_ushort((unsigned short) (s)))
/* on Windows, long is the same size as int */
#define normal_int_SWAP(s)	((int) _byteswap_ulong((unsigned long) (s)))
#define long_long_SWAP(s)	((lng) _byteswap_uint64((unsigned __int64) (s)))
#else
#define short_int_SWAP(s) ((short)(((0x00ff&(s))<<8) | ((0xff00&(s))>>8)))

#define normal_int_SWAP(i) (((0x000000ff&(i))<<24) | ((0x0000ff00&(i))<<8) | \
			    ((0x00ff0000&(i))>>8)  | ((0xff000000&(i))>>24))
#define long_long_SWAP(l)						\
		((((lng)normal_int_SWAP(l))<<32) |		\
		 (0xffffffff&normal_int_SWAP(l>>32)))
#endif

#ifdef HAVE_HGE
#define huge_int_SWAP(h)								\
		((((hge)long_long_SWAP(h))<<64) |				\
		 (0xffffffffffffffff&long_long_SWAP(h>>64)))
#endif
#define DEC_TOSTR(TYPE)								\
	do {											\
		char buf[64];								\
		TYPE v = *(const TYPE *) a;					\
		int scale = (int) (ptrdiff_t) extra;		\
		int cur = 63, i, done = 0;					\
		int neg = v < 0;							\
		ssize_t l;									\
		if (is_##TYPE##_nil(v)) {					\
			if (*Buf == NULL || *len < 5){			\
				GDKfree(*Buf);						\
				*len = 5;							\
				*Buf = GDKzalloc(*len);				\
				if (*Buf == NULL) {					\
					return -1;						\
				}									\
			}										\
			strcpy(*Buf, "NULL");					\
			return 4;								\
		}											\
		if (v<0)									\
			v = -v;									\
		buf[cur--] = 0;								\
		if (scale){									\
			for (i=0; i<scale; i++) {				\
				buf[cur--] = (char) (v%10 + '0');	\
				v /= 10;							\
			}										\
			buf[cur--] = '.';						\
		}											\
		while (v) {									\
			buf[cur--] = (char ) (v%10 + '0');		\
			v /= 10;								\
			done = 1;								\
		}											\
		if (!done)									\
			buf[cur--] = '0';						\
		if (neg)									\
			buf[cur--] = '-';						\
		l = (64-cur-1);								\
		if (*Buf == NULL || (ssize_t) *len < l) {	\
			GDKfree(*Buf);							\
			*len = (size_t) l+1;					\
			*Buf = GDKzalloc(*len);					\
			if (*Buf == NULL) {						\
				return -1;							\
			}										\
		}											\
		strcpy(*Buf, buf+cur+1);					\
		return l-1;									\
	} while (0)

static ssize_t
dec_tostr(void *extra, char **Buf, size_t *len, int type, const void *a)
{
	/* support dec map to bte, sht, int and lng */
	if (type == TYPE_bte) {
		DEC_TOSTR(bte);
	} else if (type == TYPE_sht) {
		DEC_TOSTR(sht);
	} else if (type == TYPE_int) {
		DEC_TOSTR(int);
	} else if (type == TYPE_lng) {
		DEC_TOSTR(lng);
#ifdef HAVE_HGE
	} else if (type == TYPE_hge) {
		DEC_TOSTR(hge);
#endif
	} else {
		GDKerror("Decimal cannot be mapped to %s\n", ATOMname(type));
	}
	return -1;
}

struct time_res {
	int fraction;
	int has_tz;
	lng timezone;
};

static ssize_t
sql_time_tostr(void *TS_RES, char **buf, size_t *len, int type, const void *A)
{
	struct time_res *ts_res = TS_RES;
	ssize_t len1;
	size_t big = 128;
	char buf1[128], *s1 = buf1, *s;
	daytime tmp;

	(void) type;
	tmp = *(const daytime *) A;
	if (ts_res->has_tz)
		tmp = daytime_add_usec_modulo(tmp, ts_res->timezone * 1000);

	len1 = daytime_precision_tostr(&s1, &big, tmp, ts_res->fraction, true);
	if (len1 < 0)
		return -1;
	if (len1 == 3 && strcmp(s1, "nil") == 0) {
		if (*len < 4 || *buf == NULL) {
			GDKfree(*buf);
			*buf = GDKzalloc(*len = 4);
			if (*buf == NULL)
				return -1;
		}
		strcpy(*buf, "nil");
		return len1;
	}

	if (*buf == NULL || *len < (size_t) len1 + 8) {
		GDKfree(*buf);
		*buf = (str) GDKzalloc(*len = len1 + 8);
		if (*buf == NULL) {
			return -1;
		}
	}
	s = *buf;
	strcpy(s, buf1);
	s += len1;

	if (ts_res->has_tz) {
		lng timezone = llabs(ts_res->timezone / 60000);
		s += sprintf(s, "%c%02d:%02d",
			     (ts_res->timezone >= 0) ? '+' : '-',
			     (int) (timezone / 60), (int) (timezone % 60));
	}
	return (ssize_t) (s - *buf);
}

static ssize_t
sql_timestamp_tostr(void *TS_RES, char **buf, size_t *len, int type, const void *A)
{
	struct time_res *ts_res = TS_RES;
	ssize_t len1, len2;
	size_t big = 128;
	char buf1[128], buf2[128], *s, *s1 = buf1, *s2 = buf2;
	timestamp tmp;
	lng timezone = ts_res->timezone;
	date days;
	daytime usecs;

	(void) type;
	tmp = *(const timestamp *)A;
	if (ts_res->has_tz) {
		tmp = timestamp_add_usec(tmp, timezone * 1000);
	}
	days = timestamp_date(tmp);
	usecs = timestamp_daytime(tmp);
	len1 = date_tostr(&s1, &big, &days, true);
	len2 = daytime_precision_tostr(&s2, &big, usecs, ts_res->fraction, true);
	if (len1 < 0 || len2 < 0) {
		GDKfree(s1);
		GDKfree(s2);
		return -1;
	}

	if ((len1 == 3 && strcmp(s1, "nil") == 0) ||
	    (len2 == 3 && strcmp(s2, "nil") == 0)) {
		if (*len < 4 || *buf == NULL) {
			GDKfree(*buf);
			*buf = GDKzalloc(*len = 4);
			if (*buf == NULL)
				return -1;
		}
		strcpy(*buf, "nil");
		return len1;
	}

	if (*buf == NULL || *len < (size_t) len1 + (size_t) len2 + 8) {
		GDKfree(*buf);
		*buf = (str) GDKzalloc(*len = (size_t) (len1 + len2 + 8));
		if (*buf == NULL) {
			return -1;
		}
	}
	s = *buf;
	strcpy(s, buf1);
	s += len1;
	*s++ = ' ';
	strcpy(s, buf2);
	s += len2;
	s[0] = 0;

	if (ts_res->has_tz) {
		timezone = ts_res->timezone / 60000;
		*s++ = (ts_res->timezone >= 0) ? '+' : '-';
		sprintf(s, "%02d:%02d", (int) (llabs(timezone) / 60), (int) (llabs(timezone) % 60));
		s += 5;
	}
	return (ssize_t) (s - *buf);
}

static int
bat_max_strlength(BAT *b)
{
	BUN p, q;
	int l = 0;
	int max = 0;
	BATiter bi = bat_iterator(b);

	BATloop(b, p, q) {
		l = UTF8_strwidth((const char *) BUNtvar(bi, p));

		if (is_int_nil(l))
			l = 0;
		if (l > max)
			max = l;
	}
	bat_iterator_end(&bi);
	return max;
}

#define bat_max_length(TPE, HIGH) \
static size_t \
bat_max_##TPE##length(BAT *b) \
{ \
	BUN p, q; \
	HIGH max = 0, min = 0; \
	size_t ret = 0; \
	BATiter bi = bat_iterator(b); \
	const TPE *restrict vals = (const TPE *) bi.base; \
 \
	BATloop(b, p, q) { \
		HIGH m = 0; \
		TPE l = vals[p]; \
 \
		if (!is_##TPE##_nil(l)) \
			m = l; \
		if (m > max) \
			max = m; \
		if (m < min) \
			min = m; \
	} \
	bat_iterator_end(&bi); \
	if (-min > max / 10) { \
		max = -min; \
		ret++;		/* '-' */ \
	} \
	while (max /= 10) \
		ret++; \
	ret++; \
	return ret; \
}

bat_max_length(bte, lng)
bat_max_length(sht, lng)
bat_max_length(int, lng)
bat_max_length(lng, lng)
#ifdef HAVE_HGE
bat_max_length(hge, hge)
#endif

#define DEC_FRSTR(X)													\
	do {																\
		sql_column *col = c->extra;										\
		sql_subtype *t = &col->type;									\
		unsigned int scale = t->scale;									\
		unsigned int i;													\
		bool neg = false;												\
		X *r;															\
		X res = 0;														\
		while(isspace((unsigned char) *s))								\
			s++;														\
		if (*s == '-'){													\
			neg = true;													\
			s++;														\
		} else if (*s == '+'){											\
			s++;														\
		}																\
		for (i = 0; *s && *s != c->decsep && ((res == 0 && *s == '0') || i < t->digits - t->scale); s++) { \
			if (c->decskip && *s == c->decskip)							\
				continue;												\
			if (!isdigit((unsigned char) *s))							\
				break;													\
			res *= 10;													\
			res += (*s-'0');											\
			if (res)													\
				i++;													\
		}																\
		if (*s == c->decsep) {											\
			s++;														\
			while (*s && scale > 0) {									\
				if (isdigit((unsigned char) *s)) {						\
					res *= 10;											\
					res += *s++ - '0';									\
					scale--;											\
				} else if (c->decskip && *s == c->decskip) {			\
					s++;												\
				} else {												\
					break;												\
				}														\
			}															\
		}																\
		while(*s && isspace((unsigned char) *s))						\
			s++;														\
		while (scale > 0) {												\
			res *= 10;													\
			scale--;													\
		}																\
		if (*s)															\
			return NULL;												\
		r = c->data;													\
		if (r == NULL &&												\
		    (r = GDKzalloc(sizeof(X))) == NULL)							\
			return NULL;												\
		c->data = r;													\
		if (neg)														\
			*r = -res;													\
		else															\
			*r = res;													\
		return (void *) r;												\
	} while (0)

static void *
dec_frstr(Column *c, int type, const char *s)
{
	assert(c->decsep != '\0');

	/* support dec map to bte, sht, int and lng */
	if( strcmp(s,"nil")== 0)
		return NULL;
	if (type == TYPE_bte) {
		DEC_FRSTR(bte);
	} else if (type == TYPE_sht) {
		DEC_FRSTR(sht);
	} else if (type == TYPE_int) {
		DEC_FRSTR(int);
	} else if (type == TYPE_lng) {
		DEC_FRSTR(lng);
#ifdef HAVE_HGE
	} else if (type == TYPE_hge) {
		DEC_FRSTR(hge);
#endif
	}
	return NULL;
}

static void *
sec_frstr(Column *c, int type, const char *s)
{
	/* read a sec_interval value
	 * this knows that the stored scale is always 3 */
	unsigned int i, neg = 0;
	lng *r;
	lng res = 0;

	(void) c;
	(void) type;
	assert(type == TYPE_lng);

	if (*s == '-') {
		neg = 1;
		s++;
	} else if (*s == '+') {
		neg = 0;
		s++;
	}
	for (i = 0; i < (19 - 3) && *s && *s != c->decsep; i++, s++) {
		if (c->decskip && *s == c->decskip) {
			i--;
			continue;
		}
		if (!isdigit((unsigned char) *s))
			return NULL;
		res *= 10;
		res += (*s - '0');
	}
	i = 0;
	if (*s) {
		if (*s != c->decsep)
			return NULL;
		s++;
		for (; *s && i < 3; i++, s++) {
			if (c->decskip && *s == c->decskip) {
				i--;
				continue;
			}
			if (!isdigit((unsigned char) *s))
				return NULL;
			res *= 10;
			res += (*s - '0');
		}
	}
	if (*s)
		return NULL;
	for (; i < 3; i++) {
		res *= 10;
	}
	r = c->data;
	if (r == NULL && (r = (lng *) GDKzalloc(sizeof(lng))) == NULL)
		return NULL;
	c->data = r;
	if (neg)
		*r = -res;
	else
		*r = res;
	return (void *) r;
}

static void *
fltdbl_frStr(Column *c, int type, const char *s)
{
	// The regular fltFromStr/dblFromStr functions do not take decimal commas
	// and thousands separators into account. When these are in use, this
	// function first converts them to decimal dots and empty strings,
	// respectively. We use a fixed size buffer so abnormally long floats such
	// as
	// +00000000000000000000000000000000000000000000000000000000000000000000001.5e1
	// will be rejected.

	// According to Stack Overflow https://stackoverflow.com/questions/1701055/what-is-the-maximum-length-in-chars-needed-to-represent-any-double-value
	// 24 bytes is a reasonable buffer but we'll make it a bit larger.
	char tmp[120];
	if (c->decskip || c->decsep != '.') {
		char *p = &tmp[0];

		while (GDKisspace(*s))
			s++;
		while (*s != '\0') {
			if (p >= tmp + sizeof(tmp) - 1) {
				// If the input is this big it's probably an error.
				// Exception: only whitespace remains.
				while (GDKisspace(*s))
					s++;
				if (*s == '\0') {
					// there was only trailing whitespace
					break;
				} else {
					// not just trailing whitespace, abort!
					return NULL;
				}
			}
			char ch = *s++;
			if (ch == c->decskip) {
				continue;
			} else if (ch == c->decsep) {
				ch = '.';
			} else if (ch == '.') {
				// We're mapping c->decsep to '.', if there are already
				// periods in the input we're losing information
				return NULL;
			}
			*p++ = ch;
		}
		// If we're here either we either encountered the end of s or the buffer is
		// full. In the latter case we still need to write the NUL.
		// We left room for it.
		*p = '\0';

		// now process the converted text rather than the original
		s = &tmp[0];
	}

	ssize_t len = (*BATatoms[type].atomFromStr) (s, &c->len, &c->data, false);
	return (len > 0) ? c->data : NULL;
}

/* Literal parsing for SQL all pass through this routine */
static void *
_ASCIIadt_frStr(Column *c, int type, const char *s)
{
	ssize_t len;

	len = (*BATatoms[type].atomFromStr) (s, &c->len, &c->data, false);
	if (len < 0)
		return NULL;
	switch (type) {
	case TYPE_bte:
	case TYPE_int:
	case TYPE_lng:
	case TYPE_sht:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
		if (len == 0 || s[len]) {
			/* decimals can be converted to integers when *.000 */
			if (s[len++] == '.') {
				while (s[len] == '0')
					len++;
				if (s[len] == 0)
					return c->data;
			}
			return NULL;
		}
		break;
	case TYPE_str: {
		sql_column *col = (sql_column *) c->extra;

		s = c->data;
		if (col->type.digits > 0 && len > 0 && !strNil(s) && UTF8_strlen(s) > (int) col->type.digits) {
			return NULL;
		}
		break;
	}
	default:
		break;
	}
	return c->data;
}


static ssize_t
_ASCIIadt_toStr(void *extra, char **buf, size_t *len, int type, const void *a)
{
	if (type == TYPE_str) {
		Column *c = extra;
		char *dst;
		const char *src = a;
		size_t l = escapedStrlen(src, c->sep, c->rsep, c->quote), l2 = 0;

		if (c->quote)
			l = escapedStrlen(src, NULL, NULL, c->quote);
		else
			l = escapedStrlen(src, c->sep, c->rsep, 0);
		if (l + 3 > *len) {
			GDKfree(*buf);
			*len = 2 * l + 3;
			*buf = GDKzalloc(*len);
			if (*buf == NULL) {
				return -1;
			}
		}
		dst = *buf;
		if (c->quote) {
			dst[0] = c->quote;
			l2 = 1;
			l = escapedStr(dst + l2, src, *len - l2, NULL, NULL, c->quote);
		} else {
			l = escapedStr(dst + l2, src, *len - l2, c->sep, c->rsep, 0);
		}
		if (l2) {
			dst[l + l2] = c->quote;
			l2++;
		}
		dst[l + l2] = 0;
		return l + l2;
	} else {
		return (*BATatoms[type].atomToStr) (buf, len, a, true);
	}
}


static int
has_whitespace(const char *s)
{
	if (*s == ' ' || *s == '\t')
		return 1;
	while (*s)
		s++;
	s--;
	if (*s == ' ' || *s == '\t')
		return 1;
	return 0;
}

str
mvc_import_table(Client cntxt, BAT ***bats, mvc *m, bstream *bs, sql_table *t, const char *sep, const char *rsep, const char *ssep, const char *ns, lng sz, lng offset, int best, bool from_stdin, bool escape, const char *decsep, const char *decskip)
{
	int i = 0, j;
	node *n;
	Tablet as;
	Column *fmt;
	str msg = MAL_SUCCEED;

	*bats =0;	// initialize the receiver

	if (!bs)
		throw(IO, "sql.copy_from", SQLSTATE(42000) "No stream (pointer) provided");
	if (mnstr_errnr(bs->s) != MNSTR_NO__ERROR) {
		mnstr_error_kind errnr = mnstr_errnr(bs->s);
		const char *stream_msg = mnstr_peek_error(bs->s);
		msg = createException(IO, "sql.copy_from", SQLSTATE(42000) "Stream not open %s: %s", mnstr_error_kind_name(errnr), stream_msg ? stream_msg : "unknown error");
		return msg;
	}
	if (offset < 0 || offset > (lng) BUN_MAX)
		throw(IO, "sql.copy_from", SQLSTATE(42000) "Offset out of range");

	if (offset > 0)
		offset--;
	if (ol_first_node(t->columns)) {
		stream *out = m->scanner.ws;

		as = (Tablet) {
			.nr_attrs = ol_length(t->columns),
			.nr = (sz < 1) ? BUN_NONE : (BUN) sz,
			.offset = (BUN) offset,
			.error = NULL,
			.tryall = 0,
			.complaints = NULL,
			.filename = m->scanner.rs == bs ? NULL : "",
		};
		fmt = GDKzalloc(sizeof(Column) * (as.nr_attrs + 1));
		if (fmt == NULL)
			throw(IO, "sql.copy_from", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		as.format = fmt;
		if (!isa_block_stream(bs->s))
			out = NULL;

		for (n = ol_first_node(t->columns), i = 0; n; n = n->next, i++) {
			sql_column *col = n->data;

			fmt[i].name = col->base.name;
			fmt[i].sep = (n->next) ? sep : rsep;
			fmt[i].rsep = rsep;
			fmt[i].seplen = _strlen(fmt[i].sep);
			fmt[i].decsep = decsep[0],
			fmt[i].decskip = decskip != NULL ? decskip[0] : '\0',
			fmt[i].type = sql_subtype_string(m->ta, &col->type);
			fmt[i].adt = ATOMindex(col->type.type->impl);
			fmt[i].tostr = &_ASCIIadt_toStr;
			fmt[i].frstr = &_ASCIIadt_frStr;
			fmt[i].extra = col;
			fmt[i].len = ATOMlen(fmt[i].adt, ATOMnilptr(fmt[i].adt));
			fmt[i].data = GDKzalloc(fmt[i].len);
			if(fmt[i].data == NULL || fmt[i].type == NULL) {
				for (j = 0; j < i; j++) {
					GDKfree(fmt[j].data);
					BBPunfix(fmt[j].c->batCacheid);
				}
				GDKfree(fmt[i].data);
				GDKfree(fmt);
				throw(IO, "sql.copy_from", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			fmt[i].c = NULL;
			fmt[i].ws = !has_whitespace(fmt[i].sep);
			fmt[i].quote = ssep ? ssep[0] : 0;
			fmt[i].nullstr = ns;
			fmt[i].null_length = strlen(ns);
			fmt[i].nildata = ATOMnilptr(fmt[i].adt);
			fmt[i].skip = (col->base.name[0] == '%');
			if (col->type.type->eclass == EC_DEC) {
				fmt[i].tostr = &dec_tostr;
				fmt[i].frstr = &dec_frstr;
			} else if (col->type.type->eclass == EC_SEC) {
				fmt[i].tostr = &dec_tostr;
				fmt[i].frstr = &sec_frstr;
			} else if (col->type.type->eclass == EC_FLT) {
				// no need to override .tostr, only .frstr
				fmt[i].frstr = &fltdbl_frStr;
			}
			fmt[i].size = ATOMsize(fmt[i].adt);
		}
		if ((msg = TABLETcreate_bats(&as, (BUN) (sz < 0 ? 1000 : sz))) == MAL_SUCCEED){
			if (!sz || (SQLload_file(cntxt, &as, bs, out, sep, rsep, ssep ? ssep[0] : 0, offset, sz, best, from_stdin, t->base.name, escape) != BUN_NONE &&
				(best || !as.error))) {
				*bats = (BAT**) GDKzalloc(sizeof(BAT *) * as.nr_attrs);
				if ( *bats == NULL){
					TABLETdestroy_format(&as);
					throw(IO, "sql.copy_from", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				msg = TABLETcollect(*bats,&as);
			}
		}
		if (as.error) {
			if( !best) msg = createException(SQL, "sql.copy_from", SQLSTATE(42000) "Failed to import table '%s', %s", t->base.name, getExceptionMessage(as.error));
			freeException(as.error);
			as.error = NULL;
		}
		for (n = ol_first_node(t->columns), i = 0; n; n = n->next, i++) {
			fmt[i].sep = NULL;
			fmt[i].rsep = NULL;
			fmt[i].nullstr = NULL;
		}
		TABLETdestroy_format(&as);
	}
	return msg;
}

/*
 * mvc_export_result dumps the sql header information and the
 * first part (reply_size) of the result set. It should be produced in Monet format to
 * enable mapi to work with it.
 */

static int
mvc_export_warning(stream *s, str w)
{
	str tmp = NULL;
	while (w != NULL && *w != '\0') {
		if ((tmp = strchr(w, (int) '\n')) != NULL)
			*tmp++ = '\0';
		if (mnstr_printf(s, "#%s", w) < 0)
			return -4;
		w = tmp;
	}
	return 1;
}

static int
mvc_export_binary_bat(stream *s, BAT* bn, bstream *in)
{
	BATiter bni = bat_iterator(bn);
	bool sendtheap = bni.type != TYPE_void, sendtvheap = sendtheap && bni.vh;

	if (mnstr_printf(s, /*JSON*/"{"
					 "\"version\":1,"
					 "\"ttype\":%d,"
					 "\"hseqbase\":" OIDFMT ","
					 "\"tseqbase\":" OIDFMT ","
					 "\"tsorted\":%d,"
					 "\"trevsorted\":%d,"
					 "\"tkey\":%d,"
					 "\"tnonil\":%d,"
					 "\"tdense\":%d,"
					 "\"size\":" BUNFMT ","
					 "\"tailsize\":%zu,"
					 "\"theapsize\":%zu"
					 "}\n",
					 bni.type,
					 bn->hseqbase, bn->tseqbase,
					 bni.sorted, bni.revsorted,
					 bni.key,
					 bni.nonil,
					 BATtdensebi(&bni),
					 bn->batCount,
					 sendtheap ? (size_t)bni.count << bni.shift : 0,
					 sendtvheap && bni.count > 0 ? bni.vhfree : 0) < 0) {
		bat_iterator_end(&bni);
		return -4;
	}

	if (sendtheap && bni.count > 0) {
		if (mnstr_write(s, /* tail */ bni.base, bni.count * bni.width, 1) < 1) {
			bat_iterator_end(&bni);
			return -4;
		}
		if (sendtvheap && mnstr_write(s, /* tvheap */ bni.vh->base, bni.vhfree, 1) < 1) {
			bat_iterator_end(&bni);
			return -4;
		}
	}
	bat_iterator_end(&bni);
	if (bstream_getoob(in))
		return -5;
	return 0;
}

static int
create_prepare_result(backend *b, cq *q, int nrows)
{
	int error = 0;

	BAT* btype		= COLnew(0, TYPE_str, nrows, TRANSIENT);
	BAT* bimpl		= COLnew(0, TYPE_str, nrows, TRANSIENT);
	BAT* bdigits	= COLnew(0, TYPE_int, nrows, TRANSIENT);
	BAT* bscale		= COLnew(0, TYPE_int, nrows, TRANSIENT);
	BAT* bschema	= COLnew(0, TYPE_str, nrows, TRANSIENT);
	BAT* btable		= COLnew(0, TYPE_str, nrows, TRANSIENT);
	BAT* bcolumn	= COLnew(0, TYPE_str, nrows, TRANSIENT);
	node *n;

	const int nr_columns = (b->client->protocol == PROTOCOL_COLUMNAR || GDKembedded()) ? 7 : 6;

	int len1 = 0, len4 = 0, len5 = 0, len6 = 0, len7 =0;	/* column widths */
	int len2 = 1, len3 = 1;
	sql_arg *a;
	sql_subtype *t;
	sql_rel *r = q->rel;

	if (!btype || !bimpl || !bdigits || !bscale || !bschema || !btable || !bcolumn) {
		error = -1;
		goto wrapup;
	}

	if (r && (is_topn(r->op) || is_sample(r->op)))
		r = r->l;
	if (q->type != Q_UPDATE && r && is_project(r->op) && r->exps) {
		unsigned int max2 = 10, max3 = 10;	/* to help calculate widths */
		nrows += list_length(r->exps);

		for (n = r->exps->h; n; n = n->next) {
			const char *name = NULL, *rname = NULL, *schema = NULL;
			sql_exp *e = n->data;
			int slen;

			t = exp_subtype(e);
			slen = (int) strlen(t->type->base.name);
			if (slen > len1)
				len1 = slen;
			while (t->digits >= max2) {
				len2++;
				max2 *= 10;
			}
			while (t->scale >= max3) {
				len3++;
				max3 *= 10;
			}
			rname = exp_relname(e);
			if (!rname && e->type == e_column && e->l)
				rname = e->l;
			slen = name ? (int) strlen(name) : 0;
			if (slen > len5)
				len5 = slen;
			name = exp_name(e);
			if (!name && e->type == e_column && e->r)
				name = e->r;
			slen = name ? (int) strlen(name) : 0;
			if (slen > len6)
				len6 = slen;
			slen = (int) strlen(t->type->impl);
			if (slen > len7)
				len7 = slen;

			if (!schema)
				schema = "";

			if (!rname)
				rname = "";

			if (!name)
				name = "";

			if (	BUNappend(btype,	t->type->base.name	, false) != GDK_SUCCEED ||
					BUNappend(bimpl,	t->type->impl		, false) != GDK_SUCCEED ||
					BUNappend(bdigits,	&t->digits			, false) != GDK_SUCCEED ||
					BUNappend(bscale,	&t->scale			, false) != GDK_SUCCEED ||
					BUNappend(bschema,	schema				, false) != GDK_SUCCEED ||
					BUNappend(btable,	rname				, false) != GDK_SUCCEED ||
					BUNappend(bcolumn,	name				, false) != GDK_SUCCEED) {
				error = -3;
				goto wrapup;
			}
		}
	}

	if (q->f->ops) {
		int i;

		for (n = q->f->ops->h, i = 0; n; n = n->next, i++) {
			a = n->data;
			t = &a->type;

			if (	BUNappend(btype,	t->type->base.name	, false) != GDK_SUCCEED ||
					BUNappend(bimpl,	t->type->impl		, false) != GDK_SUCCEED ||
					BUNappend(bdigits,	&t->digits			, false) != GDK_SUCCEED ||
					BUNappend(bscale,	&t->scale			, false) != GDK_SUCCEED ||
					BUNappend(bschema,	str_nil				, false) != GDK_SUCCEED ||
					BUNappend(btable,	str_nil				, false) != GDK_SUCCEED ||
					BUNappend(bcolumn,	str_nil				, false) != GDK_SUCCEED) {
				error = -3;
				goto wrapup;
			}
		}
	}

	// A little hack to inform the result receiver of the name of the compiled mal program.
	if (b->client->protocol == PROTOCOL_COLUMNAR) {
		if (	BUNappend(btype,	str_nil		, false) != GDK_SUCCEED ||
				BUNappend(bimpl,	str_nil		, false) != GDK_SUCCEED ||
				BUNappend(bdigits,	&int_nil	, false) != GDK_SUCCEED ||
				BUNappend(bscale,	&int_nil	, false) != GDK_SUCCEED ||
				BUNappend(bschema,	str_nil		, false) != GDK_SUCCEED ||
				BUNappend(btable,	q->f->imp	, false) != GDK_SUCCEED ||
				BUNappend(bcolumn,	str_nil		, false) != GDK_SUCCEED) {
			error = -3;
			goto wrapup;
		}
	}

	b->results = res_table_create(
							b->mvc->session->tr,
							b->result_id++,
							b->mb? b->mb->tag: 0 /*TODO check if this is sensible*/,
							nr_columns,
							Q_PREPARE,
							b->results);
	if (!b->results) {
		error = -1;
		goto wrapup;
	}

	if (	mvc_result_column(b, ".prepare", "type"		, "varchar",	len1, 0, btype	) ||
			mvc_result_column(b, ".prepare", "digits"	, "int",		len2, 0, bdigits) ||
			mvc_result_column(b, ".prepare", "scale"	, "int",		len3, 0, bscale	) ||
			mvc_result_column(b, ".prepare", "schema"	, "varchar",	len4, 0, bschema) ||
			mvc_result_column(b, ".prepare", "table"	, "varchar",	len5, 0, btable	) ||
			mvc_result_column(b, ".prepare", "column"	, "varchar",	len6, 0, bcolumn)) {
		error = -1;
		goto wrapup;
	}

	if ((b->client->protocol == PROTOCOL_COLUMNAR || GDKembedded()) && mvc_result_column(b, "prepare", "impl" , "varchar", len7, 0, bimpl))
		error = -1;

	wrapup:
		BBPreclaim(btype);
		BBPreclaim(bdigits);
		BBPreclaim(bimpl);
		BBPreclaim(bscale);
		BBPreclaim(bschema);
		BBPreclaim(btable);
		BBPreclaim(bcolumn);
		if (error < 0 && b->results) {
			res_table_destroy(b->results);
			b->results = NULL;
		}
		return error;
}

int
mvc_export_prepare(backend *b, stream *out)
{
	cq *q = b->q;
	int nparam = q->f->ops ? list_length(q->f->ops) : 0;
	int nrows = nparam, res;

	if ((res = create_prepare_result(b, q, nrows)) < 0)
		return res;

	return mvc_export_result(b, out, b->results->id /*TODO is this right?*/, true, 0 /*TODO*/, 0 /*TODO*/);
}

/*
 * improved formatting of positive integers
 */

static ssize_t
mvc_send_bte(stream *s, bte cnt)
{
	char buf[50], *b;
	int neg = cnt < 0;
	if (neg)
		cnt = -cnt;
	b = buf + 49;
	do {
		*b-- = (char) ('0' + (cnt % 10));
		cnt /= 10;
	} while (cnt > 0);
	if (neg)
		*b = '-';
	else
		b++;
	return mnstr_write(s, b, 50 - (b - buf), 1);
}

static ssize_t
mvc_send_sht(stream *s, sht cnt)
{
	char buf[50], *b;
	int neg = cnt < 0;
	if (neg)
		cnt = -cnt;
	b = buf + 49;
	do {
		*b-- = (char) ('0' + (cnt % 10));
		cnt /= 10;
	} while (cnt > 0);
	if (neg)
		*b = '-';
	else
		b++;
	return mnstr_write(s, b, 50 - (b - buf), 1);
}

static ssize_t
mvc_send_int(stream *s, int cnt)
{
	char buf[50], *b;
	int neg = cnt < 0;
	if (neg)
		cnt = -cnt;
	b = buf + 49;
	do {
		*b-- = (char) ('0' + (cnt % 10));
		cnt /= 10;
	} while (cnt > 0);
	if (neg)
		*b = '-';
	else
		b++;
	return mnstr_write(s, b, 50 - (b - buf), 1);
}

static ssize_t
mvc_send_lng(stream *s, lng cnt)
{
	char buf[50], *b;
	int neg = cnt < 0;
	if (neg)
		cnt = -cnt;
	b = buf + 49;
	do {
		*b-- = (char) ('0' + (cnt % 10));
		cnt /= 10;
	} while (cnt > 0);
	if (neg)
		*b = '-';
	else
		b++;
	return mnstr_write(s, b, 50 - (b - buf), 1);
}

#ifdef HAVE_HGE
static ssize_t
mvc_send_hge(stream *s, hge cnt)
{
	char buf[50], *b;
	int neg = cnt <0;
	if(neg) cnt = -cnt;
	b= buf+49;
	do{
		*b--= (char) ('0'+ (cnt % 10));
		cnt /=10;
	} while(cnt>0);
	if( neg)
		*b = '-';
	else b++;
	return mnstr_write(s, b, 50 - (b - buf), 1);
}
#endif

ssize_t
convert2str(mvc *m, sql_class eclass, int d, int sc, int has_tz, const void *p, int mtype, char **buf, size_t *len)
{
	ssize_t l = 0;

	if (!p || ATOMcmp(mtype, ATOMnilptr(mtype), p) == 0) {
		(*buf)[0] = '\200';
		(*buf)[1] = 0;
	} else if (eclass == EC_DEC) {
		l = dec_tostr((void *) (ptrdiff_t) sc, buf, len, mtype, p);
	} else if (eclass == EC_TIME || eclass == EC_TIME_TZ) {
		struct time_res ts_res;
		ts_res.has_tz = has_tz;
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_time_tostr((void *) &ts_res, buf, len, mtype, p);
	} else if (eclass == EC_TIMESTAMP || eclass == EC_TIMESTAMP_TZ) {
		struct time_res ts_res;
		ts_res.has_tz = has_tz;
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_timestamp_tostr((void *) &ts_res, buf, len, mtype, p);
	} else if (eclass == EC_SEC) {
		l = dec_tostr((void *) (ptrdiff_t) 3, buf, len, mtype, p);
	} else if (eclass == EC_BIT) {
		bit b = *(bit *) p;
		if (*len == 0 || *len > 5) {
			if (b) {
				strcpy(*buf, "true");
				l = 4;
			} else {
				strcpy(*buf, "false");
				l = 5;
			}
		} else {
			(*buf)[0] = b?'t':'f';
			(*buf)[1] = 0;
			l = 1;
		}
	} else {
		l = (*BATatoms[mtype].atomToStr) (buf, len, p, false);
	}
	return l;
}

static int
export_value(mvc *m, stream *s, sql_class eclass, const char *sqlname, int d, int sc, ptr p, int mtype, char **buf, size_t *len, const char *ns)
{
	int ok = 0;
	ssize_t l = 0;

	if (!p || ATOMcmp(mtype, ATOMnilptr(mtype), p) == 0) {
		if (mnstr_write(s, ns, strlen(ns), 1) < 1)
			ok = -4;
	} else if (eclass == EC_DEC) {
		l = dec_tostr((void *) (ptrdiff_t) sc, buf, len, mtype, p);
		if (l > 0 && mnstr_write(s, *buf, l, 1) < 1)
			ok = -4;
	} else if (eclass == EC_TIME || eclass == EC_TIME_TZ) {
		struct time_res ts_res;
		ts_res.has_tz = (strcmp(sqlname, "timetz") == 0);
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_time_tostr((void *) &ts_res, buf, len, mtype, p);
		if (l >= 0 && mnstr_write(s, *buf, l, 1) < 1)
			ok = -4;
	} else if (eclass == EC_TIMESTAMP || eclass == EC_TIMESTAMP_TZ) {
		struct time_res ts_res;
		ts_res.has_tz = (strcmp(sqlname, "timestamptz") == 0);
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_timestamp_tostr((void *) &ts_res, buf, len, mtype, p);
		if (l >= 0 && mnstr_write(s, *buf, l, 1) < 1)
			ok = -4;
	} else if (eclass == EC_SEC) {
		l = dec_tostr((void *) (ptrdiff_t) 3, buf, len, mtype, p);
		if (l >= 0 && mnstr_write(s, *buf, l, 1) < 1)
			ok = -4;
	} else {
		switch (mtype) {
		case TYPE_bte:
			if (mvc_send_bte(s, *(bte *) p) < 1)
				ok = -4;
			break;
		case TYPE_sht:
			if (mvc_send_sht(s, *(sht *) p) < 1)
				ok = -4;
			break;
		case TYPE_int:
			if (mvc_send_int(s, *(int *) p) < 1)
				ok = -4;
			break;
		case TYPE_lng:
			if (mvc_send_lng(s, *(lng *) p) < 1)
				ok = -4;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			if (mvc_send_hge(s, *(hge *) p) < 1)
				ok = -4;
			break;
#endif
		default:
			l = (*BATatoms[mtype].atomToStr) (buf, len, p, true);
			if (l >= 0 && mnstr_write(s, *buf, l, 1) < 1)
				ok = -4;
		}
	}
	return ok;
}

static int
mvc_export_row(backend *b, stream *s, res_table *t, const char *btag, const char *sep, const char *rsep, const char *ssep, const char *ns)
{
	mvc *m = b->mvc;
	size_t seplen = strlen(sep);
	size_t rseplen = strlen(rsep);
	char *buf = NULL;
	size_t len = 0;
	int i, ok = 1;
	int csv = (b->output_format == OFMT_CSV);
	int json = (b->output_format == OFMT_JSON);

	if (!s || !t)
		return 0;

	(void) ssep;
	if (csv && btag[0] && mnstr_write(s, btag, strlen(btag), 1) < 1)
		ok = -4;
	if (json) {
		sep = ", ";
		seplen = strlen(sep);
	}
	for (i = 0; i < t->nr_cols && ok > -1; i++) {
		res_col *c = t->cols + i;

		if (i != 0 && mnstr_write(s, sep, seplen, 1) < 1) {
			ok = -4;
			break;
		}
		if (json && (mnstr_write(s, c->name, strlen(c->name), 1) < 1 || mnstr_write(s, ": ", 2, 1) < 1)) {
			ok = -4;
			break;
		}
		ok = export_value(m, s, c->type.type->eclass, c->type.type->base.name, c->type.digits, c->type.scale, c->p, c->mtype, &buf, &len, ns);
	}
	_DELETE(buf);
	if (ok > -1 && mnstr_write(s, rsep, rseplen, 1) < 1)
		ok = -4;
	b->results = res_tables_remove(b->results, t);
	return ok;
}

static int
mvc_export_table_columnar(stream *s, res_table *t, bstream *in)
{
	int i, res = 0;

	if (!s || !t)
		return 0;

	for (i = 1; i <= t->nr_cols; i++) {
		res_col *c = t->cols + (i - 1);

		if (!c->b)
			break;

		BAT *b = BATdescriptor(c->b);
		if (b == NULL)
			return -2;

		res = mvc_export_binary_bat(s, b, in);
		BBPunfix(b->batCacheid);
		if (res < 0)
			return res;
	}

	return res;
}

static int
mvc_export_table_(mvc *m, int output_format, stream *s, res_table *t, BUN offset, BUN nr, const char *btag, const char *sep, const char *rsep, const char *ssep, const char *ns)
{
	Tablet as;
	Column *fmt;
	int i, ok = 0;
	struct time_res *tres;
	int csv = (output_format == OFMT_CSV);
	int json = (output_format == OFMT_JSON);
	char *bj;

	if (!s || !t)
		return 0;

	as.nr_attrs = t->nr_cols + 1;	/* for the leader */
	as.nr = nr;
	as.offset = offset;
	fmt = as.format = (Column *) GDKzalloc(sizeof(Column) * (as.nr_attrs + 1));
	tres = GDKzalloc(sizeof(struct time_res) * (as.nr_attrs));
	if (fmt == NULL || tres == NULL) {
		GDKfree(fmt);
		GDKfree(tres);
		return -4;
	}

	fmt[0].c = NULL;
	fmt[0].sep = (csv) ? btag : "";
	fmt[0].rsep = rsep;
	fmt[0].seplen = _strlen(fmt[0].sep);
	fmt[0].ws = 0;
	fmt[0].nullstr = NULL;

	for (i = 1; i <= t->nr_cols; i++) {
		res_col *c = t->cols + (i - 1);

		if (!c->b)
			break;

		fmt[i].c = BATdescriptor(c->b);
		if (fmt[i].c == NULL) {
			while (--i >= 1) {
				bat_iterator_end(&fmt[i].ci);
				BBPunfix(fmt[i].c->batCacheid);
			}
			GDKfree(fmt);
			GDKfree(tres);
			return -2;
		}
		fmt[i].ci = bat_iterator(fmt[i].c);
		fmt[i].name = NULL;
		if (csv) {
			fmt[i].sep = ((i - 1) < (t->nr_cols - 1)) ? sep : rsep;
			fmt[i].seplen = _strlen(fmt[i].sep);
			fmt[i].rsep = rsep;
		}
		if (json) {
			res_col *p = t->cols + (i - 1);

			/*
			 * We define the "proper" way of returning
			 * a relational table in json format as a
			 * json array of objects, where each row is
			 * represented as a json object.
			 */
			if (i == 1) {
				bj = SA_NEW_ARRAY(m->sa, char, strlen(p->name) + strlen(btag));
				snprintf(bj, strlen(p->name) + strlen(btag), btag, p->name);
				fmt[i - 1].sep = bj;
				fmt[i - 1].seplen = _strlen(fmt[i - 1].sep);
				fmt[i - 1].rsep = NULL;
			} else if (i <= t->nr_cols) {
				bj = SA_NEW_ARRAY(m->sa, char, strlen(p->name) + strlen(sep));
				snprintf(bj, strlen(p->name) + 10, sep, p->name);
				fmt[i - 1].sep = bj;
				fmt[i - 1].seplen = _strlen(fmt[i - 1].sep);
				fmt[i - 1].rsep = NULL;
			}
			if (i == t->nr_cols) {
				fmt[i].sep = rsep;
				fmt[i].seplen = _strlen(fmt[i].sep);
				fmt[i].rsep = NULL;
			}
		}
		fmt[i].type = ATOMname(fmt[i].c->ttype);
		fmt[i].adt = fmt[i].c->ttype;
		fmt[i].tostr = &_ASCIIadt_toStr;
		fmt[i].frstr = &_ASCIIadt_frStr;
		fmt[i].extra = fmt + i;
		fmt[i].data = NULL;
		fmt[i].len = 0;
		fmt[i].ws = 0;
		fmt[i].quote = ssep ? ssep[0] : 0;
		fmt[i].nullstr = ns;
		if (c->type.type->eclass == EC_DEC) {
			fmt[i].tostr = &dec_tostr;
			fmt[i].frstr = &dec_frstr;
			fmt[i].extra = (void *) (ptrdiff_t) c->type.scale;
		} else if (c->type.type->eclass == EC_TIMESTAMP || c->type.type->eclass == EC_TIMESTAMP_TZ) {
			struct time_res *ts_res = tres + (i - 1);
			ts_res->has_tz = EC_TEMP_TZ(c->type.type->eclass);
			ts_res->fraction = c->type.digits ? c->type.digits - 1 : 0;
			ts_res->timezone = m->timezone;

			fmt[i].tostr = &sql_timestamp_tostr;
			fmt[i].frstr = NULL;
			fmt[i].extra = ts_res;
		} else if (c->type.type->eclass == EC_TIME || c->type.type->eclass == EC_TIME_TZ) {
			struct time_res *ts_res = tres + (i - 1);
			ts_res->has_tz = (strcmp(c->type.type->base.name, "timetz") == 0);
			ts_res->fraction = c->type.digits ? c->type.digits - 1 : 0;
			ts_res->timezone = m->timezone;

			fmt[i].tostr = &sql_time_tostr;
			fmt[i].frstr = NULL;
			fmt[i].extra = ts_res;
		} else if (c->type.type->eclass == EC_SEC) {
			fmt[i].tostr = &dec_tostr;
			fmt[i].frstr = &sec_frstr;
			fmt[i].extra = (void *) (ptrdiff_t) 3;
		} else {
			fmt[i].extra = fmt + i;
		}
	}
	if (i == t->nr_cols + 1)
		ok = TABLEToutput_file(&as, NULL, s, m->scanner.rs);
	for (i = 0; i <= t->nr_cols; i++) {
		fmt[i].sep = NULL;
		fmt[i].rsep = NULL;
		fmt[i].type = NULL;
		fmt[i].nullstr = NULL;
	}
	for (i = 1; i <= t->nr_cols; i++)
		bat_iterator_end(&fmt[i].ci);
	TABLETdestroy_format(&as);
	GDKfree(tres);
	if (ok < 0)
		return ok;
	if (mnstr_errnr(s) != MNSTR_NO__ERROR)
		return -4;
	return 0;
}

static int
mvc_export_table(backend *b, stream *s, res_table *t, BUN offset, BUN nr, const char *btag, const char *sep, const char *rsep, const char *ssep, const char *ns)
{
	return mvc_export_table_(b->mvc, b->output_format, s, t, offset, nr, btag, sep, rsep, ssep, ns);
}

int
mvc_export(mvc *m, stream *s, res_table *t, BUN nr)
{
	backend b = {0};
	b.mvc = m;
	b.results = t;
	b.reloptimizer = 0;
	t->nr_rows = nr;
	if (mvc_export_head(&b, s, t->id, TRUE, TRUE, 0/*starttime*/, 0/*maloptimizer*/) < 0)
		return -1;
	return mvc_export_table_(m, OFMT_CSV, s, t, 0, nr, "[ ", ",\t", "\t]\n", "\"", "NULL");
}


static lng
get_print_width(int mtype, sql_class eclass, int digits, int scale, int tz, bat bid, ptr p)
{
	size_t count = 0, incr = 0;

	if (eclass == EC_SEC)
		incr = 1;
	else if (mtype == TYPE_oid)
		incr = 2;
	mtype = ATOMbasetype(mtype);
	if (mtype == TYPE_str) {
		if (eclass == EC_CHAR && digits) {
			return digits;
		} else {
			int l = 0;
			if (bid) {
				BAT *b = BATdescriptor(bid);

				if (b) {
					/* in practice, b can be a
					 * void(nil) bat, an oid bat
					 * with all nil values, or an
					 * empty void/oid bat */
					if (ATOMstorage(b->ttype) == TYPE_str)
						l = bat_max_strlength(b);
					else
						l = 0;
					BBPunfix(b->batCacheid);
				} else {
					return -2;
				}
			} else if (p) {
				l = UTF8_strwidth((const char *) p);
				if (is_int_nil(l))
					l = 0;
			}
			return l;
		}
	} else if (eclass == EC_NUM || eclass == EC_POS || eclass == EC_MONTH || eclass == EC_SEC) {
		count = 0;
		if (bid) {
			BAT *b = BATdescriptor(bid);

			if (b) {
				if (mtype == TYPE_bte) {
					count = bat_max_btelength(b);
				} else if (mtype == TYPE_sht) {
					count = bat_max_shtlength(b);
				} else if (mtype == TYPE_int) {
					count = bat_max_intlength(b);
				} else if (mtype == TYPE_lng) {
					count = bat_max_lnglength(b);
#ifdef HAVE_HGE
				} else if (mtype == TYPE_hge) {
					count = bat_max_hgelength(b);
#endif
				} else if (mtype == TYPE_void) {
					count = 4;
				} else {
					assert(0);
				}
				count += incr;
				BBPunfix(b->batCacheid);
			} else {
				return -2;
			}
		} else {
			if (p) {
#ifdef HAVE_HGE
				hge val = 0;
#else
				lng val = 0;
#endif
				if (mtype == TYPE_bte) {
					val = *((bte *) p);
				} else if (mtype == TYPE_sht) {
					val = *((sht *) p);
				} else if (mtype == TYPE_int) {
					val = *((int *) p);
				} else if (mtype == TYPE_lng) {
					val = *((lng *) p);
#ifdef HAVE_HGE
				} else if (mtype == TYPE_hge) {
					val = *((hge *) p);
#endif
				} else {
					assert(0);
				}

				if (val < 0)
					count++;
				while (val /= 10)
					count++;
				count++;
				count += incr;
			} else {
				count = 0;
			}
		}
		if (eclass == EC_SEC && count < 5)
			count = 5;
		return count;
		/* the following two could be done once by taking the
		   max value and calculating the number of digits from that
		   value, instead of the maximum values taken now, which
		   include the optional sign */
	} else if (eclass == EC_FLT) {
		/* floats are printed using "%.9g":
		 * [sign]+digit+period+[max 8 digits]+E+[sign]+[max 2 digits] */
		if (mtype == TYPE_flt) {
			return 15;
			/* doubles are printed using "%.17g":
			 * [sign]+digit+period+[max 16 digits]+E+[sign]+[max 3 digits] */
		} else {	/* TYPE_dbl */
			return 24;
		}
	} else if (eclass == EC_DEC) {
		count = 1 + digits;
		if (scale > 0)
			count += 1;
		if (scale == digits) // for preceding 0, e.g. 0.
			count += 1;
		return count;
	} else if (eclass == EC_DATE) {
		return 10;
	} else if (eclass == EC_TIME || eclass == EC_TIME_TZ) {
		count = 8;
		if (tz)		/* time zone */
			count += 6;	/* +03:30 */
		if (digits > 1)	/* fractional seconds precision (including dot) */
			count += digits;
		return count;
	} else if (eclass == EC_TIMESTAMP || eclass == EC_TIMESTAMP_TZ) {
		count = 10 + 1 + 8;
		if (tz)		/* time zone */
			count += 6;	/* +03:30 */
		if (digits)	/* fractional seconds precision */
			count += digits;
		return count;
	} else if (eclass == EC_BIT) {
		return 5;	/* max(strlen("true"), strlen("false")) */
	} else if (strcmp(ATOMname(mtype), "uuid") == 0) {
		return 36;	/* xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx */
	} else {
		return 0;
	}
}

static int
export_length(stream *s, int mtype, sql_class eclass, int digits, int scale, int tz, bat bid, ptr p)
{
	lng length = get_print_width(mtype, eclass, digits, scale, tz, bid, p);
	if (length < 0)
		return -2;
	if (mvc_send_lng(s, length) != 1)
		return -4;
	return 0;
}

int
mvc_export_operation(backend *b, stream *s, str w, lng starttime, lng mal_optimizer)
{
	mvc *m = b->mvc;

	assert(m->type == Q_SCHEMA || m->type == Q_TRANS);
	if (m->type == Q_SCHEMA) {
		if (!s)
			return 0;
		if (mnstr_printf(s, "&3 " LLFMT " " LLFMT "\n", starttime > 0 ? GDKusec() - starttime : 0, mal_optimizer) < 0)
			return -4;
	} else {
		if (m->session->auto_commit) {
			if (mnstr_write(s, "&4 t\n", 5, 1) != 1)
				return -4;
		} else {
			if (mnstr_write(s, "&4 f\n", 5, 1) != 1)
				return -4;
		}
	}

	if (mvc_export_warning(s, w) != 1)
		return -4;
	return 0;
}


int
mvc_affrows(mvc *m, stream *s, lng val, str w, oid query_id, lng last_id, lng starttime, lng maloptimizer, lng reloptimizer)
{
	sqlvar_set_number(find_global_var(m, mvc_bind_schema(m, "sys"), "rowcnt"), val);

	/* if we don't have a stream, nothing can go wrong, so we return
	 * success.  This is especially vital for execution of internal SQL
	 * commands, since they don't get a stream to suppress their output.
	 * If we would fail on having no stream here, those internal commands
	 * fail too.
	 */
	if (!s || GDKembedded())
		return 0;
	if (mnstr_write(s, "&2 ", 3, 1) != 1 ||
	    mvc_send_lng(s, val) != 1 ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    mvc_send_lng(s, last_id) != 1 ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    mvc_send_lng(s, (lng) query_id) != 1 ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    mvc_send_lng(s, starttime > 0 ? GDKusec() - starttime : 0) != 1 ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    mvc_send_lng(s, maloptimizer) != 1 ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    mvc_send_lng(s, reloptimizer) != 1 ||
	    mnstr_write(s, "\n", 1, 1) != 1)
		return -4;
	if (mvc_export_warning(s, w) != 1)
		return -4;

	return 0;
}

int
mvc_export_affrows(backend *b, stream *s, lng val, str w, oid query_id, lng starttime, lng maloptimizer)
{
	b->rowcnt = val;
	return mvc_affrows(b->mvc, s, val, w, query_id, b->last_id, starttime, maloptimizer, b->reloptimizer);
}

int
mvc_export_head(backend *b, stream *s, int res_id, int only_header, int compute_lengths, lng starttime, lng maloptimizer)
{
	mvc *m = b->mvc;
	int i, res = 0;
	BUN count = 0;
	res_table *t = res_tables_find(b->results, res_id);

	if (!s || !t)
		return 0;

	/* query type: Q_TABLE || Q_PREPARE */
	assert(t->query_type == Q_TABLE || t->query_type == Q_PREPARE);
	if (mnstr_write(s, "&", 1, 1) != 1 || mvc_send_int(s, (int) t->query_type) != 1 || mnstr_write(s, " ", 1, 1) != 1)
		return -4;

	/* id */
	int result_id = t->query_type == Q_PREPARE?b->q->id:t->id;
	if (mvc_send_int(s, result_id) != 1 || mnstr_write(s, " ", 1, 1) != 1)
		return -4;

	/* tuple count */
	if (only_header) {
		if (t->cols[0].b) {
			count = t->nr_rows;
		} else {
			count = 1;
		}
	}
	b->rowcnt = count;
	sqlvar_set_number(find_global_var(m, mvc_bind_schema(m, "sys"), "rowcnt"), b->rowcnt);
	if (mvc_send_lng(s, (lng) count) != 1 || mnstr_write(s, " ", 1, 1) != 1)
		return -4;

	/* column count */
	if (mvc_send_int(s, t->nr_cols) != 1 || mnstr_write(s, " ", 1, 1) != 1)
		return -4;

	/* row count, min(count, reply_size) */
	/* the columnar protocol ignores the reply size by fetching the
	 * entire resultset at once, so don't set it; also, the MAPI
	 * protocol doesn't allow for retrieving rows using the Xexport*
	 * commands for Q_PREPARE results (due to an oversight), so we send
	 * it all in the first response */
	if (mvc_send_int(s, (b->client && b->client->protocol != PROTOCOL_COLUMNAR && m->reply_size >= 0 && (BUN) m->reply_size < count && t->query_type != Q_PREPARE) ? m->reply_size : (int) count) != 1)
		return -4;

	// export query id
	if (mnstr_write(s, " ", 1, 1) != 1 || mvc_send_lng(s, (lng) t->query_id) != 1)
		return -4;

	// export query time
	if (mnstr_write(s, " ", 1, 1) != 1 || mvc_send_lng(s, starttime > 0 ? GDKusec() - starttime : 0) != 1)
		return -4;

	// export MAL optimizer time
	if (mnstr_write(s, " ", 1, 1) != 1 || mvc_send_lng(s, maloptimizer) != 1)
		return -4;

	if (mnstr_write(s, " ", 1, 1) != 1 || mvc_send_lng(s, b->reloptimizer) != 1)
		return -4;

	if (mnstr_write(s, "\n% ", 3, 1) != 1)
		return -4;
	for (i = 0; i < t->nr_cols; i++) {
		res_col *c = t->cols + i;
		size_t len = strlen(c->tn);

		if (len && mnstr_write(s, c->tn, len, 1) != 1)
			return -4;
		if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
			return -4;
	}
	if (mnstr_write(s, " # table_name\n% ", 16, 1) != 1)
		return -4;

	for (i = 0; i < t->nr_cols; i++) {
		res_col *c = t->cols + i;

		if (strpbrk(c->name, ", \t#\"\\")) {
			char *p;
			if (mnstr_write(s, "\"", 1, 1) != 1)
				return -4;
			for (p = c->name; *p; p++) {
				if (*p == '"' || *p == '\\') {
					if (mnstr_write(s, "\\", 1, 1) != 1)
						return -4;
				}
				if (mnstr_write(s, p, 1, 1) != 1)
					return -4;
			}
			if (mnstr_write(s, "\"", 1, 1) != 1)
				return -4;
		} else {
			if (mnstr_write(s, c->name, strlen(c->name), 1) != 1)
				return -4;
		}

		if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
			return -4;
	}
	if (mnstr_write(s, " # name\n% ", 10, 1) != 1)
		return -4;

	for (i = 0; i < t->nr_cols; i++) {
		res_col *c = t->cols + i;

		if (mnstr_write(s, c->type.type->base.name, strlen(c->type.type->base.name), 1) != 1)
			return -4;
		if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
			return -4;
	}
	if (mnstr_write(s, " # type\n% ", 10, 1) != 1)
		return -4;
	if (compute_lengths) {
		for (i = 0; i < t->nr_cols; i++) {
			res_col *c = t->cols + i;
			int mtype = c->type.type->localtype;
			sql_class eclass = c->type.type->eclass;

			if ((res = export_length(s, mtype, eclass, c->type.digits, c->type.scale, type_has_tz(&c->type), c->b, c->p)) < 0)
				return res;
			if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
				return -4;
		}
		if (mnstr_write(s, " # length\n", 10, 1) != 1)
			return -4;
	}
	if (b->sizeheader) {
		if (mnstr_write(s, "% ", 2, 1) != 1)
			return -4;
		for (i = 0; i < t->nr_cols; i++) {
			res_col *c = t->cols + i;

			if (mnstr_printf(s, "%u %u", c->type.digits, c->type.scale) < 0)
				return -4;
			if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
				return -4;
		}
		if (mnstr_write(s, " # typesizes\n", 13, 1) != 1)
			return -4;
	}
	return res;
}

static int
mvc_export_file(backend *b, stream *s, res_table *t)
{
	int res = 0;
	BUN count;

	if (!t->cols[0].b) {
		res = mvc_export_row(b, s, t, "", t->tsep, t->rsep, t->ssep, t->ns);
	} else {
		count = t->nr_rows;

		res = mvc_export_table(b, s, t, 0, count, "", t->tsep, t->rsep, t->ssep, t->ns);
		b->results = res_tables_remove(b->results, t);
	}
	return res;
}

int
mvc_export_result(backend *b, stream *s, int res_id, bool header, lng starttime, lng maloptimizer)
{
	mvc *m = b->mvc;
	int clean = 0, res = 0;
	BUN count;
	res_table *t = res_tables_find(b->results, res_id);
	int json = (b->output_format == OFMT_JSON);

	if (!s || !t)
		return 0;

	/* Proudly supporting SQLstatementIntern's output flag */
	if (b->output_format == OFMT_NONE)
		return 0;

	assert(t->query_type == Q_TABLE || t->query_type == Q_PREPARE);
	if (t->tsep) {
		/* need header */
		if (header && (res = mvc_export_head(b, s, res_id, TRUE, TRUE, starttime, maloptimizer)) < 0)
			return res;
		return mvc_export_file(b, s, t);
	}

	if (!json && (res = mvc_export_head(b, s, res_id, TRUE, TRUE, starttime, maloptimizer)) < 0)
		return res;

	assert(t->cols[0].b);

	if (b->client->protocol == PROTOCOL_COLUMNAR) {
		if (mnstr_flush(s, MNSTR_FLUSH_DATA) < 0)
			return -4;
		return mvc_export_table_columnar(s, t, m->scanner.rs);
	}

	/* for Q_PREPARE results, send everything */
	count = t->query_type == Q_PREPARE ? t->nr_rows : (BUN) m->reply_size;
	if (m->reply_size != -2 && (count <= 0 || count >= t->nr_rows)) {
		count = t->nr_rows;
		clean = 1;
	}
	if (json) {
		switch(count) {
		case 0:
			res = mvc_export_table(b, s, t, 0, count, "{\t", "", "}\n", "\"", "null");
			break;
		case 1:
			res = mvc_export_table(b, s, t, 0, count, "{\n\t\"%s\" : ", ",\n\t\"%s\" : ", "\n}\n", "\"", "null");
			break;
		case 2:
			res = mvc_export_table(b, s, t, 0, 1, "[\n\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t},\n", "\"", "null");
			res = mvc_export_table(b, s, t, 1, count - 1, "\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t}\n]\n", "\"", "null");
			break;
		default:
			res = mvc_export_table(b, s, t, 0, 1, "[\n\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t},\n", "\"", "null");
			res = mvc_export_table(b, s, t, 1, count - 2, "\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t},\n", "\"", "null");
			res = mvc_export_table(b, s, t, count - 1, 1, "\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t}\n]\n", "\"", "null");
		}
	} else {
		res = mvc_export_table(b, s, t, 0, count, "[ ", ",\t", "\t]\n", "\"", "NULL");
	}
	if (clean)
		b->results = res_tables_remove(b->results, t);

	if (res > -1)
		res = 1;
	return res;
}

int
mvc_export_chunk(backend *b, stream *s, int res_id, BUN offset, BUN nr)
{
	int res = 0;
	res_table *t = res_tables_find(b->results, res_id);
	BUN cnt;

	if (!s || !t)
		return 0;

	cnt = nr;
	if (cnt == 0)
		cnt = t->nr_rows;
	if (offset >= t->nr_rows)
		cnt = 0;
	if (cnt == BUN_NONE || offset + cnt > t->nr_rows)
		cnt = t->nr_rows - offset;

	/* query type: Q_BLOCK */
	if (mnstr_write(s, "&6 ", 3, 1) != 1)
		return -4;

	/* result id */
	if (mvc_send_int(s, res_id) != 1 || mnstr_write(s, " ", 1, 1) != 1)
		return -4;

	/* column count */
	if (mvc_send_int(s, t->nr_cols) != 1 || mnstr_write(s, " ", 1, 1) != 1)
		return -4;

	/* row count */
	if (mvc_send_lng(s, (lng) cnt) != 1 || mnstr_write(s, " ", 1, 1) != 1)
		return -4;

	/* block offset */
	if (mvc_send_lng(s, (lng) offset) != 1)
		return -4;

	if (mnstr_write(s, "\n", 1, 1) != 1)
		return -4;

	res = mvc_export_table(b, s, t, offset, cnt, "[ ", ",\t", "\t]\n", "\"", "NULL");
	return res;
}

int
mvc_result_table(backend *be, oid query_id, int nr_cols, mapi_query_t type)
{
	res_table *t = res_table_create(be->mvc->session->tr, be->result_id++, query_id, nr_cols, type, be->results);
	be->results = t;
	return t ? t->id : -1;
}

int
mvc_result_column(backend *be, const char *tn, const char *name, const char *typename, int digits, int scale, BAT *b)
{
	/* return 0 on success, non-zero on failure */
	return res_col_create(be->mvc->session->tr, be->results, tn, name, typename, digits, scale, true, b->ttype, b, false) ? 0 : -1;
}

int
mvc_result_value(backend *be, const char *tn, const char *name, const char *typename, int digits, int scale, ptr *p, int mtype)
{
	/* return 0 on success, non-zero on failure */
	return res_col_create(be->mvc->session->tr, be->results, tn, name, typename, digits, scale, false, mtype, p, false) ? 0 : -1;
}

/* Translate error code from export function to error string */
const char *
mvc_export_error(backend *be, stream *s, int err_code)
{
	(void) be;
	switch (err_code) {
	case -1: /* Allocation failure */
		return MAL_MALLOC_FAIL;
	case -2: /* BAT descriptor error */
		return RUNTIME_OBJECT_MISSING;
	case -3: /* GDK error */
		return GDKerrbuf;
	case -4: /* Stream error */
		return mnstr_peek_error(s);
	case -5:
		return "Query aborted";
	default: /* Unknown, must be a bug */
		return "Unknown internal error";
	}
}

static ssize_t
align_dump(stream *s, uint64_t pos, unsigned int alignment)
{
	uint64_t a = (uint64_t)alignment;
	// must be a power of two
	assert(a > 0);
	assert((a & (a-1)) == 0);

	static char zeroes[32] = { 0 };
#ifdef _MSC_VER
#pragma warning(suppress:4146)
#endif
	uint64_t gap = (~pos + 1) % a;
	return mnstr_write(s, zeroes, 1, (size_t)gap);
}


struct bindump_record {
	BAT *bat;
	type_record_t *type_rec;
	int64_t start;
	int64_t length;
};

int
mvc_export_bin_chunk(backend *b, stream *s, int res_id, BUN offset, BUN nr)
{
	int ret = -42;
	struct bindump_record *colinfo;
	stream *countstream = NULL;
	uint64_t byte_count = 0;
	uint64_t toc_pos = 0;
	BUN end_row = offset + nr;

	res_table *res = res_tables_find(b->results, res_id);
	if (res == NULL)
		return 0;

	colinfo = GDKzalloc(res->nr_cols * sizeof(*colinfo));
	if (!colinfo) {
		ret = -1;
		goto end;
	}
	for (int i = 0; i < res->nr_cols; i++)
		colinfo[i].bat = NULL;
	for (int i = 0; i < res->nr_cols; i++) {
		bat bat_id = res->cols[i].b;
		BAT *b = BATdescriptor(bat_id);
		if (!b) {
			ret = -1;
			goto end;
		}
		colinfo[i].bat = b;

		if (BATcount(b) < end_row)
			end_row = BATcount(b);

		int tpe = BATttype(b);
		const char *gdk_name = ATOMname(tpe);
		type_record_t *rec = find_type_rec(gdk_name);
		if (!rec || !can_dump_binary_column(rec)) {
			GDKerror("column %d: don't know how to dump data type '%s'", i, gdk_name);
			ret = -3;
			goto end;
		}
		colinfo[i].type_rec = rec;
	}

	// The byte_counting_stream keeps track of the byte offsets
	countstream = byte_counting_stream(s, &byte_count);

	// Make sure the message starts with a & and not with a !
	mnstr_printf(countstream, "&6 %d %d " BUNFMT " " BUNFMT "\n", res_id, res->nr_cols, end_row - offset, offset);

	for (int i = 0; i < res->nr_cols; i++) {
		align_dump(countstream, byte_count, 32); // 32 looks nice in tcpflow
		struct bindump_record *info = &colinfo[i];
		info->start = byte_count;
		str msg = dump_binary_column(info->type_rec, info->bat, offset, end_row - offset, false, countstream);
		if (msg != MAL_SUCCEED) {
			GDKerror("%s", msg);
			GDKfree(msg);
			ret = -3;
			goto end;
		}
		info->length = byte_count - info->start;
	}

	assert(byte_count > 0);

	align_dump(countstream, byte_count, 32);
	toc_pos = byte_count;
	for (int i = 0; i < res->nr_cols; i++) {
		struct bindump_record *info = &colinfo[i];
		lng start = info->start;
		lng length = info->length;
		mnstr_writeLng(countstream, start);
		mnstr_writeLng(countstream, length);
	}

	mnstr_writeLng(countstream, toc_pos);
	ret = 0;

end:
	if (colinfo) {
		for (int i = 0; i < res->nr_cols; i++) {
			if (colinfo[i].bat)
				BBPunfix(colinfo[i].bat->batCacheid);
		}
		GDKfree(colinfo);
	}
	mnstr_destroy(countstream);
	return ret;
}
