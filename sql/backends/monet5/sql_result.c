/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

static inline int
STRwidth(const char *restrict s)
{
	int len = 0;
	int c;
	int n;

	if (strNil(s))
		return int_nil;
	c = 0;
	n = 0;
	while (*s != 0) {
		if ((*s & 0x80) == 0) {
			assert(n == 0);
			len++;
			n = 0;
		} else if ((*s & 0xC0) == 0x80) {
			c = (c << 6) | (*s & 0x3F);
			if (--n == 0) {
				/* last byte of a multi-byte character */
				len++;
				/* this list was created by combining
				 * the code points marked as
				 * Emoji_Presentation in
				 * /usr/share/unicode/emoji/emoji-data.txt
				 * and code points marked either F or
				 * W in EastAsianWidth.txt; this list
				 * is up-to-date with Unicode 9.0 */
				if ((0x1100 <= c && c <= 0x115F) ||
				    (0x231A <= c && c <= 0x231B) ||
				    (0x2329 <= c && c <= 0x232A) ||
				    (0x23E9 <= c && c <= 0x23EC) ||
				    c == 0x23F0 ||
				    c == 0x23F3 ||
				    (0x25FD <= c && c <= 0x25FE) ||
				    (0x2614 <= c && c <= 0x2615) ||
				    (0x2648 <= c && c <= 0x2653) ||
				    c == 0x267F ||
				    c == 0x2693 ||
				    c == 0x26A1 ||
				    (0x26AA <= c && c <= 0x26AB) ||
				    (0x26BD <= c && c <= 0x26BE) ||
				    (0x26C4 <= c && c <= 0x26C5) ||
				    c == 0x26CE ||
				    c == 0x26D4 ||
				    c == 0x26EA ||
				    (0x26F2 <= c && c <= 0x26F3) ||
				    c == 0x26F5 ||
				    c == 0x26FA ||
				    c == 0x26FD ||
				    c == 0x2705 ||
				    (0x270A <= c && c <= 0x270B) ||
				    c == 0x2728 ||
				    c == 0x274C ||
				    c == 0x274E ||
				    (0x2753 <= c && c <= 0x2755) ||
				    c == 0x2757 ||
				    (0x2795 <= c && c <= 0x2797) ||
				    c == 0x27B0 ||
				    c == 0x27BF ||
				    (0x2B1B <= c && c <= 0x2B1C) ||
				    c == 0x2B50 ||
				    c == 0x2B55 ||
				    (0x2E80 <= c && c <= 0x2E99) ||
				    (0x2E9B <= c && c <= 0x2EF3) ||
				    (0x2F00 <= c && c <= 0x2FD5) ||
				    (0x2FF0 <= c && c <= 0x2FFB) ||
				    (0x3000 <= c && c <= 0x303E) ||
				    (0x3041 <= c && c <= 0x3096) ||
				    (0x3099 <= c && c <= 0x30FF) ||
				    (0x3105 <= c && c <= 0x312D) ||
				    (0x3131 <= c && c <= 0x318E) ||
				    (0x3190 <= c && c <= 0x31BA) ||
				    (0x31C0 <= c && c <= 0x31E3) ||
				    (0x31F0 <= c && c <= 0x321E) ||
				    (0x3220 <= c && c <= 0x3247) ||
				    (0x3250 <= c && c <= 0x32FE) ||
				    (0x3300 <= c && c <= 0x4DBF) ||
				    (0x4E00 <= c && c <= 0xA48C) ||
				    (0xA490 <= c && c <= 0xA4C6) ||
				    (0xA960 <= c && c <= 0xA97C) ||
				    (0xAC00 <= c && c <= 0xD7A3) ||
				    (0xF900 <= c && c <= 0xFAFF) ||
				    (0xFE10 <= c && c <= 0xFE19) ||
				    (0xFE30 <= c && c <= 0xFE52) ||
				    (0xFE54 <= c && c <= 0xFE66) ||
				    (0xFE68 <= c && c <= 0xFE6B) ||
				    (0xFF01 <= c && c <= 0xFF60) ||
				    (0xFFE0 <= c && c <= 0xFFE6) ||
				    c == 0x16FE0 ||
				    (0x17000 <= c && c <= 0x187EC) ||
				    (0x18800 <= c && c <= 0x18AF2) ||
				    (0x1B000 <= c && c <= 0x1B001) ||
				    c == 0x1F004 ||
				    c == 0x1F0CF ||
				    c == 0x1F18E ||
				    (0x1F191 <= c && c <= 0x1F19A) ||
				    /* removed 0x1F1E6..0x1F1FF */
				    (0x1F200 <= c && c <= 0x1F202) ||
				    (0x1F210 <= c && c <= 0x1F23B) ||
				    (0x1F240 <= c && c <= 0x1F248) ||
				    (0x1F250 <= c && c <= 0x1F251) ||
				    (0x1F300 <= c && c <= 0x1F320) ||
				    (0x1F32D <= c && c <= 0x1F335) ||
				    (0x1F337 <= c && c <= 0x1F37C) ||
				    (0x1F37E <= c && c <= 0x1F393) ||
				    (0x1F3A0 <= c && c <= 0x1F3CA) ||
				    (0x1F3CF <= c && c <= 0x1F3D3) ||
				    (0x1F3E0 <= c && c <= 0x1F3F0) ||
				    c == 0x1F3F4 ||
				    (0x1F3F8 <= c && c <= 0x1F43E) ||
				    c == 0x1F440 ||
				    (0x1F442 <= c && c <= 0x1F4FC) ||
				    (0x1F4FF <= c && c <= 0x1F53D) ||
				    (0x1F54B <= c && c <= 0x1F54E) ||
				    (0x1F550 <= c && c <= 0x1F567) ||
				    c == 0x1F57A ||
				    (0x1F595 <= c && c <= 0x1F596) ||
				    c == 0x1F5A4 ||
				    (0x1F5FB <= c && c <= 0x1F64F) ||
				    (0x1F680 <= c && c <= 0x1F6C5) ||
				    c == 0x1F6CC ||
				    (0x1F6D0 <= c && c <= 0x1F6D2) ||
				    (0x1F6EB <= c && c <= 0x1F6EC) ||
				    (0x1F6F4 <= c && c <= 0x1F6F6) ||
				    (0x1F910 <= c && c <= 0x1F91E) ||
				    (0x1F920 <= c && c <= 0x1F927) ||
				    c == 0x1F930 ||
				    (0x1F933 <= c && c <= 0x1F93E) ||
				    (0x1F940 <= c && c <= 0x1F94B) ||
				    (0x1F950 <= c && c <= 0x1F95E) ||
				    (0x1F980 <= c && c <= 0x1F991) ||
				    c == 0x1F9C0 ||
				    (0x20000 <= c && c <= 0x2FFFD) ||
				    (0x30000 <= c && c <= 0x3FFFD))
					len++;
			}
		} else if ((*s & 0xE0) == 0xC0) {
			assert(n == 0);
			n = 1;
			c = *s & 0x1F;
		} else if ((*s & 0xF0) == 0xE0) {
			assert(n == 0);
			n = 2;
			c = *s & 0x0F;
		} else if ((*s & 0xF8) == 0xF0) {
			assert(n == 0);
			n = 3;
			c = *s & 0x07;
		} else if ((*s & 0xFC) == 0xF8) {
			assert(n == 0);
			n = 4;
			c = *s & 0x03;
		} else {
			assert(0);
			n = 0;
		}
		s++;
	}
	return len;
}

static int
bat_max_strlength(BAT *b)
{
	BUN p, q;
	int l = 0;
	int max = 0;
	BATiter bi = bat_iterator(b);

	BATloop(b, p, q) {
		l = STRwidth((const char *) BUNtvar(bi, p));

		if (is_int_nil(l))
			l = 0;
		if (l > max)
			max = l;
	}
	return max;
}

#define bat_max_length(TPE, HIGH) \
static size_t \
bat_max_##TPE##length(BAT *b) \
{ \
	BUN p, q; \
	HIGH max = 0, min = 0; \
	size_t ret = 0; \
	const TPE *restrict vals = (const TPE *) Tloc(b, 0); \
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
		for (i = 0; *s && *s != '.' && ((res == 0 && *s == '0') || i < t->digits - t->scale); s++) { \
			if (!isdigit((unsigned char) *s))							\
				break;													\
			res *= 10;													\
			res += (*s-'0');											\
			if (res)													\
				i++;													\
		}																\
		if (*s == '.') {												\
			s++;														\
			while (*s && isdigit((unsigned char) *s) && scale > 0) {	\
				res *= 10;												\
				res += *s++ - '0';										\
				scale--;												\
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
	for (i = 0; i < (19 - 3) && *s && *s != '.'; i++, s++) {
		if (!isdigit((unsigned char) *s))
			return NULL;
		res *= 10;
		res += (*s - '0');
	}
	i = 0;
	if (*s) {
		if (*s != '.')
			return NULL;
		s++;
		for (; *s && i < 3; i++, s++) {
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
		int slen;

		s = c->data;
		slen = strNil(s) ? int_nil : UTF8_strlen(s);
		if (col->type.digits > 0 && len > 0 && slen > (int) col->type.digits) {
			len = STRwidth(c->data);
			if (len > (ssize_t) col->type.digits)
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
mvc_import_table(Client cntxt, BAT ***bats, mvc *m, bstream *bs, sql_table *t, const char *sep, const char *rsep, const char *ssep, const char *ns, lng sz, lng offset, int best, bool from_stdin, bool escape)
{
	int i = 0, j;
	node *n;
	Tablet as;
	Column *fmt;
	str msg = MAL_SUCCEED;

	*bats =0;	// initialize the receiver

	if (!bs) {
		sql_error(m, 500, "no stream (pointer) provided");
		return NULL;
	}
	if (mnstr_errnr(bs->s)) {
		mnstr_error_kind errnr = mnstr_errnr(bs->s);
		char *msg = mnstr_error(bs->s);
		sql_error(m, 500, "stream not open %s: %s", mnstr_error_kind_name(errnr), msg ? msg : "unknown error");
		free(msg);
		return NULL;
	}
	if (offset < 0 || offset > (lng) BUN_MAX) {
		sql_error(m, 500, "offset out of range");
		return NULL;
	}

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
		if (fmt == NULL) {
			sql_error(m, 500, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return NULL;
		}
		as.format = fmt;
		if (!isa_block_stream(bs->s))
			out = NULL;

		for (n = ol_first_node(t->columns), i = 0; n; n = n->next, i++) {
			sql_column *col = n->data;

			fmt[i].name = col->base.name;
			fmt[i].sep = (n->next) ? sep : rsep;
			fmt[i].rsep = rsep;
			fmt[i].seplen = _strlen(fmt[i].sep);
			fmt[i].type = sql_subtype_string(m->ta, &col->type);
			fmt[i].adt = ATOMindex(col->type.type->base.name);
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
				sql_error(m, 500, SQLSTATE(HY013) "failed to allocate space for column");
				return NULL;
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
			}
			fmt[i].size = ATOMsize(fmt[i].adt);
		}
		if ((msg = TABLETcreate_bats(&as, (BUN) (sz < 0 ? 1000 : sz))) == MAL_SUCCEED){
			if (!sz || (SQLload_file(cntxt, &as, bs, out, sep, rsep, ssep ? ssep[0] : 0, offset, sz, best, from_stdin, t->base.name, escape) != BUN_NONE &&
				(best || !as.error))) {
				*bats = (BAT**) GDKzalloc(sizeof(BAT *) * as.nr_attrs);
				if ( *bats == NULL){
					sql_error(m, 500, SQLSTATE(HY013) "failed to allocate space for column");
					TABLETdestroy_format(&as);
					return NULL;
				}
				msg = TABLETcollect(*bats,&as);
			}
		}
		if (as.error) {
			if( !best) sql_error(m, 500, "%s", getExceptionMessage(as.error));
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
			return (-1);
		w = tmp;
	}
	return (1);
}

static void
mvc_export_binary_bat(stream *s, BAT* bn) {
	bool sendtheap = bn->ttype != TYPE_void && bn->tvarsized;

		mnstr_printf(s, /*JSON*/"{"
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
				bn->ttype,
				bn->hseqbase, bn->tseqbase,
				bn->tsorted, bn->trevsorted,
				bn->tkey,
				bn->tnonil,
				BATtdense(bn),
				bn->batCount,
				(size_t)bn->batCount * Tsize(bn),
				sendtheap && bn->batCount > 0 ? bn->tvheap->free : 0
				);

		if (bn->batCount > 0) {
			mnstr_write(s, /* tail */ Tloc(bn, 0), bn->batCount * Tsize(bn), 1);
			if (sendtheap)
				mnstr_write(s, /* theap */ Tbase(bn), bn->tvheap->free, 1);
		}
}

static int
mvc_export_prepare_columnar(stream *out, cq *q, int nrows, sql_rel *r) {
	int error = -1;

	BAT* btype		= COLnew(0, TYPE_int, nrows, TRANSIENT);
	BAT* bdigits	= COLnew(0, TYPE_int, nrows, TRANSIENT);
	BAT* bscale		= COLnew(0, TYPE_int, nrows, TRANSIENT);
	BAT* bschema	= COLnew(0, TYPE_str, nrows, TRANSIENT);
	BAT* btable		= COLnew(0, TYPE_str, nrows, TRANSIENT);
	BAT* bcolumn	= COLnew(0, TYPE_str, nrows, TRANSIENT);
	node *n;
	sql_subtype *t;
	sql_arg *a;

	if (!btype || !bdigits || !bscale || !bschema || !btable || !bcolumn)
		goto bailout;

	if (r && is_project(r->op) && r->exps) {
		for (n = r->exps->h; n; n = n->next) {
			const char *name, *rname, *schema = NULL;
			sql_exp *e = n->data;

			t = exp_subtype(e);
			name = exp_name(e);
			if (!name && e->type == e_column && e->r)
				name = e->r;
			rname = exp_relname(e);
			if (!rname && e->type == e_column && e->l)
				rname = e->l;

			if (!schema)
				schema = "";

			if (!rname)
				rname = "";

			if (!name)
				name = "";

			if (	BUNappend(btype,	&t->type->localtype	, false) != GDK_SUCCEED ||
					BUNappend(bdigits,	&t->digits			, false) != GDK_SUCCEED ||
					BUNappend(bscale,	&t->scale			, false) != GDK_SUCCEED ||
					BUNappend(bschema,	schema				, false) != GDK_SUCCEED ||
					BUNappend(btable,	rname				, false) != GDK_SUCCEED ||
					BUNappend(bcolumn,	name				, false) != GDK_SUCCEED)
				goto bailout;
		}
	}

	if (q->f->ops) {
		int i;

		for (n = q->f->ops->h, i = 0; n; n = n->next, i++) {
			a = n->data;
			t = &a->type;

			if (	BUNappend(btype,	&t->type->localtype	, false) != GDK_SUCCEED ||
					BUNappend(bdigits,	&t->digits			, false) != GDK_SUCCEED ||
					BUNappend(bscale,	&t->scale			, false) != GDK_SUCCEED ||
					BUNappend(bschema,	str_nil				, false) != GDK_SUCCEED ||
					BUNappend(btable,	str_nil				, false) != GDK_SUCCEED ||
					BUNappend(bcolumn,	str_nil				, false) != GDK_SUCCEED)
				goto bailout;
		}
	}

	// Little hack to get the name of the corresponding compiled MAL function known to the result receiver.
	if (BUNappend(btable, q->f->imp, false) != GDK_SUCCEED)
		goto bailout;

	mvc_export_binary_bat(out, btype);
	mvc_export_binary_bat(out, bdigits);
	mvc_export_binary_bat(out, bscale);
	mvc_export_binary_bat(out, bschema);
	mvc_export_binary_bat(out, btable);
	mvc_export_binary_bat(out, bcolumn);

	error = 0;

	bailout:
		BBPreclaim(btype);
		BBPreclaim(bdigits);
		BBPreclaim(bscale);
		BBPreclaim(bschema);
		BBPreclaim(btable);
		BBPreclaim(bcolumn);
		return error;
}

int
mvc_export_prepare(backend *b, stream *out, str w)
{
	cq *q = b->q;
	node *n;
	int nparam = q->f->ops ? list_length(q->f->ops) : 0;
	int nrows = nparam;
	size_t len1 = 0, len4 = 0, len5 = 0, len6 = 0;	/* column widths */
	int len2 = 1, len3 = 1;
	sql_arg *a;
	sql_subtype *t;
	sql_rel *r = q->rel;

	if(!out || GDKembedded())
		return 0;

	if (r && (is_topn(r->op) || is_sample(r->op)))
		r = r->l;
	if (r && is_project(r->op) && r->exps) {
		unsigned int max2 = 10, max3 = 10;	/* to help calculate widths */
		nrows += list_length(r->exps);

		for (n = r->exps->h; n; n = n->next) {
			const char *name;
			sql_exp *e = n->data;
			size_t slen;

			t = exp_subtype(e);
			slen = strlen(t->type->sqlname);
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
			name = exp_relname(e);
			if (!name && e->type == e_column && e->l)
				name = e->l;
			slen = name ? strlen(name) : 0;
			if (slen > len5)
				len5 = slen;
			name = exp_name(e);
			if (!name && e->type == e_column && e->r)
				name = e->r;
			slen = name ? strlen(name) : 0;
			if (slen > len6)
				len6 = slen;
		}
	}

	/* calculate column widths */
	if (q->f->ops) {
		unsigned int max2 = 10, max3 = 10;	/* to help calculate widths */

		for (n = q->f->ops->h; n; n = n->next) {
			size_t slen;

			a = n->data;
			t = &a->type;
			slen = strlen(t->type->sqlname);
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
		}
	}

	/* write header, query type: Q_PREPARE */
	if (mnstr_printf(out, "&5 %d %d 6 %d\n"	/* TODO: add type here: r(esult) or u(pdate) */
			"%% .prepare,\t.prepare,\t.prepare,\t.prepare,\t.prepare,\t.prepare # table_name\n" "%% type,\tdigits,\tscale,\tschema,\ttable,\tcolumn # name\n" "%% varchar,\tint,\tint,\tstr,\tstr,\tstr # type\n" "%% %zu,\t%d,\t%d,\t"
			"%zu,\t%zu,\t%zu # length\n", q->id, nrows, nrows, len1, len2, len3, len4, len5, len6) < 0) {
		return -1;
	}

	if (b->client->protocol == PROTOCOL_COLUMNAR) {
		if (mnstr_flush(out, MNSTR_FLUSH_DATA) < 0)
			return -1;
		if (mvc_export_prepare_columnar(out, q, nrows, r) < 0)
			return -1;
	}
	else {
		if (r && is_project(r->op) && r->exps) {
			for (n = r->exps->h; n; n = n->next) {
				const char *name, *rname, *schema = NULL;
				sql_exp *e = n->data;

				t = exp_subtype(e);
				name = exp_name(e);
				if (!name && e->type == e_column && e->r)
					name = e->r;
				rname = exp_relname(e);
				if (!rname && e->type == e_column && e->l)
					rname = e->l;

				if (mnstr_printf(out, "[ \"%s\",\t%u,\t%u,\t\"%s\",\t\"%s\",\t\"%s\"\t]\n", t->type->sqlname, t->digits, t->scale, schema ? schema : "", rname ? rname : "", name ? name : "") < 0) {
					return -1;
				}
			}
		}

		if (q->f->ops) {
			int i;

			for (n = q->f->ops->h, i = 0; n; n = n->next, i++) {
				a = n->data;
				t = &a->type;

				if (!t || mnstr_printf(out, "[ \"%s\",\t%u,\t%u,\tNULL,\tNULL,\tNULL\t]\n", t->type->sqlname, t->digits, t->scale) < 0)
					return -1;
			}
		}
	}

	if (mvc_export_warning(out, w) != 1)
		return -1;
	return 0;
}

/*
 * improved formatting of positive integers
 */

static int
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
	return mnstr_write(s, b, 50 - (b - buf), 1) == 1;
}

static int
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
	return mnstr_write(s, b, 50 - (b - buf), 1) == 1;
}

static int
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
	return mnstr_write(s, b, 50 - (b - buf), 1) == 1;
}

static int
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
	return mnstr_write(s, b, 50 - (b - buf), 1) == 1;
}

#ifdef HAVE_HGE
static int
mvc_send_hge(stream *s, hge cnt){
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
	return mnstr_write(s, b, 50- (b-buf),1)==1;
}
#endif

ssize_t
convert2str(mvc *m, sql_class eclass, int d, int sc, int has_tz, ptr p, int mtype, char **buf, size_t *len)
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
		size_t ll = strlen(ns);
		ok = (mnstr_write(s, ns, ll, 1) == 1);
	} else if (eclass == EC_DEC) {
		l = dec_tostr((void *) (ptrdiff_t) sc, buf, len, mtype, p);
		if (l > 0)
			ok = (mnstr_write(s, *buf, l, 1) == 1);
	} else if (eclass == EC_TIME || eclass == EC_TIME_TZ) {
		struct time_res ts_res;
		ts_res.has_tz = (strcmp(sqlname, "timetz") == 0);
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_time_tostr((void *) &ts_res, buf, len, mtype, p);
		if (l >= 0)
			ok = (mnstr_write(s, *buf, l, 1) == 1);
	} else if (eclass == EC_TIMESTAMP || eclass == EC_TIMESTAMP_TZ) {
		struct time_res ts_res;
		ts_res.has_tz = (strcmp(sqlname, "timestamptz") == 0);
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_timestamp_tostr((void *) &ts_res, buf, len, mtype, p);
		if (l >= 0)
			ok = (mnstr_write(s, *buf, l, 1) == 1);
	} else if (eclass == EC_SEC) {
		l = dec_tostr((void *) (ptrdiff_t) 3, buf, len, mtype, p);
		if (l >= 0)
			ok = mnstr_write(s, *buf, l, 1) == 1;
	} else {
		switch (mtype) {
		case TYPE_bte:
			ok = mvc_send_bte(s, *(bte *) p);
			break;
		case TYPE_sht:
			ok = mvc_send_sht(s, *(sht *) p);
			break;
		case TYPE_int:
			ok = mvc_send_int(s, *(int *) p);
			break;
		case TYPE_lng:
			ok = mvc_send_lng(s, *(lng *) p);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ok = mvc_send_hge(s, *(hge*)p);
			break;
#endif
		default:
			l = (*BATatoms[mtype].atomToStr) (buf, len, p, true);
			if (l >= 0)
				ok = (mnstr_write(s, *buf, l, 1) == 1);
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
	if (!s)
		return 0;

	(void) ssep;
	if (csv && btag[0])
		ok = (mnstr_write(s, btag, strlen(btag), 1) == 1);
	if (json) {
		sep = ", ";
		seplen = strlen(sep);
	}
	for (i = 0; i < t->nr_cols && ok; i++) {
		res_col *c = t->cols + i;

		if (i != 0) {
			ok = (mnstr_write(s, sep, seplen, 1) == 1);
			if (!ok)
				break;
		}
		if (json) {
			mnstr_write(s, c->name, strlen(c->name), 1);
			mnstr_write(s, ": ", 2, 1);
		}
		ok = export_value(m, s, c->type.type->eclass, c->type.type->sqlname, c->type.digits, c->type.scale, c->p, c->mtype, &buf, &len, ns);
	}
	_DELETE(buf);
	if (ok)
		ok = (mnstr_write(s, rsep, rseplen, 1) == 1);
	b->results = res_tables_remove(b->results, t);
	return (ok) ? 0 : -1;
}

static int
mvc_export_table_columnar(stream *s, res_table *t) {
	int i;

	if (!t)
		return -1;
	if (!s)
		return 0;

	for (i = 1; i <= t->nr_cols; i++) {
		res_col *c = t->cols + (i - 1);

		if (!c->b)
			break;

		BAT *b = BATdescriptor(c->b);
		if (b == NULL)
			return -1;

		mvc_export_binary_bat(s, b);

		BBPunfix(b->batCacheid);
	}

	return 0;
}

static int
mvc_export_table(backend *b, stream *s, res_table *t, BAT *order, BUN offset, BUN nr, const char *btag, const char *sep, const char *rsep, const char *ssep, const char *ns)
{
	mvc *m = b->mvc;
	Tablet as;
	Column *fmt;
	int i;
	struct time_res *tres;
	int csv = (b->output_format == OFMT_CSV);
	int json = (b->output_format == OFMT_JSON);
	char *bj;

	if (!t)
		return -1;
	if (!s)
		return 0;

	as.nr_attrs = t->nr_cols + 1;	/* for the leader */
	as.nr = nr;
	as.offset = offset;
	fmt = as.format = (Column *) GDKzalloc(sizeof(Column) * (as.nr_attrs + 1));
	tres = GDKzalloc(sizeof(struct time_res) * (as.nr_attrs));
	if(fmt == NULL || tres == NULL) {
		GDKfree(fmt);
		GDKfree(tres);
		sql_error(m, 500, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
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
			while (--i >= 1)
				BBPunfix(fmt[i].c->batCacheid);
			GDKfree(fmt);
			GDKfree(tres);
			return -1;
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
			ts_res->has_tz = (strcmp(c->type.type->sqlname, "timetz") == 0);
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
	if (i == t->nr_cols + 1) {
		TABLEToutput_file(&as, order, s);
	}
	for (i = 0; i <= t->nr_cols; i++) {
		fmt[i].sep = NULL;
		fmt[i].rsep = NULL;
		fmt[i].type = NULL;
		fmt[i].nullstr = NULL;
	}
	TABLETdestroy_format(&as);
	GDKfree(tres);
	if (mnstr_errnr(s))
		return -1;
	return 0;
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
					assert(b);
					/* [Stefan.Manegold@cwi.nl]:
					 * Instead of an assert() or simply ignoring the problem,
					 * we could/should return an error code, but I don't know
					 * which it the correct/suitable error code -1|0|1 ?
					 *
					 return -1|0|1 ;
					 */
				}
			} else if (p) {
				l = STRwidth((const char *) p);
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
				assert(b);
				/* [Stefan.Manegold@cwi.nl]:
				 * Instead of an assert() or simply ignoring the problem,
				 * we could/should return an error code, but I don't know
				 * which it the correct/suitable error code -1|0|1 ?
				 *
				 return -1|0|1 ;
				 */
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
	int ok = 1;
	lng length = get_print_width(mtype, eclass, digits, scale, tz, bid, p);
	ok = mvc_send_lng(s, length);
	return ok;
}

int
mvc_export_operation(backend *b, stream *s, str w, lng starttime, lng mal_optimizer)
{
	mvc *m = b->mvc;

	assert(m->type == Q_SCHEMA || m->type == Q_TRANS);
	if (m->type == Q_SCHEMA) {
		if (!s || mnstr_printf(s, "&3 " LLFMT " " LLFMT "\n", starttime > 0 ? GDKusec() - starttime : 0, mal_optimizer) < 0)
			return -1;
	} else {
		if (m->session->auto_commit) {
			if (mnstr_write(s, "&4 t\n", 5, 1) != 1)
				return -1;
		} else {
			if (mnstr_write(s, "&4 f\n", 5, 1) != 1)
				return -1;
		}
	}

	if (mvc_export_warning(s, w) != 1)
		return -1;
	return 0;
}

int
mvc_export_affrows(backend *b, stream *s, lng val, str w, oid query_id, lng starttime, lng maloptimizer)
{
	mvc *m = b->mvc;
	/* if we don't have a stream, nothing can go wrong, so we return
	 * success.  This is especially vital for execution of internal SQL
	 * commands, since they don't get a stream to suppress their output.
	 * If we would fail on having no stream here, those internal commands
	 * fail too.
	 */
	if (!s)
		return 0;

	b->rowcnt = val;
	sqlvar_set_number(find_global_var(m, mvc_bind_schema(m, "sys"), "rowcnt"), b->rowcnt);
	if(GDKembedded())
		return 0;
	if (mnstr_write(s, "&2 ", 3, 1) != 1 ||
	    !mvc_send_lng(s, val) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, b->last_id) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, (lng) query_id) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, starttime > 0 ? GDKusec() - starttime : 0) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, maloptimizer) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, b->reloptimizer) ||
	    mnstr_write(s, "\n", 1, 1) != 1)
		return -1;
	if (mvc_export_warning(s, w) != 1)
		return -1;

	return 0;
}

static int
export_error(BAT *order)
{
	if (order)
		BBPunfix(order->batCacheid);
	return -1;
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

	/* query type: Q_TABLE */
	if (!(mnstr_write(s, "&1 ", 3, 1) == 1))
		return -1;

	/* id */
	if (!mvc_send_int(s, t->id) || mnstr_write(s, " ", 1, 1) != 1)
		return -1;

	/* tuple count */
	if (only_header) {
		if (t->order) {
			count = t->nr_rows;
		} else {
			count = 1;
		}
	}
	b->rowcnt = count;
	sqlvar_set_number(find_global_var(m, mvc_bind_schema(m, "sys"), "rowcnt"), b->rowcnt);
	if (!mvc_send_lng(s, (lng) count) || mnstr_write(s, " ", 1, 1) != 1)
		return -1;

	/* column count */
	if (!mvc_send_int(s, t->nr_cols) || mnstr_write(s, " ", 1, 1) != 1)
		return -1;

	/* row count, min(count, reply_size) */
	if (!mvc_send_int(s, (m->reply_size >= 0 && (BUN) m->reply_size < count) ? m->reply_size : (int) count))
		return -1;

	// export query id
	if (mnstr_write(s, " ", 1, 1) != 1 || !mvc_send_lng(s, (lng) t->query_id))
		return -1;

	// export query time
	if (mnstr_write(s, " ", 1, 1) != 1 || !mvc_send_lng(s, starttime > 0 ? GDKusec() - starttime : 0))
		return -1;

	// export MAL optimizer time
	if (mnstr_write(s, " ", 1, 1) != 1 || !mvc_send_lng(s, maloptimizer))
		return -1;

	if (mnstr_write(s, " ", 1, 1) != 1 || !mvc_send_lng(s, b->reloptimizer))
		return -1;

	if (mnstr_write(s, "\n% ", 3, 1) != 1)
		return -1;
	for (i = 0; i < t->nr_cols; i++) {
		res_col *c = t->cols + i;
		size_t len = strlen(c->tn);

		if (len && mnstr_write(s, c->tn, len, 1) != 1)
			return -1;
		if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
			return -1;
	}
	if (mnstr_write(s, " # table_name\n% ", 16, 1) != 1)
		return -1;

	for (i = 0; i < t->nr_cols; i++) {
		res_col *c = t->cols + i;

		if (strpbrk(c->name, ", \t#\"\\")) {
			char *p;
			if (mnstr_write(s, "\"", 1, 1) != 1)
				return -1;
			for (p = c->name; *p; p++) {
				if (*p == '"' || *p == '\\') {
					if (mnstr_write(s, "\\", 1, 1) != 1)
						return -1;
				}
				if (mnstr_write(s, p, 1, 1) != 1)
					return -1;
			}
			if (mnstr_write(s, "\"", 1, 1) != 1)
				return -1;
		} else {
			if (mnstr_write(s, c->name, strlen(c->name), 1) != 1)
				return -1;
		}

		if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
			return -1;
	}
	if (mnstr_write(s, " # name\n% ", 10, 1) != 1)
		return -1;

	for (i = 0; i < t->nr_cols; i++) {
		res_col *c = t->cols + i;

		if (mnstr_write(s, c->type.type->sqlname, strlen(c->type.type->sqlname), 1) != 1)
			return -1;
		if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
			return -1;
	}
	if (mnstr_write(s, " # type\n% ", 10, 1) != 1)
		return -1;
	if (compute_lengths) {
		for (i = 0; i < t->nr_cols; i++) {
			res_col *c = t->cols + i;
			int mtype = c->type.type->localtype;
			sql_class eclass = c->type.type->eclass;

			if (!export_length(s, mtype, eclass, c->type.digits, c->type.scale, type_has_tz(&c->type), c->b, c->p))
				return -1;
			if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
				return -1;
		}
		if (mnstr_write(s, " # length\n", 10, 1) != 1)
			return -1;
	}
	if (b->sizeheader) {
		if (mnstr_write(s, "% ", 2, 1) != 1)
			return -1;
		for (i = 0; i < t->nr_cols; i++) {
			res_col *c = t->cols + i;

			if (mnstr_printf(s, "%u %u", c->type.digits, c->type.scale) < 0)
				return -1;
			if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
				return -1;
		}
		if (mnstr_write(s, " # typesizes\n", 13, 1) != 1)
			return -1;
	}
	return res;
}

static int
mvc_export_file(backend *b, stream *s, res_table *t)
{
	int res = 0;
	BUN count;
	BAT *order = NULL;

	if (!t->order) {
		res = mvc_export_row(b, s, t, "", t->tsep, t->rsep, t->ssep, t->ns);
	} else {
		order = BATdescriptor(t->order);
		if (!order)
			return -1;
		count = t->nr_rows;

		res = mvc_export_table(b, s, t, order, 0, count, "", t->tsep, t->rsep, t->ssep, t->ns);
		BBPunfix(order->batCacheid);
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
	BAT *order = NULL;
	int json = (b->output_format == OFMT_JSON);

	if (!s || !t)
		return 0;

	/* Proudly supporting SQLstatementIntern's output flag */
	if (b->output_format == OFMT_NONE) {
		return 0;
	}
	/* we shouldn't have anything else but Q_TABLE here */
	assert(t->query_type == Q_TABLE);
	if (t->tsep) {
		if (header) {
			/* need header */
			if (mvc_export_head(b, s, t->id, TRUE, TRUE, starttime, maloptimizer) < 0)
				return -1;
		}
		return mvc_export_file(b, s, t);
	}

	if (!json) {
		if (mvc_export_head(b, s, res_id, TRUE, TRUE, starttime, maloptimizer) < 0)
			return -1;
	}

	assert(t->order);

	if (b->client->protocol == PROTOCOL_COLUMNAR) {
		if (mnstr_flush(s, MNSTR_FLUSH_DATA) < 0)
			return -1;
		return mvc_export_table_columnar(s, t);
	}

	order = BATdescriptor(t->order);
	if (!order)
		return -1;

	count = m->reply_size;
	if (m->reply_size != -2 && (count <= 0 || count >= t->nr_rows)) {
		count = t->nr_rows;
		clean = 1;
	}
	if (json) {
		switch(count) {
		case 0:
			res = mvc_export_table(b, s, t, order, 0, count, "{\t", "", "}\n", "\"", "null");
			break;
		case 1:
			res = mvc_export_table(b, s, t, order, 0, count, "{\n\t\"%s\" : ", ",\n\t\"%s\" : ", "\n}\n", "\"", "null");
			break;
		case 2:
			res = mvc_export_table(b, s, t, order, 0, 1, "[\n\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t},\n", "\"", "null");
			res = mvc_export_table(b, s, t, order, 1, count - 1, "\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t}\n]\n", "\"", "null");
			break;
		default:
			res = mvc_export_table(b, s, t, order, 0, 1, "[\n\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t},\n", "\"", "null");
			res = mvc_export_table(b, s, t, order, 1, count - 2, "\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t},\n", "\"", "null");
			res = mvc_export_table(b, s, t, order, count - 1, 1, "\t{\n\t\t\"%s\" : ", ",\n\t\t\"%s\" : ", "\n\t}\n]\n", "\"", "null");
		}
	} else {
		res = mvc_export_table(b, s, t, order, 0, count, "[ ", ",\t", "\t]\n", "\"", "NULL");
	}
	BBPunfix(order->batCacheid);
	if (clean)
		b->results = res_tables_remove(b->results, t);

	if (res > 0)
		res = mvc_export_warning(s, "");
	return res;
}


int
mvc_export_chunk(backend *b, stream *s, int res_id, BUN offset, BUN nr)
{
	int res = 0;
	res_table *t = res_tables_find(b->results, res_id);
	BAT *order = NULL;
	BUN cnt;

	if (!s || !t)
		return 0;

	order = BATdescriptor(t->order);
	if (!order)
		return -1;
	cnt = nr;
	if (cnt == 0)
		cnt = t->nr_rows;
	if (offset >= t->nr_rows)
		cnt = 0;
	if (cnt == BUN_NONE || offset + cnt > t->nr_rows)
		cnt = t->nr_rows - offset;

	/* query type: Q_BLOCK */
	if (!(mnstr_write(s, "&6 ", 3, 1) == 1))
		return export_error(order);

	/* result id */
	if (!mvc_send_int(s, res_id) || mnstr_write(s, " ", 1, 1) != 1)
		return export_error(order);

	/* column count */
	if (!mvc_send_int(s, t->nr_cols) || mnstr_write(s, " ", 1, 1) != 1)
		return export_error(order);

	/* row count */
	if (!mvc_send_lng(s, (lng) cnt) || mnstr_write(s, " ", 1, 1) != 1)
		return export_error(order);

	/* block offset */
	if (!mvc_send_lng(s, (lng) offset))
		return export_error(order);

	if (mnstr_write(s, "\n", 1, 1) != 1)
		return export_error(order);

	res = mvc_export_table(b, s, t, order, offset, cnt, "[ ", ",\t", "\t]\n", "\"", "NULL");
	BBPunfix(order->batCacheid);
	return res;
}


int
mvc_result_table(backend *be, oid query_id, int nr_cols, mapi_query_t type, BAT *order)
{
	res_table *t = res_table_create(be->mvc->session->tr, be->result_id++, query_id, nr_cols, type, be->results, order);
	be->results = t;
	if(t)
		return t->id;
	else
		return -1;
}

int
mvc_result_column(backend *be, char *tn, char *name, char *typename, int digits, int scale, BAT *b)
{
	/* return 0 on success, non-zero on failure */
	return res_col_create(be->mvc->session->tr, be->results, tn, name, typename, digits, scale, TYPE_bat, b) == NULL;
}

int
mvc_result_value(backend *be, const char *tn, const char *name, const char *typename, int digits, int scale, ptr *p, int mtype)
{
	/* return 0 on success, non-zero on failure */
	return res_col_create(be->mvc->session->tr, be->results, tn, name, typename, digits, scale, mtype, p) == NULL;
}
