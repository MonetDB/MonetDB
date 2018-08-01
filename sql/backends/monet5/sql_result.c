/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * author N.J. Nes
 */

#include "monetdb_config.h"
#include "sql_result.h"
#include "str.h"
#include "tablet.h"
#include "mtime.h"
#include "bat/res_table.h"
#include "bat/bat_storage.h"
#include "rel_exp.h"

#ifndef HAVE_LLABS
#define llabs(x)	((x) < 0 ? -(x) : (x))
#endif

// stpcpy definition, for systems that do not have stpcpy
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
mystpcpy (char *yydest, const char *yysrc) {
	char *yyd = yydest;
	const char *yys = yysrc;

	while ((*yyd++ = *yys++) != '\0')
	continue;

	return yyd - 1;
}

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
#define long_long_SWAP(l)				\
		((((lng)normal_int_SWAP(l))<<32) |	\
		 (0xffffffff&normal_int_SWAP(l>>32)))
#endif

#ifdef HAVE_HGE
#define huge_int_SWAP(h)					\
		((((hge)long_long_SWAP(h))<<64) |		\
		 (0xffffffffffffffff&long_long_SWAP(h>>64)))
#endif

static lng 
mnstr_swap_lng(stream *s, lng lngval) {
	return mnstr_byteorder(s) != 1234 ? long_long_SWAP(lngval) : lngval;
}

#define DEC_TOSTR(TYPE)							\
	do {								\
		char buf[64];						\
		TYPE v = *(const TYPE *) a;				\
		int scale = (int) (ptrdiff_t) extra;			\
		int cur = 63, i, done = 0;				\
		int neg = v < 0;					\
		ssize_t l;						\
		if (is_##TYPE##_nil(v)) {				\
			if (*len < 5){					\
				if (*Buf)				\
					GDKfree(*Buf);			\
				*len = 5;				\
				*Buf = GDKzalloc(*len);			\
				if (*Buf == NULL) {			\
					return -1;			\
				}					\
			}						\
			strcpy(*Buf, "NULL");				\
			return 4;					\
		}							\
		if (v<0)						\
			v = -v;						\
		buf[cur--] = 0;						\
		if (scale){						\
			for (i=0; i<scale; i++) {			\
				buf[cur--] = (char) (v%10 + '0');	\
				v /= 10;				\
			}						\
			buf[cur--] = '.';				\
		}							\
		while (v) {						\
			buf[cur--] = (char ) (v%10 + '0');		\
			v /= 10;					\
			done = 1;					\
		}							\
		if (!done)						\
			buf[cur--] = '0';				\
		if (neg)						\
			buf[cur--] = '-';				\
		l = (64-cur-1);						\
		if ((ssize_t) *len < l){				\
			if (*Buf)					\
				GDKfree(*Buf);				\
			*len = (size_t) l+1;				\
			*Buf = GDKzalloc(*len);				\
			if (*Buf == NULL) {				\
				return -1;				\
			}						\
		}							\
		strcpy(*Buf, buf+cur+1);				\
		return l-1;						\
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
	int i;
	ssize_t len1;
	size_t big = 128;
	char buf1[128], *s1 = buf1, *s;
	lng val = 0, timezone = ts_res->timezone;
	daytime tmp;
	const daytime *a = A;
	daytime mtime = 24 * 60 * 60 * 1000;

	(void) type;
	if (ts_res->has_tz)
		val = *a + timezone;
	else
		val = *a;
	if (val < 0)
		val = mtime + val;
	if (val > mtime)
		val = val - mtime;
	tmp = (daytime) val;

	len1 = daytime_tostr(&s1, &big, &tmp);
	if (len1 < 0)
		return -1;
	if (len1 == 3 && strcmp(s1, "nil") == 0) {
		if (*len < 4 || *buf == NULL) {
			if (*buf)
				GDKfree(*buf);
			*buf = (str) GDKzalloc(*len = 4);
			if (*buf == NULL) {
				return -1;
			}
		}
		strcpy(*buf, s1);
		return len1;
	}

	/* fixup the fraction, default is 3 */
	len1 += (ts_res->fraction - 3);
	if (ts_res->fraction == 0)
		len1--;

	if (*len < (size_t) len1 + 8) {
		if (*buf)
			GDKfree(*buf);
		*buf = (str) GDKzalloc(*len = len1 + 8);
		if (*buf == NULL) {
			return -1;
		}
	}
	s = *buf;
	strcpy(s, buf1);
	s += len1;
	s[0] = 0;
	/* extra zero's for usec's */
	for (i = 3; i < ts_res->fraction; i++)
		s[-i + 2] = '0';

	if (ts_res->has_tz) {
		timezone = ts_res->timezone / 60000;
		*s++ = (ts_res->timezone >= 0) ? '+' : '-';
		sprintf(s, "%02d:%02d", (int) (llabs(timezone) / 60), (int) (llabs(timezone) % 60));
		s += 5;
	}
	return (ssize_t) (s - *buf);
}

static ssize_t
sql_timestamp_tostr(void *TS_RES, char **buf, size_t *len, int type, const void *A)
{
	struct time_res *ts_res = TS_RES;
	int i;
	ssize_t len1, len2;
	size_t big = 128;
	char buf1[128], buf2[128], *s, *s1 = buf1, *s2 = buf2;
	timestamp tmp;
	const timestamp *a = A;
	lng timezone = ts_res->timezone;

	(void) type;
	if (ts_res->has_tz) {
		MTIMEtimestamp_add(&tmp, a, &timezone);
		len1 = date_tostr(&s1, &big, &tmp.days);
		len2 = daytime_tostr(&s2, &big, &tmp.msecs);
	} else {
		len1 = date_tostr(&s1, &big, &a->days);
		len2 = daytime_tostr(&s2, &big, &a->msecs);
	}
	if (len1 < 0 || len2 < 0) {
		GDKfree(s1);
		GDKfree(s2);
		return -1;
	}

	/* fixup the fraction, default is 3 */
	len2 += (ts_res->fraction - 3);
	if (ts_res->fraction == 0)
		len2--;

	if (*len < (size_t) len1 + (size_t) len2 + 8) {
		if (*buf)
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
	/* extra zero's for usec's */
	for (i = 3; i < ts_res->fraction; i++)
		s[-i + 2] = '0';

	if (ts_res->has_tz) {
		timezone = ts_res->timezone / 60000;
		*s++ = (ts_res->timezone >= 0) ? '+' : '-';
		sprintf(s, "%02d:%02d", (int) (llabs(timezone) / 60), (int) (llabs(timezone) % 60));
		s += 5;
	}
	return (ssize_t) (s - *buf);
}

static int
STRwidth(const char *s)
{
	int len = 0;
	int c;
	int n;

	if (GDK_STRNIL(s))
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
		l = STRwidth((const char *) BUNtail(bi, p));

		if (is_int_nil(l))
			l = 0;
		if (l > max)
			max = l;
	}
	return max;
}

static size_t
bat_max_btelength(BAT *b)
{
	BUN p, q;
	lng max = 0;
	lng min = 0;
	size_t ret = 0;
	BATiter bi = bat_iterator(b);

	BATloop(b, p, q) {
		lng m = 0;
		bte l = *((bte *) BUNtail(bi, p));

		if (!is_bte_nil(l))
			m = l;
		if (m > max)
			max = m;
		if (m < min)
			min = m;
	}

	if (-min > max / 10) {
		max = -min;
		ret++;		/* '-' */
	}
	while (max /= 10)
		ret++;
	ret++;
	return ret;
}

static size_t
bat_max_shtlength(BAT *b)
{
	BUN p, q;
	lng max = 0;
	lng min = 0;
	size_t ret = 0;
	BATiter bi = bat_iterator(b);

	BATloop(b, p, q) {
		lng m = 0;
		sht l = *((sht *) BUNtail(bi, p));

		if (!is_sht_nil(l))
			m = l;
		if (m > max)
			max = m;
		if (m < min)
			min = m;
	}

	if (-min > max / 10) {
		max = -min;
		ret++;		/* '-' */
	}
	while (max /= 10)
		ret++;
	ret++;
	return ret;
}

static size_t
bat_max_intlength(BAT *b)
{
	BUN p, q;
	lng max = 0;
	lng min = 0;
	size_t ret = 0;
	BATiter bi = bat_iterator(b);

	BATloop(b, p, q) {
		lng m = 0;
		int l = *((int *) BUNtail(bi, p));

		if (!is_int_nil(l))
			m = l;
		if (m > max)
			max = m;
		if (m < min)
			min = m;
	}

	if (-min > max / 10) {
		max = -min;
		ret++;		/* '-' */
	}
	while (max /= 10)
		ret++;
	ret++;
	return ret;
}

static size_t
bat_max_lnglength(BAT *b)
{
	BUN p, q;
	lng max = 0;
	lng min = 0;
	size_t ret = 0;
	BATiter bi = bat_iterator(b);

	BATloop(b, p, q) {
		lng m = 0;
		lng l = *((lng *) BUNtail(bi, p));

		if (!is_lng_nil(l))
			m = l;
		if (m > max)
			max = m;
		if (m < min)
			min = m;
	}

	if (-min > max / 10) {
		max = -min;
		ret++;		/* '-' */
	}
	while (max /= 10)
		ret++;
	ret++;
	return ret;
}

#ifdef HAVE_HGE
static size_t
bat_max_hgelength(BAT *b)
{
	BUN p, q;
	hge max = 0;
	hge min = 0;
	size_t ret = 0;
	BATiter bi = bat_iterator(b);

	BATloop(b, p, q) {
		hge m = 0;
		hge l = *((hge *)BUNtail(bi, p));

		if (!is_hge_nil(l))
			m = l;
		if (m > max) max = m;
		if (m < min) min = m;
	}

	if (-min > max / 10) {
		max = -min;
		ret++;		/* '-' */
	}
	while (max /= 10)
		ret++;
	ret++;
	return ret;
}
#endif

#define DEC_FRSTR(X)							\
	do {								\
		sql_column *col = c->extra;				\
		sql_subtype *t = &col->type;				\
									\
		unsigned int i, neg = 0;				\
		X *r;							\
		X res = 0;						\
		while(isspace((unsigned char) *s))			\
			s++;						\
		if (*s == '-'){						\
			neg = 1;					\
			s++;						\
		} else if (*s == '+'){					\
			neg = 0;					\
			s++;						\
		}							\
		for (i = 0; *s && *s != '.' && ((res == 0 && *s == '0') || i < t->digits - t->scale); s++) { \
			if (!*s || *s < '0' || *s > '9')		\
				return NULL;				\
			res *= 10;					\
			res += (*s-'0');				\
			if (res)					\
				i++;					\
		}							\
		if (!*s && t->scale) {					\
			for( i = 0; i < t->scale; i++) {		\
				res *= 10;				\
			}						\
		}							\
		while(isspace((unsigned char) *s))			\
			s++;						\
		if (*s) {						\
			if (*s != '.')					\
				return NULL;				\
			s++;						\
			for (i = 0; *s && *s >= '0' && *s <= '9' && i < t->scale; i++, s++) { \
				res *= 10;				\
				res += *s - '0';			\
			}						\
			while(isspace((unsigned char) *s))		\
				s++;					\
			for (; i < t->scale; i++) {			\
				res *= 10;				\
			}						\
		}							\
		if (*s)							\
			return NULL;					\
		r = c->data;						\
		if (r == NULL &&					\
		    (r = GDKzalloc(sizeof(X))) == NULL)			\
			return NULL;					\
		c->data = r;						\
		if (neg)						\
			*r = -res;					\
		else							\
			*r = res;					\
		return (void *) r;					\
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

	if( strcmp(s,"nil")== 0)
		return NULL;
	if (*s == '-') {
		neg = 1;
		s++;
	} else if (*s == '+') {
		neg = 0;
		s++;
	}
	for (i = 0; i < (19 - 3) && *s && *s != '.'; i++, s++) {
		if (!*s || *s < '0' || *s > '9')
			return NULL;
		res *= 10;
		res += (*s - '0');
	}
	if (!*s) {
		for (i = 0; i < 3; i++) {
			res *= 10;
		}
	}
	if (*s) {
		if (*s != '.')
			return NULL;
		s++;
		for (i = 0; *s && i < 3; i++, s++) {
			if (*s < '0' || *s > '9')
				return NULL;
			res *= 10;
			res += (*s - '0');
		}
		for (; i < 3; i++) {
			res *= 10;
		}
	}
	if (*s)
		return NULL;
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
	const char *e; 

	if (type == TYPE_str) {
		sql_column *col = (sql_column *) c->extra;
		int slen;

		for (e = s; *e; e++)
			;
		len = (ssize_t) (e - s + 1);

		/* or shouldn't len rather be ssize_t, here? */

		if ((ssize_t) c->len < len) {
			void *p;
			c->len = (size_t) len;
			if ((p = GDKrealloc(c->data, c->len)) == NULL) {
				GDKfree(c->data);
				c->data = NULL;
				c->len = 0;
				return NULL;
			}
			c->data = p;
		}
		if (s == e || *s == 0) {
			len = -1;
			*(char *) c->data = 0;
		} else if ((len = GDKstrFromStr(c->data, (unsigned char *) s, (ssize_t) (e - s))) < 0) {
			return NULL;
		}
		s = c->data;
		STRLength(&slen, (const str *) &s);
		if (col->type.digits > 0 && len > 0 && slen > (int) col->type.digits) {
			len = STRwidth(c->data);
			if (len > (ssize_t) col->type.digits)
				return NULL;
		}
		return c->data;
	}
	// All other values are not allowed to the MonetDB nil value
	if( strcmp(s,"nil")== 0)
		return NULL;

	len = (*BATatoms[type].atomFromStr) (s, &c->len, &c->data);
	if (len < 0)
		return NULL;
	if (len == 0 || s[len]) {
		/* decimals can be converted to integers when *.000 */
		if (s[len++] == '.')
			switch (type) {
			case TYPE_bte:
			case TYPE_int:
			case TYPE_lng:
			case TYPE_sht:
#ifdef HAVE_HGE
			case TYPE_hge:
#endif
				while (s[len] == '0')
					len++;
				if (s[len] == 0)
					return c->data;
			}
		return NULL;
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
		return (*BATatoms[type].atomToStr) (buf, len, a);
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
mvc_import_table(Client cntxt, BAT ***bats, mvc *m, bstream *bs, sql_table *t, char *sep, char *rsep, char *ssep, char *ns, lng sz, lng offset, int locked, int best)
{
	int i = 0, j;
	node *n;
	Tablet as;
	Column *fmt;
	BUN cnt = 0;
	str msg = MAL_SUCCEED;

	*bats =0;	// initialize the receiver

	if (!bs) {
		sql_error(m, 500, "no stream (pointer) provided");
		m->type = -1;
		return NULL;
	}
	if (mnstr_errnr(bs->s)) {
		sql_error(m, 500, "stream not open %d", mnstr_errnr(bs->s));
		m->type = -1;
		return NULL;
	}
	if (offset < 0 || offset > (lng) BUN_MAX) {
		sql_error(m, 500, "offset out of range");
		m->type = -1;
		return NULL;
	}

	if (locked) {
		/* flush old changes to disk */
		sql_trans_end(m->session);
		store_apply_deltas();
		sql_trans_begin(m->session);
	}

	if (offset > 0)
		offset--;
	if (t->columns.set) {
		stream *out = m->scanner.ws;

		memset((char *) &as, 0, sizeof(as));
		as.nr_attrs = list_length(t->columns.set);
		as.nr = (sz < 1) ? BUN_NONE : (BUN) sz;
		as.offset = (BUN) offset;
		as.error = NULL;
		as.tryall = 0;
		as.complaints = NULL;
		as.filename = m->scanner.rs == bs ? NULL : "";
		fmt = as.format = (Column *) GDKzalloc(sizeof(Column) * (as.nr_attrs + 1));
		if (fmt == NULL) {
			sql_error(m, 500, "failed to allocate memory ");
			return NULL;
		}
		if (!isa_block_stream(bs->s))
			out = NULL;

		for (n = t->columns.set->h, i = 0; n; n = n->next, i++) {
			sql_column *col = n->data;

			fmt[i].name = col->base.name;
			fmt[i].sep = (n->next) ? sep : rsep;
			fmt[i].rsep = rsep;
			fmt[i].seplen = _strlen(fmt[i].sep);
			fmt[i].type = sql_subtype_string(&col->type);
			fmt[i].adt = ATOMindex(col->type.type->base.name);
			fmt[i].tostr = &_ASCIIadt_toStr;
			fmt[i].frstr = &_ASCIIadt_frStr;
			fmt[i].extra = col;
			fmt[i].len = ATOMlen(fmt[i].adt, ATOMnilptr(fmt[i].adt));
			fmt[i].data = GDKzalloc(fmt[i].len);
			if(fmt[i].data == NULL || fmt[i].type == NULL) {
				for (j = 0; j < i; j++) {
					GDKfree(fmt[j].type);
					GDKfree(fmt[j].data);
					BBPunfix(fmt[j].c->batCacheid);
				}
				GDKfree(fmt[i].type);
				GDKfree(fmt[i].data);
				sql_error(m, 500, "failed to allocate space for column");
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

			if (locked) {
				BAT *b = store_funcs.bind_col(m->session->tr, col, RDONLY);
				if (b == NULL) {
					for (j = 0; j < i; j++) {
						GDKfree(fmt[j].type);
						GDKfree(fmt[j].data);
						BBPunfix(fmt[j].c->batCacheid);
					}
					GDKfree(fmt[i].type);
					GDKfree(fmt[i].data);
					sql_error(m, 500, "failed to bind to table column");
					return NULL;
				}

				HASHdestroy(b);

				fmt[i].c = b;
				cnt = BATcount(b);
				if (sz > 0 && BATcapacity(b) < (BUN) sz) {
					if (BATextend(fmt[i].c, (BUN) sz) != GDK_SUCCEED) {
						for (j = 0; j <= i; j++) {
							GDKfree(fmt[j].type);
							GDKfree(fmt[j].data);
							BBPunfix(fmt[j].c->batCacheid);
						}
						sql_error(m, 500, "failed to allocate space for column");
						return NULL;
					}
				}
				fmt[i].ci = bat_iterator(fmt[i].c);
				fmt[i].c->batDirtydesc = TRUE;
			}
		}
		if ( (locked || (msg = TABLETcreate_bats(&as, (BUN) (sz < 0 ? 1000 : sz))) == MAL_SUCCEED)  ){
			if (!sz || (SQLload_file(cntxt, &as, bs, out, sep, rsep, ssep ? ssep[0] : 0, offset, sz, best) != BUN_NONE && 
				(best || !as.error))) {
				*bats = (BAT**) GDKzalloc(sizeof(BAT *) * as.nr_attrs);
				if ( *bats == NULL){
					sql_error(m, 500, "failed to allocate space for column");
					TABLETdestroy_format(&as);
					return NULL;
				}
				if (locked)
					msg = TABLETcollect_parts(*bats,&as, cnt);
				else
					msg = TABLETcollect(*bats,&as);
			} else if (locked) {	/* restore old counts */
				for (n = t->columns.set->h, i = 0; n; n = n->next, i++) {
					sql_column *col = n->data;
					BAT *b = store_funcs.bind_col(m->session->tr, col, RDONLY);
					if (b == NULL)
						sql_error(m, 500, "failed to bind to temporary column");
					else {
						BATsetcount(b, cnt);
						BBPunfix(b->batCacheid);
					}
				}
			}
		}
		if (locked) {	/* fix delta structures and transaction */
			for (n = t->columns.set->h, i = 0; n; n = n->next, i++) {
				sql_column *c = n->data;
				BAT *b = store_funcs.bind_col(m->session->tr, c, RDONLY);
				sql_delta *d = c->data;

				c->base.wtime = t->base.wtime = t->s->base.wtime = m->session->tr->wtime = m->session->tr->wstime;
				if ( b == NULL)
					sql_error(m, 500, "failed to bind to delta column");
				else {
					d->ibase = (oid) (d->cnt = BATcount(b));
					BBPunfix(b->batCacheid);
				}
			}
		}
		if (as.error) {
			if( !best) sql_error(m, 500, "%s", as.error);
			freeException(as.error);
			as.error = NULL;
		}
		for (n = t->columns.set->h, i = 0; n; n = n->next, i++) {
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

int
mvc_export_prepare(mvc *c, stream *out, cq *q, str w)
{
	node *n;
	int nparam = c->params ? list_length(c->params) : 0;
	int nrows = nparam;
	size_t len1 = 0, len4 = 0, len5 = 0, len6 = 0;	/* column widths */
	int len2 = 1, len3 = 1;
	sql_arg *a;
	sql_subtype *t;
	sql_rel *r = q->rel;

	if (!out)
		return 0;

	if (is_topn(r->op))
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
			name = e->rname;
			if (!name && e->type == e_column && e->l)
				name = e->l;
			slen = name ? strlen(name) : 0;
			if (slen > len5)
				len5 = slen;
			name = e->name;
			if (!name && e->type == e_column && e->r)
				name = e->r;
			slen = name ? strlen(name) : 0;
			if (slen > len6)
				len6 = slen;
		}
	}
	/* calculate column widths */
	if (c->params) {
		unsigned int max2 = 10, max3 = 10;	/* to help calculate widths */

		for (n = c->params->h; n; n = n->next) {
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

	if (r && is_project(r->op) && r->exps) {
		for (n = r->exps->h; n; n = n->next) {
			const char *name, *rname, *schema = NULL;
			sql_exp *e = n->data;

			t = exp_subtype(e);
			name = e->name;
			if (!name && e->type == e_column && e->r)
				name = e->r;
			rname = e->rname;
			if (!rname && e->type == e_column && e->l)
				rname = e->l;

			if (mnstr_printf(out, "[ \"%s\",\t%u,\t%u,\t\"%s\",\t\"%s\",\t\"%s\"\t]\n", t->type->sqlname, t->digits, t->scale, schema ? schema : "", rname ? rname : "", name ? name : "") < 0) {
				return -1;
			}
		}
	}
	if (c->params) {
		int i;

		q->paramlen = nparam;
		q->params = SA_NEW_ARRAY(q->sa, sql_subtype, nrows);
		for (n = c->params->h, i = 0; n; n = n->next, i++) {
			a = n->data;
			t = &a->type;

			if (t) {
				if (mnstr_printf(out, "[ \"%s\",\t%u,\t%u,\tNULL,\tNULL,\tNULL\t]\n", t->type->sqlname, t->digits, t->scale) < 0) {
					return -1;
				}
				/* add to the query cache parameters */
				q->params[i] = *t;
			} else {
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

int
convert2str(mvc *m, int eclass, int d, int sc, int has_tz, ptr p, int mtype, char **buf, int len)
{
	size_t len2 = (size_t) len;
	ssize_t l = 0;

	if (!p || ATOMcmp(mtype, ATOMnilptr(mtype), p) == 0) {
		(*buf)[0] = '\200';
		(*buf)[1] = 0;
	} else if (eclass == EC_DEC) {
		l = dec_tostr((void *) (ptrdiff_t) sc, buf, &len2, mtype, p);
	} else if (eclass == EC_TIME) {
		struct time_res ts_res;
		ts_res.has_tz = has_tz;
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_time_tostr((void *) &ts_res, buf, &len2, mtype, p);

	} else if (eclass == EC_TIMESTAMP) {
		struct time_res ts_res;
		ts_res.has_tz = has_tz;
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_timestamp_tostr((void *) &ts_res, buf, &len2, mtype, p);
	} else if (eclass == EC_BIT) {
		bit b = *(bit *) p;
		if (len <= 0 || len > 5) {
			if (b)
				strcpy(*buf, "true");
			else
				strcpy(*buf, "false");
		} else {
			(*buf)[0] = b?'t':'f';
			(*buf)[1] = 0;
		}
	} else {
		l = (*BATatoms[mtype].atomToStr) (buf, &len2, p);
	}
	return (int) l;
}

static int
export_value(mvc *m, stream *s, int eclass, char *sqlname, int d, int sc, ptr p, int mtype, char **buf, size_t *len, str ns)
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
	} else if (eclass == EC_TIME) {
		struct time_res ts_res;
		ts_res.has_tz = (strcmp(sqlname, "timetz") == 0);
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_time_tostr((void *) &ts_res, buf, len, mtype, p);
		if (l >= 0)
			ok = (mnstr_write(s, *buf, l, 1) == 1);
	} else if (eclass == EC_TIMESTAMP) {
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
			l = (*BATatoms[mtype].atomToStr) (buf, len, p);
			if (l >= 0)
				ok = (mnstr_write(s, *buf, l, 1) == 1);
		}
	}
	return ok;
}

static int
mvc_export_row(backend *b, stream *s, res_table *t, str btag, str sep, str rsep, str ssep, str ns)
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
	m->results = res_tables_remove(m->results, t);
	return (ok) ? 0 : -1;
}

static int type_supports_binary_transfer(sql_type *type) {
	return
		type->eclass == EC_BIT ||
		type->eclass == EC_POS ||
		type->eclass == EC_CHAR ||
		type->eclass == EC_STRING ||
		type->eclass == EC_DEC ||
		type->eclass == EC_BLOB ||
		type->eclass == EC_FLT ||
		type->eclass == EC_NUM ||
		type->eclass == EC_DATE ||
		type->eclass == EC_TIME ||
		type->eclass == EC_SEC ||
		type->eclass == EC_MONTH ||
		type->eclass == EC_TIMESTAMP;
}

static int write_str_term(stream* s, const char* const val) {
	return mnstr_writeStr(s, val) && mnstr_writeBte(s, 0);
}

// align to 8 bytes
static char* 
eight_byte_align(char* ptr) {
	return (char*) (((size_t) ptr + 7) & ~7);
}

static int
mvc_export_table_prot10(backend *b, stream *s, res_table *t, BAT *order, BUN offset, BUN nr) {
	lng count = 0;
	size_t row = 0;
	size_t srow = 0;
	size_t varsized = 0;
	size_t length_prefixed = 0;
	lng fixed_lengths = 0;
	int fres = 0;
	size_t i = 0;
	size_t bsize = b->client->blocksize;
	BATiter *iterators = NULL;
	char *result = NULL;
	size_t length = 0;
	int initial_transfer = 1;

	(void) order; // FIXME: respect explicitly ordered output

	iterators = GDKzalloc(sizeof(BATiter) * t->nr_cols);
	if (!iterators) {
		return -1;
	}

	// ensure the buffer is currently empty
	assert(bs2_buffer(s).pos == 0);

	// inspect all the columns to figure out how many bytes it takes to transfer one row
	for (i = 0; i < (size_t) t->nr_cols; i++) {
		res_col *c = t->cols + i;
		BAT *b = BATdescriptor(c->b);
		int mtype;
		size_t typelen;
		int convert_to_string = !type_supports_binary_transfer(c->type.type);
		sql_type *type = c->type.type;

		if (b == NULL) {
			while (i > 0) {
				i--;
				BBPunfix(iterators[i].b->batCacheid);
			}
			GDKfree(iterators);
			return -1;
		}
		mtype = b->ttype;
		typelen = ATOMsize(mtype);
		iterators[i] = bat_iterator(b);
		
		if (type->eclass == EC_TIMESTAMP || type->eclass == EC_DATE) {
			// dates and timestamps are converted to Unix Timestamps
			mtype = TYPE_lng;
			typelen = sizeof(lng);	
		}
		if (ATOMvarsized(mtype) || convert_to_string) {
			varsized++;
			length_prefixed++;
		} else {
			fixed_lengths += typelen;
		}
	}

	// now perform the actual transfer
	row = srow = offset;
	count = offset + nr;
	while (row < (size_t) count) {
		char* message_header;
		char *buf = bs2_buffer(s).buf;
		size_t crow = 0;
		size_t bytes_left = bsize - sizeof(lng) - 2 * sizeof(char) - 1;
		// potential padding that has to be added for each column
		bytes_left -= t->nr_cols * 7;

		// every varsized member has an 8-byte header indicating the length of the header in the block
		// subtract this from the amount of bytes left
		bytes_left -= length_prefixed * sizeof(lng);

		if (varsized == 0) {
			// no varsized elements, so we can immediately compute the amount of elements
			if (fixed_lengths == 0) {
				row = (size_t) count;
			} else {
				row = (size_t) (srow + bytes_left / fixed_lengths);
				row = row > (size_t) count ? (size_t) count : row;
			}
		} else {
			size_t rowsize = 0;
			// we have varsized elements, so we have to loop to determine how many rows fit into a buffer
			while (row < (size_t) count) {
				rowsize = (size_t) fixed_lengths;
				for (i = 0; i < (size_t) t->nr_cols; i++) {
					res_col *c = t->cols + i;
					int mtype = iterators[i].b->ttype;
					int convert_to_string = !type_supports_binary_transfer(c->type.type);
					if (convert_to_string || ATOMvarsized(mtype)) {
						if (c->type.type->eclass == EC_BLOB) {
							blob *b = (blob*) BUNtail(iterators[i], row);
							rowsize += sizeof(lng) + ((b->nitems == ~(size_t) 0) ? 0 : b->nitems);
						} else {
							ssize_t slen = 0;
							if (convert_to_string) {
								void *element = (void*) BUNtail(iterators[i], crow);
								if ((slen = BATatoms[mtype].atomToStr(&result, &length, element)) < 0) {
									fres = -1;
									goto cleanup;
								}
							} else {
								slen = (ssize_t) strlen((const char*) BUNtail(iterators[i], row));
							}
							rowsize += slen + 1;
						}
					}
				}
				if (bytes_left < rowsize) {
					break;
				}
				bytes_left -= rowsize;
				row++;
			}
			if (row == srow) {
				lng new_size = rowsize + 1024;
				if (!mnstr_writeLng(s, (lng) -1) || 
					!mnstr_writeLng(s, new_size) || 
					mnstr_flush(s) < 0) {
					fres = -1;
					goto cleanup;
				}
				row = srow + 1;
				if (bs2_resizebuf(s, (size_t) new_size) < 0) {
					// failed to resize stream buffer
					fres = -1;
					goto cleanup;
				}
				buf = bs2_buffer(s).buf;
				bsize = (size_t) new_size;
			}
		}

		// have to transfer at least one row
		assert(row > srow);
		// buffer has to be empty currently
		assert(bs2_buffer(s).pos == 0);

		// initial message
		message_header = "+\n";
		if (initial_transfer == 0) {
			// continuation message
			message_header = "-\n";
		}
		initial_transfer = 0;
		
		if (!mnstr_writeStr(s, message_header) || !mnstr_writeLng(s, (lng)(row - srow))) {
			fres = -1;
			goto cleanup;
		}
		buf += sizeof(lng) + 2 * sizeof(char);

		for (i = 0; i < (size_t) t->nr_cols; i++) {
			res_col *c = t->cols + i;
			int mtype = iterators[i].b->ttype;
			int convert_to_string = !type_supports_binary_transfer(c->type.type);
			buf = eight_byte_align(buf);
			if (ATOMvarsized(mtype) || convert_to_string) {
				if (c->type.type->eclass == EC_BLOB) {
					// transfer blobs as [lng][data] combination
					char *startbuf = buf;
					buf += sizeof(lng);
					for (crow = srow; crow < row; crow++) {
						blob *b = (blob*) BUNtail(iterators[i], crow);
						if (b->nitems == ~(size_t) 0) {
							(*(lng*)buf) = mnstr_swap_lng(s, -1);
							buf += sizeof(lng);
						} else {
							(*(lng*)buf) = mnstr_swap_lng(s, (lng) b->nitems);
							buf += sizeof(lng);
							memcpy(buf, b->data, b->nitems);
							buf += b->nitems;
						}
					}
					// after the loop we know the size of the column, so write it
					*((lng*)startbuf) = mnstr_swap_lng(s, buf - (startbuf + sizeof(lng)));
				} else {
					// for variable length strings and large fixed strings we use varints
					// variable columns are prefixed by a length, 
					// but since we don't know the length yet, just skip over it for now
					char *startbuf = buf;
					buf += sizeof(lng);
					for (crow = srow; crow < row; crow++) {
						void *element = (void*) BUNtail(iterators[i], crow);
						const char* str;
						if (convert_to_string) {
							if (BATatoms[mtype].atomCmp(element, BATatoms[mtype].atomNull) == 0) {
								str = str_nil;
							} else {
								if (BATatoms[mtype].atomToStr(&result, &length, element) < 0) {
									fres = -1;
									goto cleanup;
								}
								// string conversion functions add quotes for the old protocol
								// because obviously adding quotes in the string conversion function
								// makes total sense, rather than adding the quotes in the protocol
								// thus because of this totally, 100% sensical implementation
								// we remove the quotes again here
								if (result[0] == '"') {
									result[strlen(result) - 1] = '\0';
									str = result + 1;
								} else {
									str = result;
								}
							}
						} else {
							str = (char*) element;
						}
						buf = mystpcpy(buf, str) + 1;
						assert(buf - bs2_buffer(s).buf <= (lng) bsize);
					}
					*((lng*)startbuf) = mnstr_swap_lng(s, buf - (startbuf + sizeof(lng)));
				}
			} else {
				size_t atom_size = ATOMsize(mtype);
				if (c->type.type->eclass == EC_DEC) {
					atom_size = ATOMsize(mtype);
				}
				if (c->type.type->eclass == EC_TIMESTAMP) {
					// convert timestamp values to epoch
					lng time;
					size_t j = 0;
					int swap = mnstr_byteorder(s) != 1234;
					timestamp *times = (timestamp*) Tloc(iterators[i].b, srow);
					lng *bufptr = (lng*) buf;
					for(j = 0; j < (row - srow); j++) {
						MTIMEepoch2lng(&time, times + j);
						bufptr[j] = swap ? long_long_SWAP(time) : time;
					}
					atom_size = sizeof(lng);
				} else if (c->type.type->eclass == EC_DATE) {
					// convert dates into timestamps since epoch
					lng time;
					timestamp tstamp;
					size_t j = 0;
					int swap = mnstr_byteorder(s) != 1234;
					date *dates = (date*) Tloc(iterators[i].b, srow);
					lng *bufptr = (lng*) buf;
					for(j = 0; j < (row - srow); j++) {
						tstamp.payload.p_days = dates[j];
						MTIMEepoch2lng(&time, &tstamp);
						bufptr[j] = swap ? long_long_SWAP(time) : time;
					}
					atom_size = sizeof(lng);
				} else {
					if (mnstr_byteorder(s) != 1234) {
						size_t j = 0;
						switch (ATOMstorage(mtype)) {
						case TYPE_sht: {
							short *bufptr = (short*) buf;
							short *exported_values = (short*) Tloc(iterators[i].b, srow);
							for(j = 0; j < (row - srow); j++) {
								bufptr[j] = short_int_SWAP(exported_values[j]);
							}
							break;
						}
						case TYPE_int: {
							int *bufptr = (int*) buf;
							int *exported_values = (int*) Tloc(iterators[i].b, srow);
							for(j = 0; j < (row - srow); j++) {
								bufptr[j] = normal_int_SWAP(exported_values[j]);
							}
							break;
						}
						case TYPE_lng: {
							lng *bufptr = (lng*) buf;
							lng *exported_values = (lng*) Tloc(iterators[i].b, srow);
							for(j = 0; j < (row - srow); j++) {
								bufptr[j] = long_long_SWAP(exported_values[j]);
							}
							break;
						}
#ifdef HAVE_HGE
						case TYPE_hge: {
							hge *bufptr = (hge*) buf;
							hge *exported_values = (hge*) Tloc(iterators[i].b, srow);
							for(j = 0; j < (row - srow); j++) {
								bufptr[j] = huge_int_SWAP(exported_values[j]);
							}
							break;
						}
#endif
						}
					} else {
						memcpy(buf, Tloc(iterators[i].b, srow), (row - srow) * atom_size);
					}
				}
				buf += (row - srow) * atom_size;
			}
		}

		assert(buf >= bs2_buffer(s).buf);
		if (buf - bs2_buffer(s).buf > (lng) bsize) {
			fprintf(stderr, "Too many bytes in the buffer.\n");
			fres = -1;
			goto cleanup;
		}

		bs2_setpos(s, buf - bs2_buffer(s).buf);
		// flush the current chunk
		if (mnstr_flush(s) < 0) {
			fres = -1;
			goto cleanup;
		}
		srow = row;
	}
cleanup:
	if (iterators) {
		for (i = 0; i < (size_t) t->nr_cols; i++)
			BBPunfix(iterators[i].b->batCacheid);
		GDKfree(iterators);
	}
	if (result) {
		GDKfree(result);
	}
	if (mnstr_errnr(s))
		return -1;
	return fres;
}

static int
mvc_export_table(backend *b, stream *s, res_table *t, BAT *order, BUN offset, BUN nr, char *btag, char *sep, char *rsep, char *ssep, char *ns)
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

	if (b->client->protocol == PROTOCOL_10) {
		return mvc_export_table_prot10(b, s, t, order, offset, nr);
	}

	as.nr_attrs = t->nr_cols + 1;	/* for the leader */
	as.nr = nr;
	as.offset = offset;
	fmt = as.format = (Column *) GDKzalloc(sizeof(Column) * (as.nr_attrs + 1));
	tres = GDKzalloc(sizeof(struct time_res) * (as.nr_attrs));
	if(fmt == NULL || tres == NULL) {
		GDKfree(fmt);
		GDKfree(tres);
		sql_error(m, 500, "failed to allocate space");
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
		} else if (c->type.type->eclass == EC_TIMESTAMP) {
			struct time_res *ts_res = tres + (i - 1);
			ts_res->has_tz = (strcmp(c->type.type->sqlname, "timestamptz") == 0);
			ts_res->fraction = c->type.digits ? c->type.digits - 1 : 0;
			ts_res->timezone = m->timezone;

			fmt[i].tostr = &sql_timestamp_tostr;
			fmt[i].frstr = NULL;
			fmt[i].extra = ts_res;
		} else if (c->type.type->eclass == EC_TIME) {
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
get_print_width(int mtype, int eclass, int digits, int scale, int tz, bat bid, ptr p)
{
	size_t count = 0, incr = 0;;

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
					l = bat_max_strlength(b);
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
		return count;
	} else if (eclass == EC_DATE) {
		return 10;
	} else if (eclass == EC_TIME) {
		count = 8;
		if (tz)		/* time zone */
			count += 6;	/* +03:30 */
		if (digits > 1)	/* fractional seconds precision (including dot) */
			count += digits;
		return count;
	} else if (eclass == EC_TIMESTAMP) {
		count = 10 + 1 + 8;
		if (tz)		/* time zone */
			count += 6;	/* +03:30 */
		if (digits)	/* fractional seconds precision */
			count += digits;
		return count;
	} else if (eclass == EC_BIT) {
		return 5;	/* max(strlen("true"), strlen("false")) */
	} else {
		return 0;
	}
}

static int
export_length(stream *s, int mtype, int eclass, int digits, int scale, int tz, bat bid, ptr p) {
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

	m->rowcnt = val;
	stack_set_number(m, "rowcnt", m->rowcnt);
	if (mnstr_write(s, "&2 ", 3, 1) != 1 ||
	    !mvc_send_lng(s, val) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, m->last_id) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, (lng) query_id) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, starttime > 0 ? GDKusec() - starttime : 0) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, maloptimizer) ||
	    mnstr_write(s, " ", 1, 1) != 1 ||
	    !mvc_send_lng(s, m->Topt) ||
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

static int
mvc_export_head_prot10(backend *b, stream *s, int res_id, int only_header, int compute_lengths) {
	mvc *m = b->mvc;
	size_t i = 0;
	BUN count = 0;
	res_table *t = res_tables_find(m->results, res_id);
	BAT *order = NULL;
	int fres = 0;

	if (!t || !s) {
		return 0;
	}

	/* tuple count */
	if (only_header) {
		if (t->order) {
			order = BBPquickdesc(t->order, FALSE);
			if (!order)
				return -1;

			count = BATcount(order);
		} else
			count = 1;
	}
	m->rowcnt = count;

	// protocol 10 result sets start with "*\n" followed by the binary data:
	// [tableid][queryid][rowcount][colcount][timezone]
	if (!mnstr_writeStr(s, "*\n") || 
		!mnstr_writeInt(s, t->id) || 
		!mnstr_writeLng(s, (lng) t->query_id) ||
		!mnstr_writeLng(s, count) || !mnstr_writeLng(s, (lng) t->nr_cols)) {
		fres = -1;
		goto cleanup;
	}
	// write timezone to the client
	if (!mnstr_writeInt(s, m->timezone)) {
		fres = -1;
		goto cleanup;
	}

	// after that, the data of the individual columns is written
	for (i = 0; i < (size_t) t->nr_cols; i++) {
		res_col *c = t->cols + i;
		BAT *b = BATdescriptor(c->b);
		int mtype;
		int typelen;
		int nil_len = -1;
		int nil_type;
		int retval = -1;
		int convert_to_string = !type_supports_binary_transfer(c->type.type);
		sql_type *type = c->type.type;
		lng print_width = -1;

		if (b == NULL)
			return -1;

		mtype = b->ttype;
		typelen = ATOMsize(mtype);
		nil_type = ATOMstorage(mtype);

		// if the client wants print widths, we compute them for this column
		if (compute_lengths) {
			print_width = get_print_width(mtype, type->eclass, c->type.digits, c->type.scale, type_has_tz(&c->type), b->batCacheid, c->p);
		}

		if (type->eclass == EC_TIMESTAMP || type->eclass == EC_DATE) {
			// timestamps are converted to Unix Timestamps
			mtype = TYPE_lng;
			typelen = sizeof(lng);	
		}

		if (convert_to_string) {
			nil_type = TYPE_str;
		}

		if (ATOMvarsized(mtype) || convert_to_string) {
			// variable length columns have typelen set to -1
			typelen = -1;
			nil_len = (int) strlen(str_nil) + 1;
		} else {
			nil_len = typelen;
		}

		// column data has the following binary format:
		// [tablename]\0[columnname]\0[sqltypename]\0[typelen][digits][scale][nil_length][nil_value][print_width]
		if (!write_str_term(s, c->tn) || !write_str_term(s, c->name) || !write_str_term(s, type->sqlname) ||
				!mnstr_writeInt(s, typelen) || !mnstr_writeInt(s, c->type.digits) || !mnstr_writeInt(s, type->eclass == EC_SEC ? 3 : c->type.scale)) {
			fres = -1;
			BBPunfix(b->batCacheid);
			goto cleanup;
		}

		if ((b->tnil == 0 && b->tnonil == 1) || type->eclass == EC_BLOB) {
			nil_len = 0;
		}

		BBPunfix(b->batCacheid);

		// write NULL values for this column to the stream
		// NULL values are encoded as [size:int][NULL value] ([size] is always [typelen] for fixed size columns)
		if (!mnstr_writeInt(s, nil_len)) {
			fres = -1;
			goto cleanup;
		}
		// transfer the actual NULL value
		if (nil_len > 0) {
			switch(nil_type) {
				case TYPE_str:
					retval = write_str_term(s, str_nil);
					break;
				case TYPE_bit:
				case TYPE_bte:
					retval = mnstr_writeBte(s, bte_nil);
					break;
				case TYPE_sht:
					retval = mnstr_writeSht(s, sht_nil);
					break;
				case TYPE_int:
					retval = mnstr_writeInt(s, int_nil);
					break;
				case TYPE_lng:
					retval = mnstr_writeLng(s, lng_nil);
					break;
				case TYPE_flt:
					retval = mnstr_writeFlt(s, flt_nil);
					break;
				case TYPE_dbl:
					retval = mnstr_writeDbl(s, dbl_nil);
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					retval = mnstr_writeHge(s, hge_nil);
					break;
#endif
				case TYPE_void:
					break;
				default:
					assert(0);
					fres = -1;
					goto cleanup;
			}
		}
		if (!retval) {
			fres = -1;
			goto cleanup;
		}
		// transfer the computed print width
		if (!mnstr_writeLng(s, print_width)) {
			fres = -1;
			goto cleanup;
		}
	}
	if (mnstr_flush(s) < 0) {
		fres = -1;
		goto cleanup;
	}
cleanup:
	return fres;
}

int
mvc_export_head(backend *b, stream *s, int res_id, int only_header, int compute_lengths, lng starttime, lng maloptimizer)
{
	mvc *m = b->mvc;
	int i, res = 0;
	BUN count = 0;
	res_table *t = res_tables_find(m->results, res_id);
	BAT *order = NULL;

	if (!s || !t)
		return 0;


	if (b->client->protocol == PROTOCOL_10) {
		// export head result set 10
		return mvc_export_head_prot10(b, s, res_id, only_header, compute_lengths);
	}

	/* query type: Q_TABLE */
	if (!(mnstr_write(s, "&1 ", 3, 1) == 1))
		return -1;

	/* id */
	if (!mvc_send_int(s, t->id) || mnstr_write(s, " ", 1, 1) != 1)
		return -1;

	/* tuple count */
	if (only_header) {
		if (t->order) {
			order = BBPquickdesc(t->order, FALSE);
			if (!order)
				return -1;

			count = BATcount(order);
		} else
			count = 1;
	}
	m->rowcnt = count;
	stack_set_number(m, "rowcnt", m->rowcnt);
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

	if (mnstr_write(s, " ", 1, 1) != 1 || !mvc_send_lng(s, m->Topt))
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
			int eclass = c->type.type->eclass;

			if (!export_length(s, mtype, eclass, c->type.digits, c->type.scale, type_has_tz(&c->type), c->b, c->p))
				return -1;
			if (i + 1 < t->nr_cols && mnstr_write(s, ",\t", 2, 1) != 1)
				return -1;
		}
		if (mnstr_write(s, " # length\n", 10, 1) != 1)
			return -1;
	}
	if (m->sizeheader) {
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
mvc_export_file(backend *b, stream *s, res_table *t, lng starttime, lng maloptimizer)
{
	mvc *m = b->mvc;
	int res = 0;
	BUN count;
	BAT *order = NULL;

	if (m->scanner.ws == s)
		/* need header */
		mvc_export_head(b, s, t->id, TRUE, TRUE, starttime, maloptimizer);

	if (!t->order) {
		res = mvc_export_row(b, s, t, "", t->tsep, t->rsep, t->ssep, t->ns);
	} else {
		order = BATdescriptor(t->order);
		if (!order)
			return -1;
		count = BATcount(order);

		res = mvc_export_table(b, s, t, order, 0, count, "", t->tsep, t->rsep, t->ssep, t->ns);
		BBPunfix(order->batCacheid);
		m->results = res_tables_remove(m->results, t);
	}
	return res;
}

int
mvc_export_result(backend *b, stream *s, int res_id, lng starttime, lng maloptimizer)
{
	mvc *m = b->mvc;
	int clean = 0, res = 0;
	BUN count;
	res_table *t = res_tables_find(m->results, res_id);
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
	if (t->tsep)
		return mvc_export_file(b, s, t, starttime, maloptimizer);

	if (!json) {
		mvc_export_head(b, s, res_id, TRUE, TRUE, starttime, maloptimizer);
	}

	assert(t->order);

	order = BATdescriptor(t->order);
	if (!order)
		return -1;

	count = m->reply_size;
	if (m->reply_size != -2 && (count <= 0 || count >= BATcount(order))) {
		count = BATcount(order);
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
		m->results = res_tables_remove(m->results, t);

	if (res > 0)
		res = mvc_export_warning(s, "");
	return res;
}


int
mvc_export_chunk(backend *b, stream *s, int res_id, BUN offset, BUN nr)
{
	mvc *m = b->mvc;
	int res = 0;
	res_table *t = res_tables_find(m->results, res_id);
	BAT *order = NULL;
	BUN cnt;

	if (!s || !t)
		return 0;

	order = BATdescriptor(t->order);
	if (!order)
		return -1;
	cnt = nr;
	if (cnt == 0)
		cnt = BATcount(order);
	if (offset >= BATcount(order))
		cnt = 0;
	if (offset + cnt > BATcount(order))
		cnt = BATcount(order) - offset;

	if (b->client->protocol != PROTOCOL_10) {
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
	}

	res = mvc_export_table(b, s, t, order, offset, cnt, "[ ", ",\t", "\t]\n", "\"", "NULL");
	BBPunfix(order->batCacheid);	
	return res;
}


int
mvc_result_table(mvc *m, oid query_id, int nr_cols, int type, BAT *order)
{
	res_table *t = res_table_create(m->session->tr, m->result_id++, query_id, nr_cols, type, m->results, order);
	m->results = t;
	if(t)
		return t->id;
	else
		return -1;
}

int
mvc_result_column(mvc *m, char *tn, char *name, char *typename, int digits, int scale, BAT *b)
{
	/* return 0 on success, non-zero on failure */
	return res_col_create(m->session->tr, m->results, tn, name, typename, digits, scale, TYPE_bat, b) == NULL;
}

int
mvc_result_value(mvc *m, const char *tn, const char *name, const char *typename, int digits, int scale, ptr *p, int mtype)
{
	/* return 0 on success, non-zero on failure */
	return res_col_create(m->session->tr, m->results, tn, name, typename, digits, scale, mtype, p) == NULL;
}
