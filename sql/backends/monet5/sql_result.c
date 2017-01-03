/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * author N.J. Nes
 */

#include "monetdb_config.h"
#include "sql_result.h"
#include <str.h>
#include <tablet.h>
#include <mtime.h>
#include <bat/res_table.h>
#include <bat/bat_storage.h>
#include <rel_exp.h>

#ifndef HAVE_LLABS
#define llabs(x)	((x) < 0 ? -(x) : (x))
#endif

#define DEC_TOSTR(TYPE)							\
	do {								\
		char buf[64];						\
		TYPE v = *(const TYPE *) a;				\
		int scale = (int) (ptrdiff_t) extra;			\
		int cur = 63, i, done = 0;				\
		int neg = v < 0;					\
		int l;							\
		if (v == TYPE##_nil) {					\
			if (*len < 5){					\
				if (*Buf)				\
					GDKfree(*Buf);			\
				*len = 5;				\
				*Buf = GDKzalloc(*len);			\
				if (*Buf == NULL) {			\
					GDKerror("Allocation failed\n"); \
					return 0;			\
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
		if (*len < l){						\
			if (*Buf)					\
				GDKfree(*Buf);				\
			*len = l+1;					\
			*Buf = GDKzalloc(*len);				\
			if (*Buf == NULL) {				\
				GDKerror("Allocation failed\n");	\
				return 0;				\
			}						\
		}							\
		strcpy(*Buf, buf+cur+1);				\
		return l-1;						\
	} while (0)

static int
dec_tostr(void *extra, char **Buf, int *len, int type, const void *a)
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
	return 0;
}

struct time_res {
	int fraction;
	int has_tz;
	lng timezone;
};

static int
sql_time_tostr(void *TS_RES, char **buf, int *len, int type, const void *A)
{
	struct time_res *ts_res = TS_RES;
	int i, len1, big = 128;
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
	if (len1 == 3 && strcmp(s1, "nil") == 0) {
		if (*len < 4 || *buf == NULL) {
			if (*buf)
				GDKfree(*buf);
			*buf = (str) GDKzalloc(*len = 4);
			if (*buf == NULL) {
				GDKerror("Allocation failed\n");
				return 0;
			}
		}
		strcpy(*buf, s1);
		return len1;
	}

	/* fixup the fraction, default is 3 */
	len1 += (ts_res->fraction - 3);
	if (ts_res->fraction == 0)
		len1--;

	if (*len < len1 + 8) {
		if (*buf)
			GDKfree(*buf);
		*buf = (str) GDKzalloc(*len = len1 + 8);
		if (*buf == NULL) {
			GDKerror("Allocation failed\n");
			return 0;
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
	return (int) (s - *buf);
}

static int
sql_timestamp_tostr(void *TS_RES, char **buf, int *len, int type, const void *A)
{
	struct time_res *ts_res = TS_RES;
	int i, len1, len2, big = 128;
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

	/* fixup the fraction, default is 3 */
	len2 += (ts_res->fraction - 3);
	if (ts_res->fraction == 0)
		len2--;

	if (*len < len1 + len2 + 8) {
		if (*buf)
			GDKfree(*buf);
		*buf = (str) GDKzalloc(*len = len1 + len2 + 8);
		if (*buf == NULL) {
			GDKerror("Allocation failed\n");
			return 0;
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
	return (int) (s - *buf);
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
				/* the following code points are all East
				 * Asian Fullwidth and East Asian Wide
				 * characters as defined in Unicode 8.0 */
				if ((0x1100 <= c && c <= 0x115F) ||
				    c == 0x2329 ||
				    c == 0x232A ||
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
				    (0x3250 <= c && c <= 0x4DBF) ||
				    (0x4E00 <= c && c <= 0xA48C) ||
				    (0xA490 <= c && c <= 0xA4C6) ||
				    (0xA960 <= c && c <= 0xA97C) ||
				    (0xAC00 <= c && c <= 0xD7A3) ||
				    (0xF900 <= c && c <= 0xFAFF) ||
				    (0xFE10 <= c && c <= 0xFE19) ||
				    (0xFE30 <= c && c <= 0xFE52) ||
				    (0xFE54 <= c && c <= 0xFE66) ||
				    (0xFE68 <= c && c <= 0xFE6B) ||
				    (0xFF01 <= c && c <= 0xFFE6) ||
				    (0x1B000 <= c && c <= 0x1B001) ||
				    (0x1F200 <= c && c <= 0x1F202) ||
				    (0x1F210 <= c && c <= 0x1F23A) ||
				    (0x1F240 <= c && c <= 0x1F248) ||
				    (0x1F250 <= c && c <= 0x1F251) ||
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

		if (l == int_nil)
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

		if (l != bte_nil)
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

		if (l != sht_nil)
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

		if (l != int_nil)
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

		if (l != lng_nil)
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

		if (l != hge_nil)
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
		while(isspace(*s))					\
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
		while(isspace(*s))					\
			s++;						\
		if (*s) {						\
			if (*s != '.')					\
				return NULL;				\
			s++;						\
			for (i = 0; *s && *s >= '0' && *s <= '9' && i < t->scale; i++, s++) {	\
				res *= 10;				\
				res += *s - '0';			\
			}						\
			while(isspace(*s))				\
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
	int len;
	const char *e; 

	if (type == TYPE_str) {
		sql_column *col = (sql_column *) c->extra;
		int len, slen;

		for (e = s; *e; e++)
			;
		len = (int) (e - s + 1);	/* 64bit: should check for overflow */

		/* or shouldn't len rather be ssize_t, here? */

		if (c->len < len) {
			void *p;
			c->len = len;
			if ((p = GDKrealloc(c->data, len)) == NULL) {
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
		} else if ((len = (int) GDKstrFromStr(c->data, (unsigned char *) s, (ssize_t) (e - s))) < 0) {
			/* 64bit: should check for overflow */
			/* or shouldn't len rather be ssize_t, here? */
			return NULL;
		}
		s = c->data;
		STRLength(&slen, (const str *) &s);
		if (col->type.digits > 0 && len > 0 && slen > (int) col->type.digits) {
			len = STRwidth(c->data);
			if (len > (int) col->type.digits)
				return NULL;
		}
		return c->data;
	}
	// All other values are not allowed to the MonetDB nil value
	if( strcmp(s,"nil")== 0)
		return NULL;

	len = (*BATatoms[type].atomFromStr) (s, &c->len, (ptr) &c->data);
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


static int
_ASCIIadt_toStr(void *extra, char **buf, int *len, int type, const void *a)
{
	if (type == TYPE_str) {
		Column *c = extra;
		char *dst;
		const char *src = a;
		int l = escapedStrlen(src, c->sep, c->rsep, c->quote), l2 = 0;

		if (c->quote)
			l = escapedStrlen(src, NULL, NULL, c->quote);
		else
			l = escapedStrlen(src, c->sep, c->rsep, 0);
		if (l + 3 > *len) {
			GDKfree(*buf);
			*len = 2 * l + 3;
			*buf = GDKzalloc(*len);
			if (*buf == NULL) {
				GDKerror("Allocation failed\n");
				return 0;
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
	int i = 0;
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
			fmt[i].len = fmt[i].nillen = ATOMlen(fmt[i].adt, ATOMnilptr(fmt[i].adt));
			fmt[i].data = GDKzalloc(fmt[i].len);
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
				if (b == NULL)
					sql_error(m, 500, "failed to bind to table column");

				HASHdestroy(b);

				fmt[i].c = b;
				cnt = BATcount(b);
				if (sz > 0 && BATcapacity(b) < (BUN) sz) {
					if (BATextend(fmt[i].c, (BUN) sz) != GDK_SUCCEED) {
						for (i--; i >= 0; i--)
							BBPunfix(fmt[i].c->batCacheid);
						sql_error(m, 500, "failed to allocate space for column");
						return NULL;
					}
				}
				fmt[i].ci = bat_iterator(fmt[i].c);
				fmt[i].c->batDirty = TRUE;
			}
		}
		if ( (locked || (msg = TABLETcreate_bats(&as, (BUN) (sz < 0 ? 1000 : sz))) == MAL_SUCCEED)  ){
			if (!sz || (SQLload_file(cntxt, &as, bs, out, sep, rsep, ssep ? ssep[0] : 0, offset, sz, best) != BUN_NONE && 
				(best || !as.error))) {
				*bats = (BAT**) GDKzalloc(sizeof(BAT *) * as.nr_attrs);
				if ( *bats == NULL){
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
			 "%% .prepare,\t.prepare,\t.prepare,\t.prepare,\t.prepare,\t.prepare # table_name\n" "%% type,\tdigits,\tscale,\tschema,\ttable,\tcolumn # name\n" "%% varchar,\tint,\tint,\tstr,\tstr,\tstr # type\n" "%% " SZFMT ",\t%d,\t%d,\t"
			 SZFMT ",\t" SZFMT ",\t" SZFMT " # length\n", q->id, nrows, nrows, len1, len2, len3, len4, len5, len6) < 0) {
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

			if (mnstr_printf(out, "[ \"%s\",\t%d,\t%d,\t\"%s\",\t\"%s\",\t\"%s\"\t]\n", t->type->sqlname, t->digits, t->scale, schema ? schema : "", rname ? rname : "", name ? name : "") < 0) {
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
				if (mnstr_printf(out, "[ \"%s\",\t%d,\t%d,\tNULL,\tNULL,\tNULL\t]\n", t->type->sqlname, t->digits, t->scale) < 0) {
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
	int l = 0;

	if (!p || ATOMcmp(mtype, ATOMnilptr(mtype), p) == 0) {
		(*buf)[0] = '\200';
		(*buf)[1] = 0;
	} else if (eclass == EC_DEC) {
		l = dec_tostr((void *) (ptrdiff_t) sc, buf, &len, mtype, p);
	} else if (eclass == EC_TIME) {
		struct time_res ts_res;
		ts_res.has_tz = has_tz;
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_time_tostr((void *) &ts_res, buf, &len, mtype, p);

	} else if (eclass == EC_TIMESTAMP) {
		struct time_res ts_res;
		ts_res.has_tz = has_tz;
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_timestamp_tostr((void *) &ts_res, buf, &len, mtype, p);
	} else if (eclass == EC_BIT) {
		bit b = *(bit *) p;
		if (b == bit_nil) {
			(*buf)[0] = 'N';
			(*buf)[1] = 'U';
			(*buf)[2] = 'L';
			(*buf)[3] = 'L';
			(*buf)[4] = 0;
		} else if (b) {
			(*buf)[0] = '1';
			(*buf)[1] = 0;
		} else {
			(*buf)[0] = '0';
			(*buf)[1] = 0;
		}
	} else {
		l = (*BATatoms[mtype].atomToStr) (buf, &len, p);
	}
	return l;
}

static int
export_value(mvc *m, stream *s, int eclass, char *sqlname, int d, int sc, ptr p, int mtype, char **buf, int *len, str ns)
{
	int ok = 0;
	int l = 0;

	if (!p || ATOMcmp(mtype, ATOMnilptr(mtype), p) == 0) {
		size_t ll = strlen(ns);
		ok = (mnstr_write(s, ns, ll, 1) == 1);
	} else if (eclass == EC_DEC) {
		l = dec_tostr((void *) (ptrdiff_t) sc, buf, len, mtype, p);
		ok = (mnstr_write(s, *buf, l, 1) == 1);
	} else if (eclass == EC_TIME) {
		struct time_res ts_res;
		ts_res.has_tz = (strcmp(sqlname, "timetz") == 0);
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_time_tostr((void *) &ts_res, buf, len, mtype, p);

		ok = (mnstr_write(s, *buf, l, 1) == 1);
	} else if (eclass == EC_TIMESTAMP) {
		struct time_res ts_res;
		ts_res.has_tz = (strcmp(sqlname, "timestamptz") == 0);
		ts_res.fraction = d ? d - 1 : 0;
		ts_res.timezone = m->timezone;
		l = sql_timestamp_tostr((void *) &ts_res, buf, len, mtype, p);

		ok = (mnstr_write(s, *buf, l, 1) == 1);
	} else if (eclass == EC_SEC) {
		l = dec_tostr((void *) (ptrdiff_t) 3, buf, len, mtype, p);
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
		default:{
			l = (*BATatoms[mtype].atomToStr) (buf, len, p);
			ok = (mnstr_write(s, *buf, l, 1) == 1);
		}
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
	int len = 0;
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
	if (len)
		_DELETE(buf);
	if (ok)
		ok = (mnstr_write(s, rsep, rseplen, 1) == 1);
	m->results = res_tables_remove(m->results, t);
	return (ok) ? 0 : -1;
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

	as.nr_attrs = t->nr_cols + 1;	/* for the leader */
	as.nr = nr;
	as.offset = offset;
	fmt = as.format = (Column *) GDKzalloc(sizeof(Column) * (as.nr_attrs + 1));
	tres = GDKzalloc(sizeof(struct time_res) * (as.nr_attrs));

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
		fmt[i].nillen = 0;
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

static int
export_length(stream *s, int mtype, int eclass, int digits, int scale, int tz, bat bid, ptr p)
{
	int ok = 1;
	size_t count = 0, incr = 0;;

	if (eclass == EC_SEC)
		incr = 1;
	else if (mtype == TYPE_oid)
		incr = 2;
	mtype = ATOMbasetype(mtype);
	if (mtype == TYPE_str) {
		if (eclass == EC_CHAR) {
			ok = mvc_send_int(s, digits);
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
				if (l == int_nil)
					l = 0;
			}
			ok = mvc_send_int(s, l);
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
		ok = mvc_send_lng(s, (lng) count);
		/* the following two could be done once by taking the
		   max value and calculating the number of digits from that
		   value, instead of the maximum values taken now, which
		   include the optional sign */
	} else if (eclass == EC_FLT) {
		/* floats are printed using "%.9g":
		 * [sign]+digit+period+[max 8 digits]+E+[sign]+[max 2 digits] */
		if (mtype == TYPE_flt) {
			ok = mvc_send_int(s, 15);
			/* doubles are printed using "%.17g":
			 * [sign]+digit+period+[max 16 digits]+E+[sign]+[max 3 digits] */
		} else {	/* TYPE_dbl */
			ok = mvc_send_int(s, 24);
		}
	} else if (eclass == EC_DEC) {
		count = 1 + digits;
		if (scale > 0)
			count += 1;
		ok = mvc_send_lng(s, (lng) count);
	} else if (eclass == EC_DATE) {
		ok = mvc_send_int(s, 10);
	} else if (eclass == EC_TIME) {
		count = 8;
		if (tz)		/* time zone */
			count += 6;	/* +03:30 */
		if (digits > 1)	/* fractional seconds precision (including dot) */
			count += digits;
		ok = mvc_send_lng(s, (lng) count);
	} else if (eclass == EC_TIMESTAMP) {
		count = 10 + 1 + 8;
		if (tz)		/* time zone */
			count += 6;	/* +03:30 */
		if (digits)	/* fractional seconds precision */
			count += digits;
		ok = mvc_send_lng(s, (lng) count);
	} else if (eclass == EC_BIT) {
		ok = mvc_send_int(s, 5);	/* max(strlen("true"), strlen("false")) */
	} else {
		ok = mvc_send_int(s, 0);
	}
	return ok;
}

int
mvc_export_value(backend *b, stream *s, int qtype, str tn, str cn, str type, int d, int sc, int eclass, ptr p, int mtype, str w, str ns)
{
	mvc *m = b->mvc;
	char *buf = NULL;
	int len = 0;
	int ok = 1;
	char *rsep = "\t]\n";
	int csv = (b->output_format == OFMT_CSV);
	int json = (b->output_format == OFMT_JSON);

#ifdef NDEBUG
	(void) qtype;		/* pacify compiler in case asserts are disabled */
#endif
	assert(qtype == Q_TABLE);

	if (csv && 
	   (mnstr_write(s, "&1 0 1 1 1\n", 11, 1) != 1 ||
	   	/* fallback to default tuplecount (1) and id (0) */
	    	/* TODO first header name then values */
	    mnstr_write(s, "% ", 2, 1) != 1 || 
	    mnstr_write(s, tn, strlen(tn), 1) != 1 || 
	    mnstr_write(s, " # table_name\n% ", 16, 1) != 1 || 
	    mnstr_write(s, cn, strlen(cn), 1) != 1 ||
	    mnstr_write(s, " # name\n% ", 10, 1) != 1 ||
	    mnstr_write(s, type, strlen(type), 1) != 1 ||
	    mnstr_write(s, " # type\n% ", 10, 1) != 1 ||
	    !export_length(s, mtype, eclass, d, sc, has_tz(eclass, type), 0, p) ||
	    mnstr_write(s, " # length\n[ ", 12, 1) != 1))
		ok = 0; 
	if (ok) {
		if (json) {
			mnstr_write(s, cn, strlen(cn), 1);
			mnstr_write(s, ": ", 2, 1);
		}
		ok = export_value(m, s, eclass, type, d, sc, p, mtype, &buf, &len, ns);
	}

	if (ok && !json)
		ok = (mnstr_write(s, rsep, strlen(rsep), 1) == 1);

	if (buf)
		_DELETE(buf);

	if (ok)
		ok = mvc_export_warning(s, w);
	return ok;
}

int
mvc_export_operation(backend *b, stream *s, str w)
{
	mvc *m = b->mvc;

	assert(m->type == Q_SCHEMA || m->type == Q_TRANS);
	if (m->type == Q_SCHEMA) {
		if (!s || mnstr_write(s, "&3\n", 3, 1) != 1)
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
mvc_export_affrows(backend *b, stream *s, lng val, str w)
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
	if (mnstr_write(s, "&2 ", 3, 1) != 1 || !mvc_send_lng(s, val) || mnstr_write(s, " ", 1, 1) != 1 || !mvc_send_lng(s, m->last_id) || mnstr_write(s, "\n", 1, 1) != 1)
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
mvc_export_head(backend *b, stream *s, int res_id, int only_header)
{
	mvc *m = b->mvc;
	int i, res = 0;
	BUN count = 0;
	res_table *t = res_tables_find(m->results, res_id);
	BAT *order = NULL;

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

		if (strchr(c->name, ',') || strchr(c->name, ' ') || strchr(c->name , '\t') || strchr(c->name, '#')) {
			if (mnstr_write(s, "\"", 1, 1) != 1)
				return -1;
			if (strchr(c->name, '"')) {
				char *p;
				for (p = c->name; *p; p++) {
					if (*p == '"') {
						if (mnstr_write(s, "\\", 1, 1) != 1)
							return -1;
					}
					if (mnstr_write(s, p, 1, 1) != 1)
						return -1;
				}
			} else {
				if (mnstr_write(s, c->name, strlen(c->name), 1) != 1)
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
mvc_export_file(backend *b, stream *s, res_table *t)
{
	mvc *m = b->mvc;
	int res = 0;
	BUN count;
	BAT *order = NULL;

	if (m->scanner.ws == s)
		/* need header */
		mvc_export_head(b, s, t->id, TRUE);

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
mvc_export_result(backend *b, stream *s, int res_id)
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
		return mvc_export_file(b, s, t);

	if (!json) {
		mvc_export_head(b, s, res_id, TRUE);
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


	/* query type: Q_BLOCK */
	if (!(mnstr_write(s, "&6 ", 3, 1) == 1))
		return export_error(order);

	/* result id */
	if (!mvc_send_int(s, res_id) || mnstr_write(s, " ", 1, 1) != 1)
		return export_error(order);

	/* column count */
	if (!mvc_send_int(s, t->nr_cols) || mnstr_write(s, " ", 1, 1) != 1)
		return export_error(order);

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
mvc_result_table(mvc *m, int nr_cols, int type, BAT *order)
{
	res_table *t = res_table_create(m->session->tr, m->result_id++, nr_cols, type, m->results, order);
	m->results = t;
	return t->id;
}

int
mvc_result_column(mvc *m, char *tn, char *name, char *typename, int digits, int scale, BAT *b)
{
	(void) res_col_create(m->session->tr, m->results, tn, name, typename, digits, scale, TYPE_bat, b);
	return 0;
}

int
mvc_result_value(mvc *m, char *tn, char *name, char *typename, int digits, int scale, ptr *p, int mtype)
{
	(void) res_col_create(m->session->tr, m->results, tn, name, typename, digits, scale, mtype, p);
	return 0;
}
