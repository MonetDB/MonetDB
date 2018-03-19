/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#define dec_round_body_nonil	FUN(TYPE, dec_round_body_nonil)
#define dec_round_body		FUN(TYPE, dec_round_body)
#define dec_round_wrap		FUN(TYPE, dec_round_wrap)
#define bat_dec_round_wrap	FUN(TYPE, bat_dec_round_wrap)
#define round_body_nonil	FUN(TYPE, round_body_nonil)
#define round_body		FUN(TYPE, round_body)
#define round_wrap		FUN(TYPE, round_wrap)
#define bat_round_wrap		FUN(TYPE, bat_round_wrap)
#define nil_2dec		FUN(nil_2dec, TYPE)
#define str_2dec		FUN(str_2dec, TYPE)
#define nil_2num		FUN(nil_2num, TYPE)
#define str_2num		FUN(str_2num, TYPE)
#define batnil_2dec		FUN(batnil_2dec, TYPE)
#define batstr_2dec		FUN(batstr_2dec, TYPE)
#define batnil_2num		FUN(batnil_2num, TYPE)
#define batstr_2num		FUN(batstr_2num, TYPE)
#define dec2second_interval	FUN(TYPE, dec2second_interval)

static inline TYPE
dec_round_body_nonil(TYPE v, TYPE r)
{
	TYPE add = r >> 1;

	assert(!ISNIL(TYPE)(v));

	if (v < 0)
		add = -add;
	v += add;
	return v / r;
}

static inline TYPE
dec_round_body(TYPE v, TYPE r)
{
	/* shortcut nil */
	if (ISNIL(TYPE)(v)) {
		return NIL(TYPE);
	} else {
		return dec_round_body_nonil(v, r);
	}
}

str
dec_round_wrap(TYPE *res, const TYPE *v, const TYPE *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
bat_dec_round_wrap(bat *_res, const bat *_v, const TYPE *r)
{
	BAT *res, *v;
	TYPE *src, *dst;
	BUN i, cnt;
	int nonil;		/* TRUE: we know there are no NIL (NULL) values */

	/* basic sanity checks */
	assert(_res && _v && r);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (v->ttype != TPE(TYPE)) {
		BBPunfix(v->batCacheid);
		throw(MAL, "round", SQLSTATE(42000) "Argument 1 must have a " STRING(TYPE) " tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = COLnew(v->hseqbase, TPE(TYPE), cnt, TRANSIENT);
	if (res == NULL) {
		BBPunfix(v->batCacheid);
		throw(MAL, "round", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (TYPE *) Tloc(v, 0);
	dst = (TYPE *) Tloc(res, 0);

	nonil = TRUE;
	if (v->tnonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (ISNIL(TYPE)(src[i])) {
				nonil = FALSE;
				dst[i] = NIL(TYPE);
			} else {
				dst[i] = dec_round_body_nonil(src[i], *r);
			}
		}
	}

	/* set result BAT properties */
	BATsetcount(res, cnt);
	/* hard to predict correct tail properties in general */
	res->tnonil = nonil;
	res->tnil = !nonil;
	res->tdense = FALSE;
	res->tsorted = v->tsorted;
	res->trevsorted = v->trevsorted;
	BATkey(res, FALSE);

	/* release argument BAT descriptors */
	BBPunfix(v->batCacheid);

	/* keep result */
	BBPkeepref(*_res = res->batCacheid);

	return MAL_SUCCEED;
}

static inline TYPE
round_body_nonil(TYPE v, int d, int s, int r)
{
	TYPE res = NIL(TYPE);

	assert(!ISNIL(TYPE)(v));

	if (-r > d) {
		res = 0;
	} else if (r > 0 && r < s) {
		int dff = s - r;
		BIG rnd = scales[dff] >> 1;
		BIG lres;
		if (v > 0)
			lres = ((v + rnd) / scales[dff]) * scales[dff];
		else
			lres = ((v - rnd) / scales[dff]) * scales[dff];
#if TPE(TYPE) != TYPE_lng && (!defined(HAVE_HGE) || TPE(TYPE) != TYPE_hge)
		assert((lng) GDKmin(TYPE) < lres && lres <= (lng) GDKmax(TYPE));
#endif
		res = (TYPE) lres;
	} else if (r <= 0 && -r + s > 0) {
		int dff = -r + s;
		BIG rnd = scales[dff] >> 1;
		BIG lres;
		if (v > 0)
			lres = ((v + rnd) / scales[dff]) * scales[dff];
		else
			lres = ((v - rnd) / scales[dff]) * scales[dff];
#if TPE(TYPE) != TYPE_lng && (!defined(HAVE_HGE) || TPE(TYPE) != TYPE_hge)
		assert((lng) GDKmin(TYPE) < lres && lres <= (lng) GDKmax(TYPE));
#endif
		res = (TYPE) lres;
	} else {
		res = v;
	}
	return res;
}

static inline TYPE
round_body(TYPE v, int d, int s, int r)
{
	/* shortcut nil */
	if (ISNIL(TYPE)(v)) {
		return NIL(TYPE);
	} else {
		return round_body_nonil(v, d, s, r);
	}
}

str
round_wrap(TYPE *res, const TYPE *v, const int *d, const int *s, const bte *r)
{
	/* basic sanity checks */
	assert(res && v && r && d && s);

	*res = round_body(*v, *d, *s, *r);
	return MAL_SUCCEED;
}

str
bat_round_wrap(bat *_res, const bat *_v, const int *d, const int *s, const bte *r)
{
	BAT *res, *v;
	TYPE *src, *dst;
	BUN i, cnt;
	int nonil;		/* TRUE: we know there are no NIL (NULL) values */

	/* basic sanity checks */
	assert(_res && _v && r && d && s);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (v->ttype != TPE(TYPE)) {
		BBPunfix(v->batCacheid);
		throw(MAL, "round", SQLSTATE(42000) "Argument 1 must have a " STRING(TYPE) " tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = COLnew(v->hseqbase, TPE(TYPE), cnt, TRANSIENT);
	if (res == NULL) {
		BBPunfix(v->batCacheid);
		throw(MAL, "round", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (TYPE *) Tloc(v, 0);
	dst = (TYPE *) Tloc(res, 0);

	nonil = TRUE;
	if (v->tnonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = round_body_nonil(src[i], *d, *s, *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (ISNIL(TYPE)(src[i])) {
				nonil = FALSE;
				dst[i] = NIL(TYPE);
			} else {
				dst[i] = round_body_nonil(src[i], *d, *s, *r);
			}
		}
	}

	/* set result BAT properties */
	BATsetcount(res, cnt);
	/* hard to predict correct tail properties in general */
	res->tnonil = nonil;
	res->tnil = !nonil;
	res->tdense = FALSE;
	res->tsorted = v->tsorted;
	res->trevsorted = v->trevsorted;
	BATkey(res, FALSE);

	/* release argument BAT descriptors */
	BBPunfix(v->batCacheid);

	/* keep result */
	BBPkeepref(*_res = res->batCacheid);

	return MAL_SUCCEED;
}

str
nil_2dec(TYPE *res, const void *val, const int *d, const int *sc)
{
	(void) val;
	(void) d;
	(void) sc;

	*res = NIL(TYPE);
	return MAL_SUCCEED;
}

str
str_2dec(TYPE *res, const str *val, const int *d, const int *sc)
{
	char *s;
	char *dot, *end;
	int digits;
	int scale;
	BIG value;

	s = *val;
	if (GDK_STRNIL(s)) {
		*res = NIL(TYPE);
		return MAL_SUCCEED;
	}
	dot = strchr(s, '.');
	if (dot != NULL) {
		s = strip_extra_zeros(s);
		digits = _strlen(s) - 1;
		scale = _strlen(dot + 1);
	} else {
		digits = _strlen(s);
		scale = 0;
	}
	end = NULL;
	value = 0;

	if (digits < 0)
		throw(SQL, STRING(TYPE), SQLSTATE(42000) "Decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);
	if (*d < 0 || *d >= (int) (sizeof(scales) / sizeof(scales[0])))
		throw(SQL, STRING(TYPE), SQLSTATE(42000) "Decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);

	value = decimal_from_str(s, &end);
	if (*s == '+' || *s == '-')
		digits--;
	if (scale < *sc) {
		/* the current scale is too small, increase it by adding 0's */
		int dff = *sc - scale;	/* CANNOT be 0! */

		value *= scales[dff];
		scale += dff;
		digits += dff;
	} else if (scale > *sc) {
		/* the current scale is too big, decrease it by correctly rounding */
		/* we should round properly, and check for overflow (res >= 10^digits+scale) */
		int dff = scale - *sc;	/* CANNOT be 0 */
		BIG rnd = scales[dff] >> 1;

		if (value > 0)
			value += rnd;
		else
			value -= rnd;
		value /= scales[dff];
		scale -= dff;
		digits -= dff;
		if (value >= scales[*d] || value <= -scales[*d]) {
			throw(SQL, STRING(TYPE), SQLSTATE(42000) "Rounding of decimal (%s) doesn't fit format (%d.%d)", *val, *d, *sc);
		}
	}
	if (value <= -scales[*d] || value >= scales[*d]  || *end) {
		throw(SQL, STRING(TYPE), SQLSTATE(42000) "Decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);
	}
	*res = (TYPE) value;
	return MAL_SUCCEED;
}

str
nil_2num(TYPE *res, const void *v, const int *len)
{
	int zero = 0;
	return nil_2dec(res, v, len, &zero);
}

str
str_2num(TYPE *res, const str *v, const int *len)
{
	int zero = 0;
	return str_2dec(res, v, len, &zero);
}

str
batnil_2dec(bat *res, const bat *bid, const int *d, const int *sc)
{
	BAT *b, *dst;
	BUN p, q;

	(void) d;
	(void) sc;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2dec_" STRING(TYPE), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TPE(TYPE), BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		TYPE r = NIL(TYPE);
		if (BUNappend(dst, &r, FALSE) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2dec(bat *res, const bat *bid, const int *d, const int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2dec_" STRING(TYPE), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TPE(TYPE), BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		TYPE r;
		msg = str_2dec(&r, &v, d, sc);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, FALSE) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2num(bat *res, const bat *bid, const int *len)
{
	int zero = 0;
	return batnil_2dec(res, bid, len, &zero);
}

str
batstr_2num(bat *res, const bat *bid, const int *len)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2num_" STRING(TYPE), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TPE(TYPE), BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.num_" STRING(TYPE), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		TYPE r;
		msg = str_2num(&r, &v, len);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, FALSE) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.num_" STRING(TYPE), SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
dec2second_interval(lng *res, const int *sc, const TYPE *dec, const int *ek, const int *sk)
{
	BIG value = *dec;

	(void) ek;
	(void) sk;
	if (*sc < 3) {
		int d = 3 - *sc;
		value *= scales[d];
	} else if (*sc > 3) {
		int d = *sc - 3;
		lng rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
	}
#if defined(HAVE_HGE) && TPE(TYPE) == TYPE_hge
	assert((hge) GDK_lng_min <= value && value <= (hge) GDK_lng_max);
#endif
	*res = value;
	return MAL_SUCCEED;
}

#undef dec_round_body_nonil
#undef dec_round_body
#undef dec_round_wrap
#undef bat_dec_round_wrap
#undef round_body_nonil
#undef round_body
#undef round_wrap
#undef bat_round_wrap
#undef nil_2dec
#undef str_2dec
#undef nil_2num
#undef str_2num
#undef batnil_2dec
#undef batstr_2dec
#undef batnil_2num
#undef batstr_2num
#undef dec2second_interval
