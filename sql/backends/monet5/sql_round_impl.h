/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
#define str_2dec_body		FUN(str_2dec_body, TYPE)
#define str_2dec		FUN(str_2dec, TYPE)
#define batnil_2dec		FUN(batnil_2dec, TYPE)
#define batstr_2dec		FUN(batstr_2dec, TYPE)
#define dec2second_interval	FUN(TYPE, dec2second_interval)
#define batdec2second_interval	FUN(TYPE, batdec2second_interval)

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

	if (*r <= 0)
		throw(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");
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

	if (*r <= 0)
		throw(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");
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
		throw(MAL, "round", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (TYPE *) Tloc(v, 0);
	dst = (TYPE *) Tloc(res, 0);

	nonil = TRUE;
	if (v->tnonil) {
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
	res->tseqbase = oid_nil;
	res->tsorted = v->tsorted;
	res->trevsorted = v->trevsorted;
	BATkey(res, false);

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
		res = (TYPE) lres;
	} else if (r <= 0 && -r + s > 0) {
		int dff = -r + s;
		BIG rnd = scales[dff] >> 1;
		BIG lres;
		if (v > 0)
			lres = ((v + rnd) / scales[dff]) * scales[dff];
		else
			lres = ((v - rnd) / scales[dff]) * scales[dff];
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
	bool nonil;		/* TRUE: we know there are no NIL (NULL) values */

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
		throw(MAL, "round", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (TYPE *) Tloc(v, 0);
	dst = (TYPE *) Tloc(res, 0);

	nonil = true;
	if (v->tnonil) {
		for (i = 0; i < cnt; i++)
			dst[i] = round_body_nonil(src[i], *d, *s, *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (ISNIL(TYPE)(src[i])) {
				nonil = false;
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
	res->tseqbase = oid_nil;
	res->tsorted = v->tsorted;
	res->trevsorted = v->trevsorted;
	BATkey(res, false);

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

static inline str
str_2dec_body(TYPE *res, const str val, const int d, const int sc)
{
	char *s = val;
	char *dot, *end;
	int digits;
	int scale;
	BIG value;

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
		throw(SQL, STRING(TYPE), SQLSTATE(42000) "Decimal (%s) doesn't have format (%d.%d)", s, d, sc);
	if (d < 0 || d >= (int) (sizeof(scales) / sizeof(scales[0])))
		throw(SQL, STRING(TYPE), SQLSTATE(42000) "Decimal (%s) doesn't have format (%d.%d)", s, d, sc);

	value = decimal_from_str(s, &end);
	if (*s == '+' || *s == '-')
		digits--;
	if (scale < sc) {
		/* the current scale is too small, increase it by adding 0's */
		int dff = sc - scale;	/* CANNOT be 0! */
		if (dff >= MAX_SCALE)
			throw(SQL, STRING(TYPE), SQLSTATE(42000) "Rounding of decimal (%s) doesn't fit format (%d.%d)", s, d, sc);

		value *= scales[dff];
		scale += dff;
		digits += dff;
	} else if (scale > sc) {
		/* the current scale is too big, decrease it by correctly rounding */
		/* we should round properly, and check for overflow (res >= 10^digits+scale) */
		int dff = scale - sc;	/* CANNOT be 0 */

		if (dff >= MAX_SCALE)
			throw(SQL, STRING(TYPE), SQLSTATE(42000) "Rounding of decimal (%s) doesn't fit format (%d.%d)", s, d, sc);

		BIG rnd = scales[dff] >> 1;

		if (value > 0)
			value += rnd;
		else
			value -= rnd;
		value /= scales[dff];
		scale -= dff;
		digits -= dff;
		if (value >= scales[d] || value <= -scales[d])
			throw(SQL, STRING(TYPE), SQLSTATE(42000) "Rounding of decimal (%s) doesn't fit format (%d.%d)", s, d, sc);
	}
	if (value <= -scales[d] || value >= scales[d]  || *end)
		throw(SQL, STRING(TYPE), SQLSTATE(42000) "Decimal (%s) doesn't have format (%d.%d)", s, d, sc);
	*res = (TYPE) value;
	return MAL_SUCCEED;
}

str
str_2dec(TYPE *res, const str *val, const int *d, const int *sc)
{
	str v = *val;

	if (strNil(v)) {
		*res = NIL(TYPE);
		return MAL_SUCCEED;
	} else {
		return str_2dec_body(res, v, *d, *sc);
	}
}

str
batnil_2dec(bat *res, const bat *bid, const int *d, const int *sc)
{
	BAT *b, *dst;
	BUN p, q;

	(void) d;
	(void) sc;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2dec_" STRING(TYPE), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
	}
	dst = COLnew(b->hseqbase, TPE(TYPE), BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	const TYPE r = NIL(TYPE);
	BATloop(b, p, q) {
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2dec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int d = *getArgReference_int(stk, pci, pci->argc == 5 ? 3 : 2), sk = *getArgReference_int(stk, pci, pci->argc == 5 ? 4 : 3);
	BAT *b = NULL, *s = NULL, *res = NULL;
	bat *r = getArgReference_bat(stk, pci, 0), *sid = pci->argc == 5 ? getArgReference_bat(stk, pci, 2) : NULL;
	BUN q = 0;
	BATiter bi;
	oid off;
	struct canditer ci = {0};
	TYPE *restrict ret;
	bool nils = false;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
		msg = createException(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	bi = bat_iterator(b);
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	q = canditer_init(&ci, b, s);
	if (!(res = COLnew(ci.hseq, TPE(TYPE), q, TRANSIENT))) {
		msg = createException(SQL, "sql.dec_" STRING(TYPE), SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	ret = (TYPE*) Tloc(res, 0);

	for (BUN i = 0 ; i < q ; i++) {
		BUN p = (BUN) (canditer_next(&ci) - off);
		const str next = BUNtail(bi, p);

		if (strNil(next)) {
			ret[i] = NIL(TYPE);
			nils = true;
		} else if ((msg = str_2dec_body(&(ret[i]), next, d, sk)))
			goto bailout;
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
dec2second_interval(lng *res, const int *sc, const TYPE *dec, const int *ek, const int *sk)
{
	BIG value = *dec;

	(void) ek;
	(void) sk;
	if (ISNIL(TYPE)(*dec)) {
		value = lng_nil;
	} else if (*sc < 3) {
		int d = 3 - *sc;
		value *= scales[d];
	} else if (*sc > 3) {
		int d = *sc - 3;
		lng rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
	}
	*res = value;
	return MAL_SUCCEED;
}

str
batdec2second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int sc = *getArgReference_int(stk, pci, 1);
	BAT *b = NULL, *s = NULL, *res = NULL;
	bat *r = getArgReference_bat(stk, pci, 0), *sid = pci->argc == 6 ? getArgReference_bat(stk, pci, 3) : NULL;
	BUN q = 0;
	oid off;
	struct canditer ci = {0};
	TYPE *restrict src;
	BIG *restrict ret, multiplier = 1, divider = 1, offset = 0;
	bool nils = false;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
		msg = createException(SQL, "batcalc.batdec2second_interval", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.batdec2second_interval", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	q = canditer_init(&ci, b, s);
	if (!(res = COLnew(ci.hseq, TYPE_lng, q, TRANSIENT))) {
		msg = createException(SQL, "batcalc.batdec2second_interval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	src = Tloc(b, 0);
	ret = Tloc(res, 0);

	if (sc < 3) {
		int d = 3 - sc;
		multiplier = scales[d];
	} else if (sc > 3) {
		int d = sc - 3;
		lng rnd = scales[d] >> 1;

		offset = rnd;
		divider = scales[d];
	}

	if (sc < 3) {
		for (BUN i = 0 ; i < q ; i++) {
			BUN p = (BUN) (canditer_next(&ci) - off);
			if (ISNIL(TYPE)(src[p])) {
				ret[i] = lng_nil;
				nils = true;
			} else {
				BIG next = (BIG) src[p];
				next *= multiplier;
				ret[i] = next;
			}
		}
	} else if (sc > 3) {
		for (BUN i = 0 ; i < q ; i++) {
			BUN p = (BUN) (canditer_next(&ci) - off);
			if (ISNIL(TYPE)(src[p])) {
				ret[i] = lng_nil;
				nils = true;
			} else {
				BIG next = (BIG) src[p];
				next += offset;
				next /= divider;
				ret[i] = next;
			}
		}
	} else {
		for (BUN i = 0 ; i < q ; i++) {
			BUN p = (BUN) (canditer_next(&ci) - off);
			if (ISNIL(TYPE)(src[p])) {
				ret[i] = lng_nil;
				nils = true;
			} else {
				ret[i] = (BIG) src[p];
			}
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
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
#undef str_2dec_body
#undef str_2dec
#undef batnil_2dec
#undef batstr_2dec
#undef dec2second_interval
#undef batdec2second_interval
