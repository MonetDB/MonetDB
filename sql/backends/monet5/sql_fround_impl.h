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
#define trunc_wrap		FUN(TYPE, trunc_wrap)

static inline TYPE
dec_round_body_nonil(TYPE v, TYPE r)
{
	assert(!ISNIL(TYPE)(v));

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
round_body_nonil(TYPE v, int r)
{
	TYPE res = NIL(TYPE);

	assert(!ISNIL(TYPE)(v));

	if (r < 0) {
		int d = -r;
		TYPE rnd = (TYPE) (scales[d] >> 1);

		res = (TYPE) (floor(((v + rnd) / ((TYPE) (scales[d])))) * scales[d]);
	} else if (r > 0) {
		int d = r;

		res = (TYPE) (floor(v * (TYPE) scales[d] + .5) / scales[d]);
	} else {
		res = (TYPE) round(v);
	}
	return res;
}

static inline TYPE
round_body(TYPE v, int r)
{
	/* shortcut nil */
	if (ISNIL(TYPE)(v)) {
		return NIL(TYPE);
	} else {
		return round_body_nonil(v, r);
	}
}

str
round_wrap(TYPE *res, const TYPE *v, const bte *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = round_body(*v, *r);
	return MAL_SUCCEED;
}

str
bat_round_wrap(bat *_res, const bat *_v, const bte *r)
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
			dst[i] = round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (ISNIL(TYPE)(src[i])) {
				nonil = FALSE;
				dst[i] = NIL(TYPE);
			} else {
				dst[i] = round_body_nonil(src[i], *r);
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
trunc_wrap(TYPE *res, const TYPE *v, const int *r)
{
	/* shortcut nil */
	if (ISNIL(TYPE)(*v)) {
		*res = NIL(TYPE);
	} else if (*r < 0) {
		int d = -*r;
		*res = (TYPE) (trunc((*v) / ((TYPE) scales[d])) * scales[d]);
	} else if (*r > 0) {
		int d = *r;
		*res = (TYPE) (trunc(*v * (TYPE) scales[d]) / ((TYPE) scales[d]));
	} else {
		*res = (TYPE) trunc(*v);
	}
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
#undef trunc_wrap
