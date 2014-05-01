/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

static inline TYPE
FUN(TYPE, dec_round_body_nonil)(TYPE v, TYPE r)
{
	assert(v != NIL(TYPE));

	return v / r;
}

static inline TYPE
FUN(TYPE, dec_round_body)(TYPE v, TYPE r)
{
	/* shortcut nil */
	if (v == NIL(TYPE)) {
		return NIL(TYPE);
	} else {
		return FUN(TYPE, dec_round_body_nonil)(v, r);
	}
}

str
FUN(TYPE, dec_round_wrap)(TYPE *res, TYPE *v, TYPE *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = FUN(TYPE, dec_round_body)(*v, *r);
	return MAL_SUCCEED;
}

str
FUN(TYPE, bat_dec_round_wrap)(bat *_res, bat *_v, TYPE *r)
{
	BAT *res, *v;
	TYPE *src, *dst;
	BUN i, cnt;
	bit nonil;		/* TRUE: we know there are no NIL (NULL) values */
	bit nil;		/* TRUE: we know there is at least one NIL (NULL) value */

	/* basic sanity checks */
	assert(_res && _v && r);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (!BAThdense(v)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dense head");
	}
	if (v->ttype != TPE(TYPE)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a " STRING(TYPE) " tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TPE(TYPE), cnt);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (TYPE *) Tloc(v, BUNfirst(v));
	dst = (TYPE *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = FUN(TYPE, dec_round_body_nonil)(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == NIL(TYPE)) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = NIL(TYPE);
			} else {
				dst[i] = FUN(TYPE, dec_round_body_nonil)(src[i], *r);
			}
		}
	}

	/* set result BAT properties */
	BATsetcount(res, cnt);
	/* result head is aligned with agument head */
	ALIGNsetH(res, v);
	/* hard to predict correct tail properties in general */
	res->T->nonil = nonil;
	res->T->nil = nil;
	res->tdense = FALSE;
	res->tsorted = v->tsorted;
	BATkey(BATmirror(res), FALSE);

	/* release argument BAT descriptors */
	BBPreleaseref(v->batCacheid);

	/* keep result */
	BBPkeepref(*_res = res->batCacheid);

	return MAL_SUCCEED;
}

static inline TYPE
FUN(TYPE, round_body_nonil)(TYPE v, bte r)
{
	TYPE res = NIL(TYPE);

	assert(v != NIL(TYPE));

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
FUN(TYPE, round_body)(TYPE v, bte r)
{
	/* shortcut nil */
	if (v == NIL(TYPE)) {
		return NIL(TYPE);
	} else {
		return FUN(TYPE, round_body_nonil)(v, r);
	}
}

str
FUN(TYPE, round_wrap)(TYPE *res, TYPE *v, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = FUN(TYPE, round_body)(*v, *r);
	return MAL_SUCCEED;
}

str
FUN(TYPE, bat_round_wrap)(bat *_res, bat *_v, bte *r)
{
	BAT *res, *v;
	TYPE *src, *dst;
	BUN i, cnt;
	bit nonil;		/* TRUE: we know there are no NIL (NULL) values */
	bit nil;		/* TRUE: we know there is at least one NIL (NULL) value */

	/* basic sanity checks */
	assert(_res && _v && r);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (!BAThdense(v)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dense head");
	}
	if (v->ttype != TPE(TYPE)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a " STRING(TYPE) " tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TPE(TYPE), cnt);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (TYPE *) Tloc(v, BUNfirst(v));
	dst = (TYPE *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = FUN(TYPE, round_body_nonil)(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == NIL(TYPE)) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = NIL(TYPE);
			} else {
				dst[i] = FUN(TYPE, round_body_nonil)(src[i], *r);
			}
		}
	}

	/* set result BAT properties */
	BATsetcount(res, cnt);
	/* result head is aligned with agument head */
	ALIGNsetH(res, v);
	/* hard to predict correct tail properties in general */
	res->T->nonil = nonil;
	res->T->nil = nil;
	res->tdense = FALSE;
	res->tsorted = v->tsorted;
	BATkey(BATmirror(res), FALSE);

	/* release argument BAT descriptors */
	BBPreleaseref(v->batCacheid);

	/* keep result */
	BBPkeepref(*_res = res->batCacheid);

	return MAL_SUCCEED;
}

str
FUN(TYPE, trunc_wrap)(TYPE *res, TYPE *v, int *r)
{
	/* shortcut nil */
	if (*v == NIL(TYPE)) {
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
