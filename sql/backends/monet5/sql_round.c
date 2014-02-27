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

#include "monetdb_config.h"
#include "sql.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include <sql_storage.h>
#include <sql_scenario.h>
#include <store_sequence.h>
#include <sql_datetime.h>
#include <rel_optimizer.h>
#include <rel_distribute.h>
#include <rel_select.h>
#include <rel_exp.h>
#include <rel_dump.h>
#include <rel_bin.h>
#include "clients.h"
#include "mal_instruction.h"

static lng scales[20] = {
	LL_CONSTANT(1),
	LL_CONSTANT(10),
	LL_CONSTANT(100),
	LL_CONSTANT(1000),
	LL_CONSTANT(10000),
	LL_CONSTANT(100000),
	LL_CONSTANT(1000000),
	LL_CONSTANT(10000000),
	LL_CONSTANT(100000000),
	LL_CONSTANT(1000000000),
	LL_CONSTANT(10000000000),
	LL_CONSTANT(100000000000),
	LL_CONSTANT(1000000000000),
	LL_CONSTANT(10000000000000),
	LL_CONSTANT(100000000000000),
	LL_CONSTANT(1000000000000000),
	LL_CONSTANT(10000000000000000),
	LL_CONSTANT(100000000000000000),
	LL_CONSTANT(1000000000000000000)
};

static inline bte
bte_dec_round_body_nonil(bte v, bte r)
{
	bte add = r >> 1;

	assert(v != bte_nil);

	if (v < 0)
		add = -add;
	v += add;
	return v / r;
}

static inline bte
bte_dec_round_body(bte v, bte r)
{
	/* shortcut nil */
	if (v == bte_nil) {
		return bte_nil;
	} else {
		return bte_dec_round_body_nonil(v, r);
	}
}

str
bte_dec_round_wrap(bte *res, bte *v, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r);
	*res = bte_dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
bte_bat_dec_round_wrap(bat *_res, bat *_v, bte *r)
{
	BAT *res, *v;
	bte *src, *dst;
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
	if (v->ttype != TYPE_bte) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a bte tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_bte, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (bte *) Tloc(v, BUNfirst(v));
	dst = (bte *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = bte_dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == bte_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = bte_nil;
			} else {
				dst[i] = bte_dec_round_body_nonil(src[i], *r);
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

static inline bte
bte_round_body_nonil(bte v, int d, int s, bte r)
{
	bte res = bte_nil;

	assert(v != bte_nil);

	if (-r > d) {
		res = 0;
	} else if (r > 0 && r < s) {
		int dff = s - r;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_bte_min < lres && lres <= (lng) GDK_bte_max);
		res = (bte) lres;
	} else if (r <= 0 && -r + s > 0) {
		int dff = -r + s;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_bte_min < lres && lres <= (lng) GDK_bte_max);
		res = (bte) lres;
	} else {
		res = v;
	}
	return res;
}

static inline bte
bte_round_body(bte v, int d, int s, bte r)
{
	/* shortcut nil */
	if (v == bte_nil) {
		return bte_nil;
	} else {
		return bte_round_body_nonil(v, d, s, r);
	}
}

str
bte_round_wrap(bte *res, bte *v, int *d, int *s, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r && d && s);

	*res = bte_round_body(*v, *d, *s, *r);
	return MAL_SUCCEED;
}

str
bte_bat_round_wrap(bat *_res, bat *_v, int *d, int *s, bte *r)
{
	BAT *res, *v;
	bte *src, *dst;
	BUN i, cnt;
	bit nonil;		/* TRUE: we know there are no NIL (NULL) values */
	bit nil;		/* TRUE: we know there is at least one NIL (NULL) value */

	/* basic sanity checks */
	assert(_res && _v && r && d && s);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (!BAThdense(v)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dense head");
	}
	if (v->ttype != TYPE_bte) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a bte tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_bte, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (bte *) Tloc(v, BUNfirst(v));
	dst = (bte *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = bte_round_body_nonil(src[i], *d, *s, *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == bte_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = bte_nil;
			} else {
				dst[i] = bte_round_body_nonil(src[i], *d, *s, *r);
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
nil_2dec_bte(bte *res, void *val, int *d, int *sc)
{
	(void) val;
	(void) d;
	(void) sc;

	*res = bte_nil;
	return MAL_SUCCEED;
}

str
str_2dec_bte(bte *res, str *val, int *d, int *sc)
{
	char *s = strip_extra_zeros(*val);
	char *dot = strchr(s, '.');
	int digits = _strlen(s) - 1;
	int scale = digits - (int) (dot - s);
	lng value = 0;

	if (!dot) {
		if (GDK_STRNIL(*val)) {
			*res = bte_nil;
			return MAL_SUCCEED;
		} else {
			throw(SQL, "bte", "\"%s\" is no decimal value (doesn't contain a '.')", *val);
		}
	}

	value = decimal_from_str(s);
	if (*s == '+' || *s == '-')
		digits--;
	if (scale < *sc) {
		/* the current scale is too small, increase it by adding 0's */
		int d = *sc - scale;	/* CANNOT be 0! */

		value *= scales[d];
		scale += d;
		digits += d;
	} else if (scale > *sc) {
		/* the current scale is too big, decrease it by correctly rounding */
		int d = scale - *sc;	/* CANNOT be 0 */
		lng rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
		scale -= d;
		digits -= d;
	}
	if (digits > *d) {
		throw(SQL, "bte", "decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);
	}
	*res = (bte) value;
	return MAL_SUCCEED;
}

str
nil_2num_bte(bte *res, void *v, int *len)
{
	int zero = 0;
	return nil_2dec_bte(res, v, len, &zero);
}

str
str_2num_bte(bte *res, str *v, int *len)
{
	int zero = 0;
	return str_2dec_bte(res, v, len, &zero);
}

str
batnil_2dec_bte(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	(void) d;
	(void) sc;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte r = bte_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2dec_bte(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		bte r;
		msg = str_2dec_bte(&r, &v, d, sc);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2num_bte(int *res, int *bid, int *len)
{
	int zero = 0;
	return batnil_2dec_bte(res, bid, len, &zero);
}

str
batstr_2num_bte(int *res, int *bid, int *len)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2num_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		bte r;
		msg = str_2num_bte(&r, &v, len);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
bte_dec2second_interval(lng *res, int *sc, bte *dec, int *ek, int *sk)
{
	lng value = *dec;

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
	*res = value;
	return MAL_SUCCEED;
}

static inline sht
sht_dec_round_body_nonil(sht v, sht r)
{
	sht add = r >> 1;

	assert(v != sht_nil);

	if (v < 0)
		add = -add;
	v += add;
	return v / r;
}

static inline sht
sht_dec_round_body(sht v, sht r)
{
	/* shortcut nil */
	if (v == sht_nil) {
		return sht_nil;
	} else {
		return sht_dec_round_body_nonil(v, r);
	}
}

str
sht_dec_round_wrap(sht *res, sht *v, sht *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = sht_dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
sht_bat_dec_round_wrap(bat *_res, bat *_v, sht *r)
{
	BAT *res, *v;
	sht *src, *dst;
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
	if (v->ttype != TYPE_sht) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a sht tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_sht, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (sht *) Tloc(v, BUNfirst(v));
	dst = (sht *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = sht_dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == sht_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = sht_nil;
			} else {
				dst[i] = sht_dec_round_body_nonil(src[i], *r);
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

static inline sht
sht_round_body_nonil(sht v, int d, int s, bte r)
{
	sht res = sht_nil;

	assert(v != sht_nil);

	if (-r > d) {
		res = 0;
	} else if (r > 0 && r < s) {
		int dff = s - r;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_sht_min < lres && lres <= (lng) GDK_sht_max);
		res = (sht) lres;
	} else if (r <= 0 && -r + s > 0) {
		int dff = -r + s;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_sht_min < lres && lres <= (lng) GDK_sht_max);
		res = (sht) lres;
	} else {
		res = v;
	}
	return res;
}

static inline sht
sht_round_body(sht v, int d, int s, bte r)
{
	/* shortcut nil */
	if (v == sht_nil) {
		return sht_nil;
	} else {
		return sht_round_body_nonil(v, d, s, r);
	}
}

str
sht_round_wrap(sht *res, sht *v, int *d, int *s, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r && d && s);

	*res = sht_round_body(*v, *d, *s, *r);
	return MAL_SUCCEED;
}

str
sht_bat_round_wrap(bat *_res, bat *_v, int *d, int *s, bte *r)
{
	BAT *res, *v;
	sht *src, *dst;
	BUN i, cnt;
	bit nonil;		/* TRUE: we know there are no NIL (NULL) values */
	bit nil;		/* TRUE: we know there is at least one NIL (NULL) value */

	/* basic sanity checks */
	assert(_res && _v && r && d && s);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (!BAThdense(v)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dense head");
	}
	if (v->ttype != TYPE_sht) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a sht tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_sht, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (sht *) Tloc(v, BUNfirst(v));
	dst = (sht *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = sht_round_body_nonil(src[i], *d, *s, *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == sht_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = sht_nil;
			} else {
				dst[i] = sht_round_body_nonil(src[i], *d, *s, *r);
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
nil_2dec_sht(sht *res, void *val, int *d, int *sc)
{
	(void) val;
	(void) d;
	(void) sc;

	*res = sht_nil;
	return MAL_SUCCEED;
}

str
str_2dec_sht(sht *res, str *val, int *d, int *sc)
{
	char *s = strip_extra_zeros(*val);
	char *dot = strchr(s, '.');
	int digits = _strlen(s) - 1;
	int scale = digits - (int) (dot - s);
	lng value = 0;

	if (!dot) {
		if (GDK_STRNIL(*val)) {
			*res = sht_nil;
			return MAL_SUCCEED;
		} else {
			throw(SQL, "sht", "\"%s\" is no decimal value (doesn't contain a '.')", *val);
		}
	}

	value = decimal_from_str(s);
	if (*s == '+' || *s == '-')
		digits--;
	if (scale < *sc) {
		/* the current scale is too small, increase it by adding 0's */
		int d = *sc - scale;	/* CANNOT be 0! */

		value *= scales[d];
		scale += d;
		digits += d;
	} else if (scale > *sc) {
		/* the current scale is too big, decrease it by correctly rounding */
		int d = scale - *sc;	/* CANNOT be 0 */
		lng rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
		scale -= d;
		digits -= d;
	}
	if (digits > *d) {
		throw(SQL, "sht", "decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);
	}
	*res = (sht) value;
	return MAL_SUCCEED;
}

str
nil_2num_sht(sht *res, void *v, int *len)
{
	int zero = 0;
	return nil_2dec_sht(res, v, len, &zero);
}

str
str_2num_sht(sht *res, str *v, int *len)
{
	int zero = 0;
	return str_2dec_sht(res, v, len, &zero);
}

str
batnil_2dec_sht(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	(void) d;
	(void) sc;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht r = sht_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2dec_sht(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		sht r;
		msg = str_2dec_sht(&r, &v, d, sc);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2num_sht(int *res, int *bid, int *len)
{
	int zero = 0;
	return batnil_2dec_sht(res, bid, len, &zero);
}

str
batstr_2num_sht(int *res, int *bid, int *len)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2num_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		sht r;
		msg = str_2num_sht(&r, &v, len);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sht_dec2second_interval(lng *res, int *sc, sht *dec, int *ek, int *sk)
{
	lng value = *dec;

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
	*res = value;
	return MAL_SUCCEED;
}

static inline int
int_dec_round_body_nonil(int v, int r)
{
	int add = r >> 1;

	assert(v != int_nil);

	if (v < 0)
		add = -add;
	v += add;
	return v / r;
}

static inline int
int_dec_round_body(int v, int r)
{
	/* shortcut nil */
	if (v == int_nil) {
		return int_nil;
	} else {
		return int_dec_round_body_nonil(v, r);
	}
}

str
int_dec_round_wrap(int *res, int *v, int *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = int_dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
int_bat_dec_round_wrap(bat *_res, bat *_v, int *r)
{
	BAT *res, *v;
	int *src, *dst;
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
	if (v->ttype != TYPE_int) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a int tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_int, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (int *) Tloc(v, BUNfirst(v));
	dst = (int *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = int_dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == int_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = int_nil;
			} else {
				dst[i] = int_dec_round_body_nonil(src[i], *r);
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

static inline int
int_round_body_nonil(int v, int d, int s, bte r)
{
	int res = int_nil;

	assert(v != int_nil);

	if (-r > d) {
		res = 0;
	} else if (r > 0 && r < s) {
		int dff = s - r;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_int_min < lres && lres <= (lng) GDK_int_max);
		res = (int) lres;
	} else if (r <= 0 && -r + s > 0) {
		int dff = -r + s;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_int_min < lres && lres <= (lng) GDK_int_max);
		res = (int) lres;
	} else {
		res = v;
	}
	return res;
}

static inline int
int_round_body(int v, int d, int s, bte r)
{
	/* shortcut nil */
	if (v == int_nil) {
		return int_nil;
	} else {
		return int_round_body_nonil(v, d, s, r);
	}
}

str
int_round_wrap(int *res, int *v, int *d, int *s, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r && d && s);

	*res = int_round_body(*v, *d, *s, *r);
	return MAL_SUCCEED;
}

str
int_bat_round_wrap(bat *_res, bat *_v, int *d, int *s, bte *r)
{
	BAT *res, *v;
	int *src, *dst;
	BUN i, cnt;
	bit nonil;		/* TRUE: we know there are no NIL (NULL) values */
	bit nil;		/* TRUE: we know there is at least one NIL (NULL) value */

	/* basic sanity checks */
	assert(_res && _v && r && d && s);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (!BAThdense(v)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dense head");
	}
	if (v->ttype != TYPE_int) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a int tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_int, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (int *) Tloc(v, BUNfirst(v));
	dst = (int *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = int_round_body_nonil(src[i], *d, *s, *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == int_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = int_nil;
			} else {
				dst[i] = int_round_body_nonil(src[i], *d, *s, *r);
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
nil_2dec_int(int *res, void *val, int *d, int *sc)
{
	(void) val;
	(void) d;
	(void) sc;

	*res = int_nil;
	return MAL_SUCCEED;
}

str
str_2dec_int(int *res, str *val, int *d, int *sc)
{
	char *s = strip_extra_zeros(*val);
	char *dot = strchr(s, '.');
	int digits = _strlen(s) - 1;
	int scale = digits - (int) (dot - s);
	lng value = 0;

	if (!dot) {
		if (GDK_STRNIL(*val)) {
			*res = int_nil;
			return MAL_SUCCEED;
		} else {
			throw(SQL, "int", "\"%s\" is no decimal value (doesn't contain a '.')", *val);
		}
	}

	value = decimal_from_str(s);
	if (*s == '+' || *s == '-')
		digits--;
	if (scale < *sc) {
		/* the current scale is too small, increase it by adding 0's */
		int d = *sc - scale;	/* CANNOT be 0! */

		value *= scales[d];
		scale += d;
		digits += d;
	} else if (scale > *sc) {
		/* the current scale is too big, decrease it by correctly rounding */
		int d = scale - *sc;	/* CANNOT be 0 */
		lng rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
		scale -= d;
		digits -= d;
	}
	if (digits > *d) {
		throw(SQL, "int", "decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);
	}
	*res = (int) value;
	return MAL_SUCCEED;
}

str
nil_2num_int(int *res, void *v, int *len)
{
	int zero = 0;
	return nil_2dec_int(res, v, len, &zero);
}

str
str_2num_int(int *res, str *v, int *len)
{
	int zero = 0;
	return str_2dec_int(res, v, len, &zero);
}

str
batnil_2dec_int(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	(void) d;
	(void) sc;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int r = int_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2dec_int(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		int r;
		msg = str_2dec_int(&r, &v, d, sc);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2num_int(int *res, int *bid, int *len)
{
	int zero = 0;
	return batnil_2dec_int(res, bid, len, &zero);
}

str
batstr_2num_int(int *res, int *bid, int *len)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2num_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		int r;
		msg = str_2num_int(&r, &v, len);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
int_dec2second_interval(lng *res, int *sc, int *dec, int *ek, int *sk)
{
	lng value = *dec;

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
	*res = value;
	return MAL_SUCCEED;
}

static inline wrd
wrd_dec_round_body_nonil(wrd v, wrd r)
{
	wrd add = r >> 1;

	assert(v != wrd_nil);

	if (v < 0)
		add = -add;
	v += add;
	return v / r;
}

static inline wrd
wrd_dec_round_body(wrd v, wrd r)
{
	/* shortcut nil */
	if (v == wrd_nil) {
		return wrd_nil;
	} else {
		return wrd_dec_round_body_nonil(v, r);
	}
}

str
wrd_dec_round_wrap(wrd *res, wrd *v, wrd *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = wrd_dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
wrd_bat_dec_round_wrap(bat *_res, bat *_v, wrd *r)
{
	BAT *res, *v;
	wrd *src, *dst;
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
	if (v->ttype != TYPE_wrd) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a wrd tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_wrd, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (wrd *) Tloc(v, BUNfirst(v));
	dst = (wrd *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = wrd_dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == wrd_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = wrd_nil;
			} else {
				dst[i] = wrd_dec_round_body_nonil(src[i], *r);
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

static inline wrd
wrd_round_body_nonil(wrd v, int d, int s, bte r)
{
	wrd res = wrd_nil;

	assert(v != wrd_nil);

	if (-r > d) {
		res = 0;
	} else if (r > 0 && r < s) {
		int dff = s - r;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_wrd_min < lres && lres <= (lng) GDK_wrd_max);
		res = (wrd) lres;
	} else if (r <= 0 && -r + s > 0) {
		int dff = -r + s;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_wrd_min < lres && lres <= (lng) GDK_wrd_max);
		res = (wrd) lres;
	} else {
		res = v;
	}
	return res;
}

static inline wrd
wrd_round_body(wrd v, int d, int s, bte r)
{
	/* shortcut nil */
	if (v == wrd_nil) {
		return wrd_nil;
	} else {
		return wrd_round_body_nonil(v, d, s, r);
	}
}

str
wrd_round_wrap(wrd *res, wrd *v, int *d, int *s, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r && d && s);

	*res = wrd_round_body(*v, *d, *s, *r);
	return MAL_SUCCEED;
}

str
wrd_bat_round_wrap(bat *_res, bat *_v, int *d, int *s, bte *r)
{
	BAT *res, *v;
	wrd *src, *dst;
	BUN i, cnt;
	bit nonil;		/* TRUE: we know there are no NIL (NULL) values */
	bit nil;		/* TRUE: we know there is at least one NIL (NULL) value */

	/* basic sanity checks */
	assert(_res && _v && r && d && s);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (!BAThdense(v)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dense head");
	}
	if (v->ttype != TYPE_wrd) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a wrd tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_wrd, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (wrd *) Tloc(v, BUNfirst(v));
	dst = (wrd *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = wrd_round_body_nonil(src[i], *d, *s, *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == wrd_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = wrd_nil;
			} else {
				dst[i] = wrd_round_body_nonil(src[i], *d, *s, *r);
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
nil_2dec_wrd(wrd *res, void *val, int *d, int *sc)
{
	(void) val;
	(void) d;
	(void) sc;

	*res = wrd_nil;
	return MAL_SUCCEED;
}

str
str_2dec_wrd(wrd *res, str *val, int *d, int *sc)
{
	char *s = strip_extra_zeros(*val);
	char *dot = strchr(s, '.');
	int digits = _strlen(s) - 1;
	int scale = digits - (int) (dot - s);
	lng value = 0;

	if (!dot) {
		if (GDK_STRNIL(*val)) {
			*res = wrd_nil;
			return MAL_SUCCEED;
		} else {
			throw(SQL, "wrd", "\"%s\" is no decimal value (doesn't contain a '.')", *val);
		}
	}

	value = decimal_from_str(s);
	if (*s == '+' || *s == '-')
		digits--;
	if (scale < *sc) {
		/* the current scale is too small, increase it by adding 0's */
		int d = *sc - scale;	/* CANNOT be 0! */

		value *= scales[d];
		scale += d;
		digits += d;
	} else if (scale > *sc) {
		/* the current scale is too big, decrease it by correctly rounding */
		int d = scale - *sc;	/* CANNOT be 0 */
		lng rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
		scale -= d;
		digits -= d;
	}
	if (digits > *d) {
		throw(SQL, "wrd", "decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);
	}
	*res = (wrd) value;
	return MAL_SUCCEED;
}

str
nil_2num_wrd(wrd *res, void *v, int *len)
{
	int zero = 0;
	return nil_2dec_wrd(res, v, len, &zero);
}

str
str_2num_wrd(wrd *res, str *v, int *len)
{
	int zero = 0;
	return str_2dec_wrd(res, v, len, &zero);
}

str
batnil_2dec_wrd(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	(void) d;
	(void) sc;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd r = wrd_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2dec_wrd(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		wrd r;
		msg = str_2dec_wrd(&r, &v, d, sc);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2num_wrd(int *res, int *bid, int *len)
{
	int zero = 0;
	return batnil_2dec_wrd(res, bid, len, &zero);
}

str
batstr_2num_wrd(int *res, int *bid, int *len)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2num_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		wrd r;
		msg = str_2num_wrd(&r, &v, len);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
wrd_dec2second_interval(lng *res, int *sc, wrd *dec, int *ek, int *sk)
{
	lng value = *dec;

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
	*res = value;
	return MAL_SUCCEED;
}

static inline lng
lng_dec_round_body_nonil(lng v, lng r)
{
	lng add = r >> 1;

	assert(v != lng_nil);

	if (v < 0)
		add = -add;
	v += add;
	return v / r;
}

static inline lng
lng_dec_round_body(lng v, lng r)
{
	/* shortcut nil */
	if (v == lng_nil) {
		return lng_nil;
	} else {
		return lng_dec_round_body_nonil(v, r);
	}
}

str
lng_dec_round_wrap(lng *res, lng *v, lng *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = lng_dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
lng_bat_dec_round_wrap(bat *_res, bat *_v, lng *r)
{
	BAT *res, *v;
	lng *src, *dst;
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
	if (v->ttype != TYPE_lng) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a lng tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_lng, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (lng *) Tloc(v, BUNfirst(v));
	dst = (lng *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = lng_dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == lng_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = lng_nil;
			} else {
				dst[i] = lng_dec_round_body_nonil(src[i], *r);
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

static inline lng
lng_round_body_nonil(lng v, int d, int s, bte r)
{
	lng res = lng_nil;

	assert(v != lng_nil);

	if (-r > d) {
		res = 0;
	} else if (r > 0 && r < s) {
		int dff = s - r;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_lng_min < lres && lres <= (lng) GDK_lng_max);
		res = (lng) lres;
	} else if (r <= 0 && -r + s > 0) {
		int dff = -r + s;
		lng rnd = scales[dff] >> 1;
		lng lres;
		if (v > 0)
			lres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			lres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((lng) GDK_lng_min < lres && lres <= (lng) GDK_lng_max);
		res = (lng) lres;
	} else {
		res = v;
	}
	return res;
}

static inline lng
lng_round_body(lng v, int d, int s, bte r)
{
	/* shortcut nil */
	if (v == lng_nil) {
		return lng_nil;
	} else {
		return lng_round_body_nonil(v, d, s, r);
	}
}

str
lng_round_wrap(lng *res, lng *v, int *d, int *s, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r && d && s);

	*res = lng_round_body(*v, *d, *s, *r);
	return MAL_SUCCEED;
}

str
lng_bat_round_wrap(bat *_res, bat *_v, int *d, int *s, bte *r)
{
	BAT *res, *v;
	lng *src, *dst;
	BUN i, cnt;
	bit nonil;		/* TRUE: we know there are no NIL (NULL) values */
	bit nil;		/* TRUE: we know there is at least one NIL (NULL) value */

	/* basic sanity checks */
	assert(_res && _v && r && d && s);

	/* get argument BAT descriptor */
	if ((v = BATdescriptor(*_v)) == NULL)
		throw(MAL, "round", RUNTIME_OBJECT_MISSING);

	/* more sanity checks */
	if (!BAThdense(v)) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dense head");
	}
	if (v->ttype != TYPE_lng) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a lng tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_lng, cnt, TRANSIENT);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (lng *) Tloc(v, BUNfirst(v));
	dst = (lng *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = lng_round_body_nonil(src[i], *d, *s, *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == lng_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = lng_nil;
			} else {
				dst[i] = lng_round_body_nonil(src[i], *d, *s, *r);
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
nil_2dec_lng(lng *res, void *val, int *d, int *sc)
{
	(void) val;
	(void) d;
	(void) sc;

	*res = lng_nil;
	return MAL_SUCCEED;
}

str
str_2dec_lng(lng *res, str *val, int *d, int *sc)
{
	char *s = strip_extra_zeros(*val);
	char *dot = strchr(s, '.');
	int digits = _strlen(s) - 1;
	int scale = digits - (int) (dot - s);
	lng value = 0;

	if (!dot) {
		if (GDK_STRNIL(*val)) {
			*res = lng_nil;
			return MAL_SUCCEED;
		} else {
			throw(SQL, "lng", "\"%s\" is no decimal value (doesn't contain a '.')", *val);
		}
	}

	value = decimal_from_str(s);
	if (*s == '+' || *s == '-')
		digits--;
	if (scale < *sc) {
		/* the current scale is too small, increase it by adding 0's */
		int d = *sc - scale;	/* CANNOT be 0! */

		value *= scales[d];
		scale += d;
		digits += d;
	} else if (scale > *sc) {
		/* the current scale is too big, decrease it by correctly rounding */
		int d = scale - *sc;	/* CANNOT be 0 */
		lng rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
		scale -= d;
		digits -= d;
	}
	if (digits > *d) {
		throw(SQL, "lng", "decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);
	}
	*res = (lng) value;
	return MAL_SUCCEED;
}

str
nil_2num_lng(lng *res, void *v, int *len)
{
	int zero = 0;
	return nil_2dec_lng(res, v, len, &zero);
}

str
str_2num_lng(lng *res, str *v, int *len)
{
	int zero = 0;
	return str_2dec_lng(res, v, len, &zero);
}

str
batnil_2dec_lng(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	(void) d;
	(void) sc;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng r = lng_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2dec_lng(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		lng r;
		msg = str_2dec_lng(&r, &v, d, sc);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2num_lng(int *res, int *bid, int *len)
{
	int zero = 0;
	return batnil_2dec_lng(res, bid, len, &zero);
}

str
batstr_2num_lng(int *res, int *bid, int *len)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2num_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		lng r;
		msg = str_2num_lng(&r, &v, len);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
lng_dec2second_interval(lng *res, int *sc, lng *dec, int *ek, int *sk)
{
	lng value = *dec;

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
	*res = value;
	return MAL_SUCCEED;
}
