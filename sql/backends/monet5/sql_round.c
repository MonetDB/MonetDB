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

#ifdef HAVE_HGE
static hge scales[39] = {
	(hge) LL_CONSTANT(1),
	(hge) LL_CONSTANT(10),
	(hge) LL_CONSTANT(100),
	(hge) LL_CONSTANT(1000),
	(hge) LL_CONSTANT(10000),
	(hge) LL_CONSTANT(100000),
	(hge) LL_CONSTANT(1000000),
	(hge) LL_CONSTANT(10000000),
	(hge) LL_CONSTANT(100000000),
	(hge) LL_CONSTANT(1000000000),
	(hge) LL_CONSTANT(10000000000),
	(hge) LL_CONSTANT(100000000000),
	(hge) LL_CONSTANT(1000000000000),
	(hge) LL_CONSTANT(10000000000000),
	(hge) LL_CONSTANT(100000000000000),
	(hge) LL_CONSTANT(1000000000000000),
	(hge) LL_CONSTANT(10000000000000000),
	(hge) LL_CONSTANT(100000000000000000),
	(hge) LL_CONSTANT(1000000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000000U)
};
#else
static lng scales[19] = {
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
#endif

#define CONCAT_2(a, b)		a##b
#define CONCAT_3(a, b, c)	a##b##c

#define NIL(t)			CONCAT_2(t, _nil)
#define TPE(t)			CONCAT_2(TYPE_, t)
#define GDKmin(t)		CONCAT_3(GDK_, t, _min)
#define GDKmax(t)		CONCAT_3(GDK_, t, _max)
#define FUN(a, b)		CONCAT_3(a, _, b)

#define STRING(a)		#a

#define TYPE bte
#include "sql_round_impl.h"
#undef TYPE

#define TYPE sht
#include "sql_round_impl.h"
#undef TYPE

#define TYPE int
#include "sql_round_impl.h"
#undef TYPE

#define TYPE wrd
#include "sql_round_impl.h"
#undef TYPE

#define TYPE lng
#include "sql_round_impl.h"
#undef TYPE

#ifdef HAVE_HGE
static inline hge
hge_dec_round_body_nonil(hge v, hge r)
{
	hge add = r >> 1;

	assert(v != hge_nil);

	if (v < 0)
		add = -add;
	v += add;
	return v / r;
}

static inline hge
hge_dec_round_body(hge v, hge r)
{
	/* shortcut nil */
	if (v == hge_nil) {
		return hge_nil;
	} else {
		return hge_dec_round_body_nonil(v, r);
	}
}

str
hge_dec_round_wrap(hge *res, hge *v, hge *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = hge_dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
hge_bat_dec_round_wrap(bat *_res, bat *_v, hge *r)
{
	BAT *res, *v;
	hge *src, *dst;
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
	if (v->ttype != TYPE_hge) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a hge tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_hge, cnt);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (hge *) Tloc(v, BUNfirst(v));
	dst = (hge *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = hge_dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == hge_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = hge_nil;
			} else {
				dst[i] = hge_dec_round_body_nonil(src[i], *r);
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

static inline hge
hge_round_body_nonil(hge v, int d, int s, bte r)
{
	hge res = hge_nil;

	assert(v != hge_nil);

	if (-r > d) {
		res = 0;
	} else if (r > 0 && r < s) {
		int dff = s - r;
		hge rnd = scales[dff] >> 1;
		hge hres;
		if (v > 0)
			hres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			hres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((hge) GDK_hge_min < hres && hres <= (hge) GDK_hge_max);
		res = (hge) hres;
	} else if (r <= 0 && -r + s > 0) {
		int dff = -r + s;
		hge rnd = scales[dff] >> 1;
		hge hres;
		if (v > 0)
			hres = (((v + rnd) / scales[dff]) * scales[dff]);
		else
			hres = (((v - rnd) / scales[dff]) * scales[dff]);
		assert((hge) GDK_hge_min < hres && hres <= (hge) GDK_hge_max);
		res = (hge) hres;
	} else {
		res = v;
	}
	return res;
}

static inline hge
hge_round_body(hge v, int d, int s, bte r)
{
	/* shortcut nil */
	if (v == hge_nil) {
		return hge_nil;
	} else {
		return hge_round_body_nonil(v, d, s, r);
	}
}

str
hge_round_wrap(hge *res, hge *v, int *d, int *s, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r && d && s);

	*res = hge_round_body(*v, *d, *s, *r);
	return MAL_SUCCEED;
}

str
hge_bat_round_wrap(bat *_res, bat *_v, int *d, int *s, bte *r)
{
	BAT *res, *v;
	hge *src, *dst;
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
	if (v->ttype != TYPE_hge) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a hge tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_hge, cnt);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (hge *) Tloc(v, BUNfirst(v));
	dst = (hge *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = hge_round_body_nonil(src[i], *d, *s, *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == hge_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = hge_nil;
			} else {
				dst[i] = hge_round_body_nonil(src[i], *d, *s, *r);
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
nil_2dec_hge(hge *res, void *val, int *d, int *sc)
{
	(void) val;
	(void) d;
	(void) sc;

	*res = hge_nil;
	return MAL_SUCCEED;
}

str
str_2dec_hge(hge *res, str *val, int *d, int *sc)
{
	char *s = strip_extra_zeros(*val);
	char *dot = strchr(s, '.');
	int digits = _strlen(s) - 1;
	int scale = digits - (int) (dot - s);
	hge value = 0;

	if (!dot) {
		if (GDK_STRNIL(*val)) {
			*res = hge_nil;
			return MAL_SUCCEED;
		} else {
			throw(SQL, "hge", "\"%s\" is no decimal value (doesn't contain a '.')", *val);
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
		hge rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
		scale -= d;
		digits -= d;
	}
	if (digits > *d) {
		throw(SQL, "hge", "decimal (%s) doesn't have format (%d.%d)", *val, *d, *sc);
	}
	*res = (hge) value;
	return MAL_SUCCEED;
}

str
nil_2num_hge(hge *res, void *v, int *len)
{
	int zero = 0;
	return nil_2dec_hge(res, v, len, &zero);
}

str
str_2num_hge(hge *res, str *v, int *len)
{
	int zero = 0;
	return str_2dec_hge(res, v, len, &zero);
}

str
batnil_2dec_hge(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	(void) d;
	(void) sc;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2dec_hge", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_hge, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_hge", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		hge r = hge_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2dec_hge(int *res, int *bid, int *d, int *sc)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2dec_hge", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_hge, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec_hge", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		hge r;
		msg = str_2dec_hge(&r, &v, d, sc);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2num_hge(int *res, int *bid, int *len)
{
	int zero = 0;
	return batnil_2dec_hge(res, bid, len, &zero);
}

str
batstr_2num_hge(int *res, int *bid, int *len)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2num_hge", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_hge, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num_hge", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		hge r;
		msg = str_2num_hge(&r, &v, len);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
hge_dec2second_interval(lng *res, int *sc, hge *dec, int *ek, int *sk)
{
	hge value = *dec;

	(void) ek;
	(void) sk;
	if (*sc < 3) {
		int d = 3 - *sc;
		value *= scales[d];
	} else if (*sc > 3) {
		int d = *sc - 3;
		hge rnd = scales[d] >> 1;

		value += rnd;
		value /= scales[d];
	}
	assert((hge) GDK_lng_min < value && value <= (hge) GDK_lng_max);
	*res = (lng) value;
	return MAL_SUCCEED;
}
#endif
