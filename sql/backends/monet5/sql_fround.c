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

static inline flt
flt_dec_round_body_nonil(flt v, flt r)
{
	assert(v != flt_nil);

	return v / r;
}

static inline flt
flt_dec_round_body(flt v, flt r)
{
	/* shortcut nil */
	if (v == flt_nil) {
		return flt_nil;
	} else {
		return flt_dec_round_body_nonil(v, r);
	}
}

str
flt_dec_round_wrap(flt *res, flt *v, flt *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = flt_dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
flt_bat_dec_round_wrap(bat *_res, bat *_v, flt *r)
{
	BAT *res, *v;
	flt *src, *dst;
	BUN i, cnt;
	bit nonil;  /* TRUE: we know there are no NIL (NULL) values */
	bit nil; /* TRUE: we know there is at least one NIL (NULL) value */

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
	if (v->ttype != TYPE_flt) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a flt tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_flt, cnt);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (flt *) Tloc(v, BUNfirst(v));
	dst = (flt *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = flt_dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == flt_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = flt_nil;
			} else {
				dst[i] = flt_dec_round_body_nonil(src[i], *r);
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

static inline flt
flt_round_body_nonil(flt v, bte r)
{
	flt res = flt_nil;

	assert(v != flt_nil);

	if (r < 0) {
		int d = -r;
		flt rnd = (flt) (scales[d] >> 1);

		res = (flt) (floor(((v + rnd) / ((flt) (scales[d])))) * scales[d]);
	} else if (r > 0) {
		int d = r;

		res = (flt) (floor(v * (flt) scales[d] + .5) / scales[d]);
	} else {
		res = (flt) round(v);
	}
	return res;
}

static inline flt
flt_round_body(flt v, bte r)
{
	/* shortcut nil */
	if (v == flt_nil) {
		return flt_nil;
	} else {
		return flt_round_body_nonil(v, r);
	}
}

str
flt_round_wrap(flt *res, flt *v, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = flt_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
flt_bat_round_wrap(bat *_res, bat *_v, bte *r)
{
	BAT *res, *v;
	flt *src, *dst;
	BUN i, cnt;
	bit nonil;  /* TRUE: we know there are no NIL (NULL) values */
	bit nil; /* TRUE: we know there is at least one NIL (NULL) value */

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
	if (v->ttype != TYPE_flt) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a flt tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_flt, cnt);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (flt *) Tloc(v, BUNfirst(v));
	dst = (flt *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = flt_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == flt_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = flt_nil;
			} else {
				dst[i] = flt_round_body_nonil(src[i], *r);
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
flt_trunc_wrap(flt *res, flt *v, int *r)
{
	/* shortcut nil */
	if (*v == flt_nil) {
		*res = flt_nil;
	} else if (*r < 0) {
		int d = -*r;
		*res = (flt) (trunc((*v) / ((flt) scales[d])) * scales[d]);
	} else if (*r > 0) {
		int d = *r;
		*res = (flt) (trunc(*v * (flt) scales[d]) / ((flt) scales[d]));
	} else {
		*res = (flt) trunc(*v);
	}
	return MAL_SUCCEED;
}

static inline dbl
dbl_dec_round_body_nonil(dbl v, dbl r)
{
	assert(v != dbl_nil);

	return v / r;
}

static inline dbl
dbl_dec_round_body(dbl v, dbl r)
{
	/* shortcut nil */
	if (v == dbl_nil) {
		return dbl_nil;
	} else {
		return dbl_dec_round_body_nonil(v, r);
	}
}

str
dbl_dec_round_wrap(dbl *res, dbl *v, dbl *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = dbl_dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
dbl_bat_dec_round_wrap(bat *_res, bat *_v, dbl *r)
{
	BAT *res, *v;
	dbl *src, *dst;
	BUN i, cnt;
	bit nonil;  /* TRUE: we know there are no NIL (NULL) values */
	bit nil; /* TRUE: we know there is at least one NIL (NULL) value */

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
	if (v->ttype != TYPE_dbl) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dbl tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_dbl, cnt);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (dbl *) Tloc(v, BUNfirst(v));
	dst = (dbl *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = dbl_dec_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == dbl_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = dbl_nil;
			} else {
				dst[i] = dbl_dec_round_body_nonil(src[i], *r);
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

static inline dbl
dbl_round_body_nonil(dbl v, bte r)
{
	dbl res = dbl_nil;

	assert(v != dbl_nil);

	if (r < 0) {
		int d = -r;
		dbl rnd = (dbl) (scales[d] >> 1);

		res = (dbl) (floor(((v + rnd) / ((dbl) (scales[d])))) * scales[d]);
	} else if (r > 0) {
		int d = r;

		res = (dbl) (floor(v * (dbl) scales[d] + .5) / scales[d]);
	} else {
		res = (dbl) round(v);
	}
	return res;
}

static inline dbl
dbl_round_body(dbl v, bte r)
{
	/* shortcut nil */
	if (v == dbl_nil) {
		return dbl_nil;
	} else {
		return dbl_round_body_nonil(v, r);
	}
}

str
dbl_round_wrap(dbl *res, dbl *v, bte *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	*res = dbl_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
dbl_bat_round_wrap(bat *_res, bat *_v, bte *r)
{
	BAT *res, *v;
	dbl *src, *dst;
	BUN i, cnt;
	bit nonil;  /* TRUE: we know there are no NIL (NULL) values */
	bit nil; /* TRUE: we know there is at least one NIL (NULL) value */

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
	if (v->ttype != TYPE_dbl) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", "argument 1 must have a dbl tail");
	}
	cnt = BATcount(v);

	/* allocate result BAT */
	res = BATnew(TYPE_void, TYPE_dbl, cnt);
	if (res == NULL) {
		BBPreleaseref(v->batCacheid);
		throw(MAL, "round", MAL_MALLOC_FAIL);
	}

	/* access columns as arrays */
	src = (dbl *) Tloc(v, BUNfirst(v));
	dst = (dbl *) Tloc(res, BUNfirst(res));

	nil = FALSE;
	nonil = TRUE;
	if (v->T->nonil == TRUE) {
		for (i = 0; i < cnt; i++)
			dst[i] = dbl_round_body_nonil(src[i], *r);
	} else {
		for (i = 0; i < cnt; i++) {
			if (src[i] == dbl_nil) {
				nil = TRUE;
				nonil = FALSE;
				dst[i] = dbl_nil;
			} else {
				dst[i] = dbl_round_body_nonil(src[i], *r);
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
dbl_trunc_wrap(dbl *res, dbl *v, int *r)
{
	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = dbl_nil;
	} else if (*r < 0) {
		int d = -*r;
		*res = (dbl) (trunc((*v) / ((dbl) scales[d])) * scales[d]);
	} else if (*r > 0) {
		int d = *r;
		*res = (dbl) (trunc(*v * (dbl) scales[d]) / ((dbl) scales[d]));
	} else {
		*res = (dbl) trunc(*v);
	}
	return MAL_SUCCEED;
}
