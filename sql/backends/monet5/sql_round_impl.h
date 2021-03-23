/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#define dec_round_body		FUN(TYPE, dec_round_body)
#define dec_round_wrap		FUN(TYPE, dec_round_wrap)
#define bat_dec_round_wrap	FUN(TYPE, bat_dec_round_wrap)
#define bat_dec_round_wrap_cst	FUN(TYPE, bat_dec_round_wrap_cst)
#define bat_dec_round_wrap_nocst	FUN(TYPE, bat_dec_round_wrap_nocst)
#define round_body		FUN(TYPE, round_body)
#define round_wrap		FUN(TYPE, round_wrap)
#define bat_round_wrap		FUN(TYPE, bat_round_wrap)
#define bat_round_wrap_cst	FUN(TYPE, bat_round_wrap_cst)
#define bat_round_wrap_nocst	FUN(TYPE, bat_round_wrap_nocst)
#define nil_2dec		FUN(nil_2dec, TYPE)
#define str_2dec_body		FUN(str_2dec_body, TYPE)
#define str_2dec		FUN(str_2dec, TYPE)
#define batnil_2dec		FUN(batnil_2dec, TYPE)
#define batstr_2dec		FUN(batstr_2dec, TYPE)
#define dec2second_interval	FUN(TYPE, dec2second_interval)
#define batdec2second_interval	FUN(TYPE, batdec2second_interval)

static inline TYPE
dec_round_body(TYPE v, TYPE r)
{
	TYPE add = r >> 1;

	assert(!ISNIL(TYPE)(v));

	if (v < 0)
		add = -add;
	v += add;
	return v / r;
}

str
dec_round_wrap(TYPE *res, const TYPE *v, const TYPE *r)
{
	/* basic sanity checks */
	assert(res && v && r);

	if (ISNIL(TYPE)(*r))
		throw(MAL, "round", SQLSTATE(42000) "Argument 2 to round function cannot be null");
	if (*r <= 0)
		throw(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");

	*res = ISNIL(TYPE)(*v) ? NIL(TYPE) : dec_round_body(*v, *r);
	return MAL_SUCCEED;
}

str
bat_dec_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	TYPE *restrict src, *restrict dst, x, r = *(TYPE *)getArgReference(stk, pci, 2);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (ISNIL(TYPE)(r)) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function cannot be null");
		goto bailout;
	}
	if (r <= 0) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (b->ttype != TPE(TYPE)) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 1 must have a " STRING(TYPE) " tail");
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TPE(TYPE), q, TRANSIENT))) {
		msg = createException(MAL, "round", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	src = (TYPE *) Tloc(b, 0);
	dst = (TYPE *) Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			x = src[p1];

			if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = dec_round_body(x, r);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			x = src[p1];

			if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = dec_round_body(x, r);
			}
		}
	}
bailout:
	finalize_ouput_copy_sorted_property(res, bn, b, msg, nils, q, true);
	unfix_inputs(2, b, bs);
	return msg;
}

str
bat_dec_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	TYPE *restrict src, *restrict dst, x = *(TYPE *)getArgReference(stk, pci, 1), r;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (b->ttype != TPE(TYPE)) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 1 must have a " STRING(TYPE) " tail");
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TPE(TYPE), q, TRANSIENT))) {
		msg = createException(MAL, "round", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	src = (TYPE *) Tloc(b, 0);
	dst = (TYPE *) Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			r = src[p1];

			if (ISNIL(TYPE)(r)) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function cannot be null");
				goto bailout;
			} else if (r <= 0) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");
				goto bailout;
			} else if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = dec_round_body(x, r);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			r = src[p1];

			if (ISNIL(TYPE)(r)) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function cannot be null");
				goto bailout;
			} else if (r <= 0) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");
				goto bailout;
			} else if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = dec_round_body(x, r);
			}
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(res, bn, b, msg, nils, q, false);
	unfix_inputs(2, b, bs);
	return msg;
}

str
bat_dec_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	BUN q = 0;
	TYPE *src1, *src2, *restrict dst, x, rr;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (left->ttype != TPE(TYPE) || right->ttype != TPE(TYPE)) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Arguments must have a " STRING(TYPE) " tail");
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, lefts);
	if (canditer_init(&ci2, right, rights) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "round", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TPE(TYPE), q, TRANSIENT))) {
		msg = createException(MAL, "round", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	src1 = (TYPE *) Tloc(left, 0);
	src2 = (TYPE *) Tloc(right, 0);
	dst = (TYPE *) Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			x = src1[p1];
			rr = src2[p2];

			if (ISNIL(TYPE)(rr)) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function cannot be null");
				goto bailout;
			} else if (rr <= 0) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");
				goto bailout;
			} else if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = dec_round_body(x, rr);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			x = src1[p1];
			rr = src2[p2];

			if (ISNIL(TYPE)(rr)) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function cannot be null");
				goto bailout;
			} else if (rr <= 0) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");
				goto bailout;
			} else if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = dec_round_body(x, rr);
			}
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(res, bn, left, msg, nils, q, false);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static inline TYPE
round_body(TYPE v, int d, int s, int r)
{
	TYPE res = NIL(TYPE);

	assert(!ISNIL(TYPE)(v));

	if (r > 0 && r < s) {
		int dff = s - r;
		BIG rnd = scales[dff] >> 1;
		BIG lres;
		if (v > 0)
			lres = ((v + rnd) / scales[dff]) * scales[dff];
		else
			lres = ((v - rnd) / scales[dff]) * scales[dff];
		res = (TYPE) lres;
	} else if (r <= 0 && -r > -s) {
		int dff = -r + s;
		if (dff > d) {
			res = 0;
		} else {
			BIG rnd = scales[dff] >> 1;
			BIG lres;
			if (v > 0)
				lres = ((v + rnd) / scales[dff]) * scales[dff];
			else
				lres = ((v - rnd) / scales[dff]) * scales[dff];
			res = (TYPE) lres;
		}
	} else {
		res = v;
	}
	return res;
}

str
round_wrap(TYPE *res, const TYPE *v, const bte *r, const int *d, const int *s)
{
	/* basic sanity checks */
	assert(res && v && r && d && s);

	*res = (ISNIL(TYPE)(*v) || is_bte_nil(*r)) ? NIL(TYPE) : round_body(*v, *d, *s, *r);
	return MAL_SUCCEED;
}

str
bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	TYPE *restrict src, *restrict dst, x;
	bte r = *getArgReference_bte(stk, pci, 2);
	int d = *getArgReference_int(stk, pci, pci->argc == 6 ? 4 : 3), s = *getArgReference_int(stk, pci, pci->argc == 6 ? 5 : 4);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (b->ttype != TPE(TYPE)) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 1 must have a " STRING(TYPE) " tail");
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TPE(TYPE), q, TRANSIENT))) {
		msg = createException(MAL, "round", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	src = (TYPE *) Tloc(b, 0);
	dst = (TYPE *) Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			x = src[p1];

			if (ISNIL(TYPE)(x) || is_bte_nil(r)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, d, s, r);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			x = src[p1];

			if (ISNIL(TYPE)(x) || is_bte_nil(r)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, d, s, r);
			}
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(res, bn, b, msg, nils, q, true);
	unfix_inputs(2, b, bs);
	return msg;
}

str
bat_round_wrap_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	TYPE *restrict dst, x = *(TYPE *)getArgReference(stk, pci, 1);
	bte *restrict src, r;
	int d = *getArgReference_int(stk, pci, pci->argc == 6 ? 4 : 3), s = *getArgReference_int(stk, pci, pci->argc == 6 ? 5 : 4);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (b->ttype != TYPE_bte) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 must have a bte tail");
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TPE(TYPE), q, TRANSIENT))) {
		msg = createException(MAL, "round", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	src = (bte *) Tloc(b, 0);
	dst = (TYPE *) Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			r = src[p1];

			if (ISNIL(TYPE)(x) || is_bte_nil(r)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, d, s, r);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			r = src[p1];

			if (ISNIL(TYPE)(x) || is_bte_nil(r)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, d, s, r);
			}
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(res, bn, b, msg, nils, q, false);
	unfix_inputs(2, b, bs);
	return msg;
}

str
bat_round_wrap_nocst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	BUN q = 0;
	TYPE *restrict dst, *src1, x;
	bte *src2, rr;
	int d = *getArgReference_int(stk, pci, pci->argc == 7 ? 5 : 3), s = *getArgReference_int(stk, pci, pci->argc == 7 ? 6 : 4);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 7 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 7 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (left->ttype != TPE(TYPE)) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 1 must have a " STRING(TYPE) " tail");
		goto bailout;
	}
	if (right->ttype != TYPE_bte) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 must have a bte tail");
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "round", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, lefts);
	if (canditer_init(&ci2, right, rights) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "round", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TPE(TYPE), q, TRANSIENT))) {
		msg = createException(MAL, "round", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	src1 = (TYPE *) Tloc(left, 0);
	src2 = (bte *) Tloc(right, 0);
	dst = (TYPE *) Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			x = src1[p1];
			rr = src2[p2];

			if (ISNIL(TYPE)(x) || is_bte_nil(rr)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, d, s, rr);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			x = src1[p1];
			rr = src2[p2];

			if (ISNIL(TYPE)(x) || is_bte_nil(rr)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, d, s, rr);
			}
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(res, bn, left, msg, nils, q, false);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
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
	int digits;
	int scale;
	BIG value;

	if (d < 0 || d >= (int) (sizeof(scales) / sizeof(scales[0])))
		throw(SQL, STRING(TYPE), SQLSTATE(42000) "Decimal (%s) doesn't have format (%d.%d)", s, d, sc);

	int has_errors;
	value = 0;

	// s = strip_extra_zeros(s);

	value = decimal_from_str(s, &digits, &scale, &has_errors);
	if (has_errors)
		throw(SQL, STRING(TYPE), SQLSTATE(42000) "Decimal (%s) doesn't have format (%d.%d)", s, d, sc);

	// handle situations where the de facto scale is different from the formal scale.
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
	if (value <= -scales[d] || value >= scales[d])
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

	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			const str next = BUNtail(bi, p);

			if (strNil(next)) {
				ret[i] = NIL(TYPE);
				nils = true;
			} else if ((msg = str_2dec_body(&(ret[i]), next, d, sk)))
				goto bailout;
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next(&ci) - off);
			const str next = BUNtail(bi, p);

			if (strNil(next)) {
				ret[i] = NIL(TYPE);
				nils = true;
			} else if ((msg = str_2dec_body(&(ret[i]), next, d, sk)))
				goto bailout;
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(r, res, b, msg, nils, q, false);
	unfix_inputs(2, b, s);
	return msg;
}

str
dec2second_interval(lng *res, const int *sc, const TYPE *dec, const int *ek, const int *sk)
{
	BIG value = *dec;
	int scale = *sc;

	if (scale < 0 || (size_t) scale >= sizeof(scales) / sizeof(scales[0]))
		throw(SQL, "calc.dec2second_interval", SQLSTATE(42000) "Digits out of bounds");

	(void) ek;
	(void) sk;
	if (ISNIL(TYPE)(*dec)) {
		value = lng_nil;
	} else if (scale < 3) {
		int d = 3 - scale;
		value *= scales[d];
	} else if (scale > 3) {
		int d = scale - 3;
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
	if (sc < 0 || (size_t) sc >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(SQL, "batcalc.batdec2second_interval", SQLSTATE(42000) "Digits out of bounds");
		goto bailout;
	}
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

	/* the cast from decimal to interval is now deactivated. So adding the canditer_next_dense case is not worth */
	if (sc < 3) {
		for (BUN i = 0 ; i < q ; i++) {
			oid p = (canditer_next(&ci) - off);
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
			oid p = (canditer_next(&ci) - off);
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
			oid p = (canditer_next(&ci) - off);
			if (ISNIL(TYPE)(src[p])) {
				ret[i] = lng_nil;
				nils = true;
			} else {
				ret[i] = (BIG) src[p];
			}
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(r, res, b, msg, nils, q, false);
	unfix_inputs(2, b, s);
	return msg;
}

#undef dec_round_body
#undef dec_round_wrap
#undef bat_dec_round_wrap
#undef bat_dec_round_wrap_cst
#undef bat_dec_round_wrap_nocst
#undef round_body
#undef round_wrap
#undef bat_round_wrap
#undef bat_round_wrap_cst
#undef bat_round_wrap_nocst
#undef nil_2dec
#undef str_2dec_body
#undef str_2dec
#undef batnil_2dec
#undef batstr_2dec
#undef dec2second_interval
#undef batdec2second_interval
