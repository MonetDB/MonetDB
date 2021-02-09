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
#define trunc_wrap		FUN(TYPE, trunc_wrap)

static inline TYPE
dec_round_body(TYPE v, TYPE r)
{
	assert(!ISNIL(TYPE)(v));

	return v / r;
}

str
dec_round_wrap(TYPE *res, const TYPE *v, const TYPE *r)
{
	/* basic sanity checks */
	assert(res && v);
	TYPE rr = *r;

	if (ISNIL(TYPE)(rr))
		throw(MAL, "round", SQLSTATE(42000) "Argument 2 to round function cannot be null");
	if (rr <= 0)
		throw(MAL, "round", SQLSTATE(42000) "Argument 2 to round function must be positive");
	*res = ISNIL(TYPE)(*v) ? NIL(TYPE) : dec_round_body(*v, rr);
	if (isinf(*res))
		throw(MAL, "round", SQLSTATE(22003) "Overflow in round");
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
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
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
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
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
		msg = createException(MAL, "round", SQLSTATE(42000) "Argument 2 must have a " STRING(TYPE) " tail");
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
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
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
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
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
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
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
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
			}
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(res, bn, left, msg, nils, q, false);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static inline TYPE
round_body(TYPE v, int r)
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

str
round_wrap(TYPE *res, const TYPE *v, const bte *r)
{
	/* basic sanity checks */
	assert(res && v && r);
	bte rr = *r;

	if (is_bte_nil(rr) || (size_t) abs(rr) >= sizeof(scales) / sizeof(scales[0]))
		throw(MAL, "round", SQLSTATE(42000) "Digits out of bounds");
	*res = (ISNIL(TYPE)(*v)) ? NIL(TYPE) : round_body(*v, rr);
	if (isinf(*res))
		throw(MAL, "round", SQLSTATE(22003) "Overflow in round");
	return MAL_SUCCEED;
}

str
bat_round_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	TYPE *restrict src, *restrict dst, x;
	bte r = *getArgReference_bte(stk, pci, 2);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (is_bte_nil(r) || (size_t) abs(r) >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(MAL, "round", SQLSTATE(42000) "Digits out of bounds");
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
				dst[i] = round_body(x, r);
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
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
				dst[i] = round_body(x, r);
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
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

			if (is_bte_nil(r) || (size_t) abs(r) >= sizeof(scales) / sizeof(scales[0])) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Digits out of bounds");
				goto bailout;
			} else if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, r);
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			r = src[p1];

			if (is_bte_nil(r) || (size_t) abs(r) >= sizeof(scales) / sizeof(scales[0])) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Digits out of bounds");
				goto bailout;
			} else if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, r);
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
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
	TYPE *src1, *restrict dst, x;
	bte *src2, rr;
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

			if (is_bte_nil(rr) || (size_t) abs(rr) >= sizeof(scales) / sizeof(scales[0])) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Digits out of bounds");
				goto bailout;
			} else if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, rr);
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			x = src1[p1];
			rr = src2[p2];

			if (is_bte_nil(rr) || (size_t) abs(rr) >= sizeof(scales) / sizeof(scales[0])) {
				msg = createException(MAL, "round", SQLSTATE(42000) "Digits out of bounds");
				goto bailout;
			} else if (ISNIL(TYPE)(x)) {
				dst[i] = NIL(TYPE);
				nils = true;
			} else {
				dst[i] = round_body(x, rr);
				if (isinf(dst[i])) {
					msg = createException(MAL, "round", SQLSTATE(22003) "Overflow in round");
					goto bailout;
				}
			}
		}
	}

bailout:
	finalize_ouput_copy_sorted_property(res, bn, left, msg, nils, q, false);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

str
trunc_wrap(TYPE *res, const TYPE *v, const int *r)
{
	int rr = *r;
	if ((size_t) abs(rr) >= sizeof(scales) / sizeof(scales[0]))
		throw(MAL, "trunc", SQLSTATE(42000) "Digits out of bounds");

	/* shortcut nil */
	if (ISNIL(TYPE)(*v)) {
		*res = NIL(TYPE);
	} else if (rr < 0) {
		int d = -rr;
		*res = (TYPE) (trunc((*v) / ((TYPE) scales[d])) * scales[d]);
	} else if (rr > 0) {
		int d = rr;
		*res = (TYPE) (trunc(*v * (TYPE) scales[d]) / ((TYPE) scales[d]));
	} else {
		*res = (TYPE) trunc(*v);
	}
	return MAL_SUCCEED;
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
#undef trunc_wrap
