/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

/* Generally, the functions return a new BAT aligned with the input
 * BAT(s).  If there are multiple input BATs, they must be aligned.
 * If there is a candidate list, the calculations are only done for
 * the candidates, all other values are NIL (so that the output is
 * still aligned). */

static inline gdk_return
checkbats(BATiter *b1i, BATiter *b2i, const char *func)
{
	if (b1i->count != b2i->count) {
		GDKerror("%s: inputs not the same size.\n", func);
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) NOT */

#define NOT(x)		(~(x))
#define NOTBIT(x)	(!(x))

BAT *
BATcalcnot(BAT *b, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils = 0;
	BUN i;
	oid x, bhseqbase;
	struct canditer ci;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	bhseqbase = b->hseqbase;
	canditer_init(&ci, b, s);
	if (ci.ncand == 0)
		return BATconstant(ci.hseq, b->ttype,
				   ATOMnilptr(b->ttype), ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_msk:
		if (ci.tpe == cand_dense) {
			const uint32_t *restrict src = (const uint32_t *) bi.base + (ci.seq - b->hseqbase) / 32;
			uint32_t *restrict dst = Tloc(bn, 0);
			int bits = (ci.seq - b->hseqbase) % 32;
			BUN ncand = (ci.ncand + 31) / 32;
			if (bits == 0) {
				TIMEOUT_LOOP_IDX(i, ncand, timeoffset) {
					dst[i] = ~src[i];
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX(i, ncand, timeoffset) {
					dst[i] = (~src[i] >> bits) | ~(src[i + 1] >> (32 - bits));
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
			if (ci.ncand % 32 != 0)
				dst[ci.ncand / 32] &= (1U << (ci.ncand % 32)) - 1;
		} else {
			TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {
				x = canditer_next(&ci) - bhseqbase;
				mskSetVal(bn, i, !Tmskval(&bi, x));
			}
			TIMEOUT_CHECK(timeoffset,
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		}
		break;
	case TYPE_bte:
		if (bi.type == TYPE_bit) {
			UNARY_2TYPE_FUNC(bit, bit, NOTBIT);
		} else {
			UNARY_2TYPE_FUNC_nilcheck(bte, bte, NOT, ON_OVERFLOW1(bte, "NOT"));
		}
		break;
	case TYPE_sht:
		UNARY_2TYPE_FUNC_nilcheck(sht, sht, NOT, ON_OVERFLOW1(sht, "NOT"));
		break;
	case TYPE_int:
		UNARY_2TYPE_FUNC_nilcheck(int, int, NOT, ON_OVERFLOW1(int, "NOT"));
		break;
	case TYPE_lng:
		UNARY_2TYPE_FUNC_nilcheck(lng, lng, NOT, ON_OVERFLOW1(lng, "NOT"));
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		UNARY_2TYPE_FUNC_nilcheck(hge, hge, NOT, ON_OVERFLOW1(hge, "NOT"));
		break;
#endif
	default:
		GDKerror("type %s not supported.\n", ATOMname(bi.type));
		goto bailout;
	}

	BATsetcount(bn, ci.ncand);

	/* NOT reverses the order, but NILs mess it up */
	bn->tsorted = nils == 0 && bi.revsorted;
	bn->trevsorted = nils == 0 && bi.sorted;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tkey = bi.key && nils <= 1;
	bat_iterator_end(&bi);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;

bailout:
	bat_iterator_end(&bi);
	BBPunfix(bn->batCacheid);
	return NULL;
}

gdk_return
VARcalcnot(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_msk:
		ret->val.mval = !v->val.mval;
		break;
	case TYPE_bte:
		if (is_bit_nil(v->val.btval))
			ret->val.btval = bit_nil;
		else if (v->vtype == TYPE_bit)
			ret->val.btval = !v->val.btval;
		else {
			ret->val.btval = ~v->val.btval;
			if (is_bte_nil(ret->val.btval)) {
				GDKerror("22003!overflow in calculation "
					 "NOT(" FMTbte ").\n", v->val.btval);
				return GDK_FAIL;
			}
		}
		break;
	case TYPE_sht:
		if (is_sht_nil(v->val.shval))
			ret->val.shval = sht_nil;
		else {
			ret->val.shval = ~v->val.shval;
			if (is_sht_nil(ret->val.shval)) {
				GDKerror("22003!overflow in calculation "
					 "NOT(" FMTsht ").\n", v->val.shval);
				return GDK_FAIL;
			}
		}
		break;
	case TYPE_int:
		if (is_int_nil(v->val.ival))
			ret->val.ival = int_nil;
		else {
			ret->val.ival = ~v->val.ival;
			if (is_int_nil(ret->val.ival)) {
				GDKerror("22003!overflow in calculation "
					 "NOT(" FMTint ").\n", v->val.ival);
				return GDK_FAIL;
			}
		}
		break;
	case TYPE_lng:
		if (is_lng_nil(v->val.lval))
			ret->val.lval = lng_nil;
		else {
			ret->val.lval = ~v->val.lval;
			if (is_lng_nil(ret->val.lval)) {
				GDKerror("22003!overflow in calculation "
					 "NOT(" FMTlng ").\n", v->val.lval);
				return GDK_FAIL;
			}
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (is_hge_nil(v->val.hval))
			ret->val.hval = hge_nil;
		else {
			ret->val.hval = ~v->val.hval;
			if (is_hge_nil(ret->val.hval)) {
				GDKerror("22003!overflow in calculation "
					 "NOT(" FMThge ").\n",
					 CSThge v->val.hval);
				return GDK_FAIL;
			}
		}
		break;
#endif
	default:
		GDKerror("bad input type %s.\n", ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* negate value (any numeric type) */

#define NEGATE(x)	(-(x))

BAT *
BATcalcnegate(BAT *b, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils = 0;
	BUN i;
	oid x, bhseqbase;
	struct canditer ci;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}


	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	bhseqbase = b->hseqbase;
	canditer_init(&ci, b, s);
	if (ci.ncand == 0)
		return BATconstant(ci.hseq, b->ttype,
				   ATOMnilptr(b->ttype), ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		UNARY_2TYPE_FUNC(bte, bte, NEGATE);
		break;
	case TYPE_sht:
		UNARY_2TYPE_FUNC(sht, sht, NEGATE);
		break;
	case TYPE_int:
		UNARY_2TYPE_FUNC(int, int, NEGATE);
		break;
	case TYPE_lng:
		UNARY_2TYPE_FUNC(lng, lng, NEGATE);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		UNARY_2TYPE_FUNC(hge, hge, NEGATE);
		break;
#endif
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, flt, NEGATE);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, dbl, NEGATE);
		break;
	default:
		GDKerror("type %s not supported.\n", ATOMname(bi.type));
		goto bailout;
	}

	BATsetcount(bn, ci.ncand);

	/* unary - reverses the order, but NILs mess it up */
	bn->tsorted = nils == 0 && bi.revsorted;
	bn->trevsorted = nils == 0 && bi.sorted;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tkey = bi.key && nils <= 1;
	bat_iterator_end(&bi);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
bailout:
	bat_iterator_end(&bi);
	BBPunfix(bn->batCacheid);
	return NULL;
}

gdk_return
VARcalcnegate(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (is_bte_nil(v->val.btval))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = -v->val.btval;
		break;
	case TYPE_sht:
		if (is_sht_nil(v->val.shval))
			ret->val.shval = sht_nil;
		else
			ret->val.shval = -v->val.shval;
		break;
	case TYPE_int:
		if (is_int_nil(v->val.ival))
			ret->val.ival = int_nil;
		else
			ret->val.ival = -v->val.ival;
		break;
	case TYPE_lng:
		if (is_lng_nil(v->val.lval))
			ret->val.lval = lng_nil;
		else
			ret->val.lval = -v->val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (is_hge_nil(v->val.hval))
			ret->val.hval = hge_nil;
		else
			ret->val.hval = -v->val.hval;
		break;
#endif
	case TYPE_flt:
		if (is_flt_nil(v->val.fval))
			ret->val.fval = flt_nil;
		else
			ret->val.fval = -v->val.fval;
		break;
	case TYPE_dbl:
		if (is_dbl_nil(v->val.dval))
			ret->val.dval = dbl_nil;
		else
			ret->val.dval = -v->val.dval;
		break;
	default:
		GDKerror("bad input type %s.\n", ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* absolute value (any numeric type) */

BAT *
BATcalcabsolute(BAT *b, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils = 0;
	BUN i;
	oid x, bhseqbase;
	struct canditer ci;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}


	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	bhseqbase = b->hseqbase;
	canditer_init(&ci, b, s);
	if (ci.ncand == 0)
		return BATconstant(ci.hseq, b->ttype,
				   ATOMnilptr(b->ttype), ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		UNARY_2TYPE_FUNC(bte, bte, (bte) abs);
		break;
	case TYPE_sht:
		UNARY_2TYPE_FUNC(sht, sht, (sht) abs);
		break;
	case TYPE_int:
		UNARY_2TYPE_FUNC(int, int, abs);
		break;
	case TYPE_lng:
		UNARY_2TYPE_FUNC(lng, lng, llabs);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		UNARY_2TYPE_FUNC(hge, hge, ABSOLUTE);
		break;
#endif
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, flt, fabsf);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, dbl, fabs);
		break;
	default:
		GDKerror("bad input type %s.\n", ATOMname(bi.type));
		goto bailout;
	}
	bat_iterator_end(&bi);

	BATsetcount(bn, ci.ncand);

	/* ABSOLUTE messes up order (unless all values were negative
	 * or all values were positive, but we don't know anything
	 * about that) */
	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
bailout:
	bat_iterator_end(&bi);
	BBPunfix(bn->batCacheid);
	return NULL;
}

gdk_return
VARcalcabsolute(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (is_bte_nil(v->val.btval))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = (bte) abs(v->val.btval);
		break;
	case TYPE_sht:
		if (is_sht_nil(v->val.shval))
			ret->val.shval = sht_nil;
		else
			ret->val.shval = (sht) abs(v->val.shval);
		break;
	case TYPE_int:
		if (is_int_nil(v->val.ival))
			ret->val.ival = int_nil;
		else
			ret->val.ival = abs(v->val.ival);
		break;
	case TYPE_lng:
		if (is_lng_nil(v->val.lval))
			ret->val.lval = lng_nil;
		else
			ret->val.lval = llabs(v->val.lval);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (is_hge_nil(v->val.hval))
			ret->val.hval = hge_nil;
		else
			ret->val.hval = ABSOLUTE(v->val.hval);
		break;
#endif
	case TYPE_flt:
		if (is_flt_nil(v->val.fval))
			ret->val.fval = flt_nil;
		else
			ret->val.fval = fabsf(v->val.fval);
		break;
	case TYPE_dbl:
		if (is_dbl_nil(v->val.dval))
			ret->val.dval = dbl_nil;
		else
			ret->val.dval = fabs(v->val.dval);
		break;
	default:
		GDKerror("bad input type %s.\n", ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* is the value equal to zero (any numeric type) */

#define ISZERO(x)		((bit) ((x) == 0))

BAT *
BATcalciszero(BAT *b, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils = 0;
	BUN i;
	oid x, bhseqbase;
	struct canditer ci;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	bhseqbase = b->hseqbase;
	canditer_init(&ci, b, s);
	if (ci.ncand == 0)
		return BATconstant(ci.hseq, TYPE_bit,
				   ATOMnilptr(TYPE_bit), ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, TYPE_bit, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		UNARY_2TYPE_FUNC(bte, bit, ISZERO);
		break;
	case TYPE_sht:
		UNARY_2TYPE_FUNC(sht, bit, ISZERO);
		break;
	case TYPE_int:
		UNARY_2TYPE_FUNC(int, bit, ISZERO);
		break;
	case TYPE_lng:
		UNARY_2TYPE_FUNC(lng, bit, ISZERO);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		UNARY_2TYPE_FUNC(hge, bit, ISZERO);
		break;
#endif
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, bit, ISZERO);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, bit, ISZERO);
		break;
	default:
		GDKerror("bad input type %s.\n", ATOMname(bi.type));
		goto bailout;
	}
	bat_iterator_end(&bi);

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
bailout:
	bat_iterator_end(&bi);
	BBPunfix(bn->batCacheid);
	return NULL;
}

gdk_return
VARcalciszero(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bit;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (is_bte_nil(v->val.btval))
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.btval);
		break;
	case TYPE_sht:
		if (is_sht_nil(v->val.shval))
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.shval);
		break;
	case TYPE_int:
		if (is_int_nil(v->val.ival))
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.ival);
		break;
	case TYPE_lng:
		if (is_lng_nil(v->val.lval))
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.lval);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (is_hge_nil(v->val.hval))
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.hval);
		break;
#endif
	case TYPE_flt:
		if (is_flt_nil(v->val.fval))
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.fval);
		break;
	case TYPE_dbl:
		if (is_dbl_nil(v->val.dval))
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.dval);
		break;
	default:
		GDKerror("bad input type %s.\n", ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* sign of value (-1 for negative, 0 for 0, +1 for positive; any
 * numeric type) */

#define SIGN(x)		((bte) ((x) < 0 ? -1 : (x) > 0))

BAT *
BATcalcsign(BAT *b, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils = 0;
	BUN i;
	oid x, bhseqbase;
	struct canditer ci;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	bhseqbase = b->hseqbase;
	canditer_init(&ci, b, s);
	if (ci.ncand == 0)
		return BATconstant(ci.hseq, TYPE_bte,
				   ATOMnilptr(TYPE_bte), ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, TYPE_bte, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		UNARY_2TYPE_FUNC(bte, bte, SIGN);
		break;
	case TYPE_sht:
		UNARY_2TYPE_FUNC(sht, bte, SIGN);
		break;
	case TYPE_int:
		UNARY_2TYPE_FUNC(int, bte, SIGN);
		break;
	case TYPE_lng:
		UNARY_2TYPE_FUNC(lng, bte, SIGN);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		UNARY_2TYPE_FUNC(hge, bte, SIGN);
		break;
#endif
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, bte, SIGN);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, bte, SIGN);
		break;
	default:
		GDKerror("bad input type %s.\n", ATOMname(bi.type));
		goto bailout;
	}

	BATsetcount(bn, ci.ncand);

	/* SIGN is ordered if the input is ordered (negative comes
	 * first, positive comes after) and NILs stay in the same
	 * position */
	bn->tsorted = bi.sorted || ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = bi.revsorted || ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bat_iterator_end(&bi);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
bailout:
	bat_iterator_end(&bi);
	BBPunfix(bn->batCacheid);
	return NULL;
}

gdk_return
VARcalcsign(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bte;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (is_bte_nil(v->val.btval))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.btval);
		break;
	case TYPE_sht:
		if (is_sht_nil(v->val.shval))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.shval);
		break;
	case TYPE_int:
		if (is_int_nil(v->val.ival))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.ival);
		break;
	case TYPE_lng:
		if (is_lng_nil(v->val.lval))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.lval);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (is_hge_nil(v->val.hval))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.hval);
		break;
#endif
	case TYPE_flt:
		if (is_flt_nil(v->val.fval))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.fval);
		break;
	case TYPE_dbl:
		if (is_dbl_nil(v->val.dval))
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.dval);
		break;
	default:
		GDKerror("bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* is the value nil (any type) */

#define ISNIL_TYPE(TYPE, NOTNIL)					\
	do {								\
		const TYPE *restrict src = (const TYPE *) bi.base;	\
		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {		\
			x = canditer_next(&ci) - bhseqbase;		\
			dst[i] = (bit) (is_##TYPE##_nil(src[x]) ^ NOTNIL); \
		}							\
		TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
	} while (0)

static BAT *
BATcalcisnil_implementation(BAT *b, BAT *s, bool notnil)
{
	lng t0 = 0;
	BAT *bn;
	BUN i;
	oid x;
	struct canditer ci;
	bit *restrict dst;
	BUN nils = 0;
	oid bhseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	bhseqbase = b->hseqbase;
	canditer_init(&ci, b, s);

	if (b->tnonil || BATtdense(b)) {
		return BATconstant(ci.hseq, TYPE_bit, &(bit){notnil},
				   ci.ncand, TRANSIENT);
	} else if (b->ttype == TYPE_void) {
		/* non-nil handled above */
		assert(is_oid_nil(b->tseqbase));
		return BATconstant(ci.hseq, TYPE_bit, &(bit){!notnil},
				   ci.ncand, TRANSIENT);
	}

	bn = COLnew(ci.hseq, TYPE_bit, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, 0);

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		ISNIL_TYPE(bte, notnil);
		break;
	case TYPE_sht:
		ISNIL_TYPE(sht, notnil);
		break;
	case TYPE_int:
		ISNIL_TYPE(int, notnil);
		break;
	case TYPE_lng:
		ISNIL_TYPE(lng, notnil);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		ISNIL_TYPE(hge, notnil);
		break;
#endif
	case TYPE_flt:
		ISNIL_TYPE(flt, notnil);
		break;
	case TYPE_dbl:
		ISNIL_TYPE(dbl, notnil);
		break;
	case TYPE_uuid:
		ISNIL_TYPE(uuid, notnil);
		break;
	default:
	{
		int (*atomcmp)(const void *, const void *) = ATOMcompare(bi.type);
		const void *nil = ATOMnilptr(bi.type);

		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {
			x = canditer_next(&ci) - bhseqbase;
			dst[i] = (bit) (((*atomcmp)(BUNtail(bi, x), nil) == 0) ^ notnil);
		}
		TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		break;
	}
	}

	BATsetcount(bn, ci.ncand);

	/* If b sorted, all nils are at the start, i.e. bn starts with
	 * 1's and ends with 0's, hence bn is revsorted.  Similarly
	 * for revsorted. At the notnil case, these properties remain the same */
	if (notnil) {
		bn->tsorted = bi.sorted;
		bn->trevsorted = bi.revsorted;
	} else {
		bn->tsorted = bi.revsorted;
		bn->trevsorted = bi.sorted;
	}
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tkey = ci.ncand <= 1;
	bat_iterator_end(&bi);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  ",notnil=%s -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  notnil ? "true" : "false", ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
bailout:
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalcisnil(BAT *b, BAT *s)
{
	return BATcalcisnil_implementation(b, s, false);
}

BAT *
BATcalcisnotnil(BAT *b, BAT *s)
{
	return BATcalcisnil_implementation(b, s, true);
}

gdk_return
VARcalcisnil(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bit;
	ret->val.btval = (bit) VALisnil(v);
	return GDK_SUCCEED;
}

gdk_return
VARcalcisnotnil(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bit;
	ret->val.btval = (bit) !VALisnil(v);
	return GDK_SUCCEED;
}

#define MINMAX_TYPE(TYPE, OP)						\
	do {								\
		TYPE *tb1 = b1i.base, *tb2 = b2i.base, *restrict tbn = Tloc(bn, 0); \
		if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {	\
			TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) { \
				oid x1 = canditer_next_dense(&ci1) - b1hseqbase; \
				oid x2 = canditer_next_dense(&ci2) - b2hseqbase; \
				TYPE p1 = tb1[x1], p2 = tb2[x2];	\
				if (is_##TYPE##_nil(p1) || is_##TYPE##_nil(p2)) { \
					nils = true;			\
					tbn[i] = TYPE##_nil;		\
				} else {				\
					tbn[i] = p1 OP p2 ? p1 : p2;	\
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset,			\
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
		} else {						\
			TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) { \
				oid x1 = canditer_next(&ci1) - b1hseqbase; \
				oid x2 = canditer_next(&ci2) - b2hseqbase; \
				TYPE p1 = tb1[x1], p2 = tb2[x2];	\
				if (is_##TYPE##_nil(p1) || is_##TYPE##_nil(p2)) { \
					nils = true;			\
					tbn[i] = TYPE##_nil;		\
				} else {				\
					tbn[i] = p1 OP p2 ? p1 : p2;	\
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset,			\
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
		}							\
	} while (0)

BAT *
BATcalcmin(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	bool nils = false;
	struct canditer ci1, ci2;
	oid b1hseqbase, b2hseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	b1hseqbase = b1->hseqbase;
	b2hseqbase = b2->hseqbase;
	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("inputs have incompatible types\n");
		return NULL;
	}

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, ATOMtype(b1->ttype), ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	switch (ATOMbasetype(b1i.type)) {
	case TYPE_bte:
		MINMAX_TYPE(bte, <);
		break;
	case TYPE_sht:
		MINMAX_TYPE(sht, <);
		break;
	case TYPE_int:
		MINMAX_TYPE(int, <);
		break;
	case TYPE_lng:
		MINMAX_TYPE(lng, <);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MINMAX_TYPE(hge, <);
		break;
#endif
	case TYPE_flt:
		MINMAX_TYPE(flt, <);
		break;
	case TYPE_dbl:
		MINMAX_TYPE(dbl, <);
		break;
	default: {
		const void *restrict nil = ATOMnilptr(b1i.type);
		int (*cmp)(const void *, const void *) = ATOMcompare(b1i.type);

		if (ATOMvarsized(b1i.type)) {
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next_dense(&ci1) - b1hseqbase;
					oid x2 = canditer_next_dense(&ci2) - b2hseqbase;
					const void *p1 = BUNtvar(b1i, x1);
					const void *p2 = BUNtvar(b2i, x2);
					if (cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
						nils = true;
						p1 = nil;
					} else {
						p1 = cmp(p1, p2) < 0 ? p1 : p2;
					}
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next(&ci1) - b1hseqbase;
					oid x2 = canditer_next(&ci2) - b2hseqbase;
					const void *p1 = BUNtvar(b1i, x1);
					const void *p2 = BUNtvar(b2i, x2);
					if (cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
						nils = true;
						p1 = nil;
					} else {
						p1 = cmp(p1, p2) < 0 ? p1 : p2;
					}
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		} else {
			uint8_t *restrict bcast = (uint8_t *) Tloc(bn, 0);
			uint16_t width = bn->twidth;
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next_dense(&ci1) - b1hseqbase;
					oid x2 = canditer_next_dense(&ci2) - b2hseqbase;
					const void *p1 = BUNtloc(b1i, x1);
					const void *p2 = BUNtloc(b2i, x2);
					if (cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
						nils = true;
						p1 = nil;
					} else {
						p1 = cmp(p1, p2) < 0 ? p1 : p2;
					}
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next(&ci1) - b1hseqbase;
					oid x2 = canditer_next(&ci2) - b2hseqbase;
					const void *p1 = BUNtloc(b1i, x1);
					const void *p2 = BUNtloc(b2i, x2);
					if (cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
						nils = true;
						p1 = nil;
					} else {
						p1 = cmp(p1, p2) < 0 ? p1 : p2;
					}
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		}
	}
	}

	bn->tnil = nils;
	bn->tnonil = !nils;
	BATsetcount(bn, ci1.ncand);
	if (ci1.ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(b1i.type) == TYPE_oid ? ci1.ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
  bailout:
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);
	BBPreclaim(bn);
	return NULL;
}

#define MINMAX_NONIL_TYPE(TYPE, OP)					\
	do {								\
		TYPE *tb1 = b1i.base, *tb2 = b2i.base, *restrict tbn = Tloc(bn, 0); \
		if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {	\
			TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) { \
				oid x1 = canditer_next_dense(&ci1) - b1hseqbase; \
				oid x2 = canditer_next_dense(&ci2) - b2hseqbase; \
				TYPE p1 = tb1[x1], p2 = tb2[x2];	\
				if (is_##TYPE##_nil(p1)) {		\
					if (is_##TYPE##_nil(p2)) {	\
						tbn[i] = TYPE##_nil;	\
						nils = true;		\
					} else {			\
						tbn[i] = p2;		\
					}				\
				} else {				\
					tbn[i] = !is_##TYPE##_nil(p2) && p2 OP p1 ? p2 : p1; \
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset,			\
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
		} else {						\
			TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) { \
				oid x1 = canditer_next(&ci1) - b1hseqbase; \
				oid x2 = canditer_next(&ci2) - b2hseqbase; \
				TYPE p1 = tb1[x1], p2 = tb2[x2];	\
				if (is_##TYPE##_nil(p1)) {		\
					if (is_##TYPE##_nil(p2)) {	\
						tbn[i] = TYPE##_nil;	\
						nils = true;		\
					} else {			\
						tbn[i] = p2;		\
					}				\
				} else {				\
					tbn[i] = !is_##TYPE##_nil(p2) && p2 OP p1 ? p2 : p1; \
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset,			\
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
		}							\
	} while (0)

BAT *
BATcalcmin_no_nil(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	bool nils = false;
	struct canditer ci1, ci2;
	oid b1hseqbase, b2hseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	b1hseqbase = b1->hseqbase;
	b2hseqbase = b2->hseqbase;
	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("inputs have incompatible types\n");
		return NULL;
	}

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, ATOMtype(b1->ttype), ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	switch (ATOMbasetype(b1i.type)) {
	case TYPE_bte:
		MINMAX_NONIL_TYPE(bte, <);
		break;
	case TYPE_sht:
		MINMAX_NONIL_TYPE(sht, <);
		break;
	case TYPE_int:
		MINMAX_NONIL_TYPE(int, <);
		break;
	case TYPE_lng:
		MINMAX_NONIL_TYPE(lng, <);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MINMAX_NONIL_TYPE(hge, <);
		break;
#endif
	case TYPE_flt:
		MINMAX_NONIL_TYPE(flt, <);
		break;
	case TYPE_dbl:
		MINMAX_NONIL_TYPE(dbl, <);
		break;
	default: {
		const void *restrict nil = ATOMnilptr(b1i.type);
		int (*cmp)(const void *, const void *) = ATOMcompare(b1i.type);

		if (ATOMvarsized(b1i.type)) {
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next_dense(&ci1) - b1hseqbase;
					oid x2 = canditer_next_dense(&ci2) - b2hseqbase;
					const void *p1 = BUNtvar(b1i, x1);
					const void *p2 = BUNtvar(b2i, x2);
					if (cmp(p1, nil) == 0) {
						if (cmp(p2, nil) == 0) {
							/* both values are nil */
							nils = true;
						} else {
							p1 = p2;
						}
					} else {
						p1 = cmp(p2, nil) != 0 && cmp(p2, p1) < 0 ? p2 : p1;
					}
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next(&ci1) - b1hseqbase;
					oid x2 = canditer_next(&ci2) - b2hseqbase;
					const void *p1 = BUNtvar(b1i, x1);
					const void *p2 = BUNtvar(b2i, x2);
					if (cmp(p1, nil) == 0) {
						if (cmp(p2, nil) == 0) {
							/* both values are nil */
							nils = true;
						} else {
							p1 = p2;
						}
					} else {
						p1 = cmp(p2, nil) != 0 && cmp(p2, p1) < 0 ? p2 : p1;
					}
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		} else {
			uint8_t *restrict bcast = (uint8_t *) Tloc(bn, 0);
			uint16_t width = bn->twidth;
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next_dense(&ci1) - b1hseqbase;
					oid x2 = canditer_next_dense(&ci2) - b2hseqbase;
					const void *p1 = BUNtloc(b1i, x1);
					const void *p2 = BUNtloc(b2i, x2);
					if (cmp(p1, nil) == 0) {
						if (cmp(p2, nil) == 0) {
							/* both values are nil */
							nils = true;
						} else {
							p1 = p2;
						}
					} else {
						p1 = cmp(p2, nil) != 0 && cmp(p2, p1) < 0 ? p2 : p1;
					}
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next(&ci1) - b1hseqbase;
					oid x2 = canditer_next(&ci2) - b2hseqbase;
					const void *p1 = BUNtloc(b1i, x1);
					const void *p2 = BUNtloc(b2i, x2);
					if (cmp(p1, nil) == 0) {
						if (cmp(p2, nil) == 0) {
							/* both values are nil */
							nils = true;
						} else {
							p1 = p2;
						}
					} else {
						p1 = cmp(p2, nil) != 0 && cmp(p2, p1) < 0 ? p2 : p1;
					}
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		}
	}
	}

	bn->tnil = nils;
	bn->tnonil = !nils;
	BATsetcount(bn, ci1.ncand);
	if (ci1.ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(b1i.type) == TYPE_oid ? ci1.ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
  bailout:
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);
	BBPreclaim(bn);
	return NULL;
}

#define MINMAX_CST_TYPE(TYPE, OP)					\
	do {								\
		TYPE *restrict tb = bi.base, *restrict tbn = Tloc(bn, 0), pp2 = *(TYPE*) p2; \
		TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {	\
			oid x = canditer_next(&ci) - bhseqbase;		\
			TYPE p1 = tb[x];				\
			if (is_##TYPE##_nil(p1)) {			\
				nils = true;				\
				tbn[i] = TYPE##_nil;			\
			} else {					\
				tbn[i] = p1 OP pp2 ? p1 : pp2;		\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset,				\
			      GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
	} while (0)

BAT *
BATcalcmincst(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	bool nils = false;
	struct canditer ci;
	const void *p2;
	const void *restrict nil;
	int (*cmp)(const void *, const void *);
	oid bhseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	nil = ATOMnilptr(b->ttype);
	cmp = ATOMcompare(b->ttype);
	bhseqbase = b->hseqbase;
	if (ATOMtype(b->ttype) != v->vtype) {
		GDKerror("inputs have incompatible types\n");
		return NULL;
	}

	canditer_init(&ci, b, s);
	p2 = VALptr(v);
	if (ci.ncand == 0 ||
		cmp(p2, nil) == 0 ||
		(b->ttype == TYPE_void && is_oid_nil(b->tseqbase)))
		return BATconstantV(ci.hseq, b->ttype, nil, ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, ATOMtype(b->ttype), ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		MINMAX_CST_TYPE(bte, <);
		break;
	case TYPE_sht:
		MINMAX_CST_TYPE(sht, <);
		break;
	case TYPE_int:
		MINMAX_CST_TYPE(int, <);
		break;
	case TYPE_lng:
		MINMAX_CST_TYPE(lng, <);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MINMAX_CST_TYPE(hge, <);
		break;
#endif
	case TYPE_flt:
		MINMAX_CST_TYPE(flt, <);
		break;
	case TYPE_dbl:
		MINMAX_CST_TYPE(dbl, <);
		break;
	default:
		if (ATOMvarsized(bi.type)) {
			TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
				oid x = canditer_next(&ci) - bhseqbase;
				const void *restrict p1 = BUNtvar(bi, x);
				if (cmp(p1, nil) == 0) {
					nils = true;
					p1 = nil;
				} else {
					p1 = cmp(p1, p2) < 0 ? p1 : p2;
				}
				if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
					goto bailout;
				}
			}
			TIMEOUT_CHECK(timeoffset,
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		} else {
			uint8_t *restrict bcast = (uint8_t *) Tloc(bn, 0);
			uint16_t width = bn->twidth;
			TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
				oid x = canditer_next(&ci) - bhseqbase;
				const void *restrict p1 = BUNtloc(bi, x);
				if (cmp(p1, nil) == 0) {
					nils = true;
					p1 = nil;
				} else {
					p1 = cmp(p1, p2) < 0 ? p1 : p2;
				}
				memcpy(bcast, p1, width);
				bcast += width;
			}
			TIMEOUT_CHECK(timeoffset,
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		}
	}
	bat_iterator_end(&bi);

	bn->tnil = nils;
	bn->tnonil = !nils;
	BATsetcount(bn, ci.ncand);
	if (ci.ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(bn->ttype) == TYPE_oid ? ci.ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
  bailout:
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmin(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcmincst(b, v, s);
}

#define MINMAX_NONIL_CST_TYPE(TYPE, OP)					\
	do {								\
		TYPE *restrict tb = bi.base, *restrict tbn = Tloc(bn, 0), pp2 = *(TYPE*) p2; \
		if (is_##TYPE##_nil(pp2)) {				\
			TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) { \
				oid x = canditer_next(&ci) - bhseqbase; \
				TYPE p1 = tb[x];			\
				nils |= is_##TYPE##_nil(p1);		\
				tbn[i] = p1;				\
			}						\
			TIMEOUT_CHECK(timeoffset,			\
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
		} else {						\
			TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) { \
				oid x = canditer_next(&ci) - bhseqbase; \
				TYPE p1 = tb[x];			\
				if (is_##TYPE##_nil(p1)) {		\
					tbn[i] = pp2;			\
				} else {				\
					tbn[i] = p1 OP pp2 ? p1 : pp2;	\
				}					\
			}						\
			TIMEOUT_CHECK(timeoffset,			\
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
		}							\
	} while (0)

BAT *
BATcalcmincst_no_nil(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	bool nils = false;
	struct canditer ci;
	const void *p2;
	const void *restrict nil;
	int (*cmp)(const void *, const void *);
	oid bhseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	nil = ATOMnilptr(b->ttype);
	cmp = ATOMcompare(b->ttype);
	bhseqbase = b->hseqbase;
	if (ATOMtype(b->ttype) != v->vtype) {
		GDKerror("inputs have incompatible types\n");
		return NULL;
	}

	canditer_init(&ci, b, s);
	if (ci.ncand == 0)
		return BATconstantV(ci.hseq, b->ttype, nil, ci.ncand, TRANSIENT);

	p2 = VALptr(v);
	if (b->ttype == TYPE_void &&
		is_oid_nil(b->tseqbase) &&
		is_oid_nil(* (const oid *) p2))
		return BATconstant(ci.hseq, TYPE_void, &oid_nil, ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, ATOMtype(b->ttype), ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		MINMAX_NONIL_CST_TYPE(bte, <);
		break;
	case TYPE_sht:
		MINMAX_NONIL_CST_TYPE(sht, <);
		break;
	case TYPE_int:
		MINMAX_NONIL_CST_TYPE(int, <);
		break;
	case TYPE_lng:
		MINMAX_NONIL_CST_TYPE(lng, <);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MINMAX_NONIL_CST_TYPE(hge, <);
		break;
#endif
	case TYPE_flt:
		MINMAX_NONIL_CST_TYPE(flt, <);
		break;
	case TYPE_dbl:
		MINMAX_NONIL_CST_TYPE(dbl, <);
		break;
	default:
		if (ATOMvarsized(bi.type)) {
			if (cmp(p2, nil) == 0) {
				TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
					oid x = canditer_next(&ci) - bhseqbase;
					const void *restrict p1 = BUNtvar(bi, x);
					nils |= cmp(p1, nil) == 0;
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
					oid x = canditer_next(&ci) - bhseqbase;
					const void *restrict p1 = BUNtvar(bi, x);
					p1 = cmp(p1, nil) == 0 || cmp(p2, p1) < 0 ? p2 : p1;
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		} else {
			uint8_t *restrict bcast = (uint8_t *) Tloc(bn, 0);
			uint16_t width = bn->twidth;
			if (cmp(p2, nil) == 0) {
				TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
					oid x = canditer_next(&ci) - bhseqbase;
					const void *restrict p1 = BUNtloc(bi, x);
					nils |= cmp(p1, nil) == 0;
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
					oid x = canditer_next(&ci) - bhseqbase;
					const void *restrict p1 = BUNtloc(bi, x);
					p1 = cmp(p1, nil) == 0 || cmp(p2, p1) < 0 ? p2 : p1;
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		}
	}
	bat_iterator_end(&bi);

	bn->tnil = nils;
	bn->tnonil = !nils;
	BATsetcount(bn, ci.ncand);
	if (ci.ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(bn->ttype) == TYPE_oid ? ci.ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
  bailout:
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmin_no_nil(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcmincst_no_nil(b, v, s);
}

BAT *
BATcalcmax(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	bool nils = false;
	struct canditer ci1, ci2;
	oid b1hseqbase, b2hseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	b1hseqbase = b1->hseqbase;
	b2hseqbase = b2->hseqbase;
	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("inputs have incompatible types\n");
		return NULL;
	}

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, ATOMtype(b1->ttype), ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	switch (ATOMbasetype(b1i.type)) {
	case TYPE_bte:
		MINMAX_TYPE(bte, >);
		break;
	case TYPE_sht:
		MINMAX_TYPE(sht, >);
		break;
	case TYPE_int:
		MINMAX_TYPE(int, >);
		break;
	case TYPE_lng:
		MINMAX_TYPE(lng, >);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MINMAX_TYPE(hge, >);
		break;
#endif
	case TYPE_flt:
		MINMAX_TYPE(flt, >);
		break;
	case TYPE_dbl:
		MINMAX_TYPE(dbl, >);
		break;
	default: {
		const void *restrict nil = ATOMnilptr(b1i.type);
		int (*cmp)(const void *, const void *) = ATOMcompare(b1i.type);

		if (ATOMvarsized(b1i.type)) {
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next_dense(&ci1) - b1hseqbase;
					oid x2 = canditer_next_dense(&ci2) - b2hseqbase;
					const void *p1 = BUNtvar(b1i, x1);
					const void *p2 = BUNtvar(b2i, x2);
					if (cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
						nils = true;
						p1 = nil;
					} else {
						p1 = cmp(p1, p2) > 0 ? p1 : p2;
					}
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next(&ci1) - b1hseqbase;
					oid x2 = canditer_next(&ci2) - b2hseqbase;
					const void *p1 = BUNtvar(b1i, x1);
					const void *p2 = BUNtvar(b2i, x2);
					if (cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
						nils = true;
						p1 = nil;
					} else {
						p1 = cmp(p1, p2) > 0 ? p1 : p2;
					}
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		} else {
			uint8_t *restrict bcast = (uint8_t *) Tloc(bn, 0);
			uint16_t width = bn->twidth;
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next_dense(&ci1) - b1hseqbase;
					oid x2 = canditer_next_dense(&ci2) - b2hseqbase;
					const void *p1 = BUNtloc(b1i, x1);
					const void *p2 = BUNtloc(b2i, x2);
					if (cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
						nils = true;
						p1 = nil;
					} else {
						p1 = cmp(p1, p2) > 0 ? p1 : p2;
					}
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next(&ci1) - b1hseqbase;
					oid x2 = canditer_next(&ci2) - b2hseqbase;
					const void *p1 = BUNtloc(b1i, x1);
					const void *p2 = BUNtloc(b2i, x2);
					if (cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
						nils = true;
						p1 = nil;
					} else {
						p1 = cmp(p1, p2) > 0 ? p1 : p2;
					}
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		}
	}
	}

	bn->tnil = nils;
	bn->tnonil = !nils;
	BATsetcount(bn, ci1.ncand);
	if (ci1.ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(b1i.type) == TYPE_oid ? ci1.ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
  bailout:
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalcmax_no_nil(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	bool nils = false;
	struct canditer ci1, ci2;
	oid b1hseqbase, b2hseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	b1hseqbase = b1->hseqbase;
	b2hseqbase = b2->hseqbase;
	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("inputs have incompatible types\n");
		return NULL;
	}

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, ATOMtype(b1->ttype), ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	switch (ATOMbasetype(b1i.type)) {
	case TYPE_bte:
		MINMAX_NONIL_TYPE(bte, >);
		break;
	case TYPE_sht:
		MINMAX_NONIL_TYPE(sht, >);
		break;
	case TYPE_int:
		MINMAX_NONIL_TYPE(int, >);
		break;
	case TYPE_lng:
		MINMAX_NONIL_TYPE(lng, >);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MINMAX_NONIL_TYPE(hge, >);
		break;
#endif
	case TYPE_flt:
		MINMAX_NONIL_TYPE(flt, >);
		break;
	case TYPE_dbl:
		MINMAX_NONIL_TYPE(dbl, >);
		break;
	default: {
		const void *restrict nil = ATOMnilptr(b1i.type);
		int (*cmp)(const void *, const void *) = ATOMcompare(b1i.type);

		if (ATOMvarsized(b1i.type)) {
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next_dense(&ci1) - b1hseqbase;
					oid x2 = canditer_next_dense(&ci2) - b2hseqbase;
					const void *p1, *p2;
					p1 = BUNtvar(b1i, x1);
					p2 = BUNtvar(b2i, x2);
					if (cmp(p1, nil) == 0) {
						if (cmp(p2, nil) == 0) {
							/* both values are nil */
							nils = true;
						} else {
							p1 = p2;
						}
					} else {
						p1 = cmp(p2, nil) != 0 && cmp(p2, p1) > 0 ? p2 : p1;
					}
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next(&ci1) - b1hseqbase;
					oid x2 = canditer_next(&ci2) - b2hseqbase;
					const void *p1, *p2;
					p1 = BUNtvar(b1i, x1);
					p2 = BUNtvar(b2i, x2);
					if (cmp(p1, nil) == 0) {
						if (cmp(p2, nil) == 0) {
							/* both values are nil */
							nils = true;
						} else {
							p1 = p2;
						}
					} else {
						p1 = cmp(p2, nil) != 0 && cmp(p2, p1) > 0 ? p2 : p1;
					}
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		} else {
			uint8_t *restrict bcast = (uint8_t *) Tloc(bn, 0);
			uint16_t width = bn->twidth;
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next_dense(&ci1) - b1hseqbase;
					oid x2 = canditer_next_dense(&ci2) - b2hseqbase;
					const void *p1, *p2;
					p1 = BUNtloc(b1i, x1);
					p2 = BUNtloc(b2i, x2);
					if (cmp(p1, nil) == 0) {
						if (cmp(p2, nil) == 0) {
							/* both values are nil */
							nils = true;
						} else {
							p1 = p2;
						}
					} else {
						p1 = cmp(p2, nil) != 0 && cmp(p2, p1) > 0 ? p2 : p1;
					}
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci1.ncand, timeoffset) {
					oid x1 = canditer_next(&ci1) - b1hseqbase;
					oid x2 = canditer_next(&ci2) - b2hseqbase;
					const void *p1, *p2;
					p1 = BUNtloc(b1i, x1);
					p2 = BUNtloc(b2i, x2);
					if (cmp(p1, nil) == 0) {
						if (cmp(p2, nil) == 0) {
							/* both values are nil */
							nils = true;
						} else {
							p1 = p2;
						}
					} else {
						p1 = cmp(p2, nil) != 0 && cmp(p2, p1) > 0 ? p2 : p1;
					}
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		}
	}
	}

	bn->tnil = nils;
	bn->tnonil = !nils;
	BATsetcount(bn, ci1.ncand);
	if (ci1.ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(b1i.type) == TYPE_oid ? ci1.ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
  bailout:
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalcmaxcst(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	bool nils = false;
	struct canditer ci;
	const void *p2;
	const void *restrict nil;
	int (*cmp)(const void *, const void *);
	oid bhseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	nil = ATOMnilptr(b->ttype);
	cmp = ATOMcompare(b->ttype);
	bhseqbase = b->hseqbase;
	if (ATOMtype(b->ttype) != v->vtype) {
		GDKerror("inputs have incompatible types\n");
		return NULL;
	}

	canditer_init(&ci, b, s);
	p2 = VALptr(v);
	if (ci.ncand == 0 ||
		cmp(p2, nil) == 0 ||
		(b->ttype == TYPE_void && is_oid_nil(b->tseqbase)))
		return BATconstantV(ci.hseq, b->ttype, nil, ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, ATOMtype(b->ttype), ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		MINMAX_CST_TYPE(bte, >);
		break;
	case TYPE_sht:
		MINMAX_CST_TYPE(sht, >);
		break;
	case TYPE_int:
		MINMAX_CST_TYPE(int, >);
		break;
	case TYPE_lng:
		MINMAX_CST_TYPE(lng, >);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MINMAX_CST_TYPE(hge, >);
		break;
#endif
	case TYPE_flt:
		MINMAX_CST_TYPE(flt, >);
		break;
	case TYPE_dbl:
		MINMAX_CST_TYPE(dbl, >);
		break;
	default:
		if (ATOMvarsized(bi.type)) {
			TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
				oid x = canditer_next(&ci) - bhseqbase;
				const void *restrict p1 = BUNtvar(bi, x);
				if (cmp(p1, nil) == 0) {
					nils = true;
					p1 = nil;
				} else {
					p1 = cmp(p1, p2) > 0 ? p1 : p2;
				}
				if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
					goto bailout;
				}
			}
			TIMEOUT_CHECK(timeoffset,
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		} else {
			uint8_t *restrict bcast = (uint8_t *) Tloc(bn, 0);
			uint16_t width = bn->twidth;
			TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
				oid x = canditer_next(&ci) - bhseqbase;
				const void *restrict p1 = BUNtloc(bi, x);
				if (cmp(p1, nil) == 0) {
					nils = true;
					p1 = nil;
				} else {
					p1 = cmp(p1, p2) > 0 ? p1 : p2;
				}
				memcpy(bcast, p1, width);
				bcast += width;
			}
			TIMEOUT_CHECK(timeoffset,
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		}
	}
	bat_iterator_end(&bi);

	bn->tnil = nils;
	bn->tnonil = !nils;
	BATsetcount(bn, ci.ncand);
	if (ci.ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(bn->ttype) == TYPE_oid ? ci.ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
  bailout:
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmax(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcmaxcst(b, v, s);
}

BAT *
BATcalcmaxcst_no_nil(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	bool nils = false;
	struct canditer ci;
	const void *p2;
	const void *restrict nil;
	int (*cmp)(const void *, const void *);
	oid bhseqbase;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	nil = ATOMnilptr(b->ttype);
	cmp = ATOMcompare(b->ttype);
	bhseqbase = b->hseqbase;
	if (ATOMtype(b->ttype) != v->vtype) {
		GDKerror("inputs have incompatible types\n");
		return NULL;
	}

	canditer_init(&ci, b, s);
	if (ci.ncand == 0)
		return BATconstantV(ci.hseq, b->ttype, nil, ci.ncand, TRANSIENT);

	cmp = ATOMcompare(b->ttype);
	p2 = VALptr(v);
	if (b->ttype == TYPE_void &&
		is_oid_nil(b->tseqbase) &&
		is_oid_nil(* (const oid *) p2))
		return BATconstant(ci.hseq, TYPE_void, &oid_nil, ci.ncand, TRANSIENT);

	bn = COLnew(ci.hseq, ATOMtype(b->ttype), ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	BATiter bi = bat_iterator(b);
	switch (ATOMbasetype(bi.type)) {
	case TYPE_bte:
		MINMAX_NONIL_CST_TYPE(bte, >);
		break;
	case TYPE_sht:
		MINMAX_NONIL_CST_TYPE(sht, >);
		break;
	case TYPE_int:
		MINMAX_NONIL_CST_TYPE(int, >);
		break;
	case TYPE_lng:
		MINMAX_NONIL_CST_TYPE(lng, >);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MINMAX_NONIL_CST_TYPE(hge, >);
		break;
#endif
	case TYPE_flt:
		MINMAX_NONIL_CST_TYPE(flt, >);
		break;
	case TYPE_dbl:
		MINMAX_NONIL_CST_TYPE(dbl, >);
		break;
	default:
		if (ATOMvarsized(bi.type)) {
			if (cmp(p2, nil) == 0) {
				TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
					oid x = canditer_next(&ci) - bhseqbase;
					const void *restrict p1 = BUNtvar(bi, x);
					nils |= cmp(p1, nil) == 0;
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
					oid x = canditer_next(&ci) - bhseqbase;
					const void *restrict p1 = BUNtvar(bi, x);
					p1 = cmp(p1, nil) == 0 || cmp(p2, p1) > 0 ? p2 : p1;
					if (tfastins_nocheckVAR(bn, i, p1) != GDK_SUCCEED) {
						goto bailout;
					}
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		} else {
			uint8_t *restrict bcast = (uint8_t *) Tloc(bn, 0);
			uint16_t width = bn->twidth;
			if (cmp(p2, nil) == 0) {
				TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
					oid x = canditer_next(&ci) - bhseqbase;
					const void *restrict p1 = BUNtloc(bi, x);
					nils |= cmp(p1, nil) == 0;
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, timeoffset) {
					oid x = canditer_next(&ci) - bhseqbase;
					const void *restrict p1 = BUNtloc(bi, x);
					p1 = cmp(p1, nil) == 0 || cmp(p2, p1) > 0 ? p2 : p1;
					memcpy(bcast, p1, width);
					bcast += width;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			}
		}
	}
	bat_iterator_end(&bi);

	bn->tnil = nils;
	bn->tnonil = !nils;
	BATsetcount(bn, ci.ncand);
	if (ci.ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(bn->ttype) == TYPE_oid ? ci.ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
  bailout:
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmax_no_nil(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcmaxcst_no_nil(b, v, s);
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) exclusive OR */

#define XOR(a, b)	((a) ^ (b))
#define XORBIT(a, b)	(((a) == 0) != ((b) == 0))

static BUN
xor_typeswitchloop(const void *lft, bool incr1,
		   const void *rgt, bool incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   oid candoff1, oid candoff2,
		   bool nonil, const char *func)
{
	BUN i, j, k;
	BUN nils = 0;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bit, bit, bit, XORBIT);
			else
				BINARY_3TYPE_FUNC(bit, bit, bit, XORBIT);
		} else {
			if (nonil)
				BINARY_3TYPE_FUNC_nonil_nilcheck(bte, bte, bte, XOR, ON_OVERFLOW(bte, bte, "XOR"));
			else
				BINARY_3TYPE_FUNC_nilcheck(bte, bte, bte, XOR, ON_OVERFLOW(bte, bte, "XOR"));
		}
		break;
	case TYPE_sht:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil_nilcheck(sht, sht, sht, XOR, ON_OVERFLOW(sht, sht, "XOR"));
		else
			BINARY_3TYPE_FUNC_nilcheck(sht, sht, sht, XOR, ON_OVERFLOW(sht, sht, "XOR"));
		break;
	case TYPE_int:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil_nilcheck(int, int, int, XOR, ON_OVERFLOW(int, int, "XOR"));
		else
			BINARY_3TYPE_FUNC_nilcheck(int, int, int, XOR, ON_OVERFLOW(int, int, "XOR"));
		break;
	case TYPE_lng:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil_nilcheck(lng, lng, lng, XOR, ON_OVERFLOW(lng, lng, "XOR"));
		else
			BINARY_3TYPE_FUNC_nilcheck(lng, lng, lng, XOR, ON_OVERFLOW(lng, lng, "XOR"));
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil_nilcheck(hge, hge, hge, XOR, ON_OVERFLOW(hge, hge, "XOR"));
		else
			BINARY_3TYPE_FUNC_nilcheck(hge, hge, hge, XOR, ON_OVERFLOW(hge, hge, "XOR"));
		break;
#endif
	default:
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return BUN_NONE;
	}

	return nils;
}

BAT *
BATcalcxor(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci1, ci2;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

        bn = COLnew(ci1.hseq, b1->ttype, ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci1.ncand == 0)
		return bn;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	nils = xor_typeswitchloop(b1i.base, true,
				  b2i.base, true,
				  Tloc(bn, 0),
				  b1i.type,
				  &ci1, &ci2,
				  b1->hseqbase, b2->hseqbase,
				  b1i.nonil && b2i.nonil,
				  __func__);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci1.ncand);

	bn->tsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->trevsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->tkey = ci1.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcxorcst(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = xor_typeswitchloop(bi.base, true,
				  VALptr(v), false,
				  Tloc(bn, 0), bi.type,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  b->hseqbase, 0,
				  bi.nonil && ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0,
				  __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstxor(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcxorcst(b, v, s);
}

gdk_return
VARcalcxor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("incompatible input types.\n");
		return GDK_FAIL;
	}

	if (xor_typeswitchloop(VALptr(lft), false,
			       VALptr(rgt), false,
			       VALget(ret), lft->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, false, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) OR */

#define or3(a,b)	((a) == 1 || (b) == 1 ? 1 : is_bit_nil(a) || is_bit_nil(b) ? bit_nil : 0)

#define OR(a, b)	((a) | (b))

static BUN
or_typeswitchloop(const void *lft, bool incr1,
		  const void *rgt, bool incr2,
		  void *restrict dst, int tp,
		  struct canditer *restrict ci1,
		  struct canditer *restrict ci2,
		  oid candoff1, oid candoff2,
		  bool nonil, const char *func)
{
	BUN i = 0, j = 0, k;
	BUN nils = 0;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	/* note, we don't have to check whether the result is equal to
	 * NIL when using bitwise OR: there is only a single bit set in
	 * NIL, which means that at least one of the operands must have
	 * only that single bit set (and the other either also or
	 * no bits set), so that that operand is already NIL */
	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			/* implement tri-Boolean algebra */
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {
				if (incr1)
					i = canditer_next(ci1) - candoff1;
				if (incr2)
					j = canditer_next(ci2) - candoff2;
				bit v1 = ((const bit *) lft)[i];
				bit v2 = ((const bit *) rgt)[j];
				((bit *) dst)[k] = or3(v1, v2);
				nils += is_bit_nil(((bit *) dst)[k]);
			}
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));
		} else {
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bte, OR);
			else
				BINARY_3TYPE_FUNC(bte, bte, bte, OR);
		}
		break;
	case TYPE_sht:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(sht, sht, sht, OR);
		else
			BINARY_3TYPE_FUNC(sht, sht, sht, OR);
		break;
	case TYPE_int:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(int, int, int, OR);
		else
			BINARY_3TYPE_FUNC(int, int, int, OR);
		break;
	case TYPE_lng:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(lng, lng, lng, OR);
		else
			BINARY_3TYPE_FUNC(lng, lng, lng, OR);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(hge, hge, hge, OR);
		else
			BINARY_3TYPE_FUNC(hge, hge, hge, OR);
		break;
#endif
	default:
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return BUN_NONE;
	}

	return nils;
}

BAT *
BATcalcor(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci1, ci2;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci1.ncand == 0)
		return bn;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	nils = or_typeswitchloop(b1i.base, true,
				 b2i.base, true,
				 Tloc(bn, 0),
				 b1i.type,
				 &ci1, &ci2, b1->hseqbase, b2->hseqbase,
				 b1i.nonil && b2i.nonil,
				 __func__);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci1.ncand);

	bn->tsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->trevsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->tkey = ci1.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcorcst(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci, b, s);

	if (b->ttype == TYPE_bit && v->vtype == TYPE_bit && v->val.btval == 1) {
		/* true OR anything (including NIL) equals true */
		return BATconstant(ci.hseq, TYPE_bit, &(bit){1},
				   ci.ncand, TRANSIENT);
	}

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = or_typeswitchloop(bi.base, true,
				 VALptr(v), false,
				 Tloc(bn, 0), bi.type,
				 &ci,
				 &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				 b->hseqbase, 0,
				 bi.nonil && ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0,
				 __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstor(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcorcst(b, v, s);
}

gdk_return
VARcalcor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("incompatible input types.\n");
		return GDK_FAIL;
	}

	if (or_typeswitchloop(VALptr(lft), false,
			      VALptr(rgt), false,
			      VALget(ret), lft->vtype,
			      &(struct canditer){.tpe=cand_dense, .ncand=1},
			      &(struct canditer){.tpe=cand_dense, .ncand=1},
			      0, 0, false, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) exclusive AND */

#define and3(a,b)	((a) == 0 || (b) == 0 ? 0 : is_bit_nil(a) || is_bit_nil(b) ? bit_nil : 1)

#define AND(a, b)	((a) & (b))

static BUN
and_typeswitchloop(const void *lft, bool incr1,
		   const void *rgt, bool incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   oid candoff1, oid candoff2,
		   bool nonil, const char *func)
{
	BUN i = 0, j = 0, k;
	BUN nils = 0;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			/* implement tri-Boolean algebra */
			TIMEOUT_LOOP_IDX(k, ci1->ncand, timeoffset) {
				if (incr1)
					i = canditer_next(ci1) - candoff1;
				if (incr2)
					j = canditer_next(ci2) - candoff2;
				bit v1 = ((const bit *) lft)[i];
				bit v2 = ((const bit *) rgt)[j];
				((bit *) dst)[k] = and3(v1, v2);
				nils += is_bit_nil(((bit *) dst)[k]);
			}
			TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(BUN_NONE));
		} else {
			if (nonil)
				BINARY_3TYPE_FUNC_nonil_nilcheck(bte, bte, bte, AND, ON_OVERFLOW(bte, bte, "AND"));
			else
				BINARY_3TYPE_FUNC_nilcheck(bte, bte, bte, AND, ON_OVERFLOW(bte, bte, "AND"));
		}
		break;
	case TYPE_sht:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil_nilcheck(sht, sht, sht, AND, ON_OVERFLOW(sht, sht, "AND"));
		else
			BINARY_3TYPE_FUNC_nilcheck(sht, sht, sht, AND, ON_OVERFLOW(sht, sht, "AND"));
		break;
	case TYPE_int:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil_nilcheck(int, int, int, AND, ON_OVERFLOW(int, int, "AND"));
		else
			BINARY_3TYPE_FUNC_nilcheck(int, int, int, AND, ON_OVERFLOW(int, int, "AND"));
		break;
	case TYPE_lng:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil_nilcheck(lng, lng, lng, AND, ON_OVERFLOW(lng, lng, "AND"));
		else
			BINARY_3TYPE_FUNC_nilcheck(lng, lng, lng, AND, ON_OVERFLOW(lng, lng, "AND"));
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil_nilcheck(hge, hge, hge, AND, ON_OVERFLOW(hge, hge, "AND"));
		else
			BINARY_3TYPE_FUNC_nilcheck(hge, hge, hge, AND, ON_OVERFLOW(hge, hge, "AND"));
		break;
#endif
	default:
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return BUN_NONE;
	}

	return nils;
}

BAT *
BATcalcand(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci1, ci2;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci1.ncand == 0)
		return bn;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	nils = and_typeswitchloop(b1i.base, true,
				  b2i.base, true,
				  Tloc(bn, 0),
				  b1i.type,
				  &ci1, &ci2, b1->hseqbase, b2->hseqbase,
				  b1i.nonil && b2i.nonil,
				  __func__);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci1.ncand);

	bn->tsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->trevsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->tkey = ci1.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcandcst(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci, b, s);

	if (b->ttype == TYPE_bit && v->vtype == TYPE_bit && v->val.btval == 0) {
		/* false AND anything (including NIL) equals false */
		return BATconstant(ci.hseq, TYPE_bit, &(bit){0},
				   ci.ncand, TRANSIENT);
	}

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = and_typeswitchloop(bi.base, true,
				  VALptr(v), false,
				  Tloc(bn, 0), bi.type,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  b->hseqbase, 0,
				  bi.nonil && ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0,
				  __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstand(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcandcst(b, v, s);
}

gdk_return
VARcalcand(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("incompatible input types.\n");
		return GDK_FAIL;
	}

	if (and_typeswitchloop(VALptr(lft), false,
			       VALptr(rgt), false,
			       VALget(ret), lft->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, false, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* left shift (any integral type) */

#define LSH(a, b)		((a) << (b))

#define SHIFT_CHECK(a, b)	((b) < 0 || (b) >= 8 * (int) sizeof(a))
#define NO_SHIFT_CHECK(a, b)	0

/* In standard C, left shift is undefined if any of the following
 * conditions hold:
 * - right operand is negative or larger or equal to the width of the
 *   left operand;
 * - left operand is negative;
 * - left operand times two-to-the-power of the right operand is not
 *   representable in the (promoted) type of the left operand. */
#define LSH_CHECK(a, b, TYPE)	(SHIFT_CHECK(a, b) || (a) < 0 || (a) > (GDK_##TYPE##_max >> (b)))
#define LSH_CHECK_bte(a, b)	LSH_CHECK(a, b, bte)
#define LSH_CHECK_sht(a, b)	LSH_CHECK(a, b, sht)
#define LSH_CHECK_int(a, b)	LSH_CHECK(a, b, int)
#define LSH_CHECK_lng(a, b)	LSH_CHECK(a, b, lng)

static BUN
lsh_typeswitchloop(const void *lft, int tp1, bool incr1,
		   const void *rgt, int tp2, bool incr2,
		   void *restrict dst,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   oid candoff1, oid candoff2,
		   const char *func)
{
	BUN i, j, k;
	BUN nils = 0;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(bte, bte, bte, LSH,
						LSH_CHECK_bte);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(bte, sht, bte, LSH,
						LSH_CHECK_bte);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(bte, int, bte, LSH,
						LSH_CHECK_bte);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(bte, lng, bte, LSH,
						LSH_CHECK_bte);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(bte, hge, bte, LSH,
						SHIFT_CHECK);
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(sht, bte, sht, LSH,
						LSH_CHECK_sht);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(sht, sht, sht, LSH,
						LSH_CHECK_sht);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(sht, int, sht, LSH,
						LSH_CHECK_sht);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(sht, lng, sht, LSH,
						LSH_CHECK_sht);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(sht, hge, sht, LSH,
						SHIFT_CHECK);
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(int, bte, int, LSH,
						LSH_CHECK_int);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(int, sht, int, LSH,
						LSH_CHECK_int);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(int, int, int, LSH,
						LSH_CHECK_int);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(int, lng, int, LSH,
						LSH_CHECK_int);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(int, hge, int, LSH,
						SHIFT_CHECK);
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(lng, bte, lng, LSH,
						LSH_CHECK_lng);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(lng, sht, lng, LSH,
						LSH_CHECK_lng);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(lng, int, lng, LSH,
						LSH_CHECK_lng);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(lng, lng, lng, LSH,
						LSH_CHECK_lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(lng, hge, lng, LSH,
						SHIFT_CHECK);
			break;
#endif
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(hge, bte, hge, LSH,
						NO_SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(hge, sht, hge, LSH,
						SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(hge, int, hge, LSH,
						SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(hge, lng, hge, LSH,
						SHIFT_CHECK);
			break;
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(hge, hge, hge, LSH,
						SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
#endif
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
  checkfail:
	return BUN_NONE;
}

BAT *
BATcalclsh(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci1, ci2;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci1.ncand == 0)
		return bn;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	nils = lsh_typeswitchloop(b1i.base, b1i.type, true,
				  b2i.base, b2i.type, true,
				  Tloc(bn, 0),
				  &ci1, &ci2, b1->hseqbase, b2->hseqbase,
				  __func__);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci1.ncand);

	bn->tsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->trevsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->tkey = ci1.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalclshcst(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = lsh_typeswitchloop(bi.base, bi.type, true,
				  VALptr(v), v->vtype, false,
				  Tloc(bn, 0),
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  b->hseqbase, 0,
				  __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstlsh(const ValRecord *v, BAT *b, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, v->vtype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = lsh_typeswitchloop(VALptr(v), v->vtype, false,
				  bi.base, bi.type, true,
				  Tloc(bn, 0),
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  &ci,
				  0, b->hseqbase,
				  __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

gdk_return
VARcalclsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = lft->vtype;
	if (lsh_typeswitchloop(VALptr(lft), lft->vtype, false,
			       VALptr(rgt), rgt->vtype, false,
			       VALget(ret),
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* right shift (any integral type) */

#define RSH(a, b)	((a) >> (b))

static BUN
rsh_typeswitchloop(const void *lft, int tp1, bool incr1,
		   const void *rgt, int tp2, bool incr2,
		   void *restrict dst,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   oid candoff1, oid candoff2,
		   const char *restrict func)
{
	BUN i, j, k;
	BUN nils = 0;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(bte, bte, bte, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(bte, sht, bte, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(bte, int, bte, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(bte, lng, bte, RSH,
						SHIFT_CHECK);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(bte, hge, bte, RSH,
						SHIFT_CHECK);
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(sht, bte, sht, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(sht, sht, sht, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(sht, int, sht, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(sht, lng, sht, RSH,
						SHIFT_CHECK);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(sht, hge, sht, RSH,
						SHIFT_CHECK);
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(int, bte, int, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(int, sht, int, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(int, int, int, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(int, lng, int, RSH,
						SHIFT_CHECK);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(int, hge, int, RSH,
						SHIFT_CHECK);
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(lng, bte, lng, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(lng, sht, lng, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(lng, int, lng, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(lng, lng, lng, RSH,
						SHIFT_CHECK);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(lng, hge, lng, RSH,
						SHIFT_CHECK);
			break;
#endif
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(hge, bte, hge, RSH,
						NO_SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(hge, sht, hge, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(hge, int, hge, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(hge, lng, hge, RSH,
						SHIFT_CHECK);
			break;
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(hge, hge, hge, RSH,
						SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
#endif
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
  checkfail:
	return BUN_NONE;
}

BAT *
BATcalcrsh(BAT *b1, BAT *b2, BAT *s1, BAT *s2)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci1, ci2;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	canditer_init(&ci1, b1, s1);
	canditer_init(&ci2, b2, s2);
	if (ci1.ncand != ci2.ncand || ci1.hseq != ci2.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ci1.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci1.ncand == 0)
		return bn;

	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	nils = rsh_typeswitchloop(b1i.base, b1i.type, true,
				  b2i.base, b2i.type, true,
				  Tloc(bn, 0),
				  &ci1, &ci2, b1->hseqbase, b2->hseqbase,
				  __func__);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci1.ncand);

	bn->tsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->trevsorted = ci1.ncand <= 1 || nils == ci1.ncand;
	bn->tkey = ci1.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  ",s1=" ALGOOPTBATFMT ",s2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(s1), ALGOOPTBATPAR(s2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcrshcst(BAT *b, const ValRecord *v, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = rsh_typeswitchloop(bi.base, bi.type, true,
				  VALptr(v), v->vtype, false,
				  Tloc(bn, 0),
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  b->hseqbase, 0,
				  __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalccstrsh(const ValRecord *v, BAT *b, BAT *s)
{
	lng t0 = 0;
	BAT *bn;
	BUN nils;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	canditer_init(&ci, b, s);

	bn = COLnew(ci.hseq, v->vtype, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci.ncand == 0)
		return bn;

	BATiter bi = bat_iterator(b);
	nils = rsh_typeswitchloop(VALptr(v), v->vtype, false,
				  bi.base, bi.type, true,
				  Tloc(bn, 0),
				  &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				  &ci,
				  0, b->hseqbase,
				  __func__);
	bat_iterator_end(&bi);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ci.ncand);

	bn->tsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->trevsorted = ci.ncand <= 1 || nils == ci.ncand;
	bn->tkey = ci.ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

gdk_return
VARcalcrsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = lft->vtype;
	if (rsh_typeswitchloop(VALptr(lft), lft->vtype, false,
			       VALptr(rgt), rgt->vtype, false,
			       VALget(ret),
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       0, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* between (any "linear" type) */

#define LTbte(a,b)	((a) < (b))
#define LTsht(a,b)	((a) < (b))
#define LTint(a,b)	((a) < (b))
#define LTlng(a,b)	((a) < (b))
#define LThge(a,b)	((a) < (b))
#define LToid(a,b)	((a) < (b))
#define LTflt(a,b)	((a) < (b))
#define LTdbl(a,b)	((a) < (b))
#define LTany(a,b)	((*atomcmp)(a, b) < 0)
#define EQbte(a,b)	((a) == (b))
#define EQsht(a,b)	((a) == (b))
#define EQint(a,b)	((a) == (b))
#define EQlng(a,b)	((a) == (b))
#define EQhge(a,b)	((a) == (b))
#define EQoid(a,b)	((a) == (b))
#define EQflt(a,b)	((a) == (b))
#define EQdbl(a,b)	((a) == (b))
#define EQany(a,b)	((*atomcmp)(a, b) == 0)

#define is_any_nil(v)	((v) == NULL || (*atomcmp)((v), nil) == 0)

#define less3(a,b,i,t)	(is_##t##_nil(a) || is_##t##_nil(b) ? bit_nil : LT##t(a, b) || (i && EQ##t(a, b)))
#define grtr3(a,b,i,t)	(is_##t##_nil(a) || is_##t##_nil(b) ? bit_nil : LT##t(b, a) || (i && EQ##t(a, b)))
#define not3(a)		(is_bit_nil(a) ? bit_nil : !(a))

#define between3(v, lo, linc, hi, hinc, TYPE)				\
	and3(grtr3(v, lo, linc, TYPE), less3(v, hi, hinc, TYPE))

#define BETWEEN(v, lo, hi, TYPE)					\
	(is_##TYPE##_nil(v)						\
	 ? nils_false ? 0 : bit_nil					\
	 : (bit) (anti							\
		  ? (symmetric						\
		     ? not3(or3(between3(v, lo, linc, hi, hinc, TYPE),	\
				between3(v, hi, hinc, lo, linc, TYPE)))	\
		     : not3(between3(v, lo, linc, hi, hinc, TYPE)))	\
		  : (symmetric						\
		     ? or3(between3(v, lo, linc, hi, hinc, TYPE),	\
			   between3(v, hi, hinc, lo, linc, TYPE))	\
		     : between3(v, lo, linc, hi, hinc, TYPE))))

#define BETWEEN_LOOP_TYPE(TYPE, canditer_next)				\
	do {								\
		i = j = k = 0;						\
		TIMEOUT_LOOP_IDX(l, ncand, timeoffset) {		\
			if (incr1)					\
				i = canditer_next(ci) - seqbase1;	\
			if (incr2)					\
				j = canditer_next(cilo) - seqbase2;	\
			if (incr3)					\
				k = canditer_next(cihi) - seqbase3;	\
			dst[l] = BETWEEN(((const TYPE *) src)[i],	\
					 ((const TYPE *) lo)[j],	\
					 ((const TYPE *) hi)[k],	\
					 TYPE);				\
			nils += is_bit_nil(dst[l]);			\
		}							\
		TIMEOUT_CHECK(timeoffset,				\
			      GOTO_LABEL_TIMEOUT_HANDLER(bailout));	\
	} while (0)

static BAT *
BATcalcbetween_intern(const void *src, bool incr1, const char *hp1, int wd1,
		      const void *lo, bool incr2, const char *hp2, int wd2,
		      const void *hi, bool incr3, const char *hp3, int wd3,
		      int tp,
		      struct canditer *restrict ci,
		      struct canditer *restrict cilo,
		      struct canditer *restrict cihi,
		      oid seqbase1, oid seqbase2, oid seqbase3,
		      bool symmetric, bool anti,
		      bool linc, bool hinc, bool nils_false, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	BUN i, j, k, l, ncand = ci->ncand;
	bit *restrict dst;
	const void *nil;
	int (*atomcmp)(const void *, const void *);

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	bn = COLnew(ci->hseq, TYPE_bit, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	dst = (bit *) Tloc(bn, 0);

	tp = ATOMbasetype(tp);

	switch (tp) {
	case TYPE_bte:
		if (ci->tpe == cand_dense && cilo->tpe == cand_dense && cihi->tpe == cand_dense)
			BETWEEN_LOOP_TYPE(bte, canditer_next_dense);
		else
			BETWEEN_LOOP_TYPE(bte, canditer_next);
		break;
	case TYPE_sht:
		if (ci->tpe == cand_dense && cilo->tpe == cand_dense && cihi->tpe == cand_dense)
			BETWEEN_LOOP_TYPE(sht, canditer_next_dense);
		else
			BETWEEN_LOOP_TYPE(sht, canditer_next);
		break;
	case TYPE_int:
		if (ci->tpe == cand_dense && cilo->tpe == cand_dense && cihi->tpe == cand_dense)
			BETWEEN_LOOP_TYPE(int, canditer_next_dense);
		else
			BETWEEN_LOOP_TYPE(int, canditer_next);
		break;
	case TYPE_lng:
		if (ci->tpe == cand_dense && cilo->tpe == cand_dense && cihi->tpe == cand_dense)
			BETWEEN_LOOP_TYPE(lng, canditer_next_dense);
		else
			BETWEEN_LOOP_TYPE(lng, canditer_next);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (ci->tpe == cand_dense && cilo->tpe == cand_dense && cihi->tpe == cand_dense)
			BETWEEN_LOOP_TYPE(hge, canditer_next_dense);
		else
			BETWEEN_LOOP_TYPE(hge, canditer_next);
		break;
#endif
	case TYPE_flt:
		if (ci->tpe == cand_dense && cilo->tpe == cand_dense && cihi->tpe == cand_dense)
			BETWEEN_LOOP_TYPE(flt, canditer_next_dense);
		else
			BETWEEN_LOOP_TYPE(flt, canditer_next);
		break;
	case TYPE_dbl:
		if (ci->tpe == cand_dense && cilo->tpe == cand_dense && cihi->tpe == cand_dense)
			BETWEEN_LOOP_TYPE(dbl, canditer_next_dense);
		else
			BETWEEN_LOOP_TYPE(dbl, canditer_next);
		break;
	default:
		assert(tp != TYPE_oid);
		if (!ATOMlinear(tp) ||
		    (atomcmp = ATOMcompare(tp)) == NULL) {
			BBPunfix(bn->batCacheid);
			GDKerror("%s: bad input type %s.\n",
				 func, ATOMname(tp));
			return NULL;
		}
		nil = ATOMnilptr(tp);
		i = j = k = 0;
		TIMEOUT_LOOP_IDX(l, ncand, timeoffset) {
			if (incr1)
				i = canditer_next(ci) - seqbase1;
			if (incr2)
				j = canditer_next(cilo) - seqbase2;
			if (incr3)
				k = canditer_next(cihi) - seqbase3;
			const void *p1, *p2, *p3;
			p1 = hp1
				? (const void *) (hp1 + VarHeapVal(src, i, wd1))
				: (const void *) ((const char *) src + i * wd1);
			p2 = hp2
				? (const void *) (hp2 + VarHeapVal(lo, j, wd2))
				: (const void *) ((const char *) lo + j * wd2);
			p3 = hp3
				? (const void *) (hp3 + VarHeapVal(hi, k, wd3))
				: (const void *) ((const char *) hi + k * wd3);
			dst[l] = BETWEEN(p1, p2, p3, any);
			nils += is_bit_nil(dst[l]);
		}
		TIMEOUT_CHECK(timeoffset,
			      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		break;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
bailout:
	BBPunfix(bn->batCacheid);
	return NULL;
}

BAT *
BATcalcbetween(BAT *b, BAT *lo, BAT *hi, BAT *s, BAT *slo, BAT *shi,
	       bool symmetric, bool linc, bool hinc, bool nils_false, bool anti)
{
	lng t0 = 0;
	BAT *bn;
	struct canditer ci, cilo, cihi;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	BATcheck(lo, NULL);
	BATcheck(hi, NULL);

	canditer_init(&ci, b, s);
	canditer_init(&cilo, lo, slo);
	canditer_init(&cihi, hi, shi);
	if (ci.ncand != cilo.ncand || ci.hseq != cilo.hseq ||
	    ci.ncand != cihi.ncand || ci.hseq != cihi.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	if (BATtvoid(b) &&
	    BATtvoid(lo) &&
	    BATtvoid(hi)) {
		bit res;

		res = BETWEEN(b->tseqbase, lo->tseqbase, hi->tseqbase, oid);
		return BATconstant(ci.hseq, TYPE_bit, &res, ci.ncand,
				   TRANSIENT);
	}

	BATiter bi = bat_iterator(b);
	BATiter loi = bat_iterator(lo);
	BATiter hii = bat_iterator(hi);
	bn = BATcalcbetween_intern(bi.base, 1,
				   bi.vh ? bi.vh->base : NULL,
				   bi.width,
				   loi.base, 1,
				   loi.vh ? loi.vh->base : NULL,
				   loi.width,
				   hii.base, 1,
				   hii.vh ? hii.vh->base : NULL,
				   hii.width,
				   bi.type,
				   &ci, &cilo, &cihi,
				   b->hseqbase, lo->hseqbase, hi->hseqbase,
				   symmetric, anti, linc, hinc,
				   nils_false, __func__);
	bat_iterator_end(&bi);
	bat_iterator_end(&loi);
	bat_iterator_end(&hii);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",lo=" ALGOBATFMT ",hi=" ALGOBATFMT
		  ",s=" ALGOOPTBATFMT ",slo=" ALGOOPTBATFMT ",shi=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOBATPAR(lo), ALGOBATPAR(hi),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(slo), ALGOOPTBATPAR(shi),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcbetweencstcst(BAT *b, const ValRecord *lo, const ValRecord *hi,
		     BAT *s,
		     bool symmetric, bool linc, bool hinc, bool nils_false,
		     bool anti)
{
	lng t0 = 0;
	BAT *bn;
	struct canditer ci;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(lo->vtype) ||
	    ATOMbasetype(b->ttype) != ATOMbasetype(hi->vtype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci, b, s);

	BATiter bi = bat_iterator(b);
	bn = BATcalcbetween_intern(bi.base, 1,
				   bi.vh ? bi.vh->base : NULL,
				   bi.width,
				   VALptr(lo), 0, NULL, 0,
				   VALptr(hi), 0, NULL, 0,
				   bi.type,
				   &ci,
				   &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				   &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				   b->hseqbase, 0, 0, symmetric, anti,
				   linc, hinc, nils_false,
				   __func__);
	bat_iterator_end(&bi);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcbetweenbatcst(BAT *b, BAT *lo, const ValRecord *hi, BAT *s, BAT *slo,
		     bool symmetric, bool linc, bool hinc, bool nils_false,
		     bool anti)
{
	lng t0 = 0;
	BAT *bn;
	struct canditer ci, cilo;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	BATcheck(lo, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(hi->vtype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci, b, s);
	canditer_init(&cilo, lo, slo);
	if (ci.ncand != cilo.ncand || ci.hseq != cilo.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	BATiter bi = bat_iterator(b);
	BATiter loi = bat_iterator(lo);
	bn = BATcalcbetween_intern(bi.base, 1,
				   bi.vh ? bi.vh->base : NULL,
				   bi.width,
				   loi.base, 1,
				   loi.vh ? loi.vh->base : NULL,
				   loi.width,
				   VALptr(hi), 0, NULL, 0,
				   bi.type,
				   &ci,
				   &cilo,
				   &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				   b->hseqbase, lo->hseqbase, 0,
				   symmetric, anti,
				   linc, hinc, nils_false,
				   __func__);
	bat_iterator_end(&bi);
	bat_iterator_end(&loi);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",lo=" ALGOBATFMT
		  ",s=" ALGOOPTBATFMT ",slo=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOBATPAR(lo),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(slo),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcbetweencstbat(BAT *b, const ValRecord *lo, BAT *hi, BAT *s, BAT *shi,
		     bool symmetric, bool linc, bool hinc, bool nils_false,
		     bool anti)
{
	lng t0 = 0;
	BAT *bn;
	struct canditer ci, cihi;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	BATcheck(hi, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(lo->vtype)) {
		GDKerror("incompatible input types.\n");
		return NULL;
	}

	canditer_init(&ci, b, s);
	canditer_init(&cihi, hi, shi);
	if (ci.ncand != cihi.ncand || ci.hseq != cihi.hseq) {
		GDKerror("inputs not the same size.\n");
		return NULL;
	}

	BATiter bi = bat_iterator(b);
	BATiter hii = bat_iterator(hi);
	bn = BATcalcbetween_intern(bi.base, 1,
				   bi.vh ? bi.vh->base : NULL,
				   bi.width,
				   VALptr(lo), 0, NULL, 0,
				   hii.base, 1,
				   hii.vh ? hii.vh->base : NULL,
				   hii.width,
				   bi.type,
				   &ci,
				   &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				   &cihi,
				   b->hseqbase, 0, hi->hseqbase,
				   symmetric, anti,
				   linc, hinc, nils_false,
				   __func__);
	bat_iterator_end(&bi);
	bat_iterator_end(&hii);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",hi=" ALGOBATFMT
		  ",s=" ALGOOPTBATFMT ",shi=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOBATPAR(hi),
		  ALGOOPTBATPAR(s), ALGOOPTBATPAR(shi),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

gdk_return
VARcalcbetween(ValPtr ret, const ValRecord *v, const ValRecord *lo,
	       const ValRecord *hi, bool symmetric, bool linc, bool hinc,
	       bool nils_false, bool anti)
{
	int t;
	int (*atomcmp)(const void *, const void *);
	const void *nil;

	t = v->vtype;
	if (t != lo->vtype || t != hi->vtype) {
		GDKerror("incompatible input types.\n");
		return GDK_FAIL;
	}
	if (!ATOMlinear(t)) {
		GDKerror("non-linear input type.\n");
		return GDK_FAIL;
	}

	t = ATOMbasetype(t);

	ret->vtype = TYPE_bit;
	switch (t) {
	case TYPE_bte:
		ret->val.btval = BETWEEN(v->val.btval, lo->val.btval, hi->val.btval, bte);
		break;
	case TYPE_sht:
		ret->val.btval = BETWEEN(v->val.shval, lo->val.shval, hi->val.shval, sht);
		break;
	case TYPE_int:
		ret->val.btval = BETWEEN(v->val.ival, lo->val.ival, hi->val.ival, int);
		break;
	case TYPE_lng:
		ret->val.btval = BETWEEN(v->val.lval, lo->val.lval, hi->val.lval, lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		ret->val.btval = BETWEEN(v->val.hval, lo->val.hval, hi->val.hval, hge);
		break;
#endif
	case TYPE_flt:
		ret->val.btval = BETWEEN(v->val.fval, lo->val.fval, hi->val.fval, flt);
		break;
	case TYPE_dbl:
		ret->val.btval = BETWEEN(v->val.dval, lo->val.dval, hi->val.dval, dbl);
		break;
	default:
		nil = ATOMnilptr(t);
		atomcmp = ATOMcompare(t);
		ret->val.btval = BETWEEN(VALptr(v), VALptr(lo), VALptr(hi), any);
		break;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* if-then-else (any type) */

#define IFTHENELSELOOP(TYPE)						\
	do {								\
		TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {			\
			if (src[i] && !is_bit_nil(src[i])) {		\
				((TYPE *) dst)[i] = ((TYPE *) col1)[k]; \
			} else {					\
				((TYPE *) dst)[i] = ((TYPE *) col2)[l]; \
			}						\
			k += incr1;					\
			l += incr2;					\
		}							\
		TIMEOUT_CHECK(timeoffset,				\
			      GOTO_LABEL_TIMEOUT_HANDLER(bailout));	\
	} while (0)
#define IFTHENELSELOOP_msk(TYPE)					\
	do {								\
		TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {			\
			if (n == 32) {					\
				n = 0;					\
				mask = src[i / 32];			\
			}						\
			((TYPE *) dst)[i] = mask & (1U << n) ?		\
				((TYPE *) col1)[k] :			\
				((TYPE *) col2)[l];			\
			k += incr1;					\
			l += incr2;					\
			n++;						\
		}							\
		TIMEOUT_CHECK(timeoffset,				\
			      GOTO_LABEL_TIMEOUT_HANDLER(bailout));	\
	} while (0)

static BAT *
BATcalcifthenelse_intern(BATiter *bi,
			 const void *col1, bool incr1, const char *heap1,
			 int width1, bool nonil1, oid seq1,
			 const void *col2, bool incr2, const char *heap2,
			 int width2, bool nonil2, oid seq2,
			 int tpe)
{
	BAT *bn;
	void *restrict dst;
	BUN i, k, l;
	const void *p;
	BUN cnt = bi->count;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	/* col1 and col2 can only be NULL for void columns */
	assert(col1 != NULL || ATOMtype(tpe) == TYPE_oid);
	assert(col2 != NULL || ATOMtype(tpe) == TYPE_oid);
	assert(col1 != NULL || heap1 == NULL);
	assert(col2 != NULL || heap2 == NULL);
	assert(col1 != NULL || incr1 == true);
	assert(col2 != NULL || incr2 == true);

	bn = COLnew(bi->b->hseqbase, ATOMtype(tpe), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (cnt == 0)
		return bn;

	dst = (void *) Tloc(bn, 0);
	k = l = 0;
	if (bn->tvheap) {
		assert((heap1 != NULL && width1 > 0) || (width1 == 0 && incr1 == 0));
		assert((heap2 != NULL && width2 > 0) || (width2 == 0 && incr2 == 0));
		if (ATOMstorage(bi->type) == TYPE_msk) {
			const uint32_t *src = bi->base;
			BUN n = cnt / 32;
			TIMEOUT_LOOP_IDX(i, n + 1, timeoffset) {
				BUN rem = i == n ? cnt % 32 : 32;
				uint32_t mask = rem != 0 ? src[i] : 0;
				for (BUN j = 0; j < rem; j++) {
					if (mask & (1U << j)) {
						if (heap1)
							p = heap1 + VarHeapVal(col1, k, width1);
						else
							p = col1;
					} else {
						if (heap2)
							p = heap2 + VarHeapVal(col2, l, width2);
						else
							p = col2;
					}
					if (tfastins_nocheckVAR(bn, i, p) != GDK_SUCCEED) {
						goto bailout;
					}
					k += incr1;
					l += incr2;
				}
			}
			TIMEOUT_CHECK(timeoffset,
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		} else {
			const bit *src = bi->base;
			TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {
				if (src[i] && !is_bit_nil(src[i])) {
					if (heap1)
						p = heap1 + VarHeapVal(col1, k, width1);
					else
						p = col1;
				} else {
					if (heap2)
						p = heap2 + VarHeapVal(col2, l, width2);
					else
						p = col2;
				}
				if (tfastins_nocheckVAR(bn, i, p) != GDK_SUCCEED) {
					goto bailout;
				}
				k += incr1;
				l += incr2;
			}
			TIMEOUT_CHECK(timeoffset,
				      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		}
	} else {
		assert(heap1 == NULL);
		assert(heap2 == NULL);
		if (ATOMstorage(bi->type) == TYPE_msk) {
			const uint32_t *src = bi->base;
			uint32_t mask = 0;
			BUN n = 32;
			if (ATOMtype(tpe) == TYPE_oid) {
				TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {
					if (n == 32) {
						n = 0;
						mask = src[i / 32];
					}
					((oid *) dst)[i] = mask & (1U << n) ?
						col1 ? ((oid *)col1)[k] : seq1 :
						col2 ? ((oid *)col2)[l] : seq2;
					k += incr1;
					l += incr2;
					seq1 += incr1;
					seq2 += incr2;
					n++;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else if (ATOMstorage(tpe) == TYPE_msk) {
				uint32_t v1, v2;
				if (incr1) {
					v1 = 0;
				} else {
					v1 = * (msk *) col1 ? ~0U : 0U;
				}
				if (incr2) {
					v2 = 0;
				} else {
					v2 = * (msk *) col2 ? ~0U : 0U;
				}
				n = (cnt + 31) / 32;
				TIMEOUT_LOOP_IDX(i, n, timeoffset) {
					if (incr1)
						v1 = ((uint32_t *) col1)[i];
					if (incr2)
						v2 = ((uint32_t *) col2)[i];
					((uint32_t *) dst)[i] = (src[i] & v1)
						| (~src[i] & v2);
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else {
				switch (bn->twidth) {
				case 1:
					IFTHENELSELOOP_msk(bte);
					break;
				case 2:
					IFTHENELSELOOP_msk(sht);
					break;
				case 4:
					IFTHENELSELOOP_msk(int);
					break;
				case 8:
					IFTHENELSELOOP_msk(lng);
					break;
				case 16:
#ifdef HAVE_HGE
					IFTHENELSELOOP_msk(hge);
#else
					IFTHENELSELOOP_msk(uuid);
#endif
					break;
				default:
					TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {
						if (n == 32) {
							n = 0;
							mask = src[i / 32];
						}
						if (mask & (1U << n))
							p = ((const char *) col1) + k * width1;
						else
							p = ((const char *) col2) + l * width2;
						memcpy(dst, p, bn->twidth);
						dst = (void *) ((char *) dst + bn->twidth);
						k += incr1;
						l += incr2;
						n++;
					}
					TIMEOUT_CHECK(timeoffset,
						      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
				}
			}
		} else {
			const bit *src = bi->base;
			if (ATOMtype(tpe) == TYPE_oid) {
				TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {
					if (src[i] && !is_bit_nil(src[i])) {
						((oid *) dst)[i] = col1 ? ((oid *) col1)[k] : seq1;
					} else {
						((oid *) dst)[i] = col2 ? ((oid *) col2)[k] : seq2;
					}
					k += incr1;
					l += incr2;
					seq1 += incr1;
					seq2 += incr2;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			} else if (ATOMstorage(tpe) == TYPE_msk) {
				uint32_t v1, v2;
				uint32_t *d = dst;
				if (incr1) {
					v1 = 0;
				} else {
					v1 = * (msk *) col1 ? ~0U : 0U;
				}
				if (incr2) {
					v2 = 0;
				} else {
					v2 = * (msk *) col2 ? ~0U : 0U;
				}
				i = 0;
				TIMEOUT_LOOP(cnt / 32, timeoffset) {
					uint32_t mask = 0;
					if (incr1)
						v1 = ((uint32_t *) col1)[i/32];
					if (incr2)
						v2 = ((uint32_t *) col2)[i/32];
					for (int n = 0; n < 32; n++) {
						mask |= (uint32_t) (src[i] != 0) << n;
					}
					*d++ = (mask & v1) | (~mask & v2);
					i += 32;
				}
				TIMEOUT_CHECK(timeoffset,
					      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
				/* do the last word */
				if (i < cnt) {
					uint32_t mask = 0;
					if (incr1)
						v1 = ((uint32_t *) col1)[i/32];
					if (incr2)
						v2 = ((uint32_t *) col2)[i/32];
					for (int n = 0; n < 32; n++) {
						mask |= (uint32_t) (src[i] != 0) << n;
						if (++i == cnt)
							break;
					}
					*d++ = (mask & v1) | (~mask & v2);
				}
			} else {
				switch (bn->twidth) {
				case 1:
					IFTHENELSELOOP(bte);
					break;
				case 2:
					IFTHENELSELOOP(sht);
					break;
				case 4:
					IFTHENELSELOOP(int);
					break;
				case 8:
					IFTHENELSELOOP(lng);
					break;
				case 16:
#ifdef HAVE_HGE
					IFTHENELSELOOP(hge);
#else
					IFTHENELSELOOP(uuid);
#endif
					break;
				default:
					TIMEOUT_LOOP_IDX(i, cnt, timeoffset) {
						if (src[i] && !is_bit_nil(src[i])) {
							p = ((const char *) col1) + k * width1;
						} else {
							p = ((const char *) col2) + l * width2;
						}
						memcpy(dst, p, bn->twidth);
						dst = (void *) ((char *) dst + bn->twidth);
						k += incr1;
						l += incr2;
					}
					TIMEOUT_CHECK(timeoffset,
						      GOTO_LABEL_TIMEOUT_HANDLER(bailout));
				}
			}
		}
	}

	BATsetcount(bn, cnt);

	bn->tsorted = cnt <= 1;
	bn->trevsorted = cnt <= 1;
	bn->tkey = cnt <= 1;
	bn->tnil = 0;
	bn->tnonil = nonil1 && nonil2;

	return bn;
bailout:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalcifthenelse(BAT *b, BAT *b1, BAT *b2)
{
	lng t0 = 0;
	BAT *bn;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	BATcheck(b1, NULL);
	BATcheck(b2, NULL);

	BATiter bi = bat_iterator(b);
	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	if (checkbats(&bi, &b1i, __func__) != GDK_SUCCEED ||
	    checkbats(&bi, &b2i, __func__) != GDK_SUCCEED) {
		bat_iterator_end(&bi);
		bat_iterator_end(&b1i);
		bat_iterator_end(&b2i);
		return NULL;
	}
	if (b->ttype != TYPE_bit || ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		bat_iterator_end(&bi);
		bat_iterator_end(&b1i);
		bat_iterator_end(&b2i);
		GDKerror("\"then\" and \"else\" BATs have different types.\n");
		return NULL;
	}
	bn = BATcalcifthenelse_intern(&bi,
				      b1i.base, true, b1i.vh ? b1i.vh->base : NULL, b1i.width, b1i.nonil, b1->tseqbase,
				      b2i.base, true, b2i.vh ? b2i.vh->base : NULL, b2i.width, b2i.nonil, b2->tseqbase,
				      b1i.type);
	bat_iterator_end(&bi);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",b1=" ALGOBATFMT ",b2=" ALGOBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOBATPAR(b1), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcifthenelsecst(BAT *b, BAT *b1, const ValRecord *c2)
{
	lng t0 = 0;
	BAT *bn;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	BATcheck(b1, NULL);
	BATcheck(c2, NULL);

	BATiter bi = bat_iterator(b);
	BATiter b1i = bat_iterator(b1);
	if (checkbats(&bi, &b1i, __func__) != GDK_SUCCEED) {
		bat_iterator_end(&bi);
		bat_iterator_end(&b1i);
		return NULL;
	}
	if (b->ttype != TYPE_bit || ATOMtype(b1->ttype) != ATOMtype(c2->vtype)) {
		bat_iterator_end(&bi);
		bat_iterator_end(&b1i);
		GDKerror("\"then\" and \"else\" BATs have different types.\n");
		return NULL;
	}
	bn = BATcalcifthenelse_intern(&bi,
				      b1i.base, true, b1i.vh ? b1i.vh->base : NULL, b1i.width, b1i.nonil, b1->tseqbase,
				      VALptr(c2), false, NULL, 0, !VALisnil(c2), 0,
				      b1i.type);
	bat_iterator_end(&bi);
	bat_iterator_end(&b1i);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",b1=" ALGOBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOBATPAR(b1),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcifthencstelse(BAT *b, const ValRecord *c1, BAT *b2)
{
	lng t0 = 0;
	BAT *bn;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	BATcheck(c1, NULL);
	BATcheck(b2, NULL);

	BATiter bi = bat_iterator(b);
	BATiter b2i = bat_iterator(b2);
	if (checkbats(&bi, &b2i, __func__) != GDK_SUCCEED) {
		bat_iterator_end(&bi);
		bat_iterator_end(&b2i);
		return NULL;
	}
	if (b->ttype != TYPE_bit || ATOMtype(b2->ttype) != ATOMtype(c1->vtype)) {
		bat_iterator_end(&bi);
		bat_iterator_end(&b2i);
		GDKerror("\"then\" and \"else\" BATs have different types.\n");
		return NULL;
	}
	bn = BATcalcifthenelse_intern(&bi,
				      VALptr(c1), false, NULL, 0, !VALisnil(c1), 0,
				      b2i.base, true, b2i.vh ? b2i.vh->base : NULL, b2i.width, b2i.nonil, b2->tseqbase,
				      c1->vtype);
	bat_iterator_end(&bi);
	bat_iterator_end(&b2i);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",b2=" ALGOBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOBATPAR(b2),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}

BAT *
BATcalcifthencstelsecst(BAT *b, const ValRecord *c1, const ValRecord *c2)
{
	lng t0 = 0;
	BAT *bn;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	BATcheck(c1, NULL);
	BATcheck(c2, NULL);

	if (b->ttype != TYPE_bit || ATOMtype(c1->vtype) != ATOMtype(c2->vtype)) {
		GDKerror("\"then\" and \"else\" BATs have different types.\n");
		return NULL;
	}
	BATiter bi = bat_iterator(b);
	bn = BATcalcifthenelse_intern(&bi,
				      VALptr(c1), false, NULL, 0, !VALisnil(c1), 0,
				      VALptr(c2), false, NULL, 0, !VALisnil(c2), 0,
				      c1->vtype);
	bat_iterator_end(&bi);

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);

	return bn;
}
