/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"
#include <math.h>

#ifndef HAVE_NEXTAFTERF
#define nextafter       _nextafter
#include "mutils.h"             /* nextafterf */
#endif

/* Define symbol FULL_IMPLEMENTATION to get implementations for all
 * sensible output types for +, -, *, /.  Without the symbol, all
 * combinations of input types are supported, but only output types
 * that are either the largest of the input types or one size larger
 * (if available) for +, -, *.  For division the output type can be
 * either input type of flt or dbl. */

/* format strings for the seven/eight basic types we deal with */
#define FMTbte	"%d"
#define FMTsht	"%d"
#define FMTint	"%d"
#define FMTlng	LLFMT
#ifdef HAVE_HGE
#define FMThge	"%.40g"
#endif
#define FMTflt	"%.9g"
#define FMTdbl	"%.17g"
#define FMToid	OIDFMT

/* casts; only required for type hge, since there is no genuine format
 * string for it (i.e., for __int128) (yet?) */
#define CSTbte
#define CSTsht
#define CSTint
#define CSTlng
#ifdef HAVE_HGE
#define CSThge  (dbl)
#endif
#define CSTflt
#define CSTdbl
#define CSToid

#define CMP(a, b)	((bte) (((a) > (b)) - ((a) < (b))))
#define LE(a, b)	((bit) ((a) <= (b)))
#define GE(a, b)	((bit) ((a) >= (b)))
#define EQ(a, b)	((bit) ((a) == (b)))
#define NE(a, b)	((bit) ((a) != (b)))
#define LSH(a, b)	((a) << (b))
#define RSH(a, b)	((a) >> (b))

#define CALC_DECL						\
	BUN start, cnt;						\
	const oid *restrict cand;				\
	oid hseq;		/* seq base for result */	\
	BUN nils;		/* # nils in result */		\
	oid hoff		/* offset */

#define CALC_PREINIT(b1, b2, bn)					\
	do {								\
		if (projected != 0 && s == NULL) {			\
			assert(0);					\
			GDKerror("%s: candidate list missing\n",	\
				 __func__);				\
			return NULL;					\
		}							\
		switch (projected) {					\
		case 0:			/* neither operand was projected */ \
			if (b1->hseqbase != b2->hseqbase ||		\
			    BATcount(b1) != BATcount(b2)) {		\
				GDKerror("%s: operands are not aligned\n", \
					 __func__);			\
				return NULL;				\
			}						\
			bn = b1;					\
			break;						\
		case 1:							\
			bn = b1;					\
			break;						\
		case 2:							\
			bn = b2;					\
			break;						\
		default:						\
			assert(0);					\
			GDKerror("%s: illegal projected argument\n",	\
				 __func__);				\
			return NULL;					\
		}							\
	} while (0)

#define CALC_INIT(b)							\
	do {								\
		start = 0;						\
		cnt = BATcount(b);					\
		cand = NULL;						\
		hoff = hseq = b->hseqbase;				\
		nils = 0;						\
									\
		if (s) {						\
			hseq = s->hseqbase;				\
			if (BATcount(s) == 0) {				\
				cnt = 0;				\
			} else if (BATtdense(s)) {			\
				start = s->tseqbase;			\
				cnt = BATcount(s);			\
				if (start <= b->hseqbase) {		\
					hseq += b->hseqbase - start;	\
					cnt -= b->hseqbase - start;	\
					start = 0;			\
				} else if (start >= b->hseqbase + BATcount(b)) { \
					cnt = 0;			\
					start = 0;			\
				} else {				\
					start -= b->hseqbase;		\
				}					\
				if (start + cnt > BATcount(b))		\
					cnt = BATcount(b) - start;	\
			} else {					\
				oid x = b->hseqbase;			\
									\
				start = SORTfndfirst(s, &x);		\
				hseq += start;				\
				x += BATcount(b);			\
				cnt = SORTfndfirst(s, &x) - start;	\
				if (cnt > 0)				\
					cand = (const oid *) Tloc(s, start); \
				start = 0;				\
			}						\
		}							\
	} while (0)

#define CALC_POSTINIT(b1, b2)						\
	do {								\
		switch (projected) {					\
		case 1:							\
			if (b2->hseqbase != hseq || BATcount(b2) != cnt) { \
				GDKerror("%s: projected operand not "	\
					 "aligned with result\n",	\
					 __func__);			\
				return NULL;				\
			}						\
			break;						\
		case 2:							\
			if (b1->hseqbase != hseq || BATcount(b1) != cnt) { \
				GDKerror("%s: projected operand not "	\
					 "aligned with result\n",	\
					 __func__);			\
				return NULL;				\
			}						\
			break;						\
		default:						\
			break;						\
		}							\
	} while (0)

#define UNARY_2TYPE_FUNC(TYPE1, TYPE2, FUNC)				\
	do {								\
		const TYPE1 *restrict src = (const TYPE1 *) Tloc(b, 0);	\
		TYPE2 *restrict dst = (TYPE2 *) Tloc(bn, 0);		\
		TYPE1 v;						\
		BUN i;							\
									\
		if (b->tnonil) {					\
			if (cand) {					\
				for (i = 0; i < cnt; i++) {		\
					v = src[cand[i] - hoff];	\
					dst[i] = FUNC(v);		\
				}					\
			} else {					\
				for (i = 0; i < cnt; i++) {		\
					v = src[start + i];		\
					dst[i] = FUNC(v);		\
				}					\
			}						\
		} else {						\
			if (cand) {					\
				for (i = 0; i < cnt; i++) {		\
					v = src[cand[i] - hoff];	\
					if (v == TYPE1##_nil) {		\
						nils++;			\
						dst[i] = TYPE2##_nil;	\
					} else {			\
						dst[i] = FUNC(v);	\
					}				\
				}					\
			} else {					\
				for (i = 0; i < cnt; i++) {		\
					v = src[start + i];		\
					if (v == TYPE1##_nil) {		\
						nils++;			\
						dst[i] = TYPE2##_nil;	\
					} else {			\
						dst[i] = FUNC(v);	\
					}				\
				}					\
			}						\
		}							\
	} while (0)

#define UNARY_GENERIC_FUNC(FUNC, cmp, nil)				\
	do {								\
		const void *p, *pd;					\
		BATiter bi = bat_iterator(b);				\
		BUN i;							\
		for (i = 0; i < cnt; i++) {				\
			p = BUNtail(bi, cand ? cand[i] - hoff : start + i); \
			if ((pd = FUNC(p)) == NULL)			\
				break;					\
			tfastins_nocheck(bn, i, pd, Tsize(bn));		\
			nils += (*cmp)(pd, nil) == 0;			\
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC(TYPE1, TYPE2, TYPE3, FUNC)			\
	do {								\
		BUN i;							\
		TYPE1 v1 = v1p ? * (const TYPE1 *) v1p : 0;		\
		TYPE2 v2 = v2p ? * (const TYPE2 *) v2p : 0;		\
		for (i = 0; i < cnt; i++) {				\
			if (src1)					\
				v1 = ((const TYPE1 *) src1)[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
			if (src2)					\
				v2 = ((const TYPE2 *) src2)[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
			if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {	\
				((TYPE3 *) dst)[i] = TYPE3##_nil;	\
				nils++;					\
			} else {					\
				((TYPE3 *) dst)[i] = FUNC(v1, v2);	\
			}						\
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_CHECK(TYPE1, TYPE2, TYPE3, FUNC, CHECK, ON_ERROR) \
	do {								\
		BUN i;							\
		TYPE1 v1 = v1p ? * (const TYPE1 *) v1p : 0;		\
		TYPE2 v2 = v2p ? * (const TYPE2 *) v2p : 0;		\
		for (i = 0; i < cnt; i++) {				\
			if (src1)					\
				v1 = ((const TYPE1 *) src1)[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
			if (src2)					\
				v2 = ((const TYPE2 *) src2)[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
			if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {	\
				((TYPE3 *) dst)[i] = TYPE3##_nil;	\
				nils++;					\
			} else if (CHECK(v1, v2)) {			\
				if (abort_on_error)			\
					ON_ERROR;			\
				((TYPE3 *) dst)[i] = TYPE3##_nil;	\
				nils++;					\
			} else {					\
				((TYPE3 *) dst)[i] = FUNC(v1, v2);	\
			}						\
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_nonil(TYPE1, TYPE2, TYPE3, FUNC)		\
	do {								\
		BUN i;							\
		TYPE1 v1 = v1p ? * (const TYPE1 *) v1p : 0;		\
		TYPE2 v2 = v2p ? * (const TYPE2 *) v2p : 0;		\
		for (i = 0; i < cnt; i++) {				\
			if (src1)					\
				v1 = ((const TYPE1 *) src1)[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
			if (src2)					\
				v2 = ((const TYPE2 *) src2)[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
			((TYPE3 *) dst)[i] = FUNC(v1, v2);		\
		}							\
	} while (0)

#define BINARY_GENERIC_FUNC(FUNC, cmp, nil)				\
	do {								\
		const void *p1, *p2, *pd;				\
		BATiter b1i = bat_iterator(b1);				\
		BATiter b2i = bat_iterator(b2);				\
		BUN i;							\
		for (i = 0; i < cnt; i++) {				\
			p1 = BUNtail(b1i, projected == 1 ? i : cand ? cand[i] - hoff : start + i); \
			p2 = BUNtail(b2i, projected == 2 ? i : cand ? cand[i] - hoff : start + i); \
			if ((pd = FUNC(p1, p2)) == NULL)		\
				break;					\
			tfastins_nocheck(bn, i, pd, Tsize(bn));		\
			nils += (*cmp)(pd, nil) == 0;			\
		}							\
	} while (0)

#define ON_OVERFLOW(TYPE1, TYPE2, OP)				\
	do {							\
		GDKerror("22003!overflow in calculation "	\
			 FMT##TYPE1 OP FMT##TYPE2 ".\n",	\
			 CST##TYPE1 v1, CST##TYPE2 v2);		\
		return BUN_NONE;				\
	} while (0)

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) NOT */

#define NOT(x)		(~(x))
#define NOTBIT(x)	(!(x))

BAT *
BATcalcnot(BAT *b, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
		if (b->ttype == TYPE_bit)
			UNARY_2TYPE_FUNC(bit, bit, NOTBIT);
		else
			UNARY_2TYPE_FUNC(bte, bte, NOT);
		break;
	case TYPE_sht:
		UNARY_2TYPE_FUNC(sht, sht, NOT);
		break;
	case TYPE_int:
		UNARY_2TYPE_FUNC(int, int, NOT);
		break;
	case TYPE_lng:
		UNARY_2TYPE_FUNC(lng, lng, NOT);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		UNARY_2TYPE_FUNC(hge, hge, NOT);
		break;
#endif
	default:
		BBPreclaim(bn);
		GDKerror("%s: type %s not supported.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || (nils == 0 && b->trevsorted);
	bn->trevsorted = cnt <= 1 || (nils == 0 && b->tsorted);
	bn->tkey = cnt <= 1 || b->tkey;

	return bn;
}

gdk_return
VARcalcnot(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (v->val.btval == bit_nil)
			ret->val.btval = bit_nil;
		else if (v->vtype == TYPE_bit)
			ret->val.btval = !v->val.btval;
		else
			ret->val.btval = ~v->val.btval;
		break;
	case TYPE_sht:
		if (v->val.shval == sht_nil)
			ret->val.shval = sht_nil;
		else
			ret->val.shval = ~v->val.shval;
		break;
	case TYPE_int:
		if (v->val.ival == int_nil)
			ret->val.ival = int_nil;
		else
			ret->val.ival = ~v->val.ival;
		break;
	case TYPE_lng:
		if (v->val.lval == lng_nil)
			ret->val.lval = lng_nil;
		else
			ret->val.lval = ~v->val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (v->val.hval == hge_nil)
			ret->val.hval = hge_nil;
		else
			ret->val.hval = ~v->val.hval;
		break;
#endif
	default:
		GDKerror("%s: bad input type %s.\n", __func__,
			 ATOMname(v->vtype));
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
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	switch (ATOMbasetype(b->ttype)) {
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
		BBPreclaim(bn);
		GDKerror("%s: type %s not supported.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || (nils == 0 && b->trevsorted);
	bn->trevsorted = cnt <= 1 || (nils == 0 && b->tsorted);
	bn->tkey = cnt <= 1 || b->tkey;

	return bn;
}

gdk_return
VARcalcnegate(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (v->val.btval == bte_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = -v->val.btval;
		break;
	case TYPE_sht:
		if (v->val.shval == sht_nil)
			ret->val.shval = sht_nil;
		else
			ret->val.shval = -v->val.shval;
		break;
	case TYPE_int:
		if (v->val.ival == int_nil)
			ret->val.ival = int_nil;
		else
			ret->val.ival = -v->val.ival;
		break;
	case TYPE_lng:
		if (v->val.lval == lng_nil)
			ret->val.lval = lng_nil;
		else
			ret->val.lval = -v->val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (v->val.hval == hge_nil)
			ret->val.hval = hge_nil;
		else
			ret->val.hval = -v->val.hval;
		break;
#endif
	case TYPE_flt:
		if (v->val.fval == flt_nil)
			ret->val.fval = flt_nil;
		else
			ret->val.fval = -v->val.fval;
		break;
	case TYPE_dbl:
		if (v->val.dval == dbl_nil)
			ret->val.dval = dbl_nil;
		else
			ret->val.dval = -v->val.dval;
		break;
	default:
		GDKerror("%s: bad input type %s.\n", __func__,
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* absolute value (any numeric type) */

BAT *
BATcalcabsolute(BAT *b, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	switch (ATOMbasetype(b->ttype)) {
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
		BBPreclaim(bn);
		GDKerror("%s: type %s not supported.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, cnt);

	/* ABSOLUTE messes up order (unless all values were negative
	 * or all values were positive, but we don't know anything
	 * about that) */
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1 || b->tkey & 1;

	return bn;
}

gdk_return
VARcalcabsolute(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (v->val.btval == bte_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = (bte) abs(v->val.btval);
		break;
	case TYPE_sht:
		if (v->val.shval == sht_nil)
			ret->val.shval = sht_nil;
		else
			ret->val.shval = (sht) abs(v->val.shval);
		break;
	case TYPE_int:
		if (v->val.ival == int_nil)
			ret->val.ival = int_nil;
		else
			ret->val.ival = abs(v->val.ival);
		break;
	case TYPE_lng:
		if (v->val.lval == lng_nil)
			ret->val.lval = lng_nil;
		else
			ret->val.lval = llabs(v->val.lval);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (v->val.hval == hge_nil)
			ret->val.hval = hge_nil;
		else
			ret->val.hval = ABSOLUTE(v->val.hval);
		break;
#endif
	case TYPE_flt:
		if (v->val.fval == flt_nil)
			ret->val.fval = flt_nil;
		else
			ret->val.fval = fabsf(v->val.fval);
		break;
	case TYPE_dbl:
		if (v->val.dval == dbl_nil)
			ret->val.dval = dbl_nil;
		else
			ret->val.dval = fabs(v->val.dval);
		break;
	default:
		GDKerror("VARcalcabsolute: bad input type %s.\n",
			 ATOMname(v->vtype));
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
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
		UNARY_2TYPE_FUNC(bte, bte, ISZERO);
		break;
	case TYPE_sht:
		UNARY_2TYPE_FUNC(sht, sht, ISZERO);
		break;
	case TYPE_int:
		UNARY_2TYPE_FUNC(int, int, ISZERO);
		break;
	case TYPE_lng:
		UNARY_2TYPE_FUNC(lng, lng, ISZERO);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		UNARY_2TYPE_FUNC(hge, hge, ISZERO);
		break;
#endif
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, flt, ISZERO);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, dbl, ISZERO);
		break;
	default:
		BBPreclaim(bn);
		GDKerror("%s: type %s not supported.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;
}

gdk_return
VARcalciszero(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bit;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (v->val.btval == bte_nil)
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.btval);
		break;
	case TYPE_sht:
		if (v->val.shval == sht_nil)
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.shval);
		break;
	case TYPE_int:
		if (v->val.ival == int_nil)
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.ival);
		break;
	case TYPE_lng:
		if (v->val.lval == lng_nil)
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.lval);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (v->val.hval == hge_nil)
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.hval);
		break;
#endif
	case TYPE_flt:
		if (v->val.fval == flt_nil)
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.fval);
		break;
	case TYPE_dbl:
		if (v->val.dval == dbl_nil)
			ret->val.btval = bit_nil;
		else
			ret->val.btval = ISZERO(v->val.dval);
		break;
	default:
		GDKerror("%s: bad input type %s.\n", __func__,
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* sign of value (-1 for negative, 0 for 0, +1 for positive; any
 * numeric type) */

#define SIGN(x)		CMP((x), 0)

BAT *
BATcalcsign(BAT *b, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, TYPE_bte, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	switch (ATOMbasetype(b->ttype)) {
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
		BBPreclaim(bn);
		GDKerror("%s: type %s not supported.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, cnt);
	/* SIGN is ordered if the input is ordered (negative comes
	 * first, positive comes after) and NILs stay in the same
	 * position */
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = b->tsorted || cnt <= 1 || nils == cnt;
	bn->trevsorted = b->trevsorted || cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;
}

gdk_return
VARcalcsign(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bte;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (v->val.btval == bte_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.btval);
		break;
	case TYPE_sht:
		if (v->val.shval == sht_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.shval);
		break;
	case TYPE_int:
		if (v->val.ival == int_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.ival);
		break;
	case TYPE_lng:
		if (v->val.lval == lng_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.lval);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (v->val.hval == hge_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.hval);
		break;
#endif
	case TYPE_flt:
		if (v->val.fval == flt_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.fval);
		break;
	case TYPE_dbl:
		if (v->val.dval == dbl_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = SIGN(v->val.dval);
		break;
	default:
		GDKerror("VARcalcsign: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* is the value (not) nil (any type) */

#define MAYBENIL_TYPE(TYPE)						\
	do {								\
		const TYPE *restrict src = (const TYPE *) Tloc(b, 0);	\
									\
		if (cand) {						\
			for (i = 0; i < cnt; i++)			\
				dst[i] = (bit) ((src[cand[i] - hoff] == TYPE##_nil) ^ notnil); \
		} else {						\
			for (i = 0; i < cnt; i++)			\
				dst[i] = (bit) ((src[start + i] == TYPE##_nil) ^ notnil); \
		}							\
	} while (0)

static BAT *
BATcalcmaybenil(BAT *b, BAT *s, int notnil)
{
	BAT *bn;
	CALC_DECL;
	bit *restrict dst;
	BUN i;

	CALC_INIT(b);

	(void) nils;		/* not used in this function */

	bn = COLnew(hseq, TYPE_bit, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, 0);

	switch (ATOMbasetype(b->ttype)) {
	default: {
		int (*cmp)(const void *, const void *) = ATOMcompare(b->ttype);
		const void *nil = ATOMnilptr(b->ttype);
		BATiter bi = bat_iterator(b);

		for (i = 0; i < cnt; i++)
			dst[i] = (bit) ((cmp(BUNtail(bi, cand ? cand[i] - hoff : start + i), nil) == 0) ^ notnil);
		break;
	}
	case TYPE_bte:
		MAYBENIL_TYPE(bte);
		break;
	case TYPE_sht:
		MAYBENIL_TYPE(sht);
		break;
	case TYPE_int:
		MAYBENIL_TYPE(int);
		break;
	case TYPE_lng:
		MAYBENIL_TYPE(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MAYBENIL_TYPE(hge);
		break;
#endif
	case TYPE_flt:
		MAYBENIL_TYPE(flt);
		break;
	case TYPE_dbl:
		MAYBENIL_TYPE(dbl);
		break;
	}

	BATsetcount(bn, cnt);

	/* If b sorted, all nils are at the start, i.e. bn starts with
	 * 1's and ends with 0's, hence bn is revsorted.  Similarly
	 * for revsorted. */
	bn->tsorted = b->trevsorted || cnt <= 1;
	bn->trevsorted = b->tsorted || cnt <= 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	bn->tkey = cnt <= 1;

	return bn;
}

BAT *
BATcalcisnil(BAT *b, BAT *s)
{
	return BATcalcmaybenil(b, s, 0);
}

BAT *
BATcalcisnotnil(BAT *b, BAT *s)
{
	return BATcalcmaybenil(b, s, 1);
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

/* ---------------------------------------------------------------------- */
/* the smaller of the values */

#define MINF(p1, p2)	((*cmp)(p1, nil) == 0 || (*cmp)(p2, nil) == 0 ? \
			 nil :						\
			 (*cmp)(p1, p2) <= 0 ? p1 : p2)

/* if either value is nil, the result is nil, otherwise, the result is
 * the smaller of the two values */
BAT *
BATcalcmin(BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;
	int (*cmp)(const void *, const void *);
	const void *nil;

	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}
	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, ATOMtype(b1->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	cmp = ATOMcompare(bn->ttype);
	nil = ATOMnilptr(bn->ttype);

	BINARY_GENERIC_FUNC(MINF, cmp, nil);

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

#define MIN_NO_NIL(p1, p2)	((*cmp)(p1, nil) == 0 ?		\
				 p2 :				\
				 (*cmp)(p2, nil) == 0 ?		\
				 p1 :				\
				 (*cmp)(p1, p2) <= 0 ? p1 : p2)

/* if either value is nil, the result is the other value, otherwise,
 * the result is the smaller of the two values */
BAT *
BATcalcmin_no_nil(BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;
	int (*cmp)(const void *, const void *);
	const void *nil;

	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}
	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, ATOMtype(b1->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	cmp = ATOMcompare(bn->ttype);
	nil = ATOMnilptr(bn->ttype);

	BINARY_GENERIC_FUNC(MIN_NO_NIL, cmp, nil);

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

#define MINCF(p)	((*cmp)(p, nil) == 0 ?		\
			 nil :				\
			 (*cmp)(p, val) < 0 ? p : val)

BAT *
BATcalcmincst(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	CALC_DECL;
	int (*cmp)(const void *, const void *);
	const void *nil;
	const void *val;

	if (ATOMtype(b->ttype) != ATOMtype(v->vtype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}
	CALC_INIT(b);

	cmp = ATOMcompare(b->ttype);
	nil = ATOMnilptr(b->ttype);
	val = VALptr(v);

	if ((b->ttype == TYPE_void && b->tseqbase == oid_nil) ||
	    v->vtype == TYPE_void || (*cmp)(val, nil) == 0)
		return BATconstant(hseq, ATOMtype(b->ttype), nil,
				   cnt, TRANSIENT);

	bn = COLnew(hseq, ATOMtype(b->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	UNARY_GENERIC_FUNC(MINCF, cmp, nil);

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmin(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcmincst(b, v, s);
}

#define MINC_NO_NIL(p)		((*cmp)(p, nil) == 0 ?		\
				 val :				\
				 (*cmp)(p, val) < 0 ? p : val)

BAT *
BATcalcmincst_no_nil(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	CALC_DECL;
	int (*cmp)(const void *, const void *);
	const void *nil;
	const void *val;

	if (ATOMtype(b->ttype) != ATOMtype(v->vtype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}
	CALC_INIT(b);

	cmp = ATOMcompare(b->ttype);
	nil = ATOMnilptr(b->ttype);
	val = VALptr(v);

	if (b->ttype == TYPE_void && b->tseqbase == oid_nil) {
		if (v->vtype == TYPE_void || (*cmp)(val, nil) == 0)
			return BATconstant(hseq, TYPE_void, &oid_nil,
					   cnt, TRANSIENT);
		else
			return BATconstant(hseq, ATOMtype(b->ttype), val,
					   cnt, TRANSIENT);
	}

	if (v->vtype == TYPE_void || (*cmp)(val, nil) == 0) {
		if (s)
			return BATproject(s, b);
		else
			return COLcopy(b, b->ttype, 0, TRANSIENT);
	}

	bn = COLnew(hseq, ATOMtype(b->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	UNARY_GENERIC_FUNC(MINC_NO_NIL, cmp, nil);

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmin_no_nil(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcmincst_no_nil(b, v, s);
}

/* ---------------------------------------------------------------------- */
/* the larger of the values */

#define MAXF(p1, p2)	((*cmp)(p1, nil) == 0 || (*cmp)(p2, nil) == 0 ? \
			 nil :						\
			 (*cmp)(p1, p2) > 0 ? p1 : p2)

/* if either value is nil, the result is nil, otherwise, the result is
 * the larger of the two values */
BAT *
BATcalcmax(BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;
	int (*cmp)(const void *, const void *);
	const void *nil;

	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}
	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, ATOMtype(b1->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	cmp = ATOMcompare(bn->ttype);
	nil = ATOMnilptr(bn->ttype);

	BINARY_GENERIC_FUNC(MAXF, cmp, nil);

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

#define MAX_NO_NIL(p1, p2)	((*cmp)(p1, nil) == 0 ?		\
				 p2 :				\
				 (*cmp)(p2, nil) == 0 ?		\
				 p1 :				\
				 (*cmp)(p1, p2) > 0 ? p1 : p2)

/* if either value is nil, the result is the other value, otherwise,
 * the result is the larger of the two values */
BAT *
BATcalcmax_no_nil(BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;
	int (*cmp)(const void *, const void *);
	const void *nil;

	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}
	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, ATOMtype(b1->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	cmp = ATOMcompare(bn->ttype);
	nil = ATOMnilptr(bn->ttype);

	BINARY_GENERIC_FUNC(MAX_NO_NIL, cmp, nil);

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

#define MAXCF(p)	((*cmp)(p, nil) == 0 ?		\
			 nil :				\
			 (*cmp)(p, val) > 0 ? p : val)

BAT *
BATcalcmaxcst(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	CALC_DECL;
	int (*cmp)(const void *, const void *);
	const void *nil;
	const void *val;

	if (ATOMtype(b->ttype) != ATOMtype(v->vtype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}
	CALC_INIT(b);

	cmp = ATOMcompare(b->ttype);
	nil = ATOMnilptr(b->ttype);
	val = VALptr(v);

	if ((b->ttype == TYPE_void && b->tseqbase == oid_nil) ||
	    v->vtype == TYPE_void || (*cmp)(val, nil) == 0)
		return BATconstant(hseq, ATOMtype(b->ttype), nil,
				   cnt, TRANSIENT);

	bn = COLnew(hseq, ATOMtype(b->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	UNARY_GENERIC_FUNC(MAXCF, cmp, nil);

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmax(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcmaxcst(b, v, s);
}

#define MAXC_NO_NIL(p)		((*cmp)(p, nil) == 0 ?		\
				 val :				\
				 (*cmp)(p, val) > 0 ? p : val)

BAT *
BATcalcmaxcst_no_nil(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	CALC_DECL;
	int (*cmp)(const void *, const void *);
	const void *nil;
	const void *val;

	if (ATOMtype(b->ttype) != ATOMtype(v->vtype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}
	CALC_INIT(b);

	cmp = ATOMcompare(b->ttype);
	nil = ATOMnilptr(b->ttype);
	val = VALptr(v);

	if (b->ttype == TYPE_void && b->tseqbase == oid_nil) {
		if (v->vtype == TYPE_void || (*cmp)(val, nil) == 0)
			return BATconstant(hseq, TYPE_void, &oid_nil,
					   cnt, TRANSIENT);
		else
			return BATconstant(hseq, ATOMtype(b->ttype), val,
					   cnt, TRANSIENT);
	}

	if (v->vtype == TYPE_void || (*cmp)(val, nil) == 0) {
		if (s)
			return BATproject(s, b);
		else
			return COLcopy(b, b->ttype, 0, TRANSIENT);
	}

	bn = COLnew(hseq, ATOMtype(b->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	UNARY_GENERIC_FUNC(MAXC_NO_NIL, cmp, nil);

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmax_no_nil(const ValRecord *v, BAT *b, BAT *s)
{
	return BATcalcmaxcst_no_nil(b, v, s);
}

/* ---------------------------------------------------------------------- */
/* addition (any numeric type) or concatenation (strings) */

#define ADD_3TYPE_enlarge(TYPE1, TYPE2, TYPE3)				\
static BUN								\
add_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
		const TYPE2 *src2, const TYPE2 *v2p,			\
		TYPE3 *restrict dst, const oid *restrict cand,		\
		BUN start, BUN cnt, oid hoff, TYPE3 max,		\
		int projected, int abort_on_error)			\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = v1 + v2;				\
			if (dst[i] < -max || dst[i] > max) {		\
				if (abort_on_error)			\
					ON_OVERFLOW(TYPE1, TYPE2, "+");	\
				dst[i] = TYPE3##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

#define ADD_3TYPE(TYPE1, TYPE2, TYPE3, IF)				\
static BUN								\
add_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
		const TYPE2 *src2, const TYPE2 *v2p,			\
		TYPE3 *restrict dst, const oid *restrict cand,		\
		BUN start, BUN cnt, oid hoff, TYPE3 max,		\
		int projected, int abort_on_error)			\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			ADD##IF##_WITH_CHECK(TYPE1, v1, TYPE2, v2, TYPE3, dst[i], \
					     max, ON_OVERFLOW(TYPE1, TYPE2, "+")); \
		}							\
	}								\
	return nils;							\
}

ADD_3TYPE(bte, bte, bte, I)
ADD_3TYPE_enlarge(bte, bte, sht)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, bte, int)
ADD_3TYPE_enlarge(bte, bte, lng)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, bte, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(bte, bte, flt)
ADD_3TYPE_enlarge(bte, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(bte, sht, sht, I)
ADD_3TYPE_enlarge(bte, sht, int)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, sht, lng)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, sht, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(bte, sht, flt)
ADD_3TYPE_enlarge(bte, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(bte, int, int, I)
ADD_3TYPE_enlarge(bte, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, int, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(bte, int, flt)
ADD_3TYPE_enlarge(bte, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(bte, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, lng, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, lng, flt)
ADD_3TYPE_enlarge(bte, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
#ifdef HAVE_HGE
ADD_3TYPE(bte, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, hge, flt)
ADD_3TYPE_enlarge(bte, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
#endif	/* HAVE_HGE */
ADD_3TYPE(bte, flt, flt, F)
ADD_3TYPE_enlarge(bte, flt, dbl)
ADD_3TYPE(bte, dbl, dbl, F)
ADD_3TYPE(sht, bte, sht, I)
ADD_3TYPE_enlarge(sht, bte, int)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, bte, lng)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, bte, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(sht, bte, flt)
ADD_3TYPE_enlarge(sht, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(sht, sht, sht, I)
ADD_3TYPE_enlarge(sht, sht, int)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, sht, lng)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, sht, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(sht, sht, flt)
ADD_3TYPE_enlarge(sht, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(sht, int, int, I)
ADD_3TYPE_enlarge(sht, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, int, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(sht, int, flt)
ADD_3TYPE_enlarge(sht, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(sht, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, lng, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, lng, flt)
ADD_3TYPE_enlarge(sht, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
#ifdef HAVE_HGE
ADD_3TYPE(sht, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, hge, flt)
ADD_3TYPE_enlarge(sht, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
#endif	/* HAVE_HGE */
ADD_3TYPE(sht, flt, flt, F)
ADD_3TYPE_enlarge(sht, flt, dbl)
ADD_3TYPE(sht, dbl, dbl, F)
ADD_3TYPE(int, bte, int, I)
ADD_3TYPE_enlarge(int, bte, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, bte, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(int, bte, flt)
ADD_3TYPE_enlarge(int, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(int, sht, int, I)
ADD_3TYPE_enlarge(int, sht, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, sht, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(int, sht, flt)
ADD_3TYPE_enlarge(int, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(int, int, int, I)
ADD_3TYPE_enlarge(int, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, int, hge)
#endif	/* HAVE_HGE */
ADD_3TYPE_enlarge(int, int, flt)
ADD_3TYPE_enlarge(int, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(int, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, lng, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(int, lng, flt)
ADD_3TYPE_enlarge(int, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
#ifdef HAVE_HGE
ADD_3TYPE(int, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(int, hge, flt)
ADD_3TYPE_enlarge(int, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
#endif	/* HAVE_HGE */
ADD_3TYPE(int, flt, flt, F)
ADD_3TYPE_enlarge(int, flt, dbl)
ADD_3TYPE(int, dbl, dbl, F)
ADD_3TYPE(lng, bte, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, bte, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, bte, flt)
ADD_3TYPE_enlarge(lng, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(lng, sht, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, sht, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, sht, flt)
ADD_3TYPE_enlarge(lng, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(lng, int, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, int, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, int, flt)
ADD_3TYPE_enlarge(lng, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(lng, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, lng, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, lng, flt)
ADD_3TYPE_enlarge(lng, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
#ifdef HAVE_HGE
ADD_3TYPE(lng, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, hge, flt)
ADD_3TYPE_enlarge(lng, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
#endif	/* HAVE_HGE */
ADD_3TYPE(lng, flt, flt, F)
ADD_3TYPE_enlarge(lng, flt, dbl)
ADD_3TYPE(lng, dbl, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(hge, bte, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, bte, flt)
ADD_3TYPE_enlarge(hge, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(hge, sht, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, sht, flt)
ADD_3TYPE_enlarge(hge, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(hge, int, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, int, flt)
ADD_3TYPE_enlarge(hge, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(hge, lng, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, lng, flt)
ADD_3TYPE_enlarge(hge, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(hge, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, hge, flt)
ADD_3TYPE_enlarge(hge, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
ADD_3TYPE(hge, flt, flt, F)
ADD_3TYPE_enlarge(hge, flt, dbl)
ADD_3TYPE(hge, dbl, dbl, F)
#endif	/* HAVE_HGE */
ADD_3TYPE(flt, bte, flt, F)
ADD_3TYPE_enlarge(flt, bte, dbl)
ADD_3TYPE(flt, sht, flt, F)
ADD_3TYPE_enlarge(flt, sht, dbl)
ADD_3TYPE(flt, int, flt, F)
ADD_3TYPE_enlarge(flt, int, dbl)
ADD_3TYPE(flt, lng, flt, F)
ADD_3TYPE_enlarge(flt, lng, dbl)
#ifdef HAVE_HGE
ADD_3TYPE(flt, hge, flt, F)
ADD_3TYPE_enlarge(flt, hge, dbl)
#endif	/* HAVE_HGE */
ADD_3TYPE(flt, flt, flt, F)
ADD_3TYPE_enlarge(flt, flt, dbl)
ADD_3TYPE(flt, dbl, dbl, F)
ADD_3TYPE(dbl, bte, dbl, F)
ADD_3TYPE(dbl, sht, dbl, F)
ADD_3TYPE(dbl, int, dbl, F)
ADD_3TYPE(dbl, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(dbl, hge, dbl, F)
#endif	/* HAVE_HGE */
ADD_3TYPE(dbl, flt, dbl, F)
ADD_3TYPE(dbl, dbl, dbl, F)

static BUN
addswitch(const void *src1, int tp1, const void *src2, int tp2, void *restrict dst, int tp,
	  const void *v1p, const void *v2p, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int abort_on_error, int projected,
	  const char *func)
{
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				return add_bte_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_bte_max, projected, abort_on_error);
			case TYPE_sht:
				return add_bte_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				return add_bte_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return add_bte_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_bte_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_bte_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_bte_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				return add_bte_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return add_bte_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return add_bte_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_bte_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_bte_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_bte_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return add_bte_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return add_bte_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_bte_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_bte_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_bte_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return add_bte_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_bte_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_bte_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_bte_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return add_bte_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_bte_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_bte_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return add_bte_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_bte_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return add_bte_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_sht:
				return add_sht_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return add_sht_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return add_sht_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_sht_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_sht_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_sht_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				return add_sht_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return add_sht_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return add_sht_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_sht_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_sht_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_sht_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return add_sht_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return add_sht_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_sht_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_sht_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_sht_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return add_sht_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_sht_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_sht_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_sht_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return add_sht_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_sht_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_sht_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return add_sht_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_sht_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return add_sht_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_int:
				return add_int_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return add_int_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_int_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_int_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_int_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				return add_int_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return add_int_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_int_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_int_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_int_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return add_int_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return add_int_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_int_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return add_int_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_int_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return add_int_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_int_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_int_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_int_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return add_int_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_int_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_int_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return add_int_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_int_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return add_int_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_lng:
				return add_lng_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_lng_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_lng_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_lng_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				return add_lng_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_lng_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_lng_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_lng_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				return add_lng_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_lng_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_lng_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_lng_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return add_lng_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return add_lng_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_lng_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_lng_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return add_lng_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_lng_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_lng_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return add_lng_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_lng_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return add_lng_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_hge:
				return add_hge_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
			case TYPE_flt:
#ifdef FULL_IMPLEMENTATION
				return add_hge_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_hge_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				return add_hge_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_hge_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_hge_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				return add_hge_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_hge_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_hge_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				return add_hge_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_hge_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_hge_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return add_hge_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return add_hge_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_hge_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return add_hge_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_hge_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return add_hge_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
#endif	/* HAVE_HGE */
	case TYPE_flt:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_flt:
				return add_flt_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_flt_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				return add_flt_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_flt_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				return add_flt_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_flt_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				return add_flt_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_flt_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				return add_flt_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_flt_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return add_flt_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return add_flt_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return add_flt_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_dbl:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_dbl:
				return add_dbl_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				return add_dbl_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				return add_dbl_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				return add_dbl_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				return add_dbl_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				return add_dbl_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return add_dbl_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	default:
		goto unsupported;
	}

  unsupported:
	GDKerror("%s: type combination (add(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

/* calculate the "addition" (concatenation) of the strings s1 and s2,
 * and store in the buffer buf which is the result value, reallocating
 * if necessary.  If alloc fails, result value is NULL. */
#define ADDSTR(s1, s2)	(strcmp(s1, str_nil) == 0 || strcmp(s2, str_nil) == 0 ? str_nil : (((l1 = strlen(s1)) + (l2 = strlen(s2)) >= slen ? (slen = l1 + l2 + 1024, GDKfree(buf), buf = GDKmalloc(slen)) : buf) ? strcpy(strcpy(buf, s1) + l1, s2) - l1 : NULL))
/* like ADDSTR, but s1 is implicit, constant, and not NIL, and l1 is
 * already calculated */
#define ADDSTR1(s2)	(strcmp(s2, str_nil) == 0 ? str_nil : ((l1 + (l2 = strlen(s2)) >= slen ? (slen = l1 + l2 + 1024, GDKfree(buf), buf = GDKmalloc(slen)) : buf) ? strcpy(strcpy(buf, p1) + l1, s2) - l1 : NULL))
/* like ADDSTR, but s2 is implicit, constant, and not NIL, and l2 is
 * already calculated */
#define ADDSTR2(s1)	(strcmp(s1, str_nil) == 0 ? str_nil : (((l1 = strlen(s1)) + l2 >= slen ? (slen = l1 + l2 + 1024, GDKfree(buf), buf = GDKmalloc(slen)) : buf) ? strcpy(strcpy(buf, s1) + l1, p2) - l1 : NULL))

BAT *
BATcalcadd(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error, int projected)
{
	BAT *bn;
	CALC_DECL;

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	if (tp == TYPE_str) {
		size_t l1, l2, slen = 4096;
		char *buf = GDKmalloc(slen);

		if (b1->ttype != TYPE_str || b2->ttype != TYPE_str) {
			BBPreclaim(bn);
			GDKerror("%s: type combination (add(%s,%s)->%s) not supported.\n",
				 __func__,
				 ATOMname(b1->ttype), ATOMname(b2->ttype), ATOMname(tp));
			return NULL;
		}

		BINARY_GENERIC_FUNC(ADDSTR, strcmp, str_nil);
		if (buf == NULL) {
		  bunins_failed:
			BBPreclaim(bn);
			return NULL;
		}
		GDKfree(buf);
		BATsetcount(bn, cnt);
		bn->tnil = nils != 0;
		bn->tnonil = nils == 0;
		bn->tsorted = cnt <= 1 || nils == cnt;
		bn->trevsorted = cnt <= 1 || nils == cnt;
		bn->tkey = cnt <= 1;
		return bn;
	}

	nils = addswitch(Tloc(b1, 0), b1->ttype, Tloc(b2, 0), b2->ttype, Tloc(bn, 0), tp, NULL, NULL,
			 hoff, start, cnt, cand, abort_on_error, projected,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if both inputs are sorted the same way, and no overflow
	 * occurred (we only know for sure if no nils were produced),
	 * the result is also sorted */
	bn->tsorted = cnt <= 1 || nils == cnt || (b1->tsorted & b2->tsorted && nils == 0);
	bn->trevsorted = cnt <= 1 || nils == cnt || (b1->trevsorted & b2->trevsorted && nils == 0);
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcaddcst(BAT *b, const ValRecord *v2, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	if (tp == TYPE_str) {
		const char *p2;

		if (b->ttype != TYPE_str || v2->vtype != TYPE_str) {
			BBPreclaim(bn);
			GDKerror("%s: type combination (add(%s,%s)->%s) not supported.\n",
				 __func__,
				 ATOMname(b->ttype), ATOMname(v2->vtype), ATOMname(tp));
			return NULL;
		}

		p2 = v2->val.sval;
		if (strcmp(p2, str_nil) == 0) {
			BUN i;

			for (i = 0; i < cnt; i++) {
				tfastins_nocheck(bn, i, str_nil, Tsize(bn));
			}
			nils = cnt;
		} else {
			size_t l1, l2 = strlen(p2), slen = 4096;
			char *buf = GDKmalloc(slen);

			UNARY_GENERIC_FUNC(ADDSTR2, strcmp, str_nil);
			if (buf == NULL)
				goto bunins_failed;
			GDKfree(buf);
		}
		BATsetcount(bn, cnt);
		bn->tnil = nils != 0;
		bn->tnonil = nils == 0;
		bn->tsorted = cnt <= 1 || nils == cnt;
		bn->trevsorted = cnt <= 1 || nils == cnt;
		bn->tkey = cnt <= 1;
		return bn;

	  bunins_failed:
		BBPreclaim(bn);
		return NULL;
	}

	nils = addswitch(Tloc(b, 0), b->ttype, NULL, v2->vtype, Tloc(bn, 0), tp, NULL,
			 VALptr(v2), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if no nils were produced), the result is also
	 * sorted */
	bn->tsorted = cnt <= 1 || nils == cnt || (b->tsorted && nils == 0);
	bn->trevsorted = cnt <= 1 || nils == cnt || (b->trevsorted && nils == 0);
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstadd(const ValRecord *v1, BAT *b, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	if (tp == TYPE_str) {
		const char *p1;

		if (v1->vtype != TYPE_str || b->ttype != TYPE_str) {
			BBPreclaim(bn);
			GDKerror("%s: type combination (add(%s,%s)->%s) not supported.\n",
				 __func__,
				 ATOMname(v1->vtype), ATOMname(b->ttype), ATOMname(tp));
			return NULL;
		}

		p1 = v1->val.sval;
		if (strcmp(p1, str_nil) == 0) {
			BUN i;

			for (i = 0; i < cnt; i++) {
				tfastins_nocheck(bn, i, str_nil, Tsize(bn));
			}
			nils = cnt;
		} else {
			size_t l1 = strlen(p1), l2, slen = 4096;
			char *buf = GDKmalloc(slen);

			UNARY_GENERIC_FUNC(ADDSTR1, strcmp, str_nil);
			if (buf == NULL)
				goto bunins_failed;
			GDKfree(buf);
		}
		BATsetcount(bn, cnt);
		bn->tnil = nils != 0;
		bn->tnonil = nils == 0;
		bn->tsorted = cnt <= 1 || nils == cnt;
		bn->trevsorted = cnt <= 1 || nils == cnt;
		bn->tkey = cnt <= 1;
		return bn;

	  bunins_failed:
		BBPreclaim(bn);
		return NULL;
	}

	nils = addswitch(NULL, v1->vtype, Tloc(b, 0), b->ttype, Tloc(bn, 0), tp, VALptr(v1),
			 NULL, hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if no nils were produced), the result is also
	 * sorted */
	bn->tsorted = cnt <= 1 || nils == cnt || (b->tsorted && nils == 0);
	bn->trevsorted = cnt <= 1 || nils == cnt || (b->trevsorted && nils == 0);
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcadd(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   int abort_on_error)
{

	if (ret->vtype == TYPE_str) {
		char *s;

		if (lft->vtype != TYPE_str || rgt->vtype != TYPE_str) {
			GDKerror("%s: type combination (add(%s,%s)->%s) not supported.\n",
				 __func__,
				 ATOMname(lft->vtype), ATOMname(rgt->vtype), ATOMname(ret->vtype));
			return GDK_FAIL;
		}
		if (strcmp(lft->val.sval, str_nil) == 0 ||
		    strcmp(rgt->val.sval, str_nil) == 0) {
			s = GDKstrdup(str_nil);
		} else {
			size_t l1 = strlen(lft->val.sval);
			s = GDKmalloc(l1 + strlen(rgt->val.sval) + 1);
			if (s)
				strcpy(strcpy(s, lft->val.sval) + l1, rgt->val.sval);
		}
		if (s == NULL)
			return GDK_FAIL;
		VALset(ret, TYPE_str, s);
		return GDK_SUCCEED;
	}

	if (addswitch(NULL, lft->vtype, NULL, rgt->vtype, VALget(ret),
		      ret->vtype, VALptr(lft), VALptr(rgt), 0, 0, 1, NULL,
		      abort_on_error, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

BAT *
BATcalcincr(BAT *b, BAT *s, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;
	bte one = 1;

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = addswitch(Tloc(b, 0), b->ttype, NULL, TYPE_bte, Tloc(bn, 0), bn->ttype, NULL,
			 &one, hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if no nils were produced), the result is also
	 * sorted */
	bn->tsorted = cnt <= 1 || nils == cnt || (b->tsorted && nils == 0);
	bn->trevsorted = cnt <= 1 || nils == cnt || (b->trevsorted && nils == 0);
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcincr(ValPtr ret, const ValRecord *v, int abort_on_error)
{
	bte one = 1;

	if (addswitch(NULL, v->vtype, NULL, TYPE_bte, VALget(ret),
		      ret->vtype, VALptr(v), &one, 0, 0, 1, NULL,
		      abort_on_error, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* subtraction (any numeric type) */

#define SUB_3TYPE_enlarge(TYPE1, TYPE2, TYPE3)				\
static BUN								\
sub_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
		const TYPE2 *src2, const TYPE2 *v2p,			\
		TYPE3 *restrict dst, const oid *restrict cand,		\
		BUN start, BUN cnt, oid hoff, TYPE3 max,		\
		int projected, int abort_on_error)			\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = v1 - v2;				\
			if (dst[i] < -max || dst[i] > max) {		\
				if (abort_on_error)			\
					ON_OVERFLOW(TYPE1, TYPE2, "-");	\
				dst[i] = TYPE3##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

#define SUB_3TYPE(TYPE1, TYPE2, TYPE3, IF)				\
static BUN								\
sub_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
		const TYPE2 *src2, const TYPE2 *v2p,			\
		TYPE3 *restrict dst, const oid *restrict cand,		\
		BUN start, BUN cnt, oid hoff, TYPE3 max,		\
		int projected, int abort_on_error)			\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			SUB##IF##_WITH_CHECK(TYPE1, v1, TYPE2, v2, TYPE3, dst[i], \
					     max, ON_OVERFLOW(TYPE1, TYPE2, "-")); \
		}							\
	}								\
	return nils;							\
}

SUB_3TYPE(bte, bte, bte, I)
SUB_3TYPE_enlarge(bte, bte, sht)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, bte, int)
SUB_3TYPE_enlarge(bte, bte, lng)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, bte, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(bte, bte, flt)
SUB_3TYPE_enlarge(bte, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(bte, sht, sht, I)
SUB_3TYPE_enlarge(bte, sht, int)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, sht, lng)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, sht, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(bte, sht, flt)
SUB_3TYPE_enlarge(bte, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(bte, int, int, I)
SUB_3TYPE_enlarge(bte, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, int, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(bte, int, flt)
SUB_3TYPE_enlarge(bte, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(bte, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, lng, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, lng, flt)
SUB_3TYPE_enlarge(bte, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
#ifdef HAVE_HGE
SUB_3TYPE(bte, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, hge, flt)
SUB_3TYPE_enlarge(bte, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
#endif	/* HAVE_HGE */
SUB_3TYPE(bte, flt, flt, F)
SUB_3TYPE_enlarge(bte, flt, dbl)
SUB_3TYPE(bte, dbl, dbl, F)
SUB_3TYPE(sht, bte, sht, I)
SUB_3TYPE_enlarge(sht, bte, int)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, bte, lng)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, bte, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(sht, bte, flt)
SUB_3TYPE_enlarge(sht, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(sht, sht, sht, I)
SUB_3TYPE_enlarge(sht, sht, int)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, sht, lng)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, sht, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(sht, sht, flt)
SUB_3TYPE_enlarge(sht, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(sht, int, int, I)
SUB_3TYPE_enlarge(sht, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, int, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(sht, int, flt)
SUB_3TYPE_enlarge(sht, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(sht, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, lng, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, lng, flt)
SUB_3TYPE_enlarge(sht, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
#ifdef HAVE_HGE
SUB_3TYPE(sht, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, hge, flt)
SUB_3TYPE_enlarge(sht, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
#endif	/* HAVE_HGE */
SUB_3TYPE(sht, flt, flt, F)
SUB_3TYPE_enlarge(sht, flt, dbl)
SUB_3TYPE(sht, dbl, dbl, F)
SUB_3TYPE(int, bte, int, I)
SUB_3TYPE_enlarge(int, bte, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, bte, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(int, bte, flt)
SUB_3TYPE_enlarge(int, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(int, sht, int, I)
SUB_3TYPE_enlarge(int, sht, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, sht, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(int, sht, flt)
SUB_3TYPE_enlarge(int, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(int, int, int, I)
SUB_3TYPE_enlarge(int, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, int, hge)
#endif	/* HAVE_HGE */
SUB_3TYPE_enlarge(int, int, flt)
SUB_3TYPE_enlarge(int, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(int, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, lng, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(int, lng, flt)
SUB_3TYPE_enlarge(int, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
#ifdef HAVE_HGE
SUB_3TYPE(int, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(int, hge, flt)
SUB_3TYPE_enlarge(int, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
#endif	/* HAVE_HGE */
SUB_3TYPE(int, flt, flt, F)
SUB_3TYPE_enlarge(int, flt, dbl)
SUB_3TYPE(int, dbl, dbl, F)
SUB_3TYPE(lng, bte, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, bte, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, bte, flt)
SUB_3TYPE_enlarge(lng, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(lng, sht, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, sht, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, sht, flt)
SUB_3TYPE_enlarge(lng, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(lng, int, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, int, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, int, flt)
SUB_3TYPE_enlarge(lng, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(lng, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, lng, hge)
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, lng, flt)
SUB_3TYPE_enlarge(lng, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
#ifdef HAVE_HGE
SUB_3TYPE(lng, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, hge, flt)
SUB_3TYPE_enlarge(lng, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
#endif	/* HAVE_HGE */
SUB_3TYPE(lng, flt, flt, F)
SUB_3TYPE_enlarge(lng, flt, dbl)
SUB_3TYPE(lng, dbl, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(hge, bte, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, bte, flt)
SUB_3TYPE_enlarge(hge, bte, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(hge, sht, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, sht, flt)
SUB_3TYPE_enlarge(hge, sht, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(hge, int, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, int, flt)
SUB_3TYPE_enlarge(hge, int, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(hge, lng, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, lng, flt)
SUB_3TYPE_enlarge(hge, lng, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(hge, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, hge, flt)
SUB_3TYPE_enlarge(hge, hge, dbl)
#endif	/* FULL_IMPLEMENTATION */
SUB_3TYPE(hge, flt, flt, F)
SUB_3TYPE_enlarge(hge, flt, dbl)
SUB_3TYPE(hge, dbl, dbl, F)
#endif	/* HAVE_HGE */
SUB_3TYPE(flt, bte, flt, F)
SUB_3TYPE_enlarge(flt, bte, dbl)
SUB_3TYPE(flt, sht, flt, F)
SUB_3TYPE_enlarge(flt, sht, dbl)
SUB_3TYPE(flt, int, flt, F)
SUB_3TYPE_enlarge(flt, int, dbl)
SUB_3TYPE(flt, lng, flt, F)
SUB_3TYPE_enlarge(flt, lng, dbl)
#ifdef HAVE_HGE
SUB_3TYPE(flt, hge, flt, F)
SUB_3TYPE_enlarge(flt, hge, dbl)
#endif	/* HAVE_HGE */
SUB_3TYPE(flt, flt, flt, F)
SUB_3TYPE_enlarge(flt, flt, dbl)
SUB_3TYPE(flt, dbl, dbl, F)
SUB_3TYPE(dbl, bte, dbl, F)
SUB_3TYPE(dbl, sht, dbl, F)
SUB_3TYPE(dbl, int, dbl, F)
SUB_3TYPE(dbl, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(dbl, hge, dbl, F)
#endif	/* HAVE_HGE */
SUB_3TYPE(dbl, flt, dbl, F)
SUB_3TYPE(dbl, dbl, dbl, F)

static BUN
subswitch(const void *src1, int tp1, const void *src2, int tp2, void *restrict dst, int tp,
	  const void *v1p, const void *v2p, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int abort_on_error, int projected,
	  const char *func)
{
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				return sub_bte_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_bte_max, projected, abort_on_error);
			case TYPE_sht:
				return sub_bte_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				return sub_bte_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return sub_bte_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_bte_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_bte_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_bte_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				return sub_bte_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return sub_bte_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return sub_bte_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_bte_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_bte_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_bte_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return sub_bte_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return sub_bte_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_bte_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_bte_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_bte_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return sub_bte_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_bte_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_bte_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_bte_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return sub_bte_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_bte_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_bte_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return sub_bte_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_bte_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return sub_bte_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_sht:
				return sub_sht_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return sub_sht_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return sub_sht_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_sht_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_sht_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_sht_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				return sub_sht_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return sub_sht_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return sub_sht_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_sht_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_sht_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_sht_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return sub_sht_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return sub_sht_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_sht_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_sht_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_sht_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return sub_sht_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_sht_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_sht_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_sht_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return sub_sht_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_sht_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_sht_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return sub_sht_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_sht_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return sub_sht_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_int:
				return sub_int_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return sub_int_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_int_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_int_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_int_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				return sub_int_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return sub_int_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_int_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_int_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_int_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return sub_int_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return sub_int_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_int_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return sub_int_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_int_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return sub_int_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_int_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_int_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_int_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return sub_int_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_int_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_int_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return sub_int_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_int_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return sub_int_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_lng:
				return sub_lng_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_lng_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_lng_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_lng_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				return sub_lng_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_lng_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_lng_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_lng_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				return sub_lng_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_lng_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_lng_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_lng_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return sub_lng_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return sub_lng_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_lng_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_lng_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return sub_lng_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_lng_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_lng_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return sub_lng_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_lng_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return sub_lng_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_hge:
				return sub_hge_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
			case TYPE_flt:
#ifdef FULL_IMPLEMENTATION
				return sub_hge_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_hge_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				return sub_hge_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_hge_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_hge_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				return sub_hge_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_hge_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_hge_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				return sub_hge_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_hge_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_hge_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return sub_hge_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return sub_hge_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_hge_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return sub_hge_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_hge_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return sub_hge_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
#endif	/* HAVE_HGE */
	case TYPE_flt:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_flt:
				return sub_flt_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_flt_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				return sub_flt_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_flt_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				return sub_flt_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_flt_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				return sub_flt_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_flt_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				return sub_flt_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_flt_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return sub_flt_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return sub_flt_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return sub_flt_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_dbl:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_dbl:
				return sub_dbl_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				return sub_dbl_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				return sub_dbl_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				return sub_dbl_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				return sub_dbl_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				return sub_dbl_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return sub_dbl_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	default:
		goto unsupported;
	}

  unsupported:
	GDKerror("%s: type combination (sub(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcsub(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error, int projected)
{
	BAT *bn;
	CALC_DECL;

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = subswitch(Tloc(b1, 0), b1->ttype, Tloc(b2, 0), b2->ttype, Tloc(bn, 0), tp, NULL, NULL,
			 hoff, start, cnt, cand, abort_on_error, projected,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcsubcst(BAT *b, const ValRecord *v2, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = subswitch(Tloc(b, 0), b->ttype, NULL, v2->vtype, Tloc(bn, 0), tp, NULL,
			 VALptr(v2), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if no nils were produced), the result is also
	 * sorted */
	bn->tsorted = cnt <= 1 || nils == cnt || (b->tsorted && nils == 0);
	bn->trevsorted = cnt <= 1 || nils == cnt || (b->trevsorted && nils == 0);
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstsub(const ValRecord *v1, BAT *b, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = subswitch(NULL, v1->vtype, Tloc(b, 0), b->ttype, Tloc(bn, 0), tp, VALptr(v1),
			 NULL, hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if no nils were produced), the result is also
	 * sorted in the opposite direction */
	bn->tsorted = cnt <= 1 || nils == cnt || (b->trevsorted && nils == 0);
	bn->trevsorted = cnt <= 1 || nils == cnt || (b->tsorted && nils == 0);
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcsub(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   int abort_on_error)
{

	if (subswitch(NULL, lft->vtype, NULL, rgt->vtype, VALget(ret),
		      ret->vtype, VALptr(lft), VALptr(rgt), 0, 0, 1, NULL,
		      abort_on_error, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

BAT *
BATcalcdecr(BAT *b, BAT *s, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;
	bte one = 1;

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = subswitch(Tloc(b, 0), b->ttype, NULL, TYPE_bte, Tloc(bn, 0), bn->ttype, NULL,
			 &one, hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if no nils were produced), the result is also
	 * sorted */
	bn->tsorted = cnt <= 1 || nils == cnt || (b->tsorted && nils == 0);
	bn->trevsorted = cnt <= 1 || nils == cnt || (b->trevsorted && nils == 0);
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcdecr(ValPtr ret, const ValRecord *v, int abort_on_error)
{
	bte one = 1;

	if (subswitch(NULL, v->vtype, NULL, TYPE_bte, VALget(ret),
		      ret->vtype, VALptr(v), &one, 0, 0, 1, NULL,
		      abort_on_error, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* multiplication (any numeric type) */

#define MUL_4TYPE(TYPE1, TYPE2, TYPE3, TYPE4)				\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
		const TYPE2 *src2, const TYPE2 *v2p,			\
		TYPE3 *restrict dst, const oid *restrict cand,		\
		BUN start, BUN cnt, oid hoff, TYPE3 max,		\
		int projected, int abort_on_error)			\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			MULI4_WITH_CHECK(TYPE1, v1, TYPE2, v2, TYPE3, dst[i], max, TYPE4, ON_OVERFLOW(TYPE1, TYPE2, "*")); \
		}							\
	}								\
	return nils;							\
}

#define MUL_3TYPE_enlarge(TYPE1, TYPE2, TYPE3)				\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
		const TYPE2 *src2, const TYPE2 *v2p,			\
		TYPE3 *restrict dst, const oid *restrict cand,		\
		BUN start, BUN cnt, oid hoff, TYPE3 max,		\
		int projected, int abort_on_error)					\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (TYPE3) v1 * v2;			\
			if (dst[i] < -max || dst[i] > max) {		\
				if (abort_on_error)			\
					ON_OVERFLOW(TYPE1, TYPE2, "*");	\
				dst[i] = TYPE3##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

#define MUL_2TYPE_lng(TYPE1, TYPE2)					\
static BUN								\
mul_##TYPE1##_##TYPE2##_lng(const TYPE1 *src1, const TYPE1 *v1p,	\
			    const TYPE2 *src2, const TYPE2 *v2p,	\
			    lng *restrict dst, const oid *restrict cand, \
			    BUN start, BUN cnt, oid hoff, lng max,	\
			    int projected, int abort_on_error)		\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = lng_nil;				\
			nils++;						\
		} else {						\
			LNGMUL_CHECK(TYPE1, v1, TYPE2, v2, dst[i], max, \
				     ON_OVERFLOW(TYPE1, TYPE2, "*"));	\
		}							\
	}								\
	return nils;							\
}

#ifdef HAVE_HGE
#define MUL_2TYPE_hge(TYPE1, TYPE2)					\
static BUN								\
mul_##TYPE1##_##TYPE2##_hge(const TYPE1 *src1, const TYPE1 *v1p,	\
			    const TYPE2 *src2, const TYPE2 *v2p,	\
			    hge *restrict dst, const oid *restrict cand, \
			    BUN start, BUN cnt, oid hoff, hge max,	\
			    int projected, int abort_on_error)		\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = hge_nil;				\
			nils++;						\
		} else {						\
			HGEMUL_CHECK(TYPE1, v1, TYPE2, v2, dst[i], max, \
				     ON_OVERFLOW(TYPE1, TYPE2, "*"));	\
		}							\
	}								\
	return nils;							\
}
#endif	/* HAVE_HGE */

#define MUL_3TYPE_float(TYPE1, TYPE2, TYPE3)				\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
		const TYPE2 *src2, const TYPE2 *v2p,			\
		TYPE3 *restrict dst, const oid *restrict cand,		\
		BUN start, BUN cnt, oid hoff, TYPE3 max,		\
			    int projected, int abort_on_error)		\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			FLTDBLMUL_CHECK(TYPE1, v1, TYPE2, v2, TYPE3, dst[i], \
					max, ON_OVERFLOW(TYPE1, TYPE2, "*")); \
		}							\
	}								\
	return nils;							\
}

MUL_4TYPE(bte, bte, bte, sht)
MUL_3TYPE_enlarge(bte, bte, sht)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, bte, int)
MUL_3TYPE_enlarge(bte, bte, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, bte, hge)
#endif
MUL_3TYPE_enlarge(bte, bte, flt)
MUL_3TYPE_enlarge(bte, bte, dbl)
#endif
MUL_4TYPE(bte, sht, sht, int)
MUL_3TYPE_enlarge(bte, sht, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, sht, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, sht, hge)
#endif
MUL_3TYPE_enlarge(bte, sht, flt)
MUL_3TYPE_enlarge(bte, sht, dbl)
#endif
MUL_4TYPE(bte, int, int, lng)
MUL_3TYPE_enlarge(bte, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, int, hge)
#endif
MUL_3TYPE_enlarge(bte, int, flt)
MUL_3TYPE_enlarge(bte, int, dbl)
#endif
MUL_2TYPE_lng(bte, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, lng, hge)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, lng, flt)
MUL_3TYPE_enlarge(bte, lng, dbl)
#endif
#ifdef HAVE_HGE
MUL_2TYPE_hge(bte, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, hge, flt)
MUL_3TYPE_enlarge(bte, hge, dbl)
#endif
#endif
MUL_3TYPE_float(bte, flt, flt)
MUL_3TYPE_enlarge(bte, flt, dbl)
MUL_3TYPE_float(bte, dbl, dbl)
MUL_4TYPE(sht, bte, sht, int)
MUL_3TYPE_enlarge(sht, bte, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, bte, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, bte, hge)
#endif
MUL_3TYPE_enlarge(sht, bte, flt)
MUL_3TYPE_enlarge(sht, bte, dbl)
#endif
MUL_4TYPE(sht, sht, sht, int)
MUL_3TYPE_enlarge(sht, sht, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, sht, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, sht, hge)
#endif
MUL_3TYPE_enlarge(sht, sht, flt)
MUL_3TYPE_enlarge(sht, sht, dbl)
#endif
MUL_4TYPE(sht, int, int, lng)
MUL_3TYPE_enlarge(sht, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, int, hge)
#endif
MUL_3TYPE_enlarge(sht, int, flt)
MUL_3TYPE_enlarge(sht, int, dbl)
#endif
MUL_2TYPE_lng(sht, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, lng, hge)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, lng, flt)
MUL_3TYPE_enlarge(sht, lng, dbl)
#endif
#ifdef HAVE_HGE
MUL_2TYPE_hge(sht, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, hge, flt)
MUL_3TYPE_enlarge(sht, hge, dbl)
#endif
#endif
MUL_3TYPE_float(sht, flt, flt)
MUL_3TYPE_enlarge(sht, flt, dbl)
MUL_3TYPE_float(sht, dbl, dbl)
MUL_4TYPE(int, bte, int, lng)
MUL_3TYPE_enlarge(int, bte, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, bte, hge)
#endif
MUL_3TYPE_enlarge(int, bte, flt)
MUL_3TYPE_enlarge(int, bte, dbl)
#endif
MUL_4TYPE(int, sht, int, lng)
MUL_3TYPE_enlarge(int, sht, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, sht, hge)
#endif
MUL_3TYPE_enlarge(int, sht, flt)
MUL_3TYPE_enlarge(int, sht, dbl)
#endif
MUL_4TYPE(int, int, int, lng)
MUL_3TYPE_enlarge(int, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, int, hge)
#endif
MUL_3TYPE_enlarge(int, int, flt)
MUL_3TYPE_enlarge(int, int, dbl)
#endif
MUL_2TYPE_lng(int, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, lng, hge)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(int, lng, flt)
MUL_3TYPE_enlarge(int, lng, dbl)
#endif
#ifdef HAVE_HGE
MUL_2TYPE_hge(int, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(int, hge, flt)
MUL_3TYPE_enlarge(int, hge, dbl)
#endif
#endif
MUL_3TYPE_float(int, flt, flt)
MUL_3TYPE_enlarge(int, flt, dbl)
MUL_3TYPE_float(int, dbl, dbl)
MUL_2TYPE_lng(lng, bte)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, bte, hge)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, bte, flt)
MUL_3TYPE_enlarge(lng, bte, dbl)
#endif
MUL_2TYPE_lng(lng, sht)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, sht, hge)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, sht, flt)
MUL_3TYPE_enlarge(lng, sht, dbl)
#endif
MUL_2TYPE_lng(lng, int)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, int, hge)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, int, flt)
MUL_3TYPE_enlarge(lng, int, dbl)
#endif
MUL_2TYPE_lng(lng, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, lng, hge)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, lng, flt)
MUL_3TYPE_enlarge(lng, lng, dbl)
#endif
#ifdef HAVE_HGE
MUL_2TYPE_hge(lng, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, hge, flt)
MUL_3TYPE_enlarge(lng, hge, dbl)
#endif
#endif
MUL_3TYPE_float(lng, flt, flt)
MUL_3TYPE_enlarge(lng, flt, dbl)
MUL_3TYPE_float(lng, dbl, dbl)
#ifdef HAVE_HGE
MUL_2TYPE_hge(hge, bte)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, bte, flt)
MUL_3TYPE_enlarge(hge, bte, dbl)
#endif
MUL_2TYPE_hge(hge, sht)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, sht, flt)
MUL_3TYPE_enlarge(hge, sht, dbl)
#endif
MUL_2TYPE_hge(hge, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, int, flt)
MUL_3TYPE_enlarge(hge, int, dbl)
#endif
MUL_2TYPE_hge(hge, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, lng, flt)
MUL_3TYPE_enlarge(hge, lng, dbl)
#endif
MUL_2TYPE_hge(hge, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, hge, flt)
MUL_3TYPE_enlarge(hge, hge, dbl)
#endif
MUL_3TYPE_float(hge, flt, flt)
MUL_3TYPE_enlarge(hge, flt, dbl)
MUL_3TYPE_float(hge, dbl, dbl)
#endif
MUL_3TYPE_float(flt, bte, flt)
MUL_3TYPE_enlarge(flt, bte, dbl)
MUL_3TYPE_float(flt, sht, flt)
MUL_3TYPE_enlarge(flt, sht, dbl)
MUL_3TYPE_float(flt, int, flt)
MUL_3TYPE_enlarge(flt, int, dbl)
MUL_3TYPE_float(flt, lng, flt)
MUL_3TYPE_enlarge(flt, lng, dbl)
#ifdef HAVE_HGE
MUL_3TYPE_float(flt, hge, flt)
MUL_3TYPE_enlarge(flt, hge, dbl)
#endif
MUL_3TYPE_float(flt, flt, flt)
MUL_3TYPE_enlarge(flt, flt, dbl)
MUL_3TYPE_float(flt, dbl, dbl)
MUL_3TYPE_float(dbl, bte, dbl)
MUL_3TYPE_float(dbl, sht, dbl)
MUL_3TYPE_float(dbl, int, dbl)
MUL_3TYPE_float(dbl, lng, dbl)
#ifdef HAVE_HGE
MUL_3TYPE_float(dbl, hge, dbl)
#endif
MUL_3TYPE_float(dbl, flt, dbl)
MUL_3TYPE_float(dbl, dbl, dbl)

static BUN
mulswitch(const void *src1, int tp1, const void *src2, int tp2, void *restrict dst, int tp,
	  const void *v1p, const void *v2p, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int abort_on_error, int projected,
	  const char *func)
{
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				return mul_bte_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_bte_max, projected, abort_on_error);
			case TYPE_sht:
				return mul_bte_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				return mul_bte_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return mul_bte_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_bte_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_bte_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_bte_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				return mul_bte_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return mul_bte_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return mul_bte_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_bte_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_bte_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_bte_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return mul_bte_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return mul_bte_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_bte_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_bte_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_bte_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return mul_bte_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_bte_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_bte_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_bte_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:

				return mul_bte_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_bte_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_bte_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return mul_bte_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_bte_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return mul_bte_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_sht:
				return mul_sht_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return mul_sht_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return mul_sht_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_sht_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_sht_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_sht_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				return mul_sht_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
			case TYPE_int:
				return mul_sht_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				return mul_sht_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_sht_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_sht_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_sht_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return mul_sht_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return mul_sht_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_sht_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_sht_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_sht_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return mul_sht_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_sht_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_sht_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_sht_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return mul_sht_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_sht_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_sht_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return mul_sht_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_sht_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return mul_sht_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_int:
				return mul_int_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return mul_int_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_int_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_int_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_int_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				return mul_int_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return mul_int_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_int_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_int_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_int_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				return mul_int_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
			case TYPE_lng:
				return mul_int_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_int_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
			case TYPE_flt:
				return mul_int_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_int_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return mul_int_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_int_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_int_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_int_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return mul_int_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_int_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_int_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return mul_int_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_int_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return mul_int_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_lng:
				return mul_lng_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_lng_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_lng_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_lng_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				return mul_lng_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_lng_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_lng_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_lng_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				return mul_lng_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_lng_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_lng_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_lng_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				return mul_lng_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
#ifdef HAVE_HGE
			case TYPE_hge:
				return mul_lng_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#endif	/* HAVE_HGE */
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_lng_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_lng_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return mul_lng_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_lng_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_lng_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return mul_lng_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_lng_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return mul_lng_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_hge:
				return mul_hge_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
			case TYPE_flt:
#ifdef FULL_IMPLEMENTATION
				return mul_hge_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_hge_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				return mul_hge_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_hge_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_hge_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				return mul_hge_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_hge_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_hge_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				return mul_hge_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_hge_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_hge_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				return mul_hge_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				return mul_hge_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_hge_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
#endif	/* FULL_IMPLEMENTATION */
			default:
				goto unsupported;
			}
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return mul_hge_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_hge_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return mul_hge_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
#endif	/* HAVE_HGE */
	case TYPE_flt:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_flt:
				return mul_flt_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_flt_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				return mul_flt_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_flt_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				return mul_flt_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_flt_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				return mul_flt_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_flt_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				return mul_flt_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_flt_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				return mul_flt_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
			case TYPE_dbl:
				return mul_flt_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return mul_flt_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	case TYPE_dbl:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_dbl:
				return mul_dbl_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				return mul_dbl_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				return mul_dbl_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				return mul_dbl_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				return mul_dbl_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				return mul_dbl_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				return mul_dbl_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
			default:
				goto unsupported;
			}
		default:
			goto unsupported;
		}
	default:
		goto unsupported;
	}

  unsupported:
	GDKerror("%s: type combination (mul(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcmul(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error, int projected)
{
	BAT *bn;
	CALC_DECL;

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = mulswitch(Tloc(b1, 0), b1->ttype, Tloc(b2, 0), b2->ttype, Tloc(bn, 0), tp, NULL, NULL,
			 hoff, start, cnt, cand, abort_on_error, projected,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcmulcst(BAT *b, const ValRecord *v2, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = mulswitch(Tloc(b, 0), b->ttype, NULL, v2->vtype, Tloc(bn, 0), tp, NULL,
			 VALptr(v2), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if no nils were produced), the result is also
	 * sorted, or reverse sorted if the constant is negative */
	if (cnt <= 1 || cnt == nils) {
		bn->tsorted = bn->trevsorted = 1;
	} else if (nils == 0) {
		ValRecord sign;

		VARcalcsign(&sign, v2);
		bn->tsorted = sign.val.btval == 0 ||
			(sign.val.btval > 0 && b->tsorted) ||
			(sign.val.btval < 0 && b->trevsorted);
		bn->trevsorted = sign.val.btval == 0 ||
			(sign.val.btval > 0 && b->trevsorted) ||
			(sign.val.btval < 0 && b->tsorted);
	} else {
		bn->tsorted = bn->trevsorted = 0;
	}
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstmul(const ValRecord *v1, BAT *b, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = mulswitch(NULL, v1->vtype, Tloc(b, 0), b->ttype, Tloc(bn, 0), tp, VALptr(v1),
			 NULL, hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if no nils were produced), the result is also
	 * sorted, or reverse sorted if the constant is negative */
	if (cnt <= 1 || cnt == nils) {
		bn->tsorted = bn->trevsorted = 1;
	} else if (nils == 0) {
		ValRecord sign;

		VARcalcsign(&sign, v1);
		bn->tsorted = sign.val.btval == 0 ||
			(sign.val.btval > 0 && b->tsorted) ||
			(sign.val.btval < 0 && b->trevsorted);
		bn->trevsorted = sign.val.btval == 0 ||
			(sign.val.btval > 0 && b->trevsorted) ||
			(sign.val.btval < 0 && b->tsorted);
	} else {
		bn->tsorted = bn->trevsorted = 0;
	}
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcmul(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   int abort_on_error)
{

	if (mulswitch(NULL, lft->vtype, NULL, rgt->vtype, VALget(ret),
		      ret->vtype, VALptr(lft), VALptr(rgt), 0, 0, 1, NULL,
		      abort_on_error, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* division (any numeric type) */

#define DIV_3TYPE_I(TYPE1, TYPE2, TYPE3)				\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
				const TYPE2 *src2, const TYPE2 *v2p,	\
				TYPE3 *restrict dst, const oid *restrict cand, \
				BUN start, BUN cnt, oid hoff, TYPE3 max, \
				int projected, int abort_on_error)	\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else if (v2 == 0) {					\
			if (abort_on_error)				\
				return BUN_NONE + 1;			\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (TYPE3) (v1 / v2);			\
			if (dst[i] < -max || dst[i] > max) {		\
				if (abort_on_error)			\
					ON_OVERFLOW(TYPE1, TYPE2, "/");	\
				dst[i] = TYPE3##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

#define DIV_3TYPE_IF(TYPE1, TYPE2, TYPE3)				\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
				const TYPE2 *src2, const TYPE2 *v2p,	\
				TYPE3 *restrict dst, const oid *restrict cand, \
				BUN start, BUN cnt, oid hoff, TYPE3 max, \
				int projected, int abort_on_error)	\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else if (v2 == 0) {					\
			if (abort_on_error)				\
				return BUN_NONE + 1;			\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (TYPE3) v1 / v2;			\
			if (dst[i] < -max || dst[i] > max) {		\
				if (abort_on_error)			\
					ON_OVERFLOW(TYPE1, TYPE2, "/");	\
				dst[i] = TYPE3##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

#define DIV_3TYPE_float(TYPE1, TYPE2, TYPE3)				\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
				const TYPE2 *src2, const TYPE2 *v2p,	\
				TYPE3 *restrict dst, const oid *restrict cand, \
				BUN start, BUN cnt, oid hoff, TYPE3 max, \
				int projected, int abort_on_error)	\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else if (v2 == 0) {					\
			if (abort_on_error)				\
				return BUN_NONE + 1;			\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else if ((v2 > -1 && v2 < 1 &&			\
			    GDK_##TYPE3##_max * ABSOLUTE(v2) < ABSOLUTE(v1)) ||	\
			   (dst[i] = (TYPE3) v1 / v2) < -max ||		\
			   dst[i] > max) {				\
			if (abort_on_error)				\
				ON_OVERFLOW(TYPE1, TYPE2, "/");		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		}							\
	}								\
	return nils;							\
}

DIV_3TYPE_I(bte, bte, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(bte, bte, sht)
DIV_3TYPE_I(bte, bte, int)
DIV_3TYPE_I(bte, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(bte, bte, hge)
#endif
#endif
DIV_3TYPE_IF(bte, bte, flt)
DIV_3TYPE_IF(bte, bte, dbl)
DIV_3TYPE_I(bte, sht, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(bte, sht, sht)
DIV_3TYPE_I(bte, sht, int)
DIV_3TYPE_I(bte, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(bte, sht, hge)
#endif
#endif
DIV_3TYPE_IF(bte, sht, flt)
DIV_3TYPE_IF(bte, sht, dbl)
DIV_3TYPE_I(bte, int, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(bte, int, sht)
DIV_3TYPE_I(bte, int, int)
DIV_3TYPE_I(bte, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(bte, int, hge)
#endif
#endif
DIV_3TYPE_IF(bte, int, flt)
DIV_3TYPE_IF(bte, int, dbl)
DIV_3TYPE_I(bte, lng, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(bte, lng, sht)
DIV_3TYPE_I(bte, lng, int)
DIV_3TYPE_I(bte, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(bte, lng, hge)
#endif
#endif
DIV_3TYPE_IF(bte, lng, flt)
DIV_3TYPE_IF(bte, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE_I(bte, hge, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(bte, hge, sht)
DIV_3TYPE_I(bte, hge, int)
DIV_3TYPE_I(bte, hge, lng)
DIV_3TYPE_I(bte, hge, hge)
#endif
DIV_3TYPE_IF(bte, hge, flt)
DIV_3TYPE_IF(bte, hge, dbl)
#endif
DIV_3TYPE_float(bte, flt, flt)
DIV_3TYPE_float(bte, flt, dbl)
DIV_3TYPE_float(bte, dbl, dbl)
DIV_3TYPE_I(sht, bte, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(sht, bte, int)
DIV_3TYPE_I(sht, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(sht, bte, hge)
#endif
#endif
DIV_3TYPE_IF(sht, bte, flt)
DIV_3TYPE_IF(sht, bte, dbl)
DIV_3TYPE_I(sht, sht, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(sht, sht, int)
DIV_3TYPE_I(sht, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(sht, sht, hge)
#endif
#endif
DIV_3TYPE_IF(sht, sht, flt)
DIV_3TYPE_IF(sht, sht, dbl)
DIV_3TYPE_I(sht, int, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(sht, int, int)
DIV_3TYPE_I(sht, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(sht, int, hge)
#endif
#endif
DIV_3TYPE_IF(sht, int, flt)
DIV_3TYPE_IF(sht, int, dbl)
DIV_3TYPE_I(sht, lng, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(sht, lng, int)
DIV_3TYPE_I(sht, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(sht, lng, hge)
#endif
#endif
DIV_3TYPE_IF(sht, lng, flt)
DIV_3TYPE_IF(sht, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE_I(sht, hge, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(sht, hge, int)
DIV_3TYPE_I(sht, hge, lng)
DIV_3TYPE_I(sht, hge, hge)
#endif
DIV_3TYPE_IF(sht, hge, flt)
DIV_3TYPE_IF(sht, hge, dbl)
#endif
DIV_3TYPE_float(sht, flt, flt)
DIV_3TYPE_float(sht, flt, dbl)
DIV_3TYPE_float(sht, dbl, dbl)
DIV_3TYPE_I(int, bte, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(int, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(int, bte, hge)
#endif
#endif
DIV_3TYPE_IF(int, bte, flt)
DIV_3TYPE_IF(int, bte, dbl)
DIV_3TYPE_I(int, sht, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(int, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(int, sht, hge)
#endif
#endif
DIV_3TYPE_IF(int, sht, flt)
DIV_3TYPE_IF(int, sht, dbl)
DIV_3TYPE_I(int, int, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(int, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(int, int, hge)
#endif
#endif
DIV_3TYPE_IF(int, int, flt)
DIV_3TYPE_IF(int, int, dbl)
DIV_3TYPE_I(int, lng, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(int, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE_I(int, lng, hge)
#endif
#endif
DIV_3TYPE_IF(int, lng, flt)
DIV_3TYPE_IF(int, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE_I(int, hge, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(int, hge, lng)
DIV_3TYPE_I(int, hge, hge)
#endif
DIV_3TYPE_IF(int, hge, flt)
DIV_3TYPE_IF(int, hge, dbl)
#endif
DIV_3TYPE_float(int, flt, flt)
DIV_3TYPE_float(int, flt, dbl)
DIV_3TYPE_float(int, dbl, dbl)
DIV_3TYPE_I(lng, bte, lng)
#ifdef HAVE_HGE
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(lng, bte, hge)
#endif
#endif
DIV_3TYPE_IF(lng, bte, flt)
DIV_3TYPE_IF(lng, bte, dbl)
DIV_3TYPE_I(lng, sht, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
DIV_3TYPE_I(lng, sht, hge)
#endif
#endif
DIV_3TYPE_IF(lng, sht, flt)
DIV_3TYPE_IF(lng, sht, dbl)
DIV_3TYPE_I(lng, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
DIV_3TYPE_I(lng, int, hge)
#endif
#endif
DIV_3TYPE_IF(lng, int, flt)
DIV_3TYPE_IF(lng, int, dbl)
DIV_3TYPE_I(lng, lng, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
DIV_3TYPE_I(lng, lng, hge)
#endif
#endif
DIV_3TYPE_IF(lng, lng, flt)
DIV_3TYPE_IF(lng, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE_I(lng, hge, lng)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE_I(lng, hge, hge)
#endif
DIV_3TYPE_IF(lng, hge, flt)
DIV_3TYPE_IF(lng, hge, dbl)
#endif
DIV_3TYPE_float(lng, flt, flt)
DIV_3TYPE_float(lng, flt, dbl)
DIV_3TYPE_float(lng, dbl, dbl)
#ifdef HAVE_HGE
DIV_3TYPE_I(hge, bte, hge)
DIV_3TYPE_IF(hge, bte, flt)
DIV_3TYPE_IF(hge, bte, dbl)
DIV_3TYPE_I(hge, sht, hge)
DIV_3TYPE_IF(hge, sht, flt)
DIV_3TYPE_IF(hge, sht, dbl)
DIV_3TYPE_I(hge, int, hge)
DIV_3TYPE_IF(hge, int, flt)
DIV_3TYPE_IF(hge, int, dbl)
DIV_3TYPE_I(hge, lng, hge)
DIV_3TYPE_IF(hge, lng, flt)
DIV_3TYPE_IF(hge, lng, dbl)
DIV_3TYPE_I(hge, hge, hge)
DIV_3TYPE_IF(hge, hge, flt)
DIV_3TYPE_IF(hge, hge, dbl)
DIV_3TYPE_float(hge, flt, flt)
DIV_3TYPE_float(hge, flt, dbl)
DIV_3TYPE_float(hge, dbl, dbl)
#endif
DIV_3TYPE_IF(flt, bte, flt)
DIV_3TYPE_IF(flt, bte, dbl)
DIV_3TYPE_IF(flt, sht, flt)
DIV_3TYPE_IF(flt, sht, dbl)
DIV_3TYPE_IF(flt, int, flt)
DIV_3TYPE_IF(flt, int, dbl)
DIV_3TYPE_IF(flt, lng, flt)
DIV_3TYPE_IF(flt, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE_IF(flt, hge, flt)
DIV_3TYPE_IF(flt, hge, dbl)
#endif
DIV_3TYPE_float(flt, flt, flt)
DIV_3TYPE_float(flt, flt, dbl)
DIV_3TYPE_float(flt, dbl, dbl)
DIV_3TYPE_IF(dbl, bte, dbl)
DIV_3TYPE_IF(dbl, sht, dbl)
DIV_3TYPE_IF(dbl, int, dbl)
DIV_3TYPE_IF(dbl, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE_IF(dbl, hge, dbl)
#endif
DIV_3TYPE_float(dbl, flt, dbl)
DIV_3TYPE_float(dbl, dbl, dbl)

static BUN
divswitch(const void *src1, int tp1, const void *src2, int tp2, void *restrict dst, int tp,
	  const void *v1p, const void *v2p, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int abort_on_error, int projected,
	  const char *func)
{
	BUN nils = 0;

	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_bte_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_bte_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_sht_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_bte_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_bte_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_int_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_bte_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_int_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_bte_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_lng_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_bte_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_lng_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_lng_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_bte_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_hge_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_bte_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_hge_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_hge_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_hge_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = div_bte_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_bte_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_bte_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_bte_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_sht_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_sht_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_int_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_sht_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_lng_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_lng_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_sht_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_hge_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_sht_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_hge_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_hge_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = div_sht_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_sht_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_sht_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_sht_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_int:
				nils = div_int_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_int_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				nils = div_int_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_int_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = div_int_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_int_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_int:
				nils = div_int_lng_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_int_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_int:
				nils = div_int_hge_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_int_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_hge_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = div_int_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_int_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_int_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_int_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_lng_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_lng_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_lng_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* HAVE_HGE */
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_lng_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_hge_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_lng_max, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_hge:
				nils = div_lng_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
#endif	/* FULL_IMPLEMENTATION */
			case TYPE_flt:
				nils = div_lng_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_lng_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_lng_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_hge_max, projected, abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_hge_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_hge_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
#endif	/* HAVE_HGE */
	case TYPE_flt:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_flt_max, projected, abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_flt_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif	/* HAVE_HGE */
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, GDK_dbl_max, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	default:
		goto unsupported;
	}

	if (nils == BUN_NONE + 1) {
		GDKerror("22012!division by zero.\n");
		return BUN_NONE;
	}
	return nils;

  unsupported:
	GDKerror("%s: type combination (div(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcdiv(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error, int projected)
{
	BAT *bn;
	CALC_DECL;

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = divswitch(Tloc(b1, 0), b1->ttype, Tloc(b2, 0), b2->ttype, Tloc(bn, 0), tp, NULL, NULL,
			 hoff, start, cnt, cand, abort_on_error, projected,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcdivcst(BAT *b, const ValRecord *v2, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = divswitch(Tloc(b, 0), b->ttype, NULL, v2->vtype, Tloc(bn, 0), tp, NULL,
			 VALptr(v2), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* if the input is sorted, and no zero division or overflow
	 * occurred (we only know for sure if no nils were produced),
	 * the result is also sorted, or reverse sorted if the
	 * constant is negative */
	if (cnt <= 1 || cnt == nils) {
		bn->tsorted = bn->trevsorted = 1;
	} else if (nils == 0) {
		ValRecord sign;

		VARcalcsign(&sign, v2);
		bn->tsorted = (sign.val.btval > 0 && b->tsorted) ||
			(sign.val.btval < 0 && b->trevsorted);
		bn->trevsorted = (sign.val.btval > 0 && b->trevsorted) ||
			(sign.val.btval < 0 && b->tsorted);
	} else {
		bn->tsorted = bn->trevsorted = 0;
	}
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstdiv(const ValRecord *v1, BAT *b, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = divswitch(NULL, v1->vtype, Tloc(b, 0), b->ttype, Tloc(bn, 0), tp, VALptr(v1),
			 NULL, hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	/* we can't say much about sortedness */
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcdiv(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   int abort_on_error)
{

	if (divswitch(NULL, lft->vtype, NULL, rgt->vtype, VALget(ret),
		      ret->vtype, VALptr(lft), VALptr(rgt), 0, 0, 1, NULL,
		      abort_on_error, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* modulo (any numeric type) */

#define MOD_3TYPE(TYPE1, TYPE2, TYPE3)					\
static BUN								\
mod_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
				const TYPE2 *src2, const TYPE2 *v2p,	\
				TYPE3 *restrict dst, const oid *restrict cand, \
				BUN start, BUN cnt, oid hoff,		\
				int projected, int abort_on_error)	\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else if (v2 == 0) {					\
			if (abort_on_error)				\
				return BUN_NONE + 1;			\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (TYPE3) (v1 % v2);			\
		}							\
	}								\
	return nils;							\
}

#define FMOD_3TYPE(TYPE1, TYPE2, TYPE3, FMOD)				\
static BUN								\
mod_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *src1, const TYPE1 *v1p,	\
				const TYPE2 *src2, const TYPE2 *v2p,	\
				TYPE3 *restrict dst, const oid *restrict cand, \
				BUN start, BUN cnt, oid hoff,		\
				int projected, int abort_on_error)	\
{									\
	TYPE1 v1 = v1p ? *v1p : 0;					\
	TYPE2 v2 = v2p ? *v2p : 0;					\
	BUN nils = 0;							\
	BUN i;								\
									\
	assert((src1 == NULL) != (v1p == NULL));			\
	assert((src2 == NULL) != (v2p == NULL));			\
	for (i = 0; i < cnt; i++) {					\
		if (src1)						\
			v1 = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
		if (src2)						\
			v2 = src2[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
		if (v1 == TYPE1##_nil || v2 == TYPE2##_nil) {		\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else if (v2 == 0) {					\
			if (abort_on_error)				\
				return BUN_NONE + 1;			\
			dst[i] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (TYPE3) FMOD((TYPE3) v1, (TYPE3) v2);	\
		}							\
	}								\
	return nils;							\
}

MOD_3TYPE(bte, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, bte, sht)
MOD_3TYPE(bte, bte, int)
MOD_3TYPE(bte, bte, lng)
#ifdef HAVE_HGE
MOD_3TYPE(bte, bte, hge)
#endif
#endif
MOD_3TYPE(bte, sht, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, sht, sht)
MOD_3TYPE(bte, sht, int)
MOD_3TYPE(bte, sht, lng)
#ifdef HAVE_HGE
MOD_3TYPE(bte, sht, hge)
#endif
#endif
MOD_3TYPE(bte, int, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, int, sht)
MOD_3TYPE(bte, int, int)
MOD_3TYPE(bte, int, lng)
#ifdef HAVE_HGE
MOD_3TYPE(bte, int, hge)
#endif
#endif
MOD_3TYPE(bte, lng, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, lng, sht)
MOD_3TYPE(bte, lng, int)
MOD_3TYPE(bte, lng, lng)
#ifdef HAVE_HGE
MOD_3TYPE(bte, lng, hge)
#endif
#endif
#ifdef HAVE_HGE
MOD_3TYPE(bte, hge, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, hge, sht)
MOD_3TYPE(bte, hge, int)
MOD_3TYPE(bte, hge, lng)
MOD_3TYPE(bte, hge, hge)
#endif
#endif
MOD_3TYPE(sht, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, bte, sht)
MOD_3TYPE(sht, bte, int)
MOD_3TYPE(sht, bte, lng)
#ifdef HAVE_HGE
MOD_3TYPE(sht, bte, hge)
#endif
#endif
MOD_3TYPE(sht, sht, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, sht, int)
MOD_3TYPE(sht, sht, lng)
#ifdef HAVE_HGE
MOD_3TYPE(sht, sht, hge)
#endif
#endif
MOD_3TYPE(sht, int, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, int, int)
MOD_3TYPE(sht, int, lng)
#ifdef HAVE_HGE
MOD_3TYPE(sht, int, hge)
#endif
#endif
MOD_3TYPE(sht, lng, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, lng, int)
MOD_3TYPE(sht, lng, lng)
#ifdef HAVE_HGE
MOD_3TYPE(sht, lng, hge)
#endif
#endif
#ifdef HAVE_HGE
MOD_3TYPE(sht, hge, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, hge, int)
MOD_3TYPE(sht, hge, lng)
MOD_3TYPE(sht, hge, hge)
#endif
#endif
MOD_3TYPE(int, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, bte, sht)
MOD_3TYPE(int, bte, int)
MOD_3TYPE(int, bte, lng)
#ifdef HAVE_HGE
MOD_3TYPE(int, bte, hge)
#endif
#endif
MOD_3TYPE(int, sht, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, sht, int)
MOD_3TYPE(int, sht, lng)
#ifdef HAVE_HGE
MOD_3TYPE(int, sht, hge)
#endif
#endif
MOD_3TYPE(int, int, int)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, int, lng)
#ifdef HAVE_HGE
MOD_3TYPE(int, int, hge)
#endif
#endif
MOD_3TYPE(int, lng, int)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, lng, lng)
#ifdef HAVE_HGE
MOD_3TYPE(int, lng, hge)
#endif
#endif
#ifdef HAVE_HGE
MOD_3TYPE(int, hge, int)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, hge, lng)
MOD_3TYPE(int, hge, hge)
#endif
#endif
MOD_3TYPE(lng, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(lng, bte, sht)
MOD_3TYPE(lng, bte, int)
MOD_3TYPE(lng, bte, lng)
#ifdef HAVE_HGE
MOD_3TYPE(lng, bte, hge)
#endif
#endif
MOD_3TYPE(lng, sht, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(lng, sht, int)
MOD_3TYPE(lng, sht, lng)
#ifdef HAVE_HGE
MOD_3TYPE(lng, sht, hge)
#endif
#endif
MOD_3TYPE(lng, int, int)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(lng, int, lng)
#ifdef HAVE_HGE
MOD_3TYPE(lng, int, hge)
#endif
#endif
MOD_3TYPE(lng, lng, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MOD_3TYPE(lng, lng, hge)
#endif
#endif
#ifdef HAVE_HGE
MOD_3TYPE(lng, hge, lng)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(lng, hge, hge)
#endif
#endif
#ifdef HAVE_HGE
MOD_3TYPE(hge, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(hge, bte, sht)
MOD_3TYPE(hge, bte, int)
MOD_3TYPE(hge, bte, lng)
MOD_3TYPE(hge, bte, hge)
#endif
MOD_3TYPE(hge, sht, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(hge, sht, int)
MOD_3TYPE(hge, sht, lng)
MOD_3TYPE(hge, sht, hge)
#endif
MOD_3TYPE(hge, int, int)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(hge, int, lng)
MOD_3TYPE(hge, int, hge)
#endif
MOD_3TYPE(hge, lng, lng)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(hge, lng, hge)
#endif
MOD_3TYPE(hge, hge, hge)
#endif

FMOD_3TYPE(bte, flt, flt, fmodf)
FMOD_3TYPE(sht, flt, flt, fmodf)
FMOD_3TYPE(int, flt, flt, fmodf)
FMOD_3TYPE(lng, flt, flt, fmodf)
#ifdef HAVE_HGE
FMOD_3TYPE(hge, flt, flt, fmodf)
#endif
FMOD_3TYPE(flt, bte, flt, fmodf)
FMOD_3TYPE(flt, sht, flt, fmodf)
FMOD_3TYPE(flt, int, flt, fmodf)
FMOD_3TYPE(flt, lng, flt, fmodf)
#ifdef HAVE_HGE
FMOD_3TYPE(flt, hge, flt, fmodf)
#endif
FMOD_3TYPE(flt, flt, flt, fmodf)
FMOD_3TYPE(bte, dbl, dbl, fmod)
FMOD_3TYPE(sht, dbl, dbl, fmod)
FMOD_3TYPE(int, dbl, dbl, fmod)
FMOD_3TYPE(lng, dbl, dbl, fmod)
#ifdef HAVE_HGE
FMOD_3TYPE(hge, dbl, dbl, fmod)
#endif
FMOD_3TYPE(flt, dbl, dbl, fmod)
FMOD_3TYPE(dbl, bte, dbl, fmod)
FMOD_3TYPE(dbl, sht, dbl, fmod)
FMOD_3TYPE(dbl, int, dbl, fmod)
FMOD_3TYPE(dbl, lng, dbl, fmod)
#ifdef HAVE_HGE
FMOD_3TYPE(dbl, hge, dbl, fmod)
#endif
FMOD_3TYPE(dbl, flt, dbl, fmod)
FMOD_3TYPE(dbl, dbl, dbl, fmod)

static BUN
modswitch(const void *src1, int tp1, const void *src2, int tp2, void *restrict dst, int tp,
	  const void *v1p, const void *v2p, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int abort_on_error, int projected,
	  const char *func)
{
	BUN nils = 0;

	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_sht_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_int_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_int_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_lng_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_lng_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_lng_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_hge_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_hge_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_hge_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_hge_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_bte_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_bte_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_bte_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = mod_sht_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_sht_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_sht_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_sht_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_sht:
				nils = mod_sht_int_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_sht:
				nils = mod_sht_lng_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_lng_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_sht:
				nils = mod_sht_hge_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_hge_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_hge_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_sht_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_sht_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_sht_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = mod_int_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_int_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_int_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_int_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_int_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mod_int_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_int_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_int:
				nils = mod_int_lng_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_int_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_int:
				nils = mod_int_hge_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_int_hge_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_int_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_int_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_int_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = mod_lng_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_lng_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_lng_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_lng_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_lng_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mod_lng_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_lng_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mod_lng_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_lng:
				nils = mod_lng_hge_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_hge:
				nils = mod_lng_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_lng_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_lng_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = mod_hge_bte_bte(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_hge_bte_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_int:
				nils = mod_hge_bte_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_hge_bte_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_bte_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_hge_sht_sht(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_hge_sht_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_hge_sht_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_sht_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mod_hge_int_int(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_hge_int_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_int_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mod_hge_lng_lng(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_hge:
				nils = mod_hge_lng_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mod_hge_hge_hge(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_hge_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_hge_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
#endif
	case TYPE_flt:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_bte_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_sht_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_int_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_lng_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_hge_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_flt_flt(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_flt_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_bte_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_sht_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_int_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_lng_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_hge_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_flt_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_dbl_dbl(src1, v1p, src2, v2p, dst, cand, start, cnt, hoff, projected, abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	default:
		goto unsupported;
	}

	if (nils == BUN_NONE + 1) {
		GDKerror("22012!modision by zero.\n");
		return BUN_NONE;
	}
	return nils;

  unsupported:
	GDKerror("%s: type combination (mod(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcmod(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error, int projected)
{
	BAT *bn;
	CALC_DECL;

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = modswitch(Tloc(b1, 0), b1->ttype, Tloc(b2, 0), b2->ttype, Tloc(bn, 0), tp, NULL, NULL,
			 hoff, start, cnt, cand, abort_on_error, projected,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcmodcst(BAT *b, const ValRecord *v2, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = modswitch(Tloc(b, 0), b->ttype, NULL, v2->vtype, Tloc(bn, 0), tp, NULL,
			 VALptr(v2), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstmod(const ValRecord *v1, BAT *b, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = modswitch(NULL, v1->vtype, Tloc(b, 0), b->ttype, Tloc(bn, 0), tp, VALptr(v1),
			 NULL, hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcmod(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   int abort_on_error)
{

	if (modswitch(NULL, lft->vtype, NULL, rgt->vtype, VALget(ret),
		      ret->vtype, VALptr(lft), VALptr(rgt), 0, 0, 1, NULL,
		      abort_on_error, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) exclusive OR */

#define XOR(v1, v2)	((v1) ^ (v2))
#define XORBIT(v1, v2)	(((v1) == 0) != ((v2) == 0))

static BUN
xorswitch(int tp, const void *src1, const void *v1p,
	  const void *src2, const void *v2p,
	  void *restrict dst, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int projected, const char *func)
{
	BUN nils = 0;

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit)
			BINARY_3TYPE_FUNC(bit, bit, bit, XORBIT);
		else
			BINARY_3TYPE_FUNC(bte, bte, bte, XOR);
		break;
	case TYPE_sht:
		BINARY_3TYPE_FUNC(sht, sht, sht, XOR);
		break;
	case TYPE_int:
		BINARY_3TYPE_FUNC(int, int, int, XOR);
		break;
	case TYPE_lng:
		BINARY_3TYPE_FUNC(lng, lng, lng, XOR);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		BINARY_3TYPE_FUNC(hge, hge, hge, XOR);
		break;
#endif
	default:
		GDKerror("%s: type %s not supported.\n", func, ATOMname(tp));
		return BUN_NONE;
	}
	return nils;
}

BAT *
BATcalcxor(BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, b1->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = xorswitch(b1->ttype, Tloc(b1, 0), NULL, Tloc(b2, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, projected,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcxorcst(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = xorswitch(b->ttype, Tloc(b, 0), NULL, NULL, VALptr(v),
			 Tloc(bn, 0), hoff, start, cnt, cand, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstxor(const ValRecord *v, BAT *b, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = xorswitch(b->ttype, NULL, VALptr(v), Tloc(b, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcxor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return GDK_FAIL;
	}

	if (xorswitch(lft->vtype, NULL, VALptr(lft), NULL, VALptr(rgt),
		      VALget(ret), 0, 0, 1, NULL, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) OR */

#define OR(v1, v2)	((v1) | (v2))
#define ORBIT(v1, v2)	((v1) == 0 ? (v2) == 0 ? 0 : (v2) == bit_nil ? (nils++, bit_nil) : 1 : (v1) == bit_nil ? (v2) == 0 || (v2) == bit_nil ? (nils++, bit_nil) : 1 : 1)

static BUN
orswitch(int tp, const void *src1, const void *v1p,
	 const void *src2, const void *v2p,
	 void *restrict dst, oid hoff, BUN start, BUN cnt,
	 const oid *restrict cand, int projected, const char *func)
{
	BUN nils = 0;

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit)
			BINARY_3TYPE_FUNC_nonil(bit, bit, bit, ORBIT);
		else
			BINARY_3TYPE_FUNC(bte, bte, bte, OR);
		break;
	case TYPE_sht:
		BINARY_3TYPE_FUNC(sht, sht, sht, OR);
		break;
	case TYPE_int:
		BINARY_3TYPE_FUNC(int, int, int, OR);
		break;
	case TYPE_lng:
		BINARY_3TYPE_FUNC(lng, lng, lng, OR);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		BINARY_3TYPE_FUNC(hge, hge, hge, OR);
		break;
#endif
	default:
		GDKerror("%s: type %s not supported.\n", func, ATOMname(tp));
		return BUN_NONE;
	}
	return nils;
}

BAT *
BATcalcor(BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, b1->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = orswitch(b1->ttype, Tloc(b1, 0), NULL, Tloc(b2, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, projected,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcorcst(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = orswitch(b->ttype, Tloc(b, 0), NULL, NULL, VALptr(v),
			 Tloc(bn, 0), hoff, start, cnt, cand, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstor(const ValRecord *v, BAT *b, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = orswitch(b->ttype, NULL, VALptr(v), Tloc(b, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return GDK_FAIL;
	}

	if (orswitch(lft->vtype, NULL, VALptr(lft), NULL, VALptr(rgt),
		      VALget(ret), 0, 0, 1, NULL, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) AND */

#define AND(v1, v2)	((v1) & (v2))
#define ANDBIT(v1, v2)	((v1) == 0 || (v2) == 0 ? 0 : (v1) != bit_nil && (v2) != bit_nil ? 1 : (nils++, bit_nil))

static BUN
andswitch(int tp, const void *src1, const void *v1p,
	  const void *src2, const void *v2p,
	  void *restrict dst, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int projected, const char *func)
{
	BUN nils = 0;

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit)
			BINARY_3TYPE_FUNC_nonil(bit, bit, bit, ANDBIT);
		else
			BINARY_3TYPE_FUNC(bte, bte, bte, AND);
		break;
	case TYPE_sht:
		BINARY_3TYPE_FUNC(sht, sht, sht, AND);
		break;
	case TYPE_int:
		BINARY_3TYPE_FUNC(int, int, int, AND);
		break;
	case TYPE_lng:
		BINARY_3TYPE_FUNC(lng, lng, lng, AND);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		BINARY_3TYPE_FUNC(hge, hge, hge, AND);
		break;
#endif
	default:
		GDKerror("%s: type %s not supported.\n", func, ATOMname(tp));
		return BUN_NONE;
	}
	return nils;
}

BAT *
BATcalcand(BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, b1->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = andswitch(b1->ttype, Tloc(b1, 0), NULL, Tloc(b2, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, projected,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcandcst(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = andswitch(b->ttype, Tloc(b, 0), NULL, NULL, VALptr(v),
			 Tloc(bn, 0), hoff, start, cnt, cand, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstand(const ValRecord *v, BAT *b, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = andswitch(b->ttype, NULL, VALptr(v), Tloc(b, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcand(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return GDK_FAIL;
	}

	if (andswitch(lft->vtype, NULL, VALptr(lft), NULL, VALptr(rgt),
		      VALget(ret), 0, 0, 1, NULL, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* left shift (any integral type) */

#define LSH_CHECK(a, b, TYPE)	((b) < 0 || (b) >= 8 * (int) sizeof(a) || (a) < 0 || (a) > (GDK_##TYPE##_max >> (b)))
#define LSH_CHECK_bte(a, b)	LSH_CHECK(a, b, bte)
#define LSH_CHECK_sht(a, b)	LSH_CHECK(a, b, sht)
#define LSH_CHECK_int(a, b)	LSH_CHECK(a, b, int)
#define LSH_CHECK_lng(a, b)	LSH_CHECK(a, b, lng)
#define LSH_CHECK_hge(a, b)	LSH_CHECK(a, b, hge)
#define LSH_CHECK_hge2(a, b)	((b) < 0 || (a) < 0 || (a) > (GDK_hge_max >> (b)))

static BUN
lshswitch(int tp1, const void *src1, const void *v1p,
	  int tp2, const void *src2, const void *v2p,
	  void *restrict dst, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int abort_on_error, int projected,
	  const char *func)
{
	BUN nils = 0;

	switch (ATOMbasetype(tp1)) {
	case TYPE_bte:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(bte, bte, bte, LSH, LSH_CHECK_bte, ON_OVERFLOW(bte, bte, "<<"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(bte, sht, bte, LSH, LSH_CHECK_bte, ON_OVERFLOW(bte, sht, "<<"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(bte, int, bte, LSH, LSH_CHECK_bte, ON_OVERFLOW(bte, int, "<<"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(bte, lng, bte, LSH, LSH_CHECK_bte, ON_OVERFLOW(bte, lng, "<<"));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(bte, hge, bte, LSH, LSH_CHECK_bte, ON_OVERFLOW(bte, hge, "<<"));
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(sht, bte, sht, LSH, LSH_CHECK_sht, ON_OVERFLOW(sht, bte, "<<"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(sht, sht, sht, LSH, LSH_CHECK_sht, ON_OVERFLOW(sht, sht, "<<"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(sht, int, sht, LSH, LSH_CHECK_sht, ON_OVERFLOW(sht, int, "<<"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(sht, lng, sht, LSH, LSH_CHECK_sht, ON_OVERFLOW(sht, lng, "<<"));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(sht, hge, sht, LSH, LSH_CHECK_sht, ON_OVERFLOW(sht, hge, "<<"));
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(int, bte, int, LSH, LSH_CHECK_int, ON_OVERFLOW(int, bte, "<<"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(int, sht, int, LSH, LSH_CHECK_int, ON_OVERFLOW(int, sht, "<<"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(int, int, int, LSH, LSH_CHECK_int, ON_OVERFLOW(int, int, "<<"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(int, lng, int, LSH, LSH_CHECK_int, ON_OVERFLOW(int, lng, "<<"));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(int, hge, int, LSH, LSH_CHECK_int, ON_OVERFLOW(int, hge, "<<"));
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(lng, bte, lng, LSH, LSH_CHECK_lng, ON_OVERFLOW(lng, bte, "<<"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(lng, sht, lng, LSH, LSH_CHECK_lng, ON_OVERFLOW(lng, sht, "<<"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(lng, int, lng, LSH, LSH_CHECK_lng, ON_OVERFLOW(lng, int, "<<"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(lng, lng, lng, LSH, LSH_CHECK_lng, ON_OVERFLOW(lng, lng, "<<"));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(lng, hge, lng, LSH, LSH_CHECK_lng, ON_OVERFLOW(lng, hge, "<<"));
			break;
#endif
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(hge, bte, hge, LSH, LSH_CHECK_hge2, ON_OVERFLOW(hge, bte, "<<"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(hge, sht, hge, LSH, LSH_CHECK_hge, ON_OVERFLOW(hge, sht, "<<"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(hge, int, hge, LSH, LSH_CHECK_hge, ON_OVERFLOW(hge, int, "<<"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(hge, lng, hge, LSH, LSH_CHECK_hge, ON_OVERFLOW(hge, lng, "<<"));;
			break;
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(hge, hge, hge, LSH, LSH_CHECK_hge, ON_OVERFLOW(hge, hge, "<<"));
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
	GDKerror("%s: type combination (lsh(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp1));
	return BUN_NONE;
}

BAT *
BATcalclsh(BAT *b1, BAT *b2, BAT *s, int abort_on_error, int projected)
{
	BAT *bn;
	CALC_DECL;

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, b1->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = lshswitch(b1->ttype, Tloc(b1, 0), NULL,
			 b2->ttype, Tloc(b2, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, abort_on_error,
			 projected, __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalclshcst(BAT *b, const ValRecord *v, BAT *s, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = lshswitch(b->ttype, Tloc(b, 0), NULL, v->vtype, NULL, VALptr(v),
			 Tloc(bn, 0), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstlsh(const ValRecord *v, BAT *b, BAT *s, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = lshswitch(v->vtype, NULL, VALptr(v), b->ttype, Tloc(b, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalclsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return GDK_FAIL;
	}

	if (lshswitch(lft->vtype, NULL, VALptr(lft),
		      rgt->vtype, NULL, VALptr(rgt),
		      VALget(ret), 0, 0, 1, NULL, abort_on_error, 0,
		      __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* left shift (any integral type) */

#define RSH_CHECK(a, b)		((b) < 0 || (b) >= 8 * (int) sizeof(a))
#define RSH_CHECK2(a, b)	((b) < 0)

static BUN
rshswitch(int tp1, const void *src1, const void *v1p,
	  int tp2, const void *src2, const void *v2p,
	  void *restrict dst, oid hoff, BUN start, BUN cnt,
	  const oid *restrict cand, int abort_on_error, int projected,
	  const char *func)
{
	BUN nils = 0;

	switch (ATOMbasetype(tp1)) {
	case TYPE_bte:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(bte, bte, bte, RSH, RSH_CHECK, ON_OVERFLOW(bte, bte, ">>"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(bte, sht, bte, RSH, RSH_CHECK, ON_OVERFLOW(bte, sht, ">>"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(bte, int, bte, RSH, RSH_CHECK, ON_OVERFLOW(bte, int, ">>"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(bte, lng, bte, RSH, RSH_CHECK, ON_OVERFLOW(bte, lng, ">>"));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(bte, hge, bte, RSH, RSH_CHECK, ON_OVERFLOW(bte, hge, ">>"));
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(sht, bte, sht, RSH, RSH_CHECK, ON_OVERFLOW(sht, bte, ">>"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(sht, sht, sht, RSH, RSH_CHECK, ON_OVERFLOW(sht, sht, ">>"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(sht, int, sht, RSH, RSH_CHECK, ON_OVERFLOW(sht, int, ">>"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(sht, lng, sht, RSH, RSH_CHECK, ON_OVERFLOW(sht, lng, ">>"));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(sht, hge, sht, RSH, RSH_CHECK, ON_OVERFLOW(sht, hge, ">>"));
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(int, bte, int, RSH, RSH_CHECK, ON_OVERFLOW(int, bte, ">>"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(int, sht, int, RSH, RSH_CHECK, ON_OVERFLOW(int, sht, ">>"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(int, int, int, RSH, RSH_CHECK, ON_OVERFLOW(int, int, ">>"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(int, lng, int, RSH, RSH_CHECK, ON_OVERFLOW(int, lng, ">>"));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(int, hge, int, RSH, RSH_CHECK, ON_OVERFLOW(int, hge, ">>"));
			break;
#endif
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(lng, bte, lng, RSH, RSH_CHECK, ON_OVERFLOW(lng, bte, ">>"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(lng, sht, lng, RSH, RSH_CHECK, ON_OVERFLOW(lng, sht, ">>"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(lng, int, lng, RSH, RSH_CHECK, ON_OVERFLOW(lng, int, ">>"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(lng, lng, lng, RSH, RSH_CHECK, ON_OVERFLOW(lng, lng, ">>"));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(lng, hge, lng, RSH, RSH_CHECK, ON_OVERFLOW(lng, hge, ">>"));
			break;
#endif
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(hge, bte, hge, RSH, RSH_CHECK2, ON_OVERFLOW(hge, bte, ">>"));
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(hge, sht, hge, RSH, RSH_CHECK, ON_OVERFLOW(hge, sht, ">>"));
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(hge, int, hge, RSH, RSH_CHECK, ON_OVERFLOW(hge, int, ">>"));
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(hge, lng, hge, RSH, RSH_CHECK, ON_OVERFLOW(hge, lng, ">>"));;
			break;
		case TYPE_hge:
			BINARY_3TYPE_FUNC_CHECK(hge, hge, hge, RSH, RSH_CHECK, ON_OVERFLOW(hge, hge, ">>"));
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
	GDKerror("%s: type combination (rsh(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp1));
	return BUN_NONE;
}

BAT *
BATcalcrsh(BAT *b1, BAT *b2, BAT *s, int abort_on_error, int projected)
{
	BAT *bn;
	CALC_DECL;

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	bn = COLnew(hseq, b1->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = rshswitch(b1->ttype, Tloc(b1, 0), NULL,
			 b2->ttype, Tloc(b2, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, abort_on_error,
			 projected, __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcrshcst(BAT *b, const ValRecord *v, BAT *s, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = rshswitch(b->ttype, Tloc(b, 0), NULL, v->vtype, NULL, VALptr(v),
			 Tloc(bn, 0), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalccstrsh(const ValRecord *v, BAT *b, BAT *s, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	bn = COLnew(hseq, b->ttype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = rshswitch(v->vtype, NULL, VALptr(v), b->ttype, Tloc(b, 0), NULL,
			 Tloc(bn, 0), hoff, start, cnt, cand, abort_on_error, 0,
			 __func__);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcrsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return GDK_FAIL;
	}

	if (rshswitch(lft->vtype, NULL, VALptr(lft),
		      rgt->vtype, NULL, VALptr(rgt),
		      VALget(ret), 0, 0, 1, NULL, abort_on_error, 0,
		      __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* comparisons */

#define tpe bit
#define tpe_nil bit_nil
#define TYPE_tpe TYPE_bit

/* ---------------------------------------------------------------------- */
/* less than (any "linear" type) */

#define OP LT
#define opswitch ltswitch
#define BATcalcop BATcalclt
#define BATcalcopcst BATcalcltcst
#define BATcalccstop BATcalccstlt
#define VARcalcop VARcalclt

#include "gdk_calc_compare.h"

#undef OP
#undef opswitch
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* less than or equal (any "linear" type) */

#define OP LE
#define opswitch leswitch
#define BATcalcop BATcalcle
#define BATcalcopcst BATcalclecst
#define BATcalccstop BATcalccstle
#define VARcalcop VARcalcle

#include "gdk_calc_compare.h"

#undef OP
#undef opswitch
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* greater than (any "linear" type) */

#define OP GT
#define opswitch gtswitch
#define BATcalcop BATcalcgt
#define BATcalcopcst BATcalcgtcst
#define BATcalccstop BATcalccstgt
#define VARcalcop VARcalcgt

#include "gdk_calc_compare.h"

#undef OP
#undef opswitch
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* greater than or equal (any "linear" type) */

#define OP GE
#define opswitch geswitch
#define BATcalcop BATcalcge
#define BATcalcopcst BATcalcgecst
#define BATcalccstop BATcalccstge
#define VARcalcop VARcalcge

#include "gdk_calc_compare.h"

#undef OP
#undef opswitch
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* eqaul (any "linear" type) */

#define OP EQ
#define opswitch eqswitch
#define BATcalcop BATcalceq
#define BATcalcopcst BATcalceqcst
#define BATcalccstop BATcalccsteq
#define VARcalcop VARcalceq

#include "gdk_calc_compare.h"

#undef OP
#undef opswitch
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* not eqaul (any "linear" type) */

#define OP NE
#define opswitch neswitch
#define BATcalcop BATcalcne
#define BATcalcopcst BATcalcnecst
#define BATcalccstop BATcalccstne
#define VARcalcop VARcalcne

#include "gdk_calc_compare.h"

#undef OP
#undef opswitch
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop


#undef tpe
#undef tpe_nil
#undef TYPE_tpe

/* ---------------------------------------------------------------------- */
/* generic comparison (any "linear" type) */

#define tpe bit
#define tpe_nil bit_nil
#define TYPE_tpe TYPE_bit

#define OP CMP
#define opswitch cmpswitch
#define BATcalcop BATcalccmp
#define BATcalcopcst BATcalccmpcst
#define BATcalccstop BATcalccstcmp
#define VARcalcop VARcalccmp

#include "gdk_calc_compare.h"

#undef OP
#undef opswitch
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

#undef tpe
#undef tpe_nil
#undef TYPE_tpe

/* ---------------------------------------------------------------------- */
/* between (any "linear" type) */

#define BETWEEN(v, lo, hi)	((bit) (((lo) <= (v) && (v) <= (hi)) || \
					(symmetric &&			\
					 (hi) <= (v) && (v) <= (lo))))

#define TRIADICLOOP(TYPE1, TYPE2, TYPE3, FUNC)				\
	do {								\
		BUN i;							\
		TYPE1 v1 = v1p ? * (const TYPE1 *) v1p : 0;		\
		TYPE2 v2 = v2p ? * (const TYPE2 *) v2p : 0;		\
		TYPE2 v3 = v3p ? * (const TYPE2 *) v3p : 0;		\
		for (i = 0; i < cnt; i++) {				\
			if (src1)					\
				v1 = ((const TYPE1 *) src1)[projected == 1 ? i : cand ? cand[i] - hoff : start + i]; \
			if (src2)					\
				v2 = ((const TYPE2 *) src2)[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
			if (src3)					\
				v3 = ((const TYPE2 *) src3)[projected == 2 ? i : cand ? cand[i] - hoff : start + i]; \
			if (v1 == TYPE1##_nil || v2 == TYPE2##_nil || v3 == TYPE2##_nil) {	\
				((TYPE3 *) dst)[i] = TYPE3##_nil;	\
				nils++;					\
			} else {					\
				((TYPE3 *) dst)[i] = FUNC(v1, v2, v3);	\
			}						\
		}							\
	} while (0)

static BUN
betweenswitch(BAT *b1, const void *src1, const void *v1p,
	      BAT *b2, const void *src2, const void *v2p,
	      BAT *b3, const void *src3, const void *v3p,
	      bit *restrict dst, int tp, oid hoff, BUN start, BUN cnt,
	      const oid *restrict cand, int symmetric,
	      int projected)
{
	BUN nils = 0;

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		TRIADICLOOP(bte, bte, bit, BETWEEN);
		break;
	case TYPE_sht:
		TRIADICLOOP(sht, sht, bit, BETWEEN);
		break;
	case TYPE_int:
		TRIADICLOOP(int, int, bit, BETWEEN);
		break;
	case TYPE_lng:
		TRIADICLOOP(lng, lng, bit, BETWEEN);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		TRIADICLOOP(hge, hge, bit, BETWEEN);
		break;
#endif
	case TYPE_flt:
		TRIADICLOOP(flt, flt, bit, BETWEEN);
		break;
	case TYPE_dbl:
		TRIADICLOOP(dbl, dbl, bit, BETWEEN);
		break;
	default: {
		BATiter b1i;
		BATiter b2i;
		BATiter b3i;
		BUN i;
		int (*cmp)(const void *, const void *);
		const void *nil;

		b1i = bat_iterator(b1);
		b2i = bat_iterator(b2);
		b3i = bat_iterator(b3);
		cmp = ATOMcompare(tp);
		nil = ATOMnilptr(tp);

		for (i = 0; i < cnt; i++) {
			if (b1)
				v1p = BUNtail(b1i, projected == 1 ? i : cand ? cand[i] - hoff : start + i);
			if (b2)
				v2p = BUNtail(b2i, projected == 2 ? i : cand ? cand[i] - hoff : start + i);
			if (b3)
				v3p = BUNtail(b3i, projected == 2 ? i : cand ? cand[i] - hoff : start + i);
			if (cmp(v1p, nil) == 0 ||
			    cmp(v2p, nil) == 0 ||
			    cmp(v3p, nil) == 0) {
				dst[i] = bit_nil;
				nils++;
			} else {
				dst[i] = (bit) ((cmp(v1p, v2p) >= 0 &&
						 cmp(v1p, v3p) <= 0) ||
						(symmetric &&
						 cmp(v1p, v2p) <= 0 &&
						 cmp(v1p, v3p) >= 0));
			}
		}
		break;
	}
	}

	return nils;
}

BAT *
BATcalcbetween(BAT *b, BAT *lo, BAT *hi, BAT *s, int symmetric, int projected)
{
	BAT *bn;
	CALC_DECL;

	/* if both lo and hi are empty, the hseqbases do not have to
	 * be equal */
	if ((lo->hseqbase != hi->hseqbase && BATcount(lo) != 0) ||
	    BATcount(lo) != BATcount(hi)) {
		GDKerror("%s: lo and hi BATs are not aligned.\n", __func__);
		return NULL;
	}

	if (ATOMbasetype(ATOMtype(b->ttype)) != ATOMbasetype(ATOMtype(lo->ttype)) ||
	    ATOMbasetype(ATOMtype(b->ttype)) != ATOMbasetype(ATOMtype(hi->ttype))) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b, lo, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b, lo);

	if (b->ttype == TYPE_void &&
	    lo->ttype == TYPE_void &&
	    hi->ttype == TYPE_void) {
		if (b->tseqbase == oid_nil ||
		    lo->tseqbase == oid_nil ||
		    hi->tseqbase == oid_nil) {
			bit v = bit_nil;
			return BATconstant(hseq, TYPE_bit, &v, cnt, TRANSIENT);
		} else if (projected == 0) {
			bit v = BETWEEN(b->tseqbase, lo->tseqbase, hi->tseqbase);
			return BATconstant(hseq, TYPE_bit, &v, cnt, TRANSIENT);
		}
	}

	bn = COLnew(hseq, TYPE_bit, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = betweenswitch(b, Tloc(b, 0), NULL,
			     lo, Tloc(lo, 0), NULL, hi, Tloc(hi, 0), NULL,
			     (bit *) Tloc(bn, 0),
			     (b->ttype == TYPE_void ||
			      lo->ttype == TYPE_void ||
			      hi->ttype == TYPE_void) ? TYPE_void : b->ttype,
			     hoff, start, cnt,
			     cand, symmetric, projected);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcbetweencstcst(BAT *b, const ValRecord *lo, const ValRecord *hi, BAT *s, int symmetric)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(ATOMtype(b->ttype)) != ATOMbasetype(lo->vtype) ||
	    ATOMbasetype(ATOMtype(b->ttype)) != ATOMbasetype(hi->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	if (b->ttype == TYPE_void && b->tseqbase == oid_nil) {
		bit v = bit_nil;
		return BATconstant(hseq, TYPE_bit, &v, cnt, TRANSIENT);
	}

	bn = COLnew(hseq, TYPE_bit, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = betweenswitch(b, Tloc(b, 0), NULL,
			     NULL, NULL, VALptr(lo),
			     NULL, NULL, VALptr(hi),
			     (bit *) Tloc(bn, 0), b->ttype, hoff, start, cnt,
			     cand, symmetric, 0);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcbetweenbatcst(BAT *b, BAT *lo, const ValRecord *hi, BAT *s, int symmetric, int projected)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(ATOMtype(b->ttype)) != ATOMbasetype(ATOMtype(lo->ttype)) ||
	    ATOMbasetype(ATOMtype(b->ttype)) != ATOMbasetype(hi->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b, lo, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b, lo);

	if ((b->ttype == TYPE_void && b->tseqbase == oid_nil) ||
	    (lo->ttype == TYPE_void && lo->tseqbase == oid_nil)) {
		bit v = bit_nil;
		return BATconstant(hseq, TYPE_bit, &v, cnt, TRANSIENT);
	}

	bn = COLnew(hseq, TYPE_bit, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = betweenswitch(b, Tloc(b, 0), NULL,
			     lo, Tloc(lo, 0), NULL, NULL, NULL, VALptr(hi),
			     (bit *) Tloc(bn, 0),
			     (b->ttype == TYPE_void ||
			      lo->ttype == TYPE_void) ? TYPE_void : b->ttype,
			     hoff, start, cnt,
			     cand, symmetric, projected);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcbetweencstbat(BAT *b, const ValRecord *lo, BAT *hi, BAT *s, int symmetric, int projected)
{
	BAT *bn;
	CALC_DECL;

	if (ATOMbasetype(ATOMtype(b->ttype)) != ATOMbasetype(lo->vtype) ||
	    ATOMbasetype(ATOMtype(b->ttype)) != ATOMbasetype(ATOMtype(hi->ttype))) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b, hi, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b, hi);

	if ((b->ttype == TYPE_void && b->tseqbase == oid_nil) ||
	    (hi->ttype == TYPE_void && hi->tseqbase == oid_nil)) {
		bit v = bit_nil;
		return BATconstant(hseq, TYPE_bit, &v, cnt, TRANSIENT);
	}

	bn = COLnew(hseq, TYPE_bit, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = betweenswitch(b, Tloc(b, 0), NULL,
			     NULL, NULL, VALptr(lo), hi, Tloc(hi, 0), NULL,
			     (bit *) Tloc(bn, 0),
			     (b->ttype == TYPE_void ||
			      hi->ttype == TYPE_void) ? TYPE_void : b->ttype,
			     hoff, start, cnt,
			     cand, symmetric, projected);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

gdk_return
VARcalcbetween(ValPtr ret, const ValRecord *v, const ValRecord *lo,
	       const ValRecord *hi, int symmetric)
{
	assert(ret->vtype == TYPE_bit);
	if (ATOMbasetype(v->vtype) != ATOMbasetype(lo->vtype) ||
	    ATOMbasetype(v->vtype) != ATOMbasetype(hi->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return GDK_FAIL;
	}

	if (betweenswitch(NULL, NULL, VALptr(v),
			  NULL, NULL, VALptr(lo),
			  NULL, NULL, VALptr(hi),
			  VALget(ret), v->vtype, 0, 0, 1,
			  NULL, symmetric, 0) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* if-then-else (any type) */

#define IFTHENELSE(b, v1, v2)	((b) ? (v1) : (v2))

static BUN
ifthenelseswitch(const bit *src1, const bit *v1p,
		 BAT *b2, const void *src2, const void *v2p,
		 BAT *b3, const void *src3, const void *v3p,
		 BAT *bn, void *restrict dst, int tp,
		 oid hoff, BUN start, BUN cnt,
		 const oid *restrict cand, int projected)
{
	BUN nils = 0;

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		TRIADICLOOP(bit, bte, bte, IFTHENELSE);
		break;
	case TYPE_sht:
		TRIADICLOOP(bit, sht, sht, IFTHENELSE);
		break;
	case TYPE_int:
		TRIADICLOOP(bit, int, int, IFTHENELSE);
		break;
	case TYPE_lng:
		TRIADICLOOP(bit, lng, lng, IFTHENELSE);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		TRIADICLOOP(bit, hge, hge, IFTHENELSE);
		break;
#endif
	case TYPE_flt:
		TRIADICLOOP(bit, flt, flt, IFTHENELSE);
		break;
	case TYPE_dbl:
		TRIADICLOOP(bit, dbl, dbl, IFTHENELSE);
		break;
	default: {
		BATiter b2i;
		BATiter b3i;
		BUN i;
		int (*cmp)(const void *, const void *);
		const void *nil;
		bit v = v1p ? *v1p : 0;

		b2i = bat_iterator(b2);
		b3i = bat_iterator(b3);
		cmp = ATOMcompare(tp);
		nil = ATOMnilptr(tp);

		for (i = 0; i < cnt; i++) {
			if (src1)
				v = src1[projected == 1 ? i : cand ? cand[i] - hoff : start + i];
			if (v == bit_nil) {
				tfastins_nocheck(bn, i, nil, Tsize(bn));
				nils++;
			} else {
				const void *p;
				p = IFTHENELSE(v,
					       b2 ? BUNtail(b2i, projected == 2 ? i : cand ? cand[i] - hoff : start + i) : v2p,
					       b3 ? BUNtail(b3i, projected == 2 ? i : cand ? cand[i] - hoff : start + i) : v3p);
				tfastins_nocheck(bn, i, p, Tsize(bn));
				nils += cmp(p, nil) == 0;
			}
		}
		break;
	}
	}

	return nils;

  bunins_failed:
	return BUN_NONE;
}

BAT *
BATcalcifthenelse(BAT *b, BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;

	/* if both b1 and b2 are empty, the hseqbases do not have to
	 * be equal */
	if ((b1->hseqbase != b2->hseqbase && BATcount(b1) != 0) ||
	    BATcount(b1) != BATcount(b2)) {
		GDKerror("%s: then and else BATs are not aligned.\n", __func__);
		return NULL;
	}

	if (b->ttype != TYPE_bit ||
	    ATOMbasetype(ATOMtype(b1->ttype)) != ATOMbasetype(ATOMtype(b2->ttype))) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b, b1, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b, b1);

	bn = COLnew(hseq, ATOMtype(b1->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = ifthenelseswitch((bit *) Tloc(b, 0), NULL,
				b1, Tloc(b1, 0), NULL,
				b2, Tloc(b2, 0), NULL,
				bn, Tloc(bn, 0),
				(b1->ttype == TYPE_void ||
				 b2->ttype == TYPE_void) ? TYPE_void : b1->ttype,
				hoff, start, cnt, cand, projected);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcifthencstelsecst(BAT *b, const ValRecord *v1, const ValRecord *v2, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	if (b->ttype != TYPE_bit ||
	    ATOMbasetype(v1->vtype) != ATOMbasetype(v2->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_INIT(b);

	if (b->ttype == TYPE_void && b->tseqbase == oid_nil) {
		bit v = bit_nil;
		return BATconstant(hseq, TYPE_bit, &v, cnt, TRANSIENT);
	}

	bn = COLnew(hseq, v1->vtype, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = ifthenelseswitch((bit *) Tloc(b, 0), NULL,
				NULL, NULL, VALptr(v1),
				NULL, NULL, VALptr(v2),
				bn, Tloc(bn, 0), v1->vtype, hoff, start, cnt,
				cand, 0);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcifthenelsecst(BAT *b, BAT *b1, const ValRecord *v2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;

	if (b->ttype != TYPE_bit ||
	    ATOMbasetype(ATOMtype(b1->ttype)) != ATOMbasetype(v2->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b, b1, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b, b1);

	bn = COLnew(hseq, ATOMtype(b1->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = ifthenelseswitch((bit *) Tloc(b, 0), NULL,
				b1, Tloc(b1, 0), NULL, NULL, NULL, VALptr(v2),
				bn, Tloc(bn, 0),
				b1->ttype, hoff, start, cnt, cand, projected);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

BAT *
BATcalcifthencstelse(BAT *b, const ValRecord *v1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;

	if (b->ttype != TYPE_bit ||
	    ATOMbasetype(v1->vtype) != ATOMbasetype(ATOMtype(b2->ttype))) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	CALC_PREINIT(b, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b, b2);

	bn = COLnew(hseq, ATOMtype(b2->ttype), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = ifthenelseswitch((bit *) Tloc(b, 0), NULL,
				NULL, NULL, VALptr(v1), b2, Tloc(b2, 0), NULL,
				bn, Tloc(bn, 0),
				b2->ttype, hoff, start, cnt, cand, projected);

	if (nils == BUN_NONE) {
		BBPreclaim(bn);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	return bn;
}

/* ---------------------------------------------------------------------- */
/* type conversion (cast) */

#define CONV_OVERFLOW(TYPE1, TYPE2, value)				\
	do {								\
		GDKerror("22003!overflow in conversion of "		\
			 FMT##TYPE1 " to %s.\n", CST##TYPE1 (value), #TYPE2); \
		return BUN_NONE;					\
	} while (0)

#define CONVERT_enlarge(TYPE1, TYPE2)					\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  const oid *restrict cand, BUN start, BUN cnt, \
			  oid hoff, TYPE2 min, TYPE2 max, int abort_on_error) \
{									\
	TYPE1 v1;							\
	BUN nils = 0;							\
	BUN i;								\
									\
	for (i = 0; i < cnt; i++) {					\
		v1 = src[cand ? cand[i] - hoff : start + i];		\
		if (v1 == TYPE1##_nil) {				\
			dst[i] = TYPE2##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (TYPE2) v1;				\
			if (dst[i] < min || dst[i] > max) {		\
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, TYPE2, v1); \
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

#define CONVERT_reduce(TYPE1, TYPE2)					\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  const oid *restrict cand, BUN start, BUN cnt, \
			  oid hoff, TYPE2 min, TYPE2 max, int abort_on_error) \
{									\
	TYPE1 v1;							\
	BUN nils = 0;							\
	BUN i;								\
									\
	for (i = 0; i < cnt; i++) {					\
		v1 = src[cand ? cand[i] - hoff : start + i];		\
		if (v1 == TYPE1##_nil) {				\
			dst[i] = TYPE2##_nil;				\
			nils++;						\
		} else if (v1 < (TYPE1) min || v1 > (TYPE1) max) {	\
			if (abort_on_error)				\
				CONV_OVERFLOW(TYPE1, TYPE2, v1);	\
			dst[i] = TYPE2##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (TYPE2) v1;				\
		}							\
	}								\
	return nils;							\
}

#ifdef TRUNCATE_NUMBERS
#define roundflt(x)	(x)
#define rounddbl(x)	(x)
#else
#define roundflt(x)	roundf(x)
#define rounddbl(x)	round(x)
#endif

#ifndef HAVE_ROUND
static inline double
round(double val)
{
	/* round to nearest integer, away from zero */
	if (val < 0)
		return -floor(-val + 0.5);
	else
		return floor(val + 0.5);
}
#define roundf(x)	((float)round((double)(x)))
#endif

#define CONVERT_float(TYPE1, TYPE2)					\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  const oid *restrict cand, BUN start, BUN cnt, \
			  oid hoff, TYPE2 min, TYPE2 max, int abort_on_error) \
{									\
	TYPE1 v1;							\
	BUN nils = 0;							\
	BUN i;								\
									\
	for (i = 0; i < cnt; i++) {					\
		v1 = src[cand ? cand[i] - hoff : start + i];		\
		if (v1 == TYPE1##_nil) {				\
			dst[i] = TYPE2##_nil;				\
			nils++;						\
		} else if (v1 < (TYPE1) min || v1 > (TYPE1) max) {	\
			if (abort_on_error)				\
				CONV_OVERFLOW(TYPE1, TYPE2, v1);	\
			dst[i] = TYPE2##_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (TYPE2) round##TYPE1(v1);		\
		}							\
	}								\
	return nils;							\
}

#define CONVERT_2bit(TYPE)						\
static BUN								\
convert_##TYPE##_bit(const TYPE *src, bit *restrict dst,		\
		     const oid *restrict cand, BUN start, BUN cnt,	\
		     oid hoff, bit min, bit max)			\
{									\
	TYPE v1;							\
	BUN nils = 0;							\
	BUN i;								\
									\
	(void) min; (void) max;						\
	for (i = 0; i < cnt; i++) {					\
		v1 = src[cand ? cand[i] - hoff : start + i];		\
		if (v1 == TYPE##_nil) {					\
			dst[i] = bit_nil;				\
			nils++;						\
		} else {						\
			dst[i] = (bit) (v1 != 0);			\
		}							\
	}								\
	return nils;							\
}

CONVERT_2bit(bte)
CONVERT_reduce(bte, bte)
CONVERT_enlarge(bte, sht)
CONVERT_enlarge(bte, int)
CONVERT_enlarge(bte, lng)
#ifdef HAVE_HGE
CONVERT_enlarge(bte, hge)
#endif
CONVERT_enlarge(bte, flt)
CONVERT_enlarge(bte, dbl)

CONVERT_2bit(sht)
CONVERT_reduce(sht, bte)
CONVERT_reduce(sht, sht)
CONVERT_enlarge(sht, int)
CONVERT_enlarge(sht, lng)
#ifdef HAVE_HGE
CONVERT_enlarge(sht, hge)
#endif
CONVERT_enlarge(sht, flt)
CONVERT_enlarge(sht, dbl)

CONVERT_2bit(int)
CONVERT_reduce(int, bte)
CONVERT_reduce(int, sht)
CONVERT_reduce(int, int)
CONVERT_enlarge(int, lng)
#ifdef HAVE_HGE
CONVERT_enlarge(int, hge)
#endif
CONVERT_enlarge(int, flt)
CONVERT_enlarge(int, dbl)

CONVERT_2bit(lng)
CONVERT_reduce(lng, bte)
CONVERT_reduce(lng, sht)
CONVERT_reduce(lng, int)
CONVERT_reduce(lng, lng)
#ifdef HAVE_HGE
CONVERT_enlarge(lng, hge)
#endif
CONVERT_enlarge(lng, flt)
CONVERT_enlarge(lng, dbl)

#ifdef HAVE_HGE
CONVERT_2bit(hge)
CONVERT_reduce(hge, bte)
CONVERT_reduce(hge, sht)
CONVERT_reduce(hge, int)
CONVERT_reduce(hge, lng)
CONVERT_reduce(hge, hge)
CONVERT_enlarge(hge, flt)
CONVERT_enlarge(hge, dbl)
#endif

CONVERT_2bit(flt)
CONVERT_float(flt, bte)
CONVERT_float(flt, sht)
CONVERT_float(flt, int)
CONVERT_float(flt, lng)
#ifdef HAVE_HGE
CONVERT_float(flt, hge)
#endif
CONVERT_reduce(flt, flt)
CONVERT_enlarge(flt, dbl)

CONVERT_2bit(dbl)
CONVERT_float(dbl, bte)
CONVERT_float(dbl, sht)
CONVERT_float(dbl, int)
CONVERT_float(dbl, lng)
#ifdef HAVE_HGE
CONVERT_float(dbl, hge)
#endif
CONVERT_reduce(dbl, flt)
CONVERT_reduce(dbl, dbl)

static BUN
convert_any_str(BAT *b, BAT *bn, const oid *restrict cand, BUN start, BUN cnt, oid hoff)
{
	char *dst = NULL;
	const void *src;
	int len = 0;
	BUN nils = 0;
	BUN i;
	BATiter bi = bat_iterator(b);
	int (*atomtostr)(str *, int *, const void *) = BATatoms[b->ttype].atomToStr;
	int (*atomcmp)(const void *, const void *) = ATOMcompare(b->ttype);
	const void *nil = ATOMnilptr(b->ttype);

	for (i = 0; i < cnt; i++) {
		src = BUNtail(bi, cand ? cand[i] - hoff : start + i);
		if ((*atomcmp)(src, nil) == 0) {
			tfastins_nocheck(bn, i, str_nil, bn->twidth);
			nils++;
		} else {
			(*atomtostr)(&dst, &len, src);
			tfastins_nocheck(bn, i, dst, bn->twidth);
		}
	}
	GDKfree(dst);
	return nils;
  bunins_failed:
	GDKfree(dst);
	return BUN_NONE + 2;
}

static BUN
convert_str_any(BAT *b, BAT *bn, const oid *restrict cand, BUN start, BUN cnt, oid hoff, int abort_on_error)
{
	void *dst = NULL;
	const char *src;
	int len = 0, l;
	BUN nils = 0;
	BUN i;
	BATiter bi = bat_iterator(b);
	int (*atomfromstr)(const char *, int *, ptr *) = BATatoms[bn->ttype].atomFromStr;
	int (*atomcmp)(const void *, const void *) = ATOMcompare(bn->ttype);
	const void *nil = ATOMnilptr(bn->ttype);

	for (i = 0; i < cnt; i++) {
		src = BUNtvar(bi, cand ? cand[i] - hoff : start + i);
		if (strcmp(src, str_nil) == 0) {
			tfastins_nocheck(bn, i, nil, bn->twidth);
			nils++;
		} else {
			if ((l = (*atomfromstr)(src, &len, &dst)) <= 0 ||
			    l < (int) strlen(src)) {
				if (abort_on_error) {
					GDKerror("22018!conversion of string "
						 "'%s' to type %s failed.\n",
						 src, ATOMname(bn->ttype));
					GDKfree(dst);
					return BUN_NONE;
				}
				tfastins_nocheck(bn, i, nil, bn->twidth);
				nils++;
			} else {
				tfastins_nocheck(bn, i, dst, bn->twidth);
				nils += (*atomcmp)(dst, nil) == 0;
			}
		}
	}
	GDKfree(dst);
	return nils;
  bunins_failed:
	GDKfree(dst);
	return BUN_NONE + 2;
}

static BUN
convertswitch(const void *src, int tp1, void *restrict dst, int tp, const oid *restrict cand, BUN start, BUN cnt, oid hoff, int abort_on_error)
{
	switch (ATOMbasetype(tp1)) {
	case TYPE_bte:
		switch (ATOMbasetype(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit)
				return convert_bte_bit(src, dst, cand, start, cnt, hoff, 0, 1);
			return convert_bte_bte(src, dst, cand, start, cnt, hoff, GDK_bte_min+1, GDK_bte_max, abort_on_error);
		case TYPE_sht:
			return convert_bte_sht(src, dst, cand, start, cnt, hoff, GDK_sht_min+1, GDK_sht_max, abort_on_error);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp == TYPE_oid)
				return convert_bte_int(src, dst, cand, start, cnt, hoff, 0, GDK_int_max, abort_on_error);
#endif
			return convert_bte_int(src, dst, cand, start, cnt, hoff, GDK_int_min+1, GDK_int_max, abort_on_error);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp == TYPE_oid)
				return convert_bte_lng(src, dst, cand, start, cnt, hoff, 0, GDK_lng_max, abort_on_error);
#endif
			return convert_bte_lng(src, dst, cand, start, cnt, hoff, GDK_lng_min+1, GDK_lng_max, abort_on_error);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_bte_hge(src, dst, cand, start, cnt, hoff, GDK_hge_min+1, GDK_hge_max, abort_on_error);
#endif
		case TYPE_flt:
			return convert_bte_flt(src, dst, cand, start, cnt, hoff, nextafterf(GDK_flt_min, 0), GDK_flt_max, abort_on_error);
		case TYPE_dbl:
			return convert_bte_dbl(src, dst, cand, start, cnt, hoff, nextafter(GDK_dbl_min, 0), GDK_dbl_max, abort_on_error);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_sht:
		switch (ATOMbasetype(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit)
				return convert_sht_bit(src, dst, cand, start, cnt, hoff, 0, 1);
			return convert_sht_bte(src, dst, cand, start, cnt, hoff, GDK_bte_min+1, GDK_bte_max, abort_on_error);
		case TYPE_sht:
			return convert_sht_sht(src, dst, cand, start, cnt, hoff, GDK_sht_min+1, GDK_sht_max, abort_on_error);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp == TYPE_oid)
				return convert_sht_int(src, dst, cand, start, cnt, hoff, 0, GDK_int_max, abort_on_error);
#endif
			return convert_sht_int(src, dst, cand, start, cnt, hoff, GDK_int_min+1, GDK_int_max, abort_on_error);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp == TYPE_oid)
				return convert_sht_lng(src, dst, cand, start, cnt, hoff, 0, GDK_lng_max, abort_on_error);
#endif
			return convert_sht_lng(src, dst, cand, start, cnt, hoff, GDK_lng_min+1, GDK_lng_max, abort_on_error);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_sht_hge(src, dst, cand, start, cnt, hoff, GDK_hge_min+1, GDK_hge_max, abort_on_error);
#endif
		case TYPE_flt:
			return convert_sht_flt(src, dst, cand, start, cnt, hoff, nextafterf(GDK_flt_min, 0), GDK_flt_max, abort_on_error);
		case TYPE_dbl:
			return convert_sht_dbl(src, dst, cand, start, cnt, hoff, nextafter(GDK_dbl_min, 0), GDK_dbl_max, abort_on_error);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_int:
		switch (ATOMbasetype(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit)
				return convert_int_bit(src, dst, cand, start, cnt, hoff, 0, 1);
			return convert_int_bte(src, dst, cand, start, cnt, hoff, GDK_bte_min+1, GDK_bte_max, abort_on_error);
		case TYPE_sht:
			return convert_int_sht(src, dst, cand, start, cnt, hoff, GDK_sht_min+1, GDK_sht_max, abort_on_error);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp == TYPE_oid)
				return convert_int_int(src, dst, cand, start, cnt, hoff, 0, GDK_int_max, abort_on_error);
#endif
			return convert_int_int(src, dst, cand, start, cnt, hoff, GDK_int_min+1, GDK_int_max, abort_on_error);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp == TYPE_oid)
				return convert_int_lng(src, dst, cand, start, cnt, hoff, 0, GDK_lng_max, abort_on_error);
#endif
			return convert_int_lng(src, dst, cand, start, cnt, hoff, GDK_lng_min+1, GDK_lng_max, abort_on_error);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_int_hge(src, dst, cand, start, cnt, hoff, GDK_hge_min+1, GDK_hge_max, abort_on_error);
#endif
		case TYPE_flt:
			return convert_int_flt(src, dst, cand, start, cnt, hoff, nextafterf(GDK_flt_min, 0), GDK_flt_max, abort_on_error);
		case TYPE_dbl:
			return convert_int_dbl(src, dst, cand, start, cnt, hoff, nextafter(GDK_dbl_min, 0), GDK_dbl_max, abort_on_error);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_lng:
		switch (ATOMbasetype(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit)
				return convert_lng_bit(src, dst, cand, start, cnt, hoff, 0, 1);
			return convert_lng_bte(src, dst, cand, start, cnt, hoff, GDK_bte_min+1, GDK_bte_max, abort_on_error);
		case TYPE_sht:
			return convert_lng_sht(src, dst, cand, start, cnt, hoff, GDK_sht_min+1, GDK_sht_max, abort_on_error);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp == TYPE_oid)
				return convert_lng_int(src, dst, cand, start, cnt, hoff, 0, GDK_int_max, abort_on_error);
#endif
			return convert_lng_int(src, dst, cand, start, cnt, hoff, GDK_int_min+1, GDK_int_max, abort_on_error);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp == TYPE_oid)
				return convert_lng_lng(src, dst, cand, start, cnt, hoff, 0, GDK_lng_max, abort_on_error);
#endif
			return convert_lng_lng(src, dst, cand, start, cnt, hoff, GDK_lng_min+1, GDK_lng_max, abort_on_error);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_lng_hge(src, dst, cand, start, cnt, hoff, GDK_hge_min+1, GDK_hge_max, abort_on_error);
#endif
		case TYPE_flt:
			return convert_lng_flt(src, dst, cand, start, cnt, hoff, nextafterf(GDK_flt_min, 0), GDK_flt_max, abort_on_error);
		case TYPE_dbl:
			return convert_lng_dbl(src, dst, cand, start, cnt, hoff, nextafter(GDK_dbl_min, 0), GDK_dbl_max, abort_on_error);
		default:
			return BUN_NONE + 1;
		}
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (ATOMbasetype(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit)
				return convert_hge_bit(src, dst, cand, start, cnt, hoff, 0, 1);
			return convert_hge_bte(src, dst, cand, start, cnt, hoff, GDK_bte_min+1, GDK_bte_max, abort_on_error);
		case TYPE_sht:
			return convert_hge_sht(src, dst, cand, start, cnt, hoff, GDK_sht_min+1, GDK_sht_max, abort_on_error);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp == TYPE_oid)
				return convert_hge_int(src, dst, cand, start, cnt, hoff, 0, GDK_int_max, abort_on_error);
#endif
			return convert_hge_int(src, dst, cand, start, cnt, hoff, GDK_int_min+1, GDK_int_max, abort_on_error);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp == TYPE_oid)
				return convert_hge_lng(src, dst, cand, start, cnt, hoff, 0, GDK_lng_max, abort_on_error);
#endif
			return convert_hge_lng(src, dst, cand, start, cnt, hoff, GDK_lng_min+1, GDK_lng_max, abort_on_error);
		case TYPE_hge:
			return convert_hge_hge(src, dst, cand, start, cnt, hoff, GDK_hge_min+1, GDK_hge_max, abort_on_error);
		case TYPE_flt:
			return convert_hge_flt(src, dst, cand, start, cnt, hoff, nextafterf(GDK_flt_min, 0), GDK_flt_max, abort_on_error);
		case TYPE_dbl:
			return convert_hge_dbl(src, dst, cand, start, cnt, hoff, nextafter(GDK_dbl_min, 0), GDK_dbl_max, abort_on_error);
		default:
			return BUN_NONE + 1;
		}
#endif
	case TYPE_flt:
		switch (ATOMbasetype(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit)
				return convert_flt_bit(src, dst, cand, start, cnt, hoff, 0, 1);
			return convert_flt_bte(src, dst, cand, start, cnt, hoff, GDK_bte_min+1, GDK_bte_max, abort_on_error);
		case TYPE_sht:
			return convert_flt_sht(src, dst, cand, start, cnt, hoff, GDK_sht_min+1, GDK_sht_max, abort_on_error);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp == TYPE_oid)
				return convert_flt_int(src, dst, cand, start, cnt, hoff, 0, GDK_int_max, abort_on_error);
#endif
			return convert_flt_int(src, dst, cand, start, cnt, hoff, GDK_int_min+1, GDK_int_max, abort_on_error);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp == TYPE_oid)
				return convert_flt_lng(src, dst, cand, start, cnt, hoff, 0, GDK_lng_max, abort_on_error);
#endif
			return convert_flt_lng(src, dst, cand, start, cnt, hoff, GDK_lng_min+1, GDK_lng_max, abort_on_error);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_flt_hge(src, dst, cand, start, cnt, hoff, GDK_hge_min+1, GDK_hge_max, abort_on_error);
#endif
		case TYPE_flt:
			return convert_flt_flt(src, dst, cand, start, cnt, hoff, nextafterf(GDK_flt_min, 0), GDK_flt_max, abort_on_error);
		case TYPE_dbl:
			return convert_flt_dbl(src, dst, cand, start, cnt, hoff, nextafter(GDK_dbl_min, 0), GDK_dbl_max, abort_on_error);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_dbl:
		switch (ATOMbasetype(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit)
				return convert_dbl_bit(src, dst, cand, start, cnt, hoff, 0, 1);
			return convert_dbl_bte(src, dst, cand, start, cnt, hoff, GDK_bte_min+1, GDK_bte_max, abort_on_error);
		case TYPE_sht:
			return convert_dbl_sht(src, dst, cand, start, cnt, hoff, GDK_sht_min+1, GDK_sht_max, abort_on_error);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp == TYPE_oid)
				return convert_dbl_int(src, dst, cand, start, cnt, hoff, 0, GDK_int_max, abort_on_error);
#endif
			return convert_dbl_int(src, dst, cand, start, cnt, hoff, GDK_int_min+1, GDK_int_max, abort_on_error);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp == TYPE_oid)
				return convert_dbl_lng(src, dst, cand, start, cnt, hoff, 0, GDK_lng_max, abort_on_error);
#endif
			return convert_dbl_lng(src, dst, cand, start, cnt, hoff, GDK_lng_min+1, GDK_lng_max, abort_on_error);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_dbl_hge(src, dst, cand, start, cnt, hoff, GDK_hge_min+1, GDK_hge_max, abort_on_error);
#endif
		case TYPE_flt:
			return convert_dbl_flt(src, dst, cand, start, cnt, hoff, nextafterf(GDK_flt_min, 0), GDK_flt_max, abort_on_error);
		case TYPE_dbl:
			return convert_dbl_dbl(src, dst, cand, start, cnt, hoff, nextafter(GDK_dbl_min, 0), GDK_dbl_max, abort_on_error);
		default:
			return BUN_NONE + 1;
		}
	default:
		return BUN_NONE + 1;
	}
}

static BUN
convert_void_any(oid tseq, void *restrict dst, int tp, const oid *restrict cand, BUN start, BUN cnt, oid hoff, int abort_on_error, const char *func)
{
	BUN nils = 0;
	BUN i;

	if (cand) {
#if SIZEOF_OID == SIZEOF_INT
		int off = (int) tseq - (int) hoff;
		return addswitch(cand, TYPE_int, NULL, TYPE_int, dst, tp, NULL, &off, 0, 0, cnt, NULL, abort_on_error, 1, func);
#else
		lng off = (lng) tseq - (lng) hoff;
		return addswitch(cand, TYPE_lng, NULL, TYPE_lng, dst, tp, NULL, &off, 0, 0, cnt, NULL, abort_on_error, 1, func);
#endif
	}
	if (tp == TYPE_bit) {
		for (i = 0; i < cnt; i++)
			((bit *) dst)[i] = 1;
		if (tseq == 0 && start == 0 && cnt > 0)
			((bit *) dst)[0] = 0;
		return 0;
	}
	/* below we repurpose hoff since it isn't needed for its
	 * original purpose */
	tseq += start;
	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tseq + cnt - 1 > (oid) GDK_bte_max) {
			if (tseq > (oid) GDK_bte_max)
				hoff = 0;
			else
				hoff = (oid) GDK_bte_max + 1 - tseq;
			if (abort_on_error)
				CONV_OVERFLOW(oid, bte, tseq + hoff);
			for (i = hoff; i < cnt; i++)
				((bte *) dst)[i] = bte_nil;
			nils = cnt - hoff;
			cnt = hoff;
		}
		for (i = 0; i < cnt; i++)
			((bte *) dst)[i] = (bte) (tseq + i);
		break;
	case TYPE_sht:
		if (tseq + cnt - 1 > (oid) GDK_sht_max) {
			if (tseq > (oid) GDK_sht_max)
				hoff = 0;
			else
				hoff = (oid) GDK_sht_max + 1 - tseq;
			if (abort_on_error)
				CONV_OVERFLOW(oid, sht, tseq + hoff);
			for (i = hoff; i < cnt; i++)
				((sht *) dst)[i] = sht_nil;
			nils = cnt - hoff;
			cnt = hoff;
		}
		for (i = 0; i < cnt; i++)
			((sht *) dst)[i] = (sht) (tseq + i);
		break;
	case TYPE_int:
#if SIZEOF_OID > SIZEOF_INT
		if (tseq + cnt - 1 > (oid) GDK_int_max) {
			if (tseq > (oid) GDK_int_max)
				hoff = 0;
			else
				hoff = (oid) GDK_int_max + 1 - tseq;
			if (abort_on_error)
				CONV_OVERFLOW(oid, int, tseq + hoff);
			for (i = hoff; i < cnt; i++)
				((int *) dst)[i] = int_nil;
			nils = cnt - hoff;
			cnt = hoff;
		}
#endif
		for (i = 0; i < cnt; i++)
			((int *) dst)[i] = (int) (tseq + i);
		break;
	case TYPE_lng:
		for (i = 0; i < cnt; i++)
			((lng *) dst)[i] = (lng) (tseq + i);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		for (i = 0; i < cnt; i++)
			((hge *) dst)[i] = (hge) (tseq + i);
		break;
#endif
	case TYPE_flt:
		for (i = 0; i < cnt; i++)
			((flt *) dst)[i] = (flt) (tseq + i);
		break;
	case TYPE_dbl:
		for (i = 0; i < cnt; i++)
			((dbl *) dst)[i] = (dbl) (tseq + i);
		break;
	default:
		return BUN_NONE + 1;
	}
	return nils;
}

BAT *
BATconvert(BAT *b, BAT *s, int tp, int abort_on_error)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);
	if (tp == TYPE_void)
		tp = TYPE_oid;

	if (b->ttype == TYPE_void && b->tseqbase == oid_nil)
		return BATconstant(hseq, tp, ATOMnilptr(tp), cnt, TRANSIENT);

	bn = COLnew(hseq, tp, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	if (b->ttype == TYPE_void)
		nils = convert_void_any(b->tseqbase, Tloc(bn, 0), tp, cand, start, cnt, hoff, abort_on_error, __func__);
	else if (ATOMstorage(b->ttype) == TYPE_str)
		nils = convert_str_any(b, bn, cand, start, cnt, hoff, abort_on_error);
	else if (ATOMstorage(bn->ttype) == TYPE_str)
		nils = convert_any_str(b, bn, cand, start, cnt, hoff);
	else
		nils = convertswitch(Tloc(b, 0), b->ttype, Tloc(bn, 0), tp, cand, start, cnt, hoff, abort_on_error);

	if (nils >= BUN_NONE) {
		BBPreclaim(bn);
		if (nils == BUN_NONE + 1)
			GDKerror("%s: type combination (convert(%s)->%s) not supported.\n", __func__, ATOMname(b->ttype), ATOMname(tp));
		else if (nils == BUN_NONE + 2)
			GDKerror("%s: could not insert value into BAT.\n", __func__);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = cnt <= 1;
	bn->trevsorted = cnt <= 1;
	bn->tkey = cnt <= 1;

	return bn;
}

gdk_return
VARconvert(ValPtr ret, const ValRecord *v, int abort_on_error)
{
	BUN nils;

	if (ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) == 0) {
		if (VALinit(ret, ret->vtype, ATOMnilptr(ret->vtype)) == NULL)
			return GDK_FAIL;
		return GDK_SUCCEED;
	}
	if (ATOMstorage(v->vtype) == TYPE_str) {
		void *p = NULL;
		int l = 0;

		if (ATOMbasetype(ret->vtype) == TYPE_str) {
			ret->val.sval = GDKstrdup(v->val.sval);
			ret->len = v->len;
			return ret->val.sval ? GDK_SUCCEED : GDK_FAIL;
		}

		if (ATOMfromstr(ret->vtype, &p, &l, v->val.sval) < (int) strlen(v->val.sval)) {
			GDKfree(p);
			GDKerror("22018!conversion of string "
				 "'%s' to type %s failed.\n",
				 v->val.sval, ATOMname(ret->vtype));
			return GDK_FAIL;
		}
		ret = VALinit(ret, ret->vtype, p);
		GDKfree(p);
		return ret == NULL ? GDK_FAIL : GDK_SUCCEED;
	}
	if (ATOMbasetype(ret->vtype) == TYPE_str) {
		ret->val.sval = NULL;
		ret->len = 0;
		(*BATatoms[v->vtype].atomToStr)(&ret->val.sval, &ret->len, VALptr(v));
		return ret->val.sval ? GDK_SUCCEED : GDK_FAIL;
	}
	nils = convertswitch(VALptr(v), v->vtype, VALget(ret), ret->vtype, NULL, 0, 1, 0, abort_on_error);
	if (nils == BUN_NONE + 1)
		GDKerror("%s: type combination (convert(%s)->%s) not supported.\n", __func__, ATOMname(v->vtype), ATOMname(ret->vtype));
	return nils >= BUN_NONE ? GDK_FAIL : GDK_SUCCEED;
}
