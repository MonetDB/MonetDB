/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"
#include <math.h>

/* Define symbol FULL_IMPLEMENTATION to get implementations for all
 * sensible output types for +, -, *, /.  Without the symbol, all
 * combinations of input types are supported, but only output types
 * that are either the largest of the input types or one size larger
 * (if available) for +, -, *.  For division the output type can be
 * either input type of flt or dbl. */

#define FULL_IMPLEMENTATION

/* Generally, the functions return a new BAT aligned with the input
 * BAT(s).  If there are multiple input BATs, they must be aligned.
 * If there is a candidate list, the calculations are only done for
 * the candidates, all other values are NIL (so that the output is
 * still aligned). */

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

/* Most of the internal routines return a count of the number of NIL
 * values they produced.  They indicate an error by returning a value
 * >= BUN_NONE.  BUN_NONE means that the error was dealt with by
 * calling GDKerror (generally for overflow or conversion errors).
 * BUN_NONE+1 is returned by the DIV and MOD functions to indicate
 * division by zero.  */

/* replace BATconstant with a version that produces a void bat for
 * TYPE_oid/nil */
#define BATconstantV(HSEQ, TAILTYPE, VALUE, CNT, ROLE)			\
	((TAILTYPE) == TYPE_oid && ((CNT) == 0 || *(oid*)(VALUE) == oid_nil) \
	 ? BATconstant(HSEQ, TYPE_void, VALUE, CNT, ROLE)		\
	 : BATconstant(HSEQ, TAILTYPE, VALUE, CNT, ROLE))

static gdk_return
checkbats(BAT *b1, BAT *b2, const char *func)
{
	if (b1->batCount != b2->batCount) {
		GDKerror("%s: inputs not the same size.\n", func);
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

#define UNARY_2TYPE_FUNC(TYPE1, TYPE2, FUNC)				\
	do {								\
		const TYPE1 *restrict src = (const TYPE1 *) Tloc(b, 0);	\
		TYPE2 *restrict dst = (TYPE2 *) Tloc(bn, 0);		\
		for (i = 0; i < ci.ncand; i++) {			\
			x = canditer_next(&ci) - b->hseqbase;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				nils++;					\
				dst[i] = TYPE2##_nil;			\
			} else {					\
				dst[i] = FUNC(src[x]);			\
			}						\
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC(TYPE1, TYPE2, TYPE3, FUNC)			\
	do {								\
		for (k = 0; k < ci1->ncand; k++) {			\
			x1 = canditer_next(ci1) - candoff1;		\
			x2 = canditer_next(ci2) - candoff2;		\
			i = x1 * incr1;					\
			j = x2 * incr2;					\
			TYPE1 v1 = ((const TYPE1 *) lft)[i];		\
			TYPE2 v2 = ((const TYPE2 *) rgt)[j];		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
				nils++;					\
				((TYPE3 *) dst)[k] = TYPE3##_nil;	\
			} else {					\
				((TYPE3 *) dst)[k] = FUNC(v1, v2);	\
			}						\
		}							\
	} while (0)

/* special case for EQ and NE where we have a nil_matches flag for
 * when it is set */
#define BINARY_3TYPE_FUNC_nilmatch(TYPE1, TYPE2, TYPE3, FUNC)		\
	do {								\
		for (k = 0; k < ci1->ncand; k++) {			\
			x1 = canditer_next(ci1) - candoff1;		\
			x2 = canditer_next(ci2) - candoff2;		\
			i = x1 * incr1;					\
			j = x2 * incr2;					\
			TYPE1 v1 = ((const TYPE1 *) lft)[i];		\
			TYPE2 v2 = ((const TYPE2 *) rgt)[j];		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
				((TYPE3 *) dst)[k] = FUNC(is_##TYPE1##_nil(v1), is_##TYPE2##_nil(v2)); \
			} else {					\
				((TYPE3 *) dst)[k] = FUNC(v1, v2);	\
			}						\
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_nonil(TYPE1, TYPE2, TYPE3, FUNC)		\
	do {								\
		for (k = 0; k < ci1->ncand; k++) {			\
			x1 = canditer_next(ci1) - candoff1;		\
			x2 = canditer_next(ci2) - candoff2;		\
			i = x1 * incr1;					\
			j = x2 * incr2;					\
			TYPE1 v1 = ((const TYPE1 *) lft)[i];		\
			TYPE2 v2 = ((const TYPE2 *) rgt)[j];		\
			((TYPE3 *) dst)[k] = FUNC(v1, v2);		\
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_CHECK(TYPE1, TYPE2, TYPE3, FUNC, CHECK)	\
	do {								\
		for (k = 0; k < ci1->ncand; k++) {			\
			x1 = canditer_next(ci1) - candoff1;		\
			x2 = canditer_next(ci2) - candoff2;		\
			i = x1 * incr1;					\
			j = x2 * incr2;					\
			TYPE1 v1 = ((const TYPE1 *) lft)[i];		\
			TYPE2 v2 = ((const TYPE2 *) rgt)[j];		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(v1) || is_##TYPE2##_nil(v2)) { \
				nils++;					\
				((TYPE3 *) dst)[k] = TYPE3##_nil;	\
			} else if (CHECK(v1, v2)) {			\
				if (abort_on_error) {			\
					GDKerror("%s: shift operand too large in " \
						 #FUNC"("FMT##TYPE1","FMT##TYPE2").\n", \
						 func,			\
						 CST##TYPE1 v1,		\
						 CST##TYPE2 v2);	\
					goto checkfail;			\
				}					\
				((TYPE3 *)dst)[k] = TYPE3##_nil;	\
				nils++;					\
			} else {					\
				((TYPE3 *) dst)[k] = FUNC(v1, v2);	\
			}						\
		}							\
	} while (0)

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) NOT */

#define NOT(x)		(~(x))
#define NOTBIT(x)	(!(x))

BAT *
BATcalcnot(BAT *b, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN i, ncand;
	oid x;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0)
		return BATconstant(ci.hseq, b->ttype,
				   ATOMnilptr(b->ttype), ncand, TRANSIENT);

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
		if (b->ttype == TYPE_bit) {
			UNARY_2TYPE_FUNC(bit, bit, NOTBIT);
		} else {
			UNARY_2TYPE_FUNC(bte, bte, NOT);
		}
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
		BBPunfix(bn->batCacheid);
		GDKerror("%s: type %s not supported.\n",
			 __func__, ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* NOT reverses the order, but NILs mess it up */
	bn->tsorted = nils == 0 && b->trevsorted;
	bn->trevsorted = nils == 0 && b->tsorted;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tkey = b->tkey && nils <= 1;

	if (nils != 0 && !b->tnil) {
		b->tnil = true;
		b->batDirtydesc = true;
	}

	return bn;
}

gdk_return
VARcalcnot(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMbasetype(v->vtype)) {
	case TYPE_bte:
		if (is_bit_nil(v->val.btval))
			ret->val.btval = bit_nil;
		else if (v->vtype == TYPE_bit)
			ret->val.btval = !v->val.btval;
		else
			ret->val.btval = ~v->val.btval;
		break;
	case TYPE_sht:
		if (is_sht_nil(v->val.shval))
			ret->val.shval = sht_nil;
		else
			ret->val.shval = ~v->val.shval;
		break;
	case TYPE_int:
		if (is_int_nil(v->val.ival))
			ret->val.ival = int_nil;
		else
			ret->val.ival = ~v->val.ival;
		break;
	case TYPE_lng:
		if (is_lng_nil(v->val.lval))
			ret->val.lval = lng_nil;
		else
			ret->val.lval = ~v->val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (is_hge_nil(v->val.hval))
			ret->val.hval = hge_nil;
		else
			ret->val.hval = ~v->val.hval;
		break;
#endif
	default:
		GDKerror("VARcalcnot: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* negate value (any numeric type) */

#define NEGATE(x)	(-(x))

BAT *
BATcalcnegate(BAT *b, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN i, ncand;
	oid x;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0)
		return BATconstant(ci.hseq, b->ttype,
				   ATOMnilptr(b->ttype), ncand, TRANSIENT);

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
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
		BBPunfix(bn->batCacheid);
		GDKerror("%s: type %s not supported.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* unary - reverses the order, but NILs mess it up */
	bn->tsorted = nils == 0 && b->trevsorted;
	bn->trevsorted = nils == 0 && b->tsorted;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tkey = b->tkey && nils <= 1;

	if (nils != 0 && !b->tnil) {
		b->tnil = true;
		b->batDirtydesc = true;
	}

	return bn;
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
		GDKerror("VARcalcnegate: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* absolute value (any numeric type) */

BAT *
BATcalcabsolute(BAT *b, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils= 0;
	BUN i, ncand;
	oid x;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0)
		return BATconstant(ci.hseq, b->ttype,
				   ATOMnilptr(b->ttype), ncand, TRANSIENT);

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
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
		BBPunfix(bn->batCacheid);
		GDKerror("%s: bad input type %s.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* ABSOLUTE messes up order (unless all values were negative
	 * or all values were positive, but we don't know anything
	 * about that) */
	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	if (nils && !b->tnil) {
		b->tnil = true;
		b->batDirtydesc = true;
	}

	return bn;
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
BATcalciszero(BAT *b, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN i, ncand;
	oid x;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0)
		return BATconstant(ci.hseq, TYPE_bit,
				   ATOMnilptr(TYPE_bit), ncand, TRANSIENT);

	bn = COLnew(ci.hseq, TYPE_bit, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	switch (ATOMbasetype(b->ttype)) {
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
		BBPunfix(bn->batCacheid);
		GDKerror("%s: bad input type %s.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	if (nils != 0 && !b->tnil) {
		b->tnil = true;
		b->batDirtydesc = true;
	}

	return bn;
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
		GDKerror("VARcalciszero: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* sign of value (-1 for negative, 0 for 0, +1 for positive; any
 * numeric type) */

#define SIGN(x)		((bte) ((x) < 0 ? -1 : (x) > 0))

BAT *
BATcalcsign(BAT *b, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN i, ncand;
	oid x;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0)
		return BATconstant(ci.hseq, TYPE_bte,
				   ATOMnilptr(TYPE_bte), ncand, TRANSIENT);

	bn = COLnew(ci.hseq, TYPE_bte, ncand, TRANSIENT);
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
		BBPunfix(bn->batCacheid);
		GDKerror("%s: bad input type %s.\n", __func__,
			 ATOMname(b->ttype));
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* SIGN is ordered if the input is ordered (negative comes
	 * first, positive comes after) and NILs stay in the same
	 * position */
	bn->tsorted = b->tsorted || ncand <= 1 || nils == ncand;
	bn->trevsorted = b->trevsorted || ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	if (nils != 0 && !b->tnil) {
		b->tnil = true;
		b->batDirtydesc = true;
	}

	return bn;
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
		GDKerror("VARcalcsign: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* is the value nil (any type) */

#define ISNIL_TYPE(TYPE, NOTNIL)					\
	do {								\
		const TYPE *restrict src = (const TYPE *) Tloc(b, 0);	\
		for (i = 0; i < ci.ncand; i++) {			\
			x = canditer_next(&ci) - b->hseqbase;		\
			if (rv && !rv[i]) {				\
				dst[i] = bit_nil;			\
				nils++;					\
			} else {					\
				dst[i] = (bit) (is_##TYPE##_nil(src[i]) ^ NOTNIL); \
			}						\
		}							\
	} while (0)

static BAT *
BATcalcisnil_implementation(BAT *b, BAT *s, BAT *r, bool notnil)
{
	BAT *bn;
	BUN i, ncand;
	oid x;
	struct canditer ci;
	bit *restrict dst;
	BUN nils = 0;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	if (b->tnonil || BATtdense(b)) {
		return BATconstant(ci.hseq, TYPE_bit, &(bit){0},
				   ncand, TRANSIENT);
	} else if (b->ttype == TYPE_void) {
		/* non-nil handled above */
		assert(is_oid_nil(b->tseqbase));
		return BATconstant(ci.hseq, TYPE_bit, &(bit){1},
				   ncand, TRANSIENT);
	}

	bn = COLnew(ci.hseq, TYPE_bit, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, 0);

	switch (ATOMbasetype(b->ttype)) {
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
	default:
	{
		BATiter bi = bat_iterator(b);
		int (*atomcmp)(const void *, const void *) = ATOMcompare(b->ttype);
		const void *nil = ATOMnilptr(b->ttype);

		for (i = 0; i < ci.ncand; i++) {
			x = canditer_next(&ci) - b->hseqbase;
			dst[i] = (bit) (((*atomcmp)(BUNtail(bi, x), nil) == 0) ^ notnil);
		}
		break;
	}
	}

	BATsetcount(bn, ncand);

	/* If b sorted, all nils are at the start, i.e. bn starts with
	 * 1's and ends with 0's, hence bn is revsorted.  Similarly
	 * for revsorted. */
	bn->tsorted = b->trevsorted;
	bn->trevsorted = b->tsorted;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tkey = ncand <= 1;

	return bn;
}

BAT *
BATcalcisnil(BAT *b, BAT *s, BAT *r)
{
	return BATcalcisnil_implementation(b, s, r, false);
}

BAT *
BATcalcisnotnil(BAT *b, BAT *s, BAT *r)
{
	return BATcalcisnil_implementation(b, s, r, true);
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

BAT *
BATcalcmin(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN ncand;
	struct canditer ci1, ci2;
	const void *restrict nil;
	BATiter b1i, b2i;
	int (*cmp)(const void *, const void *);
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	nil = ATOMnilptr(b1->ttype);
	bn = COLnew(ci1.hseq, ATOMtype(b1->ttype), ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	cmp = ATOMcompare(b1->ttype);
	b1i = bat_iterator(b1);
	b2i = bat_iterator(b2);

	for (BUN i = 0; i < ncand; i++) {
		oid x1 = canditer_next(&ci1) - b1->hseqbase;
		oid x2 = canditer_next(&ci2) - b2->hseqbase;
		const void *p1 = BUNtail(b1i, x1);
		const void *p2 = BUNtail(b2i, x2);
		if ((rv != NULL && !rv[i]) ||
		    cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
			nils++;
			p1 = nil;
		} else if (cmp(p1, p2) > 0) {
			p1 = p2;
		}
		if (bunfastapp(bn, p1) != GDK_SUCCEED)
			goto bunins_failed;
	}

	bn->tnil = nils > 0;
	bn->tnonil = nils == 0;
	if (ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(b1->ttype) == TYPE_oid ? ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	return bn;
  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalcmin_no_nil(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN ncand;
	struct canditer ci1, ci2;
	const void *restrict nil;
	BATiter b1i, b2i;
	int (*cmp)(const void *, const void *);
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	nil = ATOMnilptr(b1->ttype);
	bn = COLnew(ci1.hseq, ATOMtype(b1->ttype), ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	cmp = ATOMcompare(b1->ttype);
	b1i = bat_iterator(b1);
	b2i = bat_iterator(b2);

	for (BUN i = 0; i < ncand; i++) {
		oid x1 = canditer_next(&ci1) - b1->hseqbase;
		oid x2 = canditer_next(&ci2) - b2->hseqbase;
		const void *p1 = BUNtail(b1i, x1);
		const void *p2 = BUNtail(b2i, x2);
		if ((rv != NULL && !rv[i]) ||
		    cmp(p1, nil) == 0) {
			if (cmp(p2, nil) == 0) {
				/* both values are nil */
				nils++;
			} else {
				p1 = p2;
			}
		} else if (cmp(p2, nil) != 0 && cmp(p1, p2) > 0) {
			p1 = p2;
		}
		if (bunfastapp(bn, p1) != GDK_SUCCEED)
			goto bunins_failed;
	}

	bn->tnil = nils > 0;
	bn->tnonil = nils == 0;
	if (ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(b1->ttype) == TYPE_oid ? ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	return bn;
  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalcmincst(BAT *b, const ValRecord *v, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN ncand;
	BUN i;
	struct canditer ci;
	oid x;
	const void *restrict nil;
	const void *p1, *p2;
	BATiter bi;
	int (*cmp)(const void *, const void *);
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	if (ATOMtype(b->ttype) != v->vtype) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}

	nil = ATOMnilptr(b->ttype);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	nil = ATOMnilptr(b->ttype);
	cmp = ATOMcompare(b->ttype);
	p2 = VALptr(v);
	if (ncand == 0 ||
	    cmp(p2, nil) == 0 ||
	    (b->ttype == TYPE_void && is_oid_nil(b->tseqbase)))
		return BATconstantV(ci.hseq, b->ttype, nil, ncand, TRANSIENT);

	bn = COLnew(ci.hseq, ATOMtype(b->ttype), ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	bi = bat_iterator(b);

	for (i = 0; i < ci.ncand; i++) {
		x = canditer_next(&ci) - b->hseqbase;
		p1 = BUNtail(bi, x);
		if ((rv != NULL && !rv[i]) || cmp(p1, nil) == 0) {
			nils++;
			p1 = nil;
		} else if (cmp(p1, p2) > 0) {
			p1 = p2;
		}
		if (bunfastapp(bn, p1) != GDK_SUCCEED)
			goto bunins_failed;
	}

	bn->theap.dirty = true;
	bn->tnil = nils > 0;
	bn->tnonil = nils == 0;
	if (ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(bn->ttype) == TYPE_oid ? ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	return bn;
  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmin(const ValRecord *v, BAT *b, BAT *s, BAT *r)
{
	return BATcalcmincst(b, v, s, r);
}

BAT *
BATcalcmincst_no_nil(BAT *b, const ValRecord *v, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN ncand;
	BUN i;
	struct canditer ci;
	oid x;
	const void *restrict nil;
	const void *p1, *p2;
	BATiter bi;
	int (*cmp)(const void *, const void *);
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	if (ATOMtype(b->ttype) != v->vtype) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}

	nil = ATOMnilptr(b->ttype);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0)
		return BATconstantV(ci.hseq, b->ttype,
				    nil, ncand, TRANSIENT);

	cmp = ATOMcompare(b->ttype);
	p2 = VALptr(v);
	if (b->ttype == TYPE_void &&
	    is_oid_nil(b->tseqbase) &&
	    is_oid_nil(* (const oid *) p2))
		return BATconstant(ci.hseq, TYPE_void,
				   &oid_nil, ncand, TRANSIENT);

	bn = COLnew(ci.hseq, ATOMtype(b->ttype), ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	bi = bat_iterator(b);
	if (cmp(p2, nil) == 0)
		p2 = NULL;

	for (i = 0; i < ci.ncand; i++) {
		x = canditer_next(&ci) - b->hseqbase;
		if (rv != NULL && !rv[i]) {
			p1 = nil;
			nils++;
		} else {
			p1 = BUNtail(bi, x);
			if (p2) {
				if (cmp(p1, nil) == 0) {
					p1 = p2;
				} else if (cmp(p1, p2) > 0) {
					p1 = p2;
				}
			} else {
				nils += cmp(p1, nil) == 0;
			}
		}
		if (bunfastapp(bn, p1) != GDK_SUCCEED)
			goto bunins_failed;
	}

	bn->theap.dirty = true;
	bn->tnil = nils > 0;
	bn->tnonil = nils == 0;
	if (ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(bn->ttype) == TYPE_oid ? ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	return bn;
  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmin_no_nil(const ValRecord *v, BAT *b, BAT *s, BAT *r)
{
	return BATcalcmincst_no_nil(b, v, s, r);
}

BAT *
BATcalcmax(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN ncand;
	struct canditer ci1, ci2;
	const void *restrict nil;
	BATiter b1i, b2i;
	int (*cmp)(const void *, const void *);
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	nil = ATOMnilptr(b1->ttype);
	bn = COLnew(ci1.hseq, ATOMtype(b1->ttype), ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	cmp = ATOMcompare(b1->ttype);
	b1i = bat_iterator(b1);
	b2i = bat_iterator(b2);

	for (BUN i = 0; i < ncand; i++) {
		oid x1 = canditer_next(&ci1) - b1->hseqbase;
		oid x2 = canditer_next(&ci2) - b2->hseqbase;
		const void *p1 = BUNtail(b1i, x1);
		const void *p2 = BUNtail(b2i, x2);
		if ((rv != NULL && !rv[i]) ||
		    cmp(p1, nil) == 0 || cmp(p2, nil) == 0) {
			nils++;
			p1 = nil;
		} else if (cmp(p1, p2) < 0) {
			p1 = p2;
		}
		if (bunfastapp(bn, p1) != GDK_SUCCEED)
			goto bunins_failed;
	}

	bn->tnil = nils > 0;
	bn->tnonil = nils == 0;
	if (ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(b1->ttype) == TYPE_oid ? ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	return bn;
  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalcmax_no_nil(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN ncand;
	struct canditer ci1, ci2;
	const void *restrict nil;
	BATiter b1i, b2i;
	int (*cmp)(const void *, const void *);
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	nil = ATOMnilptr(b1->ttype);
	bn = COLnew(ci1.hseq, ATOMtype(b1->ttype), ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	cmp = ATOMcompare(b1->ttype);
	b1i = bat_iterator(b1);
	b2i = bat_iterator(b2);

	for (BUN i = 0; i < ncand; i++) {
		oid x1 = canditer_next(&ci1) - b1->hseqbase;
		oid x2 = canditer_next(&ci2) - b2->hseqbase;
		const void *p1, *p2;
		if (rv != NULL && !rv[i]) {
			p1 = nil;
			nils++;
		} else {
			p1 = BUNtail(b1i, x1);
			p2 = BUNtail(b2i, x2);
			if (cmp(p1, nil) == 0) {
				if (cmp(p2, nil) == 0) {
					/* both values are nil */
					nils++;
				} else {
					p1 = p2;
				}
			} else if (cmp(p2, nil) != 0 && cmp(p1, p2) < 0) {
				p1 = p2;
			}
		}
		if (bunfastapp(bn, p1) != GDK_SUCCEED)
			goto bunins_failed;
	}

	bn->tnil = nils > 0;
	bn->tnonil = nils == 0;
	if (ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(b1->ttype) == TYPE_oid ? ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	return bn;
  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalcmaxcst(BAT *b, const ValRecord *v, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN ncand;
	BUN i;
	struct canditer ci;
	oid x;
	const void *restrict nil;
	const void *p1, *p2;
	BATiter bi;
	int (*cmp)(const void *, const void *);
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	if (ATOMtype(b->ttype) != v->vtype) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}

	nil = ATOMnilptr(b->ttype);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	nil = ATOMnilptr(b->ttype);
	cmp = ATOMcompare(b->ttype);
	p2 = VALptr(v);
	if (ncand == 0 ||
	    cmp(p2, nil) == 0 ||
	    (b->ttype == TYPE_void && is_oid_nil(b->tseqbase)))
		return BATconstantV(ci.hseq, b->ttype, nil, ncand, TRANSIENT);

	bn = COLnew(ci.hseq, ATOMtype(b->ttype), ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	bi = bat_iterator(b);

	for (i = 0; i < ci.ncand; i++) {
		x = canditer_next(&ci) - b->hseqbase;
		p1 = BUNtail(bi, x);
		if ((rv != NULL && !rv[i]) || cmp(p1, nil) == 0) {
			nils++;
			p1 = nil;
		} else if (cmp(p1, p2) < 0) {
			p1 = p2;
		}
		if (bunfastapp(bn, p1) != GDK_SUCCEED)
			goto bunins_failed;
	}

	bn->theap.dirty = true;
	bn->tnil = nils > 0;
	bn->tnonil = nils == 0;
	if (ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(bn->ttype) == TYPE_oid ? ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	return bn;
  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmax(const ValRecord *v, BAT *b, BAT *s, BAT *r)
{
	return BATcalcmaxcst(b, v, s, r);
}

BAT *
BATcalcmaxcst_no_nil(BAT *b, const ValRecord *v, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils = 0;
	BUN ncand;
	BUN i;
	struct canditer ci;
	oid x;
	const void *restrict nil;
	const void *p1, *p2;
	BATiter bi;
	int (*cmp)(const void *, const void *);
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	if (ATOMtype(b->ttype) != v->vtype) {
		GDKerror("%s: inputs have incompatible types\n", __func__);
		return NULL;
	}

	nil = ATOMnilptr(b->ttype);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0)
		return BATconstantV(ci.hseq, b->ttype,
				    nil, ncand, TRANSIENT);

	cmp = ATOMcompare(b->ttype);
	p2 = VALptr(v);
	if (b->ttype == TYPE_void &&
	    is_oid_nil(b->tseqbase) &&
	    is_oid_nil(* (const oid *) p2))
		return BATconstant(ci.hseq, TYPE_void,
				   &oid_nil, ncand, TRANSIENT);

	bn = COLnew(ci.hseq, ATOMtype(b->ttype), ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	bi = bat_iterator(b);
	if (cmp(p2, nil) == 0)
		p2 = NULL;

	for (i = 0; i < ci.ncand; i++) {
		x = canditer_next(&ci) - b->hseqbase;
		if (rv != NULL && !rv[i]) {
			p1 = nil;
			nils++;
		} else {
			p1 = BUNtail(bi, x);
			if (p2) {
				if (cmp(p1, nil) == 0) {
					p1 = p2;
				} else if (cmp(p1, p2) < 0) {
					p1 = p2;
				}
			} else {
				nils += cmp(p1, nil) == 0;
			}
		}
		if (bunfastapp(bn, p1) != GDK_SUCCEED)
			goto bunins_failed;
	}

	bn->theap.dirty = true;
	bn->tnil = nils > 0;
	bn->tnonil = nils == 0;
	if (ncand <= 1) {
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		bn->tseqbase = ATOMtype(bn->ttype) == TYPE_oid ? ncand == 1 ? *(oid*)Tloc(bn,0) : 0 : oid_nil;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tseqbase = oid_nil;
	}
	return bn;
  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATcalccstmax_no_nil(const ValRecord *v, BAT *b, BAT *s, BAT *r)
{
	return BATcalcmaxcst_no_nil(b, v, s, r);
}

/* ---------------------------------------------------------------------- */
/* addition (any numeric type) */

#define ON_OVERFLOW(TYPE1, TYPE2, OP)				\
	do {							\
		GDKerror("22003!overflow in calculation "	\
			 FMT##TYPE1 OP FMT##TYPE2 ".\n",	\
			 CST##TYPE1 lft[i], CST##TYPE2 rgt[j]);	\
		return BUN_NONE;				\
	} while (0)

#define ADD_3TYPE(TYPE1, TYPE2, TYPE3, IF)				\
static BUN								\
add_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			ADD##IF##_WITH_CHECK(lft[i], rgt[j],		\
					     TYPE3, dst[k],		\
					     max,			\
					     ON_OVERFLOW(TYPE1, TYPE2, "+")); \
		}							\
	}								\
	return nils;							\
}

#define ADD_3TYPE_enlarge(TYPE1, TYPE2, TYPE3, IF)			\
static BUN								\
add_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
	const bool couldoverflow = (max < (TYPE3) GDK_##TYPE1##_max + (TYPE3) GDK_##TYPE2##_max); \
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else if (couldoverflow) {				\
			ADD##IF##_WITH_CHECK(lft[i], rgt[j],		\
					     TYPE3, dst[k],		\
					     max,			\
					     ON_OVERFLOW(TYPE1, TYPE2, "+")); \
		} else {						\
			dst[k] = (TYPE3) lft[i] + rgt[j];		\
		}							\
	}								\
	return nils;							\
}

ADD_3TYPE(bte, bte, bte, I)
ADD_3TYPE_enlarge(bte, bte, sht, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, bte, int, I)
ADD_3TYPE_enlarge(bte, bte, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, bte, hge, I)
#endif
ADD_3TYPE_enlarge(bte, bte, flt, F)
ADD_3TYPE_enlarge(bte, bte, dbl, F)
#endif
ADD_3TYPE(bte, sht, sht, I)
ADD_3TYPE_enlarge(bte, sht, int, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, sht, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, sht, hge, I)
#endif
ADD_3TYPE_enlarge(bte, sht, flt, F)
ADD_3TYPE_enlarge(bte, sht, dbl, F)
#endif
ADD_3TYPE(bte, int, int, I)
ADD_3TYPE_enlarge(bte, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, int, hge, I)
#endif
ADD_3TYPE_enlarge(bte, int, flt, F)
ADD_3TYPE_enlarge(bte, int, dbl, F)
#endif
ADD_3TYPE(bte, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(bte, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, lng, flt, F)
ADD_3TYPE_enlarge(bte, lng, dbl, F)
#endif
#ifdef HAVE_HGE
ADD_3TYPE(bte, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, hge, flt, F)
ADD_3TYPE_enlarge(bte, hge, dbl, F)
#endif
#endif
ADD_3TYPE(bte, flt, flt, F)
ADD_3TYPE_enlarge(bte, flt, dbl, F)
ADD_3TYPE(bte, dbl, dbl, F)
ADD_3TYPE(sht, bte, sht, I)
ADD_3TYPE_enlarge(sht, bte, int, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, bte, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, bte, hge, I)
#endif
ADD_3TYPE_enlarge(sht, bte, flt, F)
ADD_3TYPE_enlarge(sht, bte, dbl, F)
#endif
ADD_3TYPE(sht, sht, sht, I)
ADD_3TYPE_enlarge(sht, sht, int, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, sht, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, sht, hge, I)
#endif
ADD_3TYPE_enlarge(sht, sht, flt, F)
ADD_3TYPE_enlarge(sht, sht, dbl, F)
#endif
ADD_3TYPE(sht, int, int, I)
ADD_3TYPE_enlarge(sht, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, int, hge, I)
#endif
ADD_3TYPE_enlarge(sht, int, flt, F)
ADD_3TYPE_enlarge(sht, int, dbl, F)
#endif
ADD_3TYPE(sht, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(sht, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, lng, flt, F)
ADD_3TYPE_enlarge(sht, lng, dbl, F)
#endif
#ifdef HAVE_HGE
ADD_3TYPE(sht, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, hge, flt, F)
ADD_3TYPE_enlarge(sht, hge, dbl, F)
#endif
#endif
ADD_3TYPE(sht, flt, flt, F)
ADD_3TYPE_enlarge(sht, flt, dbl, F)
ADD_3TYPE(sht, dbl, dbl, F)
ADD_3TYPE(int, bte, int, I)
ADD_3TYPE_enlarge(int, bte, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, bte, hge, I)
#endif
ADD_3TYPE_enlarge(int, bte, flt, F)
ADD_3TYPE_enlarge(int, bte, dbl, F)
#endif
ADD_3TYPE(int, sht, int, I)
ADD_3TYPE_enlarge(int, sht, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, sht, hge, I)
#endif
ADD_3TYPE_enlarge(int, sht, flt, F)
ADD_3TYPE_enlarge(int, sht, dbl, F)
#endif
ADD_3TYPE(int, int, int, I)
ADD_3TYPE_enlarge(int, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, int, hge, I)
#endif
ADD_3TYPE_enlarge(int, int, flt, F)
ADD_3TYPE_enlarge(int, int, dbl, F)
#endif
ADD_3TYPE(int, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(int, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(int, lng, flt, F)
ADD_3TYPE_enlarge(int, lng, dbl, F)
#endif
#ifdef HAVE_HGE
ADD_3TYPE(int, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(int, hge, flt, F)
ADD_3TYPE_enlarge(int, hge, dbl, F)
#endif
#endif
ADD_3TYPE(int, flt, flt, F)
ADD_3TYPE_enlarge(int, flt, dbl, F)
ADD_3TYPE(int, dbl, dbl, F)
ADD_3TYPE(lng, bte, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, bte, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, bte, flt, F)
ADD_3TYPE_enlarge(lng, bte, dbl, F)
#endif
ADD_3TYPE(lng, sht, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, sht, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, sht, flt, F)
ADD_3TYPE_enlarge(lng, sht, dbl, F)
#endif
ADD_3TYPE(lng, int, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, int, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, int, flt, F)
ADD_3TYPE_enlarge(lng, int, dbl, F)
#endif
ADD_3TYPE(lng, lng, lng, I)
#ifdef HAVE_HGE
ADD_3TYPE_enlarge(lng, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, lng, flt, F)
ADD_3TYPE_enlarge(lng, lng, dbl, F)
#endif
#ifdef HAVE_HGE
ADD_3TYPE(lng, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, hge, flt, F)
ADD_3TYPE_enlarge(lng, hge, dbl, F)
#endif
#endif
ADD_3TYPE(lng, flt, flt, F)
ADD_3TYPE_enlarge(lng, flt, dbl, F)
ADD_3TYPE(lng, dbl, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(hge, bte, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, bte, flt, F)
ADD_3TYPE_enlarge(hge, bte, dbl, F)
#endif
ADD_3TYPE(hge, sht, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, sht, flt, F)
ADD_3TYPE_enlarge(hge, sht, dbl, F)
#endif
ADD_3TYPE(hge, int, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, int, flt, F)
ADD_3TYPE_enlarge(hge, int, dbl, F)
#endif
ADD_3TYPE(hge, lng, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, lng, flt, F)
ADD_3TYPE_enlarge(hge, lng, dbl, F)
#endif
ADD_3TYPE(hge, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(hge, hge, flt, F)
ADD_3TYPE_enlarge(hge, hge, dbl, F)
#endif
ADD_3TYPE(hge, flt, flt, F)
ADD_3TYPE_enlarge(hge, flt, dbl, F)
ADD_3TYPE(hge, dbl, dbl, F)
#endif
ADD_3TYPE(flt, bte, flt, F)
ADD_3TYPE_enlarge(flt, bte, dbl, F)
ADD_3TYPE(flt, sht, flt, F)
ADD_3TYPE_enlarge(flt, sht, dbl, F)
ADD_3TYPE(flt, int, flt, F)
ADD_3TYPE_enlarge(flt, int, dbl, F)
ADD_3TYPE(flt, lng, flt, F)
ADD_3TYPE_enlarge(flt, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(flt, hge, flt, F)
ADD_3TYPE_enlarge(flt, hge, dbl, F)
#endif
ADD_3TYPE(flt, flt, flt, F)
ADD_3TYPE_enlarge(flt, flt, dbl, F)
ADD_3TYPE(flt, dbl, dbl, F)
ADD_3TYPE(dbl, bte, dbl, F)
ADD_3TYPE(dbl, sht, dbl, F)
ADD_3TYPE(dbl, int, dbl, F)
ADD_3TYPE(dbl, lng, dbl, F)
#ifdef HAVE_HGE
ADD_3TYPE(dbl, hge, dbl, F)
#endif
ADD_3TYPE(dbl, flt, dbl, F)
ADD_3TYPE(dbl, dbl, dbl, F)

static BUN
add_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1, struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2, bool abort_on_error,
		   const char *func)
{
	BUN nils;

	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	tp = ATOMbasetype(tp);
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = add_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = add_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = add_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = add_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = add_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = add_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = add_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = add_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = add_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_bte_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = add_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = add_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = add_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = add_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = add_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = add_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = add_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = add_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = add_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_sht_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = add_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_int_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				nils = add_int_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = add_int_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = add_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = add_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = add_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_int_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = add_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				nils = add_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				nils = add_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = add_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = add_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = add_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_lng_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = add_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_hge_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				nils = add_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_hge_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				nils = add_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_hge_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				nils = add_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_hge_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = add_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = add_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_hge_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = add_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = add_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = add_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = add_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = add_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = add_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = add_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = add_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = add_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = add_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = add_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = add_dbl_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = add_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = add_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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

	return nils;

  unsupported:
	GDKerror("%s: type combination (add(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

static BUN
addstr_loop(BAT *b1, const char *l, BAT *b2, const char *r, BAT *bn,
	    struct canditer *restrict ci1, struct canditer *restrict ci2,
	    const bit *restrict rv)
{
	BUN nils = 0;
	char *s;
	size_t slen, llen, rlen;
	BATiter b1i, b2i;
	oid candoff1, candoff2;

	assert(b1 != NULL || b2 != NULL); /* at least one not NULL */
	candoff1 = b1 ? b1->hseqbase : 0;
	candoff2 = b2 ? b2->hseqbase : 0;
	b1i = bat_iterator(b1);
	b2i = bat_iterator(b2);
	slen = 1024;
	s = GDKmalloc(slen);
	if (s == NULL)
		return BUN_NONE;
	for (BUN i = 0; i < ci1->ncand; i++) {
		oid x1 = canditer_next(ci1) - candoff1;
		oid x2 = canditer_next(ci2) - candoff2;
		if (b1)
			l = BUNtvar(b1i, x1);
		if (b2)
			r = BUNtvar(b2i, x2);
		if ((rv != NULL && !rv[i]) || strNil(l) || strNil(r)) {
			nils++;
			if (tfastins_nocheckVAR(bn, i, str_nil, Tsize(bn)) != GDK_SUCCEED)
				goto bunins_failed;
		} else {
			llen = strlen(l);
			rlen = strlen(r);
			if (llen + rlen >= slen) {
				slen = llen + rlen + 1024;
				GDKfree(s);
				s = GDKmalloc(slen);
				if (s == NULL)
					goto bunins_failed;
			}
			(void) stpcpy(stpcpy(s, l), r);
			if (tfastins_nocheckVAR(bn, i, s, Tsize(bn)) != GDK_SUCCEED)
				goto bunins_failed;
		}
	}
	GDKfree(s);
	bn->theap.dirty = true;
	return nils;

  bunins_failed:
	GDKfree(s);
	return BUN_NONE;
}

BAT *
BATcalcadd(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci1.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	if (b1->ttype == TYPE_str && b2->ttype == TYPE_str && tp == TYPE_str) {
		nils = addstr_loop(b1, NULL, b2, NULL, bn, &ci1, &ci2, rv);
	} else {
		nils = add_typeswitchloop(Tloc(b1, 0),
					  b1->ttype, 1,
					  Tloc(b2, 0),
					  b2->ttype, 1,
					  Tloc(bn, 0), tp,
					  &ci1, &ci2, rv,
					  b1->hseqbase, b2->hseqbase,
					  abort_on_error, __func__);
	}

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if both inputs are sorted the same way, and no overflow
	 * occurred (we only know for sure if abort_on_error is set),
	 * the result is also sorted */
	bn->tsorted = (abort_on_error && b1->tsorted & b2->tsorted && nils == 0)
		|| ncand <= 1 || nils == ncand;
	bn->trevsorted = (abort_on_error && b1->trevsorted & b2->trevsorted && nils == 0)
		|| ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcaddcst(BAT *b, const ValRecord *v, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	if (b->ttype == TYPE_str && v->vtype == TYPE_str && tp == TYPE_str) {
		nils = addstr_loop(b, NULL, NULL, v->val.sval, bn, &ci, &(struct canditer){.tpe=cand_dense, .ncand=ncand}, rv);
	} else {
		nils = add_typeswitchloop(Tloc(b, 0), b->ttype, 1,
					  VALptr(v), v->vtype, 0,
					  Tloc(bn, 0), tp,
					  &ci,
					  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
					  rv,
					  b->hseqbase, 0,
					  abort_on_error, __func__);
	}

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->tsorted = (abort_on_error && b->tsorted && nils == 0) ||
		ncand <= 1 || nils == ncand;
	bn->trevsorted = (abort_on_error && b->trevsorted && nils == 0) ||
		ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstadd(const ValRecord *v, BAT *b, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	if (b->ttype == TYPE_str && v->vtype == TYPE_str && tp == TYPE_str) {
		nils = addstr_loop(NULL, v->val.sval, b, NULL, bn, &(struct canditer){.tpe=cand_dense, .ncand=ncand}, &ci, rv);
	} else {
		nils = add_typeswitchloop(VALptr(v), v->vtype, 0,
					  Tloc(b, 0), b->ttype, 1,
					  Tloc(bn, 0), tp,
					  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
					  &ci,
					  rv,
					  0, b->hseqbase,
					  abort_on_error, __func__);
	}

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->tsorted = (abort_on_error && b->tsorted && nils == 0) ||
		ncand <= 1 || nils == ncand;
	bn->trevsorted = (abort_on_error && b->trevsorted && nils == 0) ||
		ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

gdk_return
VARcalcadd(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	if (add_typeswitchloop(VALptr(lft), lft->vtype, 0,
			       VALptr(rgt), rgt->vtype, 0,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

static BAT *
BATcalcincrdecr(BAT *b, BAT *s, BAT *r, bool abort_on_error,
		BUN (*typeswitchloop)(const void *, int, int,
				      const void *, int, int,
				      void *, int,
				      struct canditer *restrict,
				      struct canditer *restrict,
				      const bit *restrict,
				      oid, oid, bool, const char *),
		const char *func)
{
	BAT *bn;
	BUN nils= 0;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, func, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = (*typeswitchloop)(Tloc(b, 0), b->ttype, 1,
				 &(bte){1}, TYPE_bte, 0,
				 Tloc(bn, 0), bn->ttype,
				 &(struct canditer){.tpe=cand_dense, .ncand=1},
				 &ci,
				 rv,
				 0, b->hseqbase,
				 abort_on_error, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->tsorted = (abort_on_error && b->tsorted) ||
		ncand <= 1 || nils == ncand;
	bn->trevsorted = (abort_on_error && b->trevsorted) ||
		ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	if (nils && !b->tnil) {
		b->tnil = true;
		b->batDirtydesc = true;
	}
	if (nils == 0 && !b->tnonil) {
		b->tnonil = true;
		b->batDirtydesc = true;
	}

	return bn;
}

BAT *
BATcalcincr(BAT *b, BAT *s, BAT *r, bool abort_on_error)
{
	return BATcalcincrdecr(b, s, r, abort_on_error, add_typeswitchloop,
			       __func__);
}

gdk_return
VARcalcincr(ValPtr ret, const ValRecord *v, bool abort_on_error)
{
	if (add_typeswitchloop(VALptr(v), v->vtype, 0,
			       &(bte){1}, TYPE_bte, 0,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* subtraction (any numeric type) */

#define SUB_3TYPE(TYPE1, TYPE2, TYPE3, IF)				\
static BUN								\
sub_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			SUB##IF##_WITH_CHECK(lft[i], rgt[j],		\
					     TYPE3, dst[k],		\
					     max,			\
					     ON_OVERFLOW(TYPE1, TYPE2, "-")); \
		}							\
	}								\
	return nils;							\
}

#define SUB_3TYPE_enlarge(TYPE1, TYPE2, TYPE3, IF)			\
static BUN								\
sub_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
	const bool couldoverflow = (max < (TYPE3) GDK_##TYPE1##_max + (TYPE3) GDK_##TYPE2##_max); \
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else if (couldoverflow) {				\
			SUB##IF##_WITH_CHECK(lft[i], rgt[j],		\
					     TYPE3, dst[k],		\
					     max,			\
					     ON_OVERFLOW(TYPE1, TYPE2, "-")); \
		} else {						\
			dst[k] = (TYPE3) lft[i] - rgt[j];		\
		}							\
	}								\
	return nils;							\
}

SUB_3TYPE(bte, bte, bte, I)
SUB_3TYPE_enlarge(bte, bte, sht, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, bte, int, I)
SUB_3TYPE_enlarge(bte, bte, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, bte, hge, I)
#endif
SUB_3TYPE_enlarge(bte, bte, flt, F)
SUB_3TYPE_enlarge(bte, bte, dbl, F)
#endif
SUB_3TYPE(bte, sht, sht, I)
SUB_3TYPE_enlarge(bte, sht, int, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, sht, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, sht, hge, I)
#endif
SUB_3TYPE_enlarge(bte, sht, flt, F)
SUB_3TYPE_enlarge(bte, sht, dbl, F)
#endif
SUB_3TYPE(bte, int, int, I)
SUB_3TYPE_enlarge(bte, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, int, hge, I)
#endif
SUB_3TYPE_enlarge(bte, int, flt, F)
SUB_3TYPE_enlarge(bte, int, dbl, F)
#endif
SUB_3TYPE(bte, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(bte, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, lng, flt, F)
SUB_3TYPE_enlarge(bte, lng, dbl, F)
#endif
#ifdef HAVE_HGE
SUB_3TYPE(bte, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, hge, flt, F)
SUB_3TYPE_enlarge(bte, hge, dbl, F)
#endif
#endif
SUB_3TYPE(bte, flt, flt, F)
SUB_3TYPE_enlarge(bte, flt, dbl, F)
SUB_3TYPE(bte, dbl, dbl, F)
SUB_3TYPE(sht, bte, sht, I)
SUB_3TYPE_enlarge(sht, bte, int, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, bte, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, bte, hge, I)
#endif
SUB_3TYPE_enlarge(sht, bte, flt, F)
SUB_3TYPE_enlarge(sht, bte, dbl, F)
#endif
SUB_3TYPE(sht, sht, sht, I)
SUB_3TYPE_enlarge(sht, sht, int, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, sht, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, sht, hge, I)
#endif
SUB_3TYPE_enlarge(sht, sht, flt, F)
SUB_3TYPE_enlarge(sht, sht, dbl, F)
#endif
SUB_3TYPE(sht, int, int, I)
SUB_3TYPE_enlarge(sht, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, int, hge, I)
#endif
SUB_3TYPE_enlarge(sht, int, flt, F)
SUB_3TYPE_enlarge(sht, int, dbl, F)
#endif
SUB_3TYPE(sht, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(sht, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, lng, flt, F)
SUB_3TYPE_enlarge(sht, lng, dbl, F)
#endif
#ifdef HAVE_HGE
SUB_3TYPE(sht, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, hge, flt, F)
SUB_3TYPE_enlarge(sht, hge, dbl, F)
#endif
#endif
SUB_3TYPE(sht, flt, flt, F)
SUB_3TYPE_enlarge(sht, flt, dbl, F)
SUB_3TYPE(sht, dbl, dbl, F)
SUB_3TYPE(int, bte, int, I)
SUB_3TYPE_enlarge(int, bte, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, bte, hge, I)
#endif
SUB_3TYPE_enlarge(int, bte, flt, F)
SUB_3TYPE_enlarge(int, bte, dbl, F)
#endif
SUB_3TYPE(int, sht, int, I)
SUB_3TYPE_enlarge(int, sht, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, sht, hge, I)
#endif
SUB_3TYPE_enlarge(int, sht, flt, F)
SUB_3TYPE_enlarge(int, sht, dbl, F)
#endif
SUB_3TYPE(int, int, int, I)
SUB_3TYPE_enlarge(int, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, int, hge, I)
#endif
SUB_3TYPE_enlarge(int, int, flt, F)
SUB_3TYPE_enlarge(int, int, dbl, F)
#endif
SUB_3TYPE(int, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(int, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(int, lng, flt, F)
SUB_3TYPE_enlarge(int, lng, dbl, F)
#endif
#ifdef HAVE_HGE
SUB_3TYPE(int, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(int, hge, flt, F)
SUB_3TYPE_enlarge(int, hge, dbl, F)
#endif
#endif
SUB_3TYPE(int, flt, flt, F)
SUB_3TYPE_enlarge(int, flt, dbl, F)
SUB_3TYPE(int, dbl, dbl, F)
SUB_3TYPE(lng, bte, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, bte, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, bte, flt, F)
SUB_3TYPE_enlarge(lng, bte, dbl, F)
#endif
SUB_3TYPE(lng, sht, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, sht, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, sht, flt, F)
SUB_3TYPE_enlarge(lng, sht, dbl, F)
#endif
SUB_3TYPE(lng, int, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, int, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, int, flt, F)
SUB_3TYPE_enlarge(lng, int, dbl, F)
#endif
SUB_3TYPE(lng, lng, lng, I)
#ifdef HAVE_HGE
SUB_3TYPE_enlarge(lng, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, lng, flt, F)
SUB_3TYPE_enlarge(lng, lng, dbl, F)
#endif
#ifdef HAVE_HGE
SUB_3TYPE(lng, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, hge, flt, F)
SUB_3TYPE_enlarge(lng, hge, dbl, F)
#endif
#endif
SUB_3TYPE(lng, flt, flt, F)
SUB_3TYPE_enlarge(lng, flt, dbl, F)
SUB_3TYPE(lng, dbl, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(hge, bte, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, bte, flt, F)
SUB_3TYPE_enlarge(hge, bte, dbl, F)
#endif
SUB_3TYPE(hge, sht, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, sht, flt, F)
SUB_3TYPE_enlarge(hge, sht, dbl, F)
#endif
SUB_3TYPE(hge, int, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, int, flt, F)
SUB_3TYPE_enlarge(hge, int, dbl, F)
#endif
SUB_3TYPE(hge, lng, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, lng, flt, F)
SUB_3TYPE_enlarge(hge, lng, dbl, F)
#endif
SUB_3TYPE(hge, hge, hge, I)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(hge, hge, flt, F)
SUB_3TYPE_enlarge(hge, hge, dbl, F)
#endif
SUB_3TYPE(hge, flt, flt, F)
SUB_3TYPE_enlarge(hge, flt, dbl, F)
SUB_3TYPE(hge, dbl, dbl, F)
#endif
SUB_3TYPE(flt, bte, flt, F)
SUB_3TYPE_enlarge(flt, bte, dbl, F)
SUB_3TYPE(flt, sht, flt, F)
SUB_3TYPE_enlarge(flt, sht, dbl, F)
SUB_3TYPE(flt, int, flt, F)
SUB_3TYPE_enlarge(flt, int, dbl, F)
SUB_3TYPE(flt, lng, flt, F)
SUB_3TYPE_enlarge(flt, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(flt, hge, flt, F)
SUB_3TYPE_enlarge(flt, hge, dbl, F)
#endif
SUB_3TYPE(flt, flt, flt, F)
SUB_3TYPE_enlarge(flt, flt, dbl, F)
SUB_3TYPE(flt, dbl, dbl, F)
SUB_3TYPE(dbl, bte, dbl, F)
SUB_3TYPE(dbl, sht, dbl, F)
SUB_3TYPE(dbl, int, dbl, F)
SUB_3TYPE(dbl, lng, dbl, F)
#ifdef HAVE_HGE
SUB_3TYPE(dbl, hge, dbl, F)
#endif
SUB_3TYPE(dbl, flt, dbl, F)
SUB_3TYPE(dbl, dbl, dbl, F)

static BUN
sub_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1, struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *func)
{
	BUN nils;

	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	tp = ATOMbasetype(tp);
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = sub_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = sub_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = sub_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = sub_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = sub_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = sub_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = sub_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = sub_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = sub_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_bte_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = sub_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = sub_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = sub_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = sub_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = sub_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = sub_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = sub_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = sub_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_sht_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_int_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				nils = sub_int_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = sub_int_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = sub_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = sub_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = sub_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_int_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				nils = sub_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				nils = sub_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = sub_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = sub_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = sub_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_lng_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_hge_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				nils = sub_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_hge_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				nils = sub_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_hge_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				nils = sub_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_hge_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = sub_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = sub_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_hge_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = sub_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = sub_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = sub_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = sub_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = sub_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = sub_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_dbl_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = sub_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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

	return nils;

  unsupported:
	GDKerror("%s: type combination (sub(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcsub(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci1.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = sub_typeswitchloop(Tloc(b1, 0), b1->ttype, 1,
				  Tloc(b2, 0), b2->ttype, 1,
				  Tloc(bn, 0), tp,
				  &ci1, &ci2, rv,
				  b1->hseqbase, b2->hseqbase,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcsubcst(BAT *b, const ValRecord *v, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = sub_typeswitchloop(Tloc(b, 0), b->ttype, 1,
				  VALptr(v), v->vtype, 0,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  rv,
				  b->hseqbase, 0,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->tsorted = (abort_on_error && b->tsorted && nils == 0) ||
		ncand <= 1 || nils == ncand;
	bn->trevsorted = (abort_on_error && b->trevsorted && nils == 0) ||
		ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstsub(const ValRecord *v, BAT *b, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = sub_typeswitchloop(VALptr(v), v->vtype, 0,
				  Tloc(b, 0), b->ttype, 1,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  rv,
				  0, b->hseqbase,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is
	 * sorted in the opposite direction (except that NILs mess
	 * things up */
	bn->tsorted = (abort_on_error && nils == 0 && b->trevsorted) ||
		ncand <= 1 || nils == ncand;
	bn->trevsorted = (abort_on_error && nils == 0 && b->tsorted) ||
		ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

gdk_return
VARcalcsub(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	if (sub_typeswitchloop(VALptr(lft), lft->vtype, 0,
			       VALptr(rgt), rgt->vtype, 0,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

BAT *
BATcalcdecr(BAT *b, BAT *s, BAT *r, bool abort_on_error)
{
	return BATcalcincrdecr(b, s, r, abort_on_error, sub_typeswitchloop,
			       __func__);
}

gdk_return
VARcalcdecr(ValPtr ret, const ValRecord *v, bool abort_on_error)
{
	if (sub_typeswitchloop(VALptr(v), v->vtype, 0,
			       &(bte){1}, TYPE_bte, 0,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* multiplication (any numeric type) */

/* TYPE4 must be a type larger than both TYPE1 and TYPE2 so that
 * multiplying into it doesn't cause overflow */
#define MUL_4TYPE(TYPE1, TYPE2, TYPE3, TYPE4, IF)			\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			MUL##IF##4_WITH_CHECK(lft[i], rgt[j],		\
					      TYPE3, dst[k],		\
					      max,			\
					      TYPE4,			\
					      ON_OVERFLOW(TYPE1, TYPE2, "*")); \
		}							\
	}								\
	return nils;							\
}

#define MUL_3TYPE_enlarge(TYPE1, TYPE2, TYPE3, IF)			\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
	const bool couldoverflow = (max < (TYPE3) GDK_##TYPE1##_max * (TYPE3) GDK_##TYPE2##_max); \
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else if (couldoverflow) {				\
			MUL##IF##4_WITH_CHECK(lft[i], rgt[j],		\
					      TYPE3, dst[k],		\
					      max,			\
					      TYPE3,			\
					      ON_OVERFLOW(TYPE1, TYPE2, "*")); \
		} else {						\
			dst[k] = (TYPE3) lft[i] * rgt[j];		\
		}							\
	}								\
	return nils;							\
}

#ifdef HAVE_HGE

#define MUL_2TYPE_lng(TYPE1, TYPE2)	MUL_4TYPE(TYPE1, TYPE2, lng, hge, I)

#define MUL_2TYPE_hge(TYPE1, TYPE2)					\
static BUN								\
mul_##TYPE1##_##TYPE2##_hge(const TYPE1 *lft, int incr1,		\
			    const TYPE2 *rgt, int incr2,		\
			    hge *restrict dst, hge max,			\
			    struct canditer *restrict ci1,		\
			    struct canditer *restrict ci2,		\
			    const bit *restrict rv,			\
			    oid candoff1, oid candoff2,			\
			    bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = hge_nil;				\
			nils++;						\
		} else {						\
			HGEMUL_CHECK(lft[i], rgt[j],			\
				     dst[k],				\
				     max,				\
				     ON_OVERFLOW(TYPE1, TYPE2, "*"));	\
		}							\
	}								\
	return nils;							\
}

#else

#define MUL_2TYPE_lng(TYPE1, TYPE2)					\
static BUN								\
mul_##TYPE1##_##TYPE2##_lng(const TYPE1 *lft, int incr1,		\
			    const TYPE2 *rgt, int incr2,		\
			    lng *restrict dst, lng max,			\
			    struct canditer *restrict ci1,		\
			    struct canditer *restrict ci2,		\
			    const bit *restrict rv,			\
			    oid candoff1, oid candoff2,			\
			    bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = lng_nil;				\
			nils++;						\
		} else {						\
			LNGMUL_CHECK(lft[i], rgt[j],			\
				     dst[k],				\
				     max,				\
				     ON_OVERFLOW(TYPE1, TYPE2, "*"));	\
		}							\
	}								\
	return nils;							\
}

#endif

#define MUL_2TYPE_float(TYPE1, TYPE2, TYPE3)				\
static BUN								\
mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)	\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			/* only check for overflow, not for underflow */ \
			dst[k] = (TYPE3) (lft[i] * rgt[j]);		\
			if (isinf(dst[k]) || ABSOLUTE(dst[k]) > max) {	\
				if (abort_on_error)			\
					ON_OVERFLOW(TYPE1, TYPE2, "*");	\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

MUL_4TYPE(bte, bte, bte, sht, I)
MUL_3TYPE_enlarge(bte, bte, sht, I)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, bte, int, I)
MUL_3TYPE_enlarge(bte, bte, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, bte, hge, I)
#endif
MUL_3TYPE_enlarge(bte, bte, flt, F)
MUL_3TYPE_enlarge(bte, bte, dbl, F)
#endif
MUL_4TYPE(bte, sht, sht, int, I)
MUL_3TYPE_enlarge(bte, sht, int, I)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, sht, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, sht, hge, I)
#endif
MUL_3TYPE_enlarge(bte, sht, flt, F)
MUL_3TYPE_enlarge(bte, sht, dbl, F)
#endif
MUL_4TYPE(bte, int, int, lng, I)
MUL_3TYPE_enlarge(bte, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, int, hge, I)
#endif
MUL_3TYPE_enlarge(bte, int, flt, F)
MUL_3TYPE_enlarge(bte, int, dbl, F)
#endif
MUL_2TYPE_lng(bte, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(bte, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, lng, flt, F)
MUL_3TYPE_enlarge(bte, lng, dbl, F)
#endif
#ifdef HAVE_HGE
MUL_2TYPE_hge(bte, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, hge, flt, F)
MUL_3TYPE_enlarge(bte, hge, dbl, F)
#endif
#endif
MUL_2TYPE_float(bte, flt, flt)
MUL_3TYPE_enlarge(bte, flt, dbl, F)
MUL_2TYPE_float(bte, dbl, dbl)
MUL_4TYPE(sht, bte, sht, int, I)
MUL_3TYPE_enlarge(sht, bte, int, I)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, bte, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, bte, hge, I)
#endif
MUL_3TYPE_enlarge(sht, bte, flt, F)
MUL_3TYPE_enlarge(sht, bte, dbl, F)
#endif
MUL_4TYPE(sht, sht, sht, int, I)
MUL_3TYPE_enlarge(sht, sht, int, I)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, sht, lng, I)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, sht, hge, I)
#endif
MUL_3TYPE_enlarge(sht, sht, flt, F)
MUL_3TYPE_enlarge(sht, sht, dbl, F)
#endif
MUL_4TYPE(sht, int, int, lng, I)
MUL_3TYPE_enlarge(sht, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, int, hge, I)
#endif
MUL_3TYPE_enlarge(sht, int, flt, F)
MUL_3TYPE_enlarge(sht, int, dbl, F)
#endif
MUL_2TYPE_lng(sht, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(sht, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, lng, flt, F)
MUL_3TYPE_enlarge(sht, lng, dbl, F)
#endif
#ifdef HAVE_HGE
MUL_2TYPE_hge(sht, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, hge, flt, F)
MUL_3TYPE_enlarge(sht, hge, dbl, F)
#endif
#endif
MUL_2TYPE_float(sht, flt, flt)
MUL_3TYPE_enlarge(sht, flt, dbl, F)
MUL_2TYPE_float(sht, dbl, dbl)
MUL_4TYPE(int, bte, int, lng, I)
MUL_3TYPE_enlarge(int, bte, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, bte, hge, I)
#endif
MUL_3TYPE_enlarge(int, bte, flt, F)
MUL_3TYPE_enlarge(int, bte, dbl, F)
#endif
MUL_4TYPE(int, sht, int, lng, I)
MUL_3TYPE_enlarge(int, sht, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, sht, hge, I)
#endif
MUL_3TYPE_enlarge(int, sht, flt, F)
MUL_3TYPE_enlarge(int, sht, dbl, F)
#endif
MUL_4TYPE(int, int, int, lng, I)
MUL_3TYPE_enlarge(int, int, lng, I)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, int, hge, I)
#endif
MUL_3TYPE_enlarge(int, int, flt, F)
MUL_3TYPE_enlarge(int, int, dbl, F)
#endif
MUL_2TYPE_lng(int, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(int, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(int, lng, flt, F)
MUL_3TYPE_enlarge(int, lng, dbl, F)
#endif
#ifdef HAVE_HGE
MUL_2TYPE_hge(int, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(int, hge, flt, F)
MUL_3TYPE_enlarge(int, hge, dbl, F)
#endif
#endif
MUL_2TYPE_float(int, flt, flt)
MUL_3TYPE_enlarge(int, flt, dbl, F)
MUL_2TYPE_float(int, dbl, dbl)
MUL_2TYPE_lng(lng, bte)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, bte, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, bte, flt, F)
MUL_3TYPE_enlarge(lng, bte, dbl, F)
#endif
MUL_2TYPE_lng(lng, sht)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, sht, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, sht, flt, F)
MUL_3TYPE_enlarge(lng, sht, dbl, F)
#endif
MUL_2TYPE_lng(lng, int)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, int, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, int, flt, F)
MUL_3TYPE_enlarge(lng, int, dbl, F)
#endif
MUL_2TYPE_lng(lng, lng)
#ifdef HAVE_HGE
MUL_3TYPE_enlarge(lng, lng, hge, I)
#endif
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, lng, flt, F)
MUL_3TYPE_enlarge(lng, lng, dbl, F)
#endif
#ifdef HAVE_HGE
MUL_2TYPE_hge(lng, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, hge, flt, F)
MUL_3TYPE_enlarge(lng, hge, dbl, F)
#endif
#endif
MUL_2TYPE_float(lng, flt, flt)
MUL_3TYPE_enlarge(lng, flt, dbl, F)
MUL_2TYPE_float(lng, dbl, dbl)
#ifdef HAVE_HGE
MUL_2TYPE_hge(hge, bte)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, bte, flt, F)
MUL_3TYPE_enlarge(hge, bte, dbl, F)
#endif
MUL_2TYPE_hge(hge, sht)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, sht, flt, F)
MUL_3TYPE_enlarge(hge, sht, dbl, F)
#endif
MUL_2TYPE_hge(hge, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, int, flt, F)
MUL_3TYPE_enlarge(hge, int, dbl, F)
#endif
MUL_2TYPE_hge(hge, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, lng, flt, F)
MUL_3TYPE_enlarge(hge, lng, dbl, F)
#endif
MUL_2TYPE_hge(hge, hge)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(hge, hge, flt, F)
MUL_3TYPE_enlarge(hge, hge, dbl, F)
#endif
MUL_2TYPE_float(hge, flt, flt)
MUL_3TYPE_enlarge(hge, flt, dbl, F)
MUL_2TYPE_float(hge, dbl, dbl)
#endif
MUL_2TYPE_float(flt, bte, flt)
MUL_3TYPE_enlarge(flt, bte, dbl, F)
MUL_2TYPE_float(flt, sht, flt)
MUL_3TYPE_enlarge(flt, sht, dbl, F)
MUL_2TYPE_float(flt, int, flt)
MUL_3TYPE_enlarge(flt, int, dbl, F)
MUL_2TYPE_float(flt, lng, flt)
MUL_3TYPE_enlarge(flt, lng, dbl, F)
#ifdef HAVE_HGE
MUL_2TYPE_float(flt, hge, flt)
MUL_3TYPE_enlarge(flt, hge, dbl, F)
#endif
MUL_2TYPE_float(flt, flt, flt)
MUL_3TYPE_enlarge(flt, flt, dbl, F)
MUL_2TYPE_float(flt, dbl, dbl)
MUL_2TYPE_float(dbl, bte, dbl)
MUL_2TYPE_float(dbl, sht, dbl)
MUL_2TYPE_float(dbl, int, dbl)
MUL_2TYPE_float(dbl, lng, dbl)
#ifdef HAVE_HGE
MUL_2TYPE_float(dbl, hge, dbl)
#endif
MUL_2TYPE_float(dbl, flt, dbl)
MUL_2TYPE_float(dbl, dbl, dbl)

static BUN
mul_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *func)
{
	BUN nils;

	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	tp = ATOMbasetype(tp);
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = mul_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mul_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mul_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mul_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mul_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mul_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mul_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mul_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mul_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mul_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mul_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_int_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				nils = mul_int_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mul_int_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = mul_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mul_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				nils = mul_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				nils = mul_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mul_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mul_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				nils = mul_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				nils = mul_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				nils = mul_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mul_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_hge_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mul_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mul_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mul_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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

	return nils;

  unsupported:
	GDKerror("%s: type combination mul(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

static BAT *
BATcalcmuldivmod(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r, int tp,
		 bool abort_on_error,
		 BUN (*typeswitchloop)(const void *, int, int,
				       const void *, int, int,
				       void *restrict, int,
				       struct canditer *restrict,
				       struct canditer *restrict,
				       const bit *restrict,
				       oid, oid, bool, const char *),
		 const char *func)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, func, NULL);
	BATcheck(b2, func, NULL);

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", func);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci1.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = (*typeswitchloop)(Tloc(b1, 0), b1->ttype, 1,
				 Tloc(b2, 0), b2->ttype, 1,
				 Tloc(bn, 0), tp,
				 &ci1, &ci2, rv, b1->hseqbase, b2->hseqbase,
				 abort_on_error, func);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcmul(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r, int tp, bool abort_on_error)
{
	return BATcalcmuldivmod(b1, b2, s1, s2, r, tp, abort_on_error,
				mul_typeswitchloop, __func__);
}

BAT *
BATcalcmulcst(BAT *b, const ValRecord *v, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = mul_typeswitchloop(Tloc(b, 0), b->ttype, 1,
				  VALptr(v), v->vtype, 0,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  rv,
				  b->hseqbase, 0,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted, or reverse sorted if the constant is negative */
	if (abort_on_error) {
		ValRecord sign;

		VARcalcsign(&sign, v);
		bn->tsorted = (sign.val.btval >= 0 && b->tsorted && nils == 0) ||
			(sign.val.btval <= 0 && b->trevsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
		bn->trevsorted = (sign.val.btval >= 0 && b->trevsorted && nils == 0) ||
			(sign.val.btval <= 0 && b->tsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
	} else {
		bn->tsorted = ncand <= 1 || nils == ncand;
		bn->trevsorted = ncand <= 1 || nils == ncand;
	}
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstmul(const ValRecord *v, BAT *b, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = mul_typeswitchloop(VALptr(v), v->vtype, 0,
				  Tloc(b, 0), b->ttype, 1,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  rv,
				  0, b->hseqbase,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted, or reverse sorted if the constant is negative */
	if (abort_on_error) {
		ValRecord sign;

		VARcalcsign(&sign, v);
		bn->tsorted = (sign.val.btval >= 0 && b->tsorted && nils == 0) ||
			(sign.val.btval <= 0 && b->trevsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
		bn->trevsorted = (sign.val.btval >= 0 && b->trevsorted && nils == 0) ||
			(sign.val.btval <= 0 && b->tsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
	} else {
		bn->tsorted = ncand <= 1 || nils == ncand;
		bn->trevsorted = ncand <= 1 || nils == ncand;
	}
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

gdk_return
VARcalcmul(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	if (mul_typeswitchloop(VALptr(lft), lft->vtype, 0,
			       VALptr(rgt), rgt->vtype, 0,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* division (any numeric type) */

#define DIV_3TYPE(TYPE1, TYPE2, TYPE3)					\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else if (rgt[j] == 0) {				\
			if (abort_on_error)				\
				return BUN_NONE + 1;			\
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[k] = (TYPE3) (lft[i] / rgt[j]);		\
			if (dst[k] < -max || dst[k] > max) {		\
				if (abort_on_error)			\
					return BUN_NONE + 2;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

#define DIV_3TYPE_float(TYPE1, TYPE2, TYPE3)				\
static BUN								\
div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst, TYPE3 max,		\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else if (rgt[j] == 0 ||				\
			   (ABSOLUTE(rgt[j]) < 1 &&			\
			    GDK_##TYPE3##_max * ABSOLUTE(rgt[j]) < lft[i])) { \
			/* only check for overflow, not for underflow */ \
			if (abort_on_error) {				\
				if (rgt[j] == 0)			\
					return BUN_NONE + 1;		\
				ON_OVERFLOW(TYPE1, TYPE2, "/");		\
			}						\
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[k] = (TYPE3) lft[i] / rgt[j];		\
			if (dst[k] < -max || dst[k] > max) {		\
				if (abort_on_error)			\
					return BUN_NONE + 2;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			}						\
		}							\
	}								\
	return nils;							\
}

DIV_3TYPE(bte, bte, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, bte, sht)
DIV_3TYPE(bte, bte, int)
DIV_3TYPE(bte, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE(bte, bte, hge)
#endif
#endif
DIV_3TYPE(bte, bte, flt)
DIV_3TYPE(bte, bte, dbl)
DIV_3TYPE(bte, sht, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, sht, sht)
DIV_3TYPE(bte, sht, int)
DIV_3TYPE(bte, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE(bte, sht, hge)
#endif
#endif
DIV_3TYPE(bte, sht, flt)
DIV_3TYPE(bte, sht, dbl)
DIV_3TYPE(bte, int, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, int, sht)
DIV_3TYPE(bte, int, int)
DIV_3TYPE(bte, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE(bte, int, hge)
#endif
#endif
DIV_3TYPE(bte, int, flt)
DIV_3TYPE(bte, int, dbl)
DIV_3TYPE(bte, lng, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, lng, sht)
DIV_3TYPE(bte, lng, int)
DIV_3TYPE(bte, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE(bte, lng, hge)
#endif
#endif
DIV_3TYPE(bte, lng, flt)
DIV_3TYPE(bte, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(bte, hge, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, hge, sht)
DIV_3TYPE(bte, hge, int)
DIV_3TYPE(bte, hge, lng)
DIV_3TYPE(bte, hge, hge)
#endif
DIV_3TYPE(bte, hge, flt)
DIV_3TYPE(bte, hge, dbl)
#endif
DIV_3TYPE_float(bte, flt, flt)
DIV_3TYPE_float(bte, flt, dbl)
DIV_3TYPE_float(bte, dbl, dbl)
DIV_3TYPE(sht, bte, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, bte, int)
DIV_3TYPE(sht, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE(sht, bte, hge)
#endif
#endif
DIV_3TYPE(sht, bte, flt)
DIV_3TYPE(sht, bte, dbl)
DIV_3TYPE(sht, sht, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, sht, int)
DIV_3TYPE(sht, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE(sht, sht, hge)
#endif
#endif
DIV_3TYPE(sht, sht, flt)
DIV_3TYPE(sht, sht, dbl)
DIV_3TYPE(sht, int, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, int, int)
DIV_3TYPE(sht, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE(sht, int, hge)
#endif
#endif
DIV_3TYPE(sht, int, flt)
DIV_3TYPE(sht, int, dbl)
DIV_3TYPE(sht, lng, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, lng, int)
DIV_3TYPE(sht, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE(sht, lng, hge)
#endif
#endif
DIV_3TYPE(sht, lng, flt)
DIV_3TYPE(sht, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(sht, hge, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, hge, int)
DIV_3TYPE(sht, hge, lng)
DIV_3TYPE(sht, hge, hge)
#endif
DIV_3TYPE(sht, hge, flt)
DIV_3TYPE(sht, hge, dbl)
#endif
DIV_3TYPE_float(sht, flt, flt)
DIV_3TYPE_float(sht, flt, dbl)
DIV_3TYPE_float(sht, dbl, dbl)
DIV_3TYPE(int, bte, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, bte, lng)
#ifdef HAVE_HGE
DIV_3TYPE(int, bte, hge)
#endif
#endif
DIV_3TYPE(int, bte, flt)
DIV_3TYPE(int, bte, dbl)
DIV_3TYPE(int, sht, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, sht, lng)
#ifdef HAVE_HGE
DIV_3TYPE(int, sht, hge)
#endif
#endif
DIV_3TYPE(int, sht, flt)
DIV_3TYPE(int, sht, dbl)
DIV_3TYPE(int, int, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, int, lng)
#ifdef HAVE_HGE
DIV_3TYPE(int, int, hge)
#endif
#endif
DIV_3TYPE(int, int, flt)
DIV_3TYPE(int, int, dbl)
DIV_3TYPE(int, lng, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, lng, lng)
#ifdef HAVE_HGE
DIV_3TYPE(int, lng, hge)
#endif
#endif
DIV_3TYPE(int, lng, flt)
DIV_3TYPE(int, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(int, hge, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, hge, lng)
DIV_3TYPE(int, hge, hge)
#endif
DIV_3TYPE(int, hge, flt)
DIV_3TYPE(int, hge, dbl)
#endif
DIV_3TYPE_float(int, flt, flt)
DIV_3TYPE_float(int, flt, dbl)
DIV_3TYPE_float(int, dbl, dbl)
DIV_3TYPE(lng, bte, lng)
#ifdef HAVE_HGE
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(lng, bte, hge)
#endif
#endif
DIV_3TYPE(lng, bte, flt)
DIV_3TYPE(lng, bte, dbl)
DIV_3TYPE(lng, sht, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
DIV_3TYPE(lng, sht, hge)
#endif
#endif
DIV_3TYPE(lng, sht, flt)
DIV_3TYPE(lng, sht, dbl)
DIV_3TYPE(lng, int, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
DIV_3TYPE(lng, int, hge)
#endif
#endif
DIV_3TYPE(lng, int, flt)
DIV_3TYPE(lng, int, dbl)
DIV_3TYPE(lng, lng, lng)
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
DIV_3TYPE(lng, lng, hge)
#endif
#endif
DIV_3TYPE(lng, lng, flt)
DIV_3TYPE(lng, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(lng, hge, lng)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(lng, hge, hge)
#endif
DIV_3TYPE(lng, hge, flt)
DIV_3TYPE(lng, hge, dbl)
#endif
DIV_3TYPE_float(lng, flt, flt)
DIV_3TYPE_float(lng, flt, dbl)
DIV_3TYPE_float(lng, dbl, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(hge, bte, hge)
DIV_3TYPE(hge, bte, flt)
DIV_3TYPE(hge, bte, dbl)
DIV_3TYPE(hge, sht, hge)
DIV_3TYPE(hge, sht, flt)
DIV_3TYPE(hge, sht, dbl)
DIV_3TYPE(hge, int, hge)
DIV_3TYPE(hge, int, flt)
DIV_3TYPE(hge, int, dbl)
DIV_3TYPE(hge, lng, hge)
DIV_3TYPE(hge, lng, flt)
DIV_3TYPE(hge, lng, dbl)
DIV_3TYPE(hge, hge, hge)
DIV_3TYPE(hge, hge, flt)
DIV_3TYPE(hge, hge, dbl)
DIV_3TYPE_float(hge, flt, flt)
DIV_3TYPE_float(hge, flt, dbl)
DIV_3TYPE_float(hge, dbl, dbl)
#endif
DIV_3TYPE(flt, bte, flt)
DIV_3TYPE(flt, bte, dbl)
DIV_3TYPE(flt, sht, flt)
DIV_3TYPE(flt, sht, dbl)
DIV_3TYPE(flt, int, flt)
DIV_3TYPE(flt, int, dbl)
DIV_3TYPE(flt, lng, flt)
DIV_3TYPE(flt, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(flt, hge, flt)
DIV_3TYPE(flt, hge, dbl)
#endif
DIV_3TYPE_float(flt, flt, flt)
DIV_3TYPE_float(flt, flt, dbl)
DIV_3TYPE_float(flt, dbl, dbl)
DIV_3TYPE(dbl, bte, dbl)
DIV_3TYPE(dbl, sht, dbl)
DIV_3TYPE(dbl, int, dbl)
DIV_3TYPE(dbl, lng, dbl)
#ifdef HAVE_HGE
DIV_3TYPE(dbl, hge, dbl)
#endif
DIV_3TYPE_float(dbl, flt, dbl)
DIV_3TYPE_float(dbl, dbl, dbl)

static BUN
div_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1, struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *func)
{
	BUN nils;

	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	tp = ATOMbasetype(tp);
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_sht_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_int_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_int_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_lng_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_lng_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_lng_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_bte:
				nils = div_bte_hge_bte(lft, incr1, rgt, incr2,
						       dst, GDK_bte_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_hge_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_hge_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = div_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_int_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_lng_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_lng_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_sht:
				nils = div_sht_hge_sht(lft, incr1, rgt, incr2,
						       dst, GDK_sht_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_hge_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = div_int_bte_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_int:
				nils = div_int_sht_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = div_int_int_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_int_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_int:
				nils = div_int_lng_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_int:
				nils = div_int_hge_int(lft, incr1, rgt, incr2,
						       dst, GDK_int_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = div_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = div_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = div_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
#endif
			case TYPE_flt:
				nils = div_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_lng:
				nils = div_lng_hge_lng(lft, incr1, rgt, incr2,
						       dst, GDK_lng_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_hge:
				nils = div_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_lng_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = div_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = div_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, GDK_hge_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_hge_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_hge_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = div_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = div_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, GDK_flt_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = div_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_hge_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = div_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, GDK_dbl_max,
						       ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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

	if (nils == BUN_NONE + 1)
		GDKerror("22012!division by zero.\n");

	return nils;

  unsupported:
	GDKerror("%s: type combination (div(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcdiv(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r, int tp, bool abort_on_error)
{
	return BATcalcmuldivmod(b1, b2, s1, s2, r, tp, abort_on_error,
				div_typeswitchloop, __func__);
}

BAT *
BATcalcdivcst(BAT *b, const ValRecord *v, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = div_typeswitchloop(Tloc(b, 0), b->ttype, 1,
				  VALptr(v), v->vtype, 0,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  rv,
				  b->hseqbase, 0,
				  abort_on_error, __func__);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	/* if the input is sorted, and no zero division occurred (we
	 * only know for sure if abort_on_error is set), the result is
	 * also sorted, or reverse sorted if the constant is
	 * negative */
	if (abort_on_error) {
		ValRecord sign;

		VARcalcsign(&sign, v);
		bn->tsorted = (sign.val.btval > 0 && b->tsorted && nils == 0) ||
			(sign.val.btval < 0 && b->trevsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
		bn->trevsorted = (sign.val.btval > 0 && b->trevsorted && nils == 0) ||
			(sign.val.btval < 0 && b->tsorted && nils == 0) ||
			ncand <= 1 || nils == ncand;
	} else {
		bn->tsorted = ncand <= 1 || nils == ncand;
		bn->trevsorted = ncand <= 1 || nils == ncand;
	}
	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstdiv(const ValRecord *v, BAT *b, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0)
		return BATconstant(ci.hseq, tp, ATOMnilptr(tp),
				   ncand, TRANSIENT);

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = div_typeswitchloop(VALptr(v), v->vtype, 0,
				  Tloc(b, 0), b->ttype, 1,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  rv,
				  0, b->hseqbase,
				  abort_on_error, __func__);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

gdk_return
VARcalcdiv(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	if (div_typeswitchloop(VALptr(lft), lft->vtype, 0,
			       VALptr(rgt), rgt->vtype, 0,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) >= BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* modulo (any numeric type) */

#define MOD_3TYPE(TYPE1, TYPE2, TYPE3)					\
static BUN								\
mod_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst,			\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else if (rgt[j] == 0) {				\
			if (abort_on_error)				\
				return BUN_NONE + 1;			\
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[k] = (TYPE3) lft[i] % rgt[j];		\
		}							\
	}								\
	return nils;							\
}

#define FMOD_3TYPE(TYPE1, TYPE2, TYPE3, FUNC)				\
static BUN								\
mod_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1,		\
				const TYPE2 *rgt, int incr2,		\
				TYPE3 *restrict dst,			\
				struct canditer *restrict ci1,		\
				struct canditer *restrict ci2,		\
				const bit *restrict rv,			\
				oid candoff1, oid candoff2,		\
				bool abort_on_error)			\
{									\
	BUN nils = 0;							\
									\
	for (BUN k = 0; k < ci1->ncand; k++) {				\
		oid x1 = canditer_next(ci1) - candoff1;			\
		oid x2 = canditer_next(ci2) - candoff2;			\
		BUN i = x1 * incr1;					\
		BUN j = x2 * incr2;					\
		if ((rv != NULL && !rv[i]) ||				\
		    is_##TYPE1##_nil(lft[i]) || is_##TYPE2##_nil(rgt[j])) { \
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else if (rgt[j] == 0) {				\
			if (abort_on_error)				\
				return BUN_NONE + 1;			\
			dst[k] = TYPE3##_nil;				\
			nils++;						\
		} else {						\
			dst[k] = (TYPE3) FUNC((TYPE3) lft[i],		\
					      (TYPE3) rgt[j]);		\
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
mod_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1, struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *func)
{
	BUN nils;

	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	tp = ATOMbasetype(tp);
	switch (tp1) {
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
			switch (tp) {
			case TYPE_bte:
				nils = mod_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_bte_sht_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_bte_int_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_int_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_bte_lng_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_lng_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_lng_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_bte_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_bte_hge_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_hge_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_hge_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_hge_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_bte_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_sht_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_sht_int_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_sht_lng_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_lng_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_sht_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_sht_hge_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_hge_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_hge_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_sht_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_int_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_int_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_int_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_int_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_int_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_int_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_int_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_int_lng_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_int_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_int_hge_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_int_hge_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_int_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_lng_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_lng_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_lng_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_lng_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_lng_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_lng_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
#ifdef HAVE_HGE
			case TYPE_hge:
				nils = mod_lng_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_lng_hge_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_hge:
				nils = mod_lng_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_hge_bte_bte(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_hge_bte_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_hge_bte_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_hge_bte_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_bte_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_sht:
				nils = mod_hge_sht_sht(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_hge_sht_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_hge_sht_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_sht_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_int:
				nils = mod_hge_int_int(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_hge_int_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			case TYPE_hge:
				nils = mod_hge_int_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_lng:
				nils = mod_hge_lng_lng(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_hge:
				nils = mod_hge_lng_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_hge:
			switch (tp) {
			case TYPE_hge:
				nils = mod_hge_hge_hge(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_hge_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_hge_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_hge_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_flt:
				nils = mod_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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
				nils = mod_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_hge_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
#endif
		case TYPE_flt:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (tp) {
			case TYPE_dbl:
				nils = mod_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, ci1, ci2, rv, candoff1, candoff2,
						       abort_on_error);
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

	if (nils == BUN_NONE + 1)
		GDKerror("22012!division by zero.\n");

	return nils;

  unsupported:
	GDKerror("%s: type combination (mod(%s,%s)->%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcmod(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r, int tp, bool abort_on_error)
{
	return BATcalcmuldivmod(b1, b2, s1, s2, r, tp, abort_on_error,
				mod_typeswitchloop, __func__);
}

BAT *
BATcalcmodcst(BAT *b, const ValRecord *v, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = mod_typeswitchloop(Tloc(b, 0), b->ttype, 1,
				  VALptr(v), v->vtype, 0,
				  Tloc(bn, 0), tp,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  rv,
				  b->hseqbase, 0,
				  abort_on_error, __func__);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstmod(const ValRecord *v, BAT *b, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = mod_typeswitchloop(VALptr(v), v->vtype, 0,
				  Tloc(b, 0), b->ttype, 1,
				  Tloc(bn, 0), tp,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  rv,
				  0, b->hseqbase,
				  abort_on_error, __func__);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

gdk_return
VARcalcmod(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	if (mod_typeswitchloop(VALptr(lft), lft->vtype, 0,
			       VALptr(rgt), rgt->vtype, 0,
			       VALget(ret), ret->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) >= BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) exclusive OR */

#define XOR(a, b)	((a) ^ (b))
#define XORBIT(a, b)	(((a) == 0) != ((b) == 0))

static BUN
xor_typeswitchloop(const void *lft, int incr1,
		   const void *rgt, int incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2,
		   bool nonil, const char *func)
{
	oid x1, x2;
	BUN i, j, k;
	BUN nils = 0;

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bit, bit, bit, XORBIT);
			else
				BINARY_3TYPE_FUNC(bit, bit, bit, XORBIT);
		} else {
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bte, XOR);
			else
				BINARY_3TYPE_FUNC(bte, bte, bte, XOR);
		}
		break;
	case TYPE_sht:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(sht, sht, sht, XOR);
		else
			BINARY_3TYPE_FUNC(sht, sht, sht, XOR);
		break;
	case TYPE_int:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(int, int, int, XOR);
		else
			BINARY_3TYPE_FUNC(int, int, int, XOR);
		break;
	case TYPE_lng:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(lng, lng, lng, XOR);
		else
			BINARY_3TYPE_FUNC(lng, lng, lng, XOR);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(hge, hge, hge, XOR);
		else
			BINARY_3TYPE_FUNC(hge, hge, hge, XOR);
		break;
#endif
	default:
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return BUN_NONE;
	}

	return nils;
}

BAT *
BATcalcxor(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = xor_typeswitchloop(Tloc(b1, 0), 1,
				  Tloc(b2, 0), 1,
				  Tloc(bn, 0),
				  b1->ttype,
				  &ci1, &ci2,
				  rv,
				  b1->hseqbase, b2->hseqbase,
				  b1->tnonil && b2->tnonil,
				  __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcxorcst(BAT *b, const ValRecord *v, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = xor_typeswitchloop(Tloc(b, 0), 1,
				  VALptr(v), 0,
				  Tloc(bn, 0), b->ttype,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  rv,
				  b->hseqbase, 0,
				  b->tnonil && ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0,
				  __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstxor(const ValRecord *v, BAT *b, BAT *s, BAT *r)
{
	return BATcalcxorcst(b, v, s, r);
}

gdk_return
VARcalcxor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("VARcalccstxor: incompatible input types.\n");
		return GDK_FAIL;
	}

	if (xor_typeswitchloop(VALptr(lft), 0,
			       VALptr(rgt), 0,
			       VALget(ret), lft->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, false, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) OR */

#define or3(a,b)	((a) == 1 || (b) == 1 ? 1 : is_bit_nil(a) || is_bit_nil(b) ? bit_nil : 0)

#define OR(a, b)	((a) | (b))

static BUN
or_typeswitchloop(const void *lft, int incr1,
		  const void *rgt, int incr2,
		  void *restrict dst, int tp,
		  struct canditer *restrict ci1,
		  struct canditer *restrict ci2,
		  const bit *restrict rv,
		  oid candoff1, oid candoff2,
		  bool nonil, const char *func)
{
	oid x1, x2;
	BUN i, j, k;
	BUN nils = 0;

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			/* implement tri-Boolean algebra */
			for (k = 0; k < ci1->ncand; k++) {
				x1 = canditer_next(ci1) - candoff1;
				x2 = canditer_next(ci2) - candoff2;
				i = x1 * incr1;
				j = x2 * incr2;
				bit v1 = ((const bit *) lft)[i];
				bit v2 = ((const bit *) rgt)[j];
				((bit *) dst)[k] = or3(v1, v2);
				nils += is_bit_nil(((bit *) dst)[k]);
			}
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
BATcalcor(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = or_typeswitchloop(Tloc(b1, 0), 1,
				 Tloc(b2, 0), 1,
				 Tloc(bn, 0),
				 b1->ttype,
				 &ci1, &ci2, rv, b1->hseqbase, b2->hseqbase,
				 b1->tnonil && b2->tnonil,
				 __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcorcst(BAT *b, const ValRecord *v, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = or_typeswitchloop(Tloc(b, 0), 1,
				 VALptr(v), 0,
				 Tloc(bn, 0), b->ttype,
				 &ci,
				 &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				 rv,
				 b->hseqbase, 0,
				 b->tnonil && ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0,
				 __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstor(const ValRecord *v, BAT *b, BAT *s, BAT *r)
{
	return BATcalcorcst(b, v, s, r);
}

gdk_return
VARcalcor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("VARcalccstor: incompatible input types.\n");
		return GDK_FAIL;
	}

	if (or_typeswitchloop(VALptr(lft), 0,
			      VALptr(rgt), 0,
			      VALget(ret), lft->vtype,
			      &(struct canditer){.tpe=cand_dense, .ncand=1},
			      &(struct canditer){.tpe=cand_dense, .ncand=1},
			      NULL,
			      0, 0, false, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* logical (for type bit) or bitwise (for integral types) exclusive AND */

#define and3(a,b)	((a) == 0 || (b) == 0 ? 0 : is_bit_nil(a) || is_bit_nil(b) ? bit_nil : 1)

#define AND(a, b)	((a) & (b))

static BUN
and_typeswitchloop(const void *lft, int incr1,
		   const void *rgt, int incr2,
		   void *restrict dst, int tp,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2,
		   bool nonil, const char *func)
{
	oid x1, x2;
	BUN i, j, k;
	BUN nils = 0;

	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			/* implement tri-Boolean algebra */
			for (k = 0; k < ci1->ncand; k++) {
				x1 = canditer_next(ci1) - candoff1;
				x2 = canditer_next(ci2) - candoff2;
				i = x1 * incr1;
				j = x2 * incr2;
				bit v1 = ((const bit *) lft)[i];
				bit v2 = ((const bit *) rgt)[j];
				((bit *) dst)[k] = and3(v1, v2);
				nils += is_bit_nil(((bit *) dst)[k]);
			}
		} else {
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bte, AND);
			else
				BINARY_3TYPE_FUNC(bte, bte, bte, AND);
		}
		break;
	case TYPE_sht:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(sht, sht, sht, AND);
		else
			BINARY_3TYPE_FUNC(sht, sht, sht, AND);
		break;
	case TYPE_int:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(int, int, int, AND);
		else
			BINARY_3TYPE_FUNC(int, int, int, AND);
		break;
	case TYPE_lng:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(lng, lng, lng, AND);
		else
			BINARY_3TYPE_FUNC(lng, lng, lng, AND);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(hge, hge, hge, AND);
		else
			BINARY_3TYPE_FUNC(hge, hge, hge, AND);
		break;
#endif
	default:
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return BUN_NONE;
	}

	return nils;
}

BAT *
BATcalcand(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (ATOMbasetype(b1->ttype) != ATOMbasetype(b2->ttype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = and_typeswitchloop(Tloc(b1, 0), 1,
				  Tloc(b2, 0), 1,
				  Tloc(bn, 0),
				  b1->ttype,
				  &ci1, &ci2, rv, b1->hseqbase, b2->hseqbase,
				  b1->tnonil && b2->tnonil,
				  __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcandcst(BAT *b, const ValRecord *v, BAT *s, BAT *r)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(v->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = and_typeswitchloop(Tloc(b, 0), 1,
				  VALptr(v), 0,
				  Tloc(bn, 0), b->ttype,
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  rv,
				  b->hseqbase, 0,
				  b->tnonil && ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0,
				  __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstand(const ValRecord *v, BAT *b, BAT *s, BAT *r)
{
	return BATcalcandcst(b, v, s, r);
}

gdk_return
VARcalcand(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMbasetype(lft->vtype) != ATOMbasetype(rgt->vtype)) {
		GDKerror("VARcalccstand: incompatible input types.\n");
		return GDK_FAIL;
	}

	if (and_typeswitchloop(VALptr(lft), 0,
			       VALptr(rgt), 0,
			       VALget(ret), lft->vtype,
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
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
lsh_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *restrict dst,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *func)
{
	oid x1, x2;
	BUN i, j, k;
	BUN nils = 0;

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
BATcalclsh(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = lsh_typeswitchloop(Tloc(b1, 0), b1->ttype, 1,
				  Tloc(b2, 0), b2->ttype, 1,
				  Tloc(bn, 0),
				  &ci1, &ci2, rv, b1->hseqbase, b2->hseqbase,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalclshcst(BAT *b, const ValRecord *v, BAT *s, BAT *r, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = lsh_typeswitchloop(Tloc(b, 0), b->ttype, 1,
				  VALptr(v), v->vtype, 0,
				  Tloc(bn, 0),
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  rv,
				  b->hseqbase, 0,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstlsh(const ValRecord *v, BAT *b, BAT *s, BAT *r, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, v->vtype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = lsh_typeswitchloop(VALptr(v), v->vtype, 0,
				  Tloc(b, 0), b->ttype, 1,
				  Tloc(bn, 0),
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  rv,
				  0, b->hseqbase,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

gdk_return
VARcalclsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	ret->vtype = lft->vtype;
	if (lsh_typeswitchloop(VALptr(lft), lft->vtype, 0,
			       VALptr(rgt), rgt->vtype, 0,
			       VALget(ret),
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* right shift (any integral type) */

#define RSH(a, b)	((a) >> (b))

static BUN
rsh_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *restrict dst,
		   struct canditer *restrict ci1,
		   struct canditer *restrict ci2,
		   const bit *restrict rv,
		   oid candoff1, oid candoff2,
		   bool abort_on_error, const char *restrict func)
{
	oid x1, x2;
	BUN i, j, k;
	BUN nils = 0;

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
BATcalcrsh(BAT *b1, BAT *b2, BAT *s1, BAT *s2, BAT *r, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci1, ci2;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	ncand = canditer_init(&ci1, b1, s1);
	if (canditer_init(&ci2, b2, s2) != ncand ||
	    ci1.hseq != ci2.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci1.hseq, b1->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = rsh_typeswitchloop(Tloc(b1, 0), b1->ttype, 1,
				  Tloc(b2, 0), b2->ttype, 1,
				  Tloc(bn, 0),
				  &ci1, &ci2, rv, b1->hseqbase, b2->hseqbase,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcrshcst(BAT *b, const ValRecord *v, BAT *s, BAT *r, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = rsh_typeswitchloop(Tloc(b, 0), b->ttype, 1,
				  VALptr(v), v->vtype, 0,
				  Tloc(bn, 0),
				  &ci,
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  rv,
				  b->hseqbase, 0,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalccstrsh(const ValRecord *v, BAT *b, BAT *s, BAT *r, bool abort_on_error)
{
	BAT *bn;
	BUN nils;
	BUN ncand;
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	bn = COLnew(ci.hseq, v->vtype, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ncand == 0)
		return bn;

	nils = rsh_typeswitchloop(VALptr(v), v->vtype, 0,
				  Tloc(b, 0), b->ttype, 1,
				  Tloc(bn, 0),
				  &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				  &ci,
				  rv,
				  0, b->hseqbase,
				  abort_on_error, __func__);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tsorted = ncand <= 1 || nils == ncand;
	bn->trevsorted = ncand <= 1 || nils == ncand;
	bn->tkey = ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

gdk_return
VARcalcrsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt,
	   bool abort_on_error)
{
	ret->vtype = lft->vtype;
	if (rsh_typeswitchloop(VALptr(lft), lft->vtype, 0,
			       VALptr(rgt), rgt->vtype, 0,
			       VALget(ret),
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       &(struct canditer){.tpe=cand_dense, .ncand=1},
			       NULL,
			       0, 0, abort_on_error, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* ---------------------------------------------------------------------- */
/* less than (any "linear" type) */

/* these three are for all simple comparisons (6 in all) */
#define TYPE_TPE		TYPE_bit
#define TPE			bit
#define TPE_nil			bit_nil
#define is_TPE_nil		is_bit_nil

#define OP			LT
#define op_typeswitchloop	lt_typeswitchloop
#define BATcalcop_intern	BATcalclt_intern
#define BATcalcop		BATcalclt
#define BATcalcopcst		BATcalcltcst
#define BATcalccstop		BATcalccstlt
#define VARcalcop		VARcalclt

#include "gdk_calc_compare.h"

#undef OP
#undef op_typeswitchloop
#undef BATcalcop_intern
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* greater than (any "linear" type) */

#define OP			GT
#define op_typeswitchloop	gt_typeswitchloop
#define BATcalcop_intern	BATcalcgt_intern
#define BATcalcop		BATcalcgt
#define BATcalcopcst		BATcalcgtcst
#define BATcalccstop		BATcalccstgt
#define VARcalcop		VARcalcgt

#include "gdk_calc_compare.h"

#undef OP
#undef op_typeswitchloop
#undef BATcalcop_intern
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* less than or equal (any "linear" type) */

#define LE(a, b)	((bit) ((a) <= (b)))

#define OP			LE
#define op_typeswitchloop	le_typeswitchloop
#define BATcalcop_intern	BATcalcle_intern
#define BATcalcop		BATcalcle
#define BATcalcopcst		BATcalclecst
#define BATcalccstop		BATcalccstle
#define VARcalcop		VARcalcle

#include "gdk_calc_compare.h"

#undef OP
#undef op_typeswitchloop
#undef BATcalcop_intern
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* greater than or equal (any "linear" type) */

#define GE(a, b)	((bit) ((a) >= (b)))

#define OP			GE
#define op_typeswitchloop	ge_typeswitchloop
#define BATcalcop_intern	BATcalcge_intern
#define BATcalcop		BATcalcge
#define BATcalcopcst		BATcalcgecst
#define BATcalccstop		BATcalccstge
#define VARcalcop		VARcalcge

#include "gdk_calc_compare.h"

#undef OP
#undef op_typeswitchloop
#undef BATcalcop_intern
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* equal (any type) */

#define EQ(a, b)	((bit) ((a) == (b)))

#define OP			EQ
#define op_typeswitchloop	eq_typeswitchloop
#define BATcalcop_intern	BATcalceq_intern
#define BATcalcop		BATcalceq
#define BATcalcopcst		BATcalceqcst
#define BATcalccstop		BATcalccsteq
#define VARcalcop		VARcalceq

#define NIL_MATCHES_FLAG 1

#include "gdk_calc_compare.h"

#undef OP
#undef op_typeswitchloop
#undef BATcalcop_intern
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

/* ---------------------------------------------------------------------- */
/* not equal (any type) */

#define NE(a, b)	((bit) ((a) != (b)))

#define OP			NE
#define op_typeswitchloop	ne_typeswitchloop
#define BATcalcop_intern	BATcalcne_intern
#define BATcalcop		BATcalcne
#define BATcalcopcst		BATcalcnecst
#define BATcalccstop		BATcalccstne
#define VARcalcop		VARcalcne

#include "gdk_calc_compare.h"

#undef NIL_MATCHES_FLAG

#undef OP
#undef op_typeswitchloop
#undef BATcalcop_intern
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

#undef TYPE_TPE
#undef TPE
#undef TPE_nil
#undef is_TPE_nil

/* ---------------------------------------------------------------------- */
/* generic comparison (any "linear" type) */

/* #define CMP(a, b)	((bte) ((a) < (b) ? -1 : (a) > (b))) */
#define CMP(a, b)	((bte) (((a) > (b)) - ((a) < (b))))

#define TYPE_TPE		TYPE_bte
#define TPE			bte
#define TPE_nil			bte_nil
#define is_TPE_nil		is_bte_nil

#define OP			CMP
#define op_typeswitchloop	cmp_typeswitchloop
#define BATcalcop_intern	BATcalccmp_intern
#define BATcalcop		BATcalccmp
#define BATcalcopcst		BATcalccmpcst
#define BATcalccstop		BATcalccstcmp
#define VARcalcop		VARcalccmp

#include "gdk_calc_compare.h"

#undef OP
#undef op_typeswitchloop
#undef BATcalcop_intern
#undef BATcalcop
#undef BATcalcopcst
#undef BATcalccstop
#undef VARcalcop

#undef TYPE_TPE
#undef TPE
#undef TPE_nil
#undef is_TPE_nil

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

#define BETWEEN_LOOP_TYPE(TYPE)						\
	do {								\
		for (l = 0; l < ci->ncand; l++) {			\
			x1 = canditer_next(ci) - seqbase1;		\
			x2 = canditer_next(cilo) - seqbase2;		\
			x3 = canditer_next(cihi) - seqbase3;		\
			if (rv != NULL && !rv[l]) {			\
				dst[l] = bit_nil;			\
				nils++;					\
			} else {					\
				i = x1 * incr1;				\
				j = x2 * incr2;				\
				k = x3 * incr3;				\
				dst[l] = BETWEEN(((const TYPE *) src)[i], \
						 ((const TYPE *) lo)[j], \
						 ((const TYPE *) hi)[k], \
						 TYPE);			\
				nils += is_bit_nil(dst[l]);		\
			}						\
		}							\
	} while (0)

static BAT *
BATcalcbetween_intern(const void *src, int incr1, const char *hp1, int wd1,
		      const void *lo, int incr2, const char *hp2, int wd2,
		      const void *hi, int incr3, const char *hp3, int wd3,
		      int tp,
		      struct canditer *restrict ci,
		      struct canditer *restrict cilo,
		      struct canditer *restrict cihi,
		      const bit *restrict rv,
		      oid seqbase1, oid seqbase2, oid seqbase3,
		      bool symmetric, bool anti,
		      bool linc, bool hinc, bool nils_false, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	BUN i, j, k, l;
	bit *restrict dst;
	const void *nil;
	int (*atomcmp)(const void *, const void *);
	oid x1, x2, x3;

	bn = COLnew(ci->hseq, TYPE_bit, ci->ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (ci->ncand == 0)
		return bn;

	dst = (bit *) Tloc(bn, 0);

	tp = ATOMbasetype(tp);

	switch (tp) {
	case TYPE_bte:
		BETWEEN_LOOP_TYPE(bte);
		break;
	case TYPE_sht:
		BETWEEN_LOOP_TYPE(sht);
		break;
	case TYPE_int:
		BETWEEN_LOOP_TYPE(int);
		break;
	case TYPE_lng:
		BETWEEN_LOOP_TYPE(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		BETWEEN_LOOP_TYPE(hge);
		break;
#endif
	case TYPE_flt:
		BETWEEN_LOOP_TYPE(flt);
		break;
	case TYPE_dbl:
		BETWEEN_LOOP_TYPE(dbl);
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
		for (l = 0; l < ci->ncand; l++) {
			x1 = canditer_next(ci) - seqbase1;
			x2 = canditer_next(cilo) - seqbase2;
			x3 = canditer_next(cihi) - seqbase3;
			if (rv != NULL && !rv[l]) {
				dst[l] = bit_nil;
				nils++;
			} else {
				i = x1 * incr1;
				j = x2 * incr2;
				k = x3 * incr3;
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
		}
		break;
	}

	BATsetcount(bn, ci->ncand);

	bn->tsorted = ci->ncand <= 1 || nils == ci->ncand;
	bn->trevsorted = ci->ncand <= 1 || nils == ci->ncand;
	bn->tkey = ci->ncand <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcbetween(BAT *b, BAT *lo, BAT *hi, BAT *s, BAT *slo, BAT *shi, BAT *r,
	       bool symmetric, bool linc, bool hinc, bool nils_false, bool anti)
{
	BAT *bn;
	BUN ncand;
	struct canditer ci, cilo, cihi;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	BATcheck(lo, __func__, NULL);
	BATcheck(hi, __func__, NULL);

	ncand = canditer_init(&ci, b, s);
	if (canditer_init(&cilo, lo, slo) != ncand ||
	    ci.hseq != cilo.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (canditer_init(&cihi, hi, shi) != ncand ||
	    ci.hseq != cihi.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	if (BATtvoid(b) &&
	    BATtvoid(lo) &&
	    BATtvoid(hi)) {
		bit res;

		res = BETWEEN(b->tseqbase, lo->tseqbase, hi->tseqbase, oid);
		return BATconstant(ci.hseq, TYPE_bit, &res, ncand,
				   TRANSIENT);
	}

	bn = BATcalcbetween_intern(Tloc(b, 0), 1,
				   b->tvheap ? b->tvheap->base : NULL,
				   b->twidth,
				   Tloc(lo, 0), 1,
				   lo->tvheap ? lo->tvheap->base : NULL,
				   lo->twidth,
				   Tloc(hi, 0), 1,
				   hi->tvheap ? hi->tvheap->base : NULL,
				   hi->twidth,
				   b->ttype,
				   &ci, &cilo, &cihi, rv,
				   b->hseqbase, lo->hseqbase, hi->hseqbase,
				   symmetric, anti, linc, hinc,
				   nils_false, __func__);

	return bn;
}

BAT *
BATcalcbetweencstcst(BAT *b, const ValRecord *lo, const ValRecord *hi,
		     BAT *s, BAT *r,
		     bool symmetric, bool linc, bool hinc, bool nils_false,
		     bool anti)
{
	struct canditer ci;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(lo->vtype) ||
	    ATOMbasetype(b->ttype) != ATOMbasetype(hi->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ci.ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	return BATcalcbetween_intern(Tloc(b, 0), 1,
				     b->tvheap ? b->tvheap->base : NULL,
				     b->twidth,
				     VALptr(lo), 0, NULL, 0,
				     VALptr(hi), 0, NULL, 0,
				     b->ttype,
				     &ci,
				     &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				     &(struct canditer){.tpe=cand_dense, .ncand=ci.ncand},
				     rv,
				     b->hseqbase, 0, 0, symmetric, anti,
				     linc, hinc, nils_false,
				     __func__);
}

BAT *
BATcalcbetweenbatcst(BAT *b, BAT *lo, const ValRecord *hi, BAT *s, BAT *slo, BAT *r,
		     bool symmetric, bool linc, bool hinc, bool nils_false,
		     bool anti)
{
	struct canditer ci, cilo;
	BUN ncand;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	BATcheck(lo, __func__, NULL);

	if (checkbats(b, lo, __func__) != GDK_SUCCEED)
		return NULL;

	if (ATOMbasetype(b->ttype) != ATOMbasetype(hi->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci, b, s);
	if (canditer_init(&cilo, lo, slo) != ncand ||
	    ci.hseq != cilo.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	return BATcalcbetween_intern(Tloc(b, 0), 1,
				     b->tvheap ? b->tvheap->base : NULL,
				     b->twidth,
				     Tloc(lo, 0), 1,
				     lo->tvheap ? lo->tvheap->base : NULL,
				     lo->twidth,
				     VALptr(hi), 0, NULL, 0,
				     b->ttype,
				     &ci,
				     &cilo,
				     &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				     rv,
				     b->hseqbase, lo->hseqbase, 0,
				     symmetric, anti,
				     linc, hinc, nils_false,
				     __func__);
}

BAT *
BATcalcbetweencstbat(BAT *b, const ValRecord *lo, BAT *hi, BAT *s, BAT *shi, BAT *r,
		     bool symmetric, bool linc, bool hinc, bool nils_false,
		     bool anti)
{
	struct canditer ci, cihi;
	BUN ncand;
	const bit *rv = r ? Tloc(r, 0) : NULL;

	BATcheck(b, __func__, NULL);
	BATcheck(hi, __func__, NULL);

	if (ATOMbasetype(b->ttype) != ATOMbasetype(lo->vtype)) {
		GDKerror("%s: incompatible input types.\n", __func__);
		return NULL;
	}

	ncand = canditer_init(&ci, b, s);
	if (canditer_init(&cihi, hi, shi) != ncand ||
	    ci.hseq != cihi.hseq) {
		GDKerror("%s: inputs not the same size.\n", __func__);
		return NULL;
	}
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}

	return BATcalcbetween_intern(Tloc(b, 0), 1,
				     b->tvheap ? b->tvheap->base : NULL,
				     b->twidth,
				     VALptr(lo), 0, NULL, 0,
				     Tloc(hi, 0), 1,
				     hi->tvheap ? hi->tvheap->base : NULL,
				     hi->twidth,
				     b->ttype,
				     &ci,
				     &(struct canditer){.tpe=cand_dense, .ncand=ncand},
				     &cihi,
				     rv,
				     b->hseqbase, 0, hi->hseqbase,
				     symmetric, anti,
				     linc, hinc, nils_false,
				     __func__);
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
		GDKerror("VARcalcbetween: incompatible input types.\n");
		return GDK_FAIL;
	}
	if (!ATOMlinear(t)) {
		GDKerror("VARcalcbetween: non-linear input type.\n");
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
		for (i = 0; i < cnt; i++) {				\
			if (is_bit_nil(src[i])) {			\
				((TYPE *) dst)[i] = * (TYPE *) nil;	\
				nils++;					\
			} else if (src[i]) {				\
				((TYPE *) dst)[i] = ((TYPE *) col1)[k]; \
			} else {					\
				((TYPE *) dst)[i] = ((TYPE *) col2)[l]; \
			}						\
			k += incr1;					\
			l += incr2;					\
		}							\
	} while (0)
#define IFTHENELSELOOP_oid()						\
	do {								\
		for (i = 0; i < cnt; i++) {				\
			if (is_bit_nil(src[i])) {			\
				((oid *) dst)[i] = oid_nil;		\
				nils++;					\
			} else if (src[i]) {				\
				((oid *) dst)[i] = col1 ? ((oid *) col1)[k] : seq1; \
			} else {					\
				((oid *) dst)[i] = col2 ? ((oid *) col2)[k] : seq2; \
			}						\
			k += incr1;					\
			l += incr2;					\
			seq1 += incr1;					\
			seq2 += incr2;					\
		}							\
	} while (0)

static BAT *
BATcalcifthenelse_intern(BAT *b,
			 const void *col1, int incr1, const char *heap1,
			 int width1, bool nonil1, oid seq1,
			 const void *col2, int incr2, const char *heap2,
			 int width2, bool nonil2, oid seq2,
			 int tpe)
{
	BAT *bn;
	void *restrict dst;
	BUN i, k, l;
	BUN nils = 0;
	const void *nil;
	const void *p;
	const bit *src;
	BUN cnt = b->batCount;

	/* col1 and col2 can only be NULL for void columns */
	assert(col1 != NULL || ATOMtype(tpe) == TYPE_oid);
	assert(col2 != NULL || ATOMtype(tpe) == TYPE_oid);
	assert(col1 != NULL || heap1 == NULL);
	assert(col2 != NULL || heap2 == NULL);
	assert(col1 != NULL || incr1 == 1);
	assert(col2 != NULL || incr2 == 1);

	bn = COLnew(b->hseqbase, ATOMtype(tpe), cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;
	if (cnt == 0)
		return bn;

	src = (const bit *) Tloc(b, 0);

	nil = ATOMnilptr(tpe);
	dst = (void *) Tloc(bn, 0);
	k = l = 0;
	if (bn->tvarsized) {
		assert((heap1 != NULL && width1 > 0) || (width1 == 0 && incr1 == 0));
		assert((heap2 != NULL && width2 > 0) || (width2 == 0 && incr2 == 0));
		for (i = 0; i < cnt; i++) {
			if (is_bit_nil(src[i])) {
				p = nil;
				nils++;
			} else if (src[i]) {
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
			if (tfastins_nocheckVAR(bn, i, p, Tsize(bn)) != GDK_SUCCEED) {
				BBPreclaim(bn);
				return NULL;
			}
			k += incr1;
			l += incr2;
		}
	} else {
		assert(heap1 == NULL);
		assert(heap2 == NULL);
		if (ATOMtype(tpe) == TYPE_oid) {
			IFTHENELSELOOP_oid();
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
#ifdef HAVE_HGE
			case 16:
				IFTHENELSELOOP(hge);
				break;
#endif
			default:
				for (i = 0; i < cnt; i++) {
					if (is_bit_nil(src[i])) {
						p = nil;
						nils++;
					} else if (src[i]) {
						p = ((const char *) col1) + k * width1;
					} else {
						p = ((const char *) col2) + l * width2;
					}
					memcpy(dst, p, bn->twidth);
					dst = (void *) ((char *) dst + bn->twidth);
					k += incr1;
					l += incr2;
				}
			}
		}
	}

	BATsetcount(bn, cnt);
	bn->theap.dirty = true;

	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0 && nonil1 && nonil2;

	return bn;
}

BAT *
BATcalcifthenelse(BAT *b, BAT *b1, BAT *b2)
{
	BATcheck(b, __func__, NULL);
	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (checkbats(b, b1, __func__) != GDK_SUCCEED)
		return NULL;
	if (checkbats(b, b2, __func__) != GDK_SUCCEED)
		return NULL;
	if (b->ttype != TYPE_bit || ATOMtype(b1->ttype) != ATOMtype(b2->ttype)) {
		GDKerror("%s: \"then\" and \"else\" BATs have different types.\n", __func__);
		return NULL;
	}
	return BATcalcifthenelse_intern(b,
					Tloc(b1, 0), 1, b1->tvheap ? b1->tvheap->base : NULL, b1->twidth, b1->tnonil, b1->tseqbase,
					Tloc(b2, 0), 1, b2->tvheap ? b2->tvheap->base : NULL, b2->twidth, b2->tnonil, b2->tseqbase,
					b1->ttype);
}

BAT *
BATcalcifthenelsecst(BAT *b, BAT *b1, const ValRecord *c2)
{
	BATcheck(b, __func__, NULL);
	BATcheck(b1, __func__, NULL);
	BATcheck(c2, __func__, NULL);

	if (checkbats(b, b1, __func__) != GDK_SUCCEED)
		return NULL;
	if (b->ttype != TYPE_bit || ATOMtype(b1->ttype) != ATOMtype(c2->vtype)) {
		GDKerror("%s: \"then\" and \"else\" BATs have different types.\n", __func__);
		return NULL;
	}
	return BATcalcifthenelse_intern(b,
					Tloc(b1, 0), 1, b1->tvheap ? b1->tvheap->base : NULL, b1->twidth, b1->tnonil, b1->tseqbase,
					VALptr(c2), 0, NULL, 0, !VALisnil(c2), 0,
					b1->ttype);
}

BAT *
BATcalcifthencstelse(BAT *b, const ValRecord *c1, BAT *b2)
{
	BATcheck(b, __func__, NULL);
	BATcheck(c1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (checkbats(b, b2, __func__) != GDK_SUCCEED)
		return NULL;
	if (b->ttype != TYPE_bit || ATOMtype(b2->ttype) != ATOMtype(c1->vtype)) {
		GDKerror("%s: \"then\" and \"else\" BATs have different types.\n", __func__);
		return NULL;
	}
	return BATcalcifthenelse_intern(b,
					VALptr(c1), 0, NULL, 0, !VALisnil(c1), 0,
					Tloc(b2, 0), 1, b2->tvheap ? b2->tvheap->base : NULL, b2->twidth, b2->tnonil, b2->tseqbase,
					c1->vtype);
}

BAT *
BATcalcifthencstelsecst(BAT *b, const ValRecord *c1, const ValRecord *c2)
{
	BATcheck(b, __func__, NULL);
	BATcheck(c1, __func__, NULL);
	BATcheck(c2, __func__, NULL);

	if (b->ttype != TYPE_bit || ATOMtype(c1->vtype) != ATOMtype(c2->vtype)) {
		GDKerror("%s: \"then\" and \"else\" BATs have different types.\n", __func__);
		return NULL;
	}
	return BATcalcifthenelse_intern(b,
					VALptr(c1), 0, NULL, 0, !VALisnil(c1), 0,
					VALptr(c2), 0, NULL, 0, !VALisnil(c2), 0,
					c1->vtype);
}

/* ---------------------------------------------------------------------- */
/* type conversion (cast) */

/* a note on the return values from the internal conversion functions:
 *
 * the functions return the number of NIL values produced (or at
 * least, 0 if no NIL, and != 0 if there were any);
 * the return value is BUN_NONE if there was overflow and a message
 * was generated;
 * the return value is BUN_NONE + 1 if the types were not compatible;
 * the return value is BUN_NONE + 2 if inserting a value into a BAT
 * failed (only happens for conversion to str).
 */

#define convertimpl_copy(TYPE)						\
static BUN								\
convert_##TYPE##_##TYPE(const TYPE *src, TYPE *restrict dst,		\
			struct canditer *restrict ci,			\
			const bit *restrict rv,				\
			oid candoff, bool *reduce)			\
{									\
	BUN i, nils = 0;						\
	oid x;								\
									\
	(void) rv;							\
	*reduce = false;						\
	if (ci->tpe == cand_dense) {					\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next_dense(ci) - candoff;		\
			nils += is_##TYPE##_nil(src[x]);		\
			dst[i] = src[x];				\
		}							\
	} else {							\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next(ci) - candoff;		\
			nils += is_##TYPE##_nil(src[x]);		\
			dst[i] = src[x];				\
		}							\
	}								\
	return nils;							\
}

#define convertimpl_enlarge(TYPE1, TYPE2)				\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  struct canditer *restrict ci,			\
			  const bit *restrict rv,			\
			  oid candoff, bool *reduce)			\
{									\
	BUN i, nils = 0;						\
	oid x;								\
									\
	(void) rv;							\
	*reduce = false;						\
	if (ci->tpe == cand_dense) {					\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next_dense(ci) - candoff;		\
			if (is_##TYPE1##_nil(src[x])) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else						\
				dst[i] = (TYPE2) src[x];		\
		}							\
	} else {							\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next(ci) - candoff;		\
			if (is_##TYPE1##_nil(src[x])) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else						\
				dst[i] = (TYPE2) src[x];		\
		}							\
	}								\
	return nils;							\
}

#define convertimpl_enlarge_float(TYPE1, TYPE2, MANT_DIG)		\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  struct canditer *restrict ci,			\
			  const bit *restrict rv,			\
			  oid candoff, bool *reduce)			\
{									\
	BUN i, nils = 0;						\
	oid x;								\
									\
	*reduce = 8 * sizeof(TYPE1) > MANT_DIG;				\
	if (ci->tpe == cand_dense) {					\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next_dense(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else						\
				dst[i] = (TYPE2) src[x];		\
		}							\
	} else {							\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else						\
				dst[i] = (TYPE2) src[x];		\
		}							\
	}								\
	return nils;							\
}

#define CONV_OVERFLOW(TYPE1, TYPE2, value)				\
	do {								\
		GDKerror("22003!overflow in conversion of "		\
			 FMT##TYPE1 " to %s.\n", CST##TYPE1 (value), TYPE2); \
		return BUN_NONE;					\
	} while (0)

#define convertimpl_oid_enlarge(TYPE1)					\
static BUN								\
convert_##TYPE1##_oid(const TYPE1 *src, oid *restrict dst,		\
		      struct canditer *restrict ci,			\
		      const bit *restrict rv,				\
		      oid candoff, bool abort_on_error, bool *reduce)	\
{									\
	BUN i, nils = 0;						\
	oid x;								\
									\
	*reduce = false;						\
	if (ci->tpe == cand_dense) {					\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next_dense(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (src[x] < 0) {			\
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, "oid", src[i]); \
				*reduce = true;				\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (is_oid_nil((dst[i] = (oid) src[x])) && \
				   abort_on_error)			\
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
		}							\
	} else {							\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (src[x] < 0) {			\
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, "oid", src[x]); \
				*reduce = true;				\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (is_oid_nil((dst[i] = (oid) src[x])) && \
				   abort_on_error)			\
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
		}							\
	}								\
	return nils;							\
}

#define convertimpl_oid_reduce(TYPE1)					\
static BUN								\
convert_##TYPE1##_oid(const TYPE1 *src, oid *restrict dst,		\
		      struct canditer *restrict ci,			\
		      const bit *restrict rv,				\
		      oid candoff, bool abort_on_error, bool *reduce)	\
{									\
	BUN i, nils = 0;						\
	oid x;								\
									\
	*reduce = false;						\
	if (ci->tpe == cand_dense) {					\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next_dense(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (src[x] < 0 ||			\
				   src[x] > (TYPE1) GDK_oid_max) {	\
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, "oid", src[x]); \
				*reduce = true;				\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (is_oid_nil((dst[i] = (oid) src[x])) && \
				   abort_on_error)			\
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
		}							\
	} else {							\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (src[x] < 0 ||			\
				   src[x] > (TYPE1) GDK_oid_max) {	\
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, "oid", src[x]); \
				*reduce = true;				\
				dst[i] = oid_nil;			\
				nils++;					\
			} else if (is_oid_nil((dst[i] = (oid) src[x])) && \
				   abort_on_error)			\
				CONV_OVERFLOW(TYPE1, "oid", src[x]);	\
		}							\
	}								\
	return nils;							\
}

#define convertimpl_reduce(TYPE1, TYPE2)				\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  struct canditer *restrict ci,			\
			  const bit *restrict rv,			\
			  oid candoff, bool abort_on_error, bool *reduce) \
{									\
	BUN i, nils = 0;						\
	oid x;								\
									\
	*reduce = false;						\
	if (ci->tpe == cand_dense) {					\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next_dense(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else if (src[x] < (TYPE1) GDK_##TYPE2##_min || \
				   src[x] > (TYPE1) GDK_##TYPE2##_max) { \
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, #TYPE2, src[x]); \
				*reduce = true;				\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else						\
				dst[i] = (TYPE2) src[x];		\
		}							\
	} else {							\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else if (src[x] < (TYPE1) GDK_##TYPE2##_min || \
				   src[x] > (TYPE1) GDK_##TYPE2##_max) { \
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, #TYPE2, src[x]); \
				*reduce = true;				\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else						\
				dst[i] = (TYPE2) src[x];		\
		}							\
	}								\
	return nils;							\
}

/* Special version of the above for converting from floating point.
 * The final assignment rounds the value which can still come out to
 * the NIL representation, so we need to check for that. */
#ifdef TRUNCATE_NUMBERS
#define roundflt(x)	(x)
#define rounddbl(x)	(x)
#else
#define roundflt(x)	roundf(x)
#define rounddbl(x)	round(x)
#endif

#define convertimpl_reduce_float(TYPE1, TYPE2)				\
static BUN								\
convert_##TYPE1##_##TYPE2(const TYPE1 *src, TYPE2 *restrict dst,	\
			  struct canditer *restrict ci,			\
			  const bit *restrict rv,			\
			  oid candoff, bool abort_on_error, bool *reduce) \
{									\
	BUN i, nils = 0;						\
	oid x;								\
									\
	*reduce = true;							\
	if (ci->tpe == cand_dense) {					\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next_dense(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else if (src[x] < (TYPE1) GDK_##TYPE2##_min || \
				   src[x] > (TYPE1) GDK_##TYPE2##_max) { \
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, #TYPE2, src[x]); \
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else {					\
				dst[i] = (TYPE2) round##TYPE1(src[x]);	\
				if (is_##TYPE2##_nil(dst[i]) &&		\
				    abort_on_error)			\
					CONV_OVERFLOW(TYPE1, #TYPE2, src[x]); \
			}						\
		}							\
	} else {							\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE1##_nil(src[x])) {			\
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else if (src[x] < (TYPE1) GDK_##TYPE2##_min || \
				   src[x] > (TYPE1) GDK_##TYPE2##_max) { \
				if (abort_on_error)			\
					CONV_OVERFLOW(TYPE1, #TYPE2, src[x]); \
				dst[i] = TYPE2##_nil;			\
				nils++;					\
			} else {					\
				dst[i] = (TYPE2) round##TYPE1(src[x]);	\
				if (is_##TYPE2##_nil(dst[i]) &&		\
				    abort_on_error)			\
					CONV_OVERFLOW(TYPE1, #TYPE2, src[x]); \
			}						\
		}							\
	}								\
	return nils;							\
}

#define convert2bit_impl(TYPE)						\
static BUN								\
convert_##TYPE##_bit(const TYPE *src, bit *restrict dst,		\
		     struct canditer *restrict ci,			\
		     const bit *restrict rv,				\
		     oid candoff, bool *reduce)				\
{									\
	BUN i, nils = 0;						\
	oid x;								\
									\
	*reduce = true;							\
	if (ci->tpe == cand_dense) {					\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next_dense(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE##_nil(src[x])) {			\
				dst[i] = bit_nil;			\
				nils++;					\
			} else						\
				dst[i] = (bit) (src[x] != 0);		\
		}							\
	} else {							\
		for (i = 0; i < ci->ncand; i++) {			\
			x = canditer_next(ci) - candoff;		\
			if ((rv != NULL && !rv[i]) ||			\
			    is_##TYPE##_nil(src[x])) {			\
				dst[i] = bit_nil;			\
				nils++;					\
			} else						\
				dst[i] = (bit) (src[x] != 0);		\
		}							\
	}								\
	return nils;							\
}

convertimpl_copy(bte)
convertimpl_enlarge(bte, sht)
convertimpl_enlarge(bte, int)
convertimpl_oid_enlarge(bte)
convertimpl_enlarge(bte, lng)
#ifdef HAVE_HGE
convertimpl_enlarge(bte, hge)
#endif
convertimpl_enlarge_float(bte, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(bte, dbl, DBL_MANT_DIG)

convertimpl_reduce(sht, bte)
convertimpl_copy(sht)
convertimpl_enlarge(sht, int)
convertimpl_oid_enlarge(sht)
convertimpl_enlarge(sht, lng)
#ifdef HAVE_HGE
convertimpl_enlarge(sht, hge)
#endif
convertimpl_enlarge_float(sht, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(sht, dbl, DBL_MANT_DIG)

convertimpl_reduce(int, bte)
convertimpl_reduce(int, sht)
convertimpl_copy(int)
convertimpl_oid_enlarge(int)
convertimpl_enlarge(int, lng)
#ifdef HAVE_HGE
convertimpl_enlarge(int, hge)
#endif
convertimpl_enlarge_float(int, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(int, dbl, DBL_MANT_DIG)

convertimpl_reduce(lng, bte)
convertimpl_reduce(lng, sht)
convertimpl_reduce(lng, int)
#if SIZEOF_OID == SIZEOF_LNG
convertimpl_oid_enlarge(lng)
#else
convertimpl_oid_reduce(lng)
#endif
convertimpl_copy(lng)
#ifdef HAVE_HGE
convertimpl_enlarge(lng, hge)
#endif
convertimpl_enlarge_float(lng, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(lng, dbl, DBL_MANT_DIG)

#ifdef HAVE_HGE
convertimpl_reduce(hge, bte)
convertimpl_reduce(hge, sht)
convertimpl_reduce(hge, int)
convertimpl_oid_reduce(hge)
convertimpl_reduce(hge, lng)
convertimpl_copy(hge)
convertimpl_enlarge_float(hge, flt, FLT_MANT_DIG)
convertimpl_enlarge_float(hge, dbl, DBL_MANT_DIG)
#endif

convertimpl_reduce_float(flt, bte)
convertimpl_reduce_float(flt, sht)
convertimpl_reduce_float(flt, int)
convertimpl_oid_reduce(flt)
convertimpl_reduce_float(flt, lng)
#ifdef HAVE_HGE
convertimpl_reduce_float(flt, hge)
#endif
convertimpl_copy(flt)
convertimpl_enlarge_float(flt, dbl, DBL_MANT_DIG)

convertimpl_reduce_float(dbl, bte)
convertimpl_reduce_float(dbl, sht)
convertimpl_reduce_float(dbl, int)
convertimpl_oid_reduce(dbl)
convertimpl_reduce_float(dbl, lng)
#ifdef HAVE_HGE
convertimpl_reduce_float(dbl, hge)
#endif
#undef rounddbl
/* no rounding here */
#define rounddbl(x)	(x)
convertimpl_reduce_float(dbl, flt)
convertimpl_copy(dbl)

convert2bit_impl(bte)
convert2bit_impl(sht)
convert2bit_impl(int)
convert2bit_impl(lng)
#ifdef HAVE_HGE
convert2bit_impl(hge)
#endif
convert2bit_impl(flt)
convert2bit_impl(dbl)

static BUN
convert_any_str(BAT *b, BAT *bn, struct canditer *restrict ci,
		const bit *restrict rv)
{
	int tp = b->ttype;
	oid candoff = b->hseqbase;
	str dst = 0;
	size_t len = 0;
	BUN nils = 0;
	BUN i;
	const void *nil = ATOMnilptr(tp);
	const void *restrict src;
	ssize_t (*atomtostr)(str *, size_t *, const void *, bool) = BATatoms[tp].atomToStr;
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tp);
	oid x;

	if (atomtostr == BATatoms[TYPE_str].atomToStr) {
		/* compatible with str, we just copy the value */
		BATiter bi = bat_iterator(b);

		assert(b->ttype != TYPE_void);
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			if (rv != NULL && !rv[i]) {
				src = str_nil;
				nils++;
			} else {
				src = BUNtvar(bi, x);
				if (strNil(src))
					nils++;
			}
			if (tfastins_nocheckVAR(bn, i, src, bn->twidth) != GDK_SUCCEED)
				goto bunins_failed;
		}
	} else if (b->tvarsized) {
		BATiter bi = bat_iterator(b);

		assert(b->ttype != TYPE_void);
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			src = BUNtvar(bi, x);
			if ((rv != NULL && !rv[i]) ||
			    (*atomcmp)(src, nil) == 0) {
				nils++;
				if (tfastins_nocheckVAR(bn, i, str_nil, bn->twidth) != GDK_SUCCEED)
					goto bunins_failed;
			} else {
				if ((*atomtostr)(&dst, &len, src, false) < 0)
					goto bunins_failed;
				if (tfastins_nocheckVAR(bn, i, dst, bn->twidth) != GDK_SUCCEED)
					goto bunins_failed;
			}
		}
	} else {
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			src = Tloc(b, x);
			if ((rv != NULL && !rv[i]) ||
			    (*atomcmp)(src, nil) == 0) {
				nils++;
				if (tfastins_nocheckVAR(bn, i, str_nil, bn->twidth) != GDK_SUCCEED)
					goto bunins_failed;
			} else {
				if ((*atomtostr)(&dst, &len, src, false) < 0)
					goto bunins_failed;
				if (tfastins_nocheckVAR(bn, i, dst, bn->twidth) != GDK_SUCCEED)
					goto bunins_failed;
			}
		}
	}
	bn->theap.dirty = true;
	BATsetcount(bn, ci->ncand);
	GDKfree(dst);
	return nils;
  bunins_failed:
	GDKfree(dst);
	return BUN_NONE + 2;
}

static BUN
convert_str_any(BAT *b, int tp, void *restrict dst,
		struct canditer *restrict ci,
		const bit *restrict rv,
		oid candoff, bool abort_on_error)
{
	BUN nils = 0;
	const void *nil = ATOMnilptr(tp);
	size_t len = ATOMsize(tp);
	ssize_t l;
	ssize_t (*atomfromstr)(const char *, size_t *, ptr *, bool) = BATatoms[tp].atomFromStr;
	BATiter bi = bat_iterator(b);

	for (BUN i = 0; i < ci->ncand; i++) {
		oid x = canditer_next(ci) - candoff;
		const char *s = BUNtvar(bi, x);
		if ((rv != NULL && !rv[i]) || strNil(s)) {
			memcpy(dst, nil, len);
			nils++;
		} else {
			void *d = dst;
			if ((l = (*atomfromstr)(s, &len, &d, false)) < 0 ||
			    l < (ssize_t) strlen(s)) {
				if (abort_on_error) {
					GDKclrerr();
					GDKerror("22018!conversion of string "
						 "'%s' to type %s failed.\n",
						 s, ATOMname(tp));
					return BUN_NONE;
				}
				memcpy(dst, nil, len);
			}
			assert(len == ATOMsize(tp));
			if (ATOMcmp(tp, dst, nil) == 0)
				nils++;
		}
		dst = (void *) ((char *) dst + len);
	}
	return nils;
}

static BUN
convert_void_any(oid seq, BAT *bn,
		 struct canditer *restrict ci, const bit *restrict rv,
		 oid candoff, bool abort_on_error, bool *reduce)
{
	BUN nils = 0;
	BUN i;
	int tp = bn->ttype;
	void *restrict dst = Tloc(bn, 0);
	ssize_t (*atomtostr)(str *, size_t *, const void *, bool) = BATatoms[TYPE_oid].atomToStr;
	char *s = NULL;
	size_t len = 0;
	oid x;

	*reduce = false;
	assert(!is_oid_nil(seq));

	/* we only need to check rv if there is a chance of overflow */
	switch (ATOMbasetype(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			if (ci->ncand > 0) {
				x = canditer_next(ci) - candoff;
				((bit *) dst)[0] = x + seq != 0;
			}
			for (i = 1; i < ci->ncand; i++) {
				((bit *) dst)[i] = 1;
			}
		} else {
			for (i = 0; i < ci->ncand; i++) {
				x = canditer_next(ci) - candoff;
				if (rv != NULL && !rv[i]) {
					((bte *) dst)[i] = bte_nil;
					nils++;
				} else if (seq + x > GDK_bte_max) {
					if (abort_on_error)
						CONV_OVERFLOW(oid, "bte", seq + x);
					((bte *) dst)[i] = bte_nil;
					nils++;
				} else
					((bte *) dst)[i] = (bte) (seq + x);
			}
		}
		break;
	case TYPE_sht:
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			if (rv != NULL && !rv[i]) {
				((sht *) dst)[i] = bte_nil;
				nils++;
			} else if (seq + x > GDK_sht_max) {
				if (abort_on_error)
					CONV_OVERFLOW(oid, "sht", seq + x);
				((sht *) dst)[i] = sht_nil;
				nils++;
			} else
				((sht *) dst)[i] = (sht) (seq + x);
		}
		break;
	case TYPE_int:
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
#if SIZEOF_OID > SIZEOF_INT
			if (rv != NULL && !rv[i]) {
				((int *) dst)[i] = int_nil;
				nils++;
			} else if (seq + x > GDK_int_max) {
				if (abort_on_error)
					CONV_OVERFLOW(oid, "int", seq + x);
				((int *) dst)[i] = int_nil;
				nils++;
			} else
#endif
				((int *) dst)[i] = (int) (seq + x);
		}
		break;
	case TYPE_lng:
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			((lng *) dst)[i] = (lng) (seq + x);
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			((hge *) dst)[i] = (hge) (seq + x);
		}
		break;
#endif
	case TYPE_flt:
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			((flt *) dst)[i] = (flt) (seq + x);
		}
		break;
	case TYPE_dbl:
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			((dbl *) dst)[i] = (dbl) (seq + x);
		}
		break;
	case TYPE_str:
		for (i = 0; i < ci->ncand; i++) {
			x = canditer_next(ci) - candoff;
			if ((*atomtostr)(&s, &len, &(oid){seq + x}, false) < 0)
				goto bunins_failed;
			if (tfastins_nocheckVAR(bn, i, s, bn->twidth) != GDK_SUCCEED)
				goto bunins_failed;
		}
		GDKfree(s);
		s = NULL;
		break;
	default:
		return BUN_NONE + 1;
	}

	bn->theap.dirty = true;
	return nils;

  bunins_failed:
	GDKfree(s);
	return BUN_NONE + 2;
}

static BUN
convert_typeswitchloop(const void *src, int stp, void *restrict dst, int dtp,
		       struct canditer *restrict ci, const bit *restrict rv,
		       oid candoff, bool abort_on_error, bool *reduce)
{
	switch (ATOMbasetype(stp)) {
	case TYPE_bte:
		switch (ATOMbasetype(dtp)) {
		case TYPE_bte:
			if (dtp == TYPE_bit)
				return convert_bte_bit(src, dst, ci, rv,
						       candoff, reduce);
			return convert_bte_bte(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_sht:
			return convert_bte_sht(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_bte_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_bte_int(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_bte_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_bte_lng(src, dst, ci, rv, candoff,
					       reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_bte_hge(src, dst, ci, rv, candoff,
					       reduce);
#endif
		case TYPE_flt:
			return convert_bte_flt(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_dbl:
			return convert_bte_dbl(src, dst, ci, rv, candoff,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_sht:
		switch (ATOMbasetype(dtp)) {
		case TYPE_bte:
			if (dtp == TYPE_bit)
				return convert_sht_bit(src, dst, ci, rv,
						       candoff, reduce);
			return convert_sht_bte(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_sht:
			return convert_sht_sht(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_sht_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_sht_int(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_sht_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_sht_lng(src, dst, ci, rv, candoff,
					       reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_sht_hge(src, dst, ci, rv, candoff,
					       reduce);
#endif
		case TYPE_flt:
			return convert_sht_flt(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_dbl:
			return convert_sht_dbl(src, dst, ci, rv, candoff,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_int:
		switch (ATOMbasetype(dtp)) {
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_int_bit(src, dst, ci, rv,
						       candoff, reduce);
			}
			return convert_int_bte(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_sht:
			return convert_int_sht(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_int_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_int_int(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_int_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_int_lng(src, dst, ci, rv, candoff,
					       reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_int_hge(src, dst, ci, rv, candoff,
					       reduce);
#endif
		case TYPE_flt:
			return convert_int_flt(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_dbl:
			return convert_int_dbl(src, dst, ci, rv, candoff,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_lng:
		switch (ATOMbasetype(dtp)) {
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_lng_bit(src, dst, ci, rv,
						       candoff, reduce);
			}
			return convert_lng_bte(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_sht:
			return convert_lng_sht(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_lng_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_lng_int(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_lng_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_lng_lng(src, dst, ci, rv, candoff,
					       reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_lng_hge(src, dst, ci, rv, candoff,
					       reduce);
#endif
		case TYPE_flt:
			return convert_lng_flt(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_dbl:
			return convert_lng_dbl(src, dst, ci, rv, candoff,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (ATOMbasetype(dtp)) {
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_hge_bit(src, dst, ci, rv,
						       candoff, reduce);
			}
			return convert_hge_bte(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_sht:
			return convert_hge_sht(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_int:
			return convert_hge_int(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_lng:
			return convert_hge_lng(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_hge:
			return convert_hge_hge(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_oid:
			return convert_hge_oid(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_flt:
			return convert_hge_flt(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_dbl:
			return convert_hge_dbl(src, dst, ci, rv, candoff,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
#endif
	case TYPE_flt:
		switch (ATOMbasetype(dtp)) {
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_flt_bit(src, dst, ci, rv,
						       candoff, reduce);
			}
			return convert_flt_bte(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_sht:
			return convert_flt_sht(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_flt_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_flt_int(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_flt_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_flt_lng(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_flt_hge(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
#endif
		case TYPE_flt:
			return convert_flt_flt(src, dst, ci, rv, candoff,
					       reduce);
		case TYPE_dbl:
			return convert_flt_dbl(src, dst, ci, rv, candoff,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	case TYPE_dbl:
		switch (ATOMbasetype(dtp)) {
		case TYPE_bte:
			if (dtp == TYPE_bit) {
				return convert_dbl_bit(src, dst, ci, rv,
						       candoff, reduce);
			}
			return convert_dbl_bte(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_sht:
			return convert_dbl_sht(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (dtp == TYPE_oid)
				return convert_dbl_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_dbl_int(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (dtp == TYPE_oid)
				return convert_dbl_oid(src, dst, ci, rv,
						       candoff, abort_on_error,
						       reduce);
#endif
			return convert_dbl_lng(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
#ifdef HAVE_HGE
		case TYPE_hge:
			return convert_dbl_hge(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
#endif
		case TYPE_flt:
			return convert_dbl_flt(src, dst, ci, rv, candoff,
					       abort_on_error, reduce);
		case TYPE_dbl:
			return convert_dbl_dbl(src, dst, ci, rv, candoff,
					       reduce);
		default:
			return BUN_NONE + 1;
		}
	default:
		return BUN_NONE + 1;
	}
}

BAT *
BATconvert(BAT *b, BAT *s, BAT *r, int tp, bool abort_on_error)
{
	BAT *bn;
	BUN nils = 0;	/* in case no conversion defined */
	struct canditer ci;
	BUN cnt, ncand;
	const bit *rv = r ? Tloc(r, 0) : NULL;
	/* set reduce to true if there are (potentially) multiple
	 * (different) source values that map to the same destination
	 * value */
	bool reduce = false;

	BATcheck(b, __func__, NULL);
	if (tp == TYPE_void)
		tp = TYPE_oid;

	cnt = BATcount(b);
	ncand = canditer_init(&ci, b, s);
	if (r && (BATcount(r) != ncand || r->ttype != TYPE_bit)) {
		GDKerror("%s: r bat not the correct size or of wrong type\n",
			 __func__);
		return NULL;
	}
	if (ncand == 0 || (b->ttype == TYPE_void && is_oid_nil(b->tseqbase)))
		return BATconstant(ci.hseq, tp,
				   ATOMnilptr(tp), ncand, TRANSIENT);

	if (cnt == ncand && tp != TYPE_bit &&
	    ATOMbasetype(b->ttype) == ATOMbasetype(tp) &&
	    (tp != TYPE_oid || b->ttype == TYPE_oid) &&
	    (tp != TYPE_str ||
	     BATatoms[b->ttype].atomToStr == BATatoms[TYPE_str].atomToStr)) {
		return COLcopy(b, tp, false, TRANSIENT);
	}
	if (ATOMstorage(tp) == TYPE_ptr) {
		GDKerror("BATconvert: type combination (convert(%s)->%s) "
			 "not supported.\n",
			 ATOMname(b->ttype), ATOMname(tp));
		return NULL;
	}

	bn = COLnew(ci.hseq, tp, ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;

	if (b->ttype == TYPE_void)
		nils = convert_void_any(b->tseqbase, bn,
					&ci, rv, b->hseqbase,
					abort_on_error, &reduce);
	else if (tp == TYPE_str)
		nils = convert_any_str(b, bn, &ci, rv);
	else if (b->ttype == TYPE_str) {
		reduce = true;
		nils = convert_str_any(b, tp, Tloc(bn, 0),
				       &ci, rv, b->hseqbase,
				       abort_on_error);
	} else
		nils = convert_typeswitchloop(Tloc(b, 0), b->ttype,
					      Tloc(bn, 0), tp,
					      &ci, rv, b->hseqbase,
					      abort_on_error, &reduce);

	if (nils >= BUN_NONE) {
		BBPunfix(bn->batCacheid);
		if (nils == BUN_NONE + 1) {
			GDKerror("%s: type combination (convert(%s)->%s) "
				 "not supported.\n", __func__,
				 ATOMname(b->ttype), ATOMname(tp));
		} else if (nils == BUN_NONE + 2) {
			GDKerror("%s: could not insert value into BAT.\n", __func__);
		}
		return NULL;
	}

	BATsetcount(bn, ncand);

	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	if ((bn->ttype != TYPE_bit && b->ttype != TYPE_str) ||
	    BATcount(bn) < 2) {
		bn->tsorted = nils == 0 && b->tsorted;
		bn->trevsorted = nils == 0 && b->trevsorted;
	} else {
		bn->tsorted = false;
		bn->trevsorted = false;
	}
	if (!reduce || BATcount(bn) < 2)
		bn->tkey = b->tkey && nils <= 1;
	else
		bn->tkey = false;

	return bn;
}

gdk_return
VARconvert(ValPtr ret, const ValRecord *v, bool abort_on_error)
{
	ptr p;
	BUN nils = 0;
	bool reduce;

	if (ret->vtype == TYPE_str) {
		if (v->vtype == TYPE_void ||
		    (*ATOMcompare(v->vtype))(VALptr(v),
					     ATOMnilptr(v->vtype)) == 0) {
			ret->val.sval = GDKstrdup(str_nil);
		} else if (BATatoms[v->vtype].atomToStr == BATatoms[TYPE_str].atomToStr) {
			ret->val.sval = GDKstrdup(v->val.sval);
		} else {
			ret->len = 0;
			ret->val.sval = NULL;
			if ((*BATatoms[v->vtype].atomToStr)(&ret->val.sval,
							    &ret->len,
							    VALptr(v),
							    false) < 0) {
				GDKfree(ret->val.sval);
				ret->val.sval = NULL;
				ret->len = 0;
				nils = BUN_NONE;
			}
		}
		if (ret->val.sval == NULL)
			nils = BUN_NONE;
	} else if (ret->vtype == TYPE_void) {
		if (abort_on_error &&
		    ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0) {
			GDKerror("22003!cannot convert non-nil to void.\n");
			nils = BUN_NONE;
		}
		ret->val.oval = oid_nil;
	} else if (v->vtype == TYPE_void) {
		if (VALinit(ret, ret->vtype, ATOMnilptr(ret->vtype)) == NULL)
			nils = BUN_NONE;
	} else if (v->vtype == TYPE_str) {
		if (strNil(v->val.sval)) {
			if (VALinit(ret, ret->vtype, ATOMnilptr(ret->vtype)) == NULL)
				nils = BUN_NONE;
		} else if (ATOMstorage(ret->vtype) == TYPE_ptr) {
			nils = BUN_NONE + 1;
		} else {
			ssize_t l;
			size_t len;

			if (ATOMextern(ret->vtype)) {
				/* let atomFromStr allocate memory
				 * which we later give away to ret */
				p = NULL;
				len = 0;
			} else {
				/* use the space provided by ret */
				p = VALget(ret);
				len = ATOMsize(ret->vtype);
			}
			if ((l = (*BATatoms[ret->vtype].atomFromStr)(
				     v->val.sval, &len, &p, false)) < 0 ||
			    l < (ssize_t) strlen(v->val.sval)) {
				if (ATOMextern(ret->vtype))
					GDKfree(p);
				GDKclrerr();
				GDKerror("22018!conversion of string "
					 "'%s' to type %s failed.\n",
					 v->val.sval, ATOMname(ret->vtype));
				nils = BUN_NONE;
			} else {
				/* now give value obtained to ret */
				assert(ATOMextern(ret->vtype) ||
				       p == VALget(ret));
				ret->len = (int) len;
				if (ATOMextern(ret->vtype))
					VALset(ret, ret->vtype, p);
			}
		}
	} else {
		nils = convert_typeswitchloop(VALptr(v), v->vtype,
					      VALget(ret), ret->vtype,
					      &(struct canditer){.tpe=cand_dense, .ncand=1},
					      NULL,
					      0, abort_on_error, &reduce);
	}
	if (nils == BUN_NONE + 1) {
		GDKerror("VARconvert: conversion from type %s to type %s "
			 "unsupported.\n",
			 ATOMname(v->vtype), ATOMname(ret->vtype));
		return GDK_FAIL;
	}
	ret->len = ATOMlen(ret->vtype, VALptr(ret));
	return nils == BUN_NONE ? GDK_FAIL : GDK_SUCCEED;
}
