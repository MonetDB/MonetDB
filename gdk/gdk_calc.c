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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* Define symbol FULL_IMPLEMENTATION to get implementations for all
 * sensible output types for +, -, *, /.  Without the symbol, all
 * combinations of input types are supported, but only output types
 * that are either the largest of the input types or one size larger
 * (if available) for +, -, *.  For division the output type can be
 * either input type of flt or dbl. */

static int
checkbats(BAT *b1, BAT *b2, const char *func)
{
	if (b2 != NULL) {
		if (b1->U->count != b2->U->count) {
			GDKerror("%s: inputs not the same size.\n", func);
			return GDK_FAIL;
		}
	}
	return GDK_SUCCEED;
}

#define UNARY_2TYPE_FUNC(TYPE1, TYPE2, FUNC)				\
	do {								\
		const TYPE1 *src = (const TYPE1 *) Tloc(b, b->U->first); \
		TYPE2 *dst = (TYPE2 *) Tloc(bn, bn->U->first);		\
		if (b->T->nonil) {					\
			for (i = 0; i < b->U->count; i++)		\
				dst[i] = FUNC(src[i]);			\
		} else {						\
			for (i = 0; i < b->U->count; i++) {		\
				if (src[i] == TYPE1##_nil) {		\
					nils++;				\
					dst[i] = TYPE2##_nil;		\
				} else {				\
					dst[i] = FUNC(src[i]);		\
				}					\
			}						\
		}							\
	} while (0)

#define UNARY_2TYPE_FUNC_CHECK(TYPE1, TYPE2, FUNC, CHECK)		\
	do {								\
		const TYPE1 *src = (const TYPE1 *) Tloc(b, b->U->first); \
		TYPE2 *dst = (TYPE2 *) Tloc(bn, bn->U->first);		\
		if (b->T->nonil) {					\
			for (i = 0; i < b->U->count; i++)		\
				dst[i] = FUNC(src[i]);			\
		} else {						\
			for (i = 0; i < b->U->count; i++) {		\
				if (src[i] == TYPE1##_nil) {		\
					nils++;				\
					dst[i] = TYPE2##_nil;		\
				} else if (CHECK(src[i], TYPE1)) {	\
					if (abort_on_error)		\
						goto checkfail;		\
					dst[i] = TYPE2##_nil;		\
					nils++;				\
				} else {				\
					dst[i] = FUNC(src[i]);		\
				}					\
			}						\
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC(TYPE1, TYPE2, TYPE3, FUNC)			\
	do {								\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (((const TYPE1 *) lft)[i] == TYPE1##_nil ||	\
			    ((const TYPE2 *) rgt)[j] == TYPE2##_nil) {	\
				nils++;					\
				((TYPE3 *) dst)[k] = TYPE3##_nil;	\
			} else {					\
				((TYPE3 *) dst)[k] = FUNC(((const TYPE1 *) lft)[i], ((const TYPE2 *) rgt)[j]); \
			}						\
		}							\
	} while (0)

#define BINARY_3TYPE_FUNC_nonil(TYPE1, TYPE2, TYPE3, FUNC)		\
	do {								\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) \
			((TYPE3 *) dst)[k] = FUNC(((const TYPE1 *) lft)[i], ((const TYPE2 *) rgt)[j]); \
	} while (0)

#define BINARY_3TYPE_FUNC_CHECK(TYPE1, TYPE2, TYPE3, FUNC, CHECK)	\
	do {								\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (((const TYPE1 *) lft)[i] == TYPE1##_nil ||	\
			    ((const TYPE2 *) rgt)[j] == TYPE2##_nil) {	\
				nils++;					\
				((TYPE3 *) dst)[k] = TYPE3##_nil;	\
			} else if (CHECK(((const TYPE1 *) lft)[i], ((const TYPE2 *) rgt)[j])) { \
				if (abort_on_error)			\
					goto checkfail;			\
				((TYPE3 *)dst)[k] = TYPE3##_nil;	\
				nils++;					\
			} else {					\
				((TYPE3 *) dst)[k] = FUNC(((const TYPE1 *) lft)[i], ((const TYPE2 *) rgt)[j]); \
			}						\
		}							\
	} while (0)

#define NOT(x)		(~(x))
#define NOTBIT(x)	(!(x))

BAT *
BATcalcnot(BAT *b, int accum)
{
	BAT *bn;
	BUN nils = 0;
	BUN i;

	BATcheck(b, "BATcalcnot");
	if (checkbats(b, NULL, "BATcalcnot") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	switch (ATOMstorage(b->T->type)) {
	case TYPE_bte:
		if (b->T->type == TYPE_bit) {
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
	default:
		BBPunfix(bn->batCacheid);
		GDKerror("BATcalcnot: type %s not supported.\n", ATOMname(b->T->type));
		return NULL;
	}

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* NOT reverses the order, but NILs mess it up */
	bn->T->sorted = nils == 0 ? b->T->revsorted : 0;
	bn->T->revsorted = nils == 0 ? b->T->sorted : 0;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	bn->T->key = b->T->key & 1;

	if (nils != 0 && !b->T->nil) {
		b->T->nil = 1;
		b->P->descdirty = 1;
	}
	if (nils == 0 && !b->T->nonil) {
		b->T->nonil = 1;
		b->P->descdirty = 1;
	}

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcnot(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMstorage(v->vtype)) {
	case TYPE_bte:
		if (v->vtype == TYPE_bit) {
			if (v->val.btval == bit_nil)
				ret->val.btval = bit_nil;
			else
				ret->val.btval = !v->val.btval;
		} else {
			if (v->val.btval == bte_nil)
				ret->val.btval = bte_nil;
			else
				ret->val.btval = ~v->val.btval;
		}
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
	default:
		GDKerror("VARcalcnot: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

#define NEGATE(x)	(-(x))

BAT *
BATcalcnegate(BAT *b, int accum)
{
	BAT *bn;
	BUN nils = 0;
	BUN i;

	BATcheck(b, "BATcalcnegate");
	if (checkbats(b, NULL, "BATcalcnegate") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	switch (ATOMstorage(b->T->type)) {
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
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, flt, NEGATE);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, dbl, NEGATE);
		break;
	default:
		BBPunfix(bn->batCacheid);
		GDKerror("BATcalcnegate: type %s not supported.\n", ATOMname(b->T->type));
		return NULL;
	}

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* unary - reverses the order, but NILs mess it up */
	bn->T->sorted = nils == 0 ? b->T->revsorted : 0;
	bn->T->revsorted = nils == 0 ? b->T->sorted : 0;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	bn->T->key = b->T->key & 1;

	if (nils != 0 && !b->T->nil) {
		b->T->nil = 1;
		b->P->descdirty = 1;
	}
	if (nils == 0 && !b->T->nonil) {
		b->T->nonil = 1;
		b->P->descdirty = 1;
	}

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcnegate(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMstorage(v->vtype)) {
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
		GDKerror("VARcalcnegate: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

#define ABSOLUTE(x)	((x) < 0 ? -(x) : (x))

BAT *
BATcalcabsolute(BAT *b, int accum)
{
	BAT *bn;
	BUN nils= 0;
	BUN i;

	BATcheck(b, "BATcalcabsolute");
	if (checkbats(b, NULL, "BATcalcabsolute") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	switch (ATOMstorage(b->T->type)) {
	case TYPE_bte:
		UNARY_2TYPE_FUNC(bte, bte, ABSOLUTE);
		break;
	case TYPE_sht:
		UNARY_2TYPE_FUNC(sht, sht, ABSOLUTE);
		break;
	case TYPE_int:
		UNARY_2TYPE_FUNC(int, int, ABSOLUTE);
		break;
	case TYPE_lng:
		UNARY_2TYPE_FUNC(lng, lng, ABSOLUTE);
		break;
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, flt, ABSOLUTE);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, dbl, ABSOLUTE);
		break;
	default:
		BBPunfix(bn->batCacheid);
		GDKerror("BATcalcabsolute: bad input type %s.\n",
			 ATOMname(b->T->type));
		return NULL;
	}

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* ABSOLUTE messes up order (unless all values were negative
	 * or all values were positive, but we don't know anything
	 * about that) */
	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (nils && !b->T->nil) {
		b->T->nil = 1;
		b->P->descdirty = 1;
	}
	if (nils == 0 && !b->T->nonil) {
		b->T->nonil = 1;
		b->P->descdirty = 1;
	}

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcabsolute(ValPtr ret, const ValRecord *v)
{
	ret->vtype = v->vtype;
	switch (ATOMstorage(v->vtype)) {
	case TYPE_bte:
		if (v->val.btval == bte_nil)
			ret->val.btval = bte_nil;
		else
			ret->val.btval = ABSOLUTE(v->val.btval);
		break;
	case TYPE_sht:
		if (v->val.shval == sht_nil)
			ret->val.shval = sht_nil;
		else
			ret->val.shval = ABSOLUTE(v->val.shval);
		break;
	case TYPE_int:
		if (v->val.ival == int_nil)
			ret->val.ival = int_nil;
		else
			ret->val.ival = ABSOLUTE(v->val.ival);
		break;
	case TYPE_lng:
		if (v->val.lval == lng_nil)
			ret->val.lval = lng_nil;
		else
			ret->val.lval = ABSOLUTE(v->val.lval);
		break;
	case TYPE_flt:
		if (v->val.fval == flt_nil)
			ret->val.fval = flt_nil;
		else
			ret->val.fval = ABSOLUTE(v->val.fval);
		break;
	case TYPE_dbl:
		if (v->val.dval == dbl_nil)
			ret->val.dval = dbl_nil;
		else
			ret->val.dval = ABSOLUTE(v->val.dval);
		break;
	default:
		GDKerror("VARcalcabsolute: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

#define ISZERO(x)		((bit) ((x) == 0))

BAT *
BATcalciszero(BAT *b)
{
	BAT *bn;
	BUN nils = 0;
	BUN i;

	BATcheck(b, "BATcalciszero");
	if (checkbats(b, NULL, "BATcalciszero") == GDK_FAIL)
		return NULL;

	bn = BATnew(TYPE_void, TYPE_bit, b->U->count);
	if (bn == NULL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	switch (ATOMstorage(b->T->type)) {
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
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, bit, ISZERO);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, bit, ISZERO);
		break;
	default:
		BBPunfix(bn->batCacheid);
		GDKerror("BATcalciszero: bad input type %s.\n",
			 ATOMname(b->T->type));
		return NULL;
	}

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (nils != 0 && !b->T->nil) {
		b->T->nil = 1;
		b->P->descdirty = 1;
	}
	if (nils == 0 && !b->T->nonil) {
		b->T->nonil = 1;
		b->P->descdirty = 1;
	}

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalciszero(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bit;
	switch (ATOMstorage(v->vtype)) {
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
		GDKerror("VARcalciszero: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

#define SIGN(x)		((bte) ((x) < 0 ? -1 : (x) > 0))

BAT *
BATcalcsign(BAT *b)
{
	BAT *bn;
	BUN nils = 0;
	BUN i;

	BATcheck(b, "BATcalcsign");
	if (checkbats(b, NULL, "BATcalcsign") == GDK_FAIL)
		return NULL;

	bn = BATnew(TYPE_void, TYPE_bte, b->U->count);
	if (bn == NULL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	switch (ATOMstorage(b->T->type)) {
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
	case TYPE_flt:
		UNARY_2TYPE_FUNC(flt, bte, SIGN);
		break;
	case TYPE_dbl:
		UNARY_2TYPE_FUNC(dbl, bte, SIGN);
		break;
	default:
		BBPunfix(bn->batCacheid);
		GDKerror("BATcalcsign: bad input type %s.\n",
			 ATOMname(b->T->type));
		return NULL;
	}

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* SIGN is ordered if the input is ordered (negative comes
	 * first, positive comes after) and NILs stay in the same
	 * position */
	bn->T->sorted = b->T->sorted || bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->T->revsorted || bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (nils != 0 && !b->T->nil) {
		b->T->nil = 1;
		b->P->descdirty = 1;
	}
	if (nils == 0 && !b->T->nonil) {
		b->T->nonil = 1;
		b->P->descdirty = 1;
	}

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcsign(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bte;
	switch (ATOMstorage(v->vtype)) {
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

#define ISNIL_TYPE(TYPE)						\
	do {								\
		const TYPE *src = (const TYPE *) Tloc(b, b->U->first);	\
		for (i = 0; i < b->U->count; i++)			\
			dst[i] = (bit) (src[i] == TYPE##_nil);		\
	} while (0)

BAT *
BATcalcisnil(BAT *b)
{
	BAT *bn;
	BUN i;
	bit *dst;

	BATcheck(b, "BATcalcisnil");

	if (b->T->nonil ||
	    (b->T->type == TYPE_void && b->tseqbase != oid_nil)) {
		bit zero = 0;

		return BATconst(b, TYPE_bit, &zero);
	} else if (b->T->type == TYPE_void && b->tseqbase == oid_nil) {
		bit one = 1;

		return BATconst(b, TYPE_bit, &one);
	}

	bn = BATnew(TYPE_void, TYPE_bit, b->U->count);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, bn->U->first);

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	switch (ATOMstorage(b->T->type)) {
	case TYPE_bte:
		ISNIL_TYPE(bte);
		break;
	case TYPE_sht:
		ISNIL_TYPE(sht);
		break;
	case TYPE_int:
	case TYPE_bat:
		ISNIL_TYPE(int);
		break;
	case TYPE_lng:
		ISNIL_TYPE(lng);
		break;
	case TYPE_flt:
		ISNIL_TYPE(flt);
		break;
	case TYPE_dbl:
		ISNIL_TYPE(dbl);
		break;
	default:
	{
		BATiter bi = bat_iterator(b);
		BUN j;
		ptr v;
		ptr nil = ATOMnilptr(b->T->type);
		int (*atomcmp)(ptr, ptr) = BATatoms[b->T->type].atomCmp;

		BATloop(b, i, j) {
			v = BUNtail(bi, i);
			*dst++ = (bit) ((*atomcmp)(v, nil) == 0);
		}
		break;
	}
	}

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* If b sorted, all nils are at the start, i.e. bn starts with
	 * 1's and ends with 0's, hence bn is revsorted.  Similarly
	 * for revsorted. */
	bn->T->sorted = b->T->revsorted;
	bn->T->revsorted = b->T->sorted;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	bn->T->key = bn->U->count <= 1;

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcisnil(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bit;
	switch (ATOMstorage(v->vtype)) {
	case TYPE_void:
		ret->val.btval = (bit) 1;
		break;
	case TYPE_bte:
		ret->val.btval = (bit) (v->val.btval == bte_nil);
		break;
	case TYPE_sht:
		ret->val.btval = (bit) (v->val.shval == sht_nil);
		break;
	case TYPE_int:
	case TYPE_bat:
		ret->val.btval = (bit) (v->val.ival == int_nil);
		break;
	case TYPE_lng:
		ret->val.btval = (bit) (v->val.lval == lng_nil);
		break;
	case TYPE_flt:
		ret->val.btval = (bit) (v->val.fval == flt_nil);
		break;
	case TYPE_dbl:
		ret->val.btval = (bit) (v->val.dval == dbl_nil);
		break;
	default:
		ret->val.btval = (bit) (*BATatoms[v->vtype].atomCmp)(VALptr((ValPtr) v), ATOMnilptr(v->vtype)) == 0;
		break;
	}
	return GDK_SUCCEED;
}

int
VARcalcisnotnil(ValPtr ret, const ValRecord *v)
{
	ret->vtype = TYPE_bit;
	switch (ATOMstorage(v->vtype)) {
	case TYPE_void:
		ret->val.btval = (bit) 0;
		break;
	case TYPE_bte:
		ret->val.btval = (bit) (v->val.btval != bte_nil);
		break;
	case TYPE_sht:
		ret->val.btval = (bit) (v->val.shval != sht_nil);
		break;
	case TYPE_int:
	case TYPE_bat:
		ret->val.btval = (bit) (v->val.ival != int_nil);
		break;
	case TYPE_lng:
		ret->val.btval = (bit) (v->val.lval != lng_nil);
		break;
	case TYPE_flt:
		ret->val.btval = (bit) (v->val.fval != flt_nil);
		break;
	case TYPE_dbl:
		ret->val.btval = (bit) (v->val.dval != dbl_nil);
		break;
	default:
		ret->val.btval = (bit) (*BATatoms[v->vtype].atomCmp)(VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0;
		break;
	}
	return GDK_SUCCEED;
}

#define ADD_3TYPE(TYPE1, TYPE2, TYPE3)					\
	static BUN							\
	add_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt, int abort_on_error) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] < 1) {			\
				if (GDK_##TYPE3##_min - rgt[j] >= lft[i]) { \
					if (abort_on_error)		\
						return BUN_NONE;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				} else {				\
					dst[k] = (TYPE3) lft[i] + rgt[j]; \
				}					\
			} else {					\
				if (GDK_##TYPE3##_max - rgt[j] < lft[i]) { \
					if (abort_on_error)		\
						return BUN_NONE;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				} else {				\
					dst[k] = (TYPE3) lft[i] + rgt[j]; \
				}					\
			}						\
		}							\
		return nils;						\
	}

#define ADD_3TYPE_enlarge(TYPE1, TYPE2, TYPE3)				\
	static BUN							\
	add_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] + rgt[j];	\
			}						\
		}							\
		return nils;						\
	}

ADD_3TYPE(bte, bte, bte)
ADD_3TYPE_enlarge(bte, bte, sht)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, bte, int)
ADD_3TYPE_enlarge(bte, bte, lng)
ADD_3TYPE_enlarge(bte, bte, flt)
ADD_3TYPE_enlarge(bte, bte, dbl)
#endif
ADD_3TYPE(bte, sht, sht)
ADD_3TYPE_enlarge(bte, sht, int)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, sht, lng)
ADD_3TYPE_enlarge(bte, sht, flt)
ADD_3TYPE_enlarge(bte, sht, dbl)
#endif
ADD_3TYPE(bte, int, int)
ADD_3TYPE_enlarge(bte, int, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, int, flt)
ADD_3TYPE_enlarge(bte, int, dbl)
#endif
ADD_3TYPE(bte, lng, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(bte, lng, flt)
ADD_3TYPE_enlarge(bte, lng, dbl)
#endif
ADD_3TYPE(bte, flt, flt)
ADD_3TYPE_enlarge(bte, flt, dbl)
ADD_3TYPE(bte, dbl, dbl)
ADD_3TYPE(sht, bte, sht)
ADD_3TYPE_enlarge(sht, bte, int)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, bte, lng)
ADD_3TYPE_enlarge(sht, bte, flt)
ADD_3TYPE_enlarge(sht, bte, dbl)
#endif
ADD_3TYPE(sht, sht, sht)
ADD_3TYPE_enlarge(sht, sht, int)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, sht, lng)
ADD_3TYPE_enlarge(sht, sht, flt)
ADD_3TYPE_enlarge(sht, sht, dbl)
#endif
ADD_3TYPE(sht, int, int)
ADD_3TYPE_enlarge(sht, int, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, int, flt)
ADD_3TYPE_enlarge(sht, int, dbl)
#endif
ADD_3TYPE(sht, lng, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(sht, lng, flt)
ADD_3TYPE_enlarge(sht, lng, dbl)
#endif
ADD_3TYPE(sht, flt, flt)
ADD_3TYPE_enlarge(sht, flt, dbl)
ADD_3TYPE(sht, dbl, dbl)
ADD_3TYPE(int, bte, int)
ADD_3TYPE_enlarge(int, bte, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(int, bte, flt)
ADD_3TYPE_enlarge(int, bte, dbl)
#endif
ADD_3TYPE(int, sht, int)
ADD_3TYPE_enlarge(int, sht, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(int, sht, flt)
ADD_3TYPE_enlarge(int, sht, dbl)
#endif
ADD_3TYPE(int, int, int)
ADD_3TYPE_enlarge(int, int, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(int, int, flt)
ADD_3TYPE_enlarge(int, int, dbl)
#endif
ADD_3TYPE(int, lng, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(int, lng, flt)
ADD_3TYPE_enlarge(int, lng, dbl)
#endif
ADD_3TYPE(int, flt, flt)
ADD_3TYPE_enlarge(int, flt, dbl)
ADD_3TYPE(int, dbl, dbl)
ADD_3TYPE(lng, bte, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, bte, flt)
ADD_3TYPE_enlarge(lng, bte, dbl)
#endif
ADD_3TYPE(lng, sht, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, sht, flt)
ADD_3TYPE_enlarge(lng, sht, dbl)
#endif
ADD_3TYPE(lng, int, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, int, flt)
ADD_3TYPE_enlarge(lng, int, dbl)
#endif
ADD_3TYPE(lng, lng, lng)
#ifdef FULL_IMPLEMENTATION
ADD_3TYPE_enlarge(lng, lng, flt)
ADD_3TYPE_enlarge(lng, lng, dbl)
#endif
ADD_3TYPE(lng, flt, flt)
ADD_3TYPE_enlarge(lng, flt, dbl)
ADD_3TYPE(lng, dbl, dbl)
ADD_3TYPE(flt, bte, flt)
ADD_3TYPE_enlarge(flt, bte, dbl)
ADD_3TYPE(flt, sht, flt)
ADD_3TYPE_enlarge(flt, sht, dbl)
ADD_3TYPE(flt, int, flt)
ADD_3TYPE_enlarge(flt, int, dbl)
ADD_3TYPE(flt, lng, flt)
ADD_3TYPE_enlarge(flt, lng, dbl)
ADD_3TYPE(flt, flt, flt)
ADD_3TYPE_enlarge(flt, flt, dbl)
ADD_3TYPE(flt, dbl, dbl)
ADD_3TYPE(dbl, bte, dbl)
ADD_3TYPE(dbl, sht, dbl)
ADD_3TYPE(dbl, int, dbl)
ADD_3TYPE(dbl, lng, dbl)
ADD_3TYPE(dbl, flt, dbl)
ADD_3TYPE(dbl, dbl, dbl)

static BUN
add_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *dst, int tp, BUN cnt,
		   int abort_on_error, const char *func)
{
	BUN nils;

	switch (ATOMstorage(tp1)) {
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = add_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = add_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = add_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_lng:
				nils = add_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = add_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = add_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = add_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = add_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = add_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = add_bte_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = add_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = add_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = add_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = add_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = add_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = add_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = add_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = add_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = add_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = add_sht_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = add_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = add_int_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = add_int_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = add_int_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = add_int_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_int_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = add_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = add_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = add_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = add_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = add_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = add_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = add_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = add_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = add_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = add_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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

	if (nils == BUN_NONE)
		GDKerror("22003!overflow in calculation.\n");

	return nils;

  unsupported:
	GDKerror("%s: type combination (add(%s,%s)->%s) not supported.\n", func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcadd(BAT *b1, BAT *b2, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcadd");
	BATcheck(b2, "BATcalcadd");

	if (checkbats(b1, b2, "BATcalcadd") == GDK_FAIL)
		return NULL;

	if (accum == 1) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		assert(b1->T->type == tp);
		bn = b1;
		BBPfix(b1->batCacheid);
	} else if (accum == 2) {
		assert(BBP_refs(b2->batCacheid) == 1);
		if (BBP_lrefs(b2->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b2));
		assert(b2->T->type == tp);
		bn = b2;
		BBPfix(b2->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, tp, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = add_typeswitchloop(Tloc(b1, b1->U->first), b1->T->type, 1,
				  Tloc(b2, b2->U->first), b2->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b1->U->count, abort_on_error, "BATcalcadd");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	/* if both inputs are sorted the same way, and no overflow
	 * occurred (we only know for sure if abort_on_error is set),
	 * the result is also sorted */
	bn->T->sorted = (abort_on_error && b1->T->sorted & b2->T->sorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = (abort_on_error && b1->T->revsorted & b2->T->revsorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcaddcst(BAT *b, const ValRecord *v, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcaddcst");

	if (checkbats(b, NULL, "BATcalcaddcst") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = add_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalcaddcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->T->sorted = (abort_on_error && b->T->sorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = (abort_on_error && b->T->revsorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstadd(const ValRecord *v, BAT *b, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstadd");

	if (checkbats(b, NULL, "BATcalccstadd") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = add_typeswitchloop(VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(b, b->U->first), b->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalccstadd");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->T->sorted = (abort_on_error && b->T->sorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = (abort_on_error && b->T->revsorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcadd(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	if (add_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0,
			       (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0,
			       VALptr(ret), ret->vtype, 1,
			       abort_on_error, "VARcalcadd") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

BAT *
BATcalcincr(BAT *b, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils= 0;
	bte one = 1;

	BATcheck(b, "BATcalcincr");
	if (checkbats(b, NULL, "BATcalcincr") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = add_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  &one, TYPE_bte, 0,
				  Tloc(bn, bn->U->first), bn->T->type,
				  b->U->count, abort_on_error, "BATcalcincr");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->T->sorted = (abort_on_error && b->T->sorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = (abort_on_error && b->T->revsorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (nils && !b->T->nil) {
		b->T->nil = 1;
		b->P->descdirty = 1;
	}
	if (nils == 0 && !b->T->nonil) {
		b->T->nonil = 1;
		b->P->descdirty = 1;
	}

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcincr(ValPtr ret, const ValRecord *v, int abort_on_error)
{
	bte one = 1;

	if (add_typeswitchloop((const void *) VALptr((ValPtr) v), v->vtype, 0,
			       &one, TYPE_bte, 0,
			       VALptr(ret), ret->vtype, 1,
			       abort_on_error, "VARcalcincr") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define SUB_3TYPE(TYPE1, TYPE2, TYPE3)					\
	static BUN							\
	sub_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt, int abort_on_error) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] < 1) {			\
				if (GDK_##TYPE3##_max + rgt[j] < lft[i]) { \
					if (abort_on_error)		\
						return BUN_NONE;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				} else {				\
					dst[k] = (TYPE3) lft[i] - rgt[j]; \
				}					\
			} else {					\
				if (GDK_##TYPE3##_min + rgt[j] >= lft[i]) { \
					if (abort_on_error)		\
						return BUN_NONE;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				} else {				\
					dst[k] = (TYPE3) lft[i] - rgt[j]; \
				}					\
			}						\
		}							\
		return nils;						\
	}

#define SUB_3TYPE_enlarge(TYPE1, TYPE2, TYPE3)				\
	static BUN							\
	sub_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] - rgt[j];	\
			}						\
		}							\
		return nils;						\
	}

SUB_3TYPE(bte, bte, bte)
SUB_3TYPE_enlarge(bte, bte, sht)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, bte, int)
SUB_3TYPE_enlarge(bte, bte, lng)
SUB_3TYPE_enlarge(bte, bte, flt)
SUB_3TYPE_enlarge(bte, bte, dbl)
#endif
SUB_3TYPE(bte, sht, sht)
SUB_3TYPE_enlarge(bte, sht, int)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, sht, lng)
SUB_3TYPE_enlarge(bte, sht, flt)
SUB_3TYPE_enlarge(bte, sht, dbl)
#endif
SUB_3TYPE(bte, int, int)
SUB_3TYPE_enlarge(bte, int, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, int, flt)
SUB_3TYPE_enlarge(bte, int, dbl)
#endif
SUB_3TYPE(bte, lng, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(bte, lng, flt)
SUB_3TYPE_enlarge(bte, lng, dbl)
#endif
SUB_3TYPE(bte, flt, flt)
SUB_3TYPE_enlarge(bte, flt, dbl)
SUB_3TYPE(bte, dbl, dbl)
SUB_3TYPE(sht, bte, sht)
SUB_3TYPE_enlarge(sht, bte, int)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, bte, lng)
SUB_3TYPE_enlarge(sht, bte, flt)
SUB_3TYPE_enlarge(sht, bte, dbl)
#endif
SUB_3TYPE(sht, sht, sht)
SUB_3TYPE_enlarge(sht, sht, int)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, sht, lng)
SUB_3TYPE_enlarge(sht, sht, flt)
SUB_3TYPE_enlarge(sht, sht, dbl)
#endif
SUB_3TYPE(sht, int, int)
SUB_3TYPE_enlarge(sht, int, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, int, flt)
SUB_3TYPE_enlarge(sht, int, dbl)
#endif
SUB_3TYPE(sht, lng, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(sht, lng, flt)
SUB_3TYPE_enlarge(sht, lng, dbl)
#endif
SUB_3TYPE(sht, flt, flt)
SUB_3TYPE_enlarge(sht, flt, dbl)
SUB_3TYPE(sht, dbl, dbl)
SUB_3TYPE(int, bte, int)
SUB_3TYPE_enlarge(int, bte, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(int, bte, flt)
SUB_3TYPE_enlarge(int, bte, dbl)
#endif
SUB_3TYPE(int, sht, int)
SUB_3TYPE_enlarge(int, sht, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(int, sht, flt)
SUB_3TYPE_enlarge(int, sht, dbl)
#endif
SUB_3TYPE(int, int, int)
SUB_3TYPE_enlarge(int, int, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(int, int, flt)
SUB_3TYPE_enlarge(int, int, dbl)
#endif
SUB_3TYPE(int, lng, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(int, lng, flt)
SUB_3TYPE_enlarge(int, lng, dbl)
#endif
SUB_3TYPE(int, flt, flt)
SUB_3TYPE_enlarge(int, flt, dbl)
SUB_3TYPE(int, dbl, dbl)
SUB_3TYPE(lng, bte, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, bte, flt)
SUB_3TYPE_enlarge(lng, bte, dbl)
#endif
SUB_3TYPE(lng, sht, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, sht, flt)
SUB_3TYPE_enlarge(lng, sht, dbl)
#endif
SUB_3TYPE(lng, int, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, int, flt)
SUB_3TYPE_enlarge(lng, int, dbl)
#endif
SUB_3TYPE(lng, lng, lng)
#ifdef FULL_IMPLEMENTATION
SUB_3TYPE_enlarge(lng, lng, flt)
SUB_3TYPE_enlarge(lng, lng, dbl)
#endif
SUB_3TYPE(lng, flt, flt)
SUB_3TYPE_enlarge(lng, flt, dbl)
SUB_3TYPE(lng, dbl, dbl)
SUB_3TYPE(flt, bte, flt)
SUB_3TYPE_enlarge(flt, bte, dbl)
SUB_3TYPE(flt, sht, flt)
SUB_3TYPE_enlarge(flt, sht, dbl)
SUB_3TYPE(flt, int, flt)
SUB_3TYPE_enlarge(flt, int, dbl)
SUB_3TYPE(flt, lng, flt)
SUB_3TYPE_enlarge(flt, lng, dbl)
SUB_3TYPE(flt, flt, flt)
SUB_3TYPE_enlarge(flt, flt, dbl)
SUB_3TYPE(flt, dbl, dbl)
SUB_3TYPE(dbl, bte, dbl)
SUB_3TYPE(dbl, sht, dbl)
SUB_3TYPE(dbl, int, dbl)
SUB_3TYPE(dbl, lng, dbl)
SUB_3TYPE(dbl, flt, dbl)
SUB_3TYPE(dbl, dbl, dbl)

static BUN
sub_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *dst, int tp, BUN cnt,
		   int abort_on_error, const char *func)
{
	BUN nils;

	switch (ATOMstorage(tp1)) {
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = sub_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = sub_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = sub_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_lng:
				nils = sub_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = sub_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = sub_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = sub_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = sub_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = sub_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = sub_bte_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = sub_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = sub_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = sub_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = sub_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = sub_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = sub_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = sub_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = sub_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = sub_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = sub_sht_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = sub_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = sub_int_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = sub_int_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = sub_int_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = sub_int_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_int_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = sub_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = sub_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = sub_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = sub_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = sub_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = sub_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = sub_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = sub_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = sub_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = sub_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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

	if (nils == BUN_NONE)
		GDKerror("22003!overflow in calculation.\n");

	return nils;

  unsupported:
	GDKerror("%s: type combination (sub(%s,%s)->%s) not supported.\n", func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcsub(BAT *b1, BAT *b2, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcsub");
	BATcheck(b2, "BATcalcsub");

	if (checkbats(b1, b2, "BATcalcsub") == GDK_FAIL)
		return NULL;

	if (accum == 1) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		assert(b1->T->type == tp);
		bn = b1;
		BBPfix(b1->batCacheid);
	} else if (accum == 2) {
		assert(BBP_refs(b2->batCacheid) == 1);
		if (BBP_lrefs(b2->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b2));
		assert(b2->T->type == tp);
		bn = b2;
		BBPfix(b2->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, tp, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = sub_typeswitchloop(Tloc(b1, b1->U->first), b1->T->type, 1,
				  Tloc(b2, b2->U->first), b2->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b1->U->count, abort_on_error, "BATcalcsub");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcsubcst(BAT *b, const ValRecord *v, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcsubcst");

	if (checkbats(b, NULL, "BATcalcsubcst") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = sub_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalcsubcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->T->sorted = (abort_on_error && b->T->sorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = (abort_on_error && b->T->revsorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstsub(const ValRecord *v, BAT *b, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;
	int savesorted;

	BATcheck(b, "BATcalccstsub");

	if (checkbats(b, NULL, "BATcalccstsub") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = sub_typeswitchloop(VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(b, b->U->first), b->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalccstsub");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is
	 * sorted in the opposite direction (except that NILs mess
	 * things up */
	/* note that if b == bn (accum is set), the first assignment
	 * changes a value that we need on the right-hand side of the
	 * second assignment, so we need to save the value */
	savesorted = b->T->sorted;
	bn->T->sorted = (abort_on_error && nils == 0 && b->T->revsorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = (abort_on_error && nils == 0 && savesorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcsub(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	if (sub_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0,
			       (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0,
			       VALptr(ret), ret->vtype, 1,
			       abort_on_error, "VARcalcsub") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

BAT *
BATcalcdecr(BAT *b, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils= 0;
	bte one = 1;

	BATcheck(b, "BATcalcdecr");
	if (checkbats(b, NULL, "BATcalcdecr") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = sub_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  &one, TYPE_bte, 0,
				  Tloc(bn, bn->U->first), bn->T->type,
				  b->U->count, abort_on_error, "BATcalcdecr");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted */
	bn->T->sorted = (abort_on_error && b->T->sorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = (abort_on_error && b->T->revsorted) ||
		bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (nils && !b->T->nil) {
		b->T->nil = 1;
		b->P->descdirty = 1;
	}
	if (nils == 0 && !b->T->nonil) {
		b->T->nonil = 1;
		b->P->descdirty = 1;
	}

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcdecr(ValPtr ret, const ValRecord *v, int abort_on_error)
{
	bte one = 1;

	if (sub_typeswitchloop((const void *) VALptr((ValPtr) v), v->vtype, 0,
			       &one, TYPE_bte, 0,
			       VALptr(ret), ret->vtype, 1,
			       abort_on_error, "VARcalcdecr") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* TYPE4 must be a type larger than both TYPE1 and TYPE2 so that
 * multiplying into it doesn't cause overflow */
#define MUL_4TYPE(TYPE1, TYPE2, TYPE3, TYPE4)				\
	static BUN							\
	mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt, int abort_on_error) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
		TYPE4 c;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				c = (TYPE4) lft[i] * rgt[j];		\
				if (c <= (TYPE4) GDK_##TYPE3##_min ||	\
				    c > (TYPE4) GDK_##TYPE3##_max) {	\
					if (abort_on_error)		\
						return BUN_NONE;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				} else {				\
					dst[k] = (TYPE3) c;		\
				}					\
			}						\
		}							\
		return nils;						\
	}

#define MUL_3TYPE_enlarge(TYPE1, TYPE2, TYPE3)				\
	static BUN							\
	mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] * rgt[j];	\
			}						\
		}							\
		return nils;						\
	}

#ifdef HAVE_LONG_LONG
typedef unsigned long long ulng;
#else
typedef unsigned __int64 ulng;
#endif

#define MUL_2TYPE_lng(TYPE1, TYPE2)					\
	static BUN							\
	mul_##TYPE1##_##TYPE2##_lng(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, lng *dst, BUN cnt, int abort_on_error) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
		lng a, b;						\
		unsigned int a1, a2, b1, b2;				\
		ulng c;							\
		int sign;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = lng_nil;			\
				nils++;					\
			} else {					\
				sign = 1;				\
				if ((a = lft[i]) < 0) {			\
					sign = -sign;			\
					a = -a;				\
				}					\
				if ((b = rgt[j]) < 0) {			\
					sign = -sign;			\
					b = -b;				\
				}					\
				a1 = (unsigned int) (a >> 32);		\
				a2 = (unsigned int) a;			\
				b1 = (unsigned int) (b >> 32);		\
				b2 = (unsigned int) b;			\
				/* result = (a1*b1<<64) + ((a1*b2+a2*b1)<<32) + a2*b2 */ \
				if (a1 && b1)	/* a1*b1 != 0 ==> overflow */ \
					goto overflow;			\
				c = (ulng) a1 * b2 + (ulng) a2 * b1;	\
				if (c >> 31)	/* use 32 for unsigned */ \
					goto overflow;			\
				c <<= 32;				\
				c += (ulng) a2 * b2;			\
				if ((c >> 63) == 0)			\
					dst[k] = sign * (lng) c;	\
				else {					\
				  overflow:				\
					if (abort_on_error)		\
						return BUN_NONE;	\
					dst[k] = lng_nil;		\
					nils++;				\
				}					\
			}						\
		}							\
		return nils;						\
	}

#define MUL_2TYPE_float(TYPE1, TYPE2, TYPE3)				\
	static BUN							\
	mul_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt, int abort_on_error) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				/* only check for overflow, not for	\
				 * underflow */				\
				if (ABSOLUTE(lft[i]) > 1 &&		\
				    GDK_##TYPE3##_max / ABSOLUTE(lft[i]) < ABSOLUTE(rgt[j])) { \
					if (abort_on_error)		\
						return BUN_NONE;	\
					dst[k] = TYPE3##_nil;		\
					nils++;				\
				} else {				\
					dst[k] = (TYPE3) lft[i] * rgt[j]; \
				}					\
			}						\
		}							\
		return nils;						\
	}

MUL_4TYPE(bte, bte, bte, sht)
MUL_3TYPE_enlarge(bte, bte, sht)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, bte, int)
MUL_3TYPE_enlarge(bte, bte, lng)
MUL_3TYPE_enlarge(bte, bte, flt)
MUL_3TYPE_enlarge(bte, bte, dbl)
#endif
MUL_4TYPE(bte, sht, sht, int)
MUL_3TYPE_enlarge(bte, sht, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, sht, lng)
MUL_3TYPE_enlarge(bte, sht, flt)
MUL_3TYPE_enlarge(bte, sht, dbl)
#endif
MUL_4TYPE(bte, int, int, lng)
MUL_3TYPE_enlarge(bte, int, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, int, flt)
MUL_3TYPE_enlarge(bte, int, dbl)
#endif
MUL_2TYPE_lng(bte, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(bte, lng, flt)
MUL_3TYPE_enlarge(bte, lng, dbl)
#endif
MUL_2TYPE_float(bte, flt, flt)
MUL_3TYPE_enlarge(bte, flt, dbl)
MUL_2TYPE_float(bte, dbl, dbl)
MUL_4TYPE(sht, bte, sht, int)
MUL_3TYPE_enlarge(sht, bte, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, bte, lng)
MUL_3TYPE_enlarge(sht, bte, flt)
MUL_3TYPE_enlarge(sht, bte, dbl)
#endif
MUL_4TYPE(sht, sht, sht, int)
MUL_3TYPE_enlarge(sht, sht, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, sht, lng)
MUL_3TYPE_enlarge(sht, sht, flt)
MUL_3TYPE_enlarge(sht, sht, dbl)
#endif
MUL_4TYPE(sht, int, int, lng)
MUL_3TYPE_enlarge(sht, int, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, int, flt)
MUL_3TYPE_enlarge(sht, int, dbl)
#endif
MUL_2TYPE_lng(sht, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(sht, lng, flt)
MUL_3TYPE_enlarge(sht, lng, dbl)
#endif
MUL_2TYPE_float(sht, flt, flt)
MUL_3TYPE_enlarge(sht, flt, dbl)
MUL_2TYPE_float(sht, dbl, dbl)
MUL_4TYPE(int, bte, int, lng)
MUL_3TYPE_enlarge(int, bte, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(int, bte, flt)
MUL_3TYPE_enlarge(int, bte, dbl)
#endif
MUL_4TYPE(int, sht, int, lng)
MUL_3TYPE_enlarge(int, sht, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(int, sht, flt)
MUL_3TYPE_enlarge(int, sht, dbl)
#endif
MUL_4TYPE(int, int, int, lng)
MUL_3TYPE_enlarge(int, int, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(int, int, flt)
MUL_3TYPE_enlarge(int, int, dbl)
#endif
MUL_2TYPE_lng(int, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(int, lng, flt)
MUL_3TYPE_enlarge(int, lng, dbl)
#endif
MUL_2TYPE_float(int, flt, flt)
MUL_3TYPE_enlarge(int, flt, dbl)
MUL_2TYPE_float(int, dbl, dbl)
MUL_2TYPE_lng(lng, bte)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, bte, flt)
MUL_3TYPE_enlarge(lng, bte, dbl)
#endif
MUL_2TYPE_lng(lng, sht)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, sht, flt)
MUL_3TYPE_enlarge(lng, sht, dbl)
#endif
MUL_2TYPE_lng(lng, int)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, int, flt)
MUL_3TYPE_enlarge(lng, int, dbl)
#endif
MUL_2TYPE_lng(lng, lng)
#ifdef FULL_IMPLEMENTATION
MUL_3TYPE_enlarge(lng, lng, flt)
MUL_3TYPE_enlarge(lng, lng, dbl)
#endif
MUL_2TYPE_float(lng, flt, flt)
MUL_3TYPE_enlarge(lng, flt, dbl)
MUL_2TYPE_float(lng, dbl, dbl)
MUL_2TYPE_float(flt, bte, flt)
MUL_3TYPE_enlarge(flt, bte, dbl)
MUL_2TYPE_float(flt, sht, flt)
MUL_3TYPE_enlarge(flt, sht, dbl)
MUL_2TYPE_float(flt, int, flt)
MUL_3TYPE_enlarge(flt, int, dbl)
MUL_2TYPE_float(flt, lng, flt)
MUL_3TYPE_enlarge(flt, lng, dbl)
MUL_2TYPE_float(flt, flt, flt)
MUL_3TYPE_enlarge(flt, flt, dbl)
MUL_2TYPE_float(flt, dbl, dbl)
MUL_2TYPE_float(dbl, bte, dbl)
MUL_2TYPE_float(dbl, sht, dbl)
MUL_2TYPE_float(dbl, int, dbl)
MUL_2TYPE_float(dbl, lng, dbl)
MUL_2TYPE_float(dbl, flt, dbl)
MUL_2TYPE_float(dbl, dbl, dbl)

static BUN
mul_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *dst, int tp, BUN cnt,
		   int abort_on_error, const char *func)
{
	BUN nils;

	switch (ATOMstorage(tp1)) {
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = mul_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_sht:
				nils = mul_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mul_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_lng:
				nils = mul_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = mul_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = mul_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mul_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = mul_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = mul_bte_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = mul_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = mul_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mul_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = mul_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = mul_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mul_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mul_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_flt:
				nils = mul_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = mul_sht_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = mul_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = mul_int_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = mul_int_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = mul_int_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mul_int_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_int_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = mul_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = mul_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = mul_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = mul_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = mul_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_flt:
				nils = mul_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			case TYPE_dbl:
				nils = mul_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = mul_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = mul_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = mul_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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

	if (nils == BUN_NONE)
		GDKerror("22003!overflow in calculation.\n");

	return nils;

  unsupported:
	GDKerror("%s: type combination mul(%s,%s)->%s) not supported.\n", func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcmul(BAT *b1, BAT *b2, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcmul");
	BATcheck(b2, "BATcalcmul");

	if (checkbats(b1, b2, "BATcalcmul") == GDK_FAIL)
		return NULL;

	if (accum == 1) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		assert(b1->T->type == tp);
		bn = b1;
		BBPfix(b1->batCacheid);
	} else if (accum == 2) {
		assert(BBP_refs(b2->batCacheid) == 1);
		if (BBP_lrefs(b2->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b2));
		assert(b2->T->type == tp);
		bn = b2;
		BBPfix(b2->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, tp, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = mul_typeswitchloop(Tloc(b1, b1->U->first), b1->T->type, 1,
				  Tloc(b2, b2->U->first), b2->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b1->U->count, abort_on_error, "BATcalcmul");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcmulcst(BAT *b, const ValRecord *v, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcmulcst");

	if (checkbats(b, NULL, "BATcalcmulcst") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = mul_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalcmulcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted, or reverse sorted if the constant is negative */
	if (abort_on_error) {
		ValRecord sign;
		int savesorted;

		savesorted = b->T->sorted; /* in case b == bn (accum set) */
		VARcalcsign(&sign, v);
		bn->T->sorted = (sign.val.btval >= 0 && b->T->sorted) ||
			(sign.val.btval <= 0 && b->T->revsorted && nils == 0) ||
			bn->U->count <= 1 || nils == bn->U->count;
		bn->T->revsorted = (sign.val.btval >= 0 && b->T->revsorted) ||
			(sign.val.btval <= 0 && savesorted && nils == 0) ||
			bn->U->count <= 1 || nils == bn->U->count;
	} else {
		bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
		bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	}
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstmul(const ValRecord *v, BAT *b, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstmul");

	if (checkbats(b, NULL, "BATcalccstmul") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = mul_typeswitchloop(VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(b, b->U->first), b->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalccstmul");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no overflow occurred (we only
	 * know for sure if abort_on_error is set), the result is also
	 * sorted, or reverse sorted if the constant is negative */
	if (abort_on_error) {
		ValRecord sign;
		int savesorted;

		savesorted = b->T->sorted; /* in case b == bn (accum set) */
		VARcalcsign(&sign, v);
		bn->T->sorted = (sign.val.btval >= 0 && b->T->sorted) ||
			(sign.val.btval <= 0 && b->T->revsorted && nils == 0) ||
			bn->U->count <= 1 || nils == bn->U->count;
		bn->T->revsorted = (sign.val.btval >= 0 && b->T->revsorted) ||
			(sign.val.btval <= 0 && savesorted && nils == 0) ||
			bn->U->count <= 1 || nils == bn->U->count;
	} else {
		bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
		bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	}
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcmul(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	if (mul_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0,
			       (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0,
			       VALptr(ret), ret->vtype, 1,
			       abort_on_error, "VARcalcmul") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define DIV_3TYPE(TYPE1, TYPE2, TYPE3)					\
	static BUN							\
	div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt, int abort_on_error) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] == 0) {			\
				if (abort_on_error)			\
					return BUN_NONE;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] / rgt[j];	\
			}						\
		}							\
		return nils;						\
	}

#define DIV_3TYPE_float(TYPE1, TYPE2, TYPE3)				\
	static BUN							\
	div_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt, int abort_on_error) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] == 0 ||			\
				   (ABSOLUTE(rgt[j]) < 1 &&		\
				    GDK_##TYPE3##_max * ABSOLUTE(rgt[j]) < lft[i])) { \
				/* only check for overflow, not for	\
				 * underflow */				\
				if (abort_on_error)			\
					return BUN_NONE + (rgt[j] != 0); \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] / rgt[j];	\
			}						\
		}							\
		return nils;						\
	}

DIV_3TYPE(bte, bte, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, bte, sht)
DIV_3TYPE(bte, bte, int)
DIV_3TYPE(bte, bte, lng)
#endif
DIV_3TYPE(bte, bte, flt)
DIV_3TYPE(bte, bte, dbl)
DIV_3TYPE(bte, sht, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, sht, sht)
DIV_3TYPE(bte, sht, int)
DIV_3TYPE(bte, sht, lng)
#endif
DIV_3TYPE(bte, sht, flt)
DIV_3TYPE(bte, sht, dbl)
DIV_3TYPE(bte, int, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, int, sht)
DIV_3TYPE(bte, int, int)
DIV_3TYPE(bte, int, lng)
#endif
DIV_3TYPE(bte, int, flt)
DIV_3TYPE(bte, int, dbl)
DIV_3TYPE(bte, lng, bte)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(bte, lng, sht)
DIV_3TYPE(bte, lng, int)
DIV_3TYPE(bte, lng, lng)
#endif
DIV_3TYPE(bte, lng, flt)
DIV_3TYPE(bte, lng, dbl)
DIV_3TYPE_float(bte, flt, flt)
DIV_3TYPE_float(bte, flt, dbl)
DIV_3TYPE_float(bte, dbl, dbl)
DIV_3TYPE(sht, bte, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, bte, int)
DIV_3TYPE(sht, bte, lng)
#endif
DIV_3TYPE(sht, bte, flt)
DIV_3TYPE(sht, bte, dbl)
DIV_3TYPE(sht, sht, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, sht, int)
DIV_3TYPE(sht, sht, lng)
#endif
DIV_3TYPE(sht, sht, flt)
DIV_3TYPE(sht, sht, dbl)
DIV_3TYPE(sht, int, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, int, int)
DIV_3TYPE(sht, int, lng)
#endif
DIV_3TYPE(sht, int, flt)
DIV_3TYPE(sht, int, dbl)
DIV_3TYPE(sht, lng, sht)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(sht, lng, int)
DIV_3TYPE(sht, lng, lng)
#endif
DIV_3TYPE(sht, lng, flt)
DIV_3TYPE(sht, lng, dbl)
DIV_3TYPE_float(sht, flt, flt)
DIV_3TYPE_float(sht, flt, dbl)
DIV_3TYPE_float(sht, dbl, dbl)
DIV_3TYPE(int, bte, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, bte, lng)
#endif
DIV_3TYPE(int, bte, flt)
DIV_3TYPE(int, bte, dbl)
DIV_3TYPE(int, sht, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, sht, lng)
#endif
DIV_3TYPE(int, sht, flt)
DIV_3TYPE(int, sht, dbl)
DIV_3TYPE(int, int, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, int, lng)
#endif
DIV_3TYPE(int, int, flt)
DIV_3TYPE(int, int, dbl)
DIV_3TYPE(int, lng, int)
#ifdef FULL_IMPLEMENTATION
DIV_3TYPE(int, lng, lng)
#endif
DIV_3TYPE(int, lng, flt)
DIV_3TYPE(int, lng, dbl)
DIV_3TYPE_float(int, flt, flt)
DIV_3TYPE_float(int, flt, dbl)
DIV_3TYPE_float(int, dbl, dbl)
DIV_3TYPE(lng, bte, lng)
DIV_3TYPE(lng, bte, flt)
DIV_3TYPE(lng, bte, dbl)
DIV_3TYPE(lng, sht, lng)
DIV_3TYPE(lng, sht, flt)
DIV_3TYPE(lng, sht, dbl)
DIV_3TYPE(lng, int, lng)
DIV_3TYPE(lng, int, flt)
DIV_3TYPE(lng, int, dbl)
DIV_3TYPE(lng, lng, lng)
DIV_3TYPE(lng, lng, flt)
DIV_3TYPE(lng, lng, dbl)
DIV_3TYPE_float(lng, flt, flt)
DIV_3TYPE_float(lng, flt, dbl)
DIV_3TYPE_float(lng, dbl, dbl)
DIV_3TYPE(flt, bte, flt)
DIV_3TYPE(flt, bte, dbl)
DIV_3TYPE(flt, sht, flt)
DIV_3TYPE(flt, sht, dbl)
DIV_3TYPE(flt, int, flt)
DIV_3TYPE(flt, int, dbl)
DIV_3TYPE(flt, lng, flt)
DIV_3TYPE(flt, lng, dbl)
DIV_3TYPE_float(flt, flt, flt)
DIV_3TYPE_float(flt, flt, dbl)
DIV_3TYPE_float(flt, dbl, dbl)
DIV_3TYPE(dbl, bte, dbl)
DIV_3TYPE(dbl, sht, dbl)
DIV_3TYPE(dbl, int, dbl)
DIV_3TYPE(dbl, lng, dbl)
DIV_3TYPE_float(dbl, flt, dbl)
DIV_3TYPE_float(dbl, dbl, dbl)

static BUN
div_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *dst, int tp, BUN cnt,
		   int abort_on_error, const char *func)
{
	BUN nils;

	switch (ATOMstorage(tp1)) {
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = div_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = div_bte_sht_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = div_bte_int_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_int_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = div_bte_lng_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = div_bte_lng_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = div_bte_lng_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_bte_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_bte_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_bte_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_bte_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = div_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = div_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = div_sht_int_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = div_sht_lng_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = div_sht_lng_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = div_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_sht_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_sht_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_sht_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_sht_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = div_int_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = div_int_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = div_int_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = div_int_lng_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = div_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			case TYPE_flt:
				nils = div_int_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_int_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_int_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_int_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = div_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_lng_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = div_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_lng_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = div_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_lng_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = div_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_flt:
				nils = div_lng_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_lng_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_lng_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_lng_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_flt_bte_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_flt_sht_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_flt_int_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_flt_lng_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_flt:
				nils = div_flt_flt_flt(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_dbl:
				nils = div_flt_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_flt_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_dbl_bte_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_dbl_sht_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_dbl_int_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_dbl_lng_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_flt:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_dbl_flt_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			default:
				goto unsupported;
			}
			break;
		case TYPE_dbl:
			switch (ATOMstorage(tp)) {
			case TYPE_dbl:
				nils = div_dbl_dbl_dbl(lft, incr1, rgt, incr2,
						       dst, cnt,
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

	if (nils == BUN_NONE)
		GDKerror("22012!division by zero.\n");
	else if (nils == BUN_NONE + 1)
		GDKerror("22003!overflow in calculation.\n");

	return nils;

  unsupported:
	GDKerror("%s: type combination (div(%s,%s)->%s) not supported.\n", func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcdiv(BAT *b1, BAT *b2, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcdiv");
	BATcheck(b2, "BATcalcdiv");

	if (checkbats(b1, b2, "BATcalcdiv") == GDK_FAIL)
		return NULL;

	if (accum == 1) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		assert(b1->T->type == tp);
		bn = b1;
		BBPfix(b1->batCacheid);
	} else if (accum == 2) {
		assert(BBP_refs(b2->batCacheid) == 1);
		if (BBP_lrefs(b2->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b2));
		assert(b2->T->type == tp);
		bn = b2;
		BBPfix(b2->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, tp, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = div_typeswitchloop(Tloc(b1, b1->U->first), b1->T->type, 1,
				  Tloc(b2, b2->U->first), b2->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b1->U->count, abort_on_error, "BATcalcdiv");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcdivcst(BAT *b, const ValRecord *v, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcdivcst");

	if (checkbats(b, NULL, "BATcalcdivcst") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = div_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalcdivcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	/* if the input is sorted, and no zero division occurred (we
	 * only know for sure if abort_on_error is set), the result is
	 * also sorted, or reverse sorted if the constant is
	 * negative */
	if (abort_on_error) {
		ValRecord sign;
		int savesorted;

		savesorted = b->T->sorted; /* in case b == bn (accum set) */
		VARcalcsign(&sign, v);
		bn->T->sorted = (sign.val.btval > 0 && b->T->sorted) ||
			(sign.val.btval < 0 && b->T->revsorted && nils == 0) ||
			bn->U->count <= 1 || nils == bn->U->count;
		bn->T->revsorted = (sign.val.btval > 0 && b->T->revsorted) ||
			(sign.val.btval < 0 && savesorted && nils == 0) ||
			bn->U->count <= 1 || nils == bn->U->count;
	} else {
		bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
		bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	}
	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstdiv(const ValRecord *v, BAT *b, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstdiv");

	if (checkbats(b, NULL, "BATcalccstdiv") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = div_typeswitchloop(VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(b, b->U->first), b->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalccstdiv");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcdiv(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	if (div_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0,
			       (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0,
			       VALptr(ret), ret->vtype, 1,
			       abort_on_error, "VARcalcdiv") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define MOD_3TYPE(TYPE1, TYPE2, TYPE3)					\
	static BUN							\
	mod_##TYPE1##_##TYPE2##_##TYPE3(const TYPE1 *lft, int incr1, const TYPE2 *rgt, int incr2, TYPE3 *dst, BUN cnt, int abort_on_error) \
	{								\
		BUN i, j, k;						\
		BUN nils = 0;						\
									\
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) { \
			if (lft[i] == TYPE1##_nil || rgt[j] == TYPE2##_nil) { \
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else if (rgt[j] == 0) {			\
				if (abort_on_error)			\
					return BUN_NONE;		\
				dst[k] = TYPE3##_nil;			\
				nils++;					\
			} else {					\
				dst[k] = (TYPE3) lft[i] % rgt[j];	\
			}						\
		}							\
		return nils;						\
	}

MOD_3TYPE(bte, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, bte, sht)
MOD_3TYPE(bte, bte, int)
MOD_3TYPE(bte, bte, lng)
#endif
MOD_3TYPE(bte, sht, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, sht, sht)
MOD_3TYPE(bte, sht, int)
MOD_3TYPE(bte, sht, lng)
#endif
MOD_3TYPE(bte, int, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, int, sht)
MOD_3TYPE(bte, int, int)
MOD_3TYPE(bte, int, lng)
#endif
MOD_3TYPE(bte, lng, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(bte, lng, sht)
MOD_3TYPE(bte, lng, int)
MOD_3TYPE(bte, lng, lng)
#endif
MOD_3TYPE(sht, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, bte, sht)
MOD_3TYPE(sht, bte, int)
MOD_3TYPE(sht, bte, lng)
#endif
MOD_3TYPE(sht, sht, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, sht, int)
MOD_3TYPE(sht, sht, lng)
#endif
MOD_3TYPE(sht, int, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, int, int)
MOD_3TYPE(sht, int, lng)
#endif
MOD_3TYPE(sht, lng, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(sht, lng, int)
MOD_3TYPE(sht, lng, lng)
#endif
MOD_3TYPE(int, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, bte, sht)
MOD_3TYPE(int, bte, int)
MOD_3TYPE(int, bte, lng)
#endif
MOD_3TYPE(int, sht, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, sht, int)
MOD_3TYPE(int, sht, lng)
#endif
MOD_3TYPE(int, int, int)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, int, lng)
#endif
MOD_3TYPE(int, lng, int)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(int, lng, lng)
#endif
MOD_3TYPE(lng, bte, bte)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(lng, bte, sht)
MOD_3TYPE(lng, bte, int)
MOD_3TYPE(lng, bte, lng)
#endif
MOD_3TYPE(lng, sht, sht)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(lng, sht, int)
MOD_3TYPE(lng, sht, lng)
#endif
MOD_3TYPE(lng, int, int)
#ifdef FULL_IMPLEMENTATION
MOD_3TYPE(lng, int, lng)
#endif
MOD_3TYPE(lng, lng, lng)

static BUN
mod_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *dst, int tp, BUN cnt,
		   int abort_on_error, const char *func)
{
	BUN nils;

	switch (ATOMstorage(tp1)) {
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = mod_bte_bte_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = mod_bte_sht_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = mod_bte_int_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_int_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = mod_bte_lng_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_bte_lng_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_bte_lng_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_bte_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = mod_sht_bte_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_sht_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_sht_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = mod_sht_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = mod_sht_int_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = mod_sht_lng_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_sht_lng_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_sht_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = mod_int_bte_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_int_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_int_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = mod_int_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_int_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_int_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = mod_int_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_int_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = mod_int_lng_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_int_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			switch (ATOMstorage(tp)) {
			case TYPE_bte:
				nils = mod_lng_bte_bte(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_sht:
				nils = mod_lng_bte_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_int:
				nils = mod_lng_bte_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_bte_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_sht:
			switch (ATOMstorage(tp)) {
			case TYPE_sht:
				nils = mod_lng_sht_sht(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_int:
				nils = mod_lng_sht_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
			case TYPE_lng:
				nils = mod_lng_sht_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_int:
			switch (ATOMstorage(tp)) {
			case TYPE_int:
				nils = mod_lng_int_int(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#ifdef FULL_IMPLEMENTATION
			case TYPE_lng:
				nils = mod_lng_int_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
						       abort_on_error);
				break;
#endif
			default:
				goto unsupported;
			}
			break;
		case TYPE_lng:
			switch (ATOMstorage(tp)) {
			case TYPE_lng:
				nils = mod_lng_lng_lng(lft, incr1, rgt, incr2,
						       dst, cnt,
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

	if (nils == BUN_NONE)
		GDKerror("22012!division by zero.\n");

	return nils;

  unsupported:
	GDKerror("%s: type combination (mod(%s,%s)->%s) not supported.\n", func, ATOMname(tp1), ATOMname(tp2), ATOMname(tp));
	return BUN_NONE;
}

BAT *
BATcalcmod(BAT *b1, BAT *b2, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcmod");
	BATcheck(b2, "BATcalcmod");

	if (checkbats(b1, b2, "BATcalcmod") == GDK_FAIL)
		return NULL;

	if (accum == 1) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		assert(b1->T->type == tp);
		bn = b1;
		BBPfix(b1->batCacheid);
	} else if (accum == 2) {
		assert(BBP_refs(b2->batCacheid) == 1);
		if (BBP_lrefs(b2->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b2));
		assert(b2->T->type == tp);
		bn = b2;
		BBPfix(b2->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, tp, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = mod_typeswitchloop(Tloc(b1, b1->U->first), b1->T->type, 1,
				  Tloc(b2, b2->U->first), b2->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b1->U->count, abort_on_error, "BATcalcmod");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcmodcst(BAT *b, const ValRecord *v, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcmodcst");

	if (checkbats(b, NULL, "BATcalcmodcst") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = mod_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalcmodcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstmod(const ValRecord *v, BAT *b, int tp, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstmod");

	if (checkbats(b, NULL, "BATcalccstmod") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		assert(b->T->type == tp);
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, tp, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = mod_typeswitchloop(VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(b, b->U->first), b->T->type, 1,
				  Tloc(bn, bn->U->first), tp,
				  b->U->count, abort_on_error, "BATcalccstmod");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcmod(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	if (mod_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0,
			       (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0,
			       VALptr(ret), ret->vtype, 1,
			       abort_on_error, "VARcalcmod") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define XOR(a, b)	((a) ^ (b))
#define XORBIT(a, b)	(((a) == 0) != ((b) == 0))

static BUN
xor_typeswitchloop(const void *lft, int incr1,
		   const void *rgt, int incr2,
		   void *dst, int tp, BUN cnt, int nonil,
		   const char *func)
{
	BUN nils = 0;
	BUN i, j, k;

	switch (ATOMstorage(tp)) {
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
	default:
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return BUN_NONE;
	}

	return nils;
}

BAT *
BATcalcxor(BAT *b1, BAT *b2, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcxor");
	BATcheck(b2, "BATcalcxor");

	if (checkbats(b1, b2, "BATcalcxor") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b1->T->type) != ATOMstorage(b2->T->type)) {
		GDKerror("BATcalcxor: incompatible input types.\n");
		return NULL;
	}

	if (accum == 1) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		bn = b1;
		BBPfix(b1->batCacheid);
	} else if (accum == 2) {
		assert(BBP_refs(b2->batCacheid) == 1);
		if (BBP_lrefs(b2->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b2));
		bn = b2;
		BBPfix(b2->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b1->T->type, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = xor_typeswitchloop(Tloc(b1, b1->U->first), 1,
				  Tloc(b2, b2->U->first), 1,
				  Tloc(bn, bn->U->first),
				  b1->T->type, b1->U->count,
				  b1->T->nonil && b2->T->nonil,
				  "BATcalcxor");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcxorcst(BAT *b, const ValRecord *v, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcxorcst");

	if (checkbats(b, NULL, "BATcalcxorcst") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(v->vtype)) {
		GDKerror("BATcalcxorcst: incompatible input types.\n");
		return NULL;
	}

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = xor_typeswitchloop(Tloc(b, b->U->first), 1,
				  VALptr((ValPtr) v), 0,
				  Tloc(bn, bn->U->first), b->T->type,
				  b->U->count,
				  b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
				  "BATcalcxorcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstxor(const ValRecord *v, BAT *b, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstxor");

	if (checkbats(b, NULL, "BATcalccstxor") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(v->vtype)) {
		GDKerror("BATcalccstxor: incompatible input types.\n");
		return NULL;
	}

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = xor_typeswitchloop(VALptr((ValPtr) v), 0,
				  Tloc(b, b->U->first), 1,
				  Tloc(bn, bn->U->first), b->T->type,
				  b->U->count,
				  b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
				  "BATcalccstxor");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcxor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMstorage(lft->vtype) != ATOMstorage(rgt->vtype)) {
		GDKerror("VARcalccstxor: incompatible input types.\n");
		return GDK_FAIL;
	}

	if (xor_typeswitchloop(VALptr((ValPtr) lft), 0,
			       VALptr((ValPtr) rgt), 0,
			       VALptr(ret), lft->vtype, 1, 0,
			       "VARcalcxor") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define OR(a, b)	((a) | (b))

static BUN
or_typeswitchloop(const void *lft, int incr1,
		  const void *rgt, int incr2,
		  void *dst, int tp, BUN cnt, int nonil,
		  const char *func)
{
	BUN nils = 0;
	BUN i, j, k;

	switch (ATOMstorage(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			/* implement tri-Boolean algebra */
			for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++
) {
				/* note that any value not equal to 0
				 * and not equal to bit_nil (0x80) is
				 * considered true */
				if (((const bit *) lft)[i] & 0x7F ||
				    ((const bit *) rgt)[j] & 0x7F) {
					/* either one is true */
					((bit *) dst)[k] = 1;
				} else if (((const bit *) lft)[i] == 0 &&
					   ((const bit *) rgt)[j] == 0) {
					/* both are false */
					((bit *) dst)[k] = 0;
				} else {
					((bit *) dst)[k] = bit_nil;
					nils++;
				}
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
	default:
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return BUN_NONE;
	}

	return nils;
}

BAT *
BATcalcor(BAT *b1, BAT *b2, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcor");
	BATcheck(b2, "BATcalcor");

	if (checkbats(b1, b2, "BATcalcor") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b1->T->type) != ATOMstorage(b2->T->type)) {
		GDKerror("BATcalcor: incompatible input types.\n");
		return NULL;
	}

	if (accum == 1) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		bn = b1;
		BBPfix(b1->batCacheid);
	} else if (accum == 2) {
		assert(BBP_refs(b2->batCacheid) == 1);
		if (BBP_lrefs(b2->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b2));
		bn = b2;
		BBPfix(b2->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b1->T->type, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = or_typeswitchloop(Tloc(b1, b1->U->first), 1,
				 Tloc(b2, b2->U->first), 1,
				 Tloc(bn, bn->U->first),
				 b1->T->type, b1->U->count,
				 b1->T->nonil && b2->T->nonil,
				 "BATcalcor");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcorcst(BAT *b, const ValRecord *v, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcorcst");

	if (checkbats(b, NULL, "BATcalcorcst") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(v->vtype)) {
		GDKerror("BATcalcorcst: incompatible input types.\n");
		return NULL;
	}

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = or_typeswitchloop(Tloc(b, b->U->first), 1,
				 VALptr((ValPtr) v), 0,
				 Tloc(bn, bn->U->first), b->T->type,
				 b->U->count,
				 b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
				 "BATcalcorcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstor(const ValRecord *v, BAT *b, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstor");

	if (checkbats(b, NULL, "BATcalccstor") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(v->vtype)) {
		GDKerror("BATcalccstor: incompatible input types.\n");
		return NULL;
	}

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = or_typeswitchloop(VALptr((ValPtr) v), 0,
				 Tloc(b, b->U->first), 1,
				 Tloc(bn, bn->U->first), b->T->type,
				 b->U->count,
				 b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
				 "BATcalccstor");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMstorage(lft->vtype) != ATOMstorage(rgt->vtype)) {
		GDKerror("VARcalccstor: incompatible input types.\n");
		return GDK_FAIL;
	}

	if (or_typeswitchloop(VALptr((ValPtr) lft), 0,
			      VALptr((ValPtr) rgt), 0,
			      VALptr(ret), lft->vtype, 1, 0,
			      "VARcalcor") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define AND(a, b)	((a) & (b))

static BUN
and_typeswitchloop(const void *lft, int incr1,
		   const void *rgt, int incr2,
		   void *dst, int tp, BUN cnt, int nonil,
		   const char *func)
{
	BUN nils = 0;
	BUN i, j, k;

	switch (ATOMstorage(tp)) {
	case TYPE_bte:
		if (tp == TYPE_bit) {
			/* implement tri-Boolean algebra */
			for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) {
				if (((const bit *) lft)[i] == 0 ||
				    ((const bit *) rgt)[j] == 0) {
					/* either one is false */
					((bit *) dst)[k] = 0;
				} else if (((const bit *) lft)[i] != bit_nil &&
					   ((const bit *) rgt)[j] != bit_nil) {
					/* both are true */
					((bit *) dst)[k] = 1;
				} else {
					((bit *) dst)[k] = bit_nil;
					nils++;
				}
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
	default:
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return BUN_NONE;
	}

	return nils;
}

BAT *
BATcalcand(BAT *b1, BAT *b2, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcand");
	BATcheck(b2, "BATcalcand");

	if (checkbats(b1, b2, "BATcalcand") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b1->T->type) != ATOMstorage(b2->T->type)) {
		GDKerror("BATcalcand: incompatible input types.\n");
		return NULL;
	}

	if (accum == 1) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		bn = b1;
		BBPfix(b1->batCacheid);
	} else if (accum == 2) {
		assert(BBP_refs(b2->batCacheid) == 1);
		if (BBP_lrefs(b2->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b2));
		bn = b2;
		BBPfix(b2->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b1->T->type, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = and_typeswitchloop(Tloc(b1, b1->U->first), 1,
				  Tloc(b2, b2->U->first), 1,
				  Tloc(bn, bn->U->first),
				  b1->T->type, b1->U->count,
				  b1->T->nonil && b2->T->nonil,
				  "BATcalcand");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcandcst(BAT *b, const ValRecord *v, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcandcst");

	if (checkbats(b, NULL, "BATcalcandcst") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(v->vtype)) {
		GDKerror("BATcalcandcst: incompatible input types.\n");
		return NULL;
	}

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = and_typeswitchloop(Tloc(b, b->U->first), 1,
				  VALptr((ValPtr) v), 0,
				  Tloc(bn, bn->U->first), b->T->type,
				  b->U->count,
				  b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
				  "BATcalcandcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstand(const ValRecord *v, BAT *b, int accum)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstand");

	if (checkbats(b, NULL, "BATcalccstand") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(v->vtype)) {
		GDKerror("BATcalccstand: incompatible input types.\n");
		return NULL;
	}

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = and_typeswitchloop(VALptr((ValPtr) v), 0,
				  Tloc(b, b->U->first), 1,
				  Tloc(bn, bn->U->first), b->T->type,
				  b->U->count,
				  b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
				  "BATcalccstand");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcand(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	if (ATOMstorage(lft->vtype) != ATOMstorage(rgt->vtype)) {
		GDKerror("VARcalccstand: incompatible input types.\n");
		return GDK_FAIL;
	}

	if (and_typeswitchloop(VALptr((ValPtr) lft), 0,
			       VALptr((ValPtr) rgt), 0,
			       VALptr(ret), lft->vtype, 1, 0,
			       "VARcalcand") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define LSH(a, b)		((a) << (b))

#define SHIFT_CHECK(a, b)	((b) < 0 || (b) >= 8 * (int) sizeof(a))

static BUN
lsh_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *dst, BUN cnt,
		   int abort_on_error, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;

	switch (ATOMstorage(tp1)) {
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(bte, bte, bte, LSH, SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(bte, sht, bte, LSH, SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(bte, int, bte, LSH, SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(bte, lng, bte, LSH, SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(sht, bte, sht, LSH, SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(sht, sht, sht, LSH, SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(sht, int, sht, LSH, SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(sht, lng, sht, LSH, SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(int, bte, int, LSH, SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(int, sht, int, LSH, SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(int, int, int, LSH, SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(int, lng, int, LSH, SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(lng, bte, lng, LSH, SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(lng, sht, lng, LSH, SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(lng, int, lng, LSH, SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(lng, lng, lng, LSH, SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  checkfail:
	GDKerror("%s: shift operand too large.\n", func);
	return BUN_NONE;
  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

BAT *
BATcalclsh(BAT *b1, BAT *b2, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalclsh");
	BATcheck(b2, "BATcalclsh");

	if (checkbats(b1, b2, "BATcalclsh") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		bn = b1;
		BBPfix(b1->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b1->T->type, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = lsh_typeswitchloop(Tloc(b1, b1->U->first), b1->T->type, 1,
				  Tloc(b2, b2->U->first), b2->T->type, 1,
				  Tloc(bn, bn->U->first),
				  b1->U->count, abort_on_error,
				  "BATcalclsh");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalclshcst(BAT *b, const ValRecord *v, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalclshcst");

	if (checkbats(b, NULL, "BATcalclshcst") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = lsh_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(bn, bn->U->first),
				  b->U->count, abort_on_error,
				  "BATcalclshcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstlsh(const ValRecord *v, BAT *b, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstlsh");

	if (checkbats(b, NULL, "BATcalccstlsh") == GDK_FAIL)
		return NULL;

	bn = BATnew(TYPE_void, v->vtype, b->U->count);
	if (bn == NULL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = lsh_typeswitchloop(VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(b, b->U->first), b->T->type, 1,
				  Tloc(bn, bn->U->first),
				  b->U->count, abort_on_error,
				  "BATcalccstlsh");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalclsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	ret->vtype = lft->vtype;
	if (lsh_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0,
			       (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0,
			       VALptr(ret), 1,
			       abort_on_error, "VARcalclsh") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define RSH(a, b)	((a) >> (b))

static BUN
rsh_typeswitchloop(const void *lft, int tp1, int incr1,
		   const void *rgt, int tp2, int incr2,
		   void *dst, BUN cnt,
		   int abort_on_error, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;

	switch (ATOMstorage(tp1)) {
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(bte, bte, bte, RSH, SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(bte, sht, bte, RSH, SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(bte, int, bte, RSH, SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(bte, lng, bte, RSH, SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(sht, bte, sht, RSH, SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(sht, sht, sht, RSH, SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(sht, int, sht, RSH, SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(sht, lng, sht, RSH, SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(int, bte, int, RSH, SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(int, sht, int, RSH, SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(int, int, int, RSH, SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(int, lng, int, RSH, SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC_CHECK(lng, bte, lng, RSH, SHIFT_CHECK);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC_CHECK(lng, sht, lng, RSH, SHIFT_CHECK);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC_CHECK(lng, int, lng, RSH, SHIFT_CHECK);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC_CHECK(lng, lng, lng, RSH, SHIFT_CHECK);
			break;
		default:
			goto unsupported;
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  checkfail:
	GDKerror("%s: shift operand too large.\n", func);
	return BUN_NONE;
  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

BAT *
BATcalcrsh(BAT *b1, BAT *b2, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b1, "BATcalcrsh");
	BATcheck(b2, "BATcalcrsh");

	if (checkbats(b1, b2, "BATcalcrsh") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b1->batCacheid) == 1);
		if (BBP_lrefs(b1->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b1));
		bn = b1;
		BBPfix(b1->batCacheid);
	} else {
		assert(accum == 0);
		bn = BATnew(TYPE_void, b1->T->type, b1->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	nils = rsh_typeswitchloop(Tloc(b1, b1->U->first), b1->T->type, 1,
				  Tloc(b2, b2->U->first), b2->T->type, 1,
				  Tloc(bn, bn->U->first),
				  b1->U->count, abort_on_error,
				  "BATcalcrsh");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b1->U->count);
	bn = BATseqbase(bn, b1->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcrshcst(BAT *b, const ValRecord *v, int accum, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalcrshcst");

	if (checkbats(b, NULL, "BATcalcrshcst") == GDK_FAIL)
		return NULL;

	if (accum) {
		assert(BBP_refs(b->batCacheid) == 1);
		if (BBP_lrefs(b->batCacheid) > 1) {
			GDKerror("logical reference too high to be used as accumulator\n");
			return NULL;
		}
		assert(!isVIEW(b));
		bn = b;
		BBPfix(b->batCacheid);
	} else {
		bn = BATnew(TYPE_void, b->T->type, b->U->count);
		if (bn == NULL)
			return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = rsh_typeswitchloop(Tloc(b, b->U->first), b->T->type, 1,
				  VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(bn, bn->U->first),
				  b->U->count, abort_on_error,
				  "BATcalcrshcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (!accum && b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstrsh(const ValRecord *v, BAT *b, int abort_on_error)
{
	BAT *bn;
	BUN nils;

	BATcheck(b, "BATcalccstrsh");

	if (checkbats(b, NULL, "BATcalccstrsh") == GDK_FAIL)
		return NULL;

	bn = BATnew(TYPE_void, v->vtype, b->U->count);
	if (bn == NULL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	nils = rsh_typeswitchloop(VALptr((ValPtr) v), v->vtype, 0,
				  Tloc(b, b->U->first), b->T->type, 1,
				  Tloc(bn, bn->U->first),
				  b->U->count, abort_on_error,
				  "BATcalccstrsh");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcrsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error)
{
	ret->vtype = lft->vtype;
	if (rsh_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0,
			       (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0,
			       VALptr(ret), 1,
			       abort_on_error, "VARcalcrsh") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define LT(a, b)	((bit) ((a) < (b)))

static BUN
lt_typeswitchloop(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		  const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		  bit *dst, BUN cnt, int nonil, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;
	const char *s1, *s2;

	/* bit and oid can only be compared with each other */
	if ((tp1 == TYPE_bit) != (tp2 == TYPE_bit))
		goto unsupported;
	if ((tp1 == TYPE_oid || tp1 == TYPE_void) != (tp2 == TYPE_oid || tp2 == TYPE_void))
		goto unsupported;

	switch (ATOMstorage(tp1)) {
	case TYPE_void: {
		oid v;

		assert(incr1 == 1);
		assert(tp2 == TYPE_oid || incr2 == 1); /* if void, incr2==1 */
		v = * (const oid *) lft;
		if (v == oid_nil || tp2 == TYPE_void) {
			bit res = v == oid_nil || * (const oid *) rgt == oid_nil ?
				bit_nil :
				LT(v, * (const oid *) rgt);

			for (k = 0; k < cnt; k++)
				dst[k] = res;
			if (res == bit_nil)
				nils = cnt;
		} else {
			for (j = k = 0; k < cnt; v++, j += incr2, k++) {
				if (((const oid *) rgt)[j] == oid_nil) {
					nils++;
					dst[k] = bit_nil;
				} else {
					dst[k] = LT(v, ((const oid *) rgt)[j]);
				}
			}
		}
		break;
	}
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bit, LT);
			else
				BINARY_3TYPE_FUNC(bte, bte, bit, LT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, sht, bit, LT);
			else
				BINARY_3TYPE_FUNC(bte, sht, bit, LT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, int, bit, LT);
			else
				BINARY_3TYPE_FUNC(bte, int, bit, LT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, lng, bit, LT);
			else
				BINARY_3TYPE_FUNC(bte, lng, bit, LT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, flt, bit, LT);
			else
				BINARY_3TYPE_FUNC(bte, flt, bit, LT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, dbl, bit, LT);
			else
				BINARY_3TYPE_FUNC(bte, dbl, bit, LT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, bte, bit, LT);
			else
				BINARY_3TYPE_FUNC(sht, bte, bit, LT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, sht, bit, LT);
			else
				BINARY_3TYPE_FUNC(sht, sht, bit, LT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, int, bit, LT);
			else
				BINARY_3TYPE_FUNC(sht, int, bit, LT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, lng, bit, LT);
			else
				BINARY_3TYPE_FUNC(sht, lng, bit, LT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, flt, bit, LT);
			else
				BINARY_3TYPE_FUNC(sht, flt, bit, LT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, dbl, bit, LT);
			else
				BINARY_3TYPE_FUNC(sht, dbl, bit, LT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = LT(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, bte, bit, LT);
			else
				BINARY_3TYPE_FUNC(int, bte, bit, LT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, sht, bit, LT);
			else
				BINARY_3TYPE_FUNC(int, sht, bit, LT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, int, bit, LT);
			else
				BINARY_3TYPE_FUNC(int, int, bit, LT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, lng, bit, LT);
			else
				BINARY_3TYPE_FUNC(int, lng, bit, LT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, flt, bit, LT);
			else
				BINARY_3TYPE_FUNC(int, flt, bit, LT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, dbl, bit, LT);
			else
				BINARY_3TYPE_FUNC(int, dbl, bit, LT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = LT(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, bte, bit, LT);
			else
				BINARY_3TYPE_FUNC(lng, bte, bit, LT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, sht, bit, LT);
			else
				BINARY_3TYPE_FUNC(lng, sht, bit, LT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, int, bit, LT);
			else
				BINARY_3TYPE_FUNC(lng, int, bit, LT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, lng, bit, LT);
			else
				BINARY_3TYPE_FUNC(lng, lng, bit, LT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, flt, bit, LT);
			else
				BINARY_3TYPE_FUNC(lng, flt, bit, LT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, dbl, bit, LT);
			else
				BINARY_3TYPE_FUNC(lng, dbl, bit, LT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, bte, bit, LT);
			else
				BINARY_3TYPE_FUNC(flt, bte, bit, LT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, sht, bit, LT);
			else
				BINARY_3TYPE_FUNC(flt, sht, bit, LT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, int, bit, LT);
			else
				BINARY_3TYPE_FUNC(flt, int, bit, LT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, lng, bit, LT);
			else
				BINARY_3TYPE_FUNC(flt, lng, bit, LT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, flt, bit, LT);
			else
				BINARY_3TYPE_FUNC(flt, flt, bit, LT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, dbl, bit, LT);
			else
				BINARY_3TYPE_FUNC(flt, dbl, bit, LT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, bte, bit, LT);
			else
				BINARY_3TYPE_FUNC(dbl, bte, bit, LT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, sht, bit, LT);
			else
				BINARY_3TYPE_FUNC(dbl, sht, bit, LT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, int, bit, LT);
			else
				BINARY_3TYPE_FUNC(dbl, int, bit, LT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, lng, bit, LT);
			else
				BINARY_3TYPE_FUNC(dbl, lng, bit, LT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, flt, bit, LT);
			else
				BINARY_3TYPE_FUNC(dbl, flt, bit, LT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, dbl, bit, LT);
			else
				BINARY_3TYPE_FUNC(dbl, dbl, bit, LT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		if (tp1 != tp2)
			goto unsupported;
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) {
			s1 = hp1 ? hp1 + VarHeapVal(lft, i, wd1) : (const char *) lft;
			s2 = hp2 ? hp2 + VarHeapVal(rgt, j, wd2) : (const char *) rgt;
			if (s1 == NULL || strcmp(s1, str_nil) == 0 ||
			    s2 == NULL || strcmp(s2, str_nil) == 0) {
				nils++;
				dst[k] = bit_nil;
			} else {
				dst[k] = (bit) (strcmp(s1, s2) < 0);
			}
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

static BAT *
BATcalclt_intern(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		 const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		 BUN cnt, int nonil, oid seqbase, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	bit *dst;

	bn = BATnew(TYPE_void, TYPE_bit, cnt);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, bn->U->first);

	nils = lt_typeswitchloop(lft, tp1, incr1, hp1, wd1,
				 rgt, tp2, incr2, hp2, wd2,
				 dst, cnt, nonil, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn = BATseqbase(bn, seqbase);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	return bn;
}

BAT *
BATcalclt(BAT *b1, BAT *b2)
{
	BAT *bn;

	BATcheck(b1, "BATcalclt");
	BATcheck(b2, "BATcalclt");

	if (checkbats(b1, b2, "BATcalclt") == GDK_FAIL)
		return NULL;

	if (BATtvoid(b1) && BATtvoid(b2)) {
		bit res;

		if (b1->T->seq == oid_nil || b2->T->seq == oid_nil)
			res = bit_nil;
		else
			res = b1->T->seq < b2->T->seq;

		return BATconst(b1, TYPE_bit, &res);
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalclt_intern(b1->T->type == TYPE_void ? (void *) &b1->T->seq : (void *) Tloc(b1, b1->U->first), b1->T->type, 1,
			      b1->T->vheap ? b1->T->vheap->base : NULL,
			      b1->T->width,
			      b2->T->type == TYPE_void ? (void *) &b2->T->seq : (void *) Tloc(b2, b2->U->first), b2->T->type, 1,
			      b2->T->vheap ? b2->T->vheap->base : NULL,
			      b2->T->width,
			      b1->U->count,
			      b1->T->nonil && b2->T->nonil,
			      b1->H->seq, "BATcalclt");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcltcst(BAT *b, const ValRecord *v)
{
	BAT *bn;

	BATcheck(b, "BATcalcltcst");

	if (checkbats(b, NULL, "BATcalcltcst") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalclt_intern(Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      (const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalcltcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstlt(const ValRecord *v, BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATcalccstlt");

	if (checkbats(b, NULL, "BATcalccstlt") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalclt_intern((const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalccstlt");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalclt(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_bit;
	if (lt_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0, NULL, 0,
			      (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0, NULL, 0,
			      VALptr(ret), 1, 0, "VARcalclt") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define GT(a, b)	((bit) ((a) > (b)))

static BUN
gt_typeswitchloop(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		  const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		  bit *dst, BUN cnt, int nonil, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;
	const char *s1, *s2;

	/* bit and oid can only be compared with each other */
	if ((tp1 == TYPE_bit) != (tp2 == TYPE_bit))
		goto unsupported;
	if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
		goto unsupported;

	switch (ATOMstorage(tp1)) {
	case TYPE_void: {
		oid v;

		assert(incr1 == 1);
		assert(tp2 == TYPE_oid || incr2 == 1); /* if void, incr2==1 */
		v = * (const oid *) lft;
		if (v == oid_nil || tp2 == TYPE_void) {
			bit res = v == oid_nil || * (const oid *) rgt == oid_nil ?
				bit_nil :
				GT(v, * (const oid *) rgt);

			for (k = 0; k < cnt; k++)
				dst[k] = res;
			if (res == bit_nil)
				nils = cnt;
		} else {
			for (j = k = 0; k < cnt; v++, j += incr2, k++) {
				if (((const oid *) rgt)[j] == oid_nil) {
					nils++;
					dst[k] = bit_nil;
				} else {
					dst[k] = GT(v, ((const oid *) rgt)[j]);
				}
			}
		}
		break;
	}
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bit, GT);
			else
				BINARY_3TYPE_FUNC(bte, bte, bit, GT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, sht, bit, GT);
			else
				BINARY_3TYPE_FUNC(bte, sht, bit, GT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, int, bit, GT);
			else
				BINARY_3TYPE_FUNC(bte, int, bit, GT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, lng, bit, GT);
			else
				BINARY_3TYPE_FUNC(bte, lng, bit, GT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, flt, bit, GT);
			else
				BINARY_3TYPE_FUNC(bte, flt, bit, GT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, dbl, bit, GT);
			else
				BINARY_3TYPE_FUNC(bte, dbl, bit, GT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, bte, bit, GT);
			else
				BINARY_3TYPE_FUNC(sht, bte, bit, GT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, sht, bit, GT);
			else
				BINARY_3TYPE_FUNC(sht, sht, bit, GT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, int, bit, GT);
			else
				BINARY_3TYPE_FUNC(sht, int, bit, GT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, lng, bit, GT);
			else
				BINARY_3TYPE_FUNC(sht, lng, bit, GT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, flt, bit, GT);
			else
				BINARY_3TYPE_FUNC(sht, flt, bit, GT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, dbl, bit, GT);
			else
				BINARY_3TYPE_FUNC(sht, dbl, bit, GT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = GT(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, bte, bit, GT);
			else
				BINARY_3TYPE_FUNC(int, bte, bit, GT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, sht, bit, GT);
			else
				BINARY_3TYPE_FUNC(int, sht, bit, GT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, int, bit, GT);
			else
				BINARY_3TYPE_FUNC(int, int, bit, GT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, lng, bit, GT);
			else
				BINARY_3TYPE_FUNC(int, lng, bit, GT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, flt, bit, GT);
			else
				BINARY_3TYPE_FUNC(int, flt, bit, GT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, dbl, bit, GT);
			else
				BINARY_3TYPE_FUNC(int, dbl, bit, GT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = GT(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, bte, bit, GT);
			else
				BINARY_3TYPE_FUNC(lng, bte, bit, GT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, sht, bit, GT);
			else
				BINARY_3TYPE_FUNC(lng, sht, bit, GT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, int, bit, GT);
			else
				BINARY_3TYPE_FUNC(lng, int, bit, GT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, lng, bit, GT);
			else
				BINARY_3TYPE_FUNC(lng, lng, bit, GT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, flt, bit, GT);
			else
				BINARY_3TYPE_FUNC(lng, flt, bit, GT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, dbl, bit, GT);
			else
				BINARY_3TYPE_FUNC(lng, dbl, bit, GT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, bte, bit, GT);
			else
				BINARY_3TYPE_FUNC(flt, bte, bit, GT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, sht, bit, GT);
			else
				BINARY_3TYPE_FUNC(flt, sht, bit, GT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, int, bit, GT);
			else
				BINARY_3TYPE_FUNC(flt, int, bit, GT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, lng, bit, GT);
			else
				BINARY_3TYPE_FUNC(flt, lng, bit, GT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, flt, bit, GT);
			else
				BINARY_3TYPE_FUNC(flt, flt, bit, GT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, dbl, bit, GT);
			else
				BINARY_3TYPE_FUNC(flt, dbl, bit, GT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, bte, bit, GT);
			else
				BINARY_3TYPE_FUNC(dbl, bte, bit, GT);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, sht, bit, GT);
			else
				BINARY_3TYPE_FUNC(dbl, sht, bit, GT);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, int, bit, GT);
			else
				BINARY_3TYPE_FUNC(dbl, int, bit, GT);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, lng, bit, GT);
			else
				BINARY_3TYPE_FUNC(dbl, lng, bit, GT);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, flt, bit, GT);
			else
				BINARY_3TYPE_FUNC(dbl, flt, bit, GT);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, dbl, bit, GT);
			else
				BINARY_3TYPE_FUNC(dbl, dbl, bit, GT);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		if (tp1 != tp2)
			goto unsupported;
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) {
			s1 = hp1 ? hp1 + VarHeapVal(lft, i, wd1) : (const char *) lft;
			s2 = hp2 ? hp2 + VarHeapVal(rgt, j, wd2) : (const char *) rgt;
			if (s1 == NULL || strcmp(s1, str_nil) == 0 ||
			    s2 == NULL || strcmp(s2, str_nil) == 0) {
				nils++;
				dst[k] = bit_nil;
			} else {
				dst[k] = (bit) (strcmp(s1, s2) > 0);
			}
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

static BAT *
BATcalcgt_intern(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		 const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		 BUN cnt, int nonil, oid seqbase, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	bit *dst;

	bn = BATnew(TYPE_void, TYPE_bit, cnt);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, bn->U->first);

	nils = gt_typeswitchloop(lft, tp1, incr1, hp1, wd1,
				 rgt, tp2, incr2, hp2, wd2,
				 dst, cnt, nonil, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn = BATseqbase(bn, seqbase);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	return bn;
}

BAT *
BATcalcgt(BAT *b1, BAT *b2)
{
	BAT *bn;

	BATcheck(b1, "BATcalcgt");
	BATcheck(b2, "BATcalcgt");

	if (checkbats(b1, b2, "BATcalcgt") == GDK_FAIL)
		return NULL;

	if (BATtvoid(b1) && BATtvoid(b2)) {
		bit res;

		if (b1->T->seq == oid_nil || b2->T->seq == oid_nil)
			res = bit_nil;
		else
			res = b1->T->seq > b2->T->seq;

		return BATconst(b1, TYPE_bit, &res);
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcgt_intern(Tloc(b1, b1->U->first), b1->T->type, 1,
			      b1->T->vheap ? b1->T->vheap->base : NULL,
			      b1->T->width,
			      Tloc(b2, b2->U->first), b2->T->type, 1,
			      b2->T->vheap ? b2->T->vheap->base : NULL,
			      b2->T->width,
			      b1->U->count,
			      b1->T->nonil && b2->T->nonil,
			      b1->H->seq, "BATcalcgt");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcgtcst(BAT *b, const ValRecord *v)
{
	BAT *bn;

	BATcheck(b, "BATcalcgtcst");

	if (checkbats(b, NULL, "BATcalcgtcst") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcgt_intern(Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      (const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalcgtcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstgt(const ValRecord *v, BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATcalccstgt");

	if (checkbats(b, NULL, "BATcalccstgt") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcgt_intern((const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalccstgt");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcgt(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_bit;
	if (gt_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0, NULL, 0,
			      (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0, NULL, 0,
			      VALptr(ret), 1, 0, "VARcalcgt") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define LE(a, b)	((bit) ((a) <= (b)))

static BUN
le_typeswitchloop(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		  const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		  bit *dst, BUN cnt, int nonil, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;
	const char *s1, *s2;

	/* bit and oid can only be compared with each other */
	if ((tp1 == TYPE_bit) != (tp2 == TYPE_bit))
		goto unsupported;
	if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
		goto unsupported;

	switch (ATOMstorage(tp1)) {
	case TYPE_void: {
		oid v;

		assert(incr1 == 1);
		assert(tp2 == TYPE_oid || incr2 == 1); /* if void, incr2==1 */
		v = * (const oid *) lft;
		if (v == oid_nil || tp2 == TYPE_void) {
			bit res = v == oid_nil || * (const oid *) rgt == oid_nil ?
				bit_nil :
				LE(v, * (const oid *) rgt);

			for (k = 0; k < cnt; k++)
				dst[k] = res;
			if (res == bit_nil)
				nils = cnt;
		} else {
			for (j = k = 0; k < cnt; v++, j += incr2, k++) {
				if (((const oid *) rgt)[j] == oid_nil) {
					nils++;
					dst[k] = bit_nil;
				} else {
					dst[k] = LE(v, ((const oid *) rgt)[j]);
				}
			}
		}
		break;
	}
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bit, LE);
			else
				BINARY_3TYPE_FUNC(bte, bte, bit, LE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, sht, bit, LE);
			else
				BINARY_3TYPE_FUNC(bte, sht, bit, LE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, int, bit, LE);
			else
				BINARY_3TYPE_FUNC(bte, int, bit, LE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, lng, bit, LE);
			else
				BINARY_3TYPE_FUNC(bte, lng, bit, LE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, flt, bit, LE);
			else
				BINARY_3TYPE_FUNC(bte, flt, bit, LE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, dbl, bit, LE);
			else
				BINARY_3TYPE_FUNC(bte, dbl, bit, LE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, bte, bit, LE);
			else
				BINARY_3TYPE_FUNC(sht, bte, bit, LE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, sht, bit, LE);
			else
				BINARY_3TYPE_FUNC(sht, sht, bit, LE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, int, bit, LE);
			else
				BINARY_3TYPE_FUNC(sht, int, bit, LE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, lng, bit, LE);
			else
				BINARY_3TYPE_FUNC(sht, lng, bit, LE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, flt, bit, LE);
			else
				BINARY_3TYPE_FUNC(sht, flt, bit, LE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, dbl, bit, LE);
			else
				BINARY_3TYPE_FUNC(sht, dbl, bit, LE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = LE(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, bte, bit, LE);
			else
				BINARY_3TYPE_FUNC(int, bte, bit, LE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, sht, bit, LE);
			else
				BINARY_3TYPE_FUNC(int, sht, bit, LE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, int, bit, LE);
			else
				BINARY_3TYPE_FUNC(int, int, bit, LE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, lng, bit, LE);
			else
				BINARY_3TYPE_FUNC(int, lng, bit, LE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, flt, bit, LE);
			else
				BINARY_3TYPE_FUNC(int, flt, bit, LE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, dbl, bit, LE);
			else
				BINARY_3TYPE_FUNC(int, dbl, bit, LE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = LE(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, bte, bit, LE);
			else
				BINARY_3TYPE_FUNC(lng, bte, bit, LE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, sht, bit, LE);
			else
				BINARY_3TYPE_FUNC(lng, sht, bit, LE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, int, bit, LE);
			else
				BINARY_3TYPE_FUNC(lng, int, bit, LE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, lng, bit, LE);
			else
				BINARY_3TYPE_FUNC(lng, lng, bit, LE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, flt, bit, LE);
			else
				BINARY_3TYPE_FUNC(lng, flt, bit, LE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, dbl, bit, LE);
			else
				BINARY_3TYPE_FUNC(lng, dbl, bit, LE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, bte, bit, LE);
			else
				BINARY_3TYPE_FUNC(flt, bte, bit, LE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, sht, bit, LE);
			else
				BINARY_3TYPE_FUNC(flt, sht, bit, LE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, int, bit, LE);
			else
				BINARY_3TYPE_FUNC(flt, int, bit, LE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, lng, bit, LE);
			else
				BINARY_3TYPE_FUNC(flt, lng, bit, LE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, flt, bit, LE);
			else
				BINARY_3TYPE_FUNC(flt, flt, bit, LE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, dbl, bit, LE);
			else
				BINARY_3TYPE_FUNC(flt, dbl, bit, LE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, bte, bit, LE);
			else
				BINARY_3TYPE_FUNC(dbl, bte, bit, LE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, sht, bit, LE);
			else
				BINARY_3TYPE_FUNC(dbl, sht, bit, LE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, int, bit, LE);
			else
				BINARY_3TYPE_FUNC(dbl, int, bit, LE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, lng, bit, LE);
			else
				BINARY_3TYPE_FUNC(dbl, lng, bit, LE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, flt, bit, LE);
			else
				BINARY_3TYPE_FUNC(dbl, flt, bit, LE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, dbl, bit, LE);
			else
				BINARY_3TYPE_FUNC(dbl, dbl, bit, LE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		if (tp1 != tp2)
			goto unsupported;
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) {
			s1 = hp1 ? hp1 + VarHeapVal(lft, i, wd1) : (const char *) lft;
			s2 = hp2 ? hp2 + VarHeapVal(rgt, j, wd2) : (const char *) rgt;
			if (s1 == NULL || strcmp(s1, str_nil) == 0 ||
			    s2 == NULL || strcmp(s2, str_nil) == 0) {
				nils++;
				dst[k] = bit_nil;
			} else {
				dst[k] = (bit) (strcmp(s1, s2) <= 0);
			}
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

static BAT *
BATcalcle_intern(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		 const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		 BUN cnt, int nonil, oid seqbase, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	bit *dst;

	bn = BATnew(TYPE_void, TYPE_bit, cnt);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, bn->U->first);

	nils = le_typeswitchloop(lft, tp1, incr1, hp1, wd1,
				 rgt, tp2, incr2, hp2, wd2,
				 dst, cnt, nonil, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn = BATseqbase(bn, seqbase);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	return bn;
}

BAT *
BATcalcle(BAT *b1, BAT *b2)
{
	BAT *bn;

	BATcheck(b1, "BATcalcle");
	BATcheck(b2, "BATcalcle");

	if (checkbats(b1, b2, "BATcalcle") == GDK_FAIL)
		return NULL;

	if (BATtvoid(b1) && BATtvoid(b2)) {
		bit res;

		if (b1->T->seq == oid_nil || b2->T->seq == oid_nil)
			res = bit_nil;
		else
			res = b1->T->seq <= b2->T->seq;

		return BATconst(b1, TYPE_bit, &res);
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcle_intern(Tloc(b1, b1->U->first), b1->T->type, 1,
			      b1->T->vheap ? b1->T->vheap->base : NULL,
			      b1->T->width,
			      Tloc(b2, b2->U->first), b2->T->type, 1,
			      b2->T->vheap ? b2->T->vheap->base : NULL,
			      b2->T->width,
			      b1->U->count,
			      b1->T->nonil && b2->T->nonil,
			      b1->H->seq, "BATcalcle");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalclecst(BAT *b, const ValRecord *v)
{
	BAT *bn;

	BATcheck(b, "BATcalclecst");

	if (checkbats(b, NULL, "BATcalclecst") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcle_intern(Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      (const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalclecst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstle(const ValRecord *v, BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATcalccstle");

	if (checkbats(b, NULL, "BATcalccstle") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcle_intern((const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalccstle");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcle(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_bit;
	if (le_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0, NULL, 0,
			      (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0, NULL, 0,
			      VALptr(ret), 1, 0, "VARcalcle") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define GE(a, b)	((bit) ((a) >= (b)))

static BUN
ge_typeswitchloop(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		  const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		  bit *dst, BUN cnt, int nonil, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;
	const char *s1, *s2;

	/* bit and oid can only be compared with each other */
	if ((tp1 == TYPE_bit) != (tp2 == TYPE_bit))
		goto unsupported;
	if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
		goto unsupported;

	switch (ATOMstorage(tp1)) {
	case TYPE_void: {
		oid v;

		assert(incr1 == 1);
		assert(tp2 == TYPE_oid || incr2 == 1); /* if void, incr2==1 */
		v = * (const oid *) lft;
		if (v == oid_nil || tp2 == TYPE_void) {
			bit res = v == oid_nil || * (const oid *) rgt == oid_nil ?
				bit_nil :
				GE(v, * (const oid *) rgt);

			for (k = 0; k < cnt; k++)
				dst[k] = res;
			if (res == bit_nil)
				nils = cnt;
		} else {
			for (j = k = 0; k < cnt; v++, j += incr2, k++) {
				if (((const oid *) rgt)[j] == oid_nil) {
					nils++;
					dst[k] = bit_nil;
				} else {
					dst[k] = GE(v, ((const oid *) rgt)[j]);
				}
			}
		}
		break;
	}
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bit, GE);
			else
				BINARY_3TYPE_FUNC(bte, bte, bit, GE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, sht, bit, GE);
			else
				BINARY_3TYPE_FUNC(bte, sht, bit, GE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, int, bit, GE);
			else
				BINARY_3TYPE_FUNC(bte, int, bit, GE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, lng, bit, GE);
			else
				BINARY_3TYPE_FUNC(bte, lng, bit, GE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, flt, bit, GE);
			else
				BINARY_3TYPE_FUNC(bte, flt, bit, GE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, dbl, bit, GE);
			else
				BINARY_3TYPE_FUNC(bte, dbl, bit, GE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, bte, bit, GE);
			else
				BINARY_3TYPE_FUNC(sht, bte, bit, GE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, sht, bit, GE);
			else
				BINARY_3TYPE_FUNC(sht, sht, bit, GE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, int, bit, GE);
			else
				BINARY_3TYPE_FUNC(sht, int, bit, GE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, lng, bit, GE);
			else
				BINARY_3TYPE_FUNC(sht, lng, bit, GE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, flt, bit, GE);
			else
				BINARY_3TYPE_FUNC(sht, flt, bit, GE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, dbl, bit, GE);
			else
				BINARY_3TYPE_FUNC(sht, dbl, bit, GE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = GE(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, bte, bit, GE);
			else
				BINARY_3TYPE_FUNC(int, bte, bit, GE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, sht, bit, GE);
			else
				BINARY_3TYPE_FUNC(int, sht, bit, GE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, int, bit, GE);
			else
				BINARY_3TYPE_FUNC(int, int, bit, GE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, lng, bit, GE);
			else
				BINARY_3TYPE_FUNC(int, lng, bit, GE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, flt, bit, GE);
			else
				BINARY_3TYPE_FUNC(int, flt, bit, GE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, dbl, bit, GE);
			else
				BINARY_3TYPE_FUNC(int, dbl, bit, GE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = GE(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, bte, bit, GE);
			else
				BINARY_3TYPE_FUNC(lng, bte, bit, GE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, sht, bit, GE);
			else
				BINARY_3TYPE_FUNC(lng, sht, bit, GE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, int, bit, GE);
			else
				BINARY_3TYPE_FUNC(lng, int, bit, GE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, lng, bit, GE);
			else
				BINARY_3TYPE_FUNC(lng, lng, bit, GE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, flt, bit, GE);
			else
				BINARY_3TYPE_FUNC(lng, flt, bit, GE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, dbl, bit, GE);
			else
				BINARY_3TYPE_FUNC(lng, dbl, bit, GE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, bte, bit, GE);
			else
				BINARY_3TYPE_FUNC(flt, bte, bit, GE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, sht, bit, GE);
			else
				BINARY_3TYPE_FUNC(flt, sht, bit, GE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, int, bit, GE);
			else
				BINARY_3TYPE_FUNC(flt, int, bit, GE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, lng, bit, GE);
			else
				BINARY_3TYPE_FUNC(flt, lng, bit, GE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, flt, bit, GE);
			else
				BINARY_3TYPE_FUNC(flt, flt, bit, GE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, dbl, bit, GE);
			else
				BINARY_3TYPE_FUNC(flt, dbl, bit, GE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, bte, bit, GE);
			else
				BINARY_3TYPE_FUNC(dbl, bte, bit, GE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, sht, bit, GE);
			else
				BINARY_3TYPE_FUNC(dbl, sht, bit, GE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, int, bit, GE);
			else
				BINARY_3TYPE_FUNC(dbl, int, bit, GE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, lng, bit, GE);
			else
				BINARY_3TYPE_FUNC(dbl, lng, bit, GE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, flt, bit, GE);
			else
				BINARY_3TYPE_FUNC(dbl, flt, bit, GE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, dbl, bit, GE);
			else
				BINARY_3TYPE_FUNC(dbl, dbl, bit, GE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		if (tp1 != tp2)
			goto unsupported;
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) {
			s1 = hp1 ? hp1 + VarHeapVal(lft, i, wd1) : (const char *) lft;
			s2 = hp2 ? hp2 + VarHeapVal(rgt, j, wd2) : (const char *) rgt;
			if (s1 == NULL || strcmp(s1, str_nil) == 0 ||
			    s2 == NULL || strcmp(s2, str_nil) == 0) {
				nils++;
				dst[k] = bit_nil;
			} else {
				dst[k] = (bit) (strcmp(s1, s2) >= 0);
			}
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

static BAT *
BATcalcge_intern(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		 const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		 BUN cnt, int nonil, oid seqbase, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	bit *dst;

	bn = BATnew(TYPE_void, TYPE_bit, cnt);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, bn->U->first);

	nils = ge_typeswitchloop(lft, tp1, incr1, hp1, wd1,
				 rgt, tp2, incr2, hp2, wd2,
				 dst, cnt, nonil, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn = BATseqbase(bn, seqbase);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	return bn;
}

BAT *
BATcalcge(BAT *b1, BAT *b2)
{
	BAT *bn;

	BATcheck(b1, "BATcalcge");
	BATcheck(b2, "BATcalcge");

	if (checkbats(b1, b2, "BATcalcge") == GDK_FAIL)
		return NULL;

	if (BATtvoid(b1) && BATtvoid(b2)) {
		bit res;

		if (b1->T->seq == oid_nil || b2->T->seq == oid_nil)
			res = bit_nil;
		else
			res = b1->T->seq >= b2->T->seq;

		return BATconst(b1, TYPE_bit, &res);
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcge_intern(Tloc(b1, b1->U->first), b1->T->type, 1,
			      b1->T->vheap ? b1->T->vheap->base : NULL,
			      b1->T->width,
			      Tloc(b2, b2->U->first), b2->T->type, 1,
			      b2->T->vheap ? b2->T->vheap->base : NULL,
			      b2->T->width,
			      b1->U->count,
			      b1->T->nonil && b2->T->nonil,
			      b1->H->seq, "BATcalcge");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcgecst(BAT *b, const ValRecord *v)
{
	BAT *bn;

	BATcheck(b, "BATcalcgecst");

	if (checkbats(b, NULL, "BATcalcgecst") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcge_intern(Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      (const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalcgecst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstge(const ValRecord *v, BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATcalccstge");

	if (checkbats(b, NULL, "BATcalccstge") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcge_intern((const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalccstge");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcge(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_bit;
	if (ge_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0, NULL, 0,
			      (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0, NULL, 0,
			      VALptr(ret), 1, 0, "VARcalcge") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define EQ(a, b)	((bit) ((a) == (b)))

static BUN
eq_typeswitchloop(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		  const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		  bit *dst, BUN cnt, int nonil, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;
	const char *s1, *s2;

	/* bit and oid can only be compared with each other */
	if ((tp1 == TYPE_bit) != (tp2 == TYPE_bit))
		goto unsupported;
	if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
		goto unsupported;

	switch (ATOMstorage(tp1)) {
	case TYPE_void: {
		oid v;

		assert(incr1 == 1);
		assert(tp2 == TYPE_oid || incr2 == 1); /* if void, incr2==1 */
		v = * (const oid *) lft;
		if (v == oid_nil || tp2 == TYPE_void) {
			bit res = v == oid_nil || * (const oid *) rgt == oid_nil ?
				bit_nil :
				EQ(v, * (const oid *) rgt);

			for (k = 0; k < cnt; k++)
				dst[k] = res;
			if (res == bit_nil)
				nils = cnt;
		} else {
			for (j = k = 0; k < cnt; v++, j += incr2, k++) {
				if (((const oid *) rgt)[j] == oid_nil) {
					nils++;
					dst[k] = bit_nil;
				} else {
					dst[k] = EQ(v, ((const oid *) rgt)[j]);
				}
			}
		}
		break;
	}
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bit, EQ);
			else
				BINARY_3TYPE_FUNC(bte, bte, bit, EQ);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, sht, bit, EQ);
			else
				BINARY_3TYPE_FUNC(bte, sht, bit, EQ);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, int, bit, EQ);
			else
				BINARY_3TYPE_FUNC(bte, int, bit, EQ);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, lng, bit, EQ);
			else
				BINARY_3TYPE_FUNC(bte, lng, bit, EQ);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, flt, bit, EQ);
			else
				BINARY_3TYPE_FUNC(bte, flt, bit, EQ);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, dbl, bit, EQ);
			else
				BINARY_3TYPE_FUNC(bte, dbl, bit, EQ);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, bte, bit, EQ);
			else
				BINARY_3TYPE_FUNC(sht, bte, bit, EQ);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, sht, bit, EQ);
			else
				BINARY_3TYPE_FUNC(sht, sht, bit, EQ);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, int, bit, EQ);
			else
				BINARY_3TYPE_FUNC(sht, int, bit, EQ);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, lng, bit, EQ);
			else
				BINARY_3TYPE_FUNC(sht, lng, bit, EQ);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, flt, bit, EQ);
			else
				BINARY_3TYPE_FUNC(sht, flt, bit, EQ);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, dbl, bit, EQ);
			else
				BINARY_3TYPE_FUNC(sht, dbl, bit, EQ);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = EQ(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, bte, bit, EQ);
			else
				BINARY_3TYPE_FUNC(int, bte, bit, EQ);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, sht, bit, EQ);
			else
				BINARY_3TYPE_FUNC(int, sht, bit, EQ);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, int, bit, EQ);
			else
				BINARY_3TYPE_FUNC(int, int, bit, EQ);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, lng, bit, EQ);
			else
				BINARY_3TYPE_FUNC(int, lng, bit, EQ);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, flt, bit, EQ);
			else
				BINARY_3TYPE_FUNC(int, flt, bit, EQ);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, dbl, bit, EQ);
			else
				BINARY_3TYPE_FUNC(int, dbl, bit, EQ);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = EQ(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, bte, bit, EQ);
			else
				BINARY_3TYPE_FUNC(lng, bte, bit, EQ);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, sht, bit, EQ);
			else
				BINARY_3TYPE_FUNC(lng, sht, bit, EQ);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, int, bit, EQ);
			else
				BINARY_3TYPE_FUNC(lng, int, bit, EQ);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, lng, bit, EQ);
			else
				BINARY_3TYPE_FUNC(lng, lng, bit, EQ);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, flt, bit, EQ);
			else
				BINARY_3TYPE_FUNC(lng, flt, bit, EQ);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, dbl, bit, EQ);
			else
				BINARY_3TYPE_FUNC(lng, dbl, bit, EQ);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, bte, bit, EQ);
			else
				BINARY_3TYPE_FUNC(flt, bte, bit, EQ);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, sht, bit, EQ);
			else
				BINARY_3TYPE_FUNC(flt, sht, bit, EQ);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, int, bit, EQ);
			else
				BINARY_3TYPE_FUNC(flt, int, bit, EQ);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, lng, bit, EQ);
			else
				BINARY_3TYPE_FUNC(flt, lng, bit, EQ);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, flt, bit, EQ);
			else
				BINARY_3TYPE_FUNC(flt, flt, bit, EQ);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, dbl, bit, EQ);
			else
				BINARY_3TYPE_FUNC(flt, dbl, bit, EQ);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, bte, bit, EQ);
			else
				BINARY_3TYPE_FUNC(dbl, bte, bit, EQ);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, sht, bit, EQ);
			else
				BINARY_3TYPE_FUNC(dbl, sht, bit, EQ);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, int, bit, EQ);
			else
				BINARY_3TYPE_FUNC(dbl, int, bit, EQ);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, lng, bit, EQ);
			else
				BINARY_3TYPE_FUNC(dbl, lng, bit, EQ);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, flt, bit, EQ);
			else
				BINARY_3TYPE_FUNC(dbl, flt, bit, EQ);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, dbl, bit, EQ);
			else
				BINARY_3TYPE_FUNC(dbl, dbl, bit, EQ);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		if (tp1 != tp2)
			goto unsupported;
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) {
			s1 = hp1 ? hp1 + VarHeapVal(lft, i, wd1) : (const char *) lft;
			s2 = hp2 ? hp2 + VarHeapVal(rgt, j, wd2) : (const char *) rgt;
			if (s1 == NULL || strcmp(s1, str_nil) == 0 ||
			    s2 == NULL || strcmp(s2, str_nil) == 0) {
				nils++;
				dst[k] = bit_nil;
			} else {
				dst[k] = (bit) (strcmp(s1, s2) == 0);
			}
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

static BAT *
BATcalceq_intern(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		 const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		 BUN cnt, int nonil, oid seqbase, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	bit *dst;

	bn = BATnew(TYPE_void, TYPE_bit, cnt);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, bn->U->first);

	nils = eq_typeswitchloop(lft, tp1, incr1, hp1, wd1,
				 rgt, tp2, incr2, hp2, wd2,
				 dst, cnt, nonil, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn = BATseqbase(bn, seqbase);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	return bn;
}

BAT *
BATcalceq(BAT *b1, BAT *b2)
{
	BAT *bn;

	BATcheck(b1, "BATcalceq");
	BATcheck(b2, "BATcalceq");

	if (checkbats(b1, b2, "BATcalceq") == GDK_FAIL)
		return NULL;

	if (BATtvoid(b1) && BATtvoid(b2)) {
		bit res;

		if (b1->T->seq == oid_nil || b2->T->seq == oid_nil)
			res = bit_nil;
		else
			res = b1->T->seq == b2->T->seq;

		return BATconst(b1, TYPE_bit, &res);
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalceq_intern(Tloc(b1, b1->U->first), b1->T->type, 1,
			      b1->T->vheap ? b1->T->vheap->base : NULL,
			      b1->T->width,
			      Tloc(b2, b2->U->first), b2->T->type, 1,
			      b2->T->vheap ? b2->T->vheap->base : NULL,
			      b2->T->width,
			      b1->U->count,
			      b1->T->nonil && b2->T->nonil,
			      b1->H->seq, "BATcalceq");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalceqcst(BAT *b, const ValRecord *v)
{
	BAT *bn;

	BATcheck(b, "BATcalceqcst");

	if (checkbats(b, NULL, "BATcalceqcst") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalceq_intern(Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      (const void *) VALptr((ValPtr) v), v->vtype, 0, NULL, 0,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalceqcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccsteq(const ValRecord *v, BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATcalccsteq");

	if (checkbats(b, NULL, "BATcalccsteq") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalceq_intern((const void *) VALptr((ValPtr) v), v->vtype, 0, NULL, 0,
			      Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalccsteq");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalceq(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_bit;
	if (eq_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0, NULL, 0,
			      (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0, NULL, 0,
			      VALptr(ret), 1, 0, "VARcalceq") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define NE(a, b)	((bit) ((a) != (b)))

static BUN
ne_typeswitchloop(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		  const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		  bit *dst, BUN cnt, int nonil, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;
	const char *s1, *s2;

	/* bit and oid can only be compared with each other */
	if ((tp1 == TYPE_bit) != (tp2 == TYPE_bit))
		goto unsupported;
	if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
		goto unsupported;

	switch (ATOMstorage(tp1)) {
	case TYPE_void: {
		oid v;

		assert(incr1 == 1);
		assert(tp2 == TYPE_oid || incr2 == 1); /* if void, incr2==1 */
		v = * (const oid *) lft;
		if (v == oid_nil || tp2 == TYPE_void) {
			bit res = v == oid_nil || * (const oid *) rgt == oid_nil ?
				bit_nil :
				NE(v, * (const oid *) rgt);

			for (k = 0; k < cnt; k++)
				dst[k] = res;
			if (res == bit_nil)
				nils = cnt;
		} else {
			for (j = k = 0; k < cnt; v++, j += incr2, k++) {
				if (((const oid *) rgt)[j] == oid_nil) {
					nils++;
					dst[k] = bit_nil;
				} else {
					dst[k] = NE(v, ((const oid *) rgt)[j]);
				}
			}
		}
		break;
	}
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bit, NE);
			else
				BINARY_3TYPE_FUNC(bte, bte, bit, NE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, sht, bit, NE);
			else
				BINARY_3TYPE_FUNC(bte, sht, bit, NE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, int, bit, NE);
			else
				BINARY_3TYPE_FUNC(bte, int, bit, NE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, lng, bit, NE);
			else
				BINARY_3TYPE_FUNC(bte, lng, bit, NE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, flt, bit, NE);
			else
				BINARY_3TYPE_FUNC(bte, flt, bit, NE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, dbl, bit, NE);
			else
				BINARY_3TYPE_FUNC(bte, dbl, bit, NE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, bte, bit, NE);
			else
				BINARY_3TYPE_FUNC(sht, bte, bit, NE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, sht, bit, NE);
			else
				BINARY_3TYPE_FUNC(sht, sht, bit, NE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, int, bit, NE);
			else
				BINARY_3TYPE_FUNC(sht, int, bit, NE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, lng, bit, NE);
			else
				BINARY_3TYPE_FUNC(sht, lng, bit, NE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, flt, bit, NE);
			else
				BINARY_3TYPE_FUNC(sht, flt, bit, NE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, dbl, bit, NE);
			else
				BINARY_3TYPE_FUNC(sht, dbl, bit, NE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = NE(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, bte, bit, NE);
			else
				BINARY_3TYPE_FUNC(int, bte, bit, NE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, sht, bit, NE);
			else
				BINARY_3TYPE_FUNC(int, sht, bit, NE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, int, bit, NE);
			else
				BINARY_3TYPE_FUNC(int, int, bit, NE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, lng, bit, NE);
			else
				BINARY_3TYPE_FUNC(int, lng, bit, NE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, flt, bit, NE);
			else
				BINARY_3TYPE_FUNC(int, flt, bit, NE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, dbl, bit, NE);
			else
				BINARY_3TYPE_FUNC(int, dbl, bit, NE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = NE(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, bte, bit, NE);
			else
				BINARY_3TYPE_FUNC(lng, bte, bit, NE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, sht, bit, NE);
			else
				BINARY_3TYPE_FUNC(lng, sht, bit, NE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, int, bit, NE);
			else
				BINARY_3TYPE_FUNC(lng, int, bit, NE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, lng, bit, NE);
			else
				BINARY_3TYPE_FUNC(lng, lng, bit, NE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, flt, bit, NE);
			else
				BINARY_3TYPE_FUNC(lng, flt, bit, NE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, dbl, bit, NE);
			else
				BINARY_3TYPE_FUNC(lng, dbl, bit, NE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, bte, bit, NE);
			else
				BINARY_3TYPE_FUNC(flt, bte, bit, NE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, sht, bit, NE);
			else
				BINARY_3TYPE_FUNC(flt, sht, bit, NE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, int, bit, NE);
			else
				BINARY_3TYPE_FUNC(flt, int, bit, NE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, lng, bit, NE);
			else
				BINARY_3TYPE_FUNC(flt, lng, bit, NE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, flt, bit, NE);
			else
				BINARY_3TYPE_FUNC(flt, flt, bit, NE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, dbl, bit, NE);
			else
				BINARY_3TYPE_FUNC(flt, dbl, bit, NE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, bte, bit, NE);
			else
				BINARY_3TYPE_FUNC(dbl, bte, bit, NE);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, sht, bit, NE);
			else
				BINARY_3TYPE_FUNC(dbl, sht, bit, NE);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, int, bit, NE);
			else
				BINARY_3TYPE_FUNC(dbl, int, bit, NE);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, lng, bit, NE);
			else
				BINARY_3TYPE_FUNC(dbl, lng, bit, NE);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, flt, bit, NE);
			else
				BINARY_3TYPE_FUNC(dbl, flt, bit, NE);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, dbl, bit, NE);
			else
				BINARY_3TYPE_FUNC(dbl, dbl, bit, NE);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		if (tp1 != tp2)
			goto unsupported;
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) {
			s1 = hp1 ? hp1 + VarHeapVal(lft, i, wd1) : (const char *) lft;
			s2 = hp2 ? hp2 + VarHeapVal(rgt, j, wd2) : (const char *) rgt;
			if (s1 == NULL || strcmp(s1, str_nil) == 0 ||
			    s2 == NULL || strcmp(s2, str_nil) == 0) {
				nils++;
				dst[k] = bit_nil;
			} else {
				dst[k] = (bit) (strcmp(s1, s2) != 0);
			}
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

static BAT *
BATcalcne_intern(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		 const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		 BUN cnt, int nonil, oid seqbase, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	bit *dst;

	bn = BATnew(TYPE_void, TYPE_bit, cnt);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, bn->U->first);

	nils = ne_typeswitchloop(lft, tp1, incr1, hp1, wd1,
				 rgt, tp2, incr2, hp2, wd2,
				 dst, cnt, nonil, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn = BATseqbase(bn, seqbase);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	return bn;
}

BAT *
BATcalcne(BAT *b1, BAT *b2)
{
	BAT *bn;

	BATcheck(b1, "BATcalcne");
	BATcheck(b2, "BATcalcne");

	if (checkbats(b1, b2, "BATcalcne") == GDK_FAIL)
		return NULL;

	if (BATtvoid(b1) && BATtvoid(b2)) {
		bit res;

		if (b1->T->seq == oid_nil || b2->T->seq == oid_nil)
			res = bit_nil;
		else
			res = b1->T->seq != b2->T->seq;

		return BATconst(b1, TYPE_bit, &res);
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcne_intern(Tloc(b1, b1->U->first), b1->T->type, 1,
			      b1->T->vheap ? b1->T->vheap->base : NULL,
			      b1->T->width,
			      Tloc(b2, b2->U->first), b2->T->type, 1,
			      b2->T->vheap ? b2->T->vheap->base : NULL,
			      b2->T->width,
			      b1->U->count,
			      b1->T->nonil && b2->T->nonil,
			      b1->H->seq, "BATcalcne");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcnecst(BAT *b, const ValRecord *v)
{
	BAT *bn;

	BATcheck(b, "BATcalcnecst");

	if (checkbats(b, NULL, "BATcalcnecst") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcne_intern(Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      (const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalcnecst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstne(const ValRecord *v, BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATcalccstne");

	if (checkbats(b, NULL, "BATcalccstne") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcne_intern((const void *) VALptr((ValPtr) v), v->vtype, 0,
			      NULL, 0,
			      Tloc(b, b->U->first), b->T->type, 1,
			      b->T->vheap ? b->T->vheap->base : NULL,
			      b->T->width,
			      b->U->count,
			      b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			      b->H->seq, "BATcalccstne");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcne(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_bit;
	if (ne_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0, NULL, 0,
			      (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0, NULL, 0,
			      VALptr(ret), 1, 0, "VARcalcne") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define CMP(a, b)	((bte) ((a) < (b) ? -1 : (a) > (b)))

static BUN
cmp_typeswitchloop(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		   const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		   bte *dst, BUN cnt, int nonil, const char *func)
{
	BUN nils = 0;
	BUN i, j, k;
	const char *s1, *s2;

	/* bit and oid can only be compared with each other */
	if ((tp1 == TYPE_bit) != (tp2 == TYPE_bit))
		goto unsupported;
	if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
		goto unsupported;

	switch (ATOMstorage(tp1)) {
	case TYPE_void: {
		oid v;

		assert(incr1 == 1);
		assert(tp2 == TYPE_oid || incr2 == 1); /* if void, incr2==1 */
		v = * (const oid *) lft;
		if (v == oid_nil || tp2 == TYPE_void) {
			bte res = v == oid_nil || * (const oid *) rgt == oid_nil ?
				bte_nil :
				CMP(v, * (const oid *) rgt);

			for (k = 0; k < cnt; k++)
				dst[k] = res;
			if (res == bte_nil)
				nils = cnt;
		} else {
			for (j = k = 0; k < cnt; v++, j += incr2, k++) {
				if (((const oid *) rgt)[j] == oid_nil) {
					nils++;
					dst[k] = bte_nil;
				} else {
					dst[k] = CMP(v, ((const oid *) rgt)[j]);
				}
			}
		}
		break;
	}
	case TYPE_bte:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, bte, CMP);
			else
				BINARY_3TYPE_FUNC(bte, bte, bte, CMP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, sht, bte, CMP);
			else
				BINARY_3TYPE_FUNC(bte, sht, bte, CMP);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, int, bte, CMP);
			else
				BINARY_3TYPE_FUNC(bte, int, bte, CMP);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, lng, bte, CMP);
			else
				BINARY_3TYPE_FUNC(bte, lng, bte, CMP);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, flt, bte, CMP);
			else
				BINARY_3TYPE_FUNC(bte, flt, bte, CMP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, dbl, bte, CMP);
			else
				BINARY_3TYPE_FUNC(bte, dbl, bte, CMP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, bte, bte, CMP);
			else
				BINARY_3TYPE_FUNC(sht, bte, bte, CMP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, sht, bte, CMP);
			else
				BINARY_3TYPE_FUNC(sht, sht, bte, CMP);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, int, bte, CMP);
			else
				BINARY_3TYPE_FUNC(sht, int, bte, CMP);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, lng, bte, CMP);
			else
				BINARY_3TYPE_FUNC(sht, lng, bte, CMP);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, flt, bte, CMP);
			else
				BINARY_3TYPE_FUNC(sht, flt, bte, CMP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, dbl, bte, CMP);
			else
				BINARY_3TYPE_FUNC(sht, dbl, bte, CMP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = CMP(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, bte, bte, CMP);
			else
				BINARY_3TYPE_FUNC(int, bte, bte, CMP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, sht, bte, CMP);
			else
				BINARY_3TYPE_FUNC(int, sht, bte, CMP);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, int, bte, CMP);
			else
				BINARY_3TYPE_FUNC(int, int, bte, CMP);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, lng, bte, CMP);
			else
				BINARY_3TYPE_FUNC(int, lng, bte, CMP);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, flt, bte, CMP);
			else
				BINARY_3TYPE_FUNC(int, flt, bte, CMP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, dbl, bte, CMP);
			else
				BINARY_3TYPE_FUNC(int, dbl, bte, CMP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
		if (tp1 == TYPE_oid && tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (v == oid_nil) {
				for (k = 0; k < cnt; k++)
					dst[k] = bit_nil;
				nils = cnt;
			} else {
				for (i = k = 0; k < cnt; i += incr1, v++, k++) {
					if (((const oid *) lft)[i] == oid_nil) {
						nils++;
						dst[k] = bit_nil;
					} else {
						dst[k] = CMP(((const oid *) lft)[i], v);
					}
				}
			}
			break;
		}
#endif
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, bte, bte, CMP);
			else
				BINARY_3TYPE_FUNC(lng, bte, bte, CMP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, sht, bte, CMP);
			else
				BINARY_3TYPE_FUNC(lng, sht, bte, CMP);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, int, bte, CMP);
			else
				BINARY_3TYPE_FUNC(lng, int, bte, CMP);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if ((tp1 == TYPE_oid) != (tp2 == TYPE_oid))
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, lng, bte, CMP);
			else
				BINARY_3TYPE_FUNC(lng, lng, bte, CMP);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, flt, bte, CMP);
			else
				BINARY_3TYPE_FUNC(lng, flt, bte, CMP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, dbl, bte, CMP);
			else
				BINARY_3TYPE_FUNC(lng, dbl, bte, CMP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_flt:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, bte, bte, CMP);
			else
				BINARY_3TYPE_FUNC(flt, bte, bte, CMP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, sht, bte, CMP);
			else
				BINARY_3TYPE_FUNC(flt, sht, bte, CMP);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, int, bte, CMP);
			else
				BINARY_3TYPE_FUNC(flt, int, bte, CMP);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, lng, bte, CMP);
			else
				BINARY_3TYPE_FUNC(flt, lng, bte, CMP);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, flt, bte, CMP);
			else
				BINARY_3TYPE_FUNC(flt, flt, bte, CMP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, dbl, bte, CMP);
			else
				BINARY_3TYPE_FUNC(flt, dbl, bte, CMP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMstorage(tp2)) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, bte, bte, CMP);
			else
				BINARY_3TYPE_FUNC(dbl, bte, bte, CMP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, sht, bte, CMP);
			else
				BINARY_3TYPE_FUNC(dbl, sht, bte, CMP);
			break;
		case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, int, bte, CMP);
			else
				BINARY_3TYPE_FUNC(dbl, int, bte, CMP);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			if (tp2 == TYPE_oid)
				goto unsupported;
#endif
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, lng, bte, CMP);
			else
				BINARY_3TYPE_FUNC(dbl, lng, bte, CMP);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, flt, bte, CMP);
			else
				BINARY_3TYPE_FUNC(dbl, flt, bte, CMP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, dbl, bte, CMP);
			else
				BINARY_3TYPE_FUNC(dbl, dbl, bte, CMP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		if (tp1 != tp2)
			goto unsupported;
		for (i = j = k = 0; k < cnt; i += incr1, j += incr2, k++) {
			s1 = hp1 ? hp1 + VarHeapVal(lft, i, wd1) : (const char *) lft;
			s2 = hp2 ? hp2 + VarHeapVal(rgt, j, wd2) : (const char *) rgt;
			if (s1 == NULL || strcmp(s1, str_nil) == 0 ||
			    s2 == NULL || strcmp(s2, str_nil) == 0) {
				nils++;
				dst[k] = bte_nil;
			} else {
				int x = strcmp(s1, s2);
				dst[k] = (bte) (x < 0 ? -1 : x > 0);
			}
		}
		break;
	default:
		goto unsupported;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

static BAT *
BATcalccmp_intern(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		  const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		  BUN cnt, int nonil, oid seqbase, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	bte *dst;

	bn = BATnew(TYPE_void, TYPE_bte, cnt);
	if (bn == NULL)
		return NULL;

	dst = (bte *) Tloc(bn, bn->U->first);

	nils = cmp_typeswitchloop(lft, tp1, incr1, hp1, wd1,
				  rgt, tp2, incr2, hp2, wd2,
				  dst, cnt, nonil, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn = BATseqbase(bn, seqbase);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	return bn;
}

BAT *
BATcalccmp(BAT *b1, BAT *b2)
{
	BAT *bn;

	BATcheck(b1, "BATcalccmp");
	BATcheck(b2, "BATcalccmp");

	if (checkbats(b1, b2, "BATcalccmp") == GDK_FAIL)
		return NULL;

	if (BATtvoid(b1) && BATtvoid(b2)) {
		bte res;

		if (b1->T->seq == oid_nil || b2->T->seq == oid_nil)
			res = bte_nil;
		else
			res = CMP(b1->T->seq, b2->T->seq);

		return BATconst(b1, TYPE_bte, &res);
	}

	BATaccessBegin(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(b2, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalccmp_intern(Tloc(b1, b1->U->first), b1->T->type, 1,
			       b1->T->vheap ? b1->T->vheap->base : NULL,
			       b1->T->width,
			       Tloc(b2, b2->U->first), b2->T->type, 1,
			       b2->T->vheap ? b2->T->vheap->base : NULL,
			       b2->T->width,
			       b1->U->count,
			       b1->T->nonil && b2->T->nonil,
			       b1->H->seq, "BATcalccmp");

	BATaccessEnd(b1, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b2, USE_TAIL, MMAP_SEQUENTIAL);

	if (b1->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b1, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccmpcst(BAT *b, const ValRecord *v)
{
	BAT *bn;

	BATcheck(b, "BATcalccmpcst");

	if (checkbats(b, NULL, "BATcalccmpcst") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalccmp_intern(Tloc(b, b->U->first), b->T->type, 1,
			       b->T->vheap ? b->T->vheap->base : NULL,
			       b->T->width,
			       (const void *) VALptr((ValPtr) v), v->vtype, 0,
			       NULL, 0,
			       b->U->count,
			       b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			       b->H->seq, "BATcalccmpcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalccstcmp(const ValRecord *v, BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATcalccstcmp");

	if (checkbats(b, NULL, "BATcalccstcmp") == GDK_FAIL)
		return NULL;

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalccmp_intern((const void *) VALptr((ValPtr) v), v->vtype, 0,
			       NULL, 0,
			       Tloc(b, b->U->first), b->T->type, 1,
			       b->T->vheap ? b->T->vheap->base : NULL,
			       b->T->width,
			       b->U->count,
			       b->T->nonil && ATOMcmp(v->vtype, VALptr((ValPtr) v), ATOMnilptr(v->vtype)) != 0,
			       b->H->seq, "BATcalccstcmp");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalccmp(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_bte;
	if (cmp_typeswitchloop((const void *) VALptr((ValPtr) lft), lft->vtype, 0, NULL, 0,
			       (const void *) VALptr((ValPtr) rgt), rgt->vtype, 0, NULL, 0,
			       VALptr(ret), 1, 0, "VARcalccmp") == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define BETWEEN(v, lo, hi, TYPE)					\
	((v) == TYPE##_nil || ((lo) == TYPE##_nil && (hi) == TYPE##_nil) ? \
	 (nils++, bit_nil) :						\
	 (bit) (((lo) == TYPE##_nil || (v) >= (lo)) &&			\
		((hi) == TYPE##_nil || (v) <= (hi))))

#define BETWEEN_LOOP_TYPE(TYPE)						\
	do {								\
		for (i = j = k = l = 0; l < cnt; i += incr1, j += incr2, k += incr3, l++) { \
			dst[l] = BETWEEN(((const TYPE *) src)[i],	\
					 ((const TYPE *) lo)[j],	\
					 ((const TYPE *) hi)[k],	\
					 TYPE);				\
		}							\
	} while (0)

static BAT *
BATcalcbetween_intern(const void *src, int incr1,
		      const void *lo, int incr2,
		      const void *hi, int incr3,
		      int tp, BUN cnt,
		      oid seqbase, const char *func)
{
	BAT *bn;
	BUN nils = 0;
	BUN i, j, k, l;
	bit *dst;

	bn = BATnew(TYPE_void, TYPE_bit, cnt);
	if (bn == NULL)
		return NULL;

	dst = (bit *) Tloc(bn, bn->U->first);

	switch (ATOMstorage(tp)) {
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
	case TYPE_flt:
		BETWEEN_LOOP_TYPE(flt);
		break;
	case TYPE_dbl:
		BETWEEN_LOOP_TYPE(dbl);
		break;
	default:
		BBPunfix(bn->batCacheid);
		GDKerror("%s: bad input type %s.\n", func, ATOMname(tp));
		return NULL;
	}

	BATsetcount(bn, cnt);
	bn = BATseqbase(bn, seqbase);

	bn->T->sorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->revsorted = bn->U->count <= 1 || nils == bn->U->count;
	bn->T->key = bn->U->count <= 1;
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;

	return bn;
}

BAT *
BATcalcbetween(BAT *b, BAT *lo, BAT *hi)
{
	BAT *bn;

	BATcheck(b, "BATcalcbetween");
	BATcheck(lo, "BATcalcbetween");
	BATcheck(hi, "BATcalcbetween");

	if (checkbats(b, lo, "BATcalcbetween") == GDK_FAIL)
		return NULL;
	if (checkbats(b, hi, "BATcalcbetween") == GDK_FAIL)
		return NULL;

	if (b->T->type == TYPE_void &&
	    lo->T->type == TYPE_void &&
	    hi->T->type == TYPE_void) {
		bit res;

		if (b->T->seq == oid_nil ||
		    (lo->T->seq == oid_nil && hi->T->seq == oid_nil))
			res = bit_nil;
		else
			res = (bit) ((lo->T->seq == oid_nil ||
				      b->T->seq >= lo->T->seq) &&
				     (hi->T->seq == oid_nil ||
				      b->T->seq <= hi->T->seq));

		return BATconst(b, TYPE_bit, &res);
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(lo, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(hi, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcbetween_intern(Tloc(b, b->U->first), 1,
			       Tloc(lo, lo->U->first), 1,
			       Tloc(hi, hi->U->first), 1,
			       b->T->type, b->U->count,
			       b->H->seq, "BATcalcbetween");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(lo, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(hi, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcbetweencstcst(BAT *b, const ValRecord *lo, const ValRecord *hi)
{
	BAT *bn;

	BATcheck(b, "BATcalcbetweencstcst");

	if (checkbats(b, NULL, "BATcalcbetweencstcst") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(lo->vtype) ||
	    ATOMstorage(b->T->type) != ATOMstorage(hi->vtype)) {
		GDKerror("BATcalcbetweencstcst: incompatible input types.\n");
		return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcbetween_intern(Tloc(b, b->U->first), 1,
			       (const void *) VALptr((ValPtr) lo), 0,
			       (const void *) VALptr((ValPtr) hi), 0,
			       b->T->type, b->U->count,
			       b->H->seq, "BATcalcbetweencstcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcbetweenbatcst(BAT *b, BAT *lo, const ValRecord *hi)
{
	BAT *bn;

	BATcheck(b, "BATcalcbetweenbatcst");

	if (checkbats(b, lo, "BATcalcbetweenbatcst") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(hi->vtype)) {
		GDKerror("BATcalcbetweenbatcst: incompatible input types.\n");
		return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(lo, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcbetween_intern(Tloc(b, b->U->first), 1,
			       Tloc(lo, lo->U->first), 1,
			       (const void *) VALptr((ValPtr) hi), 0,
			       b->T->type, b->U->count,
			       b->H->seq, "BATcalcbetweenbatcst");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(lo, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

BAT *
BATcalcbetweencstbat(BAT *b, const ValRecord *lo, BAT *hi)
{
	BAT *bn;

	BATcheck(b, "BATcalcbetweencstbat");

	if (checkbats(b, hi, "BATcalcbetweencstbat") == GDK_FAIL)
		return NULL;

	if (ATOMstorage(b->T->type) != ATOMstorage(lo->vtype)) {
		GDKerror("BATcalcbetweencstbat: incompatible input types.\n");
		return NULL;
	}

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(hi, USE_TAIL, MMAP_SEQUENTIAL);

	bn = BATcalcbetween_intern(Tloc(b, b->U->first), 1,
			       (const void *) VALptr((ValPtr) lo), 0,
			       Tloc(hi, hi->U->first), 1,
			       b->T->type, b->U->count,
			       b->H->seq, "BATcalcbetweencstbat");

	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(hi, USE_TAIL, MMAP_SEQUENTIAL);

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;
}

int
VARcalcbetween(ValPtr ret, const ValRecord *v, const ValRecord *lo, const ValRecord *hi)
{
	BUN nils = 0;		/* to make reusing BETWEEN macro easier */

	if (ATOMstorage(v->vtype) != ATOMstorage(lo->vtype) ||
	    ATOMstorage(v->vtype) != ATOMstorage(hi->vtype)) {
		GDKerror("VARcalcbetween: incompatible input types.\n");
		return GDK_FAIL;
	}

	ret->vtype = TYPE_bit;
	switch (ATOMstorage(v->vtype)) {
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
	case TYPE_flt:
		ret->val.btval = BETWEEN(v->val.fval, lo->val.fval, hi->val.fval, flt);
		break;
	case TYPE_dbl:
		ret->val.btval = BETWEEN(v->val.dval, lo->val.dval, hi->val.dval, dbl);
		break;
	default:
		GDKerror("VARcalcbetween: bad input type %s.\n",
			 ATOMname(v->vtype));
		return GDK_FAIL;
	}
	(void) nils;
	return GDK_SUCCEED;
}

#define convertimpl_enlarge(TYPE1, TYPE2)			\
static BUN							\
convert_##TYPE1##_##TYPE2(TYPE1 *src, TYPE2 *dst, BUN cnt)	\
{								\
	BUN nils = 0;						\
								\
	while (cnt-- > 0) {					\
		if (*src == TYPE1##_nil) {			\
			*dst = TYPE2##_nil;			\
			nils++;					\
		} else						\
			*dst = (TYPE2) *src;			\
		src++;						\
		dst++;						\
	}							\
	return nils;						\
}

#define convertimpl_reduce(TYPE1, TYPE2)			\
static BUN							\
convert_##TYPE1##_##TYPE2(TYPE1 *src, TYPE2 *dst, BUN cnt,	\
			  int abort_on_error)		\
{								\
	BUN nils = 0;						\
								\
	while (cnt-- > 0) {					\
		if (*src == TYPE1##_nil) {			\
			*dst = TYPE2##_nil;			\
			nils++;					\
		} else if (*src <= (TYPE1) GDK_##TYPE2##_min ||	\
			   *src > (TYPE1) GDK_##TYPE2##_max) {	\
			if (abort_on_error)			\
				return BUN_NONE;		\
			*dst = TYPE2##_nil;			\
			nils++;					\
		} else						\
			*dst = (TYPE2) *src;			\
		src++;						\
		dst++;						\
	}							\
	return nils;						\
}

#define convert2bit_impl(TYPE)				\
static BUN						\
convert_##TYPE##_bit(TYPE *src, bit *dst, BUN cnt)	\
{							\
	BUN nils = 0;					\
							\
	while (cnt-- > 0) {				\
		if (*src == TYPE##_nil) {		\
			*dst = bit_nil;			\
			nils++;				\
		} else					\
			*dst = (bit) (*src != 0);	\
		src++;					\
		dst++;					\
	}						\
	return nils;					\
}

convertimpl_enlarge(bte, sht)
convertimpl_enlarge(bte, int)
convertimpl_enlarge(bte, lng)
convertimpl_enlarge(bte, flt)
convertimpl_enlarge(bte, dbl)

convertimpl_reduce(sht, bte)
convertimpl_enlarge(sht, int)
convertimpl_enlarge(sht, lng)
convertimpl_enlarge(sht, flt)
convertimpl_enlarge(sht, dbl)

convertimpl_reduce(int, bte)
convertimpl_reduce(int, sht)
convertimpl_enlarge(int, lng)
convertimpl_enlarge(int, flt)
convertimpl_enlarge(int, dbl)

convertimpl_reduce(lng, bte)
convertimpl_reduce(lng, sht)
convertimpl_reduce(lng, int)
convertimpl_enlarge(lng, flt)
convertimpl_enlarge(lng, dbl)

convertimpl_reduce(flt, bte)
convertimpl_reduce(flt, sht)
convertimpl_reduce(flt, int)
convertimpl_reduce(flt, lng)
convertimpl_enlarge(flt, dbl)

convertimpl_reduce(dbl, bte)
convertimpl_reduce(dbl, sht)
convertimpl_reduce(dbl, int)
convertimpl_reduce(dbl, lng)
convertimpl_reduce(dbl, flt)

convert2bit_impl(bte)
convert2bit_impl(sht)
convert2bit_impl(int)
convert2bit_impl(lng)
convert2bit_impl(flt)
convert2bit_impl(dbl)

static BUN
convert_any_str(int tp, void *src, BAT *bn, BUN cnt)
{
	str dst = 0;
	int len = 0;
	BUN nils = 0;
	BUN i;
	void *nil = ATOMnilptr(tp);
	int (*atomtostr)(str *, int *, ptr) = BATatoms[tp].atomToStr;

	for (i = 0; i < cnt; i++) {
		(*atomtostr)(&dst, &len, src);
		if (ATOMcmp(tp, src, nil) == 0)
			nils++;
		tfastins_nocheck(bn, i, dst, bn->T->width);
	}
	BATsetcount(bn, cnt);
	if (dst)
		GDKfree(dst);
	return nils;
  bunins_failed:
	if (dst)
		GDKfree(dst);
	return BUN_NONE;
}

static BUN
convert_str_any(BAT *b, int tp, void *dst, int abort_on_error)
{
	BUN i, j;
	BUN nils = 0;
	void *nil = ATOMnilptr(tp);
	char *s;
	ptr d;
	int len = ATOMsize(tp);
	int (*atomfromstr)(str, int *, ptr *) = BATatoms[tp].atomFromStr;
	BATiter bi = bat_iterator(b);

	BATloop(b, i, j) {
		s = BUNtail(bi, i);
		d = dst;
		if ((*atomfromstr)(s, &len, &d) <= 0) {
			if (abort_on_error)
				return BUN_NONE;
			memcpy(dst, nil, len);
		}
		assert(len == ATOMsize(tp));
		if (ATOMcmp(tp, dst, nil) == 0)
			nils++;
		dst = (void *) ((char *) dst + len);
	}
	return nils;
}

BAT *
BATconvert(BAT *b, int tp, int abort_on_error)
{
	BAT *bn;
	void *src, *dst;
	BUN nils = 0;	/* in case no conversion defined */

	assert(ATOMstorage(TYPE_wrd) == ATOMstorage(TYPE_int) ||
	       ATOMstorage(TYPE_wrd) == ATOMstorage(TYPE_lng));
	assert(ATOMstorage(TYPE_oid) == ATOMstorage(TYPE_int) ||
	       ATOMstorage(TYPE_oid) == ATOMstorage(TYPE_lng));

	BATcheck(b, "BATconvert");
	if (tp == TYPE_void)
		tp = TYPE_oid;

	if (tp != TYPE_bit && ATOMstorage(b->T->type) == ATOMstorage(tp))
		return BATcopy(b, b->H->type, tp, 0);

	bn = BATnew(TYPE_void, tp, b->U->count);
	if (bn == NULL)
		return NULL;

	src = (void *) Tloc(b, b->U->first);
	dst = (void *) Tloc(bn, bn->U->first);

	switch (ATOMstorage(b->T->type)) {
	case TYPE_bte:
		switch (ATOMstorage(tp)) {
		case TYPE_sht:
			nils = convert_bte_sht(src, dst, b->U->count);
			break;
		case TYPE_int:
			nils = convert_bte_int(src, dst, b->U->count);
			break;
		case TYPE_lng:
			nils = convert_bte_lng(src, dst, b->U->count);
			break;
		case TYPE_flt:
			nils = convert_bte_flt(src, dst, b->U->count);
			break;
		case TYPE_dbl:
			nils = convert_bte_dbl(src, dst, b->U->count);
			break;
		case TYPE_str:
			nils = convert_any_str(b->T->type, src, bn, b->U->count);
			break;
		case TYPE_bte:
			assert(tp == TYPE_bit);
			nils = convert_bte_bit(src, dst, b->U->count);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMstorage(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit) {
				nils = convert_sht_bit(src, dst, b->U->count);
				break;
			}
			nils = convert_sht_bte(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_int:
			nils = convert_sht_int(src, dst, b->U->count);
			break;
		case TYPE_lng:
			nils = convert_sht_lng(src, dst, b->U->count);
			break;
		case TYPE_flt:
			nils = convert_sht_flt(src, dst, b->U->count);
			break;
		case TYPE_dbl:
			nils = convert_sht_dbl(src, dst, b->U->count);
			break;
		case TYPE_str:
			nils = convert_any_str(b->T->type, src, bn, b->U->count);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (ATOMstorage(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit) {
				nils = convert_int_bit(src, dst, b->U->count);
				break;
			}
			nils = convert_int_bte(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_sht:
			nils = convert_int_sht(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_lng:
			nils = convert_int_lng(src, dst, b->U->count);
			break;
		case TYPE_flt:
			nils = convert_int_flt(src, dst, b->U->count);
			break;
		case TYPE_dbl:
			nils = convert_int_dbl(src, dst, b->U->count);
			break;
		case TYPE_str:
			nils = convert_any_str(b->T->type, src, bn, b->U->count);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (ATOMstorage(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit) {
				nils = convert_lng_bit(src, dst, b->U->count);
				break;
			}
			nils = convert_lng_bte(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_sht:
			nils = convert_lng_sht(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_int:
			nils = convert_lng_int(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_flt:
			nils = convert_lng_flt(src, dst, b->U->count);
			break;
		case TYPE_dbl:
			nils = convert_lng_dbl(src, dst, b->U->count);
			break;
		case TYPE_str:
			nils = convert_any_str(b->T->type, src, bn, b->U->count);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_flt:
		switch (ATOMstorage(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit) {
				nils = convert_flt_bit(src, dst, b->U->count);
				break;
			}
			nils = convert_flt_bte(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_sht:
			nils = convert_flt_sht(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_int:
			nils = convert_flt_int(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_lng:
			nils = convert_flt_lng(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_dbl:
			nils = convert_flt_dbl(src, dst, b->U->count);
			break;
		case TYPE_str:
			nils = convert_any_str(b->T->type, src, bn, b->U->count);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMstorage(tp)) {
		case TYPE_bte:
			if (tp == TYPE_bit) {
				nils = convert_dbl_bit(src, dst, b->U->count);
				break;
			}
			nils = convert_dbl_bte(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_sht:
			nils = convert_dbl_sht(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_int:
			nils = convert_dbl_int(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_lng:
			nils = convert_dbl_lng(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_flt:
			nils = convert_dbl_flt(src, dst, b->U->count,
					       abort_on_error);
			break;
		case TYPE_str:
			nils = convert_any_str(b->T->type, src, bn, b->U->count);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		nils = convert_str_any(b, b->T->type, dst, abort_on_error);
		break;
	default:
		goto unsupported;
	}

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		if (b->T->type)
			GDKerror("22018!conversion from string to type %s failed.\n",
				 ATOMname(tp));
		else
			GDKerror("22003!overflow in conversion.\n");
		return NULL;
	}

	BATsetcount(bn, b->U->count);
	bn = BATseqbase(bn, b->H->seq);

	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	bn->T->sorted = nils == 0 && b->T->sorted;
	bn->T->revsorted = nils == 0 && b->T->revsorted;
	bn->T->key = (b->T->key & 1) && nils <= 1;

	if (b->H->type != bn->H->type) {
		BAT *bnn = VIEWcreate(b, bn);
		BBPunfix(bn->batCacheid);
		bn = bnn;
	}

	return bn;

  unsupported:
	BBPunfix(bn->batCacheid);
	GDKerror("BATconvert: type combination (convert(%s)->%s) not supported.\n", ATOMname(b->T->type), ATOMname(tp));
	return NULL;
}

int
VARconvert(ValPtr ret, const ValRecord *v, int abort_on_error)
{
	ptr p;

	switch (ATOMstorage(ret->vtype)) {
	case TYPE_bte:
		ret->len = (int) sizeof(ret->val.btval);
		switch (ATOMstorage(v->vtype)) {
		case TYPE_void:
			ret->val.btval = bte_nil;
			break;
		case TYPE_bte:
			if (v->val.btval == bte_nil) {
				ret->val.btval = bte_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.btval = (bte) (v->val.btval != 0);
			} else {
				ret->val.btval = (bte) v->val.btval;
			}
			break;
		case TYPE_sht:
			if (v->val.shval == sht_nil) {
				ret->val.btval = bte_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.btval = (bte) (v->val.shval != 0);
			} else if (v->val.shval <= (sht) GDK_bte_min ||
				   v->val.shval > (sht) GDK_bte_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.btval = bte_nil;
			} else {
				ret->val.btval = (bte) v->val.shval;
			}
			break;
		case TYPE_int:
			if (v->val.ival == int_nil) {
				ret->val.btval = bte_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.btval = (bte) (v->val.ival != 0);
			} else if (v->val.ival <= (int) GDK_bte_min ||
				   v->val.ival > (int) GDK_bte_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.btval = bte_nil;
			} else {
				ret->val.btval = (bte) v->val.ival;
			}
			break;
		case TYPE_lng:
			if (v->val.lval == lng_nil) {
				ret->val.btval = bte_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.btval = (bte) (v->val.lval != 0);
			} else if (v->val.lval <= (lng) GDK_bte_min ||
				   v->val.lval > (lng) GDK_bte_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.btval = bte_nil;
			} else {
				ret->val.btval = (bte) v->val.lval;
			}
			break;
		case TYPE_flt:
			if (v->val.fval == flt_nil) {
				ret->val.btval = bte_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.btval = (bte) (v->val.fval != 0);
			} else if (v->val.fval <= (flt) GDK_bte_min ||
				   v->val.fval > (flt) GDK_bte_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.btval = bte_nil;
			} else {
				ret->val.btval = (bte) v->val.fval;
			}
			break;
		case TYPE_dbl:
			if (v->val.dval == dbl_nil) {
				ret->val.btval = bte_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.btval = (bte) (v->val.dval != 0);
			} else if (v->val.dval <= (dbl) GDK_bte_min ||
				   v->val.dval > (dbl) GDK_bte_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.btval = bte_nil;
			} else {
				ret->val.btval = (bte) v->val.dval;
			}
			break;
		case TYPE_str:
			if (v->val.sval == NULL ||
			    strcmp(v->val.sval, str_nil) == 0) {
				ret->val.btval = bte_nil;
			} else {
				p = &ret->val.btval;
				if ((*BATatoms[ret->vtype].atomFromStr)(v->val.sval, &ret->len, &p) <= 0) {
					if (abort_on_error)
						goto strconvert;
					ret->val.btval = bte_nil;
				}
				assert(p == &ret->val.btval);
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		ret->len = (int) sizeof(ret->val.shval);
		switch (ATOMstorage(v->vtype)) {
		case TYPE_void:
			ret->val.shval = sht_nil;
			break;
		case TYPE_bte:
			if (v->val.btval == bte_nil) {
				ret->val.shval = sht_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.shval = (sht) v->val.btval != 0;
			} else {
				ret->val.shval = (sht) v->val.btval;
			}
			break;
		case TYPE_sht:
			ret->val.shval = v->val.shval;
			break;
		case TYPE_int:
			if (v->val.ival == int_nil) {
				ret->val.shval = sht_nil;
			} else if (v->val.ival <= (int) GDK_sht_min ||
				   v->val.ival > (int) GDK_sht_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.shval = sht_nil;
			} else {
				ret->val.shval = (sht) v->val.ival;
			}
			break;
		case TYPE_lng:
			if (v->val.lval == lng_nil) {
				ret->val.shval = sht_nil;
			} else if (v->val.lval <= (lng) GDK_sht_min ||
				   v->val.lval > (lng) GDK_sht_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.shval = sht_nil;
			} else {
				ret->val.shval = (sht) v->val.lval;
			}
			break;
		case TYPE_flt:
			if (v->val.fval == flt_nil) {
				ret->val.shval = sht_nil;
			} else if (v->val.fval <= (flt) GDK_sht_min ||
				   v->val.fval > (flt) GDK_sht_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.shval = sht_nil;
			} else {
				ret->val.shval = (sht) v->val.fval;
			}
			break;
		case TYPE_dbl:
			if (v->val.dval == dbl_nil) {
				ret->val.shval = sht_nil;
			} else if (v->val.dval <= (dbl) GDK_sht_min ||
				   v->val.dval > (dbl) GDK_sht_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.shval = sht_nil;
			} else {
				ret->val.shval = (sht) v->val.dval;
			}
			break;
		case TYPE_str:
			if (v->val.sval == NULL ||
			    strcmp(v->val.sval, str_nil) == 0) {
				ret->val.shval = sht_nil;
			} else {
				p = &ret->val.shval;
				if ((*BATatoms[ret->vtype].atomFromStr)(v->val.sval, &ret->len, &p) <= 0) {
					if (abort_on_error)
						goto strconvert;
					ret->val.shval = sht_nil;
				}
				assert(p == &ret->val.shval);
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		ret->len = (int) sizeof(ret->val.ival);
		switch (ATOMstorage(v->vtype)) {
		case TYPE_void:
			ret->val.ival = int_nil;
			break;
		case TYPE_bte:
			if (v->val.btval == bte_nil) {
				ret->val.ival = int_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.ival = (int) v->val.btval != 0;
			} else {
				ret->val.ival = (int) v->val.btval;
			}
			break;
		case TYPE_sht:
			if (v->val.shval == sht_nil) {
				ret->val.ival = int_nil;
			} else {
				ret->val.ival = (int) v->val.shval;
			}
			break;
		case TYPE_int:
			ret->val.ival = v->val.ival;
			break;
		case TYPE_lng:
			if (v->val.lval == lng_nil) {
				ret->val.ival = int_nil;
			} else if (v->val.lval <= (lng) GDK_int_min ||
				   v->val.lval > (lng) GDK_int_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.ival = int_nil;
			} else {
				ret->val.ival = (int) v->val.lval;
			}
			break;
		case TYPE_flt:
			if (v->val.fval == flt_nil) {
				ret->val.ival = int_nil;
			} else if (v->val.fval <= (flt) GDK_int_min ||
				   v->val.fval > (flt) GDK_int_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.ival = int_nil;
			} else {
				ret->val.ival = (int) v->val.fval;
			}
			break;
		case TYPE_dbl:
			if (v->val.dval == dbl_nil) {
				ret->val.ival = int_nil;
			} else if (v->val.dval <= (dbl) GDK_int_min ||
				   v->val.dval > (dbl) GDK_int_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.ival = int_nil;
			} else {
				ret->val.ival = (int) v->val.dval;
			}
			break;
		case TYPE_str:
			if (v->val.sval == NULL ||
			    strcmp(v->val.sval, str_nil) == 0) {
				ret->val.ival = int_nil;
			} else {
				p = &ret->val.ival;
				if ((*BATatoms[ret->vtype].atomFromStr)(v->val.sval, &ret->len, &p) <= 0) {
					if (abort_on_error)
						goto strconvert;
					ret->val.ival = int_nil;
				}
				assert(p == &ret->val.ival);
			}
			break;
		default:
			goto unsupported;
		}
#if SIZEOF_OID == SIZEOF_INT
		if (ret->vtype == TYPE_oid &&
		    ret->val.ival != int_nil &&
		    ret->val.ival < 0) {
			if (abort_on_error)
				goto overflow;
			ret->val.oval = oid_nil;
		}
#endif
		break;
	case TYPE_lng:
		ret->len = (int) sizeof(ret->val.lval);
		switch (ATOMstorage(v->vtype)) {
		case TYPE_void:
			ret->val.lval = lng_nil;
			break;
		case TYPE_bte:
			if (v->val.btval == bte_nil) {
				ret->val.lval = lng_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.lval = (lng) v->val.btval != 0;
			} else {
				ret->val.lval = (lng) v->val.btval;
			}
			break;
		case TYPE_sht:
			if (v->val.shval == sht_nil) {
				ret->val.lval = lng_nil;
			} else {
				ret->val.lval = (lng) v->val.shval;
			}
			break;
		case TYPE_int:
			if (v->val.ival == int_nil) {
				ret->val.lval = lng_nil;
			} else {
				ret->val.lval = (lng) v->val.ival;
			}
			break;
		case TYPE_lng:
			ret->val.lval = v->val.lval;
			break;
		case TYPE_flt:
			if (v->val.fval == flt_nil) {
				ret->val.lval = lng_nil;
			} else if (v->val.fval <= (flt) GDK_lng_min ||
				   v->val.fval > (flt) GDK_lng_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.lval = lng_nil;
			} else {
				ret->val.lval = (lng) v->val.fval;
			}
			break;
		case TYPE_dbl:
			if (v->val.dval == dbl_nil) {
				ret->val.lval = lng_nil;
			} else if (v->val.dval <= (dbl) GDK_lng_min ||
				   v->val.dval > (dbl) GDK_lng_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.lval = lng_nil;
			} else {
				ret->val.lval = (lng) v->val.dval;
				/* it can still go wrong...
				 * select cast(power(2,63) as bigint)
				 */
				if (ret->val.lval == lng_nil && abort_on_error)
					goto overflow;
			}
			break;
		case TYPE_str:
			if (v->val.sval == NULL ||
			    strcmp(v->val.sval, str_nil) == 0) {
				ret->val.lval = lng_nil;
			} else {
				p = &ret->val.lval;
				if ((*BATatoms[ret->vtype].atomFromStr)(v->val.sval, &ret->len, &p) <= 0) {
					if (abort_on_error)
						goto strconvert;
					ret->val.lval = lng_nil;
				}
				assert(p == &ret->val.lval);
			}
			break;
		default:
			goto unsupported;
		}
#if SIZEOF_OID == SIZEOF_LNG
		if (ret->vtype == TYPE_oid &&
		    ret->val.lval != lng_nil &&
		    ret->val.lval < 0) {
			if (abort_on_error)
				goto overflow;
			ret->val.oval = oid_nil;
		}
#endif
		break;
	case TYPE_flt:
		ret->len = (int) sizeof(ret->val.fval);
		switch (ATOMstorage(v->vtype)) {
		case TYPE_void:
			ret->val.fval = flt_nil;
			break;
		case TYPE_bte:
			if (v->val.btval == bte_nil) {
				ret->val.fval = flt_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.fval = (flt) v->val.btval != 0;
			} else {
				ret->val.fval = (flt) v->val.btval;
			}
			break;
		case TYPE_sht:
			if (v->val.shval == sht_nil) {
				ret->val.fval = flt_nil;
			} else {
				ret->val.fval = (flt) v->val.shval;
			}
			break;
		case TYPE_int:
			if (v->val.ival == int_nil) {
				ret->val.fval = flt_nil;
			} else {
				ret->val.fval = (flt) v->val.ival;
			}
			break;
		case TYPE_lng:
			if (v->val.lval == lng_nil) {
				ret->val.fval = flt_nil;
			} else {
				ret->val.fval = (flt) v->val.lval;
			}
			break;
		case TYPE_flt:
			ret->val.fval = v->val.fval;
			break;
		case TYPE_dbl:
			if (v->val.dval == dbl_nil) {
				ret->val.fval = flt_nil;
			} else if (v->val.dval <= (dbl) GDK_flt_min ||
				   v->val.dval > (dbl) GDK_flt_max) {
				if (abort_on_error)
					goto overflow;
				ret->val.fval = flt_nil;
			} else {
				ret->val.fval = (flt) v->val.dval;
			}
			break;
		case TYPE_str:
			if (v->val.sval == NULL ||
			    strcmp(v->val.sval, str_nil) == 0) {
				ret->val.fval = flt_nil;
			} else {
				p = &ret->val.fval;
				if ((*BATatoms[ret->vtype].atomFromStr)(v->val.sval, &ret->len, &p) <= 0) {
					if (abort_on_error)
						goto strconvert;
					ret->val.fval = flt_nil;
				}
				assert(p == &ret->val.fval);
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		ret->len = (int) sizeof(ret->val.dval);
		switch (ATOMstorage(v->vtype)) {
		case TYPE_void:
			ret->val.dval = dbl_nil;
			break;
		case TYPE_bte:
			if (v->val.btval == bte_nil) {
				ret->val.dval = dbl_nil;
			} else if (ret->vtype == TYPE_bit) {
				ret->val.dval = (dbl) v->val.btval != 0;
			} else {
				ret->val.dval = (dbl) v->val.btval;
			}
			break;
		case TYPE_sht:
			if (v->val.shval == sht_nil) {
				ret->val.dval = dbl_nil;
			} else {
				ret->val.dval = (dbl) v->val.shval;
			}
			break;
		case TYPE_int:
			if (v->val.ival == int_nil) {
				ret->val.dval = dbl_nil;
			} else {
				ret->val.dval = (dbl) v->val.ival;
			}
			break;
		case TYPE_lng:
			if (v->val.lval == lng_nil) {
				ret->val.dval = dbl_nil;
			} else {
				ret->val.dval = (dbl) v->val.lval;
			}
			break;
		case TYPE_flt:
			if (v->val.fval == flt_nil) {
				ret->val.dval = dbl_nil;
			} else {
				ret->val.dval = (dbl) v->val.fval;
			}
			break;
		case TYPE_dbl:
			ret->val.dval = v->val.dval;
			break;
		case TYPE_str:
			if (v->val.sval == NULL ||
			    strcmp(v->val.sval, str_nil) == 0) {
				ret->val.dval = dbl_nil;
			} else {
				p = &ret->val.dval;
				if ((*BATatoms[ret->vtype].atomFromStr)(v->val.sval, &ret->len, &p) <= 0) {
					if (abort_on_error)
						goto strconvert;
					ret->val.dval = dbl_nil;
				}
				assert(p == &ret->val.dval);
			}
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_str:
		if ((*BATatoms[v->vtype].atomCmp)(VALptr((ValPtr) v),
						  ATOMnilptr(v->vtype)) == 0) {
			ret->val.sval = GDKstrdup(str_nil);
		} else if (v->vtype == TYPE_str) {
			ret->val.sval = GDKstrdup(v->val.sval);
		} else {
			ret->val.sval = NULL;
			(*BATatoms[v->vtype].atomToStr)(&ret->val.sval, &ret->len, VALptr((ValPtr) v));
		}
		break;
	default:
		goto unsupported;
	}
	return GDK_SUCCEED;

  overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
  strconvert:
	GDKerror("22018!conversion of string '%s' to type %s failed.\n",
		 v->val.sval, ATOMname(ret->vtype));
	return GDK_FAIL;
  unsupported:
	GDKerror("VARconvert: conversion from type %s to type %s unsupported.\n",
		 ATOMname(v->vtype), ATOMname(ret->vtype));
	return GDK_FAIL;
}

/* signed version of BUN */
#if SIZEOF_BUN == SIZEOF_INT
#define SBUN	int
#else
#define SBUN	lng
#endif

#define AVERAGE_TYPE(TYPE)						\
	do {								\
		TYPE a = 0, x, an, xn, z1;				\
		for (i = 0; i < cnt; i++) {				\
			x = ((TYPE *) src)[i];				\
			if (x == TYPE##_nil)				\
				continue;				\
			n++;						\
			/* calculate z1 = (x - a) / n, rounded down	\
			 * (towards \ negative infinity), and		\
			 * calculate z2 = remainder of \ the division	\
			 * (i.e. 0 <= z2 < n); do this without		\
			 * causing overflow */				\
			an = (TYPE) (a / (SBUN) n);			\
			xn = (TYPE) (x / (SBUN) n);			\
			/* z1 will be (x - a) / n rounded towards -INF */ \
			z1 = xn - an;					\
			xn = x - (TYPE) (xn * (SBUN) n);		\
			an = a - (TYPE) (an * (SBUN) n);		\
			/* z2 will be remainder of above division */	\
			if (xn >= an) {					\
				z2 = (BUN) (xn - an);			\
				/* loop invariant: (x - a) - z1 * n == z2 */ \
				while (z2 >= n) {			\
					z2 -= n;			\
					z1++;				\
				}					\
			} else {					\
				z2 = (BUN) (an - xn);			\
				/* loop invariant (until we break): (x - a) - z1 * n == -z2 */ \
				for (;;) {				\
					z1--;				\
					if (z2 < n) {			\
						z2 = n - z2; /* proper remainder */ \
						break;			\
					}				\
					z2 -= n;			\
				}					\
			}						\
			a += z1;					\
			r += z2;					\
			if (r >= n) {					\
				r -= n;					\
				a++;					\
			}						\
		}							\
		*avg = n > 0 ? a + (dbl) r / n : dbl_nil;		\
	} while (0)

#define AVERAGE_FLOATTYPE(TYPE)						\
	do {								\
		double a = 0;						\
		TYPE x;							\
		for (i = 0; i < cnt; i++) {				\
			x = ((TYPE *) src)[i];				\
			if (x == TYPE##_nil)				\
				continue;				\
			n++;						\
			if ((a > 0) == (x > 0)) {			\
				a += (x - a) / (SBUN) n;		\
			} else {					\
				/* no overflow at the cost of an extra	\
				 * division and slight loss of		\
				 * precision */				\
				a = a - a / (SBUN) n + x / (SBUN) n;	\
			}						\
		}							\
		*avg = n > 0 ? a : dbl_nil;				\
	} while (0)

int
BATcalcavg(BAT *b, dbl *avg, BUN *vals)
{
	BUN n = 0, r = 0, i = 0, z2, cnt;
	void *src;

	src = Tloc(b, b->U->first);
	cnt = BATcount(b);

	BATaccessBegin(b, USE_TAIL, MMAP_SEQUENTIAL);
	switch (ATOMstorage(b->T->type)) {
	case TYPE_bte:
		AVERAGE_TYPE(bte);
		break;
	case TYPE_sht:
		AVERAGE_TYPE(sht);
		break;
	case TYPE_int:
		AVERAGE_TYPE(int);
		break;
	case TYPE_lng:
		AVERAGE_TYPE(lng);
		break;
	case TYPE_flt:
		AVERAGE_FLOATTYPE(flt);
		break;
	case TYPE_dbl:
		AVERAGE_FLOATTYPE(dbl);
		break;
	default:
		GDKerror("BATcalcavg: average of type %s unsupported.\n",
			 ATOMname(b->T->type));
		return GDK_FAIL;
	}
	BATaccessEnd(b, USE_TAIL, MMAP_SEQUENTIAL);
	if (vals)
		*vals = n;
	return GDK_SUCCEED;
}
