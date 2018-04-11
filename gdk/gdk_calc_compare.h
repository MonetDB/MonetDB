/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* this file is included multiple times by gdk_calc.c */

static BUN
op_typeswitchloop(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		  const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		  TPE *restrict dst, BUN cnt, BUN start, BUN end, const oid *restrict cand,
		  const oid *candend, oid candoff, int nonil, const char *func)
{
	BUN nils = 0;
	BUN i, j, k, loff = 0, roff = 0;
	const void *restrict nil;
	int (*atomcmp)(const void *, const void *);

	switch (tp1) {
	case TYPE_void: {
		oid v = oid_nil;

		assert(incr1 == 1);
		assert(tp2 == TYPE_oid || incr2 == 1); /* if void, incr2==1 */
		if (lft)
			v = * (const oid *) lft;
		CANDLOOP(dst, k, TPE_nil, 0, start);
		if (is_oid_nil(v) || tp2 == TYPE_void) {
			TPE res = is_oid_nil(v) || is_oid_nil(* (const oid *) rgt) ?
				TPE_nil :
				OP(v, * (const oid *) rgt);

			if (is_TPE_nil(res) || cand == NULL) {
				for (k = start; k < end; k++)
					dst[k] = res;
				if (is_TPE_nil(res))
					nils = end - start;
			} else {
				for (k = start; k < end; k++) {
					CHECKCAND(dst, k, candoff, TPE_nil);
					dst[k] = res;
				}
			}
		} else {
			for (v += start, j = start * incr2, k = start;
			     k < end;
			     v++, j += incr2, k++) {
				CHECKCAND(dst, k, candoff, TPE_nil);
				if (is_oid_nil(((const oid *) rgt)[j])) {
					nils++;
					dst[k] = TPE_nil;
				} else {
					dst[k] = OP(v, ((const oid *) rgt)[j]);
				}
			}
		}
		CANDLOOP(dst, k, TPE_nil, end, cnt);
		break;
	}
	case TYPE_bit:
		if (tp2 != TYPE_bit)
			goto unsupported;
		if (nonil)
			BINARY_3TYPE_FUNC_nonil(bit, bit, TPE, OP);
		else
			BINARY_3TYPE_FUNC(bit, bit, TPE, OP);
		break;
	case TYPE_bte:
		switch (tp2) {
		case TYPE_bte:
		btebte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, bte, TPE, OP);
			else
				BINARY_3TYPE_FUNC(bte, bte, TPE, OP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, sht, TPE, OP);
			else
				BINARY_3TYPE_FUNC(bte, sht, TPE, OP);
			break;
		case TYPE_int:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, int, TPE, OP);
			else
				BINARY_3TYPE_FUNC(bte, int, TPE, OP);
			break;
		case TYPE_lng:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, lng, TPE, OP);
			else
				BINARY_3TYPE_FUNC(bte, lng, TPE, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, hge, TPE, OP);
			else
				BINARY_3TYPE_FUNC(bte, hge, TPE, OP);
			break;
#endif
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, flt, TPE, OP);
			else
				BINARY_3TYPE_FUNC(bte, flt, TPE, OP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(bte, dbl, TPE, OP);
			else
				BINARY_3TYPE_FUNC(bte, dbl, TPE, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (tp2) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, bte, TPE, OP);
			else
				BINARY_3TYPE_FUNC(sht, bte, TPE, OP);
			break;
		case TYPE_sht:
		shtsht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, sht, TPE, OP);
			else
				BINARY_3TYPE_FUNC(sht, sht, TPE, OP);
			break;
		case TYPE_int:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, int, TPE, OP);
			else
				BINARY_3TYPE_FUNC(sht, int, TPE, OP);
			break;
		case TYPE_lng:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, lng, TPE, OP);
			else
				BINARY_3TYPE_FUNC(sht, lng, TPE, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, hge, TPE, OP);
			else
				BINARY_3TYPE_FUNC(sht, hge, TPE, OP);
			break;
#endif
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, flt, TPE, OP);
			else
				BINARY_3TYPE_FUNC(sht, flt, TPE, OP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(sht, dbl, TPE, OP);
			else
				BINARY_3TYPE_FUNC(sht, dbl, TPE, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (tp2) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, bte, TPE, OP);
			else
				BINARY_3TYPE_FUNC(int, bte, TPE, OP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, sht, TPE, OP);
			else
				BINARY_3TYPE_FUNC(int, sht, TPE, OP);
			break;
		case TYPE_int:
		intint:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, int, TPE, OP);
			else
				BINARY_3TYPE_FUNC(int, int, TPE, OP);
			break;
		case TYPE_lng:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, lng, TPE, OP);
			else
				BINARY_3TYPE_FUNC(int, lng, TPE, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, hge, TPE, OP);
			else
				BINARY_3TYPE_FUNC(int, hge, TPE, OP);
			break;
#endif
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, flt, TPE, OP);
			else
				BINARY_3TYPE_FUNC(int, flt, TPE, OP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(int, dbl, TPE, OP);
			else
				BINARY_3TYPE_FUNC(int, dbl, TPE, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (tp2) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, bte, TPE, OP);
			else
				BINARY_3TYPE_FUNC(lng, bte, TPE, OP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, sht, TPE, OP);
			else
				BINARY_3TYPE_FUNC(lng, sht, TPE, OP);
			break;
		case TYPE_int:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, int, TPE, OP);
			else
				BINARY_3TYPE_FUNC(lng, int, TPE, OP);
			break;
		case TYPE_lng:
		lnglng:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, lng, TPE, OP);
			else
				BINARY_3TYPE_FUNC(lng, lng, TPE, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, hge, TPE, OP);
			else
				BINARY_3TYPE_FUNC(lng, hge, TPE, OP);
			break;
#endif
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, flt, TPE, OP);
			else
				BINARY_3TYPE_FUNC(lng, flt, TPE, OP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(lng, dbl, TPE, OP);
			else
				BINARY_3TYPE_FUNC(lng, dbl, TPE, OP);
			break;
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (tp2) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(hge, bte, TPE, OP);
			else
				BINARY_3TYPE_FUNC(hge, bte, TPE, OP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(hge, sht, TPE, OP);
			else
				BINARY_3TYPE_FUNC(hge, sht, TPE, OP);
			break;
		case TYPE_int:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(hge, int, TPE, OP);
			else
				BINARY_3TYPE_FUNC(hge, int, TPE, OP);
			break;
		case TYPE_lng:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(hge, lng, TPE, OP);
			else
				BINARY_3TYPE_FUNC(hge, lng, TPE, OP);
			break;
		case TYPE_hge:
		hgehge:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(hge, hge, TPE, OP);
			else
				BINARY_3TYPE_FUNC(hge, hge, TPE, OP);
			break;
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(hge, flt, TPE, OP);
			else
				BINARY_3TYPE_FUNC(hge, flt, TPE, OP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(hge, dbl, TPE, OP);
			else
				BINARY_3TYPE_FUNC(hge, dbl, TPE, OP);
			break;
		default:
			goto unsupported;
		}
		break;
#endif
	case TYPE_flt:
		switch (tp2) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, bte, TPE, OP);
			else
				BINARY_3TYPE_FUNC(flt, bte, TPE, OP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, sht, TPE, OP);
			else
				BINARY_3TYPE_FUNC(flt, sht, TPE, OP);
			break;
		case TYPE_int:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, int, TPE, OP);
			else
				BINARY_3TYPE_FUNC(flt, int, TPE, OP);
			break;
		case TYPE_lng:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, lng, TPE, OP);
			else
				BINARY_3TYPE_FUNC(flt, lng, TPE, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, hge, TPE, OP);
			else
				BINARY_3TYPE_FUNC(flt, hge, TPE, OP);
			break;
#endif
		case TYPE_flt:
		fltflt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, flt, TPE, OP);
			else
				BINARY_3TYPE_FUNC(flt, flt, TPE, OP);
			break;
		case TYPE_dbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(flt, dbl, TPE, OP);
			else
				BINARY_3TYPE_FUNC(flt, dbl, TPE, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (tp2) {
		case TYPE_bte:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, bte, TPE, OP);
			else
				BINARY_3TYPE_FUNC(dbl, bte, TPE, OP);
			break;
		case TYPE_sht:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, sht, TPE, OP);
			else
				BINARY_3TYPE_FUNC(dbl, sht, TPE, OP);
			break;
		case TYPE_int:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, int, TPE, OP);
			else
				BINARY_3TYPE_FUNC(dbl, int, TPE, OP);
			break;
		case TYPE_lng:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, lng, TPE, OP);
			else
				BINARY_3TYPE_FUNC(dbl, lng, TPE, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, hge, TPE, OP);
			else
				BINARY_3TYPE_FUNC(dbl, hge, TPE, OP);
			break;
#endif
		case TYPE_flt:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, flt, TPE, OP);
			else
				BINARY_3TYPE_FUNC(dbl, flt, TPE, OP);
			break;
		case TYPE_dbl:
		dbldbl:
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(dbl, dbl, TPE, OP);
			else
				BINARY_3TYPE_FUNC(dbl, dbl, TPE, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_oid:
		if (tp2 == TYPE_void) {
			oid v;

			v = * (const oid *) rgt;
			if (is_oid_nil(v)) {
				for (k = 0; k < cnt; k++)
					dst[k] = TPE_nil;
				nils = cnt;
			} else {
				CANDLOOP(dst, k, TPE_nil, 0, start);
				for (i = start * incr1, v += start, k = start;
				     k < end; i += incr1, v++, k++) {
					CHECKCAND(dst, k, candoff, TPE_nil);
					if (is_oid_nil(((const oid *) lft)[i])) {
						nils++;
						dst[k] = TPE_nil;
					} else {
						dst[k] = OP(((const oid *) lft)[i], v);
					}
				}
				CANDLOOP(dst, k, TPE_nil, end, cnt);
			}
		} else if (tp2 == TYPE_oid) {
			if (nonil)
				BINARY_3TYPE_FUNC_nonil(oid, oid, TPE, OP);
			else
				BINARY_3TYPE_FUNC(oid, oid, TPE, OP);
		} else {
			goto unsupported;
		}
		break;
	case TYPE_str:
		if (tp1 != tp2)
			goto unsupported;
		CANDLOOP(dst, k, TPE_nil, 0, start);
		for (i = start * incr1, j = start * incr2, k = start;
		     k < end; i += incr1, j += incr2, k++) {
			const char *s1, *s2;
			CHECKCAND(dst, k, candoff, TPE_nil);
			s1 = hp1 ? hp1 + VarHeapVal(lft, i, wd1) : (const char *) lft;
			s2 = hp2 ? hp2 + VarHeapVal(rgt, j, wd2) : (const char *) rgt;
			if (s1 == NULL || strcmp(s1, str_nil) == 0 ||
			    s2 == NULL || strcmp(s2, str_nil) == 0) {
				nils++;
				dst[k] = TPE_nil;
			} else {
				int x = strcmp(s1, s2);
				dst[k] = OP(x, 0);
			}
		}
		CANDLOOP(dst, k, TPE_nil, end, cnt);
		break;
	default:
		if (tp1 != tp2 ||
		    !ATOMlinear(tp1) ||
		    (atomcmp = ATOMcompare(tp1)) == NULL)
			goto unsupported;
		/* a bit of a hack: for inherited types, use
		 * type-expanded version if comparison function is
		 * equal to the inherited-from comparison function,
		 * and yes, we jump right into the middle of a switch,
		 * but that is legal (although not encouraged) C */
		if (atomcmp == ATOMcompare(TYPE_bte))
			goto btebte;
		if (atomcmp == ATOMcompare(TYPE_sht))
			goto shtsht;
		if (atomcmp == ATOMcompare(TYPE_int))
			goto intint;
		if (atomcmp == ATOMcompare(TYPE_lng))
			goto lnglng;
#ifdef HAVE_HGE
		if (atomcmp == ATOMcompare(TYPE_hge))
			goto hgehge;
#endif
		if (atomcmp == ATOMcompare(TYPE_flt))
			goto fltflt;
		if (atomcmp == ATOMcompare(TYPE_dbl))
			goto dbldbl;
		nil = ATOMnilptr(tp1);
		CANDLOOP(dst, k, TPE_nil, 0, start);
		for (i = start * incr1, j = start * incr2, k = start;
		     k < end; i += incr1, j += incr2, k++,
		     loff+= wd1, roff+= wd2) {
			const void *p1, *p2;
			CHECKCAND(dst, k, candoff, TPE_nil);
			p1 = hp1 ? (const void *) (hp1 + VarHeapVal(lft, i, wd1)) : (const void *) ((const char *) lft + loff);
			p2 = hp2 ? (const void *) (hp2 + VarHeapVal(rgt, j, wd2)) : (const void *) ((const char *) rgt + roff);
			if (p1 == NULL || p2 == NULL ||
			    (*atomcmp)(p1, nil) == 0 ||
			    (*atomcmp)(p2, nil) == 0) {
				nils++;
				dst[k] = TPE_nil;
			} else {
				int x = (*atomcmp)(p1, p2);
				dst[k] = OP(x, 0);
			}
		}
		CANDLOOP(dst, k, TPE_nil, end, cnt);
		break;
	}

	return nils;

  unsupported:
	GDKerror("%s: bad input types %s,%s.\n", func,
		 ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

static BAT *
BATcalcop_intern(const void *lft, int tp1, int incr1, const char *hp1, int wd1,
		 const void *rgt, int tp2, int incr2, const char *hp2, int wd2,
		 BUN cnt, BUN start, BUN end, const oid *restrict cand,
		 const oid *candend, oid candoff, int nonil, oid seqbase,
		 const char *func)
{
	BAT *bn;
	BUN nils = 0;
	TPE *restrict dst;

	bn = COLnew(seqbase, TYPE_TPE, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	dst = (TPE *) Tloc(bn, 0);

	nils = op_typeswitchloop(lft, tp1, incr1, hp1, wd1,
				 rgt, tp2, incr2, hp2, wd2,
				 dst, cnt, start, end, cand, candend, candoff,
				 nonil, func);

	if (nils == BUN_NONE) {
		BBPunfix(bn->batCacheid);
		return NULL;
	}

	BATsetcount(bn, cnt);

	bn->tsorted = cnt <= 1 || nils == cnt;
	bn->trevsorted = cnt <= 1 || nils == cnt;
	bn->tkey = cnt <= 1;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;

	return bn;
}

BAT *
BATcalcop(BAT *b1, BAT *b2, BAT *s)
{
	BAT *bn;
	BUN start, end, cnt;
	const oid *restrict cand = NULL, *candend = NULL;

	BATcheck(b1, __func__, NULL);
	BATcheck(b2, __func__, NULL);

	if (checkbats(b1, b2, __func__) != GDK_SUCCEED)
		return NULL;

	CANDINIT(b1, s, start, end, cnt, cand, candend);

	if (BATtvoid(b1) && BATtvoid(b2) && cand == NULL) {
		TPE res;

		if (is_oid_nil(b1->tseqbase) || is_oid_nil(b2->tseqbase))
			res = TPE_nil;
		else
			res = OP(b1->tseqbase, b2->tseqbase);

		return BATconstant(b1->hseqbase, TYPE_TPE, &res, cnt, TRANSIENT);
	}

	bn = BATcalcop_intern(b1->ttype == TYPE_void ? (const void *) &b1->tseqbase : (const void *) Tloc(b1, 0),
			      ATOMtype(b1->ttype) == TYPE_oid ? b1->ttype : ATOMbasetype(b1->ttype),
			      1,
			      b1->tvheap ? b1->tvheap->base : NULL,
			      b1->twidth,
			      b2->ttype == TYPE_void ? (const void *) &b2->tseqbase : (const void *) Tloc(b2, 0),
			      ATOMtype(b2->ttype) == TYPE_oid ? b2->ttype : ATOMbasetype(b2->ttype),
			      1,
			      b2->tvheap ? b2->tvheap->base : NULL,
			      b2->twidth,
			      cnt,
			      start,
			      end,
			      cand,
			      candend,
			      b1->hseqbase,
			      cand == NULL && b1->tnonil && b2->tnonil,
			      b1->hseqbase,
			      __func__);

	return bn;
}

BAT *
BATcalcopcst(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	BUN start, end, cnt;
	const oid *restrict cand = NULL, *candend = NULL;

	BATcheck(b, __func__, NULL);

	CANDINIT(b, s, start, end, cnt, cand, candend);

	bn = BATcalcop_intern(b->ttype == TYPE_void ? (const void *) &b->tseqbase : (const void *) Tloc(b, 0),
			      ATOMtype(b->ttype) == TYPE_oid ? b->ttype : ATOMbasetype(b->ttype),
			      1,
			      b->tvheap ? b->tvheap->base : NULL,
			      b->twidth,
			      VALptr(v),
			      ATOMtype(v->vtype) == TYPE_oid ? v->vtype : ATOMbasetype(v->vtype),
			      0,
			      NULL,
			      0,
			      cnt,
			      start,
			      end,
			      cand,
			      candend,
			      b->hseqbase,
			      cand == NULL && b->tnonil && ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0,
			      b->hseqbase,
			      __func__);

	return bn;
}

BAT *
BATcalccstop(const ValRecord *v, BAT *b, BAT *s)
{
	BAT *bn;
	BUN start, end, cnt;
	const oid *restrict cand = NULL, *candend = NULL;

	BATcheck(b, __func__, NULL);

	CANDINIT(b, s, start, end, cnt, cand, candend);

	bn = BATcalcop_intern(VALptr(v),
			      ATOMtype(v->vtype) == TYPE_oid ? v->vtype : ATOMbasetype(v->vtype),
			      0,
			      NULL,
			      0,
			      b->ttype == TYPE_void ? (const void *) &b->tseqbase : (const void *) Tloc(b, 0),
			      ATOMtype(b->ttype) == TYPE_oid ? b->ttype : ATOMbasetype(b->ttype),
			      1,
			      b->tvheap ? b->tvheap->base : NULL,
			      b->twidth,
			      cnt,
			      start,
			      end,
			      cand,
			      candend,
			      b->hseqbase,
			      cand == NULL && b->tnonil && ATOMcmp(v->vtype, VALptr(v), ATOMnilptr(v->vtype)) != 0,
			      b->hseqbase,
			      __func__);

	return bn;
}

gdk_return
VARcalcop(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_TPE;
	if (op_typeswitchloop(VALptr(lft),
			      ATOMtype(lft->vtype) == TYPE_oid ? lft->vtype : ATOMbasetype(lft->vtype),
			      0,
			      NULL,
			      0,
			      VALptr(rgt),
			      ATOMtype(rgt->vtype) == TYPE_oid ? rgt->vtype : ATOMbasetype(rgt->vtype),
			      0,
			      NULL,
			      0,
			      VALget(ret),
			      1,
			      0,
			      1,
			      NULL,
			      NULL,
			      0,
			      0,
			      __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}
