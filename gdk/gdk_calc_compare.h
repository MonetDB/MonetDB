/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

static BUN
opswitch(BAT *b1, int tp1, const void *src1, const void *v1p,
	 BAT *b2, int tp2, const void *src2, const void *v2p,
	 tpe *restrict dst, oid hoff, BUN start, BUN cnt,
	 const oid *restrict cand, int projected, const char *func)
{
	BUN nils = 0;

	if (tp1 == TYPE_tpe) {
		if (tp2 == TYPE_tpe) {
			BINARY_3TYPE_FUNC(bit, bit, tpe, OP);
			return nils;
		}
		goto unsupported;
	} else if (tp2 == TYPE_bit) {
		goto unsupported;
	}

	switch (ATOMbasetype(tp1)) {
	default: {
		BATiter b1i;
		BATiter b2i;
		BUN i;
		int (*cmp)(const void *, const void *);
		const void *nil;

		if (ATOMbasetype(tp1) != ATOMbasetype(tp2) || !ATOMlinear(tp1))
			goto unsupported;

		b1i = bat_iterator(b1);
		b2i = bat_iterator(b2);
		cmp = ATOMcompare(tp1);
		nil = ATOMnilptr(tp1);

		for (i = 0; i < cnt; i++) {
			if (b1)
				v1p = BUNtail(b1i, projected == 1 ? i : cand ? cand[i] - hoff : start + i);
			if (b2)
				v2p = BUNtail(b2i, projected == 2 ? i : cand ? cand[i] - hoff : start + i);
			if (cmp(v1p, nil) == 0 || cmp(v2p, nil) == 0) {
				dst[i] = tpe_nil;
				nils++;
			} else {
				dst[i] = OP(cmp(v1p, v2p), 0);
			}
		}
		break;
	}
	case TYPE_void: {
		oid v1 = * (const oid *) src1;
		oid v2 = 0;
		BUN i;
		switch (ATOMbasetype(tp2)) {
		case TYPE_void:
			v2 = * (const oid *) src2;
			switch (projected) {
			case 1:
				for (i = 0; i < cnt; i++)
					dst[i] = OP(v1 + i, v2 + cand[i] - hoff);
				break;
			case 2:
				for (i = 0; i < cnt; i++)
					dst[i] = OP(v1 + cand[i] - hoff, v2 + i);
				break;
			default:
				assert(0);
				return BUN_NONE;
			}
			break;
		case TYPE_oid:
			if (v2p)
				v2 = * (const oid *) v2p;
			for (i = 0; i < cnt; i++) {
				if (src2)
					v2 = ((const oid *) src2)[projected == 2 ? i : cand ? cand[i] - hoff : start + i];
				if (v2 == oid_nil) {
					dst[i] = tpe_nil;
					nils++;
				} else {
					dst[i] = OP(v1 + (projected == 1 ? i : cand[i] - hoff), v2);
				}
			}
			break;
		default:
			goto unsupported;
		}
		break;
	}
	case TYPE_oid:
		switch (ATOMbasetype(tp2)) {
		case TYPE_void: {
			oid v1 = 0;
			oid v2 = 0;
			BUN i;
			v2 = * (const oid *) src2;
			if (v1p)
				v1 = * (const oid *) v1p;
			for (i = 0; i < cnt; i++) {
				if (src1)
					v1 = ((const oid *) src1)[projected == 1 ? i : cand ? cand[i] - hoff : start + i];
				if (v1 == oid_nil) {
					dst[i] = tpe_nil;
					nils++;
				} else {
					dst[i] = OP(v1, v2 + (projected == 2 ? i : cand[i] - hoff));
				}
			}
			break;
		}
		case TYPE_oid:
			BINARY_3TYPE_FUNC(oid, oid, tpe, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_bte:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC(bte, bte, tpe, OP);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC(bte, sht, tpe, OP);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC(bte, int, tpe, OP);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC(bte, lng, tpe, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC(bte, hge, tpe, OP);
			break;
#endif
		case TYPE_flt:
			BINARY_3TYPE_FUNC(bte, flt, tpe, OP);
			break;
		case TYPE_dbl:
			BINARY_3TYPE_FUNC(bte, dbl, tpe, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_sht:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC(sht, bte, tpe, OP);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC(sht, sht, tpe, OP);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC(sht, int, tpe, OP);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC(sht, lng, tpe, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC(sht, hge, tpe, OP);
			break;
#endif
		case TYPE_flt:
			BINARY_3TYPE_FUNC(sht, flt, tpe, OP);
			break;
		case TYPE_dbl:
			BINARY_3TYPE_FUNC(sht, dbl, tpe, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_int:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC(int, bte, tpe, OP);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC(int, sht, tpe, OP);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC(int, int, tpe, OP);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC(int, lng, tpe, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC(int, hge, tpe, OP);
			break;
#endif
		case TYPE_flt:
			BINARY_3TYPE_FUNC(int, flt, tpe, OP);
			break;
		case TYPE_dbl:
			BINARY_3TYPE_FUNC(int, dbl, tpe, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_lng:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC(lng, bte, tpe, OP);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC(lng, sht, tpe, OP);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC(lng, int, tpe, OP);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC(lng, lng, tpe, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC(lng, hge, tpe, OP);
			break;
#endif
		case TYPE_flt:
			BINARY_3TYPE_FUNC(lng, flt, tpe, OP);
			break;
		case TYPE_dbl:
			BINARY_3TYPE_FUNC(lng, dbl, tpe, OP);
			break;
		default:
			goto unsupported;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC(hge, bte, tpe, OP);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC(hge, sht, tpe, OP);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC(hge, int, tpe, OP);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC(hge, lng, tpe, OP);
			break;
		case TYPE_hge:
			BINARY_3TYPE_FUNC(hge, hge, tpe, OP);
			break;
		case TYPE_flt:
			BINARY_3TYPE_FUNC(hge, flt, tpe, OP);
			break;
		case TYPE_dbl:
			BINARY_3TYPE_FUNC(hge, dbl, tpe, OP);
			break;
		default:
			goto unsupported;
		}
		break;
#endif
	case TYPE_flt:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC(flt, bte, tpe, OP);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC(flt, sht, tpe, OP);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC(flt, int, tpe, OP);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC(flt, lng, tpe, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC(flt, hge, tpe, OP);
			break;
#endif
		case TYPE_flt:
			BINARY_3TYPE_FUNC(flt, flt, tpe, OP);
			break;
		case TYPE_dbl:
			BINARY_3TYPE_FUNC(flt, dbl, tpe, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	case TYPE_dbl:
		switch (ATOMbasetype(tp2)) {
		case TYPE_bte:
			BINARY_3TYPE_FUNC(dbl, bte, tpe, OP);
			break;
		case TYPE_sht:
			BINARY_3TYPE_FUNC(dbl, sht, tpe, OP);
			break;
		case TYPE_int:
			BINARY_3TYPE_FUNC(dbl, int, tpe, OP);
			break;
		case TYPE_lng:
			BINARY_3TYPE_FUNC(dbl, lng, tpe, OP);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BINARY_3TYPE_FUNC(dbl, hge, tpe, OP);
			break;
#endif
		case TYPE_flt:
			BINARY_3TYPE_FUNC(dbl, flt, tpe, OP);
			break;
		case TYPE_dbl:
			BINARY_3TYPE_FUNC(dbl, dbl, tpe, OP);
			break;
		default:
			goto unsupported;
		}
		break;
	}
	return nils;

  unsupported:
	GDKerror("%s: type combination (%s,%s) not supported.\n",
		 func, ATOMname(tp1), ATOMname(tp2));
	return BUN_NONE;
}

BAT *
BATcalcop(BAT *b1, BAT *b2, BAT *s, int projected)
{
	BAT *bn;
	CALC_DECL;

	CALC_PREINIT(b1, b2, bn);
	CALC_INIT(bn);
	CALC_POSTINIT(b1, b2);

	if (b1->ttype == TYPE_void && b2->ttype == TYPE_void) {
		if (b1->tseqbase == oid_nil || b2->tseqbase == oid_nil) {
			tpe v = tpe_nil;
			return BATconstant(hseq, TYPE_tpe, &v, cnt, TRANSIENT);
		} else if (projected == 0) {
			tpe v = OP(b1->tseqbase, b2->tseqbase);
			return BATconstant(hseq, TYPE_tpe, &v, cnt, TRANSIENT);
		}
	}

	bn = COLnew(hseq, TYPE_tpe, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = opswitch(b1, b1->ttype, b1->ttype == TYPE_void ? &b1->tseqbase : (oid *) Tloc(b1, 0), NULL,
			b2, b2->ttype, b2->ttype == TYPE_void ? &b2->tseqbase : (oid *) Tloc(b2, 0), NULL,
			(tpe *) Tloc(bn, 0), hoff, start, cnt, cand, projected, __func__);

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
BATcalcopcst(BAT *b, const ValRecord *v, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	if (b->ttype == TYPE_void && b->tseqbase == oid_nil) {
		tpe v = tpe_nil;
		return BATconstant(hseq, TYPE_tpe, &v, cnt, TRANSIENT);
	}

	bn = COLnew(hseq, TYPE_tpe, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = opswitch(b, b->ttype, b->ttype == TYPE_void ? &b->tseqbase : (oid *) Tloc(b, 0), NULL,
			NULL, v->vtype, NULL, VALptr(v),
			(tpe *) Tloc(bn, 0), hoff, start, cnt, cand, 0, __func__);

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
BATcalccstop(const ValRecord *v, BAT *b, BAT *s)
{
	BAT *bn;
	CALC_DECL;

	CALC_INIT(b);

	if (b->ttype == TYPE_void && b->tseqbase == oid_nil) {
		tpe v = tpe_nil;
		return BATconstant(hseq, TYPE_tpe, &v, cnt, TRANSIENT);
	}

	bn = COLnew(hseq, TYPE_tpe, cnt, TRANSIENT);
	if (bn == NULL)
		return NULL;

	nils = opswitch(NULL, v->vtype, NULL, VALptr(v),
			b, b->ttype, b->ttype == TYPE_void ? &b->tseqbase : (oid *) Tloc(b, 0), NULL,
			(tpe *) Tloc(bn, 0), hoff, start, cnt, cand, 0, __func__);

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
VARcalcop(ValPtr ret, const ValRecord *lft, const ValRecord *rgt)
{
	ret->vtype = TYPE_tpe;
	if (opswitch(NULL, lft->vtype, NULL, VALptr(lft),
		     NULL, rgt->vtype, NULL, VALptr(rgt),
		     VALget(ret), 0, 0, 1, NULL, 0, __func__) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
}
