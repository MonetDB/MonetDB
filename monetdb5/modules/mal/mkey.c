/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (c) Peter Boncz, Stefan Manegold, Niels Nes
 *
 * new functionality for the low-resource-consumption. It will
 * first one by one create a hash value out of the multiple attributes.
 * This hash value is computed by xoring and rotating individual hash
 * values together. We create a hash and rotate command to do this.
 */
#include "monetdb_config.h"
#include "mkey.h"

#define MKEYHASH_bte(valp)	((lng) *(bte*)(valp))
#define MKEYHASH_sht(valp)	((lng) *(sht*)(valp))
#define MKEYHASH_int(valp)	((lng) *(int*)(valp))
#define MKEYHASH_lng(valp)	((lng) *(lng*)(valp))
#ifdef HAVE_HGE
#define MKEYHASH_hge(valp)	(((lng*)(valp))[0] ^ ((lng*)(valp))[1])
#endif

static inline lng
GDK_ROTATE(lng x, int y, int z, lng m)
{
	return ((lng) ((ulng) x << y) & ~m) | ((x >> z) & m);
}

/* TODO: nil handling. however; we do not want to lose time in bulk_rotate_xor_hash with that */
str
MKEYrotate(lng *res, const lng *val, const int *n)
{
	*res = GDK_ROTATE(*val, *n, (sizeof(lng)*8) - *n, (((lng)1) << *n) - 1);
	return MAL_SUCCEED;
}

str
MKEYhash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *res;
	ptr val;
	int tpe = getArgType(mb,p,1);

	(void) cntxt;
	res= getArgReference_lng(stk,p,0);
	val= getArgReference(stk,p,1);
	switch (ATOMstorage(tpe)) {
	case TYPE_void:
	case TYPE_bat:
	case TYPE_ptr:
		// illegal types, avoid falling into the default case.
		assert(0);
	case TYPE_bte:
		*res = MKEYHASH_bte(val);
		break;
	case TYPE_sht:
		*res = MKEYHASH_sht(val);
		break;
	case TYPE_int:
	case TYPE_flt:
		*res = MKEYHASH_int(val);
		break;
	case TYPE_lng:
	case TYPE_dbl:
		*res = MKEYHASH_lng(val);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		*res = MKEYHASH_hge(val);
		break;
#endif
	default:
		if (ATOMextern(tpe))
			*res = ATOMhash(tpe, *(ptr*)val);
		else
			*res = ATOMhash(tpe, val);
		break;
	}
	return MAL_SUCCEED;
}

str
MKEYbathash(bat *res, const bat *bid)
{
	BAT *b, *dst;
	lng *r;
	BUN n;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, "mkey.bathash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	n = BATcount(b);
	dst = COLnew(b->hseqbase, TYPE_lng, n, TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "mkey.bathash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATsetcount(dst, n);

	r = (lng *) Tloc(dst, 0);

	switch (ATOMstorage(b->ttype)) {
	case TYPE_void: {
		oid o = b->tseqbase;
		if (is_oid_nil(o))
			while (n-- > 0)
				*r++ = lng_nil;
		else
			while (n-- > 0)
				*r++ = (lng) o++;
		break;
	}
	case TYPE_bte: {
		bte *v = (bte *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = MKEYHASH_bte(v);
			v++;
		}
		break;
	}
	case TYPE_sht: {
		sht *v = (sht *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = MKEYHASH_sht(v);
			v++;
		}
		break;
	}
	case TYPE_int:
	case TYPE_flt: {
		int *v = (int *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = MKEYHASH_int(v);
			v++;
		}
		break;
	}
	case TYPE_lng:
	case TYPE_dbl: {
		lng *v = (lng *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = MKEYHASH_lng(v);
			v++;
		}
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		hge *v = (hge *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = MKEYHASH_hge(v);
			v++;
		}
		break;
	}
#endif
	default: {
		BATiter bi = bat_iterator(b);
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;
		int (*cmp)(const void *, const void *) = ATOMcompare(b->ttype);
		const void *nil = ATOMnilptr(b->ttype);
		BUN i;
		const void *v;

		BATloop(b, i, n) {
			v = BUNtail(bi, i);
			if ((*cmp)(v, nil) == 0)
				*r++ = lng_nil;
			else
				*r++ = (lng) (*hash)(v);
		}
		break;
	}
	}

	if (dst->batCount <= 1) {
		BATkey(dst, 1);
		dst->tsorted = dst->trevsorted = 1;
	} else {
		BATkey(dst, 0);
		dst->tsorted = dst->trevsorted = 0;
	}
	dst->tnonil = 0;
	dst->tnil = 0;

	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MKEYrotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *dst = getArgReference_lng(stk, p, 0);
	lng h = *getArgReference_lng(stk, p, 1);
	int lbit = *getArgReference_int(stk, p, 2);
	int rbit = (int) sizeof(lng) * 8 - lbit;
	int tpe = getArgType(mb, p, 3);
	ptr *pval = getArgReference(stk, p, 3);
	lng val;
	lng mask = ((lng) 1 << lbit) - 1;

	(void) cntxt;
	switch (ATOMstorage(tpe)) {
	case TYPE_bte:
		val = MKEYHASH_bte(pval);
		break;
	case TYPE_sht:
		val = MKEYHASH_sht(pval);
		break;
	case TYPE_int:
	case TYPE_flt:
		val = MKEYHASH_int(pval);
		break;
	case TYPE_lng:
	case TYPE_dbl:
		val = MKEYHASH_lng(pval);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		val = MKEYHASH_hge(pval);
		break;
#endif
	default:
		if (ATOMextern(tpe))
			val = ATOMhash(tpe, *(ptr*)pval);
		else
			val = ATOMhash(tpe, pval);
		break;
	}
	*dst = GDK_ROTATE(h, lbit, rbit, mask) ^ val;
	return MAL_SUCCEED;
}

str
MKEYbulk_rotate_xor_hash(bat *res, const bat *hid, const int *nbits, const bat *bid)
{
	BAT *hb, *b, *bn;
	int lbit = *nbits;
	int rbit = (int) sizeof(lng) * 8 - lbit;
	lng mask = ((lng) 1 << lbit) - 1;
	lng *r;
	const lng *h;
	BUN n;

	if ((hb = BATdescriptor(*hid)) == NULL)
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPunfix(hb->batCacheid);
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (!ALIGNsynced(hb, b) && (BATcount(b) || BATcount(hb))) {
		BBPunfix(hb->batCacheid);
		BBPunfix(b->batCacheid);
		throw(MAL, "mkey.rotate_xor_hash",
			  OPERATION_FAILED ": input bats are not aligned");
	}

	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_lng, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(hb->batCacheid);
		BBPunfix(b->batCacheid);
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATsetcount(bn, n);

	r = (lng *) Tloc(bn, 0);
	h = (const lng *) Tloc(hb, 0);

	switch (ATOMstorage(b->ttype)) {
	case TYPE_bte: {
		bte *v = (bte *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_bte(v);
			v++;
			h++;
		}
		break;
	}
	case TYPE_sht: {
		sht *v = (sht *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_sht(v);
			v++;
			h++;
		}
		break;
	}
	case TYPE_int:
	case TYPE_flt: {
		int *v = (int *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_int(v);
			v++;
			h++;
		}
		break;
	}
	case TYPE_lng:
	case TYPE_dbl: {
		lng *v = (lng *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_lng(v);
			v++;
			h++;
		}
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		hge *v = (hge *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_hge(v);
			v++;
			h++;
		}
		break;
	}
#endif
	case TYPE_str:
		if (b->tvheap->hashash) {
			BATiter bi = bat_iterator(b);
			BUN i;
			BATloop(b, i, n) {
				str s = (str) BUNtvar(bi, i);
				*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ (lng) ((BUN *) s)[-1];
				h++;
			}
			break;
		}
		/* fall through */
	default: {
		BATiter bi = bat_iterator(b);
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;
		BUN i;

		BATloop(b, i, n) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ (lng) (*hash)(BUNtail(bi, i));
			h++;
		}
		break;
	}
	}
	if (bn->batCount <= 1) {
		BATkey(bn, 1);
		bn->tsorted = bn->trevsorted = 1;
	} else {
		BATkey(bn, 0);
		bn->tsorted = bn->trevsorted = 0;
	}
	bn->tnonil = 0;
	bn->tnil = 0;

	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(hb->batCacheid);
	return MAL_SUCCEED;
}

str
MKEYbulkconst_rotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	bat *res = getArgReference_bat(stk, p, 0);
	bat *hid = getArgReference_bat(stk, p, 1);
	int lbit = *getArgReference_int(stk, p, 2);
	int tpe = getArgType(mb, p, 3);
	ptr *pval = getArgReference(stk, p, 3);
	BAT *hb, *bn;
	int rbit = (int) sizeof(lng) * 8 - lbit;
	lng mask = ((lng) 1 << lbit) - 1;
	lng *r;
	const lng *h;
	lng val;
	BUN n;

	(void) cntxt;

	if ((hb = BATdescriptor(*hid)) == NULL)
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	n = BATcount(hb);

	bn = COLnew(hb->hseqbase, TYPE_lng, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(hb->batCacheid);
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATsetcount(bn, n);

	switch (ATOMstorage(tpe)) {
	case TYPE_bte:
		val = MKEYHASH_bte(pval);
		break;
	case TYPE_sht:
		val = MKEYHASH_sht(pval);
		break;
	case TYPE_int:
	case TYPE_flt:
		val = MKEYHASH_int(pval);
		break;
	case TYPE_lng:
	case TYPE_dbl:
		val = MKEYHASH_lng(pval);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		val = MKEYHASH_hge(pval);
		break;
#endif
	default:
		if (ATOMextern(tpe))
			val = ATOMhash(tpe, *(ptr*)pval);
		else
			val = ATOMhash(tpe, pval);
		break;
	}

	r = (lng *) Tloc(bn, 0);
	h = (const lng *) Tloc(hb, 0);

	while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ val;
			h++;
	}

	if (bn->batCount <= 1) {
		BATkey(bn, 1);
		bn->tsorted = bn->trevsorted = 1;
	} else {
		BATkey(bn, 0);
		bn->tsorted = bn->trevsorted = 0;
	}
	bn->tnonil = 0;
	bn->tnil = 0;

	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(hb->batCacheid);
	return MAL_SUCCEED;
}

str
MKEYconstbulk_rotate_xor_hash(bat *res, const lng *h, const int *nbits, const bat *bid)
{
	BAT *b, *bn;
	int lbit = *nbits;
	int rbit = (int) sizeof(lng) * 8 - lbit;
	lng mask = ((lng) 1 << lbit) - 1;
	lng *r;
	BUN n;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_lng, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATsetcount(bn, n);

	r = (lng *) Tloc(bn, 0);

	switch (ATOMstorage(b->ttype)) {
	case TYPE_bte: {
		bte *v = (bte *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_bte(v);
			v++;
		}
		break;
	}
	case TYPE_sht: {
		sht *v = (sht *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_sht(v);
			v++;
		}
		break;
	}
	case TYPE_int:
	case TYPE_flt: {
		int *v = (int *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_int(v);
			v++;
		}
		break;
	}
	case TYPE_lng:
	case TYPE_dbl: {
		lng *v = (lng *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_lng(v);
			v++;
		}
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		hge *v = (hge *) Tloc(b, 0);
		while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ MKEYHASH_hge(v);
			v++;
		}
		break;
	}
#endif
	case TYPE_str:
		if (b->tvheap->hashash) {
			BATiter bi = bat_iterator(b);
			BUN i;
			BATloop(b, i, n) {
				str s = (str) BUNtvar(bi, i);
				*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ (lng) ((BUN *) s)[-1];
			}
			break;
		}
		/* fall through */
	default: {
		BATiter bi = bat_iterator(b);
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;
		BUN i;

		BATloop(b, i, n) {
			*r++ = GDK_ROTATE(*h, lbit, rbit, mask) ^ (lng) (*hash)(BUNtail(bi, i));
		}
		break;
	}
	}
	if (bn->batCount <= 1) {
		BATkey(bn, 1);
		bn->tsorted = bn->trevsorted = 1;
	} else {
		BATkey(bn, 0);
		bn->tsorted = bn->trevsorted = 0;
	}
	bn->tnonil = 0;
	bn->tnil = 0;

	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}
