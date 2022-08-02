/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

#define MKEYHASH_bte(valp)	((ulng) (lng) *(const bte*)(valp))
#define MKEYHASH_sht(valp)	((ulng) (lng) *(const sht*)(valp))
#define MKEYHASH_int(valp)	((ulng) (lng) *(const int*)(valp))
#define MKEYHASH_lng(valp)	((ulng) (lng) *(const lng*)(valp))
#ifdef HAVE_HGE
#define MKEYHASH_hge(valp)	((ulng) (*(const uhge *)(valp) >> 64) ^ \
							 (ulng) *(const uhge *)(valp))
#endif

static inline ulng
GDK_ROTATE(ulng x, int y, int z)
{
	return (x << y) | (x >> z);
}

/* TODO: nil handling. however; we do not want to lose time in bulk_rotate_xor_hash with that */
str
MKEYrotate(lng *res, const lng *val, const int *n)
{
	*res = (lng) GDK_ROTATE((ulng) *val, *n, (sizeof(lng)*8) - *n);
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
		*res = lng_nil; /* It can be called from SQL */
		break;
	case TYPE_bat:
	case TYPE_ptr:
		// illegal types, avoid falling into the default case.
		assert(0);
	case TYPE_bte:
		*res = (lng) MKEYHASH_bte(val);
		break;
	case TYPE_sht:
		*res = (lng) MKEYHASH_sht(val);
		break;
	case TYPE_int:
	case TYPE_flt:
		*res = (lng) MKEYHASH_int(val);
		break;
	case TYPE_lng:
	case TYPE_dbl:
		*res = (lng) MKEYHASH_lng(val);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		*res = (lng) MKEYHASH_hge(val);
		break;
#endif
	default:
		if (ATOMextern(tpe))
			*res = (lng) ATOMhash(tpe, *(ptr*)val);
		else
			*res = (lng) ATOMhash(tpe, val);
		break;
	}
	return MAL_SUCCEED;
}

str
MKEYbathash(bat *res, const bat *bid)
{
	BAT *b, *dst;
	ulng *restrict r;
	BUN n;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, "mkey.bathash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	n = BATcount(b);
	dst = COLnew(b->hseqbase, TYPE_lng, n, TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "mkey.bathash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATsetcount(dst, n);

	r = (ulng *) Tloc(dst, 0);

	switch (ATOMstorage(b->ttype)) {
	case TYPE_void: {
		oid o = b->tseqbase;
		if (is_oid_nil(o))
			for (BUN i = 0; i < n; i++)
				r[i] = (ulng) lng_nil;
		else
			for (BUN i = 0; i < n; i++)
				r[i] = o + i;
		break;
	}
	case TYPE_bte: {
		const bte *restrict v = (const bte *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = MKEYHASH_bte(v + i);
		break;
	}
	case TYPE_sht: {
		const sht *restrict v = (const sht *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = MKEYHASH_sht(v + i);
		break;
	}
	case TYPE_int:
	case TYPE_flt: {
		const int *restrict v = (const int *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = MKEYHASH_int(v + i);
		break;
	}
	case TYPE_lng:
	case TYPE_dbl: {
		const lng *restrict v = (const lng *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = MKEYHASH_lng(v + i);
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		const hge *restrict v = (const hge *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = MKEYHASH_hge(v + i);
		break;
	}
#endif
	default: {
		BATiter bi = bat_iterator(b);
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;
		int (*cmp)(const void *, const void *) = ATOMcompare(b->ttype);
		const void *nil = ATOMnilptr(b->ttype);

		for (BUN i = 0; i < n; i++) {
			const void *restrict v = BUNtail(bi, i);
			if ((*cmp)(v, nil) == 0)
				r[i] = (ulng) lng_nil;
			else
				r[i] = (ulng) (*hash)(v);
		}
		break;
	}
	}

	if (dst->batCount <= 1) {
		BATkey(dst, true);
		dst->tsorted = dst->trevsorted = true;
	} else {
		BATkey(dst, false);
		dst->tsorted = dst->trevsorted = false;
	}
	dst->tnonil = false;
	dst->tnil = false;

	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MKEYrotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *dst = getArgReference_lng(stk, p, 0);
	ulng h = (ulng) *getArgReference_lng(stk, p, 1);
	int lbit = *getArgReference_int(stk, p, 2);
	int rbit = (int) sizeof(lng) * 8 - lbit;
	int tpe = getArgType(mb, p, 3);
	ptr *pval = getArgReference(stk, p, 3);
	ulng val;

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
	*dst = (lng) (GDK_ROTATE(h, lbit, rbit) ^ val);
	return MAL_SUCCEED;
}

str
MKEYbulk_rotate_xor_hash(bat *res, const bat *hid, const int *nbits, const bat *bid)
{
	BAT *hb, *b, *bn;
	int lbit = *nbits;
	int rbit = (int) sizeof(lng) * 8 - lbit;
	ulng *restrict r;
	const ulng *restrict h;
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
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATsetcount(bn, n);

	r = (ulng *) Tloc(bn, 0);
	h = (const ulng *) Tloc(hb, 0);

	switch (ATOMstorage(b->ttype)) {
	case TYPE_bte: {
		const bte *restrict v = (const bte *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++) {
			r[i] = GDK_ROTATE(h[i], lbit, rbit) ^ MKEYHASH_bte(v + i);
		}
		break;
	}
	case TYPE_sht: {
		const sht *restrict v = (const sht *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++) {
			r[i] = GDK_ROTATE(h[i], lbit, rbit) ^ MKEYHASH_sht(v + i);
		}
		break;
	}
	case TYPE_int:
	case TYPE_flt: {
		const int *restrict v = (const int *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++) {
			r[i] = GDK_ROTATE(h[i], lbit, rbit) ^ MKEYHASH_int(v + i);
		}
		break;
	}
	case TYPE_lng:
	case TYPE_dbl: {
		const lng *restrict v = (const lng *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++) {
			r[i] = GDK_ROTATE(h[i], lbit, rbit) ^ MKEYHASH_lng(v + i);
		}
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		const hge *restrict v = (const hge *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++) {
			r[i] = GDK_ROTATE(h[i], lbit, rbit) ^ MKEYHASH_hge(v + i);
		}
		break;
	}
#endif
	case TYPE_str:
		if (b->tvheap->hashash) {
			BATiter bi = bat_iterator(b);
			for (BUN i = 0; i < n; i++) {
				const void *restrict s = BUNtvar(bi, i);
				r[i] = GDK_ROTATE(h[i], lbit, rbit) ^ (ulng) ((const BUN *) s)[-1];
			}
			break;
		}
		/* fall through */
	default: {
		BATiter bi = bat_iterator(b);
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;

		for (BUN i = 0; i < n; i++)
			r[i] = GDK_ROTATE(h[i], lbit, rbit) ^ (ulng) (*hash)(BUNtail(bi, i));
		break;
	}
	}
	if (bn->batCount <= 1) {
		BATkey(bn, true);
		bn->tsorted = bn->trevsorted = true;
	} else {
		BATkey(bn, false);
		bn->tsorted = bn->trevsorted = false;
	}
	bn->tnonil = false;
	bn->tnil = false;

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
	ulng *r;
	const ulng *h;
	ulng val;
	BUN n;

	(void) cntxt;

	if ((hb = BATdescriptor(*hid)) == NULL)
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	n = BATcount(hb);

	bn = COLnew(hb->hseqbase, TYPE_lng, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(hb->batCacheid);
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
			val = (ulng) ATOMhash(tpe, *(ptr*)pval);
		else
			val = (ulng) ATOMhash(tpe, pval);
		break;
	}

	r = (ulng *) Tloc(bn, 0);
	h = (const ulng *) Tloc(hb, 0);

	while (n-- > 0) {
			*r++ = GDK_ROTATE(*h, lbit, rbit) ^ val;
			h++;
	}

	if (bn->batCount <= 1) {
		BATkey(bn, true);
		bn->tsorted = bn->trevsorted = true;
	} else {
		BATkey(bn, false);
		bn->tsorted = bn->trevsorted = false;
	}
	bn->tnonil = false;
	bn->tnil = false;

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
	ulng *restrict r;
	BUN n;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_lng, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "mkey.rotate_xor_hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATsetcount(bn, n);

	r = (ulng *) Tloc(bn, 0);

	switch (ATOMstorage(b->ttype)) {
	case TYPE_bte: {
		const bte *restrict v = (const bte *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = GDK_ROTATE((ulng) *h, lbit, rbit) ^ MKEYHASH_bte(v + i);
		break;
	}
	case TYPE_sht: {
		const sht *restrict v = (const sht *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = GDK_ROTATE((ulng) *h, lbit, rbit) ^ MKEYHASH_sht(v + i);
		break;
	}
	case TYPE_int:
	case TYPE_flt: {
		const int *restrict v = (const int *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = GDK_ROTATE((ulng) *h, lbit, rbit) ^ MKEYHASH_int(v + i);
		break;
	}
	case TYPE_lng:
	case TYPE_dbl: {
		const lng *restrict v = (const lng *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = GDK_ROTATE((ulng) *h, lbit, rbit) ^ MKEYHASH_lng(v + i);
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		const hge *restrict v = (const hge *) Tloc(b, 0);
		for (BUN i = 0; i < n; i++)
			r[i] = GDK_ROTATE((ulng) *h, lbit, rbit) ^ MKEYHASH_hge(v + i);
		break;
	}
#endif
	case TYPE_str:
		if (b->tvheap->hashash) {
			BATiter bi = bat_iterator(b);
			for (BUN i = 0; i < n; i++) {
				const char *restrict s = BUNtvar(bi, i);
				r[i] = GDK_ROTATE((ulng) *h, lbit, rbit) ^ (ulng) ((const BUN *) s)[-1];
			}
			break;
		}
		/* fall through */
	default: {
		BATiter bi = bat_iterator(b);
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;

		for (BUN i = 0; i < n; i++)
			r[i] = GDK_ROTATE((ulng) *h, lbit, rbit) ^ (ulng) (*hash)(BUNtail(bi, i));
		break;
	}
	}
	if (bn->batCount <= 1) {
		BATkey(bn, true);
		bn->tsorted = bn->trevsorted = true;
	} else {
		BATkey(bn, false);
		bn->tsorted = bn->trevsorted = false;
	}
	bn->tnonil = false;
	bn->tnil = false;

	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}
