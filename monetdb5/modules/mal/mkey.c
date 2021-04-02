/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
- * @- The Problem
- * When creating a join, we want to make a unique key of the attributes on both
- * sides and then join these keys. Consider the following BATs.
- *
- * @verbatim
- * orders                  customer                link
- * ====================    =====================   ===========
- *         zipcode h_nr            zipcode hnr    oid     cid
- * o1      13      9       c1      11      10      o1      c5
- * o2      11      10      c2      11      11      o2      c1
- * o3      11      11      c3      12      2       o3      c2
- * o4      12      5       c4      12      1       o4      nil
- * o5      11      10      c5      13      9       o5      c1
- * o6      12      2       c6      14      7       o6      c3
- * o7      13      9                               o7      c5
- * o8      12      1                               o8      c4
- * o9      13      9                               o9      c5
- * @end verbatim
- *
- * The current approach is designed to take minimal memory, as our previous
- * solutions to the problem did not scale well. In case of singular keys,
- * the link is executed by a simple join. Before going into the join, we
- * make sure the end result size is not too large, which is done by looking
- * at relation sizes (if the other key is unique) or, if that is not possible,
- * by computing the exact join size.
- *
- * The join algorithm was also improved to do dynamic sampling to determine
- * with high accuracy the join size, so that we can alloc in one go a memory
- * region of sufficient size. This also reduces the ds\_link memory requirements.
- *
- * For compound keys, those that consist of multiple attributes, we now compute
- * a derived column that contains an integer hash value derived from all
- * key columns.
- * This is done by computing a hash value for each individual key column
- * and combining those by bitwise XOR and left-rotation. That is, for each
- * column,we rotate the working hash value by N bits and XOR the hash value
- * of the column over it. The working hash value is initialized with zero,
- * and after all columns are processed, this working value is used as output.
- * Computing the hash value for all columns in the key for one table is done
- * by the command hash(). Hence, we do hash on both sides, and join
- * that together with a simple join:
- *
- * @code{join(hash(keys), hash(keys.reverse);}
- *
- * One complication of this procedure are nil values:
- * @table
- * @itemize
- * @item
- * it may happen that the final hash-value (an int formed by a
- * random bit pattern) accidentally has the value of int(nil).
- * Notice that join never matches nil values.
- * Hence these accidental nils must be replaced by a begin value (currently: 0).
- * @item
- * in case any of the compound key values is nil, our nil semantics
- * require us that those tuples may never match on a join. Consequently,
- * during the hash() processing of all compound key columns for computing
- * the hash value, we also maintain a bit-bat that records which tuples had
- * a nil value. The bit-bat is initialized to false, and the results of the
- * nil-check on each column is OR-ed to it.
- * Afterwards, the hash-value of all tuples that have this nil-bit set to
- * TRUE are forced to int(nil), which will exclude them from matching.
- * @end itemize
- *
- * Joining on hash values produces a @emph{superset} of the join result:
- * it may happen that  two different key combinations hash on the same value,
- * which will make them match on the join (false hits). The final part
- * of the ds\_link therefore consists of filtering out the false hits.
- * This is done incrementally by joining back the join result to the original
- * columns, incrementally one by one for each pair of corresponding
- * columns. These values are compared with each other and we AND the
- * result of this comparison together for each pair of columns.
- * The bat containing these bits is initialized to all TRUE and serves as
- * final result after all column pairs have been compared.
- * The initial join result is finally filtered with this bit-bat.
- *
- * Joining back from the initial join-result to the original columns on
- * both sides takes quite a lot of memory. For this reason, the false
- * hit-filtering is done in slices (not all tuples at one time).
- * In this way the memory requirements of this phase are kept low.
- * In fact, the most memory demanding part of the join is the int-join
- * on hash number, which takes N*24 bytes (where N= |L| = |R|).
- * In comparison, the previous CTmultigroup/CTmultiderive approach
- * took N*48 bytes. Additionally, by making it possible to use merge-sort,
- * it avoids severe performance degradation (memory thrashing) as produced
- * by the old ds\_link when the inner join relation would be larger than memory.
- *
- * If ds\_link performance is still an issue, the sort-merge join used here
- * could be replaced by partitioned hash-join with radix-cluster/decluster.
- *
- * @+ Implementation
- */

/*
 * (c) Peter Boncz, Stefan Manegold, Niels Nes
 *
 * new functionality for the low-resource-consumption. It will
 * first one by one create a hash value out of the multiple attributes.
 * This hash value is computed by xoring and rotating individual hash
 * values together. We create a hash and rotate command to do this.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_exception.h"

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
static str
MKEYrotate(lng *res, const lng *val, const int *n)
{
	*res = (lng) GDK_ROTATE((ulng) *val, *n, (sizeof(lng)*8) - *n);
	return MAL_SUCCEED;
}

static str
MKEYhash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *res = getArgReference_lng(stk,p,0);
	ptr val = getArgReference(stk,p,1);
	int tpe = getArgType(mb,p,1);

	(void) cntxt;
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

static str
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

static str
MKEYrotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *dst = getArgReference_lng(stk, p, 0);
	ulng h = (ulng) *getArgReference_lng(stk, p, 1);
	int lbit = *getArgReference_int(stk, p, 2);
	int rbit = (int) sizeof(lng) * 8 - lbit;
	int tpe = getArgType(mb, p, 3);
	ptr pval = getArgReference(stk, p, 3);
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

static str
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

static str
MKEYbulkconst_rotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	bat *res = getArgReference_bat(stk, p, 0);
	bat *hid = getArgReference_bat(stk, p, 1);
	int lbit = *getArgReference_int(stk, p, 2);
	int tpe = getArgType(mb, p, 3);
	ptr pval = getArgReference(stk, p, 3);
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

static str
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

#include "mel.h"
mel_func mkey_init_funcs[] = {
 command("mkey", "rotate", MKEYrotate, false, "left-rotate an int by nbits", args(1,3, arg("",lng),arg("v",lng),arg("nbits",int))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),argany("v",0))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",bit))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",bte))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",sht))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",int))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",flt))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",dbl))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",lng))),
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",str))),
 pattern("mkey", "bulk_rotate_xor_hash", MKEYrotate_xor_hash, false, "post: [:xor=]([:rotate=](h, nbits), [hash](b))", args(1,4, arg("",lng),arg("h",lng),arg("nbits",int),argany("v",0))),
 command("mkey", "bulk_rotate_xor_hash", MKEYconstbulk_rotate_xor_hash, false, "pre:  h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b))", args(1,4, batarg("",lng),arg("h",lng),arg("nbits",int),batargany("b",1))),
 pattern("mkey", "bulk_rotate_xor_hash", MKEYbulkconst_rotate_xor_hash, false, "pre:  h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b))", args(1,4, batarg("",lng),batarg("h",lng),arg("nbits",int),argany("v",0))),
 command("mkey", "bulk_rotate_xor_hash", MKEYbulk_rotate_xor_hash, false, "pre:  h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b))", args(1,4, batarg("",lng),batarg("h",lng),arg("nbits",int),batargany("b",1))),
 command("batmkey", "hash", MKEYbathash, false, "calculate a hash value", args(1,2, batarg("",lng),batargany("b",1))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",bte))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",bte))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",sht))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",sht))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",int))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",int))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",lng))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",lng))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",oid))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",oid))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",lng))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",lng))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",flt))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",flt))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",dbl))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",dbl))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),argany("v",0))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batargany("b",1))),
 pattern("calc", "rotate_xor_hash", MKEYrotate_xor_hash, false, "", args(1,4, arg("",lng),arg("h",lng),arg("nbits",int),argany("v",1))),
 command("batcalc", "rotate_xor_hash", MKEYbulk_rotate_xor_hash, false, "", args(1,4, batarg("",int),batarg("h",lng),arg("nbits",int),batargany("b",1))),
#ifdef HAVE_HGE
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),arg("v",hge))),
 pattern("calc", "hash", MKEYhash, false, "", args(1,2, arg("",lng),arg("v",hge))),
 command("batcalc", "hash", MKEYbathash, false, "", args(1,2, batarg("",lng),batarg("b",hge))),
#endif
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mkey_mal)
{ mal_module("mkey", NULL, mkey_init_funcs); }
