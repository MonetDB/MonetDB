/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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

#define MKEYHASH_bte(valp)	((lng) valp)
#define MKEYHASH_sht(valp)	((lng) valp)
#define MKEYHASH_int(valp)	((lng) valp)
#define MKEYHASH_lng(valp)	(valp)
#ifdef HAVE_HGE
#define MKEYHASH_hge(valp)	((lng) (valp >> 64) ^ (lng) (valp))
#endif
#if SIZEOF_OID == SIZEOF_INT
#define MKEYHASH_oid(valp)	MKEYHASH_int(valp)
#else
#define MKEYHASH_oid(valp)	MKEYHASH_lng(valp)
#endif

static inline ulng
GDK_ROTATE(ulng x, int y, int z)
{
	return (x << y) | (x >> z);
}

static str
MKEYhash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *res = getArgReference_lng(stk,pci,0);
	ptr val = getArgReference(stk,pci,1);
	int tpe = getArgType(mb,pci,1);

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
		*res = MKEYHASH_bte((*(bte*)val));
		break;
	case TYPE_sht:
		*res = MKEYHASH_sht((*(sht*)val));
		break;
	case TYPE_int:
	case TYPE_flt:
		*res = MKEYHASH_int((*(int*)val));
		break;
	case TYPE_lng:
	case TYPE_dbl:
		*res = MKEYHASH_lng((*(lng*)val));
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		*res = MKEYHASH_hge((*(hge*)val));
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

#define MKEYbathashloop(TPE) \
	do { \
		const TPE *restrict v = (const TPE *) bi.base; \
		if (ci.tpe == cand_dense) { \
			for (BUN i = 0; i < ci.ncand; i++) { \
				oid p = (canditer_next_dense(&ci) - off); \
				r[i] = (ulng) MKEYHASH_##TPE(v[p]); \
			} \
		} else { \
			for (BUN i = 0; i < ci.ncand; i++) { \
				oid p = (canditer_next(&ci) - off); \
				r[i] = (ulng) MKEYHASH_##TPE(v[p]); \
			} \
		} \
	} while (0)

static str
MKEYbathash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 3 ? getArgReference_bat(stk, pci, 2) : NULL;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	str msg = MAL_SUCCEED;
	struct canditer ci = {0};
	oid off;
	ulng *restrict r;
	BATiter bi = {0};

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batmkey.bathash", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batmkey.bathash", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	canditer_init(&ci, b, bs);
	if (!(bn = COLnew(ci.hseq, TYPE_lng, ci.ncand, TRANSIENT))) {
		msg = createException(MAL, "batmkey.bathash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	off = b->hseqbase;
	r = (ulng *) Tloc(bn, 0);

	bi = bat_iterator(b);
	switch (ATOMstorage(b->ttype)) {
	case TYPE_void: {
		oid o = b->tseqbase;
		if (is_oid_nil(o)) {
			for (BUN i = 0; i < ci.ncand; i++) {
				r[i] = (ulng) lng_nil;
			}
		} else if (ci.tpe == cand_dense) {
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				r[i] = o + p;
			}
		} else {
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next(&ci) - off);
				r[i] = o + p;
			}
		}
	} break;
	case TYPE_bte:
		MKEYbathashloop(bte);
		break;
	case TYPE_sht:
		MKEYbathashloop(sht);
		break;
	case TYPE_int:
	case TYPE_flt:
		MKEYbathashloop(int);
		break;
	case TYPE_lng:
	case TYPE_dbl:
		MKEYbathashloop(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MKEYbathashloop(hge);
		break;
#endif
	default: {
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;

		if (ci.tpe == cand_dense) {
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				r[i] = (ulng) hash(BUNtail(bi, p));
			}
		} else {
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next(&ci) - off);
				r[i] = (ulng) hash(BUNtail(bi, p));
			}
		}
	}
	}
	bat_iterator_end(&bi);

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bs)
		BBPunfix(bs->batCacheid);
	if (bn) {
		assert(msg == MAL_SUCCEED);
		BATsetcount(bn, ci.ncand);
		bn->tnonil = false;
		bn->tnil = false;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		*res = bn->batCacheid;
		BBPkeepref(bn);
	}
	return msg;
}

static str
MKEYrotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *res = getArgReference_lng(stk, pci, 0);
	ulng h = *getArgReference_lng(stk, pci, 1), val;
	int lbit = *getArgReference_int(stk, pci, 2), rbit = (int) sizeof(lng) * 8 - lbit, tpe = getArgType(mb, pci, 3);
	ptr pval = getArgReference(stk, pci, 3);

	(void) cntxt;
	switch (ATOMstorage(tpe)) {
	case TYPE_bte:
		val = (ulng) MKEYHASH_bte((*(bte*)pval));
		break;
	case TYPE_sht:
		val = (ulng) MKEYHASH_sht((*(sht*)pval));
		break;
	case TYPE_int:
	case TYPE_flt:
		val = (ulng) MKEYHASH_int((*(int*)pval));
		break;
	case TYPE_lng:
	case TYPE_dbl:
		val = (ulng) MKEYHASH_lng((*(lng*)pval));
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		val = (ulng) MKEYHASH_hge((*(hge*)pval));
		break;
#endif
	default:
		if (ATOMextern(tpe))
			val = (ulng) ATOMhash(tpe, *(ptr*)pval);
		else
			val = (ulng) ATOMhash(tpe, pval);
		break;
	}
	*res = (lng) (GDK_ROTATE(h, lbit, rbit) ^ val);
	return MAL_SUCCEED;
}

#define MKEYbulk_rotate_xor_hashloop(TPE) \
	do { \
		const ulng *restrict h = (const ulng *) hbi.base; \
		const TPE *restrict v = (const TPE *) bi.base; \
		if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) { \
			for (BUN i = 0; i < ci1.ncand; i++) { \
				oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2); \
				r[i] = GDK_ROTATE(h[p1], lbit, rbit) ^ (ulng) MKEYHASH_##TPE(v[p2]); \
			} \
		} else { \
			for (BUN i = 0; i < ci1.ncand; i++) { \
				oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2); \
				r[i] = GDK_ROTATE(h[p1], lbit, rbit) ^ (ulng) MKEYHASH_##TPE(v[p2]); \
			} \
		} \
	} while (0)

static str
MKEYbulk_rotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0), *hid = getArgReference_bat(stk, pci, 1), *bid = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL, *sid2 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;
	BAT *hb = NULL, *b = NULL, *bn = NULL, *s1 = NULL, *s2 = NULL;
	int lbit = *getArgReference_int(stk, pci, 2), rbit = (int) sizeof(lng) * 8 - lbit;
	str msg = MAL_SUCCEED;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	ulng *restrict r;
	BATiter hbi = {0}, bi = {0};

	(void) cntxt;
	(void) mb;
	if (!(hb = BATdescriptor(*hid))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (b->ttype == TYPE_msk || mask_cand(b)) {
		BAT *ob = b;
		b = BATunmask(b);
		BBPunfix(ob->batCacheid);
		if (!b) {
			BBPunfix(hb->batCacheid);
			throw(MAL, "batmkey.rotate_xor_hash", GDK_EXCEPTION);
		}
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(s1 = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(s2 = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}

	canditer_init(&ci1, hb, s1);
	canditer_init(&ci2, b, s2);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}

	if (!(bn = COLnew(ci1.hseq, TYPE_lng, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	r = (ulng *) Tloc(bn, 0);

	off1 = hb->hseqbase;
	off2 = b->hseqbase;

	hbi = bat_iterator(hb);
	bi = bat_iterator(b);
	if (complex_cand(b)) {
		const ulng *restrict h = (const ulng *) hbi.base;
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = canditer_next(&ci1) - off1;
			oid p2 = canditer_next(&ci2) - off2;
			r[i] = GDK_ROTATE(h[p1], lbit, rbit) ^ MKEYHASH_oid(*(oid*)Tpos(&bi, p2));
		}
	} else {
		switch (ATOMstorage(b->ttype)) {
		case TYPE_bte:
			MKEYbulk_rotate_xor_hashloop(bte);
			break;
		case TYPE_sht:
			MKEYbulk_rotate_xor_hashloop(sht);
			break;
		case TYPE_int:
		case TYPE_flt:
			MKEYbulk_rotate_xor_hashloop(int);
			break;
		case TYPE_lng: { /* hb and b areas may overlap, so for this case the 'restrict' keyword cannot be used */
			const ulng *h = (const ulng *) hbi.base;
			const lng *v = (const lng *) bi.base;
			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				for (BUN i = 0; i < ci1.ncand; i++) {
					oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
					r[i] = GDK_ROTATE(h[p1], lbit, rbit) ^ (ulng) MKEYHASH_lng(v[p2]);
				}
			} else {
				for (BUN i = 0; i < ci1.ncand; i++) {
					oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
					r[i] = GDK_ROTATE(h[p1], lbit, rbit) ^ (ulng) MKEYHASH_lng(v[p2]);
				}
			}
		} break;
		case TYPE_dbl:
			MKEYbulk_rotate_xor_hashloop(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			MKEYbulk_rotate_xor_hashloop(hge);
			break;
#endif
		default: {
			const ulng *restrict h = (const ulng *) hbi.base;
			BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;

			if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
				for (BUN i = 0; i < ci1.ncand; i++) {
					oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
					r[i] = GDK_ROTATE(h[p1], lbit, rbit) ^ (ulng) hash(BUNtail(bi, p2));
				}
			} else {
				for (BUN i = 0; i < ci1.ncand; i++) {
					oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
					r[i] = GDK_ROTATE(h[p1], lbit, rbit) ^ (ulng) hash(BUNtail(bi, p2));
				}
			}
			break;
		}
		}
	}
	bat_iterator_end(&hbi);
	bat_iterator_end(&bi);

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (hb)
		BBPunfix(hb->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (bn) {
		assert(msg == MAL_SUCCEED);
		BATsetcount(bn, ci1.ncand);
		bn->tnonil = false;
		bn->tnil = false;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		*res = bn->batCacheid;
		BBPkeepref(bn);
	}
	return msg;
}

static str
MKEYbulkconst_rotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0), *hid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;
	int lbit = *getArgReference_int(stk, pci, 2), tpe = getArgType(mb, pci, 3), rbit = (int) sizeof(lng) * 8 - lbit;
	ptr pval = getArgReference(stk, pci, 3);
	BAT *hb = NULL, *bn = NULL, *bs = NULL;
	str msg = MAL_SUCCEED;
	struct canditer ci = {0};
	oid off;
	ulng *restrict r, val;
	const ulng *restrict h;
	BATiter hbi = {0};

	(void) cntxt;
	if (!(hb = BATdescriptor(*hid))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	canditer_init(&ci, hb, bs);
	if (!(bn = COLnew(ci.hseq, TYPE_lng, ci.ncand, TRANSIENT))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	off = hb->hseqbase;

	switch (ATOMstorage(tpe)) {
	case TYPE_bte:
		val = (ulng) MKEYHASH_bte((*(bte*)pval));
		break;
	case TYPE_sht:
		val = (ulng) MKEYHASH_sht((*(sht*)pval));
		break;
	case TYPE_int:
	case TYPE_flt:
		val = (ulng) MKEYHASH_int((*(int*)pval));
		break;
	case TYPE_lng:
	case TYPE_dbl:
		val = (ulng) MKEYHASH_lng((*(lng*)pval));
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		val = (ulng) MKEYHASH_hge((*(hge*)pval));
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
	hbi = bat_iterator(hb);
	h = (const ulng *) hbi.base;
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			r[i] = GDK_ROTATE(h[p], lbit, rbit) ^ val;
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			r[i] = GDK_ROTATE(h[p], lbit, rbit) ^ val;
		}
	}
	bat_iterator_end(&hbi);

bailout:
	if (hb)
		BBPunfix(hb->batCacheid);
	if (bs)
		BBPunfix(bs->batCacheid);
	if (bn) {
		assert(msg == MAL_SUCCEED);
		BATsetcount(bn, ci.ncand);
		bn->tnonil = false;
		bn->tnil = false;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		*res = bn->batCacheid;
		BBPkeepref(bn);
	}
	return msg;
}

#define MKEYconstbulk_rotate_xor_hashloop(TPE) \
	do { \
		const TPE *restrict v = (const TPE *) bi.base; \
		if (ci.tpe == cand_dense) { \
			for (BUN i = 0; i < ci.ncand; i++) { \
				oid p = (canditer_next_dense(&ci) - off); \
				r[i] = h ^ (ulng) MKEYHASH_##TPE(v[p]); \
			} \
		} else { \
			for (BUN i = 0; i < ci.ncand; i++) { \
				oid p = (canditer_next(&ci) - off); \
				r[i] = h ^ (ulng) MKEYHASH_##TPE(v[p]); \
			} \
		} \
	} while (0)

static str
MKEYconstbulk_rotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;
	int lbit = *getArgReference_int(stk, pci, 2), rbit = (int) sizeof(lng) * 8 - lbit;
	BAT *b = NULL, *bn = NULL, *bs = NULL;
	str msg = MAL_SUCCEED;
	struct canditer ci = {0};
	oid off;
	ulng *restrict r, h = GDK_ROTATE((ulng) *getArgReference_lng(stk, pci, 1), lbit, rbit);
	BATiter bi = {0};

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	canditer_init(&ci, b, bs);
	if (!(bn = COLnew(ci.hseq, TYPE_lng, ci.ncand, TRANSIENT))) {
		msg = createException(MAL, "batmkey.rotate_xor_hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	off = b->hseqbase;
	r = (ulng *) Tloc(bn, 0);

	bi = bat_iterator(b);
	switch (ATOMstorage(b->ttype)) {
	case TYPE_bte:
		MKEYconstbulk_rotate_xor_hashloop(bte);
		break;
	case TYPE_sht:
		MKEYconstbulk_rotate_xor_hashloop(sht);
		break;
	case TYPE_int:
	case TYPE_flt:
		MKEYconstbulk_rotate_xor_hashloop(int);
		break;
	case TYPE_lng:
	case TYPE_dbl:
		MKEYconstbulk_rotate_xor_hashloop(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		MKEYconstbulk_rotate_xor_hashloop(hge);
		break;
#endif
	default: {
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;

		if (ci.tpe == cand_dense) {
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				r[i] = h ^ (ulng) hash(BUNtail(bi, p));
			}
		} else {
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next(&ci) - off);
				r[i] = h ^ (ulng) hash(BUNtail(bi, p));
			}
		}
		break;
	}
	}
	bat_iterator_end(&bi);

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bs)
		BBPunfix(bs->batCacheid);
	if (bn) {
		assert(msg == MAL_SUCCEED);
		BATsetcount(bn, ci.ncand);
		bn->tnonil = false;
		bn->tnil = false;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		*res = bn->batCacheid;
		BBPkeepref(bn);
	}
	return msg;
}

#include "mel.h"
mel_func mkey_init_funcs[] = {
 pattern("mkey", "hash", MKEYhash, false, "calculate a hash value", args(1,2, arg("",lng),argany("v",0))),
 pattern("batmkey", "hash", MKEYbathash, false, "calculate a hash value", args(1,2, batarg("",lng),batargany("b",0))),
 pattern("batmkey", "hash", MKEYbathash, false, "calculate a hash value, with a candidate list", args(1,3, batarg("",lng),batargany("b",0),batarg("s",oid))),
 pattern("mkey", "rotate_xor_hash", MKEYrotate_xor_hash, false, "post: [:xor=]([:rotate=](h, nbits), [hash](b))", args(1,4, arg("",lng),arg("h",lng),arg("nbits",int),argany("v",0))),
 pattern("batmkey", "rotate_xor_hash", MKEYbulkconst_rotate_xor_hash, false, "pre: h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b))", args(1,4, batarg("",lng),batarg("h",lng),arg("nbits",int),argany("v",0))),
 pattern("batmkey", "rotate_xor_hash", MKEYbulkconst_rotate_xor_hash, false, "pre: h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b)), with a candidate list", args(1,5, batarg("",lng),batarg("h",lng),arg("nbits",int),argany("v",0),batarg("s",oid))),
 pattern("batmkey", "rotate_xor_hash", MKEYconstbulk_rotate_xor_hash, false, "pre: h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b))", args(1,4, batarg("",lng),arg("h",lng),arg("nbits",int),batargany("b",0))),
 pattern("batmkey", "rotate_xor_hash", MKEYconstbulk_rotate_xor_hash, false, "pre: h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b)), with a candidate list", args(1,5, batarg("",lng),arg("h",lng),arg("nbits",int),batargany("b",0),batarg("s",oid))),
 pattern("batmkey", "rotate_xor_hash", MKEYbulk_rotate_xor_hash, false, "pre: h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b))", args(1,4, batarg("",lng),batarg("h",lng),arg("nbits",int),batargany("b",0))),
 pattern("batmkey", "rotate_xor_hash", MKEYbulk_rotate_xor_hash, false, "pre: h and b should be synced on head\npost: [:xor=]([:rotate=](h, nbits), [hash](b)), with candidate lists", args(1,6, batarg("",lng),batarg("h",lng),arg("nbits",int),batargany("b",0),batarg("s1",oid),batarg("s2",oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mkey_mal)
{ mal_module("mkey", NULL, mkey_init_funcs); }
