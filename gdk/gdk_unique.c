/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

#define VALUE(x)	(vars ? vars + VarHeapVal(vals, (x), width) : vals + (x) * width)
/* BATunique returns a bat that indicates the unique tail values of
 * the input bat.  This is essentially the same output as the
 * "extents" output of BATgroup.  The difference is that BATunique
 * does not return the grouping bat.
 *
 * The first input is the bat from which unique rows are selected, the
 * second input is an optional candidate list.
 *
 * The return value is a candidate list.
 */
BAT *
BATunique(BAT *b, BAT *s)
{
	BAT *bn;
	const void *v;
	const char *vals;
	const char *vars;
	int width;
	oid i, o, hseq;
	const char *nme;
	BUN hb;
	bool (*eq)(const void *, const void *);
	struct canditer ci;
	const char *algomsg = "";
	lng t0 = 0;

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	canditer_init(&ci, b, s);

	(void) BATordered(b);
	(void) BATordered_rev(b);
	BATiter bi = bat_iterator(b);
	if (bi.key || ci.ncand <= 1 || BATtdensebi(&bi)) {
		/* trivial: already unique */
		bn = canditer_slice(&ci, 0, ci.ncand);
		bat_iterator_end(&bi);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
			  " (already unique, slice candidates -- "
			  LLFMT "usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s),
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	if ((bi.sorted && bi.revsorted) ||
	    (bi.type == TYPE_void && is_oid_nil(b->tseqbase))) {
		/* trivial: all values are the same */
		bn = BATdense(0, ci.seq, 1);
		bat_iterator_end(&bi);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
			  " (all equal -- "
			  LLFMT "usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s),
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	assert(bi.type != TYPE_void);

	BUN initsize = BUN_NONE;
	if (s == NULL) {
		MT_rwlock_rdlock(&b->thashlock);
		if (b->thash != NULL && b->thash != (Hash *) 1)
			initsize = b->thash->nunique;
		MT_rwlock_rdunlock(&b->thashlock);
		if (initsize == BUN_NONE && bi.unique_est != 0)
			initsize = (BUN) bi.unique_est;
	}
	if (initsize == BUN_NONE)
		initsize = 1024;
	bn = COLnew(0, TYPE_oid, initsize, TRANSIENT);
	if (bn == NULL) {
		bat_iterator_end(&bi);
		return NULL;
	}
	vals = bi.base;
	if (bi.vh && bi.type)
		vars = bi.vh->base;
	else
		vars = NULL;
	width = bi.width;
	eq = ATOMequal(bi.type);
	hseq = b->hseqbase;

	if (ATOMbasetype(bi.type) == TYPE_bte ||
	    (bi.width == 1 &&
	     ATOMstorage(bi.type) == TYPE_str &&
	     GDK_ELIMDOUBLES(bi.vh))) {
		uint8_t val;

		algomsg = "unique: byte-sized atoms";
		uint32_t seen[256 >> 5];
		memset(seen, 0, sizeof(seen));
		TIMEOUT_LOOP_IDX(i, ci.ncand, qry_ctx) {
			o = canditer_next(&ci);
			val = ((const uint8_t *) vals)[o - hseq];
			uint32_t m = UINT32_C(1) << (val & 0x1F);
			if (!(seen[val >> 5] & m)) {
				seen[val >> 5] |= m;
				if (bunfastappOID(bn, o) != GDK_SUCCEED)
					goto bunins_failed;
				if (bn->batCount == 256) {
					/* there cannot be more than
					 * 256 distinct values */
					break;
				}
			}
		}
		TIMEOUT_CHECK(qry_ctx,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed, qry_ctx));
	} else if (ATOMbasetype(bi.type) == TYPE_sht ||
		   (bi.width == 2 &&
		    ATOMstorage(bi.type) == TYPE_str &&
		    GDK_ELIMDOUBLES(bi.vh))) {
		uint16_t val;

		algomsg = "unique: short-sized atoms";
		uint32_t seen[65536 >> 5];
		memset(seen, 0, sizeof(seen));
		TIMEOUT_LOOP_IDX(i, ci.ncand, qry_ctx) {
			o = canditer_next(&ci);
			val = ((const uint16_t *) vals)[o - hseq];
			uint32_t m = UINT32_C(1) << (val & 0x1F);
			if (!(seen[val >> 5] & m)) {
				seen[val >> 5] |= m;
				if (bunfastappOID(bn, o) != GDK_SUCCEED)
					goto bunins_failed;
				if (bn->batCount == 65536) {
					/* there cannot be more than
					 * 65536 distinct values */
					break;
				}
			}
		}
		TIMEOUT_CHECK(qry_ctx,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed, qry_ctx));
	} else if (bi.sorted || bi.revsorted) {
		const void *prev = NULL;
		algomsg = "unique: sorted";
		TIMEOUT_LOOP_IDX(i, ci.ncand, qry_ctx) {
			o = canditer_next(&ci);
			v = VALUE(o - hseq);
			if (prev == NULL || !(*eq)(v, prev)) {
				if (bunfastappOID(bn, o) != GDK_SUCCEED)
					goto bunins_failed;
			}
			prev = v;
		}
		TIMEOUT_CHECK(qry_ctx,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed, qry_ctx));
	} else if (BATcheckhash(b) ||
		   ((!bi.transient ||
		     (b->batRole == PERSISTENT && GDKinmemory(0))) &&
		    ci.ncand == bi.count &&
		    BAThash(b) == GDK_SUCCEED)) {
		/* we already have a hash table on b, or b is
		 * persistent and we could create a hash table */
		algomsg = "unique: existing hash";
		MT_rwlock_rdlock(&b->thashlock);
		Hash *hs = b->thash;
		if (hs == NULL) {
			MT_rwlock_rdunlock(&b->thashlock);
			goto lost_hash;
		}
		TIMEOUT_LOOP_IDX(i, ci.ncand, qry_ctx) {
			BUN p;

			o = canditer_next(&ci);
			p = o - hseq;
			v = VALUE(p);
			for (hb = HASHgetlink(hs, p);
			     hb != BUN_NONE;
			     hb = HASHgetlink(hs, hb)) {
				assert(hb < p);
				if (eq(v, BUNtail(&bi, hb)) &&
				    canditer_contains(&ci, hb + hseq)) {
					/* we've seen this value
					 * before */
					break;
				}
			}
			if (hb == BUN_NONE) {
				if (bunfastappOID(bn, o) != GDK_SUCCEED) {
					MT_rwlock_rdunlock(&b->thashlock);
					goto bunins_failed;
				}
			}
		}
		MT_rwlock_rdunlock(&b->thashlock);
		TIMEOUT_CHECK(qry_ctx,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed, qry_ctx));
	} else {
		BUN prb;
		BUN p;
		BUN mask;

	  lost_hash:
		GDKclrerr();	/* not interested in BAThash errors */
		algomsg = "unique: new partial hash";
		nme = BBP_physical(b->batCacheid);
		if (ATOMbasetype(bi.type) == TYPE_bte) {
			mask = (BUN) 1 << 8;
			eq = NULL; /* no compare needed, "hash" is perfect */
		} else if (ATOMbasetype(bi.type) == TYPE_sht) {
			mask = (BUN) 1 << 16;
			eq = NULL; /* no compare needed, "hash" is perfect */
		} else {
			mask = HASHmask(ci.ncand);
			if (mask < ((BUN) 1 << 16))
				mask = (BUN) 1 << 16;
		}
		Hash hsh = {
			.heaplink.parentid = b->batCacheid,
			.heaplink.farmid = BBPselectfarm(TRANSIENT, bi.type, hashheap),
			.heapbckt.parentid = b->batCacheid,
			.heapbckt.farmid = BBPselectfarm(TRANSIENT, bi.type, hashheap),
		};

		if (hsh.heaplink.farmid < 0 ||
		    hsh.heapbckt.farmid < 0 ||
		    snprintf(hsh.heaplink.filename, sizeof(hsh.heaplink.filename), "%s.thshunil%x", nme, (unsigned) MT_getpid()) >= (int) sizeof(hsh.heaplink.filename) ||
		    snprintf(hsh.heapbckt.filename, sizeof(hsh.heapbckt.filename), "%s.thshunib%x", nme, (unsigned) MT_getpid()) >= (int) sizeof(hsh.heapbckt.filename) ||
		    HASHnew(&hsh, bi.type, BATcount(b), mask, BUN_NONE, false) != GDK_SUCCEED) {
			GDKerror("cannot allocate hash table\n");
			HEAPfree(&hsh.heaplink, true);
			HEAPfree(&hsh.heapbckt, true);
			goto bunins_failed;
		}
		TIMEOUT_LOOP_IDX(i, ci.ncand, qry_ctx) {
			o = canditer_next(&ci);
			v = VALUE(o - hseq);
			prb = HASHprobe(&hsh, v);
			for (hb = HASHget(&hsh, prb);
			     hb != BUN_NONE;
			     hb = HASHgetlink(&hsh, hb)) {
				if (eq == NULL || eq(v, BUNtail(&bi, hb)))
					break;
			}
			if (hb == BUN_NONE) {
				p = o - hseq;
				if (bunfastappOID(bn, o) != GDK_SUCCEED) {
					HEAPfree(&hsh.heaplink, true);
					HEAPfree(&hsh.heapbckt, true);
					goto bunins_failed;
				}
				/* enter into hash table */
				HASHputlink(&hsh, p, HASHget(&hsh, prb));
				HASHput(&hsh, prb, p);
			}
		}
		HEAPfree(&hsh.heaplink, true);
		HEAPfree(&hsh.heapbckt, true);
		TIMEOUT_CHECK(qry_ctx,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed, qry_ctx));
	}
	if (BATcount(bn) == bi.count) {
		/* it turns out all values are distinct */
		MT_lock_set(&b->theaplock);
		if (BATcount(b) == bi.count) {
			/* and the input hasn't changed in the mean
			 * time--the only allowed change being appends;
			 * updates not allowed since the candidate list
			 * covers the complete bat */
			assert(b->tnokey[0] == 0);
			assert(b->tnokey[1] == 0);
			b->tkey = true;
		}
		MT_lock_unset(&b->theaplock);
	}
	bat_iterator_end(&bi);

	bn->theap->dirty = true;
	bn->tsorted = true;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = true;
	bn->tnil = false;
	bn->tnonil = true;
	bn = virtualize(bn);
	MT_thread_setalgorithm(algomsg);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT
		  ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT "usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), algomsg, GDKusec() - t0);
	return bn;

  bunins_failed:
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}
