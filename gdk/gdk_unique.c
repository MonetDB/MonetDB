/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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
	Hash *hs = NULL;
	BUN hb;
	int (*cmp)(const void *, const void *);
	struct canditer ci;
	const char *algomsg = "";
	lng t0 = 0;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

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
	cmp = ATOMcompare(bi.type);
	hseq = b->hseqbase;

	if (ATOMbasetype(bi.type) == TYPE_bte ||
	    (bi.width == 1 &&
	     ATOMstorage(bi.type) == TYPE_str &&
	     GDK_ELIMDOUBLES(bi.vh))) {
		uint8_t val;

		algomsg = "unique: byte-sized atoms";
		uint32_t seen[256 >> 5];
		memset(seen, 0, sizeof(seen));
		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {
			o = canditer_next(&ci);
			val = ((const uint8_t *) vals)[o - hseq];
			uint32_t m = UINT32_C(1) << (val & 0x1F);
			if (!(seen[val >> 5] & m)) {
				seen[val >> 5] |= m;
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
				if (bn->batCount == 256) {
					/* there cannot be more than
					 * 256 distinct values */
					break;
				}
			}
		}
		TIMEOUT_CHECK(timeoffset,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed));
	} else if (ATOMbasetype(bi.type) == TYPE_sht ||
		   (bi.width == 2 &&
		    ATOMstorage(bi.type) == TYPE_str &&
		    GDK_ELIMDOUBLES(bi.vh))) {
		uint16_t val;

		algomsg = "unique: short-sized atoms";
		uint32_t seen[65536 >> 5];
		memset(seen, 0, sizeof(seen));
		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {
			o = canditer_next(&ci);
			val = ((const uint16_t *) vals)[o - hseq];
			uint32_t m = UINT32_C(1) << (val & 0x1F);
			if (!(seen[val >> 5] & m)) {
				seen[val >> 5] |= m;
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
				if (bn->batCount == 65536) {
					/* there cannot be more than
					 * 65536 distinct values */
					break;
				}
			}
		}
		TIMEOUT_CHECK(timeoffset,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed));
	} else if (bi.sorted || bi.revsorted) {
		const void *prev = NULL;
		algomsg = "unique: sorted";
		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {
			o = canditer_next(&ci);
			v = VALUE(o - hseq);
			if (prev == NULL || (*cmp)(v, prev) != 0) {
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
			}
			prev = v;
		}
		TIMEOUT_CHECK(timeoffset,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed));
	} else if (BATcheckhash(b) ||
		   ((!bi.transient ||
		     (b->batRole == PERSISTENT && GDKinmemory(0))) &&
		    ci.ncand == bi.count &&
		    BAThash(b) == GDK_SUCCEED)) {
		BUN lo = 0;

		/* we already have a hash table on b, or b is
		 * persistent and we could create a hash table, or b
		 * is a view on a bat that already has a hash table */
		algomsg = "unique: existing hash";
		MT_rwlock_rdlock(&b->thashlock);
		hs = b->thash;
		if (hs == NULL) {
			MT_rwlock_rdunlock(&b->thashlock);
			goto lost_hash;
		}
		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {
			BUN p;

			o = canditer_next(&ci);
			p = o - hseq;
			v = VALUE(p);
			for (hb = HASHgetlink(hs, p + lo);
			     hb != BUN_NONE && hb >= lo;
			     hb = HASHgetlink(hs, hb)) {
				assert(hb < p + lo);
				if (cmp(v, BUNtail(bi, hb)) == 0 &&
				    canditer_contains(&ci, hb - lo + hseq)) {
					/* we've seen this value
					 * before */
					break;
				}
			}
			if (hb == BUN_NONE || hb < lo) {
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED) {
					MT_rwlock_rdunlock(&b->thashlock);
					hs = NULL;
					goto bunins_failed;
				}
			}
		}
		MT_rwlock_rdunlock(&b->thashlock);
		TIMEOUT_CHECK(timeoffset,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed));
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
			cmp = NULL; /* no compare needed, "hash" is perfect */
		} else if (ATOMbasetype(bi.type) == TYPE_sht) {
			mask = (BUN) 1 << 16;
			cmp = NULL; /* no compare needed, "hash" is perfect */
		} else {
			mask = HASHmask(ci.ncand);
			if (mask < ((BUN) 1 << 16))
				mask = (BUN) 1 << 16;
		}
		if ((hs = GDKzalloc(sizeof(Hash))) == NULL) {
			GDKerror("cannot allocate hash table\n");
			goto bunins_failed;
		}
		if ((hs->heaplink.farmid = BBPselectfarm(TRANSIENT, bi.type, hashheap)) < 0 ||
		    (hs->heapbckt.farmid = BBPselectfarm(TRANSIENT, bi.type, hashheap)) < 0 ||
		    snprintf(hs->heaplink.filename, sizeof(hs->heaplink.filename), "%s.thshunil%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs->heaplink.filename) ||
		    snprintf(hs->heapbckt.filename, sizeof(hs->heapbckt.filename), "%s.thshunib%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs->heapbckt.filename) ||
		    HASHnew(hs, bi.type, BATcount(b), mask, BUN_NONE, false) != GDK_SUCCEED) {
			GDKfree(hs);
			hs = NULL;
			GDKerror("cannot allocate hash table\n");
			goto bunins_failed;
		}
		TIMEOUT_LOOP_IDX(i, ci.ncand, timeoffset) {
			o = canditer_next(&ci);
			v = VALUE(o - hseq);
			prb = HASHprobe(hs, v);
			for (hb = HASHget(hs, prb);
			     hb != BUN_NONE;
			     hb = HASHgetlink(hs, hb)) {
				if (cmp == NULL || cmp(v, BUNtail(bi, hb)) == 0)
					break;
			}
			if (hb == BUN_NONE) {
				p = o - hseq;
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
				/* enter into hash table */
				HASHputlink(hs, p, HASHget(hs, prb));
				HASHput(hs, prb, p);
			}
		}
		TIMEOUT_CHECK(timeoffset,
			      GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed));
		HEAPfree(&hs->heaplink, true);
		HEAPfree(&hs->heapbckt, true);
		GDKfree(hs);
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
	if (hs != NULL) {
		HEAPfree(&hs->heaplink, true);
		HEAPfree(&hs->heapbckt, true);
		GDKfree(hs);
	}
	BBPreclaim(bn);
	return NULL;
}
