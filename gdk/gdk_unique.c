/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
	BUN cnt;
	const void *v;
	const char *vals;
	const char *vars;
	int width;
	oid i, o;
	uint16_t *seen = NULL;
	const char *nme;
	Hash *hs = NULL;
	BUN hb;
	BATiter bi;
	int (*cmp)(const void *, const void *);
	bat parent = 0;
	struct canditer ci;
	const ValRecord *prop;
	const char *algomsg = "";
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	BATcheck(b, NULL);
	cnt = canditer_init(&ci, b, s);

	if (b->tkey || cnt <= 1 || BATtdense(b)) {
		/* trivial: already unique */
		bn = canditer_slice(&ci, 0, ci.ncand);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
			  " (already unique, slice candidates -- "
			  LLFMT "usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s),
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	if ((BATordered(b) && BATordered_rev(b)) ||
	    (b->ttype == TYPE_void && is_oid_nil(b->tseqbase))) {
		/* trivial: all values are the same */
		bn = BATdense(0, ci.seq, 1);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
			  " (all equal -- "
			  LLFMT "usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s),
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	assert(b->ttype != TYPE_void);

	BUN initsize = 1024;
	if (s == NULL) {
		MT_rwlock_rdlock(&b->thashlock);
		if (b->thash != NULL && b->thash != (Hash *) 1)
			initsize = b->thash->nunique;
		else if ((prop = BATgetprop_nolock(b, GDK_NUNIQUE)) != NULL)
			initsize = prop->val.oval;
		MT_rwlock_rdunlock(&b->thashlock);
	}
	bn = COLnew(0, TYPE_oid, initsize, TRANSIENT);
	if (bn == NULL)
		return NULL;
	vals = Tloc(b, 0);
	if (b->tvarsized && b->ttype)
		vars = b->tvheap->base;
	else
		vars = NULL;
	width = Tsize(b);
	cmp = ATOMcompare(b->ttype);
	bi = bat_iterator(b);

	if (BATordered(b) || BATordered_rev(b)) {
		const void *prev = NULL;
		algomsg = "unique: sorted";
		for (i = 0; i < ci.ncand; i++) {
			o = canditer_next(&ci);
			v = VALUE(o - b->hseqbase);
			if (prev == NULL || (*cmp)(v, prev) != 0) {
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
			}
			prev = v;
		}
	} else if (ATOMbasetype(b->ttype) == TYPE_bte) {
		unsigned char val;

		algomsg = "unique: byte-sized atoms";
		assert(vars == NULL);
		seen = GDKzalloc((256 / 16) * sizeof(seen[0]));
		if (seen == NULL)
			goto bunins_failed;
		for (i = 0; i < ci.ncand; i++) {
			o = canditer_next(&ci);
			val = ((const unsigned char *) vals)[o - b->hseqbase];
			if (!(seen[val >> 4] & (1U << (val & 0xF)))) {
				seen[val >> 4] |= 1U << (val & 0xF);
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
				if (bn->batCount == 256) {
					/* there cannot be more than
					 * 256 distinct values */
					break;
				}
			}
		}
		GDKfree(seen);
		seen = NULL;
	} else if (ATOMbasetype(b->ttype) == TYPE_sht) {
		unsigned short val;

		algomsg = "unique: short-sized atoms";
		assert(vars == NULL);
		seen = GDKzalloc((65536 / 16) * sizeof(seen[0]));
		if (seen == NULL)
			goto bunins_failed;
		for (i = 0; i < ci.ncand; i++) {
			o = canditer_next(&ci);
			val = ((const unsigned short *) vals)[o - b->hseqbase];
			if (!(seen[val >> 4] & (1U << (val & 0xF)))) {
				seen[val >> 4] |= 1U << (val & 0xF);
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
				if (bn->batCount == 65536) {
					/* there cannot be more than
					 * 65536 distinct values */
					break;
				}
			}
		}
		GDKfree(seen);
		seen = NULL;
	} else if (BATcheckhash(b) ||
		   (!b->batTransient &&
		    cnt == BATcount(b) &&
		    BAThash(b) == GDK_SUCCEED) ||
		   (/* DISABLES CODE */ (0) &&
		    (parent = VIEWtparent(b)) != 0 &&
		    BATcheckhash(BBPdescriptor(parent)))) {
		BUN lo;
		oid seq;

		/* we already have a hash table on b, or b is
		 * persistent and we could create a hash table, or b
		 * is a view on a bat that already has a hash table */
		algomsg = "unique: existing hash";
		seq = b->hseqbase;
		if (b->thash == NULL && /* DISABLES CODE */ (0) && (parent = VIEWtparent(b)) != 0) {
			BAT *b2 = BBPdescriptor(parent);
			lo = b->tbaseoff - b2->tbaseoff;
			b = b2;
			bi = bat_iterator(b);
			algomsg = "unique: existing parent hash";
		} else {
			lo = 0;
		}
		hs = b->thash;
		for (i = 0; i < ci.ncand; i++) {
			BUN p;

			o = canditer_next(&ci);
			p = o - seq;
			v = VALUE(p);
			for (hb = HASHgetlink(hs, p + lo);
			     hb != HASHnil(hs) && hb >= lo;
			     hb = HASHgetlink(hs, hb)) {
				assert(hb < p + lo);
				if (cmp(v, BUNtail(bi, hb)) == 0 &&
				    canditer_contains(&ci, hb - lo + seq)) {
					/* we've seen this value
					 * before */
					break;
				}
			}
			if (hb == HASHnil(hs) || hb < lo) {
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
			}
		}
	} else {
		BUN prb;
		BUN p;
		BUN mask;

		GDKclrerr();	/* not interested in BAThash errors */
		algomsg = "unique: new partial hash";
		nme = BBP_physical(b->batCacheid);
		if (ATOMbasetype(b->ttype) == TYPE_bte) {
			mask = (BUN) 1 << 8;
			cmp = NULL; /* no compare needed, "hash" is perfect */
		} else if (ATOMbasetype(b->ttype) == TYPE_sht) {
			mask = (BUN) 1 << 16;
			cmp = NULL; /* no compare needed, "hash" is perfect */
		} else {
			mask = HASHmask(cnt);
			if (mask < ((BUN) 1 << 16))
				mask = (BUN) 1 << 16;
		}
		if ((hs = GDKzalloc(sizeof(Hash))) == NULL) {
			GDKerror("cannot allocate hash table\n");
			goto bunins_failed;
		}
		if ((hs->heaplink.farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0 ||
		    (hs->heapbckt.farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0 ||
		    snprintf(hs->heaplink.filename, sizeof(hs->heaplink.filename), "%s.thshunil%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs->heaplink.filename) ||
		    snprintf(hs->heapbckt.filename, sizeof(hs->heapbckt.filename), "%s.thshunib%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs->heapbckt.filename) ||
		    HASHnew(hs, b->ttype, BUNlast(b), mask, BUN_NONE, false) != GDK_SUCCEED) {
			GDKfree(hs);
			hs = NULL;
			GDKerror("cannot allocate hash table\n");
			goto bunins_failed;
		}
		for (i = 0; i < ci.ncand; i++) {
			o = canditer_next(&ci);
			v = VALUE(o - b->hseqbase);
			prb = HASHprobe(hs, v);
			for (hb = HASHget(hs, prb);
			     hb != HASHnil(hs);
			     hb = HASHgetlink(hs, hb)) {
				if (cmp == NULL || cmp(v, BUNtail(bi, hb)) == 0)
					break;
			}
			if (hb == HASHnil(hs)) {
				p = o - b->hseqbase;
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)
					goto bunins_failed;
				/* enter into hash table */
				HASHputlink(hs, p, HASHget(hs, prb));
				HASHput(hs, prb, p);
			}
		}
		HEAPfree(&hs->heaplink, true);
		HEAPfree(&hs->heapbckt, true);
		GDKfree(hs);
	}

	bn->theap->dirty = true;
	bn->tsorted = true;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = true;
	bn->tnil = false;
	bn->tnonil = true;
	if (BATcount(bn) == BATcount(b)) {
		/* it turns out all values are distinct */
		assert(b->tnokey[0] == 0);
		assert(b->tnokey[1] == 0);
		b->tkey = true;
		b->batDirtydesc = true;
	}
	bn = virtualize(bn);
	MT_thread_setalgorithm(algomsg);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT
		  ",s=" ALGOOPTBATFMT " -> " ALGOOPTBATFMT
		  " (%s -- " LLFMT "usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), algomsg, GDKusec() - t0);
	return bn;

  bunins_failed:
	if (seen)
		GDKfree(seen);
	if (hs != NULL && hs != b->thash) {
		HEAPfree(&hs->heaplink, true);
		HEAPfree(&hs->heapbckt, true);
		GDKfree(hs);
	}
	BBPreclaim(bn);
	return NULL;
}
