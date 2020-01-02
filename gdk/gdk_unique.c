/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
 * The inputs must be dense-headed, the first input is the bat from
 * which unique rows are selected, the second input is a list of
 * candidates.
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
	bat parent;
	struct canditer ci;

	BATcheck(b, "BATunique", NULL);
	cnt = canditer_init(&ci, b, s);

	if (b->tkey || cnt <= 1 || BATtdense(b)) {
		/* trivial: already unique */
		bn = canditer_slice(&ci, 0, ci.ncand);
		ALGODEBUG fprintf(stderr, "#%s: BATunique(b=" ALGOBATFMT
				  ",s=" ALGOOPTBATFMT ")=" ALGOOPTBATFMT
				  ": trivial case: "
				  "already unique, slice candidates\n", MT_thread_getname(),
				  ALGOBATPAR(b), ALGOOPTBATPAR(s),
				  ALGOOPTBATPAR(bn));
		return bn;
	}

	if ((BATordered(b) && BATordered_rev(b)) ||
	    (b->ttype == TYPE_void && is_oid_nil(b->tseqbase))) {
		/* trivial: all values are the same */
		bn = BATdense(0, ci.seq, 1);
		ALGODEBUG fprintf(stderr, "#%s: BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT ")=" ALGOOPTBATFMT
				  ": trivial case: all equal\n", MT_thread_getname(),
				  ALGOBATPAR(b), ALGOOPTBATPAR(s),
				  ALGOOPTBATPAR(bn));
		return bn;
	}

	assert(b->ttype != TYPE_void);

	bn = COLnew(0, TYPE_oid, 1024, TRANSIENT);
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

		ALGODEBUG fprintf(stderr, "#%s: BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): (reverse) sorted\n", MT_thread_getname(),
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
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

		ALGODEBUG fprintf(stderr, "#%s: BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): byte sized atoms\n", MT_thread_getname(),
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
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

		ALGODEBUG fprintf(stderr, "#%s: BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): short sized atoms\n", MT_thread_getname(),
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
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
		    BAThash(b) == GDK_SUCCEED) ||
		   ((parent = VIEWtparent(b)) != 0 &&
		    BATcheckhash(BBPdescriptor(parent)))) {
		BUN lo;
		oid seq;

		/* we already have a hash table on b, or b is
		 * persistent and we could create a hash table, or b
		 * is a view on a bat that already has a hash table */
		ALGODEBUG fprintf(stderr, "#%s: BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): use existing hash\n", MT_thread_getname(),
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		seq = b->hseqbase;
		if (b->thash == NULL && (parent = VIEWtparent(b)) != 0) {
			BAT *b2 = BBPdescriptor(parent);
			lo = (BUN) ((b->theap.base - b2->theap.base) >> b->tshift);
			b = b2;
			bi = bat_iterator(b);
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
				    canditer_search(&ci,
						    hb - lo + seq,
						    false) != BUN_NONE) {
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
		int len;

		GDKclrerr();	/* not interested in BAThash errors */
		ALGODEBUG fprintf(stderr, "#%s: BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): create partial hash\n", MT_thread_getname(),
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		nme = BBP_physical(b->batCacheid);
		if (ATOMbasetype(b->ttype) == TYPE_bte) {
			mask = (BUN) 1 << 8;
			cmp = NULL; /* no compare needed, "hash" is perfect */
		} else if (ATOMbasetype(b->ttype) == TYPE_sht) {
			mask = (BUN) 1 << 16;
			cmp = NULL; /* no compare needed, "hash" is perfect */
		} else {
			if (s)
				mask = HASHmask(s->batCount);
			else
				mask = HASHmask(b->batCount);
			if (mask < ((BUN) 1 << 16))
				mask = (BUN) 1 << 16;
		}
		if ((hs = GDKzalloc(sizeof(Hash))) == NULL) {
			GDKerror("BATunique: cannot allocate hash table\n");
			goto bunins_failed;
		}
		len = snprintf(hs->heap.filename, sizeof(hs->heap.filename), "%s.hash%d", nme, THRgettid());
		if (len == -1 || len >= (int) sizeof(hs->heap.filename) ||
		    HASHnew(hs, b->ttype, BUNlast(b), mask, BUN_NONE) != GDK_SUCCEED) {
			GDKfree(hs);
			hs = NULL;
			GDKerror("BATunique: cannot allocate hash table\n");
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
		HEAPfree(&hs->heap, true);
		GDKfree(hs);
	}

	bn->theap.dirty = true;
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
	ALGODEBUG fprintf(stderr, "#%s: BATunique(b=" ALGOBATFMT ","
			  "s=" ALGOOPTBATFMT ")="
			  ALGOBATFMT "\n", MT_thread_getname(),
			  ALGOBATPAR(b), ALGOOPTBATPAR(s),
			  ALGOBATPAR(bn));
	return bn;

  bunins_failed:
	if (seen)
		GDKfree(seen);
	if (hs != NULL && hs != b->thash) {
		HEAPfree(&hs->heap, true);
		GDKfree(hs);
	}
	BBPreclaim(bn);
	return NULL;
}
