/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
	BUN start, end, cnt;
	const oid *cand = NULL, *candend = NULL;
	const void *v;
	const char *vals;
	const char *vars;
	int width;
	oid i, o;
	unsigned short *seen = NULL;
	const char *nme;
	Hash *hs = NULL;
	BUN hb;
	BATiter bi;
	int (*cmp)(const void *, const void *);
	bat parent;

	BATcheck(b, "BATunique", NULL);
	if (b->tkey || BATcount(b) <= 1 || BATtdense(b)) {
		/* trivial: already unique */
		if (!b->tkey) {
			b->tkey = true;
			b->batDirtydesc = true;
		}
		if (s) {
			/* we can return a slice of the candidate list */
			oid lo = b->hseqbase;
			oid hi = lo + BATcount(b);
			ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT
					  ",s=" ALGOBATFMT "): trivial case: "
					  "already unique, slice candidates\n",
					  ALGOBATPAR(b), ALGOBATPAR(s));
			b = BATselect(s, NULL, &lo, &hi, true, false, false);
			if (b == NULL)
				return NULL;
			bn = BATproject(b, s);
			BBPunfix(b->batCacheid);
			bn = virtualize(bn);
			ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ","
					  "s=" ALGOBATFMT ")="
					  ALGOOPTBATFMT "\n",
					  ALGOBATPAR(b), ALGOBATPAR(s),
					  ALGOOPTBATPAR(bn));
			return bn;
		}
		/* we can return all values */
		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s=NULL):"
				  " trivial case: already unique, return all\n",
				  ALGOBATPAR(b));
		return BATdense(0, b->hseqbase, BATcount(b));
	}

	CANDINIT(b, s, start, end, cnt, cand, candend);

	if (start == end) {
		/* trivial: empty result */
		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): trivial case: empty\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		return BATdense(0, b->hseqbase, 0);
	}

	if ((BATordered(b) && BATordered_rev(b)) ||
	    (b->ttype == TYPE_void && is_oid_nil(b->tseqbase))) {
		/* trivial: all values are the same */
		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): trivial case: all equal\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		return BATdense(0, cand ? *cand : b->hseqbase, 1);
	}

	if (cand && BATcount(b) > 16 * BATcount(s)) {
		BAT *nb, *r, *nr;

		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s="
				  ALGOBATFMT "): recurse: few candidates\n",
				  ALGOBATPAR(b), ALGOBATPAR(s));
		nb = BATproject(s, b);
		if (nb == NULL)
			return NULL;
		r = BATunique(nb, NULL);
		if (r == NULL) {
			BBPunfix(nb->batCacheid);
			return NULL;
		}
		nr = BATproject(r, s);
		BBPunfix(nb->batCacheid);
		BBPunfix(r->batCacheid);
		nr = virtualize(nr);
		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ","
				  "s=" ALGOBATFMT ")="
				  ALGOOPTBATFMT "\n",
				  ALGOBATPAR(b), ALGOBATPAR(s),
				  ALGOOPTBATPAR(nr));
		return nr;
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

		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): (reverse) sorted\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				i = *cand++ - b->hseqbase;
				if (i >= end)
					break;
			} else {
				i = start++;
				if (i == end)
					break;
			}
			v = VALUE(i);
			if (prev == NULL || (*cmp)(v, prev) != 0) {
				o = i + b->hseqbase;
				bunfastappTYPE(oid, bn, &o);
			}
			prev = v;
		}
	} else if (ATOMbasetype(b->ttype) == TYPE_bte) {
		unsigned char val;

		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): byte sized atoms\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		assert(vars == NULL);
		seen = GDKzalloc((256 / 16) * sizeof(seen[0]));
		if (seen == NULL)
			goto bunins_failed;
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				i = *cand++ - b->hseqbase;
				if (i >= end)
					break;
			} else {
				i = start++;
				if (i == end)
					break;
			}
			val = ((const unsigned char *) vals)[i];
			if (!(seen[val >> 4] & (1U << (val & 0xF)))) {
				seen[val >> 4] |= 1U << (val & 0xF);
				o = i + b->hseqbase;
				bunfastappTYPE(oid, bn, &o);
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

		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): short sized atoms\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		assert(vars == NULL);
		seen = GDKzalloc((65536 / 16) * sizeof(seen[0]));
		if (seen == NULL)
			goto bunins_failed;
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				i = *cand++ - b->hseqbase;
				if (i >= end)
					break;
			} else {
				i = start++;
				if (i == end)
					break;
			}
			val = ((const unsigned short *) vals)[i];
			if (!(seen[val >> 4] & (1U << (val & 0xF)))) {
				seen[val >> 4] |= 1U << (val & 0xF);
				o = i + b->hseqbase;
				bunfastappTYPE(oid, bn, &o);
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
		   (b->batPersistence == PERSISTENT &&
		    BAThash(b) == GDK_SUCCEED) ||
		   ((parent = VIEWtparent(b)) != 0 &&
		    BATcheckhash(BBPdescriptor(parent)))) {
		BUN lo;
		oid seq;

		/* we already have a hash table on b, or b is
		 * persistent and we could create a hash table, or b
		 * is a view on a bat that already has a hash table */
		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): use existing hash\n",
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
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				i = *cand++ - seq;
				if (i >= end)
					break;
			} else {
				i = start++;
				if (i == end)
					break;
			}
			v = VALUE(i);
			for (hb = HASHgetlink(hs, i + lo);
			     hb != HASHnil(hs) && hb >= lo;
			     hb = HASHgetlink(hs, hb)) {
				assert(hb < i + lo);
				if (cmp(v, BUNtail(bi, hb)) == 0) {
					o = hb - lo + seq;
					if (cand == NULL ||
					    SORTfnd(s, &o) != BUN_NONE) {
						/* we've seen this
						 * value before */
						break;
					}
				}
			}
			if (hb == HASHnil(hs) || hb < lo) {
				o = i + seq;
				bunfastappTYPE(oid, bn, &o);
			}
		}
	} else {
		BUN prb;
		BUN p;
		BUN mask;

		GDKclrerr();	/* not interested in BAThash errors */
		ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ",s="
				  ALGOOPTBATFMT "): create partial hash\n",
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
		if ((hs = GDKzalloc(sizeof(Hash))) == NULL ||
		    snprintf(hs->heap.filename, sizeof(hs->heap.filename),
			     "%s.hash%d", nme, THRgettid()) < 0 ||
		    HASHnew(hs, b->ttype, BUNlast(b), mask, BUN_NONE) != GDK_SUCCEED) {
			GDKfree(hs);
			hs = NULL;
			GDKerror("BATunique: cannot allocate hash table\n");
			goto bunins_failed;
		}
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				i = *cand++ - b->hseqbase;
				if (i >= end)
					break;
			} else {
				i = start++;
				if (i == end)
					break;
			}
			v = VALUE(i);
			prb = HASHprobe(hs, v);
			for (hb = HASHget(hs, prb);
			     hb != HASHnil(hs);
			     hb = HASHgetlink(hs, hb)) {
				if (cmp == NULL || cmp(v, BUNtail(bi, hb)) == 0)
					break;
			}
			if (hb == HASHnil(hs)) {
				o = i + b->hseqbase;
				p = i;
				bunfastappTYPE(oid, bn, &o);
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
	ALGODEBUG fprintf(stderr, "#BATunique(b=" ALGOBATFMT ","
			  "s=" ALGOBATFMT ")="
			  ALGOOPTBATFMT "\n",
			  ALGOBATPAR(b), ALGOBATPAR(s),
			  ALGOOPTBATPAR(bn));
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
