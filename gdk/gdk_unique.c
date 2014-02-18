/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_calc_private.h"

#define VALUE(x)	(vars ? vars + VarHeapVal(vals, (x), width) : vals + (x) * width)
/* BATsubunique returns a bat that indicates the unique tail values of
 * the input bat.  This is essentially the same output as the
 * "extents" output of BATgroup.  The difference is that BATsubunique
 * can optionally take a candidate list, something that doesn't make
 * sense for BATgroup, and does not return the grouping bat.
 *
 * The inputs must be dense-headed, the first input is the bat from
 * which unique rows are selected, the second input is a list of
 * candidates.
 */
BAT *
BATsubunique(BAT *b, BAT *s)
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
	char *ext = NULL;
	Heap *hp = NULL;
	Hash *hs = NULL;
	BUN hb;
	BATiter bi;
	int (*cmp)(const void *, const void *);

	if (b->tkey || BATcount(b) <= 1 || BATtdense(b)) {
		/* trivial: already unique */
		if (s) {
			/* we can return a slice of the candidate list */
			oid lo = b->hseqbase;
			oid hi = lo + BATcount(b);
			return BATsubselect(s, NULL, &lo, &hi, 1, 0, 0);
		}
		/* we can return all values */
		bn = BATnew(TYPE_void, TYPE_void, BATcount(b));
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, BATcount(b));
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), b->hseqbase);
		return bn;
	}

	CANDINIT(b, s, start, end, cnt, cand, candend);

	if (start == end) {
		/* trivial: empty result */
		bn = BATnew(TYPE_void, TYPE_void, 0);
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, 0);
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), b->hseqbase);
		return bn;
	}

	if ((b->tsorted && b->trevsorted) ||
	    (b->ttype == TYPE_void && b->tseqbase == oid_nil)) {
		/* trivial: all values are the same */
		bn = BATnew(TYPE_void, TYPE_void, 1);
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, 1);
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), cand ? *cand : b->hseqbase);
		return bn;
	}

	assert(b->ttype != TYPE_void);

	bn = BATnew(TYPE_void, TYPE_oid, 1024);
	if (bn == NULL)
		return NULL;
	BATseqbase(bn, 0);
	vals = Tloc(b, BUNfirst(b));
	if (b->tvarsized && b->ttype)
		vars = b->T->vheap->base;
	else
		vars = NULL;
	width = Tsize(b);
	cmp = BATatoms[b->ttype].atomCmp;
	bi = bat_iterator(b);

	if (b->tsorted || b->trevsorted) {
		const void *prev = NULL;

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
				bunfastins(bn, NULL, &o);
			}
			prev = v;
		}
	} else if (ATOMstorage(b->ttype) == TYPE_bte) {
		unsigned char val;

		assert(vars == NULL);
		seen = GDKzalloc(256 / 16);
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
			if (!(seen[val >> 4] & (1 << (val & 0xF)))) {
				seen[val >> 4] |= 1 << (val & 0xF);
				o = i + b->hseqbase;
				bunfastins(bn, NULL, &o);
			}
		}
		GDKfree(seen);
		seen = NULL;
	} else if (ATOMstorage(b->ttype) == TYPE_sht) {
		unsigned short val;

		assert(vars == NULL);
		seen = GDKzalloc(65536 / 16);
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
			if (!(seen[val >> 4] & (1 << (val & 0xF)))) {
				seen[val >> 4] |= 1 << (val & 0xF);
				o = i + b->hseqbase;
				bunfastins(bn, NULL, &o);
			}
		}
		GDKfree(seen);
		seen = NULL;
	} else if (b->T->hash) {
		/* use existing hash table */
		hs = b->T->hash;
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
			for (hb = HASHget(hs, HASHprobe(hs, v));
			     hb != HASHnil(hs);
			     hb = HASHgetlink(hs, hb)) {
				if (hb < i + BUNfirst(b) &&
				    cmp(v, BUNtail(bi, hb)) == 0) {
					o = hb - BUNfirst(b) + b->hseqbase;
					if (cand == NULL ||
					    SORTfnd(s, &o) != BUN_NONE) {
						/* we've seen this
						 * value before */
						break;
					}
				}
			}
			if (hb == HASHnil(hs)) {
				o = i + b->hseqbase;
				bunfastins(bn, NULL, &o);
			}
		}
	} else {
		size_t nmelen;
		BUN prb;
		BUN p;

		nme = BBP_physical(b->batCacheid);
		nmelen = strlen(nme);
		if ((hp = GDKzalloc(sizeof(Heap))) == NULL ||
		    (hp->filename = GDKmalloc(nmelen + 30)) == NULL ||
		    snprintf(hp->filename, nmelen + 30,
			     "%s.hash" SZFMT, nme, MT_getpid()) < 0 ||
		    (ext = GDKstrdup(hp->filename + nmelen + 1)) == NULL ||
		    (hs = HASHnew(hp, b->ttype, BUNlast(b),
				  HASHmask(b->batCount))) == NULL) {
			if (hp) {
				if (hp->filename)
					GDKfree(hp->filename);
				GDKfree(hp);
			}
			if (ext)
				GDKfree(ext);
			hp = NULL;
			ext = NULL;
			GDKerror("BATgroup: cannot allocate hash table\n");
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
				if (cmp(v, BUNtail(bi, hb)) == 0)
					break;
			}
			if (hb == HASHnil(hs)) {
				o = i + b->hseqbase;
				p = i + BUNfirst(b);
				bunfastins(bn, NULL, &o);
				/* enter into hash table */
				HASHputlink(hs, p, HASHget(hs, prb));
				HASHput(hs, prb, p);
			}
		}
		if (hp->storage == STORE_MEM)
			HEAPfree(hp);
		else
			HEAPdelete(hp, nme, ext);
		GDKfree(hp);
		GDKfree(hs);
		GDKfree(ext);
	}

	bn->tsorted = 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = 1;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	return bn;

  bunins_failed:
	if (seen)
		GDKfree(seen);
	if (hp) {
		if (hp->storage == STORE_MEM)
			HEAPfree(hp);
		else
			HEAPdelete(hp, nme, ext);
		GDKfree(hp);
		GDKfree(hs);
		GDKfree(ext);
	}
	BBPreclaim(bn);
	return NULL;
}
