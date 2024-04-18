/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * @a Peter Boncz, Niels Nes
 * @* BAT Alignment
 * For BATs that result from a n-ary relational scheme it may help to
 * align the BATs on their head value. In particular, it permits
 * replacing a hash-join by a merge-join, which is significantly
 * faster on large tables. Especially if the BATs involved cause page
 * activity or when you can not afford the large hash structures to
 * speed-up processing.
 *
 * For orthogonality, we support alignment between arbitrary columns
 * (head or tail).
 *
 * All standard GDK set-calls update the alignment info in their
 * respective ways. For example, the routine @emph{BUNclustercopy}
 * shuffles the first argument, such that the BUNs are in the same
 * order as those in the second argument.  This operation will mark
 * both columns of the first @emph{BAT} as synced with the second
 * (likewise, @emph{Colcopy()}, which makes a copy, instead of
 * in-place shuffling, has the same alignment effect.
 *
 * Each alignment sequence is given a unique identifier, so as to
 * easily detect this situation. It is retained in the @emph{BAT
 * descriptor}.
 * @+ Alignment Design Considerations
 * Alignment primitives require the right hooks to be inserted in
 * several places in the GDK, apart form this file:
 * @itemize
 * @item @emph{ BUN update operations}.
 * The updated BATs have to be marked as un-aligned.
 * @item @emph{ set operations}.
 * For most relational operations some statements can be made about
 * the size and order of the BATs they produce. This information can
 * be formalized by indicating alignment information automatically.
 * @item @emph{ transaction operations}.
 * Alignment statuses must be kept consistent under database commits
 * and aborts.
 * @end itemize
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* Return TRUE if the two BATs are aligned (same size, same
 * hseqbase). */
int
ALIGNsynced(BAT *b1, BAT *b2)
{
	if (b1 == NULL || b2 == NULL)
		return 0;

	assert(!is_oid_nil(b1->hseqbase));
	assert(!is_oid_nil(b2->hseqbase));

	return BATcount(b1) == BATcount(b2) && b1->hseqbase == b2->hseqbase;
}

/*
 * @+ View BATS
 * The general routine for getting a 'view' BAT upon another BAT is
 * @emph{VIEWcreate}. On this @emph{#read-only} BAT (there is kernel
 * support for this), you can then make vertical slices.
 *
 * It is possible to create a view on a writable BAT. Updates in the
 * parent are then automatically reflected in the VIEW.  Note that the
 * VIEW bat itself can never be modified.
 *
 * Horizontal views should only be given out on a view BAT, but only
 * if it is dead sure the parent BAT is read-only.  This because they
 * cannot physically share the batBuns heap with the parent, as they
 * need a modified version.
 */
static void
VIEWboundsbi(BATiter *bi, BAT *view, BUN l, BUN h)
{
	BUN cnt;
	BUN baseoff;

	if (bi == NULL || view == NULL)
		return;
	if (h > bi->count)
		h = bi->count;
	baseoff = bi->baseoff;
	if (h < l)
		h = l;
	cnt = h - l;
	view->batInserted = 0;
	if (view->ttype != TYPE_void) {
		view->tbaseoff = baseoff + l;
	}
	BATsetcount(view, cnt);
	BATsetcapacity(view, cnt);
	if (view->tnosorted > l && view->tnosorted < l + cnt)
		view->tnosorted -= l;
	else
		view->tnosorted = 0;
	if (view->tnorevsorted > l && view->tnorevsorted < l + cnt)
		view->tnorevsorted -= l;
	else
		view->tnorevsorted = 0;
	if (view->tnokey[0] >= l && view->tnokey[0] < l + cnt &&
	    view->tnokey[1] >= l && view->tnokey[1] < l + cnt &&
	    view->tnokey[0] != view->tnokey[1]) {
		view->tnokey[0] -= l;
		view->tnokey[1] -= l;
	} else {
		view->tnokey[0] = view->tnokey[1] = 0;
	}
	if (view->tminpos >= l && view->tminpos < l + cnt)
		view->tminpos -= l;
	else
		view->tminpos = BUN_NONE;
	if (view->tmaxpos >= l && view->tmaxpos < l + cnt)
		view->tmaxpos -= l;
	else
		view->tmaxpos = BUN_NONE;
	view->tkey |= cnt <= 1;
}

void
VIEWbounds(BAT *b, BAT *view, BUN l, BUN h)
{
	BATiter bi = bat_iterator(b);
	VIEWboundsbi(&bi, view, l, h);
	bat_iterator_end(&bi);
}

BAT *
VIEWcreate(oid seq, BAT *b, BUN l, BUN h)
{
	BAT *bn;
	bat tp = 0;

	BATcheck(b, NULL);

	if (b->ttype == TYPE_void) {
		/* we don't do views on void bats */
		if (h > b->batCount)
			h = b->batCount;
		if (l > h)
			l = h = 0;
		return BATdense(seq, b->tseqbase + l, h - l);
	}

	bn = BATcreatedesc(seq, b->ttype, false, TRANSIENT, 0);
	if (bn == NULL)
		return NULL;
	assert(bn->theap == NULL);

	MT_lock_set(&b->theaplock);
	BATiter bi = bat_iterator_nolock(b);
	bn->batInserted = 0;
	bn->batCount = bi.count;
	bn->batCapacity = b->batCapacity;
	bn->batRestricted = BAT_READ;

	/* the T column descriptor is fully copied except for the
	 * accelerator data. We need copies because in case of a mark,
	 * we are going to override a column with a void. */
	bn->tkey = bi.key;
	bn->tseqbase = bi.tseq;
	bn->tsorted = bi.sorted;
	bn->trevsorted = bi.revsorted;
	bn->twidth = bi.width;
	bn->tshift = bi.shift;
	bn->tnonil = bi.nonil;
	bn->tnil = bi.nil;
	bn->tnokey[0] = bi.nokey[0];
	bn->tnokey[1] = bi.nokey[1];
	bn->tnosorted = bi.nosorted;
	bn->tnorevsorted = bi.norevsorted;
	bn->tminpos = bi.minpos;
	bn->tmaxpos = bi.maxpos;
	bn->tunique_est = bi.unique_est;
	bn->theap = bi.h;
	bn->tbaseoff = bi.baseoff;
	bn->tvheap = bi.vh;

	tp = VIEWtparent(b);
	if (tp == 0 && b->ttype != TYPE_void)
		tp = b->batCacheid;
	assert(b->ttype != TYPE_void || !tp);
	HEAPincref(bi.h);
	if (bi.vh)
		HEAPincref(bi.vh);
	if (l != 0 || h < bi.count)
		VIEWboundsbi(&bi, bn, l, h);
	MT_lock_unset(&b->theaplock);

	if (BBPcacheit(bn, true) != GDK_SUCCEED) {	/* enter in BBP */
		if (bn->tvheap)
			HEAPdecref(bn->tvheap, false);
		HEAPdecref(bn->theap, false);
		MT_lock_destroy(&bn->theaplock);
		MT_lock_destroy(&bn->batIdxLock);
		MT_rwlock_destroy(&bn->thashlock);
		GDKfree(bn);
		return NULL;
	}
	BBPretain(bn->theap->parentid);
	if (bn->tvheap)
		BBPretain(bn->tvheap->parentid);
	TRC_DEBUG(ALGO, ALGOBATFMT " " BUNFMT "," BUNFMT " -> " ALGOBATFMT "\n",
		  ALGOBATPAR(b), l, h, ALGOBATPAR(bn));
	return bn;
}

/*
 * The BATmaterialize routine produces in-place materialized version
 * of a void bat (which should not be a VIEW) (later we should add the
 * code for VIEWs).
 */

gdk_return
BATmaterialize(BAT *b, BUN cap)
{
	Heap *tail;
	Heap *h, *vh = NULL;
	BUN p, q;
	oid t, *x;

	BATcheck(b, GDK_FAIL);
	assert(!isVIEW(b));
	if (cap == BUN_NONE || cap < BATcapacity(b))
		cap = BATcapacity(b);
	if (b->ttype != TYPE_void) {
		/* no voids; just call BATextend to make sure of capacity */
		return BATextend(b, cap);
	}

	if ((tail = GDKmalloc(sizeof(Heap))) == NULL)
		return GDK_FAIL;
	p = 0;
	q = BATcount(b);
	assert(cap >= q - p);
	TRC_DEBUG(ALGO, "BATmaterialize(" ALGOBATFMT ")\n", ALGOBATPAR(b));

	/* cleanup possible ACC's */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);

	*tail = (Heap) {
		.farmid = BBPselectfarm(b->batRole, TYPE_oid, offheap),
		.parentid = b->batCacheid,
		.dirty = true,
	};
	settailname(tail, BBP_physical(b->batCacheid), TYPE_oid, 0);
	if (HEAPalloc(tail, cap, sizeof(oid)) != GDK_SUCCEED) {
		GDKfree(tail);
		return GDK_FAIL;
	}
	x = (oid *) tail->base;
	t = b->tseqbase;
	if (is_oid_nil(t)) {
		for (p = 0; p < q; p++)
			x[p] = oid_nil;
	} else {
		for (p = 0; p < q; p++)
			x[p] = t++;
	}
	ATOMIC_INIT(&tail->refs, 1);
	/* point of no return */
	MT_lock_set(&b->theaplock);
	assert((ATOMIC_GET(&b->theap->refs) & HEAPREFS) > 0);
	/* can only look at tvheap when lock is held */
	if (complex_cand(b)) {
		assert(b->batRole == TRANSIENT);
		if (negoid_cand(b)) {
			assert(ccand_free(b) % SIZEOF_OID == 0);
			BUN nexc = (BUN) (ccand_free(b) / SIZEOF_OID);
			const oid *exc = (const oid *) ccand_first(b);
			BUN i;
			for (p = 0, i = 0; p < q; p++) {
				while (i < nexc && t == exc[i]) {
					i++;
					t++;
				}
				x[p] = t++;
			}
		} else {
			assert(mask_cand(b));
			BUN nmsk = (BUN) (ccand_free(b) / sizeof(uint32_t));
			const uint32_t *src = (const uint32_t *) ccand_first(b);
			BUN n = 0;
			t -= (oid) CCAND(b)->firstbit;
			for (p = 0; p < nmsk; p++) {
				uint32_t val = src[p];
				if (val == 0)
					continue;
				for (uint32_t i = 0; i < 32; i++) {
					if (val & (1U << i)) {
						assert(n < q);
						x[n++] = t + p * 32 + i;
					}
				}
			}
			assert(n == q);
		}
		vh = b->tvheap;
		b->tvheap = NULL;
	}
	h = b->theap;
	b->theap = tail;
	b->tbaseoff = 0;
	b->theap->dirty = true;
	b->tunique_est = is_oid_nil(t) ? 1.0 : (double) b->batCount;
	b->ttype = TYPE_oid;
	BATsetdims(b, 0);
	BATsetcount(b, b->batCount);
	BATsetcapacity(b, cap);
	MT_lock_unset(&b->theaplock);
	if (h->parentid != b->batCacheid)
		BBPrelease(h->parentid);
	HEAPdecref(h, false);
	if (vh) {
		if (vh->parentid != b->batCacheid)
			BBPrelease(vh->parentid);
		HEAPdecref(vh, true);
	}

	return GDK_SUCCEED;
}

/*
 * Destroy a view.
 */
void
VIEWdestroy(BAT *b)
{
	assert(isVIEW(b));
	bat tp = 0, tvp = 0;

	/* remove any leftover private hash structures */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);
	STRMPdestroy(b);
	RTREEdestroy(b);

	MT_lock_set(&b->theaplock);
	PROPdestroy_nolock(b);
	/* heaps that are left after VIEWunlink are ours, so need to be
	 * destroyed (and files deleted) */
	if (b->theap) {
		tp = b->theap->parentid;
		HEAPdecref(b->theap, tp == b->batCacheid);
		b->theap = NULL;
	}
	if (b->tvheap) {
		/* should never happen: if this heap exists, then it was
		 * our own (not a view), and then it doesn't make sense
		 * that the offset heap was a view (at least one of them
		 * had to be) */
		tvp = b->tvheap->parentid;
		HEAPdecref(b->tvheap, tvp == b->batCacheid);
		b->tvheap = NULL;
	}
	MT_lock_unset(&b->theaplock);
	if (tp != 0 && tp != b->batCacheid)
		BBPrelease(tp);
	if (tvp != 0 && tvp != b->batCacheid)
		BBPrelease(tvp);
	BATfree(b);
}
