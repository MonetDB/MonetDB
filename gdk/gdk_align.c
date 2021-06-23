/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
BAT *
VIEWcreate(oid seq, BAT *b)
{
	BAT *bn;
	bat tp = 0;

	BATcheck(b, NULL);

	if (b->ttype == TYPE_void) {
		/* we don't do views on void bats */
		return BATdense(seq, b->tseqbase, b->batCount);
	}

	bn = BATcreatedesc(seq, b->ttype, false, TRANSIENT);
	if (bn == NULL)
		return NULL;
	assert(bn->theap == NULL);

	/* the T column descriptor is fully copied. We need copies
	 * because in case of a mark, we are going to override a
	 * column with a void. Take care to zero the accelerator data,
	 * though. */
	bn->batInserted = b->batInserted;
	bn->batCount = b->batCount;
	bn->batCapacity = b->batCapacity;
	MT_lock_set(&b->theaplock);
	bn->T = b->T;
	tp = VIEWtparent(b);
	if (tp == 0 && b->ttype != TYPE_void)
		tp = b->batCacheid;
	assert(b->ttype != TYPE_void || !tp);
	/* copy again now we have the correct lock */
	bn->theap = b->theap;
	HEAPincref(b->theap);
	if (b->tvheap)
		HEAPincref(b->tvheap);
	MT_lock_unset(&b->theaplock);

	if (tp)
		BBPshare(tp);
	if (bn->tvheap) {
		assert(bn->tvheap->parentid > 0);
		BBPshare(bn->tvheap->parentid);
	}

	bn->tprops = NULL;

	/* correct values after copy of column info */
	BATinit_idents(bn);
	bn->batRestricted = BAT_READ;
	bn->thash = NULL;
	/* imprints are shared, but the check is dynamic */
	bn->timprints = NULL;
	/* Order OID index */
	bn->torderidx = NULL;
	if (BBPcacheit(bn, true) != GDK_SUCCEED) {	/* enter in BBP */
		if (tp) {
			BBPunshare(tp);
			BBPunfix(tp);
		}
		if (bn->tvheap) {
			BBPunshare(bn->tvheap->parentid);
			HEAPdecref(bn->tvheap, false);
		}
		HEAPdecref(bn->theap, false);
		MT_lock_destroy(&bn->theaplock);
		MT_lock_destroy(&bn->batIdxLock);
		MT_rwlock_destroy(&bn->thashlock);
		GDKfree(bn);
		return NULL;
	}
	TRC_DEBUG(ALGO, ALGOBATFMT " -> " ALGOBATFMT "\n",
		  ALGOBATPAR(b), ALGOBATPAR(bn));
	return bn;
}

/*
 * The BATmaterialize routine produces in-place materialized version
 * of a void bat (which should not be a VIEW) (later we should add the
 * code for VIEWs).
 */

gdk_return
BATmaterialize(BAT *b)
{
	BUN cnt;
	Heap *tail;
	BUN p, q;
	oid t, *x;

	BATcheck(b, GDK_FAIL);
	assert(!isVIEW(b));
	if (b->ttype != TYPE_void) {
		/* no voids */
		return GDK_SUCCEED;
	}

	cnt = BATcapacity(b);
	if ((tail = GDKmalloc(sizeof(Heap))) == NULL)
		return GDK_FAIL;
	*tail = *b->theap;
	p = 0;
	q = BUNlast(b);
	assert(cnt >= q - p);
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
	strconcat_len(tail->filename, sizeof(tail->filename),
		      BBP_physical(b->batCacheid), ".tail", NULL);
	if (HEAPalloc(tail, cnt, sizeof(oid), 0) != GDK_SUCCEED) {
		GDKfree(tail);
		return GDK_FAIL;
	}
	x = (oid *) tail->base;
	t = b->tseqbase;
	if (is_oid_nil(t)) {
		for (BUN p = 0; p < q; p++)
			x[p] = oid_nil;
	} else {
		for (BUN p = 0; p < q; p++)
			x[p] = t++;
	}
	ATOMIC_INIT(&tail->refs, 1);
	/* point of no return */
	MT_lock_set(&b->theaplock);
	assert(ATOMIC_GET(&b->theap->refs) > 0);
	/* can only look at tvheap when lock is held */
	if (complex_cand(b)) {
		assert(b->batRole == TRANSIENT);
		if (negoid_cand(b)) {
			assert(ccand_free(b) % SIZEOF_OID == 0);
			BUN nexc = (BUN) (ccand_free(b) / SIZEOF_OID);
			const oid *exc = (const oid *) ccand_first(b);
			for (BUN p = 0, i = 0; p < q; p++) {
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
			for (BUN p = 0; p < nmsk; p++) {
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
		HEAPdecref(b->tvheap, true);
		b->tvheap = NULL;
	}
	HEAPdecref(b->theap, false);
	b->theap = tail;
	b->tbaseoff = 0;
	b->theap->dirty = true;
	MT_lock_unset(&b->theaplock);
	b->ttype = TYPE_oid;
	BATsetdims(b);
	b->batDirtydesc = true;
	BATsetcount(b, b->batCount);
	BATsetprop(b, GDK_NUNIQUE, TYPE_oid, &(oid){is_oid_nil(t) ? 1 : b->batCount});

	return GDK_SUCCEED;
}

/*
 * The @#VIEWunlink@ routine cuts a reference to the parent. Part of the view
 * destroy sequence.
 */
static void
VIEWunlink(BAT *b)
{
	if (b) {
		MT_lock_set(&b->theaplock);

		bat tp = VIEWtparent(b);
		bat vtp = VIEWvtparent(b);
		BAT *tpb = NULL;
		BAT *vtpb = NULL;

		assert(b->batCacheid > 0);
		if (tp)
			tpb = BBP_cache(tp);
		if (tp && !vtp)
			vtp = tp;
		if (vtp)
			vtpb = BBP_cache(vtp);

		if (tpb == NULL && vtpb == NULL) {
			MT_lock_unset(&b->theaplock);
			return;
		}

		/* unlink heaps shared with parent */
		if (b->theap && b->theap->parentid != b->batCacheid) {
			HEAPdecref(b->theap, false);
			b->theap = NULL;
		}
		assert(b->tvheap == NULL || b->tvheap->parentid > 0);
		if (b->tvheap && b->tvheap->parentid != b->batCacheid) {
			HEAPdecref(b->tvheap, false);
			b->tvheap = NULL;
		}
		MT_lock_unset(&b->theaplock);

		/* unlink properties shared with parent */
		if (tpb && b->tprops && b->tprops == tpb->tprops)
			b->tprops = NULL;

		/* unlink imprints shared with parent */
		if (tpb && b->timprints && b->timprints == tpb->timprints)
			b->timprints = NULL;
	}
}

/*
 * The remainder are utilities to manipulate the BAT view and not to
 * forget some details in the process.  It expects a position range in
 * the underlying BAT and compensates for outliers.
 */
void
VIEWbounds(BAT *b, BAT *view, BUN l, BUN h)
{
	BUN cnt;
	BUN baseoff;

	if (b == NULL || view == NULL)
		return;
	if (h > BATcount(b))
		h = BATcount(b);
	baseoff = b->tbaseoff;
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
}

/*
 * Destroy a view.
 */
void
VIEWdestroy(BAT *b)
{
	assert(isVIEW(b));

	/* remove any leftover private hash structures */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);
	PROPdestroy(b);
	VIEWunlink(b);

	MT_lock_set(&b->theaplock);
	if (b->theap) {
		HEAPdecref(b->theap, false);
		b->theap = NULL;
	}
	if (b->tvheap) {
		HEAPdecref(b->tvheap, false);
		b->tvheap = NULL;
	}
	MT_lock_unset(&b->theaplock);
	BATfree(b);
}
