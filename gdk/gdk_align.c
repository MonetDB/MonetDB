/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

	bn = BATcreatedesc(seq, b->ttype, false, TRANSIENT);
	if (bn == NULL)
		return NULL;

	tp = VIEWtparent(b);
	if ((tp == 0 && b->ttype != TYPE_void) || b->theap.copied)
		tp = b->batCacheid;
	assert(b->ttype != TYPE_void || !tp);
	/* the T column descriptor is fully copied. We need copies
	 * because in case of a mark, we are going to override a
	 * column with a void. Take care to zero the accelerator data,
	 * though. */
	bn->batInserted = b->batInserted;
	bn->batCount = b->batCount;
	bn->batCapacity = b->batCapacity;
	bn->T = b->T;

	if (tp)
		BBPshare(tp);
	if (bn->tvheap) {
		assert(b->tvheap);
		assert(bn->tvheap->parentid > 0);
		BBPshare(bn->tvheap->parentid);
	}

	/* note: theap points into bn which was just overwritten
	 * with a copy from the parent.  Clear the copied flag since
	 * our heap was not copied from our parent(s) even if our
	 * parent's heap was copied from its parent. */
	bn->theap.copied = false;
	bn->tprops = NULL;

	/* correct values after copy of head and tail info */
	if (tp)
		bn->theap.parentid = tp;
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
			BBPunfix(bn->tvheap->parentid);
		}
		MT_lock_destroy(&bn->batIdxLock);
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
	int tt;
	BUN cnt;
	Heap tail;
	BUN p, q;
	oid t, *x;

	BATcheck(b, GDK_FAIL);
	assert(!isVIEW(b));
	tt = b->ttype;
	cnt = BATcapacity(b);
	tail = b->theap;
	p = 0;
	q = BUNlast(b);
	assert(cnt >= q - p);
	TRC_DEBUG(ALGO, "BATmaterialize(" ALGOBATFMT ")\n", ALGOBATPAR(b));

	if (tt != TYPE_void) {
		/* no voids */
		return GDK_SUCCEED;
	}
	tt = TYPE_oid;

	/* cleanup possible ACC's */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);

	strconcat_len(b->theap.filename, sizeof(b->theap.filename),
		      BBP_physical(b->batCacheid), ".tail", NULL);
	if (HEAPalloc(&b->theap, cnt, sizeof(oid)) != GDK_SUCCEED) {
		b->theap = tail;
		return GDK_FAIL;
	}

	/* point of no return */
	b->ttype = tt;
	BATsetdims(b);
	b->batDirtydesc = true;
	b->theap.dirty = true;

	/* So now generate [t..t+cnt-1] */
	t = b->tseqbase;
	x = (oid *) b->theap.base;
	if (is_oid_nil(t)) {
		while (p < q)
			x[p++] = oid_nil;
	} else if (b->tvheap) {
		assert(b->batRole == TRANSIENT);
		assert(b->tvheap->free % SIZEOF_OID == 0);
		BUN nexc = (BUN) (b->tvheap->free / SIZEOF_OID);
		const oid *exc = (const oid *) b->tvheap->base;
		BUN i = 0;
		while (p < q) {
			while (i < nexc && t == exc[i]) {
				i++;
				t++;
			}
			x[p++] = t++;
		}
		b->tseqbase = oid_nil;
		HEAPfree(b->tvheap, true);
		b->tvheap = NULL;
	} else {
		while (p < q)
			x[p++] = t++;
	}
	BATsetcount(b, b->batCount);

	/* cleanup the old heaps */
	HEAPfree(&tail, false);
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

		if (tpb == NULL && vtpb == NULL)
			return;

		/* unlink heaps shared with parent */
		assert(b->tvheap == NULL || b->tvheap->parentid > 0);
		if (b->tvheap && b->tvheap->parentid != b->batCacheid)
			b->tvheap = NULL;

		/* unlink properties shared with parent */
		if (tpb && b->tprops && b->tprops == tpb->tprops)
			b->tprops = NULL;

		/* unlink imprints shared with parent */
		if (tpb && b->timprints && b->timprints == tpb->timprints)
			b->timprints = NULL;
	}
}

/*
 * Materialize a view into a normal BAT. If it is a slice, we really
 * want to reduce storage of the new BAT.
 */
gdk_return
VIEWreset(BAT *b)
{
	if (b == NULL)
		return GDK_FAIL;

	bat tp = VIEWtparent(b);
	bat tvp = VIEWvtparent(b);
	if (tp == 0 && tvp == 0)
		return GDK_SUCCEED;

	if (tp == 0) {
		/* only sharing the vheap */
		assert(ATOMstorage(b->ttype) == TYPE_str);
		return unshare_string_heap(b);
	}

	BAT *v = VIEWcreate(b->hseqbase, b);
	if (v == NULL)
		return GDK_FAIL;

	assert(VIEWtparent(v) != b->batCacheid);
	assert(VIEWvtparent(v) != b->batCacheid);

	const char *nme = BBP_physical(b->batCacheid);
	BUN cnt = BATcount(b);
	Heap tail = (Heap) {
		.farmid = BBPselectfarm(b->batRole, b->ttype, offheap),
	};
	strconcat_len(tail.filename, sizeof(tail.filename), nme, ".tail", NULL);
	if (b->ttype && HEAPalloc(&tail, cnt, Tsize(b)) != GDK_SUCCEED) {
		BBPunfix(v->batCacheid);
		return GDK_FAIL;
	}
	Heap *th = NULL;
	if (b->tvheap) {
		th = GDKmalloc(sizeof(Heap));
		if (th == NULL) {
			HEAPfree(&tail, true);
			BBPunfix(v->batCacheid);
			return GDK_FAIL;
		}
		*th = (Heap) {
			.farmid = BBPselectfarm(b->batRole, b->ttype, varheap),
			.parentid = b->batCacheid,
		};
		strconcat_len(th->filename, sizeof(th->filename),
			      nme, ".theap", NULL);
		if (ATOMheap(b->ttype, th, cnt) != GDK_SUCCEED) {
			HEAPfree(&tail, true);
			GDKfree(th);
			BBPunfix(v->batCacheid);
			return GDK_FAIL;
		}
	}

	BAT bak = *b;		/* backup copy */

	/* start overwriting */
	VIEWunlink(b);
	b->tvheap = th;
	b->theap = tail;
	/* we empty out b completely and then fill it from v */
	DELTAinit(b);
	b->batCapacity = cnt;
	b->batCopiedtodisk = false;
	b->batDirtydesc = true;
	b->batRestricted = BAT_WRITE;
	b->tkey = true;
	b->tsorted = b->trevsorted = true;
	b->tnosorted = b->tnorevsorted = 0;
	b->tnokey[0] = b->tnokey[1] = 0;
	b->tnil = false;
	b->tnonil = true;
	if (BATappend2(b, v, NULL, false, false) != GDK_SUCCEED) {
		/* clean up the mess */
		if (th) {
			HEAPfree(th, true);
			GDKfree(th);
		}
		HEAPfree(&tail, true);
		*b = bak;
		BBPunfix(v->batCacheid);
		return GDK_FAIL;
	}
	/* point of no return */
	if (tp) {
		BBPunshare(tp);
		BBPunfix(tp);
	}
	if (tvp) {
		BBPunshare(tvp);
		BBPunfix(tvp);
	}
	BBPunfix(v->batCacheid);
	return GDK_SUCCEED;
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
	BATiter bi = bat_iterator(b);

	if (b == NULL || view == NULL)
		return;
	if (h > BATcount(b))
		h = BATcount(b);
	if (h < l)
		h = l;
	cnt = h - l;
	view->batInserted = 0;
	view->theap.base = view->ttype ? BUNtloc(bi, l) : NULL;
	view->theap.size = tailsize(view, cnt);
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

	if (b->ttype && !b->theap.parentid) {
		HEAPfree(&b->theap, false);
	} else {
		b->theap.base = NULL;
	}
	b->tvheap = NULL;
	BATfree(b);
}
