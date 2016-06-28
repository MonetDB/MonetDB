/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
 *
 * As for performance, the most important observation to make is that
 * operations that do not need alignment, will suffer most from
 * overheads introduced in the BUN update mechanism. For this reason,
 * the alignment-delete operation has to be very cheap. It is captured
 * by the @emph{ALIGNdel} macro, and just zaps one character field in
 * the @emph{BAT} record.
 *
 * @+ Alignment Implementation
 * The @emph{BAT} record is equipped with an @emph{batAlign} field
 * that keeps both information about the head and tail column. The
 * leftmost 4 bits are for the head, the rightmost 4 for the
 * tail. This has been done to make the zap ultra-cheap.
 *
 * Both head and tail column contain an OID in the @emph{halign} and
 * @emph{talign} fields respectively to mark their alignment
 * group. All BATs with the same OID in this field (and the
 * ALIGN_SYNCED bit on) are guaranteed by the system to have equal
 * head columns. As an exception, they might also have TYPE_void head
 * columns (a virtual column).  In such a case, the tail values
 * correspond to the head values that would have been there in a
 * non-virtual column, continuing the same head-sequence as the other
 * BATs in the sync group.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

void
ALIGNcommit(BAT *b)
{
	if (b == NULL)
		return;
	if (!b->halign) {
		b->halign = OIDnew(1);
	}
	if (!b->talign) {
		b->talign = OIDnew(1);
	}
}

void
ALIGNsetH(BAT *b1, BAT *b2)
{
	if (b1 == NULL || b2 == NULL)
		return;

	assert(b1->htype == TYPE_void);
	assert(b2->htype == TYPE_void);

	if (b2->halign == 0) {
		b2->halign = OIDnew(1);
		b2->batDirtydesc = TRUE;
	}
	/* b2 is either dense or has a void(nil) tail */
	BAThseqbase(b1, b2->hseqbase);
	b1->halign = b2->halign;
	b1->batDirtydesc = TRUE;

	assert(b1->hkey & 1);
	assert(b2->hkey & 1);
	assert(b1->hsorted);
	assert(b2->hsorted);
	assert(b1->hrevsorted == (BATcount(b1) <= 1));
	assert(b2->hrevsorted == (BATcount(b2) <= 1));
}

void
ALIGNsetT(BAT *b1, BAT *b2)
{
	ssize_t diff;

	if (b1 == NULL || b2 == NULL)
		return;

	diff = (ssize_t) (BUNfirst(b1) - BUNfirst(b2));
	if (b2->talign == 0) {
		b2->talign = OIDnew(1);
		b2->batDirtydesc = TRUE;
	}
	if (BATtvoid(b2)) {
		/* b2 is either dense or has a void(nil) tail */
		if (b1->ttype != TYPE_void)
			b1->tdense = TRUE;
		else if (b2->tseqbase == oid_nil)
			b1->T->nonil = FALSE;
		BATtseqbase(b1, b2->tseqbase);
	} else if (b1->ttype != TYPE_void) {
		/* b2 is not dense, so set b1 not dense */
		b1->tdense = FALSE;
		BATtseqbase(b1, oid_nil);
		b1->T->nonil = b2->T->nonil;
	} else if (BATtkey(b2))
		BATtseqbase(b1, 0);
	BATkey(b1, BATtkey(b2));
	b1->tsorted = BATtordered(b2);
	b1->trevsorted = BATtrevordered(b2);
	b1->talign = b2->talign;
	b1->batDirtydesc = TRUE;
	b1->T->norevsorted = b2->T->norevsorted ? (BUN) (b2->T->norevsorted + diff) : 0;
	if (b2->T->nokey[0] != b2->T->nokey[1]) {
		b1->T->nokey[0] = (BUN) (b2->T->nokey[0] + diff);
		b1->T->nokey[1] = (BUN) (b2->T->nokey[1] + diff);
	} else {
		b1->T->nokey[0] = b1->T->nokey[1] = 0;
	}
	b1->T->nosorted = b2->T->nosorted ? (BUN) (b2->T->nosorted + diff): 0;
	b1->T->nodense = b2->T->nodense ? (BUN) (b2->T->nodense + diff) : 0;
}

/*
 * The routines @emph{ALIGN_synced} and @emph{ALIGN_ordered} allow to
 * simply query the alignment status of the two head columns of two
 * BATs.
 */
int
ALIGNsynced(BAT *b1, BAT *b2)
{
	BATcheck(b1, "ALIGNsynced: bat 1 required", 0);
	BATcheck(b2, "ALIGNsynced: bat 2 required", 0);

	assert(b1->htype == TYPE_void);
	assert(b2->htype == TYPE_void);
	assert(b1->hseqbase != oid_nil);
	assert(b2->hseqbase != oid_nil);

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
VIEWcreate_(oid seq, BAT *b, int slice_view)
{
	BATstore *bs;
	BAT *bn;
	bat tp = 0;

	BATcheck(b, "VIEWcreate_", NULL);

	assert(b->htype == TYPE_void);

	bs = BATcreatedesc(seq, b->ttype, FALSE, TRANSIENT);
	if (bs == NULL)
		return NULL;
	bn = &bs->B;

	tp = -VIEWtparent(b);
	if ((tp == 0 && b->ttype != TYPE_void) || b->T->heap.copied)
		tp = b->batCacheid;
	assert(b->ttype != TYPE_void || !tp);
	/* the H and T column descriptors are fully copied. We need
	 * copies because in case of a mark, we are going to override
	 * a column with a void. Take care to zero the accelerator
	 * data, though. */
	bn->batDeleted = b->batDeleted;
	bn->batFirst = b->batFirst;
	bn->batInserted = b->batInserted;
	bn->batCount = b->batCount;
	bn->batCapacity = b->batCapacity;
	*bn->T = *b->T;
	if (bn->batFirst > 0) {
		bn->T->heap.base += b->batFirst * b->T->width;
		bn->batFirst = 0;
	}

	if (tp)
		BBPshare(tp);
	if (bn->T->vheap) {
		assert(b->T->vheap);
		assert(bn->T->vheap->parentid > 0);
		BBPshare(bn->T->vheap->parentid);
	}

	/* note: T->heap points into bs which was just overwritten
	 * with a copy from the parent.  Clear the copied flag since
	 * our heap was not copied from our parent(s) even if our
	 * parent's heap was copied from its parent. */
	bn->T->heap.copied = 0;
	bn->T->props = NULL;

	/* correct values after copy of head and tail info */
	if (tp)
		bn->T->heap.parentid = -tp;
	BATinit_idents(bn);
	/* Some bits must be copied individually. */
	bn->batDirty = BATdirty(b);
	bn->batRestricted = BAT_READ;
	/* slices are unequal to their parents; cannot use accs */
	if (slice_view || !tp || isVIEW(b))
		bn->T->hash = NULL;
	else
		bn->T->hash = b->T->hash;
	/* imprints are shared, but the check is dynamic */
	bn->H->imprints = NULL;
	bn->T->imprints = NULL;
	/* Order OID index */
	bn->horderidx = NULL;
	bn->torderidx = NULL;
	BBPcacheit(bs, 1);	/* enter in BBP */
	return bn;
}

BAT *
VIEWcreate(oid seq, BAT *b)
{
	return VIEWcreate_(seq, b, FALSE);
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

	BATcheck(b, "BATmaterialize", GDK_FAIL);
	assert(!isVIEW(b));
	tt = b->ttype;
	cnt = BATcapacity(b);
	tail = b->T->heap;
	p = BUNfirst(b);
	q = BUNlast(b);
	assert(cnt >= q - p);
	ALGODEBUG fprintf(stderr, "#BATmaterialize(%d);\n", (int) b->batCacheid);

	if (!BATtdense(b) || tt != TYPE_void) {
		/* no voids */
		return GDK_SUCCEED;
	}
	tt = TYPE_oid;

	/* cleanup possible ACC's */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);

	b->T->heap.filename = NULL;
	if (HEAPalloc(&b->T->heap, cnt, sizeof(oid)) != GDK_SUCCEED) {
		b->T->heap = tail;
		return GDK_FAIL;
	}

	/* point of no return */
	b->ttype = tt;
	BATsetdims(b);
	b->batDirty = TRUE;
	b->batDirtydesc = TRUE;
	b->T->heap.dirty = TRUE;

	/* set the correct dense info */
	b->tdense = TRUE;

	/* So now generate [t..t+cnt-1] */
	t = b->tseqbase;
	x = (oid *) b->T->heap.base;
	for (; p < q; p++)
		*x++ = t++;
	cnt = t - b->tseqbase;
	BATsetcount(b, cnt);

	/* cleanup the old heaps */
	HEAPfree(&tail, 0);
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
		bat tp = -VIEWtparent(b);
		bat vtp = VIEWvtparent(b);
		BAT *tpb = NULL;
		BAT *vtpb = NULL;

		assert(b->batCacheid > 0);
		assert(b->htype == TYPE_void);
		if (tp)
			tpb = BBP_cache(tp);
		if (tp && !vtp)
			vtp = tp;
		if (vtp)
			vtpb = BBP_cache(vtp);

		if (tpb == NULL && vtpb == NULL)
			return;

		/* unlink heaps shared with parent */
		assert(b->H->vheap == NULL);
		assert(b->H->props == NULL);
		assert(b->H->hash == NULL);
		assert(b->H->imprints == NULL);
		assert(b->T->vheap == NULL || b->T->vheap->parentid > 0);
		if (b->T->vheap && b->T->vheap->parentid != b->batCacheid)
			b->T->vheap = NULL;

		/* unlink properties shared with parent */
		if (tpb && b->T->props && b->T->props == tpb->T->props)
			b->T->props = NULL;

		/* unlink hash accelerators shared with parent */
		if (tpb && b->T->hash && b->T->hash == tpb->T->hash)
			b->T->hash = NULL;

		/* unlink imprints shared with parent */
		if (tpb && b->T->imprints && b->T->imprints == tpb->T->imprints)
			b->T->imprints = NULL;
	}
}

/*
 * Materialize a view into a normal BAT. If it is a slice, we really
 * want to reduce storage of the new BAT.
 */
gdk_return
VIEWreset(BAT *b)
{
	bat tp, tvp;
	Heap head, tail, *th = NULL;
	BAT *v = NULL;

	if (b == NULL)
		return GDK_FAIL;
	assert(b->htype == TYPE_void);
	assert(b->batCacheid > 0);
	tp = -VIEWtparent(b);
	tvp = VIEWvtparent(b);
	if (tp || tvp) {
		BUN cnt;
		str nme;
		size_t nmelen;

		/* alloc heaps */
		memset(&head, 0, sizeof(Heap));
		memset(&tail, 0, sizeof(Heap));

		cnt = BATcount(b) + 1;
		nme = BBP_physical(b->batCacheid);
		nmelen = nme ? strlen(nme) : 0;

		assert(b->batCacheid > 0);
		assert(tp || tvp || !b->ttype);

		head.farmid = BBPselectfarm(b->batRole, TYPE_void, offheap);
		tail.farmid = BBPselectfarm(b->batRole, b->ttype, offheap);
		if (b->ttype) {
			tail.filename = (str) GDKmalloc(nmelen + 12);
			if (tail.filename == NULL)
				goto bailout;
			snprintf(tail.filename, nmelen + 12, "%s.tail", nme);
			if (b->ttype && HEAPalloc(&tail, cnt, Tsize(b)) != GDK_SUCCEED)
				goto bailout;
		}
		if (b->T->vheap) {
			th = GDKzalloc(sizeof(Heap));
			if (th == NULL)
				goto bailout;
			th->farmid = BBPselectfarm(b->batRole, b->ttype, varheap);
			th->filename = (str) GDKmalloc(nmelen + 12);
			if (th->filename == NULL)
				goto bailout;
			snprintf(th->filename, nmelen + 12, "%s.theap", nme);
			if (ATOMheap(b->ttype, th, cnt) != GDK_SUCCEED)
				goto bailout;
		}

		v = VIEWcreate(b->hseqbase, b);
		if (v == NULL)
			goto bailout;

		/* cut the link to your parents */
		VIEWunlink(b);
		if (tp) {
			BBPunshare(tp);
			BBPunfix(tp);
		}
		if (tvp) {
			BBPunshare(tvp);
			BBPunfix(tvp);
		}

		b->htype = TYPE_void;
		b->hvarsized = 1;
		b->H->shift = 0;
		b->H->width = 0;
		b->hseqbase = v->hseqbase;
		b->hkey = BOUND2BTRUE | 1;

		b->ttype = v->ttype;
		b->tvarsized = v->tvarsized;
		b->T->shift = v->T->shift;
		b->T->width = v->T->width;
		b->tseqbase = v->tseqbase;

		b->T->heap.parentid = 0;
		b->batRestricted = BAT_WRITE;

		/* reset BOUND2BTRUE */
		b->tkey = BATtkey(v);

		/* copy the heaps */
		b->H->heap = head;
		b->T->heap = tail;

		/* unshare from parents heap */
		if (th) {
			assert(b->T->vheap == NULL);
			b->T->vheap = th;
			th = NULL;
			b->T->vheap->parentid = b->batCacheid;
		}

		if (-v->T->heap.parentid == b->batCacheid) {
			assert(tp == 0);
			assert(b->batSharecnt > 0);
			BBPunshare(b->batCacheid);
			BBPunfix(b->batCacheid);
			v->T->heap.parentid = 0;
		}
		b->batSharecnt = 0;
		b->batCopiedtodisk = 0;
		b->batDirty = 1;

		/* reset BOUND2KEY */
		b->tkey = BATtkey(v);

		/* make the BAT empty and insert all again */
		DELTAinit(b);
		/* reset capacity */
		b->batCapacity = cnt;

		/* insert all of v in b, and quit */
		BATappend(b, v, FALSE);
		BBPreclaim(v);
	}
	return GDK_SUCCEED;
      bailout:
	BBPreclaim(v);
	HEAPfree(&head, 0);
	HEAPfree(&tail, 0);
	GDKfree(th);
	return GDK_FAIL;
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
	assert(b->htype == TYPE_void);
	assert(view->htype == TYPE_void);
	if (h > BATcount(b))
		h = BATcount(b);
	if (h < l)
		h = l;
	cnt = h - l;
	l += BUNfirst(b);
	view->batFirst = view->batDeleted = view->batInserted = 0;
	view->H->heap.base = NULL;
	view->H->heap.size = 0;
	view->T->heap.base = view->ttype ? BUNtloc(bi, l) : NULL;
	view->T->heap.size = tailsize(view, cnt);
	BATsetcount(view, cnt);
	BATsetcapacity(view, cnt);
	view->H->nosorted = view->H->norevsorted = view->H->nodense = 0;
	view->H->nokey[0] = view->H->nokey[1] = 0;
	if (view->T->nosorted > l && view->T->nosorted < l + cnt)
		view->T->nosorted -= l;
	else
		view->T->nosorted = 0;
	if (view->T->norevsorted > l && view->T->norevsorted < l + cnt)
		view->T->norevsorted -= l;
	else
		view->T->norevsorted = 0;
	if (view->T->nodense > l && view->T->nodense < l + cnt)
		view->T->nodense -= l;
	else
		view->T->nodense = 0;
	if (view->T->nokey[0] >= l && view->T->nokey[0] < l + cnt &&
	    view->T->nokey[1] >= l && view->T->nokey[1] < l + cnt &&
	    view->T->nokey[0] != view->T->nokey[1]) {
		view->T->nokey[0] -= l;
		view->T->nokey[1] -= l;
	} else {
		view->T->nokey[0] = view->T->nokey[1] = 0;
	}
}

/*
 * Destroy a view.
 */
void
VIEWdestroy(BAT *b)
{
	assert(isVIEW(b));
	assert(b->htype == TYPE_void);

	/* remove any leftover private hash structures */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);
	VIEWunlink(b);

	b->H->heap.base = NULL;
	b->H->heap.filename = NULL;

	if (b->ttype && !b->T->heap.parentid) {
		HEAPfree(&b->T->heap, 0);
	} else {
		b->T->heap.base = NULL;
		b->T->heap.filename = NULL;
	}
	b->H->vheap = NULL;
	b->T->vheap = NULL;
	BATfree(b);
}
