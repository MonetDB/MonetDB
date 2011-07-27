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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f gdk_align
 * @a Peter Boncz, Niels Nes
 * @* BAT Alignment
 * For BATs that result from a n-ary relational scheme it may help to
 * align the BATs on their head value. In particular, it permits
 * replacing a hash-join by a merge-join, which is significantly faster
 * on large tables. Especially if the BATs involved cause page activity
 * or when you can not afford the large hash structures to speed-up processing.
 * @-
 * For orthogonality, we support alignment between arbitrary columns (head or tail).
 * @-
 * All standard GDK set-calls update the alignment info in their respective
 * ways. For example, the routine @emph{BUNclustercopy} shuffles the first argument,
 * such that the BUNs are in the same order as those in the second argument.
 * This operation will mark both columns of the first @emph{BAT} as synced with
 * the second (likewise, @emph{BATcopy()}, which makes a copy, instead of in-place
 * shuffling, has the same alignment effect, @emph{BATmark()} marks the tail column
 * as synced with the head of the original @emph{BAT}, and for
 * instance @emph{BATsemijoin()} marks both return columns as aligned with its left parameter).
 *
 * Each alignment sequence is given a unique identifier, so as to easily
 * detect this situation. It is retained in the @emph{BAT descriptor}.
 * @+ Alignment Design Considerations
 * Alignment primitives require the right hooks to be inserted in
 * several places in the GDK, apart form this file:
 * @itemize
 * @item @emph{ BUN update operations}.
 * The updated BATs have to be marked as
 * un-aligned.
 * @item @emph{ set operations}.
 * For most relational operations some
 * statements can be made about the size and order of the BATs
 * they produce. This information can be formalized by indicating alignment
 * information automatically.
 * @item @emph{ transaction operations}.
 * Alignment statuses must be
 * kept consistent under database commits and aborts.
 * @end itemize
 *
 * As for performance, the most important observation to make is that
 * operations that do not need alignment, will suffer most from
 * overheads introduced in the BUN update mechanism. For this reason,
 * the alignment-delete operation has to be very cheap. It is
 * captured by the @emph{ALIGNdel} macro, and just zaps one character
 * field in the @emph{BAT} record.
 * @
 * @+ Alignment Implementation
 * The @emph{BAT} record is equipped with an @emph{batAlign} field that keeps
 * both information about the head and tail column. The leftmost
 * 4 bits are for the head, the rightmost 4 for the tail. This
 * has been done to make the zap ultra-cheap.
 *
 * Both head and tail column contain an OID in the @emph{halign} and @emph{talign}
 * fields respectively to mark their alignment group. All BATs with the
 * same OID in this field (and the ALIGN_SYNCED bit on) are guaranteed
 * by the system to have equal head columns. As an exception, they
 * might also have TYPE_void head columns (a virtual column).
 * In such a case, the tail values correspond to the head values
 * that would have been there in a non-virtual column, continuing
 * the same head-sequence as the other BATs in the sync group.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

int
ALIGNcommit(BAT *b)
{
	BATcheck(b, "ALIGNcommit");
	if (!b->halign) {
		b->halign = OIDnew(1);
	}
	if (!b->talign) {
		b->talign = OIDnew(1);
	}
	return 0;
}

int
ALIGNundo(BAT *b)
{
	BATcheck(b, "ALIGNundo");
	return 0;
}

int
ALIGNsetH(BAT *b1, BAT *b2)
{
	ssize_t diff;

	BATcheck(b1, "ALIGNsetH: bat 1 required");
	BATcheck(b2, "ALIGNsetH: bat 2 required");

	diff = (ssize_t) (BUNfirst(b1) - BUNfirst(b2));
	if (b2->halign == 0) {
		b2->halign = OIDnew(1);
		b2->batDirtydesc = TRUE;
	} else {
		/* propagate GDK_AGGR information */
		BATpropagate(b1, b2, GDK_AGGR_SIZE);
		BATpropagate(b1, b2, GDK_AGGR_CARD);
	}
	if (BAThvoid(b2)) {
		/* b2 is either dense or has a void(nil) head */
		if (b1->htype != TYPE_void)
			b1->hdense = TRUE;
		else if (b2->hseqbase == oid_nil)
			b1->H->nonil = FALSE;
		BATseqbase(b1, b2->hseqbase);
	} else if (b1->htype != TYPE_void) {
		/* b2 is not dense, so set b1 not dense */
		b1->hdense = FALSE;
		BATseqbase(b1, oid_nil);
		b1->H->nonil = b2->H->nonil;
	}
	BATkey(b1, BAThkey(b2));
	b1->hsorted = BAThordered(b2);
	b1->halign = b2->halign;
	b1->batDirtydesc = TRUE;
	b1->H->nosorted_rev = (BUN) (b2->H->nosorted_rev + diff);
	b1->H->nokey[0] = (BUN) (b2->H->nokey[0] + diff);
	b1->H->nokey[1] = (BUN) (b2->H->nokey[1] + diff);
	b1->H->nosorted = (BUN) (b2->H->nosorted + diff);
	b1->H->nodense = (BUN) (b2->H->nodense + diff);

	return 0;
}

/*
 * @-
 * The routines @emph{ALIGN_synced} and @emph{ALIGN_ordered}
 * allow to simply query the alignment status of the two head columns
 * of two BATs.
 */
int
ALIGNsynced(BAT *b1, BAT *b2)
{
	BATcheck(b1, "ALIGNsynced: bat 1 required");
	BATcheck(b2, "ALIGNsynced: bat 2 required");

	/* first try to prove head columns are not in sync */
	if (BATcount(b1) != BATcount(b2))
		return 0;
	if (ATOMtype(BAThtype(b1)) != ATOMtype(BAThtype(b2)))
		return 0;
	if (BAThvoid(b1) && BAThvoid(b2))
		return (b1->hseqbase == b2->hseqbase);

	/* then try that they are */
	if (b1->batCacheid == b2->batCacheid)
		return 1;	/* same bat. trivial case */
	if (BATcount(b1) == 0)
		return 1;	/* empty bats of same type. trivial case */
	if (b1->halign && b1->halign == b2->halign)
		return 1;	/* columns marked as equal by algorithmics */
	if (VIEWparentcol(b1) && ALIGNsynced(BBPcache(VIEWhparent(b1)), b2))
		return 1;	/* view on same bat --- left recursive def.. */
	if (VIEWparentcol(b2) && ALIGNsynced(b1, BBPcache(VIEWhparent(b2))))
		return 1;	/* view on same bat --- right recursive def.. */

	return 0;		/* we simply don't know */
}

/*
 * @+ View BATS
 * The general routine for getting a 'view' BAT upon another
 * BAT is @emph{VIEWcreate}. On this @emph{#read-only} BAT (there is kernel
 * support for this), you can then make vertical slices.
 * Use @emph{VIEWhead} for this.
 *
 * It is possible to create a view on a writable BAT. Updates
 * in the parent are then automatically reflected in the VIEW.
 * Note that the VIEW bat itself can never be modified.
 *
 * Horizontal views should only be given out on a view BAT, but
 * only if it is dead sure the parent BAT is read-only.
 * This because they cannot physically share the batBuns heap
 * with the parent, as they need a modified version.
 */
static BAT *
VIEWhcreate(BAT *h)
{
	BATstore *bs, *recycled = NULL;
	BAT *bn;
	bat hp;

	BATcheck(h, "VIEWhcreate");
	recycled = bs = BBPrecycle(TYPE_void, TYPE_void, 1);
	if (bs == NULL)
		bs = BATcreatedesc(h->htype, TYPE_void, FALSE);
	if (bs == NULL)
		return NULL;
	bn = &bs->B;

	BATsetdims(bn);
	hp = VIEWhparent(h);

	if (h->htype == TYPE_void)
		hp = 0;
	if ((hp == 0 && h->htype != TYPE_void) || h->H->heap.copied)
		hp = h->batCacheid;
	if (hp)
		BBPshare(hp);
	*bn->H = *h->H;
	*bn->U = *h->U;
	if (bn->H->vheap) {
		assert(bn->H->vheap->parentid != 0);
		BBPshare(bn->H->vheap->parentid);
	}

	/* correct values after copy of head info */
	bn->H->props = NULL;
	bn->H->heap.copied = 0;
	if (hp)
		bn->H->heap.parentid = hp;
	if (hp && isVIEW(h))
		bn->H->hash = NULL;
	BATinit_idents(bn);
	/*
	 * @-
	 * The b->P structure cannot be shared and must be copied individually.
	 */
	bn->batSet = h->batSet;
	bn->batDirty = BATdirty(h);
	bn->batRestricted = BAT_READ;

	if (recycled == NULL)
		BBPcacheit(bs, 1);		     /* enter in BBP */
	return bn;
}


BAT *
VIEWcreate_(BAT *h, BAT *t, int slice_view)
{
	BATstore *bs, *recycled = NULL;
	BAT *bn;
	bat hp = 0, tp = 0, vc = 0;

	BATcheck(h, "VIEWcreate_");
	BATcheck(t, "VIEWcreate_");
	recycled = bs = BBPrecycle(TYPE_void, TYPE_void, 1);
	if (bs == NULL)
		bs = BATcreatedesc(h->htype, t->ttype, FALSE);
	if (bs == NULL)
		return NULL;
	bn = &bs->B;

	hp = VIEWhparent(h);
	tp = VIEWtparent(t);
	if ((hp == 0 && h->htype != TYPE_void) || h->H->heap.copied)
		hp = h->batCacheid;
	if ((tp == 0 && t->ttype != TYPE_void) || t->T->heap.copied)
		tp = -t->batCacheid;
	assert(h->htype != TYPE_void || !hp);
	assert(t->ttype != TYPE_void || !tp);
	/*
	 * @-
	 * the H and T column descriptors are fully copied. We need copies
	 * because in case of a mark, we are going to override a column with
	 * a void. Take care to zero the accelerator data, though.
	 */
	*bn->U = *h->U;
	*bn->H = *h->H;
	/* we need aligned bats */
	assert(h->U->first == t->U->first);
	if (h->H == t->T) {
		vc = 1;
		tp = hp;
		bn->T = bn->H;
	} else {
		*bn->T = *t->T;
		if (bn->U->capacity > t->U->capacity)
			bn->U->capacity = t->U->capacity;
	}

	if (hp)
		BBPshare(hp);
	if (tp)
		BBPshare(tp);
	if (bn->H->vheap) {
		assert(bn->H->vheap->parentid > 0);
		BBPshare(bn->H->vheap->parentid);
	}
	if (bn->T->vheap) {
		assert(bn->T->vheap->parentid > 0);
		BBPshare(bn->T->vheap->parentid);
	}

	/* note: H/T->heap's points into bs which was just overwritten
	   with a copy from the parent(s). Clear the copied flag since our
	   heap was not copied from our parent(s) even if our parent's heap
	   was copied from its parent(s). */
	bn->H->heap.copied = bn->T->heap.copied = 0;
	bn->H->props = bn->T->props = NULL;

	/* correct values after copy of head and tail info */
	if (hp)
		bn->H->heap.parentid = hp;
	if (tp)
		bn->T->heap.parentid = tp;
	BATinit_idents(bn);
	/*
	 * @-
	 * The b->P structure cannot be shared and must be copied individually.
	 */
	bn->batSet = h->batSet;
	bn->batDirty = BATdirty(h);
	bn->batRestricted = BAT_READ;
	/*
	 * @-
	 * The U record may be shared with the parent; in that case, the
	 * search accelerators of the parent can be used. If, however, we
	 * want to take a horizontal fragment (stable=false), this cannot
	 * be done, and we need to put different information in U (so we can't
	 * use a copy.
	 */
	if (slice_view || !hp || isVIEW(h))
		/* slices are unequal to their parents; cannot use accs */
		bn->H->hash = NULL;
	else
		/* equal pointers to parent mean view uses acc of parent */
		bn->H->hash = h->H->hash;
	if (slice_view || !tp || isVIEW(t))
		bn->T->hash = NULL;
	else
		bn->T->hash = t->T->hash;
	if (recycled == NULL)
		BBPcacheit(bs, 1);	/* enter in BBP */
	/* View of VIEW combine, ie we need to fix the head of the mirror */
	if (vc) {
		BAT *bm = BATmirror(bn);
		bm->H = bn->H;
	}
	return bn;
}

BAT *
VIEWcreate(BAT *h, BAT *t)
{
	return VIEWcreate_(h, t, FALSE);
}

/*
 * @-
 * The @#VIEWhead@ routine effortlessly projects out the tail column.
 */
BAT *
VIEWhead(BAT *b)
{
	BAT *bn = VIEWhcreate(b), *bm;
	BATstore *bs;

	if (bn == NULL)
		return NULL;
	bs = BBP_desc(bn->batCacheid);
	bm = BATmirror(bn);
	if (bm == NULL)
		return NULL;
	bm->H = bn->T = &bs->T;
	bn->T->type = TYPE_void;
	bn->T->varsized = 1;
	bn->T->shift = 0;
	bn->T->width = 0;
	bn->T->heap.parentid = 0;
	bn->T->hash = NULL;
	bn->T->heap.maxsize = bn->T->heap.size = bn->T->heap.free = 0;
	bn->T->heap.base = NULL;
	BATseqbase(bm, oid_nil);
	return bn;
}

BAT *
VIEWhead_(BAT *b, int mode)
{
	BAT *bn = VIEWhead(b);

	if (bn)
		bn->batRestricted = mode;
	return bn;
}

/*
 * @-
 * the @#VIEWcombine@ routine effortlessly produces a view with double
 * vision on the head column.
 */
BAT *
VIEWcombine(BAT *b)
{
	BAT *bn = VIEWhcreate(b), *bm;

	if (bn == NULL)
		return NULL;
	bm = BATmirror(bn);
	if (bm == NULL)
		return NULL;
	if (bn->htype != TYPE_void) {
		assert(bn->T->vheap == NULL);
		bn->T = bn->H;
		bm->H = bn->H;
		if (bn->T->heap.parentid)
			BBPshare(bn->T->heap.parentid);
		if (bn->T->vheap) {
			assert(bn->T->vheap->parentid != ABS(bn->batCacheid));
			assert(bn->T->vheap->parentid > 0);
			BBPshare(bn->T->vheap->parentid);
		}
		ALIGNsetH(bn, b);
	} else {
		BATseqbase(bm, bn->hseqbase);
	}
	return bn;
}

/*
 * @-
 * The @#BATmaterialize@ routine produces in-place materialized version of
 * a void bat (which should not be a VIEW) (later we should add the code
 * for VIEWs).
 */

BAT *
BATmaterializeh(BAT *b)
{
	int ht;
	BUN cnt;
	Heap head;
	BUN p, q;
	oid h, *x;
	chr tshift;

	BATcheck(b, "BATmaterialize");
	assert(!isVIEW(b));
	ht = b->htype;
	cnt = BATcapacity(b);
	head = b->H->heap;
	p = BUNfirst(b);
	q = BUNlast(b);
	assert(cnt >= q - p);
	ALGODEBUG THRprintf(GDKout, "#BATmaterialize(%d);\n", (int) b->batCacheid);

	if (!BAThdense(b) || ht != TYPE_void) {
		/* no voids */
		return b;
	}
	ht = TYPE_oid;

	/* cleanup possible ACC's */
	HASHdestroy(b);

	b->H->heap.filename = NULL;
	if (HEAPalloc(&b->H->heap, cnt, sizeof(oid)) < 0) {
		b->H->heap = head;
		return NULL;
	}

	/* point of no return */
	b->htype = ht;
	tshift = b->T->shift;
	BATsetdims(b);
	if (b->ttype) {
		b->T->shift = tshift;	/* restore in case it got changed */
		b->T->width = 1 << tshift;
	}
	b->batDirty = TRUE;
	b->batDirtydesc = TRUE;
	b->H->heap.dirty = TRUE;

	/* set the correct dense info */
	b->hdense = TRUE;

	/* So now generate [h..h+cnt-1] */
	h = b->hseqbase;
	x = (oid *) b->H->heap.base;
	for (; p < q; p++)
		*x++ = h++;
	cnt = h - b->hseqbase;
	BATsetcount(b, cnt);

	/* cleanup the old heaps */
	HEAPfree(&head);
	return b;
}

/* only materialize the tail */
BAT *
BATmaterializet(BAT *b)
{
	return BATmirror(BATmaterializeh(BATmirror(b)));
}

BAT *
BATmaterialize(BAT *b)
{
	return BATmaterializet(BATmaterializeh(b));
}


/*
 * @-
 * The @#VIEWunlink@ routine cuts a reference to the parent. Part of the view
 * destroy sequence.
 */
void
VIEWunlink(BAT *b)
{
	if (b) {
		bat hp = VIEWhparent(b), tp = VIEWtparent(b);
		bat vhp = VIEWvhparent(b), vtp = VIEWvtparent(b);
		BAT *hpb = NULL, *tpb = NULL;
		BAT *vhpb = NULL, *vtpb = NULL;

		if (hp)
			hpb = BBP_cache(hp);
		if (tp)
			tpb = BBP_cache(tp);
		if (hp && !vhp)
			vhp = hp;
		if (vhp)
			vhpb = BBP_cache(vhp);
		if (tp && !vtp)
			vtp = tp;
		if (vtp)
			vtpb = BBP_cache(vtp);

		if (hpb == NULL && tpb == NULL && vhpb == NULL && vtpb == NULL)
			return;

		/* unlink heaps shared with parent */
		assert(b->H->vheap == NULL || b->H->vheap->parentid > 0);
		assert(b->T->vheap == NULL || b->T->vheap->parentid > 0);
		if (b->H->vheap && b->H->vheap->parentid != ABS(b->batCacheid))
			b->H->vheap = NULL;
		if (b->T->vheap && b->T->vheap->parentid != ABS(b->batCacheid))
			b->T->vheap = NULL;

		/* unlink properties shared with parent */
		if (hpb && b->H->props && b->H->props == hpb->H->props)
			b->H->props = NULL;
		if (tpb && b->T->props && b->T->props == tpb->H->props)
			b->T->props = NULL;

		/* unlink hash accelerators shared with parent */
		if (hpb && b->H->hash && b->H->hash == hpb->H->hash)
			b->H->hash = NULL;
		if (tpb && b->T->hash && b->T->hash == tpb->H->hash)
			b->T->hash = NULL;
	}
}

/*
 * @-
 * Materialize a view into a normal BAT. If it is a slice, we really
 * want to reduce storage of the new BAT.
 */
BAT *
VIEWreset(BAT *b)
{
	bat hp, tp, hvp, tvp;
	Heap head, tail, hh, th;
	BAT *n = NULL, *v = NULL;

	if (b == NULL)
		return NULL;
	hp = VIEWhparent(b);
	tp = VIEWtparent(b);
	hvp = VIEWvhparent(b);
	tvp = VIEWvtparent(b);
	if (hp || tp) {
		BAT *m;
		BATstore *bs;
		BUN cnt;
		str nme;
		size_t nmelen;

		/* alloc heaps */
		memset(&head, 0, sizeof(Heap));
		memset(&tail, 0, sizeof(Heap));
		memset(&hh, 0, sizeof(Heap));
		memset(&th, 0, sizeof(Heap));

		n = BATdescriptor(ABS(b->batCacheid)); /* normalized */
		if (n == NULL)
			goto bailout;
		m = BATmirror(n); /* mirror of normalized */
		bs = BBP_desc(n->batCacheid);
		cnt = BATcount(n) + 1;
		nme = BBP_physical(n->batCacheid);
		nmelen = nme ? strlen(nme) : 0;

		assert(n->batCacheid > 0);
		assert(hp || !b->htype);
		assert(tp || !b->ttype);

		if (n->htype) {
			head.filename = (str) GDKmalloc(nmelen + 12);
			if (head.filename == NULL)
				goto bailout;
			snprintf(head.filename, nmelen + 12, "%s.head", nme);
			if (n->htype && HEAPalloc(&head, cnt, Hsize(n)) < 0)
				goto bailout;
		}
		if (n->ttype) {
			tail.filename = (str) GDKmalloc(nmelen + 12);
			if (tail.filename == NULL)
				goto bailout;
			snprintf(tail.filename, nmelen + 12, "%s.tail", nme);
			if (n->ttype && HEAPalloc(&tail, cnt, Tsize(n)) < 0)
				goto bailout;
		}
		if (n->H->vheap) {
			hh.filename = (str) GDKmalloc(nmelen + 12);
			if (hh.filename == NULL)
				goto bailout;
			snprintf(hh.filename, nmelen + 12, "%s.hheap", nme);
			if (ATOMheap(n->htype, &hh, cnt) < 0)
				goto bailout;
		}
		if (n->T->vheap) {
			th.filename = (str) GDKmalloc(nmelen + 12);
			if (th.filename == NULL)
				goto bailout;
			snprintf(th.filename, nmelen + 12, "%s.theap", nme);
			if (ATOMheap(n->ttype, &th, cnt) < 0)
				goto bailout;
		}

		v = VIEWcreate(n, n);
		if (v == NULL)
			goto bailout;

		/* cut the link to your parents */
		VIEWunlink(n);
		if (hp) {
			BBPunshare(hp);
			BBPunfix(hp);
		}
		if (tp) {
			BBPunshare(tp);
			BBPunfix(tp);
		}
		if (hvp) {
			BBPunshare(hvp);
			BBPunfix(hvp);
		}
		if (tvp) {
			BBPunshare(tvp);
			BBPunfix(tvp);
		}

		/* make sure everything points there */
		m->U = n->U = &bs->U;
		m->P = n->P = &bs->P;
		m->T = n->H = &bs->H;
		m->H = n->T = &bs->T;

		n->H->type = v->H->type;
		n->H->varsized = v->H->varsized;
		n->H->shift = v->H->shift;
		n->H->width = v->H->width;
		n->H->seq = v->H->seq;

		n->T->type = v->T->type;
		n->T->varsized = v->T->varsized;
		n->T->shift = v->T->shift;
		n->T->width = v->T->width;
		n->T->seq = v->T->seq;

		n->H->heap.parentid = n->T->heap.parentid = 0;
		n->batRestricted = BAT_WRITE;

		/* reset BOUND2KEY */
		n->H->key = BAThkey(v);
		n->T->key = BATtkey(v);

		/* copy the heaps */
		n->H->heap = head;
		n->T->heap = tail;

		/* unshare from parents heap */
		if (hh.base) {
			assert(n->H->vheap == NULL);
			n->H->vheap = (Heap *) GDKzalloc(sizeof(Heap));
			if (n->H->vheap == NULL)
				goto bailout;
			*n->H->vheap = hh;
			n->H->vheap->parentid = n->batCacheid;
		}
		if (th.base) {
			assert(n->T->vheap == NULL);
			n->T->vheap = (Heap *) GDKzalloc(sizeof(Heap));
			if (n->T->vheap == NULL)
				goto bailout;
			*n->T->vheap = th;
			n->T->vheap->parentid = n->batCacheid;
		}

		n->batSharecnt = 0;
		n->batCopiedtodisk = 0;
		n->batDirty = 1;

		/* reset BOUND2KEY */
		n->hkey = BAThkey(v);
		n->tkey = BATtkey(v);

		/* make the BAT empty and insert all again */
		DELTAinit(n);
		/* reset capacity */
		n->U->capacity = cnt;

		/* insert all of v in n, and quit */
		BATins(n, v, FALSE);
		BBPreclaim(v);
		BBPunfix(n->batCacheid);
	}
	return b;
      bailout:
	if (v != NULL)
		BBPreclaim(v);
	if (n != NULL)
		BBPunfix(n->batCacheid);
	HEAPfree(&head);
	HEAPfree(&tail);
	HEAPfree(&hh);
	HEAPfree(&th);
	return NULL;
}

/*
 * @-
 * The remainder are utilities to manipulate the BAT view and
 * not to forget some details in the process.
 * It expects a position range in the underlying BAT and
 * compensates for outliers.
 */
void
VIEWbounds(BAT *b, BAT *view, BUN l, BUN h)
{
	BUN cnt;
	BATiter bi = bat_iterator(b);

	if (b == NULL || view == NULL) {
		GDKerror("VIEWbounds: bat argument missing");
		return;
	}
	if (h > BATcount(b))
		h = BATcount(b);
	if (h < l)
		h = l;
	l += BUNfirst(b);
	view->batFirst = view->batDeleted = view->batInserted = 0;
	cnt = h - l;
	view->H->heap.base = (view->htype) ? BUNhloc(bi, l) : NULL;
	view->T->heap.base = (view->ttype) ? BUNtloc(bi, l) : NULL;
	view->H->heap.maxsize = view->H->heap.size = headsize(view, cnt);
	view->T->heap.maxsize = view->T->heap.size = tailsize(view, cnt);
	BATsetcount(view, cnt);
	BATsetcapacity(view, cnt);
}

/*
 * @-
 * Destroy a view.
 */
void
VIEWdestroy(BAT *b)
{
	assert(isVIEW(b));

	/* remove any leftover private hash structures */
	if (b->H->hash)
		HASHremove(b);
	if (b->T->hash)
		HASHremove(BATmirror(b));
	VIEWunlink(b);

	if (b->htype && !b->H->heap.parentid) {
		HEAPfree(&b->H->heap);
	} else {
		b->H->heap.base = NULL;
		b->H->heap.filename = NULL;
	}
	if (b->ttype && !b->T->heap.parentid) {
		HEAPfree(&b->T->heap);
	} else {
		b->T->heap.base = NULL;
		b->T->heap.filename = NULL;
	}
	b->H->vheap = NULL;
	b->T->vheap = NULL;
	BATfree(b);
}

