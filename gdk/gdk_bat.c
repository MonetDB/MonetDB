/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. Nes
 * @* BAT Module
 * In this Chapter we describe the BAT implementation in more detail.
 * The routines mentioned are primarily meant to simplify the library
 * implementation.
 *
 * @+ BAT Construction
 * BATs are implemented in several blocks of memory, prepared for disk
 * storage and easy shipment over a network.
 *
 * The BAT starts with a descriptor, which indicates the required BAT
 * library version and the BAT administration details.  In particular,
 * it describes the binary relationship maintained and the location of
 * fields required for storage.
 *
 * The general layout of the BAT in this implementation is as follows.
 * Each BAT comes with a heap for the loc-size buns and, optionally,
 * with heaps to manage the variable-sized data items of both
 * dimensions.  The buns are assumed to be stored as loc-size objects.
 * This is essentially an array of structs to store the associations.
 * The size is determined at BAT creation time using an upper bound on
 * the number of elements to be accommodated.  In case of overflow,
 * its storage space is extended automatically.
 *
 * The capacity of a BAT places an upper limit on the number of BUNs
 * to be stored initially. The actual space set aside may be quite
 * large.  Moreover, the size is aligned to int boundaries to speedup
 * access and avoid some machine limitations.
 *
 * Initialization of the variable parts rely on type specific routines
 * called atomHeap.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#ifdef ALIGN
#undef ALIGN
#endif
#define ALIGN(n,b)	((b)?(b)*(1+(((n)-1)/(b))):n)

#define ATOMneedheap(tpe) (BATatoms[tpe].atomHeap != NULL)

static char *BATstring_h = "h";
static char *BATstring_t = "t";

static inline int
default_ident(char *s)
{
	return (s == BATstring_h || s == BATstring_t);
}

void
BATinit_idents(BAT *bn)
{
	bn->hident = BATstring_h;
	bn->tident = BATstring_t;
}

BATstore *
BATcreatedesc(int tt, int heapnames, int role)
{
	BAT *bn;
	BATstore *bs;

	/*
	 * Alloc space for the BAT and its dependent records.
	 */
	assert(tt >= 0);
	assert(role >= 0 && role < 32);

	bs = (BATstore *) GDKzalloc(sizeof(BATstore));

	if (bs == NULL)
		return NULL;

	/*
	 * assert needed in the kernel to get symbol eprintf resolved.
	 * Else modules using assert fail to load.
	 */
	bs->BM.H = &bs->T;
	bs->BM.T = &bs->H;
	bs->BM.S = &bs->S;
	bs->B.H = &bs->H;
	bs->B.T = &bs->T;
	bs->B.S = &bs->S;

	bn = &bs->B;

	/*
	 * Fill in basic column info
	 */
	bn->htype = TYPE_void;
	bn->ttype = tt;
	bn->hkey = TRUE | BOUND2BTRUE;
	bn->tkey = FALSE;
	bn->H->nonil = TRUE;
	bn->T->nonil = TRUE;
	bn->H->nil = FALSE;
	bn->T->nil = FALSE;
	bn->hsorted = bn->hrevsorted = 1;
	bn->tsorted = bn->trevsorted = ATOMlinear(tt) != 0;

	bn->hident = BATstring_h;
	bn->tident = BATstring_t;
	bn->halign = OIDnew(2);
	bn->talign = bn->halign + 1;
	bn->hseqbase = 0;
	bn->tseqbase = (tt == TYPE_void) ? oid_nil : 0;
	bn->batRole = role;
	bn->batPersistence = TRANSIENT;
	bn->H->props = bn->T->props = NULL;
	/*
	 * add to BBP
	 */
	BBPinsert(bs);
	/*
	 * fill in heap names, so HEAPallocs can resort to disk for
	 * very large writes.
	 */
	assert(bn->batCacheid > 0);
	bn->H->heap.filename = NULL;
	bn->T->heap.filename = NULL;
	bn->H->heap.farmid = BBPselectfarm(role, bn->htype, offheap);
	bn->T->heap.farmid = BBPselectfarm(role, bn->ttype, offheap);
	if (heapnames) {
		const char *nme = BBP_physical(bn->batCacheid);

		if (tt) {
			bn->T->heap.filename = GDKfilepath(NOFARM, NULL, nme, "tail");
			if (bn->T->heap.filename == NULL)
				goto bailout;
		}

		if (ATOMneedheap(tt)) {
			if ((bn->T->vheap = (Heap *) GDKzalloc(sizeof(Heap))) == NULL ||
			    (bn->T->vheap->filename = GDKfilepath(NOFARM, NULL, nme, "theap")) == NULL)
				goto bailout;
			bn->T->vheap->parentid = bn->batCacheid;
			bn->T->vheap->farmid = BBPselectfarm(role, bn->ttype, varheap);
		}
	}
	bn->batDirty = TRUE;
	return bs;
      bailout:
	if (tt)
		HEAPfree(&bn->T->heap, 1);
	if (bn->T->vheap) {
		HEAPfree(bn->T->vheap, 1);
		GDKfree(bn->T->vheap);
	}
	GDKfree(bs);
	return NULL;
}

bte
ATOMelmshift(int sz)
{
	bte sh;
	int i = sz >> 1;

	for (sh = 0; i != 0; sh++) {
		i >>= 1;
	}
	return sh;
}


void
BATsetdims(BAT *b)
{
	assert(b->htype == TYPE_void);
	b->H->width = 0;
	b->H->shift = 0;
	b->H->varsized = 1;

	b->T->width = b->ttype == TYPE_str ? 1 : ATOMsize(b->ttype);
	b->T->shift = ATOMelmshift(Tsize(b));
	assert_shift_width(b->T->shift, b->T->width);
	b->T->varsized = b->ttype == TYPE_void || BATatoms[b->ttype].atomPut != NULL;
}

/*
 * @- BAT allocation
 * Allocate BUN heap and variable-size atomheaps (see e.g. strHeap).
 * We now initialize new BATs with their heapname such that the
 * modified HEAPalloc/HEAPextend primitives can possibly use memory
 * mapped files as temporary heap storage.
 *
 * In case of huge bats, we want HEAPalloc to write a file to disk,
 * and memory map it. To make this possible, we must provide it with
 * filenames.
 */
static BATstore *
BATnewstorage(int tt, BUN cap, int role)
{
	BATstore *bs;
	BAT *bn;

	/* and in case we don't have assertions enabled: limit the size */
	if (cap > BUN_MAX) {
		/* shouldn't happen, but if it does... */
		assert(0);
		cap = BUN_MAX;
	}
	bs = BATcreatedesc(tt, tt != TYPE_void, role);
	if (bs == NULL)
		return NULL;
	bn = &bs->B;

	BATsetdims(bn);
	bn->batCapacity = cap;

	/* alloc the main heaps */
	if (tt && HEAPalloc(&bn->T->heap, cap, bn->T->width) != GDK_SUCCEED) {
		return NULL;
	}

	if (ATOMheap(tt, bn->T->vheap, cap) != GDK_SUCCEED) {
		if (tt)
			HEAPfree(&bn->T->heap, 1);
		GDKfree(bn->T->vheap);
		return NULL;
	}
	DELTAinit(bn);
	BBPcacheit(bs, 1);
	return bs;
}

BAT *
BATnew(int ht, int tt, BUN cap, int role)
{
	BATstore *bs;

	assert(cap <= BUN_MAX);
	assert(ht == TYPE_void);
	assert(tt != TYPE_bat);
	ERRORcheck((ht < 0) || (ht > GDKatomcnt), "BATnew:ht error\n", NULL);
	ERRORcheck((tt < 0) || (tt > GDKatomcnt), "BATnew:tt error\n", NULL);
	ERRORcheck(role < 0 || role >= 32, "BATnew:role error\n", NULL);

	(void) ht;		/* always TYPE_void */
	/* round up to multiple of BATTINY */
	if (cap < BUN_MAX - BATTINY)
		cap = (cap + BATTINY - 1) & ~(BATTINY - 1);
	if (cap < BATTINY)
		cap = BATTINY;
	/* and in case we don't have assertions enabled: limit the size */
	if (cap > BUN_MAX)
		cap = BUN_MAX;
	bs = BATnewstorage(tt, cap, role);
	return bs == NULL ? NULL : &bs->B;
}

BAT *
BATdense(oid hseq, oid tseq, BUN cnt)
{
	BAT *bn;

	bn = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATseqbase(bn, hseq);
	BATseqbase(BATmirror(bn), tseq);
	BATsetcount(bn, cnt);
	return bn;
}

BAT *
BATattach(int tt, const char *heapfile, int role)
{
	BATstore *bs;
	BAT *bn;
	struct stat st;
	int atomsize;
	BUN cap;
	char *path;

	ERRORcheck(tt <= 0 , "BATattach: bad tail type (<=0)\n", NULL);
	ERRORcheck(ATOMvarsized(tt), "BATattach: bad tail type (varsized)\n", NULL);
	ERRORcheck(heapfile == 0, "BATattach: bad heapfile name\n", NULL);
	ERRORcheck(role < 0 || role >= 32, "BATattach: role error\n", NULL);
	if (lstat(heapfile, &st) < 0) {
		GDKsyserror("BATattach: cannot stat heapfile\n");
		return NULL;
	}
	ERRORcheck(!S_ISREG(st.st_mode), "BATattach: heapfile must be a regular file\n", NULL);
	ERRORcheck(st.st_nlink != 1, "BATattach: heapfile must have only one link\n", NULL);
	atomsize = ATOMsize(tt);
	ERRORcheck(st.st_size % atomsize != 0, "BATattach: heapfile size not integral number of atoms\n", NULL);
	ERRORcheck((size_t) (st.st_size / atomsize) > (size_t) BUN_MAX, "BATattach: heapfile too large\n", NULL);
	cap = (BUN) (st.st_size / atomsize);
	bs = BATcreatedesc(tt, 1, role);
	if (bs == NULL)
		return NULL;
	bn = &bs->B;
	BATsetdims(bn);
	path = GDKfilepath(bn->T->heap.farmid, BATDIR, bn->T->heap.filename, "new");
	GDKcreatedir(path);
	if (rename(heapfile, path) < 0) {
		GDKsyserror("BATattach: cannot rename heapfile\n");
		GDKfree(path);
		HEAPfree(&bn->T->heap, 1);
		GDKfree(bs);
		return NULL;
	}
	GDKfree(path);
	bn->hseqbase = 0;
	BATkey(bn, TRUE);
	BATsetcapacity(bn, cap);
	BATsetcount(bn, cap);
	if (cap > 1) {
		bn->tsorted = 0;
		bn->trevsorted = 0;
		bn->tdense = 0;
		bn->tkey = 0;
	}
	bn->batRestricted = BAT_READ;
	bn->T->heap.size = (size_t) st.st_size;
	bn->T->heap.newstorage = bn->T->heap.storage = (bn->T->heap.size < GDK_mmap_minsize) ? STORE_MEM : STORE_MMAP;
	if (HEAPload(&bn->T->heap, BBP_physical(bn->batCacheid), "tail", TRUE) != GDK_SUCCEED) {
		HEAPfree(&bn->T->heap, 1);
		GDKfree(bs);
		return NULL;
	}
	BBPcacheit(bs, 1);
	return bn;
}

/*
 * If the BAT runs out of storage for BUNS it will reallocate space.
 * For memory mapped BATs we simple extend the administration after
 * having an assurance that the BAT still can be safely stored away.
 */
BUN
BATgrows(BAT *b)
{
	BUN oldcap, newcap;

	BATcheck(b, "BATgrows", 0);

	newcap = oldcap = BATcapacity(b);
	if (newcap < BATTINY)
		newcap = 2 * BATTINY;
	else if (newcap < 10 * BATTINY)
		newcap = 4 * newcap;
	else if (newcap < 50 * BATTINY)
		newcap = 2 * newcap;
	else if ((double) newcap * BATMARGIN <= (double) BUN_MAX)
		newcap = (BUN) ((double) newcap * BATMARGIN);
	else
		newcap = BUN_MAX;
	if (newcap == oldcap) {
		if (newcap <= BUN_MAX - 10)
			newcap += 10;
		else
			newcap = BUN_MAX;
	}
	return newcap;
}

/*
 * The routine should ensure that the BAT keeps its location in the
 * BAT buffer.
 *
 * Overflow in the other heaps are dealt with in the atom routines.
 * Here we merely copy their references into the new administration
 * space.
 */
gdk_return
BATextend(BAT *b, BUN newcap)
{
	size_t theap_size = newcap;

	assert(newcap <= BUN_MAX);
	BATcheck(b, "BATextend", GDK_FAIL);
	assert(b->htype == TYPE_void);
	/*
	 * The main issue is to properly predict the new BAT size.
	 * storage overflow. The assumption taken is that capacity
	 * overflow is rare. It is changed only when the position of
	 * the next available BUN surpasses the free area marker.  Be
	 * aware that the newcap should be greater than the old value,
	 * otherwise you may easily corrupt the administration of
	 * malloc.
	 */
	if (newcap <= BATcapacity(b)) {
		return GDK_SUCCEED;
	}

	b->batCapacity = newcap;

	theap_size *= Tsize(b);
	if (b->T->heap.base && GDKdebug & HEAPMASK)
		fprintf(stderr, "#HEAPextend in BATextend %s " SZFMT " " SZFMT "\n", b->T->heap.filename, b->T->heap.size, theap_size);
	if (b->T->heap.base &&
	    HEAPextend(&b->T->heap, theap_size, b->batRestricted == BAT_READ) != GDK_SUCCEED)
		return GDK_FAIL;
	HASHdestroy(b);
	IMPSdestroy(b);
	return GDK_SUCCEED;
}



/*
 * @+ BAT destruction
 * BATclear quickly removes all elements from a BAT. It must respect
 * the transaction rules; so stable elements must be moved to the
 * "deleted" section of the BAT (they cannot be fully deleted
 * yet). For the elements that really disappear, we must free
 * heapspace and unfix the atoms if they have fix/unfix handles. As an
 * optimization, in the case of no stable elements, we quickly empty
 * the heaps by copying a standard small empty image over them.
 */
gdk_return
BATclear(BAT *b, int force)
{
	BUN p, q;

	BATcheck(b, "BATclear", GDK_FAIL);
	assert(b->htype == TYPE_void);

	/* kill all search accelerators */
	HASHdestroy(b);
	IMPSdestroy(b);

	/* we must dispose of all inserted atoms */
	if ((b->batDeleted == b->batInserted || force) &&
	    BATatoms[b->ttype].atomDel == NULL) {
		Heap th;

		/* no stable elements: we do a quick heap clean */
		/* need to clean heap which keeps data even though the
		   BUNs got removed. This means reinitialize when
		   free > 0
		*/
		memset(&th, 0, sizeof(th));
		if (b->T->vheap) {
			th.farmid = b->T->vheap->farmid;
			if (b->T->vheap->free > 0 &&
			    ATOMheap(b->ttype, &th, 0) != GDK_SUCCEED)
				return GDK_FAIL;
		}
		assert(b->T->vheap == NULL || b->T->vheap->parentid == abs(b->batCacheid));
		if (b->T->vheap && b->T->vheap->free > 0) {
			th.parentid = b->T->vheap->parentid;
			HEAPfree(b->T->vheap, 0);
			*b->T->vheap = th;
		}
	} else {
		/* do heap-delete of all inserted atoms */
		void (*tatmdel)(Heap*,var_t*) = BATatoms[b->ttype].atomDel;

		/* TYPE_str has no del method, so we shouldn't get here */
		assert(tatmdel == NULL || b->T->width == sizeof(var_t));
		if (tatmdel) {
			BATiter bi = bat_iterator(b);

			for(p = b->batInserted, q = BUNlast(b); p < q; p++)
				(*tatmdel)(b->T->vheap, (var_t*) BUNtloc(bi,p));
		}
	}

	if (force)
		b->batFirst = b->batDeleted = b->batInserted = 0;
	else
		b->batFirst = b->batInserted;
	BATsetcount(b,0);
	BATseqbase(b, 0);
	BATseqbase(BATmirror(b), 0);
	b->batDirty = TRUE;
	BATsettrivprop(b);
	b->H->nosorted = b->H->norevsorted = b->H->nodense = 0;
	b->H->nokey[0] = b->H->nokey[1] = 0;
	b->T->nosorted = b->T->norevsorted = b->T->nodense = 0;
	b->T->nokey[0] = b->T->nokey[1] = 0;
	return GDK_SUCCEED;
}

/* free a cached BAT; leave the bat descriptor cached */
void
BATfree(BAT *b)
{
	if (b == NULL)
		return;

	/* deallocate all memory for a bat */
	if (b->batCacheid < 0)
		b = BBP_cache(-(b->batCacheid));
	if (b->hident && !default_ident(b->hident))
		GDKfree(b->hident);
	b->hident = BATstring_h;
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	b->tident = BATstring_t;
	if (b->H->props)
		PROPdestroy(b->H->props);
	b->H->props = NULL;
	if (b->T->props)
		PROPdestroy(b->T->props);
	b->T->props = NULL;
	HASHfree(b);
	IMPSfree(b);
	if (b->htype)
		HEAPfree(&b->H->heap, 0);
	else
		assert(!b->H->heap.base);
	if (b->ttype)
		HEAPfree(&b->T->heap, 0);
	else
		assert(!b->T->heap.base);
	if (b->H->vheap) {
		assert(b->H->vheap->parentid == b->batCacheid);
		HEAPfree(b->H->vheap, 0);
	}
	if (b->T->vheap) {
		assert(b->T->vheap->parentid == b->batCacheid);
		HEAPfree(b->T->vheap, 0);
	}

	b = BBP_cache(-b->batCacheid);
	if (b) {
		BBP_cache(b->batCacheid) = NULL;
	}
}

/* free a cached BAT descriptor */
void
BATdestroy( BATstore *bs )
{
	if (bs->H.id && !default_ident(bs->H.id))
		GDKfree(bs->H.id);
	bs->H.id = BATstring_h;
	if (bs->T.id && !default_ident(bs->T.id))
		GDKfree(bs->T.id);
	bs->T.id = BATstring_t;
	if (bs->H.vheap)
		GDKfree(bs->H.vheap);
	if (bs->T.vheap)
		GDKfree(bs->T.vheap);
	if (bs->H.props)
		PROPdestroy(bs->H.props);
	if (bs->T.props)
		PROPdestroy(bs->T.props);
	GDKfree(bs);
}

/*
 * @+ BAT copying
 *
 * BAT copying is an often used operation. So it deserves attention.
 * When making a copy of a BAT, the following aspects are of
 * importance:
 *
 * - the requested head and tail types. The purpose of the copy may be
 *   to slightly change these types (e.g. void <-> oid). We may also
 *   remap between types as long as they share the same
 *   ATOMstorage(type), i.e. the types have the same physical
 *   implementation. We may even want to allow 'dirty' trick such as
 *   viewing a flt-column suddenly as int.
 *
 *   To allow such changes, the desired column-types is a
 *   parameter of COLcopy.
 *
 * - access mode. If we want a read-only copy of a read-only BAT, a
 *   VIEW may do (in this case, the user may be after just an
 *   independent BAT header and id). This is indicated by the
 *   parameter (writable = FALSE).
 *
 *   In other cases, we really want an independent physical copy
 *   (writable = TRUE).  Changing the mode to BAT_WRITE will be a
 *   zero-cost operation if the BAT was copied with (writable = TRUE).
 *
 * In GDK, the result is a BAT that is BAT_WRITE iff (writable ==
 * TRUE).
 *
 * In these cases the copy becomes a logical view on the original,
 * which ensures that the original cannot be modified or destroyed
 * (which could affect the shared heaps).
 */
static gdk_return
heapcopy(BAT *bn, char *ext, Heap *dst, Heap *src)
{
	if (src->filename && src->newstorage != STORE_MEM) {
		const char *nme = BBP_physical(bn->batCacheid);

		if ((dst->filename = GDKfilepath(NOFARM, NULL, nme, ext)) == NULL)
			return GDK_FAIL;
	}
	return HEAPcopy(dst, src);
}

static void
heapmove(Heap *dst, Heap *src)
{
	if (src->filename == NULL) {
		src->filename = dst->filename;
		dst->filename = NULL;
	}
	HEAPfree(dst, 0);
	*dst = *src;
}

static int
wrongtype(int t1, int t2)
{
	/* check if types are compatible. be extremely forgiving */
	if (t1 != TYPE_void) {
		t1 = ATOMtype(ATOMstorage(t1));
		t2 = ATOMtype(ATOMstorage(t2));
		if (t1 != t2) {
			if (ATOMvarsized(t1) ||
			    ATOMvarsized(t2) ||
			    ATOMsize(t1) != ATOMsize(t2) ||
			    ATOMalign(t1) != ATOMalign(t2) ||
			    BATatoms[t1].atomFix ||
			    BATatoms[t2].atomFix)
				return TRUE;
		}
	}
	return FALSE;
}

/*
 * There are four main implementation cases:
 * (1) we are allowed to return a view (zero effort),
 * (2) the result is void,void (zero effort),
 * (3) we can copy the heaps (memcopy, or even VM page sharing)
 * (4) we must insert BUN-by-BUN into the result (fallback)
 * The latter case is still optimized for the case that the result
 * is bat[void,T] for a simple fixed-size type T. In that case we
 * do inline array[T] inserts.
 */
/* TODO make it simpler, ie copy per column */
BAT *
COLcopy(BAT *b, int tt, int writable, int role)
{
	BUN bunstocopy = BUN_NONE;
	BUN cnt;
	BAT *bn = NULL;

	BATcheck(b, "BATcopy", NULL);
	assert(tt != TYPE_bat);
	cnt = b->batCount;

	/* maybe a bit ugly to change the requested bat type?? */
	if (b->ttype == TYPE_void && !writable)
		tt = TYPE_void;

	if (tt != b->ttype && wrongtype(tt, b->ttype)) {
		GDKerror("BATcopy: wrong tail-type requested\n");
		return NULL;
	}

	/* first try case (1); create a view, possibly with different
	 * atom-types */
	if (role == b->batRole &&
	    BAThrestricted(b) == BAT_READ &&
	    BATtrestricted(b) == BAT_READ &&
	    !writable) {
		bn = VIEWcreate(b->hseqbase, b);
		if (bn == NULL)
			return NULL;
		if (bn->htype != TYPE_void) {
			/* legacy BAT, transform to "headless" */
			assert(bn->H != bn->T);
			bn->htype = TYPE_void;
			bn->hvarsized = ATOMvarsized(TYPE_void);
			bn->hseqbase = b->hseqbase;
		}
		if (tt != bn->ttype) {
			assert(bn->H != bn->T);
			bn->ttype = tt;
			bn->tvarsized = ATOMvarsized(tt);
			bn->tseqbase = b->tseqbase;
		}
	} else {
		/* check whether we need case (4); BUN-by-BUN copy (by
		 * setting bunstocopy != BUN_NONE) */
		if (ATOMsize(b->htype) != 0 ||
		    ATOMsize(tt) != ATOMsize(b->ttype)) {
			/* oops, void materialization */
			bunstocopy = cnt;
		} else if (BATatoms[tt].atomFix) {
			/* oops, we need to fix/unfix atoms */
			bunstocopy = cnt;
		} else if (isVIEW(b)) {
			/* extra checks needed for views */
			bat hp = VIEWhparent(b), tp = VIEWtparent(b);

			if (isVIEWCOMBINE(b) ||	/* oops, mirror view! */
			    /* reduced slice view: do not copy too
			     * much garbage */
			    (hp != 0 && BATcapacity(BBP_cache(hp)) > cnt + cnt) ||
			    (tp != 0 && BATcapacity(BBP_cache(tp)) > cnt + cnt))
				bunstocopy = cnt;
		}

		bn = BATnew(TYPE_void, tt, MAX(1, bunstocopy == BUN_NONE ? 0 : bunstocopy), role);
		if (bn == NULL)
			return NULL;

		if (bn->tvarsized && bn->ttype && bunstocopy == BUN_NONE) {
			bn->T->shift = b->T->shift;
			bn->T->width = b->T->width;
			if (HEAPextend(&bn->T->heap, BATcapacity(bn) << bn->T->shift, TRUE) != GDK_SUCCEED)
				goto bunins_failed;
		}

		if (tt == TYPE_void) {
			/* case (2): a void,void result => nothing to
			 * copy! */
			bn->H->heap.free = 0;
			bn->T->heap.free = 0;
		} else if (bunstocopy == BUN_NONE) {
			/* case (3): just copy the heaps; if possible
			 * with copy-on-write VM support */
			Heap bthp, thp;

			assert(b->htype == TYPE_void);
			memset(&bthp, 0, sizeof(Heap));
			memset(&thp, 0, sizeof(Heap));

			bthp.farmid = BBPselectfarm(role, b->ttype, offheap);
			thp.farmid = BBPselectfarm(role, b->ttype, varheap);
			if ((b->ttype && heapcopy(bn, "tail", &bthp, &b->T->heap) != GDK_SUCCEED) ||
			    (bn->T->vheap && heapcopy(bn, "theap", &thp, b->T->vheap) != GDK_SUCCEED)) {
				HEAPfree(&thp, 1);
				HEAPfree(&bthp, 1);
				BBPreclaim(bn);
				return NULL;
			}
			/* succeeded; replace dummy small heaps by the
			 * real ones */
			heapmove(&bn->T->heap, &bthp);
			thp.parentid = bn->batCacheid;
			if (bn->T->vheap)
				heapmove(bn->T->vheap, &thp);

			/* make sure we use the correct capacity */
			bn->batCapacity = (BUN) (bn->ttype ? bn->T->heap.size >> bn->T->shift : 0);


			/* first/inserted must point equally far into
			 * the heap as in the source */
			bn->batFirst = b->batFirst;
			bn->batInserted = b->batInserted;
		} else if (BATatoms[tt].atomFix || tt != TYPE_void || ATOMextern(tt)) {
			/* case (4): one-by-one BUN insert (really slow) */
			BUN p, q, r = BUNfirst(bn);
			BATiter bi = bat_iterator(b);

			BATloop(b, p, q) {
				const void *h = BUNhead(bi, p);
				const void *t = BUNtail(bi, p);

				bunfastins_nocheck(bn, r, h, t, Hsize(bn), Tsize(bn));
				r++;
			}
		} else if (tt != TYPE_void && b->ttype == TYPE_void) {
			/* case (4): optimized for unary void
			 * materialization */
			oid cur = b->tseqbase, *dst = (oid *) bn->T->heap.base;
			oid inc = (cur != oid_nil);

			bn->H->heap.free = 0;
			bn->T->heap.free = bunstocopy * sizeof(oid);
			while (bunstocopy--) {
				*dst++ = cur;
				cur += inc;
			}
		} else {
			/* case (4): optimized for simple array copy */
			BUN p = BUNfirst(b);

			bn->H->heap.free = 0;
			bn->T->heap.free = bunstocopy * Tsize(bn);
			memcpy(Tloc(bn, 0), Tloc(b, p), bn->T->heap.free);
		}
		/* copy all properties (size+other) from the source bat */
		BATsetcount(bn, cnt);
	}
	/* set properties (note that types may have changed in the copy) */
	ALIGNsetH(bn, b);
	if (ATOMtype(tt) == ATOMtype(b->ttype)) {
		ALIGNsetT(bn, b);
	} else if (ATOMstorage(tt) == ATOMstorage(b->ttype) &&
		   ATOMcompare(tt) == ATOMcompare(b->ttype)) {
		BUN l = BUNfirst(b), h = BUNlast(b);
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->tdense = b->tdense && ATOMtype(bn->ttype) == TYPE_oid;
		if (b->tkey)
			BATkey(BATmirror(bn), TRUE);
		bn->T->nonil = b->T->nonil;
		if (b->T->nosorted > l && b->T->nosorted < h)
			bn->T->nosorted = b->T->nosorted - l + BUNfirst(bn);
		else
			bn->T->nosorted = 0;
		if (b->T->norevsorted > l && b->T->norevsorted < h)
			bn->T->norevsorted = b->T->norevsorted - l + BUNfirst(bn);
		else
			bn->T->norevsorted = 0;
		if (b->T->nodense > l && b->T->nodense < h)
			bn->T->nodense = b->T->nodense - l + BUNfirst(bn);
		else
			bn->T->nodense = 0;
		if (b->T->nokey[0] >= l && b->T->nokey[0] < h &&
		    b->T->nokey[1] >= l && b->T->nokey[1] < h &&
		    b->T->nokey[0] != b->T->nokey[1]) {
			bn->T->nokey[0] = b->T->nokey[0] - l + BUNfirst(bn);
			bn->T->nokey[1] = b->T->nokey[1] - l + BUNfirst(bn);
		} else {
			bn->T->nokey[0] = bn->T->nokey[1] = 0;
		}
	} else {
		bn->tsorted = bn->trevsorted = 0; /* set based on count later */
		bn->tdense = bn->T->nonil = 0;
		bn->T->nosorted = bn->T->norevsorted = bn->T->nodense = 0;
		bn->T->nokey[0] = bn->T->nokey[1] = 0;
	}
	if (BATcount(bn) <= 1) {
		bn->hsorted = ATOMlinear(b->htype);
		bn->hrevsorted = ATOMlinear(b->htype);
		bn->hkey = 1;
		bn->tsorted = ATOMlinear(b->ttype);
		bn->trevsorted = ATOMlinear(b->ttype);
		bn->tkey = 1;
	}
	if (writable != TRUE)
		bn->batRestricted = BAT_READ;
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

#ifdef HAVE_HGE
#define un_move_sz16(src, dst, sz)					\
		if (sz == 16) {						\
			* (hge *) dst = * (hge *) src;			\
		} else
#else
#define un_move_sz16(src, dst, sz)
#endif

#define un_move(src, dst, sz)						\
	do {								\
		un_move_sz16(src,dst,sz)				\
		if (sz == 8) {						\
			* (lng *) dst = * (lng *) src;			\
		} else if (sz == 4) {					\
			* (int *) dst = * (int *) src;			\
		} else if (sz > 0) {					\
			char *_dst = (char *) dst;			\
			char *_src = (char *) src;			\
			char *_end = _src + sz;				\
									\
			while (_src < _end)				\
				*_dst++ = *_src++;			\
		}							\
	} while (0)
#define acc_move(l, p)							\
	do {								\
		char tmp[16];						\
		/* avoid compiler warning: dereferencing type-punned pointer \
		 * will break strict-aliasing rules */			\
		char *tmpp = tmp;					\
									\
		assert(ts <= 16);					\
									\
		/* move first to tmp */					\
		un_move(Tloc(b, l), tmpp, ts);				\
		/* move delete to first */				\
		un_move(Tloc(b, p), Tloc(b, l), ts);			\
		/* move first to deleted */				\
		un_move(tmpp, Tloc(b, p), ts);				\
	} while (0)

static void
setcolprops(BAT *b, COLrec *col, const void *x)
{
	int isnil = col->type != TYPE_void &&
		atom_CMP(x, ATOMnilptr(col->type), col->type) == 0;
	BATiter bi;
	BUN pos;
	const void *prv;
	int cmp;

	/* x may only be NULL if the column type is VOID */
	assert(x != NULL || col->type == TYPE_void);
	if (b->batCount == 0) {
		/* first value */
		col->sorted = col->revsorted = ATOMlinear(col->type) != 0;
		col->nosorted = col->norevsorted = 0;
		col->key |= 1;
		col->nokey[0] = col->nokey[1] = 0;
		col->nodense = 0;
		if (col->type == TYPE_void) {
			if (x) {
				col->seq = * (const oid *) x;
			}
			col->nil = col->seq == oid_nil;
			col->nonil = !col->nil;
		} else {
			col->nil = isnil;
			col->nonil = !isnil;
			if (col->type == TYPE_oid) {
				col->dense = !isnil;
				col->seq = * (const oid *) x;
				if (isnil)
					col->nodense = BUNlast(b);
			}
		}
	} else if (col->type == TYPE_void) {
		/* not the first value in a VOID column: we keep the
		 * seqbase, and x is not used, so only some properties
		 * are affected */
		if (col->seq != oid_nil) {
			if (col->revsorted) {
				col->norevsorted = BUNlast(b);
				col->revsorted = 0;
			}
			col->nil = 0;
			col->nonil = 1;
		} else {
			if (col->key) {
				col->nokey[0] = BUNfirst(b);
				col->nokey[1] = BUNlast(b);
				col->key = 0;
			}
			col->nil = 1;
			col->nonil = 0;
		}
	} else {
		bi = bat_iterator(b);
		pos = BUNlast(b);
		prv = col == b->H ? BUNhead(bi, pos - 1) : BUNtail(bi, pos - 1);
		cmp = atom_CMP(prv, x, col->type);

		if (col->key == 1 && /* assume outside check if BOUND2BTRUE */
		    (cmp == 0 || /* definitely not KEY */
		     (b->batCount > 1 && /* can't guarantee KEY if unordered */
		      ((col->sorted && cmp > 0) ||
		       (col->revsorted && cmp < 0) ||
		       (!col->sorted && !col->revsorted))))) {
			col->key = 0;
			if (cmp == 0) {
				col->nokey[0] = pos - 1;
				col->nokey[1] = pos;
			}
		}
		if (col->sorted && cmp > 0) {
			/* out of order */
			col->sorted = 0;
			col->nosorted = pos;
		}
		if (col->revsorted && cmp < 0) {
			/* out of order */
			col->revsorted = 0;
			col->norevsorted = pos;
		}
		if (col->dense && (cmp >= 0 || * (const oid *) prv + 1 != * (const oid *) x)) {
			col->dense = 0;
			col->nodense = pos;
		}
		if (isnil) {
			col->nonil = 0;
			col->nil = 1;
		}
	}
}

oid
MAXoid(BAT *i)
{
	BATiter ii = bat_iterator(i);
	oid o = i->hseqbase - 1;

	if (i->batCount)
		o = *(oid *) BUNhead(ii, BUNlast(i) - 1);
	if (!BAThordered(i)) {
		BUN r, s;

		BATloop(i, r, s) {
			oid v = *(oid *) BUNhead(ii, r);

			if (v > o)
				o = v;
		}
	}
	return o;
}

/*
 * @+ BUNappend
 * The BUNappend function can be used to add a single value to void
 * and oid headed bats. The new head value will be a unique number,
 * (max(bat)+1).
 */
gdk_return
BUNappend(BAT *b, const void *t, bit force)
{
	BUN p;
	size_t tsize = 0;

	BATcheck(b, "BUNappend", GDK_FAIL);
	assert(b->htype == TYPE_void);

	assert(!isVIEW(b));
	if ((b->tkey & BOUND2BTRUE) && BUNfnd(b, t) != BUN_NONE) {
		return GDK_SUCCEED;
	}

	p = BUNlast(b);		/* insert at end */
	if (p == BUN_MAX || b->batCount == BUN_MAX) {
		GDKerror("BUNappend: bat too large\n");
		return GDK_FAIL;
	}

	ALIGNapp(b, "BUNappend", force, GDK_FAIL);
	b->batDirty = 1;
	if (b->T->hash && b->T->vheap)
		tsize = b->T->vheap->size;

	if (b->ttype == TYPE_void && b->tseqbase != oid_nil) {
		if (b->batCount == 0) {
			b->tseqbase = * (const oid *) t;
		} else if (* (oid *) t == oid_nil ||
			   b->tseqbase + b->batCount != *(const oid *) t) {
			if (BATmaterialize(b) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	}

	if (unshare_string_heap(b) != GDK_SUCCEED) {
		return GDK_FAIL;
	}

	b->hrevsorted = b->batCount == 0; /* count before append */
	setcolprops(b, b->T, t);

	if (b->ttype != TYPE_void) {
		bunfastapp(b, t);
	} else {
		BATsetcount(b, b->batCount + 1);
	}


	IMPSdestroy(b); /* no support for inserts in imprints yet */

	/* first adapt the hashes; then the user-defined accelerators.
	 * REASON: some accelerator updates (qsignature) use the hashes!
	 */
	if (b->T->hash) {
		HASHins(b, p, t);
		if (tsize && tsize != b->T->vheap->size)
			HEAPwarm(b->T->vheap);
	}
	return GDK_SUCCEED;
      bunins_failed:
	return GDK_FAIL;
}

gdk_return
BUNdelete(BAT *b, oid o)
{
	BUN p;
	BATiter bi = bat_iterator(b);

	assert(b->htype == TYPE_void);
	assert(b->hseqbase != oid_nil || BATcount(b) == 0);
	if (o < b->hseqbase || o >= b->hseqbase + BATcount(b)) {
		/* value already not there */
		return GDK_SUCCEED;
	}
	assert(BATcount(b) > 0); /* follows from "if" above */
	p = o - b->hseqbase + BUNfirst(b);
	if (p < b->batInserted) {
		GDKerror("BUNdelete: cannot delete committed value\n");
		return GDK_FAIL;
	}
	b->batDirty = 1;
	ATOMunfix(b->ttype, BUNtail(bi, p));
	ATOMdel(b->ttype, b->T->vheap, (var_t *) BUNtloc(bi, p));
	if (p != BUNlast(b) - 1 &&
	    (b->ttype != TYPE_void || b->tseqbase != oid_nil)) {
		/* replace to-be-delete BUN with last BUN; materialize
		 * void column before doing so */
		if (b->ttype == TYPE_void &&
		    BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
		memcpy(Tloc(b, p), Tloc(b, BUNlast(b) - 1), Tsize(b));
		/* no longer sorted */
		b->tsorted = b->trevsorted = 0;
	}
	if (b->T->nosorted >= p)
		b->T->nosorted = 0;
	if (b->T->norevsorted >= p)
		b->T->norevsorted = 0;
	b->batCount--;
	if (b->batCount <= 1) {
		/* some trivial properties */
		b->tkey |= 1;
		b->tsorted = b->trevsorted = 1;
		b->T->nosorted = b->T->norevsorted = 0;
		if (b->batCount == 0) {
			b->T->nil = 0;
			b->T->nonil = 1;
		}
	}
	IMPSdestroy(b);
	HASHdestroy(b);
	return GDK_SUCCEED;
}

/* @-  BUN replace
 * The last operation in this context is BUN replace. It assumes that
 * the header denotes a key. The old value association is destroyed
 * (if it exists in the first place) and the new value takes its
 * place.
 *
 * In order to make updates on void columns workable; replaces on them
 * are always done in-place. Performing them without bun-movements
 * greatly simplifies the problem. The 'downside' is that when
 * transaction management has to be performed, replaced values should
 * be saved explicitly.
 */
gdk_return
BUNinplace(BAT *b, BUN p, const void *t, bit force)
{
	BUN last = BUNlast(b) - 1;
	BAT *bm = BBP_cache(-b->batCacheid);
	BATiter bi = bat_iterator(b);
	int tt;
	BUN prv, nxt;

	assert(p >= b->batInserted || force);

	/* uncommitted BUN elements */

	ALIGNinp(b, "BUNinplace", force, GDK_FAIL);	/* zap alignment info */
	if (b->T->nil &&
	    atom_CMP(BUNtail(bi, p), ATOMnilptr(b->ttype), b->ttype) == 0 &&
	    atom_CMP(t, ATOMnilptr(b->ttype), b->ttype) != 0) {
		/* if old value is nil and new value isn't, we're not
		 * sure anymore about the nil property, so we must
		 * clear it */
		b->T->nil = 0;
	}
	HASHremove(b);
	Treplacevalue(b, BUNtloc(bi, p), t);

	tt = b->ttype;
	prv = p > b->batFirst ? p - 1 : BUN_NONE;
	nxt = p < last ? p + 1 : BUN_NONE;

	if (BATtordered(b)) {
		if (prv != BUN_NONE &&
		    ATOMcmp(tt, t, BUNtail(bi, prv)) < 0) {
			b->tsorted = FALSE;
			b->T->nosorted = p;
		} else if (nxt != BUN_NONE &&
			   ATOMcmp(tt, t, BUNtail(bi, nxt)) > 0) {
			b->tsorted = FALSE;
			b->T->nosorted = nxt;
		} else if (b->ttype != TYPE_void && b->tdense) {
			if (prv != BUN_NONE &&
			    1 + * (oid *) BUNtloc(bi, prv) != * (oid *) t) {
				b->tdense = FALSE;
				b->T->nodense = p;
			} else if (nxt != BUN_NONE &&
				   * (oid *) BUNtloc(bi, nxt) != 1 + * (oid *) t) {
				b->tdense = FALSE;
				b->T->nodense = nxt;
			} else if (prv == BUN_NONE &&
				   nxt == BUN_NONE) {
				bm->hseqbase = b->tseqbase = * (oid *) t;
			}
		}
	} else if (b->T->nosorted >= p)
		b->T->nosorted = 0;
	if (BATtrevordered(b)) {
		if (prv != BUN_NONE &&
		    ATOMcmp(tt, t, BUNtail(bi, prv)) > 0) {
			b->trevsorted = FALSE;
			b->T->norevsorted = p;
		} else if (nxt != BUN_NONE &&
			   ATOMcmp(tt, t, BUNtail(bi, nxt)) < 0) {
			b->trevsorted = FALSE;
			b->T->norevsorted = nxt;
		}
	} else if (b->T->norevsorted >= p)
		b->T->norevsorted = 0;
	if (((b->ttype != TYPE_void) & b->tkey & !(b->tkey & BOUND2BTRUE)) && b->batCount > 1) {
		BATkey(bm, FALSE);
	}
	if (b->T->nonil)
		b->T->nonil = t && atom_CMP(t, ATOMnilptr(b->ttype), b->ttype) != 0;
	b->T->heap.dirty = TRUE;
	if (b->T->vheap)
		b->T->vheap->dirty = TRUE;

	return GDK_SUCCEED;

  bunins_failed:
	return GDK_FAIL;
}

/* very much like void_inplace, except this materializes a void tail
 * column if necessarry */
gdk_return
BUNreplace(BAT *b, oid id, const void *t, bit force)
{
	BATcheck(b, "BUNreplace", GDK_FAIL);
	BATcheck(t, "BUNreplace: tail value is nil", GDK_FAIL);

	if (id < b->hseqbase || id >= b->hseqbase + BATcount(b))
		return GDK_SUCCEED;

	if ((b->tkey & BOUND2BTRUE) && BUNfnd(b, t) != BUN_NONE) {
		return GDK_SUCCEED;
	}
	if (b->ttype == TYPE_void) {
		/* no need to materialize if value doesn't change */
		if (b->tseqbase == oid_nil ||
		    b->tseqbase + id - b->hseqbase == *(const oid *) t)
			return GDK_SUCCEED;
		if (BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
	}

	return BUNinplace(b, id - b->hseqbase + BUNfirst(b), t, force);
}

/* very much like BUNreplace, but this doesn't make any changes if the
 * tail column is void */
gdk_return
void_inplace(BAT *b, oid id, const void *val, bit force)
{
	assert(id >= b->hseqbase && id < b->hseqbase + BATcount(b));
	if (id < b->hseqbase || id >= b->hseqbase + BATcount(b)) {
		GDKerror("void_inplace: id out of range\n");
		return GDK_FAIL;
	}
	if ((b->tkey & BOUND2BTRUE) && BUNfnd(b, val) != BUN_NONE)
		return GDK_SUCCEED;
	if (b->ttype == TYPE_void)
		return GDK_SUCCEED;
	return BUNinplace(b, id - b->hseqbase + BUNfirst(b), val, force);
}

BUN
void_replace_bat(BAT *b, BAT *p, BAT *u, bit force)
{
	BUN nr = 0;
	BUN r, s;
	BATiter uii = bat_iterator(p);
	BATiter uvi = bat_iterator(u);

	BATloop(u, r, s) {
		oid updid = *(oid *) BUNtail(uii, r);
		const void *val = BUNtail(uvi, r);

		if (void_inplace(b, updid, val, force) != GDK_SUCCEED)
			return BUN_NONE;
		nr++;
	}
	return nr;
}

/*
 * @- BUN Lookup
 * Location of a BUN using a value should use the available indexes to
 * speed up access. If indexes are lacking then a hash index is
 * constructed under the assumption that 1) multiple access to the BAT
 * can be expected and 2) building the hash is only slightly more
 * expensive than the full linear scan.  BUN_NONE is returned if no
 * such element could be found.  In those cases where the type is
 * known and a hash index is available, one should use the inline
 * functions to speed-up processing.
 */
static BUN
slowfnd(BAT *b, const void *v)
{
	BATiter bi = bat_iterator(b);
	BUN p, q;
	int (*cmp)(const void *, const void *) = ATOMcompare(b->ttype);

	BATloop(b, p, q) {
		if ((*cmp)(v, BUNtail(bi, p)) == 0)
			return p;
	}
	return BUN_NONE;
}

BUN
BUNfnd(BAT *b, const void *v)
{
	BUN r = BUN_NONE;
	BATiter bi;

	BATcheck(b, "BUNfnd", 0);
	if (!v)
		return r;
	if (BATtvoid(b))
		return BUNfndVOID(b, v);
	if (!BATcheckhash(b)) {
		if (BATordered(b) || BATordered_rev(b))
			return SORTfnd(b, v);
	}
	bi = bat_iterator(b);
	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
		HASHfnd_bte(r, bi, v);
		break;
	case TYPE_sht:
		HASHfnd_sht(r, bi, v);
		break;
	case TYPE_int:
		HASHfnd_int(r, bi, v);
		break;
	case TYPE_flt:
		HASHfnd_flt(r, bi, v);
		break;
	case TYPE_dbl:
		HASHfnd_dbl(r, bi, v);
		break;
	case TYPE_lng:
		HASHfnd_lng(r, bi, v);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		HASHfnd_hge(r, bi, v);
		break;
#endif
	case TYPE_str:
		HASHfnd_str(r, bi, v);
		break;
	default:
		HASHfnd(r, bi, v);
	}
	return r;
  hashfnd_failed:
	/* can't build hash table, search the slow way */
	return slowfnd(b, v);
}

/*
 * @+ BAT Property Management
 *
 * The function BATcount returns the number of active elements in a
 * BAT.  Counting is type independent.  It can be implemented quickly,
 * because the system ensures a dense BUN list.
 */
void
BATsetcapacity(BAT *b, BUN cnt)
{
	b->batCapacity = cnt;
	assert(b->batCount <= cnt);
}

void
BATsetcount(BAT *b, BUN cnt)
{
	b->batCount = cnt;
	b->batDirtydesc = TRUE;
	b->H->heap.free = headsize(b, BUNfirst(b) + cnt);
	b->T->heap.free = tailsize(b, BUNfirst(b) + cnt);
	if (b->H->type == TYPE_void && b->T->type == TYPE_void)
		b->batCapacity = cnt;
	if (cnt <= 1) {
		b->hsorted = b->hrevsorted = ATOMlinear(b->htype) != 0;
		b->tsorted = b->trevsorted = ATOMlinear(b->ttype) != 0;
		b->H->nosorted = b->H->norevsorted = 0;
		b->T->nosorted = b->T->norevsorted = 0;
	}
	/* if the BAT was made smaller, we need to zap some values */
	if (b->H->nosorted >= BUNlast(b))
		b->H->nosorted = 0;
	if (b->H->norevsorted >= BUNlast(b))
		b->H->norevsorted = 0;
	if (b->H->nodense >= BUNlast(b))
		b->H->nodense = 0;
	if (b->H->nokey[0] >= BUNlast(b) || b->H->nokey[1] >= BUNlast(b)) {
		b->H->nokey[0] = 0;
		b->H->nokey[1] = 0;
	}
	if (b->T->nosorted >= BUNlast(b))
		b->T->nosorted = 0;
	if (b->T->norevsorted >= BUNlast(b))
		b->T->norevsorted = 0;
	if (b->T->nodense >= BUNlast(b))
		b->T->nodense = 0;
	if (b->T->nokey[0] >= BUNlast(b) || b->T->nokey[1] >= BUNlast(b)) {
		b->T->nokey[0] = 0;
		b->T->nokey[1] = 0;
	}
	if (b->htype == TYPE_void) {
		b->hsorted = 1;
		if (b->hseqbase == oid_nil) { /* unlikely */
			b->hkey = cnt <= 1;
			b->hrevsorted = 1;
			b->H->nil = 1;
			b->H->nonil = 0;
		} else {
			b->hkey = 1;
			b->hrevsorted = cnt <= 1;
			b->H->nil = 0;
			b->H->nonil = 1;
		}
	}
	if (b->ttype == TYPE_void) {
		b->tsorted = 1;
		if (b->tseqbase == oid_nil) {
			b->tkey = cnt <= 1;
			b->trevsorted = 1;
			b->T->nil = 1;
			b->T->nonil = 0;
		} else {
			b->tkey = 1;
			b->trevsorted = cnt <= 1;
			b->T->nil = 0;
			b->T->nonil = 1;
		}
	}
	assert(b->batCapacity >= cnt);
}

size_t
BATvmsize(BAT *b, int dirty)
{
	BATcheck(b, "BATvmsize", 0);
	if (b->batDirty || (b->batPersistence != TRANSIENT && !b->batCopiedtodisk))
		dirty = 0;
	return (!dirty || b->H->heap.dirty ? HEAPvmsize(&b->H->heap) : 0) +
		(!dirty || b->T->heap.dirty ? HEAPvmsize(&b->T->heap) : 0) +
		((!dirty || b->H->heap.dirty) && b->H->hash && b->H->hash != (Hash *) 1 ? HEAPvmsize(b->H->hash->heap) : 0) +
		((!dirty || b->T->heap.dirty) && b->T->hash && b->T->hash != (Hash *) 1 ? HEAPvmsize(b->T->hash->heap) : 0) +
		(b->H->vheap && (!dirty || b->H->vheap->dirty) ? HEAPvmsize(b->H->vheap) : 0) +
		(b->T->vheap && (!dirty || b->T->vheap->dirty) ? HEAPvmsize(b->T->vheap) : 0);
}

size_t
BATmemsize(BAT *b, int dirty)
{
	BATcheck(b, "BATmemsize", 0);
	if (b->batDirty ||
	    (b->batPersistence != TRANSIENT && !b->batCopiedtodisk))
		dirty = 0;
	return (!dirty || b->batDirtydesc ? sizeof(BATstore) : 0) +
		(!dirty || b->H->heap.dirty ? HEAPmemsize(&b->H->heap) : 0) +
		(!dirty || b->T->heap.dirty ? HEAPmemsize(&b->T->heap) : 0) +
		((!dirty || b->H->heap.dirty) && b->H->hash && b->H->hash != (Hash *) 1 ? HEAPmemsize(b->H->hash->heap) : 0) +
		((!dirty || b->T->heap.dirty) && b->T->hash && b->T->hash != (Hash *) 1 ? HEAPmemsize(b->T->hash->heap) : 0) +
		(b->H->vheap && (!dirty || b->H->vheap->dirty) ? HEAPmemsize(b->H->vheap) : 0) +
		(b->T->vheap && (!dirty || b->T->vheap->dirty) ? HEAPmemsize(b->T->vheap) : 0);
}

/*
 * The key and name properties can be changed at any time.  Keyed
 * dimensions are automatically supported by an auxiliary hash-based
 * access structure to speed up searching. Turning off the key
 * integrity property does not cause the index to disappear. It can
 * still be used to speed-up retrieval. The routine BATkey sets the
 * key property of the association head.
 */
gdk_return
BATkey(BAT *b, int flag)
{
	bat parent;

	BATcheck(b, "BATkey", GDK_FAIL);
	parent = VIEWparentcol(b);
	if (b->htype == TYPE_void) {
		if (b->hseqbase == oid_nil && flag == BOUND2BTRUE) {
			GDKerror("BATkey: nil-column cannot be kept unique.\n");
			return GDK_FAIL;
		}
		if (b->hseqbase != oid_nil && flag == FALSE) {
			GDKerror("BATkey: dense column must be unique.\n");
			return GDK_FAIL;
		}
		if (b->hseqbase == oid_nil && flag == TRUE && b->batCount > 1) {
			GDKerror("BATkey: void column cannot be unique.\n");
			return GDK_FAIL;
		}
	}
	if (flag)
		flag |= (1 | b->hkey);
	if (b->hkey != flag)
		b->batDirtydesc = TRUE;
	b->hkey = flag;
	if (!flag)
		b->hdense = 0;
	if (flag && parent && ALIGNsynced(b, BBP_cache(parent)))
		return BATkey(BBP_cache(parent), TRUE);
	return GDK_SUCCEED;
}


void
BATseqbase(BAT *b, oid o)
{
	if (b == NULL)
		return;
	assert(o <= oid_nil);
	if (ATOMtype(b->htype) == TYPE_oid) {
		if (b->hseqbase != o) {
			b->batDirtydesc = TRUE;
			/* zap alignment if column is changed by new
			 * seqbase */
			if (b->htype == TYPE_void)
				b->halign = 0;
		}
		b->hseqbase = o;
		if (b->htype == TYPE_oid && o == oid_nil) {
			b->hdense = 0;
			b->H->nodense = BUNfirst(b);
		}

		/* adapt keyness */
		if (BAThvoid(b)) {
			if (o == oid_nil) {
				b->hkey = b->batCount <= 1;
				b->H->nonil = b->batCount == 0;
				b->H->nil = b->batCount > 0;
				b->hsorted = b->hrevsorted = 1;
				b->H->nosorted = b->H->norevsorted = 0;
				if (!b->hkey) {
					b->H->nokey[0] = BUNfirst(b);
					b->H->nokey[1] = BUNfirst(b) + 1;
				} else {
					b->H->nokey[0] = b->H->nokey[1] = 0;
				}
			} else {
				if (!b->hkey) {
					b->hkey = TRUE;
					b->H->nokey[0] = b->H->nokey[1] = 0;
				}
				b->H->nonil = 1;
				b->H->nil = 0;
				b->hsorted = 1;
				b->hrevsorted = b->batCount <= 1;
				if (!b->hrevsorted)
					b->H->norevsorted = BUNfirst(b) + 1;
			}
		}
	}
}

/*
 * BATs have a logical name that is independent of their location in
 * the file system (this depends on batCacheid).  The dimensions of
 * the BAT can be given a separate name.  It helps front-ends in
 * identifying the column of interest.  The new name should be
 * recognizable as an identifier.  Otherwise interaction through the
 * front-ends becomes complicated.
 */
int
BATname(BAT *b, const char *nme)
{
	BATcheck(b, "BATname", 0);
	return BBPrename(b->batCacheid, nme);
}

str
BATrename(BAT *b, const char *nme)
{
	int ret;

	BATcheck(b, "BATrename", NULL);
	ret = BATname(b, nme);
	if (ret == 1) {
		GDKerror("BATrename: identifier expected: %s\n", nme);
	} else if (ret == BBPRENAME_ALREADY) {
		GDKerror("BATrename: name is in use: '%s'.\n", nme);
	} else if (ret == BBPRENAME_ILLEGAL) {
		GDKerror("BATrename: illegal temporary name: '%s'\n", nme);
	} else if (ret == BBPRENAME_LONG) {
		GDKerror("BATrename: name too long: '%s'\n", nme);
	}
	return BBPname(b->batCacheid);
}


void
BATroles(BAT *b, const char *hnme, const char *tnme)
{
	if (b == NULL)
		return;
	if (b->hident && !default_ident(b->hident))
		GDKfree(b->hident);
	if (hnme)
		b->hident = GDKstrdup(hnme);
	else
		b->hident = BATstring_h;
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	if (tnme)
		b->tident = GDKstrdup(tnme);
	else
		b->tident = BATstring_t;
}

/*
 * @- Change the BAT access permissions (read, append, write)
 * Regrettably, BAT access-permissions, persistent status and memory
 * map modes, interact in ways that makes one's brain sizzle. This
 * makes BATsetaccess and TMcommit (where a change in BAT persistence
 * mode is made permanent) points in which the memory map status of
 * bats needs to be carefully re-assessed and ensured.
 *
 * Another complication is the fact that during commit, concurrent
 * users may access the heaps, such that the simple solution
 * unmap;re-map is out of the question.
 * Even worse, it is not possible to even rename an open mmap file in
 * Windows. For this purpose, we dropped the old .priv scheme, which
 * relied on file moves. Now, the file that is opened with mmap is
 * always the X file, in case of newstorage=STORE_PRIV, we save in a
 * new file X.new
 *
 * we must consider the following dimensions:
 *
 * persistence:
 *     not simply the current persistence mode but whether the bat *was*
 *     present at the last commit point (BBP status & BBPEXISTING).
 *     The crucial issue is namely whether we must guarantee recovery
 *     to a previous sane state.
 *
 * access:
 *     whether the BAT is BAT_READ or BAT_WRITE. Note that BAT_APPEND
 *     is usually the same as BAT_READ (as our concern are only data pages
 *     that already existed at the last commit).
 *
 * storage:
 *     the current way the heap file X is memory-mapped;
 *     STORE_MMAP uses direct mapping (so dirty pages may be flushed
 *     at any time to disk), STORE_PRIV uses copy-on-write.
 *
 * newstorage:
 *     the current save-regime. STORE_MMAP calls msync() on the heap X,
 *     whereas STORE_PRIV writes the *entire* heap in a file: X.new
 *     If a BAT is loaded from disk, the field newstorage is used
 *     to set storage as well (so before change-access and commit-
 *     persistence mayhem, we always have newstorage=storage).
 *
 * change-access:
 *     what happens if the bat-access mode is changed from
 *     BAT_READ into BAT_WRITE (or vice versa).
 *
 * commit-persistence:
 *     what happens during commit if the bat-persistence mode was
 *     changed (from TRANSIENT into PERSISTENT, or vice versa).
 *
 * this is the scheme:
 *
 *  persistence access    newstorage storage    change-access commit-persistence
 *  =========== ========= ========== ========== ============= ==================
 * 0 transient  BAT_READ  STORE_MMAP STORE_MMAP =>2           =>4
 * 1 transient  BAT_READ  STORE_PRIV STORE_PRIV =>3           =>5
 * 2 transient  BAT_WRITE STORE_MMAP STORE_MMAP =>0           =>6+
 * 3 transient  BAT_WRITE STORE_PRIV STORE_PRIV =>1           =>7
 * 4 persistent BAT_READ  STORE_MMAP STORE_MMAP =>6+          =>0
 * 5 persistent BAT_READ  STORE_PRIV STORE_PRIV =>7           =>1
 * 6 persistent BAT_WRITE STORE_PRIV STORE_MMAP del X.new=>4+ del X.new;=>2+
 * 7 persistent BAT_WRITE STORE_PRIV STORE_PRIV =>5           =>3
 *
 * exception states:
 * a transient  BAT_READ  STORE_PRIV STORE_MMAP =>b           =>c
 * b transient  BAT_WRITE STORE_PRIV STORE_MMAP =>a           =>6
 * c persistent BAT_READ  STORE_PRIV STORE_MMAP =>6           =>a
 *
 * (+) indicates that we must ensure that the heap gets saved in its new mode
 *
 * Note that we now allow a heap with save-regime STORE_PRIV that was
 * actually mapped STORE_MMAP. In effect, the potential corruption of
 * the X file is compensated by writing out full X.new files that take
 * precedence.  When transitioning out of this state towards one with
 * both storage regime and OS as STORE_MMAP we need to move the X.new
 * files into the backup directory. Then msync the X file and (on
 * success) remove the X.new; see backup_new().
 *
 * Exception states are only reachable if the commit fails and those
 * new persistent bats have already been processed (but never become
 * part of a committed state). In that case a transition 2=>6 may end
 * up 2=>b.  Exception states a and c are reachable from b.
 *
 * Errors in HEAPchangeaccess() can be handled atomically inside the
 * routine.  The work on changing mmap modes HEAPcommitpersistence()
 * is done during the BBPsync() for all bats that are newly persistent
 * (BBPNEW). After the TMcommit(), it is done for those bats that are
 * no longer persistent after the commit (BBPDELETED), only if it
 * succeeds.  Such transient bats cannot be processed before the
 * commit, because the commit may fail and then the more unsafe
 * transient mmap modes would be present on a persistent bat.
 *
 * See dirty_bat() in BBPsync() -- gdk_bbp.mx and epilogue() in
 * gdk_tm.mx
 *
 * Including the exception states, we have 11 of the 16
 * combinations. As for the 5 avoided states, all four
 * (persistence,access) states with (STORE_MMAP,STORE_PRIV) are
 * omitted (this would amount to an msync() save regime on a
 * copy-on-write heap -- which does not work). The remaining avoided
 * state is the patently unsafe
 * (persistent,BAT_WRITE,STORE_MMAP,STORE_MMAP).
 *
 * Note that after a server restart exception states are gone, as on
 * BAT loads the saved descriptor is inspected again (which will
 * reproduce the state at the last succeeded commit).
 *
 * To avoid exception states, a TMsubcommit protocol would need to be
 * used which is too heavy for BATsetaccess().
 *
 * Note that this code is not about making heaps mmap-ed in the first
 * place.  It is just about determining which flavor of mmap should be
 * used. The MAL user is oblivious of such details.
 */

/* rather than deleting X.new, we comply with the commit protocol and
 * move it to backup storage */
static gdk_return
backup_new(Heap *hp, int lockbat)
{
	int batret, bakret, xx, ret = 0;
	char *batpath, *bakpath;
	struct stat st;

	/* file actions here interact with the global commits */
	for (xx = 0; xx <= lockbat; xx++)
		MT_lock_set(&GDKtrimLock(xx));

	/* check for an existing X.new in BATDIR, BAKDIR and SUBDIR */
	batpath = GDKfilepath(hp->farmid, BATDIR, hp->filename, ".new");
	bakpath = GDKfilepath(hp->farmid, BAKDIR, hp->filename, ".new");
	batret = stat(batpath, &st);
	bakret = stat(bakpath, &st);

	if (batret == 0 && bakret) {
		/* no backup yet, so move the existing X.new there out
		 * of the way */
		if ((ret = rename(batpath, bakpath)) < 0)
			GDKsyserror("backup_new: rename %s to %s failed\n",
				    batpath, bakpath);
		IODEBUG fprintf(stderr, "#rename(%s,%s) = %d\n", batpath, bakpath, ret);
	} else if (batret == 0) {
		/* there is a backup already; just remove the X.new */
		if ((ret = unlink(batpath)) < 0)
			GDKsyserror("backup_new: unlink %s failed\n", batpath);
		IODEBUG fprintf(stderr, "#unlink(%s) = %d\n", batpath, ret);
	}
	GDKfree(batpath);
	GDKfree(bakpath);
	for (xx = lockbat; xx >= 0; xx--)
		MT_lock_unset(&GDKtrimLock(xx));
	return ret ? GDK_FAIL : GDK_SUCCEED;
}

#define ACCESSMODE(wr,rd) ((wr)?BAT_WRITE:(rd)?BAT_READ:-1)

/* transition heap from readonly to writable */
static storage_t
HEAPchangeaccess(Heap *hp, int dstmode, int existing)
{
	if (hp->base == NULL || hp->newstorage == STORE_MEM || !existing || dstmode == -1)
		return hp->newstorage;	/* 0<=>2,1<=>3,a<=>b */

	if (dstmode == BAT_WRITE) {
		if (hp->storage != STORE_PRIV)
			hp->dirty = 1;	/* exception c does not make it dirty */
		return STORE_PRIV;	/* 4=>6,5=>7,c=>6 persistent BAT_WRITE needs STORE_PRIV */
	}
	if (hp->storage == STORE_MMAP) {	/* 6=>4 */
		hp->dirty = 1;
		return backup_new(hp, BBP_THREADMASK) != GDK_SUCCEED ? STORE_INVALID : STORE_MMAP;	/* only called for existing bats */
	}
	return hp->storage;	/* 7=>5 */
}

/* heap changes persistence mode (at commit point) */
static storage_t
HEAPcommitpersistence(Heap *hp, int writable, int existing)
{
	if (existing) {		/* existing, ie will become transient */
		if (hp->storage == STORE_MMAP && hp->newstorage == STORE_PRIV && writable) {	/* 6=>2 */
			hp->dirty = 1;
			return backup_new(hp, -1) != GDK_SUCCEED ? STORE_INVALID : STORE_MMAP;	/* only called for existing bats */
		}
		return hp->newstorage;	/* 4=>0,5=>1,7=>3,c=>a no change */
	}
	/* !existing, ie will become persistent */
	if (hp->newstorage == STORE_MEM)
		return hp->newstorage;
	if (hp->newstorage == STORE_MMAP && !writable)
		return STORE_MMAP;	/* 0=>4 STORE_MMAP */

	if (hp->newstorage == STORE_MMAP)
		hp->dirty = 1;	/* 2=>6 */
	return STORE_PRIV;	/* 1=>5,2=>6,3=>7,a=>c,b=>6 states */
}


/* change the heap modes at a commit */
gdk_return
BATcheckmodes(BAT *b, int existing)
{
	int wr = (b->batRestricted == BAT_WRITE);
	storage_t m0 = STORE_MEM, m1 = STORE_MEM, m2 = STORE_MEM, m3 = STORE_MEM;
	int dirty = 0;

	BATcheck(b, "BATcheckmodes", GDK_FAIL);

	if (b->htype) {
		m0 = HEAPcommitpersistence(&b->H->heap, wr, existing);
		dirty |= (b->H->heap.newstorage != m0);
	}

	if (b->ttype) {
		m1 = HEAPcommitpersistence(&b->T->heap, wr, existing);
		dirty |= (b->T->heap.newstorage != m1);
	}

	if (b->H->vheap) {
		int ha = (b->batRestricted == BAT_APPEND) && ATOMappendpriv(b->htype, b->H->vheap);
		m2 = HEAPcommitpersistence(b->H->vheap, wr || ha, existing);
		dirty |= (b->H->vheap->newstorage != m2);
	}
	if (b->T->vheap) {
		int ta = (b->batRestricted == BAT_APPEND) && ATOMappendpriv(b->ttype, b->T->vheap);
		m3 = HEAPcommitpersistence(b->T->vheap, wr || ta, existing);
		dirty |= (b->T->vheap->newstorage != m3);
	}
	if (m0 == STORE_INVALID || m1 == STORE_INVALID ||
	    m2 == STORE_INVALID || m3 == STORE_INVALID)
		return GDK_FAIL;

	if (dirty) {
		b->batDirtydesc = 1;
		b->H->heap.newstorage = m0;
		b->T->heap.newstorage = m1;
		if (b->H->vheap)
			b->H->vheap->newstorage = m2;
		if (b->T->vheap)
			b->T->vheap->newstorage = m3;
	}
	return GDK_SUCCEED;
}

gdk_return
BATsetaccess(BAT *b, int newmode)
{
	int bakmode, bakdirty;
	BATcheck(b, "BATsetaccess", GDK_FAIL);
	if (isVIEW(b) && newmode != BAT_READ) {
		if (VIEWreset(b) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	bakmode = b->batRestricted;
	bakdirty = b->batDirtydesc;
	if (bakmode != newmode || (b->batSharecnt && newmode != BAT_READ)) {
		int existing = BBP_status(b->batCacheid) & BBPEXISTING;
		int wr = (newmode == BAT_WRITE);
		int rd = (bakmode == BAT_WRITE);
		storage_t m0, m1, m2 = STORE_MEM, m3 = STORE_MEM;
		storage_t b0, b1, b2 = STORE_MEM, b3 = STORE_MEM;

		if (b->batSharecnt && newmode != BAT_READ) {
			BATDEBUG THRprintf(GDKout, "#BATsetaccess: %s has %d views; try creating a copy\n", BATgetId(b), b->batSharecnt);
			GDKerror("BATsetaccess: %s has %d views\n",
				 BATgetId(b), b->batSharecnt);
			return GDK_FAIL;
		}

		b0 = b->H->heap.newstorage;
		m0 = HEAPchangeaccess(&b->H->heap, ACCESSMODE(wr, rd), existing);
		b1 = b->T->heap.newstorage;
		m1 = HEAPchangeaccess(&b->T->heap, ACCESSMODE(wr, rd), existing);
		if (b->H->vheap) {
			int ha = (newmode == BAT_APPEND && ATOMappendpriv(b->htype, b->H->vheap));
			b2 = b->H->vheap->newstorage;
			m2 = HEAPchangeaccess(b->H->vheap, ACCESSMODE(wr && ha, rd && ha), existing);
		}
		if (b->T->vheap) {
			int ta = (newmode == BAT_APPEND && ATOMappendpriv(b->ttype, b->T->vheap));
			b3 = b->T->vheap->newstorage;
			m3 = HEAPchangeaccess(b->T->vheap, ACCESSMODE(wr && ta, rd && ta), existing);
		}
		if (m0 == STORE_INVALID || m1 == STORE_INVALID ||
		    m2 == STORE_INVALID || m3 == STORE_INVALID)
			return GDK_FAIL;

		/* set new access mode and mmap modes */
		b->batRestricted = newmode;
		b->batDirtydesc = TRUE;
		b->H->heap.newstorage = m0;
		b->T->heap.newstorage = m1;
		if (b->H->vheap)
			b->H->vheap->newstorage = m2;
		if (b->T->vheap)
			b->T->vheap->newstorage = m3;

		if (existing && BBPsave(b) != GDK_SUCCEED) {
			/* roll back all changes */
			b->batRestricted = bakmode;
			b->batDirtydesc = bakdirty;
			b->H->heap.newstorage = b0;
			b->T->heap.newstorage = b1;
			if (b->H->vheap)
				b->H->vheap->newstorage = b2;
			if (b->T->vheap)
				b->T->vheap->newstorage = b3;
			return GDK_FAIL;
		}
	}
	return GDK_SUCCEED;
}

int
BATgetaccess(BAT *b)
{
	BATcheck(b, "BATgetaccess", 0);
	return b->batRestricted;
}

/*
 * @- change BAT persistency (persistent,session,transient)
 * In the past, we prevented BATS with certain types from being saved at all:
 * - BATs of BATs, as having recursive bats creates cascading
 *   complexities in commits/aborts.
 * - any atom with refcounts, as the BBP has no overview of such
 *   user-defined refcounts.
 * - pointer types, as the values they point to are bound to be transient.
 *
 * However, nowadays we do allow such saves, as the BBP swapping
 * mechanism was altered to be able to save transient bats temporarily
 * to disk in order to make room.  Thus, we must be able to save any
 * transient BAT to disk.
 *
 * What we don't allow is to make such bats persistent.
 *
 * Although the persistent state does influence the allowed mmap
 * modes, this only goes for the *real* committed persistent
 * state. Making the bat persistent with BATmode does not matter for
 * the heap modes until the commit point is reached. So we do not need
 * to do anything with heap modes yet at this point.
 */
#define check_type(tp)							\
	do {								\
		if (ATOMisdescendant((tp), TYPE_ptr) ||			\
		    BATatoms[tp].atomUnfix ||				\
		    BATatoms[tp].atomFix) {				\
			GDKerror("BATmode: %s type implies that %s[%s,%s] " \
				 "cannot be made persistent.\n",	\
				 ATOMname(tp), BATgetId(b),		\
				 ATOMname(b->htype), ATOMname(b->ttype)); \
			return GDK_FAIL;				\
		}							\
	} while (0)

gdk_return
BATmode(BAT *b, int mode)
{
	BATcheck(b, "BATmode", GDK_FAIL);

	/* can only make a bat PERSISTENT if its role is already
	 * PERSISTENT */
	assert(mode == PERSISTENT || mode == TRANSIENT);
	assert(mode == TRANSIENT || b->batRole == PERSISTENT);

	if (b->batRole == TRANSIENT && mode != TRANSIENT) {
		GDKerror("cannot change mode of BAT in TRANSIENT farm.\n");
		return GDK_FAIL;
	}

	if (mode != b->batPersistence) {
		bat bid = abs(b->batCacheid);

		if (mode == PERSISTENT) {
			check_type(b->htype);
			check_type(b->ttype);
		}
		BBPdirty(1);

		if (mode == PERSISTENT && isVIEW(b)) {
			if (VIEWreset(b) != GDK_SUCCEED) {
				return GDK_FAIL;
			}
		}
		/* persistent BATs get a logical reference */
		if (mode == PERSISTENT) {
			BBPincref(bid, TRUE);
		} else if (b->batPersistence == PERSISTENT) {
			BBPdecref(bid, TRUE);
		}
		MT_lock_set(&GDKswapLock(bid));
		if (mode == PERSISTENT) {
			if (!(BBP_status(bid) & BBPDELETED))
				BBP_status_on(bid, BBPNEW, "BATmode");
			else
				BBP_status_on(bid, BBPEXISTING, "BATmode");
			BBP_status_off(bid, BBPDELETED, "BATmode");
		} else if (b->batPersistence == PERSISTENT) {
			if (!(BBP_status(bid) & BBPNEW))
				BBP_status_on(bid, BBPDELETED, "BATmode");
			BBP_status_off(bid, BBPPERSISTENT, "BATmode");
		}
		/* session bats or persistent bats that did not
		 * witness a commit yet may have been saved */
		if (b->batCopiedtodisk) {
			if (mode == PERSISTENT) {
				BBP_status_off(bid, BBPTMP, "BATmode");
			} else {
				/* TMcommit must remove it to
				 * guarantee free space */
				BBP_status_on(bid, BBPTMP, "BATmode");
			}
		}
		b->batPersistence = mode;
		MT_lock_unset(&GDKswapLock(bid));
	}
	return GDK_SUCCEED;
}

/* BATassertProps checks whether properties are set correctly.  Under
 * no circumstances will it change any properties.  Note that the
 * "nil" property is not actually used anywhere, but it is checked. */

#ifdef NDEBUG
/* assertions are disabled, turn failing tests into a message */
#undef assert
#define assert(test)	((void) ((test) || fprintf(stderr, "!WARNING: %s:%d: assertion `%s' failed\n", __FILE__, __LINE__, #test)))
#endif

static void
BATassertTailProps(BAT *b)
{
	BATiter bi = bat_iterator(b);
	BUN p, q;
	int (*cmpf)(const void *, const void *);
	int cmp;
	const void *prev = NULL, *valp, *nilp;
	int seennil = 0;

	assert(b != NULL);
	assert(b->ttype >= TYPE_void);
	assert(b->ttype < GDKatomcnt);
	assert(b->ttype != TYPE_bat);
	/* if BOUND2BTRUE is set, then so must the low order bit */
	assert(!(b->tkey & BOUND2BTRUE) || (b->tkey & 1)); /* tkey != 2 */
	assert(isVIEW(b) ||
	       b->ttype == TYPE_void ||
	       BBPfarms[b->T->heap.farmid].roles & (1 << b->batRole));
	assert(isVIEW(b) ||
	       b->T->vheap == NULL ||
	       (BBPfarms[b->T->vheap->farmid].roles & (1 << b->batRole)));

	cmpf = ATOMcompare(b->ttype);
	nilp = ATOMnilptr(b->ttype);
	p = BUNfirst(b);
	q = BUNlast(b);

	assert(b->T->heap.free >= tailsize(b, BUNlast(b)));
	if (b->ttype != TYPE_void) {
		assert(b->batCount <= b->batCapacity);
		assert(b->T->heap.size >= b->T->heap.free);
		assert(b->T->heap.size >> b->T->shift >= b->batCapacity);
	}

	/* void and str imply varsized */
	if (b->ttype == TYPE_void ||
	    ATOMstorage(b->ttype) == TYPE_str)
		assert(b->tvarsized);
	/* other "known" types are not varsized */
	if (ATOMstorage(b->ttype) > TYPE_void &&
	    ATOMstorage(b->ttype) < TYPE_str)
		assert(!b->tvarsized);
	/* shift and width have a particular relationship */
	assert(b->T->shift >= 0);
	if (b->tdense)
		assert(b->ttype == TYPE_oid || b->ttype == TYPE_void);
	/* a column cannot both have and not have NILs */
	assert(!b->T->nil || !b->T->nonil);
	assert(b->tseqbase <= oid_nil);
	if (b->ttype == TYPE_void) {
		assert(b->T->shift == 0);
		assert(b->T->width == 0);
		if (b->tseqbase == oid_nil) {
			assert(BATcount(b) == 0 || !b->T->nonil);
			assert(BATcount(b) <= 1 || !b->tkey);
			/* assert(!b->tdense); */
			assert(b->tsorted);
			assert(b->trevsorted);
		} else {
			assert(BATcount(b) == 0 || !b->T->nil);
			assert(BATcount(b) <= 1 || !b->trevsorted);
			/* assert(b->tdense); */
			assert(b->tkey);
			assert(b->tsorted);
		}
		return;
	}
	if (ATOMstorage(b->ttype) == TYPE_str)
		assert(b->T->width >= 1 && b->T->width <= ATOMsize(b->ttype));
	else
		assert(b->T->width == ATOMsize(b->ttype));
	assert(1 << b->T->shift == b->T->width);
	if (b->ttype == TYPE_oid && b->tdense) {
		assert(b->tsorted);
		assert(b->tseqbase != oid_nil);
		if (b->batCount > 0) {
			assert(b->tseqbase != oid_nil);
			assert(* (oid *) BUNtail(bi, p) == b->tseqbase);
		}
	}
	/* only linear atoms can be sorted */
	assert(!b->tsorted || ATOMlinear(b->ttype));
	assert(!b->trevsorted || ATOMlinear(b->ttype));
	if (ATOMlinear(b->ttype)) {
		assert(b->T->nosorted == 0 ||
		       (b->T->nosorted > b->batFirst &&
			b->T->nosorted < b->batFirst + b->batCount));
		assert(!b->tsorted || b->T->nosorted == 0);
		if (!b->tsorted &&
		    b->T->nosorted > b->batFirst &&
		    b->T->nosorted < b->batFirst + b->batCount)
			assert(cmpf(BUNtail(bi, b->T->nosorted - 1),
				    BUNtail(bi, b->T->nosorted)) > 0);
		assert(b->T->norevsorted == 0 ||
		       (b->T->norevsorted > b->batFirst &&
			b->T->norevsorted < b->batFirst + b->batCount));
		assert(!b->trevsorted || b->T->norevsorted == 0);
		if (!b->trevsorted &&
		    b->T->norevsorted > b->batFirst &&
		    b->T->norevsorted < b->batFirst + b->batCount)
			assert(cmpf(BUNtail(bi, b->T->norevsorted - 1),
				    BUNtail(bi, b->T->norevsorted)) < 0);
	}
	/* var heaps must have sane sizes */
	assert(b->T->vheap == NULL || b->T->vheap->free <= b->T->vheap->size);

	if (!b->tkey && !b->tsorted && !b->trevsorted &&
	    !b->T->nonil && !b->T->nil) {
		/* nothing more to prove */
		return;
	}

	PROPDEBUG { /* only do a scan if property checking is requested */
		if (b->tsorted || b->trevsorted || !b->tkey) {
			/* if sorted (either way), or we don't have to
			 * prove uniqueness, we can do a simple
			 * scan */
			/* only call compare function if we have to */
			int cmpprv = b->tsorted | b->trevsorted | b->tkey;
			int cmpnil = b->T->nonil | b->T->nil;

			BATloop(b, p, q) {
				valp = BUNtail(bi, p);
				if (prev && cmpprv) {
					cmp = cmpf(prev, valp);
					assert(!b->tsorted || cmp <= 0);
					assert(!b->trevsorted || cmp >= 0);
					assert(!b->tkey || cmp != 0);
					assert(!b->tdense || * (oid *) prev + 1 == * (oid *) valp);
				}
				if (cmpnil) {
					cmp = cmpf(valp, nilp);
					assert(!b->T->nonil || cmp != 0);
					if (cmp == 0) {
						/* we found a nil:
						 * we're done checking
						 * for them */
						seennil = 1;
						cmpnil = 0;
						if (!cmpprv) {
							/* we were
							 * only
							 * checking
							 * for nils,
							 * so nothing
							 * more to
							 * do */
							break;
						}
					}
				}
				prev = valp;
			}
		} else {	/* b->tkey && !b->tsorted && !b->trevsorted */
			/* we need to check for uniqueness the hard
			 * way (i.e. using a hash table) */
			const char *nme = BBP_physical(b->batCacheid);
			char *ext;
			size_t nmelen = strlen(nme);
			Heap *hp;
			Hash *hs = NULL;
			BUN mask;

			if ((hp = GDKzalloc(sizeof(Heap))) == NULL ||
			    (hp->filename = GDKmalloc(nmelen + 30)) == NULL) {
				if (hp)
					GDKfree(hp);
				fprintf(stderr,
					"#BATassertProps: cannot allocate "
					"hash table\n");
				goto abort_check;
			}
			snprintf(hp->filename, nmelen + 30,
				 "%s.hash" SZFMT, nme, MT_getpid());
			ext = GDKstrdup(hp->filename + nmelen + 1);
			if (ATOMsize(b->ttype) == 1)
				mask = 1 << 8;
			else if (ATOMsize(b->ttype) == 2)
				mask = 1 << 16;
			else
				mask = HASHmask(b->batCount);
			if ((hp->farmid = BBPselectfarm(TRANSIENT, b->ttype,
							hashheap)) < 0 ||
			    (hs = HASHnew(hp, b->ttype, BUNlast(b),
					  mask, BUN_NONE)) == NULL) {
				GDKfree(ext);
				GDKfree(hp->filename);
				GDKfree(hp);
				fprintf(stderr,
					"#BATassertProps: cannot allocate "
					"hash table\n");
				goto abort_check;
			}
			BATloop(b, p, q) {
				BUN hb;
				BUN prb;
				valp = BUNtail(bi, p);
				prb = HASHprobe(hs, valp);
				for (hb = HASHget(hs,prb);
				     hb != HASHnil(hs);
				     hb = HASHgetlink(hs,hb))
					if (cmpf(valp, BUNtail(bi, hb)) == 0)
						assert(!b->tkey);
				HASHputlink(hs,p, HASHget(hs,prb));
				HASHput(hs,prb,p);
				cmp = cmpf(valp, nilp);
				assert(!b->T->nonil || cmp != 0);
				if (cmp == 0)
					seennil = 1;
			}
			HEAPfree(hp, 1);
			GDKfree(hp);
			GDKfree(hs);
			GDKfree(ext);
		}
	  abort_check:
		assert(!b->T->nil || seennil);
	}
}

/* Assert that properties are set correctly.
 *
 * A BAT can have a bunch of properties set.  Mostly, the property
 * bits are set if we *know* the property holds, and not set if we
 * don't know whether the property holds (or if we know it doesn't
 * hold).  All properties are per column.
 *
 * The properties currently maintained are:
 *
 * dense	Only valid for TYPE_oid columns: each value in the
 *		column is exactly one more than the previous value.
 *		This implies sorted, key, nonil.
 * nil		There is at least one NIL value in the column.
 * nonil	There are no NIL values in the column.
 * key		All values in the column are distinct.
 * sorted	The column is sorted (ascending).  If also revsorted,
 *		then all values are equal.
 * revsorted	The column is reversely sorted (descending).  If
 *		also sorted, then all values are equal.
 *
 * The "key" property consists of two bits.  The lower bit, when set,
 * indicates that all values in the column are distinct.  The upper
 * bit, when set, indicates that all values must be distinct
 * (BOUND2BTRUE).
 *
 * Note that the functions BATseqbase and BATkey also set more
 * properties than you might suspect.  When setting properties on a
 * newly created and filled BAT, you may want to first make sure the
 * batCount is set correctly (e.g. by calling BATsetcount), then use
 * BATseqbase and BATkey, and finally set the other properties.
 */

void
BATassertProps(BAT *b)
{
	BAT *bm;
	int bbpstatus;

	/* general BAT sanity */
	assert(b != NULL);
	assert(b->batCacheid > 0);
	assert(VIEWhparent(b) == 0);
	assert(VIEWvhparent(b) == 0);
	bm = BATmirror(b);
	assert(bm != NULL);
	assert(b->H == bm->T);
	assert(b->T == bm->H);
	assert(b->S == bm->S);
	assert(b->batDeleted < BUN_MAX);
	assert(b->batFirst >= b->batDeleted);
	assert(b->batInserted >= b->batFirst);
	assert(b->batFirst + b->batCount >= b->batInserted);

	/* headless */
	assert(b->batFirst == 0);
	assert(b->htype == TYPE_void);
	assert(b->hseqbase != oid_nil);

	bbpstatus = BBP_status(b->batCacheid);
	/* only at most one of BBPDELETED, BBPEXISTING, BBPNEW may be set */
	assert(((bbpstatus & BBPDELETED) != 0) +
	       ((bbpstatus & BBPEXISTING) != 0) +
	       ((bbpstatus & BBPNEW) != 0) <= 1);

	BATassertTailProps(b);
	if (b->H != bm->H)
		BATassertTailProps(bm);
}

/* derive properties that can be derived with a simple scan: sorted,
 * revsorted, dense; if expensive is set, we also check the key
 * property
 * note that we don't check nil/nonil: we usually know pretty quickly
 * that a column is not sorted, but we usually need a full scan for
 * nonil.
 */
void
BATderiveTailProps(BAT *b, int expensive)
{
	BATiter bi = bat_iterator(b);
	BUN p, q;
	int (*cmpf)(const void *, const void *);
	int cmp;
	const void *prev = NULL, *valp, *nilp;
	int sorted, revsorted, key, dense;
	const char *nme = NULL;
	char *ext = NULL;
	size_t nmelen;
	Heap *hp = NULL;
	Hash *hs = NULL;
	BUN hb, prb;
	oid sqbs = oid_nil;

	if (b == NULL) {
		assert(0);
		return;
	}
	assert((b->tkey & BOUND2BTRUE) == 0);
	COLsettrivprop(b, b->T);
	cmpf = ATOMcompare(b->ttype);
	nilp = ATOMnilptr(b->ttype);
	b->batDirtydesc = 1;	/* we will be changing things */
	if (b->ttype == TYPE_void || b->batCount <= 1) {
		/* COLsettrivprop has already taken care of all
		 * properties except for (no)nil if count == 1 */
		if (b->batCount == 1) {
			valp = BUNtail(bi, BUNfirst(b));
			if (cmpf(valp, nilp) == 0) {
				b->T->nil = 1;
				b->T->nonil = 0;
			} else {
				b->T->nil = 0;
				b->T->nonil = 1;
			}
		}
		return;
	}
	/* tentatively set until proven otherwise */
	key = 1;
	sorted = revsorted = (ATOMlinear(b->ttype) != 0);
	dense = (b->ttype == TYPE_oid);
	/* if no* props already set correctly, we can maybe speed
	 * things up, if not set correctly, reset them now and set
	 * them later */
	if (!b->tkey &&
	    b->T->nokey[0] >= b->batFirst &&
	    b->T->nokey[0] < b->batFirst + b->batCount &&
	    b->T->nokey[1] >= b->batFirst &&
	    b->T->nokey[1] < b->batFirst + b->batCount &&
	    b->T->nokey[0] != b->T->nokey[1] &&
	    cmpf(BUNtail(bi, b->T->nokey[0]),
		 BUNtail(bi, b->T->nokey[1])) == 0) {
		/* we found proof that the column doesn't deserve the
		 * key property, no need to check the hard way */
		expensive = 0;
		key = 0;
	} else {
		b->T->nokey[0] = 0;
		b->T->nokey[1] = 0;
	}
	if (!b->tsorted &&
	    b->T->nosorted > b->batFirst &&
	    b->T->nosorted < b->batFirst + b->batCount &&
	    cmpf(BUNtail(bi, b->T->nosorted - 1),
		 BUNtail(bi, b->T->nosorted)) > 0) {
		sorted = 0;
		dense = 0;
	} else {
		b->T->nosorted = 0;
	}
	if (!b->trevsorted &&
	    b->T->norevsorted > b->batFirst &&
	    b->T->norevsorted < b->batFirst + b->batCount &&
	    cmpf(BUNtail(bi, b->T->norevsorted - 1),
		 BUNtail(bi, b->T->norevsorted)) < 0) {
		revsorted = 0;
	} else {
		b->T->norevsorted = 0;
	}
	if (dense &&
	    !b->tdense &&
	    b->T->nodense >= b->batFirst &&
	    b->T->nodense < b->batFirst + b->batCount &&
	    (b->T->nodense == b->batFirst ?
	     * (oid *) BUNtail(bi, b->T->nodense) == oid_nil :
	     * (oid *) BUNtail(bi, b->T->nodense - 1) + 1 != * (oid *) BUNtail(bi, b->T->nodense))) {
		dense = 0;
	} else {
		b->T->nodense = 0;
	}
	if (expensive) {
		BUN mask;

		nme = BBP_physical(b->batCacheid);
		nmelen = strlen(nme);
		if (ATOMsize(b->ttype) == 1)
			mask = 1 << 8;
		else if (ATOMsize(b->ttype) == 2)
			mask = 1 << 16;
		else
			mask = HASHmask(b->batCount);
		if ((hp = GDKzalloc(sizeof(Heap))) == NULL ||
		    (hp->filename = GDKmalloc(nmelen + 30)) == NULL ||
		    (hp->farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0 ||
		    snprintf(hp->filename, nmelen + 30,
			     "%s.hash" SZFMT, nme, MT_getpid()) < 0 ||
		    (ext = GDKstrdup(hp->filename + nmelen + 1)) == NULL ||
		    (hs = HASHnew(hp, b->ttype, BUNlast(b), mask, BUN_NONE)) == NULL) {
			if (hp) {
				if (hp->filename)
					GDKfree(hp->filename);
				GDKfree(hp);
			}
			if (ext)
				GDKfree(ext);
			hp = NULL;
			ext = NULL;
			fprintf(stderr,
				"#BATderiveProps: cannot allocate "
				"hash table: not doing full check\n");
		}
	}
	for (q = BUNlast(b), p = BUNfirst(b);
	     p < q && (sorted || revsorted || (key && hs));
	     p++) {
		valp = BUNtail(bi, p);
		if (prev) {
			cmp = cmpf(prev, valp);
			if (cmp < 0) {
				revsorted = 0;
				if (b->T->norevsorted == 0)
					b->T->norevsorted = p;
				if (dense &&
				    * (oid *) prev + 1 != * (oid *) valp) {
					dense = 0;
					if (b->T->nodense == 0)
						b->T->nodense = p;
				}
			} else {
				if (cmp > 0) {
					sorted = 0;
					if (b->T->nosorted == 0)
						b->T->nosorted = p;
				} else {
					key = 0;
					if (b->T->nokey[0] == 0 &&
					    b->T->nokey[1] == 0) {
						b->T->nokey[0] = p - 1;
						b->T->nokey[1] = p;
					}
				}
				if (dense) {
					dense = 0;
					if (b->T->nodense == 0)
						b->T->nodense = p;
				}
			}
		} else if (dense && (sqbs = * (oid *) valp) == oid_nil) {
			dense = 0;
			b->T->nodense = p;
		}
		prev = valp;
		if (key && hs) {
			prb = HASHprobe(hs, valp);
			for (hb = HASHget(hs,prb);
			     hb != HASHnil(hs);
			     hb = HASHgetlink(hs,hb)) {
				if (cmpf(valp, BUNtail(bi, hb)) == 0) {
					key = 0;
					b->T->nokey[0] = hb;
					b->T->nokey[1] = p;
					break;
				}
			}
			HASHputlink(hs,p, HASHget(hs,prb));
			HASHput(hs,prb,p);
		}
	}
	if (hs) {
		HEAPfree(hp, 1);
		GDKfree(hp);
		GDKfree(hs);
		GDKfree(ext);
	}
	b->tsorted = sorted;
	b->trevsorted = revsorted;
	b->tdense = dense;
	if (dense)
		b->tseqbase = sqbs;
	if (hs) {
		b->tkey = key;
	} else {
		/* we can only say something about keyness if the
		 * column is sorted */
		b->tkey = key & (sorted | revsorted);
	}
	if (sorted || revsorted) {
		/* if sorted, we only need to check the extremes to
		 * know whether there are any nils */
		if (cmpf(BUNtail(bi, BUNfirst(b)), nilp) != 0 &&
		    cmpf(BUNtail(bi, BUNlast(b) - 1), nilp) != 0) {
			b->T->nonil = 1;
			b->T->nil = 0;
		} else {
			b->T->nonil = 0;
			b->T->nil = 1;
		}
	}
#ifndef NDEBUG
	BATassertTailProps(b);
#endif
}

void
BATderiveProps(BAT *b, int expensive)
{
	if (b == NULL) {
		assert(0);
		return;
	}
	BATderiveTailProps(b, expensive);
	if (b->H != b->T)
		BATderiveTailProps(BATmirror(b), expensive);
}
