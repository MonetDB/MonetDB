/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include "mutils.h"

#ifdef ALIGN
#undef ALIGN
#endif
#define ALIGN(n,b)	((b)?(b)*(1+(((n)-1)/(b))):n)

#define ATOMneedheap(tpe) (BATatoms[tpe].atomHeap != NULL)

BAT *
BATcreatedesc(oid hseq, int tt, bool heapnames, role_t role, uint16_t width)
{
	bat bid;
	BAT *bn;
	Heap *h = NULL, *vh = NULL;

	/*
	 * Alloc space for the BAT and its dependent records.
	 */
	assert(tt >= 0);

	if (heapnames) {
		if ((h = GDKmalloc(sizeof(Heap))) == NULL) {
			return NULL;
		}
		*h = (Heap) {
			.farmid = BBPselectfarm(role, tt, offheap),
			.dirty = true,
			.refs = ATOMIC_VAR_INIT(1),
		};

		if (ATOMneedheap(tt)) {
			if ((vh = GDKmalloc(sizeof(Heap))) == NULL) {
				GDKfree(h);
				return NULL;
			}
			*vh = (Heap) {
				.farmid = BBPselectfarm(role, tt, varheap),
				.dirty = true,
				.refs = ATOMIC_VAR_INIT(1),
			};
		}
	}

	bid = BBPallocbat(tt);
	if (bid == 0) {
		GDKfree(h);
		GDKfree(vh);
		return NULL;
	}
	bn = BBP_desc(bid);

	/*
	 * Fill in basic column info
	 */
	*bn = (BAT) {
		.batCacheid = bid,
		.hseqbase = hseq,

		.ttype = tt,
		.tkey = true,
		.tnonil = true,
		.tnil = false,
		.tsorted = ATOMlinear(tt),
		.trevsorted = ATOMlinear(tt),
		.tascii = tt == TYPE_str,
		.tseqbase = oid_nil,
		.tminpos = BUN_NONE,
		.tmaxpos = BUN_NONE,
		.tunique_est = 0.0,

		.batRole = role,
		.batTransient = true,
		.batRestricted = BAT_WRITE,
		.theap = h,
		.tvheap = vh,
		.creator_tid = MT_getpid(),
	};

	if (bn->theap) {
		bn->theap->parentid = bn->batCacheid;
		const char *nme = BBP_physical(bn->batCacheid);
		settailname(bn->theap, nme, tt, width);

		if (bn->tvheap) {
			bn->tvheap->parentid = bn->batCacheid;
			strconcat_len(bn->tvheap->filename,
				      sizeof(bn->tvheap->filename),
				      nme, ".theap", NULL);
		}
	}
	char name[MT_NAME_LEN];
	snprintf(name, sizeof(name), "heaplock%d", bn->batCacheid); /* fits */
	MT_lock_init(&bn->theaplock, name);
	snprintf(name, sizeof(name), "BATlock%d", bn->batCacheid); /* fits */
	MT_lock_init(&bn->batIdxLock, name);
	snprintf(name, sizeof(name), "hashlock%d", bn->batCacheid); /* fits */
	MT_rwlock_init(&bn->thashlock, name);
	return bn;
}

uint8_t
ATOMelmshift(int sz)
{
	uint8_t sh;
	int i = sz >> 1;

	for (sh = 0; i != 0; sh++) {
		i >>= 1;
	}
	return sh;
}


void
BATsetdims(BAT *b, uint16_t width)
{
	b->twidth = b->ttype == TYPE_str ? width > 0 ? width : 1 : ATOMsize(b->ttype);
	b->tshift = ATOMelmshift(b->twidth);
	assert_shift_width(b->tshift, b->twidth);
}

const char *
BATtailname(const BAT *b)
{
	if (b->ttype == TYPE_str) {
		switch (b->twidth) {
		case 1:
			return "tail1";
		case 2:
			return "tail2";
		case 4:
#if SIZEOF_VAR_T == 8
			return "tail4";
		case 8:
#endif
			break;
		default:
			MT_UNREACHABLE();
		}
	}
	return "tail";
}

void
settailname(Heap *restrict tail, const char *restrict physnme, int tt, int width)
{
	if (tt == TYPE_str) {
		switch (width) {
		case 0:
		case 1:
			strconcat_len(tail->filename,
				      sizeof(tail->filename), physnme,
				      ".tail1", NULL);
			return;
		case 2:
			strconcat_len(tail->filename,
				      sizeof(tail->filename), physnme,
				      ".tail2", NULL);
			return;
		case 4:
#if SIZEOF_VAR_T == 8
			strconcat_len(tail->filename,
				      sizeof(tail->filename), physnme,
				      ".tail4", NULL);
			return;
		case 8:
#endif
			break;
		default:
			MT_UNREACHABLE();
		}
	}
	strconcat_len(tail->filename, sizeof(tail->filename), physnme,
		      ".tail", NULL);
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
BAT *
COLnew2(oid hseq, int tt, BUN cap, role_t role, uint16_t width)
{
	BAT *bn;

	assert(cap <= BUN_MAX);
	assert(hseq <= oid_nil);
	ERRORcheck((tt < 0) || (tt > GDKatomcnt), "tt error\n", NULL);

	/* round up to multiple of BATTINY */
	if (cap < BUN_MAX - BATTINY)
		cap = (cap + BATTINY - 1) & ~(BATTINY - 1);
	if (ATOMstorage(tt) == TYPE_msk) {
		if (cap < 8*BATTINY)
			cap = 8*BATTINY;
		else
			cap = (cap + 31) & ~(BUN)31;
	} else if (cap < BATTINY)
		cap = BATTINY;
	/* limit the size */
	if (cap > BUN_MAX)
		cap = BUN_MAX;

	bn = BATcreatedesc(hseq, tt, true, role, width);
	if (bn == NULL)
		return NULL;

	BATsetdims(bn, width);
	bn->batCapacity = cap;

	if (ATOMstorage(tt) == TYPE_msk)
		cap /= 8;	/* 8 values per byte */

	/* alloc the main heaps */
	if (tt && HEAPalloc(bn->theap, cap, bn->twidth) != GDK_SUCCEED) {
		goto bailout;
	}

	if (bn->tvheap && width == 0 && ATOMheap(tt, bn->tvheap, cap) != GDK_SUCCEED) {
		HEAPfree(bn->theap, true);
		goto bailout;
	}
	BBPcacheit(bn, true);
	TRC_DEBUG(ALGO, "-> " ALGOBATFMT "\n", ALGOBATPAR(bn));
	return bn;
  bailout:
	BBPclear(bn->batCacheid);
	return NULL;
}

BAT *
COLnew(oid hseq, int tt, BUN cap, role_t role)
{
	return COLnew2(hseq, tt, cap, role, 0);
}

BAT *
BATdense(oid hseq, oid tseq, BUN cnt)
{
	BAT *bn;

	bn = COLnew(hseq, TYPE_void, 0, TRANSIENT);
	if (bn != NULL) {
		BATtseqbase(bn, tseq);
		BATsetcount(bn, cnt);
		TRC_DEBUG(ALGO, OIDFMT "," OIDFMT "," BUNFMT
			  "-> " ALGOBATFMT "\n", hseq, tseq, cnt,
			  ALGOBATPAR(bn));
	}
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

	BATcheck(b, 0);

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
	if (ATOMstorage(b->ttype) == TYPE_msk) /* round up to multiple of 32 */
		newcap = (newcap + 31) & ~(BUN)31;
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
	size_t theap_size;
	gdk_return rc = GDK_SUCCEED;

	assert(newcap <= BUN_MAX);
	BATcheck(b, GDK_FAIL);
	/*
	 * The main issue is to properly predict the new BAT size.
	 * storage overflow. The assumption taken is that capacity
	 * overflow is rare. It is changed only when the position of
	 * the next available BUN surpasses the free area marker.  Be
	 * aware that the newcap should be greater than the old value,
	 * otherwise you may easily corrupt the administration of
	 * malloc.
	 */
	MT_lock_set(&b->theaplock);
	if (newcap <= BATcapacity(b)) {
		MT_lock_unset(&b->theaplock);
		return GDK_SUCCEED;
	}

	if (ATOMstorage(b->ttype) == TYPE_msk) {
		newcap = (newcap + 31) & ~(BUN)31; /* round up to multiple of 32 */
		theap_size = (size_t) (newcap / 8); /* in bytes */
	} else {
		theap_size = (size_t) newcap << b->tshift;
	}

	if (b->theap->base) {
		TRC_DEBUG(HEAP, "HEAPgrow in BATextend %s %zu %zu\n",
			  b->theap->filename, b->theap->size, theap_size);
		rc = HEAPgrow(&b->theap, theap_size, b->batRestricted == BAT_READ);
		if (rc == GDK_SUCCEED)
			b->batCapacity = newcap;
	} else {
		b->batCapacity = newcap;
	}
	MT_lock_unset(&b->theaplock);

	return rc;
}



/*
 * @+ BAT destruction
 * BATclear quickly removes all elements from a BAT. It must respect
 * the transaction rules; so stable elements must be moved to the
 * "deleted" section of the BAT (they cannot be fully deleted
 * yet). For the elements that really disappear, we must free
 * heapspace. As an optimization, in the case of no stable elements, we quickly empty
 * the heaps by copying a standard small empty image over them.
 */
gdk_return
BATclear(BAT *b, bool force)
{
	BUN p, q;

	BATcheck(b, GDK_FAIL);

	if (!force && b->batInserted > 0) {
		GDKerror("cannot clear committed BAT\n");
		return GDK_FAIL;
	}

	TRC_DEBUG(ALGO, ALGOBATFMT "\n", ALGOBATPAR(b));

	/* kill all search accelerators */
	HASHdestroy(b);
	OIDXdestroy(b);
	STRMPdestroy(b);
	RTREEdestroy(b);
	PROPdestroy(b);

	bat tvp = 0;

	/* we must dispose of all inserted atoms */
	MT_lock_set(&b->theaplock);
	if (force && BATatoms[b->ttype].atomDel == NULL) {
		assert(b->tvheap == NULL || b->tvheap->parentid == b->batCacheid);
		/* no stable elements: we do a quick heap clean */
		/* need to clean heap which keeps data even though the
		   BUNs got removed. This means reinitialize when
		   free > 0
		*/
		if (b->tvheap && b->tvheap->free > 0) {
			Heap *th = GDKmalloc(sizeof(Heap));

			if (th == NULL) {
				MT_lock_unset(&b->theaplock);
				return GDK_FAIL;
			}
			*th = (Heap) {
				.farmid = b->tvheap->farmid,
				.parentid = b->tvheap->parentid,
				.dirty = true,
				.hasfile = b->tvheap->hasfile,
				.refs = ATOMIC_VAR_INIT(1),
			};
			strcpy_len(th->filename, b->tvheap->filename, sizeof(th->filename));
			if (ATOMheap(b->ttype, th, 0) != GDK_SUCCEED) {
				MT_lock_unset(&b->theaplock);
				return GDK_FAIL;
			}
			tvp = b->tvheap->parentid;
			HEAPdecref(b->tvheap, false);
			b->tvheap = th;
		}
	} else {
		/* do heap-delete of all inserted atoms */
		void (*tatmdel)(Heap*,var_t*) = BATatoms[b->ttype].atomDel;

		/* TYPE_str has no del method, so we shouldn't get here */
		assert(tatmdel == NULL || b->twidth == sizeof(var_t));
		if (tatmdel) {
			BATiter bi = bat_iterator_nolock(b);

			for (p = b->batInserted, q = BATcount(b); p < q; p++)
				(*tatmdel)(b->tvheap, (var_t*) BUNtloc(bi,p));
			b->tvheap->dirty = true;
		}
	}

	b->batInserted = 0;
	b->batCount = 0;
	if (b->ttype == TYPE_void)
		b->batCapacity = 0;
	b->theap->free = 0;
	BAThseqbase(b, 0);
	BATtseqbase(b, ATOMtype(b->ttype) == TYPE_oid ? 0 : oid_nil);
	b->theap->dirty = true;
	b->tnonil = true;
	b->tnil = false;
	b->tsorted = b->trevsorted = ATOMlinear(b->ttype);
	b->tnosorted = b->tnorevsorted = 0;
	b->tkey = true;
	b->tnokey[0] = b->tnokey[1] = 0;
	b->tminpos = b->tmaxpos = BUN_NONE;
	b->tunique_est = 0;
	MT_lock_unset(&b->theaplock);
	if (tvp != 0 && tvp != b->batCacheid)
		BBPrelease(tvp);
	return GDK_SUCCEED;
}

/* free a cached BAT; leave the bat descriptor cached */
void
BATfree(BAT *b)
{
	if (b == NULL)
		return;

	/* deallocate all memory for a bat */
	MT_rwlock_rdlock(&b->thashlock);
	BUN nunique = BUN_NONE;
	if (b->thash && b->thash != (Hash *) 1) {
		nunique = b->thash->nunique;
	}
	MT_rwlock_rdunlock(&b->thashlock);
	HASHfree(b);
	OIDXfree(b);
	STRMPfree(b);
	RTREEfree(b);
	MT_lock_set(&b->theaplock);
	if (nunique != BUN_NONE) {
		b->tunique_est = (double) nunique;
	}
	/* wait until there are no other references to the heap; a
	 * reference is possible in e.g. BBPsync that uses a
	 * bat_iterator directly on the BBP_desc, i.e. without fix */
	while (b->theap && (ATOMIC_GET(&b->theap->refs) & HEAPREFS) > 1) {
		MT_lock_unset(&b->theaplock);
		MT_sleep_ms(1);
		MT_lock_set(&b->theaplock);
	}
	if (b->theap) {
		assert((ATOMIC_GET(&b->theap->refs) & HEAPREFS) == 1);
		assert(b->theap->parentid == b->batCacheid);
		HEAPfree(b->theap, false);
	}
	/* wait until there are no other references to the heap; a
	 * reference is possible in e.g. BBPsync that uses a
	 * bat_iterator directly on the BBP_desc, i.e. without fix */
	while (b->tvheap && (ATOMIC_GET(&b->tvheap->refs) & HEAPREFS) > 1) {
		MT_lock_unset(&b->theaplock);
		MT_sleep_ms(1);
		MT_lock_set(&b->theaplock);
	}
	if (b->tvheap) {
		assert((ATOMIC_GET(&b->tvheap->refs) & HEAPREFS) == 1);
		assert(b->tvheap->parentid == b->batCacheid);
		HEAPfree(b->tvheap, false);
	}
	MT_lock_unset(&b->theaplock);
}

/* free a cached BAT descriptor */
void
BATdestroy(BAT *b)
{
	if (b->tvheap) {
		GDKfree(b->tvheap);
	}
	PROPdestroy_nolock(b);
	MT_lock_destroy(&b->theaplock);
	MT_lock_destroy(&b->batIdxLock);
	MT_rwlock_destroy(&b->thashlock);
	if (b->theap) {
		GDKfree(b->theap);
	}
	if (b->oldtail) {
		ATOMIC_AND(&b->oldtail->refs, ~DELAYEDREMOVE);
		/* the bat has not been committed, so we cannot remove
		 * the old tail file */
		HEAPdecref(b->oldtail, false);
		b->oldtail = NULL;
	}
	*b = (BAT) {
		.batCacheid = 0,
	};
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
static bool
wrongtype(int t1, int t2)
{
	/* check if types are compatible. be extremely forgiving */
	if (t1 != TYPE_void) {
		t1 = ATOMtype(ATOMstorage(t1));
		t2 = ATOMtype(ATOMstorage(t2));
		if (t1 != t2) {
			if (ATOMvarsized(t1) ||
			    ATOMvarsized(t2) ||
			    t1 == TYPE_msk || t2 == TYPE_msk ||
			    ATOMsize(t1) != ATOMsize(t2))
				return true;
		}
	}
	return false;
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
BAT *
COLcopy(BAT *b, int tt, bool writable, role_t role)
{
	bool slowcopy = false;
	BAT *bn = NULL;
	BATiter bi;
	char strhash[GDK_STRHASHSIZE];

	BATcheck(b, NULL);

	/* maybe a bit ugly to change the requested bat type?? */
	if (b->ttype == TYPE_void && !writable)
		tt = TYPE_void;

	if (tt != b->ttype && wrongtype(tt, b->ttype)) {
		GDKerror("wrong tail-type requested\n");
		return NULL;
	}

	/* in case of a string bat, we save the string heap hash table
	 * while we have the lock so that we can restore it in the copy;
	 * this is because during our operation, a parallel thread could
	 * be adding strings to the vheap which would modify the hash
	 * table and that would result in buckets containing values
	 * beyond the original vheap that we're copying */
	MT_lock_set(&b->theaplock);
	BAT *pb = NULL, *pvb = NULL;
	if (b->theap->parentid != b->batCacheid) {
		pb = BBP_desc(b->theap->parentid);
		MT_lock_set(&pb->theaplock);
	}
	if (b->tvheap &&
	    b->tvheap->parentid != b->batCacheid &&
	    b->tvheap->parentid != b->theap->parentid) {
		pvb = BBP_desc(b->tvheap->parentid);
		MT_lock_set(&pvb->theaplock);
	}
	bi = bat_iterator_nolock(b);
	if (ATOMstorage(b->ttype) == TYPE_str && b->tvheap->free >= GDK_STRHASHSIZE)
		memcpy(strhash, b->tvheap->base, GDK_STRHASHSIZE);

	bat_iterator_incref(&bi);
	if (pvb)
		MT_lock_unset(&pvb->theaplock);
	if (pb)
		MT_lock_unset(&pb->theaplock);
	MT_lock_unset(&b->theaplock);

	/* first try case (1); create a view, possibly with different
	 * atom-types */
	if (!writable &&
	    role == TRANSIENT &&
	    bi.restricted == BAT_READ &&
	    ATOMstorage(b->ttype) != TYPE_msk && /* no view on TYPE_msk */
	    (bi.h == NULL ||
	     bi.h->parentid == b->batCacheid ||
	     BBP_desc(bi.h->parentid)->batRestricted == BAT_READ)) {
		bn = VIEWcreate(b->hseqbase, b, 0, BUN_MAX);
		if (bn == NULL) {
			goto bunins_failed;
		}
		if (tt != bn->ttype) {
			bn->ttype = tt;
			if (bn->tvheap && !ATOMvarsized(tt)) {
				if (bn->tvheap->parentid != bn->batCacheid)
					BBPrelease(bn->tvheap->parentid);
				HEAPdecref(bn->tvheap, false);
				bn->tvheap = NULL;
			}
			bn->tseqbase = ATOMtype(tt) == TYPE_oid ? bi.tseq : oid_nil;
		}
		bat_iterator_end(&bi);
		return bn;
	} else {
		/* check whether we need case (4); BUN-by-BUN copy (by
		 * setting slowcopy to true) */
		if (ATOMsize(tt) != ATOMsize(bi.type)) {
			/* oops, void materialization */
			slowcopy = true;
		} else if (bi.h && bi.h->parentid != b->batCacheid &&
			   BATcapacity(BBP_desc(bi.h->parentid)) > bi.count + bi.count) {
			/* reduced slice view: do not copy too much
			 * garbage */
			slowcopy = true;
		} else if (bi.vh && bi.vh->parentid != b->batCacheid &&
			   BATcount(BBP_desc(bi.vh->parentid)) > bi.count + bi.count) {
			/* reduced vheap view: do not copy too much
			 * garbage; this really is a heuristic since the
			 * vheap could be used completely, even if the
			 * offset heap is only (less than) half the size
			 * of the parent's offset heap */
			slowcopy = true;
		}

		bn = COLnew2(b->hseqbase, tt, bi.count, role, bi.width);
		if (bn == NULL) {
			goto bunins_failed;
		}
		if (bn->tvheap != NULL && bn->tvheap->base == NULL) {
			/* this combination can happen since the last
			 * argument of COLnew2 not being zero triggers a
			 * skip in the allocation of the tvheap */
			if (ATOMheap(bn->ttype, bn->tvheap, bn->batCapacity) != GDK_SUCCEED) {
				goto bunins_failed;
			}
		}

		if (tt == TYPE_void) {
			/* case (2): a void,void result => nothing to
			 * copy! */
			bn->theap->free = 0;
		} else if (!slowcopy) {
			/* case (3): just copy the heaps */
			if (bn->tvheap && HEAPextend(bn->tvheap, bi.vhfree, true) != GDK_SUCCEED) {
				goto bunins_failed;
			}
			memcpy(bn->theap->base, bi.base, bi.hfree);
			bn->theap->free = bi.hfree;
			bn->theap->dirty = true;
			if (bn->tvheap) {
				memcpy(bn->tvheap->base, bi.vh->base, bi.vhfree);
				bn->tvheap->free = bi.vhfree;
				bn->tvheap->dirty = true;
				bn->tascii = bi.ascii;
				if (ATOMstorage(b->ttype) == TYPE_str && bi.vhfree >= GDK_STRHASHSIZE)
					memcpy(bn->tvheap->base, strhash, GDK_STRHASHSIZE);
			}

			/* make sure we use the correct capacity */
			if (ATOMstorage(bn->ttype) == TYPE_msk)
				bn->batCapacity = (BUN) (bn->theap->size * 8);
			else if (bn->ttype)
				bn->batCapacity = (BUN) (bn->theap->size >> bn->tshift);
			else
				bn->batCapacity = 0;
		} else if (tt != TYPE_void || ATOMextern(tt)) {
			/* case (4): one-by-one BUN insert (really slow) */
			QryCtx *qry_ctx = MT_thread_get_qry_ctx();

			TIMEOUT_LOOP_IDX_DECL(p, bi.count, qry_ctx) {
				const void *t = BUNtail(bi, p);

				if (bunfastapp_nocheck(bn, t) != GDK_SUCCEED) {
					goto bunins_failed;
				}
			}
			TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed, qry_ctx));
			bn->theap->dirty |= bi.count > 0;
		} else if (tt != TYPE_void && bi.type == TYPE_void) {
			/* case (4): optimized for unary void
			 * materialization */
			oid cur = bi.tseq, *dst = (oid *) Tloc(bn, 0);
			const oid inc = !is_oid_nil(cur);

			for (BUN p = 0; p < bi.count; p++) {
				dst[p] = cur;
				cur += inc;
			}
			bn->theap->free = bi.count * sizeof(oid);
			bn->theap->dirty |= bi.count > 0;
		} else if (ATOMstorage(bi.type) == TYPE_msk) {
			/* convert number of bits to number of bytes,
			 * and round the latter up to a multiple of
			 * 4 (copy in units of 4 bytes) */
			bn->theap->free = ((bi.count + 31) / 32) * 4;
			memcpy(Tloc(bn, 0), bi.base, bn->theap->free);
			bn->theap->dirty |= bi.count > 0;
		} else {
			/* case (4): optimized for simple array copy */
			bn->theap->free = bi.count << bn->tshift;
			memcpy(Tloc(bn, 0), bi.base, bn->theap->free);
			bn->theap->dirty |= bi.count > 0;
		}
		/* copy all properties (size+other) from the source bat */
		BATsetcount(bn, bi.count);
	}
	/* set properties (note that types may have changed in the copy) */
	if (ATOMtype(tt) == ATOMtype(bi.type)) {
		if (ATOMtype(tt) == TYPE_oid) {
			BATtseqbase(bn, bi.tseq);
		} else {
			BATtseqbase(bn, oid_nil);
		}
		BATkey(bn, bi.key);
		bn->tsorted = bi.sorted;
		bn->trevsorted = bi.revsorted;
		bn->tnorevsorted = bi.norevsorted;
		if (bi.nokey[0] != bi.nokey[1]) {
			bn->tnokey[0] = bi.nokey[0];
			bn->tnokey[1] = bi.nokey[1];
		} else {
			bn->tnokey[0] = bn->tnokey[1] = 0;
		}
		bn->tnosorted = bi.nosorted;
		bn->tnonil = bi.nonil;
		bn->tnil = bi.nil;
		bn->tminpos = bi.minpos;
		bn->tmaxpos = bi.maxpos;
		if (!bi.key)
			bn->tunique_est = bi.unique_est;
	} else if (ATOMstorage(tt) == ATOMstorage(b->ttype) &&
		   ATOMcompare(tt) == ATOMcompare(b->ttype)) {
		BUN h = bi.count;
		bn->tsorted = bi.sorted;
		bn->trevsorted = bi.revsorted;
		BATkey(bn, bi.key);
		bn->tnonil = bi.nonil;
		bn->tnil = bi.nil;
		if (bi.nosorted > 0 && bi.nosorted < h)
			bn->tnosorted = bi.nosorted;
		else
			bn->tnosorted = 0;
		if (bi.norevsorted > 0 && bi.norevsorted < h)
			bn->tnorevsorted = bi.norevsorted;
		else
			bn->tnorevsorted = 0;
		if (bi.nokey[0] < h &&
		    bi.nokey[1] < h &&
		    bi.nokey[0] != bi.nokey[1]) {
			bn->tnokey[0] = bi.nokey[0];
			bn->tnokey[1] = bi.nokey[1];
		} else {
			bn->tnokey[0] = bn->tnokey[1] = 0;
		}
		bn->tminpos = bi.minpos;
		bn->tmaxpos = bi.maxpos;
		if (!bi.key)
			bn->tunique_est = bi.unique_est;
	} else {
		bn->tsorted = bn->trevsorted = false; /* set based on count later */
		bn->tnonil = bn->tnil = false;
		bn->tkey = false;
		bn->tnosorted = bn->tnorevsorted = 0;
		bn->tnokey[0] = bn->tnokey[1] = 0;
	}
	if (BATcount(bn) <= 1) {
		bn->tsorted = ATOMlinear(b->ttype);
		bn->trevsorted = ATOMlinear(b->ttype);
		bn->tkey = true;
		bn->tunique_est = (double) bn->batCount;
	}
	bat_iterator_end(&bi);
	if (!writable)
		bn->batRestricted = BAT_READ;
	TRC_DEBUG(ALGO, ALGOBATFMT " -> " ALGOBATFMT "\n",
		  ALGOBATPAR(b), ALGOBATPAR(bn));
	return bn;
      bunins_failed:
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}

/* Append an array of values of length count to the bat.  For
 * fixed-sized values, `values' is an array of values, for
 * variable-sized values, `values' is an array of pointers to values.
 * If values equals NULL, count times nil will be appended. */
gdk_return
BUNappendmulti(BAT *b, const void *values, BUN count, bool force)
{
	BUN p;
	BUN nunique = 0;

	BATcheck(b, GDK_FAIL);

	assert(!VIEWtparent(b));

	if (count == 0)
		return GDK_SUCCEED;

	TRC_DEBUG(ALGO, ALGOBATFMT " appending " BUNFMT " values%s\n", ALGOBATPAR(b), count, values ? "" : " (all nil)");

	p = BATcount(b);		/* insert at end */
	if (p == BUN_MAX || BATcount(b) + count >= BUN_MAX) {
		GDKerror("bat too large\n");
		return GDK_FAIL;
	}

	ALIGNapp(b, force, GDK_FAIL);
	/* load hash so that we can maintain it */
	(void) BATcheckhash(b);

	if (b->ttype == TYPE_void && BATtdense(b)) {
		const oid *ovals = values;
		bool dense = b->batCount == 0 || (ovals != NULL && b->tseqbase + 1 == ovals[0]);
		if (ovals) {
			for (BUN i = 1; dense && i < count; i++) {
				dense = ovals[i - 1] + 1 == ovals[i];
			}
		}
		if (dense) {
			MT_lock_set(&b->theaplock);
			assert(BATtdense(b)); /* no change (coverity) */
			if (b->batCount == 0)
				b->tseqbase = ovals ? ovals[0] : oid_nil;
			BATsetcount(b, BATcount(b) + count);
			MT_lock_unset(&b->theaplock);
			return GDK_SUCCEED;
		} else {
			/* we need to materialize b; allocate enough capacity */
			if (BATmaterialize(b, BATcount(b) + count) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	}

	if (unshare_varsized_heap(b) != GDK_SUCCEED) {
		return GDK_FAIL;
	}

	if (BATcount(b) + count > BATcapacity(b)) {
		/* if needed space exceeds a normal growth extend just
		 * with what's needed */
		BUN ncap = BATcount(b) + count;
		BUN grows = BATgrows(b);

		if (ncap > grows)
			grows = ncap;
		gdk_return rc = BATextend(b, grows);
		if (rc != GDK_SUCCEED)
			return rc;
	}

	const void *t = b->ttype == TYPE_msk ? &(msk){false} : ATOMnilptr(b->ttype);
	MT_lock_set(&b->theaplock);
	BATiter bi = bat_iterator_nolock(b);
	const ValRecord *prop;
	ValRecord minprop, maxprop;
	const void *minbound = NULL, *maxbound = NULL;
	if ((prop = BATgetprop_nolock(b, GDK_MIN_BOUND)) != NULL &&
	    VALcopy(&minprop, prop) != NULL)
		minbound = VALptr(&minprop);
	if ((prop = BATgetprop_nolock(b, GDK_MAX_BOUND)) != NULL &&
	    VALcopy(&maxprop, prop) != NULL)
		maxbound = VALptr(&maxprop);
	const bool notnull = BATgetprop_nolock(b, GDK_NOT_NULL) != NULL;
	bool setnil = false;
	MT_lock_unset(&b->theaplock);
	MT_rwlock_wrlock(&b->thashlock);
	if (values && b->ttype) {
		int (*atomcmp) (const void *, const void *) = ATOMcompare(b->ttype);
		const void *atomnil = ATOMnilptr(b->ttype);
		const void *minvalp = NULL, *maxvalp = NULL;
		if (b->tvheap) {
			if (bi.minpos != BUN_NONE)
				minvalp = BUNtvar(bi, bi.minpos);
			if (bi.maxpos != BUN_NONE)
				maxvalp = BUNtvar(bi, bi.maxpos);
			const void *vbase = b->tvheap->base;
			for (BUN i = 0; i < count; i++) {
				t = ((void **) values)[i];
				bool isnil = atomcmp(t, atomnil) == 0;
				gdk_return rc;
				if (notnull && isnil) {
					assert(0);
					GDKerror("NULL value not within bounds\n");
					rc = GDK_FAIL;
				} else if (minbound &&
					   !isnil &&
					   atomcmp(t, minbound) < 0) {
					assert(0);
					GDKerror("value not within bounds\n");
					rc = GDK_FAIL;
				} else if (maxbound &&
					   !isnil &&
					   atomcmp(t, maxbound) >= 0) {
					assert(0);
					GDKerror("value not within bounds\n");
					rc = GDK_FAIL;
				} else {
					rc = tfastins_nocheckVAR(b, p, t);
				}
				if (rc != GDK_SUCCEED) {
					MT_rwlock_wrunlock(&b->thashlock);
					if (minbound)
						VALclear(&minprop);
					if (maxbound)
						VALclear(&maxprop);
					return rc;
				}
				if (vbase != b->tvheap->base) {
					/* tvheap changed location, so
					 * pointers may need to be
					 * updated (not if they were
					 * initialized from t below, but
					 * we don't know) */
					BUN minpos = bi.minpos;
					BUN maxpos = bi.maxpos;
					MT_lock_set(&b->theaplock);
					bi = bat_iterator_nolock(b);
					MT_lock_unset(&b->theaplock);
					bi.minpos = minpos;
					bi.maxpos = maxpos;
					vbase = b->tvheap->base;
					if (bi.minpos != BUN_NONE)
						minvalp = BUNtvar(bi, bi.minpos);
					if (bi.maxpos != BUN_NONE)
						maxvalp = BUNtvar(bi, bi.maxpos);
				}
				if (!isnil) {
					if (p == 0) {
						bi.minpos = bi.maxpos = 0;
						minvalp = maxvalp = t;
					} else {
						if (bi.minpos != BUN_NONE &&
						    atomcmp(minvalp, t) > 0) {
							bi.minpos = p;
							minvalp = t;
						}
						if (bi.maxpos != BUN_NONE &&
						    atomcmp(maxvalp, t) < 0) {
							bi.maxpos = p;
							maxvalp = t;
						}
					}
				} else {
					setnil = true;
				}
				p++;
			}
			if (minbound)
				VALclear(&minprop);
			if (maxbound)
				VALclear(&maxprop);
			if (b->thash) {
				p -= count;
				for (BUN i = 0; i < count; i++) {
					t = ((void **) values)[i];
					HASHappend_locked(b, p, t);
					p++;
				}
				nunique = b->thash ? b->thash->nunique : 0;
			}
		} else if (ATOMstorage(b->ttype) == TYPE_msk) {
			bi.minpos = bi.maxpos = BUN_NONE;
			minvalp = maxvalp = NULL;
			assert(!b->tnil);
			for (BUN i = 0; i < count; i++) {
				t = (void *) ((char *) values + (i << b->tshift));
				mskSetVal(b, p, *(msk *) t);
				p++;
			}
		} else {
			if (bi.minpos != BUN_NONE)
				minvalp = BUNtloc(bi, bi.minpos);
			if (bi.maxpos != BUN_NONE)
				maxvalp = BUNtloc(bi, bi.maxpos);
			for (BUN i = 0; i < count; i++) {
				t = (void *) ((char *) values + (i << b->tshift));
				gdk_return rc = tfastins_nocheckFIX(b, p, t);
				if (rc != GDK_SUCCEED) {
					MT_rwlock_wrunlock(&b->thashlock);
					return rc;
				}
				if (b->thash) {
					HASHappend_locked(b, p, t);
				}
				if (atomcmp(t, atomnil) != 0) {
					if (p == 0) {
						bi.minpos = bi.maxpos = 0;
						minvalp = maxvalp = t;
					} else {
						if (bi.minpos != BUN_NONE &&
						    atomcmp(minvalp, t) > 0) {
							bi.minpos = p;
							minvalp = t;
						}
						if (bi.maxpos != BUN_NONE &&
						    atomcmp(maxvalp, t) < 0) {
							bi.maxpos = p;
							maxvalp = t;
						}
					}
				} else {
					setnil = true;
				}
				p++;
			}
			nunique = b->thash ? b->thash->nunique : 0;
		}
	} else {
		/* inserting nils, unless it's msk */
		for (BUN i = 0; i < count; i++) {
			gdk_return rc = tfastins_nocheck(b, p, t);
			if (rc != GDK_SUCCEED) {
				MT_rwlock_wrunlock(&b->thashlock);
				return rc;
			}
			if (b->thash) {
				HASHappend_locked(b, p, t);
			}
			p++;
		}
		nunique = b->thash ? b->thash->nunique : 0;
		setnil |= b->ttype != TYPE_msk;
	}
	MT_lock_set(&b->theaplock);
	if (setnil) {
		b->tnil = true;
		b->tnonil = false;
	}
	b->tminpos = bi.minpos;
	b->tmaxpos = bi.maxpos;
	if (count > BATcount(b) / gdk_unique_estimate_keep_fraction)
		b->tunique_est = 0;

	if (b->ttype == TYPE_oid) {
		/* spend extra effort on oid (possible candidate list) */
		if (values == NULL || is_oid_nil(((oid *) values)[0])) {
			b->tsorted = false;
			b->trevsorted = false;
			b->tkey = false;
			b->tseqbase = oid_nil;
		} else {
			if (b->batCount == 0) {
				b->tsorted = true;
				b->trevsorted = true;
				b->tkey = true;
				b->tseqbase = count == 1 ? ((oid *) values)[0] : oid_nil;
			} else {
				if (!is_oid_nil(b->tseqbase) &&
				    (count > 1 ||
				     b->tseqbase + b->batCount != ((oid *) values)[0]))
					b->tseqbase = oid_nil;
				if (b->tsorted && !is_oid_nil(((oid *) b->theap->base)[b->batCount - 1]) && ((oid *) b->theap->base)[b->batCount - 1] > ((oid *) values)[0]) {
					b->tsorted = false;
					if (b->tnosorted == 0)
						b->tnosorted = b->batCount;
				}
				if (b->trevsorted && !is_oid_nil(((oid *) values)[0]) && ((oid *) b->theap->base)[b->batCount - 1] < ((oid *) values)[0]) {
					b->trevsorted = false;
					if (b->tnorevsorted == 0)
						b->tnorevsorted = b->batCount;
				}
				if (b->tkey) {
					if (((oid *) b->theap->base)[b->batCount - 1] == ((oid *) values)[0]) {
						b->tkey = false;
						if (b->tnokey[1] == 0) {
							b->tnokey[0] = b->batCount - 1;
							b->tnokey[1] = b->batCount;
						}
					} else if (!b->tsorted && !b->trevsorted)
						b->tkey = false;
				}
			}
			for (BUN i = 1; i < count; i++) {
				if (is_oid_nil(((oid *) values)[i])) {
					b->tsorted = false;
					b->trevsorted = false;
					b->tkey = false;
					b->tseqbase = oid_nil;
					break;
				}
				if (((oid *) values)[i - 1] == ((oid *) values)[i]) {
					b->tkey = false;
					if (b->tnokey[1] == 0) {
						b->tnokey[0] = b->batCount + i - 1;
						b->tnokey[1] = b->batCount + i;
					}
				} else if (((oid *) values)[i - 1] > ((oid *) values)[i]) {
					b->tsorted = false;
					if (b->tnosorted == 0)
						b->tnosorted = b->batCount + i;
					if (!b->trevsorted)
						b->tkey = false;
				} else {
					if (((oid *) values)[i - 1] + 1 != ((oid *) values)[i])
						b->tseqbase = oid_nil;
					b->trevsorted = false;
					if (b->tnorevsorted == 0)
						b->tnorevsorted = b->batCount + i;
					if (!b->tsorted)
						b->tkey = false;
				}
			}
		}
	} else if (!ATOMlinear(b->ttype)) {
		b->tsorted = b->trevsorted = b->tkey = false;
	} else if (b->batCount == 0) {
		if (values == NULL) {
			b->tsorted = b->trevsorted = true;
			b->tkey = count == 1;
			b->tunique_est = 1;
		} else {
			int c;
			switch (count) {
			case 1:
				b->tsorted = b->trevsorted = b->tkey = true;
				b->tunique_est = 1;
				break;
			case 2:
				if (b->tvheap)
					c = ATOMcmp(b->ttype,
						    ((void **) values)[0],
						    ((void **) values)[1]);
				else
					c = ATOMcmp(b->ttype,
						    values,
						    (char *) values + b->twidth);
				b->tsorted = c <= 0;
				b->tnosorted = !b->tsorted;
				b->trevsorted = c >= 0;
				b->tnorevsorted = !b->trevsorted;
				b->tkey = c != 0;
				b->tnokey[0] = 0;
				b->tnokey[1] = !b->tkey;
				b->tunique_est = (double) (1 + b->tkey);
				break;
			default:
				b->tsorted = b->trevsorted = b->tkey = false;
				break;
			}
		}
	} else if (b->batCount == 1 && count == 1) {
		bi = bat_iterator_nolock(b);
		t = b->ttype == TYPE_msk ? &(msk){false} : ATOMnilptr(b->ttype);
		if (values != NULL) {
			if (b->tvheap)
				t = ((void **) values)[0];
			else
				t = values;
		}
		int c = ATOMcmp(b->ttype, BUNtail(bi, 0), t);
		b->tsorted = c <= 0;
		b->tnosorted = !b->tsorted;
		b->trevsorted = c >= 0;
		b->tnorevsorted = !b->trevsorted;
		b->tkey = c != 0;
		b->tnokey[0] = 0;
		b->tnokey[1] = !b->tkey;
		b->tunique_est = (double) (1 + b->tkey);
	} else {
		b->tsorted = b->trevsorted = b->tkey = false;
	}
	BATsetcount(b, p);
	if (nunique != 0)
		b->tunique_est = (double) nunique;
	MT_lock_unset(&b->theaplock);
	MT_rwlock_wrunlock(&b->thashlock);

	OIDXdestroy(b);
	STRMPdestroy(b);	/* TODO: use STRMPappendBitstring */
	RTREEdestroy(b);
	return GDK_SUCCEED;
}

/* Append a single value to the bat. */
gdk_return
BUNappend(BAT *b, const void *t, bool force)
{
	return BUNappendmulti(b, b->ttype && b->tvheap ? (const void *) &t : (const void *) t, 1, force);
}

gdk_return
BUNdelete(BAT *b, oid o)
{
	BATiter bi = bat_iterator(b);

	if (bi.count == 0) {
		bat_iterator_end(&bi);
		GDKerror("cannot delete from empty bat\n");
		return GDK_FAIL;
	}
	if (is_oid_nil(b->hseqbase)) {
		bat_iterator_end(&bi);
		GDKerror("cannot delete from bat with VOID hseqbase\n");
		return GDK_FAIL;
	}

	BUN p = o - b->hseqbase;

	if (bi.count - 1 != p) {
		bat_iterator_end(&bi);
		GDKerror("cannot delete anything other than last value\n");
		return GDK_FAIL;
	}
	if (b->batInserted >= bi.count) {
		bat_iterator_end(&bi);
		GDKerror("cannot delete committed value\n");
		return GDK_FAIL;
	}

	TRC_DEBUG(ALGO, ALGOBATFMT " deleting oid " OIDFMT "\n", ALGOBATPAR(b), o);
	/* load hash so that we can maintain it */
	(void) BATcheckhash(b);

	BUN nunique = HASHdelete(&bi, p, BUNtail(bi, p));
	ATOMdel(b->ttype, b->tvheap, (var_t *) BUNtloc(bi, p));
	bat_iterator_end(&bi);

	MT_lock_set(&b->theaplock);
	if (b->tmaxpos == p)
		b->tmaxpos = BUN_NONE;
	if (b->tminpos == p)
		b->tminpos = BUN_NONE;
	if (b->tnosorted >= p)
		b->tnosorted = 0;
	if (b->tnorevsorted >= p)
		b->tnorevsorted = 0;
	b->batCount--;
	if (nunique != 0)
		b->tunique_est = (double) nunique;
	else if (BATcount(b) < gdk_unique_estimate_keep_fraction)
		b->tunique_est = 0;
	if (b->batCount <= 1) {
		/* some trivial properties */
		b->tkey = true;
		b->tsorted = b->trevsorted = true;
		b->tnosorted = b->tnorevsorted = 0;
		if (b->batCount == 0) {
			b->tnil = false;
			b->tnonil = true;
		}
	}
	MT_lock_unset(&b->theaplock);
	OIDXdestroy(b);
	STRMPdestroy(b);
	RTREEdestroy(b);
	PROPdestroy(b);
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
static gdk_return
BUNinplacemulti(BAT *b, const oid *positions, const void *values, BUN count, bool force, bool autoincr)
{
	BUN prv, nxt;
	const void *val;
	int (*atomcmp) (const void *, const void *) = ATOMcompare(b->ttype);
	const void *atomnil = ATOMnilptr(b->ttype);

	MT_lock_set(&b->theaplock);
	BUN last = BATcount(b) - 1;
	BATiter bi = bat_iterator_nolock(b);
	/* zap alignment info */
	if (!force && (b->batRestricted != BAT_WRITE ||
		       ((ATOMIC_GET(&b->theap->refs) & HEAPREFS) > 1))) {
		MT_lock_unset(&b->theaplock);
		GDKerror("access denied to %s, aborting.\n",
			 BATgetId(b));
		assert(0);
		return GDK_FAIL;
	}
	TRC_DEBUG(ALGO, ALGOBATFMT " replacing " BUNFMT " values\n", ALGOBATPAR(b), count);
	if (b->ttype == TYPE_void) {
		PROPdestroy(b);
		b->tminpos = BUN_NONE;
		b->tmaxpos = BUN_NONE;
		b->tunique_est = 0.0;
	} else if (count > BATcount(b) / gdk_unique_estimate_keep_fraction) {
		b->tunique_est = 0;
	}
	const ValRecord *prop;
	ValRecord minprop, maxprop;
	const void *minbound = NULL, *maxbound = NULL;
	if ((prop = BATgetprop_nolock(b, GDK_MIN_BOUND)) != NULL &&
	    VALcopy(&minprop, prop) != NULL)
		minbound = VALptr(&minprop);
	if ((prop = BATgetprop_nolock(b, GDK_MAX_BOUND)) != NULL &&
	    VALcopy(&maxprop, prop) != NULL)
		maxbound = VALptr(&maxprop);
	const bool notnull = BATgetprop_nolock(b, GDK_NOT_NULL) != NULL;
	MT_lock_unset(&b->theaplock);
	/* load hash so that we can maintain it */
	(void) BATcheckhash(b);
	MT_rwlock_wrlock(&b->thashlock);
	for (BUN i = 0; i < count; i++) {
		BUN p = autoincr ? positions[0] - b->hseqbase + i : positions[i] - b->hseqbase;
		const void *t = b->ttype && b->tvheap ?
			((const void **) values)[i] :
			(const void *) ((const char *) values + (i << b->tshift));
		bool isnil = atomnil && atomcmp(t, atomnil) == 0;
		if (notnull && isnil) {
			assert(0);
			GDKerror("NULL value not within bounds\n");
			MT_rwlock_wrunlock(&b->thashlock);
			goto bailout;
		} else if (!isnil &&
			   ((minbound &&
			     atomcmp(t, minbound) < 0) ||
			    (maxbound &&
			     atomcmp(t, maxbound) >= 0))) {
			assert(0);
			GDKerror("value not within bounds\n");
			MT_rwlock_wrunlock(&b->thashlock);
			goto bailout;
		}

		/* retrieve old value, but if this comes from the
		 * logger, we need to deal with offsets that point
		 * outside of the valid vheap */
		if (b->ttype == TYPE_void) {
			val = BUNtpos(bi, p);
		} else if (bi.type == TYPE_msk) {
			val = BUNtmsk(bi, p);
		} else if (b->tvheap) {
			size_t off = BUNtvaroff(bi, p);
			if (off < bi.vhfree)
				val = bi.vh->base + off;
			else
				val = NULL; /* bad offset */
		} else {
			val = BUNtloc(bi, p);
		}

		if (val) {
			if (atomcmp(val, t) == 0)
				continue; /* nothing to do */
			if (!isnil &&
			    b->tnil &&
			    atomcmp(val, atomnil) == 0) {
				/* if old value is nil and new value
				 * isn't, we're not sure anymore about
				 * the nil property, so we must clear
				 * it */
				MT_lock_set(&b->theaplock);
				b->tnil = false;
				MT_lock_unset(&b->theaplock);
			}
			if (b->ttype != TYPE_void) {
				if (bi.maxpos != BUN_NONE) {
					if (!isnil && atomcmp(BUNtail(bi, bi.maxpos), t) < 0) {
						/* new value is larger
						 * than previous
						 * largest */
						bi.maxpos = p;
					} else if (bi.maxpos == p && atomcmp(BUNtail(bi, bi.maxpos), t) != 0) {
						/* old value is equal to
						 * largest and new value
						 * is smaller or nil (see
						 * above), so we don't
						 * know anymore which is
						 * the largest */
						bi.maxpos = BUN_NONE;
					}
				}
				if (bi.minpos != BUN_NONE) {
					if (!isnil && atomcmp(BUNtail(bi, bi.minpos), t) > 0) {
						/* new value is smaller
						 * than previous
						 * smallest */
						bi.minpos = p;
					} else if (bi.minpos == p && atomcmp(BUNtail(bi, bi.minpos), t) != 0) {
						/* old value is equal to
						 * smallest and new value
						 * is larger or nil (see
						 * above), so we don't
						 * know anymore which is
						 * the largest */
						bi.minpos = BUN_NONE;
					}
				}
			}
			HASHdelete_locked(&bi, p, val);	/* first delete old value from hash */
		} else {
			/* out of range old value, so the properties and
			 * hash cannot be trusted */
			PROPdestroy(b);
			Hash *hs = b->thash;
			if (hs) {
				b->thash = NULL;
				doHASHdestroy(b, hs);
			}
			MT_lock_set(&b->theaplock);
			bi.minpos = BUN_NONE;
			bi.maxpos = BUN_NONE;
			b->tunique_est = 0.0;
			MT_lock_unset(&b->theaplock);
		}
		OIDXdestroy(b);
		STRMPdestroy(b);
		RTREEdestroy(b);

		if (b->tvheap && b->ttype) {
			var_t _d;
			ptr _ptr;
			_ptr = BUNtloc(bi, p);
			switch (b->twidth) {
			case 1:
				_d = (var_t) * (uint8_t *) _ptr + GDK_VAROFFSET;
				break;
			case 2:
				_d = (var_t) * (uint16_t *) _ptr + GDK_VAROFFSET;
				break;
			case 4:
				_d = (var_t) * (uint32_t *) _ptr;
				break;
#if SIZEOF_VAR_T == 8
			case 8:
				_d = (var_t) * (uint64_t *) _ptr;
				break;
#endif
			default:
				MT_UNREACHABLE();
			}
			MT_lock_set(&b->theaplock);
			if (ATOMreplaceVAR(b, &_d, t) != GDK_SUCCEED) {
				MT_lock_unset(&b->theaplock);
				MT_rwlock_wrunlock(&b->thashlock);
				goto bailout;
			}
			MT_lock_unset(&b->theaplock);
			if (b->twidth < SIZEOF_VAR_T &&
			    (b->twidth <= 2 ? _d - GDK_VAROFFSET : _d) >= ((size_t) 1 << (8 << b->tshift))) {
				/* doesn't fit in current heap, upgrade it */
				if (GDKupgradevarheap(b, _d, 0, bi.count) != GDK_SUCCEED) {
					MT_rwlock_wrunlock(&b->thashlock);
					goto bailout;
				}
			}
			/* reinitialize iterator after possible heap upgrade */
			{
				/* save and restore minpos/maxpos */
				BUN minpos = bi.minpos;
				BUN maxpos = bi.maxpos;
				bi = bat_iterator_nolock(b);
				bi.minpos = minpos;
				bi.maxpos = maxpos;
			}
			_ptr = BUNtloc(bi, p);
			switch (b->twidth) {
			case 1:
				* (uint8_t *) _ptr = (uint8_t) (_d - GDK_VAROFFSET);
				break;
			case 2:
				* (uint16_t *) _ptr = (uint16_t) (_d - GDK_VAROFFSET);
				break;
			case 4:
				* (uint32_t *) _ptr = (uint32_t) _d;
				break;
#if SIZEOF_VAR_T == 8
			case 8:
				* (uint64_t *) _ptr = (uint64_t) _d;
				break;
#endif
			default:
				MT_UNREACHABLE();
			}
		} else if (ATOMstorage(b->ttype) == TYPE_msk) {
			mskSetVal(b, p, * (msk *) t);
		} else {
			assert(BATatoms[b->ttype].atomPut == NULL);
			switch (ATOMsize(b->ttype)) {
			case 0:	     /* void */
				break;
			case 1:
				((bte *) b->theap->base)[p] = * (bte *) t;
				break;
			case 2:
				((sht *) b->theap->base)[p] = * (sht *) t;
				break;
			case 4:
				((int *) b->theap->base)[p] = * (int *) t;
				break;
			case 8:
				((lng *) b->theap->base)[p] = * (lng *) t;
				break;
			case 16:
#ifdef HAVE_HGE
				((hge *) b->theap->base)[p] = * (hge *) t;
#else
				((uuid *) b->theap->base)[p] = * (uuid *) t;
#endif
				break;
			default:
				memcpy(BUNtloc(bi, p), t, ATOMsize(b->ttype));
				break;
			}
		}

		HASHinsert_locked(&bi, p, t);	/* insert new value into hash */

		prv = p > 0 ? p - 1 : BUN_NONE;
		nxt = p < last ? p + 1 : BUN_NONE;

		MT_lock_set(&b->theaplock);
		if (b->tsorted) {
			if (prv != BUN_NONE &&
			    atomcmp(t, BUNtail(bi, prv)) < 0) {
				b->tsorted = false;
				b->tnosorted = p;
			} else if (nxt != BUN_NONE &&
				   atomcmp(t, BUNtail(bi, nxt)) > 0) {
				b->tsorted = false;
				b->tnosorted = nxt;
			} else if (b->ttype != TYPE_void && BATtdense(b)) {
				if (prv != BUN_NONE &&
				    1 + * (oid *) BUNtloc(bi, prv) != * (oid *) t) {
					b->tseqbase = oid_nil;
				} else if (nxt != BUN_NONE &&
					   * (oid *) BUNtloc(bi, nxt) != 1 + * (oid *) t) {
					b->tseqbase = oid_nil;
				} else if (prv == BUN_NONE &&
					   nxt == BUN_NONE) {
					b->tseqbase = * (oid *) t;
				}
			}
		} else if (b->tnosorted == p || b->tnosorted == p + 1)
			b->tnosorted = 0;
		if (b->trevsorted) {
			if (prv != BUN_NONE &&
			    atomcmp(t, BUNtail(bi, prv)) > 0) {
				b->trevsorted = false;
				b->tnorevsorted = p;
			} else if (nxt != BUN_NONE &&
				   atomcmp(t, BUNtail(bi, nxt)) < 0) {
				b->trevsorted = false;
				b->tnorevsorted = nxt;
			}
		} else if (b->tnorevsorted == p || b->tnorevsorted == p + 1)
			b->tnorevsorted = 0;
		if (((b->ttype != TYPE_void) & b->tkey) && b->batCount > 1) {
			BATkey(b, false);
		} else if (!b->tkey && (b->tnokey[0] == p || b->tnokey[1] == p))
			b->tnokey[0] = b->tnokey[1] = 0;
		if (b->tnonil && ATOMstorage(b->ttype) != TYPE_msk)
			b->tnonil = t && atomcmp(t, atomnil) != 0;
		MT_lock_unset(&b->theaplock);
	}
	BUN nunique = b->thash ? b->thash->nunique : 0;
	MT_rwlock_wrunlock(&b->thashlock);
	MT_lock_set(&b->theaplock);
	if (nunique != 0) {
		b->tunique_est = (double) nunique;
		if (nunique == b->batCount && !b->tkey)
			BATkey(b, true);
	} else if (b->tkey)
		b->tunique_est = (double) b->batCount;
	b->tminpos = bi.minpos;
	b->tmaxpos = bi.maxpos;
	b->theap->dirty = true;
	if (b->tvheap)
		b->tvheap->dirty = true;
	MT_lock_unset(&b->theaplock);

	return GDK_SUCCEED;

  bailout:
	if (minbound)
		VALclear(&minprop);
	if (maxbound)
		VALclear(&maxprop);
	return GDK_FAIL;
}

/* Replace multiple values given by their positions with the given values. */
gdk_return
BUNreplacemulti(BAT *b, const oid *positions, const void *values, BUN count, bool force)
{
	BATcheck(b, GDK_FAIL);

	if (b->ttype == TYPE_void && BATmaterialize(b, BUN_NONE) != GDK_SUCCEED)
		return GDK_FAIL;

	return BUNinplacemulti(b, positions, values, count, force, false);
}

/* Replace multiple values starting from a given position with the given
 * values. */
gdk_return
BUNreplacemultiincr(BAT *b, oid position, const void *values, BUN count, bool force)
{
	BATcheck(b, GDK_FAIL);

	if (b->ttype == TYPE_void && BATmaterialize(b, BUN_NONE) != GDK_SUCCEED)
		return GDK_FAIL;

	return BUNinplacemulti(b, &position, values, count, force, true);
}

gdk_return
BUNreplace(BAT *b, oid id, const void *t, bool force)
{
	return BUNreplacemulti(b, &id, b->ttype && b->tvheap ? (const void *) &t : t, 1, force);
}

/* very much like BUNreplace, but this doesn't make any changes if the
 * tail column is void */
gdk_return
void_inplace(BAT *b, oid id, const void *val, bool force)
{
	assert(id >= b->hseqbase && id < b->hseqbase + BATcount(b));
	if (id < b->hseqbase || id >= b->hseqbase + BATcount(b)) {
		GDKerror("id out of range\n");
		return GDK_FAIL;
	}
	if (b->ttype == TYPE_void)
		return GDK_SUCCEED;
	return BUNinplacemulti(b, &id, b->ttype && b->tvheap ? (const void *) &val : (const void *) val, 1, force, false);
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
	int (*cmp)(const void *, const void *) = ATOMcompare(bi.type);

	BATloop(b, p, q) {
		if ((*cmp)(v, BUNtail(bi, p)) == 0) {
			bat_iterator_end(&bi);
			return p;
		}
	}
	bat_iterator_end(&bi);
	return BUN_NONE;
}

static BUN
mskfnd(BAT *b, msk v)
{
	BUN p, q;

	if (v) {
		/* find a 1 value */
		for (p = 0, q = (BATcount(b) + 31) / 32; p < q; p++) {
			if (((uint32_t *) b->theap->base)[p] != 0) {
				/* there's at least one 1 bit */
				return p * 32 + candmask_lobit(((uint32_t *) b->theap->base)[p]);
			}
		}
	} else {
		/* find a 0 value */
		for (p = 0, q = (BATcount(b) + 31) / 32; p < q; p++) {
			if (((uint32_t *) b->theap->base)[p] != ~0U) {
				/* there's at least one 0 bit */
				return p * 32 + candmask_lobit(~((uint32_t *) b->theap->base)[p]);
			}
		}
	}
	return BUN_NONE;
}

BUN
BUNfnd(BAT *b, const void *v)
{
	BUN r = BUN_NONE;
	BATiter bi;

	BATcheck(b, BUN_NONE);
	if (!v || BATcount(b) == 0)
		return r;
	if (complex_cand(b)) {
		struct canditer ci;
		canditer_init(&ci, NULL, b);
		return canditer_search(&ci, * (const oid *) v, false);
	}
	if (BATtvoid(b))
		return BUNfndVOID(b, v);
	if (ATOMstorage(b->ttype) == TYPE_msk) {
		return mskfnd(b, *(msk*)v);
	}
	if (!BATcheckhash(b)) {
		if (BATordered(b) || BATordered_rev(b))
			return SORTfnd(b, v);
	}
	if (BAThash(b) == GDK_SUCCEED) {
		bi = bat_iterator(b); /* outside of hashlock */
		MT_rwlock_rdlock(&b->thashlock);
		if (b->thash == NULL) {
			MT_rwlock_rdunlock(&b->thashlock);
			bat_iterator_end(&bi);
			goto hashfnd_failed;
		}
		switch (ATOMbasetype(bi.type)) {
		case TYPE_bte:
			HASHloop_bte(bi, b->thash, r, v)
				break;
			break;
		case TYPE_sht:
			HASHloop_sht(bi, b->thash, r, v)
				break;
			break;
		case TYPE_int:
			HASHloop_int(bi, b->thash, r, v)
				break;
			break;
		case TYPE_flt:
			HASHloop_flt(bi, b->thash, r, v)
				break;
			break;
		case TYPE_dbl:
			HASHloop_dbl(bi, b->thash, r, v)
				break;
			break;
		case TYPE_lng:
			HASHloop_lng(bi, b->thash, r, v)
				break;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			HASHloop_hge(bi, b->thash, r, v)
				break;
			break;
#endif
		case TYPE_uuid:
			HASHloop_uuid(bi, b->thash, r, v)
				break;
			break;
		case TYPE_str:
			HASHloop_str(bi, b->thash, r, v)
				break;
			break;
		default:
			HASHloop(bi, b->thash, r, v)
				break;
			break;
		}
		MT_rwlock_rdunlock(&b->thashlock);
		bat_iterator_end(&bi);
		return r;
	}
  hashfnd_failed:
	/* can't build hash table, search the slow way */
	GDKclrerr();
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

/* Set the batCount value for the bat and also set some dependent
 * properties.  This function should be called only when it is save from
 * concurrent use (e.g. when theaplock is being held). */
void
BATsetcount(BAT *b, BUN cnt)
{
	/* head column is always VOID, and some head properties never change */
	assert(!is_oid_nil(b->hseqbase));
	assert(cnt <= BUN_MAX);

	b->batCount = cnt;
	if (b->theap->parentid == b->batCacheid) {
		b->theap->dirty |= b->ttype != TYPE_void && cnt > 0;
		b->theap->free = tailsize(b, cnt);
	}
	if (b->ttype == TYPE_void)
		b->batCapacity = cnt;
	if (cnt <= 1) {
		b->tsorted = b->trevsorted = ATOMlinear(b->ttype);
		b->tnosorted = b->tnorevsorted = 0;
	}
	/* if the BAT was made smaller, we need to zap some values */
	if (b->tnosorted >= BATcount(b))
		b->tnosorted = 0;
	if (b->tnorevsorted >= BATcount(b))
		b->tnorevsorted = 0;
	if (b->tnokey[0] >= BATcount(b) || b->tnokey[1] >= BATcount(b)) {
		b->tnokey[0] = 0;
		b->tnokey[1] = 0;
	}
	if (b->ttype == TYPE_void) {
		b->tsorted = true;
		if (is_oid_nil(b->tseqbase)) {
			b->tkey = cnt <= 1;
			b->trevsorted = true;
			b->tnil = true;
			b->tnonil = false;
		} else {
			b->tkey = true;
			b->trevsorted = cnt <= 1;
			b->tnil = false;
			b->tnonil = true;
		}
	}
	assert(b->batCapacity >= cnt);
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
BATkey(BAT *b, bool flag)
{
	BATcheck(b, GDK_FAIL);
	if (b->ttype == TYPE_void) {
		if (BATtdense(b) && !flag) {
			GDKerror("dense column must be unique.\n");
			return GDK_FAIL;
		}
		if (is_oid_nil(b->tseqbase) && flag && b->batCount > 1) {
			GDKerror("void column cannot be unique.\n");
			return GDK_FAIL;
		}
	}
	b->tkey = flag;
	if (!flag) {
		b->tseqbase = oid_nil;
	} else {
		b->tnokey[0] = b->tnokey[1] = 0;
		b->tunique_est = (double) b->batCount;
	}
	gdk_return rc = GDK_SUCCEED;
	if (flag && VIEWtparent(b)) {
		/* if a view is key, then so is the parent if the two
		 * are aligned */
		BAT *bp = BATdescriptor(VIEWtparent(b));
		if (bp != NULL) {
			MT_lock_set(&bp->theaplock);
			if (BATcount(b) == BATcount(bp) &&
			    ATOMtype(BATttype(b)) == ATOMtype(BATttype(bp)) &&
			    !BATtkey(bp) &&
			    ((BATtvoid(b) && BATtvoid(bp) && b->tseqbase == bp->tseqbase) ||
			     BATcount(b) == 0))
				rc = BATkey(bp, true);
			MT_lock_unset(&bp->theaplock);
			BBPunfix(bp->batCacheid);
		}
	}
	return rc;
}

void
BAThseqbase(BAT *b, oid o)
{
	if (b != NULL) {
		assert(o <= GDK_oid_max);	/* i.e., not oid_nil */
		assert(o + BATcount(b) <= GDK_oid_max);
		b->hseqbase = o;
	}
}

void
BATtseqbase(BAT *b, oid o)
{
	assert(o <= oid_nil);
	if (b == NULL)
		return;
	assert(is_oid_nil(o) || o + BATcount(b) <= GDK_oid_max);
	if (ATOMtype(b->ttype) == TYPE_oid) {
		b->tseqbase = o;

		/* adapt keyness */
		if (BATtvoid(b)) {
			b->tsorted = true;
			if (is_oid_nil(o)) {
				b->tkey = b->batCount <= 1;
				b->tnonil = b->batCount == 0;
				b->tnil = b->batCount > 0;
				b->trevsorted = true;
				b->tnosorted = b->tnorevsorted = 0;
				if (!b->tkey) {
					b->tnokey[0] = 0;
					b->tnokey[1] = 1;
				} else {
					b->tnokey[0] = b->tnokey[1] = 0;
				}
			} else {
				if (!b->tkey) {
					b->tkey = true;
					b->tnokey[0] = b->tnokey[1] = 0;
				}
				b->tnonil = true;
				b->tnil = false;
				b->trevsorted = b->batCount <= 1;
				if (!b->trevsorted)
					b->tnorevsorted = 1;
				b->tunique_est = (double) b->batCount;
			}
		}
	} else {
		assert(o == oid_nil);
		b->tseqbase = oid_nil;
	}
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
 * See dirty_bat() in BBPsync() -- gdk_bbp.c and epilogue() in
 * gdk_tm.c.
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
backup_new(Heap *hp, bool lock)
{
	int batret, bakret, ret = -1;
	char batpath[MAXPATH], bakpath[MAXPATH];
	struct stat st;

	char *bak_filename = NULL;
	if ((bak_filename = strrchr(hp->filename, DIR_SEP)) != NULL)
		bak_filename++;
	else
		bak_filename = hp->filename;
	/* check for an existing X.new in BATDIR, BAKDIR and SUBDIR */
	if (GDKfilepath(batpath, sizeof(batpath), hp->farmid, BATDIR, hp->filename, "new") == GDK_SUCCEED &&
	    GDKfilepath(bakpath, sizeof(bakpath), hp->farmid, BAKDIR, bak_filename, "new") == GDK_SUCCEED) {
		/* file actions here interact with the global commits */
		if (lock)
			BBPtmlock();

		batret = MT_stat(batpath, &st);
		bakret = MT_stat(bakpath, &st);

		if (batret == 0 && bakret) {
			/* no backup yet, so move the existing X.new there out
			 * of the way */
			if ((ret = MT_rename(batpath, bakpath)) < 0)
				GDKsyserror("backup_new: rename %s to %s failed\n",
					    batpath, bakpath);
			TRC_DEBUG(IO, "rename(%s,%s) = %d\n", batpath, bakpath, ret);
		} else if (batret == 0) {
			/* there is a backup already; just remove the X.new */
			if ((ret = MT_remove(batpath)) != 0)
				GDKsyserror("backup_new: remove %s failed\n", batpath);
			TRC_DEBUG(IO, "remove(%s) = %d\n", batpath, ret);
		} else {
			ret = 0;
		}
		if (lock)
			BBPtmunlock();
	}
	return ret ? GDK_FAIL : GDK_SUCCEED;
}

#define ACCESSMODE(wr,rd) ((wr)?BAT_WRITE:(rd)?BAT_READ:-1)

/* transition heap from readonly to writable */
static storage_t
HEAPchangeaccess(Heap *hp, int dstmode, bool existing)
{
	if (hp->base == NULL || hp->newstorage == STORE_MEM || !existing || dstmode == -1)
		return hp->newstorage;	/* 0<=>2,1<=>3,a<=>b */

	if (dstmode == BAT_WRITE) {
		if (hp->storage != STORE_PRIV)
			hp->dirty = true;	/* exception c does not make it dirty */
		return STORE_PRIV;	/* 4=>6,5=>7,c=>6 persistent BAT_WRITE needs STORE_PRIV */
	}
	if (hp->storage == STORE_MMAP) {	/* 6=>4 */
		hp->dirty = true;
		return backup_new(hp, true) != GDK_SUCCEED ? STORE_INVALID : STORE_MMAP;	/* only called for existing bats */
	}
	return hp->storage;	/* 7=>5 */
}

/* heap changes persistence mode (at commit point) */
static storage_t
HEAPcommitpersistence(Heap *hp, bool writable, bool existing)
{
	if (existing) {		/* existing, ie will become transient */
		if (hp->storage == STORE_MMAP && hp->newstorage == STORE_PRIV && writable) {	/* 6=>2 */
			hp->dirty = true;
			return backup_new(hp, false) != GDK_SUCCEED ? STORE_INVALID : STORE_MMAP;	/* only called for existing bats */
		}
		return hp->newstorage;	/* 4=>0,5=>1,7=>3,c=>a no change */
	}
	/* !existing, ie will become persistent */
	if (hp->newstorage == STORE_MEM)
		return hp->newstorage;
	if (hp->newstorage == STORE_MMAP && !writable)
		return STORE_MMAP;	/* 0=>4 STORE_MMAP */

	if (hp->newstorage == STORE_MMAP)
		hp->dirty = true;	/* 2=>6 */
	return STORE_PRIV;	/* 1=>5,2=>6,3=>7,a=>c,b=>6 states */
}


#define ATOMappendpriv(t, h) (ATOMstorage(t) != TYPE_str /*|| GDK_ELIMDOUBLES(h) */)

/* change the heap modes at a commit */
gdk_return
BATcheckmodes(BAT *b, bool existing)
{
	storage_t m1 = STORE_MEM, m3 = STORE_MEM;
	bool dirty = false, wr;

	BATcheck(b, GDK_FAIL);

	wr = (b->batRestricted == BAT_WRITE);
	if (b->ttype) {
		m1 = HEAPcommitpersistence(b->theap, wr, existing);
		dirty |= (b->theap->newstorage != m1);
	}

	if (b->tvheap) {
		bool ta = (b->batRestricted == BAT_APPEND) && ATOMappendpriv(b->ttype, b->tvheap);
		m3 = HEAPcommitpersistence(b->tvheap, wr || ta, existing);
		dirty |= (b->tvheap->newstorage != m3);
	}
	if (m1 == STORE_INVALID || m3 == STORE_INVALID)
		return GDK_FAIL;

	if (dirty) {
		b->theap->newstorage = m1;
		if (b->tvheap)
			b->tvheap->newstorage = m3;
	}
	return GDK_SUCCEED;
}

BAT *
BATsetaccess(BAT *b, restrict_t newmode)
{
	restrict_t bakmode;

	BATcheck(b, NULL);
	if (newmode != BAT_READ &&
	    (isVIEW(b) || (ATOMIC_GET(&b->theap->refs) & HEAPREFS) > 1)) {
		BAT *bn = COLcopy(b, b->ttype, true, b->batRole);
		BBPunfix(b->batCacheid);
		if (bn == NULL)
			return NULL;
		b = bn;
	}
	MT_lock_set(&b->theaplock);
	bakmode = b->batRestricted;
	if (bakmode != newmode) {
		bool existing = (BBP_status(b->batCacheid) & BBPEXISTING) != 0;
		bool wr = (newmode == BAT_WRITE);
		bool rd = (bakmode == BAT_WRITE);
		storage_t m1 = STORE_MEM, m3 = STORE_MEM;
		storage_t b1 = STORE_MEM, b3 = STORE_MEM;

		if (b->theap->parentid == b->batCacheid) {
			b1 = b->theap->newstorage;
			m1 = HEAPchangeaccess(b->theap, ACCESSMODE(wr, rd), existing);
		}
		if (b->tvheap && b->tvheap->parentid == b->batCacheid) {
			bool ta = (newmode == BAT_APPEND && ATOMappendpriv(b->ttype, b->tvheap));
			b3 = b->tvheap->newstorage;
			m3 = HEAPchangeaccess(b->tvheap, ACCESSMODE(wr && ta, rd && ta), existing);
		}
		if (m1 == STORE_INVALID || m3 == STORE_INVALID) {
			MT_lock_unset(&b->theaplock);
			BBPunfix(b->batCacheid);
			return NULL;
		}

		/* set new access mode and mmap modes */
		b->batRestricted = newmode;
		if (b->theap->parentid == b->batCacheid)
			b->theap->newstorage = m1;
		if (b->tvheap && b->tvheap->parentid == b->batCacheid)
			b->tvheap->newstorage = m3;

		MT_lock_unset(&b->theaplock);
		if (existing && !isVIEW(b) && BBPsave(b) != GDK_SUCCEED) {
			/* roll back all changes */
			MT_lock_set(&b->theaplock);
			b->batRestricted = bakmode;
			b->theap->newstorage = b1;
			if (b->tvheap)
				b->tvheap->newstorage = b3;
			MT_lock_unset(&b->theaplock);
			BBPunfix(b->batCacheid);
			return NULL;
		}
	} else {
		MT_lock_unset(&b->theaplock);
	}
	return b;
}

restrict_t
BATgetaccess(BAT *b)
{
	BATcheck(b, BAT_WRITE);
	MT_lock_set(&b->theaplock);
	restrict_t restricted = b->batRestricted;
	MT_lock_unset(&b->theaplock);
	return restricted;
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
gdk_return
BATmode(BAT *b, bool transient)
{
	BATcheck(b, GDK_FAIL);

	/* can only make a bat PERSISTENT if its role is already
	 * PERSISTENT */
	assert(transient || b->batRole == PERSISTENT);
	/* cannot make a view PERSISTENT */
	assert(transient || !isVIEW(b));

	if (b->batRole == TRANSIENT && !transient) {
		GDKerror("cannot change mode of BAT in TRANSIENT farm.\n");
		return GDK_FAIL;
	}

	BATiter bi = bat_iterator(b);
	bool mustrelease = false;
	bool mustretain = false;
	bat bid = b->batCacheid;

	if (transient != bi.transient) {
		if (!transient) {
			if (ATOMisdescendant(b->ttype, TYPE_ptr)) {
				GDKerror("%s type implies that %s[%s] "
					 "cannot be made persistent.\n",
					 ATOMname(b->ttype), BATgetId(b),
					 ATOMname(b->ttype));
				bat_iterator_end(&bi);
				return GDK_FAIL;
			}
		}

		/* we need to delay the calls to BBPretain and
		 * BBPrelease until after we have released our reference
		 * to the heaps (i.e. until after bat_iterator_end),
		 * because in either case, BBPfree can be called (either
		 * directly here or in BBPtrim) which waits for the heap
		 * reference to come down.  BBPretain calls incref which
		 * waits until the trim that is waiting for us is done,
		 * so that causes deadlock, and BBPrelease can call
		 * BBPfree which causes deadlock with a single thread */
		if (!transient) {
			/* persistent BATs get a logical reference */
			mustretain = true;
		} else if (!bi.transient) {
			/* transient BATs loose their logical reference */
			mustrelease = true;
		}
		MT_lock_set(&GDKswapLock(bid));
		if (!transient) {
			if (BBP_status(bid) & BBPDELETED) {
				BBP_status_on(bid, BBPEXISTING);
				BBP_status_off(bid, BBPDELETED);
			} else
				BBP_status_on(bid, BBPNEW);
		} else if (!bi.transient) {
			if (!(BBP_status(bid) & BBPNEW))
				BBP_status_on(bid, BBPDELETED);
			BBP_status_off(bid, BBPPERSISTENT);
		}
		/* session bats or persistent bats that did not
		 * witness a commit yet may have been saved */
		MT_lock_set(&b->theaplock);
		if (b->batCopiedtodisk) {
			if (!transient) {
				BBP_status_off(bid, BBPTMP);
			} else {
				/* TMcommit must remove it to
				 * guarantee free space */
				BBP_status_on(bid, BBPTMP);
			}
		}
		b->batTransient = transient;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&GDKswapLock(bid));
	}
	bat_iterator_end(&bi);
	/* retain/release after bat_iterator_end because of refs to heaps */
	if (mustretain)
		BBPretain(bid);
	else if (mustrelease)
		BBPrelease(bid);
	return GDK_SUCCEED;
}

/* BATassertProps checks whether properties are set correctly.  Under
 * no circumstances will it change any properties.  Note that the
 * "nil" property is not actually used anywhere, but it is checked. */

#ifdef NDEBUG
/* assertions are disabled, turn failing tests into a message */
#undef assert
#define assert(test)	((void) ((test) || (TRC_CRITICAL(CHECK, "Assertion `%s' failed\n", #test), 0)))
#endif

static void
assert_ascii(const char *s)
{
	if (!strNil(s)) {
		while (*s) {
			assert((*s & 0x80) == 0);
			s++;
		}
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
 * seqbase	Only valid for TYPE_oid and TYPE_void columns: each
 *		value in the column is exactly one more than the
 *		previous value, starting at position 0 with the value
 *		stored in this property.
 *		This implies sorted, key, nonil (which therefore need
 *		to be set).
 * nil		There is at least one NIL value in the column.
 * nonil	There are no NIL values in the column.
 * key		All values in the column are distinct.
 * sorted	The column is sorted (ascending).  If also revsorted,
 *		then all values are equal.
 * revsorted	The column is reversely sorted (descending).  If
 *		also sorted, then all values are equal.
 * nosorted	BUN position which proofs not sorted (given position
 *		and one before are not ordered correctly).
 * norevsorted	BUN position which proofs not revsorted (given position
 *		and one before are not ordered correctly).
 * nokey	Pair of BUN positions that proof not all values are
 *		distinct (i.e. values at given locations are equal).
 * ascii	Only valid for TYPE_str columns: all strings in the column
 *		are ASCII, i.e. the UTF-8 encoding for all characters is a
 *		single byte.
 *
 * Note that the functions BATtseqbase and BATkey also set more
 * properties than you might suspect.  When setting properties on a
 * newly created and filled BAT, you may want to first make sure the
 * batCount is set correctly (e.g. by calling BATsetcount), then use
 * BATtseqbase and BATkey, and finally set the other properties.
 *
 * For a view, we cannot check all properties, since it is possible with
 * the way the SQL layer works, that a parent BAT gets changed, changing
 * the properties, while there is a view.  The view is supposed to look
 * at only the non-changing part of the BAT (through candidate lists),
 * but this means that the properties of the view might not be correct.
 * For this reason, for views, we skip all property checking that looks
 * at the BAT content.
 */

void
BATassertProps(BAT *b)
{
	unsigned bbpstatus;
	BUN p, q;
	int (*cmpf)(const void *, const void *);
	int cmp;
	const void *prev = NULL, *valp, *nilp;
	char filename[sizeof(b->theap->filename)];
	bool isview1, isview2;

	/* do the complete check within a lock */
	MT_lock_set(&b->theaplock);

	/* general BAT sanity */
	assert(b != NULL);
	assert(b->batCacheid > 0);
	assert(b->batCacheid < getBBPsize());
	assert(b == BBP_desc(b->batCacheid));
	assert(b->batCount >= b->batInserted);

	/* headless */
	assert(b->hseqbase <= GDK_oid_max); /* non-nil seqbase */
	assert(b->hseqbase + BATcount(b) <= GDK_oid_max);

	isview1 = b->theap->parentid != b->batCacheid;
	isview2 = b->tvheap && b->tvheap->parentid != b->batCacheid;

	bbpstatus = BBP_status(b->batCacheid);
	/* only at most one of BBPDELETED, BBPEXISTING, BBPNEW may be set */
	assert(((bbpstatus & BBPDELETED) != 0) +
	       ((bbpstatus & BBPEXISTING) != 0) +
	       ((bbpstatus & BBPNEW) != 0) <= 1);

	assert(b->ttype >= TYPE_void);
	assert(b->ttype < GDKatomcnt);
	assert(isview1 ||
	       b->ttype == TYPE_void ||
	       BBPfarms[b->theap->farmid].roles & (1 << b->batRole));
	assert(isview2 ||
	       b->tvheap == NULL ||
	       (BBPfarms[b->tvheap->farmid].roles & (1 << b->batRole)));

	cmpf = ATOMcompare(b->ttype);
	nilp = ATOMnilptr(b->ttype);

	assert(isview1 || b->theap->free >= tailsize(b, BATcount(b)));
	if (b->ttype != TYPE_void) {
		assert(b->batCount <= b->batCapacity);
		assert(isview1 || b->theap->size >= b->theap->free);
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			/* 32 values per 4-byte word (that's not the
			 * same as 8 values per byte...) */
			assert(isview1 || b->theap->size >= 4 * ((b->batCapacity + 31) / 32));
		} else
			assert(isview1 || b->theap->size >> b->tshift >= b->batCapacity);
	}
	if (!isview1) {
		strconcat_len(filename, sizeof(filename),
			      BBP_physical(b->theap->parentid),
			      b->ttype == TYPE_str ? b->twidth == 1 ? ".tail1" : b->twidth == 2 ? ".tail2" :
#if SIZEOF_VAR_T == 8
			      b->twidth == 4 ? ".tail4" :
#endif
			      ".tail" : ".tail",
			      NULL);
		assert(strcmp(b->theap->filename, filename) == 0);
	}
	if (!isview2 && b->tvheap) {
		strconcat_len(filename, sizeof(filename),
			      BBP_physical(b->tvheap->parentid),
			      ".theap",
			      NULL);
		assert(strcmp(b->tvheap->filename, filename) == 0);
	}

	/* void, str and blob imply varsized */
	if (ATOMstorage(b->ttype) == TYPE_str ||
	    ATOMstorage(b->ttype) == TYPE_blob)
		assert(b->tvheap != NULL);
	/* other "known" types are not varsized */
	if (ATOMstorage(b->ttype) > TYPE_void &&
	    ATOMstorage(b->ttype) < TYPE_str)
		assert(b->tvheap == NULL);
	/* shift and width have a particular relationship */
	if (ATOMstorage(b->ttype) == TYPE_str)
		assert(b->twidth >= 1 && b->twidth <= ATOMsize(b->ttype));
	else
		assert(b->twidth == ATOMsize(b->ttype));
	assert(b->tseqbase <= oid_nil);
	/* only oid/void columns can be dense */
	assert(is_oid_nil(b->tseqbase) || b->ttype == TYPE_oid || b->ttype == TYPE_void);
	/* a column cannot both have and not have NILs */
	assert(!b->tnil || !b->tnonil);
	/* only string columns can be ASCII */
	assert(!b->tascii || ATOMstorage(b->ttype) == TYPE_str);
	if (b->ttype == TYPE_void) {
		assert(b->tshift == 0);
		assert(b->twidth == 0);
		assert(b->tsorted);
		if (is_oid_nil(b->tseqbase)) {
			assert(b->tvheap == NULL);
			assert(BATcount(b) == 0 || !b->tnonil);
			assert(BATcount(b) <= 1 || !b->tkey);
			assert(b->trevsorted);
		} else {
			if (b->tvheap != NULL) {
				/* candidate list with exceptions */
				assert(b->batRole == TRANSIENT || b->batRole == SYSTRANS);
				assert(b->tvheap->free <= b->tvheap->size);
				assert(b->tvheap->free >= sizeof(ccand_t));
				assert((negoid_cand(b) && ccand_free(b) % SIZEOF_OID == 0) || mask_cand(b));
				if (negoid_cand(b) && ccand_free(b) > 0) {
					const oid *oids = (const oid *) ccand_first(b);
					q = ccand_free(b) / SIZEOF_OID;
					assert(oids != NULL);
					assert(b->tseqbase + BATcount(b) + q <= GDK_oid_max);
					/* exceptions within range */
					assert(oids[0] >= b->tseqbase);
					assert(oids[q - 1] < b->tseqbase + BATcount(b) + q);
					/* exceptions sorted */
					for (p = 1; p < q; p++)
						assert(oids[p - 1] < oids[p]);
				}
			}
			assert(b->tseqbase + b->batCount <= GDK_oid_max);
			assert(BATcount(b) == 0 || !b->tnil);
			assert(BATcount(b) <= 1 || !b->trevsorted);
			assert(b->tkey);
			assert(b->tnonil);
		}
		MT_lock_unset(&b->theaplock);
		return;
	}

	BATiter bi  = bat_iterator_nolock(b);

	if (BATtdense(b)) {
		assert(b->tseqbase + b->batCount <= GDK_oid_max);
		assert(b->ttype == TYPE_oid);
		assert(b->tsorted);
		assert(b->tkey);
		assert(b->tnonil);
		if ((q = b->batCount) != 0) {
			const oid *o = (const oid *) Tloc(b, 0);
			assert(*o == b->tseqbase);
			for (p = 1; p < q; p++)
				assert(o[p - 1] + 1 == o[p]);
		}
		MT_lock_unset(&b->theaplock);
		return;
	}
	assert(1 << b->tshift == b->twidth);
	/* only linear atoms can be sorted */
	assert(!b->tsorted || ATOMlinear(b->ttype));
	assert(!b->trevsorted || ATOMlinear(b->ttype));
	if (ATOMlinear(b->ttype)) {
		assert(b->tnosorted == 0 ||
		       (b->tnosorted > 0 &&
			b->tnosorted < b->batCount));
		assert(!b->tsorted || b->tnosorted == 0);
		if (!isview1 &&
		    !isview2 &&
		    !b->tsorted &&
		    b->tnosorted > 0 &&
		    b->tnosorted < b->batCount)
			assert(cmpf(BUNtail(bi, b->tnosorted - 1),
				    BUNtail(bi, b->tnosorted)) > 0);
		assert(b->tnorevsorted == 0 ||
		       (b->tnorevsorted > 0 &&
			b->tnorevsorted < b->batCount));
		assert(!b->trevsorted || b->tnorevsorted == 0);
		if (!isview1 &&
		    !isview2 &&
		    !b->trevsorted &&
		    b->tnorevsorted > 0 &&
		    b->tnorevsorted < b->batCount)
			assert(cmpf(BUNtail(bi, b->tnorevsorted - 1),
				    BUNtail(bi, b->tnorevsorted)) < 0);
	}
	/* if tkey property set, both tnokey values must be 0 */
	assert(!b->tkey || (b->tnokey[0] == 0 && b->tnokey[1] == 0));
	if (!isview1 &&
	    !isview2 &&
	    !b->tkey &&
	    (b->tnokey[0] != 0 || b->tnokey[1] != 0)) {
		/* if tkey not set and tnokey indicates a proof of
		 * non-key-ness, make sure the tnokey values are in
		 * range and indeed provide a proof */
		assert(b->tnokey[0] != b->tnokey[1]);
		assert(b->tnokey[0] < b->batCount);
		assert(b->tnokey[1] < b->batCount);
		assert(cmpf(BUNtail(bi, b->tnokey[0]),
			    BUNtail(bi, b->tnokey[1])) == 0);
	}
	/* var heaps must have sane sizes */
	assert(b->tvheap == NULL || b->tvheap->free <= b->tvheap->size);

	if (!b->tkey && !b->tsorted && !b->trevsorted &&
	    !b->tnonil && !b->tnil) {
		/* nothing more to prove */
		MT_lock_unset(&b->theaplock);
		return;
	}

	/* only do a scan if the bat is not a view */
	if (!isview1 && !isview2) {
		const ValRecord *prop;
		const void *maxval = NULL;
		const void *minval = NULL;
		const void *maxbound = NULL;
		const void *minbound = NULL;
		const bool notnull = BATgetprop_nolock(b, GDK_NOT_NULL) != NULL;
		bool seenmax = false, seenmin = false;
		bool seennil = false;

		if ((prop = BATgetprop_nolock(b, GDK_MAX_BOUND)) != NULL)
			maxbound = VALptr(prop);
		if ((prop = BATgetprop_nolock(b, GDK_MIN_BOUND)) != NULL)
			minbound = VALptr(prop);
		if (b->tmaxpos != BUN_NONE) {
			assert(b->tmaxpos < BATcount(b));
			maxval = BUNtail(bi, b->tmaxpos);
			assert(cmpf(maxval, nilp) != 0);
		}
		if (b->tminpos != BUN_NONE) {
			assert(b->tminpos < BATcount(b));
			minval = BUNtail(bi, b->tminpos);
			assert(cmpf(minval, nilp) != 0);
		}
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			/* for now, don't do extra checks for bit mask */
			;
		} else if (b->tsorted || b->trevsorted || !b->tkey) {
			/* if sorted (either way), or we don't have to
			 * prove uniqueness, we can do a simple
			 * scan */
			/* only call compare function if we have to */
			bool cmpprv = b->tsorted | b->trevsorted | b->tkey;

			BATloop(b, p, q) {
				valp = BUNtail(bi, p);
				bool isnil = cmpf(valp, nilp) == 0;
				assert(!isnil || !notnull);
				assert(!b->tnonil || !isnil);
				assert(b->ttype != TYPE_flt || !isinf(*(flt*)valp));
				assert(b->ttype != TYPE_dbl || !isinf(*(dbl*)valp));
				if (b->tascii)
					assert_ascii(valp);
				if (minbound && !isnil) {
					cmp = cmpf(minbound, valp);
					assert(cmp <= 0);
				}
				if (maxbound && !isnil) {
					cmp = cmpf(maxbound, valp);
					assert(cmp > 0);
				}
				if (maxval && !isnil) {
					cmp = cmpf(maxval, valp);
					assert(cmp >= 0);
					seenmax |= cmp == 0;
				}
				if (minval && !isnil) {
					cmp = cmpf(minval, valp);
					assert(cmp <= 0);
					seenmin |= cmp == 0;
				}
				if (prev && cmpprv) {
					cmp = cmpf(prev, valp);
					assert(!b->tsorted || cmp <= 0);
					assert(!b->trevsorted || cmp >= 0);
					assert(!b->tkey || cmp != 0);
				}
				seennil |= isnil;
				if (seennil && !cmpprv &&
				    maxval == NULL && minval == NULL &&
				    minbound == NULL && maxbound == NULL) {
					/* we've done all the checking
					 * we can do */
					break;
				}
				prev = valp;
			}
		} else {	/* b->tkey && !b->tsorted && !b->trevsorted */
			/* we need to check for uniqueness the hard
			 * way (i.e. using a hash table) */
			const char *nme = BBP_physical(b->batCacheid);
			Hash *hs = NULL;
			BUN mask;

			if ((hs = GDKzalloc(sizeof(Hash))) == NULL) {
				TRC_WARNING(BAT, "Cannot allocate hash table\n");
				goto abort_check;
			}
			if (snprintf(hs->heaplink.filename, sizeof(hs->heaplink.filename), "%s.thshprpl%x", nme, (unsigned) MT_getpid()) >= (int) sizeof(hs->heaplink.filename) ||
			    snprintf(hs->heapbckt.filename, sizeof(hs->heapbckt.filename), "%s.thshprpb%x", nme, (unsigned) MT_getpid()) >= (int) sizeof(hs->heapbckt.filename)) {
				/* cannot happen, see comment in gdk.h
				 * about sizes near definition of
				 * BBPINIT */
				GDKfree(hs);
				TRC_CRITICAL(BAT, "Heap filename is too large\n");
				goto abort_check;
			}
			if (ATOMsize(b->ttype) == 1)
				mask = (BUN) 1 << 8;
			else if (ATOMsize(b->ttype) == 2)
				mask = (BUN) 1 << 16;
			else
				mask = HASHmask(b->batCount);
			hs->heapbckt.parentid = b->batCacheid;
			hs->heaplink.parentid = b->batCacheid;
			if ((hs->heaplink.farmid = BBPselectfarm(
				     TRANSIENT, b->ttype, hashheap)) < 0 ||
			    (hs->heapbckt.farmid = BBPselectfarm(
				    TRANSIENT, b->ttype, hashheap)) < 0 ||
			    HASHnew(hs, b->ttype, BATcount(b),
				    mask, BUN_NONE, false) != GDK_SUCCEED) {
				GDKfree(hs);
				TRC_WARNING(BAT, "Cannot allocate hash table\n");
				goto abort_check;
			}
			BATloop(b, p, q) {
				BUN hb;
				BUN prb;
				valp = BUNtail(bi, p);
				bool isnil = cmpf(valp, nilp) == 0;
				assert(!isnil || !notnull);
				assert(b->ttype != TYPE_flt || !isinf(*(flt*)valp));
				assert(b->ttype != TYPE_dbl || !isinf(*(dbl*)valp));
				if (b->tascii)
					assert_ascii(valp);
				if (minbound && !isnil) {
					cmp = cmpf(minbound, valp);
					assert(cmp <= 0);
				}
				if (maxbound && !isnil) {
					cmp = cmpf(maxbound, valp);
					assert(cmp > 0);
				}
				if (maxval && !isnil) {
					cmp = cmpf(maxval, valp);
					assert(cmp >= 0);
					seenmax |= cmp == 0;
				}
				if (minval && !isnil) {
					cmp = cmpf(minval, valp);
					assert(cmp <= 0);
					seenmin |= cmp == 0;
				}
				prb = HASHprobe(hs, valp);
				for (hb = HASHget(hs, prb);
				     hb != BUN_NONE;
				     hb = HASHgetlink(hs, hb))
					if (cmpf(valp, BUNtail(bi, hb)) == 0)
						assert(!b->tkey);
				HASHputlink(hs, p, HASHget(hs, prb));
				HASHput(hs, prb, p);
				assert(!b->tnonil || !isnil);
				seennil |= isnil;
			}
			HEAPfree(&hs->heaplink, true);
			HEAPfree(&hs->heapbckt, true);
			GDKfree(hs);
		}
	  abort_check:
		GDKclrerr();
		assert(maxval == NULL || seenmax);
		assert(minval == NULL || seenmin);
		assert(!b->tnil || seennil);
	}
	MT_lock_unset(&b->theaplock);
}
