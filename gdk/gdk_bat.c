/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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

static char *BATstring_t = "t";

#define default_ident(s)	((s) == BATstring_t)

void
BATinit_idents(BAT *bn)
{
	bn->tident = BATstring_t;
}

BAT *
BATcreatedesc(oid hseq, int tt, bool heapnames, role_t role, uint16_t width)
{
	BAT *bn;

	/*
	 * Alloc space for the BAT and its dependent records.
	 */
	assert(tt >= 0);

	bn = GDKmalloc(sizeof(BAT));

	if (bn == NULL)
		return NULL;

	/*
	 * Fill in basic column info
	 */
	*bn = (BAT) {
		.hseqbase = hseq,

		.ttype = tt,
		.tkey = false,
		.tnonil = true,
		.tnil = false,
		.tsorted = ATOMlinear(tt),
		.trevsorted = ATOMlinear(tt),
		.tident = BATstring_t,
		.tseqbase = oid_nil,
		.tminpos = BUN_NONE,
		.tmaxpos = BUN_NONE,
		.tunique_est = 0.0,

		.batRole = role,
		.batTransient = true,
		.batRestricted = BAT_WRITE,
	};
	if (heapnames && (bn->theap = GDKmalloc(sizeof(Heap))) == NULL) {
		GDKfree(bn);
		return NULL;
	}

	/*
	 * add to BBP
	 */
	if (BBPinsert(bn) == 0) {
		GDKfree(bn->theap);
		GDKfree(bn);
		return NULL;
	}
	/*
	 * fill in heap names, so HEAPallocs can resort to disk for
	 * very large writes.
	 */
	if (heapnames) {
		assert(bn->theap != NULL);
		*bn->theap = (Heap) {
			.parentid = bn->batCacheid,
			.farmid = BBPselectfarm(role, bn->ttype, offheap),
			.dirty = true,
		};

		const char *nme = BBP_physical(bn->batCacheid);
		settailname(bn->theap, nme, tt, width);

		if (ATOMneedheap(tt)) {
			if ((bn->tvheap = GDKmalloc(sizeof(Heap))) == NULL) {
				BBPclear(bn->batCacheid, true);
				HEAPfree(bn->theap, true);
				GDKfree(bn->theap);
				GDKfree(bn);
				return NULL;
			}
			*bn->tvheap = (Heap) {
				.parentid = bn->batCacheid,
				.farmid = BBPselectfarm(role, bn->ttype, varheap),
				.dirty = true,
			};
			ATOMIC_INIT(&bn->tvheap->refs, 1);
			strconcat_len(bn->tvheap->filename,
				      sizeof(bn->tvheap->filename),
				      nme, ".theap", NULL);
		}
		ATOMIC_INIT(&bn->theap->refs, 1);
	} else {
		assert(bn->theap == NULL);
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
	b->tvarsized = b->ttype == TYPE_void || BATatoms[b->ttype].atomPut != NULL;
}

const char *
gettailname(const BAT *b)
{
	if (b->ttype == TYPE_str) {
		switch (b->twidth) {
		case 1:
			return "tail1";
		case 2:
			return "tail2";
#if SIZEOF_VAR_T == 8
		case 4:
			return "tail4";
#endif
		default:
			break;
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
#if SIZEOF_VAR_T == 8
		case 4:
			strconcat_len(tail->filename,
				      sizeof(tail->filename), physnme,
				      ".tail4", NULL);
			return;
#endif
		default:
			break;
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
	assert(tt != TYPE_bat);
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
	if (tt && HEAPalloc(bn->theap, cap, bn->twidth, ATOMsize(bn->ttype)) != GDK_SUCCEED) {
		goto bailout;
	}

	if (bn->tvheap && width == 0 && ATOMheap(tt, bn->tvheap, cap) != GDK_SUCCEED) {
		goto bailout;
	}
	DELTAinit(bn);
	if (BBPcacheit(bn, true) != GDK_SUCCEED) {
		goto bailout;
	}
	TRC_DEBUG(ALGO, "-> " ALGOBATFMT "\n", ALGOBATPAR(bn));
	return bn;
  bailout:
	BBPclear(bn->batCacheid, true);
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

BAT *
BATattach(int tt, const char *heapfile, role_t role)
{
	BAT *bn;
	char *p;
	size_t m;
	FILE *f;

	ERRORcheck(tt <= 0 , "bad tail type (<=0)\n", NULL);
	ERRORcheck(ATOMvarsized(tt) && ATOMstorage(tt) != TYPE_str, "bad tail type (varsized and not str)\n", NULL);
	ERRORcheck(heapfile == NULL, "bad heapfile name\n", NULL);

	if ((f = MT_fopen(heapfile, "rb")) == NULL) {
		GDKsyserror("BATattach: cannot open %s\n", heapfile);
		return NULL;
	}
	if (ATOMstorage(tt) == TYPE_str) {
		size_t n;
		char *s;
		int c, u;

		if ((bn = COLnew(0, tt, 0, role)) == NULL) {
			fclose(f);
			return NULL;
		}
		m = 4096;
		n = 0;
		u = 0;
		s = p = GDKmalloc(m);
		if (p == NULL) {
			fclose(f);
			BBPreclaim(bn);
			return NULL;
		}
		while ((c = getc(f)) != EOF) {
			if (n == m) {
				m += 4096;
				s = GDKrealloc(p, m);
				if (s == NULL) {
					GDKfree(p);
					BBPreclaim(bn);
					fclose(f);
					return NULL;
				}
				p = s;
				s = p + n;
			}
			if (c == '\n' && n > 0 && s[-1] == '\r') {
				/* deal with CR-LF sequence */
				s[-1] = c;
			} else {
				*s++ = c;
				n++;
			}
			if (u) {
				if ((c & 0xC0) == 0x80)
					u--;
				else
					goto notutf8;
			} else if ((c & 0xF8) == 0xF0)
				u = 3;
			else if ((c & 0xF0) == 0xE0)
				u = 2;
			else if ((c & 0xE0) == 0xC0)
				u = 1;
			else if ((c & 0x80) == 0x80)
				goto notutf8;
			else if (c == 0) {
				if (BUNappend(bn, p, false) != GDK_SUCCEED) {
					BBPreclaim(bn);
					fclose(f);
					GDKfree(p);
					return NULL;
				}
				s = p;
				n = 0;
			}
		}
		fclose(f);
		GDKfree(p);
		if (n > 0) {
			BBPreclaim(bn);
			GDKerror("last string is not null-terminated\n");
			return NULL;
		}
	} else {
		struct stat st;
		int atomsize;
		BUN cap;
		lng n;

		if (fstat(fileno(f), &st) < 0) {
			GDKsyserror("BATattach: cannot stat %s\n", heapfile);
			fclose(f);
			return NULL;
		}
		atomsize = ATOMsize(tt);
		if (st.st_size % atomsize != 0) {
			fclose(f);
			GDKerror("heapfile size not integral number of atoms\n");
			return NULL;
		}
		if (ATOMstorage(tt) == TYPE_msk ?
		    (st.st_size > (off_t) (BUN_MAX / 8)) :
		    ((size_t) (st.st_size / atomsize) > (size_t) BUN_MAX)) {
			fclose(f);
			GDKerror("heapfile too large\n");
			return NULL;
		}
		cap = (BUN) (ATOMstorage(tt) == TYPE_msk ?
			     st.st_size * 8 :
			     st.st_size / atomsize);
		bn = COLnew(0, tt, cap, role);
		if (bn == NULL) {
			fclose(f);
			return NULL;
		}
		p = Tloc(bn, 0);
		n = (lng) st.st_size;
		while (n > 0 && (m = fread(p, 1, (size_t) MIN(1024*1024, n), f)) > 0) {
			p += m;
			n -= m;
		}
		fclose(f);
		if (n > 0) {
			GDKerror("couldn't read the complete file\n");
			BBPreclaim(bn);
			return NULL;
		}
		BATsetcount(bn, cap);
		bn->tnonil = cap == 0;
		bn->tnil = false;
		bn->tseqbase = oid_nil;
		if (cap > 1) {
			bn->tsorted = false;
			bn->trevsorted = false;
			bn->tkey = false;
		} else {
			bn->tsorted = ATOMlinear(tt);
			bn->trevsorted = ATOMlinear(tt);
			bn->tkey = true;
		}
	}
	return bn;

  notutf8:
	fclose(f);
	BBPreclaim(bn);
	GDKfree(p);
	GDKerror("input is not UTF-8\n");
	return NULL;
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
	if (newcap <= BATcapacity(b)) {
		return GDK_SUCCEED;
	}

	if (ATOMstorage(b->ttype) == TYPE_msk) {
		newcap = (newcap + 31) & ~(BUN)31; /* round up to multiple of 32 */
		theap_size = (size_t) (newcap / 8); /* in bytes */
	} else {
		theap_size = (size_t) newcap << b->tshift;
	}
	b->batCapacity = newcap;

	if (b->theap->base) {
		TRC_DEBUG(HEAP, "HEAPgrow in BATextend %s %zu %zu\n",
			  b->theap->filename, b->theap->size, theap_size);
		return HEAPgrow(&b->theaplock, &b->theap, theap_size, b->batRestricted == BAT_READ);
	}
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
	IMPSdestroy(b);
	OIDXdestroy(b);
	STRMPdestroy(b);
	PROPdestroy(b);

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
			};
			strcpy_len(th->filename, b->tvheap->filename, sizeof(th->filename));
			if (ATOMheap(b->ttype, th, 0) != GDK_SUCCEED) {
				MT_lock_unset(&b->theaplock);
				return GDK_FAIL;
			}
			ATOMIC_INIT(&th->refs, 1);
			th->parentid = b->tvheap->parentid;
			th->dirty = true;
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

			for (p = b->batInserted, q = BUNlast(b); p < q; p++)
				(*tatmdel)(b->tvheap, (var_t*) BUNtloc(bi,p));
			b->tvheap->dirty = true;
		}
	}

	if (force)
		b->batInserted = 0;
	b->batCount = 0;
	if (b->ttype == TYPE_void)
		b->batCapacity = 0;
	b->theap->free = 0;
	BAThseqbase(b, 0);
	BATtseqbase(b, ATOMtype(b->ttype) == TYPE_oid ? 0 : oid_nil);
	b->theap->dirty = true;
	BATsettrivprop(b);
	b->tnosorted = b->tnorevsorted = 0;
	b->tnokey[0] = b->tnokey[1] = 0;
	b->tminpos = BUN_NONE;
	b->tmaxpos = BUN_NONE;
	b->tunique_est = 0.0;
	MT_lock_unset(&b->theaplock);
	return GDK_SUCCEED;
}

/* free a cached BAT; leave the bat descriptor cached */
void
BATfree(BAT *b)
{
	if (b == NULL)
		return;

	/* deallocate all memory for a bat */
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	b->tident = BATstring_t;
	MT_rwlock_rdlock(&b->thashlock);
	BUN nunique = BUN_NONE;
	if (b->thash && b->thash != (Hash *) 1) {
		nunique = b->thash->nunique;
	}
	MT_rwlock_rdunlock(&b->thashlock);
	HASHfree(b);
	IMPSfree(b);
	OIDXfree(b);
	STRMPfree(b);
	MT_lock_set(&b->theaplock);
	if (nunique != BUN_NONE) {
		b->tunique_est = (double) nunique;
	}
	if (b->theap) {
		assert(ATOMIC_GET(&b->theap->refs) == 1);
		assert(b->theap->parentid == b->batCacheid);
		HEAPfree(b->theap, false);
	}
	if (b->tvheap) {
		assert(ATOMIC_GET(&b->tvheap->refs) == 1);
		assert(b->tvheap->parentid == b->batCacheid);
		HEAPfree(b->tvheap, false);
	}
	MT_lock_unset(&b->theaplock);
}

/* free a cached BAT descriptor */
void
BATdestroy(BAT *b)
{
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	b->tident = BATstring_t;
	if (b->tvheap) {
		ATOMIC_DESTROY(&b->tvheap->refs);
		GDKfree(b->tvheap);
	}
	PROPdestroy_nolock(b);
	MT_lock_destroy(&b->theaplock);
	MT_lock_destroy(&b->batIdxLock);
	MT_rwlock_destroy(&b->thashlock);
	if (b->theap) {
		ATOMIC_DESTROY(&b->theap->refs);
		GDKfree(b->theap);
	}
	GDKfree(b);
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
			    ATOMsize(t1) != ATOMsize(t2) ||
			    BATatoms[t1].atomFix ||
			    BATatoms[t2].atomFix)
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

	BATcheck(b, NULL);
	assert(tt != TYPE_bat);

	/* maybe a bit ugly to change the requested bat type?? */
	if (b->ttype == TYPE_void && !writable)
		tt = TYPE_void;

	if (tt != b->ttype && wrongtype(tt, b->ttype)) {
		GDKerror("wrong tail-type requested\n");
		return NULL;
	}

	bi = bat_iterator(b);

	/* first try case (1); create a view, possibly with different
	 * atom-types */
	if (!writable &&
	    role == TRANSIENT &&
	    b->batRestricted == BAT_READ &&
	    ATOMstorage(b->ttype) != TYPE_msk && /* no view on TYPE_msk */
	    (!VIEWtparent(b) ||
	     BBP_cache(VIEWtparent(b))->batRestricted == BAT_READ)) {
		bn = VIEWcreate(b->hseqbase, b);
		if (bn == NULL) {
			bat_iterator_end(&bi);
			return NULL;
		}
		if (tt != bn->ttype) {
			bn->ttype = tt;
			bn->tvarsized = ATOMvarsized(tt);
			bn->tseqbase = ATOMtype(tt) == TYPE_oid ? bi.tseq : oid_nil;
		}
	} else {
		/* check whether we need case (4); BUN-by-BUN copy (by
		 * setting slowcopy to false) */
		if (ATOMsize(tt) != ATOMsize(bi.type)) {
			/* oops, void materialization */
			slowcopy = true;
		} else if (BATatoms[tt].atomFix) {
			/* oops, we need to fix/unfix atoms */
			slowcopy = true;
		} else if (bi.h && bi.h->parentid != b->batCacheid &&
			   BATcapacity(BBP_cache(bi.h->parentid)) > bi.count + bi.count) {
			/* reduced slice view: do not copy too much
			 * garbage */
			slowcopy = true;
		} else if (bi.vh && bi.vh->parentid != b->batCacheid &&
			   BATcount(BBP_cache(bi.vh->parentid)) > bi.count + bi.count) {
			/* reduced vheap view: do not copy too much
			 * garbage; this really is a heuristic since the
			 * vheap could be used completely, even if the
			 * offset heap is only (less than) half the size
			 * of the parent's offset heap */
			slowcopy = true;
		}

		bn = COLnew2(b->hseqbase, tt, bi.count, role, bi.width);
		if (bn == NULL) {
			bat_iterator_end(&bi);
			return NULL;
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
			}

			/* make sure we use the correct capacity */
			if (ATOMstorage(bn->ttype) == TYPE_msk)
				bn->batCapacity = (BUN) (bn->theap->size * 8);
			else if (bn->ttype)
				bn->batCapacity = (BUN) (bn->theap->size >> bn->tshift);
			else
				bn->batCapacity = 0;
		} else if (BATatoms[tt].atomFix || tt != TYPE_void || ATOMextern(tt)) {
			/* case (4): one-by-one BUN insert (really slow) */
			BUN p, q, r = 0;

			BATloop(b, p, q) {
				const void *t = BUNtail(bi, p);

				if (bunfastapp_nocheck(bn, t) != GDK_SUCCEED) {
					goto bunins_failed;
				}
				r++;
			}
			bn->theap->dirty |= bi.count > 0;
		} else if (tt != TYPE_void && bi.type == TYPE_void) {
			/* case (4): optimized for unary void
			 * materialization */
			oid cur = bi.tseq, *dst = (oid *) Tloc(bn, 0);
			const oid inc = !is_oid_nil(cur);

			bn->theap->free = bi.count * sizeof(oid);
			bn->theap->dirty |= bi.count > 0;
			for (BUN p = 0; p < bi.count; p++) {
				dst[p] = cur;
				cur += inc;
			}
		} else if (ATOMstorage(bi.type) == TYPE_msk) {
			/* convert number of bits to number of bytes,
			 * and round the latter up to a multiple of
			 * 4 (copy in units of 4 bytes) */
			bn->theap->free = ((bi.count + 31) / 32) * 4;
			bn->theap->dirty |= bi.count > 0;
			memcpy(Tloc(bn, 0), bi.base, bn->theap->free);
		} else {
			/* case (4): optimized for simple array copy */
			bn->theap->free = bi.count << bn->tshift;
			bn->theap->dirty |= bi.count > 0;
			memcpy(Tloc(bn, 0), bi.base, bn->theap->free);
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
		BATkey(bn, BATtkey(b));
		bn->tsorted = BATtordered(b);
		bn->trevsorted = BATtrevordered(b);
		bn->tnorevsorted = b->tnorevsorted;
		if (b->tnokey[0] != b->tnokey[1]) {
			bn->tnokey[0] = b->tnokey[0];
			bn->tnokey[1] = b->tnokey[1];
		} else {
			bn->tnokey[0] = bn->tnokey[1] = 0;
		}
		bn->tnosorted = b->tnosorted;
		bn->tnonil = b->tnonil;
		bn->tnil = b->tnil;
		bn->tminpos = bi.minpos;
		bn->tmaxpos = bi.maxpos;
		bn->tunique_est = bi.unique_est;
	} else if (ATOMstorage(tt) == ATOMstorage(b->ttype) &&
		   ATOMcompare(tt) == ATOMcompare(b->ttype)) {
		BUN h = BUNlast(b);
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		if (b->tkey)
			BATkey(bn, true);
		bn->tnonil = b->tnonil;
		bn->tnil = b->tnil;
		if (b->tnosorted > 0 && b->tnosorted < h)
			bn->tnosorted = b->tnosorted;
		else
			bn->tnosorted = 0;
		if (b->tnorevsorted > 0 && b->tnorevsorted < h)
			bn->tnorevsorted = b->tnorevsorted;
		else
			bn->tnorevsorted = 0;
		if (b->tnokey[0] < h &&
		    b->tnokey[1] < h &&
		    b->tnokey[0] != b->tnokey[1]) {
			bn->tnokey[0] = b->tnokey[0];
			bn->tnokey[1] = b->tnokey[1];
		} else {
			bn->tnokey[0] = bn->tnokey[1] = 0;
		}
		bn->tminpos = bi.minpos;
		bn->tmaxpos = bi.maxpos;
		bn->tunique_est = bi.unique_est;
	} else {
		bn->tsorted = bn->trevsorted = false; /* set based on count later */
		bn->tnonil = bn->tnil = false;
		bn->tnosorted = bn->tnorevsorted = 0;
		bn->tnokey[0] = bn->tnokey[1] = 0;
	}
	if (BATcount(bn) <= 1) {
		bn->tsorted = ATOMlinear(b->ttype);
		bn->trevsorted = ATOMlinear(b->ttype);
		bn->tkey = true;
	}
	if (!writable)
		bn->batRestricted = BAT_READ;
	TRC_DEBUG(ALGO, ALGOBATFMT " -> " ALGOBATFMT "\n",
		  ALGOBATPAR(b), ALGOBATPAR(bn));
	bat_iterator_end(&bi);
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

	BATcheck(b, GDK_FAIL);

	assert(!VIEWtparent(b));

	if (count == 0)
		return GDK_SUCCEED;

	TRC_DEBUG(ALGO, ALGOBATFMT " appending " BUNFMT " values%s\n", ALGOBATPAR(b), count, values ? "" : " (all nil)");

	p = BUNlast(b);		/* insert at end */
	if (p == BUN_MAX || BATcount(b) + count >= BUN_MAX) {
		GDKerror("bat too large\n");
		return GDK_FAIL;
	}

	ALIGNapp(b, force, GDK_FAIL);

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
			if (b->batCount == 0)
				b->tseqbase = ovals ? ovals[0] : oid_nil;
			BATsetcount(b, BATcount(b) + count);
			MT_lock_unset(&b->theaplock);
			return GDK_SUCCEED;
		} else {
			/* we need to materialize b; allocate enough capacity */
			MT_lock_set(&b->theaplock);
			b->batCapacity = BATcount(b) + count;
			MT_lock_unset(&b->theaplock);
			if (BATmaterialize(b) != GDK_SUCCEED)
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

	MT_lock_set(&b->theaplock);
	if (count > BATcount(b) / GDK_UNIQUE_ESTIMATE_KEEP_FRACTION)
		b->tunique_est = 0;
	b->theap->dirty = true;
	const void *t = b->ttype == TYPE_msk ? &(msk){false} : ATOMnilptr(b->ttype);
	if (b->ttype == TYPE_oid) {
		/* spend extra effort on oid (possible candidate list) */
		if (values == NULL || is_oid_nil(((oid *) values)[0])) {
			b->tnil = true;
			b->tnonil = false;
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
				b->tnil = false;
				b->tnonil = true;
			} else {
				if (!is_oid_nil(b->tseqbase) &&
				    (count > 1 ||
				     b->tseqbase + b->batCount != ((oid *) values)[0]))
					b->tseqbase = oid_nil;
				if (b->tsorted && ((oid *) b->theap->base)[b->batCount - 1] > ((oid *) values)[0]) {
					b->tsorted = false;
					if (b->tnosorted == 0)
						b->tnosorted = b->batCount;
				}
				if (b->trevsorted && ((oid *) b->theap->base)[b->batCount - 1] < ((oid *) values)[0]) {
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
					b->tnil = true;
					b->tnonil = false;
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
		b->tnil = b->tnonil = false;
		b->tsorted = b->trevsorted = b->tkey = false;
	} else if (b->batCount == 0) {
		if (values == NULL) {
			b->tsorted = b->trevsorted = true;
			b->tkey = count == 1;
			b->tnil = true;
			b->tnonil = false;
		} else {
			b->tsorted = b->trevsorted = b->tkey = count == 1;
			b->tnil = b->tnonil = false;
		}
	} else {
		b->tnil |= values == NULL;
		b->tnonil = false;
		b->tsorted = b->trevsorted = b->tkey = false;
	}
	MT_lock_unset(&b->theaplock);
	if (values && b->ttype) {
		int (*atomcmp) (const void *, const void *) = ATOMcompare(b->ttype);
		const void *atomnil = ATOMnilptr(b->ttype);
		const void *minvalp = NULL, *maxvalp = NULL;
		BATiter bi = bat_iterator_nolock(b);
		if (b->tvarsized) {
			if (bi.minpos != BUN_NONE)
				minvalp = BUNtvar(bi, bi.minpos);
			if (bi.maxpos != BUN_NONE)
				maxvalp = BUNtvar(bi, bi.maxpos);
			const void *vbase = b->tvheap->base;
			for (BUN i = 0; i < count; i++) {
				t = ((void **) values)[i];
				gdk_return rc = tfastins_nocheckVAR(b, p, t);
				if (rc != GDK_SUCCEED) {
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
				}
				p++;
			}
			MT_rwlock_wrlock(&b->thashlock);
			if (b->thash) {
				p -= count;
				for (BUN i = 0; i < count; i++) {
					t = ((void **) values)[i];
					HASHappend_locked(b, p, t);
					p++;
				}
			}
			MT_rwlock_wrunlock(&b->thashlock);
		} else if (ATOMstorage(b->ttype) == TYPE_msk) {
			bi.minpos = bi.maxpos = BUN_NONE;
			minvalp = maxvalp = NULL;
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
			MT_rwlock_wrlock(&b->thashlock);
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
				}
				p++;
			}
			MT_rwlock_wrunlock(&b->thashlock);
		}
		MT_lock_set(&b->theaplock);
		b->tminpos = bi.minpos;
		b->tmaxpos = bi.maxpos;
		MT_lock_unset(&b->theaplock);
	} else {
		MT_rwlock_wrlock(&b->thashlock);
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
		MT_rwlock_wrunlock(&b->thashlock);
	}
	MT_lock_set(&b->theaplock);
	BATsetcount(b, p);
	MT_lock_unset(&b->theaplock);

	IMPSdestroy(b); /* no support for inserts in imprints yet */
	OIDXdestroy(b);
	STRMPdestroy(b); 	/* TODO: use STRMPappendBitstring */
	return GDK_SUCCEED;
}

/* Append a single value to the bat. */
gdk_return
BUNappend(BAT *b, const void *t, bool force)
{
	return BUNappendmulti(b, b->ttype && b->tvarsized ? (const void *) &t : (const void *) t, 1, force);
}

gdk_return
BUNdelete(BAT *b, oid o)
{
	BUN p;
	BATiter bi = bat_iterator_nolock(b);
	const void *val;
	bool locked = false;

	assert(!is_oid_nil(b->hseqbase) || BATcount(b) == 0);
	if (o < b->hseqbase || o >= b->hseqbase + BATcount(b)) {
		/* value already not there */
		return GDK_SUCCEED;
	}
	assert(BATcount(b) > 0); /* follows from "if" above */
	p = o - b->hseqbase;
	if (p < b->batInserted) {
		GDKerror("cannot delete committed value\n");
		return GDK_FAIL;
	}
	TRC_DEBUG(ALGO, ALGOBATFMT " deleting oid " OIDFMT "\n", ALGOBATPAR(b), o);
	val = BUNtail(bi, p);
	/* writing the values should be locked, reading could be done
	 * unlocked (since we're the only thread that should be changing
	 * anything) */
	MT_lock_set(&b->theaplock);
	if (b->tmaxpos == p)
		b->tmaxpos = BUN_NONE;
	if (b->tminpos == p)
		b->tminpos = BUN_NONE;
	MT_lock_unset(&b->theaplock);
	if (ATOMunfix(b->ttype, val) != GDK_SUCCEED)
		return GDK_FAIL;
	HASHdelete(b, p, val);
	ATOMdel(b->ttype, b->tvheap, (var_t *) BUNtloc(bi, p));
	if (p != BUNlast(b) - 1 &&
	    (b->ttype != TYPE_void || BATtdense(b))) {
		/* replace to-be-delete BUN with last BUN; materialize
		 * void column before doing so */
		if (b->ttype == TYPE_void &&
		    BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			msk mval = mskGetVal(b, BUNlast(b) - 1);
			assert(b->thash == NULL);
			mskSetVal(b, p, mval);
			/* don't leave garbage */
			mskClr(b, BUNlast(b) - 1);
		} else {
			val = Tloc(b, BUNlast(b) - 1);
			HASHdelete(b, BUNlast(b) - 1, val);
			memcpy(Tloc(b, p), val, Tsize(b));
			HASHinsert(b, p, val);
			MT_lock_set(&b->theaplock);
			locked = true;
			if (b->tminpos == BUNlast(b) - 1)
				b->tminpos = p;
			if (b->tmaxpos == BUNlast(b) - 1)
				b->tmaxpos = p;
		}
		/* no longer sorted */
		if (!locked) {
			MT_lock_set(&b->theaplock);
			locked = true;
		}
		b->tsorted = b->trevsorted = false;
		b->theap->dirty = true;
	}
	if (!locked)
		MT_lock_set(&b->theaplock);
	if (b->tnosorted >= p)
		b->tnosorted = 0;
	if (b->tnorevsorted >= p)
		b->tnorevsorted = 0;
	b->batCount--;
	if (BATcount(b) < GDK_UNIQUE_ESTIMATE_KEEP_FRACTION)
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
	IMPSdestroy(b);
	OIDXdestroy(b);
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
	BUN last = BUNlast(b) - 1;
	BATiter bi = bat_iterator_nolock(b);
	int tt;
	BUN prv, nxt;
	const void *val;

	/* zap alignment info */
	if (!force && (b->batRestricted != BAT_WRITE || b->batSharecnt > 0)) {
		GDKerror("access denied to %s, aborting.\n",
			 BATgetId(b));
		return GDK_FAIL;
	}
	TRC_DEBUG(ALGO, ALGOBATFMT " replacing " BUNFMT " values\n", ALGOBATPAR(b), count);
	MT_lock_set(&b->theaplock);
	if (b->ttype == TYPE_void) {
		PROPdestroy(b);
		b->tminpos = BUN_NONE;
		b->tmaxpos = BUN_NONE;
		b->tunique_est = 0.0;
	} else if (count > BATcount(b) / GDK_UNIQUE_ESTIMATE_KEEP_FRACTION) {
		b->tunique_est = 0;
	}
	MT_lock_unset(&b->theaplock);
	MT_rwlock_wrlock(&b->thashlock);
	for (BUN i = 0; i < count; i++) {
		BUN p = autoincr ? positions[0] - b->hseqbase + i : positions[i] - b->hseqbase;
		const void *t = b->ttype && b->tvarsized ?
			((const void **) values)[i] :
			(const void *) ((const char *) values + (i << b->tshift));
		const bool isnil = ATOMlinear(b->ttype) &&
			ATOMcmp(b->ttype, t, ATOMnilptr(b->ttype)) == 0;

		/* retrieve old value, but if this comes from the
		 * logger, we need to deal with offsets that point
		 * outside of the valid vheap */
		if (b->tvarsized) {
			if (b->ttype) {
				size_t off = BUNtvaroff(bi, p);
				if (off < bi.vhfree)
					val = bi.vh->base + off;
				else
					val = NULL; /* bad offset */
			} else {
				val = BUNtpos(bi, p);
			}
		} else if (bi.type == TYPE_msk) {
			val = BUNtmsk(bi, p);
		} else {
			val = BUNtloc(bi, p);
		}

		if (val) {
			if (ATOMcmp(b->ttype, val, t) == 0)
				continue; /* nothing to do */
			if (!isnil &&
			    b->tnil &&
			    ATOMcmp(b->ttype, val, ATOMnilptr(b->ttype)) == 0) {
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
					if (!isnil && ATOMcmp(b->ttype, BUNtail(bi, bi.maxpos), t) < 0) {
						/* new value is larger
						 * than previous
						 * largest */
						bi.maxpos = p;
					} else if (bi.maxpos == p && ATOMcmp(b->ttype, BUNtail(bi, bi.maxpos), t) != 0) {
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
					if (!isnil && ATOMcmp(b->ttype, BUNtail(bi, bi.minpos), t) > 0) {
						/* new value is smaller
						 * than previous
						 * smallest */
						bi.minpos = p;
					} else if (bi.minpos == p && ATOMcmp(b->ttype, BUNtail(bi, bi.minpos), t) != 0) {
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
			HASHdelete_locked(b, p, val);	/* first delete old value from hash */
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
		IMPSdestroy(b);
		STRMPdestroy(b);

		if (b->tvarsized && b->ttype) {
			var_t _d;
			ptr _ptr;
			_ptr = BUNtloc(bi, p);
			switch (b->twidth) {
			default:	/* only three or four cases possible */
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
			}
			if (ATOMreplaceVAR(b, &_d, t) != GDK_SUCCEED) {
				MT_rwlock_wrunlock(&b->thashlock);
				return GDK_FAIL;
			}
			if (b->twidth < SIZEOF_VAR_T &&
			    (b->twidth <= 2 ? _d - GDK_VAROFFSET : _d) >= ((size_t) 1 << (8 << b->tshift))) {
				/* doesn't fit in current heap, upgrade
				 * it, can't keep hashlock while doing
				 * so */
				MT_rwlock_wrunlock(&b->thashlock);
				if (GDKupgradevarheap(b, _d, 0, bi.count) != GDK_SUCCEED) {
					return GDK_FAIL;
				}
				MT_rwlock_wrlock(&b->thashlock);
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
			default:	/* only three or four cases possible */
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
			}
		} else if (ATOMstorage(b->ttype) == TYPE_msk) {
			mskSetVal(b, p, * (msk *) t);
		} else {
			assert(BATatoms[b->ttype].atomPut == NULL);
			if (ATOMfix(b->ttype, t) != GDK_SUCCEED ||
			    ATOMunfix(b->ttype, BUNtloc(bi, p)) != GDK_SUCCEED) {
				MT_rwlock_wrunlock(&b->thashlock);
				return GDK_FAIL;
			}
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

		HASHinsert_locked(b, p, t);	/* insert new value into hash */

		tt = b->ttype;
		prv = p > 0 ? p - 1 : BUN_NONE;
		nxt = p < last ? p + 1 : BUN_NONE;

		MT_lock_set(&b->theaplock);
		if (BATtordered(b)) {
			if (prv != BUN_NONE &&
			    ATOMcmp(tt, t, BUNtail(bi, prv)) < 0) {
				b->tsorted = false;
				b->tnosorted = p;
			} else if (nxt != BUN_NONE &&
				   ATOMcmp(tt, t, BUNtail(bi, nxt)) > 0) {
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
		} else if (b->tnosorted >= p)
			b->tnosorted = 0;
		if (BATtrevordered(b)) {
			if (prv != BUN_NONE &&
			    ATOMcmp(tt, t, BUNtail(bi, prv)) > 0) {
				b->trevsorted = false;
				b->tnorevsorted = p;
			} else if (nxt != BUN_NONE &&
				   ATOMcmp(tt, t, BUNtail(bi, nxt)) < 0) {
				b->trevsorted = false;
				b->tnorevsorted = nxt;
			}
		} else if (b->tnorevsorted >= p)
			b->tnorevsorted = 0;
		if (((b->ttype != TYPE_void) & b->tkey) && b->batCount > 1) {
			BATkey(b, false);
		} else if (!b->tkey && (b->tnokey[0] == p || b->tnokey[1] == p))
			b->tnokey[0] = b->tnokey[1] = 0;
		if (b->tnonil && ATOMstorage(b->ttype) != TYPE_msk)
			b->tnonil = t && ATOMcmp(b->ttype, t, ATOMnilptr(b->ttype)) != 0;
		MT_lock_unset(&b->theaplock);
	}
	MT_rwlock_wrunlock(&b->thashlock);
	MT_lock_set(&b->theaplock);
	b->tminpos = bi.minpos;
	b->tmaxpos = bi.maxpos;
	b->theap->dirty = true;
	if (b->tvheap)
		b->tvheap->dirty = true;
	MT_lock_unset(&b->theaplock);

	return GDK_SUCCEED;
}

/* Replace multiple values given by their positions with the given values. */
gdk_return
BUNreplacemulti(BAT *b, const oid *positions, const void *values, BUN count, bool force)
{
	BATcheck(b, GDK_FAIL);

	if (b->ttype == TYPE_void && BATmaterialize(b) != GDK_SUCCEED)
		return GDK_FAIL;

	return BUNinplacemulti(b, positions, values, count, force, false);
}

/* Replace multiple values starting from a given position with the given
 * values. */
gdk_return
BUNreplacemultiincr(BAT *b, oid position, const void *values, BUN count, bool force)
{
	BATcheck(b, GDK_FAIL);

	if (b->ttype == TYPE_void && BATmaterialize(b) != GDK_SUCCEED)
		return GDK_FAIL;

	return BUNinplacemulti(b, &position, values, count, force, true);
}

gdk_return
BUNreplace(BAT *b, oid id, const void *t, bool force)
{
	return BUNreplacemulti(b, &id, b->ttype && b->tvarsized ? (const void *) &t : t, 1, force);
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
	return BUNinplacemulti(b, &id, b->ttype && b->tvarsized ? (const void *) &val : (const void *) val, 1, force, false);
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
		switch (ATOMbasetype(b->ttype)) {
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
	if (b->tnosorted >= BUNlast(b))
		b->tnosorted = 0;
	if (b->tnorevsorted >= BUNlast(b))
		b->tnorevsorted = 0;
	if (b->tnokey[0] >= BUNlast(b) || b->tnokey[1] >= BUNlast(b)) {
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
	} else
		b->tnokey[0] = b->tnokey[1] = 0;
	gdk_return rc = GDK_SUCCEED;
	if (flag && VIEWtparent(b)) {
		/* if a view is key, then so is the parent if the two
		 * are aligned */
		BAT *bp = BBP_cache(VIEWtparent(b));
		MT_lock_set(&bp->theaplock);
		if (BATcount(b) == BATcount(bp) &&
		    ATOMtype(BATttype(b)) == ATOMtype(BATttype(bp)) &&
		    !BATtkey(bp) &&
		    ((BATtvoid(b) && BATtvoid(bp) && b->tseqbase == bp->tseqbase) ||
		     BATcount(b) == 0))
			rc = BATkey(bp, true);
		MT_lock_unset(&bp->theaplock);
	}
	return rc;
}

void
BAThseqbase(BAT *b, oid o)
{
	if (b != NULL) {
		assert(o <= GDK_oid_max);	/* i.e., not oid_nil */
		assert(o + BATcount(b) <= GDK_oid_max);
		if (b->hseqbase != o) {
			b->hseqbase = o;
		}
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
			}
		}
	} else {
		assert(o == oid_nil);
		b->tseqbase = oid_nil;
	}
}

gdk_return
BATroles(BAT *b, const char *tnme)
{
	if (b == NULL)
		return GDK_SUCCEED;
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	if (tnme)
		b->tident = GDKstrdup(tnme);
	else
		b->tident = BATstring_t;
	return b->tident ? GDK_SUCCEED : GDK_FAIL;
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
	char *batpath, *bakpath;
	struct stat st;

	/* check for an existing X.new in BATDIR, BAKDIR and SUBDIR */
	batpath = GDKfilepath(hp->farmid, BATDIR, hp->filename, ".new");
	bakpath = GDKfilepath(hp->farmid, BAKDIR, hp->filename, ".new");
	if (batpath != NULL && bakpath != NULL) {
		/* file actions here interact with the global commits */
		if (lock)
			MT_lock_set(&GDKtmLock);

		batret = MT_stat(batpath, &st);
		bakret = MT_stat(bakpath, &st);

		if (batret == 0 && bakret) {
			/* no backup yet, so move the existing X.new there out
			 * of the way */
			if ((ret = MT_rename(batpath, bakpath)) < 0)
				GDKsyserror("backup_new: rename %s to %s failed\n",
					    batpath, bakpath);
			TRC_DEBUG(IO_, "rename(%s,%s) = %d\n", batpath, bakpath, ret);
		} else if (batret == 0) {
			/* there is a backup already; just remove the X.new */
			if ((ret = MT_remove(batpath)) != 0)
				GDKsyserror("backup_new: remove %s failed\n", batpath);
			TRC_DEBUG(IO_, "remove(%s) = %d\n", batpath, ret);
		} else {
			ret = 0;
		}
		if (lock)
			MT_lock_unset(&GDKtmLock);
	}
	GDKfree(batpath);
	GDKfree(bakpath);
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
	if ((isVIEW(b) || b->batSharecnt) && newmode != BAT_READ) {
		BAT *bn = COLcopy(b, b->ttype, true, TRANSIENT);
		BBPunfix(b->batCacheid);
		if (bn == NULL)
			return NULL;
		b = bn;
	}
	bakmode = (restrict_t) b->batRestricted;
	if (bakmode != newmode) {
		bool existing = (BBP_status(b->batCacheid) & BBPEXISTING) != 0;
		bool wr = (newmode == BAT_WRITE);
		bool rd = (bakmode == BAT_WRITE);
		storage_t m1, m3 = STORE_MEM;
		storage_t b1, b3 = STORE_MEM;

		b1 = b->theap->newstorage;
		m1 = HEAPchangeaccess(b->theap, ACCESSMODE(wr, rd), existing);
		if (b->tvheap) {
			bool ta = (newmode == BAT_APPEND && ATOMappendpriv(b->ttype, b->tvheap));
			b3 = b->tvheap->newstorage;
			m3 = HEAPchangeaccess(b->tvheap, ACCESSMODE(wr && ta, rd && ta), existing);
		}
		if (m1 == STORE_INVALID || m3 == STORE_INVALID) {
			BBPunfix(b->batCacheid);
			return NULL;
		}

		/* set new access mode and mmap modes */
		b->batRestricted = (unsigned int) newmode;
		b->theap->newstorage = m1;
		if (b->tvheap)
			b->tvheap->newstorage = m3;

		if (existing && BBPsave(b) != GDK_SUCCEED) {
			/* roll back all changes */
			b->batRestricted = (unsigned int) bakmode;
			b->theap->newstorage = b1;
			if (b->tvheap)
				b->tvheap->newstorage = b3;
			BBPunfix(b->batCacheid);
			return NULL;
		}
	}
	return b;
}

restrict_t
BATgetaccess(BAT *b)
{
	BATcheck(b, BAT_WRITE /* 0 */);
	assert(b->batRestricted != 3); /* only valid restrict_t values */
	return (restrict_t) b->batRestricted;
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
			GDKerror("%s type implies that %s[%s] "		\
				 "cannot be made persistent.\n",	\
				 ATOMname(tp), BATgetId(b),		\
				 ATOMname(b->ttype));			\
			return GDK_FAIL;				\
		}							\
	} while (0)

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

	if (transient != b->batTransient) {
		bat bid = b->batCacheid;

		if (!transient) {
			check_type(b->ttype);
		}

		/* persistent BATs get a logical reference */
		if (!transient) {
			BBPretain(bid);
		} else if (!b->batTransient) {
			BBPrelease(bid);
		}
		MT_lock_set(&GDKswapLock(bid));
		if (!transient) {
			if (BBP_status(bid) & BBPDELETED) {
				BBP_status_on(bid, BBPEXISTING);
				BBP_status_off(bid, BBPDELETED);
			} else
				BBP_status_on(bid, BBPNEW);
		} else if (!b->batTransient) {
			if (!(BBP_status(bid) & BBPNEW))
				BBP_status_on(bid, BBPDELETED);
			BBP_status_off(bid, BBPPERSISTENT);
		}
		/* session bats or persistent bats that did not
		 * witness a commit yet may have been saved */
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
#define assert(test)	((void) ((test) || (TRC_CRITICAL_ENDIF(BAT_, "Assertion `%s' failed\n", #test), 0)))
#endif

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
 * at only at the non-changing part of the BAT (through candidate
 * lists), but this means that the properties of the view might not be
 * correct.  For this reason, for views, we skip all property checking
 * that looks at the BAT content.
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
	bool isview;

	/* do the complete check within a lock */
	MT_lock_set(&b->theaplock);

	/* general BAT sanity */
	assert(b != NULL);
	assert(!b->batDirtydesc); /* not used anymore, must always be false */
	assert(!b->batDirtyflushed); /* not used anymore, must always be false */
	assert(b->batCacheid > 0);
	assert(b->batCount >= b->batInserted);

	/* headless */
	assert(b->hseqbase <= GDK_oid_max); /* non-nil seqbase */
	assert(b->hseqbase + BATcount(b) <= GDK_oid_max);

	isview = isVIEW(b);

	bbpstatus = BBP_status(b->batCacheid);
	/* only at most one of BBPDELETED, BBPEXISTING, BBPNEW may be set */
	assert(((bbpstatus & BBPDELETED) != 0) +
	       ((bbpstatus & BBPEXISTING) != 0) +
	       ((bbpstatus & BBPNEW) != 0) <= 1);

	assert(b->ttype >= TYPE_void);
	assert(b->ttype < GDKatomcnt);
	assert(b->ttype != TYPE_bat);
	assert(isview ||
	       b->ttype == TYPE_void ||
	       BBPfarms[b->theap->farmid].roles & (1 << b->batRole));
	assert(isview ||
	       b->tvheap == NULL ||
	       (BBPfarms[b->tvheap->farmid].roles & (1 << b->batRole)));

	cmpf = ATOMcompare(b->ttype);
	nilp = ATOMnilptr(b->ttype);

	assert(b->theap->free >= tailsize(b, BUNlast(b)));
	if (b->ttype != TYPE_void) {
		assert(b->batCount <= b->batCapacity);
		assert(b->theap->size >= b->theap->free);
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			/* 32 values per 4-byte word (that's not the
			 * same as 8 values per byte...) */
			assert(b->theap->size >= 4 * ((b->batCapacity + 31) / 32));
		} else
			assert(b->theap->size >> b->tshift >= b->batCapacity);
	}
	strconcat_len(filename, sizeof(filename),
		      BBP_physical(b->theap->parentid),
		      b->ttype == TYPE_str ? b->twidth == 1 ? ".tail1" : b->twidth == 2 ? ".tail2" :
#if SIZEOF_VAR_T == 8
		      b->twidth == 4 ? ".tail4" :
#endif
		      ".tail" : ".tail",
		      NULL);
	assert(strcmp(b->theap->filename, filename) == 0);
	if (b->tvheap) {
		strconcat_len(filename, sizeof(filename),
			      BBP_physical(b->tvheap->parentid),
			      ".theap",
			      NULL);
		assert(strcmp(b->tvheap->filename, filename) == 0);
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
	if (ATOMstorage(b->ttype) == TYPE_str)
		assert(b->twidth >= 1 && b->twidth <= ATOMsize(b->ttype));
	else
		assert(b->twidth == ATOMsize(b->ttype));
	assert(b->tseqbase <= oid_nil);
	/* only oid/void columns can be dense */
	assert(is_oid_nil(b->tseqbase) || b->ttype == TYPE_oid || b->ttype == TYPE_void);
	/* a column cannot both have and not have NILs */
	assert(!b->tnil || !b->tnonil);
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
				assert(b->batRole == TRANSIENT);
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
		if (!isview &&
		    !b->tsorted &&
		    b->tnosorted > 0 &&
		    b->tnosorted < b->batCount)
			assert(cmpf(BUNtail(bi, b->tnosorted - 1),
				    BUNtail(bi, b->tnosorted)) > 0);
		assert(b->tnorevsorted == 0 ||
		       (b->tnorevsorted > 0 &&
			b->tnorevsorted < b->batCount));
		assert(!b->trevsorted || b->tnorevsorted == 0);
		if (!isview &&
		    !b->trevsorted &&
		    b->tnorevsorted > 0 &&
		    b->tnorevsorted < b->batCount)
			assert(cmpf(BUNtail(bi, b->tnorevsorted - 1),
				    BUNtail(bi, b->tnorevsorted)) < 0);
	}
	/* if tkey property set, both tnokey values must be 0 */
	assert(!b->tkey || (b->tnokey[0] == 0 && b->tnokey[1] == 0));
	if (!isview && !b->tkey && (b->tnokey[0] != 0 || b->tnokey[1] != 0)) {
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

	/* only do a scan if property checking is requested and the bat
	 * is not a view */
	if (!isview && GDKdebug & PROPMASK) {
		const void *maxval = NULL;
		const void *minval = NULL;
		bool seenmax = false, seenmin = false;
		bool seennil = false;

		if (b->tmaxpos != BUN_NONE) {
			assert(b->tmaxpos < BATcount(b));
			maxval = BUNtail(bi, b->tmaxpos);
		}
		if (b->tminpos != BUN_NONE) {
			assert(b->tminpos < BATcount(b));
			minval = BUNtail(bi, b->tminpos);
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
			bool cmpnil = b->tnonil | b->tnil;

			BATloop(b, p, q) {
				valp = BUNtail(bi, p);
				bool isnil = cmpf(valp, nilp) == 0;
				assert(b->ttype != TYPE_flt || !isinf(*(flt*)valp));
				assert(b->ttype != TYPE_dbl || !isinf(*(dbl*)valp));
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
				if (cmpnil) {
					assert(!b->tnonil || !isnil);
					if (isnil) {
						/* we found a nil:
						 * we're done checking
						 * for them */
						seennil = true;
						cmpnil = 0;
						if (!cmpprv && maxval == NULL && minval == NULL) {
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
			Hash *hs = NULL;
			BUN mask;

			if ((hs = GDKzalloc(sizeof(Hash))) == NULL) {
				TRC_WARNING(BAT_, "Cannot allocate hash table\n");
				goto abort_check;
			}
			if (snprintf(hs->heaplink.filename, sizeof(hs->heaplink.filename), "%s.thshprpl%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs->heaplink.filename) ||
			    snprintf(hs->heapbckt.filename, sizeof(hs->heapbckt.filename), "%s.thshprpb%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs->heapbckt.filename)) {
				/* cannot happen, see comment in gdk.h
				 * about sizes near definition of
				 * BBPINIT */
				GDKfree(hs);
				TRC_CRITICAL(BAT_, "Heap filename is too large\n");
				goto abort_check;
			}
			if (ATOMsize(b->ttype) == 1)
				mask = (BUN) 1 << 8;
			else if (ATOMsize(b->ttype) == 2)
				mask = (BUN) 1 << 16;
			else
				mask = HASHmask(b->batCount);
			if ((hs->heaplink.farmid = BBPselectfarm(
				     TRANSIENT, b->ttype, hashheap)) < 0 ||
			    (hs->heapbckt.farmid = BBPselectfarm(
				    TRANSIENT, b->ttype, hashheap)) < 0 ||
			    HASHnew(hs, b->ttype, BUNlast(b),
				    mask, BUN_NONE, false) != GDK_SUCCEED) {
				GDKfree(hs);
				TRC_WARNING(BAT_, "Cannot allocate hash table\n");
				goto abort_check;
			}
			BATloop(b, p, q) {
				BUN hb;
				BUN prb;
				valp = BUNtail(bi, p);
				bool isnil = cmpf(valp, nilp) == 0;
				assert(b->ttype != TYPE_flt || !isinf(*(flt*)valp));
				assert(b->ttype != TYPE_dbl || !isinf(*(dbl*)valp));
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
