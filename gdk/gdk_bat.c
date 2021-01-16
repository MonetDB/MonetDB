/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

static char *BATstring_t = "t";

#define default_ident(s)	((s) == BATstring_t)

void
BATinit_idents(BAT *bn)
{
	bn->tident = BATstring_t;
}

BAT *
BATcreatedesc(oid hseq, int tt, bool heapnames, role_t role)
{
	BAT *bn;

	/*
	 * Alloc space for the BAT and its dependent records.
	 */
	assert(tt >= 0);

	bn = GDKzalloc(sizeof(BAT));

	if (bn == NULL)
		return NULL;

	/*
	 * Fill in basic column info
	 */
	bn->hseqbase = hseq;

	bn->ttype = tt;
	bn->tkey = false;
	bn->tnonil = true;
	bn->tnil = false;
	bn->tsorted = bn->trevsorted = ATOMlinear(tt);
	bn->tident = BATstring_t;
	bn->tseqbase = oid_nil;
	bn->tprops = NULL;

	bn->batRole = role;
	bn->batTransient = true;
	/*
	 * add to BBP
	 */
	if (BBPinsert(bn) == 0) {
		GDKfree(bn);
		return NULL;
	}
	/*
 	* Default zero for order oid index
 	*/
	bn->torderidx = NULL;
	/*
	 * fill in heap names, so HEAPallocs can resort to disk for
	 * very large writes.
	 */
	assert(bn->batCacheid > 0);

	const char *nme = BBP_physical(bn->batCacheid);
	strconcat_len(bn->theap.filename, sizeof(bn->theap.filename),
		      nme, ".tail", NULL);
	bn->theap.farmid = BBPselectfarm(role, bn->ttype, offheap);
	if (heapnames && ATOMneedheap(tt)) {
		if ((bn->tvheap = (Heap *) GDKzalloc(sizeof(Heap))) == NULL)
			goto bailout;
		strconcat_len(bn->tvheap->filename,
			      sizeof(bn->tvheap->filename),
			      nme, ".theap", NULL);
		bn->tvheap->parentid = bn->batCacheid;
		bn->tvheap->farmid = BBPselectfarm(role, bn->ttype, varheap);
	}
	char name[MT_NAME_LEN];
	snprintf(name, sizeof(name), "BATlock%d", bn->batCacheid); /* fits */
	MT_lock_init(&bn->batIdxLock, name);
	bn->batDirtydesc = true;
	return bn;
      bailout:
	BBPclear(bn->batCacheid);
	if (tt)
		HEAPfree(&bn->theap, true);
	if (bn->tvheap) {
		HEAPfree(bn->tvheap, true);
		GDKfree(bn->tvheap);
	}
	GDKfree(bn);
	return NULL;
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
BATsetdims(BAT *b)
{
	b->twidth = b->ttype == TYPE_str ? 1 : ATOMsize(b->ttype);
	b->tshift = ATOMelmshift(Tsize(b));
	assert_shift_width(b->tshift, b->twidth);
	b->tvarsized = b->ttype == TYPE_void || BATatoms[b->ttype].atomPut != NULL;
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
COLnew(oid hseq, int tt, BUN cap, role_t role)
{
	BAT *bn;

	assert(cap <= BUN_MAX);
	assert(hseq <= oid_nil);
	assert(tt != TYPE_bat);
	ERRORcheck((tt < 0) || (tt > GDKatomcnt), "tt error\n", NULL);

	/* round up to multiple of BATTINY */
	if (cap < BUN_MAX - BATTINY)
		cap = (cap + BATTINY - 1) & ~(BATTINY - 1);
	if (cap < BATTINY)
		cap = BATTINY;
	/* limit the size */
	if (cap > BUN_MAX)
		cap = BUN_MAX;

	bn = BATcreatedesc(hseq, tt, tt != TYPE_void, role);
	if (bn == NULL)
		return NULL;

	BATsetdims(bn);
	bn->batCapacity = cap;

	/* alloc the main heaps */
	if (tt && HEAPalloc(&bn->theap, cap, bn->twidth) != GDK_SUCCEED) {
		goto bailout;
	}

	if (bn->tvheap && ATOMheap(tt, bn->tvheap, cap) != GDK_SUCCEED) {
		GDKfree(bn->tvheap);
		goto bailout;
	}
	DELTAinit(bn);
	if (BBPcacheit(bn, true) != GDK_SUCCEED) {
		GDKfree(bn->tvheap);
		goto bailout;
	}
	TRC_DEBUG(ALGO, "-> " ALGOBATFMT "\n", ALGOBATPAR(bn));
	return bn;
  bailout:
	BBPclear(bn->batCacheid);
	HEAPfree(&bn->theap, true);
	MT_lock_destroy(&bn->batIdxLock);
	GDKfree(bn);
	return NULL;
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

	if ((f = fopen(heapfile, "rb")) == NULL) {
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
		unsigned int atomsize;
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
		if ((size_t) (st.st_size / atomsize) > (size_t) BUN_MAX) {
			fclose(f);
			GDKerror("heapfile too large\n");
			return NULL;
		}
		cap = (BUN) (st.st_size / atomsize);
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
			bn->tsorted = true;
			bn->trevsorted = true;
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

	b->batCapacity = newcap;

	theap_size *= Tsize(b);
	if (b->theap.base) {
		TRC_DEBUG(HEAP, "HEAPextend in BATextend %s %zu %zu\n",
			  b->theap.filename, b->theap.size, theap_size);
		if (HEAPextend(&b->theap, theap_size, b->batRestricted == BAT_READ) != GDK_SUCCEED)
			return GDK_FAIL;
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

	/* kill all search accelerators */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);
	PROPdestroy(b);

	/* we must dispose of all inserted atoms */
	if (force && BATatoms[b->ttype].atomDel == NULL) {
		assert(b->tvheap == NULL || b->tvheap->parentid == b->batCacheid);
		/* no stable elements: we do a quick heap clean */
		/* need to clean heap which keeps data even though the
		   BUNs got removed. This means reinitialize when
		   free > 0
		*/
		if (b->tvheap && b->tvheap->free > 0) {
			Heap th;

			th = (Heap) {
				.farmid = b->tvheap->farmid,
			};
			strcpy_len(th.filename, b->tvheap->filename, sizeof(th.filename));
			if (ATOMheap(b->ttype, &th, 0) != GDK_SUCCEED)
				return GDK_FAIL;
			th.parentid = b->tvheap->parentid;
			th.dirty = true;
			HEAPfree(b->tvheap, false);
			*b->tvheap = th;
		}
	} else {
		/* do heap-delete of all inserted atoms */
		void (*tatmdel)(Heap*,var_t*) = BATatoms[b->ttype].atomDel;

		/* TYPE_str has no del method, so we shouldn't get here */
		assert(tatmdel == NULL || b->twidth == sizeof(var_t));
		if (tatmdel) {
			BATiter bi = bat_iterator(b);

			for (p = b->batInserted, q = BUNlast(b); p < q; p++)
				(*tatmdel)(b->tvheap, (var_t*) BUNtloc(bi,p));
			b->tvheap->dirty = true;
		}
	}

	if (force)
		b->batInserted = 0;
	BATsetcount(b,0);
	BAThseqbase(b, 0);
	BATtseqbase(b, ATOMtype(b->ttype) == TYPE_oid ? 0 : oid_nil);
	b->batDirtydesc = true;
	b->theap.dirty = true;
	BATsettrivprop(b);
	b->tnosorted = b->tnorevsorted = 0;
	b->tnokey[0] = b->tnokey[1] = 0;
	return GDK_SUCCEED;
}

/* free a cached BAT; leave the bat descriptor cached */
void
BATfree(BAT *b)
{
	if (b == NULL)
		return;

	/* deallocate all memory for a bat */
	assert(b->batCacheid > 0);
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	b->tident = BATstring_t;
	HASHfree(b);
	IMPSfree(b);
	OIDXfree(b);
	if (b->ttype)
		HEAPfree(&b->theap, false);
	else
		assert(!b->theap.base);
	if (b->tvheap) {
		assert(b->tvheap->parentid == b->batCacheid);
		HEAPfree(b->tvheap, false);
	}
}

/* free a cached BAT descriptor */
void
BATdestroy(BAT *b)
{
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	b->tident = BATstring_t;
	if (b->tvheap)
		GDKfree(b->tvheap);
	PROPdestroy(b);
	MT_lock_destroy(&b->batIdxLock);
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
static void
heapmove(Heap *dst, Heap *src)
{
	HEAPfree(dst, false);
	*dst = *src;
}

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
/* TODO make it simpler, ie copy per column */
BAT *
COLcopy(BAT *b, int tt, bool writable, role_t role)
{
	BUN bunstocopy = BUN_NONE;
	BUN cnt;
	BAT *bn = NULL;

	BATcheck(b, NULL);
	assert(tt != TYPE_bat);
	cnt = b->batCount;

	/* maybe a bit ugly to change the requested bat type?? */
	if (b->ttype == TYPE_void && !writable)
		tt = TYPE_void;

	if (tt != b->ttype && wrongtype(tt, b->ttype)) {
		GDKerror("wrong tail-type requested\n");
		return NULL;
	}

	/* first try case (1); create a view, possibly with different
	 * atom-types */
	if (role == b->batRole &&
	    b->batRestricted == BAT_READ &&
	    (!VIEWtparent(b) ||
	     BBP_cache(VIEWtparent(b))->batRestricted == BAT_READ) &&
	    !writable) {
		bn = VIEWcreate(b->hseqbase, b);
		if (bn == NULL)
			return NULL;
		if (tt != bn->ttype) {
			bn->ttype = tt;
			bn->tvarsized = ATOMvarsized(tt);
			bn->tseqbase = ATOMtype(tt) == TYPE_oid ? b->tseqbase : oid_nil;
		}
	} else {
		/* check whether we need case (4); BUN-by-BUN copy (by
		 * setting bunstocopy != BUN_NONE) */
		if (ATOMsize(tt) != ATOMsize(b->ttype)) {
			/* oops, void materialization */
			bunstocopy = cnt;
		} else if (BATatoms[tt].atomFix) {
			/* oops, we need to fix/unfix atoms */
			bunstocopy = cnt;
		} else if (isVIEW(b)) {
			/* extra checks needed for views */
			bat tp = VIEWtparent(b);

			if (tp != 0 && BATcapacity(BBP_cache(tp)) > cnt + cnt)
				/* reduced slice view: do not copy too
				 * much garbage */
				bunstocopy = cnt;
		}

		bn = COLnew(b->hseqbase, tt, MAX(1, bunstocopy == BUN_NONE ? 0 : bunstocopy), role);
		if (bn == NULL)
			return NULL;

		if (bn->tvarsized && bn->ttype && bunstocopy == BUN_NONE) {
			bn->tshift = b->tshift;
			bn->twidth = b->twidth;
			if (HEAPextend(&bn->theap, BATcapacity(bn) << bn->tshift, true) != GDK_SUCCEED)
				goto bunins_failed;
		}

		if (tt == TYPE_void) {
			/* case (2): a void,void result => nothing to
			 * copy! */
			bn->theap.free = 0;
		} else if (bunstocopy == BUN_NONE) {
			/* case (3): just copy the heaps; if possible
			 * with copy-on-write VM support */
			Heap bthp, thp;

			bthp = (Heap) {
				.farmid = BBPselectfarm(role, b->ttype, offheap),
			};
			thp = (Heap) {
				.farmid = BBPselectfarm(role, b->ttype, varheap),
			};
			strconcat_len(bthp.filename, sizeof(bthp.filename),
				      BBP_physical(bn->batCacheid),
				      ".tail", NULL);
			strconcat_len(thp.filename, sizeof(thp.filename),
				      BBP_physical(bn->batCacheid),
				      ".theap", NULL);
			if ((b->ttype && HEAPcopy(&bthp, &b->theap) != GDK_SUCCEED) ||
			    (bn->tvheap && HEAPcopy(&thp, b->tvheap) != GDK_SUCCEED)) {
				HEAPfree(&thp, true);
				HEAPfree(&bthp, true);
				BBPreclaim(bn);
				return NULL;
			}
			/* succeeded; replace dummy small heaps by the
			 * real ones */
			heapmove(&bn->theap, &bthp);
			thp.parentid = bn->batCacheid;
			if (bn->tvheap)
				heapmove(bn->tvheap, &thp);

			/* make sure we use the correct capacity */
			bn->batCapacity = (BUN) (bn->ttype ? bn->theap.size >> bn->tshift : 0);
		} else if (BATatoms[tt].atomFix || ATOMextern(tt)) {
			/* case (4): one-by-one BUN insert (really slow) */
			BUN p, q, r = 0;
			BATiter bi = bat_iterator(b);

			BATloop(b, p, q) {
				const void *t = BUNtail(bi, p);

				if (bunfastapp_nocheck(bn, r, t, Tsize(bn)) != GDK_SUCCEED)
					goto bunins_failed;
				r++;
			}
			bn->theap.dirty |= bunstocopy > 0;
		} else if (tt != TYPE_void && b->ttype == TYPE_void) {
			/* case (4): optimized for unary void
			 * materialization */
			oid cur = b->tseqbase, *dst = (oid *) bn->theap.base;
			oid inc = !is_oid_nil(cur);

			bn->theap.free = bunstocopy * sizeof(oid);
			bn->theap.dirty |= bunstocopy > 0;
			while (bunstocopy--) {
				*dst++ = cur;
				cur += inc;
			}
		} else {
			/* case (4): optimized for simple array copy */
			bn->theap.free = bunstocopy * Tsize(bn);
			bn->theap.dirty |= bunstocopy > 0;
			memcpy(Tloc(bn, 0), Tloc(b, 0), bn->theap.free);
		}
		/* copy all properties (size+other) from the source bat */
		BATsetcount(bn, cnt);
	}
	/* set properties (note that types may have changed in the copy) */
	if (ATOMtype(tt) == ATOMtype(b->ttype)) {
		if (ATOMtype(tt) == TYPE_oid) {
			BATtseqbase(bn, b->tseqbase);
		} else {
			BATtseqbase(bn, oid_nil);
		}
		BATkey(bn, BATtkey(b));
		bn->tsorted = BATtordered(b);
		bn->trevsorted = BATtrevordered(b);
		bn->batDirtydesc = true;
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
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

#ifdef HAVE_HGE
#define un_move_sz16(src, dst, sz)			\
		if (sz == 16) {				\
			* (hge *) dst = * (hge *) src;	\
		} else
#else
#define un_move_sz16(src, dst, sz)
#endif

#define un_move(src, dst, sz)				\
	do {						\
		un_move_sz16(src,dst,sz)		\
		if (sz == 8) {				\
			* (lng *) dst = * (lng *) src;	\
		} else if (sz == 4) {			\
			* (int *) dst = * (int *) src;	\
		} else if (sz > 0) {			\
			char *_dst = (char *) dst;	\
			char *_src = (char *) src;	\
			char *_end = _src + sz;		\
							\
			while (_src < _end)		\
				*_dst++ = *_src++;	\
		}					\
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
setcolprops(BAT *b, const void *x)
{
	bool isnil = b->ttype != TYPE_void &&
		ATOMcmp(b->ttype, x, ATOMnilptr(b->ttype)) == 0;
	BATiter bi;
	BUN pos;
	const void *prv;
	int cmp;

	/* x may only be NULL if the column type is VOID */
	assert(x != NULL || b->ttype == TYPE_void);
	if (b->batCount == 0) {
		/* first value */
		b->tsorted = b->trevsorted = ATOMlinear(b->ttype);
		b->tnosorted = b->tnorevsorted = 0;
		b->tkey = true;
		b->tnokey[0] = b->tnokey[1] = 0;
		if (b->ttype == TYPE_void) {
			if (x) {
				b->tseqbase = * (const oid *) x;
			}
			b->tnil = is_oid_nil(b->tseqbase);
			b->tnonil = !b->tnil;
		} else {
			b->tnil = isnil;
			b->tnonil = !isnil;
			if (b->ttype == TYPE_oid) {
				b->tseqbase = * (const oid *) x;
			}
			if (!isnil && ATOMlinear(b->ttype)) {
				BATsetprop(b, GDK_MAX_VALUE, b->ttype, x);
				BATsetprop(b, GDK_MIN_VALUE, b->ttype, x);
				BATsetprop(b, GDK_MAX_POS, TYPE_oid, &(oid){0});
				BATsetprop(b, GDK_MIN_POS, TYPE_oid, &(oid){0});
			}
		}
		return;
	} else if (b->ttype == TYPE_void) {
		/* not the first value in a VOID column: we keep the
		 * seqbase, and x is not used, so only some properties
		 * are affected */
		if (!is_oid_nil(b->tseqbase)) {
			if (b->trevsorted) {
				b->tnorevsorted = BUNlast(b);
				b->trevsorted = false;
			}
			b->tnil = false;
			b->tnonil = true;
		} else {
			if (b->tkey) {
				b->tnokey[0] = 0;
				b->tnokey[1] = BUNlast(b);
				b->tkey = false;
			}
			b->tnil = true;
			b->tnonil = false;
		}
		return;
	} else if (ATOMlinear(b->ttype)) {
		PROPrec *prop;

		bi = bat_iterator(b);
		pos = BUNlast(b);
		prv = BUNtail(bi, pos - 1);
		cmp = ATOMcmp(b->ttype, prv, x);

		if (b->tkey &&
		    (cmp == 0 || /* definitely not KEY */
		     (b->batCount > 1 && /* can't guarantee KEY if unordered */
		      ((b->tsorted && cmp > 0) ||
		       (b->trevsorted && cmp < 0) ||
		       (!b->tsorted && !b->trevsorted))))) {
			b->tkey = false;
			if (cmp == 0) {
				b->tnokey[0] = pos - 1;
				b->tnokey[1] = pos;
			}
		}
		if (b->tsorted) {
			if (cmp > 0) {
				/* out of order */
				b->tsorted = false;
				b->tnosorted = pos;
			} else if (cmp < 0 && !isnil) {
				/* new largest value */
				BATsetprop(b, GDK_MAX_VALUE, b->ttype, x);
				BATsetprop(b, GDK_MAX_POS, TYPE_oid, &(oid){BATcount(b)});
			}
		} else if (!isnil &&
			   (prop = BATgetprop(b, GDK_MAX_VALUE)) != NULL &&
			   ATOMcmp(b->ttype, VALptr(&prop->v), x) < 0) {
			BATsetprop(b, GDK_MAX_VALUE, b->ttype, x);
			BATsetprop(b, GDK_MAX_POS, TYPE_oid, &(oid){BATcount(b)});
		}
		if (b->trevsorted) {
			if (cmp < 0) {
				/* out of order */
				b->trevsorted = false;
				b->tnorevsorted = pos;
				/* if there is a nil in the BAT, it is
				 * the smallest, but that doesn't
				 * count for the property, so the new
				 * value may still be smaller than the
				 * smallest non-nil so far */
				if (!b->tnonil && !isnil &&
				    (prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL &&
				    ATOMcmp(b->ttype, VALptr(&prop->v), x) > 0) {
					BATsetprop(b, GDK_MIN_VALUE, b->ttype, x);
					BATsetprop(b, GDK_MIN_POS, TYPE_oid, &(oid){BATcount(b)});
				}
			} else if (cmp > 0 && !isnil) {
				/* new smallest value */
				BATsetprop(b, GDK_MIN_VALUE, b->ttype, x);
				BATsetprop(b, GDK_MIN_POS, TYPE_oid, &(oid){BATcount(b)});
			}
		} else if (!isnil &&
			   (prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL &&
			   ATOMcmp(b->ttype, VALptr(&prop->v), x) > 0) {
			BATsetprop(b, GDK_MIN_VALUE, b->ttype, x);
			BATsetprop(b, GDK_MIN_POS, TYPE_oid, &(oid){BATcount(b)});
		}
		if (BATtdense(b) && (cmp >= 0 || * (const oid *) prv + 1 != * (const oid *) x)) {
			assert(b->ttype == TYPE_oid);
			b->tseqbase = oid_nil;
		}
	}
	if (isnil) {
		b->tnonil = false;
		b->tnil = true;
	}
}

/*
 * @+ BUNappend
 * The BUNappend function can be used to add a single value to void
 * and oid headed bats. The new head value will be a unique number,
 * (max(bat)+1).
 */
gdk_return
BUNappend(BAT *b, const void *t, bool force)
{
	BUN p;
	size_t tsize = 0;

	BATcheck(b, GDK_FAIL);

	assert(!VIEWtparent(b));

	p = BUNlast(b);		/* insert at end */
	if (p == BUN_MAX || b->batCount == BUN_MAX) {
		GDKerror("bat too large\n");
		return GDK_FAIL;
	}

	ALIGNapp(b, force, GDK_FAIL);
	b->batDirtydesc = true;
	if (b->thash && b->tvheap)
		tsize = b->tvheap->size;

	if (b->ttype == TYPE_void && BATtdense(b)) {
		if (b->batCount == 0) {
			b->tseqbase = * (const oid *) t;
		} else if (is_oid_nil(* (oid *) t) ||
			   b->tseqbase + b->batCount != *(const oid *) t) {
			if (BATmaterialize(b) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	}

	if (unshare_varsized_heap(b) != GDK_SUCCEED) {
		return GDK_FAIL;
	}

	setcolprops(b, t);

	if (b->ttype != TYPE_void) {
		if (bunfastapp(b, t) != GDK_SUCCEED)
			return GDK_FAIL;
		b->theap.dirty = true;
	} else {
		BATsetcount(b, b->batCount + 1);
	}


	IMPSdestroy(b); /* no support for inserts in imprints yet */
	OIDXdestroy(b);
	BATrmprop(b, GDK_NUNIQUE);
	BATrmprop(b, GDK_UNIQUE_ESTIMATE);
#if 0		/* enable if we have more properties than just min/max */
	PROPrec *prop;
	do {
		for (prop = b->tprops; prop; prop = prop->next)
			if (prop->id != GDK_MAX_VALUE &&
			    prop->id != GDK_MIN_VALUE &&
			    prop->id != GDK_HASH_BUCKETS) {
				BATrmprop(b, prop->id);
				break;
			}
	} while (prop);
#endif
	if (b->thash) {
		HASHins(b, p, t);
		if (b->thash)
			BATsetprop(b, GDK_NUNIQUE,
				   TYPE_oid, &(oid){b->thash->nunique});
		if (tsize && tsize != b->tvheap->size)
			HEAPwarm(b->tvheap);
	}
	return GDK_SUCCEED;
}

gdk_return
BUNdelete(BAT *b, oid o)
{
	BUN p;
	BATiter bi = bat_iterator(b);
	const void *val;
	PROPrec *prop;

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
	b->batDirtydesc = true;
	val = BUNtail(bi, p);
	if (ATOMcmp(b->ttype, ATOMnilptr(b->ttype), val) != 0) {
		if ((prop = BATgetprop(b, GDK_MAX_VALUE)) != NULL
		    && ATOMcmp(b->ttype, VALptr(&prop->v), val) >= 0) {
			BATrmprop(b, GDK_MAX_VALUE);
			BATrmprop(b, GDK_MAX_POS);
		}
		if ((prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL
		    && ATOMcmp(b->ttype, VALptr(&prop->v), val) <= 0) {
			BATrmprop(b, GDK_MIN_VALUE);
			BATrmprop(b, GDK_MIN_POS);
		}
	}
	if (ATOMunfix(b->ttype, val) != GDK_SUCCEED)
		return GDK_FAIL;
	ATOMdel(b->ttype, b->tvheap, (var_t *) BUNtloc(bi, p));
	if (p != BUNlast(b) - 1 &&
	    (b->ttype != TYPE_void || BATtdense(b))) {
		/* replace to-be-delete BUN with last BUN; materialize
		 * void column before doing so */
		if (b->ttype == TYPE_void &&
		    BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
		memcpy(Tloc(b, p), Tloc(b, BUNlast(b) - 1), Tsize(b));
		/* no longer sorted */
		b->tsorted = b->trevsorted = false;
		b->theap.dirty = true;
	}
	if (b->tnosorted >= p)
		b->tnosorted = 0;
	if (b->tnorevsorted >= p)
		b->tnorevsorted = 0;
	b->batCount--;
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
	IMPSdestroy(b);
	OIDXdestroy(b);
	HASHdestroy(b);
	BATrmprop(b, GDK_NUNIQUE);
	BATrmprop(b, GDK_UNIQUE_ESTIMATE);
#if 0		/* enable if we have more properties than just min/max */
	do {
		for (prop = b->tprops; prop; prop = prop->next)
			if (prop->id != GDK_MAX_VALUE &&
			    prop->id != GDK_MIN_VALUE &&
			    prop->id != GDK_HASH_BUCKETS) {
				BATrmprop(b, prop->id);
				break;
			}
	} while (prop);
#endif
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
BUNinplace(BAT *b, BUN p, const void *t, bool force)
{
	BUN last = BUNlast(b) - 1;
	BATiter bi = bat_iterator(b);
	int tt;
	BUN prv, nxt;
	const void *val;

	assert(p >= b->batInserted || force);

	/* uncommitted BUN elements */

	/* zap alignment info */
	if (!force && (b->batRestricted != BAT_WRITE || b->batSharecnt > 0)) {
		GDKerror("access denied to %s, aborting.\n",
			 BATgetId(b));
		return GDK_FAIL;
	}
	val = BUNtail(bi, p);	/* old value */
	if (b->tnil &&
	    ATOMcmp(b->ttype, val, ATOMnilptr(b->ttype)) == 0 &&
	    ATOMcmp(b->ttype, t, ATOMnilptr(b->ttype)) != 0) {
		/* if old value is nil and new value isn't, we're not
		 * sure anymore about the nil property, so we must
		 * clear it */
		b->tnil = false;
	}
	HASHdestroy(b);
	if (b->ttype != TYPE_void && ATOMlinear(b->ttype)) {
		PROPrec *prop;

		if ((prop = BATgetprop(b, GDK_MAX_VALUE)) != NULL) {
			if (ATOMcmp(b->ttype, t, ATOMnilptr(b->ttype)) != 0 &&
			    ATOMcmp(b->ttype, VALptr(&prop->v), t) < 0) {
				/* new value is larger than previous
				 * largest */
				BATsetprop(b, GDK_MAX_VALUE, b->ttype, t);
				BATsetprop(b, GDK_MAX_POS, TYPE_oid, &(oid){p});
			} else if (ATOMcmp(b->ttype, t, val) != 0 &&
				   ATOMcmp(b->ttype, VALptr(&prop->v), val) == 0) {
				/* old value is equal to largest and
				 * new value is smaller (see above),
				 * so we don't know anymore which is
				 * the largest */
				BATrmprop(b, GDK_MAX_VALUE);
				BATrmprop(b, GDK_MAX_POS);
			}
		}
		if ((prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL) {
			if (ATOMcmp(b->ttype, t, ATOMnilptr(b->ttype)) != 0 &&
			    ATOMcmp(b->ttype, VALptr(&prop->v), t) > 0) {
				/* new value is smaller than previous
				 * smallest */
				BATsetprop(b, GDK_MIN_VALUE, b->ttype, t);
				BATsetprop(b, GDK_MIN_POS, TYPE_oid, &(oid){p});
			} else if (ATOMcmp(b->ttype, t, val) != 0 &&
				   ATOMcmp(b->ttype, VALptr(&prop->v), val) <= 0) {
				/* old value is equal to smallest and
				 * new value is larger (see above), so
				 * we don't know anymore which is the
				 * smallest */
				BATrmprop(b, GDK_MIN_VALUE);
				BATrmprop(b, GDK_MIN_POS);
			}
		}
		BATrmprop(b, GDK_NUNIQUE);
		BATrmprop(b, GDK_UNIQUE_ESTIMATE);
#if 0		/* enable if we have more properties than just min/max */
		do {
			for (prop = b->tprops; prop; prop = prop->next)
				if (prop->id != GDK_MAX_VALUE &&
				    prop->id != GDK_MIN_VALUE &&
				    prop->id != GDK_HASH_BUCKETS) {
					BATrmprop(b, prop->id);
					break;
				}
		} while (prop);
#endif
	} else {
		PROPdestroy(b);
	}
	OIDXdestroy(b);
	IMPSdestroy(b);
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
		if (ATOMreplaceVAR(b->ttype, b->tvheap, &_d, t) != GDK_SUCCEED)
			return GDK_FAIL;
		if (b->twidth < SIZEOF_VAR_T &&
		    (b->twidth <= 2 ? _d - GDK_VAROFFSET : _d) >= ((size_t) 1 << (8 * b->twidth))) {
			/* doesn't fit in current heap, upgrade it */
			if (GDKupgradevarheap(b, _d, false, b->batRestricted == BAT_READ) != GDK_SUCCEED)
				return GDK_FAIL;
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
	} else {
		assert(BATatoms[b->ttype].atomPut == NULL);
		if (ATOMfix(b->ttype, t) != GDK_SUCCEED)
			return GDK_FAIL;
		if (ATOMunfix(b->ttype, BUNtloc(bi, p)) != GDK_SUCCEED)
			return GDK_FAIL;
		switch (ATOMsize(b->ttype)) {
		case 0:	     /* void */
			break;
		case 1:
			((bte *) b->theap.base)[p] = * (bte *) t;
			break;
		case 2:
			((sht *) b->theap.base)[p] = * (sht *) t;
			break;
		case 4:
			((int *) b->theap.base)[p] = * (int *) t;
			break;
		case 8:
			((lng *) b->theap.base)[p] = * (lng *) t;
			break;
#ifdef HAVE_HGE
		case 16:
			((hge *) b->theap.base)[p] = * (hge *) t;
			break;
#endif
		default:
			memcpy(BUNtloc(bi, p), t, ATOMsize(b->ttype));
			break;
		}
	}

	tt = b->ttype;
	prv = p > 0 ? p - 1 : BUN_NONE;
	nxt = p < last ? p + 1 : BUN_NONE;

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
	if (b->tnonil)
		b->tnonil = t && ATOMcmp(b->ttype, t, ATOMnilptr(b->ttype)) != 0;
	b->theap.dirty = true;
	if (b->tvheap)
		b->tvheap->dirty = true;

	return GDK_SUCCEED;
}

/* very much like void_inplace, except this materializes a void tail
 * column if necessarry */
gdk_return
BUNreplace(BAT *b, oid id, const void *t, bool force)
{
	BATcheck(b, GDK_FAIL);
	if (t == NULL) {
		GDKerror("tail value is nil");
		return GDK_FAIL;
	}

	if (id < b->hseqbase || id >= b->hseqbase + BATcount(b))
		return GDK_SUCCEED;

	if (b->ttype == TYPE_void) {
		/* no need to materialize if value doesn't change */
		if (is_oid_nil(b->tseqbase) ||
		    b->tseqbase + id - b->hseqbase == *(const oid *) t)
			return GDK_SUCCEED;
		if (BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
	}

	return BUNinplace(b, id - b->hseqbase, t, force);
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
	return BUNinplace(b, id - b->hseqbase, val, force);
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

	BATcheck(b, BUN_NONE);
	if (!v)
		return r;
	if (b->ttype == TYPE_void && b->tvheap != NULL) {
		struct canditer ci;
		canditer_init(&ci, NULL, b);
		return canditer_search(&ci, * (const oid *) v, false);
	}
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
	/* head column is always VOID, and some head properties never change */
	assert(!is_oid_nil(b->hseqbase));
	assert(cnt <= BUN_MAX);

	b->batCount = cnt;
	b->batDirtydesc = true;
	b->theap.free = tailsize(b, cnt);
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
	assert(b->batCacheid > 0);
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
	if (b->tkey != flag)
		b->batDirtydesc = true;
	b->tkey = flag;
	if (!flag) {
		b->tseqbase = oid_nil;
	} else
		b->tnokey[0] = b->tnokey[1] = 0;
	if (flag && VIEWtparent(b)) {
		/* if a view is key, then so is the parent if the two
		 * are aligned */
		BAT *bp = BBP_cache(VIEWtparent(b));
		if (BATcount(b) == BATcount(bp) &&
		    ATOMtype(BATttype(b)) == ATOMtype(BATttype(bp)) &&
		    !BATtkey(bp) &&
		    ((BATtvoid(b) && BATtvoid(bp) && b->tseqbase == bp->tseqbase) ||
		     BATcount(b) == 0))
			return BATkey(bp, true);
	}
	return GDK_SUCCEED;
}

void
BAThseqbase(BAT *b, oid o)
{
	if (b != NULL) {
		assert(o <= GDK_oid_max);	/* i.e., not oid_nil */
		assert(o + BATcount(b) <= GDK_oid_max);
		assert(b->batCacheid > 0);
		if (b->hseqbase != o) {
			b->batDirtydesc = true;
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
	assert(b->batCacheid > 0);
	if (b->tseqbase != o) {
		b->batDirtydesc = true;
	}
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
	if (batpath == NULL || bakpath == NULL) {
		ret = -1;
		goto bailout;
	}
	batret = stat(batpath, &st);
	bakret = stat(bakpath, &st);

	if (batret == 0 && bakret) {
		/* no backup yet, so move the existing X.new there out
		 * of the way */
		if ((ret = rename(batpath, bakpath)) < 0)
			GDKsyserror("backup_new: rename %s to %s failed\n",
				    batpath, bakpath);
		TRC_DEBUG(IO_, "rename(%s,%s) = %d\n", batpath, bakpath, ret);
	} else if (batret == 0) {
		/* there is a backup already; just remove the X.new */
		if ((ret = remove(batpath)) != 0)
			GDKsyserror("backup_new: remove %s failed\n", batpath);
		TRC_DEBUG(IO_, "remove(%s) = %d\n", batpath, ret);
	}
  bailout:
	GDKfree(batpath);
	GDKfree(bakpath);
	for (xx = lockbat; xx >= 0; xx--)
		MT_lock_unset(&GDKtrimLock(xx));
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
		return backup_new(hp, BBP_THREADMASK) != GDK_SUCCEED ? STORE_INVALID : STORE_MMAP;	/* only called for existing bats */
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
		hp->dirty = true;	/* 2=>6 */
	return STORE_PRIV;	/* 1=>5,2=>6,3=>7,a=>c,b=>6 states */
}


#define ATOMappendpriv(t, h) (ATOMstorage(t) != TYPE_str || GDK_ELIMDOUBLES(h))

/* change the heap modes at a commit */
gdk_return
BATcheckmodes(BAT *b, bool existing)
{
	bool wr = (b->batRestricted == BAT_WRITE);
	storage_t m1 = STORE_MEM, m3 = STORE_MEM;
	bool dirty = false;

	BATcheck(b, GDK_FAIL);

	if (b->ttype) {
		m1 = HEAPcommitpersistence(&b->theap, wr, existing);
		dirty |= (b->theap.newstorage != m1);
	}

	if (b->tvheap) {
		bool ta = (b->batRestricted == BAT_APPEND) && ATOMappendpriv(b->ttype, b->tvheap);
		m3 = HEAPcommitpersistence(b->tvheap, wr || ta, existing);
		dirty |= (b->tvheap->newstorage != m3);
	}
	if (m1 == STORE_INVALID || m3 == STORE_INVALID)
		return GDK_FAIL;

	if (dirty) {
		b->batDirtydesc = true;
		b->theap.newstorage = m1;
		if (b->tvheap)
			b->tvheap->newstorage = m3;
	}
	return GDK_SUCCEED;
}

gdk_return
BATsetaccess(BAT *b, restrict_t newmode)
{
	restrict_t bakmode;
	bool bakdirty;

	BATcheck(b, GDK_FAIL);
	if (isVIEW(b) && newmode != BAT_READ) {
		if (VIEWreset(b) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	bakmode = (restrict_t) b->batRestricted;
	bakdirty = b->batDirtydesc;
	if (bakmode != newmode || (b->batSharecnt && newmode != BAT_READ)) {
		bool existing = (BBP_status(b->batCacheid) & BBPEXISTING) != 0;
		bool wr = (newmode == BAT_WRITE);
		bool rd = (bakmode == BAT_WRITE);
		storage_t m1, m3 = STORE_MEM;
		storage_t b1, b3 = STORE_MEM;

		if (b->batSharecnt && newmode != BAT_READ) {
			TRC_DEBUG(BAT_, "%s has %d views; try creating a copy\n", BATgetId(b), b->batSharecnt);
			GDKerror("%s has %d views\n",
				 BATgetId(b), b->batSharecnt);
			return GDK_FAIL;
		}

		b1 = b->theap.newstorage;
		m1 = HEAPchangeaccess(&b->theap, ACCESSMODE(wr, rd), existing);
		if (b->tvheap) {
			bool ta = (newmode == BAT_APPEND && ATOMappendpriv(b->ttype, b->tvheap));
			b3 = b->tvheap->newstorage;
			m3 = HEAPchangeaccess(b->tvheap, ACCESSMODE(wr && ta, rd && ta), existing);
		}
		if (m1 == STORE_INVALID || m3 == STORE_INVALID)
			return GDK_FAIL;

		/* set new access mode and mmap modes */
		b->batRestricted = (unsigned int) newmode;
		b->batDirtydesc = true;
		b->theap.newstorage = m1;
		if (b->tvheap)
			b->tvheap->newstorage = m3;

		if (existing && BBPsave(b) != GDK_SUCCEED) {
			/* roll back all changes */
			b->batRestricted = (unsigned int) bakmode;
			b->batDirtydesc = bakdirty;
			b->theap.newstorage = b1;
			if (b->tvheap)
				b->tvheap->newstorage = b3;
			return GDK_FAIL;
		}
	}
	return GDK_SUCCEED;
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

	if (b->batRole == TRANSIENT && !transient) {
		GDKerror("cannot change mode of BAT in TRANSIENT farm.\n");
		return GDK_FAIL;
	}

	if (transient != b->batTransient) {
		bat bid = b->batCacheid;

		if (!transient) {
			check_type(b->ttype);
		}

		if (!transient && isVIEW(b)) {
			if (VIEWreset(b) != GDK_SUCCEED) {
				return GDK_FAIL;
			}
		}
		/* persistent BATs get a logical reference */
		if (!transient) {
			BBPretain(bid);
		} else if (!b->batTransient) {
			BBPrelease(bid);
		}
		MT_lock_set(&GDKswapLock(bid));
		if (!transient) {
			if (!(BBP_status(bid) & BBPDELETED))
				BBP_status_on(bid, BBPNEW, "BATmode");
			else
				BBP_status_on(bid, BBPEXISTING, "BATmode");
			BBP_status_off(bid, BBPDELETED, "BATmode");
		} else if (!b->batTransient) {
			if (!(BBP_status(bid) & BBPNEW))
				BBP_status_on(bid, BBPDELETED, "BATmode");
			BBP_status_off(bid, BBPPERSISTENT, "BATmode");
		}
		/* session bats or persistent bats that did not
		 * witness a commit yet may have been saved */
		if (b->batCopiedtodisk) {
			if (!transient) {
				BBP_status_off(bid, BBPTMP, "BATmode");
			} else {
				/* TMcommit must remove it to
				 * guarantee free space */
				BBP_status_on(bid, BBPTMP, "BATmode");
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
 */

void
BATassertProps(BAT *b)
{
	unsigned bbpstatus;
	BATiter bi = bat_iterator(b);
	BUN p, q;
	int (*cmpf)(const void *, const void *);
	int cmp;
	const void *prev = NULL, *valp, *nilp;

	/* general BAT sanity */
	assert(b != NULL);
	assert(b->batCacheid > 0);
	assert(b->batCount >= b->batInserted);

	/* headless */
	assert(b->hseqbase <= GDK_oid_max); /* non-nil seqbase */
	assert(b->hseqbase + BATcount(b) <= GDK_oid_max);

	bbpstatus = BBP_status(b->batCacheid);
	/* only at most one of BBPDELETED, BBPEXISTING, BBPNEW may be set */
	assert(((bbpstatus & BBPDELETED) != 0) +
	       ((bbpstatus & BBPEXISTING) != 0) +
	       ((bbpstatus & BBPNEW) != 0) <= 1);

	assert(b != NULL);
	assert(b->ttype >= TYPE_void);
	assert(b->ttype < GDKatomcnt);
	assert(b->ttype != TYPE_bat);
	assert(isVIEW(b) ||
	       b->ttype == TYPE_void ||
	       BBPfarms[b->theap.farmid].roles & (1 << b->batRole));
	assert(isVIEW(b) ||
	       b->tvheap == NULL ||
	       (BBPfarms[b->tvheap->farmid].roles & (1 << b->batRole)));

	assert(VIEWtparent(b) == 0 || b->tvheap == NULL || b->tvheap->parentid != b->batCacheid);

	cmpf = ATOMcompare(b->ttype);
	nilp = ATOMnilptr(b->ttype);

	assert(b->theap.free >= tailsize(b, BUNlast(b)));
	if (b->ttype != TYPE_void) {
		assert(b->batCount <= b->batCapacity);
		assert(b->theap.size >= b->theap.free);
		assert(b->theap.size >> b->tshift >= b->batCapacity);
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
				assert(b->tvheap->free % SIZEOF_OID == 0);
				if (b->tvheap->free > 0) {
					const oid *oids = (const oid *) b->tvheap->base;
					q = b->tvheap->free / SIZEOF_OID;
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
		return;
	}
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
		if (!b->tsorted &&
		    b->tnosorted > 0 &&
		    b->tnosorted < b->batCount)
			assert(cmpf(BUNtail(bi, b->tnosorted - 1),
				    BUNtail(bi, b->tnosorted)) > 0);
		assert(b->tnorevsorted == 0 ||
		       (b->tnorevsorted > 0 &&
			b->tnorevsorted < b->batCount));
		assert(!b->trevsorted || b->tnorevsorted == 0);
		if (!b->trevsorted &&
		    b->tnorevsorted > 0 &&
		    b->tnorevsorted < b->batCount)
			assert(cmpf(BUNtail(bi, b->tnorevsorted - 1),
				    BUNtail(bi, b->tnorevsorted)) < 0);
	}
	/* if tkey property set, both tnokey values must be 0 */
	assert(!b->tkey || (b->tnokey[0] == 0 && b->tnokey[1] == 0));
	if (!b->tkey && (b->tnokey[0] != 0 || b->tnokey[1] != 0)) {
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
		return;
	}

	PROPDEBUG { /* only do a scan if property checking is requested */
		PROPrec *prop;
		const void *maxval = NULL;
		const void *minval = NULL;
		bool seenmax = false, seenmin = false;
		bool seennil = false;

		if ((prop = BATgetprop(b, GDK_MAX_VALUE)) != NULL)
			maxval = VALptr(&prop->v);
		if ((prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL)
			minval = VALptr(&prop->v);
		if ((prop = BATgetprop(b, GDK_MAX_POS)) != NULL) {
			if (maxval) {
				assert(prop->v.vtype == TYPE_oid);
				assert(prop->v.val.oval < b->batCount);
				valp = BUNtail(bi, prop->v.val.oval);
				assert(cmpf(maxval, valp) == 0);
			}
		}
		if ((prop = BATgetprop(b, GDK_MIN_POS)) != NULL) {
			if (minval) {
				assert(prop->v.vtype == TYPE_oid);
				assert(prop->v.val.oval < b->batCount);
				valp = BUNtail(bi, prop->v.val.oval);
				assert(cmpf(minval, valp) == 0);
			}
		}
		if (b->tsorted || b->trevsorted || !b->tkey) {
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
				     hb != HASHnil(hs);
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
}
