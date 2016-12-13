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
	bn->tident = BATstring_t;
}

BAT *
BATcreatedesc(oid hseq, int tt, int heapnames, int role)
{
	BAT *bn;

	/*
	 * Alloc space for the BAT and its dependent records.
	 */
	assert(tt >= 0);
	assert(role >= 0 && role < 32);

	bn = GDKzalloc(sizeof(BAT));

	if (bn == NULL)
		return NULL;

	/*
	 * Fill in basic column info
	 */
	bn->hseqbase = hseq;

	bn->ttype = tt;
	bn->tkey = FALSE;
	bn->tunique = FALSE;
	bn->tnonil = TRUE;
	bn->tnil = FALSE;
	bn->tsorted = bn->trevsorted = ATOMlinear(tt) != 0;
	bn->tident = BATstring_t;
	bn->tseqbase = (tt == TYPE_void) ? oid_nil : 0;
	bn->tprops = NULL;

	bn->batRole = role;
	bn->batPersistence = TRANSIENT;
	/*
	 * add to BBP
	 */
	BBPinsert(bn);
	/*
 	* Default zero for order oid index
 	*/
	bn->torderidx = 0;
	/*
	 * fill in heap names, so HEAPallocs can resort to disk for
	 * very large writes.
	 */
	assert(bn->batCacheid > 0);
	bn->theap.filename = NULL;
	bn->theap.farmid = BBPselectfarm(role, bn->ttype, offheap);
	if (heapnames) {
		const char *nme = BBP_physical(bn->batCacheid);

		if (tt) {
			bn->theap.filename = GDKfilepath(NOFARM, NULL, nme, "tail");
			if (bn->theap.filename == NULL)
				goto bailout;
		}

		if (ATOMneedheap(tt)) {
			if ((bn->tvheap = (Heap *) GDKzalloc(sizeof(Heap))) == NULL ||
			    (bn->tvheap->filename = GDKfilepath(NOFARM, NULL, nme, "theap")) == NULL)
				goto bailout;
			bn->tvheap->parentid = bn->batCacheid;
			bn->tvheap->farmid = BBPselectfarm(role, bn->ttype, varheap);
		}
	}
	bn->batDirty = TRUE;
	return bn;
      bailout:
	if (tt)
		HEAPfree(&bn->theap, 1);
	if (bn->tvheap) {
		HEAPfree(bn->tvheap, 1);
		GDKfree(bn->tvheap);
	}
	GDKfree(bn);
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
static BAT *
BATnewstorage(oid hseq, int tt, BUN cap, int role)
{
	BAT *bn;

	/* and in case we don't have assertions enabled: limit the size */
	if (cap > BUN_MAX) {
		/* shouldn't happen, but if it does... */
		assert(0);
		cap = BUN_MAX;
	}
	bn = BATcreatedesc(hseq, tt, tt != TYPE_void, role);
	if (bn == NULL)
		return NULL;

	BATsetdims(bn);
	bn->batCapacity = cap;

	/* alloc the main heaps */
	if (tt && HEAPalloc(&bn->theap, cap, bn->twidth) != GDK_SUCCEED) {
		goto bailout;
	}

	if (ATOMheap(tt, bn->tvheap, cap) != GDK_SUCCEED) {
		GDKfree(bn->tvheap);
		goto bailout;
	}
	DELTAinit(bn);
	BBPcacheit(bn, 1);
	return bn;
  bailout:
	HEAPfree(&bn->theap, 1);
	GDKfree(bn);
	return NULL;
}

BAT *
COLnew(oid hseq, int tt, BUN cap, int role)
{
	assert(cap <= BUN_MAX);
	assert(hseq <= oid_nil);
	assert(tt != TYPE_bat);
	ERRORcheck((tt < 0) || (tt > GDKatomcnt), "COLnew:tt error\n", NULL);
	ERRORcheck(role < 0 || role >= 32, "COLnew:role error\n", NULL);

	/* round up to multiple of BATTINY */
	if (cap < BUN_MAX - BATTINY)
		cap = (cap + BATTINY - 1) & ~(BATTINY - 1);
	if (cap < BATTINY)
		cap = BATTINY;
	/* and in case we don't have assertions enabled: limit the size */
	if (cap > BUN_MAX)
		cap = BUN_MAX;
	return BATnewstorage(hseq, tt, cap, role);
}

BAT *
BATdense(oid hseq, oid tseq, BUN cnt)
{
	BAT *bn;

	bn = COLnew(hseq, TYPE_void, 0, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATtseqbase(bn, tseq);
	BATsetcount(bn, cnt);
	return bn;
}

BAT *
BATattach(int tt, const char *heapfile, int role)
{
	BAT *bn;
	char *p;
	size_t m;
	FILE *f;

	ERRORcheck(tt <= 0 , "BATattach: bad tail type (<=0)\n", NULL);
	ERRORcheck(ATOMvarsized(tt) && ATOMstorage(tt) != TYPE_str, "BATattach: bad tail type (varsized and not str)\n", NULL);
	ERRORcheck(heapfile == NULL, "BATattach: bad heapfile name\n", NULL);
	ERRORcheck(role < 0 || role >= 32, "BATattach: role error\n", NULL);

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
				p = GDKrealloc(p, m);
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
				if (BUNappend(bn, p, 0) != GDK_SUCCEED) {
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
			GDKerror("BATattach: last string is not null-terminated\n");
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
			GDKerror("BATattach: heapfile size not integral number of atoms\n");
			return NULL;
		}
		if ((size_t) (st.st_size / atomsize) > (size_t) BUN_MAX) {
			fclose(f);
			GDKerror("BATattach: heapfile too large\n");
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
			GDKerror("BATattach: couldn't read the complete file\n");
			BBPreclaim(bn);
			return NULL;
		}
		BATsetcount(bn, cap);
		bn->tnonil = cap == 0;
		bn->tnil = 0;
		bn->tdense = 0;
		if (cap > 1) {
			bn->tsorted = 0;
			bn->trevsorted = 0;
			bn->tkey = 0;
		} else {
			bn->tsorted = 1;
			bn->trevsorted = 1;
			bn->tkey = 1;
		}
	}
	return bn;

  notutf8:
	fclose(f);
	BBPreclaim(bn);
	GDKfree(p);
	GDKerror("BATattach: input is not UTF-8\n");
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
	if (b->theap.base && GDKdebug & HEAPMASK)
		fprintf(stderr, "#HEAPextend in BATextend %s " SZFMT " " SZFMT "\n", b->theap.filename, b->theap.size, theap_size);
	if (b->theap.base &&
	    HEAPextend(&b->theap, theap_size, b->batRestricted == BAT_READ) != GDK_SUCCEED)
		return GDK_FAIL;
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);
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

	if (!force && b->batInserted > 0) {
		GDKerror("BATclear: cannot clear committed BAT\n");
		return GDK_FAIL;
	}

	/* kill all search accelerators */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);

	/* we must dispose of all inserted atoms */
	if (force && BATatoms[b->ttype].atomDel == NULL) {
		Heap th;

		/* no stable elements: we do a quick heap clean */
		/* need to clean heap which keeps data even though the
		   BUNs got removed. This means reinitialize when
		   free > 0
		*/
		memset(&th, 0, sizeof(th));
		if (b->tvheap) {
			th.farmid = b->tvheap->farmid;
			if (b->tvheap->free > 0 &&
			    ATOMheap(b->ttype, &th, 0) != GDK_SUCCEED)
				return GDK_FAIL;
		}
		assert(b->tvheap == NULL || b->tvheap->parentid == b->batCacheid);
		if (b->tvheap && b->tvheap->free > 0) {
			th.parentid = b->tvheap->parentid;
			HEAPfree(b->tvheap, 0);
			*b->tvheap = th;
		}
	} else {
		/* do heap-delete of all inserted atoms */
		void (*tatmdel)(Heap*,var_t*) = BATatoms[b->ttype].atomDel;

		/* TYPE_str has no del method, so we shouldn't get here */
		assert(tatmdel == NULL || b->twidth == sizeof(var_t));
		if (tatmdel) {
			BATiter bi = bat_iterator(b);

			for(p = b->batInserted, q = BUNlast(b); p < q; p++)
				(*tatmdel)(b->tvheap, (var_t*) BUNtloc(bi,p));
		}
	}

	if (force)
		b->batInserted = 0;
	BATsetcount(b,0);
	BAThseqbase(b, 0);
	BATtseqbase(b, 0);
	b->batDirty = TRUE;
	BATsettrivprop(b);
	b->tnosorted = b->tnorevsorted = b->tnodense = 0;
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
	if (b->tprops)
		PROPdestroy(b->tprops);
	b->tprops = NULL;
	HASHfree(b);
	IMPSfree(b);
	OIDXfree(b);
	if (b->ttype)
		HEAPfree(&b->theap, 0);
	else
		assert(!b->theap.base);
	if (b->tvheap) {
		assert(b->tvheap->parentid == b->batCacheid);
		HEAPfree(b->tvheap, 0);
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
	if (b->tprops)
		PROPdestroy(b->tprops);
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
		if (tt != bn->ttype) {
			bn->ttype = tt;
			bn->tvarsized = ATOMvarsized(tt);
			bn->tseqbase = b->tseqbase;
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

		bn = COLnew(0, tt, MAX(1, bunstocopy == BUN_NONE ? 0 : bunstocopy), role);
		if (bn == NULL)
			return NULL;

		if (bn->tvarsized && bn->ttype && bunstocopy == BUN_NONE) {
			bn->tshift = b->tshift;
			bn->twidth = b->twidth;
			if (HEAPextend(&bn->theap, BATcapacity(bn) << bn->tshift, TRUE) != GDK_SUCCEED)
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

			memset(&bthp, 0, sizeof(Heap));
			memset(&thp, 0, sizeof(Heap));

			bthp.farmid = BBPselectfarm(role, b->ttype, offheap);
			thp.farmid = BBPselectfarm(role, b->ttype, varheap);
			if ((b->ttype && heapcopy(bn, "tail", &bthp, &b->theap) != GDK_SUCCEED) ||
			    (bn->tvheap && heapcopy(bn, "theap", &thp, b->tvheap) != GDK_SUCCEED)) {
				HEAPfree(&thp, 1);
				HEAPfree(&bthp, 1);
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


			/* first/inserted must point equally far into
			 * the heap as in the source */
			bn->batInserted = b->batInserted;
		} else if (BATatoms[tt].atomFix || tt != TYPE_void || ATOMextern(tt)) {
			/* case (4): one-by-one BUN insert (really slow) */
			BUN p, q, r = 0;
			BATiter bi = bat_iterator(b);

			BATloop(b, p, q) {
				const void *t = BUNtail(bi, p);

				bunfastapp_nocheck(bn, r, t, Tsize(bn));
				r++;
			}
		} else if (tt != TYPE_void && b->ttype == TYPE_void) {
			/* case (4): optimized for unary void
			 * materialization */
			oid cur = b->tseqbase, *dst = (oid *) bn->theap.base;
			oid inc = (cur != oid_nil);

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
	ALIGNsetH(bn, b);
	if (ATOMtype(tt) == ATOMtype(b->ttype)) {
		ALIGNsetT(bn, b);
	} else if (ATOMstorage(tt) == ATOMstorage(b->ttype) &&
		   ATOMcompare(tt) == ATOMcompare(b->ttype)) {
		BUN h = BUNlast(b);
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->tdense = b->tdense && ATOMtype(bn->ttype) == TYPE_oid;
		if (b->tkey)
			BATkey(bn, TRUE);
		bn->tnonil = b->tnonil;
		if (b->tnosorted > 0 && b->tnosorted < h)
			bn->tnosorted = b->tnosorted;
		else
			bn->tnosorted = 0;
		if (b->tnorevsorted > 0 && b->tnorevsorted < h)
			bn->tnorevsorted = b->tnorevsorted;
		else
			bn->tnorevsorted = 0;
		if (b->tnodense > 0 && b->tnodense < h)
			bn->tnodense = b->tnodense;
		else
			bn->tnodense = 0;
		if (b->tnokey[0] < h &&
		    b->tnokey[1] < h &&
		    b->tnokey[0] != b->tnokey[1]) {
			bn->tnokey[0] = b->tnokey[0];
			bn->tnokey[1] = b->tnokey[1];
		} else {
			bn->tnokey[0] = bn->tnokey[1] = 0;
		}
	} else {
		bn->tsorted = bn->trevsorted = 0; /* set based on count later */
		bn->tdense = bn->tnonil = 0;
		bn->tnosorted = bn->tnorevsorted = bn->tnodense = 0;
		bn->tnokey[0] = bn->tnokey[1] = 0;
	}
	if (BATcount(bn) <= 1) {
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
setcolprops(BAT *b, const void *x)
{
	int isnil = b->ttype != TYPE_void &&
		atom_CMP(x, ATOMnilptr(b->ttype), b->ttype) == 0;
	BATiter bi;
	BUN pos;
	const void *prv;
	int cmp;

	/* x may only be NULL if the column type is VOID */
	assert(x != NULL || b->ttype == TYPE_void);
	if (b->batCount == 0) {
		/* first value */
		b->tsorted = b->trevsorted = ATOMlinear(b->ttype) != 0;
		b->tnosorted = b->tnorevsorted = 0;
		b->tkey = 1;
		b->tnokey[0] = b->tnokey[1] = 0;
		b->tnodense = 0;
		if (b->ttype == TYPE_void) {
			if (x) {
				b->tseqbase = * (const oid *) x;
			}
			b->tnil = b->tseqbase == oid_nil;
			b->tnonil = !b->tnil;
		} else {
			b->tnil = isnil;
			b->tnonil = !isnil;
			if (b->ttype == TYPE_oid) {
				b->tdense = !isnil;
				b->tseqbase = * (const oid *) x;
				if (isnil)
					b->tnodense = BUNlast(b);
			}
		}
	} else if (b->ttype == TYPE_void) {
		/* not the first value in a VOID column: we keep the
		 * seqbase, and x is not used, so only some properties
		 * are affected */
		if (b->tseqbase != oid_nil) {
			if (b->trevsorted) {
				b->tnorevsorted = BUNlast(b);
				b->trevsorted = 0;
			}
			b->tnil = 0;
			b->tnonil = 1;
		} else {
			if (b->tkey) {
				b->tnokey[0] = 0;
				b->tnokey[1] = BUNlast(b);
				b->tkey = 0;
			}
			b->tnil = 1;
			b->tnonil = 0;
		}
	} else {
		bi = bat_iterator(b);
		pos = BUNlast(b);
		prv = BUNtail(bi, pos - 1);
		cmp = atom_CMP(prv, x, b->ttype);

		if (!b->tunique && /* assume outside check if tunique */
		    b->tkey &&
		    (cmp == 0 || /* definitely not KEY */
		     (b->batCount > 1 && /* can't guarantee KEY if unordered */
		      ((b->tsorted && cmp > 0) ||
		       (b->trevsorted && cmp < 0) ||
		       (!b->tsorted && !b->trevsorted))))) {
			b->tkey = 0;
			if (cmp == 0) {
				b->tnokey[0] = pos - 1;
				b->tnokey[1] = pos;
			}
		}
		if (b->tsorted && cmp > 0) {
			/* out of order */
			b->tsorted = 0;
			b->tnosorted = pos;
		}
		if (b->trevsorted && cmp < 0) {
			/* out of order */
			b->trevsorted = 0;
			b->tnorevsorted = pos;
		}
		if (b->tdense && (cmp >= 0 || * (const oid *) prv + 1 != * (const oid *) x)) {
			b->tdense = 0;
			b->tnodense = pos;
		}
		if (isnil) {
			b->tnonil = 0;
			b->tnil = 1;
		}
	}
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

	assert(!isVIEW(b));
	if (b->tunique && BUNfnd(b, t) != BUN_NONE) {
		return GDK_SUCCEED;
	}

	p = BUNlast(b);		/* insert at end */
	if (p == BUN_MAX || b->batCount == BUN_MAX) {
		GDKerror("BUNappend: bat too large\n");
		return GDK_FAIL;
	}

	ALIGNapp(b, "BUNappend", force, GDK_FAIL);
	b->batDirty = 1;
	if (b->thash && b->tvheap)
		tsize = b->tvheap->size;

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

	setcolprops(b, t);

	if (b->ttype != TYPE_void) {
		bunfastapp(b, t);
	} else {
		BATsetcount(b, b->batCount + 1);
	}


	IMPSdestroy(b); /* no support for inserts in imprints yet */
	OIDXdestroy(b);
	if (b->thash == (Hash *) 1) {
		/* don't bother first loading the hash to then change it */
		HASHdestroy(b);
	}
	if (b->thash) {
		HASHins(b, p, t);
		if (tsize && tsize != b->tvheap->size)
			HEAPwarm(b->tvheap);
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

	assert(b->hseqbase != oid_nil || BATcount(b) == 0);
	if (o < b->hseqbase || o >= b->hseqbase + BATcount(b)) {
		/* value already not there */
		return GDK_SUCCEED;
	}
	assert(BATcount(b) > 0); /* follows from "if" above */
	p = o - b->hseqbase;
	if (p < b->batInserted) {
		GDKerror("BUNdelete: cannot delete committed value\n");
		return GDK_FAIL;
	}
	b->batDirty = 1;
	ATOMunfix(b->ttype, BUNtail(bi, p));
	ATOMdel(b->ttype, b->tvheap, (var_t *) BUNtloc(bi, p));
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
	if (b->tnosorted >= p)
		b->tnosorted = 0;
	if (b->tnorevsorted >= p)
		b->tnorevsorted = 0;
	b->batCount--;
	if (b->batCount <= 1) {
		/* some trivial properties */
		b->tkey = 1;
		b->tsorted = b->trevsorted = 1;
		b->tnosorted = b->tnorevsorted = 0;
		if (b->batCount == 0) {
			b->tnil = 0;
			b->tnonil = 1;
		}
	}
	IMPSdestroy(b);
	OIDXdestroy(b);
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
	BATiter bi = bat_iterator(b);
	int tt;
	BUN prv, nxt;

	assert(p >= b->batInserted || force);

	/* uncommitted BUN elements */

	ALIGNinp(b, "BUNinplace", force, GDK_FAIL);	/* zap alignment info */
	if (b->tnil &&
	    atom_CMP(BUNtail(bi, p), ATOMnilptr(b->ttype), b->ttype) == 0 &&
	    atom_CMP(t, ATOMnilptr(b->ttype), b->ttype) != 0) {
		/* if old value is nil and new value isn't, we're not
		 * sure anymore about the nil property, so we must
		 * clear it */
		b->tnil = 0;
	}
	HASHdestroy(b);
	Treplacevalue(b, BUNtloc(bi, p), t);

	tt = b->ttype;
	prv = p > 0 ? p - 1 : BUN_NONE;
	nxt = p < last ? p + 1 : BUN_NONE;

	if (BATtordered(b)) {
		if (prv != BUN_NONE &&
		    ATOMcmp(tt, t, BUNtail(bi, prv)) < 0) {
			b->tsorted = FALSE;
			b->tnosorted = p;
		} else if (nxt != BUN_NONE &&
			   ATOMcmp(tt, t, BUNtail(bi, nxt)) > 0) {
			b->tsorted = FALSE;
			b->tnosorted = nxt;
		} else if (b->ttype != TYPE_void && b->tdense) {
			if (prv != BUN_NONE &&
			    1 + * (oid *) BUNtloc(bi, prv) != * (oid *) t) {
				b->tdense = FALSE;
				b->tnodense = p;
			} else if (nxt != BUN_NONE &&
				   * (oid *) BUNtloc(bi, nxt) != 1 + * (oid *) t) {
				b->tdense = FALSE;
				b->tnodense = nxt;
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
			b->trevsorted = FALSE;
			b->tnorevsorted = p;
		} else if (nxt != BUN_NONE &&
			   ATOMcmp(tt, t, BUNtail(bi, nxt)) < 0) {
			b->trevsorted = FALSE;
			b->tnorevsorted = nxt;
		}
	} else if (b->tnorevsorted >= p)
		b->tnorevsorted = 0;
	if (((b->ttype != TYPE_void) & b->tkey & !b->tunique) && b->batCount > 1) {
		BATkey(b, FALSE);
	}
	if (b->tnonil)
		b->tnonil = t && atom_CMP(t, ATOMnilptr(b->ttype), b->ttype) != 0;
	b->theap.dirty = TRUE;
	if (b->tvheap)
		b->tvheap->dirty = TRUE;

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

	if (b->tunique && BUNfnd(b, t) != BUN_NONE) {
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

	return BUNinplace(b, id - b->hseqbase, t, force);
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
	if (b->tunique && BUNfnd(b, val) != BUN_NONE)
		return GDK_SUCCEED;
	if (b->ttype == TYPE_void)
		return GDK_SUCCEED;
	return BUNinplace(b, id - b->hseqbase, val, force);
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
	/* head column is always VOID, and some head properties never change */
	assert(b->hseqbase != oid_nil);

	b->batCount = cnt;
	b->batDirtydesc = TRUE;
	b->theap.free = tailsize(b, cnt);
	if (b->ttype == TYPE_void)
		b->batCapacity = cnt;
	if (cnt <= 1) {
		b->tsorted = b->trevsorted = ATOMlinear(b->ttype) != 0;
		b->tnosorted = b->tnorevsorted = 0;
	}
	/* if the BAT was made smaller, we need to zap some values */
	if (b->tnosorted >= BUNlast(b))
		b->tnosorted = 0;
	if (b->tnorevsorted >= BUNlast(b))
		b->tnorevsorted = 0;
	if (b->tnodense >= BUNlast(b))
		b->tnodense = 0;
	if (b->tnokey[0] >= BUNlast(b) || b->tnokey[1] >= BUNlast(b)) {
		b->tnokey[0] = 0;
		b->tnokey[1] = 0;
	}
	if (b->ttype == TYPE_void) {
		b->tsorted = 1;
		if (b->tseqbase == oid_nil) {
			b->tkey = cnt <= 1;
			b->trevsorted = 1;
			b->tnil = 1;
			b->tnonil = 0;
		} else {
			b->tkey = 1;
			b->trevsorted = cnt <= 1;
			b->tnil = 0;
			b->tnonil = 1;
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
	return (!dirty || b->theap.dirty ? HEAPvmsize(&b->theap) : 0) +
		((!dirty || b->theap.dirty) && b->thash && b->thash != (Hash *) 1 ? HEAPvmsize(b->thash->heap) : 0) +
		(b->tvheap && (!dirty || b->tvheap->dirty) ? HEAPvmsize(b->tvheap) : 0);
}

size_t
BATmemsize(BAT *b, int dirty)
{
	BATcheck(b, "BATmemsize", 0);
	if (b->batDirty ||
	    (b->batPersistence != TRANSIENT && !b->batCopiedtodisk))
		dirty = 0;
	return (!dirty || b->batDirtydesc ? sizeof(BAT) : 0) +
		(!dirty || b->theap.dirty ? HEAPmemsize(&b->theap) : 0) +
		((!dirty || b->theap.dirty) && b->thash && b->thash != (Hash *) 1 ? HEAPmemsize(b->thash->heap) : 0) +
		(b->tvheap && (!dirty || b->tvheap->dirty) ? HEAPmemsize(b->tvheap) : 0);
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
	BATcheck(b, "BATkey", GDK_FAIL);
	assert(b->batCacheid > 0);
	assert(flag == 0 || flag == 1);
	assert(!b->tunique || flag);
	if (b->ttype == TYPE_void) {
		if (b->tseqbase != oid_nil && flag == FALSE) {
			GDKerror("BATkey: dense column must be unique.\n");
			return GDK_FAIL;
		}
		if (b->tseqbase == oid_nil && flag == TRUE && b->batCount > 1) {
			GDKerror("BATkey: void column cannot be unique.\n");
			return GDK_FAIL;
		}
	}
	if (b->tkey != (flag != 0))
		b->batDirtydesc = TRUE;
	b->tkey = flag != 0;
	if (!flag)
		b->tdense = 0;
	else
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
			return BATkey(bp, TRUE);
	}
	return GDK_SUCCEED;
}

void
BAThseqbase(BAT *b, oid o)
{
	if (b == NULL)
		return;
	assert(o < oid_nil);	/* i.e., not oid_nil */
	assert(o + BATcount(b) < oid_nil);
	assert(b->batCacheid > 0);
	if (b->hseqbase != o) {
		b->batDirtydesc = TRUE;
		b->hseqbase = o;
	}
}

void
BATtseqbase(BAT *b, oid o)
{
	if (b == NULL)
		return;
	assert(o <= oid_nil);
	assert(o == oid_nil || o + BATcount(b) < oid_nil);
	assert(b->batCacheid > 0);
	if (ATOMtype(b->ttype) == TYPE_oid) {
		if (b->tseqbase != o) {
			b->batDirtydesc = TRUE;
		}
		b->tseqbase = o;
		if (b->ttype == TYPE_oid && o == oid_nil) {
			b->tdense = 0;
			b->tnodense = 0;
		}

		/* adapt keyness */
		if (BATtvoid(b)) {
			if (o == oid_nil) {
				b->tkey = b->batCount <= 1;
				b->tnonil = b->batCount == 0;
				b->tnil = b->batCount > 0;
				b->tsorted = b->trevsorted = 1;
				b->tnosorted = b->tnorevsorted = 0;
				if (!b->tkey) {
					b->tnokey[0] = 0;
					b->tnokey[1] = 1;
				} else {
					b->tnokey[0] = b->tnokey[1] = 0;
				}
			} else {
				if (!b->tkey) {
					b->tkey = TRUE;
					b->tnokey[0] = b->tnokey[1] = 0;
				}
				b->tnonil = 1;
				b->tnil = 0;
				b->tsorted = 1;
				b->trevsorted = b->batCount <= 1;
				if (!b->trevsorted)
					b->tnorevsorted = 1;
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
BATroles(BAT *b, const char *tnme)
{
	if (b == NULL)
		return;
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
	storage_t m1 = STORE_MEM, m3 = STORE_MEM;
	int dirty = 0;

	BATcheck(b, "BATcheckmodes", GDK_FAIL);

	if (b->ttype) {
		m1 = HEAPcommitpersistence(&b->theap, wr, existing);
		dirty |= (b->theap.newstorage != m1);
	}

	if (b->tvheap) {
		int ta = (b->batRestricted == BAT_APPEND) && ATOMappendpriv(b->ttype, b->tvheap);
		m3 = HEAPcommitpersistence(b->tvheap, wr || ta, existing);
		dirty |= (b->tvheap->newstorage != m3);
	}
	if (m1 == STORE_INVALID || m3 == STORE_INVALID)
		return GDK_FAIL;

	if (dirty) {
		b->batDirtydesc = 1;
		b->theap.newstorage = m1;
		if (b->tvheap)
			b->tvheap->newstorage = m3;
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
		storage_t m1, m3 = STORE_MEM;
		storage_t b1, b3 = STORE_MEM;

		if (b->batSharecnt && newmode != BAT_READ) {
			BATDEBUG THRprintf(GDKout, "#BATsetaccess: %s has %d views; try creating a copy\n", BATgetId(b), b->batSharecnt);
			GDKerror("BATsetaccess: %s has %d views\n",
				 BATgetId(b), b->batSharecnt);
			return GDK_FAIL;
		}

		b1 = b->theap.newstorage;
		m1 = HEAPchangeaccess(&b->theap, ACCESSMODE(wr, rd), existing);
		if (b->tvheap) {
			int ta = (newmode == BAT_APPEND && ATOMappendpriv(b->ttype, b->tvheap));
			b3 = b->tvheap->newstorage;
			m3 = HEAPchangeaccess(b->tvheap, ACCESSMODE(wr && ta, rd && ta), existing);
		}
		if (m1 == STORE_INVALID || m3 == STORE_INVALID)
			return GDK_FAIL;

		/* set new access mode and mmap modes */
		b->batRestricted = newmode;
		b->batDirtydesc = TRUE;
		b->theap.newstorage = m1;
		if (b->tvheap)
			b->tvheap->newstorage = m3;

		if (existing && BBPsave(b) != GDK_SUCCEED) {
			/* roll back all changes */
			b->batRestricted = bakmode;
			b->batDirtydesc = bakdirty;
			b->theap.newstorage = b1;
			if (b->tvheap)
				b->tvheap->newstorage = b3;
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
			GDKerror("BATmode: %s type implies that %s[%s] " \
				 "cannot be made persistent.\n",	\
				 ATOMname(tp), BATgetId(b),		\
				 ATOMname(b->ttype));			\
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
		bat bid = b->batCacheid;

		if (mode == PERSISTENT) {
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
 * nosorted	BUN position which proofs not sorted (given position
 *		and one before are not ordered correctly).
 * norevsorted	BUN position which proofs not revsorted (given position
 *		and one before are not ordered correctly).
 * nokey	Pair of BUN positions that proof not all values are
 *		distinct (i.e. values at given locations are equal).
 *
 * In addition there is a property "unique" that, when set, indicates
 * that values must be kept unique (and hence that the "key" property
 * must be set).  This property is only used when changing (adding,
 * replacing) values.
 *
 * Note that the functions BATtseqbase and BATkey also set more
 * properties than you might suspect.  When setting properties on a
 * newly created and filled BAT, you may want to first make sure the
 * batCount is set correctly (e.g. by calling BATsetcount), then use
 * BAThseqbase and BATkey, and finally set the other properties.
 */

void
BATassertProps(BAT *b)
{
	int bbpstatus;
	BATiter bi = bat_iterator(b);
	BUN p, q;
	int (*cmpf)(const void *, const void *);
	int cmp;
	const void *prev = NULL, *valp, *nilp;
	int seennil = 0;

	/* general BAT sanity */
	assert(b != NULL);
	assert(b->batCacheid > 0);
	assert(b->batCount >= b->batInserted);

	/* headless */
	assert(b->hseqbase < oid_nil); /* non-nil seqbase */
	assert(b->hseqbase + BATcount(b) < oid_nil);

	bbpstatus = BBP_status(b->batCacheid);
	/* only at most one of BBPDELETED, BBPEXISTING, BBPNEW may be set */
	assert(((bbpstatus & BBPDELETED) != 0) +
	       ((bbpstatus & BBPEXISTING) != 0) +
	       ((bbpstatus & BBPNEW) != 0) <= 1);

	assert(b != NULL);
	assert(b->ttype >= TYPE_void);
	assert(b->ttype < GDKatomcnt);
	assert(b->ttype != TYPE_bat);
	assert(!b->tunique || b->tkey); /* if unique, then key */
	assert(isVIEW(b) ||
	       b->ttype == TYPE_void ||
	       BBPfarms[b->theap.farmid].roles & (1 << b->batRole));
	assert(isVIEW(b) ||
	       b->tvheap == NULL ||
	       (BBPfarms[b->tvheap->farmid].roles & (1 << b->batRole)));

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
	assert(b->tshift >= 0);
	if (b->tdense)
		assert(b->ttype == TYPE_oid || b->ttype == TYPE_void);
	/* a column cannot both have and not have NILs */
	assert(!b->tnil || !b->tnonil);
	assert(b->tseqbase <= oid_nil);
	if (b->ttype == TYPE_void) {
		assert(b->tshift == 0);
		assert(b->twidth == 0);
		if (b->tseqbase == oid_nil) {
			assert(BATcount(b) == 0 || !b->tnonil);
			assert(BATcount(b) <= 1 || !b->tkey);
			/* assert(!b->tdense); */
			assert(b->tsorted);
			assert(b->trevsorted);
		} else {
			assert(BATcount(b) == 0 || !b->tnil);
			assert(BATcount(b) <= 1 || !b->trevsorted);
			/* assert(b->tdense); */
			assert(b->tkey);
			assert(b->tsorted);
		}
		return;
	}
	if (ATOMstorage(b->ttype) == TYPE_str)
		assert(b->twidth >= 1 && b->twidth <= ATOMsize(b->ttype));
	else
		assert(b->twidth == ATOMsize(b->ttype));
	assert(1 << b->tshift == b->twidth);
	if (b->ttype == TYPE_oid && b->tdense) {
		assert(b->tsorted);
		assert(b->tseqbase != oid_nil);
		if (b->batCount > 0) {
			assert(b->tseqbase != oid_nil);
			assert(* (oid *) BUNtail(bi, 0) == b->tseqbase);
		}
	}
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
		if (b->tsorted || b->trevsorted || !b->tkey) {
			/* if sorted (either way), or we don't have to
			 * prove uniqueness, we can do a simple
			 * scan */
			/* only call compare function if we have to */
			int cmpprv = b->tsorted | b->trevsorted | b->tkey;
			int cmpnil = b->tnonil | b->tnil;

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
					assert(!b->tnonil || cmp != 0);
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
				assert(!b->tnonil || cmp != 0);
				if (cmp == 0)
					seennil = 1;
			}
			HEAPfree(hp, 1);
			GDKfree(hp);
			GDKfree(hs);
			GDKfree(ext);
		}
	  abort_check:
		assert(!b->tnil || seennil);
	}
}
