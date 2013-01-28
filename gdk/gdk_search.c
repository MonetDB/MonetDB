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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f gdk_search
 *
 */
/*
 * @a M. L. Kersten, P. Boncz, N. Nes
 *
 * @* Search Accelerators
 *
 * What sets BATs apart from normal arrays is their built-in ability
 * to search on both dimensions of the binary association.  The
 * easiest way to implement this is simply walk to the whole table and
 * compare against each element.  This method is of course highly
 * inefficient, much better performance can be obtained if the BATs
 * use some kind of index method to speed up searching.
 *
 * While index methods speed up searching they also have
 * disadvantages.  In the first place extra storage is needed for the
 * index. Second, insertion of data or removing old data requires
 * updating of the index structure, which takes extra time.
 *
 * This means there is a need for both indexed and non-indexed BAT,
 * the first to be used when little or no searching is needed, the
 * second to be used when searching is predominant. Also, there is no
 * best index method for all cases, different methods have different
 * storage needs and different performance. Thus, multiple index
 * methods are provided, each suited to particular types of usage.
 *
 * For query-dominant environments it pays to build a search
 * accelerator.  The main problems to be solved are:
 *
 * - avoidance of excessive storage requirements, and
 * - limited maintenance overhead.
 *
 * The idea that query intensive tasks need many different index
 * methods has been proven invalid. The current direction is multiple
 * copies of data, which can than be sorted or clustered.
 *
 * The BAT library automatically decides when an index becomes cost
 * effective.
 *
 * In situations where an index is expected, a call is made to
 * BAThash.  This operation check for indexing on the header.
 *
 * @+ Interface Declarations
 */
/*
 * @- Hash Table Creation
 * The hash indexing scheme for BATs reserves a block of memory to
 * maintain the hash table and a collision list. A one-to-one mapping
 * exists between the BAT and the collision list using the BUN
 * index. NOTE: we alloc the link list as a parallel array to the BUN
 * array; hence the hash link array has the same size as
 * BATcapacity(b) (not BATcount(b)). This allows us in the BUN insert
 * and delete to assume that there is hash space iff there is BUN
 * space. If there is no BUN space, the BATextend now destroys the
 * hash table.
 *
 * The hash mask size is a power of two, so we can do bitwise AND on
 * the hash (integer) number to quickly find the head of the bucket
 * chain.  Clearly, the hash mask size is a crucial parameter. If we
 * know that the column is unique (hkey), we use direct hashing (mask
 * size ~= BATcount). Otherwise we dynamically determine the mask size
 * by starting out with mask size = BATcount/64 (just 1.5% of memory
 * storage overhead). Then we start building the hash table on the
 * first 25% of the BAT. As we aim for max-collisions list length of
 * 4, the list on 25% should not exceed length 1. So, if a small
 * number of collisssions occurs (mask/2) then we abandon the attempt
 * and restart with a mask that is 4 times larger. This converges
 * after three cycles to direct hashing.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

BUN
HASHmask(BUN cnt)
{
	BUN m = 8;		/* minimum size */

	while (m + m < cnt)
		m += m;
	if (m + m - cnt < 2 * (cnt - m))
		m += m;
	return m;
}

static void
HASHclear(Hash *h)
{
	BUN *i, *j;

	for (i = h->hash, j = i + h->mask; i <= j; i++) {
		*i = BUN_NONE;
	}
}

Hash *
HASHnew(Heap *hp, int tpe, BUN size, BUN mask)
{
	Hash *h = NULL;
	if (HEAPalloc(hp, mask + size, sizeof(BUN)) < 0)
		return NULL;
	hp->free = (mask + size) * sizeof(BUN);
	h = (Hash *) GDKmalloc(sizeof(Hash));
	if (!h)
		return h;
	h->lim = size;
	h->mask = mask - 1;
	h->link = (BUN *) hp->base;
	h->hash = h->link + h->lim;
	h->type = tpe;
	h->heap = hp;
	HASHclear(h);		/* zero the mask */
	return h;
}

#define starthash(TYPE)							\
	do {								\
		TYPE *v = (TYPE*)BUNhloc(bi, 0);			\
		for (; r < p; r++) {					\
			BUN c = hash_##TYPE(h, v+r);			\
									\
			if (h->hash[c] == BUN_NONE && nslots-- == 0)	\
				break; /* mask too full */		\
			h->link[r] = h->hash[c];			\
			h->hash[c] = r;					\
		}							\
	} while (0)
#define finishhash(TYPE)				\
	do {						\
		TYPE *v = (TYPE*)BUNhloc(bi, 0);	\
		for (; p < q; p++) {			\
			BUN c = hash_##TYPE(h, v+p);	\
							\
			h->link[p] = h->hash[c];	\
			h->hash[c] = p;			\
		}					\
	} while (0)

/* collect HASH statistics for analysis */
static void HASHcollisions(BAT *b, Hash *h)
{
	BUN p, *i, *j;
	lng cnt, entries=0, max =0;
	double total=0;
	
	if ( b == 0 || h == 0 )
		return;
	for (i = h->hash, j = i + h->mask; i <= j; i++) 
	if ( *i != BUN_NONE){
		entries++;
		p = *i;
		cnt = 0;
		for ( ; p != BUN_NONE; p = h->link[p])
			cnt++;
		if ( cnt > max ) max = cnt;
		total += cnt;
	}

	fprintf(stderr, "BAThash: statistics (" BUNFMT ", entries " LLFMT", mask " BUNFMT", max " LLFMT ", avg %2.6f);\n", BATcount(b), entries, h->mask, max, total/entries);
}
/*
 * The prime routine for the BAT layer is to create a new hash index.
 * Its argument is the element type and the maximum number of BUNs be
 * stored under the hash function.
 */
BAT *
BAThash(BAT *b, BUN masksize)
{
	BAT *o = NULL;

	if (VIEWhparent(b)) {
		bat p = VIEWhparent(b);
		o = b;
		b = BATdescriptor(p);
		if (!ALIGNsynced(o, b) || BUNfirst(o) != BUNfirst(b)) {
			BBPunfix(b->batCacheid);
			b = o;
			o = NULL;
		}
	}
	MT_lock_set(&GDKhashLock(ABS(b->batCacheid)), "BAThash");
	if (b->H->hash == NULL) {
		unsigned int tpe = ATOMstorage(b->htype);
		BUN cnt = BATcount(b);
		BUN mask;
		BUN p = BUNfirst(b), q = BUNlast(b), r;
		Hash *h = NULL;
		Heap *hp = NULL;
		str nme = BBP_physical(b->batCacheid);
		BATiter bi = bat_iterator(b);

		ALGODEBUG fprintf(stderr, "#BAThash: create hash(" BUNFMT ");\n", BATcount(b));
		/* cnt = 0, hopefully there is a proper capacity from
		 * which we can derive enough information */
		if (!cnt)
			cnt = BATcapacity(b);

		if (b->htype == TYPE_void) {
			if (b->hseqbase == oid_nil) {
				MT_lock_unset(&GDKhashLock(ABS(b->batCacheid)), "BAThash");
				ALGODEBUG fprintf(stderr, "#BAThash: cannot create hash-table on void-NIL column.\n");
				return NULL;
			}
			ALGODEBUG fprintf(stderr, "#BAThash: creating hash-table on void column..\n");

			tpe = TYPE_void;
		}
		/* determine hash mask size p = first; then no dynamic
		 * scheme */
		if (masksize > 0) {
			mask = HASHmask(masksize);
		} else if (ATOMsize(ATOMstorage(tpe)) == 1) {
			mask = (1 << 8);
		} else if (ATOMsize(ATOMstorage(tpe)) == 2) {
			mask = (1 << 12);
		} else if (b->hkey) {
			mask = HASHmask(cnt);
		} else {
			/* dynamic hash: we start with
			 * HASHmask(cnt/64); if there are too many
			 * collisions we try HASHmask(cnt/16), then
			 * HASHmask(cnt/4), and finally
			 * HASHmask(cnt).  */
			mask = HASHmask(cnt >> 6);
			p += (cnt >> 2);	/* try out on first 25% of b */
			if (p > q)
				p = q;
		}

		if (mask < 1024)
			mask = 1024;
		do {
			BUN nslots = mask >> 3;	/* 1/8 full is too full */

			r = BUNfirst(b);
			if (hp) {
				HEAPfree(hp);
				GDKfree(hp);
			}
			if (h)
				GDKfree(h);
			/* create the hash structures */
			hp = (Heap *) GDKzalloc(sizeof(Heap));
			if (hp &&
			    (hp->filename = GDKmalloc(strlen(nme) + 12)) != NULL)
				sprintf(hp->filename, "%s.%chash", nme, b->batCacheid > 0 ? 'h' : 't');
			if (hp == NULL ||
			    hp->filename == NULL ||
			    (h = HASHnew(hp, ATOMtype(b->htype), BATcapacity(b), mask)) == NULL) {
				MT_lock_unset(&GDKhashLock(ABS(b->batCacheid)), "BAThash");
				if (hp != NULL) {
					GDKfree(hp->filename);
					GDKfree(hp);
				}
				return NULL;
			}

			switch (tpe) {
			case TYPE_bte:
				starthash(bte);
				break;
			case TYPE_sht:
				starthash(sht);
				break;
			case TYPE_int:
			case TYPE_flt:
				starthash(int);
				break;
			case TYPE_dbl:
			case TYPE_lng:
				starthash(lng);
				break;
			default:
				for (; r < p; r++) {
					ptr v = BUNhead(bi, r);
					BUN c = heap_hash_any(b->H->vheap, h, v);

					if (h->hash[c] == BUN_NONE &&
					    nslots-- == 0)
						break;	/* mask too full */
					h->link[r] = h->hash[c];
					h->hash[c] = r;
				}
				break;
			}
		} while (r < p && mask < cnt && (mask <<= 2));

		/* finish the hashtable with the current mask */
		p = r;
		switch (tpe) {
		case TYPE_bte:
			finishhash(bte);
			break;
		case TYPE_sht:
			finishhash(sht);
			break;
		case TYPE_int:
		case TYPE_flt:
			finishhash(int);
			break;
		case TYPE_dbl:
		case TYPE_lng:
			finishhash(lng);
			break;
		default:
			for (; p < q; p++) {
				ptr v = BUNhead(bi, p);
				BUN c = heap_hash_any(b->H->vheap, h, v);

				h->link[p] = h->hash[c];
				h->hash[c] = p;
			}
			break;
		}
		b->H->hash = h;
		ALGODEBUG HASHcollisions(b,b->H->hash);
	}
	MT_lock_unset(&GDKhashLock(ABS(b->batCacheid)), "BAThash");
	if (o != NULL) {
		o->H->hash = b->H->hash;
		BBPunfix(b->batCacheid);
		b = o;
	}
	return b;
}

/*
 * The entry on which a value hashes can be calculated with the
 * routine HASHprobe.
 */
BUN
HASHprobe(Hash *h, const void *v)
{
	switch (ATOMstorage(h->type)) {
	case TYPE_bte:
		return hash_bte(h, v);
	case TYPE_sht:
		return hash_sht(h, v);
	case TYPE_int:
	case TYPE_flt:
		return hash_int(h, v);
	case TYPE_dbl:
	case TYPE_lng:
		return hash_lng(h, v);
	default:
		return hash_any(h, v);
	}
}

BUN
HASHlist(Hash *h, BUN i)
{
	BUN j;
	BUN c = 1;

	while ((j = h->link[i]) != BUN_NONE) {
		c++;
		i = j;
		if (i > h->lim) {
			mnstr_printf(GDKstdout, "hash inconsistency link " BUNFMT "\n", i);
			break;
		}
	}
	return c;
}

void
HASHremove(BAT *b)
{
	if (b && b->H->hash) {
		bat p = VIEWhparent(b);
		BAT *hp = NULL;

		if (p)
			hp = BBP_cache(p);

		if ((!hp || b->H->hash != hp->H->hash) && b->H->hash != (Hash *) -1) {
			if (b->H->hash->heap->storage != STORE_MEM)
				HEAPdelete(b->H->hash->heap, BBP_physical(b->batCacheid), (b->batCacheid > 0) ? "hhash" : "thash");
			else
				HEAPfree(b->H->hash->heap);
			GDKfree(b->H->hash->heap);
			GDKfree(b->H->hash);
		}
		b->H->hash = NULL;
	}
}

void
HASHdestroy(BAT *b)
{
	if (b) {
		HASHremove(b);
		if (BATmirror(b))
			HASHremove(BATmirror(b));

	}
}

int
HASHgonebad(BAT *b, const void *v)
{
	Hash *h = b->H->hash;
	BATiter bi = bat_iterator(b);

	if (h == NULL)
		return 1;	/* no hash is bad hash? */

	if (h->mask * 2 < BATcount(b)) {
		int (*cmp) (const void *, const void *) = BATatoms[b->htype].atomCmp;
		BUN cnt, hit, i = h->hash[HASHprobe(h, v)];

		for (cnt = hit = 1; i != BUN_NONE; i = h->link[i], cnt++)
			hit += ((*cmp) (v, BUNhead(bi, i)) == 0);

		if (cnt / hit > 4)
			return 1;	/* linked list too long */

		/* in this case, linked lists are long but contain the
		 * desired values such hash tables may be useful for
		 * locating all duplicates */
	}
	return 0;		/* a-ok */
}


/*
 * By Peter sept-99. This is a simple implementation that avoids all
 * multiply and divs on most bats by using integer BUNindex numbers
 * rather than absolute pointers (the BUNptr employed to obtain a
 * pointer uses shift where possible).  Also, the gradient-based
 * approach has been dropped again, which allows all atoms to be
 * treated in one macro. Main motivation: distrust of gradient
 * performance on odmg data and its high mult/div overhead.
 */
#define SORTfndloop(TYPE, CMP, BUNtail)					\
	do {								\
		while (lo < hi) {					\
			cur = mid = (lo + hi) >> 1;			\
			cmp = CMP(BUNtail(bi, cur), v, TYPE);		\
			if (cmp < 0) {					\
				lo = ++mid;				\
				cur++;					\
			} else if (cmp > 0) {				\
				hi = mid;				\
			} else {					\
				break;					\
			}						\
		}							\
	} while (0)

static BUN
SORTfndwhich(BAT *b, const void *v, int which)
{
	BUN lo = BUNfirst(b), hi = BUNlast(b), mid;
	int cmp = 1;
	BUN cur = BUN_NONE;
	BATiter bi = bat_iterator(b);
	BUN diff, end;

	if (b == NULL || (!b->tsorted && !b->trevsorted))
		return BUN_NONE;

	if (BATtdense(b)) {
		/* no need for binary search on dense column */
		if (* (const oid *) v < b->tseqbase)
			return which == 0 ? BUN_NONE : lo;
		if (* (const oid *) v >= b->tseqbase + BATcount(b))
			return which == 0 ? BUN_NONE : hi;
		cur = (BUN) (* (const oid *) v - b->tseqbase) + lo;
		if (which > 0)
			cur++;
		return cur;
	}

	if (which < 0) {
		end = lo;
		if (lo >= hi || (b->tsorted ? atom_GE(BUNtail(bi, lo), v, b->ttype) : atom_LE(BUNtail(bi, lo), v, b->ttype))) {
			/* shortcut: if BAT is empty or first (and
			 * hence all) tail value is >= v (if sorted)
			 * or <= v (if revsorted), we're done */
			return lo;
		}
	} else if (which > 0) {
		end = hi;
		if (lo >= hi || (b->tsorted ? atom_LE(BUNtail(bi, hi - 1), v, b->ttype) : atom_GE(BUNtail(bi, hi - 1), v, b->ttype))) {
			/* shortcut: if BAT is empty or first (and
			 * hence all) tail value is <= v (if sorted)
			 * or >= v (if revsorted), we're done */
			return hi;
		}
	} else {
		end = 0;	/* not used in this case */
		if (lo >= hi) {
			/* empty BAT: value not found */
			return BUN_NONE;
		}
	}

	if (b->tsorted) {
		switch (ATOMstorage(b->ttype)) {
		case TYPE_bte:
			SORTfndloop(bte, simple_CMP, BUNtloc);
			break;
		case TYPE_sht:
			SORTfndloop(sht, simple_CMP, BUNtloc);
			break;
		case TYPE_int:
			SORTfndloop(int, simple_CMP, BUNtloc);
			break;
		case TYPE_lng:
			SORTfndloop(lng, simple_CMP, BUNtloc);
			break;
		case TYPE_flt:
			SORTfndloop(flt, simple_CMP, BUNtloc);
			break;
		case TYPE_dbl:
			SORTfndloop(dbl, simple_CMP, BUNtloc);
			break;
		default:
			if (b->tvarsized)
				SORTfndloop(b->ttype, atom_CMP, BUNtvar);
			else
				SORTfndloop(b->ttype, atom_CMP, BUNtloc);
			break;
		}
	} else {
		switch (ATOMstorage(b->ttype)) {
		case TYPE_bte:
			SORTfndloop(bte, -simple_CMP, BUNtloc);
			break;
		case TYPE_sht:
			SORTfndloop(sht, -simple_CMP, BUNtloc);
			break;
		case TYPE_int:
			SORTfndloop(int, -simple_CMP, BUNtloc);
			break;
		case TYPE_lng:
			SORTfndloop(lng, -simple_CMP, BUNtloc);
			break;
		case TYPE_flt:
			SORTfndloop(flt, -simple_CMP, BUNtloc);
			break;
		case TYPE_dbl:
			SORTfndloop(dbl, -simple_CMP, BUNtloc);
			break;
		default:
			if (b->tvarsized)
				SORTfndloop(b->ttype, -atom_CMP, BUNtvar);
			else
				SORTfndloop(b->ttype, -atom_CMP, BUNtloc);
			break;
		}
	}

	if (which < 0) {
		if (cmp == 0 && b->tkey == 0) {
			/* shift over multiple equals */
			for (diff = cur - end; diff; diff >>= 1) {
				while (cur >= end + diff &&
				       atom_EQ(BUNtail(bi, cur - diff), v, b->ttype))
					cur -= diff;
			}
		}
	} else if (which > 0) {
		if (cmp == 0 && b->tkey == 0) {
			/* shift over multiple equals */
			for (diff = (end - cur) >> 1; diff; diff >>= 1) {
				while (cur + diff < end &&
				       atom_EQ(BUNtail(bi, cur + diff), v, b->ttype))
					cur += diff;
			}
		}
		if (cmp == 0)
			cur++;
	} else {
		if (cmp) {
			/* not found */
			cur = BUN_NONE;
		}
	}
	return cur;
}

BUN
SORTfnd(BAT *b, const void *v)
{
	/* works on HEAD column! */
	return SORTfndwhich(BATmirror(b), v, 0);
}

BUN
SORTfndfirst(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, -1);
}

BUN
SORTfndlast(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, 1);
}
