/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
 * Interface Declarations
 */
/*
 * - Hash Table Creation
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

static int
HASHwidth(BUN hashsize)
{
	if (hashsize <= (BUN) BUN2_NONE)
		return BUN2;
#if SIZEOF_BUN <= 4
	return BUN4;
#else
	if (hashsize <= (BUN) BUN4_NONE)
		return BUN4;
	return BUN8;
#endif
}

BUN
HASHmask(BUN cnt)
{
	BUN m = cnt;

	/* find largest power of 2 smaller than or equal to cnt */
	m |= m >> 1;
	m |= m >> 2;
	m |= m >> 4;
	m |= m >> 8;
	m |= m >> 16;
#if SIZEOF_BUN == 8
	m |= m >> 32;
#endif
	m -= m >> 1;

	/* if cnt is more than 1/3 into the gap between m and 2*m,
	   double m */
	if (m + m - cnt < 2 * (cnt - m))
		m += m;
	if (m < BATTINY)
		m = BATTINY;
	return m;
}

static void
HASHclear(Hash *h)
{
	/* since BUN2_NONE, BUN4_NONE, BUN8_NONE
	 * are all equal to -1 (~0), i.e., have all bits set,
	 * we can use a simple memset() to clear the Hash,
	 * rather than iteratively assigning individual
	 * BUNi_NONE values in a for-loop
	 */
	memset(h->Hash, 0xFF, (h->mask + 1) * h->width);
}

#define HASH_VERSION		1
#define HASH_HEADER_SIZE	5 /* nr of size_t fields in header */

Hash *
HASHnew(Heap *hp, int tpe, BUN size, BUN mask, BUN count)
{
	Hash *h = NULL;
	int width = HASHwidth(size);

	if (HEAPalloc(hp, mask + size + HASH_HEADER_SIZE * SIZEOF_SIZE_T / width, width) != GDK_SUCCEED)
		return NULL;
	hp->free = (mask + size) * width + HASH_HEADER_SIZE * SIZEOF_SIZE_T;
	h = GDKmalloc(sizeof(Hash));
	if (h == NULL)
		return NULL;
	h->lim = size;
	h->mask = mask - 1;
	h->width = width;
	switch (width) {
	case BUN2:
		h->nil = (BUN) BUN2_NONE;
		break;
	case BUN4:
		h->nil = (BUN) BUN4_NONE;
		break;
#ifdef BUN8
	case BUN8:
		h->nil = (BUN) BUN8_NONE;
		break;
#endif
	default:
		assert(0);
	}
	h->Link = hp->base + HASH_HEADER_SIZE * SIZEOF_SIZE_T;
	h->Hash = (void *) ((char *) h->Link + h->lim * width);
	h->type = tpe;
	h->heap = hp;
	HASHclear(h);		/* zero the mask */
	((size_t *) hp->base)[0] = HASH_VERSION;
	((size_t *) hp->base)[1] = size;
	((size_t *) hp->base)[2] = mask;
	((size_t *) hp->base)[3] = width;
	((size_t *) hp->base)[4] = count;
	ALGODEBUG fprintf(stderr, "#HASHnew: create hash(size " BUNFMT ", mask " BUNFMT ",width %d, nil " BUNFMT ", total " BUNFMT " bytes);\n", size, mask, width, h->nil, (size + mask) * width);
	return h;
}

#define starthash(TYPE)							\
	do {								\
		TYPE *v = (TYPE *) BUNtloc(bi, 0);			\
		for (; r < p; r++) {					\
			BUN c = (BUN) hash_##TYPE(h, v+r);		\
									\
			if (HASHget(h, c) == HASHnil(h) && nslots-- == 0) \
				break; /* mask too full */		\
			HASHputlink(h, r, HASHget(h, c));		\
			HASHput(h, c, r);				\
		}							\
	} while (0)
#define finishhash(TYPE)					\
	do {							\
		TYPE *v = (TYPE *) BUNtloc(bi, 0);		\
		for (; p < q; p++) {				\
			BUN c = (BUN) hash_##TYPE(h, v + p);	\
								\
			HASHputlink(h, p, HASHget(h, c));	\
			HASHput(h, c, p);			\
		}						\
	} while (0)

/* collect HASH statistics for analysis */
static void
HASHcollisions(BAT *b, Hash *h)
{
	lng cnt, entries = 0, max = 0;
	double total = 0;
	BUN p, i, j, nil;

	if (b == 0 || h == 0)
		return;
	nil = HASHnil(h);
	for (i = 0, j = h->mask; i <= j; i++)
		if ((p = HASHget(h, i)) != nil) {
			entries++;
			cnt = 0;
			for (; p != nil; p = HASHgetlink(h, p))
				cnt++;
			if (cnt > max)
				max = cnt;
			total += cnt;
		}
	fprintf(stderr, "#BAThash: statistics (" BUNFMT ", entries " LLFMT ", mask " BUNFMT ", max " LLFMT ", avg %2.6f);\n", BATcount(b), entries, h->mask, max, entries == 0 ? 0 : total / entries);
}

/* Return TRUE if we have a hash on the tail, even if we need to read
 * one from disk.
 *
 * Note that the b->T->hash pointer can be NULL, meaning there is no
 * hash; (Hash *) 1, meaning there is no hash loaded, but it may exist
 * on disk; or a valid pointer to a loaded hash.  These values are
 * maintained here, in the HASHdestroy/HASHremove and HASHfree
 * functions, and in BBPdiskscan during initialization. */
int
BATcheckhash(BAT *b)
{
	int ret;
	lng t;

	t = GDKusec();
	MT_lock_set(&GDKhashLock(abs(b->batCacheid)));
	t = GDKusec() - t;
	if (b->T->hash == (Hash *) 1) {
		Hash *h;
		Heap *hp;
		const char *nme = BBP_physical(b->batCacheid);
		const char *ext = b->batCacheid > 0 ? "thash" : "hhash";
		int fd;

		b->T->hash = NULL;
		if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&
		    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, hashheap)) >= 0 &&
		    (hp->filename = GDKmalloc(strlen(nme) + 12)) != NULL) {
			sprintf(hp->filename, "%s.%s", nme, ext);

			/* check whether a persisted hash can be found */
			if ((fd = GDKfdlocate(hp->farmid, nme, "rb+", ext)) >= 0) {
				size_t hdata[HASH_HEADER_SIZE];
				struct stat st;

				if ((h = GDKmalloc(sizeof(*h))) != NULL &&
				    read(fd, hdata, sizeof(hdata)) == sizeof(hdata) &&
				    hdata[0] == (
#ifdef PERSISTENTHASH
					    ((size_t) 1 << 24) |
#endif
					    HASH_VERSION) &&
				    hdata[4] == (size_t) BATcount(b) &&
				    fstat(fd, &st) == 0 &&
				    st.st_size >= (off_t) (hp->size = hp->free = (hdata[1] + hdata[2]) * hdata[3] + HASH_HEADER_SIZE * SIZEOF_SIZE_T) &&
				    HEAPload(hp, nme, ext, 0) == GDK_SUCCEED) {
					h->lim = (BUN) hdata[1];
					h->type = ATOMtype(b->ttype);
					h->mask = (BUN) (hdata[2] - 1);
					h->heap = hp;
					h->width = (int) hdata[3];
					switch (h->width) {
					case BUN2:
						h->nil = (BUN) BUN2_NONE;
						break;
					case BUN4:
						h->nil = (BUN) BUN4_NONE;
						break;
#ifdef BUN8
					case BUN8:
						h->nil = (BUN) BUN8_NONE;
						break;
#endif
					default:
						assert(0);
					}
					h->Link = hp->base + HASH_HEADER_SIZE * SIZEOF_SIZE_T;
					h->Hash = (void *) ((char *) h->Link + h->lim * h->width);
					close(fd);
					b->T->hash = h;
					ALGODEBUG fprintf(stderr, "#BATcheckhash: reusing persisted hash %s\n", BATgetId(b));
					MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
					return 1;
				}
				GDKfree(h);
				close(fd);
				/* unlink unusable file */
				GDKunlink(hp->farmid, BATDIR, nme, ext);
			}
			GDKfree(hp->filename);
		}
		GDKfree(hp);
		GDKclrerr();	/* we're not currently interested in errors */
	}
	ret = b->T->hash != NULL;
	MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
	ALGODEBUG if (ret) fprintf(stderr, "#BATcheckhash: already has hash %s, waited " LLFMT " usec\n", BATgetId(b), t);
	return ret;
}

/*
 * The prime routine for the BAT layer is to create a new hash index.
 * Its argument is the element type and the maximum number of BUNs be
 * stored under the hash function.
 */
gdk_return
BAThash(BAT *b, BUN masksize)
{
	lng t0 = 0, t1 = 0;

	if (BATcheckhash(b)) {
		return GDK_SUCCEED;
	}
	MT_lock_set(&GDKhashLock(abs(b->batCacheid)));
	if (b->T->hash == NULL) {
		unsigned int tpe = ATOMbasetype(b->ttype);
		BUN cnt = BATcount(b);
		BUN mask, maxmask = 0;
		BUN p = BUNfirst(b), q = BUNlast(b), r;
		Hash *h = NULL;
		Heap *hp;
		const char *nme = BBP_physical(b->batCacheid);
		const char *ext = b->batCacheid > 0 ? "thash" : "hhash";
		BATiter bi = bat_iterator(b);
#ifdef PERSISTENTHASH
		int fd;
#endif

		ALGODEBUG fprintf(stderr, "#BAThash: create hash(%s#" BUNFMT ");\n", BATgetId(b), BATcount(b));
		if ((hp = GDKzalloc(sizeof(*hp))) == NULL ||
		    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, hashheap)) < 0 ||
		    (hp->filename = GDKmalloc(strlen(nme) + 12)) == NULL) {
			MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
			GDKfree(hp);
			return GDK_FAIL;
		}
		sprintf(hp->filename, "%s.%s", nme, ext);

		/* cnt = 0, hopefully there is a proper capacity from
		 * which we can derive enough information */
		if (!cnt)
			cnt = BATcapacity(b);

		if (b->ttype == TYPE_void) {
			if (b->tseqbase == oid_nil) {
				MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
				ALGODEBUG fprintf(stderr, "#BAThash: cannot create hash-table on void-NIL column.\n");
				GDKfree(hp->filename);
				GDKfree(hp);
				GDKerror("BAThash: no hash on void/nil column\n");
				return GDK_FAIL;
			}
			ALGODEBUG fprintf(stderr, "#BAThash: creating hash-table on void column..\n");

			tpe = TYPE_void;
		}
		/* determine hash mask size p = first; then no dynamic
		 * scheme */
		if (masksize > 0) {
			mask = HASHmask(masksize);
		} else if (ATOMsize(tpe) == 1) {
			mask = (1 << 8);
		} else if (ATOMsize(tpe) == 2) {
			mask = (1 << 16);
		} else if (b->tkey) {
			mask = HASHmask(cnt);
		} else {
			/* dynamic hash: we start with
			 * HASHmask(cnt)/64; if there are too many
			 * collisions we try HASHmask(cnt)/16, then
			 * HASHmask(cnt)/4, and finally
			 * HASHmask(cnt).  */
			maxmask = HASHmask(cnt);
			mask = maxmask >> 6;
			p += (cnt >> 2);	/* try out on first 25% of b */
			if (p > q)
				p = q;
		}

		t0 = GDKusec();

		do {
			BUN nslots = mask >> 3;	/* 1/8 full is too full */

			r = BUNfirst(b);
			if (h) {
				char *fnme;
				bte farmid;

				ALGODEBUG fprintf(stderr, "#BAThash: retry hash construction\n");
				fnme = GDKstrdup(hp->filename);
				farmid = hp->farmid;
				HEAPfree(hp, 1);
				memset(hp, 0, sizeof(*hp));
				hp->filename = fnme;
				hp->farmid = farmid;
				GDKfree(h);
				h = NULL;
			}
			/* create the hash structures */
			if ((h = HASHnew(hp, ATOMtype(b->ttype), BATcapacity(b), mask, BATcount(b))) == NULL) {

				MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
				GDKfree(hp->filename);
				GDKfree(hp);
				return GDK_FAIL;
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
#if SIZEOF_OID == SIZEOF_INT
			case TYPE_oid:
#endif
#if SIZEOF_WRD == SIZEOF_INT
			case TYPE_wrd:
#endif
				starthash(int);
				break;
			case TYPE_dbl:
			case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			case TYPE_oid:
#endif
#if SIZEOF_WRD == SIZEOF_LNG
			case TYPE_wrd:
#endif
				starthash(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				starthash(hge);
				break;
#endif
			default:
				for (; r < p; r++) {
					ptr v = BUNtail(bi, r);
					BUN c = (BUN) heap_hash_any(b->T->vheap, h, v);

					if (HASHget(h, c) == HASHnil(h) &&
					    nslots-- == 0)
						break;	/* mask too full */
					HASHputlink(h, r, HASHget(h, c));
					HASHput(h, c, r);
				}
				break;
			}
		} while (r < p && mask < maxmask && (mask <<= 2));

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
#if SIZEOF_OID == SIZEOF_INT
		case TYPE_oid:
#endif
#if SIZEOF_WRD == SIZEOF_INT
		case TYPE_wrd:
#endif
			finishhash(int);
			break;
		case TYPE_dbl:
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
		case TYPE_oid:
#endif
#if SIZEOF_WRD == SIZEOF_LNG
		case TYPE_wrd:
#endif
			finishhash(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			finishhash(hge);
			break;
#endif
		default:
			for (; p < q; p++) {
				ptr v = BUNtail(bi, p);
				BUN c = (BUN) heap_hash_any(b->T->vheap, h, v);

				HASHputlink(h, p, HASHget(h, c));
				HASHput(h, c, p);
			}
			break;
		}
#ifdef PERSISTENTHASH
		if ((BBP_status(b->batCacheid) & BBPEXISTING) &&
		    b->batInserted == b->batCount &&
		    HEAPsave(hp, nme, ext) == GDK_SUCCEED &&
		    (fd = GDKfdlocate(hp->farmid, nme, "rb+", ext)) >= 0) {
			ALGODEBUG fprintf(stderr, "#BAThash: persisting hash %d\n", b->batCacheid);
			((size_t *) hp->base)[0] |= 1 << 24;
			if (write(fd, hp->base, SIZEOF_SIZE_T) < 0)
				perror("write hash");
			if (!(GDKdebug & FORCEMITOMASK)) {
#if defined(NATIVE_WIN32)
				_commit(fd);
#elif defined(HAVE_FDATASYNC)
				fdatasync(fd);
#elif defined(HAVE_FSYNC)
				fsync(fd);
#endif
			}
			close(fd);
		} else
			ALGODEBUG fprintf(stderr, "#BAThash: NOT persisting hash %d\n", b->batCacheid);
#endif
		b->T->hash = h;
		t1 = GDKusec();
		ALGODEBUG fprintf(stderr, "#BAThash: hash construction " LLFMT " usec\n", t1 - t0);
		ALGODEBUG HASHcollisions(b, b->T->hash);
	}
	MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
	return GDK_SUCCEED;
}

/*
 * The entry on which a value hashes can be calculated with the
 * routine HASHprobe.
 */
BUN
HASHprobe(Hash *h, const void *v)
{
	switch (ATOMbasetype(h->type)) {
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
#ifdef HAVE_HGE
	case TYPE_hge:
		return hash_hge(h, v);
#endif
	default:
		return hash_any(h, v);
	}
}

BUN
HASHlist(Hash *h, BUN i)
{
	BUN c = 1;
	BUN j = HASHget(h, i), nil = HASHnil(h);

	if (j == nil)
		return 1;
	while ((j = HASHgetlink(h, i)) != nil) {
		c++;
		i = j;
	}
	return c;
}

void
HASHremove(BAT *b)
{
	if (b) {
		if (b->T->hash == (Hash *) 1) {
			GDKunlink(BBPselectfarm(b->batRole, b->ttype, hashheap),
				  BATDIR,
				  BBP_physical(b->batCacheid),
				  b->batCacheid > 0 ? "thash" : "hhash");
		} else if (b->T->hash) {
			bat p = -VIEWtparent(b);
			BAT *hp = NULL;

			if (p)
				hp = BBP_cache(p);

			if ((!hp || b->T->hash != hp->T->hash) && b->T->hash != (Hash *) -1) {
				ALGODEBUG if (*(size_t *) b->T->hash->heap->base & (1 << 24))
					fprintf(stderr, "#HASHremove: removing persisted hash %d\n", b->batCacheid);
				HEAPfree(b->T->hash->heap, 1);
				GDKfree(b->T->hash->heap);
				GDKfree(b->T->hash);
			}
		}
		b->T->hash = NULL;
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

void
HASHfree(BAT *b)
{
	if (b) {
		MT_lock_set(&GDKhashLock(abs(b->batCacheid)));
		if (b->T->hash && b->T->hash != (Hash *) -1) {
			if (b->T->hash != (Hash *) 1) {
				HEAPfree(b->T->hash->heap, 0);
				GDKfree(b->T->hash->heap);
				GDKfree(b->T->hash);
				b->T->hash = (Hash *) 1;
			}
		} else {
			b->T->hash = NULL;
		}
		MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
	}
}

int
HASHgonebad(BAT *b, const void *v)
{
	Hash *h = b->T->hash;
	BATiter bi = bat_iterator(b);
	BUN cnt, hit;

	if (h == NULL)
		return 1;	/* no hash is bad hash? */

	if (h->mask * 2 < BATcount(b)) {
		int (*cmp) (const void *, const void *) = ATOMcompare(b->ttype);
		BUN i = HASHget(h, (BUN) HASHprobe(h, v)), nil = HASHnil(h);
		for (cnt = hit = 1; i != nil; i = HASHgetlink(h, i), cnt++)
			hit += ((*cmp) (v, BUNtail(bi, (BUN) i)) == 0);

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

enum find_which {
	FIND_FIRST,
	FIND_ANY,
	FIND_LAST
};

static BUN
SORTfndwhich(BAT *b, const void *v, enum find_which which)
{
	BUN lo, hi, mid;
	int cmp;
	BUN cur;
	BATiter bi;
	BUN diff, end;
	int tp;

	if (b == NULL || (!b->tsorted && !b->trevsorted))
		return BUN_NONE;

	lo = BUNfirst(b);
	hi = BUNlast(b);

	if (BATtdense(b)) {
		/* no need for binary search on dense column */
		if (*(const oid *) v < b->tseqbase ||
		    *(const oid *) v == oid_nil)
			return which == FIND_ANY ? BUN_NONE : lo;
		if (*(const oid *) v >= b->tseqbase + BATcount(b))
			return which == FIND_ANY ? BUN_NONE : hi;
		cur = (BUN) (*(const oid *) v - b->tseqbase) + lo;
		return cur + (which == FIND_LAST);
	}
	if (b->ttype == TYPE_void) {
		assert(b->tseqbase == oid_nil);
		switch (which) {
		case FIND_FIRST:
			if (*(const oid *) v == oid_nil)
				return lo;
		case FIND_LAST:
			return hi;
		default:
			if (lo < hi && *(const oid *) v == oid_nil)
				return lo;
			return BUN_NONE;
		}
	}
	cmp = 1;
	cur = BUN_NONE;
	bi = bat_iterator(b);
	/* only use storage type if comparison functions are equal */
	tp = ATOMbasetype(b->ttype);

	switch (which) {
	case FIND_FIRST:
		end = lo;
		if (lo >= hi || (b->tsorted ? atom_GE(BUNtail(bi, lo), v, b->ttype) : atom_LE(BUNtail(bi, lo), v, b->ttype))) {
			/* shortcut: if BAT is empty or first (and
			 * hence all) tail value is >= v (if sorted)
			 * or <= v (if revsorted), we're done */
			return lo;
		}
		break;
	case FIND_LAST:
		end = hi;
		if (lo >= hi || (b->tsorted ? atom_LE(BUNtail(bi, hi - 1), v, b->ttype) : atom_GE(BUNtail(bi, hi - 1), v, b->ttype))) {
			/* shortcut: if BAT is empty or first (and
			 * hence all) tail value is <= v (if sorted)
			 * or >= v (if revsorted), we're done */
			return hi;
		}
		break;
	default: /* case FIND_ANY -- stupid compiler */
		end = 0;	/* not used in this case */
		if (lo >= hi) {
			/* empty BAT: value not found */
			return BUN_NONE;
		}
		break;
	}

	if (b->tsorted) {
		switch (tp) {
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
#ifdef HAVE_HGE
		case TYPE_hge:
			SORTfndloop(hge, simple_CMP, BUNtloc);
			break;
#endif
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
		switch (tp) {
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
#ifdef HAVE_HGE
		case TYPE_hge:
			SORTfndloop(hge, -simple_CMP, BUNtloc);
			break;
#endif
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

	switch (which) {
	case FIND_FIRST:
		if (cmp == 0 && b->tkey == 0) {
			/* shift over multiple equals */
			for (diff = cur - end; diff; diff >>= 1) {
				while (cur >= end + diff &&
				       atom_EQ(BUNtail(bi, cur - diff), v, b->ttype))
					cur -= diff;
			}
		}
		break;
	case FIND_LAST:
		if (cmp == 0 && b->tkey == 0) {
			/* shift over multiple equals */
			for (diff = (end - cur) >> 1; diff; diff >>= 1) {
				while (cur + diff < end &&
				       atom_EQ(BUNtail(bi, cur + diff), v, b->ttype))
					cur += diff;
			}
		}
		cur += (cmp == 0);
		break;
	default: /* case FIND_ANY -- stupid compiler */
		if (cmp) {
			/* not found */
			cur = BUN_NONE;
		}
		break;
	}
	return cur;
}

/* Return the BUN of any tail value in b that is equal to v; if no
 * match is found, return BUN_NONE.  b must be sorted (reverse of
 * forward). */
BUN
SORTfnd(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_ANY);
}

/* Return the BUN of the first (lowest numbered) tail value that is
 * equal to v; if no match is found, return the BUN of the next higher
 * value in b.  b must be sorted (reverse of forward). */
BUN
SORTfndfirst(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_FIRST);
}

/* Return the BUN of the first (lowest numbered) tail value beyond v.
 * b must be sorted (reverse of forward). */
BUN
SORTfndlast(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_LAST);
}
