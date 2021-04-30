/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
 * space.
 *
 * The hash mask size is a power of two, so we can do bitwise AND on
 * the hash (integer) number to quickly find the head of the bucket
 * chain.  Clearly, the hash mask size is a crucial parameter. If we
 * know that the column is unique (tkey), we use direct hashing (mask
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

static inline uint8_t __attribute__((__const__))
HASHwidth(BUN hashsize)
{
	(void) hashsize;
#ifdef BUN2
	if (hashsize <= (BUN) BUN2_NONE)
		return BUN2;
#endif
#ifdef BUN8
	if (hashsize > (BUN) BUN4_NONE)
		return BUN8;
#endif
	return BUN4;
}

static inline BUN __attribute__((__const__))
hashmask(BUN m)
{
	m |= m >> 1;
	m |= m >> 2;
	m |= m >> 4;
	m |= m >> 8;
	m |= m >> 16;
#if SIZEOF_BUN == 8
	m |= m >> 32;
#endif
	return m;
}

BUN
HASHmask(BUN cnt)
{
	BUN m = cnt;

#if 0
	/* find largest power of 2 smaller than or equal to cnt */
	m = hashmask(m);
	m -= m >> 1;

	/* if cnt is more than 1/3 into the gap between m and 2*m,
	   double m */
	if (m + m - cnt < 2 * (cnt - m))
		m += m;
#else
	m = m * 8 / 7;
#endif
	if (m < BATTINY)
		m = BATTINY;
	return m;
}

static inline void
HASHclear(Hash *h)
{
	/* since BUN2_NONE, BUN4_NONE, BUN8_NONE
	 * are all equal to ~0, i.e., have all bits set,
	 * we can use a simple memset() to clear the Hash,
	 * rather than iteratively assigning individual
	 * BUNi_NONE values in a for-loop
	 */
	memset(h->Bckt, 0xFF, h->nbucket * h->width);
}

#define HASH_VERSION		4
/* this is only for the change of hash function of the UUID type; if
 * HASH_VERSION is increased again from 4, the code associated with
 * HASH_VERSION_NOUUID must be deleted */
#define HASH_VERSION_NOUUID	3
#define HASH_HEADER_SIZE	7	/* nr of size_t fields in header */

static void
doHASHdestroy(BAT *b, Hash *hs)
{
	if (hs == (Hash *) 1) {
		GDKunlink(BBPselectfarm(b->batRole, b->ttype, hashheap),
			  BATDIR,
			  BBP_physical(b->batCacheid),
			  "thashl");
		GDKunlink(BBPselectfarm(b->batRole, b->ttype, hashheap),
			  BATDIR,
			  BBP_physical(b->batCacheid),
			  "thashb");
	} else if (hs) {
		bat p = VIEWtparent(b);
		BAT *hp = NULL;

		if (p)
			hp = BBP_cache(p);

		if (!hp || hs != hp->thash) {
			TRC_DEBUG(ACCELERATOR, ALGOBATFMT ": removing%s hash\n", ALGOBATPAR(b), *(size_t *) hs->heapbckt.base & (1 << 24) ? " persisted" : "");
			HEAPfree(&hs->heapbckt, true);
			HEAPfree(&hs->heaplink, true);
			GDKfree(hs);
		}
	}
}

gdk_return
HASHnew(Hash *h, int tpe, BUN size, BUN mask, BUN count, bool bcktonly)
{
	if (h->width == 0)
		h->width = HASHwidth(size);

	if (!bcktonly) {
		if (HEAPalloc(&h->heaplink, size, h->width, 0) != GDK_SUCCEED)
			return GDK_FAIL;
		h->heaplink.free = size * h->width;
		h->Link = h->heaplink.base;
	}
	if (HEAPalloc(&h->heapbckt, mask + HASH_HEADER_SIZE * SIZEOF_SIZE_T / h->width, h->width, 0) != GDK_SUCCEED)
		return GDK_FAIL;
	h->heapbckt.free = mask * h->width + HASH_HEADER_SIZE * SIZEOF_SIZE_T;
	h->nbucket = mask;
	if (mask & (mask - 1)) {
		h->mask2 = hashmask(mask);
		h->mask1 = h->mask2 >> 1;
	} else {
		/* mask is a power of two */
		h->mask1 = mask - 1;
		h->mask2 = h->mask1 << 1 | 1;
	}
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		h->nil = (BUN) BUN2_NONE;
		break;
#endif
	default:		/* BUN4 */
		h->nil = (BUN) BUN4_NONE;
		break;
#ifdef BUN8
	case BUN8:
		h->nil = (BUN) BUN8_NONE;
		break;
#endif
	}
	h->Bckt = h->heapbckt.base + HASH_HEADER_SIZE * SIZEOF_SIZE_T;
	h->type = tpe;
	HASHclear(h);		/* zero the mask */
	((size_t *) h->heapbckt.base)[0] = (size_t) HASH_VERSION;
	((size_t *) h->heapbckt.base)[1] = (size_t) size;
	((size_t *) h->heapbckt.base)[2] = (size_t) h->nbucket;
	((size_t *) h->heapbckt.base)[3] = (size_t) h->width;
	((size_t *) h->heapbckt.base)[4] = (size_t) count;
	((size_t *) h->heapbckt.base)[5] = (size_t) h->nunique;
	((size_t *) h->heapbckt.base)[6] = (size_t) h->nheads;
	TRC_DEBUG(ACCELERATOR,
		  "create hash(size " BUNFMT ", mask " BUNFMT ", width %d, total " BUNFMT " bytes);\n", size, mask, h->width, (size + mask) * h->width);
	return GDK_SUCCEED;
}

/* collect HASH statistics for analysis */
static void
HASHcollisions(BAT *b, Hash *h, const char *func)
{
	lng cnt, entries = 0, max = 0;
	double total = 0;
	BUN p, i, j, nil;

	if (b == 0 || h == 0)
		return;
	nil = HASHnil(h);
	for (i = 0, j = h->nbucket; i < j; i++)
		if ((p = HASHget(h, i)) != nil) {
			entries++;
			cnt = 0;
			for (; p != nil; p = HASHgetlink(h, p))
				cnt++;
			if (cnt > max)
				max = cnt;
			total += cnt;
		}
	TRC_DEBUG_ENDIF(ACCELERATOR,
			"%s(" ALGOBATFMT "): statistics " BUNFMT ", "
			"entries " LLFMT ", nunique " BUNFMT ", "
			"nbucket " BUNFMT ", max " LLFMT ", avg %2.6f;\n",
			func, ALGOBATPAR(b), BATcount(b), entries,
			h->nunique, h->nbucket, max,
			entries == 0 ? 0 : total / entries);
}

static gdk_return
HASHupgradehashheap(BAT *b)
{
#if defined(BUN2) || defined(BUN8)
	Hash *h = b->thash;
	int nwidth = h->width << 1;
	BUN i;

	assert(nwidth <= SIZEOF_BUN);
	assert((nwidth & (nwidth - 1)) == 0);

	if (HEAPextend(&h->heaplink, h->heaplink.size * nwidth / h->width, true) != GDK_SUCCEED ||
	    HEAPextend(&h->heapbckt, (h->heapbckt.size - HASH_HEADER_SIZE * SIZEOF_SIZE_T) * nwidth / h->width + HASH_HEADER_SIZE * SIZEOF_SIZE_T, true) != GDK_SUCCEED) {
		b->thash = NULL;
		doHASHdestroy(b, h);
		return GDK_FAIL;
	}
	h->Link = h->heaplink.base;
	h->Bckt = h->heapbckt.base + HASH_HEADER_SIZE * SIZEOF_SIZE_T;
	switch (nwidth) {
	case BUN4:
#ifdef BUN2
		switch (h->width) {
		case BUN2:
			i = h->heaplink.free / h->width;
			h->heaplink.free = i * nwidth;
			while (i > 0) {
				i--;
				BUN2type v = ((BUN2type *) h->Link)[i];
				((BUN4type *) h->Link)[i] = v == BUN2_NONE ? BUN4_NONE : v;
			}
			i = (h->heapbckt.free - HASH_HEADER_SIZE * SIZEOF_SIZE_T) / h->width;
			h->heapbckt.free = HASH_HEADER_SIZE * SIZEOF_SIZE_T + i * nwidth;
			while (i > 0) {
				i--;
				BUN2type v = ((BUN2type *) h->Bckt)[i];
				((BUN4type *) h->Bckt)[i] = v == BUN2_NONE ? BUN4_NONE : v;
			}
			break;
		}
#endif
		h->nil = BUN4_NONE;
		break;
#ifdef BUN8
	case BUN8:
		switch (h->width) {
#ifdef BUN2
		case BUN2:
			i = h->heaplink.free / h->width;
			h->heaplink.free = i * nwidth;
			while (i > 0) {
				i--;
				BUN2type v = ((BUN2type *) h->Link)[i];
				((BUN8type *) h->Link)[i] = v == BUN2_NONE ? BUN8_NONE : v;
			}
			i = (h->heapbckt.free - HASH_HEADER_SIZE * SIZEOF_SIZE_T) / h->width;
			h->heapbckt.free = HASH_HEADER_SIZE * SIZEOF_SIZE_T + i * nwidth;
			while (i > 0) {
				i--;
				BUN2type v = ((BUN2type *) h->Bckt)[i];
				((BUN8type *) h->Bckt)[i] = v == BUN2_NONE ? BUN8_NONE : v;
			}
			break;
#endif
		case BUN4:
			i = h->heaplink.free / h->width;
			h->heaplink.free = i * nwidth;
			while (i > 0) {
				i--;
				BUN4type v = ((BUN4type *) h->Link)[i];
				((BUN8type *) h->Link)[i] = v == BUN4_NONE ? BUN8_NONE : v;
			}
			i = (h->heapbckt.free - HASH_HEADER_SIZE * SIZEOF_SIZE_T) / h->width;
			h->heapbckt.free = HASH_HEADER_SIZE * SIZEOF_SIZE_T + i * nwidth;
			while (i > 0) {
				i--;
				BUN4type v = ((BUN4type *) h->Bckt)[i];
				((BUN8type *) h->Bckt)[i] = v == BUN4_NONE ? BUN8_NONE : v;
			}
			break;
		}
		h->nil = BUN8_NONE;
		break;
#endif
	}
	h->width = nwidth;
#else
	(void) b;
#endif
	return GDK_SUCCEED;
}

gdk_return
HASHgrowbucket(BAT *b)
{
	Hash *h = b->thash;
	BUN nbucket;
	BUN onbucket = h->nbucket;
	lng t0 = 0;

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	/* only needed to fix hash tables built before this fix was
	 * introduced */
	if (h->nil <= h->mask2 && HASHupgradehashheap(b) != GDK_SUCCEED)
		return GDK_FAIL;

	h->heapbckt.dirty = true;
	h->heaplink.dirty = true;
	if (((size_t *) h->heapbckt.base)[0] & (size_t) 1 << 24) {
		/* persistent hash: remove persistency */
		((size_t *) h->heapbckt.base)[0] &= ~((size_t) 1 << 24);
		if (h->heapbckt.storage != STORE_MEM) {
			if (!(GDKdebug & NOSYNCMASK) &&
			    MT_msync(h->heapbckt.base, SIZEOF_SIZE_T) < 0)
				return GDK_FAIL;
		}
	}
	while (h->nunique >= (nbucket = h->nbucket) * 7 / 8) {
		BUN new = h->nbucket;
		BUN old = new & h->mask1;
		BATiter bi = bat_iterator(b);
		BUN mask = h->mask1 + 1; /* == h->mask2 - h->mask1 */

		assert(h->heapbckt.free == nbucket * h->width + HASH_HEADER_SIZE * SIZEOF_SIZE_T);
		if (h->heapbckt.free + h->width > h->heapbckt.size) {
			if (HEAPextend(&h->heapbckt,
				       h->heapbckt.size + GDK_mmap_pagesize,
				       true) != GDK_SUCCEED) {
				return GDK_FAIL;
			}
			h->Bckt = h->heapbckt.base + HASH_HEADER_SIZE * SIZEOF_SIZE_T;
		}
		assert(h->heapbckt.free + h->width <= h->heapbckt.size);
		if (h->nbucket == h->mask2) {
			h->mask1 = h->mask2;
			h->mask2 = h->mask2 << 1 | 1;
			if (h->nil == h->mask2) {
				/* time to widen the hash table */
				if (HASHupgradehashheap(b) != GDK_SUCCEED)
					return GDK_FAIL;
			}
		}
		h->nbucket++;
		h->heapbckt.free += h->width;
		BUN lold, lnew, hb;
		lold = lnew = HASHnil(h);
		if ((hb = HASHget(h, old)) != HASHnil(h)) {
			h->nheads--;
			do {
				const void *v = BUNtail(bi, hb);
				BUN hsh = ATOMhash(h->type, v);
				assert((hsh & (mask - 1)) == old);
				if (hsh & mask) {
					/* move to new list */
					if (lnew == HASHnil(h)) {
						HASHput(h, new, hb);
						h->nheads++;
					} else
						HASHputlink(h, lnew, hb);
					lnew = hb;
				} else {
					/* move to old list */
					if (lold == HASHnil(h)) {
						h->nheads++;
						HASHput(h, old, hb);
					} else
						HASHputlink(h, lold, hb);
					lold = hb;
				}
				hb = HASHgetlink(h, hb);
			} while (hb != HASHnil(h));
		}
		if (lnew == HASHnil(h))
			HASHput(h, new, HASHnil(h));
		else
			HASHputlink(h, lnew, HASHnil(h));
		if (lold == HASHnil(h))
			HASHput(h, old, HASHnil(h));
		else
			HASHputlink(h, lold, HASHnil(h));
	}
	TRC_DEBUG_IF(ACCELERATOR) if (h->nbucket > onbucket) {
		TRC_DEBUG_ENDIF(ACCELERATOR, ALGOBATFMT " " BUNFMT
			" -> " BUNFMT " buckets (" LLFMT " usec)\n",
			ALGOBATPAR(b),
			onbucket, h->nbucket, GDKusec() - t0);
		HASHcollisions(b, h, __func__);
	}
	return GDK_SUCCEED;
}

/* Return TRUE if we have a hash on the tail, even if we need to read
 * one from disk.
 *
 * Note that the b->thash pointer can be NULL, meaning there is no
 * hash; (Hash *) 1, meaning there is no hash loaded, but it may exist
 * on disk; or a valid pointer to a loaded hash.  These values are
 * maintained here, in the HASHdestroy and HASHfree functions, and in
 * BBPdiskscan during initialization. */
bool
BATcheckhash(BAT *b)
{
	bool ret;
	lng t = 0;

	/* we don't need the lock just to read the value b->thash */
	if (b->thash == (Hash *) 1) {
		/* but when we want to change it, we need the lock */
		TRC_DEBUG_IF(ACCELERATOR) t = GDKusec();
		MT_rwlock_wrlock(&b->thashlock);
		TRC_DEBUG_IF(ACCELERATOR) t = GDKusec() - t;
		/* if still 1 now that we have the lock, we can update */
		if (b->thash == (Hash *) 1) {
			Hash *h;
			int fd;

			assert(!GDKinmemory(b->theap->farmid));
			b->thash = NULL;
			if ((h = GDKzalloc(sizeof(*h))) != NULL &&
			    (h->heaplink.farmid = BBPselectfarm(b->batRole, b->ttype, hashheap)) >= 0 &&
			    (h->heapbckt.farmid = BBPselectfarm(b->batRole, b->ttype, hashheap)) >= 0) {
				const char *nme = BBP_physical(b->batCacheid);
				strconcat_len(h->heaplink.filename,
					      sizeof(h->heaplink.filename),
					      nme, ".thashl", NULL);
				strconcat_len(h->heapbckt.filename,
					      sizeof(h->heapbckt.filename),
					      nme, ".thashb", NULL);

				/* check whether a persisted hash can be found */
				if ((fd = GDKfdlocate(h->heapbckt.farmid, nme, "rb+", "thashb")) >= 0) {
					size_t hdata[HASH_HEADER_SIZE];
					struct stat st;

					if (read(fd, hdata, sizeof(hdata)) == sizeof(hdata) &&
					    (hdata[0] == (
#ifdef PERSISTENTHASH
						    ((size_t) 1 << 24) |
#endif
						    HASH_VERSION)
#ifdef HASH_VERSION_NOUUID
					     /* if not uuid, also allow previous version */
					     || (hdata[0] == (
#ifdef PERSISTENTHASH
							 ((size_t) 1 << 24) |
#endif
							 HASH_VERSION_NOUUID) &&
						 strcmp(ATOMname(b->ttype), "uuid") != 0)
#endif
						    ) &&
					    hdata[1] > 0 &&
					    (
#ifdef BUN2
						    hdata[3] == BUN2 ||
#endif
						    hdata[3] == BUN4
#ifdef BUN8
						    || hdata[3] == BUN8
#endif
						    ) &&
					    hdata[4] == (size_t) BATcount(b) &&
					    fstat(fd, &st) == 0 &&
					    st.st_size >= (off_t) (h->heapbckt.size = h->heapbckt.free = (h->nbucket = (BUN) hdata[2]) * (BUN) (h->width = (uint8_t) hdata[3]) + HASH_HEADER_SIZE * SIZEOF_SIZE_T) &&
					    close(fd) == 0 &&
					    (fd = GDKfdlocate(h->heaplink.farmid, nme, "rb+", "thashl")) >= 0 &&
					    fstat(fd, &st) == 0 &&
					    st.st_size > 0 &&
					    st.st_size >= (off_t) (h->heaplink.size = h->heaplink.free = hdata[1] * h->width) &&
					    HEAPload(&h->heaplink, nme, "thashl", false) == GDK_SUCCEED) {
						if (HEAPload(&h->heapbckt, nme, "thashb", false) == GDK_SUCCEED) {
							if (h->nbucket & (h->nbucket - 1)) {
								h->mask2 = hashmask(h->nbucket);
								h->mask1 = h->mask2 >> 1;
							} else {
								h->mask1 = h->nbucket - 1;
								h->mask2 = h->mask1 << 1 | 1;
							}
							h->nunique = hdata[5];
							h->nheads = hdata[6];
							h->type = ATOMtype(b->ttype);
							switch (h->width) {
#ifdef BUN2
							case BUN2:
								h->nil = (BUN) BUN2_NONE;
								break;
#endif
							default: /* BUN4 */
								h->nil = (BUN) BUN4_NONE;
								break;
#ifdef BUN8
							case BUN8:
								h->nil = (BUN) BUN8_NONE;
								break;
#endif
							}
							if (h->nil > h->nbucket) {
								close(fd);
								h->Link = h->heaplink.base;
								h->Bckt = h->heapbckt.base + HASH_HEADER_SIZE * SIZEOF_SIZE_T;
								h->heaplink.parentid = b->batCacheid;
								h->heapbckt.parentid = b->batCacheid;
								h->heaplink.dirty = false;
								h->heapbckt.dirty = false;
								b->thash = h;
								TRC_DEBUG(ACCELERATOR,
									  ALGOBATFMT ": reusing persisted hash\n", ALGOBATPAR(b));
								MT_rwlock_wrunlock(&b->thashlock);
								return true;
							}
							/* if h->nil==h->nbucket
							 * (was
							 * possible in
							 * previous
							 * iterations
							 * of the
							 * code), then
							 * we can't
							 * use the
							 * hash since
							 * we can't
							 * distinguish
							 * between
							 * end-of-list
							 * and a valid
							 * link */
							HEAPfree(&h->heapbckt, false);
						}
						HEAPfree(&h->heaplink, false);
					}
					close(fd);
					/* unlink unusable file */
					GDKunlink(h->heaplink.farmid, BATDIR, nme, "thashl");
					GDKunlink(h->heapbckt.farmid, BATDIR, nme, "thashb");
				}
			}
			GDKfree(h);
			GDKclrerr();	/* we're not currently interested in errors */
		}
		MT_rwlock_wrunlock(&b->thashlock);
	}
	ret = b->thash != NULL;
	if (ret)
		TRC_DEBUG(ACCELERATOR, ALGOBATFMT ": already has hash, waited " LLFMT " usec\n", ALGOBATPAR(b), t);
	return ret;
}

gdk_return
BAThashsave(BAT *b, bool dosync)
{
	int fd;
	gdk_return rc = GDK_SUCCEED;
	Hash *h;
	lng t0 = 0;

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	if ((h = b->thash) != NULL) {
		Heap *hp = &h->heapbckt;

#ifndef PERSISTENTHASH
		/* no need to sync if not persistent */
		dosync = false;
#endif

		rc = GDK_FAIL;
		/* only persist if parent BAT hasn't changed in the
		 * mean time */
		((size_t *) hp->base)[0] = (size_t) HASH_VERSION;
		((size_t *) hp->base)[1] = (size_t) (h->heaplink.free / h->width);
		((size_t *) hp->base)[2] = (size_t) h->nbucket;
		((size_t *) hp->base)[3] = (size_t) h->width;
		((size_t *) hp->base)[4] = (size_t) BATcount(b);
		((size_t *) hp->base)[5] = (size_t) h->nunique;
		((size_t *) hp->base)[6] = (size_t) h->nheads;
		if (!b->theap->dirty &&
		    HEAPsave(&h->heaplink, h->heaplink.filename, NULL, dosync) == GDK_SUCCEED &&
		    HEAPsave(hp, hp->filename, NULL, dosync) == GDK_SUCCEED) {
			h->heaplink.dirty = false;
			if (hp->storage == STORE_MEM) {
				if ((fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) >= 0) {
					((size_t *) hp->base)[0] |= (size_t) 1 << 24;
					if (write(fd, hp->base, SIZEOF_SIZE_T) >= 0) {
						rc = GDK_SUCCEED;
						if (dosync &&
						    !(GDKdebug & NOSYNCMASK)) {
#if defined(NATIVE_WIN32)
							_commit(fd);
#elif defined(HAVE_FDATASYNC)
							fdatasync(fd);
#elif defined(HAVE_FSYNC)
							fsync(fd);
#endif
						}
						hp->dirty = false;
					} else {
						perror("write hash");
					}
					close(fd);
				}
			} else {
				((size_t *) hp->base)[0] |= (size_t) 1 << 24;
				if (dosync && !(GDKdebug & NOSYNCMASK) &&
				    MT_msync(hp->base, SIZEOF_SIZE_T) < 0) {
					((size_t *) hp->base)[0] &= ~((size_t) 1 << 24);
				} else {
					hp->dirty = false;
					rc = GDK_SUCCEED;
				}
			}
			TRC_DEBUG(ACCELERATOR,
				  ALGOBATFMT ": persisting hash %s%s (" LLFMT " usec)%s\n", ALGOBATPAR(b), hp->filename, dosync ? "" : " no sync", GDKusec() - t0, rc == GDK_SUCCEED ? "" : " failed");
		}
	}
	return rc;
}

#ifdef PERSISTENTHASH
static void
BAThashsync(void *arg)
{
	BAT *b = arg;

	/* we could check whether b->thash == NULL before getting the
	 * lock, and only lock if it isn't; however, it's very
	 * unlikely that that is the case, so we don't */
	MT_rwlock_rdlock(&b->thashlock);
	BAThashsave(b, true);
	MT_rwlock_rdunlock(&b->thashlock);
	BBPunfix(b->batCacheid);
}
#endif

#define EQbte(a, b)	((a) == (b))
#define EQsht(a, b)	((a) == (b))
#define EQint(a, b)	((a) == (b))
#define EQlng(a, b)	((a) == (b))
#ifdef HAVE_HGE
#define EQhge(a, b)	((a) == (b))
#endif
#define EQflt(a, b)	(is_flt_nil(a) ? is_flt_nil(b) : (a) == (b))
#define EQdbl(a, b)	(is_dbl_nil(a) ? is_dbl_nil(b) : (a) == (b))
#ifdef HAVE_HGE
#define EQuuid(a, b)	((a).h == (b).h)
#else
#ifdef HAVE_UUID
#define EQuuid(a, b)	(uuid_compare((a).u, (b).u) == 0)
#else
#define EQuuid(a, b)	(memcmp((a).u, (b).u, UUID_SIZE) == 0)
#endif
#endif

#define starthash(TYPE)							\
	do {								\
		const TYPE *restrict v = (const TYPE *) BUNtloc(bi, 0);	\
		for (; p < cnt1; p++) {					\
			c = hash_##TYPE(h, v + o - b->hseqbase);	\
			hget = HASHget(h, c);				\
			if (hget == hnil) {				\
				if (h->nheads == maxslots)		\
					break; /* mask too full */	\
				h->nheads++;				\
				h->nunique++;				\
			} else {					\
				for (hb = hget;				\
				     hb != hnil;			\
				     hb = HASHgetlink(h, hb)) {		\
					if (EQ##TYPE(v[o - b->hseqbase], v[hb])) \
						break;			\
				}					\
				h->nunique += hb == hnil;		\
			}						\
			HASHputlink(h, p, hget);			\
			HASHput(h, c, p);				\
			o = canditer_next(ci);				\
		}							\
	} while (0)
#define finishhash(TYPE)						\
	do {								\
		const TYPE *restrict v = (const TYPE *) BUNtloc(bi, 0);	\
		for (; p < ci->ncand; p++) {				\
			c = hash_##TYPE(h, v + o - b->hseqbase);	\
			hget = HASHget(h, c);				\
			h->nheads += hget == hnil;			\
			if (!hascand) {					\
				for (hb = hget;				\
				     hb != hnil;			\
				     hb = HASHgetlink(h, hb)) {		\
					if (EQ##TYPE(v[o - b->hseqbase], v[hb])) \
						break;			\
				}					\
				h->nunique += hb == hnil;		\
				o = canditer_next_dense(ci);		\
			} else {					\
				o = canditer_next(ci);			\
			}						\
			HASHputlink(h, p, hget);			\
			HASHput(h, c, p);				\
		}							\
	} while (0)

/* Internal function to create a hash table for the given BAT b.
 * If a candidate list s is also given, the hash table is specific for
 * the combination of the two: only values from b that are referred to
 * by s are included in the hash table, so if a result is found when
 * searching the hash table, the result is a candidate. */
Hash *
BAThash_impl(BAT *restrict b, struct canditer *restrict ci, const char *restrict ext)
{
	lng t0 = 0;
	unsigned int tpe = ATOMbasetype(b->ttype);
	BUN cnt1;
	BUN mask, maxmask = 0;
	BUN p, c;
	oid o;
	BUN hnil, hget, hb;
	Hash *h = NULL;
	const char *nme = GDKinmemory(b->theap->farmid) ? ":memory:" : BBP_physical(b->batCacheid);
	BATiter bi = bat_iterator(b);
	const ValRecord *prop;
	bool hascand = ci->tpe != cand_dense || ci->ncand != BATcount(b);

	assert(strcmp(ext, "thash") != 0 || !hascand);

	MT_thread_setalgorithm(hascand ? "create hash with candidates" : "create hash");
	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();
	TRC_DEBUG(ACCELERATOR,
		  ALGOBATFMT ": create hash;\n", ALGOBATPAR(b));
	if (b->ttype == TYPE_void) {
		if (is_oid_nil(b->tseqbase)) {
			TRC_DEBUG(ACCELERATOR,
				  "cannot create hash-table on void-NIL column.\n");
			GDKerror("no hash on void/nil column\n");
			return NULL;
		}
		TRC_DEBUG(ACCELERATOR,
			  "creating hash-table on void column..\n");
		assert(0);
		tpe = TYPE_void;
	}

	if ((h = GDKzalloc(sizeof(*h))) == NULL ||
	    (h->heaplink.farmid = BBPselectfarm(hascand ? TRANSIENT : b->batRole, b->ttype, hashheap)) < 0 ||
	    (h->heapbckt.farmid = BBPselectfarm(hascand ? TRANSIENT : b->batRole, b->ttype, hashheap)) < 0) {
		GDKfree(h);
		return NULL;
	}
	h->width = HASHwidth(BATcapacity(b));
	h->heaplink.dirty = true;
	h->heapbckt.dirty = true;
	strconcat_len(h->heaplink.filename, sizeof(h->heaplink.filename),
		      nme, ".", ext, "l", NULL);
	strconcat_len(h->heapbckt.filename, sizeof(h->heapbckt.filename),
		      nme, ".", ext, "b", NULL);
	if (HEAPalloc(&h->heaplink, hascand ? ci->ncand : BATcapacity(b),
		      h->width, 0) != GDK_SUCCEED) {
		GDKfree(h);
		return NULL;
	}
	h->heaplink.free = ci->ncand * h->width;
	h->Link = h->heaplink.base;
#ifndef NDEBUG
	/* clear unused part of Link array */
	if (h->heaplink.size > h->heaplink.free)
		memset(h->heaplink.base + h->heaplink.free, 0,
		       h->heaplink.size - h->heaplink.free);
#endif

	/* determine hash mask size */
	cnt1 = 0;
	if (ATOMsize(tpe) == 1) {
		/* perfect hash for one-byte sized atoms */
		mask = (1 << 8);
	} else if (ATOMsize(tpe) == 2) {
		/* perfect hash for two-byte sized atoms */
		mask = (1 << 16);
	} else if (b->tkey || ci->ncand <= 4096) {
		/* if key, or if small, don't bother dynamically
		 * adjusting the hash mask */
		mask = HASHmask(ci->ncand);
 	} else if (!hascand && (prop = BATgetprop_try(b, GDK_NUNIQUE)) != NULL) {
		assert(prop->vtype == TYPE_oid);
		mask = prop->val.oval * 8 / 7;
 	} else if (!hascand && (prop = BATgetprop_try(b, GDK_HASH_BUCKETS)) != NULL) {
		assert(prop->vtype == TYPE_oid);
		mask = prop->val.oval;
		maxmask = HASHmask(ci->ncand);
		if (mask > maxmask)
			mask = maxmask;
	} else {
		/* dynamic hash: we start with HASHmask(ci->ncand)/64, or,
		 * if ci->ncand large enough, HASHmask(ci->ncand)/256; if there
		 * are too many collisions we try HASHmask(ci->ncand)/64,
		 * HASHmask(ci->ncand)/16, HASHmask(ci->ncand)/4, and finally
		 * HASHmask(ci->ncand), but we might skip some of these if
		 * there are many distinct values.  */
		maxmask = HASHmask(ci->ncand);
		mask = maxmask >> 6;
		while (mask > 4096)
			mask >>= 2;
		/* try out on first 25% of b */
		cnt1 = ci->ncand >> 2;
	}

	o = canditer_next(ci);	/* always one ahead */
	for (;;) {
		lng t1 = 0;
		TRC_DEBUG_IF(ACCELERATOR) t1 = GDKusec();
		BUN maxslots = (mask >> 3) - 1;	/* 1/8 full is too full */

		h->nheads = 0;
		h->nunique = 0;
		p = 0;
		HEAPfree(&h->heapbckt, true);
		/* create the hash structures */
		if (HASHnew(h, ATOMtype(b->ttype), BATcapacity(b),
			    mask, ci->ncand, true) != GDK_SUCCEED) {
			HEAPfree(&h->heaplink, true);
			GDKfree(h);
			return NULL;
		}

		hnil = HASHnil(h);

		switch (tpe) {
		case TYPE_bte:
			starthash(bte);
			break;
		case TYPE_sht:
			starthash(sht);
			break;
		case TYPE_flt:
			starthash(flt);
			break;
		case TYPE_int:
			starthash(int);
			break;
		case TYPE_dbl:
			starthash(dbl);
			break;
		case TYPE_lng:
			starthash(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			starthash(hge);
			break;
#endif
		case TYPE_uuid:
			starthash(uuid);
			break;
		default:
			for (; p < cnt1; p++) {
				const void *restrict v = BUNtail(bi, o - b->hseqbase);
				c = hash_any(h, v);
				hget = HASHget(h, c);
				if (hget == hnil) {
					if (h->nheads == maxslots)
						break; /* mask too full */
					h->nheads++;
					h->nunique++;
				} else {
					for (hb = hget;
					     hb != hnil;
					     hb = HASHgetlink(h, hb)) {
						if (ATOMcmp(h->type,
							    v,
							    BUNtail(bi, hb)) == 0)
							break;
					}
					h->nunique += hb == hnil;
				}
				HASHputlink(h, p, hget);
				HASHput(h, c, p);
				o = canditer_next(ci);
			}
			break;
		}
		TRC_DEBUG_IF(ACCELERATOR) if (p < cnt1)
			TRC_DEBUG_ENDIF(ACCELERATOR,
					"%s: abort starthash with "
				"mask " BUNFMT " at " BUNFMT " after " LLFMT " usec\n", BATgetId(b), mask, p, GDKusec() - t1);
		if (p == cnt1 || mask == maxmask)
			break;
		mask <<= 2;
		/* if we fill up the slots fast (p <= maxslots * 1.2)
		 * increase mask size a bit more quickly */
		if (p == h->nunique) {
			/* only unique values so far: grow really fast */
			mask = maxmask;
			cnt1 = 0;
		} else if (mask < maxmask && p <= maxslots * 1.2)
			mask <<= 2;
		canditer_reset(ci);
		o = canditer_next(ci);
	}

	/* finish the hashtable with the current mask */
	switch (tpe) {
	case TYPE_bte:
		finishhash(bte);
		break;
	case TYPE_sht:
		finishhash(sht);
		break;
	case TYPE_int:
		finishhash(int);
		break;
	case TYPE_flt:
		finishhash(flt);
		break;
	case TYPE_dbl:
		finishhash(dbl);
		break;
	case TYPE_lng:
		finishhash(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		finishhash(hge);
		break;
#endif
	case TYPE_uuid:
		finishhash(uuid);
		break;
	default:
		for (; p < ci->ncand; p++) {
			const void *restrict v = BUNtail(bi, o - b->hseqbase);
			c = hash_any(h, v);
			hget = HASHget(h, c);
			h->nheads += hget == hnil;
			if (!hascand) {
				for (hb = hget;
				     hb != hnil;
				     hb = HASHgetlink(h, hb)) {
					if (ATOMcmp(h->type, v, BUNtail(bi, hb)) == 0)
						break;
				}
				h->nunique += hb == hnil;
			}
			HASHputlink(h, p, hget);
			HASHput(h, c, p);
			o = canditer_next(ci);
		}
		break;
	}
	if (!hascand) {
		BATrmprop_nolock(b, GDK_HASH_BUCKETS);
		BATrmprop_nolock(b, GDK_NUNIQUE);
	}
	h->heapbckt.parentid = b->batCacheid;
	h->heaplink.parentid = b->batCacheid;
	/* if the number of unique values is equal to the bat count,
	 * all values are necessarily distinct */
	if (h->nunique == BATcount(b) && !b->tkey) {
		b->tkey = true;
		b->batDirtydesc = true;
	}
	TRC_DEBUG_IF(ACCELERATOR) {
		TRC_DEBUG_ENDIF(ACCELERATOR,
				"hash construction " LLFMT " usec\n", GDKusec() - t0);
		HASHcollisions(b, h, __func__);
	}
	return h;
}

gdk_return
BAThash(BAT *b)
{
	assert(b->batCacheid > 0);
	if (BATcheckhash(b)) {
		return GDK_SUCCEED;
	}
	for (;;) {
		/* If multiple threads simultaneously try to build a
		 * hash on a bat, e.g. in order to perform a join, it
		 * may happen that one thread succeeds in obtaining the
		 * write lock, then builds the hash, releases the lock,
		 * acquires the read lock, and performs the join.  The
		 * other threads may then still be attempting to acquire
		 * the write lock.  But now they have to wait until the
		 * read lock is released, which can be quite a long
		 * time.  Especially if a second thread goes through the
		 * same process as the first. */
		if (MT_rwlock_wrtry(&b->thashlock))
			break;
		MT_sleep_ms(1);
		if (MT_rwlock_rdtry(&b->thashlock)) {
			if (b->thash != NULL && b->thash != (Hash *) 1) {
				MT_rwlock_rdunlock(&b->thashlock);
				return GDK_SUCCEED;
			}
			MT_rwlock_rdunlock(&b->thashlock);
		}
	}
	/* we have the write lock */
	if (b->thash == NULL) {
		struct canditer ci;
		canditer_init(&ci, b, NULL);
		if ((b->thash = BAThash_impl(b, &ci, "thash")) == NULL) {
			MT_rwlock_wrunlock(&b->thashlock);
			return GDK_FAIL;
		}
#ifdef PERSISTENTHASH
		if (BBP_status(b->batCacheid) & BBPEXISTING && !b->theap->dirty && !GDKinmemory(b->theap->farmid)) {
			MT_Id tid;
			BBPfix(b->batCacheid);
			char name[MT_NAME_LEN];
			snprintf(name, sizeof(name), "hashsync%d", b->batCacheid);
			MT_rwlock_wrunlock(&b->thashlock);
			if (MT_create_thread(&tid, BAThashsync, b,
					     MT_THR_DETACHED,
					     name) < 0) {
				/* couldn't start thread: clean up */
				BBPunfix(b->batCacheid);
			}
			return GDK_SUCCEED;
		} else
			TRC_DEBUG(ACCELERATOR,
					"NOT persisting hash %d\n", b->batCacheid);
#endif
	}
	MT_rwlock_wrunlock(&b->thashlock);
	return GDK_SUCCEED;
}

/*
 * The entry on which a value hashes can be calculated with the
 * routine HASHprobe.
 */
BUN
HASHprobe(const Hash *h, const void *v)
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
	case TYPE_uuid:
		return hash_uuid(h, v);
	default:
		return hash_any(h, v);
	}
}

static inline void
HASHappend_locked(BAT *b, BUN i, const void *v)
{
	Hash *h = b->thash;
	if (h == NULL) {
		return;
	}
	if (h == (Hash *) 1) {
		b->thash = NULL;
		doHASHdestroy(b, h);
		return;
	}
	assert(i * h->width == h->heaplink.free);
	if (HASHwidth(i + 1) > h->width &&
	     HASHupgradehashheap(b) != GDK_SUCCEED) {
		return;
	}
	if ((ATOMsize(b->ttype) > 2 &&
	     HASHgrowbucket(b) != GDK_SUCCEED) ||
	    ((i + 1) * h->width > h->heaplink.size &&
	     HEAPextend(&h->heaplink,
			i * h->width + GDK_mmap_pagesize,
			true) != GDK_SUCCEED)) {
		b->thash = NULL;
		HEAPfree(&h->heapbckt, true);
		HEAPfree(&h->heaplink, true);
		GDKfree(h);
		return;
	}
	h->Link = h->heaplink.base;
	BUN c = HASHprobe(h, v);
	h->heaplink.free += h->width;
	BUN hb = HASHget(h, c);
	BUN hb2;
	BATiter bi = bat_iterator(b);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(h->type);
	for (hb2 = hb;
	     hb2 != HASHnil(h);
	     hb2 = HASHgetlink(h, hb2)) {
		if (atomcmp(v, BUNtail(bi, hb2)) == 0)
			break;
	}
	h->nheads += hb == HASHnil(h);
	h->nunique += hb2 == HASHnil(h);
	HASHputlink(h, i, hb);
	HASHput(h, c, i);
	h->heapbckt.dirty = true;
	h->heaplink.dirty = true;
}

void
HASHappend(BAT *b, BUN i, const void *v)
{
	MT_rwlock_wrlock(&b->thashlock);
	HASHappend_locked(b, i, v);
	MT_rwlock_wrunlock(&b->thashlock);
}

/* insert value v at position p into the hash table of b */
static inline void
HASHinsert_locked(BAT *b, BUN p, const void *v)
{
	Hash *h = b->thash;
	if (h == NULL) {
		return;
	}
	if (h == (Hash *) 1) {
		b->thash = NULL;
		doHASHdestroy(b, h);
		return;
	}
	assert(p * h->width < h->heaplink.free);
	BUN c = HASHprobe(h, v);
	BUN hb = HASHget(h, c);
	BATiter bi = bat_iterator(b);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(h->type);
	if (hb == h->nil || hb < p) {
		/* bucket is empty, or bucket is used by lower numbered
		 * position */
		h->heaplink.dirty = true;
		h->heapbckt.dirty = true;
		HASHputlink(h, p, hb);
		HASHput(h, c, p);
		if (hb == h->nil) {
			h->nheads++;
		} else {
			do {
				if (atomcmp(v, BUNtail(bi, hb)) == 0) {
					/* found another row with the
					 * same value, so don't
					 * increment nunique */
					return;
				}
				hb = HASHgetlink(h, hb);
			} while (hb != h->nil);
		}
		/* this is a new value */
		h->nunique++;
		return;
	}
	bool seen = false;
	for (;;) {
		if (!seen)
			seen = atomcmp(v, BUNtail(bi, hb)) == 0;
		BUN hb2 = HASHgetlink(h, hb);
		if (hb2 == h->nil || hb2 < p) {
			h->heaplink.dirty = true;
			HASHputlink(h, p, hb2);
			HASHputlink(h, hb, p);
			while (!seen && hb2 != h->nil) {
				seen = atomcmp(v, BUNtail(bi, hb2)) == 0;
				hb2 = HASHgetlink(h, hb2);
			}
			if (!seen)
				h->nunique++;
			return;
		}
		hb = hb2;
	}
}

void
HASHinsert(BAT *b, BUN p, const void *v)
{
	MT_rwlock_wrlock(&b->thashlock);
	HASHinsert_locked(b, p, v);
	MT_rwlock_wrunlock(&b->thashlock);
}

/* delete value v at position p from the hash table of b */
static inline void
HASHdelete_locked(BAT *b, BUN p, const void *v)
{
	Hash *h = b->thash;
	if (h == NULL) {
		return;
	}
	if (h == (Hash *) 1) {
		b->thash = NULL;
		doHASHdestroy(b, h);
		return;
	}
	assert(p * h->width < h->heaplink.free);
	BUN c = HASHprobe(h, v);
	BUN hb = HASHget(h, c);
	BATiter bi = bat_iterator(b);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(h->type);
	if (hb == p) {
		BUN hb2 = HASHgetlink(h, p);
		h->heaplink.dirty = true;
		h->heapbckt.dirty = true;
		HASHput(h, c, hb2);
		HASHputlink(h, p, h->nil);
		if (hb2 == h->nil) {
			h->nheads--;
		} else {
			do {
				if (atomcmp(v, BUNtail(bi, hb2)) == 0) {
					/* found another row with the
					 * same value, so don't
					 * decrement nunique below */
					return;
				}
				hb2 = HASHgetlink(h, hb2);
			} while (hb2 != h->nil);
		}
		/* no rows found with the same value, so number of
		 * unique values is one lower */
		h->nunique--;
		return;
	}
	bool seen = false;
	for (;;) {
		if (!seen)
			seen = atomcmp(v, BUNtail(bi, hb)) == 0;
		BUN hb2 = HASHgetlink(h, hb);
		assert(hb2 != h->nil);
		if (hb2 == p) {
			for (hb2 = HASHgetlink(h, hb2);
			     !seen && hb2 != h->nil;
			     hb2 = HASHgetlink(h, hb2))
				seen = atomcmp(v, BUNtail(bi, hb2)) == 0;
			break;
		}
		hb = hb2;
	}
	h->heaplink.dirty = true;
	HASHputlink(h, hb, HASHgetlink(h, p));
	HASHputlink(h, p, h->nil);
	if (!seen)
		h->nunique--;
}

void
HASHdelete(BAT *b, BUN p, const void *v)
{
	MT_rwlock_wrlock(&b->thashlock);
	HASHdelete_locked(b, p, v);
	MT_rwlock_wrunlock(&b->thashlock);
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
HASHdestroy(BAT *b)
{
	if (b && b->thash) {
		Hash *hs;
		MT_rwlock_wrlock(&b->thashlock);
		hs = b->thash;
		b->thash = NULL;
		MT_rwlock_wrunlock(&b->thashlock);
		doHASHdestroy(b, hs);
	}
}

void
HASHfree(BAT *b)
{
	if (b && b->thash) {
		Hash *h;
		MT_rwlock_wrlock(&b->thashlock);
		if ((h = b->thash) != NULL && h != (Hash *) 1) {
			bool rmheap = h->heaplink.dirty || h->heapbckt.dirty;
			TRC_DEBUG(ACCELERATOR, ALGOBATFMT " free hash %s\n",
				  ALGOBATPAR(b),
				  rmheap ? "removing" : "keeping");

			b->thash = rmheap ? NULL : (Hash *) 1;
			HEAPfree(&h->heapbckt, rmheap);
			HEAPfree(&h->heaplink, rmheap);
			GDKfree(h);
		}
		MT_rwlock_wrunlock(&b->thashlock);
	}
}

bool
HASHgonebad(BAT *b, const void *v)
{
	Hash *h = b->thash;
	BATiter bi = bat_iterator(b);
	BUN cnt, hit;

	if (h == NULL)
		return true;	/* no hash is bad hash? */

	if (h->nbucket * 2 < BATcount(b)) {
		int (*cmp) (const void *, const void *) = ATOMcompare(b->ttype);
		BUN i = HASHget(h, (BUN) HASHprobe(h, v)), nil = HASHnil(h);
		for (cnt = hit = 1; i != nil; i = HASHgetlink(h, i), cnt++)
			hit += ((*cmp) (v, BUNtail(bi, (BUN) i)) == 0);

		if (cnt / hit > 4)
			return true;	/* linked list too long */

		/* in this case, linked lists are long but contain the
		 * desired values such hash tables may be useful for
		 * locating all duplicates */
	}
	return false;		/* a-ok */
}
