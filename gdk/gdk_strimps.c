/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */


/* Author: Panagiotis Koutsourakis
 *
 * A string imprint is an index that can be used as a prefilter in LIKE
 * queries. It has 2 components:
 *
 * - a header of 64 string element pairs.
 *
 * - a 64 bit mask for each string in the BAT that encodes the presence
 *   or absence of each element of the header in the specific item.
 *
 * A string imprint is stored in a new Heap in the BAT, aligned in 8
 * byte (64 bit) words.
 *
 * The first 64 bit word, the header descriptor, describes how the
 * header of the strimp is encoded. The least significant byte (v in the
 * schematic below) is the version number. The second (np) is the number
 * of pairs in the header. In the current implementation this is always
 * 64. The next 2 bytes (hs) is the total size of the header in
 * bytes. Finally the fifth byte is the persistence byte. The last 3
 * bytes needed to align to the 8 byte boundary should be zero, and are
 * reserved for future use.
 *
 * The following np bytes are the sizes of the pairs. These can have
 * values from 2 to 8 and are the number of bytes that the corresponding
 * pair takes up. Following that there are the bytes encoding the actual
 * pairs.
 *
 * | 1byte | 1byte | 1byte | 1byte | 1byte | 1byte | 1byte | 1byte |
 * |---------------------------------------------------------------|
 * |   v   |  np   |      hs       |   p   |      reserved         |  8bytes     ---
 * |---------------------------------------------------------------|  ___         |
 * | psz_0 | psz_1 | ...                                           |   |          |
 * |                                                               |   |          |
 * |                                                               |np bytes      |
 * |                                                               |   |          |
 * |                                                   ... | psz_n |   |       hs bytes
 * |---------------------------------------------------------------|  ___         |
 * |             pair_0            |             pair_1            |              |
 * |                              ...                              |              |
 * |                 pair_k-1                   |   pair_k         |              |
 * |                          pair_n                               |              |
 * |---------------------------------------------------------------|             ---
 *
 *
 * The bitmasks for each string in the BAT follow after this, aligned to
 * the string BAT.
 *
 * Strimp creation goes as follows:
 *
 * - Construct a histogram of the element (byte or character) pairs for
 *   all the strings in the BAT.
 *
 * - Take the 64 most frequent pairs as the Strimp Header.
 *
 * - For each string in the bat construct a 64 bit mask that encodes
 *   the presence or absence of each member of the header in the string.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"


#define swp(_a, _i, _j, TPE)			\
	do {					\
		TPE _t = ((TPE *)_a)[_i];	\
		((TPE *) _a)[_i] = ((TPE *) _a)[_j];	\
		((TPE *) _a)[_j] = _t;			\
	} while(0)


/* Macros for accessing metadada of a strimp. These are recorded in the
 * first 8 bytes of the heap.
 */
#define NPAIRS(d) (((d) >> 8) & 0xff)
#define HSIZE(d) (((d) >> 16) & 0xffff)

#undef UTF8STRIMPS 		/* Not using utf8 for now */
#ifdef UTF8STRIMPS
static bool
pair_equal(CharPair *p1, CharPair *p2) {
	if(p1->psize != p2->psize)
		return false;

	for(size_t i = 0; i < p1->psize; i++)
		if (*(p1->pbytes + i) != *(p2->pbytes + i))
			return false;

	return true;
}
#else
/* BytePairs implementation.
 *
 * All the of the following functions and macros up to #endif need to be
 * implemented for the UTF8 case.
 */
#define isIgnored(x) (isspace((x)) || isdigit((x)) || ispunct((x)))
#define pairToIndex(b1, b2) (uint16_t)(((uint16_t)b2)<<8 | ((uint16_t)b1))

static bool
pair_equal(CharPair *p1, CharPair *p2) {
	return p1->pbytes[0] == p2->pbytes[0] &&
		p1->pbytes[1] == p2->pbytes[1];

}

static int64_t
histogram_index(PairHistogramElem *hist, size_t hsize, CharPair *p) {
	(void) hist;
	(void) hsize;
	return pairToIndex(p->pbytes[0], p->pbytes[1]);
}

static bool
pair_at(PairIterator *pi, CharPair *p) {
	if (pi->pos >= pi->lim)
		return false;
	p->pbytes = (uint8_t*)pi->s + pi->pos;
	p->psize = 2;
	return true;
}

static bool
next_pair(PairIterator *pi) {
	if (pi->pos >= pi->lim)
		return false;
	pi->pos++;
	return true;
}

#endif // UTF8STRIMPS

static int8_t
STRMPpairLookup(Strimps *s, CharPair *p) {
	size_t idx = 0;
	size_t npairs = NPAIRS(((uint64_t *)s->strimps.base)[0]);
	size_t offset = 0;
	CharPair sp;

	for (idx = 0; idx < npairs; idx++) {
		sp.psize = s->sizes_base[idx];
		sp.pbytes = s->pairs_base + offset;
		if (pair_equal(&sp, p))
			return idx;
		offset += sp.psize;
	}

	return -1;
}

static bool
ignored(CharPair *p, uint8_t elm) {
	assert(elm == 0 || elm == 1);
	return isIgnored(p->pbytes[elm]);
}

/* Given a strimp header and a string compute the bitstring of which
 * digrams are present in the string. The strimp header is a map from
 * digram to index in the strimp.
 *
 * This should probably be inlined.
 */
static uint64_t
STRMPmakebitstring(const str s, Strimps *r)
{
	uint64_t ret = 0;
	int8_t pair_idx = 0;
	PairIterator pi;
	CharPair cp;

	pi.s = s;
	pi.pos = 0;
	pi.lim = strlen(s);

	while(pair_at(&pi, &cp)) {
		pair_idx = STRMPpairLookup(r, &cp);
		if (pair_idx >= 0)
			ret |= ((uint64_t)0x1 << pair_idx);
		next_pair(&pi);
	}

	return ret;
}

/* Given a histogram find the indices of the STRIMP_HEADER_SIZE largest
 * counts.
 *
 * We make one scan of histogram and every time we find a count that is
 * greater than the current minimum of the STRIMP_HEADER_SIZE, we bubble
 * it up in the header until we find a count that is greater. We carry
 * the index in the histogram because this is the information we are
 * actually interested in keeping.
 *
 * At the end of this process we have the indices of STRIMP_HEADER_SIZE
 * largest counts in the histogram. This process is O(n) in time since
 * we are doing constant work (at most 63 comparisons and swaps) for
 * each item in the histogram and as such is (theoretically) more
 * efficient than sorting (O(nlog n))and taking the STRIMP_HEADER_SIZE
 * largest elements. This depends on the size of the histogram n. For
 * some small n sorting might be more efficient, but for such inputs the
 * difference should not be noticeable.
 */
static void
STRMPchoosePairs(PairHistogramElem *hist, size_t hist_size, CharPair *cp)
{
	lng t0 = 0;
	size_t i;
	uint64_t max_counts[STRIMP_HEADER_SIZE] = {0};
	size_t indices[STRIMP_HEADER_SIZE] = {0};
	const size_t cmin_max = STRIMP_HEADER_SIZE - 1;
	size_t hidx;

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	for(i = 0; i < hist_size; i++) {
		if (max_counts[cmin_max] < hist[i].cnt) {
			max_counts[cmin_max] = hist[i].cnt;
			indices[cmin_max] = i;
			for(hidx = cmin_max; hidx > 0 && max_counts[hidx] > max_counts[hidx-1]; hidx--) {
				swp(max_counts, hidx, hidx-1, uint64_t);
				swp(indices, hidx, hidx-1, size_t);
			}
		}
	}

	for(i = 0; i < STRIMP_HEADER_SIZE; i++) {
		cp[i].pbytes = hist[indices[i]].p->pbytes;
		cp[i].psize = hist[indices[i]].p->psize;
	}

	TRC_DEBUG(ACCELERATOR, LLFMT " usec\n", GDKusec() - t0);
}

static bool
STRMPbuildHeader(BAT *b, BAT *s, CharPair *hpairs) {
	lng t0 = 0;
	BATiter bi;
	str cs;
	BUN i, ncand;
	size_t hidx;
	oid x;
	size_t hlen;
	PairHistogramElem *hist;
	PairIterator pi, *pip;
	CharPair cp, *cpp;
	struct canditer ci;


	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();
	hlen = STRIMP_HISTSIZE;
	if ((hist = (PairHistogramElem *)GDKmalloc(hlen*sizeof(PairHistogramElem))) == NULL) {
		return false;
	}

	ncand = canditer_init(&ci, b, s);
	if (ncand == 0) {
		return false;
	}

	for(hidx = 0; hidx < hlen; hidx++) {
		hist[hidx].p = NULL;
		hist[hidx].cnt = 0;
	}

	// Create Histogram
	bi = bat_iterator(b);
	pip = &pi;
	cpp = &cp;
	for (i = 0; i < ncand; i++) {
		x = canditer_next(&ci) - b->hseqbase;
		cs = (str)BUNtvar(bi, x);
		if (!strNil(cs)) {
			pi.s = cs;
			pi.pos = 0;
			pi.lim = strlen(pi.s);
			while (pair_at(pip, cpp)) {
				if(ignored(cpp, 1)) {
					/* Skip this and the next pair
					 * if the next char is ignored.
					 */
					next_pair(pip);
				} else if (ignored(cpp, 0)) {
					/* Skip this pair if the current
					 * char is ignored. This should
					 * only happen at the beginnig
					 * of a string.
					 */
					;

				} else {
					hidx = histogram_index(hist, hlen, cpp);
#ifndef UTF8STRINGS
					assert(hidx < hlen);
#else
					if (hidx >= hlen) {
						// TODO: Note and realloc. Should not happen for bytepairs.
						continue;
					}
#endif
					hist[hidx].cnt++;
					if (hist[hidx].p == NULL) {
						hist[hidx].p = (CharPair *)GDKmalloc(sizeof(CharPair));
						hist[hidx].p->psize = cpp->psize;
						hist[hidx].p->pbytes = cpp->pbytes;
					}
				}
				next_pair(pip);
			}
		}
	}
	bat_iterator_end(&bi);

	// Choose the header pairs
	STRMPchoosePairs(hist, hlen, hpairs);
	for(hidx = 0; hidx < hlen; hidx++) {
		if(hist[hidx].p) {
			GDKfree(hist[hidx].p);
			hist[hidx].p = NULL;
		}
	}
	GDKfree(hist);

	TRC_DEBUG(ACCELERATOR, LLFMT " usec\n", GDKusec() - t0);
	return true;
}

/* Create the heap for a string imprint. Returns NULL on failure. This
 * follows closely the Heap creation for the order index.
 */
static Strimps *
STRMPcreateStrimpHeap(BAT *b, BAT *s)
{
	uint8_t *h1, *h2;
	Strimps *r = NULL;
	uint64_t descriptor;
	size_t i;
	uint16_t sz;
	CharPair hpairs[STRIMP_HEADER_SIZE];
	const char *nme;

	if (b->tstrimps == NULL) {
		MT_lock_set(&b->batIdxLock);
		/* Make sure no other thread got here first */
		if (b->tstrimps == NULL &&
		    STRMPbuildHeader(b, s, hpairs)) { /* Find the header pairs, put the result in hpairs */
			sz = 8 + STRIMP_HEADER_SIZE; /* add 8-bytes for the descriptor and the pair sizes */
			for (i = 0; i < STRIMP_HEADER_SIZE; i++) {
				sz += hpairs[i].psize;
			}

			nme = GDKinmemory(b->theap->farmid) ? ":memory:" : BBP_physical(b->batCacheid);
			/* Allocate the strimps heap */
			if ((r = GDKzalloc(sizeof(Strimps))) == NULL ||
			    (r->strimps.farmid = BBPselectfarm(b->batRole, b->ttype, strimpheap)) < 0 ||
			    strconcat_len(r->strimps.filename, sizeof(r->strimps.filename), nme,
					  ".tstrimps", NULL) >= sizeof(r->strimps.filename) ||
			    HEAPalloc(&r->strimps, BATcount(b) * sizeof(uint64_t) + sz, sizeof(uint8_t), 0) != GDK_SUCCEED) {
				GDKfree(r);
				MT_lock_unset(&b->batIdxLock);
				return NULL;
			}

			descriptor = STRIMP_VERSION | ((uint64_t)STRIMP_HEADER_SIZE) << 8 | ((uint64_t)sz) << 16;

			((uint64_t *)r->strimps.base)[0] = descriptor;
			r->sizes_base = h1 = (uint8_t *)r->strimps.base + 8;
			r->pairs_base = h2 = (uint8_t *)h1 + STRIMP_HEADER_SIZE;

			for (i = 0; i < STRIMP_HEADER_SIZE; i++) {
				uint8_t psize = hpairs[i].psize;
				h1[i] = psize;
				memcpy(h2, hpairs[i].pbytes, psize);
				h2 += psize;
			}
			r->strimps_base = h2;
			r->strimps.free = sz;

			b->tstrimps = r;
			b->batDirtydesc = true;
		}
		MT_lock_unset(&b->batIdxLock);
	}
	return b->tstrimps;
}

/* This macro takes a bat and checks if the strimp construction has been
 * completed. It is completed when the strimp pointer is not null and it
 * is either 1 (i.e. it exists on disk) or the number of bitstrings
 * computed is the same as the number of elements in the BAT.
 */
#define STRIMP_COMPLETE(b)			\
	b->tstrimps != NULL &&			\
		(b->tstrimps == (Strimps *)1 ||				\
		 (b->tstrimps->strimps.free - ((char *)b->tstrimps->strimps_base - b->tstrimps->strimps.base))/sizeof(uint64_t) == b->batCount)

static bool
BATcheckstrimps(BAT *b)
{
	bool ret;
	lng t = GDKusec();

	if (b == NULL)
		return false;

	assert(b->batCacheid > 0);
	if (b->tstrimps == (Strimps *)1) {
		assert(!GDKinmemory(b->theap->farmid));
		MT_lock_set(&b->batIdxLock);
		if (b->tstrimps == (Strimps *)1) {
			Strimps *hp;
			const char *nme = BBP_physical(b->batCacheid);
			int fd;

			b->tstrimps = NULL;
			if ((hp = GDKzalloc(sizeof(Strimps))) != NULL &&
			    (hp->strimps.farmid = BBPselectfarm(b->batRole, b->ttype, strimpheap)) >= 0) {
				strconcat_len(hp->strimps.filename,
					      sizeof(hp->strimps.filename),
					      nme, ".tstrimps", NULL);

				/* check whether a persisted strimp can be found */
				if ((fd = GDKfdlocate(hp->strimps.farmid, nme, "rb+", "tstrimps")) >= 0) {
					struct stat st;
					uint64_t desc;
					uint64_t npairs;
					uint64_t hsize;
					/* Read the 8 byte long strimp
					 * descriptor.
					 *
					 * NPAIRS must be 64 in the
					 * current implementation.
					 *
					 * HSIZE must be between 200 and
					 * 584 (inclusive): 8 bytes the
					 * descritor, 64 bytes the pair
					 * sizes and n*64 bytes the
					 * actual pairs where 2 <= n <=
					 * 8.
					 */
					if (read(fd, &desc, 8) == 8
					    && (desc & 0xff) == STRIMP_VERSION
					    && ((npairs = NPAIRS(desc)) == 64)
					    && (hsize = HSIZE(desc)) >= 200 && hsize <= 584
					    && ((desc >> 32) & 0xff) == 1 /* check the persistence byte */
					    && fstat(fd, &st) == 0
					    /* TODO: We might need padding in the UTF-8 case. */
					    && st.st_size >= (off_t) (hp->strimps.free = hp->strimps.size =
								      /* header size (desc + offsets + pairs) */
								      hsize +
								      /* bitmasks */
								      BATcount(b)*sizeof(uint64_t))
					    && HEAPload(&hp->strimps, nme, "tstrimps", false) == GDK_SUCCEED) {
						hp->sizes_base = (uint8_t *)hp->strimps.base + 8; /* sizes just after the descriptor */
						hp->pairs_base = hp->sizes_base + npairs;         /* pairs just after the offsets */
						hp->strimps_base = hp->strimps.base + hsize;        /* bitmasks just after the pairs */

						close(fd);
						hp->strimps.parentid = b->batCacheid;
						b->tstrimps = hp;
						TRC_DEBUG(ACCELERATOR, "BATcheckstrimps(" ALGOBATFMT "): reusing persisted strimp\n", ALGOBATPAR(b));
						MT_lock_unset(&b->batIdxLock);
						return true;
					}
					close(fd);
					/* unlink unusable file */
					GDKunlink(hp->strimps.farmid, BATDIR, nme, "tstrimp");

				}
			}
			GDKfree(hp);
			GDKclrerr();	/* we're not currently interested in errors */
		}
		MT_lock_unset(&b->batIdxLock);
	}
	/* The string imprint is initialized if the strimp pointer is
	 * not null and the number of bitstrings is equal to the bat
	 * count.
	 */
	assert(!b->tstrimps || (b->tstrimps->strimps.free - HSIZE(((uint64_t *)b->tstrimps->strimps.base)[0]))/sizeof(uint64_t) <= b->batCount);
	ret = STRIMP_COMPLETE(b);
	if (ret) {
		TRC_DEBUG(ACCELERATOR,
			  "BATcheckstrimps(" ALGOBATFMT "): already has strimps, waited " LLFMT " usec\n",
			  ALGOBATPAR(b), GDKusec() - t);
	}

	return ret;
}

/* Filter a BAT b using a string q. Return the result as a candidate
 * list.
 */
BAT *
STRMPfilter(BAT *b, BAT *s, char *q)
{
	BAT *r = NULL;
	BUN i, ncand;
	uint64_t qbmask;
	uint64_t *bitstring_array;
	Strimps *strmps;
	oid x;
	struct canditer ci;
	lng t0 = 0;

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	if (isVIEW(b)) {
		BAT *pb = BBP_cache(VIEWtparent(b));
		if (!BATcheckstrimps(pb))
			goto sfilter_fail;
		strmps = pb->tstrimps;
	}
	else {
		if (!BATcheckstrimps(b))
			goto sfilter_fail;
		strmps = b->tstrimps;
	}

	ncand = canditer_init(&ci, b, s);
	if (ncand == 0)
		return BATdense(b->hseqbase, 0, 0);
	r = COLnew(b->hseqbase, TYPE_oid, ncand, TRANSIENT);
	if (r == NULL) {
		goto sfilter_fail;
	}

	/* TODO: Compare patterns with and without SQL pattern metachars
	 * (% and _). Theoretically they should produce the same results
	 * because bitstring creation ignores punctuation characters
	 * (see the macro isIgnored).
	 */
	qbmask = STRMPmakebitstring(q, strmps);
	bitstring_array = (uint64_t *)strmps->strimps_base;

	for (i = 0; i < ncand; i++) {
		x = canditer_next(&ci);
		if ((bitstring_array[x] & qbmask) == qbmask) {
			if (BUNappend(r, &x, false) != GDK_SUCCEED) {
				BBPunfix(r->batCacheid);
				goto sfilter_fail;
			}
		}
	}

	r->tkey = true;
	r->tsorted = true;
	r->trevsorted = BATcount(r) <= 1;
	r->tnil = false;
	r->tnonil = true;
	TRC_DEBUG(ACCELERATOR, "strimp prefiltering of " LLFMT
		  " items took " LLFMT " usec. Keeping " LLFMT
		  " items (%.2f%%).\n", ncand, GDKusec()-t0, r->batCount,
		  100*r->batCount/(double)ncand);
	TRC_DEBUG(ACCELERATOR, "r->" ALGOBATFMT "\n", ALGOBATPAR(r) );
	return virtualize(r);

 sfilter_fail:
	return NULL;
}

static void
BATstrimpsync(void *arg)
{
	BAT *b = arg;
	lng t0 = 0;
	Heap *hp;
	int fd;
	const char *failed = " failed";

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	MT_lock_set(&b->batIdxLock);
	if ((hp = &b->tstrimps->strimps)) {
		if (HEAPsave(hp, hp->filename, NULL, true, hp->free) == GDK_SUCCEED) {
			if (hp->storage == STORE_MEM) {
				if ((fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) >= 0) {
					((uint64_t *)hp->base)[0] |= (uint64_t) 1 << 32;
					if (write(fd, hp->base, sizeof(uint64_t)) >= 0) {
						failed = "";
						if (!(GDKdebug & NOSYNCMASK)) {
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
						perror("write strimps");
					}
					close(fd);
				}
			} else {
				((uint64_t *)hp->base)[0] |= (uint64_t) 1 << 32;
				if (!(GDKdebug & NOSYNCMASK) &&
				    MT_msync(hp->base, sizeof(uint64_t)) < 0) {
					((uint64_t *)hp->base)[0] &= ~((uint64_t) 1 << 32);
				} else {
					hp->dirty = false;
					failed = "";
				}
			}
			TRC_DEBUG(ACCELERATOR, "BATstrimpsync(%s): strimp persisted"
				  " (" LLFMT " usec)%s\n",
				  BATgetId(b), GDKusec() - t0, failed);
		}
	}
	MT_lock_unset(&b->batIdxLock);
	BBPunfix(b->batCacheid);
}

static void
persistStrimp(BAT *b)
{
	if((BBP_status(b->batCacheid) & BBPEXISTING)
	   && b->batInserted == b->batCount
	   && !b->theap->dirty
	   && !GDKinmemory(b->theap->farmid)) {
		MT_Id tid;
		BBPfix(b->batCacheid);
		char name[MT_NAME_LEN];
		snprintf(name, sizeof(name), "strimpsync%d", b->batCacheid);
		if (MT_create_thread(&tid, BATstrimpsync, b,
				     MT_THR_DETACHED, name) < 0)
			BBPunfix(b->batCacheid);
	} else
		TRC_DEBUG(ACCELERATOR, "persistStrimp(" ALGOBATFMT "): NOT persisting strimp\n", ALGOBATPAR(b));
}

/* Create */
gdk_return
STRMPcreate(BAT *b, BAT *s)
{
	lng t0 = 0;
	BATiter bi;
	BUN i, ncand;
	str cs;
	Strimps *h;
	uint64_t *dh;
	BAT *pb;
	oid x;
	struct canditer ci;

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();
	if (b->ttype != TYPE_str) {
		GDKerror("strimps only valid for strings\n");
		return GDK_FAIL;
	}

	/* Disable this before merging to default */
	if (VIEWtparent(b)) {
		pb = BBP_cache(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	if (BATcheckstrimps(pb))
		return GDK_SUCCEED;

	if ((h = STRMPcreateStrimpHeap(pb, s)) == NULL) {
		return GDK_FAIL;
	}
	dh = (uint64_t *)h->strimps_base + b->hseqbase;

	ncand = canditer_init(&ci, b, s);

	bi = bat_iterator(b);
	for (i = 0; i < ncand; i++) {
		x = canditer_next(&ci) - b->hseqbase;
		cs = (str)BUNtvar(bi, x);
		if (!strNil(cs))
			*dh++ = STRMPmakebitstring(cs, h);
		else
			*dh++ = 0; /* no pairs in nil values */
	}
	bat_iterator_end(&bi);

	MT_lock_set(&b->batIdxLock);
	h->strimps.free += b->batCount*sizeof(uint64_t);
	MT_lock_unset(&b->batIdxLock);

	/* The thread that reaches this point last needs to write the strimp to disk. */
	if (STRIMP_COMPLETE(pb))
		persistStrimp(pb);

	TRC_DEBUG(ACCELERATOR, "strimp creation took " LLFMT " usec\n", GDKusec()-t0);
	return GDK_SUCCEED;
}

/* Left over code */
#if 0
/* This counts how many unicode codepoints the given string
 * contains.
 */
static size_t
STRMP_utf8_strlen(const uint8_t *s)
{
	size_t ret = 0;
	size_t i;
	int m,n;
	uint8_t c;

	i = 0;
	while((c = *(s + i)) != 0) {
		if (c < 0x80)
			i++;
		else {
			for (n = 0, m=0x40; c & m; n++, m >>= 1)
				;
			/* n is now the number of 10xxxxxx bytes that should
			   follow. */
			if (n == 0 || n >= 4)
				/* TODO: handle invalid utf-8 */
				{}
			i += n+1;
		}
		ret++;
	}

	return ret;
}

/* Construct a histogram of pairs of bytes in the input BAT.
 *
 * Return the histogram in hist and the number of non-zero bins in
 * count.
 */
static gdk_return
STRMPmakehistogramBP(BAT *b, uint64_t *hist, size_t hist_size, size_t *nbins)
{
	lng t0=0;
	size_t hi;
	BUN i;
	BATiter bi;
	char *ptr, *s;
	/* uint64_t cur_min = 0; */

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();
	assert(b->ttype == TYPE_str);

	for(hi = 0; hi < hist_size; hi++)
		hist[hi] = 0;

	bi = bat_iterator(b);
	*nbins = 0;
	for(i = 0; i < b->batCount; i++) {
		s = (char *)BUNtvar(bi, i);
		if (!strNil(s)) {
			for(ptr = s; *ptr != 0 && *(ptr + 1) != 0; ptr++) {
				if (isIgnored(*(ptr+1))) {
					/* Skip this and the next pair
					 * if the next char is ignored.
					 */
					ptr++;
				}
				else if (isIgnored(*ptr)) {
					/* Skip this pair if the current
					 * char is ignored. This should
					 * only happen at the beginnig
					 * of a string.
					 */
					;
				}
				else {
					hi = pairToIndex(*(ptr), *(ptr+1));
					assert(hi < hist_size);
					if (hist[hi] == 0)
						(*nbins)++;
					hist[hi]++;
					/* if (hist[hi] > cur_min) */
					/* 	cur_min = add_to_header(hi, hist[hi]); */
				}
			}
		}
	}

	TRC_DEBUG(ACCELERATOR, LLFMT " usec\n", GDKusec() - t0);
	GDKtracer_flush_buffer();
	return GDK_SUCCEED;
}

static bool
create_header(BAT *b)
{
	uint64_t hist[STRIMP_HISTSIZE] = {0};
	size_t nbins = 0;
	StrimpHeader *header;
	if ((header = (StrimpHeader*)GDKmalloc(sizeof(StrimpHeader))) == NULL)
		return false;

	if(STRMPmakehistogramBP(b, hist, STRIMP_HISTSIZE, &nbins) != GDK_SUCCEED) {
		GDKfree(header);
		return NULL;
	}

	make_header(header, hist, STRIMP_HISTSIZE);

	return header;
}

/* Given a strimp h and a pair p, return the index i for which
 *
 * h[i] == p
 *
 * Returns -1 if p is not in h.
 *
 * TODO: Should this be inlined somehow? (probably yes)
 */
static int8_t
lookup_index(BAT *b, uint8_t *pair, uint8_t psize)
{
	size_t i,j;
	size_t idx = 0;
	Heap strimp = b->tstrimps->strimps;
	uint64_t desc = (uint64_t)strimp.base[0];
	uint8_t npairs = NPAIRS(desc);
	uint8_t *pair_sizes = b->tstrimps->sizes_base;
	uint8_t *pairs = b->tstrimps->pairs_base;

	for(i = 0; i < npairs; i++) {
		if (psize == pair_sizes[i]) {
			uint8_t *h = pairs + idx;
			for (j = 0; j < psize; j++) {
				if(pair[j] != h[j])
					break;
			}
			if (j == psize)
				return i;
		}
		idx += pair_sizes[i];
	}

	return -1;
}

/* Given a BAT return the number of digrams in it. The observation is
 * that the number of digrams is the number of characters - 1:
 *
 * 1 digram starting at character 1
 * 1 digram starting at character 2
 * [...]
 * 1 digram starting at character n - 1
 */
gdk_return
STRMPndigrams(BAT *b, size_t *n)
{
	// lng t0;
	BUN i;
	BATiter bi;
	char *s;
	// GDKtracer_set_component_level("ALGO", "DEBUG");
	// struct canditer ci;

	// t0 = GDKusec();
	// BATcheck(b, NULL);
	assert(b->ttype == TYPE_str);

	bi = bat_iterator(b);
	*n = 0;
	for (i = 0; i < b->batCount; i++) {
		s = (char *)BUNtail(bi, i);
		// *n += STRMP_strlen(s) - 1;
		*n += strlen(s) - 1;
		// TRC_DEBUG(ACCELERATOR, "s["LLFMT"]=%s\n", i, s);
	}

	// TRC_DEBUG(ACCELERATOR, LLFMT "usec\n", GDKusec() - t0);
	// GDKtracer_flush_buffer();

	return GDK_SUCCEED;
}

#endif
