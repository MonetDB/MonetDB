/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
 * - Construct a histogram of all the element pairs for all the strings
 *   in the BAT.
 *
 * - Take the np most frequent pairs as the Strimp Header.
 *
 * - For each string s in the BAT, construct an np-bit mask, m_s that
 *   encodes the presence or absence of each member of the header in the
 *   string.
 *
 * Filtering with a query string q goes as follows:
 *
 * - Use the strimp header to construct an np-bit mask for q encoding
 *   the presence or absence of each member of the header in q.
 *
 * - For each bitmask in the strimp compute the bitwise AND of m_s and
 *   q. If the result is equal to q, that means that string s contains
 *   the same strimp header elements as q, so it is kept for more
 *   detailed examination.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#include "gdk_strimps.h"


/* Macros for accessing metadada of a strimp. These are recorded in the
 * first 8 bytes of the heap.
 */
#define NPAIRS(d) (size_t)(((d) >> 8) & 0xff)
#define HSIZE(d) (size_t)(((d) >> 16) & 0xffff)

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
 * The header elemens are pairs of bytes. In this case the histogram is
 * 256*256=65536 entries long. We use the numeric value of the 2 byte
 * sequence of the pair as the index to the histogram.
 *
 * Note: All the of the following functions and macros up to #endif need to be
 * implemented for the UTF8 case.
 */

/* We disregard spaces, digits and punctuation characters */
#define isIgnored(x) (isspace((x)) || isdigit((x)) || ispunct((x)))
#define pairToIndex(b1, b2) (size_t)(((uint16_t)b2)<<8 | ((uint16_t)b1))

inline static bool
pair_equal(CharPair *p1, CharPair *p2)
{
	return p1->pbytes[0] == p2->pbytes[0] &&
		p1->pbytes[1] == p2->pbytes[1];

}

inline static size_t
histogram_index(PairHistogramElem *hist, size_t hsize, CharPair *p)
{
	(void) hist;
	(void) hsize;
	return pairToIndex(p->pbytes[0], p->pbytes[1]);
}

inline static bool
pair_at(PairIterator *pi, CharPair *p)
{
	if (pi->pos >= pi->lim)
		return false;
	p->pbytes = (uint8_t*)pi->s + pi->pos;
	p->psize = 2;
	return true;
}

inline static bool
next_pair(PairIterator *pi)
{
	if (pi->pos >= pi->lim)
		return false;
	pi->pos++;
	return true;
}

/* Returns true if the specified char is ignored.
 */
inline static bool
ignored(CharPair *p, uint8_t elm)
{
	assert(elm == 0 || elm == 1);
	return isIgnored(p->pbytes[elm]);
}

#endif // UTF8STRIMPS

/* Looks up a given pair in the strimp header. Returns the index of the
 * pair, or -1 if it is not found.
 *
 * NOTE: This routine assumes that there are no more than 128 pairs.
 */
static int8_t
STRMPpairLookup(Strimps *s, CharPair *p)
{
	size_t idx = 0;
	size_t npairs = NPAIRS(((uint64_t *)s->strimps.base)[0]);
	size_t offset = 0;
	CharPair sp;

	// The return type implies that we have no more than 128 pairs
	// in the header.
	assert(npairs <= 128);

	for (idx = 0; idx < npairs; idx++) {
		sp.psize = s->sizes_base[idx];
		sp.pbytes = s->pairs_base + offset;
		if (pair_equal(&sp, p))
			return (int8_t)idx;
		offset += sp.psize;
	}

	return -1;
}


/* Computes the bitstring of a string s with respect to the strimp r.
 *
 */
static uint64_t
STRMPmakebitstring(const char *s, Strimps *r)
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

#define SWAP(_a, _i, _j, TPE)				\
	do {						\
		TPE _t = ((TPE *)_a)[_i];		\
		((TPE *) _a)[_i] = ((TPE *) _a)[_j];	\
		((TPE *) _a)[_j] = _t;			\
	} while(0)

/* Finds the indices of the STRIMP_HEADER_SIZE largest counts in a given
 * a histogram. It returns them in the cp pointer.
 *
 * We make one scan of histogram and every time we find a count that is
 * greater than the current minimum of the STRIMP_HEADER_SIZE, we bubble
 * it up in the header until we find a count that is greater. We carry
 * the index in the histogram because this is the information we are
 * actually interested in keeping.
 *
 * At the end of this process we have the indices of STRIMP_HEADER_SIZE
 * largest counts in the histogram. This process is O(n) in time since
 * we are doing constant work (at most STRIMP_HEADER_SIZE-1 comparisons
 * and swaps) for each item in the histogram and as such is
 * (theoretically) more efficient than sorting (O(nlog n))and taking the
 * STRIMP_HEADER_SIZE largest elements. This depends on the size of the
 * histogram n. For some small n sorting might be more efficient, but
 * for such inputs the difference should not be noticeable.
 *
 * TODO: Explore if a priority queue (heap construction and 64 extract
 * maximums) is worth it. The tradeoff here is that it will complicate
 * the code but might improve performance. In a debug build the current
 * implementation takes ~200 μs.
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
				SWAP(max_counts, hidx, hidx-1, uint64_t);
				SWAP(indices, hidx, hidx-1, size_t);
			}
		}
	}

	for(i = 0; i < STRIMP_HEADER_SIZE; i++) {
		cp[i].pbytes = hist[indices[i]].p->pbytes;
		cp[i].psize = hist[indices[i]].p->psize;
	}

	TRC_DEBUG(ACCELERATOR, LLFMT " usec\n", GDKusec() - t0);
}

/* Given a BAT b and a candidate list s constructs the header elements
 * of the strimp.
 *
 * Initially creates the histogram for the all the pairs in the candidate
 * and then chooses the STRIMP_HEADER_SIZE most frequent of them.
 */
static bool
STRMPbuildHeader(BAT *b, BAT *s, CharPair *hpairs)
{
	lng t0 = 0;
	BATiter bi;
	BUN i, ncand;
	size_t hidx;
	oid x;
	size_t hlen;
	PairHistogramElem *hist;
	PairIterator pi, *pip;
	CharPair cp, *cpp;
	struct canditer ci;
	size_t values = 0;
	bool res;

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	ncand = canditer_init(&ci, b, s);
	if (ncand == 0) {
		GDKerror("Not enough distinct values to create strimp index\n");
		return false;
	}

	hlen = STRIMP_HISTSIZE;
	if ((hist = (PairHistogramElem *)GDKmalloc(hlen*sizeof(PairHistogramElem))) == NULL) {
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
		const char *cs = BUNtvar(bi, x);
		if (!strNil(cs)) {
			pi.s = cs;
			pi.pos = 0;
			pi.lim = strlen(pi.s);
			while (pair_at(pip, cpp)) {
				if(ignored(cpp, 1)) {
					/* Skip this AND the next pair
					 * if the second char of the
					 * pair is ignored.
					 */
					next_pair(pip);
				} else if (ignored(cpp, 0)) {
					/* Skip this pair if the first
					 * char is ignored. This should
					 * only happen at the beginnig
					 * of a string, since the pair
					 * will have been ignored in the
					 * previous case.
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
						values++;
						hist[hidx].p = (CharPair *)GDKmalloc(sizeof(CharPair));
						if (!hist[hidx].p) {
							bat_iterator_end(&bi);
							for (hidx = 0; hidx < hlen; hidx++)
								GDKfree(hist[hidx].p);
							GDKfree(hist);
							return false;
						}
						hist[hidx].p->psize = cpp->psize;
						hist[hidx].p->pbytes = cpp->pbytes;
					}
				}
				next_pair(pip);
			}
		}
	}
	bat_iterator_end(&bi);

	// Check that we have enough values in the histogram.
	if(values >= STRIMP_HEADER_SIZE) {
		// Choose the header pairs
		STRMPchoosePairs(hist, hlen, hpairs);
	}

	for (hidx = 0; hidx < hlen; hidx++) {
		if (hist[hidx].p) {
			GDKfree(hist[hidx].p);
			hist[hidx].p = NULL;
		}
	}
	GDKfree(hist);

	TRC_DEBUG(ACCELERATOR, LLFMT " usec\n", GDKusec() - t0);
	if (!(res = values >= STRIMP_HEADER_SIZE))
		GDKerror("Not enough distinct values to create strimp index\n");
	return res;
}

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
					size_t npairs;
					size_t hsize;
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
						hp->bitstrings_base = hp->strimps.base + hsize;        /* bitmasks just after the pairs */

						close(fd);
						ATOMIC_INIT(&hp->strimps.refs, 1);
						// STRMPincref(hp);
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
	ret = b->tstrimps != NULL;
	if (ret) {
		TRC_DEBUG(ACCELERATOR,
			  "BATcheckstrimps(" ALGOBATFMT "): already has strimps, waited " LLFMT " usec\n",
			  ALGOBATPAR(b), GDKusec() - t);
	}

	return ret;
}

#define STRMPfilterloop(next) \
	do { \
		for (i = 0; i < ncand; i++) { \
			x = next(&ci); \
			if ((bitstring_array[x] & qbmask) == qbmask) { \
				rvals[j++] = x; \
			} \
		} \
	} while (0)

/* Filter a BAT b using a string q. Return the result as a candidate
 * list.
 */
BAT *
STRMPfilter(BAT *b, BAT *s, const char *q)
{
	BAT *r = NULL;
	BUN i, ncand, j = 0;
	uint64_t qbmask;
	uint64_t *bitstring_array;
	Strimps *strmps;
	oid x, *restrict rvals;
	struct canditer ci;
	lng t0 = 0;
	BAT *pb;

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	if (isVIEW(b)) {
		pb = BBP_cache(VIEWtparent(b));
	}
	else {
		pb = b;
	}

	if (!BATcheckstrimps(pb))
		goto sfilter_fail;
	MT_lock_set(&pb->batIdxLock);
	strmps = pb->tstrimps;
	STRMPincref(strmps);
	MT_lock_unset(&pb->batIdxLock);

	ncand = canditer_init(&ci, b, s);
	if (ncand == 0) {
		STRMPdecref(strmps, false);
		return BATdense(b->hseqbase, 0, 0);
	}
	r = COLnew(b->hseqbase, TYPE_oid, ncand, TRANSIENT);
	if (r == NULL) {
		STRMPdecref(strmps, false);
		goto sfilter_fail;
	}

	qbmask = STRMPmakebitstring(q, strmps);
	bitstring_array = (uint64_t *)strmps->bitstrings_base;
	rvals = Tloc(r, 0);

	if (ci.tpe == cand_dense) {
		STRMPfilterloop(canditer_next_dense);
	} else {
		STRMPfilterloop(canditer_next);
	}

	BATsetcount(r, j);
	r->tkey = true;
	r->tsorted = true;
	r->trevsorted = BATcount(r) <= 1;
	r->tnil = false;
	r->tnonil = true;
	TRC_DEBUG(ACCELERATOR, "strimp prefiltering of " BUNFMT
		  " items took " LLFMT " usec. Keeping " BUNFMT
		  " items (%.2f%%).\n", ncand, GDKusec()-t0, r->batCount,
		  100*r->batCount/(double)ncand);
	TRC_DEBUG(ACCELERATOR, "r->" ALGOBATFMT "\n", ALGOBATPAR(r) );
	STRMPdecref(strmps, false);
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

	if ((r = b->tstrimps) == NULL &&
		STRMPbuildHeader(b, s, hpairs)) { /* Find the header pairs, put
						 the result in hpairs */
		sz = 8 + STRIMP_HEADER_SIZE; /* add 8-bytes for the descriptor and
						the pair sizes */
		for (i = 0; i < STRIMP_HEADER_SIZE; i++) {
			sz += hpairs[i].psize;
		}

		nme = GDKinmemory(b->theap->farmid) ? ":memory:"
			: BBP_physical(b->batCacheid);
		/* Allocate the strimps heap */
		if ((r = GDKzalloc(sizeof(Strimps))) == NULL ||
		    (r->strimps.farmid =
		     BBPselectfarm(b->batRole, b->ttype, strimpheap)) < 0 ||
		    strconcat_len(r->strimps.filename, sizeof(r->strimps.filename),
				  nme, ".tstrimps",
				  NULL) >= sizeof(r->strimps.filename) ||
		    HEAPalloc(&r->strimps, BATcount(b) * sizeof(uint64_t) + sz,
			      sizeof(uint8_t), 0) != GDK_SUCCEED) {
			GDKfree(r);
			return NULL;
		}

		descriptor = STRIMP_VERSION | ((uint64_t)STRIMP_HEADER_SIZE) << 8 |
			((uint64_t)sz) << 16;

		((uint64_t *)r->strimps.base)[0] = descriptor;
		r->sizes_base = h1 = (uint8_t *)r->strimps.base + 8;
		r->pairs_base = h2 = (uint8_t *)h1 + STRIMP_HEADER_SIZE;

		for (i = 0; i < STRIMP_HEADER_SIZE; i++) {
			uint8_t psize = hpairs[i].psize;
			h1[i] = psize;
			memcpy(h2, hpairs[i].pbytes, psize);
			h2 += psize;
		}
		r->bitstrings_base = h2;
		r->strimps.free = sz;
		r->rec_cnt = 0;
		ATOMIC_INIT(&r->strimps.refs, 1);
	}
	return r;
}

bool
BAThasstrimps(BAT *b)
{
	BAT *pb;
	if (VIEWtparent(b)) {
		pb = BBP_cache(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	return BATcheckstrimps(pb);

}

gdk_return
STRMPcreate(BAT *b, BAT *s)
{
	lng t0 = 0;
	BAT *pb;

	MT_thread_setalgorithm("create strimp index");
	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();
	if (ATOMstorage(b->ttype) != TYPE_str) {
		GDKerror("Cannot create strimps index for non string bats\n");
		return GDK_FAIL;
	}

	if (VIEWtparent(b)) {
		pb = BBP_cache(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	if (BATcheckstrimps(pb)) {
		return GDK_SUCCEED;
	}

	if (pb->tstrimps == NULL) {
		MT_lock_set(&pb->batIdxLock);
		if (pb->tstrimps == NULL) {
			Strimps *r;
			BATiter bi;
			BUN i, ncand;
			oid x;
			struct canditer ci;
			uint64_t *dh;

			if ((r = STRMPcreateStrimpHeap(pb, s)) == NULL) {
				MT_lock_unset(&pb->batIdxLock);
				return GDK_FAIL;
			}
			dh = (uint64_t *)r->bitstrings_base;

			/* Compute bitstrings */
			ncand = canditer_init(&ci, pb, NULL);
			bi = bat_iterator(pb);
			for (i = 0; i < ncand; i++) {
				x = canditer_next(&ci);
				const char *cs = BUNtvar(bi, x);
				if (!strNil(cs))
					*dh++ = STRMPmakebitstring(cs, r);
				else
					*dh++ = 0;
			}
			bat_iterator_end(&bi);

			r->strimps.free += ncand*sizeof(uint64_t);
			pb->tstrimps = r;
			persistStrimp(pb);
		}
		MT_lock_unset(&pb->batIdxLock);
	}
	TRC_DEBUG(ACCELERATOR, "strimp creation took " LLFMT " usec\n", GDKusec()-t0);
	return GDK_SUCCEED;
}


void
STRMPdecref(Strimps *strimps, bool remove)
{
	TRC_DEBUG(ACCELERATOR, "Decrement ref count of %s to " BUNFMT "\n",
		  strimps->strimps.filename, (BUN) (ATOMIC_GET(&strimps->strimps.refs) - 1));
	strimps->strimps.remove |= remove;
	if (ATOMIC_DEC(&strimps->strimps.refs) == 0) {
		ATOMIC_DESTROY(&strimps->strimps.refs);
		HEAPfree(&strimps->strimps, strimps->strimps.remove);
		GDKfree(strimps);
	}

}

void
STRMPincref(Strimps *strimps)
{
	TRC_DEBUG(ACCELERATOR, "Increment ref count of %s to " BUNFMT "\n",
		  strimps->strimps.filename, (BUN) (ATOMIC_GET(&strimps->strimps.refs) + 1));
	(void)ATOMIC_INC(&strimps->strimps.refs);

}

void
STRMPdestroy(BAT *b)
{
	if (b && b->tstrimps) {
		MT_lock_set(&b->batIdxLock);
		if (b->tstrimps == (Strimps *)1) {
			b->tstrimps = NULL;
			GDKunlink(BBPselectfarm(b->batRole, b->ttype, strimpheap),
				  BATDIR,
				  BBP_physical(b->batCacheid),
				  "tstrimps");
		} else if (b->tstrimps != NULL) {
			STRMPdecref(b->tstrimps, b->tstrimps->strimps.parentid == b->batCacheid);
			b->tstrimps = NULL;
		}
		MT_lock_unset(&b->batIdxLock);
	}
}

void
STRMPfree(BAT *b)
{
	if (b && b->tstrimps) {
		Strimps *s;
		MT_lock_set(&b->batIdxLock);
		if ((s = b->tstrimps) != NULL && s != (Strimps *)1) {
			if (GDKinmemory(s->strimps.farmid)) {
				b->tstrimps = NULL;
				STRMPdecref(s, s->strimps.parentid == b->batCacheid);
			}
			else {
				if (s->strimps.parentid == b->batCacheid)
					b->tstrimps = (Strimps *)1;
				else
					b->tstrimps = NULL;
				STRMPdecref(s, false);
			}

		}
		MT_lock_unset(&b->batIdxLock);
	}
}

/* Parallel creation. does not wok*/
#if 0
/* Creates the heap for a string imprint. Returns NULL on failure. This
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

	MT_lock_set(&b->batIdxLock);
	/* Make sure no other thread got here first */
        if ((r = b->tstrimps) == NULL) {
		if (STRMPbuildHeader(b, s, hpairs)) { /* Find the header pairs, put
							 the result in hpairs */
			sz = 8 + STRIMP_HEADER_SIZE; /* add 8-bytes for the descriptor and
							the pair sizes */
			for (i = 0; i < STRIMP_HEADER_SIZE; i++) {
				sz += hpairs[i].psize;
			}

			nme = GDKinmemory(b->theap->farmid) ? ":memory:"
				: BBP_physical(b->batCacheid);
			/* Allocate the strimps heap */
			if ((r = GDKzalloc(sizeof(Strimps))) == NULL ||
			    (r->strimps.farmid =
			     BBPselectfarm(b->batRole, b->ttype, strimpheap)) < 0 ||
			    strconcat_len(r->strimps.filename, sizeof(r->strimps.filename),
					  nme, ".tstrimps",
					  NULL) >= sizeof(r->strimps.filename) ||
			    HEAPalloc(&r->strimps, BATcount(b) * sizeof(uint64_t) + sz,
				      sizeof(uint8_t), 0) != GDK_SUCCEED) {
				GDKfree(r);
				MT_lock_unset(&b->batIdxLock);
				return NULL;
			}

			descriptor = STRIMP_VERSION | ((uint64_t)STRIMP_HEADER_SIZE) << 8 |
				((uint64_t)sz) << 16;

			((uint64_t *)r->strimps.base)[0] = descriptor;
			r->sizes_base = h1 = (uint8_t *)r->strimps.base + 8;
			r->pairs_base = h2 = (uint8_t *)h1 + STRIMP_HEADER_SIZE;

			for (i = 0; i < STRIMP_HEADER_SIZE; i++) {
				uint8_t psize = hpairs[i].psize;
				h1[i] = psize;
				memcpy(h2, hpairs[i].pbytes, psize);
				h2 += psize;
			}
			r->bitstrings_base = h2;
			r->strimps.free = sz;

			// incref
			b->tstrimps = r;
		}
	}
	else
		//incref
	MT_lock_unset(&b->batIdxLock);
	return r;
}

/* This macro takes a bat and checks if the strimp construction has been
 * completed. It is completed when the strimp pointer is not null and it
 * is either 1 (i.e. it exists on disk) or the number of bitstrings
 * computed is the same as the number of elements in the BAT.
 */
#define STRIMP_COMPLETE(b)						\
	b->tstrimps != NULL &&						\
		((b->tstrimps->strimps.free - ((char *)b->tstrimps->bitstrings_base - b->tstrimps->strimps.base)) == b->batCount*sizeof(uint64_t))



/* Create */
gdk_return
STRMPcreate(BAT *b, BAT *s)
{
	lng t0 = 0;
	BATiter bi;
	BUN i, ncand;
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
	dh = (uint64_t *)h->bitstrings_base + b->hseqbase;

	ncand = canditer_init(&ci, b, s);

	bi = bat_iterator(b);
	for (i = 0; i < ncand; i++) {
		x = canditer_next(&ci) - b->hseqbase;
		const char *cs = BUNtvar(bi, x);
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
	if (STRIMP_COMPLETE(pb)) {
		persistStrimp(pb);
	}

	TRC_DEBUG(ACCELERATOR, "strimp creation took " LLFMT " usec\n", GDKusec()-t0);
	return GDK_SUCCEED;
}

/* Update the strimp by computing a bitstring and adding it to the heap.
   This will probably be useful later when strimps take updates into
   account. */
gdk_return
STRMPappendBitstring(BAT *b, const char *s)
{
	lng t0 = 0;
	BAT *pb;
	uint64_t *dh;
	Strimps *strmp;
	const float extend_factor = 1.5;

	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();
	if (ATOMstorage(b->ttype) != TYPE_str) {
		GDKerror("Cannot manipulate strimps index for non string bats\n");
		return GDK_FAIL;
	}

	if (VIEWtparent(b)) {
		pb = BBP_cache(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	if (!BATcheckstrimps(pb)) {
		GDKerror("Strimp missing, cannot append value\n");
		return GDK_FAIL;
	}
	MT_lock_set(&pb->batIdxLock);
	strmp = pb->tstrimps;
	/* Extend heap if there is not enough space */
	if (strmp->strimps.free >= strmp->strimps.size + sizeof(uint64_t)) {
		size_t sizes_offset = (char *)strmp->sizes_base - strmp->strimps.base;
		size_t pairs_offset = (char *)strmp->pairs_base - strmp->strimps.base;
		size_t bitstrings_offset = (char *)strmp->bitstrings_base - strmp->strimps.base;
		if (HEAPextend(&(strmp->strimps), (size_t)(extend_factor*BATcount(pb)*sizeof(uint64_t)), false) != GDK_SUCCEED) {
			MT_lock_unset(&pb->batIdxLock);
			GDKerror("Cannot extend heap\n");
			return GDK_FAIL;
		}
		strmp->sizes_base = (uint8_t *)strmp->strimps.base + sizes_offset;
		strmp->pairs_base = (uint8_t *)strmp->strimps.base + pairs_offset;
		strmp->bitstrings_base = strmp->strimps.base + bitstrings_offset;
	}
	dh = (uint64_t *)strmp->strimps.base + pb->tstrimps->strimps.free;
	*dh = STRMPmakebitstring(s, strmp);
	strmp->strimps.free += sizeof(uint64_t);

	strmp->rec_cnt++;
	MT_lock_unset(&pb->batIdxLock);

	TRC_DEBUG(ACCELERATOR, "appending to strimp took " LLFMT " usec\n", GDKusec()-t0);
	return GDK_SUCCEED;
}
#endif
