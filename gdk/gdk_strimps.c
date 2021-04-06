/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */


/* A string imprint is an index that can be used as a prefilter in LIKE
 * queries. It has 2 components:
 *
 * - a header of 64 string element pairs (bytes in the current
 *   implementation but maybe unicode chars might make more sense).
 *
 * - a 64 bit mask for each item in the BAT that encodes the presence or
 *   absence of each element of the header in the specific item.
 *
 * A string imprint is stored in a new Heap in the BAT, aligned in 8
 * byte (64 bit) words.
 *
 * The first 64 bit word describes how the header of the strimp is
 * encoded. The most significant byte (v in the schematic below) is the
 * version number. The second (np) is the number of pairs in the
 * header. The third (b/p) is the number of bytes per pair if each pair
 * is encoded using a constant number of bytes or 0 if it is utf-8. The
 * next 2 bytes (hs) is the size of the header in bytes. The last 3
 * bytes needed to align to the 8 byte boundary should be zero, and are
 * reserved for future use.
 *
 * In the current implementation we use 64 byte pairs for the header, so
 *
 * np  == 64
 * b/p == 2
 * hs  == 128
 *
 * The actual header follows. If it ends before an 8 byte boundary it
 * is padded with zeros.
 *
 * |  v   |  np   |  b/p |      hs      |     reserved         |  8bytes
 * |                                                           |        ---
 *                         Strimp Header                                 |
 * |                                                           |  hs bytes + padding
 * |                                                           |         |
 * |                                                           |        ---
 * The bitmasks for each string in the BAT follow after this.
 *
 * Strimp creation goes as follows:
 *
 * - Construct a histogram of the element (byte or character) pairs for
 *   all the strings in the BAT.
 *
 * - Take the 64 most frequent pairs as the Strimp Header.
 *
 * - For each string in the bat construct a 64 bit mask that encodes the
 *   presence or absence of each member of the header in the string.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* This counts how many unicode codepoints the given string
 * contains.
 */
#if 0
static size_t
GDKstrimp_strlen(const uint8_t *s)
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
#endif

/* Given a BAT return the number of digrams in it. The observation is
 * that the number of digrams is the number of characters - 1:
 *
 * 1 digram starting at character 1
 * 1 digram starting at character 2
 * [...]
 * 1 digram starting at character n - 1
 */
gdk_return
GDKstrimp_ndigrams(BAT *b, size_t *n)
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
                // *n += GDKstrimp_strlen(s) - 1;
		*n += strlen(s) - 1;
		// TRC_DEBUG(ALGO, "s["LLFMT"]=%s\n", i, s);
	}

	// TRC_DEBUG(ALGO, LLFMT "usec\n", GDKusec() - t0);
	// GDKtracer_flush_buffer();

	return GDK_SUCCEED;
}

/* The isIgnored is a bit suspect in terms of unicode. There are
 * non-ASCII codepoints that are considered spaces, for example the
 * codepoints in the range U+2000-U+200f.
 */
#define isIgnored(x) (isspace((x)) || isdigit((x)) || ispunct((x)))
#define isNotIgnored(x) (!isIgnored(x))
#define pairToIndex(b1, b2) (DataPair)(((uint8_t)b1)<<8 | ((uint8_t)b2))
#define indexToPair1(idx) (idx & 0xff00) >> 8
#define indexToPair2(idx) (idx & 0xff)
#define swp(_a, _i, _j, TPE)			\
	do {					\
		TPE _t = ((TPE *)_a)[_i];	\
		((TPE *) _a)[_i] = ((TPE *) _a)[_j];	\
		((TPE *) _a)[_j] = _t;			\
	} while(0)

/* Construct a histogram of pairs of bytes in the input BAT.
 *
 * Return the histogram in hist and the number of non-zero bins in
 * count.
 */
gdk_return
GDKstrimp_make_histogram(BAT *b, uint64_t *hist, size_t hist_size, size_t *nbins)
{
	lng t0=0;
	size_t hi;
	BUN i;
	BATiter bi;
	char *ptr, *s;
	/* uint64_t cur_min = 0; */

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
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

	TRC_DEBUG(ALGO, LLFMT " usec\n", GDKusec() - t0);
	GDKtracer_flush_buffer();
	return GDK_SUCCEED;
}

/* Given a histogram find the indices of the 64 largest counts.
 *
 * We make one scan of histogram and every time we find a count that is
 * greater than the current minimum of the 64, we bubble it up in the
 * header until we find a count that is greater. We carry the index in
 * the histogram because this is the information we are actually
 * interested in keeping.
 *
 * At the end of this process we have the indices of 64 largest counts
 * in the histogram. This process is O(n) in time since we are doing
 * constant work (at most 63 comparisons and swaps) for each item in the
 * histogram and as such is (theoretically) more efficient than sorting
 * (O(nlog n))and taking the 64 largest elements. This depends on the
 * size of the histogram n. For some small n sorting might be more
 * efficient, but for such inputs the difference should not be
 * noticeable.
 *
 * In the current implementation each index is a DataPair value that is
 * constructed by pairToIndex from 2 consecutive bytes in the input.
 */
static StrimpHeader *
make_header(StrimpHeader *h, uint64_t* hist, size_t hist_size)
{
	lng t0 = 0;
	size_t i;
	uint64_t max_counts[STRIMP_HEADER_SIZE] = {0};
	const size_t cmin_max = STRIMP_HEADER_SIZE - 1;
	size_t hidx;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	for(i = 0; i < STRIMP_HEADER_SIZE; i++)
		h->bytepairs[i] = 0;

	for(i = 0; i < hist_size; i++) {
		if (max_counts[cmin_max] < hist[i]) {
			max_counts[cmin_max] = hist[i];
			h->bytepairs[cmin_max] = i;
                        for(hidx = cmin_max; hidx > 0 && max_counts[hidx] > max_counts[hidx-1]; hidx--) {
				swp(max_counts, hidx, hidx-1, uint64_t);
				swp(h->bytepairs, hidx, hidx-1, DataPair);
			}
		}
	}

	for(i = 0; i < STRIMP_HEADER_SIZE; i++) {
		TRC_DEBUG(ALGO, "%u %u: %lu", indexToPair1(h->bytepairs[i]), indexToPair2(h->bytepairs[i]), max_counts[i]);
	}

	TRC_DEBUG(ALGO, LLFMT " usec\n", GDKusec() - t0);

	return h;
}

static StrimpHeader *
create_header(BAT *b)
{
	uint64_t hist[STRIMP_HISTSIZE] = {0};
	size_t nbins = 0;
	StrimpHeader *header;
	if ((header = (StrimpHeader*)GDKmalloc(sizeof(StrimpHeader))) == NULL)
		return NULL;

	if(GDKstrimp_make_histogram(b, hist, STRIMP_HISTSIZE, &nbins) != GDK_SUCCEED) {
		GDKfree(header);
		return NULL;
	}

	make_header(header, hist, STRIMP_HISTSIZE);

	return header;
}


/* Given a strimp h and a DataPair p, return the index i for which
 *
 * h[i] == p
 *
 * Returns 0 if p is not in h.
 *
 * TODO: Should this be inlined somehow? (probably yes)
 */
static uint8_t
lookup_index(StrimpHeader *h, DataPair n)
{
	size_t i;
	for(i = 0; i < STRIMP_HEADER_SIZE; i++)
		if(h->bytepairs[i] == n)
			return i;

	return 0;
}


/* Given a strimp header and a string compute the bitstring of which
 * digrams(byte pairs) are present in the string. The strimp header is a
 * map from digram(byte pair) to index in the strimp.
 *
 * This should probably be inlined.
 */
static uint64_t
GDKstrimp_make_bitstring(const str s, StrimpHeader *h)
{
	uint64_t ret = 0;
	uint8_t pair_idx;
	char *it;

	for(it = s; *it != 0 && *(it+1) != 0; it++) {
		pair_idx = lookup_index(h, pairToIndex(*it, *(it+1)));
		ret |= 0x1 << pair_idx;
	}

	return ret;
}

/* Create the heap for a string imprint. Returns NULL on failure. */
static Heap *
createStrimpheap(BAT *b, StrimpHeader *h)
{
	Heap *r = NULL;
	uint64_t *d;
	size_t i,j;
	const char *nme;

	nme = GDKinmemory(b->theap.farmid) ? ":memory:" : BBP_physical(b->batCacheid);
	if ((r = GDKzalloc(sizeof(Heap))) == NULL ||
	    (r->farmid = BBPselectfarm(b->batRole, b->ttype, strimpheap)) < 0 ||
	    strconcat_len(r->filename, sizeof(r->filename),
			  nme, ".strimp", NULL) >= sizeof(r->filename) ||
	    HEAPalloc(r, BATcount(b) + STRIMP_OFFSET, sizeof(uint64_t)) != GDK_SUCCEED)	{
		GDKfree(r);
		return NULL;
	}
	r->free = STRIMP_OFFSET * sizeof(uint64_t);

	d = (uint64_t *)r->base;
	/* This loop assumes that we are working with byte pairs
	 * (i.e. the type of the header is uint16_t). TODO: generalize.
	 */
	for(i = 0; i < STRIMP_HEADER_SIZE; i += 4) {
		*d = 0;
		for(j = 0; j < 4; j++) {
			*d <<= 16;
			*d |= h->bytepairs[i + j];
		}
	}
	return r;
}

/* Create */
gdk_return
GDKstrimp_create_strimp(BAT *b)
{
	lng t0 = 0;
	BATiter bi;
	BUN i;
	str s;
	StrimpHeader *head;
	Heap *h;
	uint64_t *dh;

	assert(b->ttype == TYPE_str);
	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	if ((head = create_header(b)) == NULL) {
		return GDK_FAIL;
	}

	if ((h = createStrimpheap(b, head)) == NULL) {
		GDKfree(head);
		return GDK_FAIL;
	}
	dh = (uint64_t *)h->base + h->free;

	bi = bat_iterator(b);
	for (i = 0; i < b->batCount; i++) {
		s = (str)BUNtvar(bi, i);
		if (!strNil(s))
			*dh++ = GDKstrimp_make_bitstring(s, head);
		else
			*dh++ = 0; /* no pairs in nil values */

	}

	/* After we have computed the strimp, attempt to write it back
	 * to the BAT.
	 */
	MT_lock_set(&b->batIdxLock);
	b->tstrimps = h;
	b->batDirtydesc = true;
	/* persistStrimp(b) */
	MT_lock_unset(&b->batIdxLock);

	TRC_DEBUG(ALGO, "strimp creation took " LLFMT " usec\n", GDKusec()-t0);
	return GDK_SUCCEED;
}
