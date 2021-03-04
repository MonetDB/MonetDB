/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* This counts how many unicode codepoints the given string
 * contains.
 */
/* static size_t */
/* GDKstrimp_strlen(const uint8_t *s) */
/* { */
/* 	size_t ret = 0; */
/* 	size_t i; */
/* 	int m,n; */
/* 	uint8_t c; */

/* 	i = 0; */
/* 	while((c = *(s + i)) != 0) { */
/* 		if (c < 0x80) */
/* 			i++; */
/* 		else { */
/* 			for (n = 0, m=0x40; c & m; n++, m >>= 1) */
/* 				; */
/* 			/\* n is now the number of 10xxxxxx bytes that should */
/* 			   follow. *\/ */
/* 			if (n == 0 || n >= 4) */
/* 				/\* TODO: handle invalid utf-8 *\/ */
/* 				{} */
/* 			i += n+1; */
/* 		} */
/* 		ret++; */
/* 	} */

/* 	return ret; */
/* } */

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
