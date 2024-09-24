/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#define RADIX 8			/* one char at a time */
#define NBUCKETS (1 << RADIX)

gdk_return
GDKrsort(void *restrict h, void *restrict t, size_t n, size_t hs, size_t ts, bool reverse, bool nosign)
{
	const size_t niters = (8 * hs + RADIX - 1) / RADIX;
	size_t *counts = GDKmalloc(niters * NBUCKETS * sizeof(size_t));
	size_t pos[NBUCKETS];
	uint8_t *h1 = h;
	uint8_t *h2;
	uint8_t *t1 = NULL;
	uint8_t *t2 = NULL;
	Heap tmph, tmpt;

	if (counts == NULL)
		return GDK_FAIL;

	tmph = tmpt = (Heap) {
		.farmid = 1,
	};

	snprintf(tmph.filename, sizeof(tmph.filename), "%s%crsort%zuh",
		 TEMPDIR_NAME, DIR_SEP, (size_t) MT_getpid());
	if (HEAPalloc(&tmph, n, hs) != GDK_SUCCEED) {
		GDKfree(counts);
		return GDK_FAIL;
	}
	h2 = (uint8_t *) tmph.base;

	if (t) {
		snprintf(tmpt.filename, sizeof(tmpt.filename), "%s%crsort%zut",
			 TEMPDIR_NAME, DIR_SEP, (size_t) MT_getpid());
		if (HEAPalloc(&tmpt, n, ts) != GDK_SUCCEED) {
			GDKfree(counts);
			HEAPfree(&tmph, true);
			return GDK_FAIL;
		}
		t1 = t;
		t2 = (uint8_t *) tmpt.base;
	} else {
		ts = 0;
	}

	memset(counts, 0, niters * NBUCKETS * sizeof(size_t));
	for (size_t i = 0, o = 0; i < n; i++, o += hs) {
		for (size_t j = 0; j < niters; j++) {
#ifdef WORDS_BIGENDIAN
			uint8_t v = h1[o + hs - j - 1];
#else
			uint8_t v = h1[o + j];
#endif
			counts[(j << RADIX) + v]++;
		}
	}

	/* When sorting in ascending order, the negative numbers occupy
	 * the second half of the buckets in the last iteration; when
	 * sorting in descending order, the negative numbers occupy the
	 * first half.  In either case, at the end we need to put the
	 * second half first and the first half after. */
	size_t negpos = 0;
	for (size_t j = 0, b = 0; j < niters; j++, b += NBUCKETS) {
		size_t nb = counts[b] > 0;
		if (reverse) {
			pos[NBUCKETS - 1] = 0;
			for (size_t i = NBUCKETS - 1; i > 0; i--) {
				pos[i - 1] = pos[i] + counts[b + i];
				nb += counts[b + i] > 0;
			}
		} else {
			pos[0] = 0;
			for (size_t i = 1; i < NBUCKETS; i++) {
				pos[i] = pos[i - 1] + counts[b + i - 1];
				nb += counts[b + i] > 0;
			}
		}
		/* we're only interested in the position in the last
		 * iteration */
		negpos = pos[NBUCKETS / 2 - reverse];
		if (nb == 1) {
			/* no need to reshuffle data for this iteration:
			 * everything is in the same bucket */
			continue;
		}
		/* note, this loop changes the pos array */
		for (size_t i = 0, ho = 0, to = 0; i < n; i++, ho += hs, to += ts) {
#ifdef WORDS_BIGENDIAN
			uint8_t v = h1[ho + hs - j - 1];
#else
			uint8_t v = h1[ho + j];
#endif
			if (t)
				memcpy(t2 + ts * pos[v], t1 + to, ts);
			memcpy(h2 + hs * pos[v]++, h1 + ho, hs);
		}
		uint8_t *t = h1;
		h1 = h2;
		h2 = t;
		t = t1;
		t1 = t2;
		t2 = t;
	}
	GDKfree(counts);

	if (h1 != (uint8_t *) h) {
		if (nosign) {
			memcpy(h2, h1, n * hs);
			if (t)
				memcpy(t2, t1, n * ts);
		} else {
			/* copy the negative integers to the start, copy positive after */
			if (negpos < n) {
				memcpy(h2, h1 + hs * negpos, (n - negpos) * hs);
				if (t)
					memcpy(t2, t1 + ts * negpos, (n - negpos) * ts);
			}
			if (negpos > 0) {
				memcpy(h2 + hs * (n - negpos), h1, negpos * hs);
				if (t)
					memcpy(t2 + ts * (n - negpos), t1, negpos * ts);
			}
		}
	} else if (negpos != 0 && negpos != n && !nosign) {
		/* copy the negative integers to the start, copy positive after */
		if (negpos < n) {
			memcpy(h2, h1 + hs * negpos, (n - negpos) * hs);
			if (t)
				memcpy(t2, t1 + ts * negpos, (n - negpos) * ts);
		}
		if (negpos > 0) {
			memcpy(h2 + hs * (n - negpos), h1, negpos * hs);
			if (t)
				memcpy(t2 + ts * (n - negpos), t1, negpos * ts);
		}
		memcpy(h, h2, n * hs);
		if (t)
			memcpy(t, t2, n * ts);
	} /* else, everything is already in the correct place */
	HEAPfree(&tmph, true);
	if (t)
		HEAPfree(&tmpt, true);
	return GDK_SUCCEED;
}
