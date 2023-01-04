/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. Nes
 * @* Delta management
 * The basis for transaction management is to keep track of elements
 * inserted, deleted, and replaced.  This information is stored within
 * the BAT structure using three delta markers.  Inserted denotes the
 * first added BUN since the last commit. Deleted points to the BUNs
 * removed.  The deletion list is terminated at @%first@, where space
 * is reserved for swapping BUNs upon deletion. Initialization of the
 * BAT is extended as follows:
 */

/*
 * Impact on hashing and indexing.  The hash structure is maintained
 * for all elements to be deleted ?.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/*
 * batcommit really forgets the atoms guarded for an undo; we just
 * need to free their heap space (only if necessary).
 */
void
BATcommit(BAT *b, BUN size)
{
	/* called with theaplock held (or otherwise save from concurrent use) */
	if (b == NULL)
		return;
	assert(size <= BATcount(b) || size == BUN_NONE);
	TRC_DEBUG(DELTA, "BATcommit1 %s free %zu ins " BUNFMT " base %p\n",
		  BATgetId(b), b->theap->free, b->batInserted, b->theap->base);
	b->batInserted = size < BATcount(b) ? size : BATcount(b);
	TRC_DEBUG(DELTA, "BATcommit2 %s free %zu ins " BUNFMT " base %p\n",
		  BATgetId(b), b->theap->free, b->batInserted, b->theap->base);
}

/*
 * BATfakeCommit() flushed the delta info, but leaves the BAT marked
 * clean.
 */
void
BATfakeCommit(BAT *b)
{
	if (b) {
		BATcommit(b, BUN_NONE);
		b->theap->dirty = false;
		if (b->tvheap)
			b->tvheap->dirty = false;
	}
}
