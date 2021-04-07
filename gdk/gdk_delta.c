/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
	if (b == NULL)
		return;
	TRC_DEBUG(DELTA, "BATcommit1 %s free %zu ins " BUNFMT " base %p\n",
		  BATgetId(b), b->theap->free, b->batInserted, b->theap->base);
	if (!BATdirty(b)) {
		b->batDirtyflushed = false;
	}
	if (DELTAdirty(b)) {
		b->batDirtydesc = true;
	}
	b->batInserted = size < BUNlast(b) ? size : BUNlast(b);
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
		b->batDirtydesc = b->theap->dirty = false;
		if (b->tvheap)
			b->tvheap->dirty = false;
	}
}

/*
 * The routine @%BATundo@ restores the BAT to the previous commit
 * point.  The inserted elements are removed from the accelerators,
 * deleted from the heap. The guarded elements from uncommitted
 * deletes are inserted into the accelerators.
 */
void
BATundo(BAT *b)
{
	BATiter bi = bat_iterator(b);
	BUN p, bunlast, bunfirst;

	if (b == NULL)
		return;
	assert(b->theap->parentid == b->batCacheid);
	TRC_DEBUG(DELTA, "BATundo: %s \n", BATgetId(b));
	if (b->batDirtyflushed) {
		b->batDirtydesc = b->theap->dirty = true;
	} else {
		b->batDirtydesc = b->theap->dirty = false;
		if (b->tvheap)
			b->tvheap->dirty = false;
	}
	bunfirst = b->batInserted;
	bunlast = BUNlast(b) - 1;
	if (bunlast >= b->batInserted) {
		BUN i = bunfirst;
		gdk_return (*tunfix) (const void *) = BATatoms[b->ttype].atomUnfix;
		void (*tatmdel) (Heap *, var_t *) = BATatoms[b->ttype].atomDel;

		if (b->thash)
			HASHdestroy(b);
		if (tunfix || tatmdel) {
			for (p = bunfirst; p <= bunlast; p++, i++) {
				if (tunfix)
					(void) (*tunfix) (BUNtail(bi, p));
				if (tatmdel)
					(*tatmdel) (b->tvheap, (var_t *) BUNtloc(bi, p));
			}
		}
	}
	b->theap->free = tailsize(b, b->batInserted);

	BATsetcount(b, b->batInserted);
}
