/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
BATcommit(BAT *b)
{
	if (b == NULL)
		return;
	DELTADEBUG fprintf(stderr, "#BATcommit1 %s free " SZFMT " ins " BUNFMT " base " PTRFMT "\n",
			   BATgetId(b),
			   b->theap.free,
			   b->batInserted,
			   PTRFMTCAST b->theap.base);
	if (!BATdirty(b)) {
		b->batDirtyflushed = 0;
	}
	if (DELTAdirty(b)) {
		b->batDirtydesc = 1;
	}
	b->batInserted = BUNlast(b);
	DELTADEBUG fprintf(stderr, "#BATcommit2 %s free " SZFMT " ins " BUNFMT " base " PTRFMT "\n",
			   BATgetId(b),
			   b->theap.free,
			   b->batInserted,
			   PTRFMTCAST b->theap.base);
}

/*
 * BATfakeCommit() flushed the delta info, but leaves the BAT marked
 * clean.
 */
void
BATfakeCommit(BAT *b)
{
	if (b) {
		BATcommit(b);
		b->batDirty = 0;
		b->batDirtydesc = b->theap.dirty = 0;
		if (b->tvheap)
			b->tvheap->dirty = 0;
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
	DELTADEBUG fprintf(stderr, "#BATundo %s \n", BATgetId(b));
	if (b->batDirtyflushed) {
		b->batDirtydesc = b->theap.dirty = 1;
	} else {
		b->batDirty = 0;
		b->batDirtydesc = b->theap.dirty = 0;
		if (b->tvheap)
			b->tvheap->dirty = 0;
	}
	bunfirst = b->batInserted;
	bunlast = BUNlast(b) - 1;
	if (bunlast >= b->batInserted) {
		BUN i = bunfirst;
		int (*tunfix) (const void *) = BATatoms[b->ttype].atomUnfix;
		void (*tatmdel) (Heap *, var_t *) = BATatoms[b->ttype].atomDel;

		if (tunfix || tatmdel || b->thash) {
			HASHdestroy(b);
			for (p = bunfirst; p <= bunlast; p++, i++) {
				ptr t = BUNtail(bi, p);

				if (tunfix) {
					(*tunfix) (t);
				}
				if (tatmdel) {
					(*tatmdel) (b->tvheap, (var_t *) BUNtloc(bi, p));
				}
			}
		}
	}
	b->theap.free = tailsize(b, b->batInserted);

	BATsetcount(b, b->batInserted);
}
