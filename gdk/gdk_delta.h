/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _GDK_DELTA_H_
#define _GDK_DELTA_H_

/*
 * We make sure here that the BUNs section of a BAT at least starts 4
 * bytes from the BUN start.  This ensures that the first data item of
 * e.g. a BAT[void,bit] is (at least) integer aligned.  This optimizes
 * processing on such BATs (DDBENCH).
 */
static inline void
DELTAinit(BAT *b)
{
	BATsetcount(b, 0);
	b->theap->free = 0;
	b->batInserted = 0;
	b->tshift = ATOMelmshift(Tsize(b));
	TRC_DEBUG(DELTA,
		  "%s free %zu ins " BUNFMT " base %p\n",
		  BBP_logical(b->batCacheid),
		  b->theap->free,
		  b->batInserted,
		  b->theap->base);
}

/*
 * Upon saving a BAT, we should convert the delta marker BUN pointers
 * into indexes and convert them back into pointers upon reload.
 *
 * The BATdirty(b) tells you whether a BAT's main memory
 * representation differs from its saved image on stable storage. But
 * *not* whether it has changed since last transaction commit (it can
 * be storage-clean, but transaction-dirty). For this we have
 * DELTAdirty(b).
 */
#define DELTAdirty(b)	((b)->batInserted < BUNlast(b))

#endif /* _GDK_DELTA_H_ */
