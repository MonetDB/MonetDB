/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _GDK_DELTA_H_
#define _GDK_DELTA_H_

/*
 * We make sure here that the BUNs section of a BAT at least starts 4
 * bytes from the BUN start.  This ensures that the first data item of
 * e.g. a BAT[void,bit] is (at least) integer aligned.  This optimizes
 * processing on such BATs (DDBENCH).
 */
#define DELTAinit(P1)							\
	do {								\
		BATsetcount((P1), 0);					\
		(P1)->theap.free = 0;					\
		(P1)->batInserted = 0;					\
		(P1)->tshift = ATOMelmshift(Tsize(P1));			\
		DELTADEBUG fprintf(stderr,				\
			"#DELTAinit %s free " SZFMT " ins " BUNFMT	\
			" base " PTRFMT "\n",				\
			BATgetId(P1),					\
			(P1)->theap.free,				\
			(P1)->batInserted,				\
			PTRFMTCAST (P1)->theap.base);			\
	} while (0)
/*
 * Upon saving a BAT, we should convert the delta marker BUN pointers
 * into indexes and convert them back into pointers upon reload.
 *
 * The b->batDirty field tells you whether a BATs main memory
 * representation differs from its saved image on stable storage. But
 * *not* whether it has changed since last transaction commit (it can
 * be storage-clean, but transaction-dirty). For this we have
 * @%DELTAdirty(b)@.
 */
#define DELTAdirty(b)	((b)->batInserted < BUNlast(b))

#endif /* _GDK_DELTA_H_ */
