/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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
		(P1)->H->heap.free = 0;					\
		(P1)->T->heap.free = 0;					\
		(P1)->batDeleted = (P1)->batInserted = (P1)->batFirst = 0; \
		(P1)->H->shift = ATOMelmshift(Hsize(P1));		\
		(P1)->T->shift = ATOMelmshift(Tsize(P1));		\
		DELTADEBUG printf(					\
			"#DELTAinit %s free " SZFMT "," SZFMT " ins " BUNFMT \
			" del " BUNFMT " first " BUNFMT " base " PTRFMT "," \
			PTRFMT "\n",					\
			BATgetId(P1),					\
			(P1)->H->heap.free,				\
			(P1)->T->heap.free,				\
			(P1)->batInserted,				\
			(P1)->batDeleted,				\
			(P1)->batFirst,					\
			PTRFMTCAST (P1)->H->heap.base,			\
			PTRFMTCAST (P1)->T->heap.base);			\
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
#define DELTAdirty(b)	(((b)->batDeleted != BUNfirst(b)) ||\
	((b)->batInserted < BUNlast(b)))

#endif /* _GDK_DELTA_H_ */
