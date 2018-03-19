/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* This macro initializes the variables start, end, cnt, cand, and
 * candend that were passed as arguments from the input parameters b
 * and s (the candidate list).  Start and end are the start and end
 * BUNs of b that need to be considered.  They are relative to the
 * start of the heap.  Cand and candend point into the candidate list,
 * if present.  Note that if the tail of the candidate list is dense,
 * cand and candend are set to NULL and start and end are adjusted
 * instead. */
#define CANDINIT(b, s, start, end, cnt, cand, candend)			\
	do {								\
		start = 0;						\
		end = cnt = BATcount(b);				\
		cand = candend = NULL;					\
		if (s) {						\
			assert(BATttype(s) == TYPE_oid);		\
			if (BATcount(s) == 0) {				\
				start = end = 0;			\
			} else {					\
				if (BATtdense(s)) {			\
					start = (s)->tseqbase;		\
					end = start + BATcount(s);	\
				} else {				\
					oid x = (b)->hseqbase;		\
					start = SORTfndfirst((s), &x);	\
					x += BATcount(b);		\
					end = SORTfndfirst((s), &x);	\
					cand = (const oid *) Tloc((s), start); \
					candend = (const oid *) Tloc((s), end); \
					if (cand == candend) {		\
						start = end = 0;	\
					} else {			\
						assert(cand < candend);	\
						end = cand[end-start-1] + 1; \
						start = *cand;		\
					}				\
				}					\
				assert(start <= end);			\
				if (start <= (b)->hseqbase)		\
					start = 0;			\
				else if (start >= (b)->hseqbase + cnt)	\
					start = cnt;			\
				else					\
					start -= (b)->hseqbase;		\
				if (end >= (b)->hseqbase + cnt)		\
					end = cnt;			\
				else if (end <= (b)->hseqbase)		\
					end = 0;			\
				else					\
					end -= (b)->hseqbase;		\
			}						\
		}							\
	} while (0)
