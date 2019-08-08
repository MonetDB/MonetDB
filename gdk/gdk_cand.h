/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#ifndef _GDK_CAND_H_
#define _GDK_CAND_H_

/* This macro initializes the variables start, end, cnt, cand, and
 * candend that were passed as arguments from the input parameters b
 * and s (the candidate list).  Start and end are the start and end
 * BUNs of b that need to be considered.  They are relative to the
 * start of the heap.  Cand and candend point into the candidate list,
 * if present.  Note that if the candidate list is dense, cand and
 * candend are set to NULL and start and end are adjusted instead. */
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
					start = SORTfndfirst((s), &(b)->hseqbase); \
					end = SORTfndfirst((s), &(oid){(b)->hseqbase+BATcount(b)}); \
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

struct canditer {
	const oid *oids;	/* candidate or exceptions for non-dense */
	BAT *s;			/* candidate BAT the iterator is based on */
	oid seq;		/* first candidate */
	oid add;		/* value to add because of exceptions seen */
	BUN noids;		/* number of values in .oids */
	BUN ncand;		/* number of candidates */
	BUN next;		/* next BUN to return value for */
	BUN offset;		/* how much of candidate list BAT we skipped */
	enum {
		cand_dense,	/* simple dense BAT, i.e. no look ups */
		cand_materialized, /* simple materialized OID list */
		cand_except,	/* list of exceptions in vheap */
	} tpe;
};

static inline oid
canditer_next(struct canditer *ci)
{
	if (ci->next == ci->ncand)
		return oid_nil;
	switch (ci->tpe) {
	case cand_dense:
		return ci->seq + ci->next++;
	case cand_materialized:
		assert(ci->next < ci->noids);
		return ci->oids[ci->next++];
	case cand_except:
		/* work around compiler error: control reaches end of
		 * non-void function */
		break;
	}
	oid o = ci->seq + ci->add + ci->next++;
	while (ci->add < ci->noids && o == ci->oids[ci->add]) {
		ci->add++;
		o++;
	}
	return o;
}

gdk_export BUN canditer_init(struct canditer *ci, BAT *b, BAT *s);
gdk_export oid canditer_peek(struct canditer *ci);
gdk_export oid canditer_last(struct canditer *ci);
gdk_export oid canditer_prev(struct canditer *ci);
gdk_export oid canditer_peekprev(struct canditer *ci);
gdk_export oid canditer_idx(struct canditer *ci, BUN p);
gdk_export void canditer_setidx(struct canditer *ci, BUN p);
gdk_export void canditer_reset(struct canditer *ci);
gdk_export BUN canditer_search(struct canditer *ci, oid o, bool next);
gdk_export BAT *canditer_slice(struct canditer *ci, BUN lo, BUN hi);
gdk_export BAT *canditer_slice2(struct canditer *ci, BUN lo1, BUN hi1, BUN lo2, BUN hi2);

#endif	/* _GDK_CAND_H_ */
