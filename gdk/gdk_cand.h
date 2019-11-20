/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#ifndef _GDK_CAND_H_
#define _GDK_CAND_H_

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
#define canditer_next_dense(ci)		((ci)->next == (ci)->ncand ? oid_nil : (ci)->seq + (ci)->next++)
#define canditer_next_mater(ci)		((ci)->next == (ci)->ncand ? oid_nil : (ci)->oids[(ci)->next++])
static inline oid
canditer_next_except(struct canditer *ci)
{
	if (ci->next == ci->ncand)
		return oid_nil;
	oid o = ci->seq + ci->add + ci->next++;
	while (ci->add < ci->noids && o == ci->oids[ci->add]) {
		ci->add++;
		o++;
	}
	return o;
}
#define canditer_search_dense(ci, o, next) ((o) < (ci)->seq ? next ? 0 : BUN_NONE : (o) >= (ci)->seq + (ci)->ncand ? next ? (ci)->ncand : BUN_NONE : (o) - (ci)->seq)


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
gdk_export gdk_return BATnegcands( BAT *cands, BAT *odels);

#endif	/* _GDK_CAND_H_ */
