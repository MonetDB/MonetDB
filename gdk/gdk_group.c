/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_cand.h"

/* how much to extend the extent and histo bats when we run out of space */
#define GROUPBATINCR	8192

/* BATgroup returns three bats that indicate the grouping of the input
 * bat.
 *
 * Grouping means that all equal values are in the same group, and
 * differing values are in different groups.  If specified, the input
 * bat g gives a pre-existing grouping which is then subdivided.  If a
 * candidate list s is specified, groups (both the pre-existing
 * grouping in g and the output grouping) are aligned with the
 * candidate list, else they are aligned with b.
 *
 * The outputs are as follows.
 *
 * The groups bat is aligned with the candidate list s, or the input
 * bat b if there is no candidate list, and the tail has group id's
 * (type oid).
 *
 * The extents and histo bats are indexed by group id.  The tail of
 * extents is the head oid from b of a representative of the group.
 * The tail of histo is of type lng and contains the number of
 * elements from b that are member of the group.  The extents BAT can
 * be used as a candidate list (sorted and unique).
 *
 * The extents and histo bats are optionally created.  The groups bat
 * is always created.  In other words, the groups argument may not be
 * NULL, but the extents and histo arguments may be NULL.
 *
 * There are six different implementations of the grouping code.
 *
 * If it can be trivially determined that all groups are singletons,
 * we can produce the outputs trivially.
 *
 * If all values in b are known to be equal (both sorted and reverse
 * sorted), we produce a single group or copy the input group.
 *
 * If the input bats b and g are sorted (either direction) or g is not
 * specified and b is sorted, or if the subsorted flag is set (only
 * used by BATsort), we only need to compare consecutive values.
 *
 * If the input bat b is sorted, but g is not, we can compare
 * consecutive values in b and need to scan sections of g for equal
 * groups.
 *
 * If a hash table already exists on b, we can make use of it.
 *
 * Otherwise we build a partial hash table on the fly.
 *
 * A decision should be made on the order in which grouping occurs.
 * Let |b| have << different values than |g| then the linked lists
 * gets extremely long, leading to a n^2 algorithm.
 * At the MAL level, the multigroup function would perform the dynamic
 * optimization.
 */

#define GRPnotfound()							\
	do {								\
		/* no equal found: start new group */			\
		if (ngrp == maxgrps) {					\
			/* we need to extend extents and histo bats, */	\
			/* do it at most once */			\
			maxgrps = BATcount(b);				\
			if (extents) {					\
				BATsetcount(en, ngrp);			\
				if (BATextend(en, maxgrps) != GDK_SUCCEED) \
					goto error;			\
				exts = (oid *) Tloc(en, 0);		\
			}						\
			if (histo) {					\
				BATsetcount(hn, ngrp);			\
				if (BATextend(hn, maxgrps) != GDK_SUCCEED) \
					goto error;			\
				cnts = (lng *) Tloc(hn, 0);		\
			}						\
		}							\
		if (extents)						\
			exts[ngrp] = hseqb + p;				\
		if (histo)						\
			cnts[ngrp] = 1;					\
		ngrps[r] = ngrp++;					\
	} while (0)


#define GRP_compare_consecutive_values(INIT_0,INIT_1,COMP,KEEP)		\
	do {								\
		INIT_0;							\
		for (r = 0; r < cnt; r++) {				\
			if (cand) {					\
				p = *cand++ - b->hseqbase;		\
			} else {					\
				p = start++;				\
			}						\
			assert(p < end);				\
			INIT_1;						\
			if (ngrp == 0 || (grps && grps[r] != prev) || COMP) { \
				GRPnotfound();				\
			} else {					\
				ngrps[r] = ngrp - 1;			\
				if (histo)				\
					cnts[ngrp - 1]++;		\
			}						\
			KEEP;						\
			if (grps)					\
				prev = grps[r];				\
		}							\
	} while(0)

#define GRP_compare_consecutive_values_tpe(TYPE)		\
	GRP_compare_consecutive_values(				\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0);	\
			TYPE pw = 0			,	\
	/* INIT_1 */					,	\
	/* COMP   */	w[p] != pw			,	\
	/* KEEP   */	pw = w[p]				\
	)

#define GRP_compare_consecutive_values_any()			\
	GRP_compare_consecutive_values(				\
	/* INIT_0 */	pv = NULL			,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* COMP   */	cmp(v, pv) != 0			,	\
	/* KEEP   */	pv = v					\
	)


#define GRP_subscan_old_groups(INIT_0,INIT_1,COMP,KEEP)			\
	do {								\
		INIT_0;							\
		pgrp[grps[0]] = 0;					\
		j = 0;							\
		for (r = 0; r < cnt; r++) {				\
			if (cand) {					\
				p = *cand++ - b->hseqbase;		\
			} else {					\
				p = start++;				\
			}						\
			assert(p < end);				\
			INIT_1;						\
			if (ngrp != 0 && COMP) {			\
				/* range [j, r) is all same value */	\
				/* i is position where we saw r's */	\
				/* old group last */			\
				i = pgrp[grps[r]];			\
				/* p is new position where we saw this	\
				 * group */				\
				pgrp[grps[r]] = r;			\
				if (j <= i && i < r)	{		\
					/* i is position of equal */	\
					/* value in same old group */	\
					/* as r, so r gets same new */	\
					/* group as i */		\
					oid grp = ngrps[i];		\
					ngrps[r] = grp;			\
					if (histo)			\
						cnts[grp]++;		\
					if (gn->tsorted &&		\
					    grp != ngrp - 1)		\
						gn->tsorted = 0;	\
					/* we found the value/group */	\
					/* combination, go to next */	\
					/* value */			\
					continue;			\
				}					\
			} else {					\
				/* value differs from previous value */	\
				/* (or is the first) */			\
				j = r;					\
				KEEP;					\
				pgrp[grps[r]] = r;			\
			}						\
			/* start a new group */				\
			GRPnotfound();					\
		}							\
	} while(0)

#define GRP_subscan_old_groups_tpe(TYPE)			\
	GRP_subscan_old_groups(					\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0);	\
		    	TYPE pw = 0			,	\
	/* INIT_1 */					,	\
	/* COMP   */	w[p] == pw			,	\
	/* KEEP   */	pw = w[p]				\
	)

#define GRP_subscan_old_groups_any()				\
	GRP_subscan_old_groups(					\
	/* INIT_0 */	pv = NULL			,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* COMP   */	cmp(v, pv) == 0			,	\
	/* KEEP   */	pv = v					\
	)

/* If a hash table exists on b we use it.
 *
 * The algorithm is simple.  We go through b and for each value we
 * follow the hash chain starting at the next element after that value
 * to find one that is equal to the value we're currently looking at.
 * If we found such a value (including the preexisting group if we're
 * refining), we add the value to the same group.  If we reach the end
 * of the chain, we create a new group.
 *
 * If b (the original, that is) is a view on another BAT, and this
 * other BAT has a hash, we use that.  The lo and hi values are the
 * bounds of the parent BAT that we're considering.
 *
 * Note this algorithm depends critically on the fact that our hash
 * chains go from higher to lower BUNs.
 */
#define GRP_use_existing_hash_table(INIT_0,INIT_1,COMP)			\
	do {								\
		INIT_0;							\
		for (r = 0; r < cnt; r++) {				\
			if (cand) {					\
				p = cand[r] - hseqb + lo;		\
			} else {					\
				p = start + r;				\
			}						\
			assert(p < end);				\
			INIT_1;						\
			/* this loop is similar, but not equal, to */	\
			/* HASHloop: the difference is that we only */	\
			/* consider BUNs smaller than the one we're */	\
			/* looking up (p), and that we also consider */	\
			/* the input groups */				\
			if (grps) {					\
				for (hb = HASHgetlink(hs, p);		\
				     hb != HASHnil(hs) && hb >= start;	\
				     hb = HASHgetlink(hs, hb)) {	\
					oid grp;			\
					assert(hb < p);			\
					if (cand) {			\
						q = r;			\
						while (q != 0 && cand[--q] - hseqb > hb - lo) \
							;		\
						if (cand[q] - hseqb != hb - lo)	\
							continue;	\
						if (grps[q] != grps[r])	\
							continue;	\
						grp = ngrps[q];		\
					} else {			\
						if (grps[hb - lo] != grps[r]) \
							continue;	\
						grp = ngrps[hb - lo];	\
					}				\
					if (COMP) {			\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = 0; \
						break;			\
					}				\
				}					\
			} else {					\
				for (hb = HASHgetlink(hs, p);		\
				     hb != HASHnil(hs) && hb >= start;	\
				     hb = HASHgetlink(hs, hb)) {	\
					oid grp;			\
					assert(hb < p);			\
					if (cand) {			\
						q = r;			\
						while (q != 0 && cand[--q] > hb) \
							;		\
						if (cand[q] - hseqb != hb - lo)	\
							continue;	\
						grp = ngrps[q];		\
					} else {			\
						grp = ngrps[hb - lo];	\
					}				\
					if (COMP) {			\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = 0; \
						break;			\
					}				\
				}					\
			}						\
			if (hb == HASHnil(hs) || hb < lo) {		\
				GRPnotfound();				\
			}						\
		}							\
	} while(0)

#define GRP_use_existing_hash_table_tpe(TYPE)			\
	GRP_use_existing_hash_table(				\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0),	\
	/* INIT_1 */					,	\
	/* COMP   */	w[p] == w[hb]				\
	)

#define GRP_use_existing_hash_table_any()			\
	GRP_use_existing_hash_table(				\
	/* INIT_0 */					,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* COMP   */	cmp(v, BUNtail(bi, hb)) == 0		\
	)


#define GRP_create_partial_hash_table(INIT_0,INIT_1,HASH,COMP)		\
	do {								\
		oid grp;						\
		INIT_0;							\
		for (r = 0; r < cnt; r++) {				\
			if (cand) {					\
				p = cand[r] - b->hseqbase;		\
			} else {					\
				p = start + r;				\
			}						\
			assert(p < end);				\
			INIT_1;						\
			prb = HASH;					\
			if (gc) {					\
				for (hb = HASHget(hs, prb);		\
				     hb != HASHnil(hs) && hb >= start;	\
				     hb = HASHgetlink(hs, hb)) {	\
					assert(HASHgetlink(hs, hb) == HASHnil(hs) \
					       || HASHgetlink(hs, hb) < hb); \
					if (cand) {			\
						q = r;			\
						while (q != 0 && cand[--q] - b->hseqbase > hb) \
							;		\
						if (cand[q] - b->hseqbase != hb) \
							continue;	\
						if (grps[q] != grps[r])	{ \
							hb = HASHnil(hs); \
							break;		\
						}			\
						grp = ngrps[q];		\
					} else {			\
						if (grps[hb] != grps[r]) { \
							hb = HASHnil(hs); \
							break;		\
						}			\
						grp = ngrps[hb];	\
					}				\
					if (COMP) {			\
						ngrps[r] = grp; 	\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = 0; \
						break;			\
					}				\
				}					\
			} else if (grps) {				\
				prb = (prb ^ (BUN) grps[r] << bits) & hs->mask; \
				for (hb = HASHget(hs, prb);		\
				     hb != HASHnil(hs) && hb >= start;	\
				     hb = HASHgetlink(hs, hb)) {	\
					if (cand) {			\
						q = r;			\
						while (q != 0 && cand[--q] - b->hseqbase > hb) \
							;		\
						if (cand[q] - b->hseqbase != hb) \
							continue;	\
						if (grps[q] != grps[r])	\
							continue;	\
						grp = ngrps[q];		\
					} else {			\
						if (grps[hb] != grps[r]) \
							continue;	\
						grp = ngrps[hb];	\
					}				\
					if (COMP) {			\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = 0; \
						break;			\
					}				\
				}					\
			} else {					\
				for (hb = HASHget(hs, prb);		\
				     hb != HASHnil(hs) && hb >= start;	\
				     hb = HASHgetlink(hs, hb)) {	\
					if (cand) {			\
						q = r;			\
						while (q != 0 && cand[--q] - b->hseqbase > hb) \
							;		\
						if (cand[q] - b->hseqbase != hb) \
							continue;	\
						grp = ngrps[q];		\
					} else {			\
						grp = ngrps[hb];	\
					}				\
					if (COMP) {			\
						ngrps[r] = grp;	\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = 0; \
						break;			\
					}				\
				}					\
			}						\
			if (hb == HASHnil(hs) || hb < start) {		\
				GRPnotfound();				\
				/* enter new group into hash table */	\
				HASHputlink(hs, p, HASHget(hs, prb));	\
				HASHput(hs, prb, p); 			\
			}						\
		}							\
	} while (0)

#define GRP_create_partial_hash_table_tpe(TYPE)			\
	GRP_create_partial_hash_table(				\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0),	\
	/* INIT_1 */					,	\
	/* HASH   */	hash_##TYPE(hs, &w[p])		,	\
	/* COMP   */	w[p] == w[hb]				\
	)

#define GRP_create_partial_hash_table_any()			\
	GRP_create_partial_hash_table(				\
	/* INIT_0 */					,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* HASH   */	hash_any(hs, v)			,	\
	/* COMP   */	cmp(v, BUNtail(bi, hb)) == 0		\
	)


gdk_return
BATgroup_internal(BAT **groups, BAT **extents, BAT **histo,
		  BAT *b, BAT *s, BAT *g, BAT *e, BAT *h, int subsorted)
{
	BAT *gn = NULL, *en = NULL, *hn = NULL;
	int t;
	int (*cmp)(const void *, const void *);
	const oid *grps = NULL;
	oid *restrict ngrps, ngrp, prev = 0, hseqb = 0;
	oid *restrict exts = NULL;
	lng *restrict cnts = NULL;
	BUN p, q, r;
	const void *v, *pv;
	BATiter bi;
	char *ext = NULL;
	Hash *hs = NULL;
	BUN hb;
	BUN maxgrps;
#ifndef DISABLE_PARENT_HASH
	bat parent;
#endif
	BUN start, end, cnt;
	const oid *restrict cand, *candend;

	if (b == NULL) {
		GDKerror("BATgroup: b must exist\n");
		return GDK_FAIL;
	}
	assert(s == NULL || BATttype(s) == TYPE_oid);
	CANDINIT(b, s, start, end, cnt, cand, candend);
	/* set cnt to number of output rows (and number of input rows
	 * to be considered) */
	cnt = cand ? (BUN) (candend - cand) : end - start;

	/* g is NULL or [oid(dense),oid] and same size as b or s */
	assert(g == NULL || BATttype(g) == TYPE_oid || BATcount(g) == 0);
	assert(g == NULL || BATcount(g) == cnt);
	assert(g == NULL || BATcount(b) == 0 || (s ? g->hseqbase == s->hseqbase : g->hseqbase == b->hseqbase));
	/* e is NULL or [oid(dense),oid] */
	assert(e == NULL || BATttype(e) == TYPE_oid);
	/* h is NULL or [oid(dense),lng] */
	assert(h == NULL || h->ttype == TYPE_lng);
	/* e and h are aligned */
	assert(e == NULL || h == NULL || BATcount(e) == BATcount(h));
	assert(e == NULL || h == NULL || e->hseqbase == h->hseqbase);
	/* we want our output to go somewhere */
	assert(groups != NULL);

	if (cnt == 0) {
		hseqb = 0;
	} else if (cand) {
		assert(s != NULL);
		hseqb = s->hseqbase + cand - (const oid *) Tloc(s, 0);
	} else if (s) {
		assert(BATtdense(s));
		hseqb = s->hseqbase + start - s->tseqbase;
	} else {
		hseqb = b->hseqbase;
	}
	if (b->tkey || cnt <= 1 || (g && (g->tkey || BATtdense(g)))) {
		/* grouping is trivial: 1 element per group */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "s=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "trivial case: 1 element per group\n",
				  BATgetId(b), BATcount(b),
				  s ? BATgetId(s) : "NULL", s ? BATcount(s) : 0,
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
		ngrp = cnt == 0  ? 0 : cand ? s->hseqbase + (cand - (const oid *) Tloc(s, 0)) : s ? s->hseqbase + start - s->tseqbase : b->hseqbase;
		gn = COLnew(hseqb, TYPE_void, BATcount(b), TRANSIENT);
		if (gn == NULL)
			goto error;
		BATsetcount(gn, BATcount(b));
		BATtseqbase(gn, 0);
		*groups = gn;
		if (extents) {
			en = COLnew(0, TYPE_void, BATcount(b), TRANSIENT);
			if (en == NULL)
				goto error;
			BATsetcount(en, cnt);
			BATtseqbase(en, ngrp);
			*extents = en;
		}
		if (histo) {
			lng one = 1;

			hn = BATconstant(0, TYPE_lng, &one, cnt, TRANSIENT);
			if (hn == NULL)
				goto error;
			*histo = hn;
		}
		return GDK_SUCCEED;
	}
	if (BATordered(b) && BATordered_rev(b)) {
		/* all values are equal */
		if (g == NULL || (BATordered(g) && BATordered_rev(g))) {
			/* there's only a single group: 0 */
			ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "s=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
					  "trivial case: single output group\n",
				  BATgetId(b), BATcount(b),
				  s ? BATgetId(s) : "NULL", s ? BATcount(s) : 0,
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
			ngrp = 0;
			gn = BATconstant(hseqb, TYPE_oid, &ngrp, cnt, TRANSIENT);
			if (gn == NULL)
				goto error;
			*groups = gn;
			if (extents) {
				ngrp = gn->hseqbase;
				en = BATconstant(0, TYPE_void, &ngrp, 1, TRANSIENT);
				if (en == NULL)
					goto error;
				BATtseqbase(en, ngrp);
				*extents = en;
			}
			if (histo) {
				lng lcnt = (lng) cnt;

				hn = BATconstant(0, TYPE_lng, &lcnt, 1, TRANSIENT);
				if (hn == NULL)
					goto error;
				*histo = hn;
			}
			return GDK_SUCCEED;
		}
		if ((extents == NULL) == (e == NULL) &&
		    (histo == NULL) == (h == NULL)) {
			/* inherit given grouping; note that if
			 * extents/histo is to be returned, we need
			 * e/h available in order to copy them,
			 * otherwise we will need to calculate them
			 * which we will do using the "normal" case */
			ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "s=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
					  "trivial case: copy input groups\n",
				  BATgetId(b), BATcount(b),
				  s ? BATgetId(s) : "NULL", s ? BATcount(s) : 0,
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
			gn = COLcopy(g, g->ttype, 0, TRANSIENT);
			if (gn == NULL)
				goto error;
			*groups = gn;
			if (extents) {
				en = COLcopy(e, e->ttype, 0, TRANSIENT);
				if (en == NULL)
					goto error;
				*extents = en;
			}
			if (histo) {
				hn = COLcopy(h, h->ttype, 0, TRANSIENT);
				if (hn == NULL)
					goto error;
				*histo = hn;
			}
			return GDK_SUCCEED;
		}
	}
	assert(g == NULL || !BATtdense(g)); /* i.e. g->ttype == TYPE_oid */
	bi = bat_iterator(b);
	cmp = ATOMcompare(b->ttype);
	gn = COLnew(hseqb, TYPE_oid, cnt, TRANSIENT);
	if (gn == NULL)
		goto error;
	ngrps = (oid *) Tloc(gn, 0);
	maxgrps = cnt / 10;
	if (e && maxgrps < BATcount(e))
		maxgrps += BATcount(e);
	if (h && maxgrps < BATcount(h))
		maxgrps += BATcount(h);
	if (maxgrps < GROUPBATINCR)
		maxgrps = GROUPBATINCR;
	if (b->twidth <= 2 &&
	    maxgrps > ((BUN) 1 << (8 << (b->twidth == 2))))
		maxgrps = (BUN) 1 << (8 << (b->twidth == 2));
	if (extents) {
		en = COLnew(0, TYPE_oid, maxgrps, TRANSIENT);
		if (en == NULL)
			goto error;
		exts = (oid *) Tloc(en, 0);
	}
	if (histo) {
		hn = COLnew(0, TYPE_lng, maxgrps, TRANSIENT);
		if (hn == NULL)
			goto error;
		cnts = (lng *) Tloc(hn, 0);
	}
	ngrp = 0;
	BATsetcount(gn, cnt);
	if (g)
		grps = (const oid *) Tloc(g, 0);

	/* figure out if we can use the storage type also for
	 * comparing values */
	t = ATOMbasetype(b->ttype);
	/* for strings we can use the offset instead of the actual
	 * string values if we know that the strings in the string
	 * heap are unique */
	if (t == TYPE_str && GDK_ELIMDOUBLES(b->tvheap)) {
		switch (b->twidth) {
		case 1:
			t = TYPE_bte;
			break;
		case 2:
			t = TYPE_sht;
			break;
		case 4:
			t = TYPE_int;
			break;
#if SIZEOF_VAR_T == 8
		case 8:
			t = TYPE_lng;
			break;
#endif
		default:
			assert(0);
		}
	}

	if (subsorted ||
	    ((BATordered(b) || BATordered_rev(b)) &&
	     (g == NULL || BATordered(g) || BATordered_rev(g)))) {
		/* we only need to compare each entry with the previous */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "s=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "compare consecutive values\n",
				  BATgetId(b), BATcount(b),
				  s ? BATgetId(s) : "NULL", s ? BATcount(s) : 0,
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);

		switch (t) {
		case TYPE_bte:
			GRP_compare_consecutive_values_tpe(bte);
			break;
		case TYPE_sht:
			GRP_compare_consecutive_values_tpe(sht);
			break;
		case TYPE_int:
			GRP_compare_consecutive_values_tpe(int);
			break;
		case TYPE_lng:
			GRP_compare_consecutive_values_tpe(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			GRP_compare_consecutive_values_tpe(hge);
			break;
#endif
		case TYPE_flt:
			GRP_compare_consecutive_values_tpe(flt);
			break;
		case TYPE_dbl:
			GRP_compare_consecutive_values_tpe(dbl);
			break;
		default:
			GRP_compare_consecutive_values_any();
			break;
		}

		gn->tsorted = 1;
		*groups = gn;
	} else if (BATordered(b) || BATordered_rev(b)) {
		BUN i, j;
		BUN *pgrp;

		assert(g);	/* if g == NULL, we used the code above */
		/* for each value, we need to scan all previous equal
		 * values (a consecutive, possibly empty, range) to
		 * see if we can find one in the same old group
		 *
		 * we do this by maintaining for each old group the
		 * last time we saw that group, so if the last time we
		 * saw the old group of the current value is within
		 * this range, we can reuse the new group */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "s=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "subscan old groups\n",
				  BATgetId(b), BATcount(b),
				  s ? BATgetId(s) : "NULL", s ? BATcount(s) : 0,
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
		/* determine how many old groups there are */
		if (e) {
			j = BATcount(e) + (BUN) e->hseqbase;
		} else if (h) {
			j = BATcount(h) + (BUN) h->hseqbase;
		} else {
			oid m = 0;
			for (i = 0, j= BATcount(g); i < j; i++)
				m = MAX(m , grps[i]);
			j = (BUN) m + 1;
		}
		/* array to maintain last time we saw each old group */
		pgrp = GDKmalloc(sizeof(BUN) * j);
		if (pgrp == NULL)
			goto error;
		/* initialize to impossible position */
		memset(pgrp, ~0, sizeof(BUN) * j);

		gn->tsorted = 1; /* be optimistic */

		switch (t) {
		case TYPE_bte:
			GRP_subscan_old_groups_tpe(bte);
			break;
		case TYPE_sht:
			GRP_subscan_old_groups_tpe(sht);
			break;
		case TYPE_int:
			GRP_subscan_old_groups_tpe(int);
			break;
		case TYPE_lng:
			GRP_subscan_old_groups_tpe(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			GRP_subscan_old_groups_tpe(hge);
			break;
#endif
		case TYPE_flt:
			GRP_subscan_old_groups_tpe(flt);
			break;
		case TYPE_dbl:
			GRP_subscan_old_groups_tpe(dbl);
			break;
		default:
			GRP_subscan_old_groups_any();
			break;
		}

		GDKfree(pgrp);
	} else if (g == NULL && t == TYPE_bte) {
		/* byte-sized values, use 256 entry array to keep
		 * track of doled out group ids; note that we can't
		 * possibly have more than 256 groups, so the group id
		 * fits in an unsigned char */
		unsigned char *restrict bgrps = GDKmalloc(256);
		const unsigned char *restrict w = (const unsigned char *) Tloc(b, 0);
		unsigned char v;

		if (bgrps == NULL)
			goto error;
		memset(bgrps, 0xFF, 256);
		if (histo)
			memset(cnts, 0, maxgrps * sizeof(lng));
		ngrp = 0;
		gn->tsorted = 1;
		r = 0;
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				p = *cand++ - b->hseqbase;
			} else {
				p = start++;
			}
			if (p >= end)
				break;
			if ((v = bgrps[w[p]]) == 0xFF && ngrp < 256) {
				bgrps[w[p]] = v = (unsigned char) ngrp++;
				if (extents)
					exts[v] = b->hseqbase + (oid) p;
			}
			ngrps[r] = v;
			if (r > 0 && v < ngrps[r - 1])
				gn->tsorted = 0;
			if (histo)
				cnts[v]++;
			r++;
		}
		GDKfree(bgrps);
	} else if (g == NULL && t == TYPE_sht) {
		/* short-sized values, use 65536 entry array to keep
		 * track of doled out group ids; note that we can't
		 * possibly have more than 65536 groups, so the group
		 * id fits in an unsigned short */
		unsigned short *restrict sgrps = GDKmalloc(65536 * sizeof(short));
		const unsigned short *restrict w = (const unsigned short *) Tloc(b, 0);
		unsigned short v;

		if (sgrps == NULL)
			goto error;
		memset(sgrps, 0xFF, 65536 * sizeof(short));
		if (histo)
			memset(cnts, 0, maxgrps * sizeof(lng));
		ngrp = 0;
		gn->tsorted = 1;
		r = 0;
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				p = *cand++ - b->hseqbase;
			} else {
				p = start++;
			}
			if (p >= end)
				break;
			if ((v = sgrps[w[p]]) == 0xFFFF && ngrp < 65536) {
				sgrps[w[p]] = v = (unsigned short) ngrp++;
				if (extents)
					exts[v] = b->hseqbase + (oid) p;
			}
			ngrps[r] = v;
			if (r > 0 && v < ngrps[r - 1])
				gn->tsorted = 0;
			if (histo)
				cnts[v]++;
			r++;
		}
		GDKfree(sgrps);
	} else if (BATcheckhash(b) ||
		   (b->batPersistence == PERSISTENT &&
		    BAThash(b, 0) == GDK_SUCCEED)
#ifndef DISABLE_PARENT_HASH
		   || ((parent = VIEWtparent(b)) != 0 &&
		       BATcheckhash(BBPdescriptor(parent)))
#endif
		) {
		BUN lo;

		/* we already have a hash table on b, or b is
		 * persistent and we could create a hash table, or b
		 * is a view on a bat that already has a hash table */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "s=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "use existing hash table\n",
				  BATgetId(b), BATcount(b),
				  s ? BATgetId(s) : "NULL", s ? BATcount(s) : 0,
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
		hseqb = b->hseqbase;
#ifndef DISABLE_PARENT_HASH
		if (b->thash == NULL && (parent = VIEWtparent(b)) != 0) {
			/* b is a view on another bat (b2 for now).
			 * calculate the bounds [lo, lo+BATcount(b))
			 * in the parent that b uses */
			BAT *b2 = BBPdescriptor(parent);
			lo = (BUN) ((b->theap.base - b2->theap.base) >> b->tshift);
			b = b2;
			bi = bat_iterator(b);
			start += lo;
			end += lo;
		} else
#endif
		{
			lo = 0;
		}
		hs = b->thash;
		gn->tsorted = 1; /* be optimistic */

		switch (t) {
		case TYPE_bte:
			GRP_use_existing_hash_table_tpe(bte);
			break;
		case TYPE_sht:
			GRP_use_existing_hash_table_tpe(sht);
			break;
		case TYPE_int:
			GRP_use_existing_hash_table_tpe(int);
			break;
		case TYPE_lng:
			GRP_use_existing_hash_table_tpe(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			GRP_use_existing_hash_table_tpe(hge);
			break;
#endif
		case TYPE_flt:
			GRP_use_existing_hash_table_tpe(flt);
			break;
		case TYPE_dbl:
			GRP_use_existing_hash_table_tpe(dbl);
			break;
		default:
			GRP_use_existing_hash_table_any();
			break;
		}
	} else {
		bit gc = g && (g->tsorted || g->trevsorted);
		const char *nme;
		size_t nmelen;
		Heap *hp = NULL;
		BUN prb;
		BUN mask = HASHmask(cnt) >> 3;
		int bits = 3;

		GDKclrerr();	/* not interested in BAThash errors */
		/* when combining value and group-id hashes,
		 * we left-shift one of them by half the hash-mask width
		 * to better spread bits and use the entire hash-mask,
		 * and thus reduce collisions */
		while (mask >>= 1)
			bits++;
		bits /= 2;

		/* not sorted, and no pre-existing hash table: we'll
		 * build an incomplete hash table on the fly--also see
		 * BATassertProps for similar code; we also exploit if
		 * g is clustered */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "s=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "create partial hash table%s\n",
				  BATgetId(b), BATcount(b),
				  s ? BATgetId(s) : "NULL", s ? BATcount(s) : 0,
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted, gc ? " (g clustered)" : "");
		nme = BBP_physical(b->batCacheid);
		nmelen = strlen(nme);
		if (ATOMsize(t) == 1) {
			mask = 1 << 16;
			bits = 8;
		} else if (ATOMsize(t) == 2) {
			mask = 1 << 16;
			bits = 8;
		} else {
			mask = HASHmask(cnt);
			bits = 0;
		}
		if ((hp = GDKzalloc(sizeof(Heap))) == NULL ||
		    (hp->farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0 ||
		    (hp->filename = GDKmalloc(nmelen + 30)) == NULL ||
		    snprintf(hp->filename, nmelen + 30,
			     "%s.hash" SZFMT, nme, MT_getpid()) < 0 ||
		    (ext = GDKstrdup(hp->filename + nmelen + 1)) == NULL ||
		    (hs = HASHnew(hp, b->ttype, BUNlast(b),
				  MAX(HASHmask(cnt), 1 << 16), BUN_NONE)) == NULL) {
			if (hp) {
				if (hp->filename)
					GDKfree(hp->filename);
				GDKfree(hp);
			}
			if (ext)
				GDKfree(ext);
			hp = NULL;
			ext = NULL;
			GDKerror("BATgroup: cannot allocate hash table\n");
			goto error;
		}
		gn->tsorted = 1; /* be optimistic */

		switch (t) {
		case TYPE_bte:
			GRP_create_partial_hash_table_tpe(bte);
			break;
		case TYPE_sht:
			GRP_create_partial_hash_table_tpe(sht);
			break;
		case TYPE_int:
			GRP_create_partial_hash_table_tpe(int);
			break;
		case TYPE_lng:
			GRP_create_partial_hash_table_tpe(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			GRP_create_partial_hash_table_tpe(hge);
			break;
#endif
		case TYPE_flt:
			GRP_create_partial_hash_table_tpe(flt);
			break;
		case TYPE_dbl:
			GRP_create_partial_hash_table_tpe(dbl);
			break;
		default:
			GRP_create_partial_hash_table_any();
		}

		HEAPfree(hp, 1);
		GDKfree(hp);
		GDKfree(hs);
		GDKfree(ext);
	}
	if (extents) {
		BATsetcount(en, (BUN) ngrp);
		en->tkey = 1;
		en->tsorted = 1;
		en->trevsorted = ngrp == 1;
		en->tnonil = 1;
		en->tnil = 0;
		*extents = virtualize(en);
	}
	if (histo) {
		BATsetcount(hn, (BUN) ngrp);
		if (ngrp == cnt || ngrp == 1) {
			hn->tkey = ngrp == 1;
			hn->tsorted = 1;
			hn->trevsorted = 1;
		} else {
			hn->tkey = 0;
			hn->tsorted = 0;
			hn->trevsorted = 0;
		}
		hn->tnonil = 1;
		hn->tnil = 0;
		*histo = hn;
	}
	gn->tkey = ngrp == BATcount(gn);
	gn->trevsorted = ngrp == 1 || BATcount(gn) <= 1;
	gn->tnonil = 1;
	gn->tnil = 0;
	*groups = gn;
	return GDK_SUCCEED;
  error:
	if (gn)
		BBPunfix(gn->batCacheid);
	if (en)
		BBPunfix(en->batCacheid);
	if (hn)
		BBPunfix(hn->batCacheid);
	return GDK_FAIL;
}

gdk_return
BATgroup(BAT **groups, BAT **extents, BAT **histo,
	 BAT *b, BAT *s, BAT *g, BAT *e, BAT *h)
{
	return BATgroup_internal(groups, extents, histo, b, s, g, e, h, 0);
}
