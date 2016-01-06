/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* how much to extend the extent and histo bats when we run out of space */
#define GROUPBATINCR	8192

/* BATgroup returns three bats that indicate the grouping of the input
 * bat.  All input and output bats (must) have dense head columns.
 * Grouping means that all equal values are in the same group, and
 * differing values are in different groups.  If specified, the input
 * bat g gives a pre-existing grouping.  This bat must be aligned with
 * b.
 *
 * The outputs are as follows.
 * The groups bat has a dense head which is aligned with the input bat
 * b, and the tail has group id's (type oid).
 * The extents and histo bats have the group id in the head (a dense
 * sequence starting at 0).  The tail of extents is the head oid from
 * b of a representative of the group.  The tail of histo is of type
 * wrd and contains the number of elements from b that are member of
 * the group.
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
 * If the input bats b and g are sorted, or if the subsorted flag is
 * set (only used by BATsort), we only need to compare consecutive
 * values.
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
				exts = (oid *) Tloc(en, BUNfirst(en));	\
			}						\
			if (histo) {					\
				BATsetcount(hn, ngrp);			\
				if (BATextend(hn, maxgrps) != GDK_SUCCEED) \
					goto error;			\
				cnts = (wrd *) Tloc(hn, BUNfirst(hn));	\
			}						\
		}							\
		if (extents)						\
			exts[ngrp] = hseqb + (oid) (p - r);		\
		if (histo)						\
			cnts[ngrp] = 1;					\
		ngrps[p - r] = ngrp;					\
		ngrp++;							\
	} while (0)


#define GRP_compare_consecutive_values(INIT_0,INIT_1,COMP,KEEP)		\
	do {								\
		INIT_0;							\
		for (r = BUNfirst(b), p = r + 1, q = r + BATcount(b);	\
		     p < q;						\
		     p++) {						\
			INIT_1;						\
			if ((grps && *grps != prev) || COMP) {		\
				GRPnotfound();				\
			} else {					\
				ngrps[p - r] = ngrp - 1;		\
				if (histo)				\
					cnts[ngrp - 1]++;		\
			}						\
			KEEP;						\
			if (grps)					\
				prev = *grps++;				\
		}							\
	} while(0)

#define GRP_compare_consecutive_values_tpe(TYPE)		\
	GRP_compare_consecutive_values(				\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0);	\
			TYPE pw = w[BUNfirst(b)]	,	\
	/* INIT_1 */					,	\
	/* COMP   */	w[p] != pw			,	\
	/* KEEP   */	pw = w[p]				\
	)

#define GRP_compare_consecutive_values_any()			\
	GRP_compare_consecutive_values(				\
	/* INIT_0 */	pv = BUNtail(bi, BUNfirst(b))	,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* COMP   */	cmp(v, pv) != 0			,	\
	/* KEEP   */	pv = v					\
	)


#define GRP_subscan_old_groups(INIT_0,INIT_1,COMP,KEEP)			\
	do {								\
		INIT_0;							\
		pgrp[grps[0]] = BUNfirst(b);				\
		for (j = r = BUNfirst(b), p = r + 1, q = r + BATcount(b); \
		     p < q;						\
		     p++) {						\
			INIT_1;						\
			if (COMP) {					\
				/* range [j, p) is all same value */	\
				/* i is position where we saw p's old	\
				 * group last */			\
				i = pgrp[grps[p - r]];			\
				/* p is new position where we saw this	\
				 * group */				\
				pgrp[grps[p - r]] = p;			\
				if (j <= i && i < p)	{		\
					/* i is position of equal	\
					 * value in same old group as	\
					 * p, so p gets same new group	\
					 * as i */			\
					oid grp = ngrps[i - r];		\
					ngrps[p - r] = grp;		\
					if (histo)			\
						cnts[grp]++;		\
					if (gn->tsorted &&		\
					    grp != ngrp - 1)		\
						gn->tsorted = 0;	\
					/* we found the value/group	\
					 * combination, go to next	\
					 * value */			\
					continue;			\
				}					\
			} else {					\
				/* value differs from previous value */	\
				j = p;					\
				KEEP;					\
				pgrp[grps[p - r]] = p;			\
			}						\
			/* start a new group */				\
			GRPnotfound();					\
		}							\
	} while(0)

#define GRP_subscan_old_groups_tpe(TYPE)			\
	GRP_subscan_old_groups(					\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0);	\
		    	TYPE pw = w[BUNfirst(b)]	,	\
	/* INIT_1 */					,	\
	/* COMP   */	w[p] == pw			,	\
	/* KEEP   */	pw = w[p]				\
	)

#define GRP_subscan_old_groups_any()				\
	GRP_subscan_old_groups(					\
	/* INIT_0 */	pv = BUNtail(bi, BUNfirst(b))	,	\
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
		for (r = lo, p = r, q = hi;				\
		     p < q;						\
		     p++) {						\
			INIT_1;						\
			/* this loop is similar, but not equal, to */	\
			/* HASHloop: the difference is that we only */	\
			/* consider BUNs smaller than the one we're */	\
			/* looking up (p), and that we also consider */	\
			/* the input groups */				\
			if (grps) {					\
				for (hb = HASHgetlink(hs, p);		\
				     hb != HASHnil(hs) && hb >= lo;	\
				     hb = HASHgetlink(hs, hb)) {	\
					assert(hb < p);			\
					if (grps[hb - r] == grps[p - r] && \
					    COMP) {			\
						oid grp = ngrps[hb - r]; \
						ngrps[p - r] = grp;	\
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
				     hb != HASHnil(hs) && hb >= lo;	\
				     hb = HASHgetlink(hs, hb)) {	\
					assert(hb < p);			\
					if (COMP) {			\
						oid grp = ngrps[hb - r]; \
						ngrps[p - r] = grp;	\
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
		INIT_0;							\
		for (r = BUNfirst(b), p = r, q = r + BATcount(b); 	\
		     p < q;						\
		     p++) { 						\
			INIT_1;						\
			prb = HASH;					\
			if (gc) {					\
				for (hb = HASHget(hs,prb);		\
				     hb != HASHnil(hs) &&		\
				      grps[hb - r] == grps[p - r];	\
				     hb = HASHgetlink(hs,hb)) {		\
					assert(HASHgetlink(hs,hb) == HASHnil(hs) \
					       || HASHgetlink(hs,hb) < hb); \
					if (COMP) {		\
						oid grp = ngrps[hb - r]; \
						ngrps[p - r] = grp; 	\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = 0; \
						break;			\
					}				\
				}					\
				if (hb != HASHnil(hs) &&		\
				    grps[hb - r] != grps[p - r]) {	\
					/* no group assigned yet */	\
					hb = HASHnil(hs);		\
				}					\
			} else if (grps) {				\
				prb = (prb ^ (BUN) grps[p-r] << bits) & hs->mask; \
				for (hb = HASHget(hs,prb);		\
				     hb != HASHnil(hs);			\
				     hb = HASHgetlink(hs,hb)) {		\
					if (grps[hb - r] == grps[p - r] && \
					    COMP) {			\
						oid grp = ngrps[hb - r]; \
						ngrps[p - r] = grp;	\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = 0; \
						break;			\
					}				\
				}					\
			} else {					\
				for (hb = HASHget(hs,prb);		\
				     hb != HASHnil(hs);			\
				     hb = HASHgetlink(hs,hb)) {		\
					if (COMP) {		\
						oid grp = ngrps[hb - r]; \
						ngrps[p - r] = grp;	\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = 0; \
						break;			\
					}				\
				}					\
			}						\
			if (hb == HASHnil(hs)) {			\
				GRPnotfound();				\
				/* enter new group into hash table */	\
				HASHputlink(hs,p, HASHget(hs,prb));	\
				HASHput(hs,prb,p); 			\
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
		  BAT *b, BAT *g, BAT *e, BAT *h, int subsorted)
{
	BAT *gn = NULL, *en = NULL, *hn = NULL;
	int t;
	int (*cmp)(const void *, const void *);
	const oid *grps = NULL;
	oid *restrict ngrps, ngrp, prev = 0, hseqb = 0;
	oid *restrict exts = NULL;
	wrd *restrict cnts = NULL;
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

	if (b == NULL || !BAThdense(b)) {
		GDKerror("BATgroup: b must be dense-headed\n");
		return GDK_FAIL;
	}
	/* g is NULL or [oid(dense),oid] and same size as b */
	assert(g == NULL || BAThdense(g));
	assert(g == NULL || BATttype(g) == TYPE_oid || BATcount(g) == 0);
	assert(g == NULL || BATcount(b) == BATcount(g));
	assert(g == NULL || BATcount(b) == 0 || b->hseqbase == g->hseqbase);
	/* e is NULL or [oid(dense),oid] */
	assert(e == NULL || BAThdense(e));
	assert(e == NULL || BATttype(e) == TYPE_oid);
	/* h is NULL or [oid(dense),wrd] */
	assert(h == NULL || BAThdense(h));
	assert(h == NULL || h->ttype == TYPE_wrd);
	/* e and h are aligned */
	assert(e == NULL || h == NULL || BATcount(e) == BATcount(h));
	assert(e == NULL || h == NULL || e->hseqbase == h->hseqbase);
	/* we want our output to go somewhere */
	assert(groups != NULL);

	hseqb = b->hseqbase;
	if (b->tkey || BATcount(b) <= 1 || (g && (g->tkey || BATtdense(g)))) {
		/* grouping is trivial: 1 element per group */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "trivial case: 1 element per group\n",
				  BATgetId(b), BATcount(b),
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
		ngrp = BATcount(b) == 0 ? 0 : b->hseqbase;
		gn = BATnew(TYPE_void, TYPE_void, BATcount(b), TRANSIENT);
		if (gn == NULL)
			goto error;
		BATsetcount(gn, BATcount(b));
		BATseqbase(gn, ngrp);
		BATseqbase(BATmirror(gn), 0);
		*groups = gn;
		if (extents) {
			en = BATnew(TYPE_void, TYPE_void, BATcount(b), TRANSIENT);
			if (en == NULL)
				goto error;
			BATsetcount(en, BATcount(b));
			BATseqbase(en, 0);
			BATseqbase(BATmirror(en), ngrp);
			*extents = en;
		}
		if (histo) {
			wrd one = 1;

			hn = BATconstant(TYPE_wrd, &one, BATcount(b), TRANSIENT);
			if (hn == NULL)
				goto error;
			*histo = hn;
		}
		return GDK_SUCCEED;
	}
	if (b->tsorted && b->trevsorted) {
		/* all values are equal */
		if (g == NULL) {
			/* there's only a single group: 0 */
			ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
					  "trivial case: single output group\n",
				  BATgetId(b), BATcount(b),
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
			ngrp = 0;
			gn = BATconstant(TYPE_oid, &ngrp, BATcount(b), TRANSIENT);
			if (gn == NULL)
				goto error;
			BATseqbase(gn, b->hseqbase);
			*groups = gn;
			if (extents) {
				ngrp = gn->hseqbase;
				en = BATconstant(TYPE_void, &ngrp, 1, TRANSIENT);
				if (en == NULL)
					goto error;
				BATseqbase(BATmirror(en), ngrp);
				*extents = en;
			}
			if (histo) {
				wrd cnt = (wrd) BATcount(b);

				hn = BATconstant(TYPE_wrd, &cnt, 1, TRANSIENT);
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
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
					  "trivial case: copy input groups\n",
				  BATgetId(b), BATcount(b),
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
			assert(g->htype == TYPE_void);
			gn = COLcopy(g, g->ttype, 0, TRANSIENT);
			if (gn == NULL)
				goto error;
			*groups = gn;
			if (extents) {
				assert(e->htype == TYPE_void);
				en = COLcopy(e, e->ttype, 0, TRANSIENT);
				if (en == NULL)
					goto error;
				*extents = en;
			}
			if (histo) {
				assert(h->htype == TYPE_void);
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
	gn = BATnew(TYPE_void, TYPE_oid, BATcount(b), TRANSIENT);
	if (gn == NULL)
		goto error;
	ngrps = (oid *) Tloc(gn, BUNfirst(gn));
	maxgrps = BATcount(b) / 10;
	if (e && maxgrps < BATcount(e))
		maxgrps += BATcount(e);
	if (h && maxgrps < BATcount(h))
		maxgrps += BATcount(h);
	if (maxgrps < GROUPBATINCR)
		maxgrps = GROUPBATINCR;
	if (b->T->width <= 2 &&
	    maxgrps > ((BUN) 1 << (8 << (b->T->width == 2))))
		maxgrps = (BUN) 1 << (8 << (b->T->width == 2));
	if (extents) {
		en = BATnew(TYPE_void, TYPE_oid, maxgrps, TRANSIENT);
		if (en == NULL)
			goto error;
		exts = (oid *) Tloc(en, BUNfirst(en));
	}
	if (histo) {
		hn = BATnew(TYPE_void, TYPE_wrd, maxgrps, TRANSIENT);
		if (hn == NULL)
			goto error;
		cnts = (wrd *) Tloc(hn, BUNfirst(hn));
	}
	ngrp = 0;
	BATsetcount(gn, BATcount(b));
	BATseqbase(gn, b->hseqbase);
	if (g)
		grps = (const oid *) Tloc(g, BUNfirst(g));

	/* figure out if we can use the storage type also for
	 * comparing values */
	t = ATOMbasetype(b->ttype);
	/* for strings we can use the offset instead of the actual
	 * string values if we know that the strings in the string
	 * heap are unique */
	if (t == TYPE_str && GDK_ELIMDOUBLES(b->T->vheap)) {
		switch (b->T->width) {
		case 1:
			t = TYPE_bte;
			break;
		case 2:
			t = TYPE_sht;
			break;
#if SIZEOF_VAR_T == 8
		case 4:
			t = TYPE_int;
			break;
#endif
		default:
			t = TYPE_var;
			break;
		}
	}

	if (((b->tsorted || b->trevsorted) &&
	     (g == NULL || g->tsorted || g->trevsorted)) ||
	    subsorted) {
		/* we only need to compare each entry with the previous */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "compare consecutive values\n",
				  BATgetId(b), BATcount(b),
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
		if (grps)
			prev = *grps++;
		ngrps[0] = ngrp;
		ngrp++;
		if (extents)
			exts[0] = b->hseqbase;
		if (histo)
			cnts[0] = 1;

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
		}

		gn->tsorted = 1;
		*groups = gn;
	} else if (b->tsorted || b->trevsorted) {
		BUN i, j;
		BUN *pgrp;

		/* for each value, we need to scan all previous equal
		 * values (a consecutive, possibly empty, range) to
		 * see if we can find one in the same old group
		 *
		 * we do this by maintaining for each old group the
		 * last time we saw that group, so if the last time we
		 * saw the old group of the current value is within
		 * this range, we can reuse the new group */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "subscan old groups\n",
				  BATgetId(b), BATcount(b),
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
				m = MAX( m , grps[i] );
			j = (BUN) m + 1;
		}
		/* array to maintain last time we saw each old group */
		pgrp = GDKmalloc(sizeof(BUN) * j);
		if (pgrp == NULL)
			goto error;
		/* initialize to impossible position */
		memset(pgrp, ~0, sizeof(BUN) * j);

		ngrps[0] = ngrp;
		if (extents)
			exts[0] = b->hseqbase;
		if (histo)
			cnts[0] = 1;
		ngrp++;		/* the next group to be assigned */
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
		}

		GDKfree(pgrp);
	} else if (g == NULL && t == TYPE_bte) {
		/* byte-sized values, use 256 entry array to keep
		 * track of doled out group ids; note that we can't
		 * possibly have more than 256 groups, so the group id
		 * fits in an unsigned char */
		unsigned char *restrict bgrps = GDKmalloc(256);
		const unsigned char *restrict w = (const unsigned char *) Tloc(b, BUNfirst(b));
		unsigned char v;
		memset(bgrps, 0xFF, 256);
		if (histo)
			memset(cnts, 0, maxgrps * sizeof(wrd));
		ngrp = 0;
		gn->tsorted = 1;
		for (p = 0, q = BATcount(b); p < q; p++) {
			if ((v = bgrps[w[p]]) == 0xFF && ngrp < 256) {
				bgrps[w[p]] = v = (unsigned char) ngrp++;
				if (extents)
					exts[v] = b->hseqbase + (oid) p;
			}
			ngrps[p] = v;
			if (p > 0 &&v < ngrps[p - 1])
				gn->tsorted = 0;
			if (histo)
				cnts[v]++;
		}
		GDKfree(bgrps);
	} else if (g == NULL && t == TYPE_sht) {
		/* short-sized values, use 65536 entry array to keep
		 * track of doled out group ids; note that we can't
		 * possibly have more than 65536 groups, so the group
		 * id fits in an unsigned short */
		unsigned short *restrict sgrps = GDKmalloc(65536 * sizeof(short));
		const unsigned short *restrict w = (const unsigned short *) Tloc(b, BUNfirst(b));
		unsigned short v;
		memset(sgrps, 0xFF, 65536 * sizeof(short));
		if (histo)
			memset(cnts, 0, maxgrps * sizeof(wrd));
		ngrp = 0;
		gn->tsorted = 1;
		for (p = 0, q = BATcount(b); p < q; p++) {
			if ((v = sgrps[w[p]]) == 0xFFFF && ngrp < 65536) {
				sgrps[w[p]] = v = (unsigned short) ngrp++;
				if (extents)
					exts[v] = b->hseqbase + (oid) p;
			}
			ngrps[p] = v;
			if (p > 0 && v < ngrps[p - 1])
				gn->tsorted = 0;
			if (histo)
				cnts[v]++;
		}
		GDKfree(sgrps);
	} else if (BATcheckhash(b) ||
		   (b->batPersistence == PERSISTENT &&
		    BAThash(b, 0) == GDK_SUCCEED)
#ifndef DISABLE_PARENT_HASH
		   || ((parent = VIEWtparent(b)) != 0 &&
		       BATcheckhash(BBPdescriptor(-parent)))
#endif
		) {
		BUN lo, hi;

		/* we already have a hash table on b, or b is
		 * persistent and we could create a hash table, or b
		 * is a view on a bat that already has a hash table */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "use existing hash table\n",
				  BATgetId(b), BATcount(b),
				  g ? BATgetId(g) : "NULL", g ? BATcount(g) : 0,
				  e ? BATgetId(e) : "NULL", e ? BATcount(e) : 0,
				  h ? BATgetId(h) : "NULL", h ? BATcount(h) : 0,
				  subsorted);
#ifndef DISABLE_PARENT_HASH
		if (b->T->hash == NULL && (parent = VIEWtparent(b)) != 0) {
			/* b is a view on another bat (b2 for now).
			 * calculate the bounds [lo, hi) in the parent
			 * that b uses */
			BAT *b2 = BBPdescriptor(-parent);
			lo = (BUN) ((b->T->heap.base - b2->T->heap.base) >> b->T->shift) + BUNfirst(b);
			hi = lo + BATcount(b);
			hseqb = b->hseqbase;
			b = b2;
			bi = bat_iterator(b);
		} else
#endif
		{
			lo = BUNfirst(b);
			hi = BUNlast(b);
		}
		hs = b->T->hash;
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
		BUN mask = HASHmask(b->batCount) >> 3;
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
		 * BATassertTailProps and BATderiveTailProps for
		 * similar code;
		 * we also exploit if g is clustered */
		ALGODEBUG fprintf(stderr, "#BATgroup(b=%s#" BUNFMT ","
				  "g=%s#" BUNFMT ","
				  "e=%s#" BUNFMT ","
				  "h=%s#" BUNFMT ",subsorted=%d): "
				  "create partial hash table%s\n",
				  BATgetId(b), BATcount(b),
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
			mask = HASHmask(b->batCount);
			bits = 0;
		}
		if ((hp = GDKzalloc(sizeof(Heap))) == NULL ||
		    (hp->farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0 ||
		    (hp->filename = GDKmalloc(nmelen + 30)) == NULL ||
		    snprintf(hp->filename, nmelen + 30,
			     "%s.hash" SZFMT, nme, MT_getpid()) < 0 ||
		    (ext = GDKstrdup(hp->filename + nmelen + 1)) == NULL ||
		    (hs = HASHnew(hp, b->ttype, BUNlast(b),
				  MAX(HASHmask(b->batCount), 1 << 16), BUN_NONE)) == NULL) {
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
		BATseqbase(en, 0);
		en->tkey = 1;
		en->tsorted = 1;
		en->trevsorted = BATcount(en) <= 1;
		en->T->nonil = 1;
		en->T->nil = 0;
		*extents = en;
	}
	if (histo) {
		BATsetcount(hn, (BUN) ngrp);
		BATseqbase(hn, 0);
		if (BATcount(hn) <= 1) {
			hn->tkey = 1;
			hn->tsorted = 1;
			hn->trevsorted = 1;
		} else {
			hn->tkey = 0;
			hn->tsorted = 0;
			hn->trevsorted = 0;
		}
		hn->T->nonil = 1;
		hn->T->nil = 0;
		*histo = hn;
	}
	gn->tkey = ngrp == BATcount(gn);
	gn->trevsorted = BATcount(gn) <= 1;
	gn->T->nonil = 1;
	gn->T->nil = 0;
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
	 BAT *b, BAT *g, BAT *e, BAT *h)
{
	return BATgroup_internal(groups, extents, histo, b, g, e, h, 0);
}
