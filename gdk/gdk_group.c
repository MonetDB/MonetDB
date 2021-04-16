/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
			exts[ngrp] = hseqb + p - lo;			\
		if (histo)						\
			cnts[ngrp] = 1;					\
		ngrps[r] = ngrp++;					\
		maxgrppos = r;						\
	} while (0)


#define GRP_compare_consecutive_values(INIT_0,INIT_1,DIFFER,KEEP)	\
	do {								\
		INIT_0;							\
		if (ci.tpe == cand_dense) {				\
			if (grps) {					\
				MT_thread_setalgorithm("GRP_compare_consecutive_values, dense, groups"); \
				for (r = 0; r < cnt; r++) {		\
					p = canditer_next_dense(&ci) - hseqb; \
					INIT_1;				\
					if (ngrp == 0 || grps[r] != prev || DIFFER) { \
						GRPnotfound();		\
					} else {			\
						ngrps[r] = ngrp - 1;	\
						if (histo)		\
							cnts[ngrp - 1]++; \
					}				\
					KEEP;				\
					prev = grps[r];			\
				}					\
			} else {					\
				MT_thread_setalgorithm("GRP_compare_consecutive_values, dense, !groups"); \
				for (r = 0; r < cnt; r++) {		\
					p = canditer_next_dense(&ci) - hseqb; \
					INIT_1;				\
					if (ngrp == 0 || DIFFER) {	\
						GRPnotfound();		\
					} else {			\
						ngrps[r] = ngrp - 1;	\
						if (histo)		\
							cnts[ngrp - 1]++; \
					}				\
					KEEP;				\
				}					\
			}						\
		} else {						\
			if (grps) {					\
				MT_thread_setalgorithm("GRP_compare_consecutive_values, !dense, groups"); \
				for (r = 0; r < cnt; r++) {		\
					p = canditer_next(&ci) - hseqb;	\
					INIT_1;				\
					if (ngrp == 0 || grps[r] != prev || DIFFER) { \
						GRPnotfound();		\
					} else {			\
						ngrps[r] = ngrp - 1;	\
						if (histo)		\
							cnts[ngrp - 1]++; \
					}				\
					KEEP;				\
					prev = grps[r];			\
				}					\
			} else {					\
				MT_thread_setalgorithm("GRP_compare_consecutive_values, !dense, !groups"); \
				for (r = 0; r < cnt; r++) {		\
					p = canditer_next(&ci) - hseqb;	\
					INIT_1;				\
					if (ngrp == 0 || DIFFER) {	\
						GRPnotfound();		\
					} else {			\
						ngrps[r] = ngrp - 1;	\
						if (histo)		\
							cnts[ngrp - 1]++; \
					}				\
					KEEP;				\
				}					\
			}						\
		}							\
	} while(0)

#define flt_neq(a, b)	(is_flt_nil(a) ? !is_flt_nil(b) : is_flt_nil(b) || (a) != (b))
#define dbl_neq(a, b)	(is_dbl_nil(a) ? !is_dbl_nil(b) : is_dbl_nil(b) || (a) != (b))
#define bte_neq(a, b)	((a) != (b))
#define sht_neq(a, b)	((a) != (b))
#define int_neq(a, b)	((a) != (b))
#define lng_neq(a, b)	((a) != (b))
#define hge_neq(a, b)	((a) != (b))

#define GRP_compare_consecutive_values_tpe(TYPE)		\
	GRP_compare_consecutive_values(				\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0);	\
			TYPE pw = 0			,	\
	/* INIT_1 */					,	\
	/* DIFFER */	TYPE##_neq(w[p], pw)		,	\
	/* KEEP   */	pw = w[p]				\
	)

#define GRP_compare_consecutive_values_any()			\
	GRP_compare_consecutive_values(				\
	/* INIT_0 */	pv = NULL			,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* DIFFER */	cmp(v, pv) != 0			,	\
	/* KEEP   */	pv = v					\
	)


#define GRP_subscan_old_groups(INIT_0,INIT_1,EQUAL,KEEP)		\
	do {								\
		INIT_0;							\
		pgrp[grps[0]] = 0;					\
		j = 0;							\
		if (ci.tpe == cand_dense) {				\
			MT_thread_setalgorithm("GRP_subscan_old_groups, dense"); \
			for (r = 0; r < cnt; r++) {			\
				p = canditer_next_dense(&ci) - hseqb;	\
				INIT_1;					\
				if (ngrp != 0 && EQUAL) {		\
					/* range [j, r) is all same value */ \
					/* i is position where we saw r's */ \
					/* old group last */		\
					i = pgrp[grps[r]];		\
					/* p is new position where we saw this \
					 * group */			\
					pgrp[grps[r]] = r;		\
					if (j <= i && i < r)	{	\
						/* i is position of equal */ \
						/* value in same old group */ \
						/* as r, so r gets same new */ \
						/* group as i */	\
						oid grp = ngrps[i];	\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = false; \
						/* we found the value/group */ \
						/* combination, go to next */ \
						/* value */		\
						continue;		\
					}				\
				} else {				\
					/* value differs from previous value */	\
					/* (or is the first) */		\
					j = r;				\
					KEEP;				\
					pgrp[grps[r]] = r;		\
				}					\
				/* start a new group */			\
				GRPnotfound();				\
			}						\
		} else {						\
			MT_thread_setalgorithm("GRP_subscan_old_groups, !dense"); \
			for (r = 0; r < cnt; r++) {			\
				p = canditer_next(&ci) - hseqb;		\
				INIT_1;					\
				if (ngrp != 0 && EQUAL) {		\
					/* range [j, r) is all same value */ \
					/* i is position where we saw r's */ \
					/* old group last */		\
					i = pgrp[grps[r]];		\
					/* p is new position where we saw this \
					 * group */			\
					pgrp[grps[r]] = r;		\
					if (j <= i && i < r)	{	\
						/* i is position of equal */ \
						/* value in same old group */ \
						/* as r, so r gets same new */ \
						/* group as i */	\
						oid grp = ngrps[i];	\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = false; \
						/* we found the value/group */ \
						/* combination, go to next */ \
						/* value */		\
						continue;		\
					}				\
				} else {				\
					/* value differs from previous value */	\
					/* (or is the first) */		\
					j = r;				\
					KEEP;				\
					pgrp[grps[r]] = r;		\
				}					\
				/* start a new group */			\
				GRPnotfound();				\
			}						\
		}							\
	} while(0)

#define flt_equ(a, b)	(is_flt_nil(a) ? is_flt_nil(b) : !is_flt_nil(b) && (a) == (b))
#define dbl_equ(a, b)	(is_dbl_nil(a) ? is_dbl_nil(b) : !is_dbl_nil(b) && (a) == (b))
#define bte_equ(a, b)	((a) == (b))
#define sht_equ(a, b)	((a) == (b))
#define int_equ(a, b)	((a) == (b))
#define lng_equ(a, b)	((a) == (b))
#define hge_equ(a, b)	((a) == (b))
#ifdef HAVE_HGE
#define uuid_equ(a, b)	((a).h == (b).h)
#else
#ifdef HAVE_UUID
#define uuid_equ(a, b)	(uuid_compare((a).u, (b).u) == 0)
#else
#define uuid_equ(a, b)	(memcmp((a).u, (b).u, UUID_SIZE) == 0)
#endif
#endif

#define GRP_subscan_old_groups_tpe(TYPE)			\
	GRP_subscan_old_groups(					\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0);	\
		    	TYPE pw = 0			,	\
	/* INIT_1 */					,	\
	/* EQUAL  */	TYPE##_equ(w[p], pw)		,	\
	/* KEEP   */	pw = w[p]				\
	)

#define GRP_subscan_old_groups_any()				\
	GRP_subscan_old_groups(					\
	/* INIT_0 */	pv = NULL			,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* EQUAL  */	cmp(v, pv) == 0			,	\
	/* KEEP   */	pv = v					\
	)

/* If a hash table exists on b we use it.
 *
 * The algorithm is simple.  We go through b and for each value we
 * follow the hash chain starting at the next element after that value
 * to find one that is equal to the value we're currently looking at.
 * If we found such a value, we add the value to the same group.  If
 * we reach the end of the chain, we create a new group.
 *
 * If b (the original, that is) is a view on another BAT, and this
 * other BAT has a hash, we use that.  The lo and hi values are the
 * bounds of the parent BAT that we're considering.
 *
 * Note this algorithm depends critically on the fact that our hash
 * chains go from higher to lower BUNs.
 */
#define GRP_use_existing_hash_table(INIT_0,INIT_1,EQUAL)		\
	do {								\
		INIT_0;							\
		assert(grps == NULL);					\
		if (ci.tpe == cand_dense) {				\
			MT_thread_setalgorithm(phash ? "GRP_use_existing_hash_table, dense, parent hash" : "GRP_use_existing_hash_table, dense"); \
			for (r = 0; r < cnt; r++) {			\
				oid o = canditer_next_dense(&ci);	\
				p = o - hseqb + lo;			\
				INIT_1;					\
				/* this loop is similar, but not */	\
				/* equal, to HASHloop: the difference */ \
				/* is that we only consider BUNs */	\
				/* smaller than the one we're looking */ \
				/* up (p) */				\
				for (hb = HASHgetlink(hs, p);		\
				     hb != HASHnil(hs) && hb >= lo;	\
				     hb = HASHgetlink(hs, hb)) {	\
					oid grp;			\
					assert(hb < p);			\
					q = canditer_search_dense(&ci, hb + hseqb - lo, false); \
					if (q == BUN_NONE)		\
						continue;		\
					if (EQUAL) {			\
						grp = ngrps[q];		\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = false; \
						break;			\
					}				\
				}					\
				if (hb == HASHnil(hs) || hb < lo) {	\
					GRPnotfound();			\
				}					\
			}						\
		} else {						\
			MT_thread_setalgorithm(phash ? "GRP_use_existing_hash_table, !dense, parent hash" : "GRP_use_existing_hash_table, !dense"); \
			for (r = 0; r < cnt; r++) {			\
				oid o = canditer_next(&ci);		\
				p = o - hseqb + lo;			\
				INIT_1;					\
				/* this loop is similar, but not */	\
				/* equal, to HASHloop: the difference */ \
				/* is that we only consider BUNs */	\
				/* smaller than the one we're looking */ \
				/* up (p) */				\
				for (hb = HASHgetlink(hs, p);		\
				     hb != HASHnil(hs) && hb >= lo;	\
				     hb = HASHgetlink(hs, hb)) {	\
					oid grp;			\
					assert(hb < p);			\
					q = canditer_search(&ci, hb + hseqb - lo, false); \
					if (q == BUN_NONE)		\
						continue;		\
					if (EQUAL) {			\
						grp = ngrps[q];		\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = false; \
						break;			\
					}				\
				}					\
				if (hb == HASHnil(hs) || hb < lo) {	\
					GRPnotfound();			\
				}					\
			}						\
		}							\
	} while(0)

#define GRP_use_existing_hash_table_tpe(TYPE)			\
	GRP_use_existing_hash_table(				\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0),	\
	/* INIT_1 */					,	\
	/* EQUAL  */	TYPE##_equ(w[p], w[hb])			\
	)

#define GRP_use_existing_hash_table_any()			\
	GRP_use_existing_hash_table(				\
	/* INIT_0 */					,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* EQUAL  */	cmp(v, BUNtail(bi, hb)) == 0		\
	)

/* reverse the bits of an OID value */
static inline oid
rev(oid x)
{
#if SIZEOF_OID == 8
	x = ((x & 0x5555555555555555) <<  1) | ((x >>  1) & 0x5555555555555555);
	x = ((x & 0x3333333333333333) <<  2) | ((x >>  2) & 0x3333333333333333);
	x = ((x & 0x0F0F0F0F0F0F0F0F) <<  4) | ((x >>  4) & 0x0F0F0F0F0F0F0F0F);
	x = ((x & 0x00FF00FF00FF00FF) <<  8) | ((x >>  8) & 0x00FF00FF00FF00FF);
	x = ((x & 0x0000FFFF0000FFFF) << 16) | ((x >> 16) & 0x0000FFFF0000FFFF);
	x = ((x & 0x00000000FFFFFFFF) << 32) | ((x >> 32) & 0x00000000FFFFFFFF);
#else
	x = ((x & 0x55555555) <<  1) | ((x >>  1) & 0x55555555);
	x = ((x & 0x33333333) <<  2) | ((x >>  2) & 0x33333333);
	x = ((x & 0x0F0F0F0F) <<  4) | ((x >>  4) & 0x0F0F0F0F);
	x = ((x & 0x00FF00FF) <<  8) | ((x >>  8) & 0x00FF00FF);
	x = ((x & 0x0000FFFF) << 16) | ((x >> 16) & 0x0000FFFF);
#endif
	return x;
}

/* population count: count number of 1 bits in a value */
static inline int
pop(oid x)
{
#ifdef __GNUC__
#if SIZEOF_OID == SIZEOF_INT
	return __builtin_popcount(x);
#else
	return __builtin_popcountl(x);
#endif
#else
#ifdef _MSC_VER
#if SIZEOF_OID == SIZEOF_INT
	return (int) __popcnt((unsigned int) (x));
#else
	return (int) __popcnt64((unsigned __int64) (x));
#endif
#else
	/* divide and conquer implementation */
#if SIZEOF_OID == 8
	x = (x & 0x5555555555555555) + ((x >>  1) & 0x5555555555555555);
	x = (x & 0x3333333333333333) + ((x >>  2) & 0x3333333333333333);
	x = (x & 0x0F0F0F0F0F0F0F0F) + ((x >>  4) & 0x0F0F0F0F0F0F0F0F);
	x = (x & 0x00FF00FF00FF00FF) + ((x >>  8) & 0x00FF00FF00FF00FF);
	x = (x & 0x0000FFFF0000FFFF) + ((x >> 16) & 0x0000FFFF0000FFFF);
	x = (x & 0x00000000FFFFFFFF) + ((x >> 32) & 0x00000000FFFFFFFF);
#else
	x = (x & 0x55555555) + ((x >>  1) & 0x55555555);
	x = (x & 0x33333333) + ((x >>  2) & 0x33333333);
	x = (x & 0x0F0F0F0F) + ((x >>  4) & 0x0F0F0F0F);
	x = (x & 0x00FF00FF) + ((x >>  8) & 0x00FF00FF);
	x = (x & 0x0000FFFF) + ((x >> 16) & 0x0000FFFF);
#endif
	return (int) x;
#endif
#endif
}

#define GRP_create_partial_hash_table_core(INIT_1,HASH,EQUAL,ASSERT,GRPTST) \
	do {								\
		if (ci.tpe == cand_dense) {				\
			MT_thread_setalgorithm("GRP_create_partial_hash_table, dense"); \
			for (r = 0; r < cnt; r++) {			\
				p = canditer_next_dense(&ci) - hseqb;	\
				INIT_1;					\
				prb = HASH;				\
				for (hb = HASHget(hs, prb);		\
				     hb != HASHnil(hs);			\
				     hb = HASHgetlink(hs, hb)) {	\
					ASSERT;				\
					q = canditer_search_dense(&ci, hb + hseqb, false); \
					if (q == BUN_NONE)		\
						continue;		\
					GRPTST(q, r);			\
					if (EQUAL) {			\
						grp = ngrps[q];		\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = false; \
						break;			\
					}				\
				}					\
				if (hb == HASHnil(hs)) {		\
					GRPnotfound();			\
					/* enter new group into hash table */ \
					HASHputlink(hs, p, HASHget(hs, prb)); \
					HASHput(hs, prb, p);		\
				}					\
			}						\
		} else {						\
			MT_thread_setalgorithm("GRP_create_partial_hash_table, !dense"); \
			for (r = 0; r < cnt; r++) {			\
				p = canditer_next(&ci) - hseqb;		\
				INIT_1;					\
				prb = HASH;				\
				for (hb = HASHget(hs, prb);		\
				     hb != HASHnil(hs);			\
				     hb = HASHgetlink(hs, hb)) {	\
					ASSERT;				\
					q = canditer_search(&ci, hb + hseqb, false); \
					if (q == BUN_NONE)		\
						continue;		\
					GRPTST(q, r);			\
					if (EQUAL) {			\
						grp = ngrps[q];		\
						ngrps[r] = grp;		\
						if (histo)		\
							cnts[grp]++;	\
						if (gn->tsorted &&	\
						    grp != ngrp - 1)	\
							gn->tsorted = false; \
						break;			\
					}				\
				}					\
				if (hb == HASHnil(hs)) {		\
					GRPnotfound();			\
					/* enter new group into hash table */ \
					HASHputlink(hs, p, HASHget(hs, prb)); \
					HASHput(hs, prb, p);		\
				}					\
			}						\
		}							\
	} while (0)
#define GCGRPTST(i, j)	if (grps[i] != grps[j]) { hb = HASHnil(hs); break; }
#define GRPTST(i, j)	if (grps[i] != grps[j]) continue
#define NOGRPTST(i, j)	(void) 0
#define GRP_create_partial_hash_table(INIT_0,INIT_1,HASH,EQUAL)		\
	do {								\
		INIT_0;							\
		if (grps) {						\
			if (gc) {					\
				GRP_create_partial_hash_table_core(INIT_1,HASH,EQUAL,assert(HASHgetlink(hs, hb) == HASHnil(hs) || HASHgetlink(hs, hb) < hb),GCGRPTST); \
			} else {					\
				GRP_create_partial_hash_table_core(INIT_1,HASH ^ (rev(grps[r]) >> bits),EQUAL,(void)0,GRPTST); \
			}						\
		} else {						\
			GRP_create_partial_hash_table_core(INIT_1,HASH,EQUAL,(void)0,NOGRPTST); \
		}							\
	} while (0)

#define GRP_create_partial_hash_table_tpe(TYPE)			\
	GRP_create_partial_hash_table(				\
	/* INIT_0 */	const TYPE *w = (TYPE *) Tloc(b, 0),	\
	/* INIT_1 */					,	\
	/* HASH   */	hash_##TYPE(hs, &w[p])		,	\
	/* EQUAL  */	TYPE##_equ(w[p], w[hb])			\
	)

#define GRP_create_partial_hash_table_any()			\
	GRP_create_partial_hash_table(				\
	/* INIT_0 */					,	\
	/* INIT_1 */	v = BUNtail(bi, p)		,	\
	/* HASH   */	hash_any(hs, v)			,	\
	/* EQUAL  */	cmp(v, BUNtail(bi, hb)) == 0		\
	)


gdk_return
BATgroup_internal(BAT **groups, BAT **extents, BAT **histo,
		  BAT *b, BAT *s, BAT *g, BAT *e, BAT *h, bool subsorted)
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
	Hash *hs = NULL;
	BUN hb;
	BUN maxgrps;
	BUN maxgrppos = BUN_NONE;
	bat parent;
	BUN cnt;
	BUN lo = 0;
	struct canditer ci;
	oid maxgrp = oid_nil;	/* maximum value of g BAT (if subgrouping) */
	const ValRecord *prop;
	lng t0 = 0;
	const char *algomsg = "";

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	if (b == NULL) {
		GDKerror("b must exist\n");
		return GDK_FAIL;
	}
	assert(s == NULL || BATttype(s) == TYPE_oid);
	cnt = canditer_init(&ci, b, s);

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
	} else {
		hseqb = ci.seq;
	}
	if (b->tkey || cnt <= 1 || (g && (g->tkey || BATtdense(g)))) {
		/* grouping is trivial: 1 element per group */
		gn = BATdense(hseqb, 0, BATcount(b));
		if (gn == NULL)
			goto error;
		*groups = gn;
		if (extents) {
			en = canditer_slice(&ci, 0, cnt);
			if (en == NULL)
				goto error;
			*extents = en;
		}
		if (histo) {
			hn = BATconstant(0, TYPE_lng, &(lng){1}, cnt, TRANSIENT);
			if (hn == NULL)
				goto error;
			*histo = hn;
		}
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
			  ",g=" ALGOOPTBATFMT ",e=" ALGOOPTBATFMT
			  ",h=" ALGOOPTBATFMT ",subsorted=%s -> groups="
			  ALGOOPTBATFMT ",extents=" ALGOOPTBATFMT
			  ",histo=" ALGOOPTBATFMT " (1 element per group -- "
			  LLFMT " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), ALGOOPTBATPAR(g),
			  ALGOOPTBATPAR(e), ALGOOPTBATPAR(h),
			  subsorted ? "true" : "false", ALGOOPTBATPAR(gn),
			  ALGOOPTBATPAR(en), ALGOOPTBATPAR(hn), GDKusec() - t0);
		return GDK_SUCCEED;
	}
	assert(!BATtdense(b));
	if (g) {
		if (BATtdense(g))
			maxgrp = g->tseqbase + BATcount(g);
		else if (BATtordered(g))
			maxgrp = * (oid *) Tloc(g, BATcount(g) - 1);
		else {
			prop = BATgetprop(g, GDK_MAX_VALUE);
			if (prop)
				maxgrp = prop->val.oval;
			else if (BATordered(g) && BATordered_rev(g))
				maxgrp = 0;
		}
		if (maxgrp == 0)
			g = NULL; /* single group */
		else
			grps = (const oid *) Tloc(g, 0);
	}
	if (BATordered(b) && BATordered_rev(b)) {
		/* all values are equal */
		if (g == NULL || (BATordered(g) && BATordered_rev(g))) {
			/* there's only a single group: 0 */
			gn = BATconstant(hseqb, TYPE_oid, &(oid){0}, cnt, TRANSIENT);
			if (gn == NULL)
				goto error;
			*groups = gn;
			if (extents) {
				en = BATdense(0, canditer_next(&ci), 1);
				if (en == NULL)
					goto error;
				*extents = en;
			}
			if (histo) {
				hn = BATconstant(0, TYPE_lng, &(lng){(lng)cnt}, 1, TRANSIENT);
				if (hn == NULL)
					goto error;
				*histo = hn;
			}
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
				  ",g=" ALGOOPTBATFMT ",e=" ALGOOPTBATFMT
				  ",h=" ALGOOPTBATFMT ",subsorted=%s -> groups="
				  ALGOOPTBATFMT ",extents=" ALGOOPTBATFMT
				  ",histo=" ALGOOPTBATFMT " (single group -- "
				  LLFMT " usec)\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s),
				  ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
				  ALGOOPTBATPAR(h),
				  subsorted ? "true" : "false",
				  ALGOOPTBATPAR(gn), ALGOOPTBATPAR(en),
				  ALGOOPTBATPAR(hn), GDKusec() - t0);
			return GDK_SUCCEED;
		}
		if ((extents == NULL || e != NULL) &&
		    (histo == NULL || h != NULL) &&
		    cnt == BATcount(b)) {
			/* inherit given grouping; note that if
			 * extents/histo is to be returned, we need
			 * e/h available in order to copy them,
			 * otherwise we will need to calculate them
			 * which we will do using the "normal" case */
			gn = COLcopy(g, g->ttype, false, TRANSIENT);
			if (gn == NULL)
				goto error;
			if (!is_oid_nil(maxgrp)) {
				prop = BATgetprop(g, GDK_MAX_VALUE);
				if (prop)
					BATsetprop(gn, GDK_MAX_VALUE, TYPE_oid, &maxgrp);
				prop = BATgetprop(g, GDK_MAX_POS);
				if (prop)
					BATsetprop(gn, GDK_MAX_POS, TYPE_oid, &prop->val.oval);
			}

			*groups = gn;
			if (extents) {
				en = COLcopy(e, e->ttype, false, TRANSIENT);
				if (en == NULL)
					goto error;
				*extents = en;
			}
			if (histo) {
				hn = COLcopy(h, h->ttype, false, TRANSIENT);
				if (hn == NULL)
					goto error;
				*histo = hn;
			}
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
				  ",g=" ALGOOPTBATFMT ",e=" ALGOOPTBATFMT
				  ",h=" ALGOOPTBATFMT ",subsorted=%s -> groups="
				  ALGOOPTBATFMT ",extents=" ALGOOPTBATFMT
				  ",histo=" ALGOOPTBATFMT " (copy groups -- "
				  LLFMT " usec)\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s),
				  ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
				  ALGOOPTBATPAR(h),
				  subsorted ? "true" : "false",
				  ALGOOPTBATPAR(gn), ALGOOPTBATPAR(en),
				  ALGOOPTBATPAR(hn), GDKusec() - t0);
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
	MT_rwlock_rdlock(&b->thashlock);
	if (b->thash && b->thash != (Hash *) 1)
		maxgrps = b->thash->nunique;
	else if ((prop = BATgetprop_nolock(b, GDK_NUNIQUE)) != NULL)
		maxgrps = prop->val.oval;
	else
		maxgrps = cnt / 10;
	MT_rwlock_rdunlock(&b->thashlock);
	if (!is_oid_nil(maxgrp) && maxgrps < maxgrp)
		maxgrps += maxgrp;
	if (e && maxgrps < BATcount(e))
		maxgrps += BATcount(e);
	if (h && maxgrps < BATcount(h))
		maxgrps += BATcount(h);
	if (maxgrps < GROUPBATINCR)
		maxgrps = GROUPBATINCR;
	if (b->twidth <= 2)
		maxgrps = (BUN) 1 << (8 * b->twidth);
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

	hseqb = b->hseqbase;	/* abbreviation */

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
		algomsg = "compare consecutive values -- ";
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

		gn->tsorted = true;
		*groups = gn;
	} else if (BATordered(b) || BATordered_rev(b)) {
		BUN i, j;
		BUN *pgrp;

		assert(g);	/* if g == NULL or if there is a single */
		assert(grps);	/* group, we used the code above */
		/* for each value, we need to scan all previous equal
		 * values (a consecutive, possibly empty, range) to
		 * see if we can find one in the same old group
		 *
		 * we do this by maintaining for each old group the
		 * last time we saw that group, so if the last time we
		 * saw the old group of the current value is within
		 * this range, we can reuse the new group */
		algomsg = "subscan old groups -- ";
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

		gn->tsorted = true; /* be optimistic */

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

		algomsg = "byte-sized groups -- ";
		if (bgrps == NULL)
			goto error;
		memset(bgrps, 0xFF, 256);
		if (histo)
			memset(cnts, 0, maxgrps * sizeof(lng));
		ngrp = 0;
		gn->tsorted = true;
		for (r = 0; r < cnt; r++) {
			oid o = canditer_next(&ci);
			p = o - b->hseqbase;
			if ((v = bgrps[w[p]]) == 0xFF && ngrp < 256) {
				bgrps[w[p]] = v = (unsigned char) ngrp++;
				maxgrppos = r;
				if (extents)
					exts[v] = o;
			}
			ngrps[r] = v;
			if (r > 0 && v < ngrps[r - 1])
				gn->tsorted = false;
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
		const unsigned short *restrict w = (const unsigned short *) Tloc(b, 0);
		unsigned short v;

		algomsg = "short-sized groups -- ";
		if (sgrps == NULL)
			goto error;
		memset(sgrps, 0xFF, 65536 * sizeof(short));
		if (histo)
			memset(cnts, 0, maxgrps * sizeof(lng));
		ngrp = 0;
		gn->tsorted = true;
		for (r = 0; r < cnt; r++) {
			oid o = canditer_next(&ci);
			p = o - b->hseqbase;
			if ((v = sgrps[w[p]]) == 0xFFFF && ngrp < 65536) {
				sgrps[w[p]] = v = (unsigned short) ngrp++;
				maxgrppos = r;
				if (extents)
					exts[v] = o;
			}
			ngrps[r] = v;
			if (r > 0 && v < ngrps[r - 1])
				gn->tsorted = false;
			if (histo)
				cnts[v]++;
		}
		GDKfree(sgrps);
	} else if (g == NULL &&
		   (BATcheckhash(b) ||
		    (!b->batTransient &&
		     BAThash(b) == GDK_SUCCEED) ||
		    (/* DISABLES CODE */ (0) &&
		     (parent = VIEWtparent(b)) != 0 &&
		     BATcheckhash(BBPdescriptor(parent))))) {
		/* we already have a hash table on b, or b is
		 * persistent and we could create a hash table, or b
		 * is a view on a bat that already has a hash table;
		 * but don't do this if we're checking for subgroups
		 * since we may have to go through long lists of
		 * duplicates in the hash table to find an old
		 * group */
		bool phash = false;
		algomsg = "existing hash -- ";
		if (b->thash == NULL &&
		    /* DISABLES CODE */ (0) &&
		    (parent = VIEWtparent(b)) != 0) {
			/* b is a view on another bat (b2 for now).
			 * calculate the bounds [lo, lo+BATcount(b))
			 * in the parent that b uses */
			BAT *b2 = BBPdescriptor(parent);
			lo = b->tbaseoff - b2->tbaseoff;
			b = b2;
			bi = bat_iterator(b);
			algomsg = "existing parent hash -- ";
			phash = true;
		}
		hs = b->thash;
		gn->tsorted = true; /* be optimistic */

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
		case TYPE_uuid:
			GRP_use_existing_hash_table_tpe(uuid);
			break;
		default:
			GRP_use_existing_hash_table_any();
			break;
		}
	} else {
		bool gc = g != NULL && (BATordered(g) || BATordered_rev(g));
		const char *nme;
		BUN prb;
		int bits = 0;
		BUN nbucket;
		oid grp;

		GDKclrerr();	/* not interested in BAThash errors */

		/* not sorted, and no pre-existing hash table: we'll
		 * build an incomplete hash table on the fly--also see
		 * BATassertProps for similar code; we also exploit if
		 * g is clustered */
		algomsg = "new partial hash -- ";
		nme = GDKinmemory(b->theap->farmid) ? ":memory:" : BBP_physical(b->batCacheid);
		if (grps && !gc) {
			/* we manipulate the hash value after having
			 * calculated it, and when doing that, we
			 * assume the mask (i.e. nbucket-1) is a
			 * power-of-two minus one, so make sure it
			 * is */
			nbucket = cnt | cnt >> 1;
			nbucket |= nbucket >> 2;
			nbucket |= nbucket >> 4;
			nbucket |= nbucket >> 8;
			nbucket |= nbucket >> 16;
#if SIZEOF_BUN == 8
			nbucket |= nbucket >> 32;
#endif
			nbucket++;
			/* nbucket is a power of two, so pop(nbucket - 1)
			 * tells us which power of two */
			bits = 8 * SIZEOF_OID - pop(nbucket - 1);
		} else {
			nbucket = MAX(HASHmask(cnt), 1 << 16);
		}
		switch (t) {
		case TYPE_bte:
			nbucket = 256;
			break;
		case TYPE_sht:
			nbucket = 65536;
			break;
		default:
			break;
		}
		if ((hs = GDKzalloc(sizeof(Hash))) == NULL ||
		    (hs->heaplink.farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0 ||
		    (hs->heapbckt.farmid = BBPselectfarm(TRANSIENT, b->ttype, hashheap)) < 0) {
			GDKfree(hs);
			hs = NULL;
			GDKerror("cannot allocate hash table\n");
			goto error;
		}
		if (snprintf(hs->heaplink.filename, sizeof(hs->heaplink.filename), "%s.thshgrpl%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs->heaplink.filename) ||
		    snprintf(hs->heapbckt.filename, sizeof(hs->heapbckt.filename), "%s.thshgrpb%x", nme, (unsigned) THRgettid()) >= (int) sizeof(hs->heapbckt.filename) ||
		    HASHnew(hs, b->ttype, BUNlast(b), nbucket, BUN_NONE, false) != GDK_SUCCEED) {
			GDKfree(hs);
			hs = NULL;
			GDKerror("cannot allocate hash table\n");
			goto error;
		}
		gn->tsorted = true; /* be optimistic */

		switch (t) {
		case TYPE_bte:
			if (grps && !is_oid_nil(maxgrp)
#if SIZEOF_OID == SIZEOF_LNG
			    && maxgrp < ((oid) 1 << (SIZEOF_LNG * 8 - 8))
#endif
				) {
				ulng v;
				const bte *w = (bte *) Tloc(b, 0);
				GRP_create_partial_hash_table_core(
					(void) 0,
					(v = ((ulng)grps[r]<<8)|(unsigned char)w[p], hash_lng(hs, &v)),
					w[p] == w[hb] && grps[r] == grps[q],
					(void) 0,
					NOGRPTST);
			} else
				GRP_create_partial_hash_table_tpe(bte);
			break;
		case TYPE_sht:
			if (grps && !is_oid_nil(maxgrp)
#if SIZEOF_OID == SIZEOF_LNG
			    && maxgrp < ((oid) 1 << (SIZEOF_LNG * 8 - 16))
#endif
				) {
				ulng v;
				const sht *w = (sht *) Tloc(b, 0);
				GRP_create_partial_hash_table_core(
					(void) 0,
					(v = ((ulng)grps[r]<<16)|(unsigned short)w[p], hash_lng(hs, &v)),
					w[p] == w[hb] && grps[r] == grps[q],
					(void) 0,
					NOGRPTST);
			} else
				GRP_create_partial_hash_table_tpe(sht);
			break;
		case TYPE_int:
			if (grps && !is_oid_nil(maxgrp)
#if SIZEOF_OID == SIZEOF_LNG
			    && maxgrp < ((oid) 1 << (SIZEOF_LNG * 8 - 32))
#endif
				) {
				ulng v;
				const int *w = (int *) Tloc(b, 0);
				GRP_create_partial_hash_table_core(
					(void) 0,
					(v = ((ulng)grps[r]<<32)|(unsigned int)w[p], hash_lng(hs, &v)),
					w[p] == w[hb] && grps[r] == grps[q],
					(void) 0,
					NOGRPTST);
			} else
				GRP_create_partial_hash_table_tpe(int);
			break;
		case TYPE_lng:
#ifdef HAVE_HGE
			if (grps) {
				uhge v;
				const lng *w = (lng *) Tloc(b, 0);
				GRP_create_partial_hash_table_core(
					(void) 0,
					(v = ((uhge)grps[r]<<64)|(ulng)w[p], hash_hge(hs, &v)),
					w[p] == w[hb] && grps[r] == grps[q],
					(void) 0,
					NOGRPTST);
			} else
#endif
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
		case TYPE_uuid:
			GRP_create_partial_hash_table_tpe(uuid);
			break;
		default:
			GRP_create_partial_hash_table_any();
		}

		HEAPfree(&hs->heapbckt, true);
		HEAPfree(&hs->heaplink, true);
		GDKfree(hs);
	}
	if (extents) {
		BATsetcount(en, (BUN) ngrp);
		en->tkey = true;
		en->tsorted = true;
		en->trevsorted = ngrp == 1;
		en->tnonil = true;
		en->tnil = false;
		*extents = virtualize(en);
	}
	if (histo) {
		BATsetcount(hn, (BUN) ngrp);
		if (ngrp == cnt || ngrp == 1) {
			hn->tkey = ngrp == 1;
			hn->tsorted = true;
			hn->trevsorted = true;
		} else {
			hn->tkey = false;
			hn->tsorted = false;
			hn->trevsorted = false;
		}
		hn->tnonil = true;
		hn->tnil = false;
		*histo = hn;
	}
	gn->tkey = ngrp == BATcount(gn);
	gn->trevsorted = ngrp == 1 || BATcount(gn) <= 1;
	gn->tnonil = true;
	gn->tnil = false;
	ngrp--;	     /* max value is one less than number of values */
	BATsetprop(gn, GDK_MAX_VALUE, TYPE_oid, &ngrp);
	BATsetprop(gn, GDK_MAX_POS, TYPE_oid, &(oid){maxgrppos});
	*groups = gn;
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  ",g=" ALGOOPTBATFMT ",e=" ALGOOPTBATFMT
		  ",h=" ALGOOPTBATFMT ",subsorted=%s -> groups="
		  ALGOOPTBATFMT ",extents=" ALGOOPTBATFMT
		  ",histo=" ALGOOPTBATFMT " (%s" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(g), ALGOOPTBATPAR(e),
		  ALGOOPTBATPAR(h),
		  subsorted ? "true" : "false",
		  ALGOOPTBATPAR(gn), ALGOOPTBATPAR(en),
		  ALGOOPTBATPAR(hn), algomsg, GDKusec() - t0);
	return GDK_SUCCEED;
  error:
	if (hs != NULL && hs != b->thash) {
		HEAPfree(&hs->heaplink, true);
		HEAPfree(&hs->heapbckt, true);
		GDKfree(hs);
	}
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
	return BATgroup_internal(groups, extents, histo, b, s, g, e, h, false);
}
