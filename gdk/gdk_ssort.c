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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a Sjoerd Mullender
 * @* Ssort
 * This file implements a stable sort algorithm.  The algorithm is a
 * straight copy of the listsort function in the Python 2.5 source code,
 * heavily modified to fit into the MonetDB environment.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* The maximum number of entries in a MergeState's pending-runs
 * stack. This is enough to sort arrays of size up to about
 *
 * 32 * phi ** MAX_MERGE_PENDING
 *
 * where phi ~= 1.618.  85 is ridiculously large enough, good for an
 * array with 2**64 elements. */
#define MAX_MERGE_PENDING 85

/* When we get into galloping mode, we stay there until both runs win
 * less often than MIN_GALLOP consecutive times.  See listsort.txt for
 * more info. */
#define MIN_GALLOP 7

/* Avoid malloc for small temp arrays. */
#define MERGESTATE_TEMP_SIZE (256 * sizeof(void *))

/* One MergeState exists on the stack per invocation of mergesort.
 * It's just a convenient way to pass state around among the helper
 * functions. */
struct slice {
	size_t base;
	ssize_t len;
};

typedef struct {
	/* The comparison function. */
	int (*compare) (const void *, const void *);
	char *heap;
	int hs;
	int ts;
	void *bh;
	void *bt;
	/* Temporary storage for a single entry. If an entry is at
	 * most 2 lng's, we don't need to allocate anything. */
	void *th;
	void *tt;
	lng tempstorageh[2];	/* 16 bytes should be wide enough ... */
	lng tempstoraget[2];	/* ... for all our fixed-sized data */

	/* This controls when we get *into* galloping mode.  It's
	 * initialized to MIN_GALLOP.  merge_lo and merge_hi tend to
	 * nudge it higher for random data, and lower for highly
	 * structured data. */
	ssize_t min_gallop;

	/* 'ah' and 'at' are temp storage to help with merges.  They
	 * contain room for alloced[ht] entries. */
	void **ah;
	ssize_t allocedh;
	void **at;
	ssize_t allocedt;

	/* A stack of n pending runs yet to be merged.  Run #i starts
	 * at address base[i] and extends for len[i] elements.  It's
	 * always true (so long as the indices are in bounds) that
	 *
	 * pending[i].base + pending[i].len == pending[i+1].base
	 *
	 * so we could cut the storage for this, but it's a minor
	 * amount, and keeping all the info explicit simplifies the
	 * code. */
	int n;
	struct slice pending[MAX_MERGE_PENDING];

	/* 'ah' and 'at' point to this when possible, rather than muck
	 * with malloc. */
	char temparrayh[MERGESTATE_TEMP_SIZE];
	char temparrayt[MERGESTATE_TEMP_SIZE];
} MergeState;

/* Free all the temp memory owned by the MergeState.  This must be
 * called when you're done with a MergeState, and may be called before
 * then if you want to free the temp memory early. */
static void
merge_freemem(MergeState *ms)
{
	assert(ms != NULL);
	if (ms->ah != (void *) ms->temparrayh)
		GDKfree(ms->ah);
	ms->ah = (void *) ms->temparrayh;
	ms->allocedh = MERGESTATE_TEMP_SIZE;
	if (ms->at != (void *) ms->temparrayt)
		GDKfree(ms->at);
	ms->at = (void *) ms->temparrayt;
	ms->allocedt = MERGESTATE_TEMP_SIZE;
}

/* Ensure enough temp memory for 'need' array slots is available.
 * Returns 0 on success and -1 if the memory can't be gotten. */
static int
merge_getmem(MergeState *ms, ssize_t need, void ***ap,
	     ssize_t *allocedp, int s, char *temparray)
{
	assert(ms != NULL);
	need *= s;
	if (need <= *allocedp)
		return 0;
	/* Don't realloc!  That can cost cycles to copy the old data,
	 * but we don't care what's in the block. */
	if (*ap != (void *) temparray)
		GDKfree(*ap);
	*ap = GDKmalloc(need);
	if (*ap) {
		*allocedp = need;
		return 0;
	}
	GDKerror("GDKssort: not enough memory\n");
	merge_freemem(ms);	/* reset to sane state */
	return -1;
}

#define MERGE_GETMEMH(MS, NEED)                                         \
	((NEED) * (MS)->hs <= (MS)->allocedh ? 0 :                      \
	 merge_getmem(MS, NEED, &(MS)->ah, &(MS)->allocedh, (MS)->hs,   \
		      (MS)->temparrayh))
#define MERGE_GETMEMT(MS, NEED)                                         \
	((NEED) * (MS)->ts <= (MS)->allocedt ? 0 :                      \
	 merge_getmem(MS, NEED, &(MS)->at, &(MS)->allocedt, (MS)->ts,   \
		      (MS)->temparrayt))

#define PTRADD(p, n, w)		((void *) ((char *) (p) + (n) * (w)))

#define COPY_bte(d,s,w)		(* (bte *) (d) = * (bte *) (s))
#define COPY_sht(d,s,w)		(* (sht *) (d) = * (sht *) (s))
#define COPY_int(d,s,w)		(* (int *) (d) = * (int *) (s))
#define COPY_lng(d,s,w)		(* (lng *) (d) = * (lng *) (s))
#define COPY_flt(d,s,w)		(* (flt *) (d) = * (flt *) (s))
#define COPY_dbl(d,s,w)		(* (dbl *) (d) = * (dbl *) (s))
#define COPY_oid(d,s,w)		(* (oid *) (d) = * (oid *) (s))

#define COPY_any(d,s,w)							\
	do {								\
		switch (w) {						\
		case 0:							\
			break;						\
		case sizeof(bte):					\
			* (bte *) (d) = * (bte *) (s);			\
			break;						\
		case sizeof(sht):					\
			* (sht *) (d) = * (sht *) (s);			\
			break;						\
		case sizeof(int):					\
			* (int *) (d) = * (int *) (s);			\
			break;						\
		case sizeof(lng):					\
			* (lng *) (d) = * (lng *) (s);			\
			break;						\
		case 2 * sizeof(lng):					\
			* (lng *) (d) = * (lng *) (s);			\
			* ((lng *) (d) + 1) = * ((lng *) (s) + 1);	\
			break;						\
		default:						\
			memcpy((d), (s), (size_t) (w));			\
			break;						\
		}							\
	} while (0)

#define COPY_anyN(d,s,w,N)						\
	do {								\
		int i;							\
		switch (w) {						\
		case 0:							\
			break;						\
		case sizeof(bte):					\
			for(i=0; i<N; i++)				\
				((bte*)(d))[i] = ((bte*)(s))[i];	\
			break;						\
		case sizeof(sht):					\
			for(i=0; i<N; i++)				\
				((sht*)(d))[i] = ((sht*)(s))[i];	\
			break;						\
		case sizeof(int):					\
			for(i=0; i<N; i++)				\
				((int*)(d))[i] = ((int*)(s))[i];	\
			break;						\
		case sizeof(lng):					\
			for(i=0; i<N; i++)				\
				((lng*)(d))[i] = ((lng*)(s))[i];	\
			break;						\
		case 2 * sizeof(lng):					\
			for(i=0; i<(N<2); i++)				\
				((lng*)(d))[i] = ((lng*)(s))[i];	\
			break;						\
		default:						\
			memcpy((d), (s), (size_t) (w)*N);		\
			break;						\
		}							\
	} while (0)

#define ISLT_any(X, Y, ms)  (((ms)->heap ? (*(ms)->compare)((ms)->heap + VarHeapVal(X,0,(ms)->hs), (ms)->heap + VarHeapVal(Y,0,(ms)->hs)) : (*(ms)->compare)((X), (Y))) < 0)
#define ISLT_any_rev(X, Y, ms)  (((ms)->heap ? (*(ms)->compare)((ms)->heap + VarHeapVal(X,0,(ms)->hs), (ms)->heap + VarHeapVal(Y,0,(ms)->hs)) : (*(ms)->compare)((X), (Y))) > 0)
#define ISLT_bte(X, Y, ms)	(* (bte *) (X) < * (bte *) (Y))
#define ISLT_sht(X, Y, ms)	(* (sht *) (X) < * (sht *) (Y))
#define ISLT_int(X, Y, ms)	(* (int *) (X) < * (int *) (Y))
#define ISLT_lng(X, Y, ms)	(* (lng *) (X) < * (lng *) (Y))
#define ISLT_flt(X, Y, ms)	(* (flt *) (X) < * (flt *) (Y))
#define ISLT_dbl(X, Y, ms)	(* (dbl *) (X) < * (dbl *) (Y))
#define ISLT_oid(X, Y, ms)	(* (oid *) (X) < * (oid *) (Y))
#define ISLT_bte_rev(X, Y, ms)	(* (bte *) (X) > * (bte *) (Y))
#define ISLT_sht_rev(X, Y, ms)	(* (sht *) (X) > * (sht *) (Y))
#define ISLT_int_rev(X, Y, ms)	(* (int *) (X) > * (int *) (Y))
#define ISLT_lng_rev(X, Y, ms)	(* (lng *) (X) > * (lng *) (Y))
#define ISLT_flt_rev(X, Y, ms)	(* (flt *) (X) > * (flt *) (Y))
#define ISLT_dbl_rev(X, Y, ms)	(* (dbl *) (X) > * (dbl *) (Y))
#define ISLT_oid_rev(X, Y, ms)	(* (oid *) (X) > * (oid *) (Y))

/* Reverse a slice of a list in place, from lo up to (exclusive) hi. */
static void
reverse_slice(size_t lo, size_t hi, MergeState *ms)
{
	void *th, *tt;
	int hs, ts;

	assert(ms);

	th = ms->th;
	tt = ms->tt;
	hs = ms->hs;
	ts = ms->ts;

	hi--;
	while (lo < hi) {
		COPY_any(th, PTRADD(ms->bh, lo, hs), hs);
		COPY_any(PTRADD(ms->bh, lo, hs), PTRADD(ms->bh, hi, hs), hs);
		COPY_any(PTRADD(ms->bh, hi, hs), th, hs);
		COPY_any(tt, PTRADD(ms->bt, lo, ts), ts);
		COPY_any(PTRADD(ms->bt, lo, ts), PTRADD(ms->bt, hi, ts), ts);
		COPY_any(PTRADD(ms->bt, hi, ts), tt, ts);
		lo++;
		hi--;
	}
}

static ssize_t
merge_compute_minrun(ssize_t n)
{
	ssize_t r = 0;		/* becomes 1 if any 1 bits are shifted off */

	assert(n >= 0);
	while (n >= 16) {
		r |= n & 1;
		n >>= 1;
	}
	return n + r;
}


#ifndef NOEXPAND_BTE
#define T bte
#define O
#include "gdk_ssort_impl.h"
#undef O

#define O _rev
#include "gdk_ssort_impl.h"
#undef T
#undef O
#endif

#ifndef NOEXPAND_SHT
#define T sht
#define O
#include "gdk_ssort_impl.h"
#undef O

#define O _rev
#include "gdk_ssort_impl.h"
#undef T
#undef O
#endif

#ifndef NOEXPAND_INT
#define T int
#define O
#include "gdk_ssort_impl.h"
#undef O

#define O _rev
#include "gdk_ssort_impl.h"
#undef T
#undef O
#endif

#ifndef NOEXPAND_LNG
#define T lng
#define O
#include "gdk_ssort_impl.h"
#undef O

#define O _rev
#include "gdk_ssort_impl.h"
#undef T
#undef O
#endif

#ifndef NOEXPAND_FLT
#define T flt
#define O
#include "gdk_ssort_impl.h"
#undef O

#define O _rev
#include "gdk_ssort_impl.h"
#undef T
#undef O
#endif

#ifndef NOEXPAND_DBL
#define T dbl
#define O
#include "gdk_ssort_impl.h"
#undef O

#define O _rev
#include "gdk_ssort_impl.h"
#undef T
#undef O
#endif

#ifndef NOEXPAND_OID
#define T oid
#define O
#include "gdk_ssort_impl.h"
#undef O

#define O _rev
#include "gdk_ssort_impl.h"
#undef T
#undef O
#endif

#define T any
#define O
#define DEFINE_MAIN_FUNC	/* define GDKssort and GDKssort_rev */
#include "gdk_ssort_impl.h"
#undef O

#define O _rev
#include "gdk_ssort_impl.h"
#undef T
#undef O
#undef DEFINE_MAIN_FUNC
