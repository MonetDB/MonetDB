/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
	const char *heap;
	int hs;
	int ts;
	void *restrict bh;
	void *restrict bt;
	/* Temporary storage for a single entry. If an entry is at
	 * most 2 lng's, we don't need to allocate anything. */
	void *th;
	void *tt;
#ifdef HAVE_HGE
	hge tempstorageh[1];	/* 16 bytes should be wide enough ... */
	hge tempstoraget[1];	/* ... for all our fixed-sized data */
#else
	lng tempstorageh[2];	/* 16 bytes should be wide enough ... */
	lng tempstoraget[2];	/* ... for all our fixed-sized data */
#endif

	/* This controls when we get *into* galloping mode.  It's
	 * initialized to MIN_GALLOP.  merge_lo and merge_hi tend to
	 * nudge it higher for random data, and lower for highly
	 * structured data. */
	ssize_t min_gallop;

	/* 'ah' and 'at' are temp storage to help with merges.  They
	 * contain room for alloced[ht] entries. */
	void *ah;
	ssize_t allocedh;
	void *at;
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
merge_getmem(MergeState *ms, ssize_t need, void **ap,
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
	merge_freemem(ms);	/* reset to sane state */
	return -1;
}

#define MERGE_GETMEMH(MS, NEED)						\
	((NEED) * (MS)->hs <= (MS)->allocedh ? 0 :			\
	 merge_getmem(MS, NEED, &(MS)->ah, &(MS)->allocedh, (MS)->hs,	\
		      (MS)->temparrayh))
#define MERGE_GETMEMT(MS, NEED)						\
	((NEED) * (MS)->ts <= (MS)->allocedt ? 0 :			\
	 merge_getmem(MS, NEED, &(MS)->at, &(MS)->allocedt, (MS)->ts,	\
		      (MS)->temparrayt))

#define PTRADD(p, n, w)		((void *) ((char *) (p) + (n) * (w)))

#define COPY_bte(d,s,w)		(* (bte *) (d) = * (bte *) (s))
#define COPY_sht(d,s,w)		(* (sht *) (d) = * (sht *) (s))
#define COPY_int(d,s,w)		(* (int *) (d) = * (int *) (s))
#define COPY_lng(d,s,w)		(* (lng *) (d) = * (lng *) (s))
#ifdef HAVE_HGE
#define COPY_hge(d,s,w)		(* (hge *) (d) = * (hge *) (s))
#endif
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
			for (i = 0; i < N; i++)				\
				((bte*)(d))[i] = ((bte*)(s))[i];	\
			break;						\
		case sizeof(sht):					\
			for (i = 0; i < N; i++)				\
				((sht*)(d))[i] = ((sht*)(s))[i];	\
			break;						\
		case sizeof(int):					\
			for (i = 0; i < N; i++)				\
				((int*)(d))[i] = ((int*)(s))[i];	\
			break;						\
		case sizeof(lng):					\
			for (i = 0; i < N; i++)				\
				((lng*)(d))[i] = ((lng*)(s))[i];	\
			break;						\
		case 2 * sizeof(lng):					\
			for (i = 0; i < N*2; i++)			\
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
#ifdef HAVE_HGE
#define ISLT_hge(X, Y, ms)	(* (hge *) (X) < * (hge *) (Y))
#endif
#define ISLT_flt(X, Y, ms)	(* (flt *) (X) < * (flt *) (Y))
#define ISLT_dbl(X, Y, ms)	(* (dbl *) (X) < * (dbl *) (Y))
#define ISLT_oid(X, Y, ms)	(* (oid *) (X) < * (oid *) (Y))
#define ISLT_bte_rev(X, Y, ms)	(* (bte *) (X) > * (bte *) (Y))
#define ISLT_sht_rev(X, Y, ms)	(* (sht *) (X) > * (sht *) (Y))
#define ISLT_int_rev(X, Y, ms)	(* (int *) (X) > * (int *) (Y))
#define ISLT_lng_rev(X, Y, ms)	(* (lng *) (X) > * (lng *) (Y))
#ifdef HAVE_HGE
#define ISLT_hge_rev(X, Y, ms)	(* (hge *) (X) > * (hge *) (Y))
#endif
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


#define COPY		COPY_bte

#define binarysort	binarysort_bte
#define do_ssort	do_ssort_bte
#define gallop_left	gallop_left_bte
#define gallop_right	gallop_right_bte
#define ISLT		ISLT_bte
#define merge_at	merge_at_bte
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#define binarysort	binarysort_bte_rev
#define do_ssort	do_ssort_bte_rev
#define gallop_left	gallop_left_bte_rev
#define gallop_right	gallop_right_bte_rev
#define ISLT		ISLT_bte_rev
#define merge_at	merge_at_bte_rev
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#undef COPY

#define COPY		COPY_sht

#define binarysort	binarysort_sht
#define do_ssort	do_ssort_sht
#define gallop_left	gallop_left_sht
#define gallop_right	gallop_right_sht
#define ISLT		ISLT_sht
#define merge_at	merge_at_sht
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#define binarysort	binarysort_sht_rev
#define do_ssort	do_ssort_sht_rev
#define gallop_left	gallop_left_sht_rev
#define gallop_right	gallop_right_sht_rev
#define ISLT		ISLT_sht_rev
#define merge_at	merge_at_sht_rev
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#undef COPY

#define COPY		COPY_int

#define binarysort	binarysort_int
#define do_ssort	do_ssort_int
#define gallop_left	gallop_left_int
#define gallop_right	gallop_right_int
#define ISLT		ISLT_int
#define merge_at	merge_at_int
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#define binarysort	binarysort_int_rev
#define do_ssort	do_ssort_int_rev
#define gallop_left	gallop_left_int_rev
#define gallop_right	gallop_right_int_rev
#define ISLT		ISLT_int_rev
#define merge_at	merge_at_int_rev
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#undef COPY

#define COPY		COPY_lng

#define binarysort	binarysort_lng
#define do_ssort	do_ssort_lng
#define gallop_left	gallop_left_lng
#define gallop_right	gallop_right_lng
#define ISLT		ISLT_lng
#define merge_at	merge_at_lng
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#define binarysort	binarysort_lng_rev
#define do_ssort	do_ssort_lng_rev
#define gallop_left	gallop_left_lng_rev
#define gallop_right	gallop_right_lng_rev
#define ISLT		ISLT_lng_rev
#define merge_at	merge_at_lng_rev
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#undef COPY

#ifdef HAVE_HGE
#define COPY		COPY_hge

#define binarysort	binarysort_hge
#define do_ssort	do_ssort_hge
#define gallop_left	gallop_left_hge
#define gallop_right	gallop_right_hge
#define ISLT		ISLT_hge
#define merge_at	merge_at_hge
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#define binarysort	binarysort_hge_rev
#define do_ssort	do_ssort_hge_rev
#define gallop_left	gallop_left_hge_rev
#define gallop_right	gallop_right_hge_rev
#define ISLT		ISLT_hge_rev
#define merge_at	merge_at_hge_rev
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#undef COPY
#endif

#define COPY		COPY_flt

#define binarysort	binarysort_flt
#define do_ssort	do_ssort_flt
#define gallop_left	gallop_left_flt
#define gallop_right	gallop_right_flt
#define ISLT		ISLT_flt
#define merge_at	merge_at_flt
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#define binarysort	binarysort_flt_rev
#define do_ssort	do_ssort_flt_rev
#define gallop_left	gallop_left_flt_rev
#define gallop_right	gallop_right_flt_rev
#define ISLT		ISLT_flt_rev
#define merge_at	merge_at_flt_rev
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#undef COPY

#define COPY		COPY_dbl

#define binarysort	binarysort_dbl
#define do_ssort	do_ssort_dbl
#define gallop_left	gallop_left_dbl
#define gallop_right	gallop_right_dbl
#define ISLT		ISLT_dbl
#define merge_at	merge_at_dbl
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#define binarysort	binarysort_dbl_rev
#define do_ssort	do_ssort_dbl_rev
#define gallop_left	gallop_left_dbl_rev
#define gallop_right	gallop_right_dbl_rev
#define ISLT		ISLT_dbl_rev
#define merge_at	merge_at_dbl_rev
#include "gdk_ssort_impl.h"
#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#undef COPY

#define COPY		COPY_any

#define binarysort	binarysort_any
#define do_ssort	do_ssort_any
#define gallop_left	gallop_left_any
#define gallop_right	gallop_right_any
#define ISLT		ISLT_any
#define merge_at	merge_at_any

#define GDKssortimpl	GDKssort

#include "gdk_ssort_impl.h"

#undef GDKssortimpl

#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#define binarysort	binarysort_any_rev
#define do_ssort	do_ssort_any_rev
#define gallop_left	gallop_left_any_rev
#define gallop_right	gallop_right_any_rev
#define ISLT		ISLT_any_rev
#define merge_at	merge_at_any_rev

#define GDKssortimpl	GDKssort_rev
#define do_ssort_bte	do_ssort_bte_rev
#define do_ssort_sht	do_ssort_sht_rev
#define do_ssort_int	do_ssort_int_rev
#define do_ssort_lng	do_ssort_lng_rev
#ifdef HAVE_HGE
#define do_ssort_hge	do_ssort_hge_rev
#endif
#define do_ssort_flt	do_ssort_flt_rev
#define do_ssort_dbl	do_ssort_dbl_rev
#define do_ssort_any	do_ssort_any_rev

#include "gdk_ssort_impl.h"

#undef GDKssortimpl
#undef do_ssort_bte
#undef do_ssort_sht
#undef do_ssort_int
#undef do_ssort_lng
#ifdef HAVE_HGE
#undef do_ssort_hge
#endif
#undef do_ssort_flt
#undef do_ssort_dbl
#undef do_ssort_any

#undef binarysort
#undef do_ssort
#undef gallop_left
#undef gallop_right
#undef ISLT
#undef merge_at

#undef COPY
