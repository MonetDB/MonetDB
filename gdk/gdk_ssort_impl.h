/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* This file is included multiple times.  We expect a bunch of tokens
 * to be redefined differently each time (see gdk_ssort.c).  If the
 * token GDKssortimpl is defined, the main interface is defined.
 */

/*
 * This file implements a stable sort algorithm known as "timsort".
 * The algorithm is a straight copy of the listsort function in the
 * Python 2.5 source code, heavily modified to fit into the MonetDB
 * environment.
 * The original author of the sort algorithm was Tim Peters, the
 * adaptation was done by Sjoerd Mullender.
 */

/* binarysort is the best method for sorting small arrays: it does few
 * compares, but can do data movement quadratic in the number of
 * elements.
 * [lo, hi) is a contiguous slice of a list, and is sorted via binary
 * insertion.  This sort is stable. On entry, must have lo <= start <=
 * hi, and that [lo, start) is already sorted (pass start == lo if you
 * don't know!). */
static void
binarysort(size_t lo, size_t hi, size_t start, MergeState *ms)
{
	register size_t l, p, r;

	assert(lo <= start && start <= hi);
	/* assert [lo, start) is sorted */
	if (lo == start)
		start++;
	/* [lo,start) is sorted, insert start (the pivot) into this
	 * area, finding its position using binary search. */
	for (; start < hi; start++) {
		/* set l to where start belongs */
		l = lo;
		r = start;
		/* ms->t[ht] is the pivot */
		COPY(ms->th, PTRADD(ms->bh, r, ms->hs), ms->hs);
		COPY_any(ms->tt, PTRADD(ms->bt, r, ms->ts), ms->ts);
		/* Invariants:
		 * pivot >= all in [lo, l).
		 * pivot < all in [r, start).
		 * The second is vacuously true at the start. */
		assert(l < r);
		do {
			p = l + ((r - l) >> 1);
			if (ISLT(ms->th, PTRADD(ms->bh, p, ms->hs), ms))
				r = p;
			else
				l = p + 1;
		} while (l < r);
		assert(l == r);
		/* The invariants still hold, so pivot >= all in [lo,
		 * l) and pivot < all in [l, start), so pivot belongs
		 * at l.  Note that if there are elements equal to
		 * pivot, l points to the first slot after them --
		 * that's why this sort is stable. Slide over to make
		 * room.
		 * Caution: using memmove is much slower under MSVC 5;
		 * we're not usually moving many slots. */
		for (p = start, r = p - 1; p > l; p = r, r = p - 1) {
			COPY(PTRADD(ms->bh, p, ms->hs),
			     PTRADD(ms->bh, r, ms->hs), ms->hs);
			COPY_any(PTRADD(ms->bt, p, ms->ts),
				 PTRADD(ms->bt, r, ms->ts), ms->ts);
		}
		COPY(PTRADD(ms->bh, l, ms->hs), ms->th, ms->hs);
		COPY_any(PTRADD(ms->bt, l, ms->ts), ms->tt, ms->ts);
	}
}

/* Locate the proper position of key in a sorted vector; if the vector
 * contains an element equal to key, return the position immediately
 * to the left of the leftmost equal element.  [gallop_right() does
 * the same except returns the position to the right of the rightmost
 * equal element (if any).]
 *
 * "a" is a sorted vector with n elements, starting at a[0].  n must
 * be > 0.
 *
 * "hint" is an index at which to begin the search, 0 <= hint < n.
 * The closer hint is to the final result, the faster this runs.
 *
 * The return value is the int k in 0..n such that
 *
 * a[k-1] < key <= a[k]
 *
 * pretending that *(a-1) is minus infinity and a[n] is plus infinity.
 * IOW, key belongs at index k; or, IOW, the first k elements of a
 * should precede key, and the last n-k should follow key.
 *
 * Returns -1 on error.  See listsort.txt for info on the method. */
static ssize_t
gallop_left(void *key, void *a, ssize_t n, ssize_t hint, MergeState *ms)
{
	ssize_t ofs;
	ssize_t lastofs;
	ssize_t k;

	assert(key && a && n > 0 && hint >= 0 && hint < n);

	a = PTRADD(a, hint, ms->hs);
	lastofs = 0;
	ofs = 1;
	if (ISLT(a, key, ms)) {
		/* a[hint] < key -- gallop right, until
		 * a[hint + lastofs] < key <= a[hint + ofs] */
		const ssize_t maxofs = n - hint;	/* &a[n-1] is highest */
		while (ofs < maxofs) {
			if (ISLT(PTRADD(a, ofs, ms->hs), key, ms)) {
				lastofs = ofs;
				ofs = (ofs << 1) + 1;
				if (ofs <= 0)	/* int overflow */
					ofs = maxofs;
			} else	/* key <= a[hint + ofs] */
				break;
		}
		if (ofs > maxofs)
			ofs = maxofs;
		/* Translate back to offsets relative to &a[0]. */
		lastofs += hint;
		ofs += hint;
	} else {
		/* key <= a[hint] -- gallop left, until
		 * a[hint - ofs] < key <= a[hint - lastofs] */
		const ssize_t maxofs = hint + 1;	/* &a[0] is lowest */
		while (ofs < maxofs) {
			if (ISLT(PTRADD(a, -ofs, ms->hs), key, ms))
				break;
			/* key <= a[hint - ofs] */
			lastofs = ofs;
			ofs = (ofs << 1) + 1;
			if (ofs <= 0)	/* int overflow */
				ofs = maxofs;
		}
		if (ofs > maxofs)
			ofs = maxofs;
		/* Translate back to positive offsets relative to &a[0]. */
		k = lastofs;
		lastofs = hint - ofs;
		ofs = hint - k;
	}
	a = PTRADD(a, -hint, ms->hs);

	assert(-1 <= lastofs && lastofs < ofs && ofs <= n);
	/* Now a[lastofs] < key <= a[ofs], so key belongs somewhere to
	 * the right of lastofs but no farther right than ofs.  Do a
	 * binary search, with invariant a[lastofs-1] < key <=
	 * a[ofs]. */
	++lastofs;
	while (lastofs < ofs) {
		ssize_t m = lastofs + ((ofs - lastofs) >> 1);

		if (ISLT(PTRADD(a, m, ms->hs), key, ms))
			lastofs = m + 1; /* a[m] < key */
		else
			ofs = m;	/* key <= a[m] */
	}
	assert(lastofs == ofs);		/* so a[ofs-1] < key <= a[ofs] */
	return ofs;
}

/* Exactly like gallop_left(), except that if key already exists in
 * a[0:n], finds the position immediately to the right of the
 * rightmost equal value.
 *
 * The return value is the int k in 0..n such that
 *
 * a[k-1] <= key < a[k]
 *
 * or -1 if error.
 *
 * The code duplication is massive, but this is enough different given
 * that we're sticking to "<" comparisons that it's much harder to
 * follow if written as one routine with yet another "left or right?"
 * flag. */
static ssize_t
gallop_right(void *key, void *a, ssize_t n, ssize_t hint, MergeState *ms)
{
	ssize_t ofs;
	ssize_t lastofs;
	ssize_t k;

	assert(key && a && n > 0 && hint >= 0 && hint < n);

	a = PTRADD(a, hint, ms->hs);
	lastofs = 0;
	ofs = 1;
	if (ISLT(key, a, ms)) {
		/* key < a[hint] -- gallop left, until
		 * a[hint - ofs] <= key < a[hint - lastofs] */
		const ssize_t maxofs = hint + 1;	/* &a[0] is lowest */
		while (ofs < maxofs) {
			if (ISLT(key, PTRADD(a, -ofs, ms->hs), ms)) {
				lastofs = ofs;
				ofs = (ofs << 1) + 1;
				if (ofs <= 0)	/* int overflow */
					ofs = maxofs;
			} else	/* a[hint - ofs] <= key */
				break;
		}
		if (ofs > maxofs)
			ofs = maxofs;
		/* Translate back to positive offsets relative to &a[0]. */
		k = lastofs;
		lastofs = hint - ofs;
		ofs = hint - k;
	} else {
		/* a[hint] <= key -- gallop right, until
		 * a[hint + lastofs] <= key < a[hint + ofs] */
		const ssize_t maxofs = n - hint;	/* &a[n-1] is highest */
		while (ofs < maxofs) {
			if (ISLT(key, PTRADD(a, ofs, ms->hs), ms))
				break;
			/* a[hint + ofs] <= key */
			lastofs = ofs;
			ofs = (ofs << 1) + 1;
			if (ofs <= 0)	/* int overflow */
				ofs = maxofs;
		}
		if (ofs > maxofs)
			ofs = maxofs;
		/* Translate back to offsets relative to &a[0]. */
		lastofs += hint;
		ofs += hint;
	}
	a = PTRADD(a, -hint, ms->hs);

	assert(-1 <= lastofs && lastofs < ofs && ofs <= n);
	/* Now a[lastofs] <= key < a[ofs], so key belongs somewhere to
	 * the right of lastofs but no farther right than ofs.  Do a
	 * binary search, with invariant a[lastofs-1] <= key <
	 * a[ofs]. */
	++lastofs;
	while (lastofs < ofs) {
		ssize_t m = lastofs + ((ofs - lastofs) >> 1);

		if (ISLT(key, PTRADD(a, m, ms->hs), ms))
			ofs = m;	/* key < a[m] */
		else
			lastofs = m+1;	/* a[m] <= key */
	}
	assert(lastofs == ofs);		/* so a[ofs-1] <= key < a[ofs] */
	return ofs;
}

/* Merge the two runs at stack indices i and i+1.
 * Returns 0 on success, -1 on error. */
static ssize_t
merge_at(MergeState *ms, ssize_t i)
{
	size_t pa, pb;
	ssize_t na, nb;
	ssize_t k;

	assert(ms != NULL);
	assert(ms->n >= 2);
	assert(i >= 0);
	assert(i == ms->n - 2 || i == ms->n - 3);

	pa = ms->pending[i].base;
	na = ms->pending[i].len;
	pb = ms->pending[i + 1].base;
	nb = ms->pending[i + 1].len;
	assert(na > 0 && nb > 0);
	assert(pa + na == pb);

	/* Record the length of the combined runs; if i is the
	 * 3rd-last run now, also slide over the last run (which isn't
	 * involved in this merge).  The current run i+1 goes away in
	 * any case. */
	ms->pending[i].len = na + nb;
	if (i == ms->n - 3)
		ms->pending[i + 1] = ms->pending[i + 2];
	--ms->n;

	/* Where does b start in a?  Elements in a before that can be
	 * ignored (already in place). */
	k = gallop_right(PTRADD(ms->bh, pb, ms->hs),
			 PTRADD(ms->bh, pa, ms->hs), na, 0, ms);
	pa += k;
	na -= k;
	if (na == 0)
		return 0;

	/* Where does a end in b?  Elements in b after that can be
	 * ignored (already in place). */
	nb = gallop_left(PTRADD(ms->bh, pa + na - 1, ms->hs),
			 PTRADD(ms->bh, pb, ms->hs), nb, nb-1, ms);
	if (nb <= 0)
		return nb;

	/* Merge what remains of the runs, using a temp array with
	 * min(na, nb) elements. */
	if (na <= nb) {
/* Merge the na elements starting at pa with the nb elements starting
 * at pb in a stable way, in-place.  na and nb must be > 0, and pa +
 * na == pb. Must also have that *pb < *pa, that pa[na-1] belongs at
 * the end of the merge, and should have na <= nb.  See listsort.txt
 * for more info. Return 0 if successful, -1 if error. */
		size_t dest;
		ssize_t min_gallop = ms->min_gallop;

		assert(ms && na > 0 && nb > 0 && pa + na == pb);
		if (MERGE_GETMEMH(ms, na) < 0)
			return -1;
		if (MERGE_GETMEMT(ms, na) < 0)
			return -1;
		COPY_anyN(ms->ah, PTRADD(ms->bh, pa, ms->hs), ms->hs, na);
		COPY_anyN(ms->at, PTRADD(ms->bt, pa, ms->ts), ms->ts, na);
		dest = pa;
		pa = 0;

		COPY(PTRADD(ms->bh, dest, ms->hs),
		     PTRADD(ms->bh, pb, ms->hs), ms->hs);
		COPY_any(PTRADD(ms->bt, dest, ms->ts),
			 PTRADD(ms->bt, pb, ms->ts), ms->ts);
		dest++;
		pb++;
		--nb;
		if (nb == 0)
			goto SucceedA;
		if (na == 1)
			goto CopyB;

		for (;;) {
			ssize_t acount = 0;	/* # of times A won in a row */
			ssize_t bcount = 0;	/* # of times B won in a row */

			/* Do the straightforward thing until (if
			 * ever) one run appears to win
			 * consistently. */
			for (;;) {
				assert(na > 1 && nb > 0);
				k = ISLT(PTRADD(ms->bh, pb, ms->hs),
					 PTRADD(ms->ah, pa, ms->hs), ms);
				if (k) {
					COPY(PTRADD(ms->bh, dest, ms->hs),
					     PTRADD(ms->bh, pb, ms->hs),
					     ms->hs);
					COPY_any(PTRADD(ms->bt, dest, ms->ts),
						 PTRADD(ms->bt, pb, ms->ts),
						 ms->ts);
					dest++;
					pb++;
					++bcount;
					acount = 0;
					--nb;
					if (nb == 0)
						goto SucceedA;
					if (bcount >= min_gallop)
						break;
				} else {
					COPY(PTRADD(ms->bh, dest, ms->hs),
					     PTRADD(ms->ah, pa, ms->hs),
					     ms->hs);
					COPY_any(PTRADD(ms->bt, dest, ms->ts),
						 PTRADD(ms->at, pa, ms->ts),
						 ms->ts);
					dest++;
					pa++;
					++acount;
					bcount = 0;
					--na;
					if (na == 1)
						goto CopyB;
					if (acount >= min_gallop)
						break;
				}
			}

			/* One run is winning so consistently that
			 * galloping may be a huge win.  So try that,
			 * and continue galloping until (if ever)
			 * neither run appears to be winning
			 * consistently anymore. */
			++min_gallop;
			do {
				assert(na > 1 && nb > 0);
				min_gallop -= min_gallop > 1;
				ms->min_gallop = min_gallop;
				k = gallop_right(PTRADD(ms->bh, pb, ms->hs),
						 PTRADD(ms->ah, pa, ms->hs),
						 na, 0, ms);
				acount = k;
				if (k) {
					COPY_anyN(PTRADD(ms->bh, dest, ms->hs),
						  PTRADD(ms->ah, pa, ms->hs),
						  ms->hs, k);
					COPY_anyN(PTRADD(ms->bt, dest, ms->ts),
						  PTRADD(ms->at, pa, ms->ts),
						  ms->ts, k);
					dest += k;
					pa += k;
					na -= k;
					if (na == 1)
						goto CopyB;
					/* na==0 is impossible now if
					 * the comparison function is
					 * consistent, but we can't
					 * assume that it is. */
					if (na == 0)
						goto SucceedA;
				}
				COPY(PTRADD(ms->bh, dest, ms->hs),
				     PTRADD(ms->bh, pb, ms->hs), ms->hs);
				COPY_any(PTRADD(ms->bt, dest, ms->ts),
					 PTRADD(ms->bt, pb, ms->ts), ms->ts);
				dest++;
				pb++;
				--nb;
				if (nb == 0)
					goto SucceedA;

				k = gallop_left(PTRADD(ms->ah, pa, ms->hs),
						PTRADD(ms->bh, pb, ms->hs),
						nb, 0, ms);
				bcount = k;
				if (k) {
					memmove(PTRADD(ms->bh, dest, ms->hs),
						PTRADD(ms->bh, pb, ms->hs),
						k * ms->hs);
					memmove(PTRADD(ms->bt, dest, ms->ts),
						PTRADD(ms->bt, pb, ms->ts),
						k * ms->ts);
					dest += k;
					pb += k;
					nb -= k;
					if (nb == 0)
						goto SucceedA;
				}
				COPY(PTRADD(ms->bh, dest, ms->hs),
				     PTRADD(ms->ah, pa, ms->hs), ms->hs);
				COPY_any(PTRADD(ms->bt, dest, ms->ts),
					 PTRADD(ms->at, pa, ms->ts), ms->ts);
				dest++;
				pa++;
				--na;
				if (na == 1)
					goto CopyB;
			} while (acount >= MIN_GALLOP || bcount >= MIN_GALLOP);
			++min_gallop;	/* penalize it for leaving galloping mode */
			ms->min_gallop = min_gallop;
		}
	SucceedA:
		if (na) {
			COPY_anyN(PTRADD(ms->bh, dest, ms->hs),
				  PTRADD(ms->ah, pa, ms->hs), ms->hs, na);
			COPY_anyN(PTRADD(ms->bt, dest, ms->ts),
				  PTRADD(ms->at, pa, ms->ts), ms->ts, na);
		}
		return 0;
	CopyB:
		assert(na == 1 && nb > 0);
		/* The last element of pa belongs at the end of the merge. */
		memmove(PTRADD(ms->bh, dest, ms->hs),
			PTRADD(ms->bh, pb, ms->hs), nb * ms->hs);
		memmove(PTRADD(ms->bt, dest, ms->ts),
			PTRADD(ms->bt, pb, ms->ts), nb * ms->ts);
		COPY(PTRADD(ms->bh, dest + nb, ms->hs),
		     PTRADD(ms->ah, pa, ms->hs), ms->hs);
		COPY_any(PTRADD(ms->bt, dest + nb, ms->ts),
			 PTRADD(ms->at, pa, ms->ts), ms->ts);
		return 0;
	} else {
/* Merge the na elements starting at pa with the nb elements starting
 * at pb in a stable way, in-place.  na and nb must be > 0, and pa +
 * na == pb. Must also have that *pb < *pa, that pa[na-1] belongs at
 * the end of the merge, and should have na >= nb.  See listsort.txt
 * for more info. Return 0 if successful, -1 if error. */
		size_t dest;
		size_t basea;
		size_t baseb;
		ssize_t min_gallop = ms->min_gallop;

		assert(ms && na > 0 && nb > 0 && pa + na == pb);
		if (MERGE_GETMEMH(ms, nb) < 0)
			return -1;
		if (MERGE_GETMEMT(ms, nb) < 0)
			return -1;
		dest = pb + nb - 1;
		COPY_anyN(ms->ah, PTRADD(ms->bh, pb, ms->hs), ms->hs, nb);
		COPY_anyN(ms->at, PTRADD(ms->bt, pb, ms->ts), ms->ts, nb);
		basea = pa;
		baseb = 0;
		pb = nb - 1;
		pa += na - 1;

		COPY(PTRADD(ms->bh, dest, ms->hs),
		     PTRADD(ms->bh, pa, ms->hs), ms->hs);
		COPY_any(PTRADD(ms->bt, dest, ms->ts),
			 PTRADD(ms->bt, pa, ms->ts), ms->ts);
		dest--;
		pa--;
		--na;
		if (na == 0)
			goto SucceedB;
		if (nb == 1)
			goto CopyA;

		for (;;) {
			ssize_t acount = 0;	/* # of times A won in a row */
			ssize_t bcount = 0;	/* # of times B won in a row */

			/* Do the straightforward thing until (if
			 * ever) one run appears to win
			 * consistently. */
			for (;;) {
				assert(na > 0 && nb > 1);
				k = ISLT(PTRADD(ms->ah, pb, ms->hs),
					 PTRADD(ms->bh, pa, ms->hs), ms);
				if (k) {
					COPY(PTRADD(ms->bh, dest, ms->hs),
					     PTRADD(ms->bh, pa, ms->hs),
					     ms->hs);
					COPY_any(PTRADD(ms->bt, dest, ms->ts),
						 PTRADD(ms->bt, pa, ms->ts),
						 ms->ts);
					dest--;
					pa--;
					++acount;
					bcount = 0;
					--na;
					if (na == 0)
						goto SucceedB;
					if (acount >= min_gallop)
						break;
				} else {
					COPY(PTRADD(ms->bh, dest, ms->hs),
					     PTRADD(ms->ah, pb, ms->hs),
					     ms->hs);
					COPY_any(PTRADD(ms->bt, dest, ms->ts),
						 PTRADD(ms->at, pb, ms->ts),
						 ms->ts);
					dest--;
					pb--;
					++bcount;
					acount = 0;
					--nb;
					if (nb == 1)
						goto CopyA;
					if (bcount >= min_gallop)
						break;
				}
			}

			/* One run is winning so consistently that
			 * galloping may be a huge win.  So try that,
			 * and continue galloping until (if ever)
			 * neither run appears to be winning
			 * consistently anymore. */
			++min_gallop;
			do {
				assert(na > 0 && nb > 1);
				min_gallop -= min_gallop > 1;
				ms->min_gallop = min_gallop;
				k = gallop_right(PTRADD(ms->ah, pb, ms->hs),
						 PTRADD(ms->bh, basea, ms->hs),
						 na, na - 1, ms);
				k = na - k;
				acount = k;
				if (k) {
					dest -= k;
					pa -= k;
					memmove(PTRADD(ms->bh, dest + 1,
						       ms->hs),
						PTRADD(ms->bh, pa + 1, ms->hs),
						k * ms->hs);
					memmove(PTRADD(ms->bt, dest + 1,
						       ms->ts),
						PTRADD(ms->bt, pa + 1, ms->ts),
						k * ms->ts);
					na -= k;
					if (na == 0)
						goto SucceedB;
				}
				COPY(PTRADD(ms->bh, dest, ms->hs),
				     PTRADD(ms->ah, pb, ms->hs), ms->hs);
				COPY_any(PTRADD(ms->bt, dest, ms->ts),
					 PTRADD(ms->at, pb, ms->ts), ms->ts);
				dest--;
				pb--;
				--nb;
				if (nb == 1)
					goto CopyA;

				k = gallop_left(PTRADD(ms->bh, pa, ms->hs),
						PTRADD(ms->ah, baseb, ms->hs),
						nb, nb - 1, ms);
				k = nb - k;
				bcount = k;
				if (k) {
					dest -= k;
					pb -= k;
					memmove(PTRADD(ms->bh, dest + 1,
						       ms->hs),
						PTRADD(ms->ah, pb + 1, ms->hs),
						k * ms->hs);
					memmove(PTRADD(ms->bt, dest + 1,
						       ms->ts),
						PTRADD(ms->at, pb + 1, ms->ts),
						k * ms->ts);
					nb -= k;
					if (nb == 1)
						goto CopyA;
					/* nb==0 is impossible now if
					 * the comparison function is
					 * consistent, but we can't
					 * assume that it is. */
					if (nb == 0)
						goto SucceedB;
				}
				COPY(PTRADD(ms->bh, dest, ms->hs),
				     PTRADD(ms->bh, pa, ms->hs), ms->hs);
				COPY_any(PTRADD(ms->bt, dest, ms->ts),
					 PTRADD(ms->bt, pa, ms->ts), ms->ts);
				dest--;
				pa--;
				--na;
				if (na == 0)
					goto SucceedB;
			} while (acount >= MIN_GALLOP || bcount >= MIN_GALLOP);
			++min_gallop;	/* penalize it for leaving galloping mode */
			ms->min_gallop = min_gallop;
		}
	SucceedB:
		if (nb) {
			COPY_anyN(PTRADD(ms->bh, dest + 1 - nb, ms->hs),
				  PTRADD(ms->ah, baseb, ms->hs), ms->hs, nb);
			COPY_anyN(PTRADD(ms->bt, dest + 1 - nb, ms->ts),
				  PTRADD(ms->at, baseb, ms->ts), ms->ts, nb);
		}
		return 0;
	CopyA:
		assert(nb == 1 && na > 0);
		/* The first element of pb belongs at the front of the
		 * merge. */
		dest -= na;
		pa -= na;
		memmove(PTRADD(ms->bh, dest + 1, ms->hs),
			PTRADD(ms->bh, pa + 1, ms->hs),
			na * ms->hs);
		memmove(PTRADD(ms->bt, dest + 1, ms->ts),
			PTRADD(ms->bt, pa + 1, ms->ts),
			na * ms->ts);
		COPY(PTRADD(ms->bh, dest, ms->hs),
		     PTRADD(ms->ah, pb, ms->hs), ms->hs);
		COPY_any(PTRADD(ms->bt, dest, ms->ts),
			 PTRADD(ms->at, pb, ms->ts), ms->ts);
		return 0;
	}
}

static int
do_ssort(MergeState *ms, ssize_t nremaining, size_t lo, size_t hi, ssize_t minrun)
{
	do {
		int descending;
		ssize_t n;

		/* Identify next run. */
		{
/* Return the length of the run beginning at lo, in the slice [lo,
 * hi).  lo < hi is required on entry.  "A run" is the longest
 * ascending sequence, with
 *
 * lo[0] <= lo[1] <= lo[2] <= ...
 *
 * or the longest descending sequence, with
 *
 * lo[0] > lo[1] > lo[2] > ...
 *
 * Boolean descending is set to 0 in the former case, or to 1 in the
 * latter.  For its intended use in a stable mergesort, the strictness
 * of the defn of "descending" is needed so that the caller can safely
 * reverse a descending sequence without violating stability (strict >
 * ensures there are no equal elements to get out of order). */
			size_t nlo;
			size_t olo;

			assert(lo < hi);
			descending = 0;
			olo = lo;
			nlo = lo + 1;
			if (nlo == hi) {
				n = 1;
			} else {
				n = 2;
				if (ISLT(PTRADD(ms->bh, nlo, ms->hs),
					 PTRADD(ms->bh, olo, ms->hs), ms)) {
					descending = 1;
					for (olo = nlo++;
					     nlo < hi;
					     olo = nlo++, ++n) {
						if (!ISLT(PTRADD(ms->bh, nlo,
								 ms->hs),
							  PTRADD(ms->bh, olo,
								 ms->hs), ms))
							break;
					}
				}
				else {
					for (olo = nlo++;
					     nlo < hi;
					     olo = nlo++, ++n) {
						if (ISLT(PTRADD(ms->bh, nlo,
								ms->hs),
							 PTRADD(ms->bh, olo,
								ms->hs), ms))
							break;
					}
				}
			}
		}
		if (descending)
			reverse_slice(lo, lo + n, ms);
		/* If short, extend to min(minrun, nremaining). */
		if (n < minrun) {
			ssize_t force = nremaining <= minrun ? nremaining : minrun;

			binarysort(lo, lo + force, lo + n, ms);
			n = force;
		}
		/* Push run onto pending-runs stack, and maybe merge. */
		assert(ms->n < MAX_MERGE_PENDING);
		ms->pending[ms->n].base = lo;
		ms->pending[ms->n].len = n;
		ms->n++;
		{
/* Examine the stack of runs waiting to be merged, merging adjacent
 * runs until the stack invariants are re-established:
 *
 * 1. len[-3] > len[-2] + len[-1]
 * 2. len[-2] > len[-1]
 *
 * See listsort.txt for more info.
 *
 * Returns 0 on success, -1 on error. */
			struct slice *p = ms->pending;

			while (ms->n > 1) {
				ssize_t i = ms->n - 2;

				if ((i > 0 &&
				     p[i-1].len <= p[i].len + p[i+1].len) ||
				    (i > 1 &&
				     p[i-2].len <= p[i-1].len + p[i].len)) {
					if (p[i - 1].len < p[i + 1].len)
						--i;
					if (merge_at(ms, i) < 0)
						return -1;
				} else if (p[i].len <= p[i + 1].len) {
					if (merge_at(ms, i) < 0)
						return -1;
				} else
					break;
			}
		}
		/* Advance to find next run. */
		lo += n;
		nremaining -= n;
	} while (nremaining > 0);
	assert(lo == hi);

	{
/* Regardless of invariants, merge all runs on the stack until only
 * one remains.  This is used at the end of the mergesort.
 *
 * Returns 0 on success, -1 on error. */
		struct slice *p = ms->pending;

		while (ms->n > 1) {
			ssize_t n = ms->n - 2;

			if (n > 0 && p[n - 1].len < p[n + 1].len)
				--n;
			if (merge_at(ms, n) < 0)
				return -1;
		}
	}
	return 0;
}

#ifdef GDKssortimpl
/* Stable sort an array "h" (and move t accordingly).
 * "nitems" is the number of items to sort; "hs"+"ts" is the size of
 * the items, "tpe" is the type of the key within the items. If "heap"
 * is non-NULL, the key is actually an offset relative to "heap" and
 * the actual key is found at that offset (MonetDB var-sized
 * atoms). */
gdk_return
GDKssortimpl(void *h, void *t, const void *heap, size_t nitems,
	     int hs, int ts, int tpe)
{
	char temp;
	MergeState ms;
	ssize_t nremaining;
	gdk_return result = GDK_FAIL;
	size_t lo, hi;
	ssize_t minrun;

	assert(h);
	assert(hs > 0);

	ms.ah = (void *) ms.temparrayh;
	ms.allocedh = MERGESTATE_TEMP_SIZE;
	ms.at = (void *) ms.temparrayt;
	ms.allocedt = MERGESTATE_TEMP_SIZE;
	ms.n = 0;
	ms.min_gallop = MIN_GALLOP;
	ms.compare = ATOMcompare(tpe);
	ms.heap = heap;
	ms.hs = hs;
	ms.ts = ts;
	ms.bh = h;
	if (!t)
		t = &temp;
	ms.bt = t;
	ms.th = ms.tempstorageh;
	ms.tt = ms.tempstoraget;
	assert((size_t) hs <= sizeof(ms.tempstorageh));
	assert((size_t) ts <= sizeof(ms.tempstoraget));
	nremaining = (ssize_t) nitems;

	if (nremaining < 2)
		goto succeed;

	tpe = ATOMbasetype(tpe);

	/* March over the array once, left to right, finding natural
	 * runs, and extending short natural runs to minrun
	 * elements. */
	lo = 0;
	hi = lo + nremaining;
	minrun = merge_compute_minrun(nremaining);
	switch (tpe) {
	case TYPE_bte:
		if (do_ssort_bte(&ms, nremaining, lo, hi, minrun) < 0)
			goto fail;
		break;
	case TYPE_sht:
		if (do_ssort_sht(&ms, nremaining, lo, hi, minrun) < 0)
			goto fail;
		break;
	case TYPE_int:
		if (do_ssort_int(&ms, nremaining, lo, hi, minrun) < 0)
			goto fail;
		break;
	case TYPE_lng:
		if (do_ssort_lng(&ms, nremaining, lo, hi, minrun) < 0)
			goto fail;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (do_ssort_hge(&ms, nremaining, lo, hi, minrun) < 0)
			goto fail;
		break;
#endif
	case TYPE_flt:
		if (do_ssort_flt(&ms, nremaining, lo, hi, minrun) < 0)
			goto fail;
		break;
	case TYPE_dbl:
		if (do_ssort_dbl(&ms, nremaining, lo, hi, minrun) < 0)
			goto fail;
		break;
	default:
		if (do_ssort_any(&ms, nremaining, lo, hi, minrun) < 0)
			goto fail;
		break;
	}
	assert(ms.n == 1);
	assert(ms.pending[0].base == 0);
	assert(ms.pending[0].len == (ssize_t) nitems);

  succeed:
	result = GDK_SUCCEED;
  fail:
	merge_freemem(&ms);
	return result;
}
#endif	/* GDKssortimpl */
