/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* This file is included multiple times.  We expect the tokens SWAP,
 * GDKqsort_impl, LE, and LT to be redefined each time. */

/* This is an implementation of quicksort with a number of extra
 * tweaks and optimizations.  This function is an adaptation to fit
 * into the MonetDB mould from the original version by Bentley &
 * McIlroy from "Engineering a Sort Function".  Hence the following
 * copyright notice.  Comments in the code are mine (Sjoerd
 * Mullender). */

/*
 * Copyright (c) 1992, 1993
 *      The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *      This product includes software developed by the University of
 *      California, Berkeley and its contributors.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

static void
GDKqsort_impl(struct qsort_t *buf, char *restrict h, char *restrict t, size_t n)
{
	size_t a, b, c, d;
	size_t r;
	int swap_cnt;
#ifdef INITIALIZER
	INITIALIZER;
#endif

  loop:
	if (n < 7) {
		/* insertion sort for very small chunks */
		for (b = 1; b < n; b++) {
			for (a = b; a > 0 && LE(a, a - 1); a--) {
				SWAP(a, a - 1);
			}
		}
		return;
	}

	/* determine pivot */
	b = n >> 1;		/* small arrays: middle element */
	if (n > 7) {
		/* for larger arrays, take the middle value from the
		 * first, middle, and last */
		a = 0;
		c = n - 1;
		if (n > 40) {
			/* for even larger arrays, take the middle
			 * value of three middle values */
			d = n >> 3;
			a = MED3(a, a + d, a + 2 * d);
			b = MED3(b - d, b, b + d);
			c = MED3(c - 2 * d, c - d, c);
		}
		b = MED3(a, b, c);
	}
	/* move pivot to start */
	if (b != 0)
		SWAP(0, b);

	/* Bentley and McIlroy's implementation of Dijkstra's Dutch
	 * National Flag Problem */
	a = b = 1;
	c = d = n - 1;
	swap_cnt = 0;
	for (;;) {
		while (b <= c && LE(b, 0)) {
			if (EQ(b, 0)) {
				swap_cnt = 1;
				SWAP(a, b);
				a++;
			}
			b++;
		}
		while (b <= c && LE(0, c)) {
			if (EQ(0, c)) {
				swap_cnt = 1;
				SWAP(c, d);
				d--;
			}
			c--;
		}
		if (b > c)
			break;
		SWAP(b, c);
		swap_cnt = 1;
		b++;
		c--;
	}
	/* at this point we have:
	 * b == c + 1
	 * [0..a): values equal to pivot (cannot be empty)
	 * [a..b): values less than pivot (can be empty)
	 * [b..d+1): values greater than pivot (can be empty)
	 * [d+1..n): values equal to pivot (can be empty)
	 * i.e., there are b-a values less than the pivot and d-c
	 * values greater than the pivot
	 */

	if (!swap_cnt && n < 1024) {
		/* switch to insertion sort, but only for small chunks */
		for (b = 1; b < n; b++) {
			for (a = b; a > 0 && LE(a, a - 1); a--) {
				SWAP(a, a - 1);
			}
		}
		return;
	}

	/* move initial values equal to the pivot to the middle */
	r = MIN(a, b - a);
	multi_SWAP(0, b - r, r);
	/* move final values equal to the pivot to the middle */
	r = MIN(d - c, n - d - 1);
	multi_SWAP(b, n - r, r);
	/* at this point we have:
	 * b == c + 1
	 * [0..b-a): values less than pivot (to be sorted)
	 * [b-a..n-(d-c)): values equal to pivot (in place)
	 * [n-(d-c)..n): values larger than pivot (to be sorted)
	 */

	/* use recursion for smaller of the two subarrays, loop back
	 * to start for larger of the two */
	if (b - a < d - c) {
		if ((r = b - a) > 1) {
			/* sort values less than pivot */
			GDKqsort_impl(buf, h, t, r);
		}
		if ((r = d - c) > 1) {
			/* sort values greater than pivot
			 * iterate rather than recurse */
			h += (n - r) * buf->hs;
			if (t && buf->ts)
				t += (n - r) * buf->ts;
			n = r;
			goto loop;
		}
	} else {
		if ((r = d - c) > 1) {
			/* sort values greater than pivot */
			GDKqsort_impl(buf, h + (n - r) * buf->hs,
				      t ? t + (n - r) * buf->ts : NULL,
				      r);
		}
		if ((r = b - a) > 1) {
			/* sort values less than pivot
			 * iterate rather than recurse */
			n = r;
			goto loop;
		}
	}
}
