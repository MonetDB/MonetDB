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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a Lefteris Sidirourgos
 * @* Low level sample facilities
 *
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#undef BATsample

#define DRAND ((double)rand()/(double)RAND_MAX)

/*
 * @+ Uniform Sampling.
 *
 * The implementation of the uniform sampling is based on the
 * algorithm A as described in the paper "Faster Methods for Random
 * Sampling" by Jeffrey Scott Vitter. Algorithm A is not the fastest
 * one, but it only makes s calls in function random() and it is
 * simpler than the other more complex and CPU intensive algorithms in
 * the literature.
 *
 * Algorithm A instead of performing one random experiment for each
 * row to decide if it should be included in the sample or not, it
 * skips S rows and includes the S+1 row. The algorithm scans the
 * input relation sequentially and maintains the unique and sort
 * properties. The sample is without replacement.
 */

/* BATsample implements sampling for void headed BATs */
BAT *
BATsample(BAT *b, BUN n)
{
	BAT *bn;
	BUN cnt;

	BATcheck(b, "BATsample");
	assert(BAThdense(b));
	ERRORcheck(n > BUN_MAX, "BATsample: sample size larger than BUN_MAX\n");
	ALGODEBUG fprintf(stderr, "#BATsample: sample " BUNFMT " elements.\n", n);

	cnt = BATcount(b);
	/* empty sample size */
	if (n == 0) {
		bn = BATnew(TYPE_void, TYPE_void, 0);
		BATsetcount(bn, 0);
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), 0);
	/* sample size is larger than the input BAT, return all oids */
	} else if (cnt <= n) {
		bn = BATnew(TYPE_void, TYPE_void, cnt);
		BATsetcount(bn, cnt);
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), b->H->seq);
	} else {
		BUN smp = 0;
		/* we use wrd and not BUN since p may be -1 */
		wrd top = b->hseqbase + cnt - n;
		wrd p = ((wrd) b->hseqbase) - 1;
		oid *o;
		bn = BATnew(TYPE_void, TYPE_oid, n);
		if (bn == NULL) {
			GDKerror("#BATsample: memory allocation error");
			return NULL;
		}
		o = (oid *) Tloc(bn, BUNfirst(bn));
		while (smp < n-1) { /* loop until all but 1 values are sampled */
			double v = DRAND;
			double quot = (double)top/(double)cnt;
			BUN jump = 0;
			while (quot > v) { /* determine how many positions to jump */
				jump++;
				top--;
				cnt--;
				quot *= (double)top/(double)cnt;
			}
			p += (jump+1);
			cnt--;
			o[smp++] = (oid) p;
		}
		/* 1 left */
		p += (BUN) rand() % cnt;
		o[smp] = (oid) p+1;

		/* property management */
		BATsetcount(bn, n);
		bn->trevsorted = bn->batCount <= 1;
		bn->tkey = 1;
		bn->tdense = bn->batCount <= 1;
		if (bn->batCount == 1)
			bn->tseqbase = * (oid *) Tloc(bn, BUNfirst(bn));
		bn->hdense = 1;
		bn->hseqbase = 0;
		bn->hkey = 1;
		bn->hrevsorted = bn->batCount <= 1;
	}

	return bn;
}
