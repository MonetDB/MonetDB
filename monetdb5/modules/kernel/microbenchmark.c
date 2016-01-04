/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a stefan manegold
 * @+
 * The microbenchmark routines are primarilly used to create a
 * simple database for testing the performance of core routines.
 * It was originally developed in the context of the Radix Cluster
 * activities.
 * @f microbenchmark
 */
#include "monetdb_config.h"
#include <mal.h>
#include <math.h>
#include <mal_exception.h>
#include "microbenchmark.h"

#ifdef STATIC_CODE_ANALYSIS
#define rand()		0
#endif

static gdk_return
BATrandom(BAT **bn, oid *base, wrd *size, int *domain, int seed)
{
	BUN n = (BUN) * size;
	BAT *b = NULL;
	BUN p, q;

	if (*size > (wrd)BUN_MAX) {
		GDKerror("BATrandom: size must not exceed BUN_MAX");
		return GDK_FAIL;
	}

	if (*size < 0) {
		GDKerror("BATrandom: size must not be negative");
		return GDK_FAIL;
	}

	b = BATnew(TYPE_void, TYPE_int, n, TRANSIENT);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = 1;
		b->trevsorted = 0;
		b->hsorted = 1;
		b->hrevsorted = 0;
		b->tdense = FALSE;
		b->hdense = TRUE;
		BATseqbase(b, *base);
		BATkey(b, TRUE);
		BATkey(BATmirror(b), TRUE);
		*bn = b;
		return GDK_SUCCEED;
	}

	BATsetcount(b, n);
	/* create BUNs with random distribution */
	if (seed != int_nil)
		srand(seed);
	if (*domain == int_nil) {
		BATloop(b, p, q) {
			*(int *) Tloc(b, p) = rand();
		}
#if RAND_MAX < 46340	    /* 46340*46340 = 2147395600 < INT_MAX */
	} else if (*domain > RAND_MAX + 1) {
		BATloop(b, p, q) {
			*(int *) Tloc(b, p) = (rand() * (RAND_MAX + 1) + rand()) % *domain;
		}
#endif
	} else {
		BATloop(b, p, q) {
			*(int *) Tloc(b, p) = rand() % *domain;
		}
	}

	b->hsorted = 1;
	b->hrevsorted = 0;
	b->hdense = TRUE;
	BATseqbase(b, *base);
	BATkey(b, TRUE);
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->tdense = FALSE;
	BATkey(BATmirror(b), FALSE);
	*bn = b;
	return GDK_SUCCEED;
}

static gdk_return
BATuniform(BAT **bn, oid *base, wrd *size, int *domain)
{
	BUN n = (BUN) * size, i, r;
	BAT *b = NULL;
	BUN firstbun, p, q;
	int j = 0;

	if (*size > (wrd)BUN_MAX) {
		GDKerror("BATuniform: size must not exceed BUN_MAX");
		return GDK_FAIL;
	}

	if (*size < 0) {
		GDKerror("BATuniform: size must not be negative");
		return GDK_FAIL;
	}

	b = BATnew(TYPE_void, TYPE_int, n, TRANSIENT);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = 1;
		b->trevsorted = 0;
		b->hsorted = 1;
		b->hrevsorted = 0;
		b->tdense = FALSE;
		b->hdense = TRUE;
		BATseqbase(b, *base);
		BATkey(b, TRUE);
		BATkey(BATmirror(b), TRUE);
		*bn = b;
		return GDK_SUCCEED;
	}

	firstbun = BUNfirst(b);
	BATsetcount(b, n);
	/* create BUNs with uniform distribution */
	BATloop(b, p, q) {
		*(int *) Tloc(b, p) = j;
		if (++j >= *domain)
			j = 0;
	}

	/* mix BUNs randomly */
	for (r = i = 0; i < n; i++) {
		BUN idx = i + ((r += (BUN) rand()) % (n - i));
		int val;

		p = firstbun + i;
		q = firstbun + idx;
		val = *(int *) Tloc(b, p);
		*(int *) Tloc(b, p) = *(int *) Tloc(b, q);
		*(int *) Tloc(b, q) = val;
	}
	b->hsorted = 1;
	b->hrevsorted = 0;
	b->hdense = TRUE;
	BATseqbase(b, *base);
	BATkey(b, TRUE);
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->tdense = FALSE;
	BATkey(BATmirror(b), *size <= *domain);
	*bn = b;
	return GDK_SUCCEED;
}

static gdk_return
BATskewed(BAT **bn, oid *base, wrd *size, int *domain, int *skew)
{
	BUN n = (BUN) * size, i, r;
	BAT *b = NULL;
	BUN firstbun, lastbun, p, q;

	BUN skewedSize;
	int skewedDomain;

	if (*size > (wrd)BUN_MAX) {
		GDKerror("BATskewed: size must not exceed BUN_MAX = " BUNFMT, BUN_MAX);
		return GDK_FAIL;
	}

	if (*size < 0) {
		GDKerror("BATskewed: size must not be negative");
		return GDK_FAIL;
	}

	if (*skew > 100 || *skew < 0) {
		GDKerror("BATskewed: skew must be between 0 and 100");
		return GDK_FAIL;
	}

	b = BATnew(TYPE_void, TYPE_int, n, TRANSIENT);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = 1;
		b->trevsorted = 0;
		b->hsorted = 1;
		b->hrevsorted = 0;
		b->tdense = FALSE;
		b->hdense = TRUE;
		BATseqbase(b, *base);
		BATkey(b, TRUE);
		BATkey(BATmirror(b), TRUE);
		*bn = b;
		return GDK_SUCCEED;
	}

	firstbun = BUNfirst(b);
	BATsetcount(b, n);
	/* create BUNs with skewed distribution */
	skewedSize = ((*skew) * n)/100;
	skewedDomain = ((100-(*skew)) * (*domain))/100;

	lastbun = firstbun + skewedSize;
	for(p=firstbun; p <lastbun; p++)
		*(int *) Tloc(b, p) = (int)rand() % skewedDomain;

	lastbun = BUNlast(b);
	for(; p <lastbun; p++)
		*(int *) Tloc(b, p) = ((int)rand() % (*domain-skewedDomain)) + skewedDomain;

	/* mix BUNs randomly */

	for (r = i = 0; i < n; i++) {
		BUN idx = i + ((r += (BUN) rand()) % (n - i));
		int val;

		p = firstbun + i;
		q = firstbun + idx;
		val = *(int *) Tloc(b, p);
		*(int *) Tloc(b, p) = *(int *) Tloc(b, q);
		*(int *) Tloc(b, q) = val;
	}
	b->hsorted = 1;
	b->hrevsorted = 0;
	b->hdense = TRUE;
	BATseqbase(b, *base);
	BATkey(b, TRUE);
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->tdense = FALSE;
	BATkey(BATmirror(b), *size <= *domain);
	*bn = b;
	return GDK_SUCCEED;
}


/* math.h files do not have M_PI/M_E defined */
#ifndef M_PI
# define M_PI		3.14159265358979323846	/* pi */
#endif
#ifndef M_E
# define M_E		2.7182818284590452354	/* e */
#endif

static gdk_return
BATnormal(BAT **bn, oid *base, wrd *size, int *domain, int *stddev, int *mean)
{
	BUN n = (BUN) * size, i;
	unsigned int r = (unsigned int) n;
	BUN d = (BUN) (*domain < 0 ? 0 : *domain);
	BAT *b = NULL;
	BUN firstbun, p, q;
	int m = *mean, s = *stddev;
	int *itab;
	flt *ftab, tot = 0.0;

	if (*size > (wrd)BUN_MAX) {
		GDKerror("BATnormal: size must not exceed BUN_MAX");
		return GDK_FAIL;
	}

	if (*size < 0) {
		GDKerror("BATnormal: size must not be negative");
		return GDK_FAIL;
	}

	b = BATnew(TYPE_void, TYPE_int, n, TRANSIENT);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = 1;
		b->trevsorted = 0;
		b->hsorted = 1;
		b->hrevsorted = 0;
		b->tdense = FALSE;
		b->hdense = TRUE;
		BATseqbase(b, *base);
		BATkey(b, TRUE);
		BATkey(BATmirror(b), TRUE);
		*bn = b;
		return GDK_SUCCEED;
	}

	firstbun = BUNfirst(b);
	itab = (int *) GDKmalloc(d * sizeof(int));
	ftab = (flt *) itab;

	/* assert(0 <= *mean && *mean < *size); */

	/* created inverted table */
	for (i = 0; i < d; i++) {
		dbl tmp = (dbl) ((i - m) * (i - m));

		tmp = pow(M_E, -tmp / (2 * s * s)) / sqrt(2 * M_PI * s * s);
		ftab[i] = (flt) tmp;
		tot += ftab[i];
	}
	for (tot = (flt) (1.0 / tot), i = 0; i < d; i++) {
		itab[i] = (int) ((flt) n * ftab[i] * tot);
		r -= itab[i];
	}
	itab[m] += r;

	BATsetcount(b, n);
	/* create BUNs with normal distribution */
	BATloop(b, p, q) {
		while (itab[r] == 0)
			r++;
		itab[r]--;
		*(int *) Tloc(b, p) = (int) r;
	}
	GDKfree(itab);

	/* mix BUNs randomly */
	for (r = 0, i = 0; i < n; i++) {
		BUN idx = i + (BUN) ((r += (unsigned int) rand()) % (n - i));
		int val;

		p = firstbun + i;
		q = firstbun + idx;
		val = *(int *) Tloc(b, p);
		*(int *) Tloc(b, p) = *(int *) Tloc(b, q);
		*(int *) Tloc(b, q) = val;
	}
	b->hsorted = 1;
	b->hrevsorted = 0;
	b->hdense = TRUE;
	BATseqbase(b, *base);
	BATkey(b, TRUE);
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->tdense = FALSE;
	BATkey(BATmirror(b), n<2);
	*bn = b;
	return GDK_SUCCEED;
}
/*
 * @-
 * The M5 wrapper code
 */

str
MBMrandom(bat *ret, oid *base, wrd *size, int *domain){
	return MBMrandom_seed ( ret, base, size, domain, &int_nil );
}

str
MBMrandom_seed(bat *ret, oid *base, wrd *size, int *domain, const int *seed){
	BAT *bn = NULL;

	BATrandom(&bn, base, size, domain, *seed);
	if( bn ){
		if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.random", OPERATION_FAILED);
	return MAL_SUCCEED;
}


str
MBMuniform(bat *ret, oid *base, wrd *size, int *domain){
	BAT *bn = NULL;

	BATuniform(&bn, base, size, domain);
	if( bn ){
		if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
MBMnormal(bat *ret, oid *base, wrd *size, int *domain, int *stddev, int *mean){
	BAT *bn = NULL;
	BATnormal(&bn, base, size, domain, stddev, mean);
	if( bn ){
		if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}


str
MBMmix(bat *bn, bat *batid)
{
	BUN n, r, i;
	BUN firstbun, p, q;
	BAT *b;

	if ((b = BATdescriptor(*batid)) == NULL)
                throw(MAL, "microbenchmark.mix", RUNTIME_OBJECT_MISSING);

	n = BATcount(b);
	firstbun = BUNfirst(b);
	/* mix BUNs randomly */
	for (r = i = 0; i < n; i++) {
		BUN idx = i + ((r += (BUN) rand()) % (n - i));
		int val;

		p = firstbun + i;
		q = firstbun + idx;
		val = *(int *) Tloc(b, p);
		*(int *) Tloc(b, p) = *(int *) Tloc(b, q);
		*(int *) Tloc(b, q) = val;
	}

	BBPunfix(b->batCacheid);
	*bn = b->batCacheid;

	return MAL_SUCCEED;
}

str
MBMskewed(bat *ret, oid *base, wrd *size, int *domain, int *skew){
	BAT *bn = NULL;

	BATskewed(&bn, base, size, domain, skew);
	if( bn ){
		if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark,uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}
