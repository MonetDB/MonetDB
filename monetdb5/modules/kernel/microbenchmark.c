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
BATrandom(BAT **bn, oid *base, lng *size, int *domain, int seed)
{
	const BUN n = (BUN) * size;
	BUN i;
	BAT *b = NULL;
	int *restrict val;

	if (*size > (lng)BUN_MAX) {
		GDKerror("BATrandom: size must not exceed BUN_MAX");
		return GDK_FAIL;
	}

	if (*size < 0) {
		GDKerror("BATrandom: size must not be negative");
		return GDK_FAIL;
	}

	b = COLnew(*base, TYPE_int, n, TRANSIENT);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = 1;
		b->trevsorted = 0;
		b->tdense = FALSE;
		BATkey(b, TRUE);
		*bn = b;
		return GDK_SUCCEED;
	}
	val = (int *) Tloc(b, 0);

	/* create BUNs with random distribution */
	if (seed != int_nil)
		srand(seed);
	if (*domain == int_nil) {
	        for (i = 0; i < n; i++) {
			val[i] = rand();
		}
#if RAND_MAX < 46340	    /* 46340*46340 = 2147395600 < INT_MAX */
	} else if (*domain > RAND_MAX + 1) {
	        for (i = 0; i < n; i++) {
			val[i] = (rand() * (RAND_MAX + 1) + rand()) % *domain;
		}
#endif
	} else {
	        for (i = 0; i < n; i++) {
			val[i] = rand() % *domain;
		}
	}

	BATsetcount(b, n);
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->tdense = FALSE;
	BATkey(b, FALSE);
	*bn = b;
	return GDK_SUCCEED;
}

static gdk_return
BATuniform(BAT **bn, oid *base, lng *size, int *domain)
{
	const BUN n = (BUN) * size;
	BUN i, r;
	BAT *b = NULL;
	int *restrict val;
	int v;

	if (*size > (lng)BUN_MAX) {
		GDKerror("BATuniform: size must not exceed BUN_MAX");
		return GDK_FAIL;
	}

	if (*size < 0) {
		GDKerror("BATuniform: size must not be negative");
		return GDK_FAIL;
	}

	b = COLnew(*base, TYPE_int, n, TRANSIENT);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = 1;
		b->trevsorted = 0;
		b->tdense = FALSE;
		BATkey(b, TRUE);
		*bn = b;
		return GDK_SUCCEED;
	}
	val = (int *) Tloc(b, 0);

	/* create BUNs with uniform distribution */
        for (v = 0, i = 0; i < n; i++) {
		val[i] = v;
		if (++v >= *domain)
			v = 0;
	}

	/* mix BUNs randomly */
	for (r = 0, i = 0; i < n; i++) {
		const BUN j = i + ((r += rand()) % (n - i));
		const int tmp = val[i];

		val[i] = val[j];
		val[j] = tmp;
	}

	BATsetcount(b, n);
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->tdense = FALSE;
	BATkey(b, *size <= *domain);
	*bn = b;
	return GDK_SUCCEED;
}

static gdk_return
BATskewed(BAT **bn, oid *base, lng *size, int *domain, int *skew)
{
	const BUN n = (BUN) * size;
	BUN i, r;
	BAT *b = NULL;
	int *restrict val;
	const BUN skewedSize = ((*skew) * n) / 100;
	const int skewedDomain = ((100 - (*skew)) * (*domain)) / 100;

	if (*size > (lng)BUN_MAX) {
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

	b = COLnew(*base, TYPE_int, n, TRANSIENT);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = 1;
		b->trevsorted = 0;
		b->tdense = FALSE;
		BATkey(b, TRUE);
		*bn = b;
		return GDK_SUCCEED;
	}
	val = (int *) Tloc(b, 0);

	/* create BUNs with skewed distribution */
	for (i = 0; i < skewedSize; i++)
		val[i] = rand() % skewedDomain;
	for( ; i < n; i++)
		val[i] = (rand() % (*domain - skewedDomain)) + skewedDomain;

	/* mix BUNs randomly */
	for (r = 0, i = 0; i < n; i++) {
		const BUN j = i + ((r += rand()) % (n - i));
		const int tmp = val[i];

		val[i] = val[j];
		val[j] = tmp;
	}

	BATsetcount(b, n);
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->tdense = FALSE;
	BATkey(b, *size <= *domain);
	*bn = b;
	return GDK_SUCCEED;
}


/* math.h files do not have M_PI/M_E defined */
#ifndef M_PI
# define M_PI		((dbl) 3.14159265358979323846)	/* pi */
#endif
#ifndef M_E
# define M_E		((dbl) 2.7182818284590452354)	/* e */
#endif

static gdk_return
BATnormal(BAT **bn, oid *base, lng *size, int *domain, int *stddev, int *mean)
{
	const BUN n = (BUN) * size;
	BUN i, r;
	const int d = (*domain < 0 ? 0 : *domain);
	int j;
	BAT *b = NULL;
	int *restrict val;
	const int m = *mean, s = *stddev;
	unsigned int *restrict abs;
	flt *restrict rel;
	dbl tot = 0;
	const dbl s_s_2 = (dbl) s * (dbl) s * 2;
	const dbl s_sqrt_2_pi = ((dbl) s * sqrt(2 * M_PI));

	assert(sizeof(unsigned int) == sizeof(flt));

#if SIZEOF_BUN > 4
	if (n >= ((ulng) 1 << 32)) {
		GDKerror("BATnormal: size must be less than 2^32 = "LLFMT, (lng) 1 << 32);
		return GDK_FAIL;
	}
#endif
	if (*size > (lng)BUN_MAX) {
		GDKerror("BATnormal: size must not exceed BUN_MAX");
		return GDK_FAIL;
	}

	if (*size < 0) {
		GDKerror("BATnormal: size must not be negative");
		return GDK_FAIL;
	}

	b = COLnew(*base, TYPE_int, n, TRANSIENT);
	if (b == NULL) {
		return GDK_FAIL;
        }
	if (n == 0) {
		b->tsorted = 1;
		b->trevsorted = 0;
		b->tdense = FALSE;
		BATkey(b, TRUE);
		*bn = b;
		return GDK_SUCCEED;
	}
	val = (int *) Tloc(b, 0);

	abs = (unsigned int *) GDKmalloc(d * sizeof(unsigned int));
	if (abs == NULL) {
	        BBPreclaim(b);
	        return GDK_FAIL;
	}
	rel = (flt *) abs;

	/* assert(0 <= *mean && *mean < *size); */

	/* created inverted table with rel fraction per value */
	for (tot = 0, j = 0; j < d; j++) {
		const dbl j_m = (dbl) j - m;
		const dbl tmp = j_m * j_m / s_s_2;

		rel[j] = (flt) (pow(M_E, -tmp) / s_sqrt_2_pi);
		tot += rel[j];
	}
	/* just in case we get tot != 1 due to. e.g.,
	 * rounding errors, limited precision, or limited domain */
	tot = 1.0 / tot;
	/* calculate abs count per value from rel fraction */
	for (r = n, j = 0; j < d; j++) {
		assert(((dbl) n * rel[j] * tot) < (dbl) ((lng) 1 << 32));
		abs[j] = (unsigned int) ((dbl) n * rel[j] * tot);
		assert(r >= abs[j]);
		r -= abs[j];
	}
	assert(((ulng) 1 << 32) - r > abs[m]);
	abs[m] += (unsigned int) r;

	/* create BUNs with normal distribution */
	for (j = 0, i = 0; i < n && j < d; i++) {
	        while (j < d && abs[j] == 0)
	                j++;
                if (j < d) {
        	        val[i] = j;
	                abs[j]--;
                }
	}
	assert(i == n);
        while (j < d && abs[j] == 0)
                j++;
	assert(j == d);
	GDKfree(abs);


	BATsetcount(b, n);
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->tdense = FALSE;
	BATkey(b, n<2);
	*bn = b;
	return GDK_SUCCEED;
}
/*
 * @-
 * The M5 wrapper code
 */

str
MBMrandom(bat *ret, oid *base, lng *size, int *domain){
	return MBMrandom_seed ( ret, base, size, domain, &int_nil );
}

str
MBMrandom_seed(bat *ret, oid *base, lng *size, int *domain, const int *seed){
	BAT *bn = NULL;

	BATrandom(&bn, base, size, domain, *seed);
	if( bn ){
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.random", OPERATION_FAILED);
	return MAL_SUCCEED;
}


str
MBMuniform(bat *ret, oid *base, lng *size, int *domain){
	BAT *bn = NULL;

	BATuniform(&bn, base, size, domain);
	if( bn ){
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
MBMnormal(bat *ret, oid *base, lng *size, int *domain, int *stddev, int *mean){
	BAT *bn = NULL;
	BATnormal(&bn, base, size, domain, stddev, mean);
	if( bn ){
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}


str
MBMmix(bat *bn, bat *batid)
{
	BUN n, r, i;
	BAT *b;

	if ((b = BATdescriptor(*batid)) == NULL)
                throw(MAL, "microbenchmark.mix", RUNTIME_OBJECT_MISSING);

	n = BATcount(b);
	/* mix BUNs randomly */
	for (r = i = 0; i < n; i++) {
		BUN idx = i + ((r += (BUN) rand()) % (n - i));
		int val;

		val = *(int *) Tloc(b, i);
		*(int *) Tloc(b, i) = *(int *) Tloc(b, idx);
		*(int *) Tloc(b, idx) = val;
	}

	BBPunfix(b->batCacheid);
	*bn = b->batCacheid;

	return MAL_SUCCEED;
}

str
MBMskewed(bat *ret, oid *base, lng *size, int *domain, int *skew){
	BAT *bn = NULL;

	BATskewed(&bn, base, size, domain, skew);
	if( bn ){
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark,uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}
