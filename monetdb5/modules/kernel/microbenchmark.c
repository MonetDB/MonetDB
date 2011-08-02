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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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

static int
BATrandom(BAT **bn, oid *base, int *size, int *domain)
{
	BUN n = (BUN) * size;
	BAT *b = NULL;
	BUN p, q;

	if (*size < 0) {
		GDKerror("BATrandom: size must not be negative");
		return GDK_FAIL;
	}

	b = BATnew(TYPE_void, TYPE_int, n);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = GDK_SORTED;
		b->hsorted = GDK_SORTED;
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
	if (*domain == int_nil) {
		BATloop(b, p, q) {
			*(int *) Tloc(b, p) = rand();
		}
	} else {
		BATloop(b, p, q) {
			*(int *) Tloc(b, p) = rand() % *domain;
		}
	}

	b->hsorted = GDK_SORTED;
	b->hdense = TRUE;
	BATseqbase(b, *base);
	BATkey(b, TRUE);
	b->tsorted = FALSE;
	b->tdense = FALSE;
	BATkey(BATmirror(b), FALSE);
	*bn = b;
	return GDK_SUCCEED;
}

static int
BATuniform(BAT **bn, oid *base, int *size, int *domain)
{
	BUN n = (BUN) * size, i, r;
	BAT *b = NULL;
	BUN firstbun, p, q;
	int j = 0;

	if (*size < 0) {
		GDKerror("BATuniform: size must not be negative");
		return GDK_FAIL;
	}

	b = BATnew(TYPE_void, TYPE_int, n);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = GDK_SORTED;
		b->hsorted = GDK_SORTED;
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
	b->hsorted = GDK_SORTED;
	b->hdense = TRUE;
	BATseqbase(b, *base);
	BATkey(b, TRUE);
	b->tsorted = FALSE;
	b->tdense = FALSE;
	BATkey(BATmirror(b), *size <= *domain);
	*bn = b;
	return GDK_SUCCEED;
}

static int
BATskewed(BAT **bn, oid *base, int *size, int *domain, int *skew)
{
	BUN n = (BUN) * size, i, r;
	BAT *b = NULL;
	BUN firstbun, lastbun, p, q;

	int skewedSize;
	int skewedDomain;

	if (*size < 0) {
		GDKerror("BATuniform: size must not be negative");
		return GDK_FAIL;
	}

	b = BATnew(TYPE_void, TYPE_int, n);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = GDK_SORTED;
		b->hsorted = GDK_SORTED;
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
	skewedSize = ((*skew) * (*size))/100;
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
	b->hsorted = GDK_SORTED;
	b->hdense = TRUE;
	BATseqbase(b, *base);
	BATkey(b, TRUE);
	b->tsorted = FALSE;
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

static int
BATnormal(BAT **bn, oid *base, int *size, int *domain, int *stddev, int *mean)
{
	BUN n = (BUN) * size, i;
	unsigned int r = (unsigned int) n;
	BUN d = (BUN) (*domain < 0 ? 0 : *domain);
	BAT *b = NULL;
	BUN firstbun, p, q;
	int m = *mean, s = *stddev;
	int *itab;
	flt *ftab, tot = 0.0;

	if (*size < 0) {
		GDKerror("BATnormal: size must not be negative");
		return GDK_FAIL;
	}

        b = BATnew(TYPE_void, TYPE_int, n);
	if (b == NULL)
		return GDK_FAIL;
	if (n == 0) {
		b->tsorted = GDK_SORTED;
		b->hsorted = GDK_SORTED;
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
	b->hsorted = GDK_SORTED;
	b->hdense = TRUE;
	BATseqbase(b, *base);
	BATkey(b, TRUE);
	b->tsorted = FALSE;
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
MBMrandom(int *ret, oid *base, int *size, int *domain){
	BAT *bn = NULL;

	BATrandom(&bn, base, size, domain);
	if( bn ){
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.random", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
MBMuniform(int *ret, oid *base, int *size, int *domain){
	BAT *bn = NULL;

	BATuniform(&bn, base, size, domain);
	if( bn ){
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
MBMnormal(int *ret, oid *base, int *size, int *domain, int *stddev, int *mean){
	BAT *bn = NULL;
	BATnormal(&bn, base, size, domain, stddev, mean);
	if( bn ){
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark.uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}


str
MBMmix(int *bn, int *batid)
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
MBMskewed(int *ret, oid *base, int *size, int *domain, int *skew){
	BAT *bn = NULL;

	BATskewed(&bn, base, size, domain, skew);
	if( bn ){
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else throw(MAL, "microbenchmark,uniform", OPERATION_FAILED);
	return MAL_SUCCEED;
}
