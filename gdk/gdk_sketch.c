/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "gdk.h"

#if defined(HAVE_GETENTROPY) && defined(HAVE_SYS_RANDOM_H)
#include <sys/random.h>
#endif
// #include "murmurhash3.h"
#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#include "xxhash.h"

// Helper function sigma as defined in
// "New cardinality estimation algorithms for HyperLogLog sketches"
// Otmar Ertl, arXiv:1702.01284
static inline double
sigma(double x)
{
	if (x == 1.)
		return INFINITY;
	double y = 1;
	double z = x;
	double z_prime;
	do {
		x *= x;
		z_prime = z;
		z += x * y;
		y += y;
	} while (z_prime != z);
	return z;
}

// Helper function tau as defined in
// "New cardinality estimation algorithms for HyperLogLog sketches"
// Otmar Ertl, arXiv:1702.01284
static inline double
tau(double x)
{
	if (x == 0. || x == 1.)
		return 0.;
	double y = 1.0;
	double z = 1 - x;
	double z_prime;
	do {
		x = sqrt(x);
		z_prime = z;
		y *= 0.5;
		z -= pow(1 - x, 2) * y;
	} while (z_prime != z);
	return z / 3;
}

/// Estimator as defined in
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284.
/// Only difference is how the multiplicity array is computed
double
sketch_estimate(uint8_t cnt_sketch[BUCKETS][CLZ_BUCKETS])
{
	int8_t C[CLZ_BUCKETS + 1] = {0};
	for (size_t bucket = 0; bucket < BUCKETS; bucket++) {
		int K = -1;
		for (int clz = 0; clz < CLZ_BUCKETS; clz++)
			if (cnt_sketch[bucket][clz] > 0)
				K = clz;
		K == -1 ? C[0]++ : C[K + 1]++;
	}
	double t = tau(1.0 - ((double) C[CLZ_BUCKETS] / BUCKETS));
	double z = (double) BUCKETS * t;
	for (int k = CLZ_BUCKETS; k >= 1; k--)
		z = 0.5 * (z + (double) C[k]);
	double s = sigma((double) C[0] / BUCKETS);
	z += (double) BUCKETS * s;
	const double alpha = 0.7213475204444817;
	return alpha * (double) BUCKETS * BUCKETS / z;
}

#define my_is_int_nil(v) ((*(int *) v) == GDK_int_min - 1)
#define my_is_lng_nil(v) ((*(lng *) v) == GDK_lng_min - 1)
#define my_is_str_nil(v) strNil(v)

#define my_sizeof_int(v) sizeof(int)
#define my_sizeof_lng(v) sizeof(lng)
#define my_sizeof_str(v) strlen(v)

/* #define my_get_val_int(ptr, i) ptr[i] */
/* #define my_get_val_lng(ptr, i) ptr[i] */
/* #define my_get_val_str(ptr, i) */

#if !defined(HAVE_GETENTROPY) && defined(HAVE_RAND_S)
static int
getentropy(void *buffer, size_t length)
{
	uint8_t *b = buffer;
	for (size_t i = 0; i < length; i += sizeof(unsigned int)) {
		unsigned int r;
		if (rand_s(&r) != 0)
			return -1;
		for (size_t j = 0; j < sizeof(unsigned int); j++) {
			if (i + j == length)
				return 0;
			b[i + j] = (uint8_t) (r >> (j * 8));
		}
	}
	return 0;
}
static int
leading_zeroes(uint64_t x)
{
	unsigned long i;
	BitScanReverse64(&i, x);
	return (int) (63 - i);
}
#else
#define leading_zeroes(x)	__builtin_clzll(x)
#endif

#define SKETCH_UPDATE(CNTING_SKETCH, HASH)				\
	do {								\
		bucket = hash & BITS_MASK;				\
		hash |= BITS_MASK;					\
		clz = leading_zeroes(hash);				\
		assert(clz <= 58);					\
		if (CNTING_SKETCH[bucket][clz] <= 128) {		\
			CNTING_SKETCH[bucket][clz]++;			\
		} else {						\
			if (rng_i == 0 && getentropy(rng_buf, 256 /* maximum */) != 0) { \
				/* TRC_ERROR(ACCELERATOR, "Failed to generate sketch seed." */ \
				/* 	  "Aborting count distinct estimation."); */ \
				rc = GDK_FAIL;				\
				break;					\
			}						\
			uint64_t rng = rng_buf[rng_i];			\
			rng_i = rng_i > 31 ? 0 : rng_i + 1;		\
			uint8_t k = CNTING_SKETCH[bucket][clz] - 128;	\
			if ((rng & ((1ULL << k) - 1)) == 0)		\
				CNTING_SKETCH[bucket][clz]++;		\
		}							\
	} while (0)

#define SKETCH_POPULATE(TYPE)						\
	do {								\
		oid hseq = b->hseqbase;					\
		/* uint64_t murmur3_out[2]; */				\
		uint64_t hash;						\
		uint8_t bucket;						\
		uint8_t clz;						\
		uint64_t rng_buf[32];					\
		int rng_i = 0;						\
		TIMEOUT_LOOP(n_bci.ncand, qry_ctx) {			\
			oid p = canditer_next(&n_bci) - hseq;		\
			const void *ptr = BUNtail(&n_bi, p);		\
			if (!my_is_##TYPE##_nil(ptr)) {			\
				hash = XXH64(ptr, my_sizeof_##TYPE(ptr), HLLSEED); \
				SKETCH_UPDATE(cnting_sketch, hash);	\
			}						\
		}							\
	} while (0)

/* #define SKETCH_POPULATE_DENSE(TYPE)					\ */
/* 	do {								\ */
/* 		uint64_t hash;						\ */
/* 		uint8_t bucket;						\ */
/* 		uint8_t clz;						\ */
/* 		uint64_t rng_buf[32];					\ */
/* 		int rng_i = 0;						\ */
/* 		TYPE *p = Tloc(b, 0);					\ */
/* 		TIMEOUT_LOOP_IDX_DECL(i, BATcount(b), qry_ctx) {	\ */
/* 			const void *ptr = TODO;				\ */
/* 			if (!my_is_##TYPE##_nil(ptr)) {			\ */
/* 				hash = XXH64(ptr, my_sizeof_##TYPE(ptr), HLLSEED); \ */
/* 				SKETCH_UPDATE(cnting_sketch, hash);	\ */
/* 			}						\ */
/* 		}							\ */
/* 	} while (0) */

int
sketch_populate(BAT *b, BATiter *bi, struct canditer *bci,
		uint8_t cnting_sketch[BUCKETS][CLZ_BUCKETS])
{
	gdk_return rc = GDK_SUCCEED;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	/* if (!bi && !bci) { */
	/* 	switch (b->ttype) { */
	/* 	case TYPE_int: */
	/* 		SKETCH_POPULATE_DENSE(int); */
	/* 		break; */
	/* 	case TYPE_lng: */
	/* 		SKETCH_POPULATE_DENSE(lng); */
	/* 		break; */
	/* 	case TYPE_str: */
	/* 		SKETCH_POPULATE_DENSE(str); */
	/* 		break; */
	/* 	default: */
	/* 		/\* TRC_ERROR(ACCELERATOR, "Type not supported for counting sketches." *\/ */
	/* 		/\* 	  "Aborting count distinct estimation."); *\/ */
	/* 		break; */
	/* 	} */
	/* } */

	BATiter n_bi = bi ? *bi : bat_iterator(b);
	struct canditer n_bci;
	if (bci == NULL) {
		canditer_init(&n_bci, b, NULL);
	} else {
		n_bci = *bci;
		canditer_reset(&n_bci);
	}

	switch (n_bi.type) {
	case TYPE_int:
		SKETCH_POPULATE(int);
		break;
	case TYPE_lng:
		SKETCH_POPULATE(lng);
		break;
	case TYPE_str:
		SKETCH_POPULATE(str);
		break;
	default:
		/* TRC_ERROR(ACCELERATOR, "Type not supported for counting sketches." */
		/* 	  "Aborting count distinct estimation."); */
		break;
	}

	if (bi == NULL)
		bat_iterator_end(&n_bi);

	return rc;
}

/* void */
/* sketch_merge(BAT* b, BAT* n) */
/* { */
/* 	MT_lock_set(&b->batIdxLock); */
/* 	for (size_t i = 0; i < BUCKETS; i++) */
/* 		for (size_t j = 0; j < CLZ_BUCKETS; j++) */
/* 			if (n->cnting_sketch[i][j] > b->cnting_sketch[i][j]) */
/* 				b->cnting_sketch[i][j] = n->cnting_sketch[i][j]; */
/* 	b->unique_guess = sketch_estimate(b->cnting_sketch); */
/* 	MT_lock_unset(&b->batIdxLock); */
/* } */

double
bat_guess_uniques(BAT *b, BATiter *bi, struct canditer *bci)
{
	uint8_t cnting_sketch[BUCKETS][CLZ_BUCKETS] = {0};
	double unique_guess = 0;

	if (sketch_populate(b, bi, bci, cnting_sketch) == GDK_SUCCEED)
		unique_guess = sketch_estimate(cnting_sketch);

	return unique_guess;
}
