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
#include "gdk_private.h"
#if defined(HAVE_GETENTROPY) && defined(HAVE_SYS_RANDOM_H)
#include <sys/random.h>
#endif
#include "xoshiro256starstar.h"

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
	if (x == 0)
		return 64;
	unsigned long i;
	BitScanReverse64(&i, x);
	return (int) (63 - i);
}
#else
#define leading_zeroes(x) (x == 0 ? 64 : __builtin_clzll(x))
#endif

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

// Estimator as defined in
// "New cardinality estimation algorithms for HyperLogLog sketches"
// Otmar Ertl, arXiv:1702.01284.
// Only difference is how the multiplicity array is computed
double
sketch_estimate(uint8_t (*sketch)[CLZ_BUCKETS])
{
	int8_t C[CLZ_BUCKETS + 1] = {0};
	for (size_t bucket = 0; bucket < BUCKETS; bucket++) {
		int K = -1;
		for (int clz = 0; clz < CLZ_BUCKETS; clz++)
			if (sketch[bucket][clz] > 0)
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

#define SKETCH_UPDATE_ONE(SKETCH, HASH)					\
	do {								\
		uint8_t bucket = HASH & BITS_MASK;			\
		HASH |= BITS_MASK;					\
		uint8_t clz_bucket = leading_zeroes(hash);		\
		assert(clz_bucket <= 58);				\
		if (SKETCH[bucket][clz_bucket] <= 128) {		\
			SKETCH[bucket][clz_bucket]++;			\
		} else if (SKETCH[bucket][clz_bucket] < 255){		\
			uint8_t k = SKETCH[bucket][clz_bucket] - 128;	\
			uint64_t rand64_1 = next(rse);			\
			uint8_t clz_rand64_1 = leading_zeroes(rand64_1); \
			if (k < 64) {					\
				if (clz_rand64_1 == k)			\
					SKETCH[bucket][clz_bucket]++;	\
			} else {					\
				uint64_t rand64_2 = next(rse);		\
				uint8_t clz_rand64_2 = 64 + leading_zeroes(rand64_2); \
				if (clz_rand64_2 == k)			\
					SKETCH[bucket][clz_bucket]++;	\
			}						\
		} else {						\
			/* probabilistic counter is satured */		\
			continue;					\
		}							\
	} while (0)

gdk_return
sketch_update_many(BATiter *bi, uint8_t (*sketch)[CLZ_BUCKETS],
		   const void *values, size_t count)
{
	gdk_return rc = GDK_SUCCEED;
	assert(sketch && values && (count > 0));
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	random_state_engine rse;
	init_random_state_engine(rse, (uint64_t) GDKusec());

	int8_t values_t = bi->type;
	const void *atomnil = ATOMnilptr(values_t);
	bool (*atomeq) (const void *, const void *) = ATOMequal(values_t);

	TIMEOUT_LOOP_IDX_DECL(i, count, qry_ctx) {
		const void *v;
		v = ATOMvarsized(values_t) ? BUNtvar(bi, i) : BUNtloc(bi, i);
		if (atomeq(v, atomnil))
			continue;
		uint64_t hash = XXH64(v, ATOMlen(values_t, v), HLLSEED);
		SKETCH_UPDATE_ONE(sketch, hash);
	}

	return rc;
}

gdk_return
sketch_merge(uint8_t (*dst)[CLZ_BUCKETS], uint8_t (*src)[CLZ_BUCKETS])
{
	assert(dst != NULL && src != NULL);
	for (size_t i = 0; i < BUCKETS; i++)
		for (size_t j = 0; j < CLZ_BUCKETS; j++)
			if (src[i][j] > dst[i][j])
				dst[i][j] = src[i][j];
	return GDK_SUCCEED;
}

gdk_return
bat_sketch(BATiter *bi, struct canditer *bci, uint8_t (*sketch)[CLZ_BUCKETS])
{
	gdk_return rc = GDK_SUCCEED;
	lng t0 = GDKusec();
	BAT *b = bi->b;

	struct canditer n_bci;
	if (bci == NULL) {
		canditer_init(&n_bci, b, NULL);
	} else {
		n_bci = *bci;
		canditer_reset(&n_bci);
	}

	if (n_bci.tpe == cand_dense) {
		rc = sketch_update_many(bi, sketch, bi->base, n_bci.ncand);
	} else {
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		random_state_engine rse;
		init_random_state_engine(rse, (uint64_t) GDKusec());

		int8_t values_t = bi->type;
		const void *atomnil = ATOMnilptr(values_t);
		bool (*atomeq) (const void *, const void *) = ATOMequal(values_t);

		TIMEOUT_LOOP(n_bci.ncand, qry_ctx) {
			BUN o = canditer_next(&n_bci) - bi->b->hseqbase;
			const void *v;
			v = ATOMvarsized(values_t) ? BUNtvar(bi, o) : BUNtloc(bi, o);
			if (atomeq(v, atomnil))
				continue;
			uint64_t hash = XXH64(v, ATOMlen(values_t, v), HLLSEED);
			SKETCH_UPDATE_ONE(sketch, hash);
		}
	}

	TRC_DEBUG(ALGO, ALGOBATFMT " "LLFMT" usec\n", ALGOBATPAR(b),
		  GDKusec() - t0);
	return rc;
}

/* gdk_return */
/* sketch_update_many(BATiter *bi, uint8_t (*sketch)[CLZ_BUCKETS], */
/* 		   const void *values, size_t count) */
/* { */
/* 	gdk_return rc = GDK_SUCCEED; */
/* 	assert(sketch && values && (count > 0)); */
/* 	QryCtx *qry_ctx = MT_thread_get_qry_ctx(); */
/* 	random_state_engine rse; */
/* 	init_random_state_engine(rse, (uint64_t) GDKusec()); */

/* 	int8_t values_t = bi->type; */
/* 	const void *atomnil = ATOMnilptr(values_t); */
/* 	bool (*atomeq) (const void *, const void *) = ATOMequal(values_t); */

/* 	TIMEOUT_LOOP_IDX_DECL(i, count, qry_ctx) { */
/* 		const void *v; */
/* 		v = ATOMvarsized(values_t) ? BUNtvar(bi, i) : BUNtloc(bi, i); */
/* 		if (ATOMvarsized(values_t)) { */
/* 			printf("sketch_update_many bat id %d count %zu varsized value %s\n", bi->b->batCacheid, count, atomeq(v, atomnil) ? "str_nil" : (char *) v); */
/* 		} else { */
/* 			switch(values_t) { */
/* 			case TYPE_bte: */
/* 				printf("sketch_update_many bat id %d count %zu value %hhd\n", bi->b->batCacheid, count, atomeq(v, atomnil) ? bte_nil : *(int8_t *) v); */
/* 				break; */
/* 			case TYPE_sht: */
/* 				printf("sketch_update_many bat id %d count %zu value %hd\n", bi->b->batCacheid, count, atomeq(v, atomnil) ? sht_nil : *(int16_t *) v); */
/* 				break; */
/* 			case TYPE_int: */
/* 				printf("sketch_update_many bat id %d count %zu value %d\n", bi->b->batCacheid, count, atomeq(v, atomnil) ? int_nil : *(int32_t *) v); */
/* 				break; */
/* 			case TYPE_lng: */
/* 				printf("sketch_update_many bat id %d count %zu value %ld\n", bi->b->batCacheid, count, atomeq(v, atomnil) ? lng_nil : *(int64_t *) v); */
/* 				break; */
/* 			case TYPE_flt: */
/* 				printf("sketch_update_many bat id %d count %zu value %f\n", bi->b->batCacheid, count, atomeq(v, atomnil) ? flt_nil : *(float *) v); */
/* 				break; */
/* 			case TYPE_dbl: */
/* 				printf("sketch_update_many bat id %d count %zu value %f\n", bi->b->batCacheid, count, atomeq(v, atomnil) ? dbl_nil : *(double *) v); */
/* 				break; */
/* 			} */
/* 		} */
/* 		if (atomeq(v, atomnil)) */
/* 			continue; */
/* 		uint64_t hash = XXH64(v, ATOMlen(values_t, v), HLLSEED); */
/* 		SKETCH_UPDATE_ONE(sketch, hash); */
/* 	} */

/* 	return rc; */
/* } */
