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

#include <sys/random.h>
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
	if (x == 1.) return INFINITY;
	double y = 1;
	double z = x;
	double z_prime;
	do {
		x *= x;
		z_prime = z;
		z += x * y;
		y += y;
	} while(z_prime != z);
	return z;
}

// Helper function tau as defined in
// "New cardinality estimation algorithms for HyperLogLog sketches"
// Otmar Ertl, arXiv:1702.01284
static inline double
tau(double x)
{
	if (x == 0. || x == 1.) return 0.;
	double y = 1.0;
	double z = 1 - x;
	double z_prime;
	do {
		x = sqrt(x);
		z_prime = z;
		y *= 0.5;
		z -= pow(1 - x, 2) * y;
	} while(z_prime != z);
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
		for (size_t clz = 0; clz < CLZ_BUCKETS; clz++)
			if (cnt_sketch[bucket][clz] > 0)
				K = clz;
		K == -1 ? C[0]++ : C[K + 1]++;
	}
	double t = tau(1.0 - ((double)C[CLZ_BUCKETS] / BUCKETS));
	double z = (double)BUCKETS * t;
	for (int k = CLZ_BUCKETS; k >= 1; k--)
		z = 0.5 * (z + (double)C[k]);
	double s = sigma((double)C[0] / BUCKETS);
	z += (double)BUCKETS * s;
	const double alpha = 0.7213475204444817;
	return alpha * (double)BUCKETS * BUCKETS / z;
}

void
sketch_populate(BAT* b, BATiter *bi, struct canditer *bci,
		uint8_t cnting_sketch[BUCKETS][CLZ_BUCKETS])
{
	oid hseq = b->hseqbase;
	/* uint64_t murmur3_out[2]; */
	uint64_t hash;
	uint8_t bucket;

	uint8_t clz;

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	canditer_reset(bci);
	TIMEOUT_LOOP(bci->ncand, qry_ctx) {
		BUN p = canditer_next(bci) - hseq;
		const void *ptr = BUNtail(bi, p);
		switch (bi->type) {
		case TYPE_int:
			if (is_int_nil(*(int *)ptr))
				continue;
			else
				/* MurmurHash3_x64_128(ptr, sizeof(int), HLLSEED, murmur3_out); */
				/* hash = murmur3_out[1]; */
				hash = XXH64(ptr, sizeof(int), HLLSEED);
			break;
		case TYPE_lng:
			if (is_lng_nil(*(lng *)ptr))
				continue;
			else
				/* MurmurHash3_x64_128(ptr, sizeof(lng), HLLSEED, murmur3_out); */
				/* hash = murmur3_out[1]; */
				hash = XXH64(ptr, sizeof(lng), HLLSEED);
			break;
		case TYPE_str:
			if (strNil(ptr))
				continue;
			else
				/* MurmurHash3_x64_128(ptr, strlen(ptr), HLLSEED, murmur3_out); */
				/* hash = murmur3_out[1]; */
				hash = XXH64(ptr, strlen(ptr), HLLSEED);
			break;
		default:
			return;
		}
		bucket = hash & BITS_MASK;
		hash |= BITS_MASK;
		clz = __builtin_clzll(hash);
		assert(clz <= 58);
		if (cnting_sketch[bucket][clz] <= 128) {
			cnting_sketch[bucket][clz]++;
		} else {
			uint8_t k = cnting_sketch[bucket][clz] - 128;
			uint64_t rng;
			if (getentropy(&rng, sizeof(rng)) == 0 &&
			   (rng & ((1ULL << k) - 1)) == 0)
				cnting_sketch[bucket][clz]++;
		}
	}
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

	BATiter nbi = bi ? *bi : bat_iterator(b);
	struct canditer nbci;
	if (bci == NULL)
		canditer_init(&nbci, b, NULL);

	sketch_populate(b, &nbi, &nbci, cnting_sketch);
	double unique_guess = sketch_estimate(cnting_sketch);

	if (bi == NULL)
		bat_iterator_end(&nbi);

	return unique_guess;
}
