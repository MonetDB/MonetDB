#include <stdint.h>
#include <string.h>
#include "gdk.h"

/*
This 64 bit pseudo random number generator is based on the engine written by
Sebastiano Vigna from Dipartimento di Informatica of the Università degli Studi di Milano.
The original source code can be found here http://xoshiro.di.unimi.it/xoshiro256starstar.c.
The engine is supposed to perform very well on various random engine benchmarks.
The original author offered his work to the public domain.
*/

static inline ulng rotl(const ulng x, int k) {
	return (x << k) | (x >> (64 - k));
}

typedef ulng random_state_engine[4];

static inline void init_random_state_engine(random_state_engine* engine, const unsigned seed) {

    random_state_engine s = { seed << 1, seed + 1, seed >> 1, seed - 1 };

    memcpy(engine, &s, sizeof(random_state_engine));
}

static inline ulng next(random_state_engine rse) {
	const ulng output = rotl(rse[0] * 5, 7) * 9;

	const ulng t = rse[1] << 17;

	rse[2] ^= rse[0];
	rse[3] ^= rse[1];
	rse[1] ^= rse[2];
	rse[0] ^= rse[3];

	rse[2] ^= t;

	rse[3] = rotl(rse[3], 45);

	return output;
}

static inline double next_double(random_state_engine rse) {
	return (double) next(rse) / UINT64_MAX;
}
