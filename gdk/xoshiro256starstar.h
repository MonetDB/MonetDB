/*
This 64 bit pseudo random number generator is based on the engine
written by Sebastiano Vigna from Dipartimento di Informatica of the
Università degli Studi di Milano.  The original source code can be
found here http://xoshiro.di.unimi.it/xoshiro256starstar.c.  The
engine is supposed to perform very well on various random engine
benchmarks.
The original author offered his work to the public domain per the
following original comment.
*/

/*  Written in 2018 by David Blackman and Sebastiano Vigna (vigna@acm.org)

To the extent possible under law, the author has dedicated all copyright
and related and neighboring rights to this software to the public domain
worldwide. This software is distributed without any warranty.

See <http://creativecommons.org/publicdomain/zero/1.0/>. */

static inline uint64_t rotl(const uint64_t x, int k) {
	return (x << k) | (x >> (64 - k));
}

typedef uint64_t random_state_engine[4];

static inline void
init_random_state_engine(random_state_engine engine, uint64_t seed)
{
	for (int i = 0; i < 4; i++) {
		/* we use the splitmix64 generator to generate the 4
		 * values in the state array from the given seed.
		 * This code is adapted from
		 * http://xoshiro.di.unimi.it/splitmix64.c which was
		 * put in the public domain in the same way as the
		 * "next" function below. */
		uint64_t z = (seed += 0x9e3779b97f4a7c15);
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
		engine[i] = z ^ (z >> 31);
	}
}

static inline uint64_t next(random_state_engine rse) {
	const uint64_t output = rotl(rse[0] * 5, 7) * 9;

	const uint64_t t = rse[1] << 17;

	rse[2] ^= rse[0];
	rse[3] ^= rse[1];
	rse[1] ^= rse[2];
	rse[0] ^= rse[3];

	rse[2] ^= t;

	rse[3] = rotl(rse[3], 45);

	return output;
}
