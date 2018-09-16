#include <stdint.h>
#include <string.h>

static inline uint32_t rotl(const uint32_t x, int k) {
	return (x << k) | (x >> (32 - k));
}

typedef uint32_t random_state_engine[4];

void init_random_state_engine(random_state_engine* engine, uint32_t seed);
void init_random_state_engine(random_state_engine* engine, uint32_t seed) {

    random_state_engine s = { seed << 1, seed + 1, seed >> 1, seed - 1 };

    memcpy(engine, &s, sizeof(random_state_engine));
}

static inline uint32_t next_uint32(random_state_engine rse) {
	uint32_t output = rotl(rse[0] * 5, 7) * 9;

	const uint32_t t = rse[1] << 9;

	rse[2] ^= rse[0];
	rse[3] ^= rse[1];
	rse[1] ^= rse[2];
	rse[0] ^= rse[3];

	rse[2] ^= t;

	rse[3] = rotl(rse[3], 11);

	return output;
}

static inline double next_double(random_state_engine rse) {
	return (double) next_uint32(rse) / UINT32_MAX;
}
