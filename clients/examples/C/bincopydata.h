#include "monetdb_config.h"
#include "copybinary.h"
#include "copybinary_support.h"

void gen_timestamps(FILE *f, bool byteswap, long nrecs);

void gen_timestamp_times(FILE *f, bool byteswap, long nrecs);
void gen_timestamp_dates(FILE *f, bool byteswap, long nrecs);


void gen_timestamp_ms(FILE *f, bool byteswap, long nrecs);
void gen_timestamp_seconds(FILE *f, bool byteswap, long nrecs);
void gen_timestamp_minutes(FILE *f, bool byteswap, long nrecs);
void gen_timestamp_hours(FILE *f, bool byteswap, long nrecs);
void gen_timestamp_days(FILE *f, bool byteswap, long nrecs);
void gen_timestamp_months(FILE *f, bool byteswap, long nrecs);
void gen_timestamp_years(FILE *f, bool byteswap, long nrecs);

void gen_bin_uuids(FILE *f, bool byteswap, long nrecs);
void gen_text_uuids(FILE *f, bool byteswap, long nrecs);


// reproducible rng so we can rerun tests with the same data when they fail.
// Based on https://en.wikipedia.org/wiki/Lehmer_random_number_generator
struct rng {
	uint32_t state;
};
#define rng_next(rng) ((rng)->state = (uint64_t)(rng)->state * 48271 % 0x7fffffff)
#define my_favorite_rng() ((struct rng) { .state = 42 })

