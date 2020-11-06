#include "bincopydata.h"

// reproducible rng so we can rerun tests with the same data
// used to fail.
// Based on https://en.wikipedia.org/wiki/Lehmer_random_number_generator
struct rng {
	uint32_t state;
};


#define rng_next(rng) ((rng)->state = (uint64_t)(rng)->state * 48271 % 0x7fffffff)

static struct rng
my_favorite_lfsr(void)
{
	return (struct rng) { .state = 42 };
}

static copy_binary_timestamp
random_timestamp(struct rng *rng)
{
	// the % trick gives a little skew but we don't care
	copy_binary_timestamp ts = {
		.time = {
			.ms = rng_next(rng) % 1000000,
			.seconds = rng_next(rng) % 60, // 61 ??
			.minutes = rng_next(rng) % 60,
			.hours = rng_next(rng) % 24,
			.padding = 0,
		},
		.date = {
			.day = 0, // determine later
			.month = 1 + rng_next(rng) % 12,
			.year = 2030 - (int16_t)rng_next(rng),
		},
	};

	const uint32_t durations[] = { 0,
		31, 28, 31, 30,
		31, 30, 31, 31,
		30, 31, 30, 31,
	};
	uint32_t year = ts.date.year;
	uint32_t days = durations[ts.date.month];
	bool leap_year = (
		days == 28 &&
		year % 4 == 0 &&
		(year % 100 != 0 || year % 400 == 0)
	);
	if (leap_year)
		days = 29;
	ts.date.day = 1 + rng_next(rng) % days;

	return ts;
}

void fix_endian(copy_binary_timestamp *p);
void fix_endian2(int16_t *p);
void fix_endian4(uint32_t *p);

#ifdef WORDS_BIGENDIAN
void fix_endian2(int16_t *p) {
	char *bytes = (char*) p;

	char b0 = bytes[0];
	char b1 = bytes[1];

	bytes[0] = b1;
	bytes[1] = b0;
}

void fix_endian4(uint32_t *p) {
	char *bytes = (char*) p;

	char b0 = bytes[0];
	char b1 = bytes[1];
	char b2 = bytes[2];
	char b3 = bytes[3];

	bytes[0] = b3;
	bytes[1] = b2;
	bytes[2] = b1;
	bytes[3] = b0;
}
#else
void fix_endian2(int16_t *p) {
	(void)p;
}

void fix_endian4(uint32_t *p) {
	(void)p;
}
#endif

void fix_endian(copy_binary_timestamp *ts)
{
	fix_endian2(&ts->date.year);
	fix_endian4(&ts->time.ms);
}

void
gen_timestamps(FILE *f, long nrecs)
{
	struct rng rng = my_favorite_lfsr();

	for (long i = 0; i < nrecs; i++) {
		copy_binary_timestamp ts = random_timestamp(&rng);
		fix_endian(&ts);
		fwrite(&ts, sizeof(ts), 1, f);
	}
}

#define GEN_TIMESTAMP_FIELD(name, fld) \
	void name \
		(FILE *f, long nrecs) \
	{ \
		struct rng rng = my_favorite_lfsr(); \
	\
		for (long i = 0; i < nrecs; i++) { \
			copy_binary_timestamp ts = random_timestamp(&rng); \
			fix_endian(&ts); \
			fwrite(&ts.fld, sizeof(ts.fld), 1, f); \
		} \
	}

GEN_TIMESTAMP_FIELD(gen_timestamp_times, time)
GEN_TIMESTAMP_FIELD(gen_timestamp_dates, date)

GEN_TIMESTAMP_FIELD(gen_timestamp_ms, time.ms)
GEN_TIMESTAMP_FIELD(gen_timestamp_seconds, time.seconds)
GEN_TIMESTAMP_FIELD(gen_timestamp_minutes, time.minutes)
GEN_TIMESTAMP_FIELD(gen_timestamp_hours, time.hours)
GEN_TIMESTAMP_FIELD(gen_timestamp_days, date.day)
GEN_TIMESTAMP_FIELD(gen_timestamp_months, date.month)
GEN_TIMESTAMP_FIELD(gen_timestamp_years, date.year)
