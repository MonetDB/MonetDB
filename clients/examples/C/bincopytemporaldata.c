#include "bincopydata.h"

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

void
gen_timestamps(FILE *f, bool byteswap, long nrecs)
{
	struct rng rng = my_favorite_rng();

	for (long i = 0; i < nrecs; i++) {
		copy_binary_timestamp ts = random_timestamp(&rng);
		if (byteswap) {
			copy_binary_convert_timestamp(&ts);
		}
		fwrite(&ts, sizeof(ts), 1, f);
	}
}

#define GEN_TIMESTAMP_FIELD(name, fld) \
	void name \
		(FILE *f, bool byteswap, long nrecs) \
	{ \
		struct rng rng = my_favorite_rng(); \
	\
		for (long i = 0; i < nrecs; i++) { \
			copy_binary_timestamp ts = random_timestamp(&rng); \
			if (byteswap) { \
				copy_binary_convert_timestamp(&ts); \
			} \
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
