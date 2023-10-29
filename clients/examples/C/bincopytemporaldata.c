/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "bincopydata.h"

static const copy_binary_timestamp binary_nil_timestamp = {
	.time = {
		.ms = 0xFFFFFFFF,
		.seconds = 255,
		.minutes = 255,
		.hours = 255,
		.padding = 255,
	},
	.date = {
		.day = 255,
		.month = 255,
		.year =-1,
	},
};

static copy_binary_timestamp
random_timestamp(struct rng *rng)
{
	copy_binary_timestamp ts;
	if (rng_next(rng) % 10 == 9) {
		ts = binary_nil_timestamp;
		return ts;
	}

	// the % trick gives a little skew but we don't care
	ts = (copy_binary_timestamp){
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
			.year = 2030 - (int16_t)rng_next(rng) % 2300,
		},
	};

	const uint32_t durations[] = { 0,
		31, 28, 31, 30,
		31, 30, 31, 31,
		30, 31, 30, 31,
	};
	int16_t year = ts.date.year;
	uint32_t days = durations[ts.date.month];
	bool leap_year = (
		days == 28 &&
		year > 0 && year % 4 == 0 &&
		(year % 100 != 0 || year % 400 == 0)
	);
	if (leap_year)
		days = 29;
	ts.date.day = 1 + rng_next(rng) % days;

	return ts;
}

void
gen_timestamps(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	struct rng rng = my_favorite_rng();

	for (long i = 0; i < nrecs; i++) {
		copy_binary_timestamp ts = random_timestamp(&rng);
		if (byteswap) {
			copy_binary_convert_timestamp(&ts);
		}
		fwrite(&ts, sizeof(ts), 1, f);
	}
}

#define GEN_TIMESTAMP_FIELD(name, typ, fld, nilvalue) \
	void name \
		(FILE *f, bool byteswap, long nrecs, char *arg) \
	{ \
		(void)arg; \
		struct rng rng = my_favorite_rng(); \
	\
		for (long i = 0; i < nrecs; i++) { \
			copy_binary_timestamp ts = random_timestamp(&rng); \
			typ *p = &ts.fld; \
			typ tmp = ts.date.day == 255 ? nilvalue : *p; \
			if (byteswap) { \
				copy_binary_convert_timestamp(&ts); \
			} \
			fwrite(&tmp, sizeof(tmp), 1, f); \
		} \
	}

GEN_TIMESTAMP_FIELD(gen_timestamp_times, copy_binary_time, time, binary_nil_timestamp.time)
GEN_TIMESTAMP_FIELD(gen_timestamp_dates, copy_binary_date, date, binary_nil_timestamp.date)

GEN_TIMESTAMP_FIELD(gen_timestamp_ms, uint32_t, time.ms, 0x80)
GEN_TIMESTAMP_FIELD(gen_timestamp_seconds, uint8_t, time.seconds, 0x80)
GEN_TIMESTAMP_FIELD(gen_timestamp_minutes, uint8_t, time.minutes, 0x80)
GEN_TIMESTAMP_FIELD(gen_timestamp_hours, uint8_t, time.hours, 0x80)
GEN_TIMESTAMP_FIELD(gen_timestamp_days, uint8_t, date.day, 0x80)
GEN_TIMESTAMP_FIELD(gen_timestamp_months, uint8_t, date.month, 0x80)
GEN_TIMESTAMP_FIELD(gen_timestamp_years, int16_t, date.year, -1)
