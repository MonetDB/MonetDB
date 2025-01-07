/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "copybinary.h"
#include "copybinary_support.h"

_Noreturn void croak(int status, const char *msg, ...)
	__attribute__((__format__(__printf__, 2, 3)));

void gen_decimal_tinyints(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_decimal_smallints(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_decimal_ints(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_decimal_bigints(FILE *f, bool byteswap, long nrecs, char *arg);
#ifdef HAVE_HGE
void gen_decimal_hugeints(FILE *f, bool byteswap, long nrecs, char *arg);
#endif

void gen_timestamps(FILE *f, bool byteswap, long nrecs, char *arg);

void gen_timestamp_times(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_timestamp_dates(FILE *f, bool byteswap, long nrecs, char *arg);


void gen_timestamp_ms(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_timestamp_seconds(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_timestamp_minutes(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_timestamp_hours(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_timestamp_days(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_timestamp_months(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_timestamp_years(FILE *f, bool byteswap, long nrecs, char *arg);

void gen_bin_uuids(FILE *f, bool byteswap, long nrecs, char *arg);
void gen_text_uuids(FILE *f, bool byteswap, long nrecs, char *arg);


// reproducible rng so we can rerun tests with the same data when they fail.
// Based on https://en.wikipedia.org/wiki/Lehmer_random_number_generator
struct rng {
	uint32_t state;
};
#define rng_next(rng) ((rng)->state = (uint64_t)(rng)->state * 48271 % 0x7fffffff)
#define my_favorite_rng() ((struct rng) { .state = 42 })

