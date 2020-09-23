#include "bincopydata.h"

struct lfsr
{
	uint32_t bits;
};

static struct lfsr
my_favorite_lfsr(void)
{
	return (struct lfsr){42};
}

static uint32_t
lfsr_bit(struct lfsr *lfsr)
{
	const uint32_t mask = 0xB400;
	uint32_t bit = lfsr->bits & 1;
	lfsr->bits >>= 1;
	lfsr->bits ^= mask & (-bit);
	return bit;
}

static uint32_t
lfsr_bits(struct lfsr *lfsr, int n)
{
	uint32_t value = 0;
	for (int i = 0; i < n; i++)
		value = (value << 1) | lfsr_bit(lfsr);
	return value;
}


typedef struct {
	uint8_t day;
	uint8_t month;
	int16_t year;
} binary_date; // natural size: 32, natural alignment: 16

typedef struct {
	uint32_t ms;
	uint8_t seconds;
	uint8_t minutes;
	uint8_t hours;
	uint8_t padding; // implied in C, explicit elsewhere
} binary_time;		 // natural size: 64, natural alignment: 32

typedef struct {
	binary_time time;
	binary_date date;
} binary_timestamp; // natural size: 96, natural alignment: 32

static binary_timestamp
random_timestamp(struct lfsr *lfsr)
{
	// the % trick gives a little skew but we don't care
	binary_timestamp ts = {
		.time = {
			.ms = lfsr_bits(lfsr, 32) % 1000000,
			.seconds = lfsr_bits(lfsr, 16) % 60, // 61 ??
			.minutes = lfsr_bits(lfsr, 16) % 60,
			.hours = lfsr_bits(lfsr, 8) % 24,
			.padding = 0,
		},
		.date = {
			.day = 0, // determine later
			.month = 1 + lfsr_bits(lfsr, 8) % 12,
			.year = 2030 - (int16_t)lfsr_bits(lfsr, 6),
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
	ts.date.day = 1 + lfsr_bits(lfsr, 8) % days;

	return ts;
}

void fix_endian(binary_timestamp *p);
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

void fix_endian(binary_timestamp *ts)
{
	fix_endian2(&ts->date.year);
	fix_endian4(&ts->time.ms);
}

void
gen_timestamps(FILE *f, long nrecs)
{
	struct lfsr lfsr = my_favorite_lfsr();

	for (long i = 0; i < nrecs; i++) {
		binary_timestamp ts = random_timestamp(&lfsr);
		fix_endian(&ts);
		fwrite(&ts, sizeof(ts), 1, f);
	}
}

#define GEN_TIMESTAMP_FIELD(name, fld) \
	void name \
		(FILE *f, long nrecs) \
	{ \
		struct lfsr lfsr = my_favorite_lfsr(); \
	\
		for (long i = 0; i < nrecs; i++) { \
			binary_timestamp ts = random_timestamp(&lfsr); \
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
