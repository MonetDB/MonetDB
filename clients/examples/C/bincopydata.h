#include "monetdb_config.h"
#include "copybinary.h"

void gen_timestamps(FILE *f, long nrecs);

void gen_timestamp_times(FILE *f, long nrecs);
void gen_timestamp_dates(FILE *f, long nrecs);


void gen_timestamp_ms(FILE *f, long nrecs);
void gen_timestamp_seconds(FILE *f, long nrecs);
void gen_timestamp_minutes(FILE *f, long nrecs);
void gen_timestamp_hours(FILE *f, long nrecs);
void gen_timestamp_days(FILE *f, long nrecs);
void gen_timestamp_months(FILE *f, long nrecs);
void gen_timestamp_years(FILE *f, long nrecs);
