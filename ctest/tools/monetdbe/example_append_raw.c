/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

#define date_eq(d1, d2) (d1.year == d2.year && d1.month == d2.month && d1.day == d2.day)
#define time_eq(t1, t2) (t1.hours == t2.hours && t1.minutes == t2.minutes && t1.seconds == t2.seconds && t1.ms == t2.ms)

static char hexit[] = "0123456789ABCDEF";

int
main(void)
{
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	monetdbe_result* result = NULL;

	// second argument is a string for the db directory or NULL for in-memory mode
	if (monetdbe_open(&mdbe, NULL, NULL))
		error("Failed to open database")
	if ((err = monetdbe_query(mdbe, "CREATE TABLE test (x integer, y string, ts timestamp, dt date, t time, b blob)", NULL, NULL)) != NULL)
		error(err)

	// fill the empty table with data using append
	// it is important to match column order below with the column order of the table

	// int_32
	int32_t i1 = 42;
	int32_t i2 = *(int32_t*)monetdbe_null(mdbe, monetdbe_int32_t);
	int32_t ints[2] = { i1, i2 };
	monetdbe_column col0 = { .type = monetdbe_int32_t, .data = &ints, .count = 2 };

	// str
	char* dstr[2] = { "Hello", "World" };
	monetdbe_column col1 = { .type = monetdbe_str, .data = &dstr, .count = 2 };

	// timestamp
	monetdbe_data_date dt = { .day = 2, .month = 3, .year = 2020 };
	monetdbe_data_time tm = { .hours = 3, .minutes = 13, .seconds = 23, .ms = 33 };
	monetdbe_data_timestamp t1 = { .date = dt, .time = tm };
	monetdbe_data_timestamp t2 = *(monetdbe_data_timestamp*)monetdbe_null(mdbe, monetdbe_timestamp);
	monetdbe_data_timestamp tss[2] = { t1, t2 };
	monetdbe_column col2 = { .type = monetdbe_timestamp, .data = &tss, .count = 2 };

	// date
	monetdbe_data_date d1 = dt;
	monetdbe_data_date d2 = *(monetdbe_data_date*)monetdbe_null(mdbe, monetdbe_date);
	monetdbe_data_date dts[2] = { d1, d2 };
	monetdbe_column col3 = { .type = monetdbe_date, .data = &dts, .count = 2 };

	// time
	monetdbe_data_time tm1 = tm;
	monetdbe_data_time tm2 = *(monetdbe_data_time*)monetdbe_null(mdbe, monetdbe_time);
	monetdbe_data_time tms[2] = { tm1, tm2 };
	monetdbe_column col4 = { .type = monetdbe_time, .data = &tms, .count = 2 };

	// blob
	monetdbe_data_blob b1 = { .size = 1, .data = "33" };
	monetdbe_data_blob b2 = *(monetdbe_data_blob*)monetdbe_null(mdbe, monetdbe_blob);
	monetdbe_data_blob bs[2] = { b1, b2 };
	monetdbe_column col5 = { .type = monetdbe_blob, .data = &bs, .count = 2 };

	monetdbe_column* dcol[6] = { &col0, &col1, &col2, &col3, &col4, &col5 };
	if ((err = monetdbe_append(mdbe, "sys", "test", (monetdbe_column**) &dcol, 6)) != NULL)
		error(err)

	if ((err = monetdbe_query(mdbe, "SELECT * FROM test; ", &result, NULL)) != NULL)
		error(err)
	fprintf(stdout, "Query result after append with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	monetdbe_column* rcol[6];
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			if ((err = monetdbe_result_fetch(result, rcol+c, c)) != NULL)
				error(err)
			switch (rcol[c]->type) {
				case monetdbe_int32_t: {
					monetdbe_column_int32_t * col = (monetdbe_column_int32_t *) rcol[c];
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdbe_str: {
					monetdbe_column_str * col = (monetdbe_column_str *) rcol[c];
					if (col->is_null(col->data+r)) {
						printf("NULL");
					} else {
						printf("%s", (char*) col->data[r]);
					}
					break;
				}
				case monetdbe_date: {
					monetdbe_column_date * col = (monetdbe_column_date *) rcol[c];
					if (date_eq(col->data[r], col->null_value)) {
						printf("NULL");
					} else {
						printf("%d-%d-%d", col->data[r].year, col->data[r].month, col->data[r].day);
					}
					break;
				}
				case monetdbe_time: {
					monetdbe_column_time * col = (monetdbe_column_time *) rcol[c];
					if (time_eq(col->data[r], col->null_value)) {
						printf("NULL");
					} else {
						printf("%d:%d:%d.%d", col->data[r].hours, col->data[r].minutes, col->data[r].seconds, col->data[r].ms);
					}
					break;
				}
				case monetdbe_timestamp: {
					monetdbe_column_timestamp * col = (monetdbe_column_timestamp *) rcol[c];
					if (date_eq(col->data[r].date, col->null_value.date) && time_eq(col->data[r].time, col->null_value.time)) {
						printf("NULL");
					} else {
						printf("%d-%d-%d ", col->data[r].date.year, col->data[r].date.month, col->data[r].date.day);
						printf("%d:%d:%d.%d", col->data[r].time.hours, col->data[r].time.minutes, col->data[r].time.seconds, col->data[r].time.ms);
					}
					break;
				}
				case monetdbe_blob: {
					monetdbe_column_blob * col = (monetdbe_column_blob *) rcol[c];
					if (!col->data[r].data) {
						printf("NULL");
					} else {
						for (size_t i = 0; i < col->data[r].size; i++) {
							int hval = (col->data[r].data[i] >> 4) & 15;
							int lval = col->data[r].data[i] & 15;

							printf("%c%c", hexit[hval], hexit[lval]);
						}
					}
					break;
				}
				default: {
					printf("UNKNOWN");
				}
			}

			if (c + 1 < result->ncols) {
				printf(", ");
			}
		}
		printf("\n");
	}
	if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
		error(err)

	if (monetdbe_close(mdbe))
		error("Failed to close database")
	return 0;
}
