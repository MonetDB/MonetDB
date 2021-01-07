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
	if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (42, 'Hello', '2020-01-02 10:20:30', '2020-01-02', '10:20:30', '01020308'), \
															(NULL, 'World', NULL, NULL, NULL, NULL),	\
															(NULL, 'Foo', NULL, NULL, NULL, NULL), \
															(43, 'Bar', '2021-02-03 11:21:31', '2021-02-03', '11:21:31', '01020306')", NULL, NULL)) != NULL)
		error(err)

	if ((err = monetdbe_query(mdbe, "SELECT * FROM test; ", &result, NULL)) != NULL)
		error(err)
	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
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
	if ((err = monetdbe_append(mdbe, "sys", "test", rcol, 6)) != NULL)
		error(err)
	/* we can now cleanup the previous query */
	if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
		error(err)

	if ((err = monetdbe_query(mdbe, "SELECT * FROM test; ", &result, NULL)) != NULL)
		error(err)
	fprintf(stdout, "Query result after append with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
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
