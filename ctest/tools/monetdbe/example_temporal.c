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

int
main(void)
{
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	monetdbe_result* result = NULL;

	// second argument is a string for the db directory or NULL for in-memory mode
	if (monetdbe_open(&mdbe, NULL, NULL))
		error("Failed to open database")
	if ((err = monetdbe_query(mdbe, "CREATE TABLE test (x integer, d date, t time, ts timestamp, y string)", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (42, '2020-1-1', '13:13:30', '2020-1-1 13:13:30', 'Hello'), (NULL, NULL, NULL, NULL, 'World')", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "SELECT x, d, t, ts, y FROM test; ", &result, NULL)) != NULL)
		error(err)

	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			monetdbe_column* rcol;
			if ((err = monetdbe_result_fetch(result, &rcol, c)) != NULL)
				error(err)
			switch (rcol->type) {
				case monetdbe_int32_t: {
					monetdbe_column_int32_t * col = (monetdbe_column_int32_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdbe_date: {
					monetdbe_column_date * col = (monetdbe_column_date *) rcol;
					if (date_eq(col->data[r], col->null_value)) {
						printf("NULL");
					} else {
						printf("%d-%d-%d", col->data[r].year, col->data[r].month, col->data[r].day);
					}
					break;
				}
				case monetdbe_time: {
					monetdbe_column_time * col = (monetdbe_column_time *) rcol;
					if (time_eq(col->data[r], col->null_value)) {
						printf("NULL");
					} else {
						printf("%d:%d:%d.%d", col->data[r].hours, col->data[r].minutes, col->data[r].seconds, col->data[r].ms);
					}
					break;
				}
				case monetdbe_timestamp: {
					monetdbe_column_timestamp * col = (monetdbe_column_timestamp *) rcol;
					if (date_eq(col->data[r].date, col->null_value.date) && time_eq(col->data[r].time, col->null_value.time)) {
						printf("NULL");
					} else {
						printf("%d-%d-%d ", col->data[r].date.year, col->data[r].date.month, col->data[r].date.day);
						printf("%d:%d:%d.%d", col->data[r].time.hours, col->data[r].time.minutes, col->data[r].time.seconds, col->data[r].time.ms);
					}
					break;
				}
				case monetdbe_str: {
					monetdbe_column_str * col = (monetdbe_column_str *) rcol;
					if (col->is_null(col->data+r)) {
						printf("NULL");
					} else {
						printf("%s", (char*) col->data[r]);
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
