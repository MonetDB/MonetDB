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

int
main(void)
{
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	monetdbe_result* result = NULL;
    monetdbe_options opts = { 0 };

	// second argument is a string for the db directory or NULL for in-memory mode
	if (monetdbe_open(&mdbe, NULL, &opts))
		error("Failed to open database")
	if ((err = monetdbe_query(mdbe, "CREATE TABLE test (b bool, t tinyint, s smallint, x integer, l bigint, "
#ifdef HAVE_HGE
		"h hugeint, "
#else
		"h bigint, "
#endif
		"f float, d double, y string)", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (TRUE, 42, 42, 42, 42, 42, 42.42, 42.42, 'Hello'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'World')", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "SELECT b, t, s, x, l, h, f, d, y FROM test; ", &result, NULL)) != NULL)
		error(err)

	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			monetdbe_column* rcol;
			if ((err = monetdbe_result_fetch(result, &rcol, c)) != NULL)
				error(err)
			switch (rcol->type) {
				case monetdbe_bool: {
					monetdbe_column_bool * col = (monetdbe_column_bool *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%c", col->data[r]?'T':'F');
					}
					break;
				}
				case monetdbe_int8_t: {
					monetdbe_column_int8_t * col = (monetdbe_column_int8_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdbe_int16_t: {
					monetdbe_column_int16_t * col = (monetdbe_column_int16_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdbe_int32_t: {
					monetdbe_column_int32_t * col = (monetdbe_column_int32_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdbe_int64_t: {
					monetdbe_column_int64_t * col = (monetdbe_column_int64_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%" PRId64, col->data[r]);
					}
					break;
				}
#ifdef HAVE_HGE
				case monetdbe_int128_t: {
					monetdbe_column_int128_t * col = (monetdbe_column_int128_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%" PRId64 "%" PRId64, (int64_t)(col->data[r]>>64), (int64_t)(col->data[r]));
					}
					break;
				}
#endif
				case monetdbe_float: {
					monetdbe_column_float * col = (monetdbe_column_float *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%f", col->data[r]);
					}
					break;
				}
				case monetdbe_double: {
					monetdbe_column_double * col = (monetdbe_column_double *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%f", col->data[r]);
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

	/* test empty results */
	if ((err = monetdbe_query(mdbe, "SELECT b, t, s, x, l, h, f, d, y FROM test where t > 127; ", &result, NULL)) != NULL)
		error(err)
	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	for (int64_t r = 0; r < result->nrows; r++) {
		/* fetching the meta data should work */
		for (size_t c = 0; c < result->ncols; c++) {
			monetdbe_column* rcol;
			if ((err = monetdbe_result_fetch(result, &rcol, c)) != NULL)
				error(err)
		}
	}
	if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
		error(err)

	monetdbe_statement *stmt = NULL;
	if ((err = monetdbe_prepare(mdbe, "SELECT b, t FROM test where t = ?; ", &stmt)) != NULL)
		error(err)
	char s = 42;
	if ((err = monetdbe_bind(stmt, &s, 0)) != NULL)
		error(err)
	if ((err = monetdbe_execute(stmt, &result, NULL)) != NULL)
		error(err)
	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
		error(err)
	if ((err = monetdbe_cleanup_statement(mdbe, stmt)) != NULL)
		error(err)

	if (monetdbe_close(mdbe))
		error("Failed to close database")
	return 0;
}
