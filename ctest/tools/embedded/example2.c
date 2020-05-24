/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_embedded.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

int
main(void)
{
	char* err = NULL;
	monetdb_connection conn = NULL;
	monetdb_result* result = NULL;

	// first argument is a string for the db directory or NULL for in-memory mode
	if ((err = monetdb_startup(NULL, 0)) != NULL)
		error(err)
	if ((err = monetdb_connect(&conn)) != NULL)
		error(err)
	if ((err = monetdb_query(conn, "CREATE TABLE test (b bool, t tinyint, s smallint, x integer, l bigint, "
#if HAVE_HGE
		"h hugeint, "
#else
		"h bigint, "
#endif
		"f float, d double, y string)", NULL, NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdb_query(conn, "INSERT INTO test VALUES (TRUE, 42, 42, 42, 42, 42, 42.42, 42.42, 'Hello'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'World')", NULL, NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdb_query(conn, "SELECT b, t, s, x, l, h, f, d, y FROM test; ", &result, NULL, NULL)) != NULL)
		error(err)

	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			monetdb_column* rcol;
			if ((err = monetdb_result_fetch(conn, &rcol, result, c)) != NULL)
				error(err)
			switch (rcol->type) {
				case monetdb_bool: {
					monetdb_column_bool * col = (monetdb_column_bool *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%c", col->data[r]?'T':'F');
					}
					break;
				}
				case monetdb_int8_t: {
					monetdb_column_int8_t * col = (monetdb_column_int8_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdb_int16_t: {
					monetdb_column_int16_t * col = (monetdb_column_int16_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdb_int32_t: {
					monetdb_column_int32_t * col = (monetdb_column_int32_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdb_int64_t: {
					monetdb_column_int64_t * col = (monetdb_column_int64_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%" PRId64, col->data[r]);
					}
					break;
				}
#if HAVE_HGE
				case monetdb_int128_t: {
					monetdb_column_int128_t * col = (monetdb_column_int128_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%" PRId64 "%" PRId64, (int64_t)(col->data[r]>>64), (int64_t)(col->data[r]));
					}
					break;
				}
#endif
				case monetdb_float: {
					monetdb_column_float * col = (monetdb_column_float *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%f", col->data[r]);
					}
					break;
				}
				case monetdb_double: {
					monetdb_column_double * col = (monetdb_column_double *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%f", col->data[r]);
					}
					break;
				}
				case monetdb_str: {
					monetdb_column_str * col = (monetdb_column_str *) rcol;
					if (col->is_null(col->data[r])) {
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

	if ((err = monetdb_cleanup_result(conn, result)) != NULL)
		error(err)
	if ((err = monetdb_disconnect(conn)) != NULL)
		error(err)
	if ((err = monetdb_shutdown()) != NULL)
		error(err)
	return 0;
}
