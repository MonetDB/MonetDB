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
	if ((err = monetdb_query(conn, "CREATE TABLE test (x integer, y string)", NULL, NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdb_query(conn, "INSERT INTO test VALUES (42, 'Hello'), (NULL, 'World')", NULL, NULL, NULL)) != NULL)
		error(err)

	if ((err = monetdb_query(conn, "SELECT x, y FROM test; ", &result, NULL, NULL)) != NULL)
		error(err)
	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	monetdb_column* rcol[2];
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			if ((err = monetdb_result_fetch(conn, result, rcol+c, c)) != NULL)
				error(err)
			switch (rcol[c]->type) {
				case monetdb_int32_t: {
					monetdb_column_int32_t * col = (monetdb_column_int32_t *) rcol[c];
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdb_str: {
					monetdb_column_str * col = (monetdb_column_str *) rcol[c];
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
	if ((err = monetdb_append(conn, "sys", "test", rcol, 2)) != NULL)
		error(err)
	/* we can now cleanup the previous query */
	if ((err = monetdb_cleanup_result(conn, result)) != NULL)
		error(err)

	if ((err = monetdb_query(conn, "SELECT x, y FROM test; ", &result, NULL, NULL)) != NULL)
		error(err)
	fprintf(stdout, "Query result after append with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			if ((err = monetdb_result_fetch(conn, result, rcol+c, c)) != NULL)
				error(err)
			switch (rcol[c]->type) {
				case monetdb_int32_t: {
					monetdb_column_int32_t * col = (monetdb_column_int32_t *) rcol[c];
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdb_str: {
					monetdb_column_str * col = (monetdb_column_str *) rcol[c];
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
