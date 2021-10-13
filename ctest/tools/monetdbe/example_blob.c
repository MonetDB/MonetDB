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

static char hexit[] = "0123456789ABCDEF";

int
main(void)
{
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	monetdbe_result* result = NULL;

	// second argument is a string for the db directory or NULL for in-memory mode
	if (monetdbe_open(&mdbe, NULL, NULL ))
		error("Failed to open database")
	if ((err = monetdbe_query(mdbe, "CREATE TABLE test (name string, data blob)", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES ('Hello', '01020308')", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "SELECT name, data FROM test; ", &result, NULL)) != NULL)
		error(err)

	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			monetdbe_column* rcol;
			if ((err = monetdbe_result_fetch(result, &rcol, c)) != NULL)
				error(err)
			switch (rcol->type) {
				case monetdbe_blob: {
					monetdbe_column_blob * col = (monetdbe_column_blob *) rcol;
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
