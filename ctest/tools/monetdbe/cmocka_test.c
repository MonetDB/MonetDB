/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <setjmp.h>
#include "cmocka.h"
#include "test_helper.h"

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

static int setup(void **state) {
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	if ((err = monetdbe_open(&mdbe, NULL, NULL)) != NULL)
		error(err)

	*state = mdbe;
     return 0;
}

static int teardown(void **state) {
	char* err = NULL;

	monetdbe_database mdbe = *state;

	if ((err = monetdbe_close(mdbe)) != NULL)
		error(err)
     return 0;
}

static void create_table_test(void **state) {
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe, "CREATE TABLE test (x integer, y string)", NULL, NULL);

	assert_null(err);
}

static void populate_table_test(void **state) {
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe, "INSERT INTO test VALUES (42, 'Hello, '), (58, 'World!')", NULL, NULL);

	assert_null(err);
}

static void query_table_test(void **state) {

	monetdbe_result* result = NULL;
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe, "SELECT MIN(x), CONCAT(MIN(y), MAX(y)) FROM test; ", &result, NULL);

	assert_null(err);

	assert_int_equal((int) result->nrows, 1);
	assert_int_equal((int) result->ncols, 2);
	monetdbe_column* rcol;

	CHECK_COLUMN(mdbe, result, 0, int32_t, {42});

	err = monetdbe_result_fetch(result, &rcol, 1);
	assert_null(err);

	monetdbe_column_str * col_y = (monetdbe_column_str *) rcol;

	assert_string_equal((const char*) col_y->data[0], "Hello, World!");

	err = monetdbe_cleanup_result(mdbe, result);

	assert_null(err);
}

static void query_table_test2(void **state) {

	monetdbe_result* result = NULL;
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe, "SELECT x, y FROM test; ", &result, NULL);

	assert_null(err);

	assert_int_equal((int) result->nrows, 2);
	assert_int_equal((int) result->ncols, 2);

	assert_true(CHECK_COLUMN(mdbe, result, 0, int32_t, {42, 58}));
	assert_true(CHECK_COLUMN(mdbe, result, 1, str, {"Hello, ", "World!"}));

	err = monetdbe_cleanup_result(mdbe, result);

	assert_null(err);
}

int
main(void)
{
	const struct CMUnitTest tests[] = {
        cmocka_unit_test(create_table_test),
		cmocka_unit_test(populate_table_test),
		cmocka_unit_test(query_table_test),
		cmocka_unit_test(query_table_test2)
    };

	return cmocka_run_group_tests_name("SQL queries for MonetDBe", tests, setup, teardown);
}
