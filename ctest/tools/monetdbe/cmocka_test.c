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
#include <setjmp.h>
#include "cmocka.h"
#include "test_helper.h"

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

static int setup(void **state) {
	monetdbe_database mdbe = NULL;
	if (monetdbe_open(&mdbe, NULL, NULL))
		error("Failed to open database")

	*state = mdbe;
     return 0;
}

static int teardown(void **state) {
	monetdbe_database mdbe = *state;

	if (monetdbe_close(mdbe))
		error("Failed to close database")
     return 0;
}

static void create_table_test(void **state) {
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe, "CREATE TABLE test (si SMALLINT,i INTEGER, bi BIGINT, c CLOB, b BLOB, d DATE, t TIME, ts TIMESTAMP)", NULL, NULL);

	assert_null(err);
}

static void populate_table_test(void **state) {
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe,
	"INSERT INTO test VALUES "
		"(4, 40, 400, 'aaa', x'aaaaaa', '2020-06-17', '12:00:00', '2020-06-17 12:00:00'),"
		"(6, 60, 600, 'ccc', x'cccccc', '2022-06-17', '14:00:00', '2022-06-17 14:00:00'),"
		"(5, 50, 500, 'bbb', x'bbbbbb', '2021-06-17', '13:00:00', '2021-06-17 13:00:00'),"
		"(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
	NULL, NULL);

	assert_null(err);
}

static void query_table_test(void **state) {

	monetdbe_result* result = NULL;
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe, "SELECT MIN(si), MAX(i) FROM test; ", &result, NULL);

	assert_null(err);

	assert_int_equal((int) result->ncols, 2);

	assert_true(CHECK_COLUMN(result, 0, int16_t, 4));
	assert_true(CHECK_COLUMN(result, 1, int32_t, 60));

	err = monetdbe_cleanup_result(mdbe, result);

	assert_null(err);
}

static void query_table_test2(void **state) {

	monetdbe_result* result = NULL;
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe, "SELECT si, i, bi, c, b, d, t, ts FROM test; ", &result, NULL);

	assert_null(err);

	assert_int_equal((int) result->ncols, 8);

	int a;

	assert_true(CHECK_COLUMN(result, 0, int16_t, a=4, 6, 5, Null));
	assert_true(CHECK_COLUMN(result, 1, int32_t, 40, 60, 50, Null));
	assert_true(CHECK_COLUMN(result, 2, int64_t, 400, 600, 500, Null));
	assert_true(CHECK_COLUMN(result, 3, str, "aaa", "ccc", "bbb", Null));
	assert_true(CHECK_COLUMN(result, 4, blob, (3, "\xaa\xaa\xaa"), (3, "\xcc\xcc\xcc"), (3, "\xbb\xbb\xbb"), Null));
	assert_true(CHECK_COLUMN(result, 5, date, (17, 6, 2020), (17, 6, 2022), (17, 6, 2021), Null));
	assert_true(CHECK_COLUMN(result, 6, time, (0, 0, 0, 12), (0, 0, 0, 14), (0, 0, 0, 13), Null));
	assert_true(CHECK_COLUMN(result, 7, timestamp, ((17, 6, 2020), (0, 0, 0, 12)), ((17, 6, 2022), (0, 0, 0, 14)), ((17, 6, 2021), (0, 0, 0, 13)), Null));

	err = monetdbe_cleanup_result(mdbe, result);

	assert_null(err);
}

static void query_table_test3(void **state) {

	monetdbe_result* result = NULL;
	monetdbe_database mdbe = *state;
    char* err;

	err = monetdbe_query(mdbe, "SELECT SUM(si), SUM(i), SUM(bi) FROM test; ", &result, NULL);

	assert_null(err);

	assert_int_equal((int) result->ncols, 3);

	/* TODO: Figure out what is expected here.
	assert_true(CHECK_COLUMN(result, 0, BIGGEST_INTEGER_TPE, {{15}}));
	assert_true(CHECK_COLUMN(result, 1, BIGGEST_INTEGER_TPE, {{150}}));
	assert_true(CHECK_COLUMN(result, 2, BIGGEST_INTEGER_TPE, {1500}));
	*/

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
		cmocka_unit_test(query_table_test2),
		cmocka_unit_test(query_table_test3)
    };

	return cmocka_run_group_tests_name("SQL queries for MonetDBe", tests, setup, teardown);
}
