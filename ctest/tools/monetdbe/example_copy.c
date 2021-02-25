/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <monetdbe.h>
#include <unistd.h>
#include <limits.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}
#define delete_file(fpath) {  int del = remove(fpath); if (del) { error("Could not remove the file"); } }

int
main(void)
{
	char sql[1000];
	char csv_path[PATH_MAX];
	char* err = NULL;
	monetdbe_database mdbe;
	monetdbe_result* result = NULL;

	if (monetdbe_open(&mdbe, NULL, NULL))
		error("Failed to open database");

	if ((err = monetdbe_query(mdbe, "CREATE TABLE test (x integer, y string, ts timestamp, dt date, t time, b blob)", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (42, 'Hello', '2020-01-02 10:20:30', '2020-01-02', '10:20:30', '01020308'), \
															(NULL, 'World', NULL, NULL, NULL, NULL),	\
															(NULL, 'Foo', NULL, NULL, NULL, NULL), \
															(43, 'Bar', '2021-02-03 11:21:31', '2021-02-03', '11:21:31', '01020306')", NULL, NULL)) != NULL)
		error(err)

	// Get working directory and construct the CSV path
   	if (getcwd(csv_path, sizeof(csv_path)) == NULL) {
		error("Could not get the current working directory");
   	}
	strcat(csv_path, "/test.csv");

    strcpy(sql, "COPY SELECT * FROM test INTO '");
    strcat(sql, csv_path);
    strcat(sql, "' USING DELIMITERS ','");

	if ((err = monetdbe_query(mdbe, sql, NULL, NULL)) != NULL)
		error(err)
	
	if ((err = monetdbe_query(mdbe, "CREATE TABLE test_copy (x integer, y string, ts timestamp, dt date, t time, b blob)", NULL, NULL)) != NULL) {
		delete_file(csv_path)
		error(err)
	}

	memset(sql, 0, 1000);
	strcpy(sql, "COPY INTO test_copy FROM '");
    strcat(sql, csv_path);
    strcat(sql, "' DELIMITERS ','");

	if ((err = monetdbe_query(mdbe, sql, NULL, NULL)) != NULL) {
		delete_file(csv_path)
		error(err)
	}

	if ((err = monetdbe_query(mdbe, "SELECT * FROM test_copy; ", &result, NULL)) != NULL) {
		delete_file(csv_path)
		error(err)
	}

	if (result->nrows == 0) {
		delete_file(csv_path)
		error("Copy failed, database is empty")
	}

	delete_file(csv_path)

	if (monetdbe_close(mdbe))
		error("Failed to close database");

	return 0;
}
