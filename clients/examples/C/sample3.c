/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mapi.h>
#ifdef _MSC_VER
#define LLFMT "%I64d"
#else
#define LLFMT "%lld"
#endif

#define die(dbh,hdl)	do {						\
				if (hdl)				\
					mapi_explain_result(hdl,stderr); \
				else if (dbh)				\
					mapi_explain(dbh,stderr);	\
				else					\
					fprintf(stderr,"command failed\n"); \
				exit(-1);				\
			} while (0)

int
main(int argc, char **argv)
{
	Mapi dbh;
	MapiHdl hdl = NULL;
	mapi_int64 rows, i;
	char *parm[] = { "peter", 0 };
	char *parm2[] = { "25", 0 };
	int j;

	if (argc != 4) {
		printf("usage:%s <host> <port> <language>\n", argv[0]);
		exit(-1);
	}

	dbh = mapi_connect(argv[1], atoi(argv[2]), "monetdb", "monetdb", argv[3], NULL);
	if (dbh == NULL || mapi_error(dbh))
		die(dbh, hdl);

	/* mapi_trace(dbh, 1); */
	if (strcmp(argv[3], "sql") == 0) {
		/* switch of autocommit */
		if (mapi_setAutocommit(dbh, 0) != MOK || mapi_error(dbh))
			die(dbh,NULL);
		if ((hdl = mapi_query(dbh, "create table emp(name varchar(20), age int)")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "insert into emp values('John', 23)")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "insert into emp values('Mary', 22)")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "select * from emp")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
	} else if (strcmp(argv[3], "mal") == 0) {
		if ((hdl = mapi_query(dbh, "emp := bat.new(:oid,:str);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "age := bat.new(:oid,:int);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query_array(dbh, "bat.append(emp,\"?\");", parm)) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if ((hdl = mapi_query_array(dbh, "bat.append(age,?);", parm2)) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "io.print(emp,age);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
	} else {
		fprintf(stderr, "%s: unknown language, only mal and sql supported\n", argv[0]);
		exit(1);
	}

	/* Retrieve all tuples in the client cache first */
	rows = mapi_fetch_all_rows(hdl);
	if (mapi_error(dbh))
		die(dbh, hdl);
	printf("rows received " LLFMT " with %d fields\n", rows, mapi_get_field_count(hdl));

	/* Interpret the cache as a two-dimensional array */
	for (i = 0; i < rows; i++) {
		if (mapi_seek_row(hdl, i, MAPI_SEEK_SET) || mapi_fetch_row(hdl) == 0)
			break;
		for (j = 0; j < mapi_get_field_count(hdl); j++) {
			printf("%s=%s ", mapi_get_name(hdl, j), mapi_fetch_field(hdl, j));
		}
		printf("\n");
	}
	if (mapi_error(dbh))
		die(dbh, hdl);
	if (mapi_close_handle(hdl) != MOK)
		die(dbh, hdl);
	mapi_destroy(dbh);

	return 0;
}
