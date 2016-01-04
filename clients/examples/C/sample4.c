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
	mapi_int64 rows;

	if (argc != 4) {
		printf("usage:%s <host> <port> <language>\n", argv[0]);
		exit(-1);
	}

	dbh = mapi_connect(argv[1], atoi(argv[2]), "monetdb", "monetdb", argv[3], NULL);
	if (dbh == NULL || mapi_error(dbh))
		die(dbh, hdl);

	mapi_cache_limit(dbh, 2);
	/* mapi_trace_log(dbh, "/tmp/mapilog"); */
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
		rows = mapi_fetch_all_rows(hdl);
		if (mapi_error(dbh))
			die(dbh, hdl);
		printf("rows received " LLFMT "\n", rows);
		while (mapi_fetch_row(hdl)) {
			char *nme = mapi_fetch_field(hdl, 0);
			char *age = mapi_fetch_field(hdl, 1);

			printf("%s is %s\n", nme, age);
		}
	} else if (strcmp(argv[3], "mal") == 0) {
		if ((hdl = mapi_query(dbh, "emp := bat.new(:oid,:str);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "age := bat.new(:oid,:int);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "bat.append(emp, \"John\");")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "bat.append(age, 23);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "bat.append(emp, \"Mary\");")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "bat.append(age, 22);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "io.print(emp,age);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		rows = mapi_fetch_all_rows(hdl);
		if (mapi_error(dbh))
			die(dbh, hdl);
		printf("rows received " LLFMT "\n", rows);
		while (mapi_fetch_row(hdl)) {
			char *nme = mapi_fetch_field(hdl, 1);
			char *age = mapi_fetch_field(hdl, 2);

			printf("%s is %s\n", nme, age);
		}
	} else {
		fprintf(stderr, "%s: unknown language, only mal and sql supported\n", argv[0]);
		exit(1);
	}

	if (mapi_error(dbh))
		die(dbh, hdl);
	/* mapi_stat(dbh);
	   printf("mapi_ping %d\n",mapi_ping(dbh)); */
	if (mapi_close_handle(hdl) != MOK)
		die(dbh, hdl);
	mapi_destroy(dbh);

	return 0;
}
