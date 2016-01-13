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
	/* a parameter binding test */
	char *nme = 0;
	int age = 0;
	char *parm[] = { "peter", 0 };
	char *parm2[] = { "25", 0 };
	char *parm3[] = { "peter", "25", 0 };
	Mapi dbh= NULL;
	MapiHdl hdl = NULL;

	if (argc != 4) {
		printf("usage:%s <host> <port> <language>\n", argv[0]);
		exit(-1);
	}

	dbh = mapi_connect(argv[1], atoi(argv[2]), "monetdb", "monetdb", argv[3], NULL);
	if (dbh == NULL || mapi_error(dbh))
		die(dbh, hdl);

	/* mapi_trace(dbh,1); */
	if (strcmp(argv[3], "sql") == 0) {
		/* switch of autocommit */
		if (mapi_setAutocommit(dbh, 0) != MOK || mapi_error(dbh))
			die(dbh,NULL);
		if ((hdl = mapi_query(dbh, "create table emp(name varchar(20), age int)")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query_array(dbh, "insert into emp values('?', ?)", parm3)) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "select * from emp")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_bind(hdl, 0, &nme))
			die(dbh, hdl);
		if (mapi_bind_var(hdl, 1, MAPI_INT, &age))
			die(dbh, hdl);
		while (mapi_fetch_row(hdl)) {
			printf("%s is %d\n", nme, age);
		}
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
		if (mapi_bind(hdl, 1, &nme))
			die(dbh, hdl);
		if (mapi_bind_var(hdl, 2, MAPI_INT, &age))
			die(dbh, hdl);
		while (mapi_fetch_row(hdl)) {
			printf("%s is %d\n", nme, age);
		}
	} else {
		fprintf(stderr, "%s: unknown language, only mal and sql supported\n", argv[0]);
		exit(1);
	}

	if (mapi_error(dbh))
		die(dbh, hdl);
	if (mapi_close_handle(hdl) != MOK)
		die(dbh, hdl);
	mapi_destroy(dbh);

	return 0;
}
