/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mapilib/Mapi.h>
#ifdef _MSC_VER
#define snprintf _snprintf
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
	int rows;

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
	} else if (strcmp(argv[3], "mal") == 0) {
		if ((hdl = mapi_query(dbh, "emp := bat.new(:str,:int);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "bat.insert(emp,\"John\",23);")) == NULL)
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "bat.insert(emp,\"Mary\",22);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "io.print(emp);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
	} else {
		if ((hdl = mapi_query(dbh, "var emp:= new(str,int);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "emp.insert(\"John\",23);")) == NULL)
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "emp.insert(\"Mary\",22);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "print(emp);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
	}

	rows = mapi_fetch_all_rows(hdl);
	if (mapi_error(dbh))
		die(dbh, hdl);
	printf("rows received %d\n", rows);
	while (mapi_fetch_row(hdl)) {
		char *nme = mapi_fetch_field(hdl, 0);
		char *age = mapi_fetch_field(hdl, 1);

		printf("%s is %s\n", nme, age);
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
