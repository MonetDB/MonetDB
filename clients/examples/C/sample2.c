/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
	char *parm[] = { "peter", "25", 0 };
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
		if ((hdl = mapi_query_array(dbh, "insert into emp values('?', ?)", parm)) == NULL || mapi_error(dbh))
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
		if ((hdl = mapi_query_array(dbh, "bat.insert(emp,\"?\",?);", parm)) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		if ((hdl = mapi_query(dbh, "io.print(emp);")) == NULL || mapi_error(dbh))
			die(dbh, hdl);
	} else {
		fprintf(stderr, "%s: unknown language, only mal and sql supported\n", argv[0]);
		exit(1);
	}

	if (mapi_bind(hdl, 0, &nme))
		die(dbh, hdl);
	if (mapi_bind_var(hdl, 1, MAPI_INT, &age))
		die(dbh, hdl);
	while (mapi_fetch_row(hdl)) {
		printf("%s is %d\n", nme, age);
	}
	if (mapi_error(dbh))
		die(dbh, hdl);
	if (mapi_close_handle(hdl) != MOK)
		die(dbh, hdl);
	mapi_destroy(dbh);

	return 0;
}
