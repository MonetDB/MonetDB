/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code is the Monet Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2004 CWI.
 * All Rights Reserved.
 */

#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>
#include <stdio.h>
#include <string.h>

#define die(X) (mapi_explain(X, stdout), exit(-1))

int main(int argc, char **argv)
{
	Mapi dbh;
	MapiHdl hdl;
	int rows, i,j;

	if (argc != 4) {
		printf("usage:%s <host> <port> <language>\n", argv[0]);
		exit(-1);
	}

	dbh = mapi_connect(argv[1], atoi(argv[2]), "monetdb", "monetdb", argv[3]);
	if (mapi_error(dbh))
		die(dbh);

	/* mapi_trace(dbh, 1); */
	if (strcmp(argv[3], "sql") == 0) {
		if ((hdl = mapi_query(dbh, "create table emp(name varchar(20), age int)")) == NULL) 
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "insert into emp values('John', 23)")) == NULL) 
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "insert into emp values('Mary', 22)")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "select * from emp")) == NULL)
			die(dbh);
	} else {
		if ((hdl = mapi_query(dbh, "var emp:= new(str,int);")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "emp.insert(\"John\",23);")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "emp.insert(\"Mary\",22);")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "print(emp);")) == NULL)
			die(dbh);
	}

	/* Retrieve all tuples in the client cache first */
	rows = mapi_fetch_all_rows(hdl);
	printf("rows received %d with %d fields\n", rows, mapi_get_field_count(hdl));

	/* Interpret the cache as a two-dimensional array */
	for (i = 0; i < rows; i++) {
		if (mapi_seek_row(hdl, i, MAPI_SEEK_SET) ||
		    mapi_fetch_row(hdl) == 0)
			break;
		for (j = 0; j < mapi_get_field_count(hdl); j++) {
			printf("%s=%s ", mapi_get_name(hdl, j), mapi_fetch_field(hdl, j));
		}
		printf("\n");
	}
	if (mapi_error(dbh))
		mapi_explain_query(hdl, stdout);
	mapi_close_handle(hdl);
	mapi_disconnect(dbh);

	return 0;
}
