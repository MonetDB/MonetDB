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
	/* a parameter binding test */
	char *nme= 0;
       	int age= 0;
	char *parm[]={"peter", "25", 0};
	Mapi dbh;
	MapiHdl hdl;

	if (argc != 4) {
		printf("usage:%s <host> <port> <language>\n", argv[0]);
		exit(-1);
	}

	dbh = mapi_connect(argv[1], atoi(argv[2]), "monetdb", "monetdb", argv[3]);
	if (mapi_error(dbh))
		die(dbh);

	/* mapi_trace(dbh,1);*/
	if (strcmp(argv[3], "sql") == 0) {
		if ((hdl = mapi_query(dbh, "create table emp(name varchar(20), age int)")) == NULL) 
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query_array(dbh, "insert into emp values('?', ?)", parm)) == NULL) 
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "select * from emp")) == NULL)
			die(dbh);
	} else {
		if ((hdl = mapi_query(dbh, "var emp:= new(str,int);")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query_array(dbh, "emp.insert(\"?\",?);", parm)) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "print(emp);")) == NULL)
			die(dbh);
	}

	if (mapi_bind(hdl, 0, &nme))
		mapi_explain_query(hdl, stdout);
	if (mapi_bind_var(hdl, 1, MAPI_INT, &age))
		mapi_explain_query(hdl, stdout);
	while (mapi_fetch_row(hdl)) {
		printf("%s is %d\n", nme, age);
	}
	mapi_close_handle(hdl);
	mapi_disconnect(dbh);

	return 0;
}
