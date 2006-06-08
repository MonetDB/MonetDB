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
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
 * All Rights Reserved.
 */

#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>
#include <stdio.h>
#include <string.h>

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
	int i;
	char buf[40], *line;
	int port;
	int sql = 0;

	if (argc != 2 && argc != 3) {
		printf("usage: smack01 <port> [<language>]\n");
		exit(-1);
	}
	if (argc == 3)
		sql = strcmp(argv[2], "sql") == 0;

	port = atol(argv[1]);

	dbh = mapi_mapi("localhost", port, "monetdb", "monetdb", sql ? "sql" : 0);
	for (i = 0; i < 1000; i++) {
		/* printf("setup connection %d\n", i); */
		mapi_reconnect(dbh);
		if (dbh == NULL || mapi_error(dbh))
			die(dbh, hdl);

		/* switch of autocommit */
		if (sql && (mapi_setAutocommit(dbh, 0) != MOK || mapi_error(dbh)))
			die(dbh,NULL);

		if (sql)
			snprintf(buf, 40, "select %d;", i);
		else
			snprintf(buf, 40, "print(%d);", i);
		if ((hdl = mapi_query(dbh, buf)) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		while ((line = mapi_fetch_line(hdl))) {
			printf("%s \n", line);
		}
		if (mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
		mapi_disconnect(dbh);
		/* printf("close connection %d\n", i); */
	}

	mapi_destroy(dbh);

	return 0;
}
