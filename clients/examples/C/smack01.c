/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifdef _MSC_VER
/* suppress deprecation warning for snprintf */
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mapi.h>

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
	int i;
	char buf[40], *line;
	int port;
	int lang = 1;
	char *l = "sql";

	if (argc != 2 && argc != 3) {
		printf("usage: smack01 <port> [<language>]\n");
		exit(-1);
	}
	if (argc == 3) {
		l = argv[2];
		if (strcmp(argv[2], "sql") == 0) 
			lang = 1;
		else if (strcmp(argv[2], "mal") == 0)
			lang = 3;
	}


	port = atol(argv[1]);
	dbh = mapi_connect("localhost", port, "monetdb", "monetdb", l, NULL);
	if (dbh == NULL || mapi_error(dbh))
		die(dbh, hdl);

	for (i = 0; i < 1000; i++) {
		/* printf("setup connection %d\n", i); */
		mapi_reconnect(dbh);
		if (mapi_error(dbh))
			die(dbh, hdl);

		/* switch of autocommit */
		if (lang==1 && (mapi_setAutocommit(dbh, 0) != MOK || mapi_error(dbh)))
			die(dbh,NULL);

		if (lang==1)
			snprintf(buf, 40, "select %d;", i);
		else
			snprintf(buf, 40, "io.print(%d);", i);
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
