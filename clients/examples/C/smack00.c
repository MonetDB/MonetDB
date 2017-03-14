/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
	int i, port, n = 20000;
	char buf[40];
	int lang = 1;
	char *l = "sql";

	/* char *line; */

	if (argc != 2 && argc != 3) {
		printf("usage:smack00 <port> [<language>]\n");
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

	/* switch of autocommit */
	if (lang==1 && (mapi_setAutocommit(dbh, 0) != MOK || mapi_error(dbh)))
		die(dbh,NULL);

	for (i = 0; i < n; i++) {
		if (lang==1)
			snprintf(buf, 40, "select %d;", i);
		else
			snprintf(buf, 40, "io.print(%d);", i);
		if ((hdl = mapi_query(dbh, buf)) == NULL || mapi_error(dbh))
			die(dbh, hdl);
		while ( (/*line= */ mapi_fetch_line(hdl)) != NULL) {
			/*printf("%s \n", line); */
		}
		if (mapi_error(dbh))
			die(dbh, hdl);
		if (mapi_close_handle(hdl) != MOK)
			die(dbh, hdl);
	}
	mapi_destroy(dbh);

	return 0;
}
