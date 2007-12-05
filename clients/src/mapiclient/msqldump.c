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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

#include "clients_config.h"
#include "monet_utils.h"
#ifndef HAVE_GETOPT_LONG
#  include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif
#include "mapilib/Mapi.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "stream.h"
#include "msqldump.h"
#include "mprompt.h"

#ifndef HAVE_GETLOGIN
#define getlogin() "win32"
#endif

static void
usage(const char *prog)
{
	fprintf(stderr, "Usage: %s [ options ]\n", prog);
	fprintf(stderr, "Options are:\n");
	fprintf(stderr, " -h hostname | --host=hostname  /* host to connect to */\n");
	fprintf(stderr, " -P passwd   | --passwd=passwd  /* password */\n");
	fprintf(stderr, " -p portnr   | --port=portnr    /* port to connect to */\n");
	fprintf(stderr, " -d database | --database=database /* database to connect to */\n");
	fprintf(stderr, " -q          | --quiet          /* don't print welcome message */\n");
	fprintf(stderr, " -t          | --trace          /* trace mapi network interaction */\n");
	fprintf(stderr, " -u user     | --user=user      /* user id */\n");
	fprintf(stderr, " -?          | --help           /* show this usage message */\n");
	exit(-1);
}

/* hardwired defaults, only used if monet environment cannot be found */
#define DEFAULTPORT 50000	

int
main(int argc, char **argv)
{
	int port = 0;
	char *user = NULL;
	char *passwd = NULL;
	char *host = NULL;
	char *dbname = NULL;
	int trace = 0;
	int guest = 1;
	int c;
	Mapi mid;
	int quiet = 0;
	stream *out;
	static struct option long_options[] = {
		{"host", 1, 0, 'h'},
		{"passwd", 2, 0, 'P'},
		{"port", 1, 0, 'p'},
		{"database", 1, 0, 'd'},
		{"trace", 2, 0, 't'},
		{"user", 2, 0, 'u'},
		{"quiet", 0, 0, 'q'},
		{"help", 0, 0, '?'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "u::p:P::d:qh:t::?", long_options, NULL)) != -1) {
		switch (c) {
		case 'u':
			guest = 0;
			user = optarg; /* can be NULL */
			break;
		case 'P':
			guest = 0;
			passwd = optarg; /* can be NULL */
			break;
		case 'h':
			host = optarg;
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'd':
			dbname = optarg;
			break;
		case 'q':
			quiet = 1;
			break;
		case 't':
			trace = MAPI_TRACE;
			break;
		case '?':
			usage(argv[0]);
		default:
			usage(argv[0]);
		}
	}

	/* default to administrator account (eeks) when being called without
	 * any arguments, default to the current user if -u flag is given */
	if (guest) {
		user = "monetdb";
		passwd = "monetdb";
	} else {
		if (user == NULL)
			user = simple_prompt("User ", BUFSIZ, 1, getlogin());
		if (passwd == NULL)
			passwd = simple_prompt("Password", BUFSIZ, 0, NULL);
	}

	mid = mapi_connect(host, port, user, passwd, "sql", dbname);
	if (mid == NULL) {
		fprintf(stderr, "failed to allocate Mapi structure\n");
		exit(2);
	}
	if (mapi_error(mid)) {
		mapi_explain(mid, stderr);
		exit(2);
	}
	if (!quiet) {
		char *motd = mapi_get_motd(mid);

		if (motd)
			printf("%s", motd);
	}
	mapi_trace(mid, trace);

	out = file_wastream(stdout, "stdout");
	c = dump_tables(mid, out);
	stream_flush(out);

	mapi_disconnect(mid);
	return c;

}
