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
#include <monet_options.h>
#include "Mapi.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
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
	fprintf(stderr, " -c config   | --config=file    /* config filename */\n");
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
	opt *set = NULL;
	int setlen;
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
	static struct option long_options[] = {
		{"config", 1, 0, 'c'},
		{"host", 1, 0, 'h'},
		{"passwd", 2, 0, 'P'},
		{"port", 1, 0, 'p'},
		{"database", 1, 0, 'd'},
		{"set", 1, 0, 'S'},
		{"trace", 2, 0, 't'},
		{"user", 2, 0, 'u'},
		{"quiet", 0, 0, 'q'},
		{"help", 0, 0, '?'},
		{0, 0, 0, 0}
	};

	if ((setlen = mo_builtin_settings(&set)) == 0)
		usage(argv[0]);

	while ((c = getopt_long(argc, argv, "c:u::p:P::d:qh:s:t::?", long_options, NULL)) != -1) {
		switch (c) {
		case 'c':
			setlen = mo_add_option(&set, setlen, opt_cmdline, "config", optarg);
			break;
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
			setlen = mo_add_option(&set, setlen, opt_cmdline, "port", optarg);
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
		case 'S':{
			char *eq = strchr(optarg, '=');

			if (eq)
				*eq = 0;
			setlen = mo_add_option(&set, setlen, opt_cmdline, optarg, eq ? eq + 1 : "");
			if (eq)
				*eq = '=';
			break;
		}
		case '?':
			usage(argv[0]);
		default:
			usage(argv[0]);
		}
	}

	setlen = mo_system_config(&set, setlen);

	if (port == 0) {
		char *s = "mapi_port";
                int p = DEFAULTPORT;

		if ((s = mo_find_option(set, setlen, s)) != NULL) {
			port = strtol(s, NULL, 10);
		} else {
			port = p;
		}
	}

	if (host == NULL) {
		host = mo_find_option(set, setlen, "host");
		if (host == NULL)
			host = "localhost";
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

	c = dump_tables(mid, stdout);

	mapi_disconnect(mid);
	return c;

}
