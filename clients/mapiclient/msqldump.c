/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#ifndef HAVE_GETOPT_LONG
#  include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif
#include "mapi.h"
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>

#ifdef HAVE_FTIME
#include <sys/timeb.h>
#endif

#ifdef TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# ifdef HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif

#include "stream.h"
#include "msqldump.h"
#include "mprompt.h"
#include "dotmonetdb.h"

__declspec(noreturn) static void usage(const char *prog, int xit)
	__attribute__((__noreturn__));

static void
usage(const char *prog, int xit)
{
	fprintf(stderr, "Usage: %s [ options ] [ dbname ]\n", prog);
	fprintf(stderr, "\nOptions are:\n");
	fprintf(stderr, " -h hostname | --host=hostname    host to connect to\n");
	fprintf(stderr, " -p portnr   | --port=portnr      port to connect to\n");
	fprintf(stderr, " -u user     | --user=user        user id\n");
	fprintf(stderr, " -d database | --database=database  database to connect to\n");
	fprintf(stderr, " -f          | --functions        dump functions\n");
	fprintf(stderr, " -t table    | --table=table      dump a database table\n");
	fprintf(stderr, " -D          | --describe         describe database\n");
	fprintf(stderr, " -N          | --inserts          use INSERT INTO statements\n");
	fprintf(stderr, " -q          | --quiet            don't print welcome message\n");
	fprintf(stderr, " -X          | --Xdebug           trace mapi network interaction\n");
	fprintf(stderr, " -?          | --help             show this usage message\n");
	fprintf(stderr, "--functions and --table are mutually exclusive\n");
	exit(xit);
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
	int describe = 0;
	int functions = 0;
	int useinserts = 0;
	int c;
	Mapi mid;
	int quiet = 0;
	stream *out;
	char user_set_as_flag = 0;
	char *table = NULL;
	static struct option long_options[] = {
		{"host", 1, 0, 'h'},
		{"port", 1, 0, 'p'},
		{"database", 1, 0, 'd'},
		{"describe", 0, 0, 'D'},
		{"functions", 0, 0, 'f'},
		{"table", 1, 0, 't'},
		{"inserts", 0, 0, 'N'},
		{"Xdebug", 0, 0, 'X'},
		{"user", 1, 0, 'u'},
		{"quiet", 0, 0, 'q'},
		{"help", 0, 0, '?'},
		{0, 0, 0, 0}
	};

	parse_dotmonetdb(&user, &passwd, &dbname, NULL, NULL, NULL, NULL);

	while ((c = getopt_long(argc, argv, "h:p:d:Dft:NXu:q?", long_options, NULL)) != -1) {
		switch (c) {
		case 'u':
			if (user)
				free(user);
			user = strdup(optarg);
			user_set_as_flag = 1;
			break;
		case 'h':
			host = optarg;
			break;
		case 'p':
			assert(optarg != NULL);
			port = atoi(optarg);
			break;
		case 'd':
			if (dbname)
				free(dbname);
			dbname = strdup(optarg);
			break;
		case 'D':
			describe = 1;
			break;
		case 'N':
			useinserts = 1;
			break;
		case 'f':
			if (table)
				usage(argv[0], -1);
			functions = 1;
			break;
		case 't':
			if (table || functions)
				usage(argv[0], -1);
			table = optarg;
			break;
		case 'q':
			quiet = 1;
			break;
		case 'X':
			trace = MAPI_TRACE;
			break;
		case '?':
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			usage(argv[0], strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0 ? 0 : -1);
		default:
			usage(argv[0], -1);
		}
	}

	if (optind == argc - 1)
		dbname = strdup(argv[optind]);
	else if (optind != argc)
		usage(argv[0], -1);

	/* when config file would provide defaults */
	if (user_set_as_flag)
		passwd = NULL;

	if (user == NULL)
		user = simple_prompt("user", BUFSIZ, 1, prompt_getlogin());
	if (passwd == NULL)
		passwd = simple_prompt("password", BUFSIZ, 0, NULL);

	mid = mapi_connect(host, port, user, passwd, "sql", dbname);
	if (user)
		free(user);
	if (passwd)
		free(passwd);
	if (dbname)
		free(dbname);
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
			fprintf(stderr, "%s", motd);
	}
	mapi_trace(mid, trace);
	mapi_cache_limit(mid, 10000);

	out = file_wastream(stdout, "stdout");
	if (out == NULL) {
		fprintf(stderr, "failed to allocate stream\n");
		exit(2);
	}
	if (!quiet) {
		char buf[27];
		time_t t = time(0);
		char *p;

#ifdef HAVE_CTIME_R3
		ctime_r(&t, buf, sizeof(buf));
#else
#ifdef HAVE_CTIME_R
		ctime_r(&t, buf);
#else
		strncpy(buf, ctime(&t), sizeof(buf));
#endif
#endif
		if ((p = strrchr(buf, '\n')) != NULL)
			*p = 0;
		mnstr_printf(out, "-- msqldump %s %s%s %s\n",
			     describe ? "describe" : "dump",
			     functions ? "functions" : table ? "table " : "database",
			     table ? table : "", buf);
		dump_version(mid, out, "--");
	}
	if (functions)
		c = dump_functions(mid, out, NULL, NULL);
	else if (table)
		c = dump_table(mid, NULL, table, out, describe, 1, useinserts);
	else
		c = dump_database(mid, out, describe, useinserts);
	mnstr_flush(out);

	mapi_destroy(mid);
	if (mnstr_errnr(out)) {
		fprintf(stderr, "%s: %s", argv[0], mnstr_error(out));
		return 1;
	}

	mnstr_destroy(out);
	return c;
}
