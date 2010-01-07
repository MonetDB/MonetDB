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
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2010 MonetDB B.V.
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
#include <sys/stat.h>
#include <string.h>
#include "stream.h"
#include "msqldump.h"
#include "mprompt.h"

#ifdef NATIVE_WIN32
#define strdup _strdup
#endif

static void
usage(const char *prog)
{
	fprintf(stderr, "Usage: %s [ options ]\n", prog);
	fprintf(stderr, "\nOptions are:\n");
	fprintf(stderr, " -h hostname | --host=hostname    host to connect to\n");
	fprintf(stderr, " -p portnr   | --port=portnr      port to connect to\n");
	fprintf(stderr, " -u user     | --user=user        user id\n");
	fprintf(stderr, " -d database | --database=database  database to connect to\n");
	fprintf(stderr, " -f          | --functions        dump functions\n");
	fprintf(stderr, " -D          | --describe         describe database\n");
	fprintf(stderr, " -q          | --quiet            don't print welcome message\n");
	fprintf(stderr, " -t          | --trace            trace mapi network interaction\n");
	fprintf(stderr, " -?          | --help             show this usage message\n");
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
	int describe = 0;
	int functions = 0;
	int c;
	Mapi mid;
	int quiet = 0;
	stream *out;
	struct stat statb;
	stream *config = NULL;
	char user_set_as_flag = 0;
	static struct option long_options[] = {
		{"host", 1, 0, 'h'},
		{"port", 1, 0, 'p'},
		{"database", 1, 0, 'd'},
		{"describe", 0, 0, 'D'},
		{"functions", 0, 0, 'f'},
		{"trace", 2, 0, 't'},
		{"user", 1, 0, 'u'},
		{"quiet", 0, 0, 'q'},
		{"help", 0, 0, '?'},
		{0, 0, 0, 0}
	};

	if (getenv("DOTMONETDBFILE") == NULL) {
		if (stat(".monetdb", &statb) == 0) {
			config = open_rastream(".monetdb");
		} else if (getenv("HOME") != NULL) {
			char buf[1024];
			snprintf(buf, sizeof(buf), "%s/.monetdb", getenv("HOME"));
			if (stat(buf, &statb) == 0) {
				config = open_rastream(buf);
			}
		}
	} else {
		char *cfile = getenv("DOTMONETDBFILE");
		if (strcmp(cfile, "") != 0) {
			if (stat(cfile, &statb) == 0) {
				config = open_rastream(cfile);
			} else {
				fprintf(stderr, "failed to open file '%s': %s\n",
						cfile, strerror(errno));
			}
		}
	}

	if (config != NULL) {
		char buf[1024];
		char *q;
		ssize_t len;
		int line = 0;
		while ((len = stream_readline(config, buf, sizeof(buf) - 1)) > 0) {
			line++;
			buf[len - 1] = '\0'; /* drop newline */
			if (buf[0] == '#' || buf[0] == '\0')
				continue;
			if ((q = strchr(buf, '=')) == NULL) {
				fprintf(stderr, "%s:%d: syntax error: %s\n",
						stream_name(config), line, buf);
				continue;
			}
			*q++ = '\0';
			/* this basically sucks big time, as I can't easily set
			 * a default, hence I only do things I think are useful
			 * for now, needs a better solution */
			if (strcmp(buf, "user") == 0) {
				user = strdup(q); /* leak */
				q = NULL;
			} else if (strcmp(buf, "password") == 0 ||
					strcmp(buf, "passwd") == 0)
			{
				passwd = strdup(q); /* leak */
				q = NULL;
			} else if (strcmp(buf, "language") == 0) {
				/* make sure we don't barf about this as unknown
				 * property, it's supported by mclient */
				q = NULL;
			}
			if (q != NULL)
				fprintf(stderr, "%s:%d: unknown property: %s\n",
						stream_name(config), line, buf);
		}
		stream_destroy(config);
	}

	while ((c = getopt_long(argc, argv, "u:p:d:Dfqh:t::?", long_options, NULL)) != -1) {
		switch (c) {
		case 'u':
			user = optarg;
			user_set_as_flag = 1;
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
		case 'D':
			describe = 1;
			break;
		case 'f':
			functions = 1;
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

	/* when config file would provide defaults */
	if (user_set_as_flag)
		passwd = NULL;

	if (user == NULL)
		user = simple_prompt("user", BUFSIZ, 1, prompt_getlogin());
	if (passwd == NULL)
		passwd = simple_prompt("password", BUFSIZ, 0, NULL);

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
			fprintf(stderr, "%s", motd);
	}
	mapi_trace(mid, trace);
	mapi_cache_limit(mid, 10000);

	out = file_wastream(stdout, "stdout");
	if (functions)
		c = dump_functions(mid, out, NULL);
	else
		c = dump_tables(mid, out, describe);
	stream_flush(out);

	mapi_disconnect(mid);
	if (stream_errnr(out)) {
		fprintf(stderr, "%s: %s", argv[0], stream_error(out));
		return 1;
	}

	return c;

}
