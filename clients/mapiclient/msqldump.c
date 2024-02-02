/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include <sys/stat.h>
#include <string.h>
#include <time.h>

#include "stream.h"
#include "msqldump.h"
#define LIBMUTILS 1
#include "mprompt.h"
#include "mutils.h"		/* mercurial_revision */
#include "dotmonetdb.h"

static _Noreturn void usage(const char *prog, int xit);

static void
usage(const char *prog, int xit)
{
	fprintf(stderr, "Usage: %s [ options ] [ dbname ]\n", prog);
	fprintf(stderr, "\nOptions are:\n");
	fprintf(stderr, " -h hostname | --host=hostname    host to connect to\n");
	fprintf(stderr, " -p portnr   | --port=portnr      port to connect to\n");
	fprintf(stderr, " -u user     | --user=user        user id\n");
	fprintf(stderr, " -d database | --database=database  database to connect to\n");
	fprintf(stderr, " -o filename | --output=filename  write dump to filename\n");
	fprintf(stderr, " -O dir      | --outputdir=dir    write multi-file dump to dir\n");
	fprintf(stderr, " -x ext      | --compression=ext  compression method ext for multi-file dump\n");
	fprintf(stderr, " -f          | --functions        dump functions\n");
	fprintf(stderr, " -t table    | --table=table      dump a database table\n");
	fprintf(stderr, " -D          | --describe         describe database\n");
	fprintf(stderr, " -N          | --inserts          use INSERT INTO statements\n");
	fprintf(stderr, " -e          | --noescape         use NO ESCAPE\n");
	fprintf(stderr, " -q          | --quiet            don't print welcome message\n");
	fprintf(stderr, " -X          | --Xdebug           trace mapi network interaction\n");
	fprintf(stderr, " -?          | --help             show this usage message\n");
	fprintf(stderr, "--functions and --table are mutually exclusive\n");
	fprintf(stderr, "--output and --outputdir are mutually exclusive\n");
	fprintf(stderr, "--inserts and --outputdir are mutually exclusive\n");
	fprintf(stderr, "--compression only has effect with --outputdir\n");
	exit(xit);
}

int
#ifdef _MSC_VER
wmain(int argc, wchar_t **wargv)
#else
main(int argc, char **argv)
#endif
{
	int port = 0;
	const char *user = NULL;
	const char *passwd = NULL;
	const char *host = NULL;
	const char *dbname = NULL;
	const char *output = NULL;
	const char *outputdir = NULL;
	const char *ext = NULL;
	DotMonetdb dotfile = {0};
	bool trace = false;
	bool describe = false;
	bool functions = false;
	bool useinserts = false;
	bool noescape = false;
	int c;
	Mapi mid;
	bool quiet = false;
	stream *out;
	bool user_set_as_flag = false;
	char *table = NULL;
	static struct option long_options[] = {
		{"host", 1, 0, 'h'},
		{"port", 1, 0, 'p'},
		{"database", 1, 0, 'd'},
		{"output", 1, 0, 'o'},
		{"outputdir", 1, 0, 'O'},
		{"compression", 1, 0, 'x'},
		{"describe", 0, 0, 'D'},
		{"functions", 0, 0, 'f'},
		{"table", 1, 0, 't'},
		{"inserts", 0, 0, 'N'},
		{"noescape", 0, 0, 'e'},
		{"Xdebug", 0, 0, 'X'},
		{"user", 1, 0, 'u'},
		{"quiet", 0, 0, 'q'},
		{"version", 0, 0, 'v'},
		{"help", 0, 0, '?'},
		{0, 0, 0, 0}
	};
#ifdef _MSC_VER
	char **argv = malloc((argc + 1) * sizeof(char *));
	if (argv == NULL) {
		fprintf(stderr, "cannot allocate memory for argument conversion\n");
		exit(1);
	}
	for (int i = 0; i < argc; i++) {
		if ((argv[i] = wchartoutf8(wargv[i])) == NULL) {
			fprintf(stderr, "cannot convert argument to UTF-8\n");
			exit(1);
		}
	}
	argv[argc] = NULL;
#endif

	parse_dotmonetdb(&dotfile);
	user = dotfile.user;
	passwd = dotfile.passwd;
	dbname = dotfile.dbname;
	host = dotfile.host;
	port = dotfile.port;

	while ((c = getopt_long(argc, argv, "h:p:d:o:O:x:Dft:NeXu:qv?", long_options, NULL)) != -1) {
		switch (c) {
		case 'u':
			user = optarg;
			user_set_as_flag = true;
			break;
		case 'h':
			host = optarg;
			break;
		case 'p':
			assert(optarg != NULL);
			port = atoi(optarg);
			break;
		case 'd':
			dbname = optarg;
			break;
		case 'o':
			output = optarg;
			outputdir = NULL;
			break;
		case 'O':
			outputdir = optarg;
			output = NULL;
			break;
		case 'x':
			ext = optarg;
			break;
		case 'D':
			describe = true;
			break;
		case 'N':
			useinserts = true;
			break;
		case 'e':
			noescape = true;
			break;
		case 'f':
			if (table)
				usage(argv[0], -1);
			functions = true;
			break;
		case 't':
			if (table || functions)
				usage(argv[0], -1);
			table = optarg;
			break;
		case 'q':
			quiet = true;
			break;
		case 'X':
			trace = true;
			break;
		case 'v': {
			printf("msqldump, the MonetDB interactive database "
			       "dump tool, version %s", MONETDB_VERSION);
#ifdef MONETDB_RELEASE
			printf(" (%s)", MONETDB_RELEASE);
#else
			const char *rev = mercurial_revision();
			if (strcmp(rev, "Unknown") != 0)
				printf(" (hg id: %s)", rev);
#endif
			printf("\n");
			destroy_dotmonetdb(&dotfile);
			return 0;
		}
		case '?':
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			usage(argv[0], strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0 ? 0 : -1);
		default:
			usage(argv[0], -1);
		}
	}

	if ((output != NULL || useinserts) && outputdir) {
		usage(argv[0], -1);
	}

	if (optind == argc - 1)
		dbname = argv[optind];
	else if (optind != argc)
		usage(argv[0], -1);

	/* when config file would provide defaults */
	if (user_set_as_flag)
		passwd = NULL;

	if(dbname == NULL){
		printf("msqldump, please specify a database\n");
		usage(argv[0], -1);
	}
	char *user_allocated = NULL;
	if (user == NULL) {
		user_allocated = simple_prompt("user", BUFSIZ, 1, prompt_getlogin());
		user = user_allocated;
	}
	char *passwd_allocated = NULL;
	if (passwd == NULL) {
		passwd_allocated = simple_prompt("password", BUFSIZ, 0, NULL);
		passwd = passwd_allocated;
	}

	if (dbname != NULL && strchr(dbname, ':') != NULL) {
		mid = mapi_mapiuri(dbname, user, passwd, "sql");
	} else {
		mid = mapi_mapi(host, port, user, passwd, "sql", dbname);
	}
	free(user_allocated);
	user_allocated = NULL;
	free(passwd_allocated);
	passwd_allocated = NULL;
	user = NULL;
	passwd = NULL;
	dbname = NULL;
	if (mid == NULL) {
		fprintf(stderr, "failed to allocate Mapi structure\n");
		exit(2);
	}
	if (mapi_error(mid)) {
		mapi_explain(mid, stderr);
		exit(2);
	}
	mapi_set_time_zone(mid, 0);
	mapi_reconnect(mid);
	if (mapi_error(mid)) {
		mapi_explain(mid, stderr);
		exit(2);
	}
	if (!quiet) {
		const char *motd = mapi_get_motd(mid);

		if (motd)
			fprintf(stderr, "%s", motd);
	}
	mapi_trace(mid, trace);
	mapi_cache_limit(mid, -1);

	if (output) {
		out = open_wastream(output);
	} else if (outputdir) {
		size_t fnl = strlen(outputdir) + 10;
		if (ext)
			fnl += strlen(ext) + 1;
		char *fn = malloc(fnl);
		if (fn == NULL) {
			fprintf(stderr, "malloc failure\n");
			exit(2);
		}
		if (MT_mkdir(outputdir) == -1 && errno != EEXIST) {
			perror("cannot create output directory");
			exit(2);
		}
		snprintf(fn, fnl, "%s%cdump.sql", outputdir, DIR_SEP);
		out = open_wastream(fn);
		free(fn);
		(void) ext;
	} else {
		out = stdout_wastream();
	}
	if (out == NULL) {
		if (output)
			fprintf(stderr, "cannot open file: %s: %s\n",
					output, mnstr_peek_error(NULL));
		else if (outputdir)
			fprintf(stderr, "cannot open file: %s%cdump.sql: %s\n",
					outputdir, DIR_SEP, mnstr_peek_error(NULL));
		else
			fprintf(stderr, "failed to allocate stream: %s\n",
					mnstr_peek_error(NULL));
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
		strcpy_len(buf, ctime(&t), sizeof(buf));
#endif
#endif
		if ((p = strrchr(buf, '\n')) != NULL)
			*p = 0;

		mnstr_printf(out,
			     "-- msqldump version %s", MONETDB_VERSION);
#ifdef MONETDB_RELEASE
		mnstr_printf(out, " (%s)", MONETDB_RELEASE);
#else
		const char *rev = mercurial_revision();
		if (strcmp(rev, "Unknown") != 0)
			mnstr_printf(out, " (hg id: %s)", rev);
#endif
		mnstr_printf(out, " %s %s%s\n",
			     describe ? "describe" : "dump",
			     functions ? "functions" : table ? "table " : "database",
			     table ? table : "");
		dump_version(mid, out, "-- server:");
		mnstr_printf(out, "-- %s\n", buf);
	}
	if (functions) {
		mnstr_printf(out, "START TRANSACTION;\n");
		c = dump_functions(mid, out, true, NULL, NULL, NULL);
		mnstr_printf(out, "COMMIT;\n");
	} else if (table) {
		mnstr_printf(out, "START TRANSACTION;\n");
		c = dump_table(mid, NULL, table, out, outputdir, ext, describe, true, useinserts, false, noescape, true);
		mnstr_printf(out, "COMMIT;\n");
	} else
		c = dump_database(mid, out, outputdir, ext, describe, useinserts, noescape);
	mnstr_flush(out, MNSTR_FLUSH_DATA);

	mapi_destroy(mid);
	if (mnstr_errnr(out) != MNSTR_NO__ERROR) {
		fprintf(stderr, "%s: %s\n", argv[0], mnstr_peek_error(out));
		c = 1;
	}

	close_stream(out);

	destroy_dotmonetdb(&dotfile);

	return c;
}
