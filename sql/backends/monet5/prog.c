/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "monet_options.h"
#include "embeddedclient.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif

/* stolen piece */
#ifdef HAVE_FTIME
#include <sys/timeb.h>
#endif

#if TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# if HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif

#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif

#ifndef HAVE_GETOPT_LONG
#  include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif

static mapi_int64
gettime(void)
{
#ifdef HAVE_GETTIMEOFDAY
	struct timeval tp;

	gettimeofday(&tp, NULL);
	return (mapi_int64) tp.tv_sec * 1000000 + (mapi_int64) tp.tv_usec;
#else
#ifdef HAVE_FTIME
	struct timeb tb;

	ftime(&tb);
	return (mapi_int64) tb.time * 1000000 + (mapi_int64) tb.millitm * 1000;
#endif
#endif
}

static void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [ options ] [ script+ ]                   \n", prog);
	fprintf(stderr, "Options are:                                        \n");
	fprintf(stderr, " -c <config_file>    | --config=<config_file>       \n");
	fprintf(stderr, " -d<debug_level>     | --debug=<debug_level>        \n");
	fprintf(stderr, " -t                  | --time                       \n");
	fprintf(stderr, "                       --dbpath=<database_directory>\n");
	fprintf(stderr, " -s <option>=<value> | --set <option>=<value>       \n");
	fprintf(stderr, " -?                  | --help                       \n");
	exit(-1);
}


int
main(int argc, char **av)
{
	size_t curlen = 0, maxlen = BUFSIZ * 8;
	char *prog = *av;
	opt *set = NULL;
	int setlen = 0, timeflag = 0;
	mapi_int64 t0 = 0;
	Mapi mid;
	MapiHdl hdl;
	char *buf, *line;
	FILE *fp = NULL;

	static struct option long_options[] = {
		{"config", 1, 0, 'c'},
		{"dbpath", 1, 0, 0},
		{"debug", 2, 0, 'd'},
		{"time", 0, 0, 't'},
		{"set", 1, 0, 's'},
		{"help", 0, 0, '?'},
		{0, 0, 0, 0}
	};

	if (!(setlen = mo_builtin_settings(&set)))
		usage(prog);

	for (;;) {
		int option_index = 0;

		int c = getopt_long(argc, av, "c:d::?s:t",
				    long_options, &option_index);

		if (c == -1)
			break;

		switch (c) {
		case 0:
			if (strcmp(long_options[option_index].name, "dbpath") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbpath", optarg);
				break;
			}
			usage(prog);
			break;
		case 't':
			timeflag = 1;
			break;
		case 'c':
			setlen = mo_add_option(&set, setlen, opt_cmdline, "config", optarg);
			break;
		case 'd':
			if (optarg) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_debug", optarg);
			}
			break;
		case 's':{
			/* should add option to a list */
			char *tmp = strchr(optarg, '=');

			if (tmp) {
				*tmp = '\0';
				setlen = mo_add_option(&set, setlen, opt_cmdline, optarg, tmp + 1);
			} else {
				fprintf(stderr, "!ERROR: wrong format %s\n", optarg);
			}
		}
			break;
		case '?':
			usage(prog);
		default:
			fprintf(stderr, "!ERROR: getopt returned character code 0%o ??\n", c);
			usage(prog);
		}
	}

	setlen = mo_system_config(&set, setlen);

	mid = embedded_sql(set, setlen);

	/* now for each file given on the command line (or stdin) 
	   read the query and execute it
	 */
	buf = GDKmalloc(maxlen);
	if (buf == NULL) {
		fprintf(stderr, "Cannot allocate memory for query buffer\n");
		return -1;
	}
	if (optind == argc)
		fp = stdin;
	while (optind < argc || fp != NULL) {
		if (fp == NULL && (fp = fopen(av[optind], "r")) == NULL) {
			fprintf(stderr, "could no open file %s\n", av[optind]);
			break;
		}
		while ((line = fgets(buf + curlen, 1024, fp)) != NULL) {
			size_t n = strlen(line);

			curlen += n;
			if (curlen + 1024 > maxlen) {
				maxlen += 8 * BUFSIZ;
				buf = GDKrealloc(buf, maxlen + 1);
				if (buf == NULL) {
					fprintf(stderr, "Cannot allocate memory for query buffer\n");
					return -1;
				}
			}
		}
		if (fp != stdin) {
			fclose(fp);
		}
		fp = NULL;
		optind++;
		curlen = 0;
		if (timeflag)
			t0 = gettime();
		hdl = mapi_query(mid, buf);
		do {
			if (mapi_result_error(hdl) != NULL)
				mapi_explain_result(hdl, stderr);
			while ((line = mapi_fetch_line(hdl)) != NULL)
				printf("%s\n", line);
		} while (mapi_next_result(hdl) == 1);
		mapi_close_handle(hdl);
		if (timeflag)
			printf("Timer: "LLFMT" (usec)\n", gettime() - t0);
	}
	GDKfree(buf);
	mapi_destroy(mid);
	return 0;
}
