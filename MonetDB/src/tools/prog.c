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
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
 * All Rights Reserved.
 */

#include <monet_options.h>
#include "embeddedclient.h"

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

static long
gettime(void)
{
#ifdef HAVE_GETTIMEOFDAY
	struct timeval tp;

	gettimeofday(&tp, NULL);
	return (long) tp.tv_sec * 1000000 + (long) tp.tv_usec;
#else
#ifdef HAVE_FTIME
	struct timeb tb;

	ftime(&tb);
	return (long) tb.time * 1000000 + (long) tb.millitm * 1000;
#endif
#endif
}

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [ options ] [ script+ ]                   \n", prog);
	fprintf(stderr, "Options are:                                        \n");
	fprintf(stderr, " -c <config_file>    | --config=<config_file>       \n");
	fprintf(stderr, " -d<debug_level>     | --debug=<debug_level>        \n");
	fprintf(stderr, " -t                  | --time                       \n");
	fprintf(stderr, "                       --dbname=<database_name>     \n");
	fprintf(stderr, "                       --dbfarm=<database_directory>\n");
	fprintf(stderr, " -s <option>=<value> | --set <option>=<value>       \n");
	fprintf(stderr, " -?                  | --help                       \n");
	exit(-1);
}


int
main(int argc, char **av)
{
	int curlen = 0, maxlen = BUFSIZ*8;
	char *prog = *av;
	opt *set = NULL;
	int setlen = 0, time = 0, debug = 0;
	long t0 = 0;
	Mapi mid;
	MapiHdl hdl;
	char *buf, *line;

	static struct option long_options[] = {
		{"config", 1, 0, 'c'},
		{"dbname", 1, 0, 0},
		{"dbfarm", 1, 0, 0},
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
			if (strcmp(long_options[option_index].name, "dbname") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbname", optarg);
				break;
			}
			if (strcmp(long_options[option_index].name, "dbfarm") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbfarm", optarg);
				break;
			}
			usage(prog);
			break;
		case 't':
			time = 1;
			break;
		case 'c':
			setlen = mo_add_option(&set, setlen, opt_cmdline, "config", optarg);
			break;
		case 'd':
			debug = 1;
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

	mid = embedded_mil(set, setlen);

	buf = GDKmalloc(maxlen);
	while ((line = fgets(buf+curlen, 1024, stdin)) != NULL) {
		int n = strlen(line);
            	curlen += n;
            	if (curlen > (maxlen+1024)) {
               		maxlen += BUFSIZ;
               		buf = GDKrealloc(buf, maxlen + 1);
            	}
	}
	if (time)
		t0 = gettime();
	hdl = mapi_query(mid, buf);
	do {
		if (mapi_result_error(hdl) != NULL)
			mapi_explain_result(hdl, stderr);
		while ((line = mapi_fetch_line(hdl)) != NULL)
			printf("%s\n", line);
	} while (mapi_next_result(hdl) == 1);
	mapi_close_handle(hdl);
	if (time)
		printf("Timer: %ld (usec)\n", gettime()-t0);
	GDKfree(buf);
	return 0;
}
