/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "monet_options.h"
#include <mapi.h>
#include <stream.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include "mprompt.h"
#include "dotmonetdb.h"

#ifndef HAVE_GETOPT_LONG
# include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif

#define COUNTERSDEFAULT "ISTest"

/* #define _DEBUG_STETHOSCOPE_*/

static struct {
	char tag;
	char *ptag;     /* which profiler group counter is needed */
	char *name;     /* which logical counter is needed */
	int status;     /* trace it or not */
} profileCounter[] = {
	/*  0  */ { 'a', "aggregate", "total count", 0 },
	/*  1  */ { 'a', "aggregate", "total ticks", 0 },
	/*  2  */ { 'e', "event", "event id", 0 },
	/*  3  */ { 'f', "function", "function", 0 },
	/*  4  */ { 'i', "pc", "pc", 0 },
	/*  5  */ { 'T', "time", "time stamp", 0 },
	/*  6  */ { 't', "ticks", "usec ticks", 1 },
	/*  7  */ { 'c', "cpu", "utime", 0 },
	/*  8  */ { 'c', "cpu", "cutime", 0 },
	/*  9  */ { 'c', "cpu", "stime", 0 },
	/*  0  */ { 'c', "cpu", "cstime", 0 },
	/*  1  */ { 'm', "memory", "arena", 0 },/* memory details are ignored*/
	/*  2  */ { 'm', "memory", "ordblks", 0 },
	/*  3  */ { 'm', "memory", "smblks", 0 },
	/*  4  */ { 'm', "memory", "hblkhd", 0 },
	/*  5  */ { 'm', "memory", "hblks", 0 },
	/*  6  */ { 'm', "memory", "fsmblks", 0 },
	/*  7  */ { 'm', "memory", "uordblks", 0 },
	/*  8  */ { 'r', "reads", "blk reads", 0 },
	/*  9  */ { 'w', "writes", "blk writes", 0 },
	/*  0  */ { 'b', "rbytes", "rbytes", 0 },
	/*  1  */ { 'b', "wbytes", "wbytes", 0 },
	/*  2  */ { 's', "stmt", "stmt", 2 },
	/*  3  */ { 'p', "process", "pg reclaim", 0 },
	/*  4  */ { 'p', "process", "pg faults", 0 },
	/*  5  */ { 'p', "process", "swaps", 0 },
	/*  6  */ { 'p', "process", "ctxt switch", 0 },
	/*  7  */ { 'p', "process", "inv switch", 0 },
	/*  8  */ { 'I', "thread", "thread", 0 },
	/*  9  */ { 'u', "user", "user", 0 },
	/*  0  */ { 'S', "start", "start", 0 },
	/*  1  */ { 'y', "type", "type", 0 },
	/*  2  */ { 'D', "dot", "dot", 0 },
	/*  3  */ { 'F', "flow", "flow", 0 },
	/*  4  */ { 'M', "footprint", "footprint", 0 },
	/*  5  */ { 0, 0, 0, 0 }
};

static stream *conn = NULL;
static char hostname[128];

static void
usage(void)
{
	fprintf(stderr, "stethoscope [options] [dbname] +[trace options] {<mod>.<fcn>}\n");
	fprintf(stderr, "  -d | --dbname=<database_name>\n");
	fprintf(stderr, "  -u | --user=<user>\n");
	fprintf(stderr, "  -p | --port=<portnr>\n");
	fprintf(stderr, "  -h | --host=<hostname>\n");
	fprintf(stderr, "  -? | --help\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "The trace options (default '%s'):\n",COUNTERSDEFAULT);
	fprintf(stderr, "  S = monitor start of instruction profiling\n");
	fprintf(stderr, "  a = aggregate clock ticks per instruction\n");
	fprintf(stderr, "  e = event counter\n");
	fprintf(stderr, "  f = enclosing module.function name\n");
	fprintf(stderr, "  i = instruction counter\n");
	fprintf(stderr, "  I = interpreter thread number\n");
	fprintf(stderr, "  T = wall clock time\n");
	fprintf(stderr, "  t = ticks in microseconds\n");
	fprintf(stderr, "  c = cpu statistics (utime,ctime,stime,cstime)\n");
	fprintf(stderr, "  m = rss memory as provided by OS (in MB)\n");
	fprintf(stderr, "  M = memory footprint of non-persistent objects\n");
	fprintf(stderr, "  r = block reads\n");
	fprintf(stderr, "  w = block writes\n");
	fprintf(stderr, "  b = bytes read/written\n");
	fprintf(stderr, "  s = MAL statement\n");
	fprintf(stderr, "  y = MAL argument types\n");
	fprintf(stderr, "  p = process statistics, e.g. page faults, context switches\n");
	fprintf(stderr, "  u = user id\n");
	fprintf(stderr, "  D = Generate dot file upon query start\n");
	fprintf(stderr, "  F = dataflow memory claims (in MB)\n");
}

/* Any signal should be captured and turned into a graceful
 * termination of the profiling session. */
static void
stopListening(int i)
{
	(void)i;
	if (conn != NULL)
		mnstr_close(conn);
}

static int
setCounter(char *nme)
{
	int i, k = 1;

	for (i = 0; profileCounter[i].tag; i++)
		profileCounter[i].status = 0;

	for (; *nme; nme++)
		for (i = 0; profileCounter[i].tag; i++)
			if (profileCounter[i].tag == *nme)
				profileCounter[i].status = k++;
	return k;
}

#define die(dbh, hdl) while (1) {(hdl ? mapi_explain_query(hdl, stderr) :  \
					   dbh ? mapi_explain(dbh, stderr) :        \
					   fprintf(stderr, "!! command failed\n")); \
					   goto stop_disconnect;}
#define doQ(X) \
	if ((hdl = mapi_query(dbh, X)) == NULL || mapi_error(dbh) != MOK) \
			 die(dbh, hdl);

int
main(int argc, char **argv)
{
	int i, k;
	ssize_t n;
	char *e;
	char *host = NULL;
	int portnr = 0;
	char *dbname = NULL;
	char *uri = NULL;
	char *user = NULL;
	char *password = NULL;
	struct stat statb;
	char *response, *x;
	char buf[BUFSIZ + 1];
	char *mod, *fcn;
	Mapi dbh;
	MapiHdl hdl = NULL;

	static struct option long_options[6] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "port", 1, 0, 'p' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ 0, 0, 0, 0 }
	};

	/* parse config file first, command line options override */
	parse_dotmonetdb(&user, &password, NULL, NULL, NULL, NULL);

	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:p:h:?",
			long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'd':
			dbname = optarg;
			break;
		case 'u':
			if (user)
				free(user);
			user = strdup(optarg);
			/* force password prompt */
			if (password)
				free(password);
			password = NULL;
			break;
		case 'p':
			portnr = atol(optarg);
			break;
		case 'h':
			host = optarg;
			break;
		case '?':
			usage();
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			exit(strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0 ? 0 : -1);
		default:
			usage();
			exit(-1);
		}
	}

	if (dbname == NULL && optind != argc && argv[optind][0] != '+' &&
			(stat(argv[optind], &statb) != 0 || !S_ISREG(statb.st_mode)))
	{
		dbname = argv[optind];
		optind++;
	}

	if (dbname != NULL && strncmp(dbname, "mapi:monetdb://", 15) == 0) {
		uri = dbname;
		dbname = NULL;
	}

	i = optind;
	if (argc > 1 && i < argc && argv[i][0] == '+') {
		k = setCounter(argv[i] + 1);
		i++;
	} else
		k = setCounter(COUNTERSDEFAULT);

	/* DOT needs function id and PC to correlate */
	if (profileCounter[32].status) {
		profileCounter[3].status = k++;
		profileCounter[4].status = k;
	}

	if (user == NULL)
		user = simple_prompt("user", BUFSIZ, 1, prompt_getlogin());
	if (password == NULL)
		password = simple_prompt("password", BUFSIZ, 0, NULL);

	close(0); /* get rid of stdin */

	/* our hostname, how remote servers have to contact us */
	gethostname(hostname, sizeof(hostname));

	/* forget about the options we've parsed */
	argc = argc - i;
	argv = &argv[i];

#ifdef SIGPIPE
	signal(SIGPIPE, stopListening);
#endif
#ifdef SIGHUP
	signal(SIGHUP, stopListening);
#endif
#ifdef SIGQUIT
	signal(SIGQUIT, stopListening);
#endif
	signal(SIGINT, stopListening);
	signal(SIGTERM, stopListening);

	/* set up the profiler */
	if (uri)
		dbh = mapi_mapiuri(uri, user, password, "mal");
	else
		dbh = mapi_mapi(host, portnr, user, password, "mal", dbname);
	if (dbh == NULL || mapi_error(dbh))
		die(dbh, hdl);
	mapi_reconnect(dbh);
	if (mapi_error(dbh))
		die(dbh, hdl);
	host = strdup(mapi_get_host(dbh));
#ifdef _DEBUG_STETHOSCOPE_
	printf("-- connection with server %s\n", uri ? uri : host);
#endif

	/* set counters */
	x = NULL;
	for (i = 0; profileCounter[i].tag; i++) {
		/* skip duplicates */
		if (x == profileCounter[i].ptag)
			continue;
		/* deactivate any left over counter first */
		snprintf(buf, BUFSIZ, "profiler.deactivate(\"%s\");",
				profileCounter[i].ptag);
		doQ(buf);
		if (profileCounter[i].status) {
			snprintf(buf, BUFSIZ, "profiler.activate(\"%s\");",
					profileCounter[i].ptag);
			doQ(buf);
#ifdef _DEBUG_STETHOSCOPE_
			printf("-- %s\n", buf);
#endif
		}
		x = profileCounter[i].ptag;
	}

	for (portnr = 50010; portnr < 62010; portnr++) {
		if ((conn = udp_rastream(hostname, portnr, "profileStream")) != NULL)
			break;
	}
	if (conn == NULL) {
		fprintf(stderr, "!! opening stream failed: no free ports available\n");
		goto stop_cleanup;
	}

	printf("-- opened UDP profile stream %s:%d for %s\n",
			hostname, portnr, host);

	snprintf(buf, BUFSIZ, "port := profiler.openStream(\"%s\", %d);",
			hostname, portnr);
	doQ(buf);

	/* Set Filters */
	doQ("profiler.setNone();");

	if (argc == 0) {
#ifdef _DEBUG_STETHOSCOPE_
		printf("-- profiler.setAll();\n");
#endif
		doQ("profiler.setAll();");
	} else {
		for (i = 0; i < argc; i++) {
			char *c;
			char *arg = strdup(argv[i]);
			c = strchr(arg, '.');
			if (c) {
				mod = arg;
				if (mod == c)
					mod = "*";
				fcn = c + 1;
				if (*fcn == 0)
					fcn = "*";
				*c = 0;
			} else {
				fcn = arg;
				mod = "*";
			}
			snprintf(buf, BUFSIZ, "profiler.setFilter(\"%s\",\"%s\");", mod, fcn);
#ifdef _DEBUG_STETHOSCOPE_
			printf("-- %s\n", buf);
#endif
			doQ(buf);
			free(arg);
		}
	}
#ifdef _DEBUG_STETHOSCOPE_
	printf("-- profiler.start();\n");
#endif
	doQ("profiler.start();");
	fflush(NULL);

	i = 0;
	while ((n = mnstr_read(conn, buf, 1, BUFSIZ)) > 0) {
		buf[n] = 0;
		response = buf;
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			printf("%s\n", response);
			response = e + 1;
		}
		/* handle last line in buffer */
		if (*response)
			printf("%s\n", response);
		if (++i % 200) {
			i = 0;
			fflush(NULL);
		}
	}
	fflush(NULL);

stop_cleanup:
	doQ("profiler.setNone();");
	doQ("profiler.stop();");
	doQ("profiler.closeStream();");
stop_disconnect:
	if (dbh) {
		mapi_disconnect(dbh);
		mapi_destroy(dbh);
	}

	if (host != NULL) {
		printf("-- connection with server %s closed\n", uri ? uri : host);

		free(host);
	}
	free(user);
	free(password);
	return 0;
}
