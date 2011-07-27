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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/**
 * stethoscope
 * Martin Kersten
 *
 * The Stethoscope
 * The performance profiler infrastructure provides
 * precisely control through annotation of a MAL program. 
 * Often, however, inclusion of profiling statements is an afterthought.
 *
 * The program stethoscope addresses this situation by providing
 * a simple application that can attach itself to a running
 * server and extracts the profiler events from concurrent running queries.
 *
 * The arguments to @code{stethoscope} are the profiler properties to be traced
 * and the applicable filter expressions. For example,
 *   stethoscope -t bat.insert algebra.join
 * tracks the microsecond ticks of two specific MAL instructions.
 * A synopsis of the calling conventions:
 *   stethoscope [options] +[aefoTtcmibds] @{<mod>.<fcn> @}
 *     -d | --dbname=<database_name>
 *     -u | --user=<user>
 *     -P | --password=<password>
 *     -p | --port=<portnr>
 *     -h | --host=<hostname>
 * 
 * Event selector:
 *     a =aggregates
 *     e =event
 *     f =function 
 *     o =operation called
 *     i =interpreter theread
 *     T =time
 *     t =ticks
 *     c =cpu statistics
 *     m =memory resources
 *     r =block reads
 *     w =block writes
 *     b =bytes read/written
 *     s =statement
 *     y =argument types
 *     p =pgfaults,cntxtswitches
 *     S =Start profiling instruction
 * 
 * Ideally, the stream of events should be piped into a
 * 2D graphical tool, like xosview (Linux).
 * 
 * For a convenient way to watch most of the SQL interaction you may use
 * the command:
 * stethoscope -umonetdb -Pmonetdb -hhost +tis "algebra.*" "bat.*" "group.*" "sql.*" "aggr.*"
 */

#include "monetdb_config.h"
#include "monet_options.h"
#include <mapi.h>
#include <stream.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#ifdef HAVE_PTHREAD_H
#include <pthread.h>
#endif

#ifndef HAVE_GETOPT_LONG
# include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif


static struct {
	char tag;
	char *ptag;     /* which profiler group counter is needed */
	char *name;     /* which logical counter is needed */
	int status;     /* trace it or not */
} profileCounter[] = {
	/*  0  */ { 'a', "aggregate", "total count", 0 },
	/*  1  */ { 'a', "aggregate", "total ticks", 0 },
	/*  2  */ { 'e', "event", "event id", 0 },
	/*  3  */ { 'f', "pc", "function", 0 },
	/*  4  */ { 'f', "pc", "pc", 0 },
	/*  5  */ { 'o', "operation", "operation", 0 },
	/*  6  */ { 'T', "time", "time stamp", 0 },
	/*  7  */ { 't', "ticks", "usec ticks", 1 },
	/*  8  */ { 'c', "cpu", "utime", 0 },
	/*  9  */ { 'c', "cpu", "cutime", 0 },
	/*  0  */ { 'c', "cpu", "stime", 0 },
	/*  1  */ { 'c', "cpu", "cstime", 0 },
	/*  2  */ { 'm', "memory", "arena", 0 },
	/*  3  */ { 'm', "memory", "ordblks", 0 },
	/*  4  */ { 'm', "memory", "smblks", 0 },
	/*  5  */ { 'm', "memory", "hblkhd", 0 },
	/*  6  */ { 'm', "memory", "hblks", 0 },
	/*  7  */ { 'm', "memory", "fsmblks", 0 },
	/*  8  */ { 'm', "memory", "uordblks", 0 },
	/*  9  */ { 'r', "reads", "blk reads", 0 },
	/*  0  */ { 'w', "writes", "blk writes", 0 },
	/*  1  */ { 'b', "rbytes", "rbytes", 0 },
	/*  2  */ { 'b', "wbytes", "wbytes", 0 },
	/*  3  */ { 's', "stmt", "stmt", 2 },
	/*  4  */ { 'p', "process", "pg reclaim", 0 },
	/*  5  */ { 'p', "process", "pg faults", 0 },
	/*  6  */ { 'p', "process", "swaps", 0 },
	/*  7  */ { 'p', "process", "ctxt switch", 0 },
	/*  8  */ { 'p', "process", "inv switch", 0 },
	/*  9  */ { 'i', "thread", "thread", 0 },
	/*  0  */ { 'u', "user", "user", 0 },
	/*  1  */ { 'S', "start", "start", 0 },
	/*  2  */ { 'y', "type", "type", 0 },
	/*  3  */ { 0, 0, 0, 0 }
};

typedef struct _wthread {
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
	HANDLE id;
#else
	pthread_t id;
#endif
	int tid;
	char *uri;
	char *user;
	char *pass;
	stream *s;
	size_t argc;
	char **argv;
	struct _wthread *next;
} wthread;

static wthread *thds = NULL;

static void
usage(void)
{
	fprintf(stderr, "stethoscope [options] +[trace options] {<mod>.<fcn>}\n");
	fprintf(stderr, "  -d | --dbname=<database_name>\n");
	fprintf(stderr, "  -u | --user=<user>\n");
	fprintf(stderr, "  -P | --password=<password>\n");
	fprintf(stderr, "  -p | --port=<portnr>\n");
	fprintf(stderr, "  -h | --host=<hostname>\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "The trace options:\n");
	fprintf(stderr, "  S = start instruction profiling\n");
	fprintf(stderr, "  a = aggregates\n");
	fprintf(stderr, "  e = event\n");
	fprintf(stderr, "  f = function \n");
	fprintf(stderr, "  o = operation called\n");
	fprintf(stderr, "  i = interpreter thread\n");
	fprintf(stderr, "  T = time\n");
	fprintf(stderr, "  t = ticks\n");
	fprintf(stderr, "  c = cpu statistics\n");
	fprintf(stderr, "  m = memory resources\n");
	fprintf(stderr, "  r = block reads\n");
	fprintf(stderr, "  w = block writes\n");
	fprintf(stderr, "  b = bytes read/written\n");
	fprintf(stderr, "  s = statement\n");
	fprintf(stderr, "  y = argument types\n");
	fprintf(stderr, "  p = page faults, context switches\n");
	fprintf(stderr, "  u = user\n");
}

/* Any signal should be captured and turned into a graceful
 * termination of the profiling session. */
static void
stopListening(int i)
{
	wthread *walk;
	(void)i;
	/* kill all connections */
	for (walk = thds; walk != NULL; walk = walk->next) {
		if (walk->s != NULL)
			mnstr_close(walk->s);
	}
}

static void
setCounter(char *nme)
{
	int i, k = 1;

	for (i = 0; profileCounter[i].tag; i++)
		profileCounter[i].status = 0;

	for (; *nme; nme++)
		for (i = 0; profileCounter[i].tag; i++)
			if (profileCounter[i].tag == *nme)
				profileCounter[i].status = k++;
}

#define die(dbh, hdl) while (1) {(hdl ? mapi_explain_query(hdl, stderr) :  \
					   dbh ? mapi_explain(dbh, stderr) :        \
					   fprintf(stderr, "!! %scommand failed\n", id)); \
					   goto stop_disconnect;}
#define doQ(X) \
	if ((hdl = mapi_query(dbh, X)) == NULL || mapi_error(dbh) != MOK) \
			 die(dbh, hdl);

#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
static DWORD WINAPI
#else
static void *
#endif
doProfile(void *d)
{
	wthread *wthr = (wthread*)d;
	int i;
	size_t a;
	char *response, *x;
	char buf[BUFSIZ];
	char *e;
	char *mod, *fcn;
	char *host;
	int portnr;
	char id[10];
	Mapi dbh;
	MapiHdl hdl = NULL;

	/* set up the profiler */
	dbh = mapi_mapiuri(wthr->uri, wthr->user, wthr->pass, "mal");
	if (dbh == NULL || mapi_error(dbh))
		die(dbh, hdl);
	mapi_reconnect(dbh);
	if (mapi_error(dbh))
		die(dbh, hdl);
	if (wthr->tid > 0) {
		snprintf(id, 10, "[%d] ", wthr->tid);
		printf("-- connection with server %s is %s\n", wthr->uri, id);
	} else {
		id[0] = '\0';
		printf("-- connection with server %s\n", wthr->uri);
	}

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
			printf("-- %s%s\n", id, buf);
		}
		x = profileCounter[i].ptag;
	}

	host = mapi_get_host(dbh);
	for (portnr = 50010; portnr < 62010; portnr++) {
		if ((wthr->s = udp_rastream(host, portnr, "profileStream")) != NULL)
			break;
	}
	if (wthr->s == NULL) {
		fprintf(stderr, "!! %sopening stream failed: no free ports available\n",
				id);
		goto stop_cleanup;
	}

	printf("-- %sopened UDP profile stream for %s:%d\n", id, host, portnr);

	snprintf(buf, BUFSIZ, "port := profiler.openStream(\"%s\", %d);",
			host, portnr);
	doQ(buf);

	/* Set Filters */
	doQ("profiler.setNone();");

	if (wthr->argc == 0) {
		printf("-- %sprofiler.setAll();\n", id);
		doQ("profiler.setAll();");
	} else {
		for (a = 0; a < wthr->argc; a++) {
			char *c;
			char *arg = strdup(wthr->argv[a]);
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
			printf("-- %s%s\n", id, buf);
			doQ(buf);
			free(arg);
		}
	}
	printf("-- %sprofiler.start();\n", id);
	doQ("profiler.start();");
	fflush(NULL);

	i = 0;
	while (mnstr_read(wthr->s, buf, 1, BUFSIZ)) {
		response = buf;
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			printf("%s%s\n", id, response);
			response = e + 1;
		}
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
	mapi_disconnect(dbh);
	mapi_destroy(dbh);

	printf("-- %sconnection with server %s closed\n", id, wthr->uri);

	return(0);
}

int
main(int argc, char **argv)
{
	int a = 1;
	int i;
	char *host = "localhost";
	int portnr = 50000;
	char *dbname = NULL;
	char *user = NULL;
	char *password = NULL;
	char **alts, **oalts;
	wthread *walk;

	static struct option long_options[8] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "password", 1, 0, 'P' },
		{ "port", 1, 0, 'p' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ 0, 0, 0, 0 }
	};
	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:P:p:?:h:g",
			long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'd':
			dbname = optarg;
			break;
		case 'u':
			user = optarg;
			break;
		case 'P':
			password = optarg;
			break;
		case 'p':
			portnr = atol(optarg);
			break;
		case 'h':
			if (strcmp(long_options[option_index].name, "help") == 0) {
				usage();
				break;
			}
			if (strcmp(long_options[option_index].name, "host") == 0) {
				host = optarg;
				break;
			}
		default:
			usage();
			exit(0);
		}
	}

	a = optind;
	if (argc > 1 && a < argc && argv[a][0] == '+') {
		setCounter(argv[a] + 1);
		a++;
	} else
		setCounter("TtiesS");

	if (user == NULL || password == NULL) {
		fprintf(stderr, "%s: need -u and -P arguments\n", argv[0]);
		usage();
		exit(-1);
	}

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

	close(0); /* get rid of stdin */

	/* try and find multiple options, we assume that we always need a
	 * local merovingian for that, in the future we probably need to fix
	 * this in a decent manner */
	if (dbname != NULL) {
		oalts = alts = mapi_resolve(host, portnr, dbname);
	} else {
		alts = NULL;
		dbname = "";
	}

	if (alts == NULL) {
		/* nothing to redirect, so a single host to try */
		char uri[512];
		snprintf(uri, 512, "mapi:monetdb://%s:%d/%s", host, portnr, dbname);
		walk = thds = malloc(sizeof(wthread));
		walk->uri = uri;
		walk->user = user;
		walk->pass = password;
		walk->argc = argc - a;
		walk->argv = &argv[a];
		walk->tid = 0;
		walk->s = NULL;
		walk->next = NULL;
		/* In principle we could do this without a thread, but it seems
		 * that if we do it that way, ctrl-c (or any other signal)
		 * doesn't interrupt the read inside this function, and hence
		 * the function never terminates... at least on Linux */
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
		walk->id = CreateThread(NULL, 0, doProfile, walk, 0, NULL);
		CloseHandle(walk->id);
#else
		pthread_create(&walk->id, NULL, &doProfile, walk);
		pthread_join(walk->id, NULL);
#endif
		free(walk);
	} else {
		/* fork runner threads for all alternatives */
		i = 1;
		if (*alts != NULL) {
			walk = thds = malloc(sizeof(wthread));
			while (1) {
				walk->tid = i++;
				walk->uri = *alts;
				walk->user = user;
				walk->pass = password;
				walk->argc = argc - a;
				walk->argv = &argv[a];
				walk->s = NULL;
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
				walk->id = CreateThread(NULL, 0, doProfile, walk, 0, NULL);
#else
				pthread_create(&walk->id, NULL, &doProfile, walk);
#endif
				alts++;
				if (*alts == NULL)
					break;
				walk = walk->next = malloc(sizeof(wthread));
			}
			walk->next = NULL;
		} else {
			fprintf(stderr, "%s: no databases found for '%s'\n",
					argv[0], dbname);
		}
		free(oalts);
		for (walk = thds; walk != NULL; walk = walk->next) {
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
			CloseHandle(walk->id);
#else
			pthread_join(walk->id, NULL);
#endif
			free(walk->uri);
			free(walk);
		}
	}
	return 0;
}
