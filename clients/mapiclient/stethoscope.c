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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

/**
 * stethoscope
 * author Martin Kersten
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

#define COUNTERSDEFAULT "ISTest"

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
	/*  1  */ { 'm', "memory", "arena", 0 },
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
	fprintf(stderr, "The trace options (default '%s'):\n",COUNTERSDEFAULT);
	fprintf(stderr, "  S = monitor start of instruction profiling\n");
	fprintf(stderr, "  a = aggregate clock ticks per instruction\n");
	fprintf(stderr, "  e = event counter\n");
	fprintf(stderr, "  f = module.function name\n");
	fprintf(stderr, "  i = instruction counter\n");
	fprintf(stderr, "  I = interpreter thread number\n");
	fprintf(stderr, "  T = wall clock time\n");
	fprintf(stderr, "  t = ticks in microseconds\n");
	fprintf(stderr, "  c = cpu statistics (utime,ctime,stime,cstime)\n");
	fprintf(stderr, "  m = memory resources as provided by OS\n");
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
	wthread *walk;
	(void)i;
	/* kill all connections */
	for (walk = thds; walk != NULL; walk = walk->next) {
		if (walk->s != NULL)
			mnstr_close(walk->s);
	}
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
	ssize_t n;
	char *response, *x;
	char buf[BUFSIZ + 1];
	char *e;
	char *mod, *fcn;
	char *host;
	int portnr;
	char id[10];
	Mapi dbh;
	MapiHdl hdl = NULL;

	/* set up the profiler */
	id[0] = '\0';
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
	while ((n = mnstr_read(wthr->s, buf, 1, BUFSIZ)) > 0) {
		buf[n] = 0;
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
	int i, k;
	char *host = NULL;
	int portnr = 50000;
	char *dbname = NULL;
	char *user = NULL;
	char *password = NULL;

	/* some .monetdb properties are used by mclient, perhaps we need them as well later */
	struct stat statb;

	char **alts, **oalts;
	wthread *walk;
	stream * config = NULL;

	static struct option long_options[8] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "password", 1, 0, 'P' },
		{ "port", 1, 0, 'p' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ 0, 0, 0, 0 }
	};

	/* parse config file first, command line options override */
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
				fprintf(stderr,
					      "failed to open file '%s': %s\n",
					      cfile, strerror(errno));
			}
		}
	}

	if (config != NULL) {
		char buf[1024];
		char *q;
		ssize_t len;
		int line = 0;
		while ((len = mnstr_readline(config, buf, sizeof(buf) - 1)) > 0) {
			line++;
			buf[len - 1] = '\0';	/* drop newline */
			if (buf[0] == '#' || buf[0] == '\0')
				continue;
			if ((q = strchr(buf, '=')) == NULL) {
				fprintf(stderr, "%s:%d: syntax error: %s\n", mnstr_name(config), line, buf);
				continue;
			}
			*q++ = '\0';
			/* this basically sucks big time, as I can't easily set
			 * a default, hence I only do things I think are useful
			 * for now, needs a better solution */
			if (strcmp(buf, "user") == 0) {
				user = strdup(q);	/* leak */
			} else if (strcmp(buf, "password") == 0 || strcmp(buf, "passwd") == 0) {
				password = strdup(q);	/* leak */
			}
		}
		mnstr_destroy(config);
	}

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
			host = optarg;
			break;
		case '?':
		default:
			usage();
			exit(0);
		}
	}

	a = optind;
	if (argc > 1 && a < argc && argv[a][0] == '+') {
		k= setCounter(argv[a] + 1);
		a++;
	} else
		k= setCounter(COUNTERSDEFAULT);

	/* DOT needs function id and PC to correlate */
	if( profileCounter[32].status ) {
		profileCounter[3].status= k++;
		profileCounter[4].status= k;
	}

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
	if (dbname != NULL && host == NULL) {
		oalts = alts = mapi_resolve(host, portnr, dbname);
	} else 
		alts = NULL;

	if (alts == NULL || *alts == NULL) {
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
		WaitForSingleObject(walk->id, INFINITE);
		CloseHandle(walk->id);
#else
		pthread_create(&walk->id, NULL, &doProfile, walk);
		pthread_join(walk->id, NULL);
#endif
		free(walk);
	} else {
		/* fork runner threads for all alternatives */
		i = 1;
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
			printf("Alternative route created '%s'\n",walk->uri);
		}
		walk->next = NULL;
		free(oalts);
		for (walk = thds; walk != NULL; walk = walk->next) {
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
			WaitForSingleObject(walk->id, INFINITE);
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
