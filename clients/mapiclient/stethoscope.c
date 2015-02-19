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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

/* (c) M Kersten, S Manegold
 * The easiest calling method is something like:
 * tomograph -d demo --atlast=10
 * which connects to the demo database server and
 * will collect the tomograph pages for at most 10 SQL queries
 * For each page a gnuplot file, a data file, and the event trace
 * are collected for more focussed analysis.
 * 
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

#define die(dbh, hdl)						\
	do {							\
		(hdl ? mapi_explain_query(hdl, stderr) :	\
		 dbh ? mapi_explain(dbh, stderr) :		\
		 fprintf(stderr, "!! command failed\n"));	\
		goto stop_disconnect;				\
	} while (0)

#define doQ(X)								\
	do {								\
		if ((hdl = mapi_query(dbh, X)) == NULL ||	\
		    mapi_error(dbh) != MOK)			\
			die(dbh, hdl);			\
	} while (0)

static stream *conn = NULL;
static char hostname[128];
static char *basefilename = "stethoscope";
static int debug = 0;
static int beat = 50;
static Mapi dbh;
static MapiHdl hdl = NULL;

/*
 * Parsing the argument list of a MAL call to obtain un-quoted string values
 */

static void
usageStethoscope(void)
{
    fprintf(stderr, "stethoscope [options] \n");
    fprintf(stderr, "  -d | --dbname=<database_name>\n");
    fprintf(stderr, "  -u | --user=<user>\n");
    fprintf(stderr, "  -p | --port=<portnr>\n");
    fprintf(stderr, "  -h | --host=<hostname>\n");
    fprintf(stderr, "  -o | --output=<file>\n");
	fprintf(stderr, "  -b | --beat=<delay> in milliseconds (default 50)\n");
    fprintf(stderr, "  -? | --help\n");
	exit(-1);
}

/* Any signal should be captured and turned into a graceful
 * termination of the profiling session. */

static void
stopListening(int i)
{
	fprintf(stderr,"signal %d received\n",i);
	if( dbh)
		doQ("profiler.stop();");
stop_disconnect:
	// show follow up action only once
	if(dbh)
		mapi_disconnect(dbh);
	exit(0);
}

int
main(int argc, char **argv)
{
	ssize_t  n;
	size_t len;
	char *host = NULL;
	int portnr = 0;
	char *dbname = NULL;
	char *uri = NULL;
	char *user = NULL;
	char *password = NULL;
	char buf[BUFSIZ], *e, *response;
	int line = 0;
	FILE *trace = NULL;

	static struct option long_options[15] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "port", 1, 0, 'p' },
		{ "password", 1, 0, 'P' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "output", 1, 0, 'o' },
		{ "debug", 0, 0, 'D' },
		{ "beat", 1, 0, 'b' },
		{ 0, 0, 0, 0 }
	};

	/* parse config file first, command line options override */
	parse_dotmonetdb(&user, &password, NULL, NULL, NULL, NULL);

	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:p:P:h:?:o:D:b",
					long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'D':
			debug = 1;
			break;
		case 'b':
			beat = atoi(optarg ? optarg : "100");
			break;
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
		case 'P':
			if (password)
				free(password);
			password = strdup(optarg);
			break;
		case 'p':
			if (optarg)
				portnr = atoi(optarg);
			break;
		case 'h':
			host = optarg;
			break;
		case 'o':
			basefilename = strdup(optarg);
			if( strstr(basefilename,".trace"))
				*strstr(basefilename,".trace") = 0;
			printf("-- Output directed towards %s\n", basefilename);
			break;
		case '?':
			usageStethoscope();
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			exit(strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0 ? 0 : -1);
		default:
				usageStethoscope();
			exit(-1);
		}
	}

	if(debug)
		printf("stethoscope -d %s -o %s\n",dbname,basefilename);

	if (dbname != NULL && strncmp(dbname, "mapi:monetdb://", 15) == 0) {
		uri = dbname;
		dbname = NULL;
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
	close(0);

	if (user == NULL)
		user = simple_prompt("user", BUFSIZ, 1, prompt_getlogin());
	if (password == NULL)
		password = simple_prompt("password", BUFSIZ, 0, NULL);

	/* our hostname, how remote servers have to contact us */
	gethostname(hostname, sizeof(hostname));

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
	if(debug)
		fprintf(stderr,"-- connection with server %s\n", uri ? uri : host);

	for (portnr = 50010; portnr < 62010; portnr++) 
		if ((conn = udp_rastream(hostname, portnr, "profileStream")) != NULL)
			break;
	
	if ( conn == NULL) {
		fprintf(stderr, "!! opening stream failed: no free ports available\n");
		fflush(stderr);
		goto stop_disconnect;
	}

	printf("-- opened UDP profile stream %s:%d for %s\n", hostname, portnr, host);

	snprintf(buf, BUFSIZ, " port := profiler.openStream(\"%s\", %d);", hostname, portnr);
	if( debug)
		fprintf(stderr,"--%s\n",buf);
	doQ(buf);

	snprintf(buf,BUFSIZ-1,"profiler.stethoscope(%d);",beat);
	if( debug)
		fprintf(stderr,"-- %s\n",buf);
	doQ(buf);

	snprintf(buf,BUFSIZ,"%s.trace",basefilename);
	trace = fopen(buf,"w");
	if( trace == NULL)
		fprintf(stderr,"Could not create trace file\n");

	len = 0;
	while ((n = mnstr_read(conn, buf + len, 1, BUFSIZ - len)) > 0) {
		buf[len + n] = 0;
		if( trace) 
			fprintf(trace,"%s",buf);
		response = buf;
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			printf("%s\n", response);
			if (++line % 200) {
				line = 0;
			}
			response = e + 1;
		}
		/* handle last line in buffer */
		if (*response) {
			if (debug)
				printf("LASTLINE:%s", response);
			len = strlen(response);
			strncpy(buf, response, len + 1);
		} else
			len = 0;
	}

	doQ("profiler.stop();");
stop_disconnect:
	if(dbh)
		mapi_disconnect(dbh);
	printf("-- connection with server %s closed\n", uri ? uri : host);
	return 0;
}
