/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "stream.h"
#include "stream_socket.h"
#include "mapi.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <signal.h>
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#include "mprompt.h"
#include "dotmonetdb.h"
#include "eventparser.h"

#ifndef HAVE_GETOPT_LONG
# include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif

#ifdef HAVE_NETDB_H
# include <netdb.h>
# include <netinet/in.h>
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET (-1)
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
static char *filename = NULL;
static int beat = 0;
static int json = 0;
static Mapi dbh;
static MapiHdl hdl = NULL;
static FILE *trace = NULL;

/*
 * Tuple level reformatting
 */

static void
renderEvent(EventRecord *ev){
	FILE *s;
	if(trace != NULL) 
		s = trace;
	else
		s = stdout;
	if( ev->eventnr ==0 && ev->version){
		fprintf(s, "[ ");
		fprintf(s, "0,	");
		fprintf(s, "0,	");
		fprintf(s, "\"\",	" );
		fprintf(s, "0,	");
		fprintf(s, "\"system\",	"); 
		fprintf(s, "0,	");
		fprintf(s, "0,	");
		fprintf(s, "0,	");
		fprintf(s, "0,	");
		fprintf(s, "0,	");
		fprintf(s, "0,	");
		fprintf(s, "\"");
		fprintf(s, "version:%s, release:%s, threads:%s, memory:%s, host:%s, oid:%d, package:%s ", 
			ev->version, ev->release, ev->threads, ev->memory, ev->host, ev->oid, ev->package);
		fprintf(s, "\"	]\n");
		return ;
	}
	if( ev->eventnr < 0)
		return;
	fprintf(s, "[ ");
	fprintf(s, LLFMT",	", ev->eventnr);
	printf("\"%s\",	", ev->time);
	if( ev->function && *ev->function)
		fprintf(s, "\"%s[%d]%d\",	", ev->function, ev->pc, ev->tag);
	else
		fprintf(s, "\"\",	");
	fprintf(s, "%d,	", ev->thread);
	switch(ev->state){
	case MDB_START: fprintf(s, "\"start\",	"); break;
	case MDB_DONE: fprintf(s, "\"done \",	"); break;
	case MDB_WAIT: fprintf(s, "\"wait \",	"); break;
	case MDB_PING: fprintf(s, "\"ping \",	"); break;
	case MDB_SYSTEM: fprintf(s, "\"system\",	"); 
	}
	fprintf(s, LLFMT",	", ev->ticks);
	fprintf(s, LLFMT",	", ev->rss);
	fprintf(s, LLFMT",	", ev->size);
	fprintf(s, LLFMT",	", ev->inblock);
	fprintf(s, LLFMT",	", ev->oublock);
	fprintf(s, LLFMT",	", ev->majflt);
	fprintf(s, LLFMT",	", ev->swaps);
	fprintf(s, LLFMT",	", ev->csw);
	fprintf(s, "\"%s\"	]\n", ev->stmt);
}

static void
usageStethoscope(void)
{
    fprintf(stderr, "stethoscope [options] \n");
    fprintf(stderr, "  -d | --dbname=<database_name>\n");
    fprintf(stderr, "  -u | --user=<user>\n");
    fprintf(stderr, "  -P | --password=<password>\n");
    fprintf(stderr, "  -p | --port=<portnr>\n");
    fprintf(stderr, "  -h | --host=<hostname>\n");
    fprintf(stderr, "  -j | --json\n");
    fprintf(stderr, "  -o | --output=<file>\n");
	fprintf(stderr, "  -b | --beat=<delay> in milliseconds (default 50)\n");
	fprintf(stderr, "  -D | --debug\n");
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
	if(trace)
		fclose(trace);
	if(dbh)
		mapi_disconnect(dbh);
	exit(0);
}

int
main(int argc, char **argv)
{
	ssize_t  n;
	size_t len, buflen;
	char *host = NULL;
	int portnr = 0;
	char *dbname = NULL;
	char *uri = NULL;
	char *user = NULL;
	char *password = NULL;
	char buf[BUFSIZ], *buffer, *e, *response;
	int done = 0;
	EventRecord *ev = malloc(sizeof(EventRecord));

	static struct option long_options[11] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "port", 1, 0, 'p' },
		{ "password", 1, 0, 'P' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "json", 0, 0, 'j'},
		{ "output", 1, 0, 'o' },
		{ "debug", 0, 0, 'D' },
		{ "beat", 1, 0, 'b' },
		{ 0, 0, 0, 0 }
	};

	if( ev) memset((char*)ev,0, sizeof(EventRecord));
	else {
		fprintf(stderr,"could not allocate space\n");
		exit(-1);
	}

	/* parse config file first, command line options override */
	parse_dotmonetdb(&user, &password, &dbname, NULL, NULL, NULL, NULL);

	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:p:P:h:?jo:Db:",
					long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'D':
			debug = 1;
			break;
		case 'b':
			beat = atoi(optarg ? optarg : "5000");
			break;
		case 'd':
			if (dbname)
				free(dbname);
			dbname = strdup(optarg);
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
		case 'j':
			json = 1;
			break;
		case 'o':
			filename = strdup(optarg);
			printf("-- Output directed towards %s\n", filename);
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

	if(dbname == NULL){
		usageStethoscope();
		exit(-1);
	}

	if(debug)
		printf("stethoscope -d %s -o %s\n",dbname,filename);

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

	snprintf(buf,BUFSIZ-1,"profiler.setheartbeat(%d);",beat);
	if( debug)
		fprintf(stderr,"-- %s\n",buf);
	doQ(buf);

	snprintf(buf, BUFSIZ, " profiler.openstream(1);");
	if( debug)
		fprintf(stderr,"--%s\n",buf);
	doQ(buf);

	if(filename != NULL) {
		trace = fopen(filename,"w");

		if( trace == NULL) {
			fprintf(stderr,"Could not create file '%s', printing to stdout instead...\n", filename);
			filename = NULL;
		}
	}

	len = 0;
	buflen = BUFSIZ;
	buffer = (char *) malloc(buflen);
	if( buffer == NULL){
		fprintf(stderr,"Could not create input buffer\n");
		exit(-1);
	}
	conn = mapi_get_from(dbh);
	while ((n = mnstr_read(conn, buffer + len, 1, buflen - len-1)) >= 0) {
		if (n == 0 &&
		    (n = mnstr_read(conn, buffer + len, 1, buflen - len-1)) <= 0)
			break;
		buffer[len + n] = 0;
		response = buffer;
		if( debug)
				printf("%s", response);
		if(json) {
			if(trace != NULL) {
				fprintf(trace, "%s", response + len);
			} else {
				printf("%s", response + len);
				fflush(stdout);
			}
		}
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			if(!json) {
				//printf("%s\n", response);
				done= keyvalueparser(response,ev);
				if( done== 1){
					renderEvent(ev);
					resetEventRecord(ev);
				}
			}
			response = e + 1;
		}

		/* handle the case that the line is too long to
		 * fit in the buffer */
		if( response == buffer){
			char *new =  (char *) realloc(buffer, buflen + BUFSIZ);
			if( new == NULL){
				fprintf(stderr,"Could not extend input buffer\n");
				exit(-1);
			}
			buffer = new;
			buflen += BUFSIZ;
			len += n;
		}
		/* handle the case the buffer contains more than one
		 * line, and the last line is not completely read yet.
		 * Copy the first part of the incomplete line to the
		 * beginning of the buffer */
		else if (*response) {
			if (debug)
				printf("LASTLINE:%s", response);
			len = strlen(response);
			strncpy(buffer, response, len + 1);
		} else /* reset this line of buffer */
			len = 0;
	}

	doQ("profiler.stop();");
stop_disconnect:
	if(dbh)
		mapi_disconnect(dbh);
	if(trace)
		fclose(trace);
	printf("-- connection with server %s closed\n", uri ? uri : host);
	return 0;
}
