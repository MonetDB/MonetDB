/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "stream.h"
#include "stream_socket.h"
#include "mapi.h"
#include <string.h>
#include <sys/stat.h>
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
		if (hdl)					\
			mapi_explain_query(hdl, stderr);	\
		else if (dbh)					\
			mapi_explain(dbh, stderr);		\
		else						\
			fprintf(stderr, "!! command failed\n");	\
		goto stop_disconnect;				\
	} while (0)

#define doQ(X)							\
	do {							\
		if ((hdl = mapi_query(dbh, X)) == NULL ||	\
		    mapi_error(dbh) != MOK)			\
			die(dbh, hdl);				\
	} while (0)

static stream *conn = NULL;
static char hostname[128];
static char *filename = NULL;
static int beat = 0;
static int json = 0;
static Mapi dbh;
static MapiHdl hdl = NULL;
static FILE *trace ;

/*
 * Tuple level reformatting
 */

static _Noreturn void
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
	fprintf(stderr,"stethoscope: signal %d received\n",i);
	if(dbh)
		doQ("profiler.stop();");
stop_disconnect:
	// show follow up action only once
	if(trace) { fflush(trace);
		fclose(trace);
	}
	if(dbh)
		mapi_disconnect(dbh);
	/* exit(0); */
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
	int first = 1;
	EventRecord *ev = (EventRecord *) malloc(sizeof(EventRecord));
	DotMonetdb dotfile = {0};

	(void) first;
	memset(ev, 0, sizeof(EventRecord));
	static struct option long_options[13] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "port", 1, 0, 'p' },
		{ "password", 1, 0, 'P' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "json", 0, 0, 'j'},
		{ "pretty", 0, 0, 'y'},
		{ "output", 1, 0, 'o' },
		{ "debug", 0, 0, 'D' },
		{ "beat", 1, 0, 'b' },
		{ 0, 0, 0, 0 }
	};

	if(ev == NULL) {
		fprintf(stderr,"could not allocate space\n");
		exit(-1);
	}

	/* parse config file first, command line options override */
	parse_dotmonetdb(&dotfile);
        user = dotfile.user;
        password = dotfile.passwd;
        dbname = dotfile.dbname;
	host = dotfile.host;
	portnr = dotfile.port;

	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:p:P:h:?jyo:Db:",
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
		default:
			usageStethoscope();
		}
	}


	if(dbname == NULL){
		usageStethoscope();
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
	/* close(0); */

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
	if(debug)
		fprintf(stderr,"-- %s\n",buf);
	doQ(buf);

	snprintf(buf, BUFSIZ, "profiler.openstream();");
	if(debug)
		fprintf(stderr,"--%s\n",buf);
	doQ(buf);

	if(filename != NULL) {
		trace = fopen(filename,"w");

		if(trace == NULL) {
			fprintf(stderr,"Could not create file '%s' (%s), printing to stdout instead...\n", filename, strerror(errno));
			filename = NULL;
		}
	}

	len = 0;
	buflen = BUFSIZ;
	buffer = (char *) malloc(buflen);
	if(buffer == NULL){
		fprintf(stderr,"Could not create input buffer\n");
		exit(-1);
	}
	conn = mapi_get_from(dbh);
	if(!trace)
		trace = stdout;
	if(!json)
		renderHeader(trace);
	while ((n = mnstr_read(conn, buffer + len, 1, buflen - len-1)) >= 0) {
		if (n == 0 &&
		    (n = mnstr_read(conn, buffer + len, 1, buflen - len-1)) <= 0)
			break;
		buffer[len + n] = 0;
		response = buffer;
		if(debug)
				printf("%s", response);
		if(json) {
			if(trace != NULL) {
				fprintf(trace, "%s", response + len);
				fflush(trace);
			} else {
				printf("%s", response + len);
				fflush(stdout);
			}
		}
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			if(json)
				printf("%s\n", response);
			else{
				if (debug)
					printf("%s\n", response);
				done= keyvalueparser(response,ev);
				if(done== 1){
					renderSummary(trace, ev, "");
					first = 0;
				}
			}
			response = e + 1;
		}

		/* handle the case that the line is too long to
		 * fit in the buffer */
		if(response == buffer){
			char *new =  (char *) realloc(buffer, buflen + BUFSIZ);
			if(new == NULL){
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
			snprintf(buffer, len + 1, "%s", response);
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
