/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* author: M Kersten
 * Progress indicator
 * tachograph -d demo 
 * which connects to the demo database server and presents a server progress bar.
*/

#include "monetdb_config.h"
#include "monet_options.h"
#include "stream.h"
#include "stream_socket.h"
#include "mapi.h"
#include <string.h>
#include <sys/stat.h>
#include <signal.h>
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#include <math.h>
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
static char *dbname;
static Mapi dbh;
static MapiHdl hdl = NULL;
static int capturing=0;
static int lastpc;
static int showonbar;
static int pccount;

#define RUNNING 1
#define FINISHED 2
typedef struct{
	int state;
	lng etc;
	lng actual;
	lng clkticks;
	char *stmt;
} Event;

Event *events;
static int maxevents;

typedef struct {
	char *varname;
	char *source;
} Source;
Source *sources;	// original column name
int srctop, srcmax;

/*
 * Parsing the argument list of a MAL call to obtain un-quoted string values
 */

static void
usageTachograph(void)
{
    fprintf(stderr, "tachograph [options] \n");
    fprintf(stderr, "  -d | --dbname=<database_name>\n");
    fprintf(stderr, "  -u | --user=<user>\n");
    fprintf(stderr, "  -p | --port=<portnr>\n");
    fprintf(stderr, "  -h | --host=<hostname>\n");
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

char *currentfunction= 0;
int currenttag;		// to distinguish query invocations
lng starttime = 0;
lng finishtime = 0;
lng duration =0;
char *prevquery= 0;
int prevprogress =0;// pc of previous progress display
int prevlevel =0; 
size_t txtlength=0;

// limit the number of separate queries in the pool
#define QUERYPOOL 32
static int querypool = QUERYPOOL;
int queryid= 0;

static void resetTachograph(void){
	int i;
	if (debug)
		fprintf(stderr, "RESET tachograph\n");
	if( prevprogress)
		printf("\n"); 
	for(i=0; i < maxevents; i++)
	if( events[i].stmt)
		free(events[i].stmt);
	for(i=0; i< srctop; i++){
		free(sources[i].varname);
		free(sources[i].source);
	}
	capturing = 0;
	srctop=0;
	currentfunction = 0;
	currenttag = 0;
	starttime = 0;
	finishtime = 0;
	duration =0;
	prevprogress = 0;
	txtlength =0;
	prevlevel=0;
	lastpc = 0;
	showonbar = -1;
	pccount = 0;
	fflush(stdout);
	events = 0;
	queryid = (queryid+1) % querypool;
}

static char stamp[BUFSIZ]={0};
static void
rendertime(lng ticks, int flg)
{
	int t, hr,min,sec;

	if( ticks == 0){
		strcpy(stamp,"unknown ");
		return;
	}
	t = (int) (ticks/1000000);
	sec = t % 60;
	min = (t /60) %60;
	hr = (t /3600);
	if( flg)
	snprintf(stamp,BUFSIZ,"%02d:%02d:%02d.%06d", hr,min,sec, (int) ticks %1000000); 
	else
	snprintf(stamp,BUFSIZ,"%02d:%02d:%02d", hr,min,sec); 
}

#define MSGLEN 100


static void
showBar(int level, lng clk, char *stmt)
{
	lng i =0, nl;
	size_t stamplen=0;

	nl = level/2-prevlevel/2;
	if( level != 100 && (nl == 0 ||  level/2 <= prevlevel/2))
		return;
	assert(MSGLEN < BUFSIZ);
	if(prevlevel == 0)
		printf("[");
	else
	for( i= 50 - prevlevel/2 +txtlength; i>0; i--)
		printf("\b \b");
	for( i=0 ; i< nl ; i++)
		putchar('#');
	for( ; i < 50-prevlevel/2; i++)
		putchar('.');
	putchar(level ==100?']':'>');
	printf(" %3d%%",level);
	if( level == 100 || duration == 0){
		rendertime(clk,1);
		printf("  %s      ",stamp);
		stamplen= strlen(stamp)+3;
	} else
	if( duration && duration- clk > 0){
		rendertime(duration - clk,0);
		printf("  %s ETC  ", stamp);
		stamplen= strlen(stamp)+3;
	} else
	if( duration && duration- clk < 0){
		rendertime(clk - duration ,0);
		printf(" +%s ETC  ",stamp);
		stamplen= strlen(stamp)+3;
	} 
	if( stmt)
		printf("%s",stmt);
	fflush(stdout);
	txtlength = 11 + stamplen + strlen(stmt?stmt:"");
	prevlevel = level;
}

static void
initFiles(void)
{
}

static void
update(EventRecord *ev)
{
	int progress=0;
	int i;
	int uid = 0,qid = 0;
 
	/* handle a ping event, keep the current instruction in focus */
	if (ev->state >= MDB_PING ) {
		// All state events are ignored
		return;
	}

	if (debug)
		fprintf(stderr, "Update %s input %s stmt %s time " LLFMT"\n",(ev->state>=0?statenames[ev->state]:"unknown"),(ev->fcn?ev->fcn:"(null)"),(currentfunction?currentfunction:""),ev->clkticks -starttime);

	if (starttime == 0) {
		if (ev->fcn == 0 ) {
			if (debug)
				fprintf(stderr, "Skip %s input %s\n",(ev->state>=0?statenames[ev->state]:"unknown"),ev->fcn);
			return;
		}
		if (debug)
			fprintf(stderr, "Start capturing updates %s\n",ev->fcn);
	}
	if (ev->clkticks < 0) {
		/* HACK: *TRY TO* compensate for the fact that the MAL
		 * profiler chops-off day information, and assume that
		 * clkticks is < starttime because the tomograph run
		 * crossed a day boundary (midnight);
		 * we simply add 1 day (24 hours) worth of microseconds.
		 * NOTE: this surely does NOT work correctly if the
		 * tomograph run takes 24 hours or more ...
		 */
		ev->clkticks += US_DD;
	}

	/* monitor top level function brackets, we restrict ourselves to SQL queries */
	if (ev->state == MDB_START && ev->fcn && strncmp(ev->fcn, "function", 8) == 0) {
		if( capturing){
			//fprintf(stderr,"Input garbled or we lost some events\n");
			resetTachograph();
			capturing = 0;
		}
		if( (i = sscanf(ev->fcn + 9,"user.s%d_%d",&uid,&qid)) != 2){
			if( debug)
				fprintf(stderr,"Skip parsing %d, uid %d qid %d\n",i,uid,qid);
			return;
		}
		if (capturing++ == 0){
			starttime = ev->clkticks;
			finishtime = ev->clkticks + ev->ticks;
			duration = ev->ticks;
		}
		if (currentfunction == 0){
			currentfunction = strdup(ev->fcn+9);
			currenttag = ev->tag;
		}
		if (debug)
			fprintf(stderr, "Enter function %s capture %d\n", currentfunction, capturing);
		initFiles();
		return;
	}
	ev->clkticks -= starttime;

	if ( !capturing)
		return;

	if( ev->pc > lastpc)
		lastpc = ev->pc;

	/* start of instruction box */
	if (ev->state == MDB_START ) {
		if(ev->fcn && strstr(ev->fcn,"querylog.define") ){
			// extract a string argument from a known MAL signature
			maxevents = malsize;
			events = (Event*) malloc(maxevents * sizeof(Event));
			memset((char*)events, 0, maxevents * sizeof(Event));
			// use the truncated query text, beware that the \ is already escaped in the call argument.
			if(currentquery) {
				if( prevquery && strcmp(currentquery,prevquery)){
					printf("%s\n",currentquery);
					free(prevquery);
					prevquery = strdup(currentquery);
				} else
				if( prevquery == 0){
					printf("%s\n",currentquery);
					prevquery = strdup(currentquery);
				}
			}
		}
		if( ev->tag != currenttag)
			return;	// forget all except one query
		assert(ev->pc < maxevents);
		events[ev->pc].state = RUNNING;
		events[ev->pc].stmt = strdup(ev->beauty? ev->beauty:"");
		events[ev->pc].etc = ev->ticks;
		events[ev->pc].clkticks = ev->clkticks;
		showonbar = ev->pc;
		return;
	}
	/* end the instruction box */
	if (ev->state == MDB_DONE ){
			
		events[ev->pc].state= FINISHED;
		if( ev->tag != currenttag)
			return;	// forget all except one query

		progress = (int)(pccount++ / (malsize/100.0));
		for(i = lastpc; i > 0; i--)
			if( events[i].state == RUNNING )
				break;

		if( showonbar == ev->pc)
			showonbar = i < 0  ?-1 : i;
		showBar((progress>100.0?(int)100:progress), ev->clkticks, (showonbar >= 0 ? events[showonbar].stmt:NULL));
		events[ev->pc].actual= ev->ticks;
	}
	if (ev->state == MDB_DONE && ev->fcn && strncmp(ev->fcn, "function", 8) == 0) {
		if (currentfunction && strcmp(currentfunction, ev->fcn+9) == 0) {
			if( capturing == 0){
				free(currentfunction);
				currentfunction = 0;
			}
			
			showBar(100,ev->clkticks, "\n");
			if(debug)
				fprintf(stderr, "Leave function %s capture %d\n", currentfunction, capturing);
			resetTachograph();
			initFiles();
		} 
	}
}

int
main(int argc, char **argv)
{
	ssize_t  n;
	size_t len, buflen;
	char *host = NULL;
	int portnr = 0;
	char *uri = NULL;
	char *user = NULL;
	char *password = NULL;
	char buf[BUFSIZ], *buffer, *e, *response;
	int done = 0;
	EventRecord event;

	static struct option long_options[15] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "port", 1, 0, 'p' },
		{ "password", 1, 0, 'P' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "output", 1, 0, 'o' },
		{ "queries", 1, 0, 'q' },
		{ "debug", 0, 0, 'D' },
		{ 0, 0, 0, 0 }
	};

	/* parse config file first, command line options override */
	parse_dotmonetdb(&user, &password, &dbname, NULL, NULL, NULL, NULL);

	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:p:P:h:?:o:q:D",
					long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'D':
			debug = 1;
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
		case 'q':
			if (optarg)
				querypool = atoi(optarg) > 0? atoi(optarg):1;
			break;
		case 'h':
			host = optarg;
			break;
		case '?':
			usageTachograph();
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			exit(strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0 ? 0 : -1);
		default:
				usageTachograph();
			exit(-1);
		}
	}

	if( dbname == NULL){
		usageTachograph();
		exit(-1);
	}

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

	snprintf(buf,BUFSIZ-1,"profiler.setheartbeat(0);");
	if( debug)
		fprintf(stderr,"-- %s\n",buf);
	doQ(buf);

	snprintf(buf, BUFSIZ, " profiler.openstream(0);");
	if( debug)
		fprintf(stderr,"-- %s\n",buf);
	doQ(buf);

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
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			if(debug)
				printf("%s\n", response);
			done= keyvalueparser(response, &event);
			if( done == 1){
				update(&event);
			} else if( done == 0){
				if (debug  )
					fprintf(stderr, "PARSE %d:%s\n", done, response);
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
	printf("-- connection with server %s closed\n", uri ? uri : host);
	return 0;
}
