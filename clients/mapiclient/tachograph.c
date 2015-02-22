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

/* (c) M Kersten
 * Progress indicator
 * tachograph -d demo 
 * which connects to the demo database server and
 * will present a progress bar for each query.
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
#include <math.h>
#include <unistd.h>
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
static char *basefilename = "tacho";
static char *dbname;
static int beat = 5000;
static Mapi dbh;
static MapiHdl hdl = NULL;
static int capturing=0;

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
	fprintf(stderr, "  -b | --beat=<delay> in milliseconds (default 5000)\n");
    fprintf(stderr, "  -o | --output=<webfile>\n");
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
char *currentquery= 0;
lng starttime = 0;
lng finishtime = 0;
lng duration =0;
int malsize = 0;
char *prevquery= 0;
int prevprogress =0;

static FILE *tachofd;

static void resetTachograph(void){
	if (debug)
		fprintf(stderr, "RESET tachograph\n");
	malsize= 0;
	malargtop =0;
	currentfunction = 0;
	currentquery = 0;
	starttime = 0;
	finishtime = 0;
	duration =0;
	fclose(tachofd);
	tachofd = 0;
	prevprogress = 0;
	printf("\n"); 
	fflush(stdout);
}

static char stamp[BUFSIZ]={0};
static void
rendertime(lng ticks, int flg)
{
	int t, hr,min,sec;
	t = (int) (ticks/1000000);
	sec = t % 60;
	min = (t /60) %60;
	hr = (t /3600);
	if( flg)
	snprintf(stamp,BUFSIZ,"%02d:%02d:%02d.%06d", hr,min,sec, (int) ticks %1000000); 
	else
	snprintf(stamp,BUFSIZ,"%02d:%02d:%02d", hr,min,sec); 
}


static void
showBar(int progress, lng clk)
{
	int i;
	rendertime(duration,0);
	printf("%s [", stamp);
	if( prevprogress)
		for( i=76; i> prevprogress/2; i--)
			printf("\b \b");
	for( ; i < progress/2; i++)
		putchar('#');
	for( ; i < 50; i++)
		putchar('.');
	putchar(']');
	printf(" %3d%%",progress);
	if( duration && duration- clk > 0){
		rendertime(duration - clk,0);
		printf("  %s",stamp);
	} else
	if( duration && duration- clk < 0){
		rendertime(clk -duration ,0);
		printf(" -%s",stamp);
	} else
		printf("          ");
	fflush(stdout);
}

/* create the progressbar JSON file for pickup */
static void
progressBarInit(void)
{
	char buf[BUFSIZ];

	snprintf(buf,BUFSIZ,"%s_%s.json",basefilename,dbname);
	tachofd= fopen(buf,"w");
	if( tachofd == NULL){
		fprintf(stderr,"Could not create %s\n",buf);
		exit(0);
	}
	fprintf(tachofd,"{ \"tachograph\":0.1,\n");
	fprintf(tachofd," \"qid\":\"%s\",\n",currentfunction?currentfunction:"");
	fprintf(tachofd," \"query\":\"%s\",\n",currentquery?currentquery:"");
	fprintf(tachofd," \"started\": "LLFMT",\n",starttime);
	fprintf(tachofd," \"duration\":"LLFMT",\n",duration);
	fprintf(tachofd," \"instructions\":%d\n",malsize);
	fprintf(tachofd,"},\n");
	fflush(tachofd);
}

static void
update(EventRecord *ev)
{
	int progress=0;
	int i;
	char *qry, *q = 0, *c;
	int uid = 0,qid = 0;
 
	/* handle a ping event, keep the current instruction in focus */
	if (ev->state >= PING ) {
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
	if (!capturing && ev->state == START && ev->fcn && strncmp(ev->fcn, "function", 8) == 0) {
		if( (i = sscanf(ev->fcn + 9,"user.s%d_%d",&uid,&qid)) != 2){
			if( debug)
				fprintf(stderr,"Start phase parsing %d, uid %d qid %d\n",i,uid,qid);
			return;
		}
		if (capturing++ == 0){
			starttime = ev->clkticks;
			finishtime = ev->clkticks + ev->ticks;
			duration = ev->ticks;
		}
		if (currentfunction == 0)
			currentfunction = strdup(ev->fcn+9);
		if (debug)
			fprintf(stderr, "Enter function %s capture %d\n", currentfunction, capturing);
		return;
	}
	ev->clkticks -= starttime;

	if ( !capturing || ev->thread >= MAXTHREADS)
		return;

	/* start of instruction box */
	if (ev->state == START ) {
		if(ev->fcn && strstr(ev->fcn,"querylog.define") ){
			// extract a string argument
			currentquery = malarguments[0];
			malsize = atoi(malarguments[3]);
			// use the truncated query text, beware the the \ is already escaped in the call argument.
			q = qry = (char *) malloc(strlen(currentquery) * 2);
			for (c= currentquery; *c; ){
				if ( strncmp(c,"\\\\n",3) == 0){
					*q++ = '\n';
					c+=3;
				} else if ( strncmp(c,"\\\\",2) == 0){
					c+= 2;
				} else *q++ = *c++;
			}
			*q =0;
			currentquery = qry;
			if( ! (prevquery && strcmp(currentquery,prevquery)== 0) )
				printf("%s\n",qry);
			prevquery = currentquery;
			progressBarInit();
		}
		fprintf(tachofd,"{\n");
		fprintf(tachofd,"\"qid\":\"%s\",\n",currentfunction?currentfunction:"");
		fprintf(tachofd,"\"pc\":%d,\n",ev->pc);
		fprintf(tachofd,"\"time\": "LLFMT",\n",ev->clkticks);
		fprintf(tachofd,"\"status\": \"start\",\n");
		fprintf(tachofd,"\"estimate\": "LLFMT",\n",ev->ticks);
		fprintf(tachofd,"\"stmt\": \"%s\"\n",ev->stmt);
		fprintf(tachofd,"},\n");
		fflush(tachofd);
		return;
	}
	if( tachofd == NULL){
		if( debug) fprintf(stderr,"No tachofd available\n");
		return;
	}
	/* end the instruction box */
	if (ev->state == DONE ){
		fprintf(tachofd,"{\n");
		fprintf(tachofd,"\"qid\":\"%s\",\n",currentfunction?currentfunction:"");
		fprintf(tachofd,"\"pc\":%d,\n",ev->pc);
		fprintf(tachofd,"\"time\": "LLFMT",\n",ev->clkticks);
		fprintf(tachofd,"\"status\": \"done\",\n");
		fprintf(tachofd,"\"ticks\": "LLFMT",\n",ev->ticks);
		fprintf(tachofd,"\"stmt\": \"%s\"\n",ev->stmt);
		fprintf(tachofd,"},\n");
		fflush(tachofd);
		if( duration){
			progress = (int)(ev->clkticks / (duration/100.0));
			if ( progress > 100)
				progress = 100;
		} else {
			progress = (int)( ev->pc / (malsize/100.0));
		}
		if( progress > prevprogress) {
			showBar(progress,ev->clkticks);
			//printf("progress "LLFMT" %d\n", ev->clkticks,progress);
			prevprogress = progress;
		}
	}
	if (ev->state == DONE && ev->fcn && strncmp(ev->fcn, "function", 8) == 0) {
		if (currentfunction && strcmp(currentfunction, ev->fcn+9) == 0) {
			if( capturing == 0){
				free(currentfunction);
				currentfunction = 0;
			}
			showBar(100,ev->clkticks);
			capturing--;
			if(debug)
				fprintf(stderr, "Leave function %s capture %d\n", currentfunction, capturing);
			resetTachograph();
		} 
		if( capturing == 0)
			return;
	}
}

int
main(int argc, char **argv)
{
	ssize_t  n;
	size_t len;
	char *host = NULL;
	int portnr = 0;
	char *uri = NULL;
	char *user = NULL;
	char *password = NULL;
	char buf[BUFSIZ], *e, *response;
	int i = 0;
	FILE *trace = NULL;
	EventRecord event;

	static struct option long_options[15] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "port", 1, 0, 'p' },
		{ "password", 1, 0, 'P' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "beat", 1, 0, 'b' },
		{ "output", 1, 0, 'o' },
		{ "debug", 0, 0, 'D' },
		{ 0, 0, 0, 0 }
	};

	/* parse config file first, command line options override */
	parse_dotmonetdb(&user, &password, NULL, NULL, NULL, NULL);

	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:p:P:h:?:b:o:D",
					long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'b':
			beat = atoi(optarg ? optarg : "5000");
			break;
		case 'D':
			debug = 1;
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

	if(dbname == NULL){
		usageTachograph();
		exit(-1);
	}

	if(debug)
		printf("tachograph -d %s -o %s\n",dbname,basefilename);

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
	snprintf(buf,BUFSIZ,"%s_%s.trace",basefilename,dbname);
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
			if(debug)
				printf("%s\n", response);
			i= eventparser(response, &event);
			if (debug  )
				fprintf(stderr, "PARSE %d:%s\n", i, response);
			update(&event);
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
