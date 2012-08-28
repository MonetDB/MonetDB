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

#define COUNTERSDEFAULT "ISTestm"

/* #define _DEBUG_TOMOGRAPH_*/

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
static char hostname[128];
static char *filename="tomograph";
static char *title =0;
static int cores = 8;
static int debug = 0;
static int colormap = 0;

static FILE *gnudata;

static void
usage(void)
{
	fprintf(stderr, "tomograph [options]\n");
	fprintf(stderr, "  -d | --dbname=<database_name>\n");
	fprintf(stderr, "  -u | --user=<user>\n");
	fprintf(stderr, "  -P | --password=<password>\n");
	fprintf(stderr, "  -p | --port=<portnr>\n");
	fprintf(stderr, "  -h | --host=<hostname>\n");
	fprintf(stderr, "  -t | --threshold=<microseconds> default 1000\n");
	fprintf(stderr, "  -T | --title=<plit title>\n");
	fprintf(stderr, "  -o | --output=<file name prefix >\n");
	fprintf(stderr, "  -c | --cores=<number> of the target machine\n");
	fprintf(stderr, "  -m | --colormap produces colormap on tomograph.gpl\n");
	fprintf(stderr, "  -D | --debug\n");
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
#define MAXTHREADS 2048
#define MAXBOX 8192

typedef struct BOX{
	int xleft,xright;	/* box coordinates */
	int row;
	int color;
	char *label;
	int thread;
	long clkstart, clkend;
	long ticks;
	long memstart, memend;
	char *stmt;
	char *fcn;
	int state;
} Box;


int threads[MAXTHREADS];
Box box[MAXBOX];
int topbox=0;

long totalclkticks= 0; /* number of clock ticks reported */
long totalexecticks= 0; /* number of ticks reported for processing */
long lastclktick=0;

long threshold=1000;	/* default threshold is 1 ms */

long starttime=0;

static void dumpbox(int i){
	printf("[%d] %d,%d row %d color %d ", i, box[i].xleft, box[i].xright, box[i].row, box[i].color);
	if ( box[i].fcn)
		printf("%s ", box[i].fcn);
	printf("thread %d ", box[i].thread);
	printf("clk %ld - %ld ", box[i].clkstart, box[i].clkend);
	printf("mem %ld - %ld ", box[i].memstart, box[i].memend);
	printf("ticks %ld ", box[i].ticks);
	if ( box[i].stmt)
		printf("%s ", box[i].stmt);
	printf("\n");
}

/* color map management, fixed */
struct COLOR{
	int freq;
	char *mod, *fcn, *col;
} colors[] =
{
	{0,"idle","*","#FFE4C4"}, //bisque
	{0,"aggr","*","green"},
	{0,"algebra","*","yellow"},
	{0,"bat","*","orange"},
	{0,"batcalc","*","#E9967A"}, //darksalmon
	{0,"calc","*","#90EE90"}, //lightgreen
	{0,"group","*","green"},
	{0,"language","*","#FF00FF"}, //fuchsia
	{0,"mat","*","green"},
	{0,"mtime","*","greenyellow"},
	{0,"pcre","*","#DEB887"}, //burlywood
	{0,"pqueue","*","#5F9EA0"},//cadetblue
	{0,"io","*","#00FFFF"},	//aqua
	{0,"sql","*","blue"},
	{0,"*","*","#D3D3D3"}, //lightgrey
	{0,0,0,0}
};

/* produce a legenda image for the color map */
static void showmap(char *filename, int all)
{
	FILE *f;
	char buf[BUFSIZ];
	int i, k = 0;
	int w = 600;
	int h = 500;
	int object =1;

	snprintf(buf,BUFSIZ,"%s_map.gpl",filename);
	f = fopen(buf,"w");
	assert(f);

	fprintf(f,"set terminal pdfcairo enhanced color solid\n");
	fprintf(f,"set output \"%s_map.pdf\"\n",filename);
	fprintf(f,"set xrange [0:1800]\n");
	fprintf(f,"set yrange [0:520]\n");
	fprintf(f,"unset xtics\n");
	fprintf(f,"unset ytics\n");
	fprintf(f,"unset colorbox\n");
	for ( i= 0; colors[i].col; i++)
	if ( colors[i].freq || all){
		fprintf(f,"set object %d rectangle from %d, %d to %d, %d fillcolor rgb \"%s\" fillstyle solid 0.6\n",
			object++, (k % 3) * w, h-40, (k % 3) * w+w, h, colors[i].col);
		fprintf(f,"set label %d \"%s.%s\" at %d,%d\n", object++, colors[i].mod, colors[i].fcn, 
			(int) ((k % 3) *  w  + 0.2 *w) , h-25);
		if ( k % 3 == 2)
			h-= 40;
		k++;
	}
	fprintf(f,"plot -1 title \"\"\n");
}

static void updmap(int idx)
{
	char *mod, *fcn, buf[BUFSIZ], *call = buf;
	int i, fnd = 0;
	strcpy(buf,box[idx].fcn);
	mod = call;
	fcn = strchr(call,(int) '.');
	if ( fcn ){
		*fcn = 0; 
		fcn++;
	} else fcn = "*";
	for ( i =0; colors[i].col; i++)
	if ( mod && strcmp(mod,colors[i].mod)== 0) {
		if (strcmp(fcn,colors[i].fcn) == 0 || colors[i].fcn[0] == '*'){
			fnd = i;
			break;
		}
	} else
	if ( colors[i].mod[0] == '*'){
		fnd = i;
		break;
	}
	colors[fnd].freq++;
	box[idx].color = fnd;
}
		
/* gnuplot defaults */
static int width = 1000;	/* max pixelwidth of image */
static int height = 160;

static void gnuplotheader(char *filename){
	char *scale ="micro";
	int height = cores * 20;	
	fprintf(gnudata,"set terminal pdfcairo enhanced color solid\n");
	fprintf(gnudata,"set output \"%s.pdf\"\n",filename);
	fprintf(gnudata,"set xrange [0:%ld]\n", lastclktick-starttime);
	fprintf(gnudata,"set yrange [0:%d]\n", height);

/* todo
	if ( lastclktick > 1000000){
		scale = "";
		fprintf(gnudata,"set xtics 100000\n");
	} else
	if ( lastclktick > 1000){
		scale = "milli";
		fprintf(gnudata,"set xtics 1000\n");
	} else 
*/
	{
		scale = "micro";
		fprintf(gnudata,"set autoscale x\n");
	}

	fprintf(gnudata,"set ylabel \"threads\"\n");
	fprintf(gnudata,"set xlabel \"%sseconds, parallelism usage %6.1f %% (%d cores)\"\n", scale,
		((double)totalclkticks) /( cores * (lastclktick-starttime))* 100, cores);
	fprintf(gnudata,"set title \"%s\"\n", title? title:"tomogram");
	fprintf(gnudata,"set key right \n");
	fprintf(gnudata,"set grid xtics\n");
	fprintf(gnudata,"set palette model RGB defined ( 0 1.0 0.8 0.8, 1 1.0 0.8 1.0, 2 0.8 0.8 1.0, 3 0.8 1.0 1.0, 4 0.8 1.0 0.8, 5 1.0 1.0 0.8 )\n");
	fprintf(gnudata,"unset colorbox\n");
}

static int figure = 0;
static void createTomogram(void)
{
	char buf[BUFSIZ];
	int rows[MAXTHREADS];
	int top= 0;
	int object=1, i,j;
	int h = height /(2 * cores);

	if ( figure  )
		snprintf(buf,BUFSIZ,"%s_%d.gpl", filename, figure);
	else
		snprintf(buf,BUFSIZ,"%s.gpl", filename);
	figure++;
	gnudata= fopen(buf,"w");
	if ( gnudata == 0){
		printf("ERROR in creation of %s\n",buf);
		exit(-1);
	}
	printf("created tomogram file '%s' \n",buf);
	printf("Run: 'gnuplot %s' to create the '%s.pdf' file\n",buf,filename);
	printf("The colormap is stored in '%s_map.gpl'\n",filename);
	*strchr(buf,(int) '.') = 0;
	gnuplotheader(buf);

	if ( debug)
		printf("width %d height %d \n", width, height);
	/* detect all different threads and assign them a row */
	for ( i = 0; i < topbox; i++)
	if ( box[i].clkend ){
		for ( j = 0; j < top ;j++)
			if (rows[j] == box[i].thread)
				break;
		box[i].row = j;
		if ( j == top )
			rows[top++] = box[i].thread;
		updmap(i);
	}
	fprintf(gnudata,"set ytics (");
	for( i =0; i< top; i++)
		fprintf(gnudata,"\"%d\" %d%c",rows[i],i * 2 *h + h/2, (i< top-1? ',':' '));
	fprintf(gnudata,")\n");

	for ( i = 0; i < topbox; i++)
	if ( box[i].clkend ){
		box[i].xleft = box[i].clkstart;
		box[i].xright = box[i].clkend;
		if ( debug)
			dumpbox(i);
		fprintf(gnudata,"set object %d rectangle from %ld, %d to %ld, %d fillcolor rgb \"%s\" fillstyle solid 0.6\n",
			object++, box[i].clkstart, box[i].row * 2 *h, box[i].clkend, box[i].row* 2 * h + h, colors[box[i].color].col);
	}
	fprintf(gnudata,"plot -1 title \"\"\n");
	showmap(filename, 0);
	fclose(gnudata);
}

/* the main issue to deal with in the analyse is 
 * that the tomograph start can appear while the
 * system is already processing. This leads to
 * receiving 'done' events without matching 'start'
 */
static void parser(char *row){
#ifdef HAVE_STRPTIME
	char *c;
    struct tm stm;
	long clkticks = 0;
	int thread = 0;
	long ticks = 0;
	long memory = 0; /* in MB*/
	char *fcn = 0, *stmt= 0;
	int idx, state;


	if (row[0] != '[')
		return;
	c= strchr(row,(int)'"');
	if ( c &&  strncmp(c+1,"start",5) == 0) {
		state = 0;
		c += 6;
	} else
	if ( strncmp(c+1,"done",4) == 0){
		state = 1;
		c += 5;
	} else
		c= strchr(c+1,(int)'"');

	c= strchr(c+1,(int)'"');
	if ( c ){
		/* convert time to epoch in seconds*/
		memset(&stm, 0, sizeof(struct tm));
		c = strptime(c+1,"%H:%M:%S", &stm);
		clkticks = (((long)(stm.tm_hour * 60) + stm.tm_min) * 60 + stm.tm_sec) * 1000000;
		if ( c == 0)
			return;
		if (  *c == '.') {
			long usec;
			/* microseconds */
			usec = atol(c+1);
			assert(usec >=0 && usec <1000000);
			clkticks += usec;
		}
		c = strchr(c+1, (int)'"');
	}
	lastclktick= clkticks;
	c = strchr(c+1, (int)',');
	thread = atoi(c+1);
	c = strchr(c+1, (int)',');
	ticks = atol(c+1);
	c = strchr(c+1, (int)',');
	memory = atol(c+1);
	fcn = c;
	c = strstr(c+1, ":=");
	if ( c ){
		fcn = c+2;
		/* find genuine function calls */
		while ( isspace((int) *fcn) && *fcn) fcn++;
		stmt = strdup(fcn);
		if ( strchr(fcn, (int) '.') == 0)
			return;
	} else {
		fcn =strchr(fcn,(int)'"');
		if ( fcn ){
			fcn++;
			*strchr(fcn,(int)'"') = 0;
		}
	}

	if ( fcn && strchr(fcn,(int)'('))
		*strchr(fcn,(int)'(') = 0;


	if ( starttime == 0) {
		/* ignore all instructions up to the first function call */
		if (strncmp(fcn,"function",8) != 0)
			return;
		starttime = clkticks;
	}
	assert(clkticks-starttime >= 0);
	clkticks -=starttime;
	
	if ( strncmp(fcn,"end ",4) == 0)
		return;

	if (state == 1 && strncmp(fcn,"function",8) == 0){
		createTomogram();
		totalclkticks= 0; /* number of clock ticks reported */
		totalexecticks= 0; /* number of ticks reported for processing */
		if ( title == 0)
			title = strdup(fcn+9);
	}

	/* start of instruction box */
	if ( state == 0 && thread < MAXTHREADS ){
		idx = threads[thread];
		box[idx].thread = thread;
		box[idx].clkstart = clkticks;
		box[idx].memstart = memory;
		box[idx].stmt = stmt;
		box[idx].fcn = strdup(fcn);
	}
	/* end the instruction box */
	if ( state == 1 &&  thread < MAXTHREADS && fcn &&
		 clkticks - box[threads[thread]].clkstart > threshold  &&
		box[threads[thread]].fcn  && strcmp(fcn, box[threads[thread]].fcn) ==0){
		idx = threads[thread];
		box[idx].clkend = clkticks;
		box[idx].memend = memory;
		box[idx].ticks = ticks;
		totalclkticks += clkticks - box[threads[thread]].clkstart;
		totalexecticks += ticks - box[threads[thread]].ticks;
		threads[thread]= topbox++;
	}
	assert(topbox < MAXBOX);
#else
	(void) row;
#endif
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
#ifdef _DEBUG_TOMOGRAPH_
		printf("-- connection with server %s is %s\n", wthr->uri, id);
#endif
	} else {
#ifdef _DEBUG_TOMOGRAPH_
		printf("-- connection with server %s\n", wthr->uri);
#endif
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
#ifdef _DEBUG_TOMOGRAPH_
			printf("-- %s%s\n", id, buf);
#endif
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

	printf("-- %sopened UDP profile stream %s:%d for %s\n",
			id, hostname, portnr, host);

	snprintf(buf, BUFSIZ, "port := profiler.openStream(\"%s\", %d);",
			hostname, portnr);
	doQ(buf);

	/* Set Filters */
	doQ("profiler.setNone();");

	if (wthr->argc == 0) {
#ifdef _DEBUG_TOMOGRAPH_
		printf("-- %sprofiler.setAll();\n", id);
#endif
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
#ifdef _DEBUG_TOMOGRAPH_
			printf("-- %s%s\n", id, buf);
#endif
			doQ(buf);
			free(arg);
		}
	}
#ifdef _DEBUG_TOMOGRAPH_
	printf("-- %sprofiler.start();\n", id);
#endif
	doQ("profiler.start();");
	fflush(NULL);

	for ( i = 0; i < MAXTHREADS; i++)
		threads[i] = topbox++;
	i = 0;
	while ((n = mnstr_read(wthr->s, buf, 1, BUFSIZ)) > 0) {
		buf[n] = 0;
		response = buf;
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			/* TOMOGRAPH EXTENSIONS */
			parser(response);
			response = e + 1;
		}
		/* handle last line in buffer */
		if ( *response)
			printf("%s",response);
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
	int i, k=0;
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

	static struct option long_options[13] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "password", 1, 0, 'P' },
		{ "port", 1, 0, 'p' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "title", 1, 0, 'T' },
		{ "threshold", 1, 0, 't' },
		{ "cores", 1, 0, 'c' },
		{ "output", 1, 0, 'o' },
		{ "debug", 0, 0, 'D' },
		{ "colormap", 0, 0, 'm' },
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
		int c = getopt_long(argc, argv, "d:u:P:p:?:h:g:D:t:c:m",
			long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'D':
			debug = 1;
			break;
		case 'd':
			dbname = optarg;
			break;
		case 'u':
			user = optarg;
			break;
		case 'm':
			colormap=1;
			showmap("tomograph", 1);
			break;
		case 'P':
			password = optarg;
			break;
		case 'p':
			if ( optarg)
				portnr = atol(optarg);
			break;
		case 'h':
			host = optarg;
			break;
		case 'T':
			title = optarg;
			break;
		case 'o':
			filename = optarg;
			break;
		case 'c':
			cores = optarg?atoi(optarg):1;
			break;
		case 't':
			if ( optarg)
				threshold = atol(optarg);
			break;
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


	/* our hostname, how remote servers have to contact us */
	gethostname(hostname, sizeof(hostname));

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
