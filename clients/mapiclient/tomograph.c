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
 * Copyright August 2008-2013 MonetDB B.V.
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
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif
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

/* #define FOOTPRINT */
#ifdef FOOTPRINT
#define COUNTERSDEFAULT "ISTestMmrw"
#else
#define COUNTERSDEFAULT "ISTestmrw"
#endif

/* #define _DEBUG_TOMOGRAPH_*/

static struct {
	char tag;
	char *ptag;     /* which profiler group counter is needed */
	char *name;     /* which logical counter is needed */
	int status;     /* trace it or not */
}
profileCounter[] = {
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
	/*  1  */ { 'm', "memory", "arena", 0 }, /* memory details are ignored*/
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
	/*  4  */ { 'x', "ping50", "ping", 0 },
#ifdef FOOTPRINT
	/*  5  */ { 'M', "footprint", "footprint", 0 },
#endif
	/*  6  */ { 0, 0, 0, 0 }
};

typedef struct _wthread {
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
	HANDLE id;
#else
	pthread_t id;
#endif
	int tid;
	char *uri;
	char *host;
	char *dbname;
	int port;
	char *user;
	char *pass;
	stream *s;
	size_t argc;
	char **argv;
	struct _wthread *next;
	Mapi dbh;
	MapiHdl hdl;
} wthread;

static wthread *thds = NULL;
static char hostname[128];
static char *filename = "tomograph";
static char *tracefile = 0;
static lng startrange = 0, endrange = 0;
static char *inputfile = NULL;
static char *title = 0;
static int debug = 0;
static int colormap = 0;
static int fixedmap=1;
static int beat = 50;
static char *sqlstatement = NULL;
static int batchsize = 1; /* number of queries to combine in one run */
static int batch = 1; /* number of queries to combine in one run */
static lng maxio = 0;
static int cpus = 0;
static int atlas= 0;
static int atlaspage = 0;
static FILE *gnudata;

static int capturing=0;

static void
usage(void)
{
	fprintf(stderr, "tomograph [options]\n");
	fprintf(stderr, "  -d | --dbname=<database_name>\n");
	fprintf(stderr, "  -u | --user=<user>\n");
	fprintf(stderr, "  -P | --password=<password>\n");
	fprintf(stderr, "  -p | --port=<portnr>\n");
	fprintf(stderr, "  -h | --host=<hostname>\n");
	fprintf(stderr, "  -T | --title=<plot title>\n");
	fprintf(stderr, "  -s | --sql=<single sql expression>\n");
	fprintf(stderr, "  -t | --trace=<tomograph trace filename>\n");
	fprintf(stderr, "  -r | --range=<starttime>-<endtime>[ms,s] \n");
	fprintf(stderr, "  -i | --input=<profiler event file > \n");
	fprintf(stderr, "  -o | --output=<file prefix > (default 'tomograph'\n");
	fprintf(stderr, "  -b | --beat=<delay> in milliseconds (default 50)\n");
	fprintf(stderr, "  -A | --atlas=<number> of pages\n");
	fprintf(stderr, "  -B | --batch=<number> of queries per page\n");
	fprintf(stderr, "  -a | --adaptive colormap \n");
	fprintf(stderr, "  -m | --colormap=<userdefined colormap>\n");
	fprintf(stderr, "  -D | --debug\n");
	fprintf(stderr, "  -? | --help\n");
}


#define die(dbh, hdl) while (1) { (hdl ? mapi_explain_query(hdl, stderr) :	\
								   dbh ? mapi_explain(dbh, stderr) :		\
								   fprintf(stderr, "!! %scommand failed\n", id)); \
								  goto stop_disconnect; }
#define doQ(X) \
	if ((wthr->hdl = mapi_query(wthr->dbh, X)) == NULL || mapi_error(wthr->dbh) != MOK)	\
		die(wthr->dbh, wthr->hdl);
#define doQsql(X) \
	if ((hdlsql = mapi_query(dbhsql, X)) == NULL || mapi_error(dbhsql) != MOK) \
		die(dbhsql, hdlsql);


/* Any signal should be captured and turned into a graceful
 * termination of the profiling session. */
static void createTomogram(void);

static int activated = 0;
static void deactivateBeat(void)
{
	wthread *wthr;
	char *id = "deactivateBeat";
	if (activated == 0)
		return;
	activated = 0;
	if (debug)
		fprintf(stderr, "Deactivate beat\n");
	/* deactivate all connections  */
	for (wthr = thds; wthr != NULL; wthr = wthr->next)
		if (wthr->dbh) {
			doQ("profiler.deactivate(\"ping\");\n");
			doQ("profiler.stop();");
		}

	return;
stop_disconnect:
	;
}

static void
stopListening(int i)
{
	wthread *walk;
	(void) i;
	if (debug)
		fprintf(stderr, "Interrupt received\n");
	batch = 0;
	deactivateBeat();
	/* kill all connections  */
	for (walk = thds; walk != NULL; walk = walk->next) {
		if (walk->s != NULL) {
			mnstr_close(walk->s);
		}
	}
	atlaspage= atlas-1;
	batch = 0;
	createTomogram();
	exit(0);
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

static void activateBeat(void)
{
	char buf[BUFSIZ];
	char *id = "activateBeat";
	wthread *wthr;

	if (debug)
		fprintf(stderr, "Activate beat\n");
	if (activated == 1)
		return;
	activated = 1;
	snprintf(buf, BUFSIZ, "profiler.activate(\"ping%d\");\n", beat);
	/* activate all connections  */
	for (wthr = thds; wthr != NULL; wthr = wthr->next)
		if (wthr->dbh) {
			doQ(buf);
		}

	return;
stop_disconnect:
	if (wthr) {
		mapi_disconnect(wthr->dbh);
		mapi_destroy(wthr->dbh);
		wthr->dbh = 0;
	}
}

#define MAXTHREADS 2048
#define MAXBOX 32678	 /* should be > MAXTHREADS */

#define START 0
#define DONE 1
#define ACTION 2
#define PING 4
#define WAIT 5

typedef struct BOX {
	int row;
	int color;
	int thread;
	lng clkstart, clkend;
	lng ticks;
	lng memstart, memend;
	lng footstart, footend;
	lng reads, writes;
	char *stmt;
	char *fcn;
	int state;
} Box;

int threads[MAXTHREADS];
lng lastclk[MAXTHREADS];
Box box[MAXBOX];
int topbox = 0;

lng totalclkticks = 0; /* number of clock ticks reported */
lng totalexecticks = 0; /* number of ticks reported for processing */
lng lastclktick = 0;
lng totalticks; 
lng starttime = 0;
int figures = 0;
char *currentfunction= 0;
int object = 1;

static void resetTomograph(void){
	static char buf[128];
	int i;
	if(atlas) {
		snprintf(buf,128,"atlas_%02d",++atlaspage);
		filename = buf;
	} else
		filename = "tomograph";
	if (debug)
		fprintf(stderr, "RESET tomograph %d\n", atlaspage);
	for(i=0; i< MAXTHREADS; i++)
		lastclk[i]=0;
	topbox =0;
	for (i = 0; i < MAXTHREADS; i++)
		threads[i] = topbox++;

	startrange = 0, endrange = 0;
	maxio = 0;
	cpus = 0;
	batch = batchsize;
	totalclkticks = 0; 
	totalexecticks = 0;
	lastclktick = 0;
	figures = 0;
	currentfunction = 0;
	object = 1;
}

static void dumpbox(int i)
{
	printf("[%d] row %d color %d ", i, box[i].row, box[i].color);
	if (box[i].fcn)
		printf("%s ", box[i].fcn);
	printf("thread %d ", box[i].thread);
	printf("clk "LLFMT" - "LLFMT" ", box[i].clkstart, box[i].clkend);
	printf("mem "LLFMT" - "LLFMT" ", box[i].memstart, box[i].memend);
	printf("foot "LLFMT" - "LLFMT" ", box[i].footstart, box[i].footend);
	printf("ticks "LLFMT" ", box[i].ticks);
	if (box[i].stmt)
		printf("%s ", box[i].stmt);
	printf("\n");
}

/* color map management, fixed */
/* see http://www.uni-hamburg.de/Wiss/FB/15/Sustainability/schneider/gnuplot/colors.htm */
typedef struct {
	char *name;
	char *hsv;
	int red, green, blue;
} RGB;

RGB
dictionary[] = {
/* arbitrarily ordered by ascending R+G+B */
/*   0 */	{ "black", "#000000", 0, 0, 0 },
/* 100 */	{ "darkgreen", "#006400", 0, 100, 0 },
/* 128 */	{ "green", "#008000", 0, 128, 0 },
/* 128 */	{ "maroon", "#800000", 128, 0, 0 },
/* 128 */	{ "navy", "#000080", 0, 0, 128 },
/* 139 */	{ "darkblue", "#00008B", 0, 0, 139 },
/* 139 */	{ "darkred", "#8B0000", 139, 0, 0 },
/* 162 */	{ "midnightblue", "#191970", 25, 25, 112 },
/* 205 */	{ "darkslategray", "#2F4F4F", 47, 79, 79 },
/* 205 */	{ "indigo", "#4B0082", 75, 0, 130 },
/* 205 */	{ "mediumblue", "#0000CD", 0, 0, 205 },
/* 207 */	{ "forestgreen", "#228B22", 34, 139, 34 },
/* 227 */	{ "saddlebrown", "#8B4513", 139, 69, 19 },
/* 239 */	{ "darkolivegreen", "#556B2F", 85, 107, 47 },
/* 246 */	{ "firebrick", "#B22222", 178, 34, 34 },
/* 249 */	{ "brown", "#A52A2A", 165, 42, 42 },
/* 255 */	{ "blue", "#0000FF", 0, 0, 255 },
/* 255 */	{ "lime", "#00FF00", 0, 255, 0 },
/* 255 */	{ "red", "#FF0000", 255, 0, 0 },
/* 256 */	{ "olive", "#808000", 128, 128, 0 },
/* 256 */	{ "purple", "#800080", 128, 0, 128 },
/* 256 */	{ "teal", "#008080", 0, 128, 128 },
/* 272 */	{ "darkslateblue", "#483D8B", 72, 61, 139 },
/* 272 */	{ "seagreen", "#2E8B57", 46, 139, 87 },
/* 278 */	{ "darkcyan", "#008B8B", 0, 139, 139 },
/* 278 */	{ "darkmagenta", "#8B008B", 139, 0, 139 },
/* 284 */	{ "olivedrab", "#6B8E23", 107, 142, 35 },
/* 287 */	{ "sienna", "#A0522D", 160, 82, 45 },
/* 300 */	{ "crimson", "#DC143C", 220, 20, 60 },
/* 305 */	{ "limegreen", "#32CD32", 50, 205, 50 },
/* 315 */	{ "dimgray", "#696969", 105, 105, 105 },
/* 324 */	{ "orangered", "#FF4500", 255, 69, 0 },
/* 329 */	{ "darkgoldenrod", "#B8860B", 184, 134, 11 },
/* 345 */	{ "chocolate", "#D2691E", 210, 105, 30 },
/* 352 */	{ "mediumseagreen", "#3CB371", 60, 179, 113 },
/* 353 */	{ "mediumvioletred", "#C71585", 199, 21, 133 },
/* 359 */	{ "darkviolet", "#9400D3", 148, 0, 211 },
/* 376 */	{ "lawngreen", "#7CFC00", 124, 252, 0 },
/* 380 */	{ "lightseagreen", "#20B2AA", 32, 178, 170 },
/* 380 */	{ "steelblue", "#4682B4", 70, 130, 180 },
/* 381 */	{ "gray", "#7F7F7F", 127, 127, 127 },
/* 382 */	{ "chartreuse", "#7FFF00", 127, 255, 0 },
/* 382 */	{ "springgreen", "#00FF7F", 0, 255, 127 },
/* 384 */	{ "slategray", "#708090", 112, 128, 144 },
/* 389 */	{ "indianred", "#CD5C5C", 205, 92, 92 },
/* 394 */	{ "yellowgreen", "#9ACD32", 139, 205, 50 },
/* 395 */	{ "darkorange", "#FF8C00", 255, 140, 0 },
/* 395 */	{ "royalblue", "#4169E1", 65, 105, 225 },
/* 401 */	{ "peru", "#CD853F", 205, 133, 63 },
/* 401 */	{ "slateblue", "#6A5ACD", 106, 90, 205 },
/* 404 */	{ "mediumspringgreen", "#00FA9A", 0, 250, 154 },
/* 407 */	{ "blueviolet", "#8A2BE2", 138, 43, 226 },
/* 407 */	{ "darkorchid", "#9932CC", 153, 50, 204 },
/* 408 */	{ "lightslategray", "#778899", 119, 136, 153 },
/* 413 */	{ "cadetblue", "#5F9EA0", 95, 158, 160 },
/* 415 */	{ "darkturquoise", "#00CED1", 0, 206, 209 },
/* 415 */	{ "goldenrod", "#DAA520", 218, 165, 32 },
/* 420 */	{ "orange", "#FFA500", 255, 165, 0 },
/* 422 */	{ "deeppink", "#FF1493", 255, 20, 147 },
/* 425 */	{ "tomato", "#FF6347", 255, 99, 71 },
/* 429 */	{ "dodgerblue", "#1E90FF", 30, 144, 255 },
/* 446 */	{ "deepskyblue", "#00BFFF", 0, 191, 255 },
/* 462 */	{ "coral", "#FF7F50", 255, 127, 80 },
/* 465 */	{ "mediumslateblue", "#7B68EE", 123, 104, 238 },
/* 470 */	{ "gold", "#FFD700", 255, 215, 0 },
/* 474 */	{ "darkseagreen", "#8FBC8F", 143, 188, 143 },
/* 474 */	{ "rosybrown", "#BC8F8F", 188, 143, 143 },
/* 475 */	{ "greenyellow", "#ADFF2F", 173, 255, 47 },
/* 477 */	{ "mediumaquamarine", "#66CDAA", 102, 205, 170 },
/* 478 */	{ "mediumpurple", "#9370DB", 147, 112, 219 },
/* 478 */	{ "palevioletred", "#DB7093", 219, 112, 147 },
/* 479 */	{ "darkkhaki", "#BDB76B", 189, 183, 107 },
/* 482 */	{ "mediumorchid", "#BA55D3", 186, 85, 211 },
/* 485 */	{ "mediumturquoise", "#48D1CC", 72, 209, 204 },
/* 486 */	{ "cornflowerblue", "#6495ED", 100, 149, 237 },
/* 492 */	{ "salmon", "#FA8072", 250, 128, 114 },
/* 496 */	{ "lightcoral", "#F08080", 240, 128, 128 },
/* 496 */	{ "turquoise", "#40E0D0", 64, 224, 208 },
/* 504 */	{ "sandybrown", "#F4A460", 244, 164, 96 },
/* 505 */	{ "darksalmon", "#E9967A", 233, 150, 122 },
/* 507 */	{ "darkgray", "#A9A9A9", 169, 169, 169 },
/* 510 */	{ "aqua", "#00FFFF", 0, 255, 255 },
/* 510 */	{ "cyan", "#00FFFF", 0, 255, 255 },
/* 510 */	{ "fuchsia", "#FF00FF", 255, 0, 255 },
/* 510 */	{ "magenta", "#FF00FF", 255, 0, 255 },
/* 510 */	{ "yellow", "#FFFF00", 255, 255, 0 },
/* 526 */	{ "lightgreen", "#90EE90", 144, 238, 144 },
/* 530 */	{ "tan", "#D2B48C", 210, 180, 140 },
/* 537 */	{ "lightsalmon", "#FFA07A", 255, 160, 122 },
/* 540 */	{ "hotpink", "#FF69B4", 255, 105, 180 },
/* 541 */	{ "burlywood", "#DEB887", 222, 184, 135 },
/* 544 */	{ "orchid", "#DA70D6", 218, 112, 214 },
/* 555 */	{ "palegreen", "#98FB98", 152, 251, 152 },
/* 557 */	{ "navyblue", "#9FAFDF", 159, 175, 223 },
/* 576 */	{ "silver", "#C0C0C0", 192, 192, 192 },
/* 576 */	{ "skyblue", "#87CEEB", 135, 206, 235 },
/* 591 */	{ "lightskyblue", "#87CEFA", 135, 206, 250 },
/* 594 */	{ "aquamarine", "#7FFFD4", 127, 255, 212 },
/* 594 */	{ "lightsteelblue", "#B0C4DE", 176, 196, 222 },
/* 602 */	{ "plum", "#DDA0DD", 221, 160, 221 },
/* 606 */	{ "violet", "#EE82EE", 238, 130, 238 },
/* 610 */	{ "khaki", "#F0E68C", 240, 230, 140 },
/* 619 */	{ "lightblue", "#ADD8E6", 173, 216, 230 },
/* 623 */	{ "thistle", "#D8BFD8", 216, 191, 216 },
/* 630 */	{ "lightpink", "#FFB6C1", 255, 182, 193 },
/* 630 */	{ "powderblue", "#B0E0E6", 176, 224, 230 },
/* 633 */	{ "lightgrey", "#D3D3D3", 211, 211, 211 },
/* 640 */	{ "palegoldenrod", "#EEE8AA", 238, 232, 170 },
/* 646 */	{ "wheat", "#F5DEB3", 245, 222, 179 },
/* 650 */	{ "navajowhite", "#FFDEAD", 255, 222, 173 },
/* 650 */	{ "pink", "#FFC0CB", 255, 192, 203 },
/* 651 */	{ "paleturquoise", "#AFEEEE", 175, 238, 238 },
/* 658 */	{ "peachpuff", "#FFDAB9", 255, 218, 185 },
/* 660 */	{ "gainsboro", "#DCDCDC", 220, 220, 220 },
/* 664 */	{ "moccasin", "#FFE4B5", 255, 228, 181 },
/* 679 */	{ "bisque", "#FFE4C4", 255, 228, 196 },
/* 695 */	{ "blanchedalmond", "#FFEBCD", 255, 235, 205 },
/* 700 */	{ "antiquewhite", "#FAEBD7", 250, 235, 215 },
/* 707 */	{ "papayawhip", "#FFEFD5", 255, 239, 213 },
/* 708 */	{ "mistyrose", "#FFE4E1", 255, 228, 225 },
/* 710 */	{ "beige", "#F5F5DC", 245, 245, 220 },
/* 710 */	{ "lavender", "#E6E6FA", 230, 230, 250 },
/* 710 */	{ "lemonchiffon", "#FFFACD", 255, 250, 205 },
/* 710 */	{ "lightgoldenrodyellow", "#FAFAD2", 250, 250, 210 },
/* 720 */	{ "linen", "#FAF0E6", 250, 240, 230 },
/* 723 */	{ "cornsilk", "#FFF8DC", 255, 248, 220 },
/* 728 */	{ "oldlace", "#FDF5E6", 253, 245, 230 },
/* 734 */	{ "lightcyan", "#E0FFFF", 224, 255, 255 },
/* 734 */	{ "lightyellow", "#FFFFE0", 255, 255, 224 },
/* 735 */	{ "honeydew", "#F0FFF0", 240, 255, 240 },
/* 735 */	{ "whitesmoke", "#F5F5F5", 245, 245, 245 },
/* 738 */	{ "seashell", "#FFF5EE", 255, 245, 238 },
/* 740 */	{ "lavenderblush", "#FFF0F5", 255, 240, 245 },
/* 743 */	{ "aliceblue", "#F0F8FF", 240, 248, 255 },
/* 745 */	{ "floralwhite", "#FFFAF0", 255, 250, 240 },
/* 750 */	{ "azure", "#F0FFFF", 240, 255, 255 },
/* 750 */	{ "ivory", "#FFFFF0", 255, 255, 240 },
/* 750 */	{ "mintcream", "#F5FFFA", 245, 255, 250 },
/* 751 */	{ "ghostwhite", "#F8F8FF", 248, 248, 255 },
/* 755 */	{ "snow", "#FFFAFA", 255, 250, 250 },
/* 765 */	{ "white", "#FFFFFF", 255, 255, 255 },
};

#define NUM_COLORS ((int) (sizeof(dictionary) / sizeof(RGB)))
#define MAX_LEGEND_SHORT 27 /* max. size of colormap / legend */
#define PERCENTAGE 0.01 /* threshold for time filter */


/* The initial dictionary is geared towars TPCH-use */
typedef struct COLOR {
	int freq;
	lng timeused;
	char *mod, *fcn, *col;
} Color;

Color
colors[NUM_COLORS] = {{0,0,0,0,0}};

/*
 * The fixed color scheme is designed to keep the legend
 * consistent over multiple tomograms. Furthermore, groups
 * of instructions are kept within the same color group.
 * For less frequent or important operations, we re-use the color.
 *
 * The map design is initially based on the relative frequency
 * of MAL instructions encountered in TPCH and those that
 * take significant time.
 *
 * The built-in fixed color map can be over-ruled using a color
 * map file, which consists of mod\tfcn\tcol lines using
 * the color dictionary as a frame of reference.
 *
 * Use the adaptive scheme if you want distinct colors 
 * targeted for a single tomogram.
 */
Color
fixed_colors[] = {
	{ 0, 0, "aggr", "count", "darkgreen" },
	{ 0, 0, "aggr", "max", "lawngreen" },
	{ 0, 0, "aggr", "min", "lawngreen" },
	{ 0, 0, "aggr", "subcount", "darkgreen" },
	{ 0, 0, "aggr", "submin", "lawngreen" },
	{ 0, 0, "aggr", "submax", "lawngreen" },
	{ 0, 0, "aggr", "subsum", "lawngreen" },
	{ 0, 0, "aggr", "*", "limegreen" },
	{ 0, 0, "algebra", "join", "navy" },
	{ 0, 0, "algebra", "leftfetchjoin", "lightblue" },
	{ 0, 0, "algebra", "leftfetchjoinPath", "lightcyan" },
	{ 0, 0, "algebra", "leftjoin", "blue" },
	{ 0, 0, "algebra", "likesubselect", "greenyellow" },
	{ 0, 0, "algebra", "selectNotNil", "lime" },
	{ 0, 0, "algebra", "subselect", "forestgreen" },
	{ 0, 0, "algebra", "subslice", "lightgreen" },
	{ 0, 0, "algebra", "subsort", "springgreen" },
	{ 0, 0, "algebra", "tdiff", "cyan" },
	{ 0, 0, "algebra", "thetajoin", "darkblue" },
	{ 0, 0, "algebra", "thetasubselect", "lightgreen" },
	{ 0, 0, "algebra", "tinter", "seagreen" },
	{ 0, 0, "algebra", "*", "green" },
	{ 0, 0, "batcalc", "dbl", "deeppink" },
	{ 0, 0, "batcalc", "lng", "deeppink" },
	{ 0, 0, "batcalc", "hash", "coral" },
	{ 0, 0, "batcalc", "ifthenelse", "plum" },
	{ 0, 0, "batcalc", "*", "yellow" },
	{ 0, 0, "bat", "append", "salmon" },
	{ 0, 0, "bat", "insert", "salmon" },
	{ 0, 0, "bat", "mergecand", "orange" },
	{ 0, 0, "bat", "mirror", "lightsalmon" },
	{ 0, 0, "bat", "*", "sandybrown" },
	{ 0, 0, "batmtime", "*", "lightpink" },
	{ 0, 0, "batstr", "*", "yellowgreen" },
	{ 0, 0, "group", "subgroup", "linen" },
	{ 0, 0, "group", "subgroupdone", "violet" },
	{ 0, 0, "group", "*", "orchid" },
	{ 0, 0, "io", "stdout", "gray" },
	{ 0, 0, "language", "dataflow", "lightgrey" },
	{ 0, 0, "language", "*", "lightslategray" },
	{ 0, 0, "mat", "pack", "darkred" },
	{ 0, 0, "mat", "packIncrement", "red" },
	{ 0, 0, "mat", "*", "indianred" },
	{ 0, 0, "mkey", "bulk_rotate_xor_hash", "sienna" },
	{ 0, 0, "pqueue", "*", "khaki" },
	{ 0, 0, "sql", "bind", "lavender" },
	{ 0, 0, "sql", "bind_idxbat", "lavenderblush" },
	{ 0, 0, "sql", "delta", "tan" },
	{ 0, 0, "sql", "projectdelta", "gold" },
	{ 0, 0, "sql", "subdelta", "goldenrod" },
	{ 0, 0, "sql", "tid", "thistle" },
	{ 0, 0, "sql", "*", "mistyrose" },
	{ 0, 0, "*", "*", "peachpuff" },
	{ 0, 0, 0, 0, 0 }
};
/* initial mod.fcn list for adaptive colormap */
Color
base_colors[] = {
	/* reserve (base_)colors[0] for generic "*.*" */
/* 99999 */	{ 0, 0, "*", "*", 0 },
/* arbitrarily ordered by descending frequency in TPCH SF-100 with 32 threads */
/* 18169 */	{ 0, 0, "profiler", "ping", 0 },
/* 11054 */	{ 0, 0, "algebra", "leftfetchjoin", 0 },
/* 10355 */	{ 0, 0, "language", "pass", 0 },
/*  5941 */	{ 0, 0, "sql", "bind", 0 },
/*  5664 */	{ 0, 0, "mat", "packIncrement", 0 },
/*  4796 */	{ 0, 0, "algebra", "subselect", 0 },
/*  4789 */	{ 0, 0, "algebra", "join", 0 },
/*  4281 */	{ 0, 0, "profiler", "wait", 0 },
/*  2664 */	{ 0, 0, "sql", "projectdelta", 0 },
/*  2112 */	{ 0, 0, "batcalc", "!=", 0 },
/*  1886 */	{ 0, 0, "sql", "bind_idxbat", 0 },
/*  1881 */	{ 0, 0, "algebra", "leftfetchjoinPath", 0 },
/*  1013 */	{ 0, 0, "sql", "tid", 0 },
/*   832 */	{ 0, 0, "bat", "mergecand", 0 },
/*   813 */	{ 0, 0, "sql", "delta", 0 },
/*   766 */	{ 0, 0, "aggr", "subsum", 0 },
/*   610 */	{ 0, 0, "batcalc", "*", 0 },
/*   577 */	{ 0, 0, "group", "subgroupdone", 0 },
/*   481 */	{ 0, 0, "sql", "subdelta", 0 },
/*   448 */	{ 0, 0, "batcalc", "-", 0 },
/*   334 */	{ 0, 0, "bat", "mirror", 0 },
/*   300 */	{ 0, 0, "group", "subgroup", 0 },
/*   264 */	{ 0, 0, "batcalc", "==", 0 },
/*   260 */	{ 0, 0, "batcalc", "ifthenelse", 0 },
/*   209 */	{ 0, 0, "calc", "str", 0 },
/*   207 */	{ 0, 0, "aggr", "sum", 0 },
/*   200 */	{ 0, 0, "algebra", "thetasubselect", 0 },
/*   200 */	{ 0, 0, "algebra", "selectNotNil", 0 },
/*   197 */	{ 0, 0, "aggr", "subcount", 0 },
/*   166 */	{ 0, 0, "batcalc", "dbl", 0 },
/*   166 */	{ 0, 0, "algebra", "tinter", 0 },
/*   131 */	{ 0, 0, "algebra", "leftjoin", 0 },
/*   128 */	{ 0, 0, "batcalc", "isnil", 0 },
/*    98 */	{ 0, 0, "aggr", "count", 0 },
/*    97 */	{ 0, 0, "batcalc", ">", 0 },
/*    96 */	{ 0, 0, "batmtime", "year", 0 },
/*    96 */	{ 0, 0, "batcalc", "<", 0 },
/*    79 */	{ 0, 0, "sql", "assert", 0 },
/*    72 */	{ 0, 0, "sql", "rsColumn", 0 },
/*    72 */	{ 0, 0, "sql", "mvc", 0 },
/*    69 */	{ 0, 0, "mkey", "bulk_rotate_xor_hash", 0 },
/*    69 */	{ 0, 0, "calc", "lng", 0 },
/*    69 */	{ 0, 0, "batcalc", "hash", 0 },
/*    66 */	{ 0, 0, "pqueue", "utopn_max", 0 },
/*    66 */	{ 0, 0, "algebra", "tdiff", 0 },
/*    53 */	{ 0, 0, "calc", "int", 0 },
/*    47 */	{ 0, 0, "algebra", "likesubselect", 0 },
/*    44 */	{ 0, 0, "sql", "exportOperation", 0 },
/*    42 */	{ 0, 0, "algebra", "subslice", 0 },
/*    36 */	{ 0, 0, "pqueue", "utopn_min", 0 },
/*    36 */	{ 0, 0, "pqueue", "topn_max", 0 },
/*    33 */	{ 0, 0, "aggr", "submin", 0 },
/*    32 */	{ 0, 0, "batstr", "like", 0 },
/*    32 */	{ 0, 0, "batcalc", "or", 0 },
/*    32 */	{ 0, 0, "batcalc", "and", 0 },
/*    32 */	{ 0, 0, "batcalc", "+", 0 },
/*    24 */	{ 0, 0, "sql", "setVariable", 0 },
/*    23 */	{ 0, 0, "language", "dataflow", 0 },
/*    21 */	{ 0, 0, "algebra", "subsort", 0 },
/*    20 */	{ 0, 0, "sql", "catalog", 0 },
/*    19 */	{ 0, 0, "calc", "ptr", 0 },
/*    18 */	{ 0, 0, "sql", "resultSet", 0 },
/*    18 */	{ 0, 0, "sql", "exportResult", 0 },
/*    18 */	{ 0, 0, "io", "stdout", 0 },
/*    18 */	{ 0, 0, "calc", "!=", 0 },
/*    10 */	{ 0, 0, "sql", "update", 0 },
/*     9 */	{ 0, 0, "mtime", "addmonths", 0 },
/*     9 */	{ 0, 0, "calc", "ifthenelse", 0 },
/*     8 */	{ 0, 0, "sql", "copy_from", 0 },
/*     8 */	{ 0, 0, "sql", "affectedRows", 0 },
/*     8 */	{ 0, 0, "calc", "wrd", 0 },
/*     8 */	{ 0, 0, "calc", "isnil", 0 },
/*     7 */	{ 0, 0, "bat", "append", 0 },
/*     6 */	{ 0, 0, "mat", "pack", 0 },
/*     6 */	{ 0, 0, "bat", "new", 0 },
/*     5 */	{ 0, 0, "batcalc", "/", 0 },
/*     4 */	{ 0, 0, "sql", "exportValue", 0 },
/*     4 */	{ 0, 0, "calc", "date", 0 },
/*     4 */	{ 0, 0, "calc", "+", 0 },
/*     3 */	{ 0, 0, "calc", "/", 0 },
/*     3 */	{ 0, 0, "batstr", "substring", 0 },
/*     3 */	{ 0, 0, "batcalc", "lng", 0 },
/*     2 */	{ 0, 0, "calc", "min", 0 },
/*     2 */	{ 0, 0, "calc", "max", 0 },
/*     2 */	{ 0, 0, "calc", "bit", 0 },
/*     2 */	{ 0, 0, "calc", "*", 0 },
/*     2 */	{ 0, 0, "algebra", "thetajoin", 0 },
/*     1 */	{ 0, 0, "sql", "dec_round", 0 },
/*     1 */	{ 0, 0, "pqueue", "topn_min", 0 },
/*     1 */	{ 0, 0, "mtime", "date_sub_msec_interval", 0 },
/*     1 */	{ 0, 0, "iterator", "next", 0 },
/*     1 */	{ 0, 0, "iterator", "new", 0 },
/*     1 */	{ 0, 0, "calc", "dbl", 0 },
/*     1 */	{ 0, 0, "calc", "-", 0 },
/*     1 */	{ 0, 0, "calc", "==", 0 },
/*     1 */	{ 0, 0, "bat", "reverse", 0 },
/*     1 */	{ 0, 0, "bat", "insert", 0 },
/*     1 */	{ 0, 0, "algebra", "project", 0 },
/*     1 */	{ 0, 0, "algebra", "fetch", 0 },
/*     1 */	{ 0, 0, "aggr", "max", 0 },
/*     ? */	{ 0, 0, "aggr", "avg", 0 },
/*     ? */	{ 0, 0, "aggr", "subavg", 0 },
/*     0 */	{ 0, 0, 0, 0, 0 }
};

static char *getRGB(char *name)
{
	int i;
	for (i = 0; dictionary[i].name; i++)
		if (strcmp(dictionary[i].name, name) == 0)
			return dictionary[i].hsv;
	return 0;
}

static int cmp_clr ( const void * _one , const void * _two )
{
	Color *one = (Color*) _one, *two = (Color*) _two;

	/* "*.*" should always be smallest / first */
	if (one->mod && one->fcn && 
	    one->mod[0] == '*' && one->mod[1] == '\0' &&
	    one->fcn[0] == '*' && one->fcn[1] == '\0')
		return -1;
	if (two->mod && two->fcn && 
	    two->mod[0] == '*' && two->mod[1] == '\0' &&
	    two->fcn[0] == '*' && two->fcn[1] == '\0')
		return 1;

	/* order on timeused; use freq as tiebreaker */
	return ((one->timeused < two->timeused) ? -1 :
		((one->timeused > two->timeused) ? 1 :
		 ((one->freq < two->freq) ? -1 :
		  ((one->freq > two->freq) ? 1 :
		   0))));
}

static void initcolors(FILE *map)
{
	int i = 0;
	char *c;
	char buf[3][128];

	if ( map){
		/* read the color map */
		while ( fscanf(map,"%s\t%s\t%s\n", buf[0],buf[1],buf[2])== 3 && i< NUM_COLORS){
			colors[i].mod = strdup(buf[0]);
			colors[i].fcn = strdup(buf[1]);
			colors[i].freq = 0;
			colors[i].timeused = 0;
			c  = getRGB(buf[2]);
			if (c)
				colors[i].col = c;
			else
				fprintf(stderr, " %s.%s color '%s' not found\n", buf[0],buf[1],buf[2]);
			i++;
		}
			
	} else
	if (fixedmap) {
		for ( i = 0; fixed_colors[i].mod; i++){
			colors[i].mod = fixed_colors[i].mod;
			colors[i].fcn = fixed_colors[i].fcn;
			colors[i].freq = 0;
			colors[i].timeused = 0;
			c  = getRGB(fixed_colors[i].col);
			if (c)
				colors[i].col = c;
			else
				fprintf(stderr, "color '%s' not found\n", fixed_colors[i].col);
		}
	} else {
		for (i = 0; i < NUM_COLORS && base_colors[i].mod; i++) {
			colors[i].mod = base_colors[i].mod;
			colors[i].fcn = base_colors[i].fcn;
			colors[i].freq = 0;
			colors[i].timeused = 0;
			colors[i].col = dictionary[i].hsv;
		}
		for (; i < NUM_COLORS; i++) {
			colors[i].mod = 0;
			colors[i].fcn = 0;
			colors[i].freq = 0;
			colors[i].timeused = 0;
			colors[i].col = dictionary[i].hsv;
		}
	}
}

static void dumpboxes(void)
{
	FILE *f = 0;
	FILE *fcpu = 0;
	char buf[BUFSIZ];
	int i;
	lng e=0;

	if (tracefile) {
		snprintf(buf, BUFSIZ, "scratch.dat");
		f = fopen(buf, "w");
		snprintf(buf, BUFSIZ, "scratch_cpu.dat");
		fcpu = fopen(buf, "w");
	} else {
		snprintf(buf, BUFSIZ, "%s.dat", (filename ? filename : "tomograph"));
		f = fopen(buf, "w");
		snprintf(buf, BUFSIZ, "%s_cpu.dat", (filename ? filename : "tomograph"));
		fcpu = fopen(buf, "w");
	}

	for (i = 0; i < topbox; i++)
		if (box[i].clkend && box[i].fcn) {
			if (box[i].state < PING) {
				//io counters are zero at start of instruction !
				//fprintf(f,""LLFMT" %f 0 0 \n", box[i].clkstart, (box[i].memstart/1024.0));
				fprintf(f, ""LLFMT" %f %f 0 0\n", box[i].clkend, (box[i].memend / 1024.0), box[i].footend/1024.0);
			} else 
			if (box[i].state >= PING) {
				/* cpu stat events may arrive out of order, drop those */
				if ( box[i].clkstart <= e)
					continue;
				e = box[i].clkstart;
				fprintf(f, ""LLFMT" %f %f "LLFMT" "LLFMT"\n", box[i].clkstart, (box[i].memend / 1024.0), box[i].footend/1024.0, box[i].reads, box[i].writes);
				if (cpus == 0) {
					char *s = box[i].stmt;
					while (s && isspace((int) *s))
						s++;
					fprintf(fcpu, "0 ");
					while (s) {
						s = strchr(s + 1, (int) ' ');
						while (s && isspace((int) *s))
							s++;
						if (s)
							cpus++;
						fprintf(fcpu, "0 ");
					}
					fprintf(fcpu, "\n");
				}
				fprintf(fcpu, ""LLFMT" %s\n", box[i].clkend, box[i].stmt);
			}
		}

	if (f)
		(void) fclose(f);
	if (fcpu)
		(void) fclose(fcpu);
}

/* produce memory thread trace */
static void showmemory(void)
{
	int i;
	lng max = 0, min = LLONG_MAX;
	double mx, mn, mm;
	double scale = 1.0;
	const char * scalename = "MB";
	int digits;

	for (i = 0; i < topbox; i++)
		if (box[i].clkend && box[i].fcn) {
			if (box[i].memstart > max)
				max = box[i].memstart;
			if (box[i].memend > max)
				max = box[i].memend;
			if (box[i].memstart < min)
				min = box[i].memstart;
			if (box[i].memend < min)
				min = box[i].memend;

#ifdef FOOTPRINT
			if (box[i].footstart > max)
				max = box[i].footstart;
			if (box[i].footend > max)
				max = box[i].footend;
			if (box[i].footstart < min && box[i].footstart > 0)
				min = box[i].footstart;
			if (box[i].footend < min && box[i].footend > 0)
				min = box[i].footend;
#endif
		}
	if (min == max) {
		min -= 1;
		max += 1;
	}

	if (max >= 1024) {
		scale = 1024.0;
		scalename = "GB";
	}
	if (max / scale >= 100)
		digits = 0;
	else if (max / scale >= 10)
		digits = 1;
	else
		digits = 2;

	fprintf(gnudata, "\nset tmarg 1\n");
	fprintf(gnudata, "set bmarg 1\n");
	fprintf(gnudata, "set lmarg 10\n");
	fprintf(gnudata, "set rmarg 10\n");
	fprintf(gnudata, "set size 1,%s\n", "0.1");
	fprintf(gnudata, "set origin 0.0,0.87\n");

	fprintf(gnudata, "set xrange ["LLFMT".0:"LLFMT".0]\n", startrange, lastclktick - starttime);
	fprintf(gnudata, "set ylabel \"memory in %s\"\n", scalename);
	fprintf(gnudata, "unset xtics\n");
	mn = min / 1024.0;
	mx = max / 1024.0;
	mm = (mx - mn) / 50.0; /* 2% top & bottom margin */
	fprintf(gnudata, "set yrange [%f:%f]\n", mn - mm, mx + mm);
	fprintf(gnudata, "set ytics (\"%.*f\" %f, \"%.*f\" %f) nomirror\n", digits, min / scale, mn, digits, max / scale, mx);
#ifdef FOOTPRINT
	fprintf(gnudata, "plot \"%s.dat\" using 1:2 notitle with dots linecolor rgb \"blue\", \\\n", (tracefile ? "scratch" : filename));
	fprintf(gnudata, " \"%s.dat\" using 1:3 notitle with dots linecolor rgb \"black\"\n", (tracefile ? "scratch" : filename));
#else
	fprintf(gnudata, "plot \"%s.dat\" using 1:2 notitle with dots linecolor rgb \"blue\"\n", (tracefile ? "scratch" : filename));
#endif
	fprintf(gnudata, "unset yrange\n");
}

/* produce memory thread trace */
static void showcpu(void)
{
	int i;

	fprintf(gnudata, "\nset tmarg 1\n");
	fprintf(gnudata, "set bmarg 0\n");
	fprintf(gnudata, "set lmarg 10\n");
	fprintf(gnudata, "set rmarg 10\n");
	fprintf(gnudata, "set size 1,%s\n", "0.16");
	fprintf(gnudata, "set origin 0.0,%s\n", "0.72");
	fprintf(gnudata, "set ylabel \"CPU\"\n");
	fprintf(gnudata, "unset ytics\n");
	fprintf(gnudata, "set ytics 0, 1.1\n");
	fprintf(gnudata, "set grid ytics\n");
	fprintf(gnudata, "set format y ''\n");
	fprintf(gnudata, "unset border\n");

	fprintf(gnudata, "set xrange ["LLFMT".0:"LLFMT".0]\n", startrange, lastclktick - starttime);
	fprintf(gnudata, "set yrange [0:%d*1.1]\n", cpus);
	if (cpus)
		fprintf(gnudata, "plot ");
	for (i = 0; i < cpus; i++)
		fprintf(gnudata, "\"%s_cpu.dat\" using 1:($%d+(%d*1.1)) notitle with lines linecolor rgb \"%s\"%s",
				(tracefile ? "scratch" : filename), i + 2, i, (i % 2 == 0 ? "blue" : "red"), (i < cpus - 1 ? ",\\\n" : "\n"));
	fprintf(gnudata, "unset yrange\n");
	fprintf(gnudata, "unset ytics\n");
	fprintf(gnudata, "unset grid\n");
}

/* produce memory thread trace */
static void showio(void)
{
	int i;
	lng max = 0;

	for (i = 0; i < topbox; i++)
		if (box[i].clkend && box[i].state >= PING) {
			if (box[i].reads > max)
				max = box[i].reads;
			if (box[i].writes > max)
				max = box[i].writes;
		}
	max += beat;


	fprintf(gnudata, "\nset tmarg 1\n");
	fprintf(gnudata, "set bmarg 1\n");
	fprintf(gnudata, "set lmarg 10\n");
	fprintf(gnudata, "set rmarg 10\n");
	fprintf(gnudata, "set size 1,%s\n", "0.1");
	fprintf(gnudata, "set origin 0.0,0.87\n");
	fprintf(gnudata, "set xrange ["LLFMT".0:"LLFMT".0]\n", startrange, lastclktick - starttime);
	fprintf(gnudata, "set yrange [0:"LLFMT".0]\n", max / beat);
	fprintf(gnudata, "unset xtics\n");
	fprintf(gnudata, "unset ytics\n");
	fprintf(gnudata, "unset ylabel\n");
	fprintf(gnudata, "set y2tics in (0, "LLFMT".0) nomirror\n", max / beat);
	fprintf(gnudata, "set y2label \"IO per ms\"\n");
	fprintf(gnudata, "plot \"%s.dat\" using 1:(($4+$5)/%d.0) title \"reads\" with boxes fs solid linecolor rgb \"gray\" ,\\\n", (tracefile ? "scratch" : filename), beat);
	fprintf(gnudata, "\"%s.dat\" using 1:($5/%d.0) title \"writes\" with boxes fs solid linecolor rgb \"red\"  \n", (tracefile ? "scratch" : filename), beat);
	fprintf(gnudata, "unset y2label\n");
	fprintf(gnudata, "unset y2tics\n");
	fprintf(gnudata, "unset y2range\n");
	fprintf(gnudata, "unset title\n");
}

#define TME_US  1
#define TME_MS  2
#define TME_SS  4
#define TME_MM  8
#define TME_HH 16
#define TME_DD 32

#define US_MS ((lng) 1000)
#define US_SS (US_MS * 1000)
#define US_MM (US_SS * 60)
#define US_HH (US_MM * 60)
#define US_DD (US_HH * 24)

/* print time (given in microseconds) in human-readable form
 * showing the highest two relevant units */
static void fprintf_time ( FILE *f, lng time )
{
	int TME = TME_DD|TME_HH|TME_MM|TME_SS|TME_MS|TME_US;
	int tail = 0;
	const char *fmt = NULL;

	if (TME & TME_DD && (tail || time >= US_DD)) {
		fmt = LLFMT"%s";
		fprintf(f, fmt, time / US_DD, " d ");
		time %= US_DD;
		TME &= TME_HH;
		tail = 1;
	}
	if (TME & TME_HH && (tail || time >= US_HH)) {
		fmt = tail ? "%02d%s" : "%d%s";
		fprintf(f, fmt, (int) (time / US_HH), " h ");
		time %= US_HH;
		TME &= TME_MM;
		tail = 1;
	}
	if (TME & TME_MM && (tail || time >= US_MM)) {
		fmt = tail ? "%02d%s" : "%d%s";
		fprintf(f, fmt, (int) (time / US_MM), " m ");
		time %= US_MM;
		TME &= TME_SS;
		tail = 1;
	}
	if (TME & TME_SS && (tail || time >= US_SS)) {
		fmt = tail ? "%02d%s" : "%d%s";
		fprintf(f, fmt, (int) (time / US_SS), (TME & TME_MS) ? "." : " s ");
		time %= US_SS;
		TME &= TME_MS;
		tail = 1;
	}
	if (TME & TME_MS && (tail || time >= US_MS)) {
		fmt = tail ? "%03d%s" : "%d%s";
		fprintf(f, fmt, (int) (time / US_MS), (TME & TME_US) ? "." : " s ");
		time %= US_MS;
		TME &= TME_US;
		tail = 1;
	}
	if (TME & TME_US) {
		fmt = tail ? "%03d%s" : "%d%s";
		fprintf(f, fmt, (int) time, tail ? " ms " : " us ");
	}
}

/* print time (given in microseconds) in compact human-readable form
 * showing the units specified via TME */
static void fprintf_tm ( FILE *f, lng time, int TME )
{
	int tail = 0, digits = 1, scale = 1;
	const char *fmt = NULL;

	if (TME & TME_DD && (tail || time >= US_DD)) {
		fmt = LLFMT"%s";
		fprintf(f, fmt, time / US_DD, "d");
		time %= US_DD;
		TME &= TME_HH;
		tail = 1;
	}
	if (TME & TME_HH && (tail || time >= US_HH)) {
		fmt = tail ? "%02d%s" : "%d%s";
		fprintf(f, fmt, (int) (time / US_HH), "h");
		time %= US_HH;
		TME &= TME_MM;
		tail = 1;
	}
	if (TME & TME_MM && (tail || time >= US_MM)) {
		fmt = tail ? "%02d%s" : "%d%s";
		fprintf(f, fmt, (int) (time / US_MM), "m");
		time %= US_MM;
		TME &= TME_SS;
		tail = 1;
	}
	if (TME & TME_SS && (tail || time >= US_SS)) {
		fmt = tail ? "%02d%s" : "%d%s";
		fprintf(f, fmt, (int) (time / US_SS), (TME & TME_MS) ? "." : "s");
		if (time / US_SS > 99) {
			digits = 1;
			scale = 100;
		} else if (time / US_SS > 9) {
			digits = 2;
			scale = 10;
		} else {
			digits = 3;
			scale = 1;
		}
		time %= US_SS;
		TME &= TME_MS;
		tail = 1;
	}
	if (TME & TME_MS && (tail || time >= US_MS)) {
		fmt = tail ? "%0*d%s" : "%*d%s";
		fprintf(f, fmt, digits, (int) (time / US_MS / scale), (TME & TME_US) ? "." : tail ? "s" : "ms");
		if (time / US_MS > 99) {
			digits = 1;
			scale = 100;
		} else if (time / US_MS > 9) {
			digits = 2;
			scale = 10;
		} else {
			digits = 3;
			scale = 1;
		}
		time %= US_MS;
		TME &= TME_US;
		tail = 1;
	}
	if (TME & TME_US) {
		fmt = tail ? "%0*d%s" : "%*d%s";
		fprintf(f, fmt, digits, (int) (time / scale), tail ? "ms" : "us");
	}
}

/* produce a legenda image for the color map */
static void showcolormap(char *filename, int all)
{
	FILE *f = 0;
	char buf[BUFSIZ];
	int i, k = 0;
	int w = 600;
	int h = 590;
	lng totfreq = 0, tottime = 0;
	Color *clrs = colors, *_clrs_ = NULL;
	lng duration = lastclktick - starttime;

	if (all) {
		snprintf(buf, BUFSIZ, "%s.gpl", filename);
		f = fopen(buf, "w");
		assert(f);
		fprintf(f, "set terminal pdfcairo noenhanced color solid size 8.3, 11.7\n");
		fprintf(f, "set output \"%s.pdf\"\n", filename);
		fprintf(f, "set size 1,1\n");
		fprintf(f, "set xrange [0:1800]\n");
		fprintf(f, "set yrange [0:600]\n");
		fprintf(f, "unset xtics\n");
		fprintf(f, "unset ytics\n");
		fprintf(f, "unset colorbox\n");
		fprintf(f, "unset border\n");
		fprintf(f, "unset title\n");
		fprintf(f, "unset ylabel\n");
		fprintf(f, "unset xlabel\n");
		fprintf(f, "set origin 0.0,0.0\n");
	} else {
		f = gnudata;
		fprintf(f, "\nset tmarg 0\n");
		fprintf(f, "set bmarg 0\n");
		fprintf(f, "set lmarg 10\n");
		fprintf(f, "set rmarg 10\n");
		fprintf(f, "set size 1,0.4\n");
		fprintf(f, "set origin 0.0,%s\n", "-0.05");
		fprintf(f, "set xrange [0:1800]\n");
		fprintf(f, "set yrange [0:600]\n");
		fprintf(f, "unset xtics\n");
		fprintf(f, "unset ytics\n");
		fprintf(f, "unset colorbox\n");
		fprintf(f, "unset border\n");
		fprintf(f, "unset title\n");
		fprintf(f, "unset ylabel\n");
	}
	if ( !fixedmap ){
		/* create copy of colormap and sort in ascending order of timeused;
		 * "*.*" stays first (colors[0]) */
		_clrs_ = (Color*) malloc (sizeof(colors));
		if (_clrs_) {
			memcpy (_clrs_, colors, sizeof(colors));
			qsort (_clrs_, NUM_COLORS, sizeof(Color), cmp_clr);
			clrs = _clrs_;
		}
		/* show colormap / legend in descending order of timeused;
		 * show max. the MAX_LEGEND_SHORT-1 most expensive function calls;
		 * show all remaining aggregated as "*.*" */
		for (i = NUM_COLORS - 1; i >= 0; i--)
			if (clrs[i].mod && (clrs[i].freq > 0 || all)) {
				if (all || k < MAX_LEGEND_SHORT - 1 || i == 0) {
					tottime += clrs[i].timeused;
					totfreq += clrs[i].freq;

					if (k % 3 == 0)
						h -= 45;
					fprintf(f, "set object %d rectangle from %f, %f to %f, %f fillcolor rgb \"%s\" fillstyle solid 1.0\n",
							object++, (double) (k % 3) * w, (double) h - 40, (double) ((k % 3) * w + 0.15 * w), (double) h - 5, clrs[i].col);
					fprintf(f, "set label %d \"%s.%s \" at %d,%d\n",
							object++, clrs[i].mod, clrs[i].fcn, (int) ((k % 3) * w + 0.2 * w), h - 15);
					fprintf(f, "set label %d \"%d calls: ",
							object++, clrs[i].freq);
					fprintf_time(f, clrs[i].timeused);
					fprintf(f, "\" at %f,%f\n",
							(double) ((k % 3) * w + 0.2 * w), (double) h - 35);
					k++;
				} else {
					clrs[0].timeused += clrs[i].timeused;
					clrs[0].freq += clrs[i].freq;
				}
			}
		if (_clrs_) {
			clrs = colors;
			free(_clrs_);
			_clrs_ = NULL;
		}
	} else {
		/* use a heuristic fixed map to highlight the important operations */
		/* limit the legend to the most important ones <MAX_LEGEND_SHORT and > 1% */
		int	map[MAX_LEGEND_SHORT], n, m=0, j;
		for (i = 0; i < MAX_LEGEND_SHORT; i++)
			map[i] = -1;
		for (i = 0; i < NUM_COLORS - 1; i++) {
			tottime += clrs[i].timeused;
			totfreq += clrs[i].freq;
		}
		/* filter out the top N interesting elements */
		for (i = 0; i < NUM_COLORS - 1; i++)
		if (clrs[i].col && clrs[i].mod  && (clrs[i].freq > 0 || all))
		{
			if (clrs[i].timeused > PERCENTAGE * duration || clrs[i].freq > PERCENTAGE *  totfreq) {
				for ( j = 0; j < m && clrs[map[j]].timeused > clrs[i].timeused; j++)
					;
				if ( m < MAX_LEGEND_SHORT){
					for( n = m-1; n > j; n--)
						map[n] = map[n-1];
					map[j] = i;
					m++;
				}
			}
		}
		/* sort them back into the color map order */
		for ( i = 0; i < m; i++)
		for ( j = i+1; j< m; j++)
		if ( map[i] > map[j] ){
			n = map[i];
			map[i]= map[j];
			map[j] = n;
		}

		for (i = 0; i < m; i++)
		if ( clrs[map[i]].col){
				if (k % 3 == 0)
					h -= 45;
				fprintf(f, "set object %d rectangle from %f, %f to %f, %f fillcolor rgb \"%s\" fillstyle solid 1.0\n",
						object++, (double) (k % 3) * w, (double) h - 40, (double) ((k % 3) * w + 0.15 * w), (double) h - 5, clrs[map[i]].col);
				fprintf(f, "set label %d \"%s.%s \" at %d,%d\n",
						object++, clrs[map[i]].mod, clrs[map[i]].fcn, (int) ((k % 3) * w + 0.2 * w), h - 15);
				fprintf(f, "set label %d \"%d calls: ",
						object++, clrs[map[i]].freq);
				fprintf_time(f, clrs[map[i]].timeused);
				fprintf(f, "\" at %f,%f\n",
						(double) ((k % 3) * w + 0.2 * w), (double) h - 35);
				k++;
			}
	}

	h -= 45;
	fprintf(f, "set label %d \" "LLFMT" MAL instructions executed; total CPU core time: ",
			object++, totfreq);
	fprintf_time(f, tottime);
	fprintf(f, "; parallelism usage %.1f %%", totalclkticks / (totalticks / 100.0));
	fprintf(f, "\" at %d,%d\n",
			(int) (0.2 * w), h - 35);
	fprintf(f, "plot 0 notitle with lines linecolor rgb \"white\"\n");
}

static void updmap(int idx)
{
	char *mod, *fcn, buf[BUFSIZ], *call = buf;
	int i, fnd = 0;

	strcpy(buf, box[idx].fcn);
	mod = call;
	fcn = strchr(call, (int) '.');
	if (fcn) {
		*fcn = 0;
		fcn++;
	} else
		fcn = "*";
	if (fixedmap) {
		/* find first match for "mod.fcn", "*.fcn", "mod.*", "*.*" */
		for (i = 0; i < NUM_COLORS && colors[i].mod; i++)
			if (strcmp(mod, colors[i].mod) == 0 || colors[i].mod[0] == '*') {
				if (strcmp(fcn, colors[i].fcn) == 0 || colors[i].fcn[0] == '*') {
					fnd = i;
					break;
				}
			}
	} else {
		/* find "mod.fcn" */
		for (i = 1; i < NUM_COLORS && colors[i].mod; i++)
			if (strcmp(mod, colors[i].mod) == 0) {
				if (strcmp(fcn, colors[i].fcn) == 0) {
					fnd = i;
					break;
				}
			}
		if (fnd == 0 && i < NUM_COLORS) {
			/* not found, but still free slot: add new one */
			fnd = i;
			colors[fnd].mod = strdup(mod);
			colors[fnd].fcn = strdup(fcn);
			printf("added function #%d: %s.%s\n", fnd, mod, fcn);
		}
	}

	colors[fnd].freq++;
	colors[fnd].timeused += box[idx].clkend - box[idx].clkstart;
	box[idx].color = fnd;
}

/* keep the data around for re-painting with range filters*/
static void keepdata(char *filename)
{
	int i;
	FILE *f;
	char buf[BUFSIZ];

	if (tracefile)
		return;
	snprintf(buf, BUFSIZ, "%s.trace", filename);
	f = fopen(buf, "w");
	assert(f);

	for (i = 0; i < topbox; i++)
		if (box[i].clkend && box[i].fcn) {
			//if ( debug)
			//fprintf(stderr,"%3d\t%8ld\t%5ld\t%s\n", box[i].thread, box[i].clkstart, box[i].clkend-box[i].clkstart, box[i].fcn);

			fprintf(f, "%d\t"LLFMT"\t"LLFMT"\t", box[i].thread, box[i].clkstart, box[i].clkend);
			fprintf(f, ""LLFMT"\t"LLFMT"\t"LLFMT"\t", box[i].ticks, box[i].memstart, box[i].memend);
			fprintf(f, "%d\t"LLFMT"\t"LLFMT"\t", box[i].state, box[i].reads, box[i].writes);
			fprintf(f, "%s\t", box[i].fcn);
			fprintf(f, "%s\n", box[i].stmt ? box[i].stmt : box[i].fcn);
		}

	(void) fclose(f);
}

static void scandata(char *filename)
{
	FILE *f;
	char line[2*BUFSIZ], buf[BUFSIZ], buf2[BUFSIZ];
	int i = 0;

	f = fopen(filename, "r");
	if (f == 0) {
		snprintf(buf, BUFSIZ, "%s.trace", filename);
		f = fopen(buf, "r");
		if (f == NULL) {
			fprintf(stderr, "Could not open file '%s'\n", buf);
			exit(-1);
		}
	}
	starttime = 0;

	while (!feof(f)) {
		int x;
		if ( fgets(line,2 * BUFSIZ,f) == NULL) {
			fprintf(stderr, "scandata read error\n");
		}
		x = sscanf(line, "%d\t"LLFMT"\t"LLFMT"\t"LLFMT"\t"LLFMT"\t"LLFMT"\t%d\t"LLFMT"\t"LLFMT"\t%s\t%s\n", 
			&box[i].thread, &box[i].clkstart, &box[i].clkend,
			&box[i].ticks, &box[i].memstart, &box[i].memend,
			&box[i].state, &box[i].reads, &box[i].writes, buf, buf2);
		if (x != 11) {
			fprintf(stderr, "scandata: sscanf() matched %d instead of 11 items in\n'%s'\n", x, line);
		}
		if (box[i].thread < 0 || box[i].clkstart < 0 || box[i].clkend < 0 || box[i].ticks < 0 || box[i].memstart < 0 || box[i].memend < 0 || box[i].state < 0 || box[i].reads < 0 || box[i].writes < 0) {
			fprintf(stderr, "scandata: sscanf() read negative value(s) from\n'%s'\n", line);
		}
		box[i].fcn = strdup(buf);
		box[i].stmt = strdup(buf);
		/* focus on part of the time frame */
		if (endrange) {
			if (box[i].clkend < startrange || box[i].clkstart >endrange)
				continue;
			if (box[i].clkstart < startrange)
				box[i].clkstart = startrange;
			if (box[i].clkend > endrange)
				box[i].clkend = endrange;
		}
		if (box[i].clkend > lastclktick)
			lastclktick = box[i].clkend;
		if (lastclk[box[i].thread] < box[i].clkend)
			lastclk[box[i].thread] = box[i].clkend;
		totalclkticks += box[i].clkend - box[i].clkstart;
		totalexecticks += box[i].ticks - box[i].ticks;
		i++;
		assert(i < MAXBOX);
	}
	fclose(f);
	topbox = i;
}
/* gnuplot defaults */
static int height = 160;

static void gnuplotheader(char *filename)
{
	time_t tm;
	char *date, *c;
	fprintf(gnudata, "set terminal pdfcairo noenhanced color solid size 8.3,11.7\n");
	fprintf(gnudata, "set output \"%s.pdf\"\n", filename);
	fprintf(gnudata, "set size 1,1\n");
	fprintf(gnudata, "set tics front\n");
	tm = time(0);
	date = ctime(&tm);
	if (strchr(date, (int) '\n'))
		*strchr(date, (int) '\n') = 0;
	for (c = title; c && *c; c++)
		if (*c == '_')
			*c = '-';
	fprintf(gnudata, "set title \"%s\t\t%s\"\n", (title ? title : "Tomogram"), date);
	fprintf(gnudata, "set multiplot\n");
}

static void createTomogram(void)
{
	char buf[BUFSIZ];
	int rows[MAXTHREADS] = {0};
	int top = 0;
	int i, j;
	int h, prevobject = 1;
	lng w = lastclktick - starttime;
	lng scale;
	char *scalename = "\0\0\0\0";
	int digits;
	int TME;

	snprintf(buf, BUFSIZ, "%s.gpl", filename);
	gnudata = fopen(buf, "w");
	if (gnudata == 0) {
		printf("ERROR in creation of %s\n", buf);
		exit(-1);
	}
	*strchr(buf, (int) '.') = 0;
	gnuplotheader(buf);
	dumpboxes();
	showio();
	showmemory();
	showcpu();

	fprintf(gnudata, "\nset tmarg 1\n");
	fprintf(gnudata, "set bmarg 3\n");
	fprintf(gnudata, "set lmarg 10\n");
	fprintf(gnudata, "set rmarg 10\n");
	fprintf(gnudata, "set size 1,0.4\n");
	fprintf(gnudata, "set origin 0.0,%s\n", "0.32");
	fprintf(gnudata, "set xrange ["LLFMT".0:"LLFMT".0]\n", startrange, lastclktick - starttime);

	/* detect all different threads and assign them a row */
	for (i = 0; i < topbox; i++)
		if (box[i].clkend && box[i].state != PING) {
			for (j = 0; j < top; j++)
				if (rows[j] == box[i].thread)
					break;
			box[i].row = j;
			if (j == top)
				rows[top++] = box[i].thread;
			updmap(i);
		}


	height = top * 20;
	fprintf(gnudata, "set yrange [0:%d]\n", height);
	fprintf(gnudata, "set ylabel \"threads\"\n");
	fprintf(gnudata, "set key right \n");
	fprintf(gnudata, "unset colorbox\n");
	fprintf(gnudata, "unset title\n");

	if (fixedmap) {
		if (w >= 10 * US_DD) {
			scale = US_DD;
			scalename = "d\0\0days";
		} else if (w >= 10 * US_HH) {
			scale = US_HH;
			scalename = "h\0\0hours";
		} else if (w >= 10 * US_MM) {
			scale = US_MM;
			scalename = "m\0\0minutes";
		} else if (w >= US_SS) {
			scale = US_SS;
			scalename = "s\0\0seconds";
		} else if (w >= US_MS) {
			scale = US_MS;
			scalename = "ms\0milliseconds";
		} else {
			scale = 1;
			scalename = "us\0microseconds";
		}
		if (w / scale >= 1000)
			digits = 0;
		else if (w / scale >= 100)
			digits = 1;
		else if (w / scale >= 10)
			digits = 2;
		else
			digits = 3;
		w /= 10;
		fprintf(gnudata, "set xtics (\"0\" 0,");
		for (i = 1; i < 10; i++)
			fprintf(gnudata, "\"%.*f\" "LLFMT".0,", digits, (double) i * w / scale, i * w);
		fprintf(gnudata, "\"%.*f %s\" "LLFMT".0", digits, (double) i * w / scale, scalename, i * w);
		fprintf(gnudata, ")\n");
	} else {
		if (w >= US_DD) {
			TME = TME_DD|TME_HH;
		} else if (w >= US_HH) {
			TME = TME_HH|TME_MM;
		} else if (w >= US_MM) {
			TME = TME_MM|TME_SS;
		} else if (w >= US_SS) {
			TME = TME_SS|TME_MS;
		} else if (w >= US_MS) {
			TME = TME_MS|TME_US;
		} else {
			TME = TME_US;
		}
		w /= 10;
		fprintf(gnudata, "set xtics (\"0\" 0,\"");
		for (i = 1; i < 10; i++) {
			fprintf_tm(gnudata, i * w, TME);
			fprintf(gnudata, "\" "LLFMT".0,\"", i * w);
		}
		fprintf_tm(gnudata, i * w, TME);
		fprintf(gnudata, "\" "LLFMT".0)\n", i * w);
	}

	fprintf(gnudata, "set grid xtics\n");

	if (endrange > lastclktick)
		endrange = lastclktick;

	/* calculate the effective use of parallelism */
	totalticks = 0;
	for (i = 0; i < top; i++)
		totalticks += lastclk[rows[i]];

	h = 10; /* unit height of bars */
	fprintf(gnudata, "set ytics (");
	for (i = 0; i < top; i++)
		fprintf(gnudata, "\"%d\" %d%c", rows[i], i * 2 * h + h / 2, (i < top - 1 ? ',' : ' '));
	fprintf(gnudata, ")\n");

	/* mark duration of each thread */
	for (i = 0; i < top; i++)
		fprintf(gnudata, "set object %d rectangle from %d, %d to "LLFMT".0, %d\n",
				object++, 0, i * 2 * h + h/3, lastclk[rows[i]], i * 2 * h + h - h/3);

	/* fill the duration of each instruction encountered that fit our range constraint */
	for (i = 0; i < topbox; i++)
	if ( box[i].clkend)
		switch (box[i].state ) {
		default:
			if (debug)
				dumpbox(i);
			fprintf(gnudata, "set object %d rectangle from "LLFMT".0, %d to "LLFMT".0, %d fillcolor rgb \"%s\" fillstyle solid 1.0\n",
					object++, box[i].clkstart, box[i].row * 2 * h, box[i].clkend, box[i].row * 2 * h + h, colors[box[i].color].col);
			break;
		case PING:
			break;
		case WAIT:
			fprintf(gnudata, "set object %d rectangle from "LLFMT".0, %d to %f,%f front fillcolor rgb \"red\" fillstyle solid 1.0\n",
					object++, box[i].clkstart, box[i].row * 2 * h+h-h/3, box[i].clkstart + w /50.0, box[i].row *2 *h + 1.3 * h);
		}


	fprintf(gnudata, "plot 0 notitle with lines\n");
	fprintf(gnudata, "unset for[i=%d:%d] object i\n", prevobject, object - 1);
	prevobject = object - 1;
	showcolormap(0, 0);
	fprintf(gnudata, "unset multiplot\n");
	keepdata(filename);
	(void) fclose(gnudata);
	gnudata = 0;

	// show follow up action only once
	if (atlas && atlaspage == atlas-1){
		fprintf(stderr, "Created tomogram atlas\n");
		for( i = 0; i<= atlas;  i++)
			fprintf(stderr, "gnuplot atlas_%02d.gpl\n",i);
		fprintf(stderr, "gs -dNOPAUSE -sDEVICE=pdfwrite -sOUTPUTFILE=atlas.pdf -dBATCH atlas_??.pdf\n");
		exit(0);
	} else
	if (!atlas && figures++ == 0) {
		fprintf(stderr, "Created tomogram '%s'\n", buf);
			fprintf(stderr, "Run: 'gnuplot %s.gpl' to create the '%s.pdf' file\n", buf, filename);
		if (tracefile == 0) {
			fprintf(stderr, "The memory map is stored in '%s.dat'\n", filename);
			fprintf(stderr, "The trace is saved in '%s.trace' for use with --trace option\n", filename);
		}
		exit(0);
	}
}

/* the main issue to deal with in the analyse is
 * that the tomograph start can appear while the
 * system is already processing. This leads to
 * receiving 'done' events without matching 'start'
 *
 * A secondary issue is to properly count the functions
 * being monitored. 
 */

static void 
update(int state, int thread, lng clkticks, lng ticks, lng memory, lng footprint, lng reads, lng writes, char *fcn, char *stmt)
{
	int idx;
	Box b;
	char *s;

	/* ignore the flow of control statements 'function' and 'end' */
	if (fcn && strncmp(fcn, "end ", 4) == 0) {
		return;
	}
	if (starttime == 0) {
		/* ignore all instructions up to the first function call, unless input comes from a file */
		if ( inputfile == NULL && (state >= PING || fcn == 0 || strncmp(fcn, "function", 8) )){
			return;
		}
		if (debug)
			fprintf(stderr, "Start capturing updates \n");
		assert(clkticks >= 0);
		starttime = clkticks;
	}

	/* monitor top level function brackets */
	if (state == START && fcn && strncmp(fcn, "function", 8) == 0 ){
		capturing++;
		starttime = clkticks;
		if ( currentfunction == 0) 
			currentfunction = strdup(fcn+9);
		if (debug)
			fprintf(stderr, "Enter function %s capture %d\n", currentfunction, capturing);
		return;
	}
	if (state == DONE && fcn && strncmp(fcn, "function", 8) == 0 ){
		if ( currentfunction  && strcmp(currentfunction, fcn+9) == 0){
			capturing--;
			if (debug)
				fprintf(stderr, "Leave function %s capture %d\n", currentfunction, capturing);
			free(currentfunction);
			currentfunction = 0;
		} else return;

		if ( batch -- > 1)  return;

		if (atlas == atlaspage){
			deactivateBeat();
			createTomogram();
			totalclkticks = 0; /* number of clock ticks reported */
			totalexecticks = 0; /* number of ticks reported for processing */
			if (fcn && title == 0)
				title = strdup(fcn + 9);
			return;
		}
		// create a new atlas page
		createTomogram();
		totalclkticks = 0; /* number of clock ticks reported */
		totalexecticks = 0; /* number of ticks reported for processing */
		if (fcn )
			title = strdup(fcn + 9);
		resetTomograph();
		return;
	}

	if (state == DONE && strncmp(fcn, "profiler.tomograph", 18) == 0) {
		if (debug)
			fprintf(stderr, "Profiler.tomograph ends  %d\n", batch);
		if (atlas == atlaspage){
			deactivateBeat();
			createTomogram();
			totalclkticks = 0; /* number of clock ticks reported */
			totalexecticks = 0; /* number of ticks reported for processing */
			return;
		} 
		createTomogram();
		totalclkticks = 0; /* number of clock ticks reported */
		totalexecticks = 0; /* number of ticks reported for processing */
		if (fcn )
			title = strdup(fcn + 9);
		resetTomograph();
		return;
	}

	assert(clkticks >= 0);
	clkticks -= starttime;
	if (clkticks < 0) {
		/* HACK: *TRY TO* compensate for the fact that the MAL
		 * profiler chops-off day information, and assume that
		 * clkticks is < starttime because the tomograph run
		 * crossed a day boundary (midnight);
		 * we simply add 1 day (24 hours) worth of microseconds.
		 * NOTE: this surely does NOT work correctly if the
		 * tomograph run takes 24 hours or more ...
		 */
		clkticks += US_DD;
	}

	/* handle a ping event, keep the current instruction in focus */
	if (state >= PING && capturing) {
		idx = threads[thread];
		b = box[idx];
		box[idx].state = state;
		box[idx].thread = thread;
		lastclk[thread] = clkticks;
		box[idx].clkend = box[idx].clkstart = clkticks;
		box[idx].memend = box[idx].memstart = memory;
		box[idx].footstart = box[idx].footend = footprint;
		box[idx].reads = reads;
		box[idx].writes = writes;
		s = strchr(stmt, (int) ']');
		if (s)
			*s = 0;
		box[idx].stmt = stmt;
		box[idx].fcn = state == PING? "profiler.ping":"profiler.wait";
		threads[thread] = ++topbox;
		idx = threads[thread];
		box[idx] = b;
		if (reads > maxio)
			maxio = reads;
		if (writes > maxio)
			maxio = writes;
		return;
	}
	idx = threads[thread];
	/* start of instruction box */
	if (state == START && thread < MAXTHREADS) {
		if (debug)
			fprintf(stderr, "Start box %s thread %d idx %d box %d\n", currentfunction, thread,idx,topbox);
		box[idx].state = state;
		box[idx].thread = thread;
		box[idx].clkstart = clkticks;
		box[idx].memstart = memory;
		box[idx].footstart = footprint;
		box[idx].stmt = stmt;
		box[idx].fcn = fcn ? strdup(fcn) : "";
	}
	/* end the instruction box */
	if (state == DONE && thread < MAXTHREADS && fcn && box[idx].fcn && strcmp(fcn, box[idx].fcn) == 0) {
		if (debug)
			fprintf(stderr, "End box %s thread %d idx %d box %d\n", currentfunction, thread,idx,topbox);
		lastclk[thread] = clkticks;
		box[idx].clkend = clkticks;
		box[idx].memend = memory;
		box[idx].footend = footprint;
		box[idx].ticks = ticks;
		box[idx].state = ACTION;
		box[idx].reads = reads;
		box[idx].writes = writes;
		/* focus on part of the time frame */
		if (endrange) {
			if (box[idx].clkend < startrange || box[idx].clkstart >endrange)
				return;
			if (box[idx].clkstart < startrange)
				box[idx].clkstart = startrange;
			if (box[idx].clkend > endrange)
				box[idx].clkend = endrange;
		}
		threads[thread] = ++topbox;
		lastclktick = box[idx].clkend + starttime;
		totalclkticks += box[idx].clkend - box[idx].clkstart;
		totalexecticks += box[idx].ticks - box[idx].ticks;
	}
	if (topbox == MAXBOX) {
		fprintf(stderr, "Out of space for trace");
		deactivateBeat();
		createTomogram();
		exit(0);
	}
}

/*
 * Beware the UDP protocol may cause some loss
 * Incomplete records from previous runs may also appear
 */
static int parser(char *row)
{
#ifdef HAVE_STRPTIME
	char *c, *cc;
	struct tm stm;
	lng clkticks = 0;
	int thread = 0;
	lng ticks = 0;
	lng memory = 0; /* in MB*/
	lng footprint = 0; /* in MB*/
	char *fcn = 0, *stmt = 0;
	int state = 0;
	lng reads= 0, writes= 0;

	/* check basic validaty first */
	if (row[0] != '[')
		return -1;
	if ( (cc= strrchr(row,']')) == 0 || *(cc+1) !=0)
		return -1;

	c = strchr(row, (int) '"');
	if (c == 0)
		return -2;
	if (strncmp(c + 1, "start", 5) == 0) {
		state = START;
		c += 6;
	} else if (strncmp(c + 1, "done", 4) == 0) {
		state = DONE;
		c += 5;
	} else if (strncmp(c + 1, "ping", 4) == 0) {
		state = PING;
		c += 5;
	} else if (strncmp(c + 1, "wait", 4) == 0) {
		state = WAIT;
		c += 5;
	} else {
		state = 0;
		c = strchr(c + 1, (int) '"');
		if ( c == 0)
			return -2;
	}

	c = strchr(c + 1, (int) '"');
	if (c) {
		/* convert time to epoch in seconds*/
		memset(&stm, 0, sizeof(struct tm));
		c = strptime(c + 1, "%H:%M:%S", &stm);
		clkticks = (((lng) (stm.tm_hour * 60) + stm.tm_min) * 60 + stm.tm_sec) * 1000000;
		if (c == 0)
			return -11;
		if (*c == '.') {
			lng usec;
			/* microseconds */
			usec = strtoll(c + 1, NULL, 10);
			assert(usec >= 0 && usec < 1000000);
			clkticks += usec;
		}
		c = strchr(c + 1, (int) '"');
		if (clkticks < 0) {
			fprintf(stderr, "parser: read negative value "LLFMT" from\n'%s'\n", clkticks, row);
		}
	} else
		return -3;

	c = strchr(c + 1, (int) ',');
	if (c == 0)
		return -4;
	thread = atoi(c + 1);
	c = strchr(c + 1, (int) ',');
	if (c == 0)
		return -5;
	ticks = strtoll(c + 1, NULL, 10);
	c = strchr(c + 1, (int) ',');
	if (c == 0)
		return -6;
	memory = strtoll(c + 1, NULL, 10);
	c = strchr(c + 1, (int) ',');
	if (c == 0)
		return -8;

#ifdef FOOTPRINT
	footprint = strtoll(c + 1, NULL, 10);
	if (debug && state < PING)
		fprintf(stderr, "%s\n", row);
	c = strchr(c + 1, (int) ',');
	if (c == 0 && state >= PING) 
		goto wrapup;
#endif

	reads = strtoll(c + 1, NULL, 10);
	c = strchr(c + 1, (int) ',');
	if (c == 0)
		return -8;
	writes = strtoll(c + 1, NULL, 10);

	/* check basic validity */
	if ( (cc= strrchr(row,']')) == 0 || *(cc+1) !=0)
		return -1;
	c = strchr(c + 1, (int) ',');
	if (c == 0)
		return -9;
	c++;
	fcn = c;
	stmt = strdup(fcn);

	c = strstr(c + 1, ":=");
	if (c) {
		fcn = c + 2;
		/* find genuine function calls */
		while (isspace((int) *fcn) && *fcn)
			fcn++;
		if (strchr(fcn, (int) '.') == 0)
			return -10;
	} else {
		fcn = strchr(fcn, (int) '"');
		if (fcn) {
			fcn++;
			*strchr(fcn, (int) '"') = 0;
		}
	}

	if (fcn && strchr(fcn, (int) '('))
		*strchr(fcn, (int) '(') = 0;

#ifdef FOOTPRINT
wrapup:
#endif
	update(state, thread, clkticks, ticks, memory, footprint, reads, writes, fcn, stmt);
#else
	(void) row;
#endif
	return 0;
}

static void processFile(char *fname)
{
	size_t len;
	ssize_t n;
	int i;
	char buf[BUFSIZ + 1];
	char *e, *response;
	stream *s;
	
	s = open_rstream(fname);
	if ( s == NULL || mnstr_errnr(s)){
		fprintf(stderr,"ERROR Can not access '%s'\n",fname);
		return;
	}
	len = 0;
	while (s && (n = mnstr_read(s, buf, 1, BUFSIZ - len)) > 0) {
		buf[n] = 0;
		response = buf;
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			i = parser(response);
			if (debug )
				fprintf(stderr, "ERROR %d:%s\n", i, response);
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
	mnstr_close(s);
}

static void
format_result(Mapi mid, MapiHdl hdl)
{
	char *line;
	(void) mid;

	do {
		/* handle errors first */
		if (mapi_result_error(hdl) != NULL) {
			mapi_explain_result(hdl, stderr);
			/* don't need to print something like '0
			 * tuples' if we got an error */
			break;
		}
		switch (mapi_get_querytype(hdl)) {
		case Q_BLOCK:
		case Q_PARSE:
			/* should never see these */
			continue;
		case Q_UPDATE:
			fprintf(stderr, "[ " LLFMT "\t]\n", mapi_rows_affected(hdl));
			continue;
		case Q_SCHEMA:
		case Q_TRANS:
		case Q_PREPARE:
		case Q_TABLE:
			break;
		default:
			while ((line = mapi_fetch_line(hdl)) != 0) {
				if (*line == '=')
					line++;
				fprintf(stderr, "%s\n", line);
			}
		}
	} while (mapi_next_result(hdl) == 1);
}

static int
doRequest(Mapi mid, const char *buf)
{
	MapiHdl hdl;

	if (debug)
		fprintf(stderr, "Sent:%s\n", buf);
	if ((hdl = mapi_query(mid, buf)) == NULL) {
		mapi_explain(mid, stderr);
		return 1;
	}

	format_result(mid, hdl);

	return 0;
}

#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
static DWORD WINAPI
#else
static void *
#endif
doProfile(void *d)
{
	wthread *wthr = (wthread *) d;
	int i;
	size_t len;
	size_t a;
	ssize_t n;
	char *response, *x;
	char buf[BUFSIZ + 1];
	char *e;
	char *mod, *fcn;
	char *host = NULL;
	int portnr;
	char id[10];
	Mapi dbhsql = NULL;
	MapiHdl hdlsql = NULL;

	/* set up the SQL session */
	if (sqlstatement) {
		id[0] = '\0';
		if (wthr->uri)
			dbhsql = mapi_mapiuri(wthr->uri, wthr->user, wthr->pass, "sql");
		else
			dbhsql = mapi_mapi(wthr->host, wthr->port, wthr->user, wthr->pass, "sql", wthr->dbname);
		if (dbhsql == NULL || mapi_error(dbhsql))
			die(dbhsql, hdlsql);
		mapi_reconnect(dbhsql);
		if (mapi_error(dbhsql))
			die(dbhsql, hdlsql);
	}

	/* set up the profiler */
	id[0] = '\0';
	if (wthr->uri)
		wthr->dbh = mapi_mapiuri(wthr->uri, wthr->user, wthr->pass, "mal");
	else
		wthr->dbh = mapi_mapi(wthr->host, wthr->port, wthr->user, wthr->pass, "mal", wthr->dbname);
	if (wthr->dbh == NULL || mapi_error(wthr->dbh))
		die(wthr->dbh, wthr->hdl);
	mapi_reconnect(wthr->dbh);
	if (mapi_error(wthr->dbh))
		die(wthr->dbh, wthr->hdl);
	host = strdup(mapi_get_host(wthr->dbh));
	if (*host == '/') {
		fprintf(stderr, "!! UNIX domain socket not supported\n");
		goto stop_disconnect;
	}
	if (wthr->tid > 0) {
		snprintf(id, 10, "[%d] ", wthr->tid);
#ifdef _DEBUG_TOMOGRAPH_
		printf("-- connection with server %s is %s\n", wthr->uri ? wthr->uri : host, id);
#endif
	} else {
#ifdef _DEBUG_TOMOGRAPH_
		printf("-- connection with server %s\n", wthr->uri ? wthr->uri : host);
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
	activateBeat();
	doQ("profiler.start();");
	fflush(NULL);

	for (i = 0; i < MAXTHREADS; i++)
		threads[i] = topbox++;

	/* sent single query */
	if (sqlstatement) {
		doRequest(dbhsql, sqlstatement);
	}
	len = 0;
	while (wthr->s && (n = mnstr_read(wthr->s, buf, 1, BUFSIZ - len)) > 0) {
		buf[n] = 0;
		response = buf;
		while ((e = strchr(response, '\n')) != NULL) {
			*e = 0;
			/* TOMOGRAPH EXTENSIONS */
			i = parser(response);
			if (debug )
				fprintf(stderr, "PARSE %d:%s\n", i, response);
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
	fflush(NULL);

stop_cleanup:
	doQ("profiler.setNone();");
	doQ("profiler.stop();");
	doQ("profiler.closeStream();");
stop_disconnect:
	if (wthr->dbh) {
		mapi_disconnect(wthr->dbh);
		mapi_destroy(wthr->dbh);
	}

	printf("-- %sconnection with server %s closed\n", id, wthr->uri ? wthr->uri : host);

	free(host);

	return 0;
}

int
main(int argc, char **argv)
{
	int a = 1;
	int k = 0;
	char *host = "localhost";
	int portnr = 0;
	char *dbname = NULL;
	char *uri = NULL;
	char *user = NULL;
	char *password = NULL;
	struct stat statb;
	char *othermap= NULL;

	wthread *walk;

	static struct option long_options[18] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "port", 1, 0, 'p' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "title", 1, 0, 'T' },
		{ "input", 1, 0, 'i' },
		{ "trace", 1, 0, 't' },
		{ "range", 1, 0, 'r' },
		{ "output", 1, 0, 'o' },
		{ "debug", 0, 0, 'D' },
		{ "beat", 1, 0, 'b' },
		{ "batch", 1, 0, 'B' },
		{ "atlas", 1, 0, 'A' },
		{ "sql", 1, 0, 's' },
		{ "colormap", 1, 0, 'm' },
		{ "adaptive", 0, 0, 'a' },
		{ 0, 0, 0, 0 }
	};

	/* parse config file first, command line options override */
	parse_dotmonetdb(&user, &password, NULL, NULL, NULL, NULL);


	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:p:h:?T:i:t:r:o:Db:B:A:s:m:a",
				long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'a':
			fixedmap = 0;
			break;
		case 'A':
			atlas = atoi(optarg ? optarg : "1");
			filename = "atlas_00";
			break;
		case 'B':
			batchsize = batch = atoi(optarg ? optarg : "1");
			break;
		case 'b':
			beat = atoi(optarg ? optarg : "50");
			break;
		case 'D':
			debug = 1;
			break;
		case 'd':
			dbname = optarg;
			break;
		case 'i':
			inputfile = optarg;
			break;
		case 'u':
			if (user)
				free(user);
			user = optarg;
			/* force password prompt */
			if (password)
				free(password);
			password = NULL;
			break;
		case 'm':
			if ( optarg){
				othermap = optarg;
				colormap = 0;
			} else colormap =1;
			break;
		case 'P':
			if (password)
				free(password);
			password = optarg;
			break;
		case 'p':
			if (optarg)
				portnr = atoi(optarg);
			break;
		case 'h':
			host = optarg;
			break;
		case 'T':
			title = optarg;
			break;
		case 't':
			if (optarg == 0)
				tracefile = strdup(filename);
			else
				tracefile = optarg;
			break;
		case 'o':
			filename = optarg;
			break;
		case 'r':
		{
			char *s;
			if (optarg == 0)
				break;
			startrange = strtoll(optarg, NULL, 10);
			if (strchr(optarg, (int) '-'))
				endrange = strtoll(strchr(optarg, (int) '-') + 1, NULL, 10);
			else
				endrange = startrange + 1000;
			s = strchr(optarg, (int) 'm');
			if (s && *(s + 1) == 's') {
				startrange *= 1000;
				endrange *= 1000;
			} else { /* seconds */
				s = strchr(optarg, (int) 's');
				startrange *= 1000000;
				endrange *= 1000000;
			}
			break;
		}
		case 's':
			sqlstatement = optarg;
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
	if ( othermap){
		FILE *map ;
		map = fopen(othermap,"r");
		if ( map == NULL){
			printf("Could not open color map %s\n",othermap);
			exit(-1);
		}
		fixedmap =1;
		initcolors(map);
		(void) fclose(map);
	} else
		initcolors(0);

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

	a = optind;
	if (argc > 1 && a < argc && argv[a][0] == '+') {
		k = setCounter(argv[a] + 1);
		a++;
	} else
		k = setCounter(COUNTERSDEFAULT);

	if (inputfile){
		processFile(inputfile);
		createTomogram();
		exit(0);
	}
	if (tracefile) {
		/* reload existing tomogram */
		scandata(tracefile);
		createTomogram();
		exit(0);
	}
	if (colormap) {
		fixedmap = 0;
		showcolormap(filename, 1);
		printf("Color map file '%s.gpl' generated\n", filename);
		exit(0);
	}

	/* DOT needs function id and PC to correlate */
	if (profileCounter[32].status) {
		profileCounter[3].status = k++;
		profileCounter[4].status = k;
	}

	if (user == NULL)
		user = simple_prompt("user", BUFSIZ, 1, prompt_getlogin());
	if (password == NULL)
		password = simple_prompt("password", BUFSIZ, 0, NULL);

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

	/* nothing to redirect, so a single db to try */
	walk = thds = malloc(sizeof(wthread));
	walk->uri = uri;
	walk->host = host;
	walk->port = portnr;
	walk->dbname = dbname;
	walk->user = user;
	walk->pass = password;
	walk->argc = argc - a;
	walk->argv = &argv[a];
	walk->tid = 0;
	walk->s = NULL;
	walk->next = NULL;
	walk->dbh = NULL;
	walk->hdl = NULL;
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
	free(user);
	free(password);
	return 0;
}
