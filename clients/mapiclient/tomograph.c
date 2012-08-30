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
static char *inputfile=0;
static long startrange=0, endrange= 0;
static char *title =0;
static int cores = 8;
static int listing = 0;
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
	fprintf(stderr, "  -i | --input=<file name of previous run >\n");
	fprintf(stderr, "  -r | --range=<starttime>-<endtime>[ms,s] \n");
	fprintf(stderr, "  -o | --output=<file name prefix >\n");
	fprintf(stderr, "  -c | --cores=<number> of the target machine\n");
	fprintf(stderr, "  -m | --colormap produces colormap \n");
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
/* see http://www.uni-hamburg.de/Wiss/FB/15/Sustainability/schneider/gnuplot/colors.htm */
struct {
	char *name;
	char *hsv;
	int red,green,blue;
} dictionary[] ={
	{"aliceblue","#F0F8FF",240,248,255},
	{"antiquewhite","#FAEBD7",250,235,215},
	{"aqua","#00FFFF",0,255,255},
	{"aquamarine","#7FFFD4",127,255,212},
	{"azure","#F0FFFF",240,255,255},
	{"beige","#F5F5DC",245,245,220},
	{"bisque","#FFE4C4",255,228,196},
	{"black","#000000",0,0,0},
	{"blanchedalmond","#FFEBCD",255,235,205},
	{"blue","#0000FF",0, 0,255},
	{"blueviolet","#8A2BE2",138, 43,226},
	{"brown","#A52A2A",165, 42, 42},
	{"burlywood","#DEB887",222,184,135},
	{"cadetblue","#5F9EA0",95,158,160},
	{"chartreuse","#7FFF00",127,255, 0},
	{"chocolate","#D2691E",210,105, 30},
	{"coral","#FF7F50",255,127, 80},
	{"cornflowerblue","#6495ED",100,149,237},
	{"cornsilk","#FFF8DC",255,248,220},
	{"crimson","#DC143C",220,20,60},
	{"cyan","#00FFFF",0,255,255},
	{"darkblue","#00008B",0,0,139},
	{"darkcyan","#008B8B",0,139,139},
	{"darkgoldenrod","#B8860B",184,134, 11},
	{"darkgray","#A9A9A9",169,169,169},
	{"darkgreen","#006400",0,100, 0},
	{"darkkhaki","#BDB76B",189,183,107},
	{"darkmagenta","#8B008B",139, 0,139},
	{"darkolivegreen","#556B2F",85,107, 47},
	{"darkorange","#FF8C00",255,140, 0},
	{"darkorchid","#9932CC",153, 50,204},
	{"darkred","#8B0000",139, 0, 0},
	{"darksalmon","#E9967A",233,150,122},
	{"darkseagreen","#8FBC8F",143,188,143},
	{"darkslateblue","#483D8B",72, 61,139},
	{"darkslategray","#2F4F4F",47, 79, 79},
	{"darkturquoise","#00CED1",0,206,209},
	{"darkviolet","#9400D3",148, 0,211},
	{"deeppink","#FF1493",255, 20,147},
	{"deepskyblue","#00BFFF",0,191,255},
	{"dimgray","#696969",105,105,105},
	{"dodgerblue","#1E90FF",30,144,255},
	{"firebrick","#B22222",178, 34, 34},
	{"floralwhite","#FFFAF0",255,250,240},
	{"forestgreen","#228B22",34,139, 34},
	{"fuchsia","#FF00FF",255,0,255},
	{"gainsboro","#DCDCDC",220,220,220},
	{"ghostwhite","#F8F8FF",248,248,255},
	{"gold","#FFD700",255,215, 0},
	{"goldenrod","#DAA520",218,165, 32},
	{"gray","#7F7F7F",127,127,127},
	{"green","#008000",0,128,0},
	{"greenyellow","#ADFF2F",173,255, 47},
	{"honeydew","#F0FFF0",240,255,240},
	{"hotpink","#FF69B4",255,105,180},
	{"indianred","#CD5C5C",205, 92, 92},
	{"indigo","#4B0082",75,0,130},
	{"ivory","#FFFFF0",255,255,240},
	{"khaki","#F0E68C",240,230,140},
	{"lavender","#E6E6FA",230,230,250},
	{"lavenderblush","#FFF0F5",255,240,245},
	{"lawngreen","#7CFC00",124,252, 0},
	{"lemonchiffon","#FFFACD",255,250,205},
	{"lightblue","#ADD8E6",173,216,230},
	{"lightcoral","#F08080",240,128,128},
	{"lightcyan","#E0FFFF",224,255,255},
	{"lightgoldenrodyellow","#FAFAD2",250,250,210},
	{"lightgreen","#90EE90",144,238,144},
	{"lightgrey","#D3D3D3",211,211,211},
	{"lightpink","#FFB6C1",255,182,193},
	{"lightsalmon","#FFA07A",255,160,122},
	{"lightseagreen","#20B2AA",32,178,170},
	{"lightskyblue","#87CEFA",135,206,250},
	{"lightslategray","#778899",119,136,153},
	{"lightsteelblue","#B0C4DE",176,196,222},
	{"lightyellow","#FFFFE0",255,255,224},
	{"lime","#00FF00",0,255,0},
	{"limegreen","#32CD32",50,205, 50},
	{"linen","#FAF0E6",250,240,230},
	{"magenta","#FF00FF",255, 0,255},
	{"maroon","#800000",128,0,0},
	{"mediumaquamarine","#66CDAA",102,205,170},
	{"mediumblue","#0000CD",0,0,205},
	{"mediumorchid","#BA55D3",186, 85,211},
	{"mediumpurple","#9370DB",147,112,219},
	{"mediumseagreen","#3CB371",60,179,113},
	{"mediumslateblue","#7B68EE",123,104,238},
	{"mediumspringgreen","#00FA9A",0,250,154},
	{"mediumturquoise","#48D1CC",72,209,204},
	{"mediumvioletred","#C71585",199, 21,133},
	{"midnightblue","#191970",25, 25,112},
	{"mintcream","#F5FFFA",245,255,250},
	{"mistyrose","#FFE4E1",255,228,225},
	{"moccasin","#FFE4B5",255,228,181},
	{"navajowhite","#FFDEAD",255,222,173},
	{"navy","#000080",0, 0,128},
	{"navyblue","#9FAFDF",159,175,223},
	{"oldlace","#FDF5E6",253,245,230},
	{"olive","#808000",128,128,0},
	{"olivedrab","#6B8E23",107,142, 35},
	{"orange","#FFA500",255,165, 0},
	{"orangered","#FF4500",255, 69, 0},
	{"orchid","#DA70D6",218,112,214},
	{"palegoldenrod","#EEE8AA",238,232,170},
	{"palegreen","#98FB98",152,251,152},
	{"paleturquoise","#AFEEEE",175,238,238},
	{"palevioletred","#DB7093",219,112,147},
	{"papayawhip","#FFEFD5",255,239,213},
	{"peachpuff","#FFDAB9",255,218,185},
	{"peru","#CD853F",205,133, 63},
	{"pink","#FFC0CB",255,192,203},
	{"plum","#DDA0DD",221,160,221},
	{"powderblue","#B0E0E6",176,224,230},
	{"purple","#800080",128,0,128},
	{"red","#FF0000",255, 0, 0},
	{"rosybrown","#BC8F8F",188,143,143},
	{"royalblue","#4169E1",65,105,225},
	{"saddlebrown","#8B4513",139,69,19},
	{"salmon ‡","#FA8072",250,128,114},
	{"sandybrown","#F4A460",244,164, 96},
	{"seagreen","#2E8B57",46,139, 87},
	{"seashell","#FFF5EE",255,245,238},
	{"sienna","#A0522D",160, 82, 45},
	{"silver","#C0C0C0",192,192,192},
	{"skyblue","#87CEEB",135,206,235},
	{"slateblue","#6A5ACD",106, 90,205},
	{"slategray","#708090",112,128,144},
	{"snow","#FFFAFA",255,250,250},
	{"springgreen","#00FF7F",0,255,127},
	{"steelblue","#4682B4",70,130,180},
	{"tan","#D2B48C",210,180,140},
	{"teal","#008080",0,128,128},
	{"thistle","#D8BFD8",216,191,216},
	{"tomato","#FF6347",255, 99, 71},
	{"turquoise","#40E0D0",64,224,208},
	{"violet","#EE82EE",238,130,238},
	{"wheat","#F5DEB3",245,222,179},
	{"white","#FFFFFF",255,255,255},
	{"whitesmoke","#F5F5F5",245,245,245},
	{"yellow","#FFFF00",255,255, 0},
	{"yellowgreen","#9ACD32",139,205,50},
	{ 0,0,0,0,0}
};

static char *getHSV(char *name)
{
	int i;
	for (i=0; dictionary[i].name; i++)
	if ( strcmp(dictionary[i].name, name) == 0)
		return dictionary[i].hsv;
	return 0;
}


/* The initial dictionary is geared towars TPCH-use */
struct COLOR{
	int freq;
	char *mod, *fcn, *col;
} colors[] =
{
	{0,"mal","idle","white"},
	{0,"mal","*","white"},

	{0,"aggr","count","yellowgreen"},
	{0,"aggr","sum","springgreen"},
	{0,"aggr","*","green"},

	{0,"algebra","leftjoin","powderblue"},
	{0,"algebra","join","navy"},
	{0,"algebra","semijoin","lightskyblue"},
	{0,"algebra","kdifference","gray"},
	{0,"algebra","kunion","skyblue"},
	{0,"algebra","slice","darkblue"},
	{0,"algebra","sortTail","cyan"},
	{0,"algebra","markT","blue"},
	{0,"algebra","selectNotNil","mediumblue"},
	{0,"algebra","thetauselect","aqua"},
	{0,"algebra","uselect","cornflowerblue"},
	{0,"algebra","*","azure"},

	{0,"bat","mirror","sandybrown"},
	{0,"bat","reverse","sandybrown"},
	{0,"bat","*","orange"},

	{0,"batcalc","-","moccasin"},
	{0,"batcalc","*","moccasin"},
	{0,"batcalc","+","moccasin"},
	{0,"batcalc","dbl","papayawhip"},
	{0,"batcalc","str","papayawhip"},
	{0,"batcalc","*","lightyellow"},

	{0,"calc","lng","lightgreen"},
	{0,"calc","str","lightgreen"},
	{0,"calc","*","palegreen"},

	{0,"group","multicolumns","orchid"},
	{0,"group","refine","darkorchid"},
	{0,"group","*","mediumorchid"},

	{0,"language","dataflow","lightslategray"},
	{0,"language","*","darkgray"},

	{0,"mat","pack","greenyellow"},
	{0,"mat","*","green"},

	{0,"mtime","*","lightgreen"},

	{0,"pcre","like_filter","gray"},
	{0,"pcre","*","burlywood"},

	//{0,"pqueue","topn_max","lightcoral"},
	//{0,"pqueue","utopn_max","lightcoral"},
	//{0,"pqueue","utopn_min","lightcoral"},
	{0,"pqueue","*","lightcoral"},

	{0,"io","stdout","gray"},
	{0,"io","*","gray"},

	//{0,"sql","bind","thistle"},
	//{0,"sql","bind_dbat","thistle"},
	//{0,"sql","mvc","thistle"},
	//{0,"sql","resultSet ","thistle"},
	{0,"sql","*","thistle"},

	{0,"*","*","lavender"},
	{0,0,0,0}
};

int object =1;

static void initcolors(void)
{
	int i;
	char *c;
	for ( i =0; colors[i].col; i++){
		c = getHSV(colors[i].col);
		if ( c )
			colors[i].col = c;
		else
			printf("color '%s' not found\n",colors[i].col);
	}
}

/* produce memory thread trace */
static void showmemory(char *filename)
{
	FILE *f;
	char buf[BUFSIZ],buf2[BUFSIZ];
	int i;
	long max = 0;
	double w = 1800.0/(lastclktick - starttime -startrange);

	if ( filename) {
		snprintf(buf,BUFSIZ,"%s_memory.dat",filename);
		f = fopen(buf,"w");
	} else {
		snprintf(buf,BUFSIZ,"tomograph_memory.dat");
		f = fopen(buf,"w");
	}
	assert(f);


	for ( i = 0; i < topbox; i++)
	if ( box[i].clkend ){
		fprintf(f,"%ld %3.2f\n", (long)(box[i].clkstart * w), (box[i].memstart/1024.0));
		fprintf(f,"%ld %3.2f\n", (long)(box[i].clkend * w), (box[i].memend/1024.0));
		if ( box[i].memstart > max )
			max = box[i].memstart;
		if ( box[i].memend > max )
			max = box[i].memend;
	}
	(void)fclose(f);

	if ( filename) {
		snprintf(buf2,BUFSIZ,"%s_memory.gpl",filename);
		f = fopen(buf2,"w");
		assert(f);
		fprintf(f,"set terminal pdfcairo enhanced color solid\n");
		fprintf(f,"set output \"%s_memory.pdf\"\n",filename);
		fprintf(f,"set size 1, 0.4\n");
		fprintf(f,"set xrange [0:1800]\n");
		fprintf(f,"set title \"%s\"\n", title? title: ":tomogram");
		fprintf(f,"unset xtics\n");
	} else {
		f = gnudata;
		fprintf(f,"\nset tmarg 0\n");
		fprintf(f,"set bmarg 0\n");
		fprintf(f,"set lmarg 7\n");
		fprintf(f,"set rmarg 3\n");
		fprintf(f,"set size 1,0.1\n");
		fprintf(f,"set origin 0.0,0.8\n");
		fprintf(f,"set xrange [0:1800]\n");
		fprintf(f,"set ylabel \"memory\"\n");
		fprintf(f,"unset xtics\n");
	}
	fprintf(f,"set ytics (\"%2d\" %3.2f, \"0\" 0)\n", (int)(max /1024), max/1024.0);
	fprintf(f,"plot \"%s\" using 1:2 notitle with dots\n",buf);
}

/* produce a legenda image for the color map */
static void showmap(char *filename, int all)
{
	FILE *f;
	char buf[BUFSIZ];
	int i, k = 0;
	int w = 600;
	int h = 500;

	if( filename) {
		snprintf(buf,BUFSIZ,"%s_map.gpl",filename);
		f = fopen(buf,"w");
	} else f = gnudata;
	assert(f);

	if ( filename) {
		fprintf(f,"set terminal pdfcairo enhanced color solid\n");
		fprintf(f,"set output \"%s_map.pdf\"\n",filename);
		fprintf(f,"set xrange [0:1800]\n");
		fprintf(f,"set yrange [0:600]\n");
		fprintf(f,"unset xtics\n");
		fprintf(f,"unset ytics\n");
		fprintf(f,"unset colorbox\n");
		fprintf(f,"unset border\n");
		fprintf(f,"unset title\n");
		fprintf(f,"unset ylabel\n");
		fprintf(f,"unset xlabel\n");
	} else {
		f = gnudata;
		fprintf(f,"\nset tmarg 0\n");
		fprintf(f,"set bmarg 0\n");
		fprintf(f,"set lmarg 7\n");
		fprintf(f,"set rmarg 3\n");
		fprintf(f,"set size 1,0.3\n");
		fprintf(f,"set origin 0.0,0.0\n");
		fprintf(f,"set xrange [0:1800]\n");
		fprintf(f,"set yrange [0:600]\n");
		fprintf(f,"unset xtics\n");
		fprintf(f,"unset ytics\n");
		fprintf(f,"unset colorbox\n");
		fprintf(f,"unset border\n");
		fprintf(f,"unset title\n");
		fprintf(f,"unset ylabel\n");
		fprintf(f,"unset xlabel\n");
	}
	for ( i= 0; colors[i].col; i++)
	if ( colors[i].freq > 0 || all){
		fprintf(f,"set object %d rectangle from %d, %d to %d, %d fillcolor rgb \"%s\" fillstyle solid 0.6\n",
			object++, (k % 3) * w, h-40, (int)((k % 3) * w+ 0.15 * w), h-5, colors[i].col);
		fprintf(f,"set label %d \"%s.%s\" at %d,%d\n", object++, colors[i].mod, colors[i].fcn, 
			(int) ((k % 3) *  w  + 0.2 *w) , h-20);
		if ( k % 3 == 2)
			h-= 40;
		k++;
	}
	fprintf(f,"plot 0 notitle with lines\n");
	fprintf(f,"unset multiplot\n");
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
		
/* keep the data around for re-painting with different thresholds and filters*/
static void keepdata(char *filename)
{
	int i;
	FILE *f;
	char buf[BUFSIZ];

	if (inputfile) /* don't overwrite source */
		return;
	snprintf(buf,BUFSIZ,"%s.dat",filename);
	f = fopen(buf,"w");
	assert(f);

	if ( listing)
	for ( i = 0; i < topbox; i++)
	if ( box[i].clkend)
		printf("%3d\t%8ld\t%5ld\t%s\n", box[i].thread, box[i].clkstart, box[i].clkend-box[i].clkstart, box[i].fcn);
	for ( i = 0; i < topbox; i++)
	if ( box[i].clkend){
		fprintf(f,"%d\t%ld\t%ld\t%ld\n", box[i].thread, box[i].clkstart, box[i].clkend,box[i].ticks);
		fprintf(f,"%ld\t%ld\t%ld\n", box[i].ticks, box[i].memstart, box[i].memend);
		fprintf(f,"%s\n",box[i].stmt? box[i].stmt:"");
		fprintf(f,"%s\n",box[i].fcn? box[i].fcn:"");
	}
	(void)fclose(f);
	
}

static void scandata(char *filename)
{
	FILE *f;
	char buf[BUFSIZ];
	int i= 0;

	f = fopen(filename,"r");
	if ( f == 0){
		snprintf(buf,BUFSIZ,"%s.dat",filename);
		f = fopen(buf,"r");
		if ( f == NULL){
			printf("Could not open file '%s'\n",buf);
			exit(-1);
		}
	}
	starttime = 0;

	while(!feof(f)){
		(void) fscanf(f,"%d\t%ld\t%ld\t%ld\n", &box[i].thread, &box[i].clkstart, &box[i].clkend, &box[i].ticks);
		(void) fscanf(f,"%ld\t%ld\t%ld\n", &box[i].ticks, &box[i].memstart, &box[i].memend);
		(void) fgets(buf,BUFSIZ,f);
		box[i].stmt= strdup(buf);
		(void) fgets(buf,BUFSIZ,f);
		*(strchr(buf,(int)'\n'))=0;
		box[i].fcn= strdup(buf);
		if ( box[i].clkend - box[i].clkstart < threshold)
			continue;
		/* focus on part of the time frame */
		if ( endrange ){
			if (box[i].clkend < startrange || box[i].clkstart >endrange)
				continue;
			if ( box[i].clkstart < startrange)
				box[i].clkstart = startrange;
			if ( box[i].clkend > endrange)
				box[i].clkend = endrange;
		} 
		lastclktick= box[i].clkend;
		totalclkticks += box[i].clkend - box[i].clkstart;
		totalexecticks += box[i].ticks - box[i].ticks;
		i++;
		assert(i < MAXBOX);
	}
	topbox = i;
}
/* gnuplot defaults */
static int height = 160;

static void gnuplotheader(char *filename){
	fprintf(gnudata,"set terminal pdfcairo enhanced color solid size 8.3,11.7\n");
	fprintf(gnudata,"set output \"%s.pdf\"\n",filename);
	fprintf(gnudata,"set size 1,1\n");
	fprintf(gnudata,"set title \"%s\"\n", title? title: ":tomogram");
	fprintf(gnudata,"set multiplot\n");
}

static int figure = 0;
static void createTomogram(void)
{
	char buf[BUFSIZ];
	int rows[MAXTHREADS];
	int top= 0;
	int i,j;
	int h = height /(2 * cores);
	double w = (lastclktick-starttime)/10.0;
	int scale;
	char *scalename;

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
	*strchr(buf,(int) '.') = 0;
	gnuplotheader(buf);
	showmemory(0);

	fprintf(gnudata,"\nset tmarg 0\n");
	fprintf(gnudata,"set bmarg 3\n");
	fprintf(gnudata,"set lmarg 7\n");
	fprintf(gnudata,"set rmarg 3\n");
	fprintf(gnudata,"set size 1,0.5\n");
	fprintf(gnudata,"set origin 0.0,0.3\n");
	fprintf(gnudata,"set xrange [%ld:%ld]\n", startrange, lastclktick-starttime);
	fprintf(gnudata,"set yrange [0:%d]\n", height);
	fprintf(gnudata,"set ylabel \"threads\"\n");
	fprintf(gnudata,"set key right \n");
	fprintf(gnudata,"unset colorbox\n");
	fprintf(gnudata,"unset title\n");

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
	if ( w > 1000000){
		scale = 1000000;
		scalename = "";
	} else
	if ( w > 1000){
		scale= 1000;
		scalename = "milli";
	} else {
		scale =1;
		scalename = "micro";
	}

	fprintf(gnudata,"set xtics (");
	for( i =0; i< 10; i++)
		fprintf(gnudata,"\"%d\" %d,", (int)(i * w /scale), (int)(i * w));
	fprintf(gnudata,"\"%6.3f\" %d", ((double)i*w/scale), (int)(i * w));
	fprintf(gnudata,")\n");
	fprintf(gnudata,"set grid xtics\n");
	fprintf(gnudata,"set xlabel \"%sseconds, parallelism usage %6.1f %% (%d cores)\"\n", scalename,
		(((double)totalclkticks) / (cores * (endrange? endrange-startrange:lastclktick-starttime-startrange)/100)), cores);
	printf("total %ld range %ld tic %ld\n", totalclkticks, endrange-startrange, (cores * (endrange? endrange-startrange:lastclktick-starttime-startrange)/100));

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
	fprintf(gnudata,"plot 0 notitle with lines\n");
	showmap(0, 0);
	keepdata(filename);
	/* show a listing */
	(void)fclose(gnudata);

	printf("Created tomogram '%s' \n",buf);
	printf("Run: 'gnuplot %s' to create the '%s.pdf' file\n",buf,filename);
	printf("The colormap is stored in '%s_map.gpl'\n",filename);
	printf("The memory map is stored in '%s_memory.dat'\n",filename);
	printf("The trace is saved in '%s.dat' for use with --input option\n",filename);
}

/* the main issue to deal with in the analyse is 
 * that the tomograph start can appear while the
 * system is already processing. This leads to
 * receiving 'done' events without matching 'start'
 */
static void update(int state, int thread, long clkticks, long ticks, long memory, char *fcn, char *stmt) {
	int idx;
	
	if ( starttime == 0) {
		/* ignore all instructions up to the first function call */
		if (strncmp(fcn,"function",8) != 0)
			return;
		starttime = clkticks;
	}
	assert(clkticks-starttime >= 0);
	clkticks -=starttime;

	/* start of instruction box */
	if ( state == 0 && thread < MAXTHREADS ){
		/* capture long waits as idle */
		idx = threads[thread];
		/* check for long wait since last use */
		if ( idx >= 0 &&  box[idx-1].clkend && clkticks - box[idx-1].clkend >= threshold ){
			box[idx].fcn = strdup("mal.idle");
			box[idx].stmt= strdup("");
			box[idx].thread = box[idx-1].thread;
			box[idx].clkstart= box[idx-1].clkend;
			box[idx].memstart= box[idx-1].memend;
			box[idx].clkend = clkticks;
			box[idx].memend = memory;
			threads[thread]= ++topbox;
		}
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
		/* focus on part of the time frame */
		if ( endrange ){
			if (box[idx].clkend < startrange || box[idx].clkstart >endrange)
				return;
			if ( box[idx].clkstart < startrange)
				box[idx].clkstart = startrange;
			if ( box[idx].clkend > endrange)
				box[idx].clkend = endrange;
			threads[thread]= ++topbox;
		} else
			threads[thread]= ++topbox;
		lastclktick= box[idx].clkend + starttime;
		totalclkticks += box[idx].clkend - box[idx].clkstart;
		totalexecticks += box[idx].ticks - box[idx].ticks;
	}
	assert(topbox < MAXBOX);
	if (state == 1 && strncmp(fcn,"function",8) == 0){
		createTomogram();
		totalclkticks= 0; /* number of clock ticks reported */
		totalexecticks= 0; /* number of ticks reported for processing */
		if ( title == 0)
			title = strdup(fcn+9);
	}

}
static void parser(char *row){
#ifdef HAVE_STRPTIME
	char *c;
    struct tm stm;
	long clkticks = 0;
	int thread = 0;
	long ticks = 0;
	long memory = 0; /* in MB*/
	char *fcn = 0, *stmt= 0;
	int state= 0;


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
	} else {
		state = 0;
		c= strchr(c+1,(int)'"');
	}

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

	update(state, thread, clkticks, ticks, memory, fcn, stmt);
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

	static struct option long_options[16] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "password", 1, 0, 'P' },
		{ "port", 1, 0, 'p' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "title", 1, 0, 'T' },
		{ "threshold", 1, 0, 't' },
		{ "cores", 1, 0, 'c' },
		{ "input", 1, 0, 'i' },
		{ "range", 1, 0, 'r' },
		{ "output", 1, 0, 'o' },
		{ "debug", 0, 0, 'D' },
		{ "list", 0, 0, 'l' },
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

	initcolors();
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
		int c = getopt_long(argc, argv, "d:u:P:p:?:h:g:D:t:c:m:l:i:r",
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
		case 'l':
			listing = 1;
			break;
		case 'u':
			user = optarg;
			break;
		case 'm':
			colormap=1;
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
		case 'i':
			if ( optarg == 0)
				inputfile = strdup(filename);
			else
				inputfile= optarg;
			break;
		case 'o':
			filename = optarg;
			break;
		case 'c':
			cores = optarg?atoi(optarg):1;
			break;
		case 'r':
		{ char *s;
			if ( optarg == 0)
				break;
			startrange = atol(optarg);
			if ( strchr(optarg,(int)'-') )
				endrange = atol(strchr(optarg,(int)'-')+1);
			else
				endrange = startrange + 1000;
			s = strchr(optarg,(int) 'm');
			if ( s && *(s+1)=='s'){
				startrange *= 1000;
				endrange *=1000;
			} else { /* seconds */
				s = strchr(optarg,(int) 's');
				startrange *= 1000000;
				endrange *=1000000;
			}
			break;
		}
		case 't':
			if ( optarg)
				threshold = atol(optarg);
			break;
		default:
			usage();
			exit(0);
		}
	}

	if ( inputfile){
		/* reload existing tomogram */
		scandata(inputfile);
		createTomogram();
		exit(0);
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
