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
#include <pthread.h>
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
static char *basefilename = "tomograph";
static FILE *tracefd;
static lng startrange = 0, endrange = 0;
static char *inputfile = NULL;
static char *title = 0;
static int debug = 0;
static int beat = 50;
static lng maxio = 0;
static int cpus = 0;
static int atlas= 1;
static int atlaspage = 0;
static FILE *gnudata;
static Mapi dbh;
static MapiHdl hdl = NULL;

/*
 * Parsing the argument list of a MAL call to obtain un-quoted string values
 */
#define MAXMALARGS 1024
char *malarguments[MAXMALARGS];
int malargtop;

static int capturing=0;

#define MAXTHREADS 1048
#define MAXBOX 32678	 /* should be > MAXTHREADS */

static int crossings[MAXTHREADS][MAXTHREADS];
static int target[MAXTHREADS];
static int source[MAXTHREADS];

/* color map management, fixed */
/* see http://www.uni-hamburg.de/Wiss/FB/15/Sustainability/schneider/gnuplot/colors.htm */
/* The initial dictionary is geared towars TPCH-use */
typedef struct COLOR {
	int freq;
	lng timeused;
	char *mod, *fcn, *col;
} Color;

#define NUM_COLORS ((int) (sizeof(dictionary) / sizeof(RGB)))
#define MAX_LEGEND_SHORT 30 /* max. size of colormap / legend */
#define PERCENTAGE 0.01 /* threshold for time filter */

typedef struct {
	char *name;
	char *hsv;
	int red, green, blue;
} RGB;

/* the color table is randomized */
RGB
dictionary[] = {
/* 227 */	{ "saddlebrown", "#8B4513", 139, 69, 19 },
/* 557 */	{ "navyblue", "#9FAFDF", 159, 175, 223 },
/* 734 */	{ "lightyellow", "#FFFFE0", 255, 255, 224 },
/* 750 */	{ "azure", "#F0FFFF", 240, 255, 255 },
/* 496 */	{ "turquoise", "#40E0D0", 64, 224, 208 },
/* 750 */	{ "mintcream", "#F5FFFA", 245, 255, 250 },
/* 278 */	{ "darkcyan", "#008B8B", 0, 139, 139 },
/* 239 */	{ "darkolivegreen", "#556B2F", 85, 107, 47 },
/* 128 */	{ "maroon", "#800000", 128, 0, 0 },
/* 735 */	{ "whitesmoke", "#F5F5F5", 245, 245, 245 },
/* 315 */	{ "dimgray", "#696969", 105, 105, 105 },
/* 492 */	{ "salmon", "#FA8072", 250, 128, 114 },
/* 353 */	{ "mediumvioletred", "#C71585", 199, 21, 133 },
/* 477 */	{ "mediumaquamarine", "#66CDAA", 102, 205, 170 },
/* 139 */	{ "darkblue", "#00008B", 0, 0, 139 },
/* 710 */	{ "beige", "#F5F5DC", 245, 245, 220 },
/* 352 */	{ "mediumseagreen", "#3CB371", 60, 179, 113 },
/* 413 */	{ "cadetblue", "#5F9EA0", 95, 158, 160 },
/* 660 */	{ "gainsboro", "#DCDCDC", 220, 220, 220 },
/* 465 */	{ "mediumslateblue", "#7B68EE", 123, 104, 238 },
/* 623 */	{ "thistle", "#D8BFD8", 216, 191, 216 },
/* 745 */	{ "floralwhite", "#FFFAF0", 255, 250, 240 },
/* 710 */	{ "lemonchiffon", "#FFFACD", 255, 250, 205 },
/* 765 x 	{ "white", "#FFFFFF", 255, 255, 255 }, */
/* 256 */	{ "olive", "#808000", 128, 128, 0 },
/* 255 */	{ "red", "#FF0000", 255, 0, 0 },
/* 380 */	{ "steelblue", "#4682B4", 70, 130, 180 },
/* 664 */	{ "moccasin", "#FFE4B5", 255, 228, 181 },
/* 395 */	{ "darkorange", "#FF8C00", 255, 140, 0 },
/* 507 */	{ "darkgray", "#A9A9A9", 169, 169, 169 },
/* 272 */	{ "seagreen", "#2E8B57", 46, 139, 87 },
/* 755 */	{ "snow", "#FFFAFA", 255, 250, 250 },
/* 284 */	{ "olivedrab", "#6B8E23", 107, 142, 35 },
/* 382 */	{ "chartreuse", "#7FFF00", 127, 255, 0 },
/* 478 */	{ "palevioletred", "#DB7093", 219, 112, 147 },
/* 710 */	{ "lavender", "#E6E6FA", 230, 230, 250 },
/* 162 */	{ "midnightblue", "#191970", 25, 25, 112 },
/* 708 */	{ "mistyrose", "#FFE4E1", 255, 228, 225 },
/* 425 */	{ "tomato", "#FF6347", 255, 99, 71 },
/* 576 */	{ "skyblue", "#87CEEB", 135, 206, 235 },
/* 420 */	{ "orange", "#FFA500", 255, 165, 0 },
/* 128 */	{ "green", "#008000", 0, 128, 0 },
/* 740 */	{ "lavenderblush", "#FFF0F5", 255, 240, 245 },
/* 380 */	{ "lightseagreen", "#20B2AA", 32, 178, 170 },
/* 415 */	{ "darkturquoise", "#00CED1", 0, 206, 209 },
/* 526 */	{ "lightgreen", "#90EE90", 144, 238, 144 },
/* 395 */	{ "royalblue", "#4169E1", 65, 105, 225 },
/* 205 */	{ "darkslategray", "#2F4F4F", 47, 79, 79 },
/* 415 */	{ "goldenrod", "#DAA520", 218, 165, 32 },
/* 735 */	{ "honeydew", "#F0FFF0", 240, 255, 240 },
/* 246 */	{ "firebrick", "#B22222", 178, 34, 34 },
/* 255 */	{ "lime", "#00FF00", 0, 255, 0 },
/* 381 */	{ "gray", "#7F7F7F", 127, 127, 127 },
/* 128 */	{ "navy", "#000080", 0, 0, 128 },
/* 479 */	{ "darkkhaki", "#BDB76B", 189, 183, 107 },
/* 345 */	{ "chocolate", "#D2691E", 210, 105, 30 },
/* 474 */	{ "rosybrown", "#BC8F8F", 188, 143, 143 },
/* 255 */	{ "blue", "#0000FF", 0, 0, 255 },
/* 408 */	{ "lightslategray", "#778899", 119, 136, 153 },
/* 504 */	{ "sandybrown", "#F4A460", 244, 164, 96 },
/* 728 */	{ "oldlace", "#FDF5E6", 253, 245, 230 },
/* 249 */	{ "brown", "#A52A2A", 165, 42, 42 },
/* 743 */	{ "aliceblue", "#F0F8FF", 240, 248, 255 },
/* 630 */	{ "lightpink", "#FFB6C1", 255, 182, 193 },
/*   0  x	{ "black", "#000000", 0, 0, 0 }, */
/* 404 */	{ "mediumspringgreen", "#00FA9A", 0, 250, 154 },
/* 486 */	{ "cornflowerblue", "#6495ED", 100, 149, 237 },
/* 723 */	{ "cornsilk", "#FFF8DC", 255, 248, 220 },
/* 710 */	{ "lightgoldenrodyellow", "#FAFAD2", 250, 250, 210 },
/* 207 */	{ "forestgreen", "#228B22", 34, 139, 34 },
/* 707 */	{ "papayawhip", "#FFEFD5", 255, 239, 213 },
/* 640 */	{ "palegoldenrod", "#EEE8AA", 238, 232, 170 },
/* 278 */	{ "darkmagenta", "#8B008B", 139, 0, 139 },
/* 576 */	{ "silver", "#C0C0C0", 192, 192, 192 },
/* 305 */	{ "limegreen", "#32CD32", 50, 205, 50 },
/* 429 */	{ "dodgerblue", "#1E90FF", 30, 144, 255 },
/* 324 */	{ "orangered", "#FF4500", 255, 69, 0 },
/* 540 */	{ "hotpink", "#FF69B4", 255, 105, 180 },
/* 205 */	{ "mediumblue", "#0000CD", 0, 0, 205 },
/* 734 */	{ "lightcyan", "#E0FFFF", 224, 255, 255 },
/* 407 */	{ "blueviolet", "#8A2BE2", 138, 43, 226 },
/* 256 */	{ "purple", "#800080", 128, 0, 128 },
/* 389 */	{ "indianred", "#CD5C5C", 205, 92, 92 },
/* 384 */	{ "slategray", "#708090", 112, 128, 144 },
/* 750 */	{ "ivory", "#FFFFF0", 255, 255, 240 },
/* 650 */	{ "navajowhite", "#FFDEAD", 255, 222, 173 },
/* 382 */	{ "springgreen", "#00FF7F", 0, 255, 127 },
/* 606 */	{ "violet", "#EE82EE", 238, 130, 238 },
/* 510 */	{ "aqua", "#00FFFF", 0, 255, 255 },
/* 510 */	{ "cyan", "#00FFFF", 0, 255, 255 },
/* 359 */	{ "darkviolet", "#9400D3", 148, 0, 211 },
/* 602 */	{ "plum", "#DDA0DD", 221, 160, 221 },
/* 591 */	{ "lightskyblue", "#87CEFA", 135, 206, 250 },
/* 720 */	{ "linen", "#FAF0E6", 250, 240, 230 },
/* 555 */	{ "palegreen", "#98FB98", 152, 251, 152 },
/* 272 */	{ "darkslateblue", "#483D8B", 72, 61, 139 },
/* 462 */	{ "coral", "#FF7F50", 255, 127, 80 },
/* 544 */	{ "orchid", "#DA70D6", 218, 112, 214 },
/* 505 */	{ "darksalmon", "#E9967A", 233, 150, 122 },
/* 650 */	{ "pink", "#FFC0CB", 255, 192, 203 },
/* 300 */	{ "crimson", "#DC143C", 220, 20, 60 },
/* 394 */	{ "yellowgreen", "#9ACD32", 139, 205, 50 },
/* 700 */	{ "antiquewhite", "#FAEBD7", 250, 235, 215 },
/* 407 */	{ "darkorchid", "#9932CC", 153, 50, 204 },
/* 651 */	{ "paleturquoise", "#AFEEEE", 175, 238, 238 },
/* 633 */	{ "lightgrey", "#D3D3D3", 211, 211, 211 },
/* 679 */	{ "bisque", "#FFE4C4", 255, 228, 196 },
/* 422 */	{ "deeppink", "#FF1493", 255, 20, 147 },
/* 139 */	{ "darkred", "#8B0000", 139, 0, 0 },
/* 475 */	{ "greenyellow", "#ADFF2F", 173, 255, 47 },
/* 205 */	{ "indigo", "#4B0082", 75, 0, 130 },
/* 446 */	{ "deepskyblue", "#00BFFF", 0, 191, 255 },
/* 329 */	{ "darkgoldenrod", "#B8860B", 184, 134, 11 },
/* 470 */	{ "gold", "#FFD700", 255, 215, 0 },
/* 610 */	{ "khaki", "#F0E68C", 240, 230, 140 },
/* 485 */	{ "mediumturquoise", "#48D1CC", 72, 209, 204 },
/* 100 */	{ "darkgreen", "#006400", 0, 100, 0 },
/* 510 */	{ "fuchsia", "#FF00FF", 255, 0, 255 },
/* 510 */	{ "magenta", "#FF00FF", 255, 0, 255 },
/* 751 */	{ "ghostwhite", "#F8F8FF", 248, 248, 255 },
/* 658 */	{ "peachpuff", "#FFDAB9", 255, 218, 185 },
/* 478 */	{ "mediumpurple", "#9370DB", 147, 112, 219 },
/* 376 */	{ "lawngreen", "#7CFC00", 124, 252, 0 },
/* 401 */	{ "peru", "#CD853F", 205, 133, 63 },
/* 695 */	{ "blanchedalmond", "#FFEBCD", 255, 235, 205 },
/* 256 */	{ "teal", "#008080", 0, 128, 128 },
/* 594 */	{ "lightsteelblue", "#B0C4DE", 176, 196, 222 },
/* 474 */	{ "darkseagreen", "#8FBC8F", 143, 188, 143 },
/* 287 */	{ "sienna", "#A0522D", 160, 82, 45 },
/* 401 */	{ "slateblue", "#6A5ACD", 106, 90, 205 },
/* 646 */	{ "wheat", "#F5DEB3", 245, 222, 179 },
/* 738 */	{ "seashell", "#FFF5EE", 255, 245, 238 },
/* 530 */	{ "tan", "#D2B48C", 210, 180, 140 },
/* 510 */	{ "yellow", "#FFFF00", 255, 255, 0 },
/* 496 */	{ "lightcoral", "#F08080", 240, 128, 128 },
/* 630 */	{ "powderblue", "#B0E0E6", 176, 224, 230 },
/* 541 */	{ "burlywood", "#DEB887", 222, 184, 135 },
/* 594 */	{ "aquamarine", "#7FFFD4", 127, 255, 212 },
/* 619 */	{ "lightblue", "#ADD8E6", 173, 216, 230 },
/* 537 */	{ "lightsalmon", "#FFA07A", 255, 160, 122 },
/* 482 */	{ "mediumorchid", "#BA55D3", 186, 85, 211 },
};

/* initial mod.fcn list for adaptive colormap, this ensures ease of comparison */
Color
base_colors[NUM_COLORS] = {
	/* reserve (base_)colors[0] for generic "*.*" */
/* 99999 	{ 0, 0, "*", "*", 0 },*/
/* arbitrarily ordered by descending frequency in TPCH SF-100 with 32 threads */
/* 11054 */	{ 0, 0, "algebra", "leftfetchjoin", 0 },
/* 10355 */	{ 0, 0, "language", "pass", 0 },
/*  5941 */	{ 0, 0, "sql", "bind", 0 },
/*  5664 */	{ 0, 0, "mat", "packIncrement", 0 },
/*  4796 */	{ 0, 0, "algebra", "subselect", 0 },
/*  4789 */	{ 0, 0, "algebra", "join", 0 },
/*  4789 */	{ 0, 0, "algebra", "subjoin", 0 },
/*  2664 */	{ 0, 0, "sql", "projectdelta", 0 },
/*  2112 */	{ 0, 0, "batcalc", "!=", 0 },
/*  1886 */	{ 0, 0, "sql", "bind_idxbat", 0 },
/*  1881 */	{ 0, 0, "algebra", "leftfetchjoinPath", 0 },
/* 		 */	{ 0, 0, "algebra", "tinter", 0 },
/*  	 */	{ 0, 0, "algebra", "tdiff", 0 },
/*  1013 */	{ 0, 0, "sql", "tid", 0 },
/*   832 */	{ 0, 0, "bat", "mergecand", 0 },
/*   813 */	{ 0, 0, "sql", "delta", 0 },
/*   766 */	{ 0, 0, "aggr", "subsum", 0 },
/*   610 */	{ 0, 0, "batcalc", "*", 0 },
/*   577 */	{ 0, 0, "group", "subgroupdone", 0 },
/*   481 */	{ 0, 0, "sql", "subdelta", 0 },
/*   481 */	{ 0, 0, "sql", "subsort", 0 },
/*   448 */	{ 0, 0, "batcalc", "-", 0 },
/*   334 */	{ 0, 0, "bat", "mirror", 0 },
/*   300 */	{ 0, 0, "group", "subgroup", 0 },
/*   264 */	{ 0, 0, "batcalc", "==", 0 },
/*   260 */	{ 0, 0, "batcalc", "ifthenelse", 0 },
/*   209 */	{ 0, 0, "batcalc", "hge", 0 },
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
/*    	 */	{ 0, 0, "bat", "mergecand", 0 },
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
/*    32 */	{ 0, 0, "batalgebra", "like", 0 },
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


Color
colors[NUM_COLORS] = {{0,0,0,0,0}};

#ifdef NUMAprofiling
static void
showNumaHeatmap(void){
	int i,j =0;
	int max= 0;
	FILE *f;

	
	f= fopen("tomograph_heatmap.csv","a");
	if( f == NULL){
		fprintf(stderr,"Can not create tomograph_heatmap.csv\n");
		return;
	}
	for( i=0; i< MAXTHREADS; i++){
		if( target[i])
		for(j=MAXTHREADS-1; j>0 && crossings[i][j]; j--)
			;
		if (j > max) max =j;
	}
	for( i=0; i< max; i++)
	if( target[i] && source[i] ){
		for(j=0; j< max; j++)
		if( target[j] && source[j])
			fprintf(stderr,"%d\t", crossings[i][j]);
		fprintf(stderr,"\n");
	}

	for( i=0; i< MAXTHREADS; i++){
		for(j=0; j< MAXTHREADS; j++)
			crossings[i][j]=0;
		target[i]=0;
		source[i]=0;
	}
}
#endif

static void
usageTomograph(void)
{
	fprintf(stderr, "tomograph [options]\n");
	fprintf(stderr, "  -d | --dbname=<database_name>\n");
	fprintf(stderr, "  -u | --user=<user>\n");
	fprintf(stderr, "  -P | --password=<password>\n");
	fprintf(stderr, "  -p | --port=<portnr>\n");
	fprintf(stderr, "  -h | --host=<hostname>\n");
	fprintf(stderr, "  -T | --title=<plot title>\n");
	fprintf(stderr, "  -r | --range=<starttime>-<endtime>[ms,s] \n");
	fprintf(stderr, "  -i | --input=<profiler event file > \n");
	fprintf(stderr, "  -o | --output=<file prefix > (default 'tomograph'\n");
	fprintf(stderr, "  -b | --beat=<delay> in milliseconds (default 50)\n");
	fprintf(stderr, "  -A | --atlas=<number> maximum number of pages\n");
	fprintf(stderr, "  -D | --debug\n");
	fprintf(stderr, "  -? | --help\n");
	exit(-1);
}

/* Any signal should be captured and turned into a graceful
 * termination of the profiling session. */
static void createTomogram(void);

static void
stopListening(int i)
{
	fprintf(stderr,"signal %d received\n",i);
	if( dbh)
		doQ("profiler.stop();");
stop_disconnect:
	if (atlas != atlaspage  )
		createTomogram();
	// show follow up action only once
	if(atlaspage >= 1){
		fprintf(stderr, "To create the atlas run:\n");
		for (i = 0; i< atlaspage;  i++)
			fprintf(stderr, "gnuplot %s_%02d.gpl;",basefilename,i);
		fprintf(stderr, "gs -dNOPAUSE -sDEVICE=pdfwrite -sOUTPUTFILE=%s.pdf -dBATCH %s_??.pdf\n",basefilename,basefilename);
	}

	if(dbh)
		mapi_disconnect(dbh);
	exit(0);
}

#define START 1
#define DONE 2
#define ACTION 3
#define PING 4
#define WAIT 5
#define GCOLLECT 6

static char *statenames[]= {"","start","done","action","ping","wait","gccollect"};

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
	char *numa;
	int state;
} Box;

int threads[MAXTHREADS];
lng lastclk[MAXTHREADS];
Box box[MAXBOX];
int topbox = 0;
int events = 0;

lng totalclkticks = 0; /* number of clock ticks reported */
lng totalexecticks = 0; /* number of ticks reported for processing */
lng lastclktick = 0;
lng totalticks = 0;
lng starttime = 0;
int figures = 0;
char *currentfunction= 0;
char *currentquery= 0;
int object = 1;

static void resetTomograph(void){
	static char buf[128];
	int i;
	snprintf(buf,BUFSIZ,"%s_%02d.trace",basefilename,atlaspage);
	if( inputfile == 0 || strcmp(inputfile,buf) ){
		tracefd = fopen(buf,"w");
		if( tracefd == NULL)
			fprintf(stderr,"Could not create trace file\n");
	}
	if (debug)
		fprintf(stderr, "RESET tomograph %d\n", atlaspage);
	for( i =0; i < NUM_COLORS; i++){
		colors[i].freq = 0;
		colors[i].timeused = 0;
	}
	for(i=0; i< MAXTHREADS; i++)
		lastclk[i]=0;
	topbox =0;
	events = 0;
	malargtop =0;
	for (i = 0; i < MAXTHREADS; i++)
		threads[i] = topbox++;
	memset((char*) box, 0, sizeof(Box) * MAXBOX);

	maxio = 0;
	totalclkticks = 0; 
	totalexecticks = 0;
	lastclktick = 0;
	figures = 0;
	currentfunction = 0;
	currentquery = 0;
	object = 1;
}

static lng
gnuXtics(int withlabels)
{
	double scale = 1.0;
	const char * scalename = "MB";
	int digits;
	lng tw, w = lastclktick - starttime;
	int i;

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
	for (tw = (scale / 10 > 1)? scale/10:1; 15 * tw < w; tw *= 10)
		;
	if (3 * tw > w)
		tw /= 4;
	else if (6 * tw > w)
		tw /= 2;
	if (w / scale >= 1000)
		digits = 0;
	else if (w / scale >= 100)
		digits = 1;
	else if (w / scale >= 10)
		digits = 2;
	else
		digits = 3;

	if(withlabels)
		fprintf(gnudata, "set xtics (\"0\" 0.0,");
	else
		fprintf(gnudata, "set xtics ( \"\" 0.0,");
	for (i = 1; i * tw < w - 2 * tw / 3; i++){
		if( withlabels)
		fprintf(gnudata, "\"%g\" "LLFMT".0,", (double) i * tw / scale, i * tw);
		else
		fprintf(gnudata, "\"\" "LLFMT".0,", i * tw);
	}
	if( withlabels)
		fprintf(gnudata, "\"%.*f %s\" "LLFMT".0", digits, (double) w / scale, scalename, w);
	else
		fprintf(gnudata, "\"\" "LLFMT".0",   w);
	fprintf(gnudata, ")\n");
	w /= 10;
	fprintf(gnudata, "set grid xtics\n");
	return w;
}

static void dumpbox(int i)
{
	printf("object %d thread %2d[%4d] row %d color %d ", object, box[i].thread,i, box[i].row, box[i].color);
	if (box[i].fcn)
		printf("%s ", box[i].fcn);
	printf("clk "LLFMT" - "LLFMT" ", box[i].clkstart, box[i].clkend);
	printf("mem "LLFMT" - "LLFMT" ", box[i].memstart, box[i].memend);
	printf("foot "LLFMT" - "LLFMT" ", box[i].footstart, box[i].footend);
	printf("ticks "LLFMT" ", box[i].ticks);
	if (box[i].stmt)
		printf("%s ", box[i].stmt);
	printf("\n");
}

static int
cmp_clr(const void * _one , const void * _two)
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

static void
initcolors(void)
{
	int i = 0;

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

static void
dumpboxes(void)
{
	FILE *f = 0;
	char buf[BUFSIZ];
	int i;
	lng e=0;
	int written = 0;

	snprintf(buf, BUFSIZ, "%s_%02d.dat", basefilename, atlaspage);
	f = fopen(buf, "w");
	if(f == NULL){
		fprintf(stderr,"Could not create %s\n",buf);
		exit(0);
	}

	for (i = 0; i < topbox; i++)
		if (box[i].clkend && box[i].fcn) {
			if (box[i].state < PING) {
				//io counters are zero at start of instruction !
				//fprintf(f,""LLFMT" %f 0 0 \n", box[i].clkstart, (box[i].memstart/1024.0));
				fprintf(f, ""LLFMT" %f %f 0 0\n", box[i].clkend, (box[i].memend / 1024.0), box[i].footend/1024.0);
				written++;
			} else 
			if (box[i].state == WAIT) {
			} else 
			if (box[i].state == PING) {
				/* cpu stat events may arrive out of order, drop those */
				if (box[i].clkstart <= e)
					continue;
				e = box[i].clkstart-starttime;
				fprintf(f, ""LLFMT" %f %f "LLFMT" "LLFMT"\n", e, (box[i].memend / 1024.0), box[i].footend/1024.0, box[i].reads, box[i].writes);
			}
		}
	if( written == 0){
		topbox = 0;
		fprintf(stderr,"Insufficient data for %s\n",buf);
	}

	if (f)
		(void) fclose(f);
}

/* produce memory thread trace */
static void
showmemory(void)
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
	gnuXtics(0);
	mn = min / 1024.0;
	mx = max / 1024.0;
	mm = (mx - mn) / 50.0; /* 2% top & bottom margin */
	fprintf(gnudata, "set yrange [%f:%f]\n", mn - mm, mx + mm);
	fprintf(gnudata, "set ytics (\"%.*f\" %f, \"%.*f\" %f) nomirror\n", digits, min / scale, mn, digits, max / scale, mx);
	fprintf(gnudata, "plot \"%s_%02d.dat\" using 1:2 notitle with dots linecolor rgb \"blue\"\n", basefilename,atlaspage);
	fprintf(gnudata, "unset yrange\n");
}

static char *
getHeatColor(double load)
{
	if ( load > 0.75 ) return "yellow";
	if ( load > 0.5 ) return "orange";
	if ( load > 0.25 ) return "red";
	return "white";
}

/* produce memory thread trace */
static void
showcpu(void)
{
	int prev= -1, i, j = 0;
	double cpuload[MAXTHREADS];
	char *s;

	fprintf(gnudata, "\nset tmarg 1\n");
	fprintf(gnudata, "set bmarg 0\n");
	fprintf(gnudata, "set lmarg 10\n");
	fprintf(gnudata, "set rmarg 10\n");
	fprintf(gnudata, "set size 1,0.%02d\n", cpus);
	fprintf(gnudata, "set origin 0.0, 0.%d\n", 89 - cpus);
	fprintf(gnudata, "set ylabel \"CPU\"\n");
	fprintf(gnudata, "unset xtics\n");
	fprintf(gnudata, "unset ytics\n");
	fprintf(gnudata, "set ytics 0, %d\n",4);
	fprintf(gnudata, "set grid ytics\n");
	fprintf(gnudata, "set border\n");
	gnuXtics(0);

	fprintf(gnudata, "set xrange ["LLFMT".0:"LLFMT".0]\n", startrange, (endrange? endrange:lastclktick - starttime));
	fprintf(gnudata, "set yrange [0:%d]\n", cpus);
	for (i = 0; i < topbox; i++)
		j+=(box[i].state == PING);
	if( debug)
		fprintf(stderr,"Pings for cpu heat:%d\n",j);
	for (i = 0; i < topbox; i++)
		if (box[i].state == PING) {
			// decode the cpu heat
			j = 0;
			s = box[i].stmt +1;
			while (s) {
				s = strchr(s + 1, ' ');
				while (s && isspace((int) *s))
					s++;
				if( s){
					cpuload[j++] = atof(s);
				}
			}
			// paint the heatmap, the load refers the previous time slot
			if( prev >= 0)
				for(j=0; j < cpus; j++)
				fprintf(gnudata,"set object %d rectangle from "LLFMT".0, %d.0 to "LLFMT".0, %d fillcolor rgb \"%s\" fillstyle solid 1.0 noborder\n",
						object++, box[prev].clkend, j , box[i].clkstart, (j+1) , getHeatColor(cpuload[j]) );
			prev = i;
		}
	fprintf(gnudata,"  plot 0 notitle with lines\n unset for[i=1:%d] object i \n",object);
	fprintf(gnudata, "unset yrange\n");
	fprintf(gnudata, "unset ytics\n");
	fprintf(gnudata, "unset grid\n");
}

/* produce memory thread trace */
static void
showio(void)
{
	int i,b = (beat? beat:1);
	lng max = 0;

	for (i = 0; i < topbox; i++)
		if (box[i].clkend && box[i].state >= PING) {
			if (box[i].reads > max)
				max = box[i].reads;
			if (box[i].writes > max)
				max = box[i].writes;
		}
	max += b;


	fprintf(gnudata, "\nset tmarg 1\n");
	fprintf(gnudata, "set bmarg 1\n");
	fprintf(gnudata, "set lmarg 10\n");
	fprintf(gnudata, "set rmarg 10\n");
	fprintf(gnudata, "set size 1,%s\n", "0.1");
	fprintf(gnudata, "set origin 0.0,0.87\n");
	fprintf(gnudata, "set xrange ["LLFMT".0:"LLFMT".0]\n", startrange, lastclktick - starttime);
	fprintf(gnudata, "set yrange [0:"LLFMT".0]\n", max / b);
	fprintf(gnudata, "unset xtics\n");
	fprintf(gnudata, "unset ytics\n");
	fprintf(gnudata, "unset ylabel\n");
	fprintf(gnudata, "set y2tics in (0, "LLFMT".0) nomirror\n", max / b);
	fprintf(gnudata, "set y2label \"IO per ms\"\n");
#ifdef GNUPLOT_463_BUG_ON_FEDORA_20
/* this is the original version, but on Fedora 20 with
 * gnuplot-4.6.3-6.fc20.x86_64 it produces a red background on most of
 * the page */
	fprintf(gnudata, "plot \"%s_%02d.dat\" using 1:(($4+$5)/%d.0) title \"reads\" with boxes fs solid linecolor rgb \"gray\" ,\\\n", basefilename, atlaspage, beat);
	fprintf(gnudata, "\"%s_%02d.dat\" using 1:($5/%d.0) title \"writes\" with boxes fs solid linecolor rgb \"red\"  \n", basefilename, atlaspage, beat);
#else
/* this is a slightly modified version that produces decent results on
 * all platforms */
	fprintf(gnudata, "plot \"%s_%02d.dat\" using 1:(($4+$5)/%d.0) title \"reads\" with impulses linecolor rgb \"gray\" ,\\\n", basefilename, atlaspage, beat);
	fprintf(gnudata, "\"%s_%02d.dat\" using 1:($5/%d.0) title \"writes\" with impulses linecolor rgb \"red\"  \n", basefilename, atlaspage, beat);
#endif
	fprintf(gnudata, "unset y2label\n");
	fprintf(gnudata, "unset y2tics\n");
	fprintf(gnudata, "unset y2range\n");
	fprintf(gnudata, "unset title\n");
}


/* print time (given in microseconds) in human-readable form
 * showing the highest two relevant units */
static void
fprintf_time(FILE *f, lng time)
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

/* produce a legenda image for the color map */
#define COLUMNS 5

static void
showcolormap(char *filename, int all)
{
	FILE *f = 0;
	char buf[BUFSIZ];
	int i, k = 0;
	int w = 380; // 600 for 3 columns
	int h = 590;
	lng totfreq = 0, tottime = 0;
	Color *clrs = colors, *_clrs_ = NULL;

	if (all) {
		snprintf(buf, BUFSIZ, "%s_%02d.gpl", basefilename, atlaspage);
		f = fopen(buf, "w");
		if (f == NULL) {
			fprintf(stderr, "Creating file %s.gpl failed\n", filename);
			exit(1);
		}
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
	/* create copy of colormap and sort in ascending order of timeused;
	 * "*.*" stays first (colors[0]) */
	_clrs_ = (Color*) malloc(sizeof(colors));
	if (_clrs_) {
		memcpy(_clrs_, colors, sizeof(colors));
		qsort(_clrs_, NUM_COLORS, sizeof(Color), cmp_clr);
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

				if (k % COLUMNS == 0)
					h -= 45;
				fprintf(f, "set object %d rectangle from %.2f, %.2f to %.2f, %.2f fillcolor rgb \"%s\" fillstyle solid 1.0\n",
					object++, (double) (k % COLUMNS) * w, (double) h - 40, (double) ((k % COLUMNS) * w + 0.05 * w), (double) h - 5, clrs[i].col);
				fprintf(f, "set label %d \"%s.%s \" at %d,%d\n",
					object++, (clrs[i].mod?clrs[i].mod:""), clrs[i].fcn, (int) ((k % COLUMNS) * w + 0.07 * w), h - 15);
				fprintf(f, "set label %d \"%d call%s: ",
					object++, clrs[i].freq, clrs[i].freq>1?"s":"");
				fprintf_time(f, clrs[i].timeused);
				fprintf(f, "\" at %f,%f\n",
					(double) ((k % COLUMNS) * w + 0.07 * w), (double) h - 35);
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

	h -= 45;
	fprintf(f, "set label %d \" "LLFMT" MAL instructions executed; total CPU core time: ",
		object++, totfreq);
	fprintf_time(f, tottime);
	fprintf(f, "; parallelism usage %.1f %%", totalclkticks / (totalticks / 100.0));
	fprintf(f, "\" at %d,%d\n",
		(int) (0.2 * w), h - 35);
	fprintf(f, "plot 0 notitle with lines linecolor rgb \"white\"\n");
	if (all) {
		assert(f != gnudata);
		fclose(f);
	}
}

static void
updmap(int idx)
{
	char *mod, *fcn, buf[BUFSIZ], *call = buf;
	int i, fnd = 0;

	if( box[idx].fcn == 0)
		return;
	snprintf(buf, sizeof(buf), "%s", box[idx].fcn);
	mod = call;
	fcn = strchr(call, '.');
	if (fcn) {
		*fcn = 0;
		fcn++;
	} else
		fcn = "*";
	/* find "mod.fcn" */
	for (i = 1; i < NUM_COLORS && colors[i].mod; i++)
		if (strcmp(mod, colors[i].mod) == 0 &&
			strcmp(fcn, colors[i].fcn) == 0) {
			fnd = i;
			break;
		}
	if (fnd == 0 && i < NUM_COLORS) {
		/* not found, but still free slot: add new one */
		fnd = i;
		colors[fnd].mod = mod?strdup(mod): 0;
		colors[fnd].fcn = strdup(fcn);
		if( debug)
			printf("added function #%d: %s.%s\n", fnd, (mod?mod:""), fcn);
	}

	colors[fnd].freq++;
	colors[fnd].timeused += box[idx].clkend - box[idx].clkstart;
	box[idx].color = fnd;
}

/* gnuplot defaults */
static int height = 160;

static void
gnuplotheader(char *filename)
{
	time_t tm;
	char *date, *c;

	fprintf(gnudata, "set terminal pdfcairo noenhanced color solid size 8.3,11.7\n");
	fprintf(gnudata, "set output \"%s.pdf\"\n", filename);
	fprintf(gnudata, "set size 1,1\n");
	fprintf(gnudata, "set tics front\n");
	tm = time(0);
	date = ctime(&tm);
	if (strchr(date, '\n'))
		*strchr(date, '\n') = 0;
	if( title){
		for (c = title; c && *c; c++)
			if (*c == '_')
				*c = '-';
		fprintf(gnudata, "set title \"%s\t%s\"\n", date, title);
	} else 
	if( currentquery )
		fprintf(gnudata, "set title \"%s\t%s\"\n", date, currentquery);
	else
		fprintf(gnudata, "set title \"%s\tTomogram\"\n", date);
	fprintf(gnudata, "set multiplot\n");
}

static void
createTomogram(void)
{
	char buf[BUFSIZ];
	int rows[MAXTHREADS] = {0};
	int top = 0;
	int i, j;
	int h, prevobject = 1;
	lng w = lastclktick - starttime;

	if( debug)
		fprintf(stderr,"create tomogram\n");
	if( events == 0){
		fprintf(stderr,"No events found\n");
		return;
	}
	snprintf(buf, BUFSIZ, "%s_%02d.gpl", basefilename,atlaspage);
	gnudata = fopen(buf, "w");
	if (gnudata == 0) {
		printf("ERROR in creation of %s\n", buf);
		exit(-1);
	}
	if( strchr(buf,'.'))
		*strchr(buf, '.') = 0;
	gnuplotheader(buf);
	dumpboxes();
	showio();
	showmemory();
	showcpu();

	fprintf(gnudata, "\nset tmarg 1\n");
	fprintf(gnudata, "set bmarg 3\n");
	fprintf(gnudata, "set lmarg 10\n");
	fprintf(gnudata, "set rmarg 10\n");
	fprintf(gnudata, "set size 1,0.48\n");
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
			if( box[i].state != WAIT)
				updmap(i);
		}


	height = top * 20;
	fprintf(gnudata, "set yrange [0:%d]\n", height);
	fprintf(gnudata, "set ylabel \"threads\"\n");
	fprintf(gnudata, "set key right \n");
	fprintf(gnudata, "unset colorbox\n");
	fprintf(gnudata, "unset title\n");

	w = gnuXtics(1);

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
		if (box[i].clkend)
			switch (box[i].state) {
			default:
				if (debug)
					dumpbox(i);
				// always show a start line
				if ( box[i].clkend - box[i].clkstart < w/200.0)
					fprintf(gnudata, "set object %d rectangle from "LLFMT".0, %d.0 to %.2f, %d fillcolor rgb \"%s\" fillstyle solid 1.0 \n",
						object++, box[i].clkstart, box[i].row * 2 * h, box[i].clkstart+(w/200.0>1?w/200.0:1), box[i].row * 2 * h + h, colors[box[i].color].col);
				else
					fprintf(gnudata, "set object %d rectangle from "LLFMT".0, %d.0 to "LLFMT".0, %d fillcolor rgb \"%s\" fillstyle solid 1.0 \n",
						object++, box[i].clkstart, box[i].row * 2 * h, box[i].clkend, box[i].row * 2 * h + h, colors[box[i].color].col);
				break;
			case PING:
				break;
			case WAIT:
				fprintf(gnudata, "set object %d rectangle from "LLFMT".0, %d.0 to %.2f,%.2f front fillcolor rgb \"red\" fillstyle solid 1.0\n",
					object++, box[i].clkstart, box[i].row * 2 * h+h/3, box[i].clkstart+ w /25.0, box[i].row *2 *h + h - 0.3 * h);
				break;
			case GCOLLECT:
				fprintf(gnudata, "set object %d rectangle from "LLFMT".0, %d.0 to "LLFMT".0, %d fillcolor rgb \"green\" fillstyle solid 1.0 \n",
					object++, box[i].clkstart, box[i].row * 2 * h +h/3, box[i].clkend, box[i].row * 2 * h + h-h/3);
				break;
			}



	fprintf(gnudata, "plot 0 notitle with lines\n");
	fprintf(gnudata, "unset for[i=%d:%d] object i\n", prevobject, object - 1);
	prevobject = object - 1;
	showcolormap(0, 0);
	fprintf(gnudata, "unset multiplot\n");
	(void) fclose(gnudata);
	gnudata = 0;
	atlaspage++;
	if (atlas == atlaspage ) {
		stopListening(0);
	}
#ifdef NUMAprofiling
	showNumaHeatmap();
#endif
}

/* The intra-thread flow is collected for later presentation */


static void
updateNumaHeatmap(int thread, char *numa){
	char *c;
	int t;
	for( c= numa; *c && *c == '@';){
		c++;
		t =atoi(c);
		crossings[thread][t]++;
		target[thread]++;
		source[t]++;
		while(*c && *c !='@') c++;
	}
}


/* the main issue to deal with in the analysis is
 * that the tomograph start can appear while the
 * system is already processing. 
 * receiving 'done' events without matching 'start'
 *
 * A secondary issue is to properly count the functions
 * being monitored.
 */

static int ping = -1;

static void
update(int state, int thread, lng clkticks, lng ticks, lng memory, char *numa, lng footprint, lng reads, lng writes, char *fcn, char *stmt, char *line)
{
	int idx, i;
	Box b;
	char *s;
	char *qry, *q, *c;
	int uid = 0,qid = 0;
 
	if (topbox == MAXBOX) {
		fprintf(stderr, "Out of space for trace");
		createTomogram();
		exit(0);
	}
	/* handle a ping event, keep the current instruction in focus */
	if (state >= PING ) {
		if (cpus == 0 && state == PING) {
			char *s;
			if( (s= stmt,'[')) 
				s++;
			else s = stmt;
			while (s && isspace((int) *s))
				s++;
			while (s) {
				s = strchr(s + 1, ' ');
				while (s && isspace((int) *s))
					s++;
				if (s)
					cpus++;
			}
		}
		if( startrange && starttime && clkticks-starttime < startrange)
			return;
		if( endrange && starttime && clkticks-starttime > endrange)
			return;
		idx = threads[thread];
		b = box[idx];
		box[idx].state = state;
		box[idx].thread = thread;
		//lastclk[thread] = clkticks-starttime;
		box[idx].clkend = box[idx].clkstart = clkticks-starttime;
		if (state == GCOLLECT)
			box[idx].clkstart -= ticks;
		box[idx].memend = box[idx].memstart = memory;
		box[idx].footstart = box[idx].footend = footprint;
		box[idx].reads = reads;
		box[idx].writes = writes;
		s = strchr(stmt, ']');
		if (s)
			*s = 0;
		box[idx].stmt = stmt;
		if ( !capturing){
			ping = idx;
			return;
		}
		box[idx].fcn = state == PING? strdup("profiler.ping"):strdup("profiler.wait");
		threads[thread] = ++topbox;
		idx = threads[thread];
		box[idx] = b;
		if (reads > maxio)
			maxio = reads;
		if (writes > maxio)
			maxio = writes;
		return;
	}

	assert(clkticks >= 0);

	if (debug)
		fprintf(stderr, "Update %s input %s stmt %s time " LLFMT" %s\n",(state>=0?statenames[state]:"unknown"),(fcn?fcn:"(null)"),(currentfunction?currentfunction:""),clkticks -starttime,(numa?numa:""));

	if (starttime == 0) {
		if (fcn == 0 ) {
			if (debug)
				fprintf(stderr, "Skip %s input %s\n",(state>=0?statenames[state]:"unknown"),fcn);
			return;
		}
		if (debug)
			fprintf(stderr, "Start capturing updates %s\n",fcn);
	}
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

	/* monitor top level function brackets, we restrict ourselves to SQL queries */
	if (!capturing && state == START && fcn && strncmp(fcn, "function", 8) == 0) {
		if( (i = sscanf(fcn + 9,"user.s%d_%d",&uid,&qid)) != 2){
			if( debug)
				fprintf(stderr,"Start phase parsing %d, uid %d qid %d\n",i,uid,qid);
			return;
		}
		if (capturing++ == 0)
			starttime = clkticks;
		if( ping >= 0){
			box[ping].clkend = box[ping].clkstart = 0;
			ping = -1;
		}
		if (currentfunction == 0)
			currentfunction = strdup(fcn+9);
		if (debug)
			fprintf(stderr, "Enter function %s capture %d\n", currentfunction, capturing);
		if( tracefd)
			fprintf(tracefd,"%s\n",line);
		return;
	}
	clkticks -= starttime;

	if ( !capturing || thread >= MAXTHREADS)
		return;
	idx = threads[thread];
	lastclk[thread] = endrange? endrange: clkticks;

	/* track the input in the trace file */
	if( tracefd)
		fprintf(tracefd,"%s\n",line);
	/* start of instruction box */
	if (state == START ) {
		if(debug)
			fprintf(stderr, "Start box %s clicks "LLFMT" stmt %s thread %d idx %d box %d\n", (fcn?fcn:""), clkticks,currentfunction, thread,idx,topbox);
		box[idx].state = state;
		box[idx].thread = thread;
		box[idx].clkstart = box[idx].clkend = clkticks;
		box[idx].memstart = memory;
		box[idx].numa = numa;
		if(numa) updateNumaHeatmap(thread, numa);
		box[idx].footstart = footprint;
		box[idx].stmt = stmt;
		box[idx].fcn = fcn ? fcn : strdup("");
		if(fcn && strstr(fcn,"querylog.define") ){
			// extract a string argument
			currentquery = malarguments[0];
			// use the truncated query text, beware the the \ is already escaped in the call argument.
			q = qry = (char *) malloc(strlen(currentquery) * 2);
			for (c= currentquery; *c; )
				switch(*c){
				case '\\':
					c++;
					switch(*c){
					case 'n': *q++=' '; c++; break;
					case 't': *q++=' '; c++; break;
					case '\\': break;
					default:
						*q++ = *c++;
					}
					break;
				default:
					*q++ = *c++;
				}
			*q =0;
			
			if( strlen(qry) > 93){
				qry[90]= '.';
				qry[91]= '.';
				qry[92]= '.';
				qry[93]= 0;
			}
			currentquery = qry;
			fprintf(stderr,"-- page %d %s:%s\n",atlaspage, (title?title:""), currentquery);
		}
		return;
	}
	/* end the instruction box */
	if (state == DONE && fcn && strncmp(fcn, "function", 8) == 0) {
		if (currentfunction && strcmp(currentfunction, fcn+9) == 0) {
			capturing--;
			if(debug)
				fprintf(stderr, "Leave function %s capture %d\n", currentfunction, capturing);
			if( capturing == 0){
				free(currentfunction);
				currentfunction = 0;
			}
		} 
		if( tracefd){
			fflush(tracefd);
			fclose(tracefd);
			tracefd = NULL;
		}

		createTomogram();
		resetTomograph();
		return;
	}
	if (state == DONE ){
		if( debug)
			fprintf(stderr, "End box %s clicks "LLFMT" : %s thread %d idx %d box %d\n", (fcn?fcn:""), clkticks, (currentfunction?currentfunction:""), thread,idx,topbox);
		events++;
		box[idx].clkend = clkticks;
		box[idx].memend = memory;
		box[idx].footend = footprint;
		box[idx].ticks = ticks;
		box[idx].state = ACTION;
		box[idx].reads = reads;
		box[idx].writes = writes;
		/* focus on part of the time frame */
		if (endrange) {
			if( debug){
				fprintf(stderr,"range filter "LLFMT" " LLFMT"\n",startrange,endrange);
				fprintf(stderr,"expression  "LLFMT" " LLFMT"\n", box[idx].clkstart,  box[idx].clkend);
			}
			if (box[idx].clkend < startrange || box[idx].clkstart >endrange){
				fprintf(stderr,"reject "LLFMT":"LLFMT" out "LLFMT":"LLFMT"\n",box[idx].clkstart , box[idx].clkend, startrange, endrange);
				return;
			}
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
}

/*
 * Beware the UDP protocol may cause some loss
 * Incomplete records from previous runs may also appear
 */

static void
parseArguments(char *call)
{
	int i;
	char  *c = call, *l, ch;
	
	malargtop = 0;
	if( debug)
		fprintf(stderr,"call:%s\n",call);
	for( i=0; c && *c && i < MAXMALARGS; i++, c++){
		if (*c ==')')
			break;
		if (*c == 'X'){
			// skip variable
			c= strchr(c,'=');
			if( c == 0)
				break;
			c++;
			if( debug)
				fprintf(stderr,"arg:%s\n",c);
		}
		if (*c== '\\' && *(c+1) =='"'){
			c++; c++;
			// parse string
			l= strstr(c,"\\\",");
			if ( l == 0)
				l= strstr(c,"\\\")");
			if ( l ==0)
				break;
			*l= 0;
			malarguments[malargtop++] = strdup(c);
			*l = '\\';
			c= l+1;
			if (l[2] ==')')
				break;
		} else {
			// all the rest
			l = strchr(c, ch = ',');
			if( l == 0){
				l = strchr(c, ch = ')');
				break;
				}
			*l=0;
			malarguments[malargtop++] = strdup(c);
			*l=ch;
			c= l;
			l = strchr(malarguments[malargtop-1],')');
			if(l)
				*l =0;
		}
	}
	if( debug)
	for(i=0; i < malargtop; i++)
		fprintf(stderr,"arg[%d] %s\n",i,malarguments[i]);
}

static int
parser(char *row)
{
#ifdef HAVE_STRPTIME
	char *c, *cc, *v, *line = row;
	struct tm stm;
	lng clkticks = 0;
	int thread = 0;
	lng ticks = 0;
	lng memory = 0; /* in MB*/
	lng vmmemory = 0; /* in MB*/
	char *fcn = 0, *stmt = 0;
	char *numa =0;
	int state = 0;
	int eventnr = 0;
	lng reads= 0, writes= 0;
	lng rclm=0, swaps= 0, faults= 0;
	lng userid= 0;

	(void) rclm;
	(void) swaps;
	(void) faults;
	(void) eventnr;
	(void) userid;
	/* check basic validaty first */
	if (row[0] =='#')
		return 0;
	if (row[0] != '[')
		return -1;
	if ((cc= strrchr(row,']')) == 0 || *(cc+1) !=0)
		return -1;

	/* scan event record number */
	c = row+1;
	if (c == 0)
		return -2;
	eventnr = atoi(c + 1);

	/* scan event time" */
	c = strchr(c + 1, '"');
	if (c) {
		/* convert time to epoch in seconds*/
		cc =c;
		memset(&stm, 0, sizeof(struct tm));
		c = strptime(c + 1, "%H:%M:%S", &stm);
		clkticks = (((lng) stm.tm_hour * 60 + stm.tm_min) * 60 + stm.tm_sec) * 1000000;
		if (c == 0)
			return -3;
		if (*c == '.') {
			lng usec;
			/* microseconds */
			usec = strtoll(c + 1, NULL, 10);
			assert(usec >= 0 && usec < 1000000);
			clkticks += usec;
		}
		c = strchr(c + 1, '"');
		if (clkticks < 0) {
			fprintf(stderr, "parser: read negative value "LLFMT" from\n'%s'\n", clkticks, cc);
		}
	} else
		return -3;

	/* scan thread */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -4;
	thread = atoi(c + 1);

	/* scan status */
	c = strchr(c, '"');
	if (c == 0)
		return -5;
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
		c = strchr(c + 1, '"');
		if (c == 0)
			return -5;
	}


	/* scan pc */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -6;
	ticks = strtoll(c + 1, NULL, 10);

	/* scan usec */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -6;
	ticks = strtoll(c + 1, NULL, 10);

	/* scan rssMB */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -7;
	memory = strtoll(c + 1, NULL, 10);

	/* scan vmMB */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -8;
	vmmemory = strtoll(c + 1, NULL, 10);

#ifdef NUMAPROFILING
	for(; *c && *c !='"'; c++) ;
	numa = c+1;
	for(c++; *c && *c !='"'; c++)
		;
	if (*c == 0)
		return -1;
	*c = 0;
	numa= strdup(numa);
	*c = '"';
#endif

	/* scan rdblks */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -9;
	reads = strtoll(c + 1, NULL, 10);

	/* scan wrblks */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -10;
	writes = strtoll(c + 1, NULL, 10);

	/* scan rclm */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -11;
	rclm = strtoll(c + 1, NULL, 10);

	/* scan faults */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -12;
	faults = strtoll(c + 1, NULL, 10);

	/* scan swaps */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -13;
	swaps = strtoll(c + 1, NULL, 10);

	/* scan userid */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -14;
	userid = strtoll(c + 1, NULL, 10);

	/* parse the MAL call, check basic validity */
	c = strchr(c, '"');
	if (c == 0)
		return -15;
	c++;
	fcn = strdup(c);
	stmt = strdup(fcn);
	c=fcn;
	if( *c != '[')
	{
		c = strstr(c + 1, ":=");
		if (c) {
			fcn = c + 2;
			/* find genuine function calls */
			while (isspace((int) *fcn) && *fcn)
				fcn++;
			if (strchr(fcn, '.') == 0) 
				fcn = 0;
		} else {
			v=  strchr(fcn+1,'"');
			if ( v ) *v = 0;
		}
	}

	if (fcn && (v=strchr(fcn, '(')))
		*v = 0;
	if( v)
		parseArguments(v+1);

	update(state, thread, clkticks, ticks, memory, numa, vmmemory, reads, writes, fcn, stmt,line);
	if(numa) free(numa);
#else
	(void) row;
#endif
	return 0;
}

int
main(int argc, char **argv)
{
	int i, n, len;
	char *host = NULL;
	int portnr = 0;
	char *dbname = NULL;
	char *uri = NULL;
	char *user = NULL;
	char *password = NULL;
	char buf[BUFSIZ], *e, *response;
	FILE *trace = NULL;
	FILE *inpfd;
	int colormap=0;

	static struct option long_options[15] = {
		{ "dbname", 1, 0, 'd' },
		{ "user", 1, 0, 'u' },
		{ "port", 1, 0, 'p' },
		{ "password", 1, 0, 'P' },
		{ "host", 1, 0, 'h' },
		{ "help", 0, 0, '?' },
		{ "title", 1, 0, 'T' },
		{ "input", 1, 0, 'i' },
		{ "range", 1, 0, 'r' },
		{ "output", 1, 0, 'o' },
		{ "debug", 0, 0, 'D' },
		{ "beat", 1, 0, 'b' },
		{ "atlas", 1, 0, 'A' },
		{ "map", 1, 0, 'm' },
		{ 0, 0, 0, 0 }
	};

	/* parse config file first, command line options override */
	parse_dotmonetdb(&user, &password, NULL, NULL, NULL, NULL);

	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "d:u:p:P:h:?T:i:r:o:Db:A:m",
					long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 'm':
			colormap =1;
			break;
		case 'A':
			atlas = atoi(optarg ? optarg : "1");
			break;
		case 'b':
			beat = atoi(optarg ? optarg : "100");
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
		case 'T':
			title = optarg;
			break;
		case 'o':
			basefilename = strdup(optarg);
			if( strstr(basefilename,".trace"))
				*strstr(basefilename,".trace") = 0;
			printf("-- Output directed towards %s\n", basefilename);
			break;
		case 'r':
		{
			int cnt;
			if (optarg == 0)
				break;
			if( *optarg == '=')
				optarg++;
			cnt = sscanf(optarg,""LLFMT"-"LLFMT, &startrange,&endrange); 
			if( cnt != 2)
				usageTomograph();
				
			if( strstr(optarg,"ms") ){
				startrange *= 1000;
				endrange *= 1000;
			} else if( strchr(optarg,'s')){
				startrange *= 1000000;
				endrange *= 1000000;
			} else 
				usageTomograph();
			if( debug )
				fprintf(stderr,"Cut out slice "LLFMT" -"LLFMT"\n",startrange,endrange);
			break;
		}
		case '?':
			usageTomograph();
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			exit(strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0 ? 0 : -1);
		default:
			usageTomograph();
			exit(-1);
		}
	}
	initcolors();
	resetTomograph();

	if(debug)
		printf("tomograph -d %s --output=%s\n",dbname,basefilename);

	if (dbname != NULL && strncmp(dbname, "mapi:monetdb://", 15) == 0) {
		uri = dbname;
		dbname = NULL;
	}

	if (colormap) {
		showcolormap(basefilename, 1);
		printf("Color map file '%s.gpl' generated\n", basefilename);
		exit(0);
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

	/* reprocess an existing profiler trace, possibly producing the trace split   */
	snprintf(buf,BUFSIZ,"%s_%02d.trace",basefilename,atlaspage);
	if (inputfile==0 || strcmp(buf, inputfile) ){
		// avoid overwriting yourself
		tracefd = fopen(buf,"w");
		if( tracefd == NULL)
			fprintf(stderr,"Could not create trace file\n");
	}
	if (inputfile) {
		inpfd = fopen(inputfile,"r");
		if (inpfd == NULL ){
			fprintf(stderr,"ERROR Can not access '%s'\n",inputfile);
			exit(0);
		}
		len = 0;
		while ((n = fread(buf + len, 1, BUFSIZ - len, inpfd)) > 0) {
			buf[len + n] = 0;
			response = buf;
			while ((e = strchr(response, '\n')) != NULL) {
				*e = 0;
				i = parser(response);
				if (debug  )
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
	} else {
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

		snprintf(buf,BUFSIZ-1,"profiler.tomograph(%d);", beat);

		if( debug)
			fprintf(stderr,"-- %s\n",buf);
		doQ(buf);
	
		snprintf(buf,BUFSIZ,"%s_%02d.trace",basefilename,atlaspage);
		tracefd = fopen(buf,"w");
		if( tracefd == NULL)
			fprintf(stderr,"Could not create trace file\n");

		len = 0;
		while ((n = mnstr_read(conn, buf + len, 1, BUFSIZ - len)) > 0) {
			buf[len + n] = 0;
			if( trace) 
				fprintf(trace,"%s",buf);
			response = buf;
			while ((e = strchr(response, '\n')) != NULL) {
				*e = 0;
				i = parser(response);
				if (debug  )
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
	}
	if( inputfile && atlas >= atlaspage)
		createTomogram();

	if( !inputfile) 
		doQ("profiler.stop();");
stop_disconnect:
	if( !inputfile) {
		mapi_disconnect(dbh);
		printf("-- connection with server %s closed\n", uri ? uri : host);
	}
	return 0;
}
