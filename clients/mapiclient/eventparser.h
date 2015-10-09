/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* (c) M Kersten
 * The easiest calling method is something like:
 * tomograph -d demo --atlast=10
 * which connects to the demo database server and
 * will collect the tomograph pages for at most 10 SQL queries
 * For each page a gnuplot file, a data file, and the event trace
 * are collected for more focussed analysis.
 * 
*/

#ifndef _EVENT_PARSER_
#define _EVENT_PARSER_

#include <mapi.h>
#include <stream.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>

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

#define  MDB_START 1
#define  MDB_DONE 2
#define  MDB_PING 3
#define  MDB_WAIT 4
#define  MDB_SYSTEM 5

extern char *statenames[];

// the break down of a profiler event message
typedef struct  {
	int state;
	char *blk;	// name of MAL block
	int pc;		// instruction counter in block
	int tag;	// unique MAL block invocation tag
	lng eventnr;// serial event number
	int thread;	// worker thread involved
	lng clkticks;
	lng ticks;
	lng memory;
	lng tmpspace;	// size of temporary produced
	lng inblock;
	lng oublock;
	lng majflt;
	lng swaps;
	lng csw;
	char *stmt;	// MAL statement, cpu loads or commentary
	char *fcn;
	char *numa;
} EventRecord;

#define MAXMALARGS 1024
extern char *malarguments[MAXMALARGS];
extern char *maltypes[MAXMALARGS];
extern int malpc[MAXMALARGS];
extern int malcount[MAXMALARGS];
extern int malargc;
extern int malretc;
extern char *malvariables[MAXMALARGS];
extern int malvartop;
extern int debug;
extern char *monetdb_characteristics;

extern void clearArguments(void);
extern void eventdump(void);
extern int eventparser(char *row, EventRecord *ev);
extern char *stripQuotes(const char *currentquery);
#endif /*_EVENT_PARSER_*/
