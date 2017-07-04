/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_TIME_H
#include <time.h>
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

#define MAXMALARGS 2048

// the break down of a profiler event message
typedef struct  {
	// system state
	char *version;
	char *release;
	char *threads;
	char *memory;
	char *host;
	int oid;
	char *package;

	// event state
	int index; // return/arg index
	int state;
	char *function;	// name of MAL block
	char *user; 
	int pc;		// instruction counter in block
	int tag;	// unique MAL block invocation tag
	lng eventnr;// serial event number
	int thread;	// worker thread involved
	lng usec;	// usec since start of session
	char *time;	// string rep of clock
	lng clkticks;
	lng ticks;
	lng rss;
	lng size;	// size of temporary produced
	lng inblock;
	lng oublock;
	lng majflt;
	lng swaps;
	lng csw;
	char *stmt;	// MAL statement, cpu loads or commentary
	char *beauty;// MAL statement compressed
	char *fcn;	// MAL operator
	char *numa;
	char *prereq;	// pre-requisite statements
} EventRecord;

extern char *maltypes[MAXMALARGS];
extern char *malvariables[MAXMALARGS];
extern char *malvalues[MAXMALARGS];
extern int malsize;
extern int debug;
extern char *currentquery;

extern void resetEventRecord(EventRecord *ev);
extern void eventdump(void);
extern int keyvalueparser(char *txt, EventRecord *ev);
extern int lineparser(char *row, EventRecord *ev);
extern void renderJSONevent(FILE *fd, EventRecord *ev, int notfirst);
extern char *stripQuotes(char *currentquery);
#endif /*_EVENT_PARSER_*/
