/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

#include "mapi.h"
#include "stream.h"
#include <string.h>
#include <sys/stat.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>

#define TME_US  1
#define TME_MS  2
#define TME_SS  4
#define TME_MM  8
#define TME_HH 16
#define TME_DD 32

#define US_MS ((int64_t) 1000)
#define US_SS (US_MS * 1000)
#define US_MM (US_SS * 60)
#define US_HH (US_MM * 60)
#define US_DD (US_HH * 24)

#define MDB_RET 1
#define MDB_ARG 2

typedef struct{
	int index;
	int kind;	// MDB_RET, MDB_ARG
    int	bid;
	int constant;
	long count;
	char *alias;
	char *name;
	char *type;
	char *view;
	char *parent;
	char *file;
	char *persistence;
	char *seqbase;
	char *sorted;
	char *revsorted;
	char *nonil;
	char *nil;
	char *key;
	char *unique;
	char *size;
	char *value;
	char *debug;
} Argrecord;

// the break down of a profiler event message
typedef struct  {
	char *version;
	char *user;
	char *session;
	char *function;	// name of MAL block
	char *operator;
	char *module, *instruction;
	char *state;
	char *stmt;
	int thread;	// worker thread involved
	int pc;		// instruction counter in block
	int tag;	// unique MAL block invocation tag
	int64_t usec;	// usec since start of session
	char *time;	// string rep of clock
	int64_t clkticks;
	int64_t ticks;
	int64_t rss;
	int64_t inblock;
	int64_t oublock;
	int64_t majflt;
	int64_t swaps;
	int64_t nvcsw;
	int maxarg;
	Argrecord *args;
} EventRecord;

extern int debug;
extern int keyvalueparser(char *txt, EventRecord *ev);
extern void renderHeader(FILE *fd);
extern void renderSummary(FILE *fd, EventRecord *ev, char *filter);
#endif /*_EVENT_PARSER_*/
