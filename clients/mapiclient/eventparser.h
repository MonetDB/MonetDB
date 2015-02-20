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

#ifndef _EVENT_PARSER_
#define _EVENT_PARSER_

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

#define MAXTHREADS 1048
#define MAXBOX 32678	 /* should be > MAXTHREADS */

#define START 1
#define DONE 2
#define ACTION 3
#define PING 4
#define WAIT 5
#define IOSTAT 6
#define GCOLLECT 7
extern char *statenames[];

typedef struct  {
	int state;
	int pc;
	lng eventnr;
	int thread;
	lng clkticks;
	lng ticks;
	lng memory;
	lng vmmemory;
	lng inblock;
	lng oublock;
	lng majflt;
	lng swaps;
	lng csw;
	char *stmt;
	char *fcn;
	char *numa;
} EventRecord;

#define MAXMALARGS 1024
extern char *malarguments[MAXMALARGS];
extern int malargtop;
extern int debug;

extern int eventparser(char *row, EventRecord *ev);
#endif /*_EVENT_PARSER_*/
