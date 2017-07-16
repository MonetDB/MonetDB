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
 * Copyright August 2008-2016 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _CQUERY_
#define _CQUERY_
#include "mal_interpreter.h"
#include "sql_scenario.h"

//#define DEBUG_CQUERY
//#define DEBUG_CQUERY_SCHEDULER

#define REGISTER_CQUERY           1
#define REGISTER_AND_START_CQUERY 2
#define RESTART_CQUERY            3

#define CQINIT     0
#define CQREGISTER 1    /* being registered */
#define CQWAIT 	   2    /* wait for data */
#define CQRUNNING  3	/* query is running */
#define CQPAUSE    4    /* not active now */
#define CQSTOP	   5	/* stop the scheduler */

#define PAUSEDEFAULT 1000
#define MAXCQ 200           /* it is the minimum, if we need more space GDKrealloc */
#define MAXSTREAMS 128		/* limit the number of stream columns to be looked after per query*/

#define STREAM_IN	1
#define STREAM_OUT	4

typedef struct {
	str mod,fcn;	/* The SQL command to be used */
	MalBlkPtr mb;   /* The wrapped query block call in a transaction */
	MalStkPtr stk;  /* Needed for execution */

	int status;     /* query status .../wait/running/paused */
	int enabled;

	int baskets[MAXSTREAMS];	/* reference into the registered basket tables catalog */
	int inout[MAXSTREAMS]; /* how the stream tables are used, needed for locking */

	int cycles;		/* limit the number of invocations before dying */
	lng beats;		/* heart beat stride for procedures activations */

	MT_Id	tid;	/* Thread responsible */
	lng		run;	/* last executed relative to start of server */
	timestamp seen;
	str error;
	lng time;
} CQnode;

sql5_export CQnode pnet[MAXCQ];
sql5_export int pnettop;
sql5_export MT_Lock ttrLock;

sql5_export str CQregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQregisterMAL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci );
sql5_export str CQresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQderegister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQderegisterInternal(str modnme, str fcnnme, int force);
sql5_export str CQwait(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQcycles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQheartbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQerror(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str CQshow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQstatus(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQlog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQdump(void *ret);
sql5_export str CQprelude(void *ret);
#endif
