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

#define CQINIT     0
#define CQREGISTER 1    /* being registered */
#define CQWAIT 	   2    /* wait for data */
#define CQRUNNING  3	/* query is running */
#define CQPAUSE    4    /* not active now */
#define CQSTOP	   5	/* stop the scheduler */
#define CQDEREGISTER  6	/* stop the scheduler */

#define INITIAL_MAXCQ     32	/* it is the minimum, if we need more space GDKrealloc */
#define MAXSTREAMS       128	/* limit the number of stream columns to be looked after per query*/

#define STREAM_IN	1
#define STREAM_OUT	4

typedef struct {
	str mod,fcn;	/* The SQL command to be used */
	MalBlkPtr mb;   /* The wrapped query block call in a transaction */
	MalStkPtr stk;  /* Needed for execution */

	int status;     /* query status .../wait/running/paused */
	int enabled;
	str stmt;		/* actual statement call */

	int baskets[MAXSTREAMS];	/* reference into the registered basket tables catalog */
	int inout[MAXSTREAMS]; /* how the stream tables are used, needed for locking */

	int cycles;		/* limit the number of invocations before dying */
	lng beats;		/* heart beat stride for procedures activations -> must be in microseconds */
	lng run;		/* start at the CQ at that precise moment (UNIX timestamp) -> must be in microseconds */

	MT_Id	tid;	/* Thread responsible */
	timestamp seen;
	str error;
	lng time;
} CQnode;

sql5_export CQnode *pnet;
sql5_export int pnetLimit, pnettop;

sql5_export int CQlocateQueryExternal(str modname, str fcnname);
sql5_export int CQlocateBasketExternal(str schname, str tblname);

sql5_export str CQregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQprocedure(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQresumeNoAlter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQresumeAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQpauseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQderegister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQderegisterAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQwait(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQbeginAt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQcycles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQheartbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQerror(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str CQshow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQstatus(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQlog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str CQdump(void *ret);
sql5_export void CQreset(void);
sql5_export str CQprelude(void *ret);
#endif
