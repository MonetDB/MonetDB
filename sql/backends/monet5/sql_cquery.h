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
#include "sql_catalog.h"
#include "sql_scenario.h"

//#define DEBUG_CQUERY
//#define DEBUG_CQUERY_SCHEDULER

#define CQINIT        0 /* scheduler about to start */
#define CQWAIT 	      1 /* wait for data */
#define CQRUNNING     2 /* query is running */
#define CQPAUSE       3 /* not active now */
#define CQERROR       4 /* query got an error, hence is now paused */
#define CQSTOP	      5 /* request to stop the CQ by the user */
#define CQDELETE      6 /* request to stop the CQ by the scheduler itself */
#define CQDEREGISTER  7 /* CQ about to be deleted */

#define INITIAL_MAXCQ  32 /* it is the minimum, if we need more space GDKrealloc */
#define MAXSTREAMS    128 /* limit the number of stream columns to be looked after per query*/

#define STREAM_IN   1
#define STREAM_OUT  2
#define CQ_OUT      3  /* Output stream in continuous functions */

typedef struct {
	sql_func *func; /* The UDF to be called */
	MalBlkPtr mb;   /* The wrapped query block call in a transaction */
	MalStkPtr stk;  /* Needed for execution */

	int status;     /* query status .../wait/running/paused */
	int enabled;
	str alias;		/* the created alias for the continuous query */

	int baskets[MAXSTREAMS];	/* reference into the registered basket tables catalog */
	int inout[MAXSTREAMS]; /* how the stream tables are used, needed for locking */

	int cycles;		/* limit the number of invocations before dying */
	lng beats;		/* heart beat stride for procedures activations -> must be in microseconds */
	lng run;		/* start at the CQ at that precise moment (UNIX timestamp) -> must be in microseconds */

	timestamp seen; /* last time the query was seen by the scheduler */
	str error;      /* error message if happened during a call */
	lng time;       /* the amount of time the last call took in microseconds */
} CQnode;

sql5_export int CQlocateUDF(sql_func *f);
sql5_export int CQlocateBasketExternal(str schname, str tblname);

sql5_export str CQwait(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQbeginAt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQcycles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQheartbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str CQshow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQstatus(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str CQlog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str CQregister(Client cntxt, str sname, str fname, int argc, atom **args, str alias, int which, lng heartbeats, lng startat, int cycles);
sql5_export str CQresume(str alias, int with_alter, lng heartbeats, lng startat, int cycles);
sql5_export str CQpause(str alias);
sql5_export str CQderegister(Client cntxt, str alias);

sql5_export str CQpauseAll(void);
sql5_export str CQresumeAll(void);
sql5_export str CQderegisterAll(Client cntxt);

sql5_export str CQdump(void *ret);
sql5_export void CQreset(void);
sql5_export str CQprelude(void *ret);

#endif
