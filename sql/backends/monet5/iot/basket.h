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

#ifndef _BASKETS_
#define _BASKETS_

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "sql.h"

#ifdef WIN32
#ifndef LIBDATACELL
#define iot_export extern __declspec(dllimport)
#else
#define iot_export extern __declspec(dllexport)
#endif
#else
#define iot_export extern
#endif

/* #define _DEBUG_DATACELL     debug this module */
#define BSKTout GDKout
#define MAXCOL 128
#define MAXBSK 64

typedef struct{
	MT_Lock lock;
	str schema;	/* schema for the basket */
	str table;	/* table that represents the basket */
	int threshold ; /* bound to determine scheduling eligibility */
	int winsize, winstride; /* sliding window operations */
	lng timeslice, timestride; /* temporal sliding window, determined by first temporal component */
	lng beat;	/* milliseconds delay */
	int count;
	str *cols;
	bat *bats;	/* the BAT ids of the basket representation */

	/* statistics */
	int status;
	timestamp seen;
	int events; /* total number of events grabbed */
	int cycles; 
	/* collected errors */
	BAT *errors;
} *Basket, BasketRec;


#define BSKTINIT 1        
#define BSKTPAUSE 2       /* not active now */
#define BSKTRUNNING 3      
#define BSKTSTOP 4		  /* stop the thread */
#define BSKTERROR 5       /* failed to establish the stream */

#define PAUSEDEFAULT 1000

#define BSKTACTIVE 1      /* ask for events */
#define BSKTPASSIVE 2     /* wait for events */

iot_export BasketRec *baskets;

iot_export str BSKTbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str BSKTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str BSKTreset(void *ret);
iot_export int BSKTlocate(str sch, str tbl);
iot_export str BSKTdump(void *ret);

iot_export str BSKTtable(bat *schemaId, bat *nameId, bat *thresholdId, bat * winsizeId, bat *winstrideId,bat *timesliceId, bat *timestrideId, bat *beatId, bat *seenId, bat *eventsId);
iot_export str BSKTtableerrors(bat *nmeId, bat *errorId);

iot_export str BSKTnewbasket(sql_schema *s, sql_table *t, sql_trans *tr);
iot_export str BSKTdrop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

iot_export int BSKTlocate(str sch, str tbl);
iot_export int BSKTunlocate(str sch, str tbl);
iot_export int BSKTlocate(str sch, str tbl);
iot_export str BSKTupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export InstrPtr BSKTgrabInstruction(MalBlkPtr mb, str sch, str tbl);
iot_export InstrPtr BSKTupdateInstruction(MalBlkPtr mb, str sch, str tbl);

iot_export str BSKTlock(void *ret, str *sch, str *tbl, int *delay);
iot_export str BSKTunlock(void *ret, str *sch, str *tbl);
iot_export str BSKTlock2(void *ret, str *sch, str *tbl);
#endif
