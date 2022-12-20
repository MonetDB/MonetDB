/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _MAL_RUNTIME_H
#define _MAL_RUNTIME_H

#ifndef LIBMONETDB5
#error this file should not be included outside its source directory
#endif

#include "mal.h"
#include "mal_client.h"
#include "mal_instruction.h"

/* During MAL interpretation we collect performance event data.
 * Their management is orchestrated from here.
 * We need to maintain some state from ProfileBegin
*/
typedef struct{
	lng ticks;			/* at start of this profile interval */
} *RuntimeProfile, RuntimeProfileRecord;

/* The actual running queries are assembled in a queue
 * for external inspection and manipulation
 */
typedef struct QRYQUEUE{
	Client cntxt;
	MalBlkPtr mb;
	MalStkPtr stk;
	oid tag;
	str query;
	str status;
	str username;
	int idx;
	int workers;
	int memory;
	lng ticks;
	time_t start;
	time_t finished;
} *QueryQueue;

/* We keep a few statistics per user to identify unexpected behavior */
typedef struct USERSTAT{
	oid user;       /* user id in the auth administration */
	str username;
	lng querycount;
	lng totalticks;
	time_t started;
	time_t finished;
	lng maxticks;
	str maxquery;
} *UserStats;
extern size_t usrstatscnt;

typedef struct WORKINGSET{
	Client		cntxt;
	MalBlkPtr   mb;
	MalStkPtr   stk;
	InstrPtr    pci;
	lng         clock;			/* start time */
} Workingset;

extern Workingset workingset[THREADS];

extern void runtimeProfileInit(Client cntxt, MalBlkPtr mb, MalStkPtr stk);
extern void runtimeProfileFinish(Client cntxt, MalBlkPtr mb, MalStkPtr stk);
extern void runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof);
extern void runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof);
extern lng getVolume(MalStkPtr stk, InstrPtr pci, int rd);
extern lng getBatSpace(BAT *b);
extern void sqlProfilerEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, lng clk, lng ticks);

extern QueryQueue QRYqueue;
extern UserStats USRstats;
#endif
