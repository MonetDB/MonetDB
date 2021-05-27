/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _MAL_RUNTIME_H
#define _MAL_RUNTIME_H

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
mal_export size_t qhead, qtail, qsize;

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
mal_export size_t usrstatscnt;

typedef struct WORKINGSET{
	Client		cntxt;
	MalBlkPtr   mb;
	MalStkPtr   stk;
	InstrPtr    pci;
} Workingset;

mal_export Workingset workingset[THREADS];

mal_export void runtimeProfileInit(Client cntxt, MalBlkPtr mb, MalStkPtr stk);
mal_export void runtimeProfileFinish(Client cntxt, MalBlkPtr mb, MalStkPtr stk);
mal_export void runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof);
mal_export void runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof);
mal_export lng getVolume(MalStkPtr stk, InstrPtr pci, int rd);
mal_export lng getBatSpace(BAT *b);

mal_export QueryQueue QRYqueue;
mal_export UserStats USRstats;
#endif
