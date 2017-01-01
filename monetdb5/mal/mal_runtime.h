/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
	lng tag;
	str query;
	str status;
	lng start;
	lng runtime;
} *QueryQueue;

mal_export void runtimeProfileInit(Client cntxt, MalBlkPtr mb, MalStkPtr stk);
mal_export void runtimeProfileFinish(Client cntxt, MalBlkPtr mb);
mal_export void runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof);
mal_export void runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof);
mal_export void finishSessionProfiler(Client cntxt);
mal_export lng getVolume(MalStkPtr stk, InstrPtr pci, int rd);
mal_export lng getBatSpace(BAT *b);

mal_export QueryQueue QRYqueue;
#endif
