/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _MAL_PROFILER_H
#define _MAL_PROFILER_H

#include "mal_client.h"

enum event_phase {
	MAL_ENGINE = 0,
	CLIENT_START,
	CLIENT_END,
	TEXT_TO_SQL,
	SQL_TO_REL,
	REL_OPT,
	REL_TO_MAL,
	MAL_OPT,
	COMMIT,
	ROLLBACK,
	CONFLICT
};

typedef struct NonMalEvent {
	enum event_phase phase;
	Client cntxt;
	ulng clk;
	ulng* tid;
	ulng* ts;
	int state;
	ulng duration;
} NonMalEvent;

typedef struct MalEvent {
	Client cntxt;
	MalBlkPtr mb;
	MalStkPtr stk;
	InstrPtr pci;
	lng clk;					/* end time */
	lng duration;
} MalEvent;

mal_export int profilerStatus;
mal_export int profilerMode;

mal_export void profilerEvent(MalEvent *me, NonMalEvent *nme);
mal_export oid runtimeProfileSetTag(Client cntxt);

mal_export str startTrace(Client cntxt);
mal_export str stopTrace(Client cntxt);
mal_export void clearTrace(Client cntxt);
mal_export int TRACEtable(Client cntxt, BAT **r);

#ifdef LIBMONETDB5
/* only available in monetdb5 */
extern void initProfiler(void);
extern str openProfilerStream(Client cntxt, int m);
extern str closeProfilerStream(Client cntxt);

extern str startProfiler(Client cntxt);
extern str stopProfiler(Client cntxt);
extern void setHeartbeat(int delay);
extern void initHeartbeat(void);
extern void profilerHeartbeatEvent(char *alter);
extern int getprofilerlimit(void);
extern void setprofilerlimit(int limit);

extern void MPresetProfiler(stream *fdout);

extern str cleanupTraces(Client cntxt);
extern BAT *getTrace(Client cntxt, const char *nme);

extern lng getDiskSpace(void);
extern lng getDiskReads(void);
extern lng getDiskWrites(void);
extern lng getUserTime(void);
extern lng getSystemTime(void);
extern void profilerGetCPUStat(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait);
#endif

#endif
