/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _MAL_PROFILER_H
#define _MAL_PROFILER_H

#include "mal_client.h"

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

#ifdef HAVE_SYS_RESOURCE_H
# include <sys/resource.h>
typedef struct rusage Rusage;
#endif


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
} MalEvent;

mal_export int profilerStatus;
mal_export int profilerMode;

mal_export void initProfiler(void);
mal_export str openProfilerStream(Client cntxt, int m);
mal_export str closeProfilerStream(Client cntxt);

mal_export void profilerEvent(MalEvent *me, NonMalEvent *nme);
mal_export void sqlProfilerEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export oid runtimeProfileSetTag(Client cntxt);

mal_export str startProfiler(Client cntxt);
mal_export str stopProfiler(Client cntxt);
mal_export str startTrace(Client cntxt);
mal_export str stopTrace(Client cntxt);
mal_export void setHeartbeat(int delay);
mal_export void initHeartbeat(void);
mal_export void profilerHeartbeatEvent(char *alter);
mal_export int getprofilerlimit(void);
mal_export void setprofilerlimit(int limit);

mal_export void MPresetProfiler(stream *fdout);

mal_export void clearTrace(Client cntxt);
mal_export int TRACEtable(Client cntxt, BAT **r);
mal_export str cleanupTraces(Client cntxt);
mal_export BAT *getTrace(Client cntxt, const char *nme);

mal_export lng getDiskSpace(void);
mal_export lng getDiskReads(void);
mal_export lng getDiskWrites(void);
mal_export lng getUserTime(void);
mal_export lng getSystemTime(void);
mal_export void profilerGetCPUStat(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait);
#endif
