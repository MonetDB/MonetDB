/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

mal_export int malProfileMode;

mal_export void initProfiler(void);
mal_export str openProfilerStream(Client cntxt);
mal_export str closeProfilerStream(Client cntxt);

mal_export void profilerEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start);
mal_export void sqlProfilerEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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
