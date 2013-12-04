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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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

typedef struct tms Tms;
typedef struct Mallinfo Mallinfo;

#define PROFevent   0
#define PROFtime    1
#define PROFthread  2
#define PROFpc      3
#define PROFfunc    4
#define PROFticks   5
#define PROFcpu     6
#define PROFmemory  7
#define PROFreads   8
#define PROFwrites  9
#define PROFrbytes  10
#define PROFwbytes  11
#define PROFstmt    12
#define PROFaggr    13
#define PROFprocess 14
#define PROFuser    15
#define PROFstart   16
#define PROFtype    17
#define PROFdot     18
#define PROFflow   19
#define PROFping   20	/* heartbeat ping messages */
#define PROFfootprint 21

mal_export str activateCounter(str name);
mal_export str deactivateCounter(str name);
mal_export int getProfileCounter(int idx);
mal_export str openProfilerStream(stream *fd);
mal_export str closeProfilerStream(void);

mal_export void initProfiler(MalBlkPtr mb);
mal_export void profilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, int pc, int start);
mal_export void profilerHeartbeatEvent(str msg, lng ticks);
mal_export str setLogFile(stream *fd, Module cntxt, str fname);
mal_export str setLogStream(Module cntxt, str host, int port);
mal_export str setLogStreamStream(Module cntxt, stream *s);
mal_export str setStartPoint(Module cntxt, str mod, str fcn);
mal_export str setEndPoint(Module cntxt, str mod, str fcn);

mal_export int profilerAvailable(void);
mal_export str startProfiling(void);
mal_export str stopProfiling(void);
mal_export str cleanupProfiler(void);
mal_export void initHeartbeat(void);
mal_export void stopHeartbeat(void);
mal_export double HeartbeatCPUload(void);

mal_export int instrFilter(InstrPtr pci, str mod, str fcn);
mal_export void setFilter(Module cntxt, str mod, str fcn);
mal_export void setFilterOnBlock(MalBlkPtr mb, str mod, str fcn);
mal_export void clrFilter(Module cntxt, str mod, str fcn);
mal_export void setFilterVariable(MalBlkPtr mb, int i);
mal_export void clrFilterVariable(MalBlkPtr mb, int i);
mal_export stream *getProfilerStream(void);
mal_export void setFilterAll(void);

mal_export void MPresetProfiler(stream *fdout);

mal_export int malProfileMode;

mal_export void clearTrace(void);
mal_export BAT *getTrace(str ev);
mal_export int getTraceType(str nme);
mal_export void TRACEtable(BAT **r);

mal_export lng getDiskSpace(void);
mal_export lng getDiskReads(void);
mal_export lng getDiskWrites(void);
mal_export lng getUserTime(void);
mal_export lng getSystemTime(void);
mal_export void profilerGetCPUStat(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait);
mal_export void _initTrace(void);

#endif
