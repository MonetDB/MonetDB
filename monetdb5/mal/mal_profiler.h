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
 * Copyright August 2008-2014 MonetDB B.V.
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

/*
 * Recycler statistics per client
 */
typedef struct RECSTAT {
	int statements;   /* total number of statements executed */
	int recycled;     /* total number of statements recycled */
	int recycled0;    /* recycled statements per query */
	lng time0;        /* time per query */
	int curQ;         /* index of current query in Qry Patterns array*/
	int recent;       /* the most recent entry in RP touched by current query */
	int recycleMiss;  /* DBG:count of misses due to cache eviction */
	int recycleRem;   /* DBG:count of removed entries */
	lng ccCalls;      /* Number of calls to cleanCache */
	lng ccInstr;      /* Number of instructions evicted by eviction policy*/
	lng crdInstr;     /* Number of instructions not admited in RP by CRD */
	int trans;        /* Number of data transfer instructions */
	lng transKB;      /* Size in KB of transferred data */
	int recTrans;     /* Number of recycled data transfer instructions */
	lng recTransKB;   /* Size in KB of recycled transferred data */
	int RPadded0;     /* Number of instructions added to RP per query */
	int RPreset0;     /* Number of instructions evicted from RP by reset() due to updates*/
} *RecPtr, RecStat;

mal_export str activateCounter(str name);
mal_export str deactivateCounter(str name);
mal_export str openProfilerStream(stream *fd);
mal_export str closeProfilerStream(void);

mal_export void initProfiler(MalBlkPtr mb);
mal_export void profilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, int pc, int start);
mal_export void profilerHeartbeatEvent(str msg);
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

mal_export int instrFilter(InstrPtr pci, str mod, str fcn);
mal_export void setFilter(Module cntxt, str mod, str fcn);
mal_export void setFilterOnBlock(MalBlkPtr mb, str mod, str fcn);
mal_export void clrFilter(Module cntxt, str mod, str fcn);
mal_export void setFilterVariable(MalBlkPtr mb, int i);
mal_export void clrFilterVariable(MalBlkPtr mb, int i);
mal_export stream *getProfilerStream(void);

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
mal_export void _initTrace(void);

#endif
