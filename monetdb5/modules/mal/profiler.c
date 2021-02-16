/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * Martin Kersten
 * Performance profiler
 * A key issue in developing fast programs using the Monet database
 * back-end requires a keen eye on where performance is lost.
 * Although performance tracking and measurements are highly
 * application dependent, a simple to use tool makes life
 * a lot easier.
 *
 * Activation of the performance monitor has a global effect,
 * i.e. all concurrent actions on the kernel are traced,
 * but the events are only sent to the client initiated
 * the profiler thread.
 *
 * The profiler event can be handled in several ways.
 * The default strategy is to ship the event record immediately over a stream
 * to a performance monitor.
 * An alternative strategy is preparation of off-line performance analysis.
 *
 * To reduce the  interference of performance measurement with
 * the experiments, the user can use an event cache, which is
 * emptied explicitly upon need.
 */
/*
 * Using the Monet Performance Profiler is constrained by the mal_profiler.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include <time.h>
#include "mal_stack.h"
#include "mal_resolve.h"
#include "mal_exception.h"
#include "mal_client.h"
#include "mal_profiler.h"
#include "mal_interpreter.h"
#include "mal_runtime.h"

static str
CMDopenProfilerStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pc;
	return openProfilerStream(cntxt);
}

static str
CMDcloseProfilerStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void) mb;
	(void) stk;
	(void) pc;
	return closeProfilerStream(cntxt);
}

// initialize SQL tracing
static str
CMDstartProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void)mb;
	(void) stk;
	(void) pc;
	(void) cntxt;
	return startProfiler(cntxt);
}

static str
CMDstopProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;

	return stopProfiler(cntxt);
}

// called by the SQL front end optional a directory to keep the traces.
static str
CMDstartTrace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return startTrace(cntxt);
}

static str
CMDstopTrace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return stopTrace(cntxt);
}

static str
CMDnoopProfiler(void *res)
{
	(void) res;		/* fool compiler */
	return MAL_SUCCEED;
}

static str
CMDcleanupTraces(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	cleanupTraces(cntxt);
	return MAL_SUCCEED;
}

static str
CMDgetTrace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str path = *getArgReference_str(stk,pci,1);
	bat *res =  getArgReference_bat(stk,pci,0);
	BAT *bn;

	(void) cntxt;		/* fool compiler */
	(void) mb;
	bn = getTrace(cntxt, path);
	if (bn) {
		BBPkeepref(*res = bn->batCacheid);
		return MAL_SUCCEED;
	}
	throw(MAL, "getTrace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING  "%s", path);
}

static str
CMDgetprofilerlimit(int *res)
{
	*res = getprofilerlimit();
	return MAL_SUCCEED;
}

static str
CMDsetprofilerlimit(void *res, int *lim)
{
	(void) res;
	setprofilerlimit(*lim);
	return MAL_SUCCEED;
}

/*
 * Tracing an active system.
 */

static str
CMDsetHeartbeat(void *res, int *ev)
{
	(void) res;
	setHeartbeat(*ev);
	return MAL_SUCCEED;
}

static str
CMDgetDiskReads(lng *ret)
{
	*ret= getDiskReads();
	return MAL_SUCCEED;
}
static str
CMDgetDiskWrites(lng *ret)
{
	*ret= getDiskWrites();
	return MAL_SUCCEED;
}
static str
CMDgetUserTime(lng *ret)
{
	*ret= getUserTime();
	return MAL_SUCCEED;
}
static str
CMDgetSystemTime(lng *ret)
{
	*ret= getUserTime();
	return MAL_SUCCEED;
}

static str
CMDcpustats(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait)
{
	profilerGetCPUStat(user,nice,sys,idle,iowait);
	return MAL_SUCCEED;
}

static str
CMDcpuloadPercentage(int *cycles, int *io, lng *user, lng *nice, lng *sys, lng *idle, lng *iowait)
{
	lng userN, niceN, sysN, idleN, iowaitN, N;
	*cycles = 0;
	*io = 0;
	profilerGetCPUStat(&userN,&niceN,&sysN,&idleN,&iowaitN);
	N = (userN - *user + niceN - *nice + sysN - *sys);
	if ( N){
		*cycles = (int) ( ((double) N) / (N + idleN - *idle + iowaitN - *iowait) *100);
		*io = (int) ( ((double) iowaitN- *iowait) / (N + idleN - *idle + iowaitN - *iowait) *100);
	}
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func profiler_init_funcs[] = {
 pattern("profiler", "start", CMDstartProfiler, false, "Start offline performance profiling", noargs),
 pattern("profiler", "stop", CMDstopProfiler, false, "Stop offline performance profiling", args(1,1, arg("",void))),
 pattern("profiler", "starttrace", CMDstartTrace, false, "Start collecting trace information", noargs),
 pattern("profiler", "stoptrace", CMDstopTrace, false, "Stop collecting trace information", args(1,1, arg("",void))),
 command("profiler", "setheartbeat", CMDsetHeartbeat, false, "Set heart beat performance tracing", args(1,2, arg("",void),arg("b",int))),
 command("profiler", "getlimit", CMDgetprofilerlimit, false, "Set profiler limit", args(1,1, arg("",int))),
 command("profiler", "setlimit", CMDsetprofilerlimit, false, "Get profiler limit", args(1,2, arg("",void),arg("l",int))),
 pattern("profiler", "openstream", CMDopenProfilerStream, false, "Start profiling the events, send to output stream", args(1,1, arg("",void))),
 pattern("profiler", "closestream", CMDcloseProfilerStream, false, "Stop offline proviling", args(1,1, arg("",void))),
 command("profiler", "noop", CMDnoopProfiler, false, "Fetch any pending performance events", args(1,1, arg("",void))),
 pattern("profiler", "getTrace", CMDgetTrace, false, "Get the trace details of a specific event", args(1,2, batargany("",1),arg("e",str))),
 pattern("profiler", "cleanup", CMDcleanupTraces, false, "Remove the temporary tables for profiling", args(1,1, arg("",void))),
 command("profiler", "getDiskReads", CMDgetDiskReads, false, "Obtain the number of physical reads", args(1,1, arg("",lng))),
 command("profiler", "getDiskWrites", CMDgetDiskWrites, false, "Obtain the number of physical reads", args(1,1, arg("",lng))),
 command("profiler", "getUserTime", CMDgetUserTime, false, "Obtain the user timing information.", args(1,1, arg("",lng))),
 command("profiler", "getSystemTime", CMDgetSystemTime, false, "Obtain the user timing information.", args(1,1, arg("",lng))),
 command("profiler", "cpustats", CMDcpustats, false, "Extract cpu statistics from the kernel", args(5,5, arg("user",lng),arg("nice",lng),arg("sys",lng),arg("idle",lng),arg("iowait",lng))),
 command("profiler", "cpuload", CMDcpuloadPercentage, false, "Calculate the average cpu load percentage and io waiting times", args(2,7, arg("cycles",int),arg("io",int),arg("user",lng),arg("nice",lng),arg("sys",lng),arg("idle",lng),arg("iowait",lng))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_profiler_mal)
{ mal_module("profiler", NULL, profiler_init_funcs); }
