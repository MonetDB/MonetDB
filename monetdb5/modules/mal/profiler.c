/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "profiler.h"

str
CMDopenProfilerStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pc;
	return openProfilerStream(cntxt->fdout, *getArgReference_int(stk,pc,1));
}

str
CMDcloseProfilerStream(void *res)
{
	(void) res;
	return closeProfilerStream();
}

// initialize SQL tracing
str
CMDstartProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void)mb;
	(void) stk;
	(void) pc;
	(void) cntxt;
	return startProfiler();
}

str
CMDstopProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	return stopProfiler();
}

// called by the SQL front end.
str
CMDstartTrace(void *res)
{
	(void) res;
	return startTrace(0);
}

// if you haven't started the stethoscope
// then the output is saved in a file 
str
CMDstartTracePath(void *res, str *path)
{
	(void) res;
	return startTrace(*path);
}

str
CMDstopTrace(void *res)
{
	(void) res;
	return stopTrace(0);
}

str
CMDstopTracePath(void *res, str *path)
{
	(void) res;
	return stopTrace(*path);
}

str
CMDnoopProfiler(void *res)
{
	(void) res;		/* fool compiler */
	return MAL_SUCCEED;
}

str
CMDcleanupTraces(void *res)
{
	(void) res;		/* fool compiler */
	cleanupTraces();
	return MAL_SUCCEED;
}

#if 0
str
CMDclearTrace(void *res)
{
	(void) res;		/* fool compiler */
	clearTrace();
	return MAL_SUCCEED;
}
#endif	/* unused */

str
CMDgetTrace(bat *res, str *ev)
{
	BAT *bn;

	(void) res;		/* fool compiler */
	bn = getTrace(*ev);
	if (bn) {
		BBPkeepref(*res = bn->batCacheid);
		return MAL_SUCCEED;
	}
	throw(MAL, "getTrace", RUNTIME_OBJECT_MISSING  "%s",*ev);
}
/*
 * Tracing an active system.
 */

str
CMDsetHeartbeat(void *res, int *ev)
{
	(void) res;
	setHeartbeat(*ev);
	return MAL_SUCCEED;
}

str
CMDgetprofilerlimit(int *res)
{
	*res = getprofilerlimit();
	return MAL_SUCCEED;
}

str
CMDsetprofilerlimit(void *res, int *ev)
{
	(void) res;
	setprofilerlimit(*ev);
	return MAL_SUCCEED;
}

str
CMDgetDiskReads(lng *ret)
{
	*ret= getDiskReads();
	return MAL_SUCCEED;
}
str
CMDgetDiskWrites(lng *ret)
{
	*ret= getDiskWrites();
	return MAL_SUCCEED;
}
str
CMDgetUserTime(lng *ret)
{
	*ret= getUserTime();
	return MAL_SUCCEED;
}
str
CMDgetSystemTime(lng *ret)
{
	*ret= getUserTime();
	return MAL_SUCCEED;
}

str
CMDcpustats(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait)
{
	profilerGetCPUStat(user,nice,sys,idle,iowait);
	return MAL_SUCCEED;
}

str
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
