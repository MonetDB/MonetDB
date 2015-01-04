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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
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
 * @- Monet Event Logger
 * The Monet Event Logger generates records of each event of
 * interest indicated by a log filter, i.e. a pattern over
 * module and function names.
 *
 * The log record contents is derived from counters being
 * (de-)activated.
 * A complete list of recognized counters is shown below.
 *
 * @- Execution tracing
 * Tracing is a special kind of profiling, where the information
 * gathered is not sent to a remote system, but stored in the database
 * itself. Each profile event is given a separate BAT
 *
 * @verbatim
 * # thread and time since start
 * profiler.activate("tick");
 * # cpu time in nano-seconds
 * profiler.activate("cpu");
 * # memory allocation information
 * profiler.activate("memory");
 * # IO activity
 * profiler.activate("io");
 * # Module,function,program counter
 * profiler.activate("pc");
 * # actual MAL instruction executed
 * profiler.activate("statement");
 * @end verbatim
 * @-
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
CMDactivateProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	str msg= MAL_SUCCEED;

	(void) cntxt;		/* fool compiler */
	(void) mb;		/* fool compiler */
	for ( i= pci->retc; i < pci->argc && msg == MAL_SUCCEED; i++)
			msg =activateCounter(*getArgReference_str(stk,pci,i));
	return msg;
}

str
CMDdeactivateProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	str msg= MAL_SUCCEED;

	(void) cntxt;		/* fool compiler */
	(void) mb;		/* fool compiler */
	for ( i= pci->retc; i < pci->argc && msg == MAL_SUCCEED; i++)
			msg =deactivateCounter(*getArgReference_str(stk,pci,i));
	return msg;
}

str
CMDsetFilterProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	(void) mb;		/* fool compiler */
	setFilter(cntxt->nspace, *mod, *fcn);
	return MAL_SUCCEED;
}

str
CMDsetAllProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void) mb;		/* fool compiler */
	(void) stk;
	(void) pc;
	setFilter(cntxt->nspace, "*", "*");
	return MAL_SUCCEED;
}

str
CMDsetFilterVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void) cntxt;
	(void) stk;
	setFilterVariable(mb,getArg(pc,1));
	return MAL_SUCCEED;
}

str
CMDopenProfilerStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pc;
	return openProfilerStream(cntxt->fdout);
}

str
CMDcloseProfilerStream(void *res)
{
	(void) res;
	return closeProfilerStream();
}

str
CMDclrFilterProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	(void) mb;		/* fool compiler */
	clrFilter(cntxt->nspace, *mod, *fcn);
	return MAL_SUCCEED;
}

str
CMDsetNoneProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;		/* fool compiler */
	(void) stk;
	(void) pci;
	clrFilter(cntxt->nspace, "", "");
	return MAL_SUCCEED;
}

str
CMDsetProfilerFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *fnme = getArgReference_str(stk,pci,1);
	(void) mb;		/* fool compiler */
	return setLogFile(cntxt->fdout,cntxt->nspace, *fnme);
}

str
CMDsetProfilerStream (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *host = getArgReference_str(stk,pci,1);
	int *port = getArgReference_int(stk,pci,2);
	(void) mb;		/* fool compiler */
	return setLogStream(cntxt->nspace, *host, *port);
}

str
CMDstartPointProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	(void) mb;		/* fool compiler */
	return setStartPoint(cntxt->nspace, *mod, *fcn);
}

str
CMDendPointProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	(void) mb;		/* fool compiler */
	return setStartPoint(cntxt->nspace, *mod, *fcn);
}

str
CMDstopProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	return stopProfiling();
}

str
CMDstartProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	return startProfiling();
}
str
CMDnoopProfiler(void *res)
{
	(void) res;		/* fool compiler */
	return MAL_SUCCEED;
}

/*
 * @-
 * Tracing an active system.
 */
str
CMDclearTrace(void *res)
{
	(void) res;		/* fool compiler */
	clearTrace();
	return MAL_SUCCEED;
}

str
CMDdumpTrace(void *res)
{
	(void) res;		/* fool compiler */
	throw(MAL, "profiler.dump", PROGRAM_NYI);
}

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

str
CMDcleanup(void *ret){
	(void) ret;
	return cleanupProfiler();
}

str
CMDgetEvent( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	lng *clk, *reads, *writes, pc;
	clk = getArgReference_lng(stk,pci,0);
	reads = getArgReference_lng(stk,pci,1);
	writes = getArgReference_lng(stk,pci,2);

	(void) cntxt;

	pc= getPC(mb,pci)-1; /* take previous instruction */
	*clk = 0;
	*reads = mb->stmt[pc]->rbytes;
	*writes = mb->stmt[pc]->wbytes;
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
CMDtomograph(void *ret)
{
	(void) ret;
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

str
CMDgetFootprint( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	lng *l= getArgReference_lng(stk,pci,0);

	(void) cntxt;
	(void) mb;
	(void) pci;
	*l = stk->tmpspace;
	stk->tmpspace = 0;
	return MAL_SUCCEED;
}
