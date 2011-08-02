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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f profiler
 * @a Martin Kersten
 * @+ Performance profiler
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
 * @-
 * Using the Monet Performance Profiler is constrained by the mal_profiler.
 */
#include "monetdb_config.h"
#include "profiler.h"
#include "mal_client.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define profiler_export extern __declspec(dllimport)
#else
#define profiler_export extern __declspec(dllexport)
#endif
#else
#define profiler_export extern
#endif

profiler_export str CMDactivateProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDdeactivateProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDsetFilterProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDsetAllProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
profiler_export str CMDsetFilterVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
profiler_export str CMDclrFilterVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
profiler_export str CMDclrFilterProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDsetNoneProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDsetProfilerFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDsetProfilerStream (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDstartPointProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDendPointProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDstopProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDstartProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDnoopProfiler(int *res);
profiler_export str CMDclearTrace(int *res);
profiler_export str CMDdumpTrace(int *res);
profiler_export str CMDgetTrace(int *res, str *ev);
profiler_export str CMDopenProfilerStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDcloseProfilerStream(int *res);
profiler_export str CMDcleanup(int *ret);
profiler_export str CMDgetEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDclearEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDgetDiskReads(lng *ret);
profiler_export str CMDgetDiskWrites(lng *ret);
profiler_export str CMDgetUserTime(lng *ret);
profiler_export str CMDgetSystemTime(lng *ret);
profiler_export str CMDsetFootprintFlag( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDsetMemoryFlag( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDgetMemory( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDgetFootprint( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define checkProfiler(X) \
	if( ! profilerAvailable()) \
	throw(MAL, "profiler." X,\
	OPERATION_FAILED " Monet not compiled for performance monitoring");


str
CMDactivateProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	str msg= MAL_SUCCEED;

	(void) cntxt;		/* fool compiler */
	(void) mb;		/* fool compiler */
	checkProfiler("activate");
	for ( i= pci->retc; i < pci->argc && msg == MAL_SUCCEED; i++)
			msg =activateCounter(*(str*) getArgReference(stk,pci,i));
	return msg;
}

str
CMDdeactivateProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	str msg= MAL_SUCCEED;

	(void) cntxt;		/* fool compiler */
	(void) mb;		/* fool compiler */
	checkProfiler("deactivate");
	for ( i= pci->retc; i < pci->argc && msg == MAL_SUCCEED; i++)
			msg =deactivateCounter(*(str*) getArgReference(stk,pci,i));
	return msg;
}

str
CMDsetFilterProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	(void) mb;		/* fool compiler */
	checkProfiler("setFilter");
	setFilter(cntxt->nspace, *mod, *fcn);
	return MAL_SUCCEED;
}

str
CMDsetAllProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	str x = GDKstrdup("*");
	str y = GDKstrdup("*");

	(void) mb;		/* fool compiler */
	(void) stk;
	(void) pc;
	checkProfiler("setFilter");
	setFilter(cntxt->nspace, x, y);
	GDKfree(x);
	GDKfree(y);
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
CMDclrFilterVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc)
{
	(void) cntxt;
	(void) stk;
	clrFilterVariable(mb,getArg(pc,1));
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
CMDcloseProfilerStream(int *res)
{
	(void) res;
	return closeProfilerStream();
}

str
CMDclrFilterProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	(void) mb;		/* fool compiler */
	checkProfiler("clrFilter");
	clrFilter(cntxt->nspace, *mod, *fcn);
	return MAL_SUCCEED;
}

str
CMDsetNoneProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str x = GDKstrdup("");
	str y = GDKstrdup("");

	(void) mb;		/* fool compiler */
	(void) stk;
	(void) pci;
	checkProfiler("clrFilter");
	clrFilter(cntxt->nspace, x, y);
	return MAL_SUCCEED;
}

str
CMDsetProfilerFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *fnme = (str*) getArgReference(stk,pci,1);
	(void) mb;		/* fool compiler */
	checkProfiler("setProfiler");
	setLogFile(cntxt->fdout,cntxt->nspace, *fnme);
	return MAL_SUCCEED;
}

str
CMDsetProfilerStream (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *host = (str*) getArgReference(stk,pci,1);
	int *port = (int*) getArgReference(stk,pci,2);
	(void) mb;		/* fool compiler */
	checkProfiler("setProfiler");
	setLogStream(cntxt->nspace, *host, *port);
	return MAL_SUCCEED;
}

str
CMDstartPointProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	(void) mb;		/* fool compiler */
	checkProfiler("startPoint");
	setStartPoint(cntxt->nspace, *mod, *fcn);
	return MAL_SUCCEED;
}

str
CMDendPointProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	(void) mb;		/* fool compiler */
	checkProfiler("endPoint");
	setStartPoint(cntxt->nspace, *mod, *fcn);
	return MAL_SUCCEED;
}

str
CMDstopProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	checkProfiler("stop");
	stopProfiling();
	return MAL_SUCCEED;
}

str
CMDstartProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	checkProfiler("start");
	startProfiling();
	return MAL_SUCCEED;
}
str
CMDnoopProfiler(int *res)
{
	(void) res;		/* fool compiler */
	checkProfiler("noop");
	return MAL_SUCCEED;
}

/*
 * @-
 * Tracing an active system.
 */
str
CMDclearTrace(int *res)
{
	(void) res;		/* fool compiler */
	checkProfiler("clearTrace");
	clearTrace();
	return MAL_SUCCEED;
}

str
CMDdumpTrace(int *res)
{
	(void) res;		/* fool compiler */
	checkProfiler("dump");
	throw(MAL, "profiler.dump", PROGRAM_NYI);
}

str
CMDgetTrace(int *res, str *ev)
{
	BAT *bn;

	(void) res;		/* fool compiler */
	checkProfiler("getTrace");
	bn = getTrace(*ev);
	if (bn) {
		BBPincref(*res = bn->batCacheid, TRUE);
		return MAL_SUCCEED;
	}
	throw(MAL, "getTrace", RUNTIME_OBJECT_MISSING  "%s",*ev);
}

str
CMDcleanup(int *ret){
	(void) ret;
	cleanupProfiler();
	return MAL_SUCCEED;
}

str
CMDgetEvent( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	lng *clk, *reads, *writes, pc;
	clk = (lng *) getArgReference(stk,pci,0);
	reads = (lng *) getArgReference(stk,pci,1);
	writes = (lng *) getArgReference(stk,pci,2);

	(void) cntxt;
	if( mb->profiler == 0)
		throw(MAL,"profiler.getEvent", OPERATION_FAILED " Monitor not active");

	pc= getPC(mb,pci)-1; /* take previous instruction */
	*clk = mb->profiler[pc].ticks;
	*reads = mb->profiler[pc].rbytes;
	*writes = mb->profiler[pc].wbytes;
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
CMDsetFootprintFlag( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	(void) mb;
	(void) stk;
	(void) pci;
	cntxt->flags |= bigfootFlag;
	return MAL_SUCCEED;
}

str
CMDgetFootprint( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	lng *l= getArgReference(stk,pci,0);

	(void) mb;
	*l = cntxt->bigfoot;
	cntxt->flags &= ~bigfootFlag;
	cntxt->bigfoot= 0;
	cntxt->vmfoot= 0;
	return MAL_SUCCEED;
}

str
CMDsetMemoryFlag( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	(void) mb;
	(void) stk;
	(void) pci;
	cntxt->flags |= memoryFlag;
	return MAL_SUCCEED;
}

str
CMDgetMemory( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	lng *l= getArgReference(stk,pci,0);

	(void) mb;
	*l = cntxt->memory;
	cntxt->flags &= ~memoryFlag;
	cntxt->memory= 0;
	return MAL_SUCCEED;
}
