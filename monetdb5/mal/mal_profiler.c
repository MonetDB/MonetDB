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

/* (author) M.L. Kersten
 * Performance tracing
 * The interpreter comes with several variables to hold performance
 * related data.
 * Every MAL instruction record is extended with two fields: counter and timer.
 * The counter is incremented each time the instruction is taken into
 * execution. Upon return, the timer is incremented with the microseconds
 * spent.
 * In addition to the default performance data collection,
 * the user can request performance events to be collected on a statement
 * basis. Care should be taken, because it leads to a large trace file,
 * unless the results are directly passed to a performance monitor
 * front-end for filtering and summarization.
 *
 * The performance monitor has exclusive access to the event file, which
 * avoid concurrency conflicts amongst clients. It avoid cluthered
 * event records on the event stream. Since this event stream is owned
 * by a client, we should ensure that the profiler is automatically be
 * reset once the owner leaves. The routine profilerReset() handles the case.
 */
#include "monetdb_config.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_profiler.h"
#include "mal_runtime.h"
#include "mal_debugger.h"

stream *eventstream = 0;

static int offlineProfiling = FALSE;
static int cachedProfiling = FALSE;
static str myname = 0;

int
profilerAvailable(void)
{
	return 1;
}
static void offlineProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc, int start);
static void cachedProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
static int initTrace(void);
static void startHeartbeat(int delay);

int malProfileMode = 0;     /* global flag to indicate profiling mode */
static int profileAll = 0;  /* all instructions should be profiled */
static int delayswitch = 0; /* to wait before sending the profile info */
static int eventcounter = 0;

static struct {
	str name;		/* which logical counter is needed */
	int status;		/* trace it or not */
} profileCounter[] = {
	/*  0 */  { "event", 0},
	/*  1 */  { "time", 0},
	/*  2 */  { "thread", 0},
	/*  3 */  { "pc", 0},
	/*  4 */  { "function", 0},
	/*  5 */  { "ticks", 0},
	/*  6 */  { "cpu", 0},
	/*  7 */  { "memory", 0},
	/*  8 */  { "reads", 0},
	/*  9 */  { "writes", 0},
	/*  10 */  { "rbytes", 0},
	/*  11 */  { "wbytes", 0},
	/*  12 */  { "stmt", 0},
	/*  13 */  { "aggregate", 0},
	/*  14 */  { "process", 0},
	/*  15 */  { "user", 0},
	/*  16 */  { "start", 0},
	/*  17 */  { "type", 0},
	/*  18 */  { "dot", 0},
	/*  19 */  { "flow", 0},
	/*  20 */  { "ping", 0},
	/*  21 */  { "footprint", 0},
	/*  21 */  { 0, 0}
};

int
getProfileCounter(int idx){
	return profileCounter[idx].status ==1;
}

/*
 * The counters can be set individually.
 */
str
activateCounter(str name)
{
	int i;
	char *s;

	for (i = 0; profileCounter[i].name; i++)
		if (strcmp(profileCounter[i].name, name) == 0) {
			profileCounter[i].status = 1;
			return 0;
		} 
	if ( strncmp("ping",name,4) == 0){
		startHeartbeat(atoi(name+4));
		profileCounter[PROFping].status = 1;
		return 0;
	}
	/* interpret the string equivalent to the tomograph command line argument */
	for ( s= name; *s; s++)
	switch(*s){
	case 'a':
		profileCounter[PROFaggr].status = 1;
		break;
	case 'b':
		profileCounter[PROFrbytes].status = 1;
		profileCounter[PROFwbytes].status = 1;
		break;
	case 'c':
		profileCounter[PROFcpu].status = 1;
		break;
	case 'e':
		profileCounter[PROFevent].status = 1;
		break;
	case 'f':
		profileCounter[PROFfunc].status = 1;
		break;
	case 'i':
		profileCounter[PROFpc].status = 1;
		break;
	case 'I':
		profileCounter[PROFthread].status = 1;
		break;
	case 'm':
		profileCounter[PROFmemory].status = 1;
		break;
	case 'p':
		profileCounter[PROFprocess].status = 1;
		break;
	case 'r':
		profileCounter[PROFreads].status = 1;
		break;
	case 's':
		profileCounter[PROFstmt].status = 1;
		break;
	case 'S':
		profileCounter[PROFstart].status = 1;
		break;
	case 't':
		profileCounter[PROFticks].status = 1;
		break;
	case 'T':
		profileCounter[PROFtime].status = 1;
		break;
	case 'u':
		profileCounter[PROFuser].status = 1;
		break;
	case 'w':
		profileCounter[PROFwrites].status = 1;
		break;
	case 'x':
		startHeartbeat(atoi(s+1));
		profileCounter[PROFping].status = 1;
		break;
	case 'y':
		profileCounter[PROFtype].status = 1;
		break;
	default:
		throw(MAL, "activateCounter", RUNTIME_OBJECT_UNDEFINED ":%s", name);
	}
	return MAL_SUCCEED;
}

str
deactivateCounter(str name)
{
	int i;
	for (i = 0; profileCounter[i].name; i++)
		if (strcmp(profileCounter[i].name, name) == 0) {
			profileCounter[i].status = 0;
			return 0;
		} else
		if ( strncmp("ping",name,4) == 0){
			startHeartbeat(0);
			return 0;
		}
	throw(MAL, "deactivateCounter", RUNTIME_OBJECT_UNDEFINED ":%s", name);
}

/*
 * Offline processing
 * The offline processing structure is the easiest. We merely have to
 * produce a correct tuple format for the front-end.
 * To avoid unnecessary locks we first build the event as a string
 * It uses a local logbuffer[LOGLEN] and logbase, logtop, loglen
 */
#define LOGLEN 8192
#define lognew()  loglen = 0; logbase = logbuffer; *logbase = 0;

#define logadd(...)														\
	do {																\
		(void) snprintf(logbase+loglen, LOGLEN -1 - loglen, __VA_ARGS__); \
		loglen += (int) strlen(logbase+loglen);							\
	} while (0)

static void logsend(char *logbuffer)
{ int error=0;
	if (eventstream) {
		MT_lock_set(&mal_profileLock, "logsend");
		eventcounter++;
		if (profileCounter[PROFevent].status && eventcounter)
			error= mnstr_printf(eventstream,"[ %d,\t%s", eventcounter, logbuffer);
		else
			error= mnstr_printf(eventstream,"[ %s", logbuffer);
		error= mnstr_flush(eventstream);
		MT_lock_unset(&mal_profileLock, "logsend");
		if ( error) stopProfiling();
	}
}

#define flushLog() if (eventstream) mnstr_flush(eventstream);

/*
 * Event dispatching
 * The profiler strategy is encapsulated here
 * Note that the profiler itself should lead to event generations.
 */
void
profilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start)
{
	if (stk == NULL) return;
	if (pci == NULL) return;
	if (profileCounter[PROFdot].status == 1 && start && pci == NULL){
		if (mb->dotfile == 0){
			MT_lock_set(&mal_profileLock, "profilerEvent");
			showFlowGraph(mb,stk,"stethoscope");
			MT_lock_unset(&mal_profileLock, "profilerEvent");
		}
	}
	if (profileCounter[PROFstart].status == 0 && start)
		return;
	if ( !start && pci && pci->token == ENDsymbol)
		profilerHeartbeatEvent("ping", 0);
	if (myname == 0)
		myname = putName("profiler", 8);
	if (getModuleId(pci) == myname)
		return;
	if (offlineProfiling)
		offlineProfilerEvent(idx, mb, stk, pci,start);
	if (cachedProfiling && !start)
		cachedProfilerEvent(idx, mb, stk, pci);
}

static void
offlineProfilerHeader(void)
{
	char logbuffer[LOGLEN], *logbase;
	int loglen;

	if (eventstream == NULL) {
		return ;
	}
	lognew();
	logadd("# ");
	if (profileCounter[PROFevent].status) {
		logadd("event,\tstatus,\t");
	}
	if (profileCounter[PROFtime].status) {
		logadd("time,\t");
	}
	if (profileCounter[PROFthread].status) {
		logadd("thread,\t");
	}
	if (profileCounter[PROFflow].status)
		logadd("claim,\tmemory,\t");
	if (profileCounter[PROFfunc].status) {
		logadd("function,\t");
	}
	if (profileCounter[PROFpc].status) {
		logadd("pc,\t");
	}
	if (profileCounter[PROFticks].status) {
		logadd("usec,\t");
	}
	if (profileCounter[PROFcpu].status) {
		logadd("utime,\t");
		logadd("cutime,\t");
		logadd("stime,\t");
		logadd("cstime,\t");
	}

	if (profileCounter[PROFmemory].status) {
		logadd("rss,\t");
/*
		logadd("maxrss,\t");
		logadd("arena,\t");
		logadd("ordblks,\t");
		logadd("smblks,\t");
		logadd("hblkhd,\t");
		logadd("hblks,\t");
		logadd("fsmblks,\t");
		logadd("uordblks,\t");
*/
	}
	if (profileCounter[PROFfootprint].status) {
		logadd("footprint,\t");
	}
	if (profileCounter[PROFreads].status)
		logadd("blk reads,\t");
	if (profileCounter[PROFwrites].status)
		logadd("blk writes,\t");
	if (profileCounter[PROFprocess].status) {
		logadd("pg reclaim,\t");
		logadd("pg faults,\t");
		logadd("swaps,\t");
		logadd("ctxt switch,\t");
		logadd("inv switch,\t");
	}
	if (profileCounter[PROFrbytes].status)
		logadd("rbytes,\t");
	if (profileCounter[PROFwbytes].status)
		logadd("wbytes,\t");
	if (profileCounter[PROFaggr].status)
		logadd("count,\t totalticks,\t");
	if (profileCounter[PROFstmt].status)
		logadd("stmt,\t");
	if (profileCounter[PROFtype].status)
		logadd("types,\t");
	if (profileCounter[PROFuser].status)
		logadd("user,\t");
	logadd("# name \n");
	if (eventstream){
		mnstr_printf(eventstream,"%s\n", logbuffer);
		mnstr_flush(eventstream);
	}
}

void
offlineProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start)
{
	char logbuffer[LOGLEN], *logbase;
	int loglen;
	char ctm[26];
	time_t clk;
	struct timeval clock;
	/*static struct Mallinfo prevMalloc;*/

#ifdef HAVE_SYS_RESOURCE_H
	static struct rusage prevUsage;
	struct rusage infoUsage;
#endif
#ifdef HAVE_TIMES
	struct tms newTms;
#endif
/*
	struct Mallinfo infoMalloc;
*/
	str stmt, c;

	if (delayswitch > 0) {
		/* first call to profiled */
		offlineProfilerHeader();
		delayswitch--;
	}
	if (eventstream == NULL) {
		return ;
	}
	if (delayswitch == 0) {
		delayswitch = -1;
	}
	if (!profileAll && pci->trace == FALSE) {
		return;
	}
	gettimeofday(&clock, NULL);
	clk = clock.tv_sec;
#ifdef HAVE_TIMES
	times(&newTms);
#endif
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
#endif

	/* make basic profile event tuple  */
	lognew();
	if (profileCounter[PROFstart].status) {
		if ( start) {
			logadd("\"start\",\t");
		} else {
			logadd("\"done\" ,\t");
		}
	}
	if (profileCounter[PROFtime].status) {
		char *tbuf;

		/* without this cast, compilation on Windows fails with
		 * argument of type "long *" is incompatible with parameter of type "const time_t={__time64_t={__int64}} *"
		 */

#ifdef HAVE_CTIME_R3
		tbuf = ctime_r(&clk, ctm, sizeof(ctm));
#else
#ifdef HAVE_CTIME_R
		tbuf = ctime_r(&clk, ctm);
#else
		tbuf = ctime(&clk);
#endif
#endif
		if (tbuf)
			logadd("\"%.8s.%06ld\",\t", tbuf + 11, (long) clock.tv_usec);
		else
			logadd("%s,\t", "nil");
	}
	if (profileCounter[PROFthread].status) {
		logadd(" %d,\t", THRgettid());
	}
	if (profileCounter[PROFflow].status) {
		logadd("%d,\t", memoryclaims);
		logadd(LLFMT",\t", memoryclaims?((lng)(MEMORY_THRESHOLD * monet_memory)-memorypool)/1024/1024:0);
	}
	if (profileCounter[PROFfunc].status) {
		if (getModuleId(getInstrPtr(mb,0)) && getFunctionId(getInstrPtr(mb,0))) {
			logadd("\"%s.%s\",\t", getModuleId(getInstrPtr(mb,0)), getFunctionId(getInstrPtr(mb,0)));
		} else
			logadd("\"%s\",\t", operatorName(pci->token));
	}
	if (profileCounter[PROFpc].status) {
		logadd("%d,\t", getPC(mb, pci));
	}
	if (profileCounter[PROFticks].status) {
		logadd(LLFMT ",\t", start? 0: pci->ticks);
	}
#ifdef HAVE_TIMES
	if (profileCounter[PROFcpu].status && delayswitch < 0) {
// TODO
/*
		logadd(LLFMT",\t", (lng) (newTms.tms_utime - mb->profiler[pc].timer.tms_utime));
		logadd(LLFMT",\t", (lng) (newTms.tms_cutime - mb->profiler[pc].timer.tms_cutime));
		logadd(LLFMT",\t", (lng) (newTms.tms_stime - mb->profiler[pc].timer.tms_stime));
		logadd(LLFMT",\t", (lng) (newTms.tms_cstime - mb->profiler[pc].timer.tms_cstime));
*/
	}
#endif

	if (profileCounter[PROFmemory].status && delayswitch < 0) {
		logadd(SZFMT ",\t", MT_getrss()/1024/1024);
	}
	if (profileCounter[PROFfootprint].status) {
		logadd(LLFMT",\t", stk->tmpspace);
	}
#ifdef HAVE_SYS_RESOURCE_H
	if ((profileCounter[PROFreads].status ||
		 profileCounter[PROFwrites].status) && delayswitch < 0) {
		logadd("%ld,\t", infoUsage.ru_inblock - prevUsage.ru_inblock);
		logadd("%ld,\t", infoUsage.ru_oublock - prevUsage.ru_oublock);
		prevUsage = infoUsage;
	}
	if (profileCounter[PROFprocess].status && delayswitch < 0) {
		logadd("%ld,\t", infoUsage.ru_minflt - prevUsage.ru_minflt);
		logadd("%ld,\t", infoUsage.ru_majflt - prevUsage.ru_majflt);
		logadd("%ld,\t", infoUsage.ru_nswap - prevUsage.ru_nswap);
		logadd("%ld,\t", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw);
		logadd("%ld,\t", infoUsage.ru_nivcsw - prevUsage.ru_nivcsw);
		prevUsage = infoUsage;
	}
#endif
	if (profileCounter[PROFrbytes].status)
		logadd(LLFMT ",\t", pci->rbytes);
	if (profileCounter[PROFwbytes].status)
		logadd(LLFMT ",\t", pci->wbytes);

	if (profileCounter[PROFaggr].status)
		logadd("%d,\t" LLFMT ",\t", pci->calls, pci->ticks);

	if (profileCounter[PROFstmt].status) {
		/* generate actual call statement */
		str stmtq;
		stmt = instruction2str(mb, stk, pci, LIST_MAL_DEBUG);
		c = stmt;

		while (c && *c && isspace((int)*c))
			c++;
		stmtq = mal_quote(c, strlen(c));
		if (stmtq != NULL) {
			logadd(" \"%s\",\t", stmtq);
			GDKfree(stmtq);
		} else logadd(" ,\t");
		GDKfree(stmt);
	}
	if (profileCounter[PROFtype].status) {
		char abuf[BUFSIZ], *tpe;
		int i, j;
		abuf[0] = 0;
		for (i = 0; i < pci->retc; i++)
			if (getArgType(mb, pci, i) != TYPE_void) {
				j = (int)strlen(abuf);
				tpe = getTypeName(getArgType(mb, pci, i));
				snprintf(abuf + j, BUFSIZ - j, "%s:%s%s", getVarName(mb, getArg(pci, i)), tpe, (i < pci->retc - 1 ? ", " : ""));
				GDKfree(tpe);
			}
		logadd("\"%s\",\t", abuf);
	}
	if (profileCounter[PROFuser].status) {
		logadd(" %d", idx);
	}
	logadd("]\n");
	logsend(logbuffer);
}
/*
 * Postprocessing events
 * The events may be sent for offline processing through a
 * stream, including "stdout".
 */


str
setLogFile(stream *fd, Module mod, str fname)
{
	(void)mod;      /* still unused */
	MT_lock_set(&mal_profileLock, "setLogFile");
	if (eventstream ) {
		MT_lock_unset(&mal_profileLock, "setLogFile");
		throw(IO, "mal.profiler", "Log file already set");
	}
	if (strcmp(fname, "console") == 0)
		eventstream = mal_clients[0].fdout;
	else if (strcmp(fname, "stdout") == 0)
		eventstream = fd;
	else
		eventstream = open_wastream(fname);
	if (eventstream == NULL) {
		MT_lock_unset(&mal_profileLock, "setLogFile");
		throw(IO, "mal.profiler", RUNTIME_STREAM_FAILED);
	}
	MT_lock_unset(&mal_profileLock, "setLogFile");
	return MAL_SUCCEED;
}

str
setLogStream(Module cntxt, str host, int port)
{
	(void)cntxt;        /* still unused */
	MT_lock_set(&mal_profileLock, "setLogStream");
	if ((eventstream = udp_wastream(host, port, "profileStream")) == NULL) {
		MT_lock_unset(&mal_profileLock, "setLogStream");
		throw(IO, "mal.profiler", RUNTIME_STREAM_FAILED);
	}
	eventstream = wbstream(eventstream, BUFSIZ);
	MT_lock_unset(&mal_profileLock, "setLogStream");
	return MAL_SUCCEED;
}

str
setLogStreamStream(Module cntxt, stream *s)
{
	(void)cntxt;        /* still unused */
	MT_lock_set(&mal_profileLock, "setLogStreamStream");
	if ((eventstream = s) == NULL) {
		MT_lock_unset(&mal_profileLock, "setLogStreamStream");
		throw(ILLARG, "mal.profiler", "stream must not be NULL");
	}
	eventstream = wbstream(eventstream, BUFSIZ);
	MT_lock_unset(&mal_profileLock, "setLogStreamStream");
	return MAL_SUCCEED;
}

str
openProfilerStream(stream *fd)
{
	malProfileMode = TRUE;
	eventstream = fd;
	delayswitch = 1;    /* avoid an incomplete initial profile event */
	return MAL_SUCCEED;
}

str
closeProfilerStream(void)
{
	if (eventstream && eventstream != GDKout && eventstream != GDKerr) {
		(void)mnstr_close(eventstream);
		(void)mnstr_destroy(eventstream);
	}
	eventstream = NULL;
	malProfileMode = FALSE;
	return MAL_SUCCEED;
}

str
setStartPoint(Module cntxt, str mod, str fcn)
{
	(void)cntxt;
	(void)mod;
	(void)fcn;      /* still unused */
	MT_lock_set(&mal_profileLock, "setStartPoint");
	if (eventstream == NULL) {
		MT_lock_unset(&mal_profileLock, "setStartPoint");
		return MAL_SUCCEED ;
	}
	mnstr_printf(GDKout, "# start point not set\n");
	flushLog();
	MT_lock_unset(&mal_profileLock, "setStartPoint");
	return MAL_SUCCEED;
}

str
setEndPoint(Module cntxt, str mod, str fcn)
{
	(void)cntxt;
	(void)mod;
	(void)fcn;      /* still unused */
	MT_lock_set(&mal_profileLock, "setEndPoint");
	if (eventstream == NULL) {
		MT_lock_unset(&mal_profileLock, "setEndPoint");
		return MAL_SUCCEED ;
	}
	mnstr_printf(GDKout, "# end point not set\n");
	flushLog();
	MT_lock_unset(&mal_profileLock, "setEndPoint");
	return MAL_SUCCEED;
}

/*
 * When you receive the message to start profiling, we
 * should wait for the next instruction the stream
 * is initiated. This is controlled by a delay-switch
 */
static int TRACE_init = 0;
str
startProfiling(void)
{
	MT_lock_set(&mal_profileLock, "startProfiling");
	if (eventstream != NULL) {
		offlineProfiling = TRUE;
		delayswitch = 1;
	} else
		cachedProfiling = TRUE;
	if (TRACE_init == 0)
		_initTrace();
	malProfileMode = TRUE;
	eventcounter = 0;
	MT_lock_unset(&mal_profileLock, "startProfiling");
	return MAL_SUCCEED;
}

str
stopProfiling(void)
{
	MT_lock_set(&mal_profileLock, "stopProfiling");
	malProfileMode = FALSE;
	offlineProfiling = FALSE;
	cachedProfiling = FALSE;
	closeProfilerStream();
	MT_lock_unset(&mal_profileLock, "stopProfiling");
	return MAL_SUCCEED;
}

/*
 * The resetProfiler is called when the owner of the event stream
 * leaves the scene. (Unclear if parallelism may cause errors)
 */
void
MPresetProfiler(stream *fdout)
{
	if (fdout != eventstream)
		return;
	if (mal_trace)
		return;
	MT_lock_set(&mal_profileLock, "MPresetProfiler");
	eventstream = 0;
	MT_lock_unset(&mal_profileLock, "MPresetProfiler");
}

void setFilterAll(void){
	profileAll = 1;
}

/*
 * Extern sources may dump information on the profiler stream
*/
stream *
getProfilerStream(void)  
{
	return eventstream;
}

/*
 * Performance tracing is triggered on an instruction basis
 * or a the global flag 'profileAll' being set.
 * Calling setFilter(M,F) switches the performance tracing
 * bit in the instruction record. The routine clrFilter
 * clears all performance bits.
 *
 * The routines rely on waking their way through the
 * instructions space from a given context. This has been
 * abstracted away.
 */
int
instrFilter(InstrPtr pci, str mod, str fcn)
{
	if (pci && getFunctionId(pci) && fcn && mod &&
			(*fcn == '*' || fcn == getFunctionId(pci)) &&
			(*mod == '*' || mod == getModuleId(pci)))
		return 1;
	return 0;
}

/*
 * The last filter values are saved as replacement for missing
 * arguments. It can be used to set the profile bits for modules
 * that has not been checked yet, e.g created on the fly.
 */
static str modFilter[32], fcnFilter[32];
static int topFilter;

void
setFilterOnBlock(MalBlkPtr mb, str mod, str fcn)
{
	int cnt, k, i;
	InstrPtr p;

	initTrace();
	if ( profileAll )
		for (k = 0; k < mb->stop; k++)
			mb->stmt[k]->trace = 1;
	else
	for (k = 0; k < mb->stop; k++) {
		p = getInstrPtr(mb, k);
		cnt = 0;
		for (i = 0; i < topFilter; i++)
			cnt += instrFilter(p, modFilter[i], fcnFilter[i]);
		mb->stmt[k]->trace = cnt || (mod && fcn && instrFilter(p, mod, fcn));
	}
}

void
setFilter(Module cntxt, str mod, str fcn)
{
	int j;
	Module s = cntxt;
	Symbol t;
	str matchall = "*";

	(void)cntxt;
	if (mod == NULL)
		mod = matchall;
	if (fcn == NULL)
		fcn = matchall;
	profileAll = strcmp(mod, "*") == 0 && strcmp(fcn, "*") == 0;

	MT_lock_set(&mal_profileLock, "setFilter");
	if (mod && fcn && topFilter < 32) {
		modFilter[topFilter] = putName(mod, strlen(mod));
		fcnFilter[topFilter++] = putName(fcn, strlen(fcn));
	}
	while (s != NULL) {
		if (s->subscope)
			for (j = 0; j < MAXSCOPE; j++)
				if (s->subscope[j]) {
					for (t = s->subscope[j]; t != NULL; t = t->peer) {
						if (t->def)
							setFilterOnBlock(t->def, mod, fcn);
					}
				}
		s = s->outer;
	}
	MT_lock_unset(&mal_profileLock, "setFilter");
}

/*
 * Watch out. The profiling bits are only set for the shared modules and
 * the private main(). The profiler setFilter should explicitly be called in
 * each separate top level routine.
 */
void
clrFilter(Module cntxt, str mod, str fcn)
{
	int j, k;
	Module s = cntxt;
	Symbol t;

	(void)mod;
	(void)fcn;      /* still unused */

	MT_lock_set(&mal_profileLock, "clrFilter");
	for (j = 0; j < topFilter; j++) {
		modFilter[j] = NULL;
		fcnFilter[j] = NULL;
	}
	topFilter = 0;
	profileAll = FALSE;
	while (s != NULL) {
		if (s->subscope)
			for (j = 0; j < MAXSCOPE; j++)
				if (s->subscope[j]) {
					for (t = s->subscope[j]; t != NULL; t = t->peer) {
						if (t->def)
							for (k = 0; k < t->def->stop; k++)
								if (instrFilter(getInstrPtr(t->def, k), mod, fcn)) {
									t->def->stmt[k]->trace = FALSE;
								}
					}
				}
		s = s->outer;
	}
	MT_lock_unset(&mal_profileLock, "clrFilter");
}
/*
 * The instructions to be monitored can also be identified
 * using a variable. Any instruction that references it
 * is traced. Beware, this operation should be executed
 * in the context of the function to avoid loosing
 * track due to optimizers re-assigning names.
 */
void
setFilterVariable(MalBlkPtr mb, int arg)
{
	int i, k;
	InstrPtr p;

	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		for (k = 0; k < p->argc; k++)
			if (getArg(p, k) == arg) {
				initTrace();
				mb->stmt[i]->trace = TRUE;
			}
	}
}

void
clrFilterVariable(MalBlkPtr mb, int arg)
{
	int i, k;
	InstrPtr p;

	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		for (k = 0; k < p->argc; k++)
			if (getArg(p, k) == arg) {
				mb->stmt[i]->trace = FALSE;
			}
	}
}

/*
 * Offline tracing
 * The events being captured are stored in separate BATs.
 * They are made persistent to accumate information over
 * multiple sessions. This means it has to be explicitly reset
 * to avoid disc overflow using profiler.reset().
 *
 * All properties identified below are maintained, because this allows
 * for easy integration with SQL.
 */
static int TRACE_event = 0;
static BAT *TRACE_id_tag = 0;
static BAT *TRACE_id_event = 0;
static BAT *TRACE_id_time = 0;
static BAT *TRACE_id_ticks = 0;
static BAT *TRACE_id_pc = 0;
static BAT *TRACE_id_stmt = 0;
static BAT *TRACE_id_type = 0;
static BAT *TRACE_id_rbytes = 0;
static BAT *TRACE_id_wbytes = 0;
static BAT *TRACE_id_reads = 0;
static BAT *TRACE_id_writes = 0;
static BAT *TRACE_id_thread = 0;
static BAT *TRACE_id_user = 0;

void
TRACEtable(BAT **r)
{
	if (initTrace())
		return ;
	MT_lock_set(&mal_profileLock, "TRACEtable");
	r[0] = BATcopy(TRACE_id_tag, TRACE_id_tag->htype, TRACE_id_tag->ttype, 0, TRANSIENT);
	r[0] = BATcopy(TRACE_id_event, TRACE_id_event->htype, TRACE_id_event->ttype, 0, TRANSIENT);
	r[1] = BATcopy(TRACE_id_time, TRACE_id_time->htype, TRACE_id_time->ttype, 0, TRANSIENT);
	r[2] = BATcopy(TRACE_id_pc, TRACE_id_pc->htype, TRACE_id_pc->ttype, 0, TRANSIENT);
	r[3] = BATcopy(TRACE_id_thread, TRACE_id_thread->htype, TRACE_id_thread->ttype, 0, TRANSIENT);
	r[4] = BATcopy(TRACE_id_user, TRACE_id_user->htype, TRACE_id_user->ttype, 0, TRANSIENT);
	r[5] = BATcopy(TRACE_id_ticks, TRACE_id_ticks->htype, TRACE_id_ticks->ttype, 0, TRANSIENT);
	r[6] = BATcopy(TRACE_id_reads, TRACE_id_reads->htype, TRACE_id_reads->ttype, 0, TRANSIENT);
	r[7] = BATcopy(TRACE_id_writes, TRACE_id_writes->htype, TRACE_id_writes->ttype, 0, TRANSIENT);
	r[8] = BATcopy(TRACE_id_rbytes, TRACE_id_rbytes->htype, TRACE_id_rbytes->ttype, 0, TRANSIENT);
	r[9] = BATcopy(TRACE_id_wbytes, TRACE_id_wbytes->htype, TRACE_id_wbytes->ttype, 0, TRANSIENT);
	r[10] = BATcopy(TRACE_id_type, TRACE_id_type->htype, TRACE_id_type->ttype, 0, TRANSIENT);
	r[11] = BATcopy(TRACE_id_stmt, TRACE_id_stmt->htype, TRACE_id_stmt->ttype, 0, TRANSIENT);
	MT_lock_unset(&mal_profileLock, "TRACEtable");
}

static BAT *
TRACEcreate(str hnme, str tnme, int tt)
{
	BAT *b;
	char buf[128];

	snprintf(buf, 128, "trace_%s_%s", hnme, tnme);
	b = BATdescriptor(BBPindex(buf));
	if (b) 
		return b;

	b = BATnew(TYPE_void, tt, 1 << 16, PERSISTENT);
	if (b == NULL)
		return NULL;

	BATmode(b, PERSISTENT);
	BATseqbase(b, 0);
	BATkey(b, TRUE);
	BBPrename(b->batCacheid, buf);
	BATcommit(b);
	return b;
}


#define CLEANUPprofile(X)  if (X) { BBPdecref((X)->batCacheid, TRUE); (X)->batPersistence = TRANSIENT; } (X) = NULL;

static void
_cleanupProfiler(void)
{
	CLEANUPprofile(TRACE_id_tag);
	CLEANUPprofile(TRACE_id_event);
	CLEANUPprofile(TRACE_id_time);
	CLEANUPprofile(TRACE_id_pc);
	CLEANUPprofile(TRACE_id_stmt);
	CLEANUPprofile(TRACE_id_type);
	CLEANUPprofile(TRACE_id_rbytes);
	CLEANUPprofile(TRACE_id_wbytes);
	CLEANUPprofile(TRACE_id_reads);
	CLEANUPprofile(TRACE_id_writes);
	CLEANUPprofile(TRACE_id_thread);
	CLEANUPprofile(TRACE_id_user);
	TRACE_init = 0;
}

void
_initTrace(void)
{
	TRACE_id_tag = TRACEcreate("id", "tag", TYPE_int);
	TRACE_id_event = TRACEcreate("id", "event", TYPE_int);
	TRACE_id_time = TRACEcreate("id", "time", TYPE_str);
	TRACE_id_ticks = TRACEcreate("id", "ticks", TYPE_lng);
	TRACE_id_pc = TRACEcreate("id", "pc", TYPE_str);
	TRACE_id_stmt = TRACEcreate("id", "stmt", TYPE_str);
	TRACE_id_type = TRACEcreate("id", "type", TYPE_str);
	TRACE_id_rbytes = TRACEcreate("id", "rbytes", TYPE_lng);
	TRACE_id_wbytes = TRACEcreate("id", "wbytes", TYPE_lng);
	TRACE_id_reads = TRACEcreate("id", "read", TYPE_lng);
	TRACE_id_writes = TRACEcreate("id", "write", TYPE_lng);
	TRACE_id_thread = TRACEcreate("id", "thread", TYPE_int);
	TRACE_id_user = TRACEcreate("id", "user", TYPE_int);
	if (TRACE_id_event == NULL ||
		TRACE_id_tag == NULL ||
		TRACE_id_time == NULL ||
		TRACE_id_ticks == NULL ||
		TRACE_id_pc == NULL ||
		TRACE_id_stmt == NULL ||
		TRACE_id_type == NULL ||
		TRACE_id_rbytes == NULL ||
		TRACE_id_wbytes == NULL ||
		TRACE_id_reads == NULL ||
		TRACE_id_writes == NULL ||
		TRACE_id_thread == NULL ||
		TRACE_id_user == NULL
		) {
		_cleanupProfiler();
	} else {
		TRACE_init = 1;
	}
}

int
initTrace(void)
{
	if (TRACE_init)
		return 0;       /* already initialized */
	MT_lock_set(&mal_contextLock, "initTrace");
	_initTrace();
	MT_lock_unset(&mal_contextLock, "initTrace");
	return TRACE_init ? 0 : -1;
}

str
cleanupProfiler(void)
{
	MT_lock_set(&mal_contextLock, "cleanup");
	_cleanupProfiler();
	MT_lock_unset(&mal_contextLock, "cleanup");
	return MAL_SUCCEED;
}

void
clearTrace(void)
{
	if (TRACE_init == 0)
		return;     /* not initialized */
	MT_lock_set(&mal_contextLock, "cleanup");
	/* drop all trace tables */
	BBPclear(TRACE_id_tag->batCacheid);
	BBPclear(TRACE_id_event->batCacheid);
	BBPclear(TRACE_id_time->batCacheid);
	BBPclear(TRACE_id_ticks->batCacheid);
	BBPclear(TRACE_id_pc->batCacheid);
	BBPclear(TRACE_id_stmt->batCacheid);
	BBPclear(TRACE_id_type->batCacheid);
	BBPclear(TRACE_id_thread->batCacheid);
	BBPclear(TRACE_id_user->batCacheid);
	BBPclear(TRACE_id_reads->batCacheid);
	BBPclear(TRACE_id_writes->batCacheid);
	TRACE_init = 0;
	_initTrace();
	MT_lock_unset(&mal_contextLock, "cleanup");
}

BAT *
getTrace(str nme)
{
	if (TRACE_init == 0)
		return NULL;
	if (strcmp(nme, "tag") == 0)
		return BATcopy(TRACE_id_tag, TRACE_id_tag->htype, TRACE_id_tag->ttype, 0, TRANSIENT);
	if (strcmp(nme, "event") == 0)
		return BATcopy(TRACE_id_event, TRACE_id_event->htype, TRACE_id_event->ttype, 0, TRANSIENT);
	if (strcmp(nme, "time") == 0)
		return BATcopy(TRACE_id_time, TRACE_id_time->htype, TRACE_id_time->ttype, 0, TRANSIENT);
	if (strcmp(nme, "ticks") == 0)
		return BATcopy(TRACE_id_ticks, TRACE_id_ticks->htype, TRACE_id_ticks->ttype, 0, TRANSIENT);
	if (strcmp(nme, "pc") == 0)
		return BATcopy(TRACE_id_pc, TRACE_id_pc->htype, TRACE_id_pc->ttype, 0, TRANSIENT);
	if (strcmp(nme, "thread") == 0)
		return BATcopy(TRACE_id_thread, TRACE_id_thread->htype, TRACE_id_thread->ttype, 0, TRANSIENT);
	if (strcmp(nme, "user") == 0)
		return BATcopy(TRACE_id_user, TRACE_id_user->htype, TRACE_id_user->ttype, 0, TRANSIENT);
	if (strcmp(nme, "stmt") == 0)
		return BATcopy(TRACE_id_stmt, TRACE_id_stmt->htype, TRACE_id_stmt->ttype, 0, TRANSIENT);
	if (strcmp(nme, "type") == 0)
		return BATcopy(TRACE_id_type, TRACE_id_type->htype, TRACE_id_type->ttype, 0, TRANSIENT);
	if (strcmp(nme, "rbytes") == 0)
		return BATcopy(TRACE_id_rbytes, TRACE_id_rbytes->htype, TRACE_id_rbytes->ttype, 0, TRANSIENT);
	if (strcmp(nme, "wbytes") == 0)
		return BATcopy(TRACE_id_wbytes, TRACE_id_wbytes->htype, TRACE_id_wbytes->ttype, 0, TRANSIENT);
	if (strcmp(nme, "reads") == 0)
		return BATcopy(TRACE_id_reads, TRACE_id_reads->htype, TRACE_id_reads->ttype, 0, TRANSIENT);
	if (strcmp(nme, "writes") == 0)
		return BATcopy(TRACE_id_writes, TRACE_id_writes->htype, TRACE_id_writes->ttype, 0, TRANSIENT);
	return NULL;
}

int
getTraceType(str nme)
{
	if (initTrace())
		return TYPE_any;
	if (strcmp(nme, "time") == 0)
		return newColumnType( TYPE_str);
	if (strcmp(nme, "ticks") == 0)
		return newColumnType( TYPE_lng);
	if (strcmp(nme, "pc") == 0)
		return newColumnType( TYPE_str);
	if (strcmp(nme, "thread") == 0)
		return newColumnType( TYPE_int);
	if (strcmp(nme, "stmt") == 0)
		return newColumnType( TYPE_str);
	if (strcmp(nme, "rbytes") == 0)
		return newColumnType( TYPE_lng);
	if (strcmp(nme, "wbytes") == 0)
		return newColumnType( TYPE_lng);
	if (strcmp(nme, "reads") == 0 || strcmp(nme, "writes") == 0)
		return newColumnType( TYPE_lng);
	return TYPE_any;
}

void
cachedProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* static struct Mallinfo prevMalloc; */
	char buf[1024];
	char ctm[27]={0}, *tbuf;
	int tid = (int)THRgettid();
	char abuf[BUFSIZ], *tpe;
	int i, j;
	lng v1 = 0, v2= 0;
	str stmt, c;
	time_t clk;
	struct timeval clock;

#ifdef HAVE_TIMES
	struct tms newTms;
#endif

	/* struct Mallinfo infoMalloc; */
#ifdef HAVE_SYS_RESOURCE_H
	struct rusage infoUsage;
	static struct rusage prevUsage;
#endif

	if (delayswitch > 0) {
		/* first call to profiled */
		delayswitch--;
		return;
	}
	if (delayswitch == 0) {
		delayswitch = -1;
	}
	if (!(profileAll || pci->trace))
		return;
	gettimeofday(&clock, NULL);
	clk= clock.tv_sec;
#ifdef HAVE_TIMES
	times(&newTms);
#endif
	/* infoMalloc = MT_mallinfo(); */
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
#endif
	if (initTrace() || TRACE_init == 0)
		return;

	/* update the Trace tables */
	snprintf(buf, 1024, "%s.%s[%d]",
		getModuleId(getInstrPtr(mb, 0)),
		getFunctionId(getInstrPtr(mb, 0)), getPC(mb, pci));

	/* without this cast, compilation on Windows fails with
	 * argument of type "long *" is incompatible with parameter of type "const time_t={__time64_t={__int64}} *"
	 */
#ifdef HAVE_CTIME_R3
	tbuf = ctime_r(&clk, ctm, sizeof(ctm));
#else
#ifdef HAVE_CTIME_R
	tbuf = ctime_r(&clk, ctm);
#else
	tbuf = ctime(&clk);
#endif
#endif
	strncpy(ctm, (tbuf?tbuf:""),26);
	/* sneakily overwrite year with second fraction */
	snprintf(ctm + 19, 6, ".%03d", (int)(clock.tv_usec / 1000));

	/* generate actual call statement */
	stmt = instruction2str(mb, stk, pci, LIST_MAL_DEBUG);
	c = stmt;

	while (c && *c && (isspace((int)*c) || *c == '!'))
		c++;

	abuf[0] = 0;
	for (i = 0; i < pci->retc; i++)
		if (getArgType(mb, pci, i) != TYPE_void) {
			j = (int)strlen(abuf);
			tpe = getTypeName(getArgType(mb, pci, i));
			snprintf(abuf + j, BUFSIZ - j, "%s:%s%s", getVarName(mb, getArg(pci, i)), tpe, (i < pci->retc - 1 ? ", " : ""));
			GDKfree(tpe);
		}

#ifdef HAVE_SYS_RESOURCE_H
	v1 = infoUsage.ru_inblock - prevUsage.ru_inblock;
	v2 = infoUsage.ru_oublock - prevUsage.ru_oublock;
	prevUsage = infoUsage;
#endif

	// keep it a short transaction
	MT_lock_set(&mal_profileLock, "cachedProfilerEvent");
	TRACE_id_pc = BUNappend(TRACE_id_pc, buf, FALSE);
	TRACE_id_thread = BUNappend(TRACE_id_thread, &tid, FALSE);
	TRACE_id_user = BUNappend(TRACE_id_user, &idx, FALSE);
	TRACE_id_tag = BUNappend(TRACE_id_tag, &mb->tag, FALSE);
	TRACE_id_event = BUNappend(TRACE_id_event, &TRACE_event, FALSE);
	TRACE_id_time = BUNappend(TRACE_id_time, ctm, FALSE);
	TRACE_id_ticks = BUNappend(TRACE_id_ticks, &pci->ticks, FALSE);
	TRACE_id_stmt = BUNappend(TRACE_id_stmt, c, FALSE);
	TRACE_id_type = BUNappend(TRACE_id_type, &abuf, FALSE);
	TRACE_id_reads = BUNappend(TRACE_id_reads, &v1, FALSE);
	TRACE_id_writes = BUNappend(TRACE_id_writes, &v2, FALSE);
	TRACE_id_rbytes = BUNappend(TRACE_id_rbytes, &pci->rbytes, FALSE);
	TRACE_id_wbytes = BUNappend(TRACE_id_wbytes, &pci->wbytes, FALSE);
	TRACE_event++;
	eventcounter++;
	MT_lock_unset(&mal_profileLock, "cachedProfilerEvent");
	if (stmt) GDKfree(stmt);
}

lng
getDiskWrites(void)
{
#ifdef HAVE_SYS_RESOURCE_H
	struct rusage infoUsage;
	getrusage(RUSAGE_SELF, &infoUsage);
	return infoUsage.ru_oublock;
#else
	return 0;
#endif
}

lng
getDiskReads(void)
{
#ifdef HAVE_SYS_RESOURCE_H
	struct rusage infoUsage;
	getrusage(RUSAGE_SELF, &infoUsage);
	return infoUsage.ru_inblock;
#else
	return 0;
#endif
}

lng
getUserTime(void)
{
#ifdef HAVE_TIMES
	struct tms newTms;
	times(&newTms);
	return newTms.tms_utime;
#else
	return 0;
#endif
}

lng
getSystemTime(void)
{
#ifdef HAVE_TIMES
	struct tms newTms;
	times(&newTms);
	return newTms.tms_stime;
#else
	return 0;
#endif
}

lng
getDiskSpace(void)
{
	BAT *b;
	int i;
	lng size = 0;

	for (i = 1; i < getBBPsize(); i++)
		if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			b = BATdescriptor(i);
			if (b) {
				size += sizeof(BAT);
				if (!isVIEW(b)) {
					BUN cnt = BATcount(b);

					size += headsize(b, cnt);
					size += tailsize(b, cnt);
					/* the upperbound is used for the heaps */
					if (b->H->vheap)
						size += b->H->vheap->size;
					if (b->T->vheap)
						size += b->T->vheap->size;
					if (b->H->hash)
						size += sizeof(BUN) * cnt;
					if (b->T->hash)
						size += sizeof(BUN) * cnt;
				}
				BBPunfix(i);
			}
		}
	return size;
}

/* the heartbeat process produces a ping event once every X milliseconds */
#ifdef ATOMIC_LOCK
static MT_Lock hbLock MT_LOCK_INITIALIZER("hbLock");
#endif
static volatile ATOMIC_TYPE hbdelay = 0;

/* the processor statistics are gathered in Linux settings from the proc files.
 * Given the parsing involved, it should be used sparingly */

static struct{
	lng user, nice, system, idle, iowait;
	double load;
} corestat[256];

static int getCPULoad(char cpuload[BUFSIZ]){
    int cpu, len, i;
	lng user, nice, system, idle, iowait;
	size_t n;
    char buf[BUFSIZ+1],*s;
	static FILE *proc= NULL;
	lng newload;

	if ( proc == NULL || ferror(proc))
		proc = fopen("/proc/stat","r");
	else rewind(proc);
	if ( proc == NULL) {
		/* unexpected */
		return -1;
	}
	/* read complete file to avoid concurrent write issues */
	if ((n = fread(buf, 1, BUFSIZ,proc)) == 0 )
		return -1;
	buf[n] = 0;
	for ( s= buf; *s; s++)
	{
		if ( strncmp(s,"cpu",3)== 0){
			s +=3;
			if ( *s == ' ') {
				s++;
				cpu = 255; // the cpu totals stored here
			}  else 
				cpu = atoi(s);
			s= strchr(s,' ');
			if ( s== 0) goto skip;
			
			while( *s && isspace((int)*s)) s++;
			i= sscanf(s,LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT,  &user, &nice, &system, &idle, &iowait);
			if ( i != 5 )
				goto skip;
			newload = (user - corestat[cpu].user + nice - corestat[cpu].nice + system - corestat[cpu].system);
			if (  newload)
				corestat[cpu].load = (double) newload / (newload + idle - corestat[cpu].idle + iowait - corestat[cpu].iowait);
			corestat[cpu].user = user;
			corestat[cpu].nice = nice;
			corestat[cpu].system = system;
			corestat[cpu].idle = idle;
			corestat[cpu].iowait = iowait;
		} 
		skip: while( *s && *s != '\n') s++;
	}

	s= cpuload;
	len = BUFSIZ;
	// identify core processing
	for ( cpu = 0; cpuload && cpu < 255 && corestat[cpu].user; cpu++) {
		snprintf(s, len, " %.2f ",corestat[cpu].load);
		len -= (int)strlen(s);
		s += (int) strlen(s);
	}
	return 0;
}

// Give users the option to check for the system load between two heart beats
double HeartbeatCPUload(void)
{
	return corestat[255].load;
}
void profilerGetCPUStat(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait)
{
	(void) getCPULoad(0);
	*user = corestat[255].user;
	*nice = corestat[255].nice;
	*sys = corestat[255].system;
	*idle = corestat[255].idle;
	*iowait = corestat[255].iowait;
}

void profilerHeartbeatEvent(str msg, lng ticks)
{
	char logbuffer[LOGLEN], *logbase;
	char cpuload[BUFSIZ];
	int loglen;
#ifdef HAVE_SYS_RESOURCE_H
	static struct rusage prevUsage;
	struct rusage infoUsage;
#endif
	struct timeval tv;
	time_t clock;
#ifdef HAVE_TIMES
	struct tms newTms;
	struct tms prevtimer;

	if (ATOMIC_GET(hbdelay, hbLock, "profilerHeatbeatEvent") == 0 || eventstream  == NULL)
		return;
	times(&prevtimer);
#endif
#ifdef HAVE_SYS_RESOURCE_H
		getrusage(RUSAGE_SELF, &prevUsage);
#endif
	gettimeofday(&tv,NULL);

	/* without this cast, compilation on Windows fails with
	 * argument of type "long *" is incompatible with parameter of type "const time_t={__time64_t={__int64}} *"
	 */

	gettimeofday(&tv,NULL);
	clock = (time_t) tv.tv_sec;

	/* get CPU load on second boundaries only */
	if ( getCPULoad(cpuload) )
		return;
	lognew();
#ifdef HAVE_TIMES
	times(&newTms);
#endif
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
#endif

	/* make ping profile event tuple  */
	if (profileCounter[PROFstart].status) 
		logadd("\"%s\",\t",msg);
	if (profileCounter[PROFtime].status) {
		char *tbuf;
#ifdef HAVE_CTIME_R3
		char ctm[26];
		tbuf = ctime_r(&clock, ctm, sizeof(ctm));
#else
#ifdef HAVE_CTIME_R
		char ctm[26];
		tbuf = ctime_r(&clock, ctm);
#else
		tbuf = ctime(&clock);
#endif
#endif
		if (tbuf)
			logadd("\"%.8s.%06ld\",\t", tbuf + 11, (long) tv.tv_usec);
		else
			logadd("%s,\t", "nil");
	}
	if (profileCounter[PROFthread].status)
		logadd(" %d,\t", THRgettid());
	if (profileCounter[PROFflow].status) {
		logadd("%d,\t", memoryclaims);
		logadd(LLFMT",\t", memoryclaims?((lng)(MEMORY_THRESHOLD * monet_memory)-memorypool)/1024/1024:0);
	}
	if (profileCounter[PROFfunc].status) 
			logadd("\"ping\",\t");
	if (profileCounter[PROFpc].status) 
		logadd("0,\t");
	if (profileCounter[PROFticks].status) 
		logadd(LLFMT",\t", ticks);
#ifdef HAVE_TIMES
	if (profileCounter[PROFcpu].status && delayswitch < 0) {
		logadd(LLFMT",\t", (lng) (newTms.tms_utime - prevtimer.tms_utime));
		logadd(LLFMT",\t", (lng) (newTms.tms_cutime -prevtimer.tms_cutime));
		logadd(LLFMT",\t", (lng) (newTms.tms_stime - prevtimer.tms_stime));
		logadd(LLFMT",\t", (lng) (newTms.tms_cstime -prevtimer.tms_cstime));
		prevtimer = newTms;
	}
#endif
	if (profileCounter[PROFmemory].status && delayswitch < 0)
		logadd(SZFMT ",\t", MT_getrss()/1024/1024);
#ifdef HAVE_SYS_RESOURCE_H
	if ((profileCounter[PROFreads].status ||
		 profileCounter[PROFwrites].status) && delayswitch < 0) {
		logadd("%ld,\t", infoUsage.ru_inblock - prevUsage.ru_inblock);
		logadd("%ld,\t", infoUsage.ru_oublock - prevUsage.ru_oublock);
		prevUsage = infoUsage;
	}
	if (profileCounter[PROFfootprint].status)
		logadd("0,\t");
	if (profileCounter[PROFprocess].status && delayswitch < 0) {
		logadd("%ld,\t", infoUsage.ru_minflt - prevUsage.ru_minflt);
		logadd("%ld,\t", infoUsage.ru_majflt - prevUsage.ru_majflt);
		logadd("%ld,\t", infoUsage.ru_nswap - prevUsage.ru_nswap);
		logadd("%ld,\t", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw);
		logadd("%ld,\t", infoUsage.ru_nivcsw - prevUsage.ru_nivcsw);
		prevUsage = infoUsage;
	}
#endif
	if (profileCounter[PROFrbytes].status)
		logadd("0,\t");
	if (profileCounter[PROFwbytes].status)
		logadd("0,\t");

	if (profileCounter[PROFaggr].status)
		logadd("0,\t0,\t");

	if (profileCounter[PROFstmt].status)
			logadd(" %s", cpuload);
	//if (profileCounter[PROFtype].status)
		//logadd("\"\",\t");
	//if (profileCounter[PROFuser].status)
		//logadd(" 0");
	logadd("]\n");
	logsend(logbuffer);
}

static MT_Id hbthread;
static volatile ATOMIC_TYPE hbrunning;

static void profilerHeartbeat(void *dummy)
{
	int t;

	(void) dummy;
	while (ATOMIC_GET(hbrunning, hbLock, "profilerHeartbeat")) {
		/* wait until you need this info */
		while (ATOMIC_GET(hbdelay, hbLock, "profilerHeatbeatEvent") == 0 || eventstream  == NULL) {
			for (t = 1000; t > 0; t -= 50) {
				MT_sleep_ms(50);
				if (!ATOMIC_GET(hbrunning, hbLock, "profilerHeartbeat"))
					return;
			}
		}
		for (t = (int) ATOMIC_GET(hbdelay, hbLock, "profilerHeatbeatEvent"); t > 0; t -= 50) {
			MT_sleep_ms(t > 50 ? 50 : t);
			if (!ATOMIC_GET(hbrunning, hbLock, "profilerHeartbeat"))
				return;
		}
		profilerHeartbeatEvent("ping", 0);
	}
	ATOMIC_SET(hbdelay, 0, hbLock, "profilerHeatbeat");
}

void startHeartbeat(int delay)
{
	if ( delay < 0 )
		return;
	ATOMIC_SET(hbdelay, (ATOMIC_TYPE) delay, hbLock, "startHeatbeat");
}

void stopHeartbeat(void)
{
	ATOMIC_SET(hbrunning, 0, hbLock, "stopHeartbeat");
	if (hbthread)
		MT_join_thread(hbthread);
}

void initHeartbeat(void)
{
#ifdef NEED_MT_LOCK_INIT
	ATOMIC_INIT(hbLock, "hbLock");
#endif
	hbrunning = 1;
	if (MT_create_thread(&hbthread, profilerHeartbeat, NULL, MT_THR_JOINABLE) < 0) {
		/* it didn't happen */
		hbthread = 0;
		hbrunning = 0;
	}
}
