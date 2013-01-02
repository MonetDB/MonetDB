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

/*
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
static void offlineProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, int pc, int start);
static void cachedProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, int pc);
static int initTrace(void);

int malProfileMode = 0;     /* global flag to indicate profiling mode */
static int profileAll = 0;  /* all instructions should be profiled */
static int delayswitch = 0; /* to wait before sending the profile info */

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
	/*  20 */  { 0, 0}
};

/*
 * The counters can be set individually.
 */
str
activateCounter(str name)
{
	int i;
	for (i = 0; profileCounter[i].name; i++)
		if (strcmp(profileCounter[i].name, name) == 0) {
			profileCounter[i].status = 1;
			return 0;
		}
	throw(MAL, "activateCounter", RUNTIME_OBJECT_UNDEFINED ":%s", name);
}

str
deactivateCounter(str name)
{
	int i;
	for (i = 0; profileCounter[i].name; i++)
		if (strcmp(profileCounter[i].name, name) == 0) {
			profileCounter[i].status = 0;
			return 0;
		}
	throw(MAL, "deactivateCounter", RUNTIME_OBJECT_UNDEFINED ":%s", name);
}

/*
 * Offline processing
 * The offline processing structure is the easiest. We merely have to
 * produce a correct tuple format for the front-end.
 */
#define log(...)												\
	do {														\
		if (eventstream)										\
			if (mnstr_printf(eventstream, __VA_ARGS__) < 0) {	\
				closeProfilerStream();							\
			}													\
	} while (0)
#define flushLog() if (eventstream) mnstr_flush(eventstream);

/*
 * Event dispatching
 * The profiler strategy is encapsulated here
 * Note that the profiler itself should lead to event generations.
 */
void
profilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, int pc, int start)
{
	if (mb->profiler == NULL) return;
	if (profileCounter[PROFdot].status == 1 && start && pc == 0){
		if (mb->dotfile == 0){
			MT_lock_set(&mal_profileLock, "profileLock");
			showFlowGraph(mb,stk,"stethoscope");
			MT_lock_unset(&mal_profileLock, "profileLock");
		}
	}
	if (profileCounter[PROFstart].status == 0 && start)
		return;
	if (myname == 0)
		myname = putName("profiler", 8);
	if (getModuleId(getInstrPtr(mb, pc)) == myname)
		return;
	if (offlineProfiling)
		offlineProfilerEvent(idx, mb, stk, pc,start);
	if (cachedProfiling)
		cachedProfilerEvent(idx, mb, stk, pc);
}

static void
offlineProfilerHeader(void)
{
	MT_lock_set(&mal_profileLock, "profileLock");
	if (eventstream == NULL) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		return ;
	}
	log("# ");
	if (profileCounter[PROFevent].status) {
		log("event,\tstatus,\t");
	}
	if (profileCounter[PROFtime].status) {
		log("time,\t");
	}
	if (profileCounter[PROFthread].status) {
		log("thread,\t");
	}
	if (profileCounter[PROFflow].status)
		log("claim,\tmemory,\t");
	if (profileCounter[PROFfunc].status) {
		log("function,\t");
	}
	if (profileCounter[PROFpc].status) {
		log("pc,\t");
	}
	if (profileCounter[PROFticks].status) {
		log("usec,\t");
	}
	if (profileCounter[PROFcpu].status) {
		log("utime,\t");
		log("cutime,\t");
		log("stime,\t");
		log("cstime,\t");
	}

	if (profileCounter[PROFmemory].status) {
		log("maxrss,\t");
		log("arena,\t");
		log("ordblks,\t");
		log("smblks,\t");
		log("hblkhd,\t");
		log("hblks,\t");
		log("fsmblks,\t");
		log("uordblks,\t");
	}
	if (profileCounter[PROFreads].status)
		log("blk reads,\t");
	if (profileCounter[PROFwrites].status)
		log("blk writes,\t");
	if (profileCounter[PROFprocess].status) {
		log("pg reclaim,\t");
		log("pg faults,\t");
		log("swaps,\t");
		log("ctxt switch,\t");
		log("inv switch,\t");
	}
	if (profileCounter[PROFrbytes].status)
		log("rbytes,\t");
	if (profileCounter[PROFwbytes].status)
		log("wbytes,\t");
	if (profileCounter[PROFaggr].status)
		log("count,\t totalticks,\t");
	if (profileCounter[PROFstmt].status)
		log("stmt,\t");
	if (profileCounter[PROFtype].status)
		log("types,\t");
	if (profileCounter[PROFuser].status)
		log("user,\t");
	log("# name\n");
	flushLog();
	MT_lock_unset(&mal_profileLock, "profileLock");
}

void
offlineProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, int pc, int start)
{
	static struct Mallinfo prevMalloc;
	InstrPtr pci = getInstrPtr(mb, pc);

#ifdef HAVE_SYS_RESOURCE_H
	static struct rusage prevUsage;
	struct rusage infoUsage;
#endif
	static int eventcounter;
#ifdef HAVE_TIMES
	struct tms newTms;
#endif
	struct Mallinfo infoMalloc;
	str stmt, c;

	if (delayswitch > 0) {
		/* first call to profiled */
		offlineProfilerHeader();
		delayswitch--;
	}
	MT_lock_set(&mal_profileLock, "profileLock");
	if (eventstream == NULL) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		return ;
	}
	if (delayswitch == 0) {
		delayswitch = -1;
	}
	if (!profileAll && mb->profiler[pc].trace == FALSE) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		return;
	}
#ifdef HAVE_TIMES
	times(&newTms);
#endif
	infoMalloc = MT_mallinfo();
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
#endif

	/* make basic profile event tuple  */
	log("[ ");
	if (profileCounter[PROFevent].status) {
		log("%d,\t", eventcounter);
	}
	if (profileCounter[PROFstart].status) {
		if ( start) {
			log("\"start\",\t");
		} else {
			log("\"done\" ,\t");
		}
	}
	if (profileCounter[PROFtime].status) {
		char *tbuf, *c;

		/* without this cast, compilation on Windows fails with
		 * argument of type "long *" is incompatible with parameter of type "const time_t={__time64_t={__int64}} *"
		 */
		time_t clock = (time_t) mb->profiler[pc].clock.tv_sec;
		tbuf = ctime(&clock);
		if (tbuf) {
			c = strchr(tbuf, '\n');
			if (c) {
				c[-5] = 0;
			}
			tbuf[10] = '"';
			log("%s", tbuf + 10);
			log(".%06d\",\t", (int)mb->profiler[pc].clock.tv_usec);
		} else
			log("%s,\t", "nil");
	}
	if (profileCounter[PROFthread].status) {
		log(" %d,\t", THRgettid());
	}
	if (profileCounter[PROFflow].status) {
		log("%d,\t", memoryclaims);
		log(LLFMT",\t", memoryclaims?((lng)(MEMORY_THRESHOLD * monet_memory)-memorypool)/1024/1024:0);
	}
	if (profileCounter[PROFfunc].status) {
		if (getModuleId(getInstrPtr(mb,0)) && getFunctionId(getInstrPtr(mb,0))) {
			log("\"%s.%s\",\t", getModuleId(getInstrPtr(mb,0)), getFunctionId(getInstrPtr(mb,0)));
		} else
			log("\"%s\",\t", operatorName(pci->token));
	}
	if (profileCounter[PROFpc].status) {
		log("%d,\t", getPC(mb, pci));
	}
	if (profileCounter[PROFticks].status) {
		log(LLFMT ",\t", mb->profiler[pc].ticks);
	}
#ifdef HAVE_TIMES
	if (profileCounter[PROFcpu].status && delayswitch < 0) {
		log("%ld,\t", (long) (newTms.tms_utime - mb->profiler[pc].timer.tms_utime));
		log("%ld,\t", (long) (newTms.tms_cutime - mb->profiler[pc].timer.tms_cutime));
		log("%ld,\t", (long) (newTms.tms_stime - mb->profiler[pc].timer.tms_stime));
		log("%ld,\t", (long) (newTms.tms_cstime - mb->profiler[pc].timer.tms_cstime));
	}
#endif

	if (profileCounter[PROFmemory].status && delayswitch < 0) {
#ifdef HAVE_SYS_RESOURCE_H
		log("%ld,\t", infoUsage.ru_maxrss);
#endif
		log(SZFMT ",\t", (size_t)(infoMalloc.arena - prevMalloc.arena));
		log(SZFMT ",\t", (size_t)(infoMalloc.ordblks - prevMalloc.ordblks));
		log(SZFMT ",\t", (size_t)(infoMalloc.smblks - prevMalloc.smblks));
		log(SZFMT ",\t", (size_t)(infoMalloc.hblkhd - prevMalloc.hblkhd));
		log(SZFMT ",\t", (size_t)(infoMalloc.hblks - prevMalloc.hblks));
		log(SZFMT ",\t", (size_t)(infoMalloc.fsmblks - prevMalloc.fsmblks));
		log(SZFMT ",\t", (size_t)(infoMalloc.uordblks - prevMalloc.uordblks));
		prevMalloc = infoMalloc;
	}
#ifdef HAVE_SYS_RESOURCE_H
	if ((profileCounter[PROFreads].status ||
		 profileCounter[PROFwrites].status) && delayswitch < 0) {
		log("%ld,\t", infoUsage.ru_inblock - prevUsage.ru_inblock);
		log("%ld,\t", infoUsage.ru_oublock - prevUsage.ru_oublock);
		prevUsage = infoUsage;
	}
	if (profileCounter[PROFprocess].status && delayswitch < 0) {
		log("%ld,\t", infoUsage.ru_minflt - prevUsage.ru_minflt);
		log("%ld,\t", infoUsage.ru_majflt - prevUsage.ru_majflt);
		log("%ld,\t", infoUsage.ru_nswap - prevUsage.ru_nswap);
		log("%ld,\t", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw);
		log("%ld,\t", infoUsage.ru_nivcsw - prevUsage.ru_nivcsw);
		prevUsage = infoUsage;
	}
#endif
	if (profileCounter[PROFrbytes].status)
		log(LLFMT ",\t", mb->profiler[pc].rbytes);
	if (profileCounter[PROFwbytes].status)
		log(LLFMT ",\t", mb->profiler[pc].wbytes);

	if (profileCounter[PROFaggr].status)
		log("%d,\t" LLFMT ",\t", mb->profiler[pc].counter, mb->profiler[pc].totalticks);

	if (profileCounter[PROFstmt].status) {
		/* generate actual call statement */
		str stmtq;
		stmt = instruction2str(mb, stk, pci, LIST_MAL_DEBUG);
		c = stmt;

		while (c && *c && isspace((int)*c))
			c++;
		stmtq = mal_quote(c, strlen(c));
		if (stmtq != NULL) {
			log(" \"%s\",\t", stmtq);
			GDKfree(stmtq);
		} else log(" ,\t");
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
		log("\"%s\",\t", abuf);
	}
	if (profileCounter[PROFuser].status) {
		log(" %d", idx);
	}
	log(" ]\n");
	eventcounter++;
	flushLog();
	MT_lock_unset(&mal_profileLock, "profileLock");
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
	MT_lock_set(&mal_profileLock, "profileLock");
	if (eventstream ) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		throw(IO, "mal.profiler", "Log file already set");
	}
	if (strcmp(fname, "console") == 0)
		eventstream = mal_clients[0].fdout;
	else if (strcmp(fname, "stdout") == 0)
		eventstream = fd;
	else
		eventstream = open_wastream(fname);
	if (eventstream == NULL) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		throw(IO, "mal.profiler", RUNTIME_STREAM_FAILED);
	}
	MT_lock_unset(&mal_profileLock, "profileLock");
	return MAL_SUCCEED;
}

str
setLogStream(Module cntxt, str host, int port)
{
	(void)cntxt;        /* still unused */
	MT_lock_set(&mal_profileLock, "profileLock");
	if ((eventstream = udp_wastream(host, port, "profileStream")) == NULL) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		throw(IO, "mal.profiler", RUNTIME_STREAM_FAILED);
	}
	eventstream = wbstream(eventstream, BUFSIZ);
	MT_lock_unset(&mal_profileLock, "profileLock");
	return MAL_SUCCEED;
}

str
setLogStreamStream(Module cntxt, stream *s)
{
	(void)cntxt;        /* still unused */
	MT_lock_set(&mal_profileLock, "profileLock");
	if ((eventstream = s) == NULL) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		throw(ILLARG, "mal.profiler", "stream must not be NULL");
	}
	eventstream = wbstream(eventstream, BUFSIZ);
	MT_lock_unset(&mal_profileLock, "profileLock");
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
	if (eventstream) {
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
	MT_lock_set(&mal_profileLock, "profileLock");
	if (eventstream == NULL) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		return MAL_SUCCEED ;
	}
	mnstr_printf(GDKout, "# start point not set\n");
	flushLog();
	MT_lock_unset(&mal_profileLock, "profileLock");
	return MAL_SUCCEED;
}

str
setEndPoint(Module cntxt, str mod, str fcn)
{
	(void)cntxt;
	(void)mod;
	(void)fcn;      /* still unused */
	MT_lock_set(&mal_profileLock, "profileLock");
	if (eventstream == NULL) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		return MAL_SUCCEED ;
	}
	mnstr_printf(GDKout, "# end point not set\n");
	flushLog();
	MT_lock_unset(&mal_profileLock, "profileLock");
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
	MT_lock_set(&mal_profileLock, "profileLock");
	if (eventstream != NULL) {
		offlineProfiling = TRUE;
		delayswitch = 1;
	} else
		cachedProfiling = TRUE;
	if (TRACE_init == 0)
		_initTrace();
	malProfileMode = TRUE;
	MT_lock_unset(&mal_profileLock, "profileLock");
	return MAL_SUCCEED;
}

str
stopProfiling(void)
{
	malProfileMode = FALSE;
	offlineProfiling = FALSE;
	cachedProfiling = FALSE;
	closeProfilerStream();
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
	MT_lock_set(&mal_profileLock, "profileLock");
	eventstream = 0;
	MT_lock_unset(&mal_profileLock, "profileLock");
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

	initProfiler(mb);
	for (k = 0; k < mb->stop; k++) {
		p = getInstrPtr(mb, k);
		cnt = 0;
		for (i = 0; i < topFilter; i++)
			cnt += instrFilter(p, modFilter[i], fcnFilter[i]);
		mb->profiler[k].trace = profileAll || cnt ||
								(mod && fcn && instrFilter(p, mod, fcn));
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

	MT_lock_set(&mal_profileLock, "profileLock");
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
	MT_lock_unset(&mal_profileLock, "profileLock");
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
	MalBlkPtr mb;

	(void)mod;
	(void)fcn;      /* still unused */

	MT_lock_set(&mal_profileLock, "profileLock");
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
						if (t->def && (mb = t->def)->profiler)
							for (k = 0; k < t->def->stop; k++)
								if (instrFilter(getInstrPtr(t->def, k), mod, fcn)) {
									mb->profiler[k].trace = FALSE;
								}
					}
				}
		s = s->outer;
	}
	MT_lock_unset(&mal_profileLock, "profileLock");
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
				initProfiler(mb);
				mb->profiler[i].trace = TRUE;
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
				mb->profiler[i].trace = FALSE;
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
	MT_lock_set(&mal_profileLock, "profileLock");
	r[0] = BATcopy(TRACE_id_event, TRACE_id_event->htype, TRACE_id_event->ttype, 0);
	r[1] = BATcopy(TRACE_id_time, TRACE_id_time->htype, TRACE_id_time->ttype, 0);
	r[2] = BATcopy(TRACE_id_pc, TRACE_id_pc->htype, TRACE_id_pc->ttype, 0);
	r[3] = BATcopy(TRACE_id_thread, TRACE_id_thread->htype, TRACE_id_thread->ttype, 0);
	r[4] = BATcopy(TRACE_id_user, TRACE_id_user->htype, TRACE_id_user->ttype, 0);
	r[5] = BATcopy(TRACE_id_ticks, TRACE_id_ticks->htype, TRACE_id_ticks->ttype, 0);
	r[6] = BATcopy(TRACE_id_reads, TRACE_id_reads->htype, TRACE_id_reads->ttype, 0);
	r[7] = BATcopy(TRACE_id_writes, TRACE_id_writes->htype, TRACE_id_writes->ttype, 0);
	r[8] = BATcopy(TRACE_id_rbytes, TRACE_id_rbytes->htype, TRACE_id_rbytes->ttype, 0);
	r[9] = BATcopy(TRACE_id_wbytes, TRACE_id_wbytes->htype, TRACE_id_wbytes->ttype, 0);
	r[10] = BATcopy(TRACE_id_type, TRACE_id_type->htype, TRACE_id_type->ttype, 0);
	r[11] = BATcopy(TRACE_id_stmt, TRACE_id_stmt->htype, TRACE_id_stmt->ttype, 0);
	MT_lock_unset(&mal_profileLock, "profileLock");
}

static BAT *
TRACEcreate(str hnme, str tnme, int tt)
{
	BAT *b;
	char buf[128];

	snprintf(buf, 128, "trace_%s_%s", hnme, tnme);
	b = BATdescriptor(BBPindex(buf));
	if (b) {
		if (b->htype == TYPE_int)
			/* old code */
			BBPreclaim(b);
		else
			return b;
	}

	b = BATnew(TYPE_void, tt, 1 << 16);
	if (b == NULL)
		return NULL;

	BATseqbase(b, 0);
	BATkey(b, TRUE);
	BBPrename(b->batCacheid, buf);
	BATmode(b, PERSISTENT);
	BATcommit(b);
	return b;
}


#define CLEANUPprofile(X)  if (X) { BBPdecref((X)->batCacheid, TRUE); (X)->batPersistence = TRANSIENT; } (X) = NULL;

static void
_cleanupProfiler(void)
{
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
	MT_lock_set(&mal_contextLock, "profileLock");
	_initTrace();
	MT_lock_unset(&mal_contextLock, "profileLock");
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
	if (strcmp(nme, "event") == 0)
		return BATcopy(TRACE_id_event, TRACE_id_event->htype, TRACE_id_event->ttype, 0);
	if (strcmp(nme, "time") == 0)
		return BATcopy(TRACE_id_time, TRACE_id_time->htype, TRACE_id_time->ttype, 0);
	if (strcmp(nme, "ticks") == 0)
		return BATcopy(TRACE_id_ticks, TRACE_id_ticks->htype, TRACE_id_ticks->ttype, 0);
	if (strcmp(nme, "pc") == 0)
		return BATcopy(TRACE_id_pc, TRACE_id_pc->htype, TRACE_id_pc->ttype, 0);
	if (strcmp(nme, "thread") == 0)
		return BATcopy(TRACE_id_thread, TRACE_id_thread->htype, TRACE_id_thread->ttype, 0);
	if (strcmp(nme, "user") == 0)
		return BATcopy(TRACE_id_user, TRACE_id_user->htype, TRACE_id_user->ttype, 0);
	if (strcmp(nme, "stmt") == 0)
		return BATcopy(TRACE_id_stmt, TRACE_id_stmt->htype, TRACE_id_stmt->ttype, 0);
	if (strcmp(nme, "type") == 0)
		return BATcopy(TRACE_id_type, TRACE_id_type->htype, TRACE_id_type->ttype, 0);
	if (strcmp(nme, "rbytes") == 0)
		return BATcopy(TRACE_id_rbytes, TRACE_id_rbytes->htype, TRACE_id_rbytes->ttype, 0);
	if (strcmp(nme, "wbytes") == 0)
		return BATcopy(TRACE_id_wbytes, TRACE_id_wbytes->htype, TRACE_id_wbytes->ttype, 0);
	if (strcmp(nme, "reads") == 0)
		return BATcopy(TRACE_id_reads, TRACE_id_reads->htype, TRACE_id_reads->ttype, 0);
	if (strcmp(nme, "writes") == 0)
		return BATcopy(TRACE_id_writes, TRACE_id_writes->htype, TRACE_id_writes->ttype, 0);
	return NULL;
}

int
getTraceType(str nme)
{
	if (initTrace())
		return TYPE_any;
	if (strcmp(nme, "time") == 0)
		return newBatType(TYPE_void, TYPE_str);
	if (strcmp(nme, "ticks") == 0)
		return newBatType(TYPE_void, TYPE_lng);
	if (strcmp(nme, "pc") == 0)
		return newBatType(TYPE_void, TYPE_str);
	if (strcmp(nme, "thread") == 0)
		return newBatType(TYPE_void, TYPE_int);
	if (strcmp(nme, "stmt") == 0)
		return newBatType(TYPE_void, TYPE_str);
	if (strcmp(nme, "rbytes") == 0)
		return newBatType(TYPE_void, TYPE_lng);
	if (strcmp(nme, "wbytes") == 0)
		return newBatType(TYPE_void, TYPE_lng);
	if (strcmp(nme, "reads") == 0 || strcmp(nme, "writes") == 0)
		return newBatType(TYPE_void, TYPE_lng);
	return TYPE_any;
}

void
cachedProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, int pc)
{
	/* static struct Mallinfo prevMalloc; */
	static int eventcounter = 0;
	char buf[1024];
	int tid = (int)THRgettid();
	lng v = 0;

#ifdef HAVE_TIMES
	struct tms newTms;
#endif

	/* struct Mallinfo infoMalloc; */
#ifdef HAVE_SYS_RESOURCE_H
	struct rusage infoUsage;
	static struct rusage prevUsage;
#endif
	str stmt, c;
	InstrPtr pci = getInstrPtr(mb, pc);

	if (delayswitch > 0) {
		/* first call to profiled */
		delayswitch--;
		return;
	}
	if (delayswitch == 0) {
		delayswitch = -1;
	}
	if (!(profileAll || mb->profiler[pc].trace))
		return;
#ifdef HAVE_TIMES
	times(&newTms);
#endif
	/* infoMalloc = MT_mallinfo(); */
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
#endif
	MT_lock_set(&mal_profileLock, "profileLock");
	if (initTrace() || TRACE_init == 0) {
		MT_lock_unset(&mal_profileLock, "profileLock");
		return;
	}

	/* update the Trace tables */
	snprintf(buf, 1024, "%s.%s[%d]",
		getModuleId(getInstrPtr(mb, 0)),
		getFunctionId(getInstrPtr(mb, 0)), getPC(mb, pci));
	TRACE_id_pc = BUNappend(TRACE_id_pc, buf, FALSE);

	TRACE_id_thread = BUNappend(TRACE_id_thread, &tid, FALSE);

	TRACE_id_user = BUNappend(TRACE_id_user, &idx, FALSE);

	TRACE_id_event = BUNappend(TRACE_id_event, &TRACE_event, FALSE);
	TRACE_event++;

	{
		char *tbuf, *c;

		/* without this cast, compilation on Windows fails with
		 * argument of type "long *" is incompatible with parameter of type "const time_t={__time64_t={__int64}} *"
		 */
		time_t clock = (time_t) mb->profiler[pc].clock.tv_sec;
		tbuf = ctime(&clock);
		c = strchr(tbuf, '\n');
		if (c)
			snprintf(c-5, 6, ".%03d", (int)mb->profiler[pc].clock.tv_usec / 1000);
		TRACE_id_time = BUNappend(TRACE_id_time, tbuf, FALSE);
	}

	TRACE_id_ticks = BUNappend(TRACE_id_ticks, &mb->profiler[pc].ticks, FALSE);

	/* generate actual call statement */
	stmt = instruction2str(mb, stk, pci, LIST_MAL_DEBUG);
	c = stmt;

	while (c && *c && (isspace((int)*c) || *c == '!'))
		c++;
	TRACE_id_stmt = BUNappend(TRACE_id_stmt, c, FALSE);

	{
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
		TRACE_id_type = BUNappend(TRACE_id_type, &abuf, FALSE);
	}
	if (stmt) GDKfree(stmt);

/* The remainder requires their own BATs
 #ifdef HAVE_TIMES
	if( profileCounter[PROFcpu].status ){
		log("%d,\t", newTms.tms_utime - mb->profiler[pc].timer.tms_utime);
		log("%d,\t", newTms.tms_cutime - mb->profiler[pc].timer.tms_cutime);
		log("%d,\t", newTms.tms_stime - mb->profiler[pc].timer.tms_stime);
		log("%d,\t", newTms.tms_cstime - mb->profiler[pc].timer.tms_cstime);
	}
 #endif

	if( profileCounter[PROFmemory].status ){
 #ifdef HAVE_SYS_RESOURCE_H
		log("%d,\t",infoUsage.ru_maxrss);
 #endif
		log("%d,\t", infoMalloc.arena-prevMalloc.arena);
		log("%d,\t", infoMalloc.ordblks-prevMalloc.ordblks);
		log("%d,\t", infoMalloc.smblks-prevMalloc.smblks);
		log("%d,\t", infoMalloc.hblkhd-prevMalloc.hblkhd);
		log("%d,\t", infoMalloc.hblks-prevMalloc.hblks);
		log("%d,\t", infoMalloc.fsmblks-prevMalloc.fsmblks);
		log("%d,\t", infoMalloc.uordblks-prevMalloc.uordblks);
		prevMalloc = infoMalloc;
	}
 */
#ifdef HAVE_SYS_RESOURCE_H
	v = infoUsage.ru_inblock - prevUsage.ru_inblock;
	TRACE_id_reads = BUNappend(TRACE_id_reads, &v, FALSE);
	v = infoUsage.ru_oublock - prevUsage.ru_oublock;
	TRACE_id_writes = BUNappend(TRACE_id_writes, &v, FALSE);
	prevUsage = infoUsage;
#else
	TRACE_id_reads = BUNappend(TRACE_id_reads, &v, FALSE);
	TRACE_id_writes = BUNappend(TRACE_id_writes, &v, FALSE);
#endif

/*
	if( profileCounter[PROFprocess].status ){
		log("%d,\t", infoUsage.ru_minflt- prevUsage.ru_minflt);
		log("%d,\t", infoUsage.ru_majflt- prevUsage.ru_majflt);
		log("%d,\t", infoUsage.ru_nswap- prevUsage.ru_nswap);
		log("%d,\t", infoUsage.ru_nvcsw- prevUsage.ru_nvcsw);
		log("%d,\t", infoUsage.ru_nivcsw- prevUsage.ru_nivcsw);
		prevUsage = infoUsage;
	}
 #endif
 */
	TRACE_id_rbytes = BUNappend(TRACE_id_rbytes, &mb->profiler[pc].rbytes, FALSE);
	TRACE_id_wbytes = BUNappend(TRACE_id_wbytes, &mb->profiler[pc].wbytes, FALSE);

	eventcounter++;
	flushLog();
	MT_lock_unset(&mal_profileLock, "profileLock");
}
/*
 * The profile vector is added to the MAL block the first time we
 * have to safe monitor information.
 */
void initProfiler(MalBlkPtr mb)
{
	if (mb->profiler) return;
	initTrace();
	mb->profiler = (ProfPtr)GDKzalloc(mb->ssize * sizeof(ProfRecord));
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

	for (i = 1; i < BBPsize; i++)
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
