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

/* (c) M.L. Kersten
 * Performance tracing
 * The performance monitor has exclusive access to the event file, which
 * avoids concurrency conflicts amongst clients. It also avoid cluthered
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

static void offlineProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc, int start);
static void cachedProfilerEvent(int idx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
static int initTrace(void);

int malProfileMode = 0;     /* global flag to indicate profiling mode */
static int eventcounter = 0;

#ifdef HAVE_SYS_RESOURCE_H
struct rusage infoUsage;
static struct rusage prevUsage;
#endif

#define LOGLEN 8192
#define lognew()  loglen = 0; logbase = logbuffer; *logbase = 0;

#define logadd(...) {														\
	do {																\
		(void) snprintf(logbase+loglen, LOGLEN -1 - loglen, __VA_ARGS__); \
		loglen += (int) strlen(logbase+loglen);							\
	} while (0);}

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
	logadd("event,\t");
	logadd("\ttime,\t");
	logadd("thread,\t");
	logadd("status,\t");
	logadd("\tpc,\t");
	logadd("usec,\t");
	logadd("rssMB,\t");
	logadd("vmMB,\t");

#ifdef NUMAprofiling
		logadd("numa,\t");
#endif
#ifdef HAVE_SYS_RESOURCE_H
	logadd("rdblk,\t");
	logadd("wrblk,\t");
	logadd("rclm,\t");
	logadd("fault,\t");
	logadd("swaps,\t");
	//logadd("switch,\t");
	//logadd("isw,\t");
#endif
	logadd("user,\t");
	logadd("stmt,\t");
	logadd("# name \n");
	if (eventstream){
		mnstr_printf(eventstream,"%s\n", logbuffer);
		mnstr_flush(eventstream);
	}
}

/*
 * Offline processing
 * The offline processing structure is the easiest. We merely have to
 * produce a correct tuple format for the front-end.
 * To avoid unnecessary locks we first build the event as a string
 * It uses a local logbuffer[LOGLEN] and logbase, logtop, loglen
 */
static void logsend(char *logbuffer)
{ int error=0;
	if (eventstream) {
		MT_lock_set(&mal_profileLock, "logsend");
		if( eventcounter == 0)
			offlineProfilerHeader();
		eventcounter++;
		error= mnstr_printf(eventstream,"[ %d,\t%s", eventcounter, logbuffer);
		error= mnstr_flush(eventstream);
		MT_lock_unset(&mal_profileLock, "logsend");
		if ( error) stopProfiler();
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
	if ( !start && pci->token == ENDsymbol)
		profilerHeartbeatEvent("ping",0);
	if (getModuleId(pci) == myname) // ignore profiler commands from monitoring
		return;
	if (offlineProfiling)
		offlineProfilerEvent(idx, mb, stk, pci, start);
	if (cachedProfiling && !start )
		cachedProfilerEvent(idx, mb, stk, pci);
}

/*
 * Unlike previous versions we issue a fixed record of performance information.
 */
void
offlineProfilerEvent(int usrid, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start)
{
	char logbuffer[LOGLEN], *logbase;
	int loglen;
	char ctm[26];
	time_t clk;
	struct timeval clock;
	str stmt, c;
	char *tbuf;
	str stmtq;

#ifdef HAVE_TIMES
	struct tms newTms;
#endif

	if (eventstream == NULL)
		return ;

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
	logadd("\"%.8s.%06ld\",\t", tbuf + 11, (long) clock.tv_usec);
	logadd("%d,\t", THRgettid());
	if( start){
		logadd("\"start\",\t");
	} else {
		logadd("\"done \",\t");
	}
	logadd("%d,\t", pci->pc);
	logadd(LLFMT ",\t", pci->ticks);
	logadd(SZFMT ",\t", MT_getrss()/1024/1024);
	logadd(SZFMT ",\t", GDKvm_cursize()/1024/1024);

#ifdef NUMAprofiling
		logadd("\"");
		for( i= pci->retc ; i < pci->argc; i++)
		if( !isVarConstant(mb, getArg(pci,i)) && mb->var[getArg(pci,i)]->worker)
			logadd("@%d", mb->var[getArg(pci,i)]->worker);
		logadd("\",\t");
#endif

#ifdef HAVE_SYS_RESOURCE_H
	logadd("%ld,\t", infoUsage.ru_inblock - prevUsage.ru_inblock);
	logadd("%ld,\t", infoUsage.ru_oublock - prevUsage.ru_oublock);
	logadd("%ld,\t", infoUsage.ru_minflt - prevUsage.ru_minflt);
	logadd("%ld,\t", infoUsage.ru_majflt - prevUsage.ru_majflt);
	logadd("%ld,\t", infoUsage.ru_nswap - prevUsage.ru_nswap);
	//logadd("%ld,\t", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw);
	//logadd("%ld,\t", infoUsage.ru_nivcsw - prevUsage.ru_nivcsw);
	prevUsage = infoUsage;
#endif
	logadd("%d,\t", usrid);

	/* generate actual call statement */
	stmt = instruction2str(mb, stk, pci, LIST_MAL_CALL);
	c = stmt;

	while (c && *c && isspace((int)*c))
		c++;
	stmtq = mal_quote(c, strlen(c));
	if (stmtq != NULL) {
		logadd(" \"%s\",\t", stmtq);
		GDKfree(stmtq);
	} else logadd(" ,\t");
	GDKfree(stmt);
	
	logadd("]\n");
	logsend(logbuffer);
}
/*
 * Postprocessing events
 * The events may be sent for offline processing through a
 * stream, including "stdout".
 */


str
setLogFile(stream *fd, Module mod, const char *fname)
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
setLogStream(Module cntxt, const char *host, int port)
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
	malProfileMode = -1;
	eventstream = fd;
	return MAL_SUCCEED;
}

str
closeProfilerStream(void)
{
	if (eventstream && eventstream != mal_clients[0].fdout && eventstream != GDKout && eventstream != GDKerr) {
		(void)mnstr_close(eventstream);
		(void)mnstr_destroy(eventstream);
	}
	eventstream = NULL;
	malProfileMode = 0;
	return MAL_SUCCEED;
}

/*
 * When you receive the message to start profiling, we
 * should wait for the next instruction the stream
 * is initiated. This is controlled by a delay-switch
 */
static int TRACE_init = 0;

str
startProfiler(int mode, int beat)
{
	Client c;
	int i,j;

#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	prevUsage = infoUsage;
#endif
	if (myname == 0)
		myname = putName("profiler", 8);

	MT_lock_set(&mal_profileLock, "startProfiler");
	if (eventstream != NULL) {
		offlineProfiling = TRUE;
	} else {
		cachedProfiling = TRUE;
	}
	if (TRACE_init == 0)
		_initTrace();
	malProfileMode = mode;
	eventcounter = 0;
	setHeartbeat(beat); 
	MT_lock_unset(&mal_profileLock, "startProfiler");

	/* show all in progress instructions for stethoscope startup */
	if( mode > 0){
		for (i = 0; i < MAL_MAXCLIENTS; i++) {
			c = mal_clients+i;
			if ( c->active ) 
				for(j = 0; j <THREADS; j++)
				if( c->inprogress[j].mb)
				/* show the event */
					profilerEvent(i, c->inprogress[j].mb, c->inprogress[j].stk, c->inprogress[j].pci, 1);
		}
	}

	return MAL_SUCCEED;
}

str
stopProfiler(void)
{
	MT_lock_set(&mal_profileLock, "stopProfiler");
	malProfileMode = 0;
	offlineProfiling = FALSE;
	cachedProfiling = FALSE;
	setHeartbeat(0); // stop heartbeat
	closeProfilerStream();
	MT_lock_unset(&mal_profileLock, "stopProfiler");
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

/*
 * Extern sources may dump information on the profiler stream
*/
stream *
getProfilerStream(void)  
{
	return eventstream;
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
static BAT *TRACE_id_reads = 0;
static BAT *TRACE_id_writes = 0;
static BAT *TRACE_id_thread = 0;
static BAT *TRACE_id_user = 0;
static BAT *TRACE_id_rssMB = 0;
static BAT *TRACE_id_vmMB = 0;

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
	r[6] = BATcopy(TRACE_id_rssMB, TRACE_id_rssMB->htype, TRACE_id_rssMB->ttype, 0, TRANSIENT);
	r[7] = BATcopy(TRACE_id_vmMB, TRACE_id_vmMB->htype, TRACE_id_vmMB->ttype, 0, TRANSIENT);
	r[8] = BATcopy(TRACE_id_reads, TRACE_id_reads->htype, TRACE_id_reads->ttype, 0, TRANSIENT);
	r[9] = BATcopy(TRACE_id_writes, TRACE_id_writes->htype, TRACE_id_writes->ttype, 0, TRANSIENT);
	r[10] = BATcopy(TRACE_id_stmt, TRACE_id_stmt->htype, TRACE_id_stmt->ttype, 0, TRANSIENT);
	MT_lock_unset(&mal_profileLock, "TRACEtable");
}

static BAT *
TRACEcreate(const char *hnme, const char *tnme, int tt)
{
	BAT *b;
	char buf[128];

	snprintf(buf, 128, "trace_%s_%s", hnme, tnme);
	b = BATdescriptor(BBPindex(buf));
	if (b) {
		BBPincref(b->batCacheid, TRUE);
		return b;
	}

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
	CLEANUPprofile(TRACE_id_rssMB);
	CLEANUPprofile(TRACE_id_vmMB);
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
	TRACE_id_rssMB = TRACEcreate("id", "rssMB", TYPE_lng);
	TRACE_id_vmMB = TRACEcreate("id", "vmMB", TYPE_lng);
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
		TRACE_id_rssMB == NULL ||
		TRACE_id_vmMB == NULL ||
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
	BBPclear(TRACE_id_rssMB->batCacheid);
	BBPclear(TRACE_id_vmMB->batCacheid);
	BBPclear(TRACE_id_thread->batCacheid);
	BBPclear(TRACE_id_user->batCacheid);
	BBPclear(TRACE_id_reads->batCacheid);
	BBPclear(TRACE_id_writes->batCacheid);
	TRACE_init = 0;
	_initTrace();
	MT_lock_unset(&mal_contextLock, "cleanup");
}

BAT *
getTrace(const char *nme)
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
	if (strcmp(nme, "rssMB") == 0)
		return BATcopy(TRACE_id_rssMB, TRACE_id_rssMB->htype, TRACE_id_rssMB->ttype, 0, TRANSIENT);
	if (strcmp(nme, "vmMB") == 0)
		return BATcopy(TRACE_id_vmMB, TRACE_id_vmMB->htype, TRACE_id_vmMB->ttype, 0, TRANSIENT);
	if (strcmp(nme, "reads") == 0)
		return BATcopy(TRACE_id_reads, TRACE_id_reads->htype, TRACE_id_reads->ttype, 0, TRANSIENT);
	if (strcmp(nme, "writes") == 0)
		return BATcopy(TRACE_id_writes, TRACE_id_writes->htype, TRACE_id_writes->ttype, 0, TRANSIENT);
	return NULL;
}

int
getTraceType(const char *nme)
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
	if (strcmp(nme, "rssMB") == 0)
		return newColumnType( TYPE_lng);
	if (strcmp(nme, "vmMB") == 0)
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
	char ctm[27]={0};
	int tid = (int)THRgettid();
	lng v1 = 0, v2= 0;
	str stmt, c;
	time_t clk;
	struct timeval clock;
	lng rssMB = MT_getrss()/1024/1024;
	lng vmMB = GDKvm_cursize()/1024/1024;

#ifdef HAVE_TIMES
	struct tms newTms;
#endif

	/* struct Mallinfo infoMalloc; */

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
	if (ctime_r(&clk, ctm, sizeof(ctm)) == NULL)
		strncpy(ctm, "", sizeof(ctm));
#else
#ifdef HAVE_CTIME_R
	if (ctime_r(&clk, ctm) == NULL)
		strncpy(ctm, "", sizeof(ctm));
#else
	{
		char *tbuf = ctime(&clk);
		strncpy(ctm, tbuf ? tbuf : "", sizeof(ctm));
	}
#endif
#endif
	/* sneakily overwrite year with second fraction */
	snprintf(ctm + 19, 6, ".%03d", (int)(clock.tv_usec / 1000));

	/* generate actual call statement */
	stmt = instruction2str(mb, stk, pci, LIST_MAL_CALL);
	c = stmt;

	while (c && *c && (isspace((int)*c) || *c == '!'))
		c++;

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
	TRACE_id_rssMB = BUNappend(TRACE_id_rssMB, &rssMB, FALSE);
	TRACE_id_vmMB = BUNappend(TRACE_id_vmMB, &vmMB, FALSE);
	TRACE_id_reads = BUNappend(TRACE_id_reads, &v1, FALSE);
	TRACE_id_writes = BUNappend(TRACE_id_writes, &v2, FALSE);
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
	bat i;
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
	for ( s= buf; *s; s++) {
		if ( strncmp(s,"cpu",3)== 0){
			s +=3;
			if ( *s == ' ') {
				s++;
				cpu = 255; // the cpu totals stored here
			}  else {
				cpu = atoi(s);
				if (cpu < 0 || cpu > 255)
					cpu = 255;
			}
			s= strchr(s,' ');
			if (s == NULL)		/* unexpected format of file */
				break;

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
	  skip:
		while (*s && *s != '\n')
			s++;
	}

	s= cpuload;
	len = BUFSIZ;
	// identify core processing
	snprintf(s, len, "\"[ ");
	len -= (int)strlen(s);
	s += (int) strlen(s);
	for ( cpu = 0; cpuload && cpu < 255 && corestat[cpu].user; cpu++) {
		snprintf(s, len, " %.2f ",corestat[cpu].load);
		len -= (int)strlen(s);
		s += (int) strlen(s);
	}
	snprintf(s, len, "]\"");
	len -= (int)strlen(s);
	s += (int) strlen(s);
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

void profilerHeartbeatEvent(const char *msg, lng ticks)
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
	{
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
		if (tbuf){
			logadd("\"%.8s.%06ld\",\t", tbuf + 11, (long) tv.tv_usec);
		}else{
			logadd("%s,\t", "nil");
		}
	}
	logadd("%d,\t", THRgettid());
	logadd("\"%s\",\t",msg);
	logadd("0,\t");
	logadd(LLFMT",\t", ticks);
	logadd(SZFMT ",\t", MT_getrss()/1024/1024);
	logadd(SZFMT ",\t", GDKvm_cursize()/1024/1024);
#ifdef NUMAprofiling
	logadd("\"\",\t");
#endif
#ifdef HAVE_SYS_RESOURCE_H
	logadd("%ld,\t", infoUsage.ru_inblock - prevUsage.ru_inblock);
	logadd("%ld,\t", infoUsage.ru_oublock - prevUsage.ru_oublock);
	logadd("%ld,\t", infoUsage.ru_minflt - prevUsage.ru_minflt);
	logadd("%ld,\t", infoUsage.ru_majflt - prevUsage.ru_majflt);
	logadd("%ld,\t", infoUsage.ru_nswap - prevUsage.ru_nswap);
	prevUsage = infoUsage;
#endif
	logadd("0,\t");
	logadd(" %s", cpuload);
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
			for (t = 1000; t > 0; t -= 15) {
				MT_sleep_ms(15);
				if (!ATOMIC_GET(hbrunning, hbLock, "profilerHeartbeat"))
					return;
			}
		}
		for (t = (int) ATOMIC_GET(hbdelay, hbLock, "profilerHeatbeatEvent"); t > 0; t -= 15) {
			MT_sleep_ms(t > 15 ? 15 : t);
			if (!ATOMIC_GET(hbrunning, hbLock, "profilerHeartbeat"))
				return;
		}
		profilerHeartbeatEvent("ping",0);
	}
	ATOMIC_SET(hbdelay, 0, hbLock, "profilerHeatbeat");
}

void setHeartbeat(int delay)
{
	if (hbthread &&  delay < 0 ){
		ATOMIC_SET(hbrunning, 0, hbLock, "stopHeartbeat");
		MT_join_thread(hbthread);
		return;
	}
	if (delay <= 10)
		hbdelay =10;
	ATOMIC_SET(hbdelay, (ATOMIC_TYPE) delay, hbLock, "startHeatbeat");
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
