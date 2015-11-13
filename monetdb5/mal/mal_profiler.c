/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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
static oid user = 0;

static void offlineProfilerEvent(MalBlkPtr mb, MalStkPtr stk, InstrPtr pc, int start, char *alter, char *msg);
static void cachedProfilerEvent(MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
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
		loglen += snprintf(logbase+loglen, LOGLEN -1 - loglen, __VA_ARGS__); \
	} while (0);}

static void
offlineProfilerHeader(void)
{
	char logbuffer[LOGLEN], *logbase;
	int loglen;

	if (eventstream == NULL) 
		return ;

	lognew();
	logadd("# ");
	logadd("event,\t");
	logadd("time,\t");
	logadd("pc,\t");
	logadd("thread,\t");
	logadd("state,\t");
	logadd("usec,\t");
	logadd("rssMB,\t");
	logadd("tmpspace,\t");

#ifdef NUMAprofiling
		logadd("numa,\t");
#endif
#ifdef HAVE_SYS_RESOURCE_H
	logadd("inblock,\t");
	logadd("oublock,\t");
	logadd("majflt,\t");
	logadd("nswap,\t");
	logadd("switch,\t");
#endif
	logadd("stmt,\t");
	logadd("# name \n");
	mnstr_printf(eventstream,"%s\n", logbuffer);
	mnstr_flush(eventstream);
}

/*
 * Offline processing
 * The offline processing structure is the easiest. We merely have to
 * produce a correct tuple format for the front-end.
 * To avoid unnecessary locks we first build the event as a string
 * It uses a local logbuffer[LOGLEN] and logbase, logtop, loglen
 */

static void logsend(char *logbuffer)
{	int error=0;
	int showsystem = 0;
	if (eventstream) {
		MT_lock_set(&mal_profileLock, "logsend");
		if( eventcounter == 0){
			offlineProfilerHeader();
			showsystem++;
		}
		eventcounter++;
		error= mnstr_printf(eventstream,"[ %d,\t%s", eventcounter, logbuffer);
		error= mnstr_flush(eventstream);
		MT_lock_unset(&mal_profileLock, "logsend");
		if( showsystem)
			offlineProfilerEvent(0, 0, 0, 0, "system", monet_characteristics);
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
profilerEvent(oid usr, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start)
{
	if( usr != user) return; // only trace your own commands
	if (stk == NULL) return;
	if (pci == NULL) return;
	if (getModuleId(pci) == myname) // ignore profiler commands from monitoring
		return;
	if (offlineProfiling)
		offlineProfilerEvent(mb, stk, pci, start,0,0);
	if (cachedProfiling && !start )
		cachedProfilerEvent(mb, stk, pci);
	if ( start && pci->pc ==0)
		profilerHeartbeatEvent("ping");
	if ( !start && pci->token == ENDsymbol)
		profilerHeartbeatEvent("ping");
}

/*
 * Unlike previous versions we issue a fixed record of performance information.
 */
void
offlineProfilerEvent(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start, char *alter, char *msg)
{
	char logbuffer[LOGLEN], *logbase;
	int loglen;
	char ctm[26];
	time_t clk;
	struct timeval clock;
	str stmt, c;
	char *tbuf;
	char buf[BUFSIZ];
	str stmtq;

	if (eventstream == NULL)
		return ;

	if( start) // show when instruction was started
		clock = pci->clock;
	else 
		gettimeofday(&clock, NULL);
	clk = clock.tv_sec;

	/* make basic profile event tuple  */
	lognew();

#ifdef HAVE_CTIME_R3
	tbuf = ctime_r(&clk, ctm, sizeof(ctm));
#else
#ifdef HAVE_CTIME_R
	tbuf = ctime_r(&clk, ctm);
#else
	tbuf = ctime(&clk);
#endif
#endif
	tbuf[19]=0;
	/* there should be less than 10^6 == 1M usecs in 1 sec */
	assert(clock.tv_usec >= 0 && clock.tv_usec < 1000000);
	logadd("\"%s.%06d\",\t", tbuf+11, (int) clock.tv_usec);
	if( alter){
		logadd("\"user.%s[0]0\",\t",alter);
	} else {
		snprintf(buf, BUFSIZ, "%s.%s[%d]%d",
		getModuleId(getInstrPtr(mb, 0)),
		getFunctionId(getInstrPtr(mb, 0)), getPC(mb, pci), stk->tag);
		logadd("\"%s\",\t", buf);
	}

	logadd("%d,\t", THRgettid());
	if( alter) {
		logadd("\"%s\",\t",alter);
		logadd("0,\t");
	} else 
	if( start){
		logadd("\"start\",\t");
		// determine the Estimated Time of Completion
		if ( pci->calls){
			logadd(LLFMT ",\t", pci->totticks/pci->calls);
		} else{
			logadd(LLFMT ",\t", pci->ticks);
		}
	} else {
		logadd("\"done \",\t");
		logadd(LLFMT ",\t", pci->ticks);
	}
	logadd(SZFMT ",\t", MT_getrss()/1024/1024);
	logadd(LLFMT ",\t", pci? pci->wbytes/1024/1024:0);

#ifdef NUMAprofiling
	if( alter){
		logadd("\"\",\t");
	} else {
		logadd("\"");
		for( i= pci->retc ; i < pci->argc; i++)
		if( !isVarConstant(mb, getArg(pci,i)) && mb->var[getArg(pci,i)]->worker)
			logadd("@%d", mb->var[getArg(pci,i)]->worker);
		logadd("\",\t");
	}
#endif

#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	logadd("%ld,\t", infoUsage.ru_inblock - prevUsage.ru_inblock);
	logadd("%ld,\t", infoUsage.ru_oublock - prevUsage.ru_oublock);
	logadd("%ld,\t", infoUsage.ru_majflt - prevUsage.ru_majflt);
	logadd("%ld,\t", infoUsage.ru_nswap - prevUsage.ru_nswap);
	logadd("%ld,\t", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw +infoUsage.ru_nivcsw - prevUsage.ru_nivcsw);
	prevUsage = infoUsage;
#else
	logadd("0,\t0,\t0,\t0,\t0,\t");
#endif

	if ( msg){
		logadd("\"%s\"",msg);
	} else {
		// TODO Obfusate instructions unless administrator calls for it.
		
		/* generate actual call statement */
		stmt = instruction2str(mb, stk, pci, LIST_MAL_ALL);
		c = stmt;

		while (c && *c && isspace((int)*c))
			c++;
		stmtq = mal_quote(c, strlen(c));
		if (stmtq != NULL) {
			logadd(" \"%s\"", stmtq);
			GDKfree(stmtq);
		} 
		GDKfree(stmt);
	}
	
	logadd("\t]\n"); // end marker
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
		mnstr_close(eventstream);
		mnstr_destroy(eventstream);
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
startProfiler(oid usr, int mode, int beat)
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
	user = usr;
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
	user = 0;
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
	if (mal_trace) // already traced on console
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
static BAT *TRACE_id_event = 0;
static BAT *TRACE_id_time = 0;
static BAT *TRACE_id_pc = 0;
static BAT *TRACE_id_thread = 0;
static BAT *TRACE_id_ticks = 0;
static BAT *TRACE_id_inblock = 0;
static BAT *TRACE_id_oublock = 0;
static BAT *TRACE_id_rssMB = 0;
static BAT *TRACE_id_tmpspace = 0;
static BAT *TRACE_id_minflt = 0;
static BAT *TRACE_id_majflt = 0;
static BAT *TRACE_id_nvcsw = 0;
static BAT *TRACE_id_stmt = 0;

void
TRACEtable(BAT **r)
{
	if (initTrace())
		return ;
	MT_lock_set(&mal_profileLock, "TRACEtable");
	r[0] = BATcopy(TRACE_id_event, TYPE_void, TRACE_id_event->ttype, 0, TRANSIENT);
	r[1] = BATcopy(TRACE_id_time, TYPE_void, TRACE_id_time->ttype, 0, TRANSIENT);
	r[2] = BATcopy(TRACE_id_pc, TYPE_void, TRACE_id_pc->ttype, 0, TRANSIENT);
	r[3] = BATcopy(TRACE_id_thread, TYPE_void, TRACE_id_thread->ttype, 0, TRANSIENT);
	r[4] = BATcopy(TRACE_id_ticks, TYPE_void, TRACE_id_ticks->ttype, 0, TRANSIENT);
	r[5] = BATcopy(TRACE_id_rssMB, TYPE_void, TRACE_id_rssMB->ttype, 0, TRANSIENT);
	r[6] = BATcopy(TRACE_id_tmpspace, TYPE_void, TRACE_id_tmpspace->ttype, 0, TRANSIENT);
	r[7] = BATcopy(TRACE_id_inblock, TYPE_void, TRACE_id_inblock->ttype, 0, TRANSIENT);
	r[8] = BATcopy(TRACE_id_oublock, TYPE_void, TRACE_id_oublock->ttype, 0, TRANSIENT);
	r[9] = BATcopy(TRACE_id_minflt, TYPE_void, TRACE_id_minflt->ttype, 0, TRANSIENT);
	r[10] = BATcopy(TRACE_id_majflt, TYPE_void, TRACE_id_majflt->ttype, 0, TRANSIENT);
	r[11] = BATcopy(TRACE_id_nvcsw, TYPE_void, TRACE_id_nvcsw->ttype, 0, TRANSIENT);
	r[12] = BATcopy(TRACE_id_stmt, TYPE_void, TRACE_id_stmt->ttype, 0, TRANSIENT);
	MT_lock_unset(&mal_profileLock, "TRACEtable");
}

static BAT *
TRACEcreate(const char *hnme, const char *tnme, int tt)
{
	BAT *b;
	char buf[BUFSIZ];

	snprintf(buf, BUFSIZ, "trace_%s_%s", hnme, tnme);
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
	CLEANUPprofile(TRACE_id_event);
	CLEANUPprofile(TRACE_id_time);
	CLEANUPprofile(TRACE_id_pc);
	CLEANUPprofile(TRACE_id_rssMB);
	CLEANUPprofile(TRACE_id_tmpspace);
	CLEANUPprofile(TRACE_id_inblock);
	CLEANUPprofile(TRACE_id_oublock);
	CLEANUPprofile(TRACE_id_minflt);
	CLEANUPprofile(TRACE_id_majflt);
	CLEANUPprofile(TRACE_id_nvcsw);
	CLEANUPprofile(TRACE_id_thread);
	CLEANUPprofile(TRACE_id_stmt);
	TRACE_init = 0;
}

void
_initTrace(void)
{
	TRACE_id_event = TRACEcreate("id", "event", TYPE_int);
	TRACE_id_time = TRACEcreate("id", "time", TYPE_str);
	TRACE_id_pc = TRACEcreate("id", "pc", TYPE_str);
	TRACE_id_thread = TRACEcreate("id", "thread", TYPE_int);
	TRACE_id_ticks = TRACEcreate("id", "ticks", TYPE_lng);
	TRACE_id_rssMB = TRACEcreate("id", "rssMB", TYPE_lng);
	TRACE_id_tmpspace = TRACEcreate("id", "tmpspace", TYPE_lng);
	TRACE_id_inblock = TRACEcreate("id", "read", TYPE_lng);
	TRACE_id_oublock = TRACEcreate("id", "write", TYPE_lng);
	TRACE_id_minflt = TRACEcreate("id", "minflt", TYPE_lng);
	TRACE_id_majflt = TRACEcreate("id", "majflt", TYPE_lng);
	TRACE_id_nvcsw = TRACEcreate("id", "nvcsw", TYPE_lng);
	TRACE_id_stmt = TRACEcreate("id", "stmt", TYPE_str);
	if (TRACE_id_event == NULL ||
		TRACE_id_time == NULL ||
		TRACE_id_ticks == NULL ||
		TRACE_id_pc == NULL ||
		TRACE_id_stmt == NULL ||
		TRACE_id_rssMB == NULL ||
		TRACE_id_tmpspace == NULL ||
		TRACE_id_inblock == NULL ||
		TRACE_id_oublock == NULL ||
		TRACE_id_minflt == NULL ||
		TRACE_id_majflt == NULL ||
		TRACE_id_nvcsw == NULL ||
		TRACE_id_thread == NULL 
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
	BBPclear(TRACE_id_event->batCacheid);
	BBPclear(TRACE_id_time->batCacheid);
	BBPclear(TRACE_id_pc->batCacheid);
	BBPclear(TRACE_id_thread->batCacheid);
	BBPclear(TRACE_id_ticks->batCacheid);
	BBPclear(TRACE_id_rssMB->batCacheid);
	BBPclear(TRACE_id_tmpspace->batCacheid);
	BBPclear(TRACE_id_inblock->batCacheid);
	BBPclear(TRACE_id_oublock->batCacheid);
	BBPclear(TRACE_id_minflt->batCacheid);
	BBPclear(TRACE_id_majflt->batCacheid);
	BBPclear(TRACE_id_nvcsw->batCacheid);
	BBPclear(TRACE_id_stmt->batCacheid);
	TRACE_init = 0;
	_initTrace();
	MT_lock_unset(&mal_contextLock, "cleanup");
}

BAT *
getTrace(const char *nme)
{
	if (TRACE_init == 0)
		return NULL;
	if (strcmp(nme, "event") == 0)
		return BATcopy(TRACE_id_event, TYPE_void, TRACE_id_event->ttype, 0, TRANSIENT);
	if (strcmp(nme, "time") == 0)
		return BATcopy(TRACE_id_time, TYPE_void, TRACE_id_time->ttype, 0, TRANSIENT);
	if (strcmp(nme, "pc") == 0)
		return BATcopy(TRACE_id_pc, TYPE_void, TRACE_id_pc->ttype, 0, TRANSIENT);
	if (strcmp(nme, "thread") == 0)
		return BATcopy(TRACE_id_thread, TYPE_void, TRACE_id_thread->ttype, 0, TRANSIENT);
	if (strcmp(nme, "ticks") == 0)
		return BATcopy(TRACE_id_ticks, TYPE_void, TRACE_id_ticks->ttype, 0, TRANSIENT);
	if (strcmp(nme, "rssMB") == 0)
		return BATcopy(TRACE_id_rssMB, TYPE_void, TRACE_id_rssMB->ttype, 0, TRANSIENT);
	if (strcmp(nme, "tmpspace") == 0)
		return BATcopy(TRACE_id_tmpspace, TYPE_void, TRACE_id_tmpspace->ttype, 0, TRANSIENT);
	if (strcmp(nme, "reads") == 0)
		return BATcopy(TRACE_id_inblock, TYPE_void, TRACE_id_inblock->ttype, 0, TRANSIENT);
	if (strcmp(nme, "writes") == 0)
		return BATcopy(TRACE_id_oublock, TYPE_void, TRACE_id_oublock->ttype, 0, TRANSIENT);
	if (strcmp(nme, "minflt") == 0)
		return BATcopy(TRACE_id_minflt, TYPE_void, TRACE_id_minflt->ttype, 0, TRANSIENT);
	if (strcmp(nme, "majflt") == 0)
		return BATcopy(TRACE_id_majflt, TYPE_void, TRACE_id_majflt->ttype, 0, TRANSIENT);
	if (strcmp(nme, "nvcsw") == 0)
		return BATcopy(TRACE_id_nvcsw, TYPE_void, TRACE_id_nvcsw->ttype, 0, TRANSIENT);
	if (strcmp(nme, "stmt") == 0)
		return BATcopy(TRACE_id_stmt, TYPE_void, TRACE_id_stmt->ttype, 0, TRANSIENT);
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
	if (strcmp(nme, "tmpspace") == 0)
		return newColumnType( TYPE_lng);
	if (strcmp(nme, "reads") == 0 || strcmp(nme, "writes") == 0 || strcmp(nme,"minflt")==0 || strcmp(nme,"majflt")==0  || strcmp(nme,"nvcsw")==0  )
		return newColumnType( TYPE_lng);
	return TYPE_any;
}

void
cachedProfilerEvent(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* static struct Mallinfo prevMalloc; */
	char buf[BUFSIZ]= {0};
	char ctm[27]={0}, *ct= ctm+10;
	int tid = (int)THRgettid();
	lng v1 = 0, v2= 0, v3=0, v4=0, v5=0;
	str stmt, c;
	time_t clk;
	struct timeval clock;
	lng rssMB = MT_getrss()/1024/1024;
	lng tmpspace = pci->wbytes/1024/1024;
	int errors = 0;

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
	snprintf(buf, BUFSIZ, "%s.%s[%d]%d",
	getModuleId(getInstrPtr(mb, 0)),
	getFunctionId(getInstrPtr(mb, 0)), getPC(mb, pci), stk->tag);

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
	stmt = instruction2str(mb, stk, pci, LIST_MAL_ALL);
	c = stmt;

	while (c && *c && (isspace((int)*c) || *c == '!'))
		c++;

#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	v1= infoUsage.ru_inblock - prevUsage.ru_inblock;
	v2= infoUsage.ru_oublock - prevUsage.ru_oublock;
	v3= infoUsage.ru_majflt - prevUsage.ru_majflt;
	v4= infoUsage.ru_nswap - prevUsage.ru_nswap;
	v5= infoUsage.ru_nvcsw - prevUsage.ru_nvcsw +infoUsage.ru_nivcsw - prevUsage.ru_nivcsw;
	prevUsage = infoUsage;
#endif

	// keep it a short transaction
	MT_lock_set(&mal_profileLock, "cachedProfilerEvent");
	errors += BUNappend(TRACE_id_event, &TRACE_event, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_time, ct, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_pc, buf, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_thread, &tid, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_ticks, &pci->ticks, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_rssMB, &rssMB, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_tmpspace, &tmpspace, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_inblock, &v1, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_oublock, &v2, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_minflt, &v3, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_majflt, &v4, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_nvcsw, &v5, FALSE) != GDK_SUCCEED;
	errors += BUNappend(TRACE_id_stmt, c, FALSE) != GDK_SUCCEED;
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

					size += tailsize(b, cnt);
					/* the upperbound is used for the heaps */
					if (b->T->vheap)
						size += b->T->vheap->size;
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
    int cpu, len = 0, i;
	lng user, nice, system, idle, iowait;
	size_t n;
    char buf[BUFSIZ+1], *s;
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

	if( cpuload == 0)
		return 0;
	// identify core processing
	len += snprintf(cpuload, BUFSIZ, "[ ");
	for ( cpu = 0; cpuload && cpu < 255 && corestat[cpu].user; cpu++) {
		len +=snprintf(cpuload + len, BUFSIZ - len, " %.2f ",corestat[cpu].load);
	}
	(void) snprintf(cpuload + len, BUFSIZ - len, "]");
	return 0;
}

// Give users the option to check for the system load between two heart beats
double HeartbeatCPUload(void)
{
	return corestat[255].load;
}
//
// Retrieve the io statistics for the complete process group
// This information can only be obtained using root-permissions.
//
#ifdef GETIOSTAT
static str getIOactivity(void){
	Thread t,s;
	FILE *fd;
	char fnme[BUFSIZ], *buf;
	int n,i=0;
	size_t len=0;

	buf= GDKzalloc(BUFSIZ);
	if ( buf == NULL)
		return 0;
	buf[len++]='"';
	//MT_lock_set(&GDKthreadLock, "profiler.io");
	for (t = GDKthreads, s = t + THREADS; t < s; t++, i++)
		if (t->pid ){
			(void) snprintf(fnme,BUFSIZ,"/proc/"SZFMT"/io",t->pid);
			fd = fopen(fnme,"r");
			if ( fd == NULL)
				return buf;
			(void) snprintf(buf+len, BUFSIZ-len-2,"thr %d ",i);
			if ((n = fread(buf+len, 1, BUFSIZ-len-2,fd)) == 0 )
				return  buf;
			// extract the properties
			mnstr_printf(GDKout,"#got io stat:%s\n",buf);
			(void)fclose (fd);
		 }
	//MT_lock_unset(&GDKthreadLock, "profiler.io");
	buf[len++]='"';
	return buf;
}
#endif

void profilerGetCPUStat(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait)
{
	(void) getCPULoad(0);
	*user = corestat[255].user;
	*nice = corestat[255].nice;
	*sys = corestat[255].system;
	*idle = corestat[255].idle;
	*iowait = corestat[255].iowait;
}

void profilerHeartbeatEvent(char *alter)
{
	char cpuload[BUFSIZ];
	if (ATOMIC_GET(hbdelay, hbLock, "profilerHeatbeatEvent") == 0 || eventstream  == NULL)
		return;

	/* get CPU load on beat boundaries only */
	if ( getCPULoad(cpuload) )
		return;
	
	offlineProfilerEvent(0, 0, 0, 0, alter, cpuload);
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
			for (t = 1000; t > 0; t -= 25) {
				MT_sleep_ms(25);
				if (!ATOMIC_GET(hbrunning, hbLock, "profilerHeartbeat"))
					return;
			}
		}
		for (t = (int) ATOMIC_GET(hbdelay, hbLock, "profilerHeatbeatEvent"); t > 0; t -= 25) {
			MT_sleep_ms(t > 25 ? 25 : t);
			if (!ATOMIC_GET(hbrunning, hbLock, "profilerHeartbeat"))
				return;
		}
		profilerHeartbeatEvent("ping");
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
