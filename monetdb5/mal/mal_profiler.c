/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* (c) M.L. Kersten
 * Performance tracing
 * The stethoscope/tachograph and tomograph performance monitors have exclusive access 
 * to a single event stream, which avoids concurrency conflicts amongst clients. 
 * It also avoid cluthered event records on the stream. Since this event stream is owned
 * by a client, we should ensure that the profiler is automatically 
 * reset once the owner leaves. 
 */
#include "monetdb_config.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_profiler.h"
#include "mal_runtime.h"
#include "mal_debugger.h"

static void cachedProfilerEvent(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

stream *eventstream = 0;

static int offlineProfiling = FALSE;
static int sqlProfiling = FALSE;
static str myname = 0;	// avoid tracing the profiler module
static oid user = 0;
static int eventcounter = 0;

static int TRACE_init = 0;
int malProfileMode = 0;     /* global flag to indicate profiling mode */

static volatile ATOMIC_TYPE hbdelay = 0;

#ifdef HAVE_SYS_RESOURCE_H
struct rusage infoUsage;
static struct rusage prevUsage;
#endif

static struct{
	lng user, nice, system, idle, iowait;
	double load;
} corestat[256];

/*
 * Profiler trace cache
 * The trace information for a limited collection of queries is retained in 
 * a profiler cache, located in the database directory profiler_logs.
 * It can be used for post-mortem analysis. It actually should be a profiler_vault.
 *
 * The cache file name is simply derived from the MAL block invocation tag,
 * which is assured to be unique.
 * 
 * The JSON structures are also sent to the single stream upon request.
 *
 * The old profiler cache structures are removed when you
 * start of the profiler.
 */

#define MAXJSONEVENTS (1000000) /* cut off large JSON traces */
#define DEFAULTPOOLSIZE 10

typedef struct {
	int tag;
	int cnt;	// number of events in this bucket
	lng start,finish;
	char fname[BUFSIZ];
	stream *trace;
} ProfilerRecord;

ProfilerRecord *profilerPool;
static int poolSize= DEFAULTPOOLSIZE;
static int poolCounter = 0;

static void
clearPool(void)
{	int i;
	
	// remove old profiler traces
	if( profilerPool )
	for( i = 0; i < poolSize; i++){
		if ( profilerPool[i].trace)
			close_stream(profilerPool[i].trace);
		if( profilerPool[i].fname[0])
		(void) unlink(profilerPool[i].fname);
	}
}

// setting the poolsize re-initializes the pool
str 
setprofilerpoolsize(int size)
{
	if( size < 1)
		throw(MAL,"profiler.setPool", "invalid pool size");
	MT_lock_set(&mal_profileLock, "profilerpool");
	// Always cleanout the past before you set the new pool size
	if (profilerPool){
		clearPool();
		poolCounter = 0;
		poolSize = 0;
		GDKfree(profilerPool);
		profilerPool = 0;
	}
	profilerPool = GDKzalloc(size * sizeof(ProfilerRecord));
	MT_lock_unset(&mal_profileLock, "profilerpool");
	if( profilerPool == 0)
		throw(MAL,"profiler.setPool", MAL_MALLOC_FAIL);
	poolSize = size;
	return MAL_SUCCEED;
}

str
setProfilerStream(Module cntxt, const char *host, int port)
{
	(void)cntxt;        /* still unused */
	MT_lock_set(&mal_profileLock, "setstream");
	if ((eventstream = udp_wastream(host, port, "profileStream")) == NULL) {
		MT_lock_unset(&mal_profileLock, "setstream");
		throw(IO, "mal.profiler", RUNTIME_STREAM_FAILED);
	}
	eventstream = wbstream(eventstream, BUFSIZ);
	MT_lock_unset(&mal_profileLock, "setstream");
	return MAL_SUCCEED;
}

#define LOGLEN 8192
#define lognew()  loglen = 0; logbase = logbuffer; *logbase = 0;

#define logadd(...) {														\
	do {																\
		loglen += snprintf(logbase+loglen, LOGLEN -1 - loglen, __VA_ARGS__); \
		assert(loglen < LOGLEN); \
	} while (0);}


// The heart beat events should be sent to all outstanding channels.
static void logjsonInternal(int k, char *logbuffer, InstrPtr p)
{	
	char buf[BUFSIZ], *s;
	size_t len, lenhdr;

	snprintf(buf,BUFSIZ,"%d",eventcounter);
	s = strchr(logbuffer,(int) ':');
	if( s == NULL){
		return;
	}
	strncpy(s+1, buf,strlen(buf));
	len = strlen(logbuffer);

	MT_lock_set(&mal_profileLock, "logjson");
	if (eventstream) {
	// upon request the log record is sent over the profile stream
		if( eventcounter == 0){
			snprintf(buf,BUFSIZ,"%s\n",monetdb_characteristics);
			lenhdr = strlen(buf);
			(void) mnstr_write(eventstream, buf, 1, lenhdr);
		}
		(void) mnstr_write(eventstream, logbuffer, 1, len);
		(void) mnstr_flush(eventstream);
	}

	// all queries are assembled in a performance trace pool
	if( profilerPool){
		if( profilerPool[k].trace == NULL){
			if( profilerPool[k].fname[0]== 0)
				(void) mkdir("profiler_logs", 0755);
			if( profilerPool[k].fname[0]== 0)
				snprintf(profilerPool[k].fname, BUFSIZ,"profiler_logs/%d.json",k);
			
			profilerPool[k].trace = open_wastream(profilerPool[k].fname);
			if( profilerPool[k].trace == NULL){
				GDKerror("could not create profiler file");
				MT_lock_unset(&mal_profileLock, "logjson");
				return;
			}
			profilerPool[k].start = GDKusec();
			snprintf(buf,BUFSIZ,"%s\n",monetdb_characteristics);
			(void) mnstr_write(profilerPool[k].trace, buf, 1, strlen(buf));
		}
		if( profilerPool[k].trace){
			(void) mnstr_write(profilerPool[k].trace,logbuffer,1,len);
			(void) mnstr_flush(profilerPool[k].trace);
		}
		if ( profilerPool[k].trace && (( p && p->barrier == ENDsymbol && strstr(logbuffer,"done")) || profilerPool[k].cnt > MAXJSONEVENTS) ){
			(void) mnstr_flush(profilerPool[k].trace);
			(void) close_stream(profilerPool[k].trace);
			profilerPool[k].cnt = 0;
			profilerPool[k].trace = NULL;
			profilerPool[k].finish = GDKusec();
		}
	}
	eventcounter++;
	MT_lock_unset(&mal_profileLock, "logjson");
}

static void logjson(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, char *logbuffer)
{	int i,k;	

	(void) mb;
	(void) p;
	if( stk){
		logjsonInternal( k = stk->tag % poolSize, logbuffer,p);
		profilerPool[k].tag = stk->tag;
	} else
// The heart beat events should be sent to all outstanding channels.
// But only once to the stream
	for( i=0; i< poolSize; i++)
	if(profilerPool[i].trace){
		logjsonInternal( i, logbuffer, 0);
	}
}

/* JSON rendering method of performance data. 
 * The eventparser may assume this layout for ease of parsing
EXAMPLE:
{
"event":6        ,
"time":"15:37:13.799706",
"thread":3,
"function":"user.s3_1",
"pc":1,
"tag":10397,
"state":"start",
"usec":0,
"rss":215,
"size":0,
"oublock":8,
"stmt":"X_41=0@0:void := querylog.define(\"select count(*) from tables;\":str,\"default_pipe\":str,30:int);",
"short":"define( \"select count(*) from tables;\",\"default_pipe\",30 )",
"prereq":[]
}
*/
static void
renderProfilerEvent(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start)
{
	char logbuffer[LOGLEN], *logbase;
	int loglen;
	char ctm[26];
	time_t clk;
	struct timeval clock;
	str stmt, c;
	char *tbuf;
	str stmtq;


	if( start) // show when instruction was started
		clock = pci->clock;
	else 
		gettimeofday(&clock, NULL);
	clk = clock.tv_sec;

	/* make profile event tuple  */
	lognew();
	logadd("{\n\"event\":         ,\n"); // fill in later with the event counter

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
	tbuf[19]=0;
	logadd("\"time\":\"%s.%06ld\",\n", tbuf+11, (long)clock.tv_usec);
	logadd("\"thread\":%d,\n", THRgettid());

	logadd("\"function\":\"%s.%s\",\n", getModuleId(getInstrPtr(mb, 0)), getFunctionId(getInstrPtr(mb, 0)));
	logadd("\"pc\":%d,\n", mb?getPC(mb,pci):0);
	logadd("\"tag\":%d,\n", stk?stk->tag:0);

	if( start){
		logadd("\"state\":\"start\",\n");
		// determine the Estimated Time of Completion
		if ( pci->calls){
			logadd("\"usec\":"LLFMT",\n", pci->totticks/pci->calls);
		} else{
			logadd("\"usec\":"LLFMT",\n", pci->ticks);
		}
	} else {
		logadd("\"state\":\"done\",\n");
		logadd("\"usec\":"LLFMT",\n", pci->ticks);
	}
	logadd("\"rss\":"SZFMT ",\n", MT_getrss()/1024/1024);
	logadd("\"size\":"LLFMT ",\n", pci? pci->wbytes/1024/1024:0);	// result size

#ifdef NUMAprofiling
		logadd("\"numa\":[");
		if(mb)
		for( i= pci->retc ; i < pci->argc; i++)
		if( !isVarConstant(mb, getArg(pci,i)) && mb->var[getArg(pci,i)]->worker)
			logadd("%c %d", (i?',':' '), mb->var[getArg(pci,i)]->worker);
		logadd("],\n");
#endif

#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	if(infoUsage.ru_inblock - prevUsage.ru_inblock)
		logadd("\"inblock\":%ld,\n", infoUsage.ru_inblock - prevUsage.ru_inblock);
	if(infoUsage.ru_oublock - prevUsage.ru_oublock)
		logadd("\"oublock\":%ld,\n", infoUsage.ru_oublock - prevUsage.ru_oublock);
	if(infoUsage.ru_majflt - prevUsage.ru_majflt)
		logadd("\"majflt\":%ld,\n", infoUsage.ru_majflt - prevUsage.ru_majflt);
	if(infoUsage.ru_nswap - prevUsage.ru_nswap)
		logadd("\"nswap\":%ld,\n", infoUsage.ru_nswap - prevUsage.ru_nswap);
	if(infoUsage.ru_nvcsw - prevUsage.ru_nvcsw)
		logadd("\"nvcsw\":%ld,\n", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw +infoUsage.ru_nivcsw - prevUsage.ru_nivcsw);
	prevUsage = infoUsage;
#endif

	if( mb){
		char prereq[BUFSIZ];
		size_t len;
		int i,j,k,comma;
		InstrPtr q;

		/* generate actual call statement */
		stmt = instruction2str(mb, stk, pci, LIST_MAL_ALL);
		c = stmt;

		while (c && *c && isspace((int)*c))
			c++;
		if( *c){
			stmtq = mal_quote(c, strlen(c));
			if (stmtq != NULL) {
				logadd("\"stmt\":\"%s\",\n", stmtq);
				//GDKfree(stmtq);
			}
		} 
		//GDKfree(stmt);

		// ship the beautified version as well
/*
		stmt = shortStmtRendering(mb, stk, pci);
		stmtq = mal_quote(stmt, strlen(stmt));
		if (stmtq != NULL) {
			logadd("\"short\":\"%s\",\n", stmtq);
			GDKfree(stmtq);
		} 
		GDKfree(stmt);
*/

		// collect the prerequisite pre-requisite statements
		prereq[0]='[';
		prereq[1]=0;
		len = 1;
		comma=0;
		for(i= pci->retc; i < pci->argc; i++){
			for( j = pci->pc-1; j > 0; j--){
				q= getInstrPtr(mb,j);
				for( k=0; k < q->retc; k++)
					if( getArg(q,k) == getArg(pci,i))
						break;
				if( k < q->retc){
					snprintf(prereq + len, BUFSIZ-len,"%s%d", (comma?",":""), j);
					len = strlen(prereq);
					comma++;
					break;
				}
			}
		}
//#define MALARGUMENTDETAILS
#ifdef MALARGUMENTDETAILS
		logadd("\"prereq\":%s],\n", prereq);
#else
		logadd("\"prereq\":%s]\n", prereq);
#endif
		
/* EXAMPLE MAL statement argument decomposition
 * The eventparser may assume this layout for ease of parsing
{
... as above ...
"result":{"index":0,"name":"X_41","value":"0@0","count":1,"type": "void" }
...
"argument":{"index":4,"value":"30","type": "int" }
}
 */
#ifdef MALARGUMENTDETAILS
		// Also show details of the arguments for modelling
		for( j=0; j< pci->argc; j++){
			int tpe = getVarType(mb, getArg(pci,j));
			str tname = getTypeName(tpe), cv;

			logadd("\"%s\":{", j< pci->retc?"result":"argument");
			logadd("\"index\":\"%d\",", j);
			if( !isVarConstant(mb, getArg(pci,j))){
				logadd("\"name\":\"%s\",", getVarName(mb, getArg(pci,j)));
			}
			if( isaBatType(tpe) ){
				BAT *d= BBPquickdesc(abs(stk->stk[getArg(pci,j)].val.ival),TRUE);
				if( d)
					logadd("\"count\":" BUNFMT",", BATcount(d));
				GDKfree(tname);
				tname = getTypeName(getColumnType(tpe));
				logadd("\"type\":\"col[:%s]\"", tname);
			} else{
				cv = 0;
				VALformat(&cv, &stk->stk[getArg(pci,j)]);
				stmtq = mal_quote(cv, strlen(cv));
				logadd("\"value\":\"%s\",", stmtq);
				logadd("\"type\":\"%s\"", tname);
				GDKfree(cv);
				GDKfree(stmtq);
			}
			GDKfree(tname);
			logadd("}%s\n", (j< pci->argc-1?",":""));
		}
#endif
	}
	logadd("}\n"); // end marker
	logjson(mb,stk,pci,logbuffer);
}

static int
getCPULoad(char cpuload[BUFSIZ]){
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

void
profilerHeartbeatEvent(char *alter)
{
	char cpuload[BUFSIZ];
	char logbuffer[LOGLEN], *logbase;
	int loglen;
	char ctm[26];
	time_t clk;
	struct timeval clock;
	char *tbuf;

	if (ATOMIC_GET(hbdelay, hbLock, "profilerHeatbeatEvent") == 0 || eventstream  == NULL)
		return;

	/* get CPU load on beat boundaries only */
	if ( getCPULoad(cpuload) )
		return;
	gettimeofday(&clock, NULL);
	clk = clock.tv_sec;
	
	lognew();
	logadd("{\n\"event\":         ,\n"); // fill in later with the event counter
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
	logadd("\"time\":\"%s.%06ld\",\n", tbuf+11, (long)clock.tv_usec);
	logadd("\"rss\":"SZFMT ",\n", MT_getrss()/1024/1024);
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	if(infoUsage.ru_inblock - prevUsage.ru_inblock)
		logadd("\"inblock\":%ld,\n", infoUsage.ru_inblock - prevUsage.ru_inblock);
	if(infoUsage.ru_oublock - prevUsage.ru_oublock)
		logadd("\"oublock\":%ld,\n", infoUsage.ru_oublock - prevUsage.ru_oublock);
	if(infoUsage.ru_majflt - prevUsage.ru_majflt)
		logadd("\"majflt\":%ld,\n", infoUsage.ru_majflt - prevUsage.ru_majflt);
	if(infoUsage.ru_nswap - prevUsage.ru_nswap)
		logadd("\"nswap\":%ld,\n", infoUsage.ru_nswap - prevUsage.ru_nswap);
	if(infoUsage.ru_nvcsw - prevUsage.ru_nvcsw)
		logadd("\"nvcsw\":%ld,\n", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw +infoUsage.ru_nivcsw - prevUsage.ru_nivcsw);
	prevUsage = infoUsage;
#endif
	logadd("\"state\":\"%s\",\n",alter);
	logadd("\"cpuload\":\"%s\",\n",cpuload);
	logadd("}\n"); // end marker
	logjson(0,0,0,logbuffer);
}

void
profilerEvent(oid usr, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start)
{
	if (usr != user) return;
	if (stk == NULL) return;
	if (pci == NULL) return;
	if (getModuleId(pci) == myname) // ignore profiler commands from monitoring
		return;

	if( sqlProfiling)
		cachedProfilerEvent(mb, stk, pci);
		
	renderProfilerEvent(mb, stk, pci, start);
	if ( start && pci->pc ==0)
		profilerHeartbeatEvent("ping");
	if ( !start && pci->token == ENDsymbol)
		profilerHeartbeatEvent("ping");
}

str
openProfilerStream(stream *fd)
{
	if( eventstream)
		closeProfilerStream();
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
 * When we receive the message to start profiling, we
 * should wait for the next instruction the stream
 * is initiated. This is controlled by a delay-switch
 */

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
	setprofilerpoolsize(poolSize);

	MT_lock_set(&mal_profileLock, "startProfiler");

	// enable the streaming of events
	if (eventstream != NULL) {
		offlineProfiling = TRUE;
		if( beat >= 0)
			setHeartbeat(beat); 
	}

	sqlProfiling = TRUE;
	malProfileMode = mode;
	eventcounter = 0;
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
	if (TRACE_init == 0)
		return ;       /* not initialized */
	MT_lock_set(&mal_profileLock, "TRACEtable");
	r[0] = BATcopy(TRACE_id_event, TRACE_id_event->htype, TRACE_id_event->ttype, 0, TRANSIENT);
	r[1] = BATcopy(TRACE_id_time, TRACE_id_time->htype, TRACE_id_time->ttype, 0, TRANSIENT);
	r[2] = BATcopy(TRACE_id_pc, TRACE_id_pc->htype, TRACE_id_pc->ttype, 0, TRANSIENT);
	r[3] = BATcopy(TRACE_id_thread, TRACE_id_thread->htype, TRACE_id_thread->ttype, 0, TRANSIENT);
	r[4] = BATcopy(TRACE_id_ticks, TRACE_id_ticks->htype, TRACE_id_ticks->ttype, 0, TRANSIENT);
	r[5] = BATcopy(TRACE_id_rssMB, TRACE_id_rssMB->htype, TRACE_id_rssMB->ttype, 0, TRANSIENT);
	r[6] = BATcopy(TRACE_id_tmpspace, TRACE_id_tmpspace->htype, TRACE_id_tmpspace->ttype, 0, TRANSIENT);
	r[7] = BATcopy(TRACE_id_inblock, TRACE_id_inblock->htype, TRACE_id_inblock->ttype, 0, TRANSIENT);
	r[8] = BATcopy(TRACE_id_oublock, TRACE_id_oublock->htype, TRACE_id_oublock->ttype, 0, TRANSIENT);
	r[9] = BATcopy(TRACE_id_minflt, TRACE_id_minflt->htype, TRACE_id_minflt->ttype, 0, TRANSIENT);
	r[10] = BATcopy(TRACE_id_majflt, TRACE_id_majflt->htype, TRACE_id_majflt->ttype, 0, TRANSIENT);
	r[11] = BATcopy(TRACE_id_nvcsw, TRACE_id_nvcsw->htype, TRACE_id_nvcsw->ttype, 0, TRANSIENT);
	r[12] = BATcopy(TRACE_id_stmt, TRACE_id_stmt->htype, TRACE_id_stmt->ttype, 0, TRANSIENT);
	MT_lock_unset(&mal_profileLock, "TRACEtable");
}

BAT *
getTrace(const char *nme)
{
	if (TRACE_init == 0)
		return NULL;
	if (strcmp(nme, "event") == 0)
		return BATcopy(TRACE_id_event, TRACE_id_event->htype, TRACE_id_event->ttype, 0, TRANSIENT);
	if (strcmp(nme, "time") == 0)
		return BATcopy(TRACE_id_time, TRACE_id_time->htype, TRACE_id_time->ttype, 0, TRANSIENT);
	if (strcmp(nme, "pc") == 0)
		return BATcopy(TRACE_id_pc, TRACE_id_pc->htype, TRACE_id_pc->ttype, 0, TRANSIENT);
	if (strcmp(nme, "thread") == 0)
		return BATcopy(TRACE_id_thread, TRACE_id_thread->htype, TRACE_id_thread->ttype, 0, TRANSIENT);
	if (strcmp(nme, "usec") == 0)
		return BATcopy(TRACE_id_ticks, TRACE_id_ticks->htype, TRACE_id_ticks->ttype, 0, TRANSIENT);
	if (strcmp(nme, "rssMB") == 0)
		return BATcopy(TRACE_id_rssMB, TRACE_id_rssMB->htype, TRACE_id_rssMB->ttype, 0, TRANSIENT);
	if (strcmp(nme, "tmpspace") == 0)
		return BATcopy(TRACE_id_tmpspace, TRACE_id_tmpspace->htype, TRACE_id_tmpspace->ttype, 0, TRANSIENT);
	if (strcmp(nme, "reads") == 0)
		return BATcopy(TRACE_id_inblock, TRACE_id_inblock->htype, TRACE_id_inblock->ttype, 0, TRANSIENT);
	if (strcmp(nme, "writes") == 0)
		return BATcopy(TRACE_id_oublock, TRACE_id_oublock->htype, TRACE_id_oublock->ttype, 0, TRANSIENT);
	if (strcmp(nme, "minflt") == 0)
		return BATcopy(TRACE_id_minflt, TRACE_id_minflt->htype, TRACE_id_minflt->ttype, 0, TRANSIENT);
	if (strcmp(nme, "majflt") == 0)
		return BATcopy(TRACE_id_majflt, TRACE_id_majflt->htype, TRACE_id_majflt->ttype, 0, TRANSIENT);
	if (strcmp(nme, "nvcsw") == 0)
		return BATcopy(TRACE_id_nvcsw, TRACE_id_nvcsw->htype, TRACE_id_nvcsw->ttype, 0, TRANSIENT);
	if (strcmp(nme, "stmt") == 0)
		return BATcopy(TRACE_id_stmt, TRACE_id_stmt->htype, TRACE_id_stmt->ttype, 0, TRANSIENT);
	return NULL;
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

int
initTrace(void)
{
	int ret = -1;

	if (TRACE_init)
		return 0;       /* already initialized */
	MT_lock_set(&mal_contextLock, "initTrace");
	TRACE_id_event = TRACEcreate("id", "event", TYPE_int);
	TRACE_id_time = TRACEcreate("id", "time", TYPE_str);
	// TODO split pc into its components fcn,pc,tag
	TRACE_id_pc = TRACEcreate("id", "pc", TYPE_str);
	TRACE_id_thread = TRACEcreate("id", "thread", TYPE_int);
	TRACE_id_ticks = TRACEcreate("id", "usec", TYPE_lng);
	TRACE_id_rssMB = TRACEcreate("id", "rssMB", TYPE_lng);
	// rename to size
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
	)
		_cleanupProfiler();
	else
		TRACE_init = 1;
	ret = TRACE_init;
	MT_lock_unset(&mal_contextLock, "initTrace");
	return ret;
}

str
cleanupTraces(void)
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
	MT_lock_unset(&mal_contextLock, "cleanup");
	initTrace();
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
	if (TRACE_init == 0)
		return;

	/* update the Trace tables */
	snprintf(buf, BUFSIZ, "%s.%s[%d]%d",
	getModuleId(getInstrPtr(mb, 0)),
	getFunctionId(getInstrPtr(mb, 0)), getPC(mb, pci), stk->tag);

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
	GDKfree(stmt);
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

