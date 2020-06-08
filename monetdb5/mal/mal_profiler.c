/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
#include "mutils.h"         /* mercurial_revision */
#include "msabaoth.h"		/* msab_getUUID */
#include "mal_authorize.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_profiler.h"
#include "mal_runtime.h"
#include "mal_utils.h"
#include "mal_resource.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include <string.h>

static str myname = 0;	// avoid tracing the profiler module

/* The JSON rendering can be either using '\n' separators between
 * each key:value pair or as a single line.
 * The current stethoscope implementation requires the first option and
 * also the term rendering  to be set to ''
 */

/* When the MAL block contains a BARRIER block we may end up with tons
 * of profiler events. To avoid this, we stop emitting the events
 * when we reached the HIGHWATERMARK. Leaving a message in the log. */
#define HIGHWATERMARK 5


int malProfileMode = 0;     /* global flag to indicate profiling mode */
static oid malprofileruser;	/* keep track on who has claimed the channel */

static struct timeval startup_time;

static ATOMIC_TYPE hbdelay = ATOMIC_VAR_INIT(0);

#ifdef HAVE_SYS_RESOURCE_H
struct rusage infoUsage;
static struct rusage prevUsage;
#endif

#define LOGLEN 8192
#define lognew()  loglen = 0; logbase = logbuffer; *logbase = 0;

#define logadd(...)														\
	do {																\
		char tmp_buff[LOGLEN];											\
		int tmp_len = 0;												\
		tmp_len = snprintf(tmp_buff, LOGLEN, __VA_ARGS__);				\
		if (loglen + tmp_len < LOGLEN)									\
			loglen += snprintf(logbase+loglen, LOGLEN - loglen, __VA_ARGS__); \
		else {															\
			logjsonInternal(logbuffer);									\
			lognew();													\
			loglen += snprintf(logbase+loglen, LOGLEN - loglen, __VA_ARGS__); \
		}																\
	} while (0)

// The heart beat events should be sent to all outstanding channels.
static void logjsonInternal(char *logbuffer)
{
	size_t len;
	len = strlen(logbuffer);

	MT_lock_set(&mal_profileLock);
	if (maleventstream) {
	// upon request the log record is sent over the profile stream
		(void) mnstr_write(maleventstream, logbuffer, 1, len);
		(void) mnstr_flush(maleventstream);
	}
	MT_lock_unset(&mal_profileLock);
}

static char *
truncate_string(char *inp)
{
	size_t len;
	char *ret;
	size_t ret_len = LOGLEN/2;
	size_t padding = 64;

	len = strlen(inp);
	ret = (char *)GDKmalloc(ret_len + 1);
	if (ret == NULL) {
		return NULL;
	}

	snprintf(ret, ret_len + 1, "%.*s...<truncated>...%.*s",
			 (int) (ret_len/2), inp, (int) (ret_len/2 - padding),
			 inp + (len - ret_len/2 + padding));

	return ret;
}

/* JSON rendering method of performance data.
 * The eventparser may assume this layout for ease of parsing
EXAMPLE:
{
"event":6        ,
"thread":3,
"function":"user.s3_1",
"pc":1,
"tag":10397,
"state":"start",
"usec":0,
}
"stmt":"X_41=0@0:void := querylog.define(\"select count(*) from tables;\":str,\"default_pipe\":str,30:int);",
*/
static void
renderProfilerEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start)
{
	char logbuffer[LOGLEN], *logbase;
	size_t loglen;
	str c;
	str stmtq;
	lng usec;
	uint64_t microseconds;

	/* ignore generation of events for instructions that are called too often
	 * they may appear when BARRIER blocks are executed
	 * The default parameter should be sufficient for most practical cases.
	*/
	if( !start && pci->calls > HIGHWATERMARK){
		if( pci->calls == 10000 || pci->calls == 100000 || pci->calls == 1000000 || pci->calls == 10000000)
			TRC_WARNING(MAL_SERVER, "Too many calls: %d\n", pci->calls);
		return;
	}

/* The stream of events can be complete read by the DBA,
 * all other users can only see events assigned to their account
 */
	if(malprofileruser!= MAL_ADMIN && malprofileruser != cntxt->user)
		return;

	usec= pci->clock;
	microseconds = (uint64_t)usec - ((uint64_t)startup_time.tv_sec*1000000 - (uint64_t)startup_time.tv_usec);
	/* make profile event tuple  */
	lognew();
	logadd("{"); // fill in later with the event counter
	/* TODO: This could probably be optimized somehow to avoid the
	 * function call to mercurial_revision().
	 */
	// No comma at the beginning
	logadd("\"version\":\""VERSION" (hg id: %s)\"", mercurial_revision());
	logadd(",\"user\":"OIDFMT, cntxt->user);
	logadd(",\"clk\":"LLFMT, usec);
	logadd(",\"mclk\":%"PRIu64"", microseconds);
	logadd(",\"thread\":%d", THRgettid());
	logadd(",\"program\":\"%s.%s\"", getModuleId(getInstrPtr(mb, 0)), getFunctionId(getInstrPtr(mb, 0)));
	logadd(",\"pc\":%d", mb?getPC(mb,pci):0);
	logadd(",\"tag\":"OIDFMT, stk?stk->tag:0);
	if( pci->modname)
		logadd(",\"module\":\"%s\"", pci->modname ? pci->modname : "");
	if( pci->fcnname)
		logadd(",\"function\":\"%s\"", pci->fcnname ? pci->fcnname : "");
	if( pci->barrier)
		logadd(",\"barrier\":\"%s\"", operatorName(pci->barrier));
	if( pci->token < FCNcall || pci->token > PATcall)
		logadd(",\"operator\":\"%s\"", operatorName(pci->token));
    if (!GDKinmemory()) {
        char *uuid;
		str c;
		if ((c = msab_getUUID(&uuid)) == NULL) {
			logadd(",\"session\":\"%s\"", uuid);
			free(uuid);
		} else
			free(c);
    }
	logadd(",\"state\":\"%s\"", start?"start":"done");
	logadd(",\"usec\":"LLFMT, pci->ticks);

/* EXAMPLE MAL statement argument decomposition
 * The eventparser may assume this layout for ease of parsing
{
... as above ...
"result":{"clk":"173297139,"pc":1,"index":0,,"name":"X_6","type":"void","value":"0@0","eol":0}
...
"argument":{"clk":173297139,"pc":1,"index":"2","type":"str","value":"\"default_pipe\"","eol":0},
}
This information can be used to determine memory footprint and variable life times.
 */

		// Also show details of the arguments for modelling
		if(mb && pci->modname && pci->fcnname){
			int j;

			logadd(",\"args\":[");
			for(j=0; j< pci->argc; j++){
				int tpe = getVarType(mb, getArg(pci,j));
				str tname = 0, cv;
				lng total = 0;
				BUN cnt = 0;
				bat bid=0;

				if (j == 0) {
					// No comma at the beginning
					logadd("{");
				}
				else {
					logadd(",{");
				}
				if(j < pci->retc)
					logadd("\"ret\":%d", j);
				else
					logadd("\"arg\":%d", j);
				logadd(",\"var\":\"%s\"", getVarName(mb, getArg(pci,j)));
				c =getVarName(mb, getArg(pci,j));
				if(getVarSTC(mb,getArg(pci,j))){
					InstrPtr stc = getInstrPtr(mb, getVarSTC(mb,getArg(pci,j)));
					if(stc && strcmp(getModuleId(stc),"sql") ==0  && strncmp(getFunctionId(stc),"bind",4)==0)
						logadd(",\"alias\":\"%s.%s.%s\"",
							getVarConstant(mb, getArg(stc,stc->retc +1)).val.sval,
							getVarConstant(mb, getArg(stc,stc->retc +2)).val.sval,
							getVarConstant(mb, getArg(stc,stc->retc +3)).val.sval);
				}
				if(isaBatType(tpe)){
					BAT *d= BATdescriptor(bid = stk->stk[getArg(pci,j)].val.bval);
					tname = getTypeName(getBatType(tpe));
					logadd(",\"type\":\"bat[:%s]\"", tname);
					if(d) {
						BAT *v;
						cnt = BATcount(d);
						if(isVIEW(d)){
							logadd(",\"view\":\"true\"");
							logadd(",\"parent\":%d", VIEWtparent(d));
							logadd(",\"seqbase\":"BUNFMT, d->hseqbase);
							v= BBPquickdesc(VIEWtparent(d), false);
							logadd(",\"persistence\":\"%s\"", (v &&  !v->batTransient ? "persistent" : "transient"));
						} else
							logadd(",\"persistence\":\"%s\"", (d->batTransient ? "transient" : "persistent"));
						logadd(",\"sorted\":%d", d->tsorted);
						logadd(",\"revsorted\":%d", d->trevsorted);
						logadd(",\"nonil\":%d", d->tnonil);
						logadd(",\"nil\":%d", d->tnil);
						logadd(",\"key\":%d", d->tkey);
						cv = VALformat(&stk->stk[getArg(pci,j)]);
						c = strchr(cv, '>');
						*c = 0;
						logadd(",\"file\":\"%s\"", cv + 1);
						GDKfree(cv);
						total += cnt * d->twidth;
						/* keeping information about the individual auxiliary heaps is helpful during analysis. */
						if( d->thash)
							logadd(",\"hash\":" LLFMT, (lng) hashinfo(d->thash, d->batCacheid));
						if( d->tvheap)
							logadd(",\"vheap\":" LLFMT, (lng) heapinfo(d->tvheap, d->batCacheid));
						if( d->timprints)
							logadd(",\"imprints\":" LLFMT, (lng) IMPSimprintsize(d));
					/* logadd("\"debug\":\"%s\",", d->debugmessages); */
						BBPunfix(d->batCacheid);
					}
					logadd(",\"bid\":%d", bid);
					logadd(",\"count\":"BUNFMT, cnt);
					logadd(",\"size\":" LLFMT, total);
				} else{
					char *truncated = NULL;
					tname = getTypeName(tpe);
					logadd(",\"type\":\"%s\"", tname);
					logadd(",\"const\":%d", isVarConstant(mb, getArg(pci,j)));
					cv = VALformat(&stk->stk[getArg(pci,j)]);
					stmtq = cv ? mal_quote(cv, strlen(cv)) : NULL;
					if (stmtq != NULL && strlen(stmtq) > LOGLEN/2) {
						truncated = truncate_string(stmtq);
						GDKfree(stmtq);
						stmtq = truncated;
					}
					if (stmtq)
						logadd(",\"value\":\"%s\"", stmtq);
					GDKfree(cv);
					GDKfree(stmtq);
				}
				logadd(",\"eol\":%d", getVarEolife(mb,getArg(pci,j)));
				logadd(",\"used\":%d", isVarUsed(mb,getArg(pci,j)));
				logadd(",\"fixed\":%d", isVarFixed(mb,getArg(pci,j)));
				logadd(",\"udf\":%d", isVarUDFtype(mb,getArg(pci,j)));
				GDKfree(tname);
				logadd("}");
			}
			logadd("]"); // end marker for arguments
		}
	logadd("}\n"); // end marker
	logjsonInternal(logbuffer);
}

/* the OS details on cpu load are read from /proc/stat
 * We should use an OS define to react to the maximal cores
 */

static struct{
	lng user, nice, system, idle, iowait;
	double load;
} corestat[256];

static int
getCPULoad(char cpuload[BUFSIZ]){
    int cpu, len = 0, i;
	lng user, nice, system, idle, iowait;
	size_t n;
    char buf[BUFSIZ+1], *s;
	static FILE *proc= NULL;
	lng newload;

	if (proc == NULL || ferror(proc))
		proc = fopen("/proc/stat","r");
	else rewind(proc);
	if (proc == NULL) {
		/* unexpected */
		return -1;
	}
	/* read complete file to avoid concurrent write issues */
	if ((n = fread(buf, 1, BUFSIZ,proc)) == 0)
		return -1;
	buf[n] = 0;
	for (s= buf; *s; s++) {
		if (strncmp(s,"cpu",3)== 0){
			s +=3;
			if (*s == ' ') {
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

			while( *s && isspace((unsigned char)*s)) s++;
			i= sscanf(s,LLSCN" "LLSCN" "LLSCN" "LLSCN" "LLSCN,  &user, &nice, &system, &idle, &iowait);
			if ( i != 5 )
				goto skip;
			newload = (user - corestat[cpu].user + nice - corestat[cpu].nice + system - corestat[cpu].system);
			if ( newload)
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

	if(cpuload == 0)
		return 0;
	// identify core processing
	len += snprintf(cpuload, BUFSIZ, "[");
	for (cpu = 0; cpuload && cpu < 255 && corestat[cpu].user; cpu++) {
		len +=snprintf(cpuload + len, BUFSIZ - len, "%s%.2f", (cpu?",":""), corestat[cpu].load);
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
	lng usec;
	uint64_t microseconds;

	if (ATOMIC_GET(&hbdelay) == 0 || maleventstream  == 0)
		return;
	usec = GDKusec();
	microseconds = (uint64_t)startup_time.tv_sec*1000000 + (uint64_t)startup_time.tv_usec + (uint64_t)usec;

	/* get CPU load on beat boundaries only */
	if (getCPULoad(cpuload))
		return;

	lognew();
	logadd("{"); // fill in later with the event counter
	if (GDKinmemory()) {
		char *uuid, *err;
		if ((err = msab_getUUID(&uuid)) == NULL) {
			logadd("\"session\":\"%s\",", uuid);
			free(uuid);
		} else
			free(err);
	}
	logadd("\"clk\":"LLFMT",",usec);
	logadd("\"ctime\":%"PRIu64",", microseconds);
	logadd("\"rss\":%zu,", MT_getrss()/1024/1024);
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	if(infoUsage.ru_inblock - prevUsage.ru_inblock)
		logadd("\"inblock\":%ld,", infoUsage.ru_inblock - prevUsage.ru_inblock);
	if(infoUsage.ru_oublock - prevUsage.ru_oublock)
		logadd("\"oublock\":%ld,", infoUsage.ru_oublock - prevUsage.ru_oublock);
	if(infoUsage.ru_majflt - prevUsage.ru_majflt)
		logadd("\"majflt\":%ld,", infoUsage.ru_majflt - prevUsage.ru_majflt);
	if(infoUsage.ru_nswap - prevUsage.ru_nswap)
		logadd("\"nswap\":%ld,", infoUsage.ru_nswap - prevUsage.ru_nswap);
	if(infoUsage.ru_nvcsw - prevUsage.ru_nvcsw)
		logadd("\"nvcsw\":%ld,", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw +infoUsage.ru_nivcsw - prevUsage.ru_nivcsw);
	prevUsage = infoUsage;
#endif
	logadd("\"state\":\"%s\",",alter);
	logadd("\"cpuload\":%s",cpuload);
	logadd("}\n"); // end marker
	logjsonInternal(logbuffer);
}

void
profilerEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int start)
{
	(void) cntxt;
	if (stk == NULL) return;
	if (pci == NULL) return;
	if (getModuleId(pci) == myname) // ignore profiler commands from monitoring
		return;

	if(maleventstream) {
		renderProfilerEvent(cntxt, mb, stk, pci, start);
		if (!start && pci->pc ==0)
			profilerHeartbeatEvent("ping");
		if (start && pci->token == ENDsymbol)
			profilerHeartbeatEvent("ping");
	}
}

/* The first scheme dumps the events on a stream (and in the pool)
 */
str
openProfilerStream(Client cntxt)
{
	int j;

#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	prevUsage = infoUsage;
#endif
	if (myname == 0){
		myname = putName("profiler");
		logjsonInternal(monet_characteristics);
	}
	if(maleventstream){
		/* The DBA can always grab the stream, others have to wait */
		if (cntxt->user == MAL_ADMIN)
			closeProfilerStream(cntxt);
		else
			throw(MAL,"profiler.start","Profiler already running, stream not available");
	}
	malProfileMode = -1;
	maleventstream = cntxt->fdout;
	malprofileruser = cntxt->user;

	// Ignore the JSON rendering mode, use compiled time version

	/* show all in progress instructions for stethoscope startup */
	/* this code is not thread safe, because the inprogress administration may change concurrently */
	MT_lock_set(&mal_delayLock);
	for(j = 0; j <THREADS; j++)
	if(workingset[j].mb)
		/* show the event */
		profilerEvent(workingset[j].cntxt, workingset[j].mb, workingset[j].stk, workingset[j].pci, 1);
	MT_lock_unset(&mal_delayLock);
	return MAL_SUCCEED;
}

str
closeProfilerStream(Client cntxt)
{
	(void) cntxt;
	maleventstream = NULL;
	malProfileMode = 0;
	malprofileruser = 0;
	return MAL_SUCCEED;
}

/* the second scheme is to collect the profile
 * events in a local table for direct SQL inspection
 */
str
startProfiler(Client cntxt)
{
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	prevUsage = infoUsage;
#endif
	(void) cntxt;

	if(maleventstream){
		throw(MAL,"profiler.start","Profiler already running, stream not available");
	}
	MT_lock_set(&mal_profileLock);
	if (myname == 0){
		myname = putName("profiler");
	}
	malProfileMode = 1;
	MT_lock_unset(&mal_profileLock);
	logjsonInternal(monet_characteristics);
	// reset the trace table
	clearTrace(cntxt);

	return MAL_SUCCEED;
}

/* SQL tracing is simplified, because it only collects the events in the temporary table.
 */
str
startTrace(Client cntxt)
{
	cntxt->sqlprofiler = TRUE;
	clearTrace(cntxt);
	return MAL_SUCCEED;
}

str
stopTrace(Client cntxt)
{
	cntxt->sqlprofiler = FALSE;
	return MAL_SUCCEED;
}

str
stopProfiler(Client cntxt)
{
	MT_lock_set(&mal_profileLock);
	malProfileMode = 0;
	setHeartbeat(0); // stop heartbeat
	if(cntxt)
		closeProfilerStream(cntxt);
	MT_lock_unset(&mal_profileLock);
	return MAL_SUCCEED;
}

/*
 * SQL profile traces
 * The events being captured are stored in client specific BATs.
 * They are made persistent to accumate information over
 * multiple sessions. This means it has to be explicitly reset
 * to avoid disc overflow using profiler.reset().
 *
 * The information returned for the trace is purposely limited
 * to the ticks and the final MAL instruction.
 * For more detailed analysis, the stethoscope should be used.
 */

static void
_cleanupProfiler(Client cntxt)
{
	BBPunfix(cntxt->profticks->batCacheid);
	BBPunfix(cntxt->profstmt->batCacheid);
	cntxt->profticks = cntxt->profstmt = NULL;
}

static BAT *
TRACEcreate(int tt)
{
	BAT *b;

	b = COLnew(0, tt, 1 << 10, TRANSIENT);
	if (b == NULL)
		return NULL;
	return b;
}

static void
initTrace(Client cntxt)
{
	MT_lock_set(&mal_profileLock);
	if (cntxt->profticks) {
		MT_lock_unset(&mal_profileLock);
		return;       /* already initialized */
	}
	cntxt->profticks = TRACEcreate(TYPE_lng);
	cntxt->profstmt = TRACEcreate(TYPE_str);
	if (cntxt->profticks == NULL || cntxt->profstmt == NULL)
		_cleanupProfiler(cntxt);
	MT_lock_unset(&mal_profileLock);
}

int
TRACEtable(Client cntxt, BAT **r)
{
	initTrace(cntxt);
	MT_lock_set(&mal_profileLock);
	if (cntxt->profticks == NULL) {
		MT_lock_unset(&mal_profileLock);
		return -1;       /* not initialized */
	}
	r[0] = COLcopy(cntxt->profticks, cntxt->profticks->ttype, false, TRANSIENT);
	r[1] = COLcopy(cntxt->profstmt, cntxt->profstmt->ttype, false, TRANSIENT);
	MT_lock_unset(&mal_profileLock);
	return 2;
}

BAT *
getTrace(Client cntxt, const char *nme)
{
	BAT *bn = NULL;

	MT_lock_set(&mal_profileLock);
	if (cntxt->profticks) {
		if (strcmp(nme, "usec") == 0){
			bn = COLcopy(cntxt->profticks, cntxt->profticks->ttype, false, TRANSIENT);
		} else if (strcmp(nme, "stmt") == 0){
			bn = COLcopy(cntxt->profstmt, cntxt->profstmt->ttype, false, TRANSIENT);
		}
	}
	MT_lock_unset(&mal_profileLock);
	return bn;
}

void
clearTrace(Client cntxt)
{
	(void) cntxt;
	MT_lock_set(&mal_profileLock);
	if (cntxt->profticks == NULL) {
		MT_lock_unset(&mal_profileLock);
		initTrace(cntxt);
		return;     /* not initialized */
	}
	/* drop all trace tables */
	_cleanupProfiler(cntxt);
	MT_lock_unset(&mal_profileLock);
	initTrace(cntxt);
}

str
cleanupTraces(Client cntxt)
{
	clearTrace(cntxt);
	return MAL_SUCCEED;
}

void
sqlProfilerEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str stmt, c;
	int errors = 0;

	if (cntxt->profticks == NULL)
		return;

	/* generate actual call statement */
	stmt = instruction2str(mb, stk, pci, LIST_MAL_ALL);
	c = stmt;

	while (c && *c && (isspace((unsigned char)*c) || *c == '!'))
		c++;

	// keep it a short transaction
	MT_lock_set(&mal_profileLock);
 	if (cntxt->profticks == NULL) {
		MT_lock_unset(&mal_profileLock);
		return;
	}
	errors += BUNappend(cntxt->profticks, &pci->ticks, false) != GDK_SUCCEED;
	errors += BUNappend(cntxt->profstmt, c, false) != GDK_SUCCEED;
	if (errors > 0) {
		/* stop profiling if an error occurred */
		cntxt->sqlprofiler = FALSE;
	}

	MT_lock_unset(&mal_profileLock);
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

/* Calculate a pessimistic size of the disk storage */
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
					if (b->tvheap)
						size += HEAPvmsize(b->tvheap);
					if (b->thash)
						size += sizeof(BUN) * cnt;
					/* also add the size of an imprint, ordered index or mosaic */
					if(b->timprints)
						size += IMPSimprintsize(b);
					if(b->torderidx)
						size += HEAPvmsize(b->torderidx);
				}
				BBPunfix(i);
			}
		}
	return size;
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

/* the heartbeat process produces a ping event once every X milliseconds */
static MT_Id hbthread;
static ATOMIC_TYPE hbrunning = ATOMIC_VAR_INIT(0);

static void profilerHeartbeat(void *dummy)
{
	int t;
	const int timeout = GDKdebug & FORCEMITOMASK ? 10 : 25;

	(void) dummy;
	for (;;) {
		/* wait until you need this info */
		MT_thread_setworking("sleeping");
		while (ATOMIC_GET(&hbdelay) == 0 || maleventstream == NULL) {
			if (GDKexiting() || !ATOMIC_GET(&hbrunning))
				return;
			MT_sleep_ms(timeout);
		}
		for (t = (int) ATOMIC_GET(&hbdelay); t > 0; t -= timeout) {
			if (GDKexiting() || !ATOMIC_GET(&hbrunning))
				return;
			MT_sleep_ms(t > timeout ? timeout : t);
		}
		if (GDKexiting() || !ATOMIC_GET(&hbrunning))
			return;
		MT_thread_setworking("pinging");
		profilerHeartbeatEvent("ping");
	}
}

void setHeartbeat(int delay)
{
	if (delay < 0){
		ATOMIC_SET(&hbrunning, 0);
		if (hbthread)
			MT_join_thread(hbthread);
		return;
	}
	if (delay > 0 &&  delay <= 10)
		delay = 10;
	ATOMIC_SET(&hbdelay, delay);
}

/* TODO getprofilerlimit and setprofilerlimit functions */

int getprofilerlimit(void)
{
	return 0;
}

void setprofilerlimit(int limit)
{
	(void) limit;
}

void initProfiler(void)
{
	gettimeofday(&startup_time, NULL);
}

void initHeartbeat(void)
{
	ATOMIC_SET(&hbrunning, 1);
	if (MT_create_thread(&hbthread, profilerHeartbeat, NULL, MT_THR_JOINABLE,
						 "heartbeat") < 0) {
		/* it didn't happen */
		hbthread = 0;
		ATOMIC_SET(&hbrunning, 0);
	}
}
