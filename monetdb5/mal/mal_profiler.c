/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/* (c) M.L. Kersten
 * Performance tracing
 * The stethoscope/tachograph and tomograph performance monitors have exclusive
 * access to a single event stream, which avoids concurrency conflicts amongst
 * clients.
 * It also avoid cluthered event records on the stream. Since this event stream
 * is owned by a client, we should ensure that the profiler is automatically
 * reset once the owner leaves.
 */
#include "monetdb_config.h"
#include "mutils.h"				/* mercurial_revision */
#include "msabaoth.h"			/* msab_getUUID */
#include "mal_authorize.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_profiler.h"
#include "mal_runtime.h"
#include "mal_utils.h"
#include "mal_resource.h"
#include "mal_internal.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include <string.h>

/* The JSON rendering can be either using '\n' separators between
 * each key:value pair or as a single line.
 * The current stethoscope implementation requires the first option and
 * also the term rendering to be set to ''
 */

static struct timeval startup_time;

#define LOGLEN 8192

/*
 * We use a buffer (`logbuffer`) where we incrementally create the output JSON
 * object. Initially we allocate LOGLEN (8K)
 * bytes and we keep the capacity of the buffer (`logcap`) and the length of the
 * current string (`loglen`).
 *
 * We use the `logadd` function to add data to our buffer (usually key-value
 * pairs). This macro offers an interface similar to printf.
 *
 * The first snprintf below happens in a statically allocated buffer that might
 * be much smaller than logcap. We do not care. We only need to perform this
 * snprintf to get the actual length of the string that is to be produced.
 *
 * There are three cases:
 *
 * 1. The new string fits in the current buffer -> we just update the buffer
 *
 * 2. The new string does not fit in the current buffer, but is smaller than the
 * capacity of the buffer -> we output the current contents of the buffer and
 * start at the beginning.
 *
 * 3. The new string exceeds the current capacity of the buffer -> we output the
 * current contents and reallocate the buffer. The new capacity is 1.5 times the
 * length of the new string.
 */
struct logbuf {
	char *logbuffer;
	char *logbase;
	size_t loglen;
	size_t logcap;
};

static inline void
lognew(struct logbuf *logbuf)
{
	logbuf->loglen = 0;
	logbuf->logbase = logbuf->logbuffer;
	*logbuf->logbase = 0;
}

static inline void
logdel(struct logbuf *logbuf)
{
	GDKfree(logbuf->logbuffer);
	logbuf->logbuffer = NULL;
}

static bool logadd(struct logbuf *logbuf,
				   _In_z_ _Printf_format_string_ const char *fmt, ...)
		__attribute__((__format__(__printf__, 2, 3)))
		__attribute__((__warn_unused_result__));
static bool
logadd(struct logbuf *logbuf, const char *fmt, ...)
{
	char tmp_buff[LOGLEN];
	int tmp_len;
	va_list va;

	va_start(va, fmt);
	tmp_len = vsnprintf(tmp_buff, sizeof(tmp_buff), fmt, va);
	va_end(va);
	if (tmp_len < 0) {
		logdel(logbuf);
		return false;
	}
	if (logbuf->loglen + (size_t) tmp_len >= logbuf->logcap) {
		char *alloc_buff;
		logbuf->logcap += (size_t) tmp_len + (size_t) tmp_len / 2;
		if (logbuf->logcap < LOGLEN)
			logbuf->logcap = LOGLEN;
		alloc_buff = GDKrealloc(logbuf->logbuffer, logbuf->logcap);
		if (alloc_buff == NULL) {
			TRC_ERROR(MAL_SERVER,
					  "Profiler JSON buffer reallocation failure\n");
			logdel(logbuf);
			return false;
		}
		logbuf->logbuffer = alloc_buff;
		lognew(logbuf);
	}
	if (tmp_len > 0) {
		va_start(va, fmt);
		logbuf->loglen += vsnprintf(logbuf->logbase + logbuf->loglen,
									logbuf->logcap - logbuf->loglen, fmt, va);
		va_end(va);
	}
	return true;
}

static str phase_descriptions[] = {
	[CLIENT_START] = "session_start",
	[CLIENT_END] = "session_end",
	[TEXT_TO_SQL] = "text_to_sql",
	[SQL_TO_REL] = "sql_to_rel",
	[REL_OPT] = "rel_opt",
	[REL_TO_MAL] = "rel_to_mal",
	[MAL_OPT] = "mal_opt",
	[MAL_ENGINE] = "mal_engine",
	[COMMIT] = "trans_commit",
	[ROLLBACK] = "trans_rollback",
	[CONFLICT] = "trans_conflict"
};

static str
prepareMalEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
				lng clk, lng ticks)
{
	struct logbuf logbuf;
	uint64_t mclk;
	const char *algo = MT_thread_getalgorithm();

	logbuf = (struct logbuf) { 0 };

	mclk = (uint64_t) clk - ((uint64_t) startup_time.tv_sec * 1000000 -
							 (uint64_t) startup_time.tv_usec);
	/* make profile event tuple  */
	if (!logadd(&logbuf, "{"	// fill in later with the event counter
				"\"sessionid\":\"%d\""
				",\"clk\":%" PRIu64 ""
				",\"thread\":%zu"
				",\"phase\":\"%s\""
				",\"pc\":%d"
				",\"tag\":" OIDFMT,
				cntxt->idx,
				mclk,
				MT_getpid(),
				phase_descriptions[MAL_ENGINE],
				mb ? getPC(mb, pci) : 0, stk ? stk->tag : 0))
		goto cleanup_and_exit;
	if (pci->modname
		&& !logadd(&logbuf, ",\"module\":\"%s\"",
				   pci->modname ? pci->modname : ""))
		goto cleanup_and_exit;
	if (pci->fcnname
		&& !logadd(&logbuf, ",\"function\":\"%s\"",
				   pci->fcnname ? pci->fcnname : ""))
		goto cleanup_and_exit;
	if (pci->barrier
		&& !logadd(&logbuf, ",\"barrier\":\"%s\"", operatorName(pci->barrier)))
		goto cleanup_and_exit;
	if ((pci->token < FCNcall || pci->token > PATcall) &&
		!logadd(&logbuf, ",\"operator\":\"%s\"", operatorName(pci->token)))
		goto cleanup_and_exit;
	if (!logadd(&logbuf, ",\"usec\":" LLFMT, ticks))
		goto cleanup_and_exit;
	if (algo && !logadd(&logbuf, ",\"algorithm\":\"%s\"", algo))
		goto cleanup_and_exit;
	if (!logadd(&logbuf, "}\n"))	// end marker
		goto cleanup_and_exit;
	return logbuf.logbuffer;
  cleanup_and_exit:
	logdel(&logbuf);
	return NULL;
}

/* the OS details on cpu load are read from /proc/stat
 * We should use an OS define to react to the maximal cores
 */
#define MAXCORES		256
#define LASTCPU		(MAXCORES - 1)
static struct {
	lng user, nice, system, idle, iowait;
	double load;
} corestat[MAXCORES];

static int
getCPULoad(char cpuload[BUFSIZ])
{
	int cpu, len = 0, i;
	lng user, nice, system, idle, iowait;
	size_t n;
	char buf[512], *s;
	static FILE *proc = NULL;
	lng newload;

	if (proc == NULL) {
		proc = fopen("/proc/stat", "r");
		if (proc == NULL) {
			/* unexpected */
			return -1;
		}
	} else
		rewind(proc);

	while (fgets(buf, (int) sizeof(buf), proc) != NULL) {
		n = strlen(buf);
		if (strncmp(buf, "cpu", 3) == 0) {
			s = buf + 3;
			if (*s == ' ') {
				cpu = LASTCPU;	// the cpu totals stored here
			} else {
				cpu = atoi(s);
				if (cpu < 0 || cpu > LASTCPU)
					cpu = LASTCPU;
			}
			s = strchr(s, ' ');
			if (s == NULL)		/* unexpected format of file */
				break;
			while (*s && isspace((unsigned char) *s))
				s++;
			i = sscanf(s, LLSCN " " LLSCN " " LLSCN " " LLSCN " " LLSCN, &user,
					   &nice, &system, &idle, &iowait);
			if (i == 5) {
				newload = (user - corestat[cpu].user + nice - corestat[cpu].nice +
						   system - corestat[cpu].system);
				if (newload)
					corestat[cpu].load = (double) newload / (newload + idle -
															 corestat[cpu].idle + iowait -
															 corestat[cpu].iowait);
				corestat[cpu].user = user;
				corestat[cpu].nice = nice;
				corestat[cpu].system = system;
				corestat[cpu].idle = idle;
				corestat[cpu].iowait = iowait;
			}
		}

		while (buf[n - 1] != '\n') {
			if (fgets(buf, (int) sizeof(buf), proc) == NULL)
				goto exitloop;
			n = strlen(buf);
		}
	}
  exitloop:

	if (cpuload == NULL)
		return 0;
	// identify core processing
	len += snprintf(cpuload, BUFSIZ, "[");
	for (cpu = 0; cpuload && cpu < LASTCPU && corestat[cpu].user; cpu++) {
		len += snprintf(cpuload + len, BUFSIZ - len, "%s%.2f", (cpu ? "," : ""),
						corestat[cpu].load);
	}
	(void) snprintf(cpuload + len, BUFSIZ - len, "]");
	return 0;
}

/* SQL tracing is simplified, because it only collects the events in the temporary table.
 */
static void clearTrace(Client cntxt);
str
startTrace(Client cntxt)
{
	cntxt->sqlprofiler = true;
	clearTrace(cntxt);
	return MAL_SUCCEED;
}

str
stopTrace(Client cntxt)
{
	cntxt->sqlprofiler = false;
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
	BBPreclaim(cntxt->profticks);
	BBPreclaim(cntxt->profstmt);
	BBPreclaim(cntxt->profevents);
	cntxt->profticks = cntxt->profstmt = cntxt->profevents = NULL;
}

static inline BAT *
TRACEcreate(int tt)
{
	return COLnew(0, tt, 1 << 10, TRANSIENT);
}

static void
initTrace(Client cntxt)
{
	if (cntxt->profticks) {
		return;					/* already initialized */
	}
	cntxt->profticks = TRACEcreate(TYPE_lng);
	cntxt->profstmt = TRACEcreate(TYPE_str);
	cntxt->profevents = TRACEcreate(TYPE_str);
	if (cntxt->profticks == NULL || cntxt->profstmt == NULL
		|| cntxt->profevents == NULL)
		_cleanupProfiler(cntxt);
}

int
TRACEtable(Client cntxt, BAT **r)
{
	if (cntxt->sqlprofiler)
		return -1;

	if (cntxt->profticks == NULL) {
		r[0] = COLnew(0, TYPE_lng, 0, TRANSIENT);
		r[1] = COLnew(0, TYPE_str, 0, TRANSIENT);
		r[2] = COLnew(0, TYPE_str, 0, TRANSIENT);
	} else {
		r[0] = COLcopy(cntxt->profticks, cntxt->profticks->ttype, false,
					   TRANSIENT);
		r[1] = COLcopy(cntxt->profstmt, cntxt->profstmt->ttype, false,
					   TRANSIENT);
		r[2] = COLcopy(cntxt->profevents, cntxt->profevents->ttype, false,
					   TRANSIENT);
	}
	if (r[0] == NULL || r[1] == NULL || r[2] == NULL) {
		BBPreclaim(r[0]);
		BBPreclaim(r[1]);
		BBPreclaim(r[2]);
		return -1;
	}
	return 3;
}

static void
clearTrace(Client cntxt)
{
	(void) cntxt;
	if (cntxt->profticks != NULL) {
		/* drop all trace tables */
		_cleanupProfiler(cntxt);
	}
	initTrace(cntxt);
}

void
sqlProfilerEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
				 lng clk, lng ticks)
{
	str stmt, c, ev;
	int errors = 0;

	if (cntxt->profticks == NULL)
		return;

	/* generate actual call statement */
	stmt = instruction2str(mb, stk, pci, LIST_MAL_ALL | LIST_MAL_ALGO);
	c = stmt;

	/* unclear why we needed this. OLD?
	   while (c && *c && (isspace((unsigned char)*c) || *c == '!'))
	   c++;
	 */

	ev = prepareMalEvent(cntxt, mb, stk, pci, clk, ticks);
	// keep it a short transaction
	MT_lock_set(&mal_profileLock);
	if (cntxt->profticks == NULL) {
		MT_lock_unset(&mal_profileLock);
		//GDKfree(stmt);
		return;
	}
	errors += BUNappend(cntxt->profticks, &ticks, false) != GDK_SUCCEED;
	errors += BUNappend(cntxt->profstmt, c, false) != GDK_SUCCEED;
	errors += BUNappend(cntxt->profevents, ev ? ev : str_nil,
						false) != GDK_SUCCEED;
	if (errors > 0) {
		/* stop profiling if an error occurred */
		cntxt->sqlprofiler = false;
	}

	MT_lock_unset(&mal_profileLock);
	//GDKfree(stmt);
	GDKfree(ev);
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

				MT_lock_set(&b->theaplock);
				if (!isVIEW(b)) {
					BUN cnt = BATcount(b);

					/* the upperbound is used for the heaps */
					if (b->tvheap)
						size += HEAPvmsize(b->tvheap);
					MT_lock_unset(&b->theaplock);

					size += tailsize(b, cnt);
					if (b->thash)
						size += sizeof(BUN) * cnt;
					/* also add the size of an ordered index */
					if (b->torderidx)
						size += HEAPvmsize(b->torderidx);
				} else {
					MT_lock_unset(&b->theaplock);
				}
				BBPunfix(i);
			}
		}
	return size;
}


void
profilerGetCPUStat(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait)
{
	(void) getCPULoad(NULL);
	*user = corestat[LASTCPU].user;
	*nice = corestat[LASTCPU].nice;
	*sys = corestat[LASTCPU].system;
	*idle = corestat[LASTCPU].idle;
	*iowait = corestat[LASTCPU].iowait;
}

void
initProfiler(void)
{
	gettimeofday(&startup_time, NULL);
}
