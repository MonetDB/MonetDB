/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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

static const char *myname = 0;	// avoid tracing the profiler module

/* The JSON rendering can be either using '\n' separators between
 * each key:value pair or as a single line.
 * The current stethoscope implementation requires the first option and
 * also the term rendering to be set to ''
 */

int profilerStatus = 0;     /* global flag profiler status */
int profilerMode = 0;       /* global flag profiler mode, minimal or detailed */
static oid profilerUser;	/* keep track on who has claimed the channel */

static struct timeval startup_time;

static ATOMIC_TYPE hbdelay = ATOMIC_VAR_INIT(0);

#ifdef HAVE_SYS_RESOURCE_H
struct rusage infoUsage;
static struct rusage prevUsage;
#endif

#define LOGLEN 8192

static void logjsonInternal(char *logbuffer, bool flush)
{
	size_t len;
	len = strlen(logbuffer);

	(void) mnstr_write(maleventstream, logbuffer, 1, len);
	if (flush)
		(void) mnstr_flush(maleventstream, MNSTR_FLUSH_DATA);
}

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
	va_list va2;

	va_start(va, fmt);
	va_copy(va2, va);			/* we will need it again */
	tmp_len = vsnprintf(tmp_buff, sizeof(tmp_buff), fmt, va);
	if (tmp_len < 0) {
		logdel(logbuf);
		va_end(va);
		va_end(va2);
		return false;
	}
	if (tmp_len > 0) {
		if (logbuf->loglen + (size_t) tmp_len >= logbuf->logcap) {
			if ((size_t) tmp_len >= logbuf->logcap) {
				/* includes first time when logbuffer == NULL and logcap == 0 */
				char *alloc_buff;
				if (logbuf->loglen > 0)
					logjsonInternal(logbuf->logbuffer, false);
				logbuf->logcap = (size_t) tmp_len + (size_t) tmp_len/2;
				if (logbuf->logcap < LOGLEN)
					logbuf->logcap = LOGLEN;
				alloc_buff = GDKrealloc(logbuf->logbuffer, logbuf->logcap);
				if (alloc_buff == NULL) {
					TRC_ERROR(MAL_SERVER, "Profiler JSON buffer reallocation failure\n");
					logdel(logbuf);
					va_end(va);
					va_end(va2);
					return false;
				}
				logbuf->logbuffer = alloc_buff;
				lognew(logbuf);
			} else {
				logjsonInternal(logbuf->logbuffer, false);
				lognew(logbuf);
			}
		}
		logbuf->loglen += vsnprintf(logbuf->logbase + logbuf->loglen,
									logbuf->logcap - logbuf->loglen,
									fmt, va2);
	}
	va_end(va);
	va_end(va2);
	return true;
}

static str phase_descriptions[] = {
	[CLIENT_START]		= "session_start",
	[CLIENT_END]		= "session_end",
	[TEXT_TO_SQL]		= "text_to_sql",
	[SQL_TO_REL]		= "sql_to_rel",
	[REL_OPT]			= "rel_opt",
	[REL_TO_MAL]		= "rel_to_mal",
	[MAL_OPT]			= "mal_opt",
	[MAL_ENGINE]		= "mal_engine",
	[COMMIT]			= "trans_commit",
	[ROLLBACK]			= "trans_rollback",
	[CONFLICT]			= "trans_conflict"
};

static str
prepareNonMalEvent(Client cntxt, enum event_phase phase, ulng clk, ulng *tstart, ulng *tend, int state, ulng duration)
{
	oid* tag = NULL;
	str query = NULL;
	struct logbuf logbuf = {0};

	uint64_t mclk = (uint64_t)clk -
		((uint64_t)startup_time.tv_sec*1000000 - (uint64_t)startup_time.tv_usec);

	assert(cntxt);
	int sessionid = cntxt->idx;
	if (cntxt->curprg)
		tag = &cntxt->curprg->def->tag;
	if (cntxt->query && ( query = mal_quote(cntxt->query, strlen(cntxt->query)) ) == NULL)
		return NULL;

	if (!logadd(&logbuf, "{\"sessionid\":\"%d\"", sessionid))
		goto cleanup_and_exit;
	if (!logadd(&logbuf, ", \"clk\":"ULLFMT"", mclk))
		goto cleanup_and_exit;
	if (!logadd(&logbuf, ", \"thread\":%d, \"phase\":\"%s\"",
				THRgettid(), phase_descriptions[phase]))
		goto cleanup_and_exit;
	if (tstart && !logadd(&logbuf, ", \"tstart\":"ULLFMT, *tstart))
		goto cleanup_and_exit;
	if (tend && !logadd(&logbuf, ", \"tend\":"ULLFMT, *tend))
		goto cleanup_and_exit;
	if (tag && !logadd(&logbuf, ", \"tag\":"OIDFMT, *tag))
		goto cleanup_and_exit;
	if (query && phase == TEXT_TO_SQL && !logadd(&logbuf, ", \"query\":\"%s\"", query))
		goto cleanup_and_exit;
	if (state != 0 && !logadd(&logbuf, ", \"state\":\"error\""))
		goto cleanup_and_exit;
	if (!logadd(&logbuf, ", \"usec\":"ULLFMT"}\n", duration))
		goto cleanup_and_exit;
	GDKfree(query);
	return logbuf.logbuffer;
 cleanup_and_exit:
	GDKfree(query);
	logdel(&logbuf);
	return NULL;
}

static inline str
format_val2json(const ValPtr res) {
	char *buf = NULL;
	size_t sz = 0;

	if (BATatoms[res->vtype].atomNull &&
		BATatoms[res->vtype].atomCmp(VALget(res), BATatoms[res->vtype].atomNull) == 0)
		return GDKstrdup("\"nil\"");

	bool use_external = true;

	switch (res->vtype ) {
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_int:
		case TYPE_flt:
		case TYPE_dbl:
		case TYPE_lng:
#ifdef HAVE_HGE
		case TYPE_hge:
#endif
			use_external = false;
	}

	if ((*BATatoms[res->vtype].atomToStr) (&buf, &sz, VALptr(res), use_external) < 0)
		return NULL;

	if (!use_external || res->vtype == TYPE_str)
		return buf;

	ValRecord val;
	if (VALinit(&val, TYPE_str, buf) == NULL) {
		GDKfree(buf);
		return NULL;
	}

	GDKfree(buf);

	char* buf2;
	buf2 = VALformat(&val);
	VALclear(&val);

	return buf2;
}

static str
prepareMalEvent(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	struct logbuf logbuf;
	str c;
	lng clk;
	uint64_t mclk;
	bool ok;
	const char *algo = MT_thread_getalgorithm();

	/* The stream of events can be complete read by the DBA,
	 * all other users can only see events assigned to their account
	 */
	if(profilerUser!= MAL_ADMIN && profilerUser != cntxt->user)
		return NULL;

	/* align the variable namings with EXPLAIN and TRACE */
	if(pci->pc == 1)
		renameVariables(mb);

	logbuf = (struct logbuf) {0};

	clk = pci->clock;
	mclk = (uint64_t)clk - ((uint64_t)startup_time.tv_sec*1000000 - (uint64_t)startup_time.tv_usec);
	/* make profile event tuple  */
	if (!logadd(&logbuf,
				"{"				// fill in later with the event counter
				"\"sessionid\":\"%d\""
				",\"clk\":%"PRIu64""
				",\"thread\":%d"
				",\"phase\":\"%s\""
				",\"pc\":%d"
				",\"tag\":"OIDFMT,
				cntxt->idx,
				mclk,
				THRgettid(),
				phase_descriptions[MAL_ENGINE],
				mb?getPC(mb,pci):0,
				stk?stk->tag:0))
		goto cleanup_and_exit;
	if (pci->modname && !logadd(&logbuf, ",\"module\":\"%s\"", pci->modname ? pci->modname : ""))
		goto cleanup_and_exit;
	if (pci->fcnname && !logadd(&logbuf, ",\"function\":\"%s\"", pci->fcnname ? pci->fcnname : ""))
		goto cleanup_and_exit;
	if (pci->barrier && !logadd(&logbuf, ",\"barrier\":\"%s\"", operatorName(pci->barrier)))
		goto cleanup_and_exit;
	if ((pci->token < FCNcall || pci->token > PATcall) &&
		!logadd(&logbuf, ",\"operator\":\"%s\"", operatorName(pci->token)))
		goto cleanup_and_exit;
	if (!logadd(&logbuf, ",\"usec\":"LLFMT, pci->ticks))
		goto cleanup_and_exit;
	if (algo && !logadd(&logbuf, ",\"algorithm\":\"%s\"", algo))
		goto cleanup_and_exit;
	if (mb && pci->modname && pci->fcnname) {
		int j;

		if (profilerMode == 0 && stk) {
			if (!logadd(&logbuf, ",\"args\":["))
				goto cleanup_and_exit;
			for(j=0; j< pci->argc; j++){
				int tpe = getVarType(mb, getArg(pci,j));
				str tname = 0, cv;
				lng total = 0;
				BUN cnt = 0;
				bat bid=0;

				if (j == 0) {
					// No comma at the beginning
					if (!logadd(&logbuf, "{"))
						goto cleanup_and_exit;
				}
				else {
					if (!logadd(&logbuf, ",{"))
						goto cleanup_and_exit;
				}
				if (!logadd(&logbuf, "\"%s\":%d,\"var\":\"%s\"",
							j < pci->retc ? "ret" : "arg", j,
							getVarName(mb, getArg(pci,j))))
					goto cleanup_and_exit;
				c =getVarName(mb, getArg(pci,j));
				if(getVarSTC(mb,getArg(pci,j))){
					InstrPtr stc = getInstrPtr(mb, getVarSTC(mb,getArg(pci,j)));
					if (stc && getModuleId(stc) &&
						strcmp(getModuleId(stc),"sql") ==0 &&
						strncmp(getFunctionId(stc),"bind",4)==0 &&
						!logadd(&logbuf, ",\"alias\":\"%s.%s.%s\"",
								getVarConstant(mb, getArg(stc,stc->retc +1)).val.sval,
								getVarConstant(mb, getArg(stc,stc->retc +2)).val.sval,
								getVarConstant(mb, getArg(stc,stc->retc +3)).val.sval))
						goto cleanup_and_exit;
				}
				if(isaBatType(tpe)){
					BAT *d= BATdescriptor(bid = stk->stk[getArg(pci,j)].val.bval);
					tname = getTypeName(getBatType(tpe));
					ok = logadd(&logbuf, ",\"type\":\"bat[:%s]\"", tname);
					GDKfree(tname);
					if (!ok) {
						if (d)
							BBPunfix(d->batCacheid);
						goto cleanup_and_exit;
					}
					if(d) {
						MT_lock_set(&d->theaplock);
						BATiter di = bat_iterator_nolock(d);
						/* outside the lock we cannot dereference di.h or di.vh,
						 * but we can use all values without dereference and
						 * without further locking */
						MT_lock_unset(&d->theaplock);
						cnt = di.count;
						if(isVIEW(d)){
							BAT *v= BBP_cache(VIEWtparent(d));
							bool vtransient = true;
							if (v) {
								MT_lock_set(&v->theaplock);
								vtransient = v->batTransient;
								MT_lock_unset(&v->theaplock);
							}
							if (!logadd(&logbuf,
										",\"view\":\"true\""
										",\"parent\":%d"
										",\"seqbase\":"BUNFMT
										",\"mode\":\"%s\"",
										VIEWtparent(d),
										d->hseqbase,
										vtransient ? "transient" : "persistent")) {
								BBPunfix(d->batCacheid);
								goto cleanup_and_exit;
							}
						} else {
							if (!logadd(&logbuf, ",\"mode\":\"%s\"", (di.transient ? "transient" : "persistent"))) {
								BBPunfix(d->batCacheid);
								goto cleanup_and_exit;
							}
						}
						if (!logadd(&logbuf,
									",\"sorted\":%d"
									",\"revsorted\":%d"
									",\"nonil\":%d"
									",\"nil\":%d"
									",\"key\":%d",
									di.sorted,
									di.revsorted,
									di.nonil,
									di.nil,
									di.key)) {
							BBPunfix(d->batCacheid);
							goto cleanup_and_exit;
						}
						if ((di.minpos != BUN_NONE &&
							 !logadd(&logbuf, ",\"minpos\":\""BUNFMT"\"", di.minpos)) ||
							(di.maxpos != BUN_NONE &&
							 !logadd(&logbuf, ",\"maxpos\":\""BUNFMT"\"", di.maxpos)) ||
							(di.unique_est != 0 &&
							 !logadd(&logbuf, ",\"nestimate\":\"%g\"", di.unique_est))) {
							BBPunfix(d->batCacheid);
							goto cleanup_and_exit;
						}

						cv = VALformat(&stk->stk[getArg(pci,j)]);
						if (cv) {
							c = strchr(cv, '>');
							if (c)		/* unlikely that this isn't true */
								*c = 0;
							ok = logadd(&logbuf, ",\"file\":\"%s\"", cv + 1);
							GDKfree(cv);
							if (!ok) {
								BBPunfix(d->batCacheid);
								goto cleanup_and_exit;
							}
						}
						total += cnt << di.shift;
						if (!logadd(&logbuf, ",\"width\":%d", di.width)) {
							BBPunfix(d->batCacheid);
							goto cleanup_and_exit;
						}
						/* keeping information about the individual auxiliary heaps is helpful during analysis. */
						MT_rwlock_rdlock(&d->thashlock);
						if( d->thash && !logadd(&logbuf, ",\"hash\":" LLFMT, (lng) hashinfo(d->thash, d->batCacheid))) {
							MT_rwlock_rdunlock(&d->thashlock);
							BBPunfix(d->batCacheid);
							goto cleanup_and_exit;
						}
						MT_rwlock_rdunlock(&d->thashlock);
						if( di.vh && !logadd(&logbuf, ",\"vheap\":" BUNFMT, di.vhfree)) {
							BBPunfix(d->batCacheid);
							goto cleanup_and_exit;
						}
						if( d->timprints && !logadd(&logbuf, ",\"imprints\":" LLFMT, (lng) IMPSimprintsize(d))) {
							BBPunfix(d->batCacheid);
							goto cleanup_and_exit;
						}
						/* if (!logadd(&logbuf, "\"debug\":\"%s\",", d->debugmessages)) goto cleanup_and_exit; */
						BBPunfix(d->batCacheid);
					}
					if (!logadd(&logbuf,
								",\"bid\":%d"
								",\"count\":"BUNFMT
								",\"size\":" LLFMT,
								bid, cnt, total))
						goto cleanup_and_exit;
				} else{
					tname = getTypeName(tpe);
					ok = logadd(&logbuf,
								",\"type\":\"%s\""
								",\"const\":%d",
								tname, isVarConstant(mb, getArg(pci,j)));
					GDKfree(tname);
					if (!ok)
						goto cleanup_and_exit;
					cv = format_val2json(&stk->stk[getArg(pci,j)]);
					if (cv)
						ok = logadd(&logbuf, ",\"value\":%s", cv);
					GDKfree(cv);
					if (!ok)
						goto cleanup_and_exit;
				}
				if (!logadd(&logbuf, ",\"eol\":%d", getVarEolife(mb,getArg(pci,j))))
					goto cleanup_and_exit;
				// if (!logadd(&logbuf, ",\"fixed\":%d", isVarFixed(mb,getArg(pci,j)))) return NULL;
				if (!logadd(&logbuf, "}"))
					goto cleanup_and_exit;
			}
			if (!logadd(&logbuf, "]")) // end marker for arguments
				goto cleanup_and_exit;
		}
	}
	if (!logadd(&logbuf, "}\n")) // end marker
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
static struct{
	lng user, nice, system, idle, iowait;
	double load;
} corestat[MAXCORES];

static int
getCPULoad(char cpuload[BUFSIZ]){
    int cpu, len = 0, i;
	lng user, nice, system, idle, iowait;
	size_t n;
    char buf[512], *s;
	static FILE *proc= NULL;
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
				cpu = LASTCPU; // the cpu totals stored here
			}  else {
				cpu = atoi(s);
				if (cpu < 0 || cpu > LASTCPU)
					cpu = LASTCPU;
			}
			s = strchr(s,' ');
			if (s == NULL)		/* unexpected format of file */
				break;
			while (*s && isspace((unsigned char)*s))
				s++;
			i= sscanf(s, LLSCN" "LLSCN" "LLSCN" "LLSCN" "LLSCN,  &user, &nice, &system, &idle, &iowait);
			if (i == 5) {
				newload = (user - corestat[cpu].user + nice - corestat[cpu].nice + system - corestat[cpu].system);
				if (newload)
					corestat[cpu].load = (double) newload / (newload + idle - corestat[cpu].idle + iowait - corestat[cpu].iowait);
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
		len +=snprintf(cpuload + len, BUFSIZ - len, "%s%.2f", (cpu?",":""), corestat[cpu].load);
	}
	(void) snprintf(cpuload + len, BUFSIZ - len, "]");
	return 0;
}

void
profilerHeartbeatEvent(char *alter)
{
	char cpuload[BUFSIZ];
	struct logbuf logbuf;
	lng usec;
	uint64_t microseconds;

	if (ATOMIC_GET(&hbdelay) == 0 || maleventstream  == 0)
		return;
	usec = GDKusec();
	microseconds = (uint64_t)startup_time.tv_sec*1000000 + (uint64_t)startup_time.tv_usec + (uint64_t)usec;

	/* get CPU load on beat boundaries only */
	if (getCPULoad(cpuload))
		return;

	logbuf = (struct logbuf) {0};

	if (!logadd(&logbuf, "{"))	// fill in later with the event counter
		return;
	if (!GDKinmemory(0) && !GDKembedded()) {
		char *uuid = NULL, *err;
		if ((err = msab_getUUID(&uuid)) == NULL) {
			bool ok = logadd(&logbuf, "\"session\":\"%s\",", uuid);
			free(uuid);
			if (!ok)
				return;
		} else
			free(err);
	}
	if (!logadd(&logbuf, "\"clk\":"LLFMT",\"ctime\":%"PRIu64",\"rss\":%zu,",
				usec,
				microseconds,
				MT_getrss()/1024/1024))
		return;
#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	if(infoUsage.ru_inblock - prevUsage.ru_inblock && !logadd(&logbuf, "\"inblock\":%ld,", infoUsage.ru_inblock - prevUsage.ru_inblock))
		return;
	if(infoUsage.ru_oublock - prevUsage.ru_oublock && !logadd(&logbuf, "\"oublock\":%ld,", infoUsage.ru_oublock - prevUsage.ru_oublock))
		return;
	if(infoUsage.ru_majflt - prevUsage.ru_majflt && !logadd(&logbuf, "\"majflt\":%ld,", infoUsage.ru_majflt - prevUsage.ru_majflt))
		return;
	if(infoUsage.ru_nswap - prevUsage.ru_nswap && !logadd(&logbuf, "\"nswap\":%ld,", infoUsage.ru_nswap - prevUsage.ru_nswap))
		return;
	if(infoUsage.ru_nvcsw - prevUsage.ru_nvcsw && !logadd(&logbuf, "\"nvcsw\":%ld,", infoUsage.ru_nvcsw - prevUsage.ru_nvcsw +infoUsage.ru_nivcsw - prevUsage.ru_nivcsw))
		return;
	prevUsage = infoUsage;
#endif
	if (!logadd(&logbuf,
				"\"state\":\"%s\","
				"\"cpuload\":%s"
				"}\n",			// end marker
				alter, cpuload))
		return;
	logjsonInternal(logbuf.logbuffer, true);
	logdel(&logbuf);
}

void
profilerEvent(MalEvent *me, NonMalEvent *nme)
{
	str event = NULL;
	assert(me == NULL || nme == NULL);
	/* ignore profiler monitoring cmds */
	if (me != NULL && me->cntxt != NULL && getModuleId(me->pci) == myname)
		return;

	MT_lock_set(&mal_profileLock);
	if (maleventstream) {
		if (me != NULL && me->mb != NULL && nme == NULL) {
			if (me->stk == NULL ||
				me->pci == NULL ||
				(profilerMode && me->mb && getPC(me->mb, me->pci) != 0)) {
				MT_lock_unset(&mal_profileLock);
				return; /* minimal mode */
			}
			event = prepareMalEvent(me->cntxt, me->mb, me->stk, me->pci);
		}
		if (me == NULL && nme != NULL && nme->phase != MAL_ENGINE) {
			event = prepareNonMalEvent(nme->cntxt, nme->phase, nme->clk, nme->tid, nme->ts, nme->state, nme->duration);
		}
		if (event) {
			logjsonInternal(event, true);
			GDKfree(event);
		}
	}
	MT_lock_unset(&mal_profileLock);
}

/* The first scheme dumps the events on a stream (and in the pool)
 */
str
openProfilerStream(Client cntxt, int m)
{
	int j;

#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &infoUsage);
	prevUsage = infoUsage;
#endif
	MT_lock_set(&mal_profileLock);
	if (myname == 0){
		myname = putName("profiler");
		logjsonInternal(monet_characteristics, true);
	}
	if (maleventstream){
		/* The DBA can always grab the stream, others have to wait */
		if (cntxt->user == MAL_ADMIN) {
			closeProfilerStream(cntxt);
		} else {
			MT_lock_unset(&mal_profileLock);
			throw(MAL,"profiler.start","Profiler already running, stream not available");
		}
	}
	/* 4 activates profiler in minimal mode. 1 and 3 were used in prev MonetDB versions */
	/* 0 activates profiler in detailed mode */
	switch (m) {
	    case 0:
			profilerStatus = -1;
			break;
	    case 4:
			profilerStatus = -1;
			profilerMode = 1;
			break;
	    default:
			MT_lock_unset(&mal_profileLock);
			throw(MAL,"profiler.openstream","Undefined profiler mode option");
	}
	maleventstream = cntxt->fdout;
	profilerUser = cntxt->user;

	// Ignore the JSON rendering mode, use compiled time version

	/* show all in progress instructions for stethoscope startup */
	/* wait a short time for instructions to finish updating their thread admin
	 * and then follow the locking scheme */

	MT_sleep_ms(200);

	for (j = 0; j <THREADS; j++){
		Client c = 0; MalBlkPtr m = 0; MalStkPtr s = 0; InstrPtr p = 0;
		c = workingset[j].cntxt;
		m = workingset[j].mb;
		s = workingset[j].stk;
		p =  workingset[j].pci;
		if (c && m && s && p) {
			/* show the event  assuming the quadruple is aligned*/
			MT_lock_unset(&mal_profileLock);
			profilerEvent(&(struct MalEvent) {c, m, s, p},
						  NULL);
			MT_lock_set(&mal_profileLock);
		}
	}
	MT_lock_unset(&mal_profileLock);
	return MAL_SUCCEED;
}

str
closeProfilerStream(Client cntxt)
{
	(void) cntxt;
	maleventstream = NULL;
	profilerStatus = 0;
	profilerMode = 0;
	profilerUser = 0;
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

	MT_lock_set(&mal_profileLock);
	if(maleventstream){
		MT_lock_unset(&mal_profileLock);
		throw(MAL,"profiler.start","Profiler already running, stream not available");
	}
	if (myname == 0){
		myname = putName("profiler");
	}
	profilerStatus = 1;
	logjsonInternal(monet_characteristics, true);
	MT_lock_unset(&mal_profileLock);
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
	if (profilerStatus)
		profilerStatus = 0;
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
	if (cntxt->profticks)
		BBPunfix(cntxt->profticks->batCacheid);
	if (cntxt->profstmt)
		BBPunfix(cntxt->profstmt->batCacheid);
	if (cntxt->profevents)
		BBPunfix(cntxt->profevents->batCacheid);
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
	MT_lock_set(&mal_profileLock);
	if (cntxt->profticks) {
		MT_lock_unset(&mal_profileLock);
		return;       /* already initialized */
	}
	cntxt->profticks = TRACEcreate(TYPE_lng);
	cntxt->profstmt = TRACEcreate(TYPE_str);
	cntxt->profevents = TRACEcreate(TYPE_str);
	if (cntxt->profticks == NULL || cntxt->profstmt == NULL || cntxt->profevents == NULL)
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
	r[2] = COLcopy(cntxt->profevents, cntxt->profevents->ttype, false, TRANSIENT);
	MT_lock_unset(&mal_profileLock);
	return 3;
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
		} else if (strcmp(nme, "events") == 0){
			bn = COLcopy(cntxt->profevents, cntxt->profevents->ttype, false, TRANSIENT);
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

	ev = prepareMalEvent(cntxt, mb, stk, pci);
	// keep it a short transaction
	MT_lock_set(&mal_profileLock);
	if (cntxt->profticks == NULL) {
		MT_lock_unset(&mal_profileLock);
		GDKfree(stmt);
		return;
	}
	errors += BUNappend(cntxt->profticks, &pci->ticks, false) != GDK_SUCCEED;
	errors += BUNappend(cntxt->profstmt, c, false) != GDK_SUCCEED;
	errors += BUNappend(cntxt->profevents, ev ? ev : str_nil, false) != GDK_SUCCEED;
	if (errors > 0) {
		/* stop profiling if an error occurred */
		cntxt->sqlprofiler = FALSE;
	}

	MT_lock_unset(&mal_profileLock);
	GDKfree(stmt);
	GDKfree(ev);
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
					/* also add the size of an imprint, ordered index or mosaic */
					if(b->timprints)
						size += IMPSimprintsize(b);
					if(b->torderidx)
						size += HEAPvmsize(b->torderidx);
				} else {
					MT_lock_unset(&b->theaplock);
				}
				BBPunfix(i);
			}
		}
	return size;
}


void profilerGetCPUStat(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait)
{
	(void) getCPULoad(NULL);
	*user = corestat[LASTCPU].user;
	*nice = corestat[LASTCPU].nice;
	*sys = corestat[LASTCPU].system;
	*idle = corestat[LASTCPU].idle;
	*iowait = corestat[LASTCPU].iowait;
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
