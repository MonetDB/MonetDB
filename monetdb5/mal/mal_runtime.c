/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/* Author(s) M.L. Kersten
 * The MAL Runtime Profiler and system queue
 * This little helper module is used to perform instruction based profiling.
 * The QRYqueue is only update at the start/finish of a query.
 * It is also the place to keep track on the number of workers
 * The current code relies on a scan rather than a hash.
 */

#include "monetdb_config.h"
#include "mal_utils.h"
#include "mal_runtime.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "mal_profiler.h"
#include "mal_listing.h"
#include "mal_authorize.h"
#include "mal_resource.h"
#include "mal_internal.h"
#include "mal_private.h"


QueryQueue QRYqueue = NULL;
size_t qsize = 0;
static oid qtag= 1;		// A unique query identifier

UserStats  USRstats = NULL;
size_t usrstatscnt = 0;

static inline void
clearUSRstats(size_t idx)
{
	USRstats[idx] = (struct USERSTAT) {0};
}

/*
 * Find the index of the given 'user' in USRstats.
 * For a new 'user' return a new free slot.
 * If USRstats is full, extend it.
 */
static size_t
getUSRstatsIdx(MalBlkPtr mb, oid user)
{
	size_t i = 0;
	UserStats tmp = NULL;

	for (i = 0; i < usrstatscnt; i++)
		/* The array is dense, so we either find the user or an empty slot. */
		if (USRstats[i].user == user || USRstats[i].username == NULL)
			return i;

	/* expand USRstats */
	tmp = (UserStats) GDKrealloc(USRstats, sizeof (struct USERSTAT) * (size_t) (usrstatscnt += MAL_MAXCLIENTS));
	if (tmp == NULL) {
		/* It's not a fatal error if we can't extend USRstats.
		 * We don't want to affect existing USRstats. */
		addMalException(mb,"getUSRstatsIdx" MAL_MALLOC_FAIL);
		return (size_t) -1;
	}
	USRstats = tmp;
	for ( ; i < usrstatscnt; i++)
		clearUSRstats(i);
	return usrstatscnt - MAL_MAXCLIENTS;
}

static void
updateUserStats(Client cntxt, MalBlkPtr mb, lng ticks, time_t started, time_t finished, str query)
{
	// don't keep stats for context without username
 	if (cntxt->username == NULL)
 		return;

	size_t idx = getUSRstatsIdx(mb, cntxt->user);

	if (idx == (size_t) -1) {
		addMalException(mb, "updateUserStats" "Failed to get an entry in user statistics");
		return;
	}

	if (USRstats[idx].username == NULL || USRstats[idx].user != cntxt->user || strcmp(USRstats[idx].username, cntxt->username) != 0) {
		GDKfree(USRstats[idx].username);
		GDKfree(USRstats[idx].maxquery);
		clearUSRstats(idx);
		USRstats[idx].user = cntxt->user;
		USRstats[idx].username = GDKstrdup(cntxt->username);
	}
	USRstats[idx].querycount++;
	USRstats[idx].totalticks += ticks;
	if (ticks >= USRstats[idx].maxticks && query) {
		USRstats[idx].started = started;
		USRstats[idx].finished = finished;
		USRstats[idx].maxticks = ticks;
		GDKfree(USRstats[idx].maxquery);
		USRstats[idx].maxquery = GDKstrdup(query);
	}
}

/*
 * Free up the whole USRstats before mserver5 exits.
 */
static void
dropUSRstats(void)
{
	size_t i;
	MT_lock_set(&mal_delayLock);
	for(i = 0; i < usrstatscnt; i++) {
		GDKfree(USRstats[i].username);
		GDKfree(USRstats[i].maxquery);
		clearUSRstats(i);
	}
	GDKfree(USRstats);
	USRstats = NULL;
	usrstatscnt = 0;
	MT_lock_unset(&mal_delayLock);
}

static str
isaSQLquery(MalBlkPtr mb)
{
	if (mb) {
		for (int i = 1; i< mb->stop; i++) {
			InstrPtr p = getInstrPtr(mb,i);
			if (getModuleId(p) && idcmp(getModuleId(p), "querylog") == 0 && idcmp(getFunctionId(p),"define")==0)
				return getVarConstant(mb,getArg(p,1)).val.sval;
		}
	}
	return NULL;
}

/*
 * Manage the runtime profiling information
 * It is organized as a circular buffer, head/tail.
 * Elements are removed from the buffer when it becomes full.
 * This way we keep the information a little longer for inspection.
 */

/* clear the next entry for a new call unless it is a running query */
static inline void
clearQRYqueue(size_t idx)
{
	QRYqueue[idx] = (struct QRYQUEUE) {0};
}

static void
dropQRYqueue(void)
{
	MT_lock_set(&mal_delayLock);
	for(size_t i = 0; i < qsize; i++) {
		GDKfree(QRYqueue[i].query);
		GDKfree(QRYqueue[i].username);
		clearQRYqueue(i);
	}
	GDKfree(QRYqueue);
	QRYqueue = NULL;
	qsize = 0;
	qtag = 1;
	MT_lock_unset(&mal_delayLock);
}

oid
runtimeProfileSetTag(Client cntxt)
{
	MT_lock_set(&mal_delayLock);
	cntxt->curprg->def->tag = qtag++;
	MT_lock_unset(&mal_delayLock);

	return cntxt->curprg->def->tag;
}

/* At the start of every MAL block or SQL query */
void
runtimeProfileInit(Client cntxt, MalBlkPtr mb, MalStkPtr stk)
{
	static size_t qlast = 0;
	size_t i, paused = 0;
	str q;

	/* Recursive calls don't change the query queue, but later we have to check
	   how to stop/pause/resume queries doing recursive calls from multiple workers */
	if (stk->up)
		return;
	MT_lock_set(&mal_delayLock);

	if (USRstats == NULL) {
		usrstatscnt = MAL_MAXCLIENTS;
		USRstats = (UserStats) GDKzalloc( sizeof (struct USERSTAT) * usrstatscnt);
		if (USRstats == NULL) {
			addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
			MT_lock_unset(&mal_delayLock);
			return;
		}
	}

	if (QRYqueue == NULL) {
		QRYqueue = (QueryQueue) GDKzalloc( sizeof (struct QRYQUEUE) * (qsize= MAL_MAXCLIENTS));

		if (QRYqueue == NULL) {
			addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
			MT_lock_unset(&mal_delayLock);
			return;
		}
	}
	for (i = 0; i < qsize; i++) {
		paused += QRYqueue[i].status && (QRYqueue[i].status[0] == 'p' || QRYqueue[i].status[0] == 'r'); /* running, prepared or paused */
	}
	if (qsize - paused < (size_t) MAL_MAXCLIENTS) {
		qsize += MAL_MAXCLIENTS;
		QueryQueue tmp;
		tmp = (QueryQueue) GDKrealloc( QRYqueue, sizeof (struct QRYQUEUE) * qsize);
		if (tmp == NULL) {
			addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
			qsize -= MAL_MAXCLIENTS; /* undo increment */
			MT_lock_unset(&mal_delayLock);
			return;
		}
		QRYqueue = tmp;
		for (i = qsize - MAL_MAXCLIENTS; i < qsize; i++)
			clearQRYqueue(i);
	}

	// add new invocation
	for (i = 0; i < qsize; i++) {
		size_t j = qlast;
		if (++qlast >= qsize)
			qlast = 0;
		if (QRYqueue[j].stk == NULL ||
			QRYqueue[j].status == NULL ||
			(QRYqueue[j].status[0] != 'r' &&
			 QRYqueue[j].status[0] != 'p')) {
			QRYqueue[j].mb = mb;
			QRYqueue[j].tag = stk->tag = mb->tag;
			QRYqueue[j].stk = stk;				// for status pause 'p'/running '0'/ quiting 'q'
			QRYqueue[j].finished = 0;
			QRYqueue[j].start = time(0);
			q = isaSQLquery(mb);
			GDKfree(QRYqueue[j].query);
			QRYqueue[j].query = GDKstrdup(q); /* NULL in, NULL out */
			GDKfree(QRYqueue[j].username);
			if (!GDKembedded())
				QRYqueue[j].username = GDKstrdup(cntxt->username);
			QRYqueue[j].idx = cntxt->idx;
			/* give the MB upperbound by addition of 1 MB */
			QRYqueue[j].memory = 1 + (int) (stk->memory / LL_CONSTANT(1048576)); /* Convert to MB */
			QRYqueue[j].workers = (int) 1;	/* this is the first one */
			QRYqueue[j].status = "running";
			QRYqueue[j].cntxt = cntxt;
			QRYqueue[j].ticks = GDKusec();
			break;
		}
	}
	MT_lock_unset(&mal_delayLock);
	MT_lock_set(&mal_contextLock);
	cntxt->idle = 0;
	MT_lock_unset(&mal_contextLock);
}

/*
 * At the end of every MAL block or SQL query.
 *
 * Returning from a recursive call does not change the number of workers.
 */
void
runtimeProfileFinish(Client cntxt, MalBlkPtr mb, MalStkPtr stk)
{
	size_t i;
	bool found = false;

	/* Recursive calls don't change the query queue, but later we have to check
	   how to stop/pause/resume queries doing recursive calls from multiple workers */
	if (stk->up)
		return;
	MT_lock_set(&mal_delayLock);
	for (i = 0; i < qsize; i++) {
		if (QRYqueue[i].stk == stk) {
			QRYqueue[i].status = "finished";
			QRYqueue[i].finished = time(0);
			QRYqueue[i].workers = mb->workers;
			/* give the MB upperbound by addition of 1 MB */
			QRYqueue[i].memory = 1 + (int)(mb->memory / LL_CONSTANT(1048576));
			QRYqueue[i].cntxt = NULL;
			QRYqueue[i].stk = NULL;
			QRYqueue[i].mb = NULL;
			QRYqueue[i].ticks = GDKusec() - QRYqueue[i].ticks;
			updateUserStats(cntxt, mb, QRYqueue[i].ticks, QRYqueue[i].start, QRYqueue[i].finished, QRYqueue[i].query);
			// assume that the user is now idle
			MT_lock_unset(&mal_delayLock);
			MT_lock_set(&mal_contextLock);
			cntxt->idle = time(0);
			MT_lock_unset(&mal_contextLock);
			found = true;
			break;
		}
	}

	// every query that has been started has an entry in QRYqueue.  If this
	// finished query is not found, we want to print some informational
	// messages for debugging.
	if (!found) {
		assert(0);
		TRC_INFO_IF(MAL_SERVER) {
			TRC_INFO_ENDIF(MAL_SERVER, "runtimeProfilerFinish: stk (%p) not found in QRYqueue", stk);
			for (i = 0; i < qsize; i++) {
				// print some info. of queries not "finished"
				if (strcmp(QRYqueue[i].status, "finished") != 0) {
					TRC_INFO_ENDIF(MAL_SERVER, "QRYqueue[%zu]: stk(%p), tag("OIDFMT"), username(%s), start(%ld), status(%s), query(%s)",
								   i, QRYqueue[i].stk, QRYqueue[i].tag,
								   QRYqueue[i].username, QRYqueue[i].start,
								   QRYqueue[i].status, QRYqueue[i].query);
				}
			}
		}
		MT_lock_unset(&mal_delayLock);
	}

}

/* Used by mal_reset to do the grand final clean up of this area before MonetDB exits */
void
mal_runtime_reset(void)
{
	dropQRYqueue();
	dropUSRstats();
}

/*
 * Each MAL instruction is executed by a single thread, which means we can
 * keep a simple working set around to make Stethscope attachement easy.
 * The entries are privately accessed and only can be influenced by a starting stehoscope to emit work in progress.
 */
Workingset workingset[THREADS];

/* At the start of each MAL stmt */
void
runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof)
{
	int tid = THRgettid();

	assert(pci);
	/* keep track on the instructions taken in progress for stethoscope*/
	if (tid > 0 && tid <= THREADS) {
		tid--;
		if (profilerStatus) {
			MT_lock_set(&mal_profileLock);
			workingset[tid].cntxt = cntxt;
			workingset[tid].mb = mb;
			workingset[tid].stk = stk;
			workingset[tid].pci = pci;
			MT_lock_unset(&mal_profileLock);
		} else {
			workingset[tid].cntxt = cntxt;
			workingset[tid].mb = mb;
			workingset[tid].stk = stk;
			workingset[tid].pci = pci;
		}
	}
	/* always collect the MAL instruction execution time */
	pci->clock = prof->ticks = GDKusec();
}

/* At the end of each MAL stmt */
void
runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof)
{
	int tid = THRgettid();
	lng ticks = GDKusec();

	/* keep track on the instructions in progress*/
	if (tid > 0 && tid <= THREADS) {
		tid--;
		if (profilerStatus) {
			MT_lock_set(&mal_profileLock);
			workingset[tid] = (struct WORKINGSET) {0};
			MT_lock_unset(&mal_profileLock);
		} else{
			workingset[tid] = (struct WORKINGSET) {0};
		}
	}

	/* always collect the MAL instruction execution time */
	pci->clock = ticks;
	pci->ticks = ticks - prof->ticks;

	if (profilerStatus > 0 )
		profilerEvent(&(struct MalEvent) {cntxt, mb, stk, pci},
					  NULL);
	if (cntxt->sqlprofiler)
		sqlProfilerEvent(cntxt, mb, stk, pci);
	if (profilerStatus < 0) {
		/* delay profiling until you encounter start of MAL function */
		if (getInstrPtr(mb,0) == pci)
			profilerStatus = 1;
	}
}

/*
 * For performance evaluation it is handy to estimate the
 * amount of bytes produced by an instruction.
 * The actual amount is harder to guess, because an instruction
 * may trigger a side effect, such as creating a hash-index.
 * Side effects are ignored.
 */

lng
getBatSpace(BAT *b)
{
	lng space=0;
	if (b == NULL)
		return 0;
	space += BATcount(b) << b->tshift;
	if (space) {
		MT_lock_set(&b->theaplock);
		if (b->tvheap)
			space += heapinfo(b->tvheap, b->batCacheid);
		MT_lock_unset(&b->theaplock);
		MT_rwlock_rdlock(&b->thashlock);
		space += hashinfo(b->thash, b->batCacheid);
		MT_rwlock_rdunlock(&b->thashlock);
		space += IMPSimprintsize(b);
	}
	return space;
}

lng
getVolume(MalStkPtr stk, InstrPtr pci, int rd)
{
	int i, limit;
	lng vol = 0;
	BAT *b;

	if (stk == NULL)
		return 0;
	limit = rd ? pci->argc : pci->retc;
	i = rd ? pci->retc : 0;

	for (; i < limit; i++) {
		if (stk->stk[getArg(pci, i)].vtype == TYPE_bat) {
			oid cnt = 0;

			b = BBPquickdesc(stk->stk[getArg(pci, i)].val.bval);
			if (b == NULL)
				continue;
			cnt = BATcount(b);
			/* Usually reading views cost as much as full bats.
			   But when we output a slice that is not the case. */
			if (rd)
				vol += (!isVIEW(b) && !VIEWtparent(b)) ? tailsize(b, cnt) : 0;
			else if (!isVIEW(b))
				vol += tailsize(b, cnt);
		}
	}
	return vol;
}
