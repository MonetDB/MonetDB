/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal_private.h"


QueryQueue QRYqueue = NULL;
size_t qsize = 0, qhead = 0, qtail = 0;
static oid qtag= 1;		// A unique query identifier

UserStats  USRstats = NULL;
size_t usrstatscnt = 0;

static void
clearUSRstats(size_t idx)
{
	USRstats[idx].user= 0;
	USRstats[idx].username = 0;
	USRstats[idx].querycount = 0;
	USRstats[idx].totalticks = 0;
	USRstats[idx].started = 0;
	USRstats[idx].finished = 0;
	USRstats[idx].maxticks = 0;
	USRstats[idx].maxquery = 0;
}

/*
 * Find the index of the given 'user' in USRstats.
 * For a new 'user' return a new free slot.
 * If USRstats is full, extend it.
 */
static
size_t
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

static
void
updateUserStats(Client cntxt, MalBlkPtr mb, lng ticks, time_t started, time_t finished, str query)
{
	size_t idx = getUSRstatsIdx(mb, cntxt->user);

	if (idx == (size_t) -1) {
		addMalException(mb, "updateUserStats" "Failed to get an entry in user statistics");
		return;
	}

	if (USRstats[idx].username == NULL) {
		USRstats[idx].user = cntxt->user;
		USRstats[idx].username = GDKstrdup(cntxt->username);
	}
	USRstats[idx].querycount++;
	USRstats[idx].totalticks += ticks;
	if( ticks >= USRstats[idx].maxticks && query){
		USRstats[idx].started = started;
		USRstats[idx].finished = finished;
		USRstats[idx].maxticks = ticks;
		GDKfree(USRstats[idx].maxquery);
		USRstats[idx].maxquery= GDKstrdup(query);
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
	for(i = 0; i < usrstatscnt; i++){
		GDKfree(USRstats[i].username);
		GDKfree(USRstats[i].maxquery);
		clearUSRstats(i);
	}
	GDKfree(USRstats);
	USRstats = NULL;
	MT_lock_unset(&mal_delayLock);
}

static str
isaSQLquery(MalBlkPtr mb){
	int i;
	InstrPtr p;
	if (mb)
	for ( i = 1; i< mb->stop; i++){
		p = getInstrPtr(mb,i);
		if ( getModuleId(p) && idcmp(getModuleId(p), "querylog") == 0 && idcmp(getFunctionId(p),"define")==0)
			return getVarConstant(mb,getArg(p,1)).val.sval;
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
static void
clearQRYqueue(size_t idx)
{
		QRYqueue[idx].query = 0;
		QRYqueue[idx].cntxt = 0;
		QRYqueue[idx].username = 0;
		QRYqueue[idx].idx = 0;
		QRYqueue[idx].memory = 0;
		QRYqueue[idx].tag = 0;
		QRYqueue[idx].status =0;
		QRYqueue[idx].finished = 0;
		QRYqueue[idx].start = 0;
		QRYqueue[idx].stk =0;
		QRYqueue[idx].mb =0;
}

static void
advanceQRYqueue(void)
{
	qhead++;
	if( qhead == qsize)
		qhead = 0;
	if( qtail == qhead)
		qtail++;
	if( qtail == qsize)
		qtail = 0;
	/* clean out the element */
	str s = QRYqueue[qhead].query;
	if( s){
		/* don;t wipe them when they are still running, prepared, or paused */
		/* The upper layer has assured there is at least one slot available */
		if(QRYqueue[qhead].status != 0 && (QRYqueue[qhead].status[0] == 'r' || QRYqueue[qhead].status[0] == 'p')){
			advanceQRYqueue();
			return;
		}
		GDKfree(s);
		GDKfree(QRYqueue[qhead].username);
		clearQRYqueue(qhead);
	}
}

static void
dropQRYqueue(void)
{
	size_t i;
	MT_lock_set(&mal_delayLock);
	for(i = 0; i < qsize; i++){
		GDKfree(QRYqueue[i].query);
		GDKfree(QRYqueue[i].username);
		clearQRYqueue(i);
	}
	GDKfree(QRYqueue);
	QRYqueue = NULL;
	MT_lock_unset(&mal_delayLock);
}

/* At the start of every MAL block or SQL query */
void
runtimeProfileInit(Client cntxt, MalBlkPtr mb, MalStkPtr stk)
{
	size_t i, paused = 0;
	str q;
	QueryQueue tmp = NULL;

	MT_lock_set(&mal_delayLock);

	if(USRstats == NULL){
		usrstatscnt = MAL_MAXCLIENTS;
		USRstats = (UserStats) GDKzalloc( sizeof (struct USERSTAT) * usrstatscnt);
		if(USRstats == NULL) {
			addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
			MT_lock_unset(&mal_delayLock);
			return;
		}
	}

	tmp = QRYqueue;
	if ( QRYqueue == NULL) {
		QRYqueue = (QueryQueue) GDKzalloc( sizeof (struct QRYQUEUE) * (qsize= MAL_MAXCLIENTS));

		if ( QRYqueue == NULL){
			addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
			MT_lock_unset(&mal_delayLock);
			return;
		}
	}
	// check for recursive call, which does not change the number of workers
	i=qtail;
	while (i != qhead){
		if (QRYqueue[i].mb && QRYqueue[i].mb == mb &&  stk->up == QRYqueue[i].stk){
			QRYqueue[i].stk = stk;
			mb->tag = stk->tag = qtag++;
			MT_lock_unset(&mal_delayLock);
			return;
		}
		if ( QRYqueue[i].status)
			paused += (QRYqueue[i].status[0] == 'p' || QRYqueue[i].status[0] == 'r'); /* running, prepared or paused */
		i++;
		if ( i >= qsize)
			i = 0;
	}
	assert(qhead < qsize);
	if( qsize - paused < (size_t) MAL_MAXCLIENTS){
		qsize += MAL_MAXCLIENTS;
		tmp = (QueryQueue) GDKrealloc( QRYqueue, sizeof (struct QRYQUEUE) * qsize);
		if ( tmp == NULL){
			addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
			MT_lock_unset(&mal_delayLock);
			return;
		}
		QRYqueue = tmp;
		for(i = qsize - MAL_MAXCLIENTS; i < qsize; i++)
			clearQRYqueue(i);
	}

	// add new invocation
	cntxt->idle = 0;
	QRYqueue[qhead].mb = mb;
	QRYqueue[qhead].tag = qtag++;
	QRYqueue[qhead].stk = stk;				// for status pause 'p'/running '0'/ quiting 'q'
	QRYqueue[qhead].finished = 0;
	QRYqueue[qhead].start = time(0);
	q = isaSQLquery(mb);
	QRYqueue[qhead].query = q? GDKstrdup(q):0;
	GDKfree(QRYqueue[qhead].username);
	if (!GDKembedded())
		QRYqueue[qhead].username = GDKstrdup(cntxt->username);
	QRYqueue[qhead].idx = cntxt->idx;
	QRYqueue[qhead].memory = (int) (stk->memory / LL_CONSTANT(1048576)); /* Convert to MB */
	QRYqueue[qhead].workers = (int) stk->workers;
	QRYqueue[qhead].status = "running";
	QRYqueue[qhead].cntxt = cntxt;
	QRYqueue[qhead].ticks = GDKusec();
	stk->tag = mb->tag = QRYqueue[qhead].tag;
	advanceQRYqueue();
	MT_lock_unset(&mal_delayLock);
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

	MT_lock_set(&mal_delayLock);
	i=qtail;
	while (i != qhead){
		if ( QRYqueue[i].stk == stk){
			if( stk->up){
				// recursive call
				QRYqueue[i].stk = stk->up;
				mb->tag = stk->tag;
				break;
			}
			QRYqueue[i].status = "finished";
			QRYqueue[i].finished = time(0);
			QRYqueue[i].cntxt = 0;
			QRYqueue[i].stk = 0;
			QRYqueue[i].mb = 0;
			QRYqueue[i].ticks = GDKusec() - QRYqueue[i].ticks;
			updateUserStats(cntxt, mb, QRYqueue[i].ticks, QRYqueue[i].start, QRYqueue[i].finished, QRYqueue[i].query);
			// assume that the user is now idle
			cntxt->idle = time(0);
			found = true;
			break;
		}
		i++;
		if ( i >= qsize)
			i = 0;
	}

	// every query that has been started has an entry in QRYqueue.  If this
	// finished query is not found, we want to print some informational
	// messages for debugging.
	if (!found) {
		TRC_INFO_IF(MAL_SERVER) {
			TRC_INFO_ENDIF(MAL_SERVER, "runtimeProfilerFinish: stk (%p) not found in QRYqueue", stk);
			i = qtail;
			while (i != qhead){
				// print some info. of queries not "finished"
				if (strcmp(QRYqueue[i].status, "finished") != 0) {
					TRC_INFO_ENDIF(MAL_SERVER, "QRYqueue[%zu]: stk(%p), tag("OIDFMT"), username(%s), start(%ld), status(%s), query(%s)",
								   i, QRYqueue[i].stk, QRYqueue[i].tag,
								   QRYqueue[i].username, QRYqueue[i].start,
								   QRYqueue[i].status, QRYqueue[i].query);
				}
				i++;
				if ( i >= qsize)
					i = 0;
			}
		}
	}

	MT_lock_unset(&mal_delayLock);
}

/* Used by mal_reset to do the grand final clean up of this area before MonetDB exits */
void
mal_runtime_reset(void)
{
	dropQRYqueue();
	qsize = 0;
	qtag= 1;
	qhead = 0;
	qtail = 0;

	dropUSRstats();
	usrstatscnt = 0;
}

/*
 * Each MAL instruction is executed by a single thread, which means we can
 * keep a simple working set around to make Stethscope attachement easy.
 * It can also be used to later shutdown each thread safely.
 */
Workingset workingset[THREADS];

/* At the start of each MAL stmt */
void
runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof)
{
	int tid = THRgettid();

	assert(pci);
	/* keep track on the instructions taken in progress for stethoscope*/
	if( tid < THREADS){
		MT_lock_set(&mal_delayLock);
		workingset[tid].cntxt = cntxt;
		workingset[tid].mb = mb;
		workingset[tid].stk = stk;
		workingset[tid].pci = pci;
		MT_lock_unset(&mal_delayLock);
	}
	/* always collect the MAL instruction execution time */
	pci->clock = prof->ticks = GDKusec();

	/* emit the instruction upon start as well */
	if(malProfileMode > 0 )
		profilerEvent(cntxt, mb, stk, pci, TRUE);
}

/* At the end of each MAL stmt */
void
runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof)
{
	int tid = THRgettid();
	lng ticks = GDKusec();

	/* keep track on the instructions in progress*/
	if ( tid < THREADS) {
		MT_lock_set(&mal_delayLock);
		workingset[tid].mb = 0;
		workingset[tid].stk = 0;
		workingset[tid].pci = 0;
		MT_lock_unset(&mal_delayLock);
	}

	/* always collect the MAL instruction execution time */
	pci->clock = ticks;
	pci->ticks = ticks - prof->ticks;
	pci->totticks += pci->ticks;
	pci->calls++;

	if(malProfileMode > 0 )
		profilerEvent(cntxt, mb, stk, pci, FALSE);
	if( cntxt->sqlprofiler )
		sqlProfilerEvent(cntxt, mb, stk, pci);
	if( malProfileMode < 0){
		/* delay profiling until you encounter start of MAL function */
		if( getInstrPtr(mb,0) == pci)
			malProfileMode = 1;
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
getBatSpace(BAT *b){
	lng space=0;
	if( b == NULL)
		return 0;
	space += BATcount(b) * b->twidth;
	if( space){
		if( b->tvheap) space += heapinfo(b->tvheap, b->batCacheid);
		space += hashinfo(b->thash, b->batCacheid);
		space += IMPSimprintsize(b);
	}
	return space;
}

lng getVolume(MalStkPtr stk, InstrPtr pci, int rd)
{
	int i, limit;
	lng vol = 0;
	BAT *b;

	if( stk == NULL)
		return 0;
	limit = rd ? pci->argc : pci->retc;
	i = rd ? pci->retc : 0;

	for (; i < limit; i++) {
		if (stk->stk[getArg(pci, i)].vtype == TYPE_bat) {
			oid cnt = 0;

			b = BBPquickdesc(stk->stk[getArg(pci, i)].val.bval, true);
			if (b == NULL)
				continue;
			cnt = BATcount(b);
			/* Usually reading views cost as much as full bats.
			   But when we output a slice that is not the case. */
			if( rd)
				vol += (!isVIEW(b) && !VIEWtparent(b)) ? tailsize(b, cnt) : 0;
			else
			if( !isVIEW(b))
				vol += tailsize(b, cnt);
		}
	}
	return vol;
}
