/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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


QueryQueue QRYqueue;
lng qsize, qhead, qtail;
static oid qtag= 1;		// A unique query identifier

void
mal_runtime_reset(void)
{
	GDKfree(QRYqueue);
	QRYqueue = 0;
	qsize = 0;
	qtag= 1;
	qhead = 0;
	qtail = 0;
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
advanceQRYqueue(void)
{
	for( qhead++; qhead!= qtail; qhead++){
		if( qhead == qsize)
			qhead = 0;
		if(QRYqueue[qhead].status == 0 || (QRYqueue[qhead].status[0] != 'r' && QRYqueue[qhead].status[0] != 'p'))
			break;
	}
	if( qtail == qsize)
		qtail = 0;
	if( qtail == qhead)
		qtail++;
	/* clean out the element */
	if( QRYqueue[qhead].query){
		GDKfree(QRYqueue[qhead].query);
		GDKfree(QRYqueue[qhead].username);
		QRYqueue[qhead].cntxt = 0;
		QRYqueue[qhead].username = 0;
		QRYqueue[qhead].idx = 0;
		QRYqueue[qhead].memory = 0;
		QRYqueue[qhead].tag = 0;
		QRYqueue[qhead].query = 0;
		QRYqueue[qhead].status =0;
		QRYqueue[qhead].finished = 0;
		QRYqueue[qhead].start = 0;
		QRYqueue[qhead].stk =0;
		QRYqueue[qhead].mb =0;
	}
}

void
runtimeProfileInit(Client cntxt, MalBlkPtr mb, MalStkPtr stk)
{
	lng i, paused = 0;
	str q;
	QueryQueue tmp;

	MT_lock_set(&mal_delayLock);
	tmp = QRYqueue;
	if ( QRYqueue == 0)
		QRYqueue = (QueryQueue) GDKzalloc( sizeof (struct QRYQUEUE) * (size_t) (qsize= MAL_MAXCLIENTS)); /* for testing */

	if ( QRYqueue == NULL){
		addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
		GDKfree(tmp);			
		MT_lock_unset(&mal_delayLock);
		return;
	}
	// check for recursive call, which does not change the number of workers
	for( i = qtail; i != qhead; i++){
		if( i == qsize){
			i = 0;
		}
		if( i == qhead)
			break;
		if (QRYqueue[i].mb && QRYqueue[i].mb == mb &&  stk->up == QRYqueue[i].stk){
			QRYqueue[i].stk = stk;
			mb->tag = stk->tag = qtag++;
			MT_lock_unset(&mal_delayLock);
			return;
		}
		paused += QRYqueue[i].status[0] == 'p'; /* prepared or paused */
	}
	if( qsize - paused < MAL_MAXCLIENTS){
		QRYqueue = (QueryQueue) GDKrealloc( QRYqueue, sizeof (struct QRYQUEUE) * (size_t) (qsize += MAL_MAXCLIENTS));
		if ( QRYqueue == NULL){
			addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
			GDKfree(tmp);			
			MT_lock_unset(&mal_delayLock);
			return;
		}
	}

	// add new invocation
	QRYqueue[qhead].mb = mb;
	QRYqueue[qhead].tag = qtag++;
	QRYqueue[qhead].stk = stk;				// for status pause 'p'/running '0'/ quiting 'q'
	QRYqueue[qhead].finished = 
	QRYqueue[qhead].start = time(0);
	q = isaSQLquery(mb);
	QRYqueue[qhead].query = q? GDKstrdup(q):0;
	AUTHgetUsername(&QRYqueue[qhead].username, cntxt);
	QRYqueue[qhead].idx = cntxt->idx;
	QRYqueue[qhead].memory = (int) (stk->memory / LL_CONSTANT(1048576)); /* Convert to MB */
	QRYqueue[qhead].workers = (int) stk->workers;
	QRYqueue[qhead].status = "running";
	QRYqueue[qhead].cntxt = cntxt;
	stk->tag = mb->tag = QRYqueue[qhead].tag;
	advanceQRYqueue();
	MT_lock_unset(&mal_delayLock);
}

/* 
 * Returning from a recursive call does not change the number of workers.
 */

void
runtimeProfileFinish(Client cntxt, MalBlkPtr mb, MalStkPtr stk)
{
	lng i;

	(void) cntxt;
	(void) mb;

	MT_lock_set(&mal_delayLock);
	for( i=qtail; i != qhead; i++){
		if ( i >= qsize){
			i = 0;
		}
		if ( QRYqueue[i].stk == stk){
			if( stk->up){
				// recursive call
				QRYqueue[i].stk = stk->up;
				mb->tag = stk->tag;
				MT_lock_unset(&mal_delayLock);
				return;
			}
			QRYqueue[i].status = "finished";
			QRYqueue[i].finished = time(0);
			QRYqueue[i].cntxt = 0;
			QRYqueue[i].stk = 0;
			QRYqueue[i].mb = 0;
			break;
		}
		if( i == qhead)
			break;
	}

	MT_lock_unset(&mal_delayLock);
}

/*
 * Each MAL instruction is executed by a single thread, which means we can
 * keep a simple working set around to make Stethscope attachement easy.
 * It can also be used to later shutdown each thread safely.
 */
Workingset workingset[THREADS];

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
