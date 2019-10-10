/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/* Author(s) M.L. Kersten
 * The MAL Runtime Profiler and system queue
 * This little helper module is used to perform instruction based profiling.
 * We should actually keep a little list of recently executed queries to inspection.
 * [TODO] It should be moved into the Client record for speed (>500 client connections)
 * Long term archival is left to the SQL history management structure
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


// Keep a queue of running queries
QueryQueue QRYqueue;
lng qtop;
static lng qsize;
static oid qtag= 1;

#define QRYreset(I)\
		if (QRYqueue[I].query) GDKfree(QRYqueue[I].query);\
		QRYqueue[I].cntxt = 0; \
		QRYqueue[I].tag = 0; \
		QRYqueue[I].query = 0; \
		QRYqueue[I].status =0; \
		QRYqueue[I].stk =0; \
		QRYqueue[I].mb =0; \

void
mal_runtime_reset(void)
{
	GDKfree(QRYqueue);
	QRYqueue = 0;
	qtop = 0;
	qsize = 0;
	qtag= 1;
}

static str isaSQLquery(MalBlkPtr mb){
	int i;
	InstrPtr p;
	if (mb)
	for ( i = 1; i< mb->stop; i++){
		p = getInstrPtr(mb,i);
		if ( getModuleId(p) && idcmp(getModuleId(p), "querylog") == 0 && idcmp(getFunctionId(p),"define")==0)
			return getVarConstant(mb,getArg(p,1)).val.sval;
	}
	return 0;
}

/*
 * Manage the runtime profiling information
 */

void
runtimeProfileInit(Client cntxt, MalBlkPtr mb, MalStkPtr stk)
{
	lng i;
	str q;
	QueryQueue tmp;

	MT_lock_set(&mal_delayLock);
	tmp = QRYqueue;
	if ( QRYqueue == 0)
		QRYqueue = (QueryQueue) GDKzalloc( sizeof (struct QRYQUEUE) * (size_t) (qsize= 1024));
	else if ( qtop + 1 == qsize )
		QRYqueue = (QueryQueue) GDKrealloc( QRYqueue, sizeof (struct QRYQUEUE) * (size_t) (qsize += 256));
	if ( QRYqueue == NULL){
		addMalException(mb,"runtimeProfileInit" MAL_MALLOC_FAIL);
		GDKfree(tmp);			/* may be NULL, but doesn't harm */
		MT_lock_unset(&mal_delayLock);
		return;
	}
	// check for recursive call
	for( i = 0; i < qtop; i++)
		if ( QRYqueue[i].mb == mb &&  stk->up == QRYqueue[i].stk){
			QRYqueue[i].stk = stk;
			stk->tag = QRYqueue[i].tag;
			MT_lock_unset(&mal_delayLock);
			return;
		}

	// add new invocation
	if (i == qtop) {
		QRYqueue[i].mb = mb;
		QRYqueue[i].tag = qtag++;
		mb->tag = QRYqueue[i].tag;
		QRYqueue[i].stk = stk;				// for status pause 'p'/running '0'/ quiting 'q'
		QRYqueue[i].start = time(0);
		QRYqueue[i].runtime = mb->runtime; 	// the estimated execution time
		q = isaSQLquery(mb);
		QRYqueue[i].query = q? GDKstrdup(q):0;
		QRYqueue[i].status = "running";
		QRYqueue[i].cntxt = cntxt;
	}
	stk->tag = QRYqueue[i].tag;
	qtop += i == qtop;
	MT_lock_unset(&mal_delayLock);
}

/* We should keep a short list of previously executed queries/client for inspection */

void
runtimeProfileFinish(Client cntxt, MalBlkPtr mb, MalStkPtr stk)
{
	lng i,j;

	(void) cntxt;
	(void) mb;

	MT_lock_set(&mal_delayLock);
	for( i=j=0; i< qtop; i++)
	if ( QRYqueue[i].stk != stk)
		QRYqueue[j++] = QRYqueue[i];
	else  {
		if( stk->up){
			// recursive call
			QRYqueue[i].stk = stk->up;
			MT_lock_unset(&mal_delayLock);
			return;
		}
		QRYqueue[i].mb->calls++;
		QRYqueue[i].mb->runtime += (lng) ((lng)(time(0) - QRYqueue[i].start) * 1000.0/QRYqueue[i].mb->calls);
		QRYqueue[i].status = "finished";
		QRYreset(i)
	}

	qtop = j;
	QRYqueue[qtop].query = NULL; /* sentinel for SYSMONqueue() */
	MT_lock_unset(&mal_delayLock);
}

void
finishSessionProfiler(Client cntxt)
{
	lng i,j;

	(void) cntxt;

	MT_lock_set(&mal_delayLock);
	for( i=j=0; i< qtop; i++)
	if ( QRYqueue[i].cntxt != cntxt)
		QRYqueue[j++] = QRYqueue[i];
	else  {
		//reset entry
		if (QRYqueue[i].query)
			GDKfree(QRYqueue[i].query);
		QRYqueue[i].cntxt = 0;
		QRYqueue[i].tag = 0;
		QRYqueue[i].query = 0;
		QRYqueue[i].status =0;
		QRYqueue[i].stk =0;
		QRYqueue[i].mb =0;
	}
	qtop = j;
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
		profilerEvent(mb, stk, pci, TRUE, cntxt->username);
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
	pci->ticks = ticks - prof->ticks;
	pci->totticks += pci->ticks;
	pci->calls++;
	
	if(malProfileMode > 0 )
		profilerEvent(mb, stk, pci, FALSE, cntxt->username);
	if( malProfileMode < 0){
		/* delay profiling until you encounter start of MAL function */
		if( getInstrPtr(mb,0) == pci)
			malProfileMode = 1;
	}
	/* Reduce worker threads of non-admin long running transaction if needed.
 	* the punishment is equal to the duration of the last instruction */
	if ( cntxt->user != MAL_ADMIN && ticks - mb->starttime > LONGRUNNING )
		MALresourceFairness(cntxt, mb, stk, pci, pci->ticks);
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
