/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* Author(s) M.L. Kersten
 * The MAL Runtime Profiler
 * This little helper module is used to perform instruction based profiling.
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
static int qtop, qsize;
static int qtag= 1;

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
	for ( i = 0; i< mb->stop; i++){
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
	int i;
	str q;
	QueryQueue tmp;

	MT_lock_set(&mal_delayLock);
	tmp = QRYqueue;
	if ( QRYqueue == 0)
		QRYqueue = (QueryQueue) GDKzalloc( sizeof (struct QRYQUEUE) * (qsize= 256));
	else if ( qtop +1 == qsize )
		QRYqueue = (QueryQueue) GDKrealloc( QRYqueue, sizeof (struct QRYQUEUE) * (qsize +=256));
	if ( QRYqueue == NULL){
		GDKfree(tmp);			/* may be NULL, but doesn't harm */
		MT_lock_unset(&mal_delayLock);
		return;
	}
	for( i = 0; i < qtop; i++)
		if ( QRYqueue[i].mb == mb)
			break;

	if ( i == qtop ) {
		mb->tag = qtag;
		QRYqueue[i].mb = mb;	// for detecting duplicates
		QRYqueue[i].stk = stk;	// for status pause 'p'/running '0'/ quiting 'q'
		QRYqueue[i].tag = qtag++;
		QRYqueue[i].start = (lng)time(0);
		QRYqueue[i].runtime = mb->runtime;
		q = isaSQLquery(mb);
		QRYqueue[i].query = q? GDKstrdup(q):0;
		QRYqueue[i].status = "running";
		QRYqueue[i].cntxt = cntxt;
	}
	stk->tag = QRYqueue[i].tag;
	qtop += i == qtop;
	MT_lock_unset(&mal_delayLock);
}

void
runtimeProfileFinish(Client cntxt, MalBlkPtr mb)
{
	int i,j;

	(void) cntxt;
	if( QRYqueue == NULL)
		return;

	MT_lock_set(&mal_delayLock);
	for( i=j=0; i< qtop; i++)
	if ( QRYqueue[i].mb != mb)
		QRYqueue[j++] = QRYqueue[i];
	else  {
		QRYqueue[i].mb->calls++;
		QRYqueue[i].mb->runtime += (lng) (((lng)time(0) - QRYqueue[i].start) * 1000.0/QRYqueue[i].mb->calls);

		// reset entry
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
	QRYqueue[qtop].query = NULL; /* sentinel for SYSMONqueue() */
	MT_lock_unset(&mal_delayLock);
}

void
finishSessionProfiler(Client cntxt)
{
	int i,j;

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

void
runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof)
{
	int tid = THRgettid();

	assert(pci);
	/* keep track on the instructions taken in progress */
	cntxt->active = TRUE;
	if( tid < THREADS){
		cntxt->inprogress[tid].mb = mb;
		cntxt->inprogress[tid].stk = stk;
		cntxt->inprogress[tid].pci = pci;
	}

	/* always collect the MAL instruction execution time */
	gettimeofday(&pci->clock,NULL);
	prof->ticks = GDKusec();

	/* keep track of actual running instructions over BATs */
	if( isaBatType(getArgType(mb, pci, 0)) )
		(void) ATOMIC_INC(mal_running, mal_runningLock);

	/* emit the instruction upon start as well */
	if(malProfileMode > 0)
		profilerEvent(mb, stk, pci, TRUE, cntxt->username);
}

void
runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof)
{
	int tid = THRgettid();

	/* keep track on the instructions in progress*/
	if ( tid < THREADS) {
		cntxt->inprogress[tid].mb = 0;
		cntxt->inprogress[tid].stk =0;
		cntxt->inprogress[tid].pci = 0;
	}

	assert(pci);
	if( isaBatType(getArgType(mb, pci, 0)) )
		(void) ATOMIC_DEC(mal_running, mal_runningLock);

	assert(prof);
	/* always collect the MAL instruction execution time */
	pci->ticks = GDKusec() - prof->ticks;
	pci->totticks += pci->ticks;
	pci->calls++;
	
	if(malProfileMode > 0){
		pci->wbytes += getVolume(stk, pci, 1);
		pci->rbytes += getVolume(stk, pci, 0);
		profilerEvent(mb, stk, pci, FALSE, cntxt->username);
	}
	if( malProfileMode < 0){
		/* delay profiling until you encounter start of MAL function */
		if( getInstrPtr(mb,0) == pci)
			malProfileMode = 1;
	}
	cntxt->active = FALSE;
	/* reduce threads of non-admin long running transaction if needed */
	if ( cntxt->idx > 1 )
		MALresourceFairness(GDKusec()- mb->starttime);
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

			b = BBPquickdesc(stk->stk[getArg(pci, i)].val.bval, TRUE);
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
