/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

#define heapinfo(X) ((X) && (X)->base ? (X)->free: 0)
#define hashinfo(X) (((X) && (X) != (Hash *) 1 && (X)->mask)? ((X)->mask + (X)->lim + 1) * sizeof(int) + sizeof(*(X)) + cnt * sizeof(int):  0)

// Keep a queue of running queries
QueryQueue QRYqueue;
static int qtop, qsize;
static int qtag= 1;
static int calltag =0; // to identify each invocation

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

	MT_lock_set(&mal_delayLock);
	if ( QRYqueue == 0)
		QRYqueue = (QueryQueue) GDKzalloc( sizeof (struct QRYQUEUE) * (qsize= 256));
	else
	if ( qtop +1 == qsize )
		QRYqueue = (QueryQueue) GDKrealloc( QRYqueue, sizeof (struct QRYQUEUE) * (qsize +=256));
	if ( QRYqueue == NULL){
		GDKerror("runtimeProfileInit" MAL_MALLOC_FAIL);
		MT_lock_unset(&mal_delayLock);
		return;
	}
	for( i = 0; i < qtop; i++)
		if ( QRYqueue[i].mb == mb)
			break;

	stk->tag = calltag++;
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

	qtop += i == qtop;
	MT_lock_unset(&mal_delayLock);
}

void
runtimeProfileFinish(Client cntxt, MalBlkPtr mb)
{
	int i,j;

	(void) cntxt;

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
	assert(prof);
	/* always collect the MAL instruction execution time */
	pci->ticks = GDKusec() - prof->ticks;
	pci->totticks += pci->ticks;
	pci->calls++;
	
	if(malProfileMode > 0){
		pci->wbytes += getVolume(stk, pci, 1);
		if (pci->recycle)
			pci->rbytes += getVolume(stk, pci, 0);
		profilerEvent(mb, stk, pci, FALSE, cntxt->username);
	}
	if( malProfileMode < 0){
		/* delay profiling until you encounter start of MAL function */
		if( getInstrPtr(mb,0) == pci)
			malProfileMode = 1;
	}
	cntxt->active = FALSE;
}

/*
 * For performance evaluation it is handy to estimate the
 * amount of bytes produced by an instruction.
 * The actual amount is harder to guess, because an instruction
 * may trigger a side effect, such as creating a hash-index.
 * Side effects are ignored.
 */
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

			b = BBPquickdesc(abs(stk->stk[getArg(pci, i)].val.bval), TRUE);
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
