/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
#define hashinfo(X) (((X) && (X)->mask)? ((X)->mask + (X)->lim + 1) * sizeof(int) + sizeof(*(X)) + cnt * sizeof(int):  0)

// Keep a queue of running queries
QueryQueue QRYqueue;
static int qtop, qsize;
static int qtag= 1;


static str isaSQLquery(MalBlkPtr mb){
	int i;
	InstrPtr p;
	if (mb)
	for ( i = mb->stop-1 ; i > 0; i--){
		p = getInstrPtr(mb,i);
		if ( p->token == ENDsymbol)
			break;
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

	if ( malProfileMode || mb->recycle )
		setFilterOnBlock(mb, 0, 0);

	MT_lock_set(&mal_delayLock, "sysmon");
	if ( QRYqueue == 0)
		QRYqueue = (QueryQueue) GDKzalloc( sizeof (struct QRYQUEUE) * (qsize= 256));
	else
	if ( qtop +1 == qsize )
		QRYqueue = (QueryQueue) GDKrealloc( QRYqueue, sizeof (struct QRYQUEUE) * (qsize +=256));
	if ( QRYqueue == NULL){
		GDKerror("runtimeProfileInit" MAL_MALLOC_FAIL);
		MT_lock_unset(&mal_delayLock, "sysmon");
		return;
	}
	for( i = 0; i < qtop; i++)
		if ( QRYqueue[i].mb == mb)
			break;

	if ( mb->tag == 0)
		mb->tag = OIDnew(1);
	if ( i == qtop ) {
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
	MT_lock_unset(&mal_delayLock, "sysmon");
}

void
runtimeProfileFinish(Client cntxt, MalBlkPtr mb)
{
	int i,j;

	(void) cntxt;

	MT_lock_set(&mal_delayLock, "sysmon");
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
	MT_lock_unset(&mal_delayLock, "sysmon");
}

void
finishSessionProfiler(Client cntxt)
{
	int i,j;

	(void) cntxt;

	MT_lock_set(&mal_delayLock, "sysmon");
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
	MT_lock_unset(&mal_delayLock, "sysmon");
}

void
runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int stkpc, RuntimeProfile prof, int start)
{
	if ( mb->profiler == NULL)
		return;

	/* always collect the MAL instruction execution time */
	prof->stkpc = stkpc;
	mb->profiler[stkpc].clk = GDKusec();

	if (malProfileMode == 0 && mb->stmt[stkpc]->recycle == 0)
		return; /* mostly true */
	
	if (stk && mb->profiler[stkpc].trace) {
		gettimeofday(&mb->profiler[stkpc].clock, NULL);
		/* emit the instruction upon start as well */
		if(malProfileMode)
			profilerEvent(cntxt->idx, mb, stk, stkpc, start);
#ifdef HAVE_TIMES
		times(&mb->profiler[stkpc].timer);
#endif
	}
}

void
runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof)
{
	int i,j,fnd, stkpc;

	if ( mb->profiler == NULL)
		return;

	/* always collect the MAL instruction execution time */
	stkpc= prof->stkpc;
	mb->profiler[stkpc].ticks = GDKusec() - mb->profiler[stkpc].clk;

	if (malProfileMode == 0 && pci->recycle == 0)
		return; /* mostly true */

	if (getProfileCounter(PROFfootprint) && pci){
		for (i = 0; i < pci->retc; i++)
			if ( isaBatType(getArgType(mb,pci,i)) && stk->stk[getArg(pci,i)].val.bval){
				/* avoid simple alias operations */
				fnd= 0;
				for ( j= pci->retc; j< pci->argc; j++)
					if ( isaBatType(getArgType(mb,pci,j)))
						fnd+= stk->stk[getArg(pci,i)].val.bval == stk->stk[getArg(pci,j)].val.bval;
				if (fnd == 0 )
					updateFootPrint(mb,stk,getArg(pci,i));
			}
	}

	if (stk != NULL && stkpc >= 0 && (mb->profiler[stkpc].trace || pci->recycle) ) {
		gettimeofday(&mb->profiler[stkpc].clock, NULL);
		mb->profiler[stkpc].calls++;
		mb->profiler[stkpc].totalticks += mb->profiler[stkpc].ticks;
		if (pci) {
			// it is a potential expensive operation
			if (getProfileCounter(PROFrbytes) || pci->recycle)
				mb->profiler[stkpc].rbytes = getVolume(stk, pci, 0);
			if (getProfileCounter(PROFwbytes) || pci->recycle)
				mb->profiler[stkpc].wbytes = getVolume(stk, pci, 1);
		}
		if(malProfileMode)
			profilerEvent(cntxt->idx, mb, stk, stkpc, 0);
	}
}

/*
 * For performance evaluation it is handy to know the
 * maximal amount of bytes read/written. The actual
 * amount is harder to guess, because it too much
 * depends on the operation.
 */
lng getVolume(MalStkPtr stk, InstrPtr pci, int rd)
{
	int i, limit;
	lng vol = 0;
	BAT *b;
	int isview = 0;

	limit = rd == 0 ? pci->retc : pci->argc;
	i = rd ? pci->retc : 0;

	if (stk->stk[getArg(pci, 0)].vtype == TYPE_bat) {
		b = BBPquickdesc(abs(stk->stk[getArg(pci, 0)].val.bval), TRUE);
		if (b)
			isview = isVIEW(b);
	}
	for (; i < limit; i++) {
		if (stk->stk[getArg(pci, i)].vtype == TYPE_bat) {
			oid cnt = 0;

			b = BBPquickdesc(abs(stk->stk[getArg(pci, i)].val.bval), TRUE);
			if (b == NULL)
				continue;
			cnt = BATcount(b);
			/* Usually reading views cost as much as full bats.
			   But when we output a slice that is not the case. */
			vol += ((rd && !isview) || !VIEWhparent(b)) ? headsize(b, cnt) : 0;
			vol += ((rd && !isview) || !VIEWtparent(b)) ? tailsize(b, cnt) : 0;
		}
	}
	return vol;
}

void displayVolume(Client cntxt, lng vol)
{
	char buf[32];
	formatVolume(buf, (int) sizeof(buf), vol);
	mnstr_printf(cntxt->fdout, "%s", buf);
}
/*
 * The footprint maintained in the stack is the total size all non-persistent objects in MB.
 * It gives an impression of the total extra memory needed during query evaluation.
 * Note, it does imply that all that space is claimed at the same time.
 */

void
updateFootPrint(MalBlkPtr mb, MalStkPtr stk, int varid)
{
    BAT *b;
	BUN cnt;
    lng total = 0;
	int bid;

	if ( !mb || !stk)
		return ;
	if ( isaBatType(getVarType(mb,varid)) && (bid = stk->stk[varid].val.bval) != bat_nil){

		b = BATdescriptor(bid);
        if (b == NULL || isVIEW(b) || b->batPersistence == PERSISTENT)
            return;
		cnt = BATcount(b);
		if( b->H ) total += heapinfo(&b->H->heap);
		if( b->H ) total += heapinfo(b->H->vheap);

		if ( b->T ) total += heapinfo(&b->T->heap);
		if ( b->T ) total += heapinfo(b->T->vheap);
		if ( b->H ) total += hashinfo(b->H->hash);
		if ( b->T ) total += hashinfo(b->T->hash); 
		BBPreleaseref(b->batCacheid);
		// no concurrency protection (yet)
		stk->tmpspace += total/1024/1024; // keep it in MBs
    }
}
