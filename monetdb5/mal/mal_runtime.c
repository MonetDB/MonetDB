/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdbuorg/Legal/MonetDBtxtLicense
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
 * Copyright August 2008-2013 MonetDB B.V.
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
struct RUN {
	Client cntxt;
	MalBlkPtr mb;
	lng tag;
	str query;
	str status;
	lng start;
	lng runtime;
} *running;
static int qtop, qsize;
static int qtag;


static str isaSQLquery(MalBlkPtr mb){
	int i;
	InstrPtr p;
	if (mb)
	for ( i = mb->stop-1 ; i > 0; i--){
		p = getInstrPtr(mb,i);
		if ( p->token == ENDsymbol)
			break;
		if ( getModuleId(p) && idcmp(getModuleId(p), "querylog") == 0 && idcmp(getFunctionId(p),"define")==0)
			return getVarConstant(mb,getArg(p,2)).val.sval;
	}
	return 0;
}

/*
 * Manage the runtime profiling information
 */
void
runtimeProfileInit(Client cntxt, MalBlkPtr mb, RuntimeProfile prof, int initmemory)
{
	int i;
	str q;

	MT_lock_set(&mal_delayLock, "sysmon");
	if ( running == 0)
		running = (struct RUN *) GDKzalloc( sizeof (struct RUN) * (qsize= 256));
	else
	if ( qtop +1 == qsize )
		running = (struct RUN *) GDKrealloc( running, sizeof (struct RUN) * (qsize +=256));
	for( i = 0; i < qtop; i++)
		if ( running[i].mb == mb)
			break;

	prof->newclk = 0;
	prof->ppc = -2;
	prof->tcs = 0;
	prof->inblock = 0;
	prof->oublock = 0;

	if ( i == qtop ) {
		running[i].mb = mb;	// for detecting duplicates
		running[i].tag = qtag++;
		running[i].start = GDKusec();
		running[i].runtime = mb->runtime;
		q = isaSQLquery(mb);
		running[i].query = q? GDKstrdup(q):0;
		running[i].status = "running";
		running[i].cntxt = cntxt;
	}
	if (initmemory)
		prof->memory = MT_mallinfo();
	else
		memset(&prof->memory, 0, sizeof(prof->memory));
	if (malProfileMode) {
		setFilterOnBlock(mb, 0, 0);
		prof->ppc = -1;
	}

	qtop += i == qtop;
	MT_lock_unset(&mal_delayLock, "sysmon");
	
}

void
runtimeProfileFinish(Client cntxt, MalBlkPtr mb, RuntimeProfile prof)
{
	int i,j;

	(void) cntxt;
	(void) prof;


	MT_lock_set(&mal_delayLock, "sysmon");
	for( i=j=0; i< qtop; i++)
	if ( running[i].mb != mb)
		running[j++] = running[i];
	else  {
		if (running[i].query)
			GDKfree(running[i].query);
		running[i].cntxt = 0;
		running[i].tag = 0;
		running[i].query = 0;
		running[i].status =0;
		mb->calls++;
		mb->runtime += ((GDKusec() - running[i].start)- running[i].runtime)/mb->calls;
	}

	qtop = j;
	MT_lock_unset(&mal_delayLock, "sysmon");
}

void
runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int stkpc, RuntimeProfile prof, int start)
{
	if (malProfileMode == 0)
		return; /* mostly true */
	
	if (stk && mb->profiler != NULL) {
		prof->newclk = stk->clk = GDKusec();
		if (mb->profiler[stkpc].trace) {
			MT_lock_set(&mal_delayLock, "sysmon");
			gettimeofday(&stk->clock, NULL);
			prof->ppc = stkpc;
			mb->profiler[stkpc].clk = 0;
			mb->profiler[stkpc].ticks = 0;
			mb->profiler[stkpc].clock = stk->clock;
			/* emit the instruction upon start as well */
			if (malProfileMode)
				profilerEvent(cntxt->idx, mb, stk, stkpc, start);
#ifdef HAVE_TIMES
			times(&stk->timer);
			mb->profiler[stkpc].timer = stk->timer;
#endif
			mb->profiler[stkpc].clk = stk->clk;
			MT_lock_unset(&mal_delayLock, "sysmon");
		}
	}
}


void
runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, RuntimeProfile prof)
{
	int i,j,fnd, stkpc = prof->ppc;

	if (cntxt->flags & footprintFlag && pci){
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

	if (malProfileMode == 0)
		return; /* mostly true */
	if (stk != NULL && prof->ppc >= 0 && mb->profiler != NULL && mb->profiler[stkpc].trace && mb->profiler[stkpc].clk)
	{
		MT_lock_set(&mal_contextLock, "sysmon");
		gettimeofday(&mb->profiler[stkpc].clock, NULL);
		mb->profiler[stkpc].counter++;
		mb->profiler[stkpc].ticks = GDKusec() - prof->newclk;
		mb->profiler[stkpc].totalticks += mb->profiler[stkpc].ticks;
		mb->profiler[stkpc].clk += mb->profiler[stkpc].clk;
		if (pci) {
			mb->profiler[stkpc].rbytes = getVolume(stk, pci, 0);
			mb->profiler[stkpc].wbytes = getVolume(stk, pci, 1);
		}
		profilerEvent(cntxt->idx, mb, stk, stkpc, 0);
		prof->ppc = -1;
		MT_lock_unset(&mal_contextLock, "sysmon");
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
		b = BBPquickdesc(ABS(stk->stk[getArg(pci, 0)].val.bval), TRUE);
		if (b)
			isview = isVIEW(b);
	}
	for (; i < limit; i++) {
		if (stk->stk[getArg(pci, i)].vtype == TYPE_bat) {
			oid cnt = 0;

			b = BBPquickdesc(ABS(stk->stk[getArg(pci, i)].val.bval), TRUE);
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

str
runtimeSQLqueue(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *tag, *user, *query, *estimate, *started, *progress, *activity;
	int *t = (int*) getArgReference(stk,pci,0);
	int *u = (int*) getArgReference(stk,pci,1);
	int *s = (int*) getArgReference(stk,pci,2);
	int *e = (int*) getArgReference(stk,pci,3);
	int *p = (int*) getArgReference(stk,pci,4);
	int *a = (int*) getArgReference(stk,pci,5);
	int *q = (int*) getArgReference(stk,pci,6);
	lng now;
	int i, prog;
	str usr;
	
	(void) cntxt;
	(void) mb;
	tag = BATnew(TYPE_void, TYPE_lng, qsize);
	user = BATnew(TYPE_void, TYPE_str, qsize);
	started = BATnew(TYPE_void, TYPE_lng, qsize);
	estimate = BATnew(TYPE_void, TYPE_lng, qsize);
	progress = BATnew(TYPE_void, TYPE_int, qsize);
	activity = BATnew(TYPE_void, TYPE_str, qsize);
	query = BATnew(TYPE_void, TYPE_str, qsize);
	if ( tag == NULL || query == NULL || started == NULL || estimate == NULL || progress == NULL || activity == NULL){
		if (tag) BBPreleaseref(tag->batCacheid);
		if (user) BBPreleaseref(user->batCacheid);
		if (query) BBPreleaseref(query->batCacheid);
		if (activity) BBPreleaseref(activity->batCacheid);
		if (started) BBPreleaseref(started->batCacheid);
		if (estimate) BBPreleaseref(estimate->batCacheid);
		if (progress) BBPreleaseref(progress->batCacheid);
		throw(MAL, "runtimeSQLqueue", MAL_MALLOC_FAIL);
	}
	BATseqbase(tag, 0);
    BATkey(tag, TRUE);

	BATseqbase(user, 0);
    BATkey(user, TRUE);

	BATseqbase(query, 0);
    BATkey(query, TRUE);

	BATseqbase(activity, 0);
    BATkey(activity, TRUE);

	BATseqbase(estimate, 0);
    BATkey(estimate, TRUE);

	BATseqbase(started, 0);
    BATkey(started, TRUE);

	BATseqbase(progress, 0);
    BATkey(progress, TRUE);

	MT_lock_set(&mal_delayLock, "sysmon");
	for ( i = 0; i< qtop; i++)
	if( running[i].query) {
		now= GDKusec();
		if ( (now-running[i].start) > running[i].runtime)
			prog =running[i].runtime > 0 ? 100: 0;
		else
			// calculate progress based on past observations
			prog = (int) ((now- running[i].start) / (running[i].runtime/100.0));
		
		BUNappend(tag, &running[i].tag, FALSE);
		AUTHgetUsername(&usr, &cntxt);

		BUNappend(user, usr, FALSE);
		BUNappend(query, running[i].query, FALSE);
		BUNappend(activity, running[i].status, FALSE);
		BUNappend(started, &running[i].start, FALSE);
		now = running[i].start + running[i].runtime;
		BUNappend(estimate, &now, FALSE);
		BUNappend(progress, &prog, FALSE);
	}
	MT_lock_unset(&mal_delayLock, "sysmon");
	BBPkeepref( *t =tag->batCacheid);
	BBPkeepref( *u =user->batCacheid);
	BBPkeepref( *q =query->batCacheid);
	BBPkeepref( *a =activity->batCacheid);
	BBPkeepref( *e = estimate->batCacheid);
	BBPkeepref( *s =started->batCacheid);
	BBPkeepref( *p =progress->batCacheid);
	return MAL_SUCCEED;
}

str
runtimeSQLpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int i;
	(void) mb;
	(void) stk;
	(void) pci;
	
	MT_lock_set(&mal_delayLock, "sysmon");
	for ( i = 0; i < qtop; i++)
	if( running[i].cntxt == cntxt || cntxt->idx == 0 ){
		running[i].cntxt->itrace = 'x';
		running[i].status = "paused";
	}
	MT_lock_unset(&mal_delayLock, "sysmon");
	return MAL_SUCCEED;
}
str
runtimeSQLresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int i;
	(void) mb;
	(void) stk;
	(void) pci;
	
	MT_lock_set(&mal_delayLock, "sysmon");
	for ( i = 0; i < qtop; i++)
	if( running[i].cntxt == cntxt || cntxt->idx ==0){
		running[i].cntxt->itrace = 0;
		running[i].status = "running";
	}
	MT_lock_unset(&mal_delayLock, "sysmon");
	return MAL_SUCCEED;
}
str
runtimeSQLstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int i;
	(void) mb;
	(void) stk;
	(void) pci;
	
	MT_lock_set(&mal_delayLock, "sysmon");
	for ( i = 0; i < qtop; i++)
	if( running[i].cntxt == cntxt || cntxt->idx == 0){
		running[i].cntxt->itrace = 'Q';
		running[i].status = "stopping";
	}
	MT_lock_unset(&mal_delayLock, "sysmon");
	return MAL_SUCCEED;
}
