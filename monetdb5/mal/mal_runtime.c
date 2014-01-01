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
#include "mal_function.h"
#include "mal_profiler.h"
#include "mal_listing.h"

#define heapinfo(X) ((X) && (X)->base ? (X)->free: 0)
#define hashinfo(X) (((X) && (X)->mask)? ((X)->mask + (X)->lim + 1) * sizeof(int) + sizeof(*(X)) + cnt * sizeof(int):  0)
/*
 * Manage the runtime profiling information
 */
void
runtimeProfileInit(MalBlkPtr mb, RuntimeProfile prof, int initmemory)
{
	prof->newclk = 0;
	prof->ppc = -2;
	prof->tcs = 0;
	prof->inblock = 0;
	prof->oublock = 0;
	if (initmemory)
		prof->memory = MT_mallinfo();
	else
		memset(&prof->memory, 0, sizeof(prof->memory));
	if (malProfileMode) {
		setFilterOnBlock(mb, 0, 0);
		prof->ppc = -1;
	}
}

void
runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int stkpc, RuntimeProfile prof, int start)
{
	if (malProfileMode == 0)
		/* mostly true */;
	else
	if (stk && mb->profiler != NULL) {
		prof->newclk = stk->clk = GDKusec();
		if (mb->profiler[stkpc].trace) {
			MT_lock_set(&mal_contextLock, "DFLOWdelay");
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
			MT_lock_unset(&mal_contextLock, "DFLOWdelay");
		}
	}
}


void
runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, RuntimeProfile prof)
{
	int stkpc = prof->ppc;
	if (malProfileMode == 0)
		/* mostly true */;
	else if (stk != NULL && prof->ppc >= 0 && mb->profiler != NULL && mb->profiler[stkpc].trace && mb->profiler[stkpc].clk)
	{
		gettimeofday(&mb->profiler[stkpc].clock, NULL);
		mb->profiler[stkpc].counter++;
		mb->profiler[stkpc].ticks = GDKusec() - prof->newclk;
		mb->profiler[stkpc].totalticks += mb->profiler[stkpc].ticks;
		mb->profiler[stkpc].clk += mb->profiler[stkpc].clk;
		if (stkpc) {
			mb->profiler[stkpc].rbytes = getVolume(stk, getInstrPtr(mb, stkpc), 0);
			mb->profiler[stkpc].wbytes = getVolume(stk, getInstrPtr(mb, stkpc), 1);
		}
		profilerEvent(cntxt->idx, mb, stk, stkpc, 0);
		prof->ppc = -1;
	}
}

void
runtimeTiming(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int tid, MT_Lock *lock, RuntimeProfile prof)
{
	str line;
	if (cntxt->flags && stk->cmd != '\0' && stk->cmd != 't' && stk->cmd != 'C') {
		if (lock)
			MT_lock_set(lock, "timing");
		mnstr_printf(cntxt->fdout, "= ");    /* single column rendering */
		if (cntxt->flags & timerFlag) {
			char buf[32];
			snprintf(buf, sizeof(buf), LLFMT, GDKusec() - cntxt->timer);
			mnstr_printf(cntxt->fdout, "%8s usec ", buf);
		}
		if (cntxt->flags & threadFlag)
			mnstr_printf(cntxt->fdout, "@%d ", tid);
#ifdef HAVE_SYS_RESOURCE_H
		if (cntxt->flags & ioFlag) {
			struct  rusage resource;
			getrusage(RUSAGE_SELF, &resource);
			mnstr_printf(cntxt->fdout, " %3ld R",
					resource.ru_inblock);
			mnstr_printf(cntxt->fdout, " %3ld W ",
					resource.ru_oublock);
		}
#endif
		if (cntxt->flags & memoryFlag) {
			struct Mallinfo memory;
			memory = MT_mallinfo();
			if (memory.arena)
				mnstr_printf(cntxt->fdout, " " SZFMT " bytes ",
						(size_t) (memory.arena - prof->memory.arena));
			prof->memory.arena = memory.arena;
		}
		if (cntxt->flags & flowFlag) {
			/* calculate the read/write byte flow */
			displayVolume(cntxt, getVolume(stk, pci, 0));
			mnstr_printf(cntxt->fdout, "/");
			displayVolume(cntxt, getVolume(stk, pci, 1));
			mnstr_printf(cntxt->fdout, " ");
		}
		if (cntxt->flags & footprintFlag) 
			displayVolume(cntxt, getFootPrint(mb,stk));
		if (cntxt->flags & cntFlag) {
			char buf[32];
			snprintf(buf, sizeof(buf), OIDFMT, cntxt->cnt);
			mnstr_printf(cntxt->fdout, ":%6s ", buf);
		}
		line = instruction2str(mb, stk, pci, LIST_MAL_DEBUG);
		if (line) {
			mnstr_printf(cntxt->fdout, " %s\n", line);
			GDKfree(line);
		}
		if (cntxt->flags & timerFlag)
			cntxt->timer = GDKusec();
		if (lock)
			MT_lock_unset(lock, "timing");
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
 * The getFootPrint operation calculates the total size of all non-persistent
 * objects and the sizes of hashes for persistent objects. Together it gives
 * an impression of the total extra memory needed during query evaluation.
 * The operation is relatively expensive, as it goes through the complete
 * variable list and it may be influenced by concurrent activity.
 * The BAT descriptors should be accessed to private access.
 */
static lng prevtotal=0;

lng
getFootPrint(MalBlkPtr mb, MalStkPtr stk)
{
    BAT *b;
	BUN cnt;
    lng total = 0;
	int i, bid;

	if ( !mb || !stk)
		return prevtotal;
	for ( i = 1; i < mb->vtop; i++)
	if ( isaBatType(getVarType(mb,i)) && (bid = stk->stk[i].val.bval) != bat_nil){

        b = BATdescriptor(bid);
        if (b == NULL)
            continue;
        if (isVIEW(b)){
			BBPreleaseref(b->batCacheid);
            continue;
		}
        cnt = BATcount(b);
		if ( b->batPersistence != PERSISTENT) {
			if( b->H ) total += heapinfo(&b->H->heap);
			if( b->H ) total += heapinfo(b->H->vheap);

			if( b->T ) total += heapinfo(&b->T->heap);
			if( b->T ) total += heapinfo(b->T->vheap);
		}

		if ( b->H ) total += hashinfo(b->H->hash);
		if ( b->T ) total += hashinfo(b->T->hash); 
		BBPreleaseref(b->batCacheid);
    }
	if (total)
		prevtotal= total;
	return total;
}
