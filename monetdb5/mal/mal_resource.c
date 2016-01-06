/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* (author) M.L. Kersten 
 */
#include "monetdb_config.h"
#include "mal_resource.h"
#include "mal_private.h"

#define heapinfo(X) if ((X) && (X)->base) vol = (X)->free; else vol = 0;
#define hashinfo(X) if ((X) && (X) != (Hash *) 1 && (X)->mask) vol = (((X)->mask + cnt ) * (X)-> width); else vol = 0;

/* MEMORY admission does not seem to have a major impact */
lng memorypool = 0;      /* memory claimed by concurrent threads */
int memoryclaims = 0;    /* number of threads active with expensive operations */

/*
 * Running all eligible instructions in parallel creates
 * resource contention. This means we should implement
 * an admission control scheme where threads are temporarily
 * postponed if the claim for memory exceeds a threshold
 * In general such contentions will be hard to predict,
 * because they depend on the algorithm, the input sizes,
 * concurrent use of the same variables, and the output produced.
 *
 * One heuristic is based on calculating the storage footprint
 * of the operands and assuming it preferrably should fit in memory.
 * Ofcourse, there may be intermediate structures being
 * used and the size of the result is not a priori known.
 * For this, we use a high watermark on the amount of
 * physical memory we pre-allocate for the claims.
 *
 * Instructions are eligible to be executed when the
 * total footprint of all concurrent executions stays below
 * the high-watermark or it is the single expensive
 * instruction being started.
 *
 * When we run out of memory, the instruction is delayed.
 * How long depends on the other instructions to free up
 * resources. The current policy simple takes a local
 * decision by delaying the instruction based on its
 * past and the size of the memory pool size.
 * The waiting penalty decreases with each step to ensure
 * it will ultimately taken into execution, with possibly
 * all resource contention effects.
 *
 * Another option would be to maintain a priority queue of
 * suspended instructions.
 */

/*
 * The memory claim is the estimate for the amount of memory hold.
 * Views are consider cheap and ignored
 */
lng
getMemoryClaim(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int i, int flag)
{
	lng total = 0, vol = 0;
	BAT *b;
	BUN cnt;

	(void)mb;
	if (stk->stk[getArg(pci, i)].vtype == TYPE_bat) {
		b = BATdescriptor( stk->stk[getArg(pci, i)].val.bval);
		if (b == NULL)
			return 0;
		if (flag && isVIEW(b)) {
			BBPunfix(b->batCacheid);
			return 0;
		}
		cnt = BATcount(b);

		heapinfo(&b->T->heap); total += vol;
		// string heaps can be shared, consider them as space-less views
		if ( b->T->vheap && b->T->vheap->parentid ){
			heapinfo(b->T->vheap); total += vol;
		}
		hashinfo(b->T->hash); total += vol;
		total = total > (lng)(MEMORY_THRESHOLD ) ? (lng)(MEMORY_THRESHOLD ) : total;
		BBPunfix(b->batCacheid);
	}
	return total;
}

/*
 * A consequence of multiple threads is that they may claim more
 * space than available. This may cause GDKmalloc to fail.
 * In many cases this situation will be temporary, because
 * threads will ultimately release resources.
 * Therefore, we wait for it.
 *
 * Alternatively, a front-end can set the flow administration
 * program counter to -1, which leads to a soft abort.
 * [UNFORTUNATELY this approach does not (yet) work
 * because there seem to a possibility of a deadlock
 * between incref and bbptrim. Furthermore, we have
 * to be assured that the partial executed instruction
 * does not lead to ref-count errors.]
 *
 * The worker produces a result which will potentially unblock
 * instructions. This it can find itself without the help of the scheduler
 * and without the need for a lock. (does it?, parallel workers?)
 * It could also give preference to an instruction that eats away the object
 * just produced. THis way it need not be saved on disk for a long time.
 */
/*
 * The hotclaim indicates the amount of data recentely written.
 * as a result of an operation. The argclaim is the sum over the hotclaims
 * for all arguments.
 * The argclaim provides a hint on how much we actually may need to execute
 * The hotclaim is a hint how large the result would be.
 */
#ifdef USE_MAL_ADMISSION
static MT_Lock admissionLock MT_LOCK_INITIALIZER("admissionLock");

/* experiments on sf-100 on small machine showed no real improvement */
int
MALadmission(lng argclaim, lng hotclaim)
{
	/* optimistically set memory */
	if (argclaim == 0)
		return 0;

	MT_lock_set(&admissionLock);
	if (memoryclaims < 0)
		memoryclaims = 0;
	if (memorypool <= 0 && memoryclaims == 0)
		memorypool = (lng)(MEMORY_THRESHOLD );

	if (argclaim > 0) {
		if (memoryclaims == 0 || memorypool > argclaim + hotclaim) {
			memorypool -= (argclaim + hotclaim);
			memoryclaims++;
			PARDEBUG
			mnstr_printf(GDKstdout, "#DFLOWadmit %3d thread %d pool " LLFMT "claims " LLFMT "," LLFMT "\n",
						 memoryclaims, THRgettid(), memorypool, argclaim, hotclaim);
			MT_lock_unset(&admissionLock);
			return 0;
		}
		PARDEBUG
		mnstr_printf(GDKstdout, "#Delayed due to lack of memory " LLFMT " requested " LLFMT " memoryclaims %d\n", memorypool, argclaim + hotclaim, memoryclaims);
		MT_lock_unset(&admissionLock);
		return -1;
	}
	/* release memory claimed before */
	memorypool += -argclaim - hotclaim;
	memoryclaims--;
	PARDEBUG
	mnstr_printf(GDKstdout, "#DFLOWadmit %3d thread %d pool " LLFMT " claims " LLFMT "," LLFMT "\n",
				 memoryclaims, THRgettid(), memorypool, argclaim, hotclaim);
	MT_lock_unset(&admissionLock);
	return 0;
}
#endif

/* Delay threads if too much competition arises and memory
 * becomes a scarce resource.
 * If in the mean time memory becomes free, or too many sleeping 
 * re-enable worker.
 * It may happen that all threads enter the wait state. So, keep
 * one running at all time 
 * By keeping the query start time in the client record we can delay
 * them when resource stress occurs.
 */
#include "gdk_atomic.h"
static volatile ATOMIC_TYPE running;
#ifdef ATOMIC_LOCK
static MT_Lock runningLock MT_LOCK_INITIALIZER("runningLock");
#endif

void
MALresourceFairness(lng usec)
{
	size_t rss;
	unsigned int delay;
	lng clk;
	int threads;
	int delayed= 0;

	/* use GDKmem_cursize as MT_getrss() is too expensive */
	rss = GDKmem_cursize();
	/* ample of memory available*/
	if ( rss < MEMORY_THRESHOLD && usec <= TIMESLICE)
		return;

	/* worker reporting time spent  in usec! */
	clk =  usec / 1000;

	if ( clk > DELAYUNIT ) {
		PARDEBUG mnstr_printf(GDKstdout, "#delay initial "LLFMT"n", clk);
		(void) ATOMIC_DEC(running, runningLock);
		/* always keep one running to avoid all waiting  */
		while (clk > 0 && running >= 2 && delayed < MAX_DELAYS) {
			/* speed up wake up when we have memory */
			if (rss < MEMORY_THRESHOLD )
				break;
			threads = GDKnr_threads > 0 ? GDKnr_threads : 1;
			delay = (unsigned int) ( ((double)DELAYUNIT * running) / threads);
			if (delay) {
				if ( delayed++ == 0){
						PARDEBUG mnstr_printf(GDKstdout, "#delay initial %u["LLFMT"] memory  "SZFMT"[%f]\n", delay, clk, rss, MEMORY_THRESHOLD );
						PARDEBUG mnstr_flush(GDKstdout);
				}
				MT_sleep_ms(delay);
				rss = GDKmem_cursize();
			} else break;
			clk -= DELAYUNIT;
		}
		(void) ATOMIC_INC(running, runningLock);
	}
}

void
initResource(void)
{
#ifdef NEED_MT_LOCK_INIT
	ATOMIC_INIT(runningLock);
#ifdef USE_MAL_ADMISSION
	MT_lock_init(&admissionLock, "admissionLock");
#endif
#endif
	running = (ATOMIC_TYPE) GDKnr_threads;
}
