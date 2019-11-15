/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/* (author) M.L. Kersten 
 */
#include "monetdb_config.h"
#include "mal_resource.h"
#include "mal_private.h"

/* MEMORY admission does not seem to have a major impact so far. */
static lng memorypool = 0;      /* memory claimed by concurrent threads */
static int memoryclaims = 0;

void
mal_resource_reset(void)
{
	memorypool = 0;
	memoryclaims = 0;
}
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
 * Views are consider cheap and ignored.
 * Given that auxiliary structures are important for performance, 
 * we use their maximum as an indication of the memory footprint.
 * An alternative would be to focus solely on the base table cost.
 * (Good for a MSc study)
 */
lng
getMemoryClaim(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int i, int flag)
{
	lng total = 0, itotal = 0, t;
	BAT *b;

	(void)mb;
	if (stk->stk[getArg(pci, i)].vtype == TYPE_bat) {
		b = BATdescriptor( stk->stk[getArg(pci, i)].val.bval);
		if (b == NULL)
			return 0;
		if (flag && isVIEW(b)) {
			BBPunfix(b->batCacheid);
			return 0;
		}

		/* calculate the basic scan size */
		total += BATcount(b) * b->twidth;
		total += heapinfo(b->tvheap, b->batCacheid); 

		/* indices should help, find their maximum footprint */
		itotal = hashinfo(b->thash, d->batCacheid); 
		t = IMPSimprintsize(b);
		if( t > itotal)
			itotal = t;
		/* We should also consider the ordered index and mosaic */
		//total = total > (lng)(MEMORY_THRESHOLD ) ? (lng)(MEMORY_THRESHOLD ) : total;
		BBPunfix(b->batCacheid);
		if ( total < itotal)
			total = itotal;
	}
	return total;
}

/*
 * The argclaim provides a hint on how much we actually may need to execute
 *
 * The client context also keeps bounds on the memory claim/client.
 * Surpassing this bound may be a reason to not admit the instruction to proceed.
 */
static MT_Lock admissionLock = MT_LOCK_INITIALIZER("admissionLock");

/* experiments on sf-100 on small machine showed no real improvement */

int
MALadmission(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, lng argclaim)
{
	int workers;
	lng mbytes;

	(void) mb;
	(void) pci;
	/* optimistically set memory */
	if (argclaim == 0)
		return 0;
	/* if we are dealing with a check instruction, just continue */
	/* TOBEDONE */

	MT_lock_set(&admissionLock);
	/* Check if we are allowed to allocate another worker thread for this client */
	/* It is somewhat tricky, because we may be in a dataflow recursion, each of which should be counted for.
	 * A way out is to attach the thread count to the MAL stacks instead, which just limits the level
	 * of parallism for a single dataflow graph.
	 */
	workers = stk->workers;
	if( cntxt->workerlimit && cntxt->workerlimit <= workers){
		PARDEBUG
			fprintf(stderr, "#DFLOWadmit worker limit reached, %d <= %d\n", cntxt->workerlimit, workers);
		MT_lock_unset(&admissionLock);
		return -1;
	}
	/* Determine if the total memory resource is exhausted, because it is overall limitation.
	 */
	if ( memoryclaims < 0){
		PARDEBUG
			fprintf(stderr, "#DFLOWadmit memoryclaim reset ");
		memoryclaims = 0;
	}
	if ( memorypool <= 0 && memoryclaims == 0){
		PARDEBUG
			fprintf(stderr, "#DFLOWadmit memorypool reset ");
		memorypool = (lng) MEMORY_THRESHOLD;
	}

	/* the argument claim is based on the input for an instruction */
	if (argclaim > 0) {
		if ( memoryclaims == 0 || memorypool > argclaim ) {
			/* If we are low on memory resources, limit the user if he exceeds his memory budget 
			 * but make sure there is at least one thread active */
			if ( cntxt->memorylimit) {
				mbytes = (lng) cntxt->memorylimit * LL_CONSTANT(1048576);
				if (argclaim + stk->memory > mbytes){
					MT_lock_unset(&admissionLock);
					PARDEBUG
						fprintf(stderr, "#Delayed due to lack of session memory " LLFMT " requested "LLFMT"\n", 
								stk->memory, argclaim);
					return -1;
				}
			}
			memorypool -= argclaim;
			stk->memory += argclaim;
			memoryclaims++;
			PARDEBUG
				fprintf(stderr, "#DFLOWadmit %3d thread %d pool " LLFMT "claims " LLFMT "\n",
						 memoryclaims, THRgettid(), memorypool, argclaim);
			stk->workers++;
			MT_lock_unset(&admissionLock);
			return 0;
		}
		PARDEBUG
			fprintf(stderr, "#Delayed due to lack of memory " LLFMT " requested " LLFMT " memoryclaims %d\n", 
				memorypool, argclaim, memoryclaims);
		MT_lock_unset(&admissionLock);
		return -1;
	}
	
	/* return the session budget */
	if (cntxt->memorylimit) {
		PARDEBUG
			fprintf(stderr, "#Return memory to session budget " LLFMT "\n", stk->memory);
		stk->memory -= argclaim;
	}
	/* release memory claimed before */
	memorypool -= argclaim;
	memoryclaims--;
	stk->workers--;

	PARDEBUG
		fprintf(stderr, "#DFLOWadmit %3d thread %d pool " LLFMT " claims " LLFMT "\n",
				 memoryclaims, THRgettid(), memorypool, argclaim);
	MT_lock_unset(&admissionLock);
	return 0;
}
