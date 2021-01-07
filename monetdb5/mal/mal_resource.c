/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* (author) M.L. Kersten
 */
#include "monetdb_config.h"
#include "mal_resource.h"
#include "mal_private.h"

/* Memory based admission does not seem to have a major impact so far. */
static lng memorypool = 0;      /* memory claimed by concurrent threads */

void
mal_resource_reset(void)
{
	memorypool = (lng) MEMORY_THRESHOLD;
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
 * claim of the memory.
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
static MT_Lock admissionLock = MT_LOCK_INITIALIZER(admissionLock);

int
MALadmission_claim(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, lng argclaim)
{
	(void) mb;
	(void) pci;
	if (argclaim == 0)
		return 0;

	MT_lock_set(&admissionLock);
	/* Check if we are allowed to allocate another worker thread for this client */
	/* It is somewhat tricky, because we may be in a dataflow recursion, each of which should be counted for.
	 * A way out is to attach the thread count to the MAL stacks, which just limits the level
	 * of parallism for a single dataflow graph.
	 */
	if(cntxt->workerlimit && cntxt->workerlimit < stk->workers){
		MT_lock_unset(&admissionLock);
		return -1;
	}
	/* Determine if the total memory resource is exhausted, because it is overall limitation.  */
	if ( memorypool <= 0){
		// we accidently released too much memory or need to initialize
		memorypool = (lng) MEMORY_THRESHOLD;
	}

	/* the argument claim is based on the input for an instruction */
	if ( memorypool > argclaim || stk->workers == 0 ) {
		/* If we are low on memory resources, limit the user if he exceeds his memory budget
		 * but make sure there is at least one worker thread active */
		if ( cntxt->memorylimit) {
			if (argclaim + stk->memory > (lng) cntxt->memorylimit * LL_CONSTANT(1048576)){
				MT_lock_unset(&admissionLock);
				return -1;
			}
			stk->memory += argclaim;
		}
		memorypool -= argclaim;
		stk->workers++;
		MT_lock_unset(&admissionLock);
		return 0;
	}
	MT_lock_unset(&admissionLock);
	return -1;
}

void
MALadmission_release(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, lng argclaim)
{
	/* release memory claimed before */
	(void) cntxt;
	(void) mb;
	(void) pci;
	if (argclaim == 0 )
		return;

	MT_lock_set(&admissionLock);
	if ( cntxt->memorylimit) {
		stk->memory -= argclaim;
	}
	memorypool += argclaim;
	if ( memorypool > (lng) MEMORY_THRESHOLD ){
		memorypool = (lng) MEMORY_THRESHOLD;
	}
	stk->workers--;
	MT_lock_unset(&admissionLock);
	return;
}
