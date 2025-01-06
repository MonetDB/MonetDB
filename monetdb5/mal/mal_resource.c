/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/* (author) M.L. Kersten
 */
#include "monetdb_config.h"
#include "mal_exception.h"
#include "mal_resource.h"
#include "mal_private.h"
#include "mal_internal.h"
#include "mal_instruction.h"

/* Memory based admission does not seem to have a major impact so far. */
static lng memorypool = 0;		/* memory claimed by concurrent threads */

static MT_Lock admissionLock = MT_LOCK_INITIALIZER(admissionLock);

void
mal_resource_reset(void)
{
	MT_lock_set(&admissionLock);
	memorypool = (lng) MEMORY_THRESHOLD;
	MT_lock_unset(&admissionLock);
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
 * of the operands and assuming it preferably should fit in memory.
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

	(void) mb;
	if (stk->stk[getArg(pci, i)].bat) {
		bat bid = stk->stk[getArg(pci, i)].val.bval;
		if (!BBPcheck(bid))
			return 0;
		b = BBP_desc(bid);
		MT_lock_set(&b->theaplock);
		if (flag && isVIEW(b)) {
			MT_lock_unset(&b->theaplock);
			return 0;
		}

		/* calculate the basic scan size */
		total += BATcount(b) << b->tshift;
		total += heapinfo(b->tvheap, b->batCacheid);
		MT_lock_unset(&b->theaplock);

		/* indices should help, find their maximum footprint */
		MT_rwlock_rdlock(&b->thashlock);
		itotal = hashinfo(b->thash, d->batCacheid);
		MT_rwlock_rdunlock(&b->thashlock);
		/* We should also consider the ordered index size */
		t = b->torderidx
				&& b->torderidx != (Heap *) 1 ? (lng) b->torderidx->free : 0;
		if (t > itotal)
			itotal = t;
		//total = total > (lng)(MEMORY_THRESHOLD ) ? (lng)(MEMORY_THRESHOLD ) : total;
		if (total < itotal)
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
bool
MALadmission_claim(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
				   lng argclaim)
{
	(void) pci;

	/* Check if we are allowed to allocate another worker thread for this client */
	/* It is somewhat tricky, because we may be in a dataflow recursion, each of which should be counted for.
	 * A way out is to attach the thread count to the MAL stacks, which just limits the level
	 * of parallelism for a single dataflow graph.
	 */
	if (cntxt->workerlimit > 0
		&& (int) ATOMIC_GET(&cntxt->workers) >= cntxt->workerlimit)
		return false;

	if (argclaim == 0)
		return true;

	MT_lock_set(&admissionLock);
	/* Determine if the total memory resource is exhausted, because it is overall limitation.  */
	if (memorypool <= 0) {
		// we accidentally released too much memory or need to initialize
		memorypool = (lng) MEMORY_THRESHOLD;
	}

	/* the argument claim is based on the input for an instruction */
	if (memorypool > argclaim || ATOMIC_GET(&cntxt->workers) == 0) {
		/* If we are low on memory resources, limit the user if he exceeds his memory budget
		 * but make sure there is at least one worker thread active */
		if (cntxt->memorylimit) {
			if (argclaim + stk->memory >
				(lng) cntxt->memorylimit * LL_CONSTANT(1048576)
				&& ATOMIC_GET(&cntxt->workers) > 0) {
				MT_lock_unset(&admissionLock);
				return false;
			}
			stk->memory += argclaim;
		}
		memorypool -= argclaim;
		stk->memory += argclaim;
		MT_lock_set(&mal_delayLock);
		if (mb->memory < stk->memory)
			mb->memory = stk->memory;
		MT_lock_unset(&mal_delayLock);
		MT_lock_unset(&admissionLock);
		return true;
	}
	MT_lock_unset(&admissionLock);
	return false;
}

void
MALadmission_release(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
					 lng argclaim)
{
	/* release memory claimed before */
	(void) cntxt;
	(void) mb;
	(void) pci;
	if (argclaim == 0)
		return;

	MT_lock_set(&admissionLock);
	if (cntxt->memorylimit) {
		stk->memory -= argclaim;
	}
	memorypool += argclaim;
	if (memorypool > (lng) MEMORY_THRESHOLD) {
		memorypool = (lng) MEMORY_THRESHOLD;
	}
	stk->memory -= argclaim;
	MT_lock_unset(&admissionLock);
	return;
}
