/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 */
MOSprojection_SIGNATURE(NAME, TPE)
{
	BUN first = task->start;
	BUN last = first + MOSgetCnt(task->blk);

	ASSERT_ALIGNMENT_BLOCK_HEADER(task->blk, NAME, TPE);

	TPE* bt= (TPE*) task->src;

	/* Advance the candidate iterator to the first element within
	 * the oid range of the current block.
	 */
	oid c = canditer_next(task->ci);
	while (!is_oid_nil(c) && c < first ) {
		c = canditer_next(task->ci);
	}

	if 		(is_oid_nil(c)) {
		/* Nothing left to scan.
		 * So we can signal the generic select function to stop now.
		 */
		return MAL_SUCCEED;
	}

	if (task->ci->tpe == cand_dense) {
		CONCAT2(projection_loop_, NAME)(TPE, canditer_next_dense);
	}
	else {
		CONCAT2(projection_loop_, NAME)(TPE, canditer_next);
	}
	task->src = (char*) bt;

	if ((c = canditer_peekprev(task->ci)) >= last) {
		/*Restore iterator if it went pass the end*/
		(void) canditer_prev(task->ci);
	}

	return MAL_SUCCEED;
}
