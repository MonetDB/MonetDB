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

#define CAND_ITER canditer_next_dense
#include "mosaic_select_template2.h"
#undef CAND_ITER

#define CAND_ITER canditer_next
#include "mosaic_select_template2.h"
#undef CAND_ITER

#define select_general(HAS_NIL, ANTI, CAND_ITER) CONCAT6(select_general_, NAME, _, TPE, _, CAND_ITER)(HAS_NIL, ANTI, task, first, last, tl, th, li, hi)

/* definition of type-specific core scan select function */
MOSselect_SIGNATURE(NAME, TPE) {
	BUN first,last;

	ASSERT_ALIGNMENT_BLOCK_HEADER(task->blk, NAME, TPE);

	/* set the oid range covered and advance scan range*/
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

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
	else if	( nil && anti){
		if (task->ci->tpe == cand_dense)
            select_general(true, true, canditer_next_dense);
		else
            select_general(true, true, canditer_next);
	}
	else if	( !nil && anti){
		if (task->ci->tpe == cand_dense)
            select_general(false, true, canditer_next_dense);
		else
            select_general(false, true, canditer_next);
	}
	else if	( nil && !anti){
		if (task->ci->tpe == cand_dense)
            select_general(true, false, canditer_next_dense);
		else
            select_general(true, false, canditer_next);
	}
	else if	( !nil && !anti){
		if (task->ci->tpe == cand_dense)
            select_general(false, false, canditer_next_dense);
		else
            select_general(false, false, canditer_next);
	}

	if ((c = canditer_peekprev(task->ci)) >= last) {
		/*Restore iterator if it went pass the end*/
		(void) canditer_prev(task->ci);
	}

	return MAL_SUCCEED;
}

#undef select_general
