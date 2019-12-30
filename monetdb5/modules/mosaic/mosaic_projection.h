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
#ifndef _MOSAIC_PROJECTION_
#define _MOSAIC_PROJECTION_

#include "mosaic.h"

#define MOSprojection_SIGNATURE(NAME, TPE) \
str								\
MOSprojection_##NAME##_##TPE(MOStask task)\

#define MOSprojection_DEF(NAME, TPE)\
MOSprojection_SIGNATURE(NAME, TPE)\
{\
	BUN first = task->start;\
	BUN last = first + MOSgetCnt(task->blk);\
\
	ASSERT_ALIGNMENT_BLOCK_HEADER(task->blk, NAME, TPE);\
\
	TPE* bt= (TPE*) task->src;\
\
	/* Advance the candidate iterator to the first element within
	 * the oid range of the current block.
	 */\
	oid c = canditer_next(task->ci);\
	while (!is_oid_nil(c) && c < first ) {\
		c = canditer_next(task->ci);\
	}\
\
	if 		(is_oid_nil(c)) {\
		/* Nothing left to scan.
		 * So we can signal the generic select function to stop now.
		 */\
		return MAL_SUCCEED;\
	}\
\
	if (task->ci->tpe == cand_dense) {\
		projection_loop_##NAME(TPE, canditer_next_dense);\
	}\
	else {\
		projection_loop_##NAME(TPE, canditer_next);\
	}\
	task->src = (char*) bt;\
\
	if ((c = canditer_peekprev(task->ci)) >= last) {\
		/*Restore iterator if it went pass the end*/\
		(void) canditer_prev(task->ci);\
	}\
\
	return MAL_SUCCEED;\
}

#define do_projection(NAME, TPE, DUMMY_ARGUMENT)\
{\
	MOSprojection_##NAME##_##TPE(task);\
	MOSadvance_##NAME##_##TPE(task);\
}

#define MOSprojection_generic_DEF(TPE) \
static str MOSprojection_##TPE(MOStask task)\
{\
	while(task->start < task->stop ){\
		switch(MOSgetTag(task->blk)){\
		case MOSAIC_RLE:\
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_runlength\n");\
			DO_OPERATION_IF_ALLOWED(projection, runlength, TPE);\
			break;\
		case MOSAIC_CAPPED:\
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_capped\n");\
			DO_OPERATION_IF_ALLOWED(projection, capped, TPE);\
			break;\
		case MOSAIC_VAR:\
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_var\n");\
			DO_OPERATION_IF_ALLOWED(projection, var, TPE);\
			break;\
		case MOSAIC_FRAME:\
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_frame\n");\
			DO_OPERATION_IF_ALLOWED(projection, frame, TPE);\
			break;\
		case MOSAIC_DELTA:\
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_delta\n");\
			DO_OPERATION_IF_ALLOWED(projection, delta, TPE);\
			break;\
		case MOSAIC_PREFIX:\
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_prefix\n");\
			DO_OPERATION_IF_ALLOWED(projection, prefix, TPE);\
			break;\
		case MOSAIC_LINEAR:\
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_linear\n");\
			DO_OPERATION_IF_ALLOWED(projection, linear, TPE);\
			break;\
		case MOSAIC_RAW:\
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_raw\n");\
			DO_OPERATION_IF_ALLOWED(projection, raw, TPE);\
			break;\
		}\
\
		if (task->ci->next == task->ci->ncand) {\
			/* We are at the end of the candidate list.
			 * So we can stop now.
			 */\
			return MAL_SUCCEED;\
		}\
	}\
\
	return MAL_SUCCEED;\
}

#endif /* _MOSAIC_PROJECTION_ */
