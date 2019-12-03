/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (c)2014 author Martin Kersten
 */

#ifndef _MOSAIC_LINEAR_
#define _MOSAIC_LINEAR_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"

bool MOStypes_linear(BAT* b);
mal_export void MOSlayout_linear(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_linear(MOStask task);
mal_export str  MOSestimate_linear(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_linear(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_linear(MOStask task);

ALGEBRA_INTERFACES_INTEGERS_ONLY(linear);
#define DO_OPERATION_ON_linear(OPERATION, TPE, ...) DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, linear, TPE, __VA_ARGS__)

#define linear_base(TPE, TASK)       (*(DeltaTpe(TPE)*) (((char*) (TASK)->blk)+ MosaicBlkSize))
#define linear_step(TPE, TASK)  (*(DeltaTpe(TPE)*) (((char*) (TASK)->blk)+ MosaicBlkSize+ sizeof(TPE)))

#define join_inner_loop_linear(TPE, HAS_NIL, RIGHT_CI_NEXT)\
{\
	DeltaTpe(TPE) offset	= linear_base(TPE, task) ;\
	DeltaTpe(TPE) step		= linear_step(TPE, task);\
    for (oid ro = canditer_peekprev(task->ci); !is_oid_nil(ro) && ro < last; ro = RIGHT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (ro - first);\
		TPE rval =  (TPE) (offset + (i * step));\
        IF_EQUAL_APPEND_RESULT(HAS_NIL, TPE);\
	}\
	MOSadvance_linear(task);\
}

#endif /* _MOSAIC_LINEAR_ */
