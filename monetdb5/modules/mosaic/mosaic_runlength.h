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

#ifndef _MOSAIC_RLE_
#define _MOSAIC_RLE_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"

bool MOStypes_runlength(BAT* b);
mal_export void MOSlayout_runlength(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_runlength(MOStask task);
mal_export str  MOSestimate_runlength(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_runlength(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_runlength(MOStask task);

ALGEBRA_INTERFACES_ALL_TYPES(runlength);

#define DO_OPERATION_ON_runlength(OPERATION, TPE, ...) DO_OPERATION_ON_ALL_TYPES(OPERATION, runlength, TPE, __VA_ARGS__)

#define join_inner_loop_runlength(TPE, HAS_NIL, RIGHT_CI_NEXT)\
{\
    BUN first = task->start;\
    BUN last = first + MOSgetCnt(task->blk);\
    const TPE rval = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
    for (oid ro = canditer_peekprev(task->ci); !is_oid_nil(ro) && ro < last; ro = RIGHT_CI_NEXT(task->ci)) {\
        IF_EQUAL_APPEND_RESULT(HAS_NIL, TPE);\
	}\
	MOSadvance_runlength(task);\
}

#endif /* _MOSAIC_RLE_ */
