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

#ifndef _MOSAIC_RLE_
#define _MOSAIC_RLE_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"

bool MOStypes_runlength(BAT* b);
mal_export void MOSlayout_runlength(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);

#define MosaicBlkHeader_DEF_runlength(TPE)\
typedef struct {\
	MosaicBlkRec rec;\
	char padding;\
	TPE val;\
} MOSBlockHeader_runlength_##TPE;

ALGEBRA_INTERFACES_ALL_TYPES(runlength);

#define DO_OPERATION_ON_runlength(OPERATION, TPE, ...) DO_OPERATION_ON_ALL_TYPES(OPERATION, runlength, TPE, __VA_ARGS__)

#define GET_VAL_runlength(task, TPE) (((MOSBlockHeaderTpe(runlength, TPE)*) (task)->blk)->val)

#define join_inner_loop_runlength(TPE, HAS_NIL, RIGHT_CI_NEXT)\
{\
    BUN first = task->start;\
    BUN last = first + MOSgetCnt(task->blk);\
    const TPE rval = GET_VAL_runlength(task, TPE);\
    for (oid ro = canditer_peekprev(task->ci); !is_oid_nil(ro) && ro < last; ro = RIGHT_CI_NEXT(task->ci)) {\
        IF_EQUAL_APPEND_RESULT(HAS_NIL, TPE);\
	}\
}

#endif /* _MOSAIC_RLE_ */
