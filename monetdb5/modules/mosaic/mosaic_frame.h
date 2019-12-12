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

#ifndef _MOSAIC_FRAME_
#define _MOSAIC_FRAME_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"

bool MOStypes_frame(BAT* b);
mal_export void MOScreateframeDictionary(MOStask task);
mal_export void MOSlayout_frame_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSlayout_frame(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);

#define MosaicBlkHeader_DEF_frame(TPE)\
typedef struct {\
	MosaicBlkHdrGeneric base;\
	char bits;\
	TPE min;\
	BitVectorChunk bitvector; /*First chunk of bitvector to force correct alignment.*/\
} MOSBlockHeader_frame_##TPE;

ALGEBRA_INTERFACES_INTEGERS_ONLY(frame);
#define DO_OPERATION_ON_frame(OPERATION, TPE, ...) DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, frame, TPE, __VA_ARGS__)

#define MOScodevectorFrame(task, TPE) ((BitVector) &((MOSBlockHeaderTpe(frame, TPE)*) (task)->blk)->bitvector)

#define join_inner_loop_frame(TPE, HAS_NIL, RIGHT_CI_NEXT)\
{\
    MOSBlockHeaderTpe(frame, TPE)* parameters = (MOSBlockHeaderTpe(frame, TPE)*) ((task))->blk;\
	const TPE min =  parameters->min;\
	const BitVector base = MOScodevectorFrame(task, TPE);\
	const bte bits = parameters->bits;\
    for (oid ro = canditer_peekprev(task->ci); !is_oid_nil(ro) && ro < last; ro = RIGHT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (ro - first);\
		TPE rval = ADD_DELTA(TPE, min, getBitVector(base, i, bits));\
        IF_EQUAL_APPEND_RESULT(HAS_NIL, TPE);\
	}\
}

#endif /* _MOSAIC_FRAME_ */
