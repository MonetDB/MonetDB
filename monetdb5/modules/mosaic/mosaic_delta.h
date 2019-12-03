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

#ifndef _MOSAIC_DELTA_
#define _MOSAIC_DELTA_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"

bool MOStypes_delta(BAT* b);
mal_export void MOSlayout_delta(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_delta(MOStask task);
mal_export str  MOSestimate_delta(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_delta(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_delta(MOStask task);

ALGEBRA_INTERFACES_INTEGERS_ONLY(delta);
#define DO_OPERATION_ON_delta(OPERATION, TPE, ...) DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, delta, TPE, __VA_ARGS__)

typedef struct _DeltaParameters_t {
	MosaicBlkRec base;
	int bits;
	union {
		bte valbte;
		sht valsht;
		int valint;
		lng vallng;
		oid valoid;
#ifdef HAVE_HGE
		hge valhge;
#endif
	} init;
} MosaicBlkHeader_delta_t;

#define MOScodevectorDelta(Task) (((char*) (Task)->blk)+ wordaligned(sizeof(MosaicBlkHeader_delta_t), BitVectorChunk))

#define ACCUMULATE(acc, delta, sign_mask, TPE) \
( /*code assumes that acc is of type DeltaTpe(TPE)*/\
	(TPE) (\
		( (sign_mask) & (delta) )?\
			((acc) -= (DeltaTpe(TPE)) (~(IPTpe(TPE)) (sign_mask) & (IPTpe(TPE)) (delta))) :\
			((acc) += (DeltaTpe(TPE)) (~(IPTpe(TPE)) (sign_mask) & (IPTpe(TPE)) (delta)))  \
	)\
)

#define join_inner_loop_delta(TPE, HAS_NIL, RIGHT_CI_NEXT)\
{\
	MosaicBlkHeader_delta_t* parameters = (MosaicBlkHeader_delta_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorDelta(task);\
	DeltaTpe(TPE) acc = parameters->init.val##TPE; /*previous value*/\
	int bits = parameters->bits;\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);\
    TPE rval = (TPE) acc;\
    BUN j = 0;\
    for (oid ro = canditer_peekprev(task->ci); !is_oid_nil(ro) && ro < last; ro = RIGHT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (ro - first);\
        for (;j <= i; j++) {\
            TPE delta = getBitVector(base, j, bits);\
			rval = ACCUMULATE(acc, delta, sign_mask, TPE);\
        }\
        IF_EQUAL_APPEND_RESULT(HAS_NIL, TPE);\
	}\
	MOSadvance_delta(task);\
}

#endif /* _MOSAIC_DELTA_ */
