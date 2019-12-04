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
mal_export void MOSadvance_frame(MOStask task);

ALGEBRA_INTERFACES_INTEGERS_ONLY(frame);
#define DO_OPERATION_ON_frame(OPERATION, TPE, ...) DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, frame, TPE, __VA_ARGS__)

typedef struct _FrameParameters_t {
	MosaicBlkRec base;
	int bits;
	union {
		bte minbte;
		sht minsht;
		int minint;
		lng minlng;
		oid minoid;
#ifdef HAVE_HGE
		hge minhge;
#endif
	} min;
	union {
		bte maxbte;
		sht maxsht;
		int maxint;
		lng maxlng;
		oid maxoid;
#ifdef HAVE_HGE
		hge maxhge;
#endif
	} max;

} MosaicBlkHeader_frame_t;

#define MOScodevectorFrame(Task) (((char*) (Task)->blk)+ wordaligned(sizeof(MosaicBlkHeader_frame_t), BitVectorChunk))

#define join_inner_loop_frame(TPE, HAS_NIL, RIGHT_CI_NEXT)\
{\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((task))->blk;\
	const TPE min =  parameters->min.min##TPE;\
	const BitVector base = (BitVector) MOScodevectorFrame(task);\
	const bte bits = parameters->bits;\
    for (oid ro = canditer_peekprev(task->ci); !is_oid_nil(ro) && ro < last; ro = RIGHT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (ro - first);\
		TPE rval = ADD_DELTA(TPE, min, getBitVector(base, i, bits));\
        IF_EQUAL_APPEND_RESULT(HAS_NIL, TPE);\
	}\
}

#endif /* _MOSAIC_FRAME_ */
