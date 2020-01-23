/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 * Byte-wise delta encoding for SHT,INT,LNG, OID, WRD, STR-offsets, TIMESTAMP
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_delta.h"
#include "mosaic_private.h"

//#define _DEBUG_MOSAIC_

bool MOStypes_delta(BAT* b) {
	switch(b->ttype) {
	case TYPE_bit: return true; // Will be mapped to bte
	case TYPE_bte: return true;
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
#ifdef HAVE_HGE
	case TYPE_hge: return true;
#endif
	default:
		if (b->ttype == TYPE_date) {return true;} // Will be mapped to int
		if (b->ttype == TYPE_daytime) {return true;} // Will be mapped to lng
		if (b->ttype == TYPE_timestamp) {return true;} // Will be mapped to lng
	}

	return false;
}
#define BitVectorSize(CNT, BITS) wordaligned(((CNT) * (BITS) / CHAR_BIT) + ( ((CNT) * (BITS)) % CHAR_BIT != 0 ), BitVectorChunk)

void
MOSlayout_delta(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned(MosaicBlkSize + sizeof(bte) + MOSgetCnt(blk)-1,bte); break ;
	case TYPE_sht: output = wordaligned(MosaicBlkSize + sizeof(sht) + MOSgetCnt(blk)-1,sht); break ;
	case TYPE_int: output = wordaligned(MosaicBlkSize + sizeof(int) + MOSgetCnt(blk)-1,int); break ;
	case TYPE_lng: output = wordaligned(MosaicBlkSize + sizeof(lng) + MOSgetCnt(blk)-1,lng); break ;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned(MosaicBlkSize + sizeof(hge) + MOSgetCnt(blk)-1,hge); break ;
#endif
	}
	if( BUNappend(btech, "delta", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

#define TPE bte
#include "mosaic_delta_template.h"
#undef TPE
#define TPE sht
#include "mosaic_delta_template.h"
#undef TPE
#define TPE int
#include "mosaic_delta_template.h"
#undef TPE
#define TPE lng
#include "mosaic_delta_template.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_delta_template.h"
#undef TPE
#endif

#define scan_loop_delta(TPE, CANDITER_NEXT, TEST) \
{\
	MOSBlockHeaderTpe(delta, TPE)* parameters = (MOSBlockHeaderTpe(delta, TPE)*) task->blk;\
	BitVector base = (BitVector) MOScodevectorDelta(task, TPE);\
	DeltaTpe(TPE) acc = (DeltaTpe(TPE)) parameters->init; /*previous value*/\
	const bte bits = parameters->bits;\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);\
    v = (TPE) acc;\
    BUN j = 0;\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        for (;j <= i; j++) {\
            TPE delta = getBitVector(base, j, bits);\
			v = ACCUMULATE(acc, delta, sign_mask, TPE);\
        }\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

#define NAME delta
#define TPE bte
#include "mosaic_select_template.h"
#undef TPE
#define TPE sht
#include "mosaic_select_template.h"
#undef TPE
#define TPE int
#include "mosaic_select_template.h"
#undef TPE
#define TPE lng
#include "mosaic_select_template.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_select_template.h"
#undef TPE
#endif
#undef NAME

#define projection_loop_delta(TPE, CANDITER_NEXT)\
{\
	MOSBlockHeaderTpe(delta, TPE)* parameters = (MOSBlockHeaderTpe(delta, TPE)*) task->blk;\
	BitVector base = (BitVector) MOScodevectorDelta(task, TPE);\
	DeltaTpe(TPE) acc = (DeltaTpe(TPE)) parameters->init; /*previous value*/\
	const bte bits = parameters->bits;\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);\
    TPE v = (TPE) acc;\
    BUN j = 0;\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (o - first);\
        for (;j <= i; j++) {\
            TPE delta = getBitVector(base, j, bits);\
			v = ACCUMULATE(acc, delta, sign_mask, TPE);\
        }\
		*bt++ = v;\
		task->cnt++;\
	}\
}

MOSprojection_DEF(delta, bte)
MOSprojection_DEF(delta, sht)
MOSprojection_DEF(delta, int)
MOSprojection_DEF(delta, lng)
#ifdef HAVE_HGE
MOSprojection_DEF(delta, hge)
#endif

#define outer_loop_delta(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
	MOSBlockHeaderTpe(delta, TPE)* parameters = (MOSBlockHeaderTpe(delta, TPE)*) task->blk;\
	BitVector base = (BitVector) MOScodevectorDelta(task, TPE);\
	DeltaTpe(TPE) acc = (DeltaTpe(TPE)) parameters->init; /*previous value*/\
	const bte bits = parameters->bits;\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);\
    TPE lval = (TPE) acc;\
    BUN j = 0;\
    for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (lo - first);\
        for (;j <= i; j++) {\
            TPE delta = getBitVector(base, j, bits);\
			lval = ACCUMULATE(acc, delta, sign_mask, TPE);\
        }\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, lval))) {continue;};\
		}\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
}

MOSjoin_COUI_DEF(delta, bte)
MOSjoin_COUI_DEF(delta, sht)
MOSjoin_COUI_DEF(delta, int)
MOSjoin_COUI_DEF(delta, lng)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(delta, hge)
#endif
