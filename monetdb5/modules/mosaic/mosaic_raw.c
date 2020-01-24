/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 * Use a chunk that has not been compressed
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_raw.h"
#include "mosaic_private.h"

bool MOStypes_raw(BAT* b) {
	switch(b->ttype){
	case TYPE_bit: return true; // Will be mapped to bte
	case TYPE_bte: return true;
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
	case TYPE_flt: return true;
	case TYPE_dbl: return true;
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

void
MOSlayout_raw(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

		input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng); break;
	case TYPE_flt: output = wordaligned( MosaicBlkSize + sizeof(flt)* MOSgetCnt(blk),flt); break;
	case TYPE_dbl: output = wordaligned( MosaicBlkSize + sizeof(dbl)* MOSgetCnt(blk),dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + sizeof(hge)* MOSgetCnt(blk),hge); break;
#endif
	}
	if( BUNappend(btech, "raw blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

#define TPE bte
#include "mosaic_raw_template.h"
#undef TPE
#define TPE sht
#include "mosaic_raw_template.h"
#undef TPE
#define TPE int
#include "mosaic_raw_template.h"
#undef TPE
#define TPE lng
#include "mosaic_raw_template.h"
#undef TPE
#define TPE flt
#include "mosaic_raw_template.h"
#undef TPE
#define TPE dbl
#include "mosaic_raw_template.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_raw_template.h"
#undef TPE
#endif

#define scan_loop_raw(TPE, CI_NEXT, TEST) \
{\
    TPE *val= &GET_INIT_raw(task, TPE);\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CI_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        v = val[i];\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

#define NAME raw
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
#define TPE flt
#include "mosaic_select_template.h"
#undef TPE
#define TPE dbl
#include "mosaic_select_template.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_select_template.h"
#undef TPE
#endif
#undef NAME

#define projection_loop_raw(TPE, CI_NEXT)\
{	TPE *rt;\
	rt = &GET_INIT_raw(task, TPE);\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CI_NEXT(task->ci)) {\
		BUN i = (BUN) (o - first);\
		*bt++ = rt[i];\
		task->cnt++;\
	}\
}

#define NAME raw
#define TPE bte
#include "mosaic_projection_template.h"
#undef TPE
#define TPE sht
#include "mosaic_projection_template.h"
#undef TPE
#define TPE int
#include "mosaic_projection_template.h"
#undef TPE
#define TPE lng
#include "mosaic_projection_template.h"
#undef TPE
#define TPE flt
#include "mosaic_projection_template.h"
#undef TPE
#define TPE dbl
#include "mosaic_projection_template.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_projection_template.h"
#undef TPE
#endif
#undef NAME

#define outer_loop_raw(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
    TPE *vl;\
	vl = &GET_INIT_raw(task, TPE);\
    for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
        TPE lval = vl[lo-first];\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, lval))) {continue;};\
		}\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
}

MOSjoin_COUI_DEF(raw, bte)
MOSjoin_COUI_DEF(raw, sht)
MOSjoin_COUI_DEF(raw, int)
MOSjoin_COUI_DEF(raw, lng)
MOSjoin_COUI_DEF(raw, flt)
MOSjoin_COUI_DEF(raw, dbl)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(raw, hge)
#endif
