/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 * Linear encoding
 * Replace a well-behaving series by its [start,step] value.
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_linear.h"
#include "mosaic_private.h"

bool MOStypes_linear(BAT* b) {
	switch(b->ttype){
	case TYPE_bit: return true;
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

void
MOSlayout_linear(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + 2 * sizeof(bte),bte); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + 2 * sizeof(sht),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + 2 * sizeof(int),int); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + 2 * sizeof(lng),lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + 2 * sizeof(hge),hge); break;
#endif
	}
	if( BUNappend(btech, "linear blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED )
		return;
}

#define COMPRESSION_DEFINITION
#define TPE bte
#include "mosaic_linear_templates.h"
#undef TPE
#define TPE sht
#include "mosaic_linear_templates.h"
#undef TPE
#define TPE int
#include "mosaic_linear_templates.h"
#undef TPE
#define TPE lng
#include "mosaic_linear_templates.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_linear_templates.h"
#undef TPE
#endif
#undef COMPRESSION_DEFINITION

#define NAME linear
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

#define projection_loop_linear(TPE, CI_NEXT)\
{\
	DeltaTpe(TPE) offset	= linear_offset(TPE, task) ;\
	DeltaTpe(TPE) step		= linear_step(TPE, task);\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CI_NEXT(task->ci)) {\
        BUN i = (BUN) (o - first);\
		TPE value =  (TPE) (offset + (i * step));\
		*bt++ = value;\
		task->cnt++;\
	}\
}

#define NAME linear
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
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_projection_template.h"
#undef TPE
#endif
#undef NAME

#define outer_loop_linear(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
	DeltaTpe(TPE) offset	= linear_offset(TPE, task) ;\
	DeltaTpe(TPE) step		= linear_step(TPE, task);\
    for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (lo - first);\
		TPE lval =  (TPE) (offset + (i * step));\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, lval))) {continue;};\
		}\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
}

MOSjoin_COUI_DEF(linear, bte)
MOSjoin_COUI_DEF(linear, sht)
MOSjoin_COUI_DEF(linear, int)
MOSjoin_COUI_DEF(linear, lng)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(linear, hge)
#endif
