/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 * Run-length encoding framework for a single chunk
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_runlength.h"
#include "mosaic_private.h"

bool MOStypes_runlength(BAT* b) {
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
MOSlayout_runlength(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte),bte); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int),int); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng),lng); break;
	case TYPE_flt: output = wordaligned( MosaicBlkSize + sizeof(flt),flt); break;
	case TYPE_dbl: output = wordaligned( MosaicBlkSize + sizeof(dbl),dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + sizeof(hge),hge); break;
#endif
	}
	if( BUNappend(btech, "runlength blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED )
		return;
}

#define MOSadvance_DEF(TPE)\
MOSadvance_SIGNATURE(runlength, TPE)\
{\
	task->start += MOSgetCnt(task->blk);\
\
	char* blk = (char*)task->blk;\
	blk += sizeof(MOSBlockHeaderTpe(runlength, TPE));\
	blk += GET_PADDING(task->blk, runlength, TPE);\
\
	task->blk = (MosaicBlk) blk;\
}

MOSadvance_DEF(bte)
MOSadvance_DEF(sht)
MOSadvance_DEF(int)
MOSadvance_DEF(lng)
MOSadvance_DEF(flt)
MOSadvance_DEF(dbl)
#ifdef HAVE_HGE
MOSadvance_DEF(hge)
#endif

#define MOSestimate_DEF(TPE) \
MOSestimate_SIGNATURE(runlength, TPE)\
{	unsigned int i = 0;\
	(void) previous;\
	current->compression_strategy.tag = MOSAIC_RLE;\
	bool nil = !task->bsrc->tnonil;\
	TPE *v = ((TPE*) task->src) + task->start, val = *v;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	for(v++,i = 1; i < limit; i++,v++) if ( !ARE_EQUAL(*v, val, nil, TPE) ) break;\
	assert(i > 0);/*Should always compress.*/\
	current->is_applicable = true;\
	current->uncompressed_size += (BUN) (i * sizeof(TPE));\
	current->compressed_size += 2 * sizeof(MOSBlockHeaderTpe(runlength, TPE));\
	current->compression_strategy.cnt = i;\
\
	if (i > *current->max_compression_length ) *current->max_compression_length = i;\
\
	return MAL_SUCCEED;\
}

MOSestimate_DEF(bte)
MOSestimate_DEF(sht)
MOSestimate_DEF(int)
MOSestimate_DEF(lng)
MOSestimate_DEF(flt)
MOSestimate_DEF(dbl)
#ifdef HAVE_HGE
MOSestimate_DEF(hge)
#endif

#define MOSpostEstimate_DEF(TPE)\
MOSpostEstimate_SIGNATURE(runlength, TPE)\
{\
	(void) task;\
}

MOSpostEstimate_DEF(bte)
MOSpostEstimate_DEF(sht)
MOSpostEstimate_DEF(int)
MOSpostEstimate_DEF(lng)
MOSpostEstimate_DEF(flt)
MOSpostEstimate_DEF(dbl)
#ifdef HAVE_HGE
MOSpostEstimate_DEF(hge)
#endif

// rather expensive simple value non-compressed store
#define MOScompress_DEF(TPE)\
MOScompress_SIGNATURE(runlength, TPE)\
{\
	ALIGN_BLOCK_HEADER(task,  runlength, TPE);\
\
	(void) estimate;\
	BUN i ;\
	bool nil = !task->bsrc->tnonil;\
\
	MosaicBlk blk = task->blk;\
	MOSsetTag(blk, MOSAIC_RLE);\
	TPE *v = ((TPE*) task->src)+task->start, val = *v;\
	TPE *dst = &GET_VAL_runlength(task, TPE);\
	BUN limit = task->stop - task->start > MOSAICMAXCNT ? MOSAICMAXCNT: task->stop - task->start;\
	*dst = val;\
	for(v++, i =1; i<limit; i++,v++)\
	if ( !ARE_EQUAL(*v, val, nil, TPE))\
		break;\
	MOSsetCnt(blk, i);\
	task->dst +=  sizeof(TPE);\
}

MOScompress_DEF(bte)
MOScompress_DEF(sht)
MOScompress_DEF(int)
MOScompress_DEF(lng)
MOScompress_DEF(flt)
MOScompress_DEF(dbl)
#ifdef HAVE_HGE
MOScompress_DEF(hge)
#endif

#define MOSdecompress_DEF(TPE) \
MOSdecompress_SIGNATURE(runlength, TPE)\
{\
	TPE val = GET_VAL_runlength(task, TPE);\
	BUN lim = MOSgetCnt(task->blk);\
\
	BUN i;\
	for(i = 0; i < lim; i++)\
		((TPE*)task->src)[i] = val;\
	task->src += i * sizeof(TPE);\
}

MOSdecompress_DEF(bte)
MOSdecompress_DEF(sht)
MOSdecompress_DEF(int)
MOSdecompress_DEF(lng)
MOSdecompress_DEF(flt)
MOSdecompress_DEF(dbl)
#ifdef HAVE_HGE
MOSdecompress_DEF(hge)
#endif

#define scan_loop_runlength(TPE, CI_NEXT, TEST) \
{\
    v = GET_VAL_runlength(task, TPE);\
    if (TEST) {\
        for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CI_NEXT(task->ci)) {\
		    *o++ = c;\
        }\
    }\
}

#define NAME runlength
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

#define projection_loop_runlength(TPE, CI_NEXT)\
{\
	TPE rt = GET_VAL_runlength(task, TPE);\
	for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CI_NEXT(task->ci)) {\
		*bt++ = rt;\
		task->cnt++;\
	}\
}

MOSprojection_DEF(runlength, bte)
MOSprojection_DEF(runlength, sht)
MOSprojection_DEF(runlength, int)
MOSprojection_DEF(runlength, lng)
MOSprojection_DEF(runlength, flt)
MOSprojection_DEF(runlength, dbl)
#ifdef HAVE_HGE
MOSprojection_DEF(runlength, hge)
#endif

#define outer_loop_runlength(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
do {\
	const TPE lval = GET_VAL_runlength(task, TPE);\
	if (HAS_NIL && !NIL_MATCHES) {\
		if (IS_NIL(TPE, lval)) { break;}\
	}\
    for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
} while (0)

MOSjoin_COUI_DEF(runlength, bte)
MOSjoin_COUI_DEF(runlength, sht)
MOSjoin_COUI_DEF(runlength, int)
MOSjoin_COUI_DEF(runlength, lng)
MOSjoin_COUI_DEF(runlength, flt)
MOSjoin_COUI_DEF(runlength, dbl)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(runlength, hge)
#endif
