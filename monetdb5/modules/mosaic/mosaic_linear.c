/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
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
MOSlayout_linear(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
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

#define MOSadvance_DEF(TPE)\
MOSadvance_SIGNATURE(linear, TPE)\
{\
	task->start += MOSgetCnt(task->blk);\
	task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(TPE),TPE));\
}

MOSadvance_DEF(bte)
MOSadvance_DEF(sht)
MOSadvance_DEF(int)
MOSadvance_DEF(lng)
#ifdef HAVE_HGE
MOSadvance_DEF(hge)
#endif

void
MOSadvance_linear(MOStask task)
{
	// TODO: Not strictly necessary to split on type here since the logic remains the same.
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: MOSadvance_linear_bte(task); break;
	case TYPE_sht: MOSadvance_linear_sht(task); break;
	case TYPE_int: MOSadvance_linear_int(task); break;
	case TYPE_lng: MOSadvance_linear_lng(task); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOSadvance_linear_hge(task); break;
#endif
	}
}

#define MOSestimate_DEF(TPE) \
MOSestimate_SIGNATURE(linear, TPE)\
{\
	(void) previous;\
	current->compression_strategy.tag = MOSAIC_LINEAR;\
	TPE *c = ((TPE*) task->src)+task->start; /*(c)urrent value*/\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	BUN i = 1;\
	if (limit > 1 ){\
		TPE *p = c++; /*(p)revious value*/\
		DeltaTpe(TPE) step = GET_DELTA(TPE, *c, *p);\
		for( ; i < limit; i++, p++, c++) {\
			DeltaTpe(TPE) current_step = GET_DELTA(TPE, *c, *p);\
			if (  current_step != step)\
				break;\
		}\
	}\
	assert(i > 0);/*Should always compress.*/\
	current->is_applicable = true;\
	current->uncompressed_size += (BUN) (i * sizeof(TPE));\
	current->compressed_size += wordaligned( MosaicBlkSize + 2 * sizeof(TPE),TPE);\
	current->compression_strategy.cnt = (unsigned int) i;\
\
	return MAL_SUCCEED;\
}

MOSestimate_DEF(bte)
MOSestimate_DEF(sht)
MOSestimate_DEF(int)
MOSestimate_DEF(lng)
#ifdef HAVE_HGE
MOSestimate_DEF(hge)
#endif

#define MOSpostEstimate_DEF(TPE)\
MOSpostEstimate_SIGNATURE(linear, TPE)\
{\
	(void) task;\
}

MOSpostEstimate_DEF(bte)
MOSpostEstimate_DEF(sht)
MOSpostEstimate_DEF(int)
MOSpostEstimate_DEF(lng)
#ifdef HAVE_HGE
MOSpostEstimate_DEF(hge)
#endif

// rather expensive simple value non-compressed store
#define MOScompress_DEF(TPE)\
MOScompress_SIGNATURE(linear, TPE)\
{\
	MOSsetTag(task->blk,MOSAIC_LINEAR);\
	TPE *c = ((TPE*) task->src)+task->start; /*(c)urrent value*/\
	TPE step = 0;\
	BUN limit = estimate->cnt;\
	linear_base(TPE, task) = *(DeltaTpe(TPE)*) c;\
	if (limit > 1 ){\
		TPE *p = c++; /*(p)revious value*/\
		step = (TPE) GET_DELTA(TPE, *c, *p);\
	}\
	MOSsetCnt(task->blk, limit);\
	linear_step(TPE, task) = (DeltaTpe(TPE)) step;\
	task->dst = ((char*) task->blk)+ wordaligned(MosaicBlkSize +  2 * sizeof(TPE),MosaicBlkRec);\
}

MOScompress_DEF(bte)
MOScompress_DEF(sht)
MOScompress_DEF(int)
MOScompress_DEF(lng)
#ifdef HAVE_HGE
MOScompress_DEF(hge)
#endif

#define MOSdecompress_DEF(TPE) \
MOSdecompress_SIGNATURE(linear, TPE)\
{\
	MosaicBlk blk =  task->blk;\
	BUN i;\
	DeltaTpe(TPE) val	= linear_base(TPE, task);\
	DeltaTpe(TPE) step	= linear_step(TPE, task);\
	BUN lim = MOSgetCnt(blk);\
	for(i = 0; i < lim; i++, val += step) {\
		((TPE*)task->src)[i] = (TPE) val;\
	}\
	task->src += i * sizeof(TPE);\
}

MOSdecompress_DEF(bte)
MOSdecompress_DEF(sht)
MOSdecompress_DEF(int)
MOSdecompress_DEF(lng)
#ifdef HAVE_HGE
MOSdecompress_DEF(hge)
#endif

#define scan_loop_linear(TPE, CI_NEXT, TEST) {\
	DeltaTpe(TPE) offset	= linear_base(TPE, task);\
	DeltaTpe(TPE) step		= linear_step(TPE, task);\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CI_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        v = (TPE) (offset + (i * step));\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

MOSselect_DEF(linear, bte)
MOSselect_DEF(linear, sht)
MOSselect_DEF(linear, int)
MOSselect_DEF(linear, lng)
#ifdef HAVE_HGE
MOSselect_DEF(linear, hge)
#endif

#define projection_loop_linear(TPE, CI_NEXT)\
{\
	DeltaTpe(TPE) offset	= linear_base(TPE, task) ;\
	DeltaTpe(TPE) step		= linear_step(TPE, task);\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CI_NEXT(task->ci)) {\
        BUN i = (BUN) (o - first);\
		TPE value =  (TPE) (offset + (i * step));\
		*bt++ = value;\
		task->cnt++;\
	}\
}

MOSprojection_DEF(linear, bte)
MOSprojection_DEF(linear, sht)
MOSprojection_DEF(linear, int)
MOSprojection_DEF(linear, lng)
#ifdef HAVE_HGE
MOSprojection_DEF(linear, hge)
#endif

#define outer_loop_linear(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
	DeltaTpe(TPE) offset	= linear_base(TPE, task) ;\
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
