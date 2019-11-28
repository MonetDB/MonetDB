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
	case TYPE_oid: output = wordaligned( MosaicBlkSize + 2 * sizeof(oid),oid); break;
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

void
MOSadvance_linear(MOStask task)
{
	task->start += MOSgetCnt(task->blk);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(bte),bte)); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(sht),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(int),int)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(oid),oid)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(lng),lng)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(hge),hge)); break;
#endif
	}
}

#define linear_base(TPE, TASK)       (*(DeltaTpe(TPE)*) (((char*) (TASK)->blk)+ MosaicBlkSize))
#define linear_step(TPE, TASK)  (*(DeltaTpe(TPE)*) (((char*) (TASK)->blk)+ MosaicBlkSize+ sizeof(TPE)))

void
MOSskip_linear(MOStask task)
{
	MOSadvance_linear( task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define Estimate(TPE)\
{\
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
}

// calculate the expected reduction using LINEAR in terms of elements compressed
str
MOSestimate_linear(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
{
	(void) previous;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: Estimate(bte); break;
	case TYPE_sht: Estimate(sht); break;
	case TYPE_int: Estimate(int); break;
	case TYPE_oid: Estimate(oid); break;
	case TYPE_lng: Estimate(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: Estimate(hge); break;
#endif
	}
	return MAL_SUCCEED;
}

// insert a series of values into the compressor block using linear.
#define LINEARcompress(TPE)\
{\
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

	//task->dst = ((char*) blk)+ MosaicBlkSize +  2 * sizeof(TPE);
void
MOScompress_linear(MOStask task, MosaicBlkRec* estimate)
{
	MOSsetTag(task->blk,MOSAIC_LINEAR);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: LINEARcompress(bte); break;
	case TYPE_sht: LINEARcompress(sht); break;
	case TYPE_int: LINEARcompress(int); break;
	case TYPE_oid: LINEARcompress(oid); break;
	case TYPE_lng: LINEARcompress(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: LINEARcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src
#define LINEARdecompress(TPE)\
{	DeltaTpe(TPE) val	= linear_base(TPE, task);\
	DeltaTpe(TPE) step	= linear_step(TPE, task);\
	BUN lim = MOSgetCnt(blk);\
	for(i = 0; i < lim; i++, val += step) {\
		((TPE*)task->src)[i] = (TPE) val;\
	}\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_linear(MOStask task)
{
	MosaicBlk blk =  task->blk;
	BUN i;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: LINEARdecompress(bte); break;
	case TYPE_sht: LINEARdecompress(sht); break;
	case TYPE_int: LINEARdecompress(int); break;
	case TYPE_oid: LINEARdecompress(oid); break;
	case TYPE_lng: LINEARdecompress(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: LINEARdecompress(hge); break;
#endif
	}
}

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
