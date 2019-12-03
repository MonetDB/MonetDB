/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
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
MOSlayout_runlength(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte),bte); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int),int); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng),lng); break;
	case TYPE_oid: output = wordaligned( MosaicBlkSize + sizeof(oid),oid); break;
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

void
MOSadvance_runlength(MOStask task)
{

	task->start += MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(bte),bte)); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(sht),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(int),int)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(lng),lng)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(oid),oid)); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(flt),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(dbl),dbl)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(hge),hge)); break;
#endif
	}
}

#define Estimate(TPE)\
{	TPE *v = ((TPE*) task->src) + task->start, val = *v;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	for(v++,i = 1; i < limit; i++,v++) if ( !ARE_EQUAL(*v, val, nil, TPE) ) break;\
	assert(i > 0);/*Should always compress.*/\
	current->is_applicable = true;\
	current->uncompressed_size += (BUN) (i * sizeof(TPE));\
	current->compressed_size += wordaligned( MosaicBlkSize, TPE) + sizeof(TPE);\
	current->compression_strategy.cnt = i;\
}

// calculate the expected reduction using RLE in terms of elements compressed
str
MOSestimate_runlength(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
{	unsigned int i = 0;
	(void) previous;
	current->compression_strategy.tag = MOSAIC_RLE;
	bool nil = !task->bsrc->tnonil;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: Estimate(bte); break;
	case TYPE_sht: Estimate(sht); break;
	case TYPE_int: Estimate(int); break;
	case TYPE_lng: Estimate(lng); break;
	case TYPE_oid: Estimate(oid); break;
	case TYPE_flt: Estimate(flt); break;
	case TYPE_dbl: Estimate(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: Estimate(hge); break;
#endif
	}
	return MAL_SUCCEED;
}

// insert a series of values into the compressor block using rle.
#define RLEcompress(TPE)\
{	TPE *v = ((TPE*) task->src)+task->start, val = *v;\
	TPE *dst = (TPE*) task->dst;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT ? MOSAICMAXCNT: task->stop - task->start;\
	*dst = val;\
	for(v++, i =1; i<limit; i++,v++)\
	if ( !ARE_EQUAL(*v, val, nil, TPE))\
		break;\
	MOSsetCnt(blk, i);\
	task->dst +=  sizeof(TPE);\
}

void
MOScompress_runlength(MOStask task, MosaicBlkRec* estimate)
{
	(void) estimate;
	BUN i ;
	MosaicBlk blk = task->blk;
	bool nil = !task->bsrc->tnonil;

		MOSsetTag(blk, MOSAIC_RLE);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RLEcompress(bte); break;
	case TYPE_sht: RLEcompress(sht); break;
	case TYPE_int: RLEcompress(int); break;
	case TYPE_lng: RLEcompress(lng); break;
	case TYPE_oid: RLEcompress(oid); break;
	case TYPE_flt: RLEcompress(flt); break;
	case TYPE_dbl: RLEcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RLEcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src
#define RLEdecompress(TPE)\
{	TPE val = *(TPE*) compressed;\
	BUN lim = MOSgetCnt(blk);\
	for(i = 0; i < lim; i++)\
		((TPE*)task->src)[i] = val;\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_runlength(MOStask task)
{
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	char *compressed;

	compressed = (char*) blk + MosaicBlkSize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RLEdecompress(bte); break;
	case TYPE_sht: RLEdecompress(sht); break;
	case TYPE_int: RLEdecompress(int); break;
	case TYPE_lng: RLEdecompress(lng); break;
	case TYPE_oid: RLEdecompress(oid); break;
	case TYPE_flt: RLEdecompress(flt); break;
	case TYPE_dbl: RLEdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RLEdecompress(hge); break;
#endif
	}
}

#define scan_loop_runlength(TPE, CI_NEXT, TEST) \
{\
    v = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
    if (TEST) {\
        for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CI_NEXT(task->ci)) {\
		    *o++ = c;\
        }\
    }\
}

MOSselect_DEF(runlength, bte)
MOSselect_DEF(runlength, sht)
MOSselect_DEF(runlength, int)
MOSselect_DEF(runlength, lng)
MOSselect_DEF(runlength, flt)
MOSselect_DEF(runlength, dbl)
#ifdef HAVE_HGE
MOSselect_DEF(runlength, hge)
#endif

#define projection_loop_runlength(TPE, CI_NEXT)\
{\
	TPE rt = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
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
	const TPE lval = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
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
