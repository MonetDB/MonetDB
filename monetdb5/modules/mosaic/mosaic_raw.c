/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
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
MOSlayout_raw(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

		input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng); break;
	case TYPE_oid: output = wordaligned( MosaicBlkSize + sizeof(oid)* MOSgetCnt(blk),oid); break;
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

void
MOSadvance_raw(MOStask task)
{
	MosaicBlk blk = task->blk;

	task->start += MOSgetCnt(blk);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte)); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(oid)* MOSgetCnt(blk),oid)); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(flt)* MOSgetCnt(blk),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(dbl)* MOSgetCnt(blk),dbl)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(hge)* MOSgetCnt(blk),hge)); break;
#endif
	}
}

void
MOSskip_raw( MOStask task)
{
	MOSadvance_raw(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define Estimate(TPE)\
{\
	/*The raw compression technique is always applicable and only adds one item at a time.*/\
	current->compression_strategy.tag = MOSAIC_RAW;\
	current->is_applicable = true;\
	current->uncompressed_size += (BUN) sizeof(TPE);\
	unsigned int cnt = previous->compression_strategy.cnt;\
	if (previous->compression_strategy.tag == MOSAIC_RAW && cnt + 1 < (1 << CNT_BITS)) {\
		current->must_be_merged_with_previous = true;\
		cnt++;\
		current->compressed_size += sizeof(TPE);\
	}\
	else {\
		current->must_be_merged_with_previous = false;\
		cnt = 1;\
		current->compressed_size += wordaligned(MosaicBlkSize, TPE) + sizeof(TPE);\
	}\
	current->compression_strategy.cnt = cnt;\
}

str
MOSestimate_raw(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous) {

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


// append a series of values into the non-compressed block

#define RAWcompress(TYPE)\
{\
	TYPE *v = ((TYPE*)task->src) + task->start;\
	unsigned int cnt = estimate->cnt;\
	TYPE *d = (TYPE*)task->dst;\
	for(unsigned int i = 0; i<cnt; i++,v++){\
		*d++ = (TYPE) *v;\
	}\
	task->dst += sizeof(TYPE);\
	MOSsetCnt(blk,cnt);\
}

// rather expensive simple value non-compressed store
void
MOScompress_raw(MOStask task, MosaicBlkRec* estimate)
{
	MosaicBlk blk = (MosaicBlk) task->blk;

	MOSsetTag(blk,MOSAIC_RAW);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RAWcompress(bte); break;
	case TYPE_sht: RAWcompress(sht); break;
	case TYPE_int: RAWcompress(int); break;
	case TYPE_lng: RAWcompress(lng); break;
	case TYPE_oid: RAWcompress(oid); break;
	case TYPE_flt: RAWcompress(flt); break;
	case TYPE_dbl: RAWcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RAWcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src
#define RAWdecompress(TYPE)\
{ BUN lim = MOSgetCnt(blk); \
	for(i = 0; i < lim; i++) {\
	((TYPE*)task->src)[i] = ((TYPE*)compressed)[i]; \
	}\
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_raw(MOStask task)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	char *compressed;

	compressed = ((char*)blk) + MosaicBlkSize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RAWdecompress(bte); break;
	case TYPE_sht: RAWdecompress(sht); break;
	case TYPE_int: RAWdecompress(int); break;
	case TYPE_lng: RAWdecompress(lng); break;
	case TYPE_oid: RAWdecompress(oid); break;
	case TYPE_flt: RAWdecompress(flt); break;
	case TYPE_dbl: RAWdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RAWdecompress(hge); break;
#endif
	}
}

#define scan_loop_raw(TPE, CANDITER_NEXT, TEST) \
{\
    TPE *val= (TPE*) (((char*) task->blk) + MosaicBlkSize);\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        v = val[i];\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

MOSselect_SIGNATURE(raw, bte) { oid *o; BUN first,last; bte v; (void) v; first = task->start; last = first + MOSgetCnt(task->blk); bool nil = !task->bsrc->tnonil; o = task->lb; oid c = canditer_next(task->ci); while (!is_oid_nil(c) && c < first ) { c = canditer_next(task->ci); } if (is_oid_nil(c)) { return MAL_SUCCEED; } else if ( nil && anti){ if (task->ci->tpe == cand_dense) do { if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && !1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (is_bte_nil(v))
            *o++ = c;
    }
} } } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && 1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && !1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && 1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && 1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && !1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && !1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && 1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else { if( IS_NIL(bte, tl) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && (((hi && v <= th ) || (!hi && v < th )) == !1))
            *o++ = c;
    }
} } else if( IS_NIL(bte, th) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && (((li && v >= tl ) || (!li && v > tl )) == !1))
            *o++ = c;
    }
} } else if (tl == th){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && ((hi && v == th) == !1))
            *o++ = c;
    }
} } else{ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl ))) == !1))
            *o++ = c;
    }
} } }} while (0)
; else do { if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && !1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (is_bte_nil(v))
            *o++ = c;
    }
} } } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && 1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && !1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && 1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && 1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && !1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && !1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && 1) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else { if( IS_NIL(bte, tl) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && (((hi && v <= th ) || (!hi && v < th )) == !1))
            *o++ = c;
    }
} } else if( IS_NIL(bte, th) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && (((li && v >= tl ) || (!li && v > tl )) == !1))
            *o++ = c;
    }
} } else if (tl == th){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && ((hi && v == th) == !1))
            *o++ = c;
    }
} } else{ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl ))) == !1))
            *o++ = c;
    }
} } }} while (0)
; } else if ( !nil && anti){ if (task->ci->tpe == cand_dense) do { if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && !1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (is_bte_nil(v))
            *o++ = c;
    }
} } } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && 1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && !1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && 1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && 1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && !1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && !1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && 1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else { if( IS_NIL(bte, tl) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && (((hi && v <= th ) || (!hi && v < th )) == !1))
            *o++ = c;
    }
} } else if( IS_NIL(bte, th) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && (((li && v >= tl ) || (!li && v > tl )) == !1))
            *o++ = c;
    }
} } else if (tl == th){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && ((hi && v == th) == !1))
            *o++ = c;
    }
} } else{ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl ))) == !1))
            *o++ = c;
    }
} } }} while (0)
; else do { if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && !1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (is_bte_nil(v))
            *o++ = c;
    }
} } } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && 1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && !1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && 1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && 1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && !1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && !1) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && 1) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else { if( IS_NIL(bte, tl) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && (((hi && v <= th ) || (!hi && v < th )) == !1))
            *o++ = c;
    }
} } else if( IS_NIL(bte, th) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && (((li && v >= tl ) || (!li && v > tl )) == !1))
            *o++ = c;
    }
} } else if (tl == th){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && ((hi && v == th) == !1))
            *o++ = c;
    }
} } else{ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl ))) == !1))
            *o++ = c;
    }
} } }} while (0)
; } else if ( nil && !anti){ if (task->ci->tpe == cand_dense) do { if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && !0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (is_bte_nil(v))
            *o++ = c;
    }
} } } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && 0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && !0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && 0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && 0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && !0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && !0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && 0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else { if( IS_NIL(bte, tl) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && (((hi && v <= th ) || (!hi && v < th )) == !0))
            *o++ = c;
    }
} } else if( IS_NIL(bte, th) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && (((li && v >= tl ) || (!li && v > tl )) == !0))
            *o++ = c;
    }
} } else if (tl == th){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && ((hi && v == th) == !0))
            *o++ = c;
    }
} } else{ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl ))) == !0))
            *o++ = c;
    }
} } }} while (0)
; else do { if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && !0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (is_bte_nil(v))
            *o++ = c;
    }
} } } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && 0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && !0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && 0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && 0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && !0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && !0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && 0) { if(1) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else { if( IS_NIL(bte, tl) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && (((hi && v <= th ) || (!hi && v < th )) == !0))
            *o++ = c;
    }
} } else if( IS_NIL(bte, th) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && (((li && v >= tl ) || (!li && v > tl )) == !0))
            *o++ = c;
    }
} } else if (tl == th){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && ((hi && v == th) == !0))
            *o++ = c;
    }
} } else{ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(1 && IS_NIL(bte, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl ))) == !0))
            *o++ = c;
    }
} } }} while (0)
; } else if ( !nil && !anti){ if (task->ci->tpe == cand_dense) do { if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && !0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (is_bte_nil(v))
            *o++ = c;
    }
} } } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && 0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && !0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && 0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && 0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && !0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && !0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && 0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else { if( IS_NIL(bte, tl) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && (((hi && v <= th ) || (!hi && v < th )) == !0))
            *o++ = c;
    }
} } else if( IS_NIL(bte, th) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && (((li && v >= tl ) || (!li && v > tl )) == !0))
            *o++ = c;
    }
} } else if (tl == th){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && ((hi && v == th) == !0))
            *o++ = c;
    }
} } else{ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next_dense(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl ))) == !0))
            *o++ = c;
    }
} } }} while (0)
; else do { if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && !0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (is_bte_nil(v))
            *o++ = c;
    }
} } } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && li && hi && 0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && !0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( IS_NIL(bte, tl) && IS_NIL(bte, th) && !(li && hi) && 0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && 0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl == th && !(li && hi) && !0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && !0) { } else if ( !IS_NIL(bte, tl) && !IS_NIL(bte, th) && tl > th && 0) { if(0) { 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!is_bte_nil(v))
            *o++ = c;
    }
} } else 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (true)
            *o++ = c;
    }
} } else { if( IS_NIL(bte, tl) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && (((hi && v <= th ) || (!hi && v < th )) == !0))
            *o++ = c;
    }
} } else if( IS_NIL(bte, th) ){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && (((li && v >= tl ) || (!li && v > tl )) == !0))
            *o++ = c;
    }
} } else if (tl == th){ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && ((hi && v == th) == !0))
            *o++ = c;
    }
} } else{ 
{
    bte *val= (bte*) (((char*) task->blk) + MosaicBlkSize);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = canditer_next(task->ci)) {
        BUN i = (BUN) (c - first);
        v = val[i];
        /*TODO: change from control to data dependency.*/
        if (!(0 && IS_NIL(bte, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl ))) == !0))
            *o++ = c;
    }
} } }} while (0)
; } if ((c = canditer_peekprev(task->ci)) >= last) { (void) canditer_prev(task->ci); } MOSskip_raw(task); task->lb = o; return MAL_SUCCEED;}

MOSselect_DEF(raw, sht)
MOSselect_DEF(raw, int)
MOSselect_DEF(raw, lng)
MOSselect_DEF(raw, flt)
MOSselect_DEF(raw, dbl)
#ifdef HAVE_HGE
MOSselect_DEF(raw, hge)
#endif

#define projection_loop_raw(TPE, CANDITER_NEXT)\
{	TPE *rt;\
	rt = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CANDITER_NEXT(task->ci)) {\
		BUN i = (BUN) (o - first);\
		*bt++ = rt[i];\
		task->cnt++;\
	}\
}

MOSprojection_DEF(raw, bte)
MOSprojection_DEF(raw, sht)
MOSprojection_DEF(raw, int)
MOSprojection_DEF(raw, lng)
MOSprojection_DEF(raw, flt)
MOSprojection_DEF(raw, dbl)
#ifdef HAVE_HGE
MOSprojection_DEF(raw, hge)
#endif

#define join_raw_general(HAS_NIL, NIL_MATCHES, TPE)\
{	TPE *v, *w;\
	v = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(oo= (oid) first; first < last; first++, v++, oo++){\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, *v))) {continue;};\
		}\
		w = (TPE*) task->src;\
		for(n = task->stop, o = 0; n -- > 0; w++,o++) {\
			if (ARE_EQUAL(*w, *v, HAS_NIL, TPE)){\
				if( BUNappend(task->lbat, &oo, false)!= GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED)\
				throw(MAL,"mosaic.raw",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

#define join_raw(TPE) {\
	if( nil && nil_matches){\
		join_raw_general(true, true, TPE);\
	}\
	if( !nil && nil_matches){\
		join_raw_general(false, true, TPE);\
	}\
	if( nil && !nil_matches){\
		join_raw_general(true, false, TPE);\
	}\
	if( !nil && !nil_matches){\
		join_raw_general(false, false, TPE);\
	}\
}



str
MOSjoin_raw( MOStask task, bit nil_matches)
{
	BUN n,first,last;
	oid o, oo;
		// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_raw(bte); break;
		case TYPE_sht: join_raw(sht); break;
		case TYPE_int: join_raw(int); break;
		case TYPE_lng: join_raw(lng); break;
		case TYPE_oid: join_raw(oid); break;
		case TYPE_flt: join_raw(flt); break;
		case TYPE_dbl: join_raw(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_raw(hge); break;
#endif
	}
	MOSskip_raw(task);
	return MAL_SUCCEED;
}
