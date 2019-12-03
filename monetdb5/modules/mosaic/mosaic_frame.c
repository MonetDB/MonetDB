/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 *2014-2016 author Martin Kersten
 * Frame of reference compression with dictionary
 * A codevector chunk is beheaded by a reference value F from the column. The elements V in the
 * chunk are replaced by an index into a global dictionary of V-F offsets.
 *
 * The dictionary is limited to 256 entries and all indices are at most one byte.
 * The maximal achievable compression ratio depends on the size of the dictionary
 *
 * This scheme is particularly geared at evolving time series, e.g. stock markets.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_frame.h"
#include "mosaic_private.h"

#include <stdint.h>

bool MOStypes_frame(BAT* b) {
	switch (b->ttype){
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

#define toEndOfBitVector(CNT, BITS) wordaligned(((CNT) * (BITS) / CHAR_BIT) + ( ((CNT) * (BITS)) % CHAR_BIT != 0 ), lng)

#define MOSadvance_DEF(TPE)\
MOSadvance_SIGNATURE(frame, TPE)\
{\
	MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) (task)->blk;\
	int *dst = (int*)  (((char*) task->blk) + wordaligned(sizeof(MosaicBlkHeader_frame_t), BitVectorChunk));\
	long cnt = parameters->base.cnt;\
	long bytes = toEndOfBitVector(cnt, parameters->bits);\
\
	assert(cnt > 0);\
	task->start += (oid) cnt;\
	task->blk = (MosaicBlk) (((char*) dst)  + bytes);\
}

MOSadvance_DEF(bte)
MOSadvance_DEF(sht)
MOSadvance_DEF(int)
MOSadvance_DEF(lng)
#ifdef HAVE_HGE
MOSadvance_DEF(hge)
#endif

void
MOSadvance_frame(MOStask task)
{
	// TODO: Not strictly necessary to split on type here since the logic remains the same.
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: MOSadvance_frame_bte(task); break;
	case TYPE_sht: MOSadvance_frame_sht(task); break;
	case TYPE_int: MOSadvance_frame_int(task); break;
	case TYPE_lng: MOSadvance_frame_lng(task); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOSadvance_frame_hge(task); break;
#endif
	}
}

void
MOSlayout_frame(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	(void) boutput;
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0;

	input = cnt * ATOMsize(task->type);
	if( BUNappend(btech, "frame blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

#define determineFrameParameters(PARAMETERS, SRC, LIMIT, TPE) \
do {\
	TPE *val = SRC, max, min;\
	int bits = 1;\
	unsigned int i;\
	max = *val;\
	min = *val;\
	/*TODO: add additional loop to find best bit wise upper bound*/\
	for(i = 0; i < LIMIT; i++, val++){\
		TPE current_max = max;\
		TPE current_min = min;\
		bool evaluate_bits = false;\
		if (*val > current_max) {\
			current_max = *val;\
			evaluate_bits = true;\
		}\
		if (*val < current_min) {\
			current_min = *val;\
			evaluate_bits = true;\
		}\
		if (evaluate_bits) {\
		 	DeltaTpe(TPE) width = GET_DELTA(TPE, current_max, current_min);\
			int current_bits = bits;\
			while (width > ((DeltaTpe(TPE))(-1)) >> (sizeof(DeltaTpe(TPE)) * CHAR_BIT - current_bits) ) {/*keep track of number of BITS necessary to store difference*/\
				current_bits++;\
			}\
			if ( (current_bits >= (int) ((sizeof(TPE) * CHAR_BIT) / 2))\
				/*TODO: this extra condition should be removed once bitvector is extended to int64's*/\
				|| (current_bits > (int) sizeof(BitVectorChunk) * CHAR_BIT) ) {\
				/*If we can from here on not compress better then the half of the original data type, we give up. */\
				break;\
			}\
			max = current_max;\
			min = current_min;\
			bits = current_bits;\
		}\
	}\
	(PARAMETERS).min.min##TPE = min;\
	(PARAMETERS).max.max##TPE = max;\
	(PARAMETERS).bits = bits;\
	(PARAMETERS).base.cnt = i;\
} while(0)

#define estimateFrame(TASK, TPE)\
do {\
	TPE *src = getSrc(TPE, (TASK));\
	BUN limit = (TASK)->stop - (TASK)->start > MOSAICMAXCNT? MOSAICMAXCNT: (TASK)->stop - (TASK)->start;\
	MosaicBlkHeader_frame_t parameters;\
	determineFrameParameters(parameters, src, limit, TPE);\
	assert(parameters.base.cnt > 0);/*Should always compress.*/\
	current->uncompressed_size += (BUN) (parameters.base.cnt * sizeof(TPE));\
	current->compressed_size += wordaligned(sizeof(MosaicBlkHeader_frame_t), lng) + wordaligned((parameters.base.cnt * parameters.bits) / CHAR_BIT, lng);\
	current->compression_strategy.cnt = (unsigned int) parameters.base.cnt;\
} while (0)

// calculate the expected reduction using dictionary in terms of elements compressed
str
MOSestimate_frame(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous) {
	(void) previous;
	current->is_applicable = true;
	current->compression_strategy.tag = MOSAIC_FRAME;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: estimateFrame(task, bte); break;
	case TYPE_sht: estimateFrame(task, sht); break;
	case TYPE_int: estimateFrame(task, int); break;
	case TYPE_lng: estimateFrame(task, lng); break;
	case TYPE_oid: estimateFrame(task, oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateFrame(task, hge); break;
#endif
	}

	return MAL_SUCCEED;
}

#define FRAMEcompress(TASK, TPE)\
do {\
	TPE *src = getSrc(TPE, (TASK));\
	TPE delta;\
	BUN i = 0;\
	BUN limit = estimate->cnt;\
	BitVector base;\
	MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((TASK))->blk;\
	determineFrameParameters(*parameters, src, limit, TPE);\
	(TASK)->dst = MOScodevectorFrame(TASK);\
	base = (BitVector) ((TASK)->dst);\
	for(i = 0; i < parameters->base.cnt; i++, src++) {\
		/*TODO: assert that delta's actually does not cause an overflow. */\
		delta = *src - parameters->min.min##TPE;\
		setBitVector(base, i, parameters->bits, (BitVectorChunk) /*TODO: fix this once we have increased capacity of bitvector*/ delta);\
	}\
	(TASK)->dst += toEndOfBitVector(i, parameters->bits);\
} while(0)

void
MOScompress_frame(MOStask task, MosaicBlkRec* estimate)
{
	MosaicBlk blk = task->blk;

	MOSsetTag(blk,MOSAIC_FRAME);
	MOSsetCnt(blk, 0);

	switch(ATOMbasetype(task->type)) {
	case TYPE_bte: FRAMEcompress(task, bte); break;
	case TYPE_sht: FRAMEcompress(task, sht); break;
	case TYPE_int: FRAMEcompress(task, int); break;
	case TYPE_lng: FRAMEcompress(task, lng); break;
	case TYPE_oid: FRAMEcompress(task, oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: FRAMEcompress(task, hge); break;
#endif
	}
}

// the inverse operator, extend the src

#define FRAMEdecompress(TASK, TPE)\
do {\
	MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((TASK))->blk;\
	BUN lim = parameters->base.cnt;\
    TPE min = parameters->min.min##TPE;\
	BitVector base = (BitVector) MOScodevectorFrame(TASK);\
	BUN i;\
	for(i = 0; i < lim; i++){\
		TPE delta = getBitVector(base, i, parameters->bits);\
		/*TODO: assert that delta's actually does not cause an overflow. */\
		TPE val = min + delta;\
		((TPE*)(TASK)->src)[i] = val;\
	}\
	(TASK)->src += i * sizeof(TPE);\
} while(0)

void
MOSdecompress_frame(MOStask task)
{
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: FRAMEdecompress(task, bte); break;
	case TYPE_sht: FRAMEdecompress(task, sht); break;
	case TYPE_int: FRAMEdecompress(task, int); break;
	case TYPE_lng: FRAMEdecompress(task, lng); break;
	case TYPE_oid: FRAMEdecompress(task, oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: FRAMEdecompress(task, hge); break;
#endif
	}
}

#define scan_loop_frame(TPE, CANDITER_NEXT, TEST) {\
	MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) task->blk;\
    TPE min = parameters->min.min##TPE;\
	BitVector base = (BitVector) MOScodevectorFrame(task);\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        TPE delta = getBitVector(base, i, parameters->bits);\
        v = ADD_DELTA(TPE, min, delta);\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

MOSselect_DEF(frame, bte)
MOSselect_DEF(frame, sht)
MOSselect_DEF(frame, int)
MOSselect_DEF(frame, lng)
#ifdef HAVE_HGE
MOSselect_DEF(frame, hge)
#endif

#define projection_loop_frame(TPE, CANDITER_NEXT)\
{\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((task))->blk;\
	TPE frame =  parameters->min.min##TPE;\
	BitVector base = (BitVector) MOScodevectorFrame(task);\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CANDITER_NEXT(task->ci)) {\
		BUN i = (BUN) (o - first);\
		TPE w = ADD_DELTA(TPE, frame, getBitVector(base, i, parameters->bits));\
		*bt++ = w;\
		task->cnt++;\
	}\
}

MOSprojection_DEF(frame, bte)
MOSprojection_DEF(frame, sht)
MOSprojection_DEF(frame, int)
MOSprojection_DEF(frame, lng)
#ifdef HAVE_HGE
MOSprojection_DEF(frame, hge)
#endif

#define outer_loop_frame(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((task))->blk;\
	const TPE min =  parameters->min.min##TPE;\
	const BitVector base = (BitVector) MOScodevectorFrame(task);\
	const bte bits = parameters->bits;\
    for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (lo - first);\
		TPE lval = ADD_DELTA(TPE, min, getBitVector(base, i, bits));\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, lval))) {continue;};\
		}\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
}

MOSjoin_COUI_DEF(frame, bte)
MOSjoin_COUI_DEF(frame, sht)
MOSjoin_COUI_DEF(frame, int)
MOSjoin_COUI_DEF(frame, lng)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(frame, hge)
#endif
