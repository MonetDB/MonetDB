/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
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

#define MOSadvance_DEF(TPE)\
MOSadvance_SIGNATURE(frame, TPE)\
{\
	MOSBlockHeaderTpe(frame, TPE)* parameters = (MOSBlockHeaderTpe(frame, TPE)*) (task)->blk;\
	BUN cnt		= MOSgetCnt(task->blk);\
\
	assert(cnt > 0);\
	assert(MOSgetTag(task->blk) == MOSAIC_FRAME);\
\
	task->start += (oid) cnt;\
\
	char* blk = (char*)task->blk;\
	blk += sizeof(MOSBlockHeaderTpe(frame, TPE));\
	blk += BitVectorSize(cnt, parameters->bits);\
	blk += GET_PADDING(task->blk, frame, TPE);\
\
	task->blk = (MosaicBlk) blk;\
}

MOSadvance_DEF(bte)
MOSadvance_DEF(sht)
MOSadvance_DEF(int)
MOSadvance_DEF(lng)
#ifdef HAVE_HGE
MOSadvance_DEF(hge)
#endif

void
MOSlayout_frame(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
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
	bte bits = 1;\
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
			bte current_bits = bits;\
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
	(PARAMETERS).min = min;\
	(PARAMETERS).bits = bits;\
	(PARAMETERS).rec.cnt = i;\
} while(0)

#define MOSestimate_frame_DEF(TPE) \
MOSestimate_SIGNATURE(frame, TPE)\
{\
	(void) previous;\
	current->is_applicable = true;\
	current->compression_strategy.tag = MOSAIC_FRAME;\
	TPE *src = getSrc(TPE, task);\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	MOSBlockHeaderTpe(frame, TPE) parameters;\
	determineFrameParameters(parameters, src, limit, TPE);\
	assert(parameters.rec.cnt > 0);/*Should always compress.*/\
	current->uncompressed_size += (BUN) (parameters.rec.cnt * sizeof(TPE));\
	current->compressed_size += 2 * sizeof(MOSBlockHeaderTpe(frame, TPE)) + wordaligned((parameters.rec.cnt * parameters.bits) / CHAR_BIT, lng);\
	current->compression_strategy.cnt = (unsigned int) parameters.rec.cnt;\
\
	if (parameters.rec.cnt > *current->max_compression_length ) {\
		*current->max_compression_length = parameters.rec.cnt;\
	}\
\
	return MAL_SUCCEED;\
}

MOSestimate_frame_DEF(bte)
MOSestimate_frame_DEF(sht)
MOSestimate_frame_DEF(int)
MOSestimate_frame_DEF(lng)
#ifdef HAVE_HGE
MOSestimate_frame_DEF(hge)
#endif

#define MOSpostEstimate_DEF(TPE)\
MOSpostEstimate_SIGNATURE(frame, TPE)\
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
MOScompress_SIGNATURE(frame, TPE)\
{\
	ALIGN_BLOCK_HEADER(task,  frame, TPE);\
\
	MosaicBlk blk = task->blk;\
	MOSsetTag(blk,MOSAIC_FRAME);\
	MOSsetCnt(blk, 0);\
	TPE *src = getSrc(TPE, task);\
	TPE delta;\
	BUN i = 0;\
	BUN limit = estimate->cnt;\
	MOSBlockHeaderTpe(frame, TPE)* parameters = (MOSBlockHeaderTpe(frame, TPE)*) (task)->blk;\
	determineFrameParameters(*parameters, src, limit, TPE);\
	BitVector base = MOScodevectorFrame(task, TPE);\
	task->dst = (char*) base;\
	for(i = 0; i < MOSgetCnt(task->blk); i++, src++) {\
		/*TODO: assert that delta's actually does not cause an overflow. */\
		delta = *src - parameters->min;\
		setBitVector(base, i, parameters->bits, (BitVectorChunk) /*TODO: fix this once we have increased capacity of bitvector*/ delta);\
	}\
	task->dst += BitVectorSize(i, parameters->bits);\
}

MOScompress_DEF(bte)
MOScompress_DEF(sht)
MOScompress_DEF(int)
MOScompress_DEF(lng)
#ifdef HAVE_HGE
MOScompress_DEF(hge)
#endif

#define MOSdecompress_DEF(TPE) \
MOSdecompress_SIGNATURE(frame, TPE)\
{\
	MOSBlockHeaderTpe(frame, TPE)* parameters = (MOSBlockHeaderTpe(frame, TPE)*) (task)->blk;\
	BUN lim = MOSgetCnt(task->blk);\
    TPE min = parameters->min;\
	BitVector base = (BitVector) MOScodevectorFrame(task, TPE);\
	BUN i;\
	for(i = 0; i < lim; i++){\
		TPE delta = getBitVector(base, i, parameters->bits);\
		/*TODO: assert that delta's actually does not cause an overflow. */\
		TPE val = min + delta;\
		((TPE*)task->src)[i] = val;\
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

#define scan_loop_frame(TPE, CANDITER_NEXT, TEST) {\
	MOSBlockHeaderTpe(frame, TPE)* parameters = (MOSBlockHeaderTpe(frame, TPE)*) task->blk;\
    TPE min = parameters->min;\
	BitVector base = (BitVector) MOScodevectorFrame(task, TPE);\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        TPE delta = getBitVector(base, i, parameters->bits);\
        v = ADD_DELTA(TPE, min, delta);\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

#define NAME frame
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

#define projection_loop_frame(TPE, CANDITER_NEXT)\
{\
    MOSBlockHeaderTpe(frame, TPE)* parameters = (MOSBlockHeaderTpe(frame, TPE)*) ((task))->blk;\
	TPE frame =  parameters->min;\
	BitVector base = (BitVector) MOScodevectorFrame(task, TPE);\
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
    MOSBlockHeaderTpe(frame, TPE)* parameters = (MOSBlockHeaderTpe(frame, TPE)*) ((task))->blk;\
	const TPE min =  parameters->min;\
	const BitVector base = (BitVector) MOScodevectorFrame(task, TPE);\
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
