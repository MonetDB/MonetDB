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

// TODO: Revisit the whole layout stuffchunk_size is no longer correct
// we use longs as the basis for bit vectors
#define chunk_size(Task, Cnt) wordaligned(MosaicBlkSize + (Cnt * Task->hdr->framebits)/8 + (((Cnt * Task->hdr->framebits) %8) != 0), lng)

void
MOSadvance_frame(MOStask task)
{
	int *dst = (int*)  (((char*) task->blk) + MosaicBlkSize);
	long cnt = MOSgetCnt(task->blk);
	long bytes;

	assert(cnt > 0);
	task->start += (oid) cnt;
	bytes =  (cnt * task->hdr->framebits)/8 + (((cnt * task->hdr->framebits) %8) != 0) + sizeof(ulng);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, lng)); 
}

void
MOSlayout_frame(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output = chunk_size(task,cnt);
	if( BUNappend(btech, "frame blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSskip_frame(MOStask task)
{
	MOSadvance_frame( task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

typedef struct _FrameParameters_t {
	MosaicBlkRec base;
	int bits;
	union {
		bte minbte;
		sht minsht;
		int minint;
		lng minlng;
		oid minoid;
#ifdef HAVE_HGE
		hge minhge;
#endif
	} min;
	union {
		bte maxbte;
		sht maxsht;
		int maxint;
		lng maxlng;
		oid maxoid;
#ifdef HAVE_HGE
		hge maxhge;
#endif
	} max;

} MosaicBlkHeader_frame_t;

#define MOScodevectorFrame(Task) (((char*) (Task)->blk)+ wordaligned(sizeof(MosaicBlkHeader_frame_t), BitVector))

/* Use ternary operator because (in theory) we have to be careful not to get overflow's*/\
#define GET_DELTA_FOR_SIGNED_TYPE(DELTA_TPE, max, min) (min < 0? max < 0?(DELTA_TPE) (max - min) : (DELTA_TPE)(max) + (DELTA_TPE)(-1 * min) : (DELTA_TPE) (max - min))
#define GET_DELTA_FOR_UNSIGNED_TYPE(DELTA_TPE, max, min) ((DELTA_TPE) (max - min))

#define determineFrameParameters(PARAMETERS, SRC, LIMIT, TPE, DELTA_TPE, GET_DELTA) \
do {\
	TPE *val = SRC, max, min;\
	int bits = 1;\
	unsigned int i;\
	max = *val;\
	min = *val;\
	/*TODO: add additional loop to find best bit wise upper bound*/\
	for(i = 0; i < LIMIT; i++, val++){\
		bool evaluate_bits = false;\
		if (*val > max) {\
			max = *val;\
			evaluate_bits = true;\
		}\
		if (*val < min) {\
			min = *val;\
			evaluate_bits = true;\
		}\
		if (evaluate_bits) {\
		 	DELTA_TPE width = GET_DELTA(DELTA_TPE, max, min);\
			int current_bits = bits;\
			while (width > ((DELTA_TPE)(-1)) >> (sizeof(DELTA_TPE) * CHAR_BIT - current_bits) ) {/*keep track of number of BITS necessary to store difference*/\
				current_bits++;\
			}\
			if ( (current_bits >= (int) ((sizeof(TPE) * CHAR_BIT) / 2))\
				/*TODO: this extra condition should be removed once bitvector is extended to int64's*/\
				|| (current_bits > (int) sizeof(unsigned int) * CHAR_BIT) ) {\
				/*If we can from here on not compress better then the half of the original data type, we give up. */\
				break;\
			}\
			bits = current_bits;\
		}\
	}\
	(PARAMETERS).min.min##TPE = min;\
	(PARAMETERS).max.max##TPE = max;\
	(PARAMETERS).bits = bits;\
	(PARAMETERS).base.cnt = i;\
} while(0)

#define estimateFrame(TASK, TPE, DELTA_TPE, GET_DELTA)\
do {\
	TPE *src = getSrc(TPE, (TASK));\
	BUN limit = (TASK)->stop - (TASK)->start > MOSAICMAXCNT? MOSAICMAXCNT: (TASK)->stop - (TASK)->start;\
	MosaicBlkHeader_frame_t parameters;\
	determineFrameParameters(parameters, src, limit, TPE, DELTA_TPE, GET_DELTA);\
	assert(parameters.base.cnt > 0);/*Should always compress.*/\
	current->is_applicable = true;\
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
	case TYPE_bte: estimateFrame(task, bte, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_sht: estimateFrame(task, sht, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_int: estimateFrame(task, int, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_lng: estimateFrame(task, lng, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_oid: estimateFrame(task, oid,  oid, GET_DELTA_FOR_UNSIGNED_TYPE); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateFrame(task, hge, uhge, GET_DELTA_FOR_SIGNED_TYPE); break;
#endif
	}

	return MAL_SUCCEED;
}

#define FRAMEcompress(TASK, TPE, DELTA_TPE, GET_DELTA)\
do {\
	TPE *src = getSrc(TPE, (TASK));\
	TPE delta;\
	BUN i = 0;\
	BUN limit = (TASK)->stop - (TASK)->start > MOSAICMAXCNT? MOSAICMAXCNT: (TASK)->stop - (TASK)->start;\
	BitVector base;\
	MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((TASK))->blk;\
	determineFrameParameters(*parameters, src, limit, TPE, DELTA_TPE, GET_DELTA);\
	(TASK)->dst = MOScodevectorFrame(TASK);\
	base = (BitVector) ((TASK)->dst);\
	for(i = 0; i < parameters->base.cnt; i++, src++) {\
		/*TODO: assert that delta's actually does not cause an overflow. */\
		delta = *src - parameters->min.min##TPE;\
		setBitVector(base, i, parameters->bits, (unsigned int) /*TODO: fix this once we have increased capacity of bitvector*/ delta);\
	}\
	(TASK)->dst += wordaligned((i * parameters->bits / CHAR_BIT) + ( (i * parameters->bits) % CHAR_BIT ) != 0, lng);\
} while(0)

void
MOScompress_frame(MOStask task)
{
	MosaicBlk blk = task->blk;

	MOSsetTag(blk,MOSAIC_FRAME);
	MOSsetCnt(blk, 0);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: FRAMEcompress(task, bte, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_sht: FRAMEcompress(task, sht, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_int: FRAMEcompress(task, int, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_lng: FRAMEcompress(task, lng, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_oid: FRAMEcompress(task, oid, oid, GET_DELTA_FOR_UNSIGNED_TYPE); break;
#ifdef HAVE_HGE
	case TYPE_hge: FRAMEcompress(task, hge, uhge, GET_DELTA_FOR_SIGNED_TYPE); break;
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
		(TASK)->hdr->checksum2.sum##TPE += val;\
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

#define ANTI(Boolean) !(Boolean)
#define PRO(Boolean) (Boolean)

#define non_trivial_select_frame(TASK, TPE, DELTA_TPE, CMP_FLAVOR) \
do {\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((TASK))->blk;\
	TPE min =  parameters->min.min##TPE;\
	TPE max =  parameters->max.max##TPE;\
	BitVector base = (BitVector) MOScodevectorFrame(TASK);\
	if( is_nil(TPE, *(TPE*) low)) {\
		DELTA_TPE hgh2;\
		bool hi2 = *hi;\
		if (*(TPE*) hgh < min) {\
			if (CMP_FLAVOR(false) /* AKA ANTI */) for(i=0 ; first < last; first++, i++) {MOSskipit();*o++ = (oid) first;}\
		}\
		else if (*(TPE*) hgh > max || (*(TPE*) hgh == max && hi2) ) {\
			if (CMP_FLAVOR(true) /* AKA NOT ANTI */) for(i=0 ; first < last; first++, i++) {MOSskipit();*o++ = (oid) first;}\
		}\
		else { /* min >= *(TPE*) hgh <= max */\
			hgh2 = *(TPE*) hgh - min;\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				bool cmp  =  ((hi2 && delta <= hgh2 ) || (!hi2 && delta < hgh2 ));\
				if (CMP_FLAVOR(cmp) ) *o++ = (oid) first;\
			}\
		}\
	}\
	else if( is_nil(TPE, *(TPE*) hgh)){\
		DELTA_TPE low2;\
		bool li2 = *li;\
		if (*(TPE*) low > max) {\
			if (CMP_FLAVOR(false) /* AKA ANTI */) for(i=0 ; first < last; first++, i++) {MOSskipit();*o++ = (oid) first;}\
		} else\
		if (*(TPE*) low < min || (*(TPE*) low == min && li2) )  {\
			if (CMP_FLAVOR(true) /* AKA NOT ANTI */) for(i=0 ; first < last; first++, i++) {MOSskipit();*o++ = (oid) first;}\
		} else  {\
			low2 = *(TPE*) low - min;\
			for(i=0; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				bool cmp  =  ((li2 && delta >= low2 ) || (!li2 && delta > low2 ));\
				if (CMP_FLAVOR(cmp) )\
					*o++ = (oid) first;\
			}\
		}\
	}\
	else {\
		DELTA_TPE low2;\
		DELTA_TPE hgh2;\
		bool li2 = *li;\
		bool hi2 = *hi;\
		assert(!is_nil(TPE, *(TPE*) low) && !is_nil(TPE, *(TPE*) hgh));\
		if (*(TPE*) hgh < min) {\
			if (CMP_FLAVOR(false) /* AKA ANTI */) for(i=0 ; first < last; first++, i++) {MOSskipit();*o++ = (oid) first;}\
		}\
		else if (*(TPE*) low > max) {\
			if (CMP_FLAVOR(false) /* AKA ANTI */) for(i=0 ; first < last; first++, i++) {MOSskipit();*o++ = (oid) first;}\
		}\
		else if ( (*(TPE*) hgh > max || (*(TPE*) hgh == max && hi2)) && (*(TPE*) low < min || (*(TPE*) low == min && li2) ) ) {\
			if (CMP_FLAVOR(true) /* AKA NOT ANTI */) for(i=0 ; first < last; first++, i++) {MOSskipit();*o++ = (oid) first;}\
		}\
		else {\
		 	hgh2	= *(TPE*) hgh > max ? max - min : *(TPE*) hgh - min;\
			hi2		= *(TPE*) hgh > max ? true : hi2;\
			low2	= *(TPE*) low < min ? 0 : *(TPE*) low - min;\
			li2		= *(TPE*) low < min ? true : li2;\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				bool cmp  =  ((hi2 && delta <= hgh2 ) || (!hi2 && delta < hgh2 )) &&\
						((li2 && delta >= low2 ) || (!li2 && delta > low2 ));\
				if (CMP_FLAVOR(cmp))\
					*o++ = (oid) first;\
			}\
		}\
	}\
} while(0)

// TODO: Simplify (deduplication) and optimize (control deps to data deps) this macro
#define select_frame(TASK, TPE, DELTA_TPE) {\
	if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
		if (!*anti) {\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		}\
		else {\
			/* nothing is matching */\
		}\
	}\
	else if( !*anti){\
		non_trivial_select_frame(TASK, TPE, DELTA_TPE, PRO);\
	}\
	else {\
		non_trivial_select_frame(TASK, TPE, DELTA_TPE, ANTI);\
	}\
}

str
MOSselect_frame( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN i, first,last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_frame(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_frame(task, bte, ulng); break;
	case TYPE_sht: select_frame(task, sht, ulng); break;
	case TYPE_int: select_frame(task, int, ulng); break;
	case TYPE_lng: select_frame(task, lng, ulng); break;
	case TYPE_oid: select_frame(task, oid, oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_frame(task, hge, uhge); break;
#endif
	}
	MOSskip_frame(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_frame(TPE, DELTA_TPE, GET_DELTA)\
{\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((task))->blk;\
	TPE min =  parameters->min.min##TPE;\
	BitVector base = (BitVector) MOScodevectorFrame(task);\
	if ( strcmp(oper,"<") == 0) {\
		TPE hgh= *(TPE*) val;\
		if (hgh >= min) {\
			DELTA_TPE hgh2 = GET_DELTA(DELTA_TPE, hgh, min);\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				if( (delta < hgh2 ) ){\
					*o++ = (oid) first;\
				}\
			}\
		}\
		/*else nothing matches*/\
	}\
	else if ( strcmp(oper,"<=") == 0) {\
		TPE hgh= *(TPE*) val;\
		if (hgh >= min) {\
			DELTA_TPE hgh2 = GET_DELTA(DELTA_TPE, hgh, min);\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				if( (delta <= hgh2 ) ){\
					*o++ = (oid) first;\
				}\
			}\
		}\
		/*else nothing matches*/\
	}\
	else if ( strcmp(oper,">") == 0) {\
		TPE low= *(TPE*) val;\
		if (low >= min) {\
			DELTA_TPE low2 = GET_DELTA(DELTA_TPE, low, min);\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				if( (delta > low2 ) ){\
					*o++ = (oid) first;\
				}\
			}\
		}\
		else /*everything matches*/ {\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		}\
	}\
	else if ( strcmp(oper,">=") == 0) {\
		TPE low= *(TPE*) val;\
		if (low >= min) {\
			DELTA_TPE low2 = GET_DELTA(DELTA_TPE, low, min);\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				if( (delta >= low2 ) ){\
					*o++ = (oid) first;\
				}\
			}\
		}\
		else /*everything matches*/ {\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		}\
	}\
	else if ( strcmp(oper,"!=") == 0) {\
		TPE cmprnd= *(TPE*) val;\
		if (cmprnd >= min) {\
			DELTA_TPE low2 = GET_DELTA(DELTA_TPE, cmprnd, min);\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				if( (delta != low2 ) ){\
					*o++ = (oid) first;\
				}\
			}\
		}\
		else /*everything matches*/ {\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		}\
	}\
	else if ( strcmp(oper,"==") == 0) {\
		TPE cmprnd= *(TPE*) val;\
		if (cmprnd >= min) {\
			DELTA_TPE low2 = GET_DELTA(DELTA_TPE, cmprnd, min);\
			for(int i=0; first < last; first++, i++){\
				MOSskipit();\
				DELTA_TPE delta = getBitVector(base, i, parameters->bits);\
				if( (delta == low2 ) ){\
					*o++ = (oid) first;\
				}\
			}\
		}\
		/*else nothing matches*/\
	}\
}

str
MOSthetaselect_frame( MOStask task, void *val, str oper)
{
	oid *o;
	BUN first,last;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_frame(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_frame(bte, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_sht: thetaselect_frame(sht, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_int: thetaselect_frame(int, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_lng: thetaselect_frame(lng, ulng, GET_DELTA_FOR_SIGNED_TYPE); break;
	case TYPE_oid: thetaselect_frame(oid, oid, GET_DELTA_FOR_UNSIGNED_TYPE); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_frame(hge, uhge, GET_DELTA_FOR_SIGNED_TYPE); break;
#endif
	}
	MOSskip_frame(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_frame(TPE)\
{	TPE *v;\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((task))->blk;\
	TPE frame =  parameters->min.min##TPE;\
	BitVector base = (BitVector) MOScodevectorFrame(task);\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++) {\
		MOSskipit();\
		*v++ = frame + getBitVector(base, i, parameters->bits);\
		task->cnt++;\
	}\
}

str
MOSprojection_frame( MOStask task)
{
	BUN i,first,last;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_frame(bte); break;
		case TYPE_sht: projection_frame(sht); break;
		case TYPE_int: projection_frame(int); break;
		case TYPE_lng: projection_frame(lng); break;
		case TYPE_oid: projection_frame(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_frame(hge); break;
#endif
	}
	MOSskip_frame(task);
	return MAL_SUCCEED;
}

#define join_frame(TPE)\
{	TPE *w;\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((task))->blk;\
	TPE frame =  parameters->min.min##TPE;\
	BitVector base = (BitVector) MOScodevectorFrame(task);\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->stop; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			if ( *w == frame + getBitVector(base, i, parameters->bits)){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false)!= GDK_SUCCEED)\
				throw(MAL,"mosaic.frame",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

str
MOSjoin_frame( MOStask task)
{
	BUN i,n,limit;
	oid o, oo;

	// set the oid range covered and advance scan range
	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_frame(bte); break;
		case TYPE_sht: join_frame(sht); break;
		case TYPE_int: join_frame(int); break;
		case TYPE_lng: join_frame(lng); break;
		case TYPE_oid: join_frame(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_frame(hge); break;
#endif
		}
	MOSskip_frame(task);
	return MAL_SUCCEED;
}
