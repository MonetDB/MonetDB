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

#define toEndOfBitVector(CNT, BITS) wordaligned(((CNT) * (BITS) / CHAR_BIT) + ( ((CNT) * (BITS)) % CHAR_BIT != 0 ), lng)

void
MOSadvance_frame(MOStask task)
{
	MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) (task)->blk;
	int *dst = (int*)  (((char*) task->blk) + wordaligned(sizeof(MosaicBlkHeader_frame_t), unsigned int));
	long cnt = parameters->base.cnt;
	long bytes = toEndOfBitVector(cnt, parameters->bits);

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->blk = (MosaicBlk) (((char*) dst)  + bytes);
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

#define MOScodevectorFrame(Task) (((char*) (Task)->blk)+ wordaligned(sizeof(MosaicBlkHeader_frame_t), unsigned int))

#define Deltabte uint8_t
#define Deltasht uint16_t
#define Deltaint uint32_t
#define Deltalng uint64_t
#define Deltaoid uint64_t
#ifdef HAVE_HGE
#define Deltahge uhge
#endif

#define DeltaTpe(TPE) Delta##TPE

/* Use standard unsigned integer operations because (in theory) we have to be careful not to get overflow's and undefined behavior*/\
#define GET_DELTA(TPE, max, min)  ((DeltaTpe(TPE)) max - (DeltaTpe(TPE)) min)
#define ADD_DELTA(TPE, min, delta)  (TPE) ((DeltaTpe(TPE)) min + (DeltaTpe(TPE)) delta)

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
				|| (current_bits > (int) sizeof(unsigned int) * CHAR_BIT) ) {\
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
		setBitVector(base, i, parameters->bits, (unsigned int) /*TODO: fix this once we have increased capacity of bitvector*/ delta);\
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
#define ANTI_SELECT(Boolean) !(Boolean)
#define SELECT(Boolean) (Boolean)
/* generic range select
 *
 * This macro is based on the combined behavior of ALGselect2 and BATselect.
 * It should return the same output on the same input.
 *
 * A complete breakdown of the various arguments follows.  Here, v, v1
 * and v2 are values from the appropriate domain, and
 * v != nil, v1 != nil, v2 != nil, v1 < v2.
 *	tl	th	li	hi	anti	result list of OIDs for values
 *	-----------------------------------------------------------------
 *	nil	nil	true	true	false	x == nil (only way to get nil)
 *	nil	nil	true	true	true	x != nil
 *	nil	nil	A*		B*		false	x != nil *it must hold that A && B == false.
 *	nil	nil	A*		B*		true	NOTHING *it must hold that A && B == false.
 *	v	v	A*		B*		true	x != nil *it must hold that A && B == false.
 *	v	v	A*		B*		false	NOTHING *it must hold that A && B == false.
 *	v2	v1	ignored	ignored	false	NOTHING
 *	v2	v1	ignored	ignored	true	x != nil
 *	nil	v	ignored	false	false	x < v
 *	nil	v	ignored	true	false	x <= v
 *	nil	v	ignored	false	true	x >= v
 *	nil	v	ignored	true	true	x > v
 *	v	nil	false	ignored	false	x > v
 *	v	nil	true	ignored	false	x >= v
 *	v	nil	false	ignored	true	x <= v
 *	v	nil	true	ignored	true	x < v
 *	v	v	true	true	false	x == v
 *	v	v	true	true	true	x != v
 *	v1	v2	false	false	false	v1 < x < v2
 *	v1	v2	true	false	false	v1 <= x < v2
 *	v1	v2	false	true	false	v1 < x <= v2
 *	v1	v2	true	true	false	v1 <= x <= v2
 *	v1	v2	false	false	true	x <= v1 or x >= v2
 *	v1	v2	true	false	true	x < v1 or x >= v2
 *	v1	v2	false	true	true	x <= v1 or x > v2
 */
#define select_frame_general(RESULT, BASE, BITS, HSEQBASE, CNT, MIN, MAX, LOW, HIGH, LI, HI, NIL_DELTA, HAS_NIL, ANTI, TPE)\
do {\
\
	if		( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && !(ANTI)) {\
		if(HAS_NIL) {\
			for(i=0; i < (CNT); i++){\
				DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS);\
				if (delta == (NIL_DELTA)) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
			}\
		}\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && (ANTI)) {\
		if(HAS_NIL) {\
			for(i=0; i < (CNT); i++){\
				DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS);\
				if (delta != (NIL_DELTA)) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
			}\
		}\
		else for(i=0; i < (CNT); i++) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && !((LI) && (HI)) && !(ANTI)) {\
		if(HAS_NIL) {\
			for(i=0; i < (CNT); i++){\
				DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS);\
				if (delta != (NIL_DELTA)) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
			}\
		}\
		else for(i=0; i < (CNT); i++) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && !((LI) && (HI)) && (ANTI)) {\
			/*Empty result set.*/\
	}\
	else if( IS_NIL(TPE, (LOW))) {\
		DeltaTpe(TPE) hgh2;\
		bool hi2 = (HI);\
		if ((HIGH) < (MIN)) {\
			if ((ANTI) /* AKA ANTI_SELECT */) for(i=0; i < (CNT); i++) {\
				if (HAS_NIL) { DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS); if (delta == (NIL_DELTA)) { continue;}}\
				*(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		}\
		else if ((HIGH) > (MAX) || ((HIGH) == (MAX) && hi2) ) {\
			if (!(ANTI) /* AKA SELECT */) for(i=0; i < (CNT); i++) {\
				if (HAS_NIL) { DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS); if (delta == (NIL_DELTA)) { continue;}}\
				*(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		}\
		else { /* MIN >= HIGH <= MAX */\
			hgh2 = (HIGH) - (MIN);\
			for(i=0; i < (CNT); i++){\
				DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS);\
				if ((HAS_NIL) && delta == (NIL_DELTA)) {continue;}\
				bool cmp  =  ((hi2 && delta <= hgh2 ) || (!hi2 && delta < hgh2 ));\
				if (cmp == !(ANTI)) *(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		}\
	}\
	else if( IS_NIL(TPE, (HIGH))){\
		DeltaTpe(TPE) low2;\
		bool li2 = (LI);\
		if ((LOW) > (MAX)) {\
			if ((ANTI) /* AKA ANTI_SELECT */) for(i=0; i < (CNT); i++) {\
				if (HAS_NIL) { DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS); if (delta == (NIL_DELTA)) { continue;}}\
				*(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		} else\
		if ((LOW) < (MIN) || ((LOW) == (MIN) && li2) )  {\
			if (!(ANTI) /* AKA SELECT */) for(i=0; i < (CNT); i++) {\
				if (HAS_NIL) { DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS); if (delta == (NIL_DELTA)) { continue;}}\
				*(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		} else  {\
			low2 = (LOW) - (MIN);\
			for(i=0; i < (CNT); i++){\
				DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS);\
				if ((HAS_NIL) && delta == (NIL_DELTA)) {continue;}\
				bool cmp  =  ((li2 && delta >= low2 ) || (!li2 && delta > low2 ));\
				if (cmp == !(ANTI))\
					*(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		}\
	}\
	else {\
		DeltaTpe(TPE) low2;\
		DeltaTpe(TPE) hgh2;\
		bool li2 = (LI);\
		bool hi2 = (HI);\
		assert(!IS_NIL(TPE, (LOW)) && !IS_NIL(TPE, (HIGH)));\
		if (LOW == HIGH && !(LI && HI) && !(ANTI)) {\
			/*Empty result set.*/\
		}\
		else if (LOW == HIGH && !(LI && HI) && (ANTI)) {\
			if(HAS_NIL) {\
				for(i=0; i < (CNT); i++){\
					DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS);\
					if (delta != (NIL_DELTA)) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
				}\
			}\
			else for(i=0; i < (CNT); i++) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
		}\
		else if (LOW > HIGH && !(LI && HI) && !(ANTI)) {\
			/*Empty result set.*/\
		}\
		else if (LOW > HIGH && !(LI && HI) && (ANTI)) {\
			if(HAS_NIL) {\
				for(i=0; i < (CNT); i++){\
					DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS);\
					if (delta != (NIL_DELTA)) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
				}\
			}\
			else for(i=0; i < (CNT); i++) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
		}\
		else if ((HIGH) < (MIN)) {\
			if ((ANTI) /* AKA ANTI_SELECT */) for(i=0; i < (CNT); i++) {\
				if (HAS_NIL) { DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS); if (delta == (NIL_DELTA)) { continue;}}\
				*(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		}\
		else if ((LOW) > (MAX)) {\
			if ((ANTI) /* AKA ANTI_SELECT */) for(i=0; i < (CNT); i++) {\
				if (HAS_NIL) { DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS); if (delta == (NIL_DELTA)) { continue;}}\
				*(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		}\
		else if ( ((HIGH) > (MAX) || ((HIGH) == (MAX) && hi2)) && ((LOW) < (MIN) || ((LOW) == (MIN) && li2) ) ) {\
			if (!(ANTI) /* AKA SELECT */) for(i=0; i < (CNT); i++) {*(RESULT)++ = (oid) (i + (HSEQBASE));}\
		}\
		else {\
		 	hgh2	= (HIGH) > (MAX) ? (MAX) - (MIN) : (HIGH) - (MIN);\
			hi2		= (HIGH) > (MAX) ? true : hi2;\
			low2	= (LOW) < (MIN) ? 0 : (LOW) - (MIN);\
			li2		= (LOW) < (MIN) ? true : li2;\
			for(i=0; i < (CNT); i++){\
				DeltaTpe(TPE) delta = getBitVector(BASE, i, BITS);\
				if ((HAS_NIL) && delta == (NIL_DELTA)) {continue;}\
				bool cmp  =  ((hi2 && delta <= hgh2 ) || (!hi2 && delta < hgh2 )) &&\
						((li2 && delta >= low2 ) || (!li2 && delta > low2 ));\
				if (cmp == !(ANTI))\
					*(RESULT)++ = (oid) (i + (HSEQBASE));\
			}\
		}\
	}\
} while(0)

// TODO: Simplify (deduplication) and optimize (control deps to data deps) this macro
#define select_frame(RESULT, PARAMETERS, BASE, HSEQBASE, CNT, NONIL, LOW, HIGH, LI, HI, ANTI, TPE) {\
    TPE min =	(PARAMETERS)->min.min##TPE;\
	TPE max =	(PARAMETERS)->max.max##TPE;\
	int bits=	(PARAMETERS)->bits;\
\
	bool nil = false;\
	DeltaTpe(TPE) nil_delta;\
	if (!NONIL && (min == TPE##_nil || max == TPE##_nil)) {\
		/*TODO: this is a strong assumption that nil values are always the highest or the lowest value in a GDK type domain.*/\
		if	 	( (nil = (min == TPE##_nil)) ) { nil_delta = 0; }\
		else if ( (nil = (max == TPE##_nil)) ) { nil_delta = GET_DELTA(TPE, max, min); }\
	}\
	if( nil && *(ANTI)){\
		select_frame_general(RESULT, BASE, bits, HSEQBASE, CNT, min, max, *(TPE*) LOW, *(TPE*) HIGH, *LI, *HI, nil_delta, true, true, TPE);\
	}\
	if( !nil && *(ANTI)){\
		select_frame_general(RESULT, BASE, bits, HSEQBASE, CNT, min, max, *(TPE*) LOW, *(TPE*) HIGH, *LI, *HI, nil_delta, false, true, TPE);\
	}\
	if( nil && !*(ANTI)){\
		select_frame_general(RESULT, BASE, bits, HSEQBASE, CNT, min, max, *(TPE*) LOW, *(TPE*) HIGH, *LI, *HI, nil_delta, true, false, TPE);\
	}\
	if( !nil && !*(ANTI)){\
		select_frame_general(RESULT, BASE, bits, HSEQBASE, CNT, min, max, *(TPE*) LOW, *(TPE*) HIGH, *LI, *HI, nil_delta, false, false, TPE);\
	}\
}

str
MOSselect_frame( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *result;
	BUN i, hseqbase,cnt;
	MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) (task)->blk;
	BitVector base = (BitVector) MOScodevectorFrame(task);
	bool nonil = task->bsrc->tnonil;

	// set the oid range covered and advance scan range
	hseqbase = task->start;
	cnt = MOSgetCnt(task->blk);

	if (task->cl && *task->cl > cnt){
		MOSskip_frame(task);
		return MAL_SUCCEED;
	}
	result = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_frame(result, parameters, base, hseqbase, cnt, nonil, low, hgh, li, hi, anti, bte); break;
	case TYPE_sht: select_frame(result, parameters, base, hseqbase, cnt, nonil, low, hgh, li, hi, anti, sht); break;
	case TYPE_int: select_frame(result, parameters, base, hseqbase, cnt, nonil, low, hgh, li, hi, anti, int); break;
	case TYPE_lng: select_frame(result, parameters, base, hseqbase, cnt, nonil, low, hgh, li, hi, anti, lng); break;
	case TYPE_oid: select_frame(result, parameters, base, hseqbase, cnt, nonil, low, hgh, li, hi, anti, oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_frame(result, parameters, base, hseqbase, cnt, nonil, low, hgh, li, hi, anti, hge); break;
#endif
	}
	task->lb = result;
	MOSskip_frame(task);
	return MAL_SUCCEED;
}

#define thetaselect_frame_general(TPE, NIL_DELTA, HAS_NIL)\
{\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((task))->blk;\
	TPE min =  parameters->min.min##TPE;\
	BitVector base = (BitVector) MOScodevectorFrame(task);\
	if ( strcmp(oper,"<") == 0) {\
		TPE hgh= *(TPE*) val;\
		if (hgh >= min) {\
			DeltaTpe(TPE) hgh2 = GET_DELTA(TPE, hgh, min);\
			for(int i=0; first < last; first++, i++){\
				DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);\
				if (HAS_NIL) { if (delta == (NIL_DELTA)) { continue;}}\
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
			DeltaTpe(TPE) hgh2 = GET_DELTA(TPE, hgh, min);\
			for(int i=0; first < last; first++, i++){\
				DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);\
				if (HAS_NIL) { if (delta == (NIL_DELTA)) { continue;}}\
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
			DeltaTpe(TPE) low2 = GET_DELTA(TPE, low, min);\
			for(int i=0; first < last; first++, i++){\
				DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);\
				if (HAS_NIL) { if (delta == (NIL_DELTA)) { continue;}}\
				if( (delta > low2 ) ){\
					*o++ = (oid) first;\
				}\
			}\
		}\
		else /*everything matches*/ {\
			for(int i=0; first < last; first++, i++){\
				if (HAS_NIL) {\
					DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);\
					if (delta == (NIL_DELTA)) { continue;}\
				}\
				*o++ = (oid) first;\
			}\
		}\
	}\
	else if ( strcmp(oper,">=") == 0) {\
		TPE low= *(TPE*) val;\
		if (low >= min) {\
			DeltaTpe(TPE) low2 = GET_DELTA(TPE, low, min);\
			for(int i=0; first < last; first++, i++){\
				DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);\
				if (HAS_NIL) { if (delta == (NIL_DELTA)) { continue;}}\
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
			DeltaTpe(TPE) low2 = GET_DELTA(TPE, cmprnd, min);\
			for(int i=0; first < last; first++, i++){\
				DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);\
				if (HAS_NIL) { if (delta == (NIL_DELTA)) { continue;}}\
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
			DeltaTpe(TPE) low2 = GET_DELTA(TPE, cmprnd, min);\
			for(int i=0; first < last; first++, i++){\
				DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);\
				if (HAS_NIL) { if (delta == (NIL_DELTA)) { continue;}}\
				if( (delta == low2 ) ){\
					*o++ = (oid) first;\
				}\
			}\
		}\
		/*else nothing matches*/\
	}\
}

#define thetaselect_frame(TPE)\
{\
    TPE min =	parameters->min.min##TPE;\
	TPE max =	parameters->max.max##TPE;\
\
	bool nil = false;\
	DeltaTpe(TPE) nil_delta;\
	if (!task->bsrc->tnonil && (min == TPE##_nil || max == TPE##_nil)) {\
		/*TODO: this is a strong assumption that nil values are always the highest or the lowest value in a GDK type domain.*/\
		if	 	( (nil = (min == TPE##_nil)) ) { nil_delta = 0; }\
		else if ( (nil = (max == TPE##_nil)) ) { nil_delta = GET_DELTA(TPE, max, min); }\
	}\
\
	if (nil) {\
		thetaselect_frame_general(TPE, nil_delta, true);\
	}\
	else /*!nil*/ {\
		thetaselect_frame_general(TPE, nil_delta, false);\
	}\
}

str
MOSthetaselect_frame( MOStask task, void *val, str oper)
{
	oid *o;
	BUN first,last;
	MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) (task)->blk;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_frame(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_frame(bte); break;
	case TYPE_sht: thetaselect_frame(sht); break;
	case TYPE_int: thetaselect_frame(int); break;
	case TYPE_lng: thetaselect_frame(lng); break;
	case TYPE_oid: thetaselect_frame(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_frame(hge); break;
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
		TPE w = ADD_DELTA(TPE, frame, getBitVector(base, i, parameters->bits));\
		*v++ = w;\
		task->cnt++;\
	}\
\
	task->src = (char*) v;\
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


#define join_frame_general(HAS_NIL, NIL_MATCHES, TPE)\
{	TPE *w;\
	BitVector base = (BitVector) MOScodevectorFrame(task);\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->stop; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			TPE v = ADD_DELTA(TPE, min, getBitVector(base, i, parameters->bits));\
			if (HAS_NIL && !NIL_MATCHES) {\
				if ((IS_NIL(TPE, v))) {continue;};\
			}\
			if (ARE_EQUAL(*w, v, HAS_NIL, TPE)){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false)!= GDK_SUCCEED)\
				throw(MAL,"mosaic.frame",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

#define join_frame(TPE)\
{\
    MosaicBlkHeader_frame_t* parameters = (MosaicBlkHeader_frame_t*) ((task))->blk;\
	TPE min =  parameters->min.min##TPE;\
	if( nil && nil_matches){\
		join_frame_general(true, true, TPE);\
	}\
	if( !nil && nil_matches){\
		join_frame_general(false, true, TPE);\
	}\
	if( nil && !nil_matches){\
		join_frame_general(true, false, TPE);\
	}\
	if( !nil && !nil_matches){\
		join_frame_general(false, false, TPE);\
	}\
}

str
MOSjoin_frame( MOStask task, bit nil_matches)
{
	BUN i,n,limit;
	oid o, oo;
	bool nil = !task->bsrc->tnonil;

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
