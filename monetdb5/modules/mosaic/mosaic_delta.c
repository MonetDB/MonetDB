/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * Byte-wise delta encoding for SHT,INT,LNG, OID, WRD, STR-offsets, TIMESTAMP
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "gdk_bitvector.h"
#include "mosaic_delta.h"
#include "mosaic_private.h"

//#define _DEBUG_MOSAIC_

bool MOStypes_delta(BAT* b) {
	switch(b->ttype) {
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

typedef struct _DeltaParameters_t {
	MosaicBlkRec base;
	int bits;
	union {
		bte valbte;
		sht valsht;
		int valint;
		lng vallng;
		oid valoid;
#ifdef HAVE_HGE
		hge valhge;
#endif
	} init;
} MosaicBlkHeader_delta_t;

#define toEndOfBitVector(CNT, BITS) wordaligned(((CNT) * (BITS) / CHAR_BIT) + ( ((CNT) * (BITS)) % CHAR_BIT != 0 ), lng)

void
MOSadvance_delta(MOStask task)
{
	MosaicBlkHeader_delta_t* parameters = (MosaicBlkHeader_delta_t*) (task)->blk;
	int *dst = (int*)  (((char*) task->blk) + wordaligned(sizeof(MosaicBlkHeader_delta_t), unsigned int));
	long cnt = parameters->base.cnt;
	long bytes = toEndOfBitVector(cnt, parameters->bits);

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->blk = (MosaicBlk) (((char*) dst)  + bytes);
}


void
MOSlayout_delta(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned(MosaicBlkSize + sizeof(bte) + MOSgetCnt(blk)-1,bte); break ;
	case TYPE_sht: output = wordaligned(MosaicBlkSize + sizeof(sht) + MOSgetCnt(blk)-1,sht); break ;
	case TYPE_int: output = wordaligned(MosaicBlkSize + sizeof(int) + MOSgetCnt(blk)-1,int); break ;
	case TYPE_lng: output = wordaligned(MosaicBlkSize + sizeof(lng) + MOSgetCnt(blk)-1,lng); break ;
	case TYPE_oid: output = wordaligned(MosaicBlkSize + sizeof(oid) + MOSgetCnt(blk)-1,oid); break ;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned(MosaicBlkSize + sizeof(hge) + MOSgetCnt(blk)-1,hge); break ;
#endif
	}
	if( BUNappend(btech, "delta", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSskip_delta(MOStask task)
{
	MOSadvance_delta(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define MOScodevectorDelta(Task) (((char*) (Task)->blk)+ wordaligned(sizeof(MosaicBlkHeader_delta_t), unsigned int))

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
#define GET_DELTA(TPE, max, val)  ((DeltaTpe(TPE)) max - (DeltaTpe(TPE)) val)
#define ADD_DELTA(TPE, val, delta)  (TPE) ((DeltaTpe(TPE)) val + (DeltaTpe(TPE)) delta)

#define determineDeltaParameters(PARAMETERS, SRC, LIMIT, TPE) \
do {\
	TPE *val = SRC;\
	int bits = 1;\
	unsigned int i;\
	DeltaTpe(TPE) unsigned_delta = 0;\
	TPE prev_val;\
	(PARAMETERS).init.val##TPE = *val;\
\
	for(i = 1; i < LIMIT; i++){\
		prev_val = *val++;\
		DeltaTpe(TPE) current_unsigned_delta;\
		if (*val > prev_val) {\
			current_unsigned_delta = GET_DELTA(TPE, *val, prev_val);\
		}\
		else {\
			current_unsigned_delta = GET_DELTA(TPE, prev_val, *val);\
		}\
\
		if (current_unsigned_delta > unsigned_delta) {\
			int current_bits = bits;\
			while (current_unsigned_delta > ((DeltaTpe(TPE))(-1)) >> (sizeof(DeltaTpe(TPE)) * CHAR_BIT - current_bits) ) {\
				/*keep track of number of BITS necessary to store the difference*/\
				current_bits++;\
			}\
			int current_bits_with_sign_bit = current_bits + 1;\
			if ( (current_bits_with_sign_bit >= (int) ((sizeof(TPE) * CHAR_BIT) / 2))\
				/*If we can from here on not compress better then the half of the original data type, we give up. */\
				|| (current_bits_with_sign_bit > (int) sizeof(unsigned int) * CHAR_BIT) ) {\
				/*TODO: this extra condition should be removed once bitvector is extended to int64's*/\
				break;\
			}\
			bits = current_bits;\
			unsigned_delta = current_unsigned_delta;\
		}\
	}\
\
	/*Add the additional sign bit to the bit count.*/\
	bits++;\
	(PARAMETERS).base.cnt = i;\
	(PARAMETERS).bits = bits;\
} while(0)

#define estimateDelta(TASK, TPE)\
do {\
	TPE *src = getSrc(TPE, (TASK));\
	BUN limit = (TASK)->stop - (TASK)->start > MOSAICMAXCNT? MOSAICMAXCNT: (TASK)->stop - (TASK)->start;\
	MosaicBlkHeader_delta_t parameters;\
	determineDeltaParameters(parameters, src, limit, TPE);\
	assert(parameters.base.cnt > 0);/*Should always compress.*/\
	current->uncompressed_size += (BUN) (parameters.base.cnt * sizeof(TPE));\
	current->compressed_size += wordaligned(sizeof(MosaicBlkHeader_delta_t), lng) + wordaligned((parameters.base.cnt * parameters.bits) / CHAR_BIT, lng);\
	current->compression_strategy.cnt = (unsigned int) parameters.base.cnt;\
} while (0)

// calculate the expected reduction using dictionary in terms of elements compressed
str
MOSestimate_delta(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous) {
	(void) previous;
	current->is_applicable = true;
	current->compression_strategy.tag = MOSAIC_DELTA;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: estimateDelta(task, bte); break;
	case TYPE_sht: estimateDelta(task, sht); break;
	case TYPE_int: estimateDelta(task, int); break;
	case TYPE_lng: estimateDelta(task, lng); break;
	case TYPE_oid: estimateDelta(task, oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateDelta(task, hge); break;
#endif
	}

	return MAL_SUCCEED;
}

// types for safe Integer Promotion for the bitwise operations in getSuffixMask
#define IPbte uint32_t
#define IPsht uint32_t
#define IPint uint32_t
#define IPlng uint64_t
#define IPoid uint64_t
#ifdef HAVE_HGE
#define IPhge uhge
#endif

#define IPTpe(TPE) IP##TPE

#define DELTAcompress(TASK, TPE)\
do {\
	TPE *src = getSrc(TPE, (TASK));\
	BUN i = 0;\
	BUN limit = estimate->cnt;\
	BitVector base;\
	MosaicBlkHeader_delta_t* parameters = (MosaicBlkHeader_delta_t*) ((TASK))->blk;\
	determineDeltaParameters(*parameters, src, limit, TPE);\
	(TASK)->dst = MOScodevectorDelta(TASK);\
	base = (BitVector) ((TASK)->dst);\
	TPE pv = parameters->init.val##TPE; /*previous value*/\
	/*Initial delta is zero.*/\
	setBitVector(base, 0, parameters->bits, (unsigned int) 0);\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (parameters->bits - 1);\
\
	for(i = 1; i < parameters->base.cnt; i++) {\
		/*TODO: assert that delta's actually does not cause an overflow. */\
		TPE cv = *++src; /*current value*/\
		DeltaTpe(TPE) delta = (DeltaTpe(TPE)) (cv > pv ? (IPTpe(TPE)) (cv - pv) : (IPTpe(TPE)) ((sign_mask) | (IPTpe(TPE)) (pv - cv)));\
		setBitVector(base, i, parameters->bits, (unsigned int) /*TODO: fix this once we have increased capacity of bitvector*/ delta);\
		pv = cv;\
	}\
	(TASK)->dst += toEndOfBitVector(i, parameters->bits);\
} while(0)

void
MOScompress_delta(MOStask task, MosaicBlkRec* estimate)
{
	MosaicBlk blk = task->blk;

	MOSsetTag(blk,MOSAIC_DELTA);
	MOSsetCnt(blk, 0);

	switch(ATOMbasetype(task->type)) {
	case TYPE_bte: DELTAcompress(task, bte); break;
	case TYPE_sht: DELTAcompress(task, sht); break;
	case TYPE_int: DELTAcompress(task, int); break;
	case TYPE_lng: DELTAcompress(task, lng); break;
	case TYPE_oid: DELTAcompress(task, oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: DELTAcompress(task, hge); break;
#endif
	}
}

#define ACCUMULATE(acc, delta, sign_mask, TPE) \
(\
	(TPE) (\
		( (sign_mask) & (delta) )?\
			((acc) -= (DeltaTpe(TPE)) (~(IPTpe(TPE)) (sign_mask) & (IPTpe(TPE)) (delta))) :\
			((acc) += (DeltaTpe(TPE)) (~(IPTpe(TPE)) (sign_mask) & (IPTpe(TPE)) (delta)))  \
	)\
)

#define DELTAdecompress(TASK, TPE)\
do {\
	MosaicBlkHeader_delta_t* parameters = (MosaicBlkHeader_delta_t*) ((TASK))->blk;\
	BUN lim = parameters->base.cnt;\
	((TPE*)(TASK)->src)[0] = parameters->init.val##TPE; /*previous value*/\
	BitVector base = (BitVector) MOScodevectorDelta(TASK);\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (parameters->bits - 1);\
	DeltaTpe(TPE) acc = (DeltaTpe(TPE)) parameters->init.val##TPE /*unsigned accumulating value*/;\
	BUN i;\
	for(i = 0; i < lim; i++) {\
		DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);\
		((TPE*)(TASK)->src)[i] = ACCUMULATE(acc, delta, sign_mask, TPE);\
	}\
	(TASK)->src += i * sizeof(TPE);\
} while(0)

void
MOSdecompress_delta(MOStask task)
{
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DELTAdecompress(task, bte); break;
	case TYPE_sht: DELTAdecompress(task, sht); break;
	case TYPE_int: DELTAdecompress(task, int); break;
	case TYPE_lng: DELTAdecompress(task, lng); break;
	case TYPE_oid: DELTAdecompress(task, oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: DELTAdecompress(task, hge); break;
#endif
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

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
#define  select_delta_general(LOW, HIGH, LI, HI, HAS_NIL, ANTI, TPE) \
{\
	MosaicBlkHeader_delta_t* parameters = (MosaicBlkHeader_delta_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorDelta(task);\
	TPE acc = parameters->init.val##TPE; /*previous value*/\
	int bits = parameters->bits;\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);\
	if		( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && !(ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++){\
				DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
				TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
				MOSskipit();\
				if (IS_NIL(TPE, value))\
					*o++ = (oid) first;\
			}\
		}\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && (ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++){\
				DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
				TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
				MOSskipit();\
				if (!IS_NIL(TPE, value))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && !((LI) && (HI)) && !(ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++){\
				DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
				TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
				MOSskipit();\
				if (!IS_NIL(TPE, value))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && !((LI) && (HI)) && (ANTI)) {\
			/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, (LOW)) &&  !IS_NIL(TPE, (HIGH)) && (LOW) == (HIGH) && !((LI) && (HI)) && (ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++) {\
				DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
				TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
				MOSskipit();\
				if (!IS_NIL(TPE, value))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else if	( !IS_NIL(TPE, (LOW)) &&  !IS_NIL(TPE, (HIGH)) && (LOW) == (HIGH) && !((LI) && (HI)) && !(ANTI)) {\
		/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, (LOW)) &&  !IS_NIL(TPE, (HIGH)) && (LOW) > (HIGH) && !(ANTI)) {\
		/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, (LOW)) &&  !IS_NIL(TPE, (HIGH)) && (LOW) > (HIGH) && (ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++){\
				DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
				TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
				MOSskipit();\
				if (!IS_NIL(TPE, value))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else {\
		/*normal cases.*/\
		if( IS_NIL(TPE, (LOW)) ){\
			for( ; first < last; first++,i++){\
				DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
				TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
				MOSskipit();\
				if (HAS_NIL && IS_NIL(TPE, value)) { continue;}\
				bool cmp  =  (((HI) && value <= (HIGH) ) || (!(HI) && value < (HIGH) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		} else\
		if( IS_NIL(TPE, (HIGH)) ){\
			for( ; first < last; first++,i++){\
				DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
				TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
				MOSskipit();\
				if (HAS_NIL && IS_NIL(TPE, value)) { continue;}\
				bool cmp  =  (((LI) && value >= (LOW) ) || (!(LI) && value > (LOW) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++,i++){\
				DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
				TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
				MOSskipit();\
				if (HAS_NIL && IS_NIL(TPE, value)) { continue;}\
				bool cmp  =  (((HI) && value <= (HIGH) ) || (!(HI) && value < (HIGH) )) &&\
						(((LI) && value >= (LOW) ) || (!(LI) && value > (LOW) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

#define select_delta(TPE) {\
	if( nil && *anti) {\
		select_delta_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, true, true, TPE);\
	}\
	if( !nil && *anti) {\
		select_delta_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, false, true, TPE);\
	}\
	if( nil && !*anti) {\
		select_delta_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, true, false, TPE);\
	}\
	if( !nil && !*anti) {\
		select_delta_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, false, false, TPE);\
	}\
}

str
MOSselect_delta( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti) {
	oid *o;
	BUN i = 0,first,last;
	// set the oid range covered
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

		if (task->cl && *task->cl > last){
		MOSadvance_delta(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_delta(bte); break;
	case TYPE_sht: select_delta(sht); break;
	case TYPE_int: select_delta(int); break;
	case TYPE_lng: select_delta(lng); break;
	case TYPE_oid: select_delta(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_delta(hge); break;
#endif
	}
	MOSadvance_delta(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_delta_normalized(HAS_NIL, ANTI, TPE) \
for( ; first < last; first++,i++){\
	DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
	TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
	MOSskipit();\
	if (HAS_NIL && IS_NIL(TPE, value)) { continue;}\
	bool cmp =  (IS_NIL(TPE, low) || value >= low) && (value <= hgh || IS_NIL(TPE, hgh)) ;\
	if (cmp == !(ANTI))\
		*o++ = (oid) first;\
}\

#define thetaselect_delta_general(HAS_NIL, TPE)\
{ 	TPE low,hgh;\
    MosaicBlkHeader_delta_t* parameters = (MosaicBlkHeader_delta_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorDelta(task);\
	TPE acc = parameters->init.val##TPE; /*previous value*/\
	int bits = parameters->bits;\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);\
	low= hgh = TPE##_nil;\
	if ( strcmp(oper,"<") == 0){\
		hgh= *(TPE*) val;\
		hgh = PREVVALUE##TPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *(TPE*) val;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low = *(TPE*) val;\
		low = NEXTVALUE##TPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low = *(TPE*) val;\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		low = hgh = *(TPE*) val;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) val;\
	} \
	if (!anti) {\
		thetaselect_delta_normalized(HAS_NIL, false, TPE);\
	}\
	else {\
		thetaselect_delta_normalized(HAS_NIL, true, TPE);\
	}\
}

#define thetaselect_delta(TPE) {\
	if( nil ){\
		thetaselect_delta_general(true, TPE);\
	}\
	else /*!nil*/{\
		thetaselect_delta_general(false, TPE);\
	}\
}

str
MOSthetaselect_delta( MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN i=0,first,last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	if (task->cl && *task->cl > last){
		MOSskip_delta(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_delta(bte); break;
	case TYPE_sht: thetaselect_delta(sht); break;
	case TYPE_int: thetaselect_delta(int); break;
	case TYPE_lng: thetaselect_delta(lng); break;
	case TYPE_oid: thetaselect_delta(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_delta(hge); break;
#endif
	}
	MOSskip_delta(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_delta(TPE)\
{	TPE *v;\
	v= (TPE*) task->src;\
    MosaicBlkHeader_delta_t* parameters = (MosaicBlkHeader_delta_t*) task->blk;\
	TPE acc = parameters->init.val##TPE; /*previous value*/\
	int bits = parameters->bits;\
	BitVector base = (BitVector) MOScodevectorDelta(task);\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);\
	for(; first < last; first++,i++){\
		DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
		TPE value = ACCUMULATE(acc, delta, sign_mask, TPE);\
		MOSskipit();\
		*v++ = value;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_delta( MOStask task)
{
	BUN i=0, first, last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_delta(bte); break;
		case TYPE_sht: projection_delta(sht); break;
		case TYPE_int: projection_delta(int); break;
		case TYPE_lng: projection_delta(lng); break;
		case TYPE_oid: projection_delta(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_delta(hge); break;
#endif
	}
	MOSskip_delta(task);
	return MAL_SUCCEED;
}

#define join_delta_general(HAS_NIL, NIL_MATCHES, TPE)\
{   TPE *w;\
    MosaicBlkHeader_delta_t* parameters = (MosaicBlkHeader_delta_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorDelta(task);\
	TPE acc;\
	int bits = parameters->bits;\
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);\
	w = (TPE*) task->src;\
	for(n = task->stop, o = 0; n -- > 0; w++,o++){\
		for(acc = parameters->init.val##TPE, i=0, oo= (oid) first; oo < (oid) last; oo++,i++){\
			DeltaTpe(TPE) delta = (DeltaTpe(TPE)) getBitVector(base,i,bits);\
			TPE value 			= ACCUMULATE(acc, delta, sign_mask, TPE);\
			if (HAS_NIL && !NIL_MATCHES) {\
				if (IS_NIL(TPE, value)) { continue;}\
			}\
			if (ARE_EQUAL(*w, value, HAS_NIL, TPE)){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED )\
				throw(MAL,"mosaic.delta",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

#define join_delta(TPE) {\
	if( nil && nil_matches){\
		join_delta_general(true, true, TPE);\
	}\
	if( !nil && nil_matches){\
		join_delta_general(false, true, TPE);\
	}\
	if( nil && !nil_matches){\
		join_delta_general(true, false, TPE);\
	}\
	if( !nil && !nil_matches){\
		join_delta_general(false, false, TPE);\
	}\
}

str
MOSjoin_delta( MOStask task, bit nil_matches)
{
	BUN i= 0,n,first,last;
	oid o, oo;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_delta(bte); break;
		case TYPE_sht: join_delta(sht); break;
		case TYPE_int: join_delta(int); break;
		case TYPE_lng: join_delta(lng); break;
		case TYPE_oid: join_delta(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_delta(hge); break;
#endif
	}
	MOSskip_delta(task);
	return MAL_SUCCEED;
}
