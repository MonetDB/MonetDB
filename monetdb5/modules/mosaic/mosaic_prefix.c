/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * Bit_prefix compression
 * Factor out leading bits from a series of values.
 * The prefix size is determined by looking ahead in a small block.
 * To use the bitvector, we limit the extracted tail to at most 32bits
 * The administration are 2 TPE values (mask,reference value)
 * The size of the residu is stored in the reference value lower bits
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_prefix.h"
#include "mosaic_private.h"
#include "gdk_bitvector.h"

bool MOStypes_prefix(BAT* b) {
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

#define Prefixbte uint8_t
#define Prefixsht uint16_t
#define Prefixint uint32_t
#define Prefixlng uint64_t
#define Prefixoid uint64_t
#define Prefixflt uint32_t
#define Prefixdbl uint64_t
#ifdef HAVE_HGE
#define Prefixhge uhge
#endif

#define PrefixTpe(TPE) Prefix##TPE

typedef struct MosaicBlkHeader_prefix_t_ {
	MosaicBlkRec base;
	int suffix_bits;
	union {
		PrefixTpe(bte) prefixbte;
		PrefixTpe(sht) prefixsht;
		PrefixTpe(int) prefixint;
		PrefixTpe(lng) prefixlng;
		PrefixTpe(oid) prefixoid;
		PrefixTpe(flt) prefixflt;
		PrefixTpe(dbl) prefixdbl;
#ifdef HAVE_HGE
		PrefixTpe(hge) prefixhge;
#endif
	} prefix;

} MosaicBlkHeader_prefix_t;

#define MOScodevectorPrefix(Task) (((char*) (Task)->blk)+ wordaligned(sizeof(MosaicBlkHeader_prefix_t), unsigned int))
#define toEndOfBitVector(CNT, BITS) wordaligned(((CNT) * (BITS) / CHAR_BIT) + ( ((CNT) * (BITS)) % CHAR_BIT != 0 ), lng)

void
MOSlayout_prefix(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	BUN cnt = MOSgetCnt(blk), input=0, output= 0, bytes = 0;
	int bits =0;
	int size = ATOMsize(task->type);
	char buf[32];

	input = cnt * ATOMsize(task->type);
	switch(size){
	case 1:
		{	unsigned char *dst = (unsigned char*)  MOScodevector(task);
			unsigned char mask = *dst++;
			unsigned char val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned char),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits), int);
		}
		break;
	case 2:
		{	unsigned short *dst = (unsigned short*)  MOScodevector(task);
			unsigned short mask = *dst++;
			unsigned short val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned short),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , int);
		}
		break;
	case 4:
		{	unsigned int *dst = (unsigned int*)  MOScodevector(task);
			unsigned int mask = *dst++;
			unsigned int val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned int),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , int);
		}
		break;
	case 8:
		{	ulng *dst = (ulng*)  MOScodevector(task);
			ulng mask = *dst++;
			ulng val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(ulng),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , int);
		}
	}
	output = wordaligned(bytes, int); 
	snprintf(buf,32,"%d bits",bits);
	if( BUNappend(btech, "prefix blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, buf, false) != GDK_SUCCEED)
		return;
}

void
MOSadvance_prefix(MOStask task)
{
	MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) (task)->blk;
	int *dst = (int*)  (((char*) task->blk) + wordaligned(sizeof(MosaicBlkHeader_prefix_t), unsigned int));
	long cnt = parameters->base.cnt;
	long bytes = toEndOfBitVector(cnt, parameters->suffix_bits);

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->blk = (MosaicBlk) (((char*) dst)  + bytes);
}

void
MOSskip_prefix(MOStask task)
{
	MOSadvance_prefix(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}
#define OverShift(TPE) ((sizeof(IPTpe(TPE)) - sizeof(PrefixTpe(TPE))) * CHAR_BIT)
#define getSuffixMask(SUFFIX_BITS, TPE) ((PrefixTpe(TPE)) (~(~((IPTpe(TPE)) (0)) << (SUFFIX_BITS))))
#define getPrefixMask(PREFIX_BITS, TPE) ((PrefixTpe(TPE)) ( (~(~((IPTpe(TPE)) (0)) >> (PREFIX_BITS))) >> OverShift(TPE)))

#define determinePrefixParameters(PARAMETERS, SRC, LIMIT, TPE) \
do {\
	PrefixTpe(TPE) *val = (PrefixTpe(TPE)*) (SRC);\
	const int type_size_in_bits = sizeof(PrefixTpe(TPE))  * CHAR_BIT;\
	int suffix_bits = 1;\
	int prefix_bits = type_size_in_bits - suffix_bits;\
	PrefixTpe(TPE) prefix_mask = getPrefixMask(prefix_bits, TPE);\
	PrefixTpe(TPE) prefix = *val & prefix_mask;\
	/*TODO: add additional loop to find best bit wise upper bound*/\
	BUN i;\
	for(i = 0; i < (LIMIT); i++, val++){\
		int current_prefix_bits = prefix_bits;\
		int current_suffix_bits = suffix_bits;\
		PrefixTpe(TPE) current_prefix = prefix;\
		PrefixTpe(TPE) current_prefix_mask =  prefix_mask;\
\
		while ((current_prefix) != (current_prefix_mask & (*val))) {\
			current_prefix_bits--;\
			current_prefix_mask = getPrefixMask(current_prefix_bits, TPE);\
			current_prefix = prefix & current_prefix_mask;\
			current_suffix_bits++;\
		}\
\
		if (current_suffix_bits >= (int) ((sizeof(PrefixTpe(TPE)) * CHAR_BIT) / 2)) {\
			/*If we can not compress better then the half of the original data type, we give up. */\
			break;\
		}\
		if ((current_suffix_bits > (int) sizeof(unsigned int) * CHAR_BIT)) {\
			/*TODO: this extra condition should be removed once bitvector is extended to int64's*/\
			break;\
		}\
\
		prefix = current_prefix;\
		prefix_mask = current_prefix_mask;\
		prefix_bits = current_prefix_bits;\
		suffix_bits = current_suffix_bits;\
\
		assert (suffix_bits + prefix_bits == type_size_in_bits);\
		assert( (prefix | (getSuffixMask(suffix_bits, TPE) & (*val))) == *val);\
	}\
\
	(PARAMETERS).base.cnt = (unsigned int) i;\
	(PARAMETERS).suffix_bits = suffix_bits;\
	(PARAMETERS).prefix.prefix##TPE = prefix;\
} while(0)

#define estimate_prefix(TASK, TPE)\
do {\
	PrefixTpe(TPE) *src = ((PrefixTpe(TPE)*) (TASK)->src) + (TASK)->start;\
	BUN limit = (TASK)->stop - (TASK)->start > MOSAICMAXCNT? MOSAICMAXCNT: (TASK)->stop - (TASK)->start;\
	MosaicBlkHeader_prefix_t parameters;\
	determinePrefixParameters(parameters, src, limit, TPE);\
	assert(parameters.base.cnt > 0);/*Should always compress.*/\
\
	BUN store;\
	int bits;\
	int i = parameters.base.cnt;\
	bits = i * parameters.suffix_bits;\
	store = wordaligned(sizeof(MosaicBlkHeader_prefix_t),int);\
	store += wordaligned(bits/8 + ((bits % 8) >0),int);\
	assert(i > 0);/*Should always compress.*/\
	current->is_applicable = true;\
\
	current->uncompressed_size += (BUN) (i * sizeof(TPE));\
	current->compressed_size += store;\
	current->compression_strategy.cnt = (unsigned int) parameters.base.cnt;\
} while (0)

// calculate the expected reduction 
str
MOSestimate_prefix(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
{
	(void) previous;
	current->is_applicable = true;
	current->compression_strategy.tag = MOSAIC_PREFIX;

	switch(ATOMbasetype(task->type)) {
		case TYPE_bte: estimate_prefix(task, bte); break;
		case TYPE_sht: estimate_prefix(task, sht); break;
		case TYPE_int: estimate_prefix(task, int); break;
		case TYPE_lng: estimate_prefix(task, lng); break;
		case TYPE_oid: estimate_prefix(task, oid); break;
		case TYPE_flt: estimate_prefix(task, flt);	break;
		case TYPE_dbl: estimate_prefix(task, dbl); break;
	#ifdef HAVE_HGE
		case TYPE_hge: estimate_prefix(task, hge); break;
	#endif
	}
	return MAL_SUCCEED;
}

#define compress_prefix(TASK, TPE)\
do {\
	PrefixTpe(TPE)* src = (PrefixTpe(TPE)*) getSrc(TPE, TASK);\
	BUN i = 0;\
	BUN limit = estimate->cnt;\
	BitVector base;\
	MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) ((TASK))->blk;\
	determinePrefixParameters(*parameters, src, limit, TPE);\
	(TASK)->dst = MOScodevectorPrefix(TASK);\
	base = (BitVector) ((TASK)->dst);\
	PrefixTpe(TPE) suffix_mask = getSuffixMask(parameters->suffix_bits, TPE);\
	for(i = 0; i < parameters->base.cnt; i++, src++) {\
		/*TODO: assert that delta's actually does not cause an overflow. */\
		PrefixTpe(TPE) suffix = *src & suffix_mask;\
		setBitVector(base, i, parameters->suffix_bits, (unsigned int) /*TODO: fix this once we have increased capacity of bitvector*/ suffix);\
	}\
	(TASK)->dst += toEndOfBitVector(i, parameters->suffix_bits);\
} while(0)

void
MOScompress_prefix(MOStask task, MosaicBlkRec* estimate)
{
	MosaicBlk blk = task->blk;

	MOSsetTag(blk,MOSAIC_PREFIX);
	MOSsetCnt(blk, 0);

	switch(ATOMbasetype(task->type)) {
		case TYPE_bte: compress_prefix(task, bte); break;
		case TYPE_sht: compress_prefix(task, sht); break;
		case TYPE_int: compress_prefix(task, int); break;
		case TYPE_lng: compress_prefix(task, lng); break;
		case TYPE_oid: compress_prefix(task, oid); break;
		case TYPE_flt: compress_prefix(task, flt); break;
		case TYPE_dbl: compress_prefix(task, dbl); break;
	#ifdef HAVE_HGE
		case TYPE_hge: compress_prefix(task, hge); break;
	#endif
	}
}

// the inverse operator, extend the src

#define decompress_prefix(TASK, TPE)\
do {\
	MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) ((TASK))->blk;\
	BUN lim = parameters->base.cnt;\
    PrefixTpe(TPE) prefix = parameters->prefix.prefix##TPE;\
	BitVector base = (BitVector) MOScodevectorPrefix(TASK);\
	BUN i;\
	for(i = 0; i < lim; i++){\
		PrefixTpe(TPE) suffix = getBitVector(base, i, parameters->suffix_bits);\
		/*TODO: assert that suffix's actually does not cause an overflow. */\
		PrefixTpe(TPE) val = prefix | suffix;\
		((PrefixTpe(TPE)*)(TASK)->src)[i] = val;\
	}\
	(TASK)->src += i * sizeof(TPE);\
} while(0)

void
MOSdecompress_prefix(MOStask task)
{
	switch(ATOMbasetype(task->type)) {
		case TYPE_bte: decompress_prefix(task, bte); break;
		case TYPE_sht: decompress_prefix(task, sht); break;
		case TYPE_int: decompress_prefix(task, int); break;
		case TYPE_lng: decompress_prefix(task, lng); break;
		case TYPE_oid: decompress_prefix(task, oid); break;
		case TYPE_flt: decompress_prefix(task, flt); break;
		case TYPE_dbl: decompress_prefix(task, dbl); break;
	#ifdef HAVE_HGE
		case TYPE_hge: decompress_prefix(task, hge); break;
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
#define  select_prefix_general(LOW, HIGH, LI, HI, HAS_NIL, ANTI, TPE) \
{\
	MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task);\
	PrefixTpe(TPE) prefix = parameters->prefix.prefix##TPE;\
	int suffix_bits = parameters->suffix_bits;\
	if		( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && !(ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++){\
				MOSskipit();\
				PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
				TPE* value =  (TPE*) &pvalue;\
				if (IS_NIL(TPE, *value))\
					*o++ = (oid) first;\
			}\
		}\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && (ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++){\
				MOSskipit();\
				PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
				TPE* value =  (TPE*) &pvalue;\
				if (!IS_NIL(TPE, *value))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && !((LI) && (HI)) && !(ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++){\
				MOSskipit();\
				PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
				TPE* value =  (TPE*) &pvalue;\
				if (!IS_NIL(TPE, *value))\
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
			for( ; first < last; first++){\
				MOSskipit();\
				PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
				TPE* value =  (TPE*) &pvalue;\
				if (!IS_NIL(TPE, *value))\
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
				MOSskipit();\
				PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
				TPE* value =  (TPE*) &pvalue;\
				if (!IS_NIL(TPE, *value))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else {\
		/*normal cases.*/\
		if( IS_NIL(TPE, (LOW)) ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
				TPE* value =  (TPE*) &pvalue;\
				if (HAS_NIL && IS_NIL(TPE, *value)) { continue;}\
				bool cmp  =  (((HI) && *value <= (HIGH) ) || (!(HI) && *value < (HIGH) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		} else\
		if( IS_NIL(TPE, (HIGH)) ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
				TPE* value =  (TPE*) &pvalue;\
				if (HAS_NIL && IS_NIL(TPE, *value)) { continue;}\
				bool cmp  =  (((LI) && *value >= (LOW) ) || (!(LI) && *value > (LOW) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
				TPE* value =  (TPE*) &pvalue;\
				if (HAS_NIL && IS_NIL(TPE, *value)) { continue;}\
				bool cmp  =  (((HI) && *value <= (HIGH) ) || (!(HI) && *value < (HIGH) )) &&\
						(((LI) && *value >= (LOW) ) || (!(LI) && *value > (LOW) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

#define select_prefix(TPE) {\
	if( nil && *anti){\
		select_prefix_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, true, true, TPE);\
	}\
	if( !nil && *anti){\
		select_prefix_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, false, true, TPE);\
	}\
	if( nil && !*anti){\
		select_prefix_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, true, false, TPE);\
	}\
	if( !nil && !*anti){\
		select_prefix_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, false, false, TPE);\
	}\
}

str
MOSselect_prefix( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	BUN i = 0,first,last;
	// set the oid range covered
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

		if (task->cl && *task->cl > last){
		MOSadvance_prefix(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_prefix(bte); break;
	case TYPE_sht: select_prefix(sht); break;
	case TYPE_int: select_prefix(int); break;
	case TYPE_lng: select_prefix(lng); break;
	case TYPE_oid: select_prefix(oid); break;
	case TYPE_flt: select_prefix(flt); break;
	case TYPE_dbl: select_prefix(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_prefix(hge); break;
#endif
	}
	MOSadvance_prefix(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_prefix_normalized(HAS_NIL, ANTI, TPE) \
for( ; first < last; first++,i++){\
	MOSskipit();\
	PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
	TPE* value =  (TPE*) &pvalue;\
	if (HAS_NIL && IS_NIL(TPE, *value)) { continue;}\
	bool cmp = ( (IS_NIL(TPE, low) || *value >= low) && (*value <= hgh || IS_NIL(TPE, hgh)) );\
	if (cmp == !(ANTI))\
		*o++ = (oid) first;\
}

#define thetaselect_prefix_general(HAS_NIL, TPE)\
{ 	TPE low,hgh;\
    MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task);\
	PrefixTpe(TPE) prefix = parameters->prefix.prefix##TPE;\
	int suffix_bits = parameters->suffix_bits;\
	low= hgh = TPE##_nil;\
	if ( strcmp(oper,"<") == 0){\
		hgh= *(TPE*) input;\
		hgh = PREVVALUE##TPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *(TPE*) input;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low = *(TPE*) input;\
		low = NEXTVALUE##TPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low = *(TPE*) input;\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		low = hgh = *(TPE*) input;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) input;\
	} \
	if (!anti) {\
		thetaselect_prefix_normalized(HAS_NIL, false, TPE);\
	}\
	else {\
		thetaselect_prefix_normalized(HAS_NIL, true, TPE);\
	}\
}

#define thetaselect_prefix(TPE) {\
	if( nil ){\
		thetaselect_prefix_general(true, TPE);\
	}\
	else /*!nil*/{\
		thetaselect_prefix_general(false, TPE);\
	}\
}

str
MOSthetaselect_prefix( MOStask task, void *input, str oper)
{
	oid *o;
	int anti=0;
	BUN i=0,first,last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	if (task->cl && *task->cl > last){
		MOSskip_prefix(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_prefix(bte); break;
	case TYPE_sht: thetaselect_prefix(sht); break;
	case TYPE_int: thetaselect_prefix(int); break;
	case TYPE_lng: thetaselect_prefix(lng); break;
	case TYPE_oid: thetaselect_prefix(oid); break;
	case TYPE_flt: thetaselect_prefix(flt); break;
	case TYPE_dbl: thetaselect_prefix(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_prefix(hge); break;
#endif
	}
	MOSskip_prefix(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_prefix(TPE)\
{	TPE *v;\
    MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task);\
	PrefixTpe(TPE) prefix = parameters->prefix.prefix##TPE;\
	int suffix_bits = parameters->suffix_bits;\
	v= (TPE*) task->src;\
	for(; first < last; first++,i++){\
		MOSskipit();\
		PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
		TPE* value =  (TPE*) &pvalue;\
		*v++ = *value;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_prefix( MOStask task)
{ 
	BUN i=0, first,last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_prefix(bte); break;
		case TYPE_sht: projection_prefix(sht); break;
		case TYPE_int: projection_prefix(int); break;
		case TYPE_lng: projection_prefix(lng); break;
		case TYPE_oid: projection_prefix(oid); break;
		case TYPE_flt: projection_prefix(flt); break;
		case TYPE_dbl: projection_prefix(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_prefix(hge); break;
#endif
	}
	MOSskip_prefix(task);
	return MAL_SUCCEED;
}

#define join_prefix_general(HAS_NIL, NIL_MATCHES, TPE)\
{   TPE *w;\
    MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task);\
	PrefixTpe(TPE) prefix = parameters->prefix.prefix##TPE;\
	int suffix_bits = parameters->suffix_bits;\
	w = (TPE*) task->src;\
	for(n = task->stop, o = 0; n -- > 0; w++,o++){\
		for(i=0, oo= (oid) first; oo < (oid) last; oo++,i++){\
			PrefixTpe(TPE) pvalue = prefix | getBitVector(base,i,suffix_bits);\
			TPE* value =  (TPE*) &pvalue;\
			if (HAS_NIL && !NIL_MATCHES) {\
				if (IS_NIL(TPE, *value)) { continue;}\
			}\
			if (ARE_EQUAL(*w, *value, HAS_NIL, TPE)){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED )\
				throw(MAL,"mosaic.prefix",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

#define join_prefix(TPE) {\
	if( nil && nil_matches){\
		join_prefix_general(true, true, TPE);\
	}\
	if( !nil && nil_matches){\
		join_prefix_general(false, true, TPE);\
	}\
	if( nil && !nil_matches){\
		join_prefix_general(true, false, TPE);\
	}\
	if( !nil && !nil_matches){\
		join_prefix_general(false, false, TPE);\
	}\
}

str
MOSjoin_prefix( MOStask task, bit nil_matches)
{
	BUN i= 0,n,first,last;
	oid o, oo;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_prefix(bte); break;
		case TYPE_sht: join_prefix(sht); break;
		case TYPE_int: join_prefix(int); break;
		case TYPE_lng: join_prefix(lng); break;
		case TYPE_oid: join_prefix(oid); break;
		case TYPE_flt: join_prefix(flt); break;
		case TYPE_dbl: join_prefix(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_prefix(hge); break;
#endif
	}
	MOSskip_prefix(task);
	return MAL_SUCCEED;
}
