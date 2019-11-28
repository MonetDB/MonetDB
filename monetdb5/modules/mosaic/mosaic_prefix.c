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

#define MOScodevectorPrefix(Task) (((char*) (Task)->blk)+ wordaligned(sizeof(MosaicBlkHeader_prefix_t), BitVectorChunk))
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
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned char),BitVectorChunk);
			bytes += wordaligned(getBitVectorSize(cnt,bits), lng);
		}
		break;
	case 2:
		{	unsigned short *dst = (unsigned short*)  MOScodevector(task);
			unsigned short mask = *dst++;
			unsigned short val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned short),BitVectorChunk);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , lng);
		}
		break;
	case 4:
		{	unsigned int *dst = (unsigned int*)  MOScodevector(task);
			unsigned int mask = *dst++;
			unsigned int val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned int),BitVectorChunk);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , lng);
		}
		break;
	case 8:
		{	ulng *dst = (ulng*)  MOScodevector(task);
			ulng mask = *dst++;
			ulng val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(ulng),BitVectorChunk);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , lng);
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
	int *dst = (int*)  (((char*) task->blk) + wordaligned(sizeof(MosaicBlkHeader_prefix_t), BitVectorChunk));
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
		if ((current_suffix_bits > (int) sizeof(BitVectorChunk) * CHAR_BIT)) {\
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
	store = wordaligned(sizeof(MosaicBlkHeader_prefix_t), BitVectorChunk);\
	store += wordaligned(bits/CHAR_BIT + ((bits % CHAR_BIT) > 0), lng);\
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
		setBitVector(base, i, parameters->suffix_bits, (BitVectorChunk) /*TODO: fix this once we have increased capacity of bitvector*/ suffix);\
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

#define scan_loop_prefix(TPE, CI_NEXT, TEST) {\
	MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task);\
	PrefixTpe(TPE) prefix = parameters->prefix.prefix##TPE;\
	int suffix_bits = parameters->suffix_bits;\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CI_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        v = (TPE) (prefix | getBitVector(base,i,suffix_bits));\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

MOSselect_DEF(prefix, bte)
MOSselect_DEF(prefix, sht)
MOSselect_DEF(prefix, int)
MOSselect_DEF(prefix, lng)
#ifdef HAVE_HGE
MOSselect_DEF(prefix, hge)
#endif

#define projection_loop_prefix(TPE, CI_NEXT)\
{\
    MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task);\
	PrefixTpe(TPE) prefix = parameters->prefix.prefix##TPE;\
	int suffix_bits = parameters->suffix_bits;\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CI_NEXT(task->ci)) {\
		BUN i = (BUN) (o - first);\
		TPE value =  (TPE) (prefix | getBitVector(base,i,suffix_bits));\
		*bt++ = value;\
		task->cnt++;\
	}\
}

MOSprojection_DEF(prefix, bte)
MOSprojection_DEF(prefix, sht)
MOSprojection_DEF(prefix, int)
MOSprojection_DEF(prefix, lng)
#ifdef HAVE_HGE
MOSprojection_DEF(prefix, hge)
#endif

#define outer_loop_prefix(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
    MosaicBlkHeader_prefix_t* parameters = (MosaicBlkHeader_prefix_t*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task);\
	PrefixTpe(TPE) prefix = parameters->prefix.prefix##TPE;\
	int suffix_bits = parameters->suffix_bits;\
	for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
		BUN i = (BUN) (lo - first);\
		TPE lval =  (TPE) (prefix | getBitVector(base,i,suffix_bits));\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, lval))) {continue;};\
		}\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
}

MOSjoin_COUI_DEF(prefix, bte)
MOSjoin_COUI_DEF(prefix, sht)
MOSjoin_COUI_DEF(prefix, int)
MOSjoin_COUI_DEF(prefix, lng)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(prefix, hge)
#endif
