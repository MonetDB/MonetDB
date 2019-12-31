/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
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

bool MOStypes_prefix(BAT* b) {
	switch(b->ttype){
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

#define MOSadvance_DEF(TPE)\
MOSadvance_SIGNATURE(prefix, TPE)\
{\
	MOSBlockHeaderTpe(prefix, TPE)* parameters = (MOSBlockHeaderTpe(prefix, TPE)*) (task)->blk;\
	BUN cnt = MOSgetCnt(task->blk);\
\
	assert(cnt > 0);\
	assert(MOSgetTag(task->blk) == MOSAIC_PREFIX);\
\
	task->start += (oid) cnt;\
\
	char* blk = (char*)task->blk;\
	blk += sizeof(MOSBlockHeaderTpe(prefix, TPE));\
	blk += BitVectorSize(cnt, parameters->suffix_bits);\
	blk += GET_PADDING(task->blk, prefix, TPE);\
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

#define OverShift(TPE) ((sizeof(IPTpe(TPE)) - sizeof(PrefixTpe(TPE))) * CHAR_BIT)
#define getSuffixMask(SUFFIX_BITS, TPE) ((PrefixTpe(TPE)) (~(~((IPTpe(TPE)) (0)) << (SUFFIX_BITS))))
#define getPrefixMask(PREFIX_BITS, TPE) ((PrefixTpe(TPE)) ( (~(~((IPTpe(TPE)) (0)) >> (PREFIX_BITS))) >> OverShift(TPE)))

#define determinePrefixParameters(PARAMETERS, SRC, LIMIT, TPE) \
do {\
	PrefixTpe(TPE) *val = (PrefixTpe(TPE)*) (SRC);\
	const int type_size_in_bits = sizeof(PrefixTpe(TPE))  * CHAR_BIT;\
	bte suffix_bits = 1;\
	bte prefix_bits = type_size_in_bits - suffix_bits;\
	PrefixTpe(TPE) prefix_mask = getPrefixMask(prefix_bits, TPE);\
	PrefixTpe(TPE) prefix = *val & prefix_mask;\
	/*TODO: add additional loop to find best bit wise upper bound*/\
	BUN i;\
	for(i = 0; i < (LIMIT); i++, val++){\
		bte current_prefix_bits = prefix_bits;\
		bte current_suffix_bits = suffix_bits;\
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
	(PARAMETERS).rec.cnt = (unsigned int) i;\
	(PARAMETERS).suffix_bits = suffix_bits;\
	(PARAMETERS).prefix = prefix;\
} while(0)

#define MOSestimate_DEF(TPE) \
MOSestimate_SIGNATURE(prefix, TPE)\
{\
	(void) previous;\
	current->is_applicable = true;\
	current->compression_strategy.tag = MOSAIC_PREFIX;\
	PrefixTpe(TPE) *src = ((PrefixTpe(TPE)*) task->src) + task->start;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	MOSBlockHeaderTpe(prefix, TPE) parameters;\
	determinePrefixParameters(parameters, src, limit, TPE);\
	assert(parameters.rec.cnt > 0);/*Should always compress.*/\
\
	BUN store;\
	int bits;\
	int i = parameters.rec.cnt;\
	bits = i * parameters.suffix_bits;\
	store = 2 * sizeof(MOSBlockHeaderTpe(prefix, TPE));\
	store += wordaligned(bits/CHAR_BIT + ((bits % CHAR_BIT) > 0), lng);\
	assert(i > 0);/*Should always compress.*/\
	current->is_applicable = true;\
\
	current->uncompressed_size += (BUN) (i * sizeof(TPE));\
	current->compressed_size += store;\
	current->compression_strategy.cnt = (unsigned int) parameters.rec.cnt;\
\
	if (parameters.rec.cnt > *current->max_compression_length ) {\
		*current->max_compression_length = parameters.rec.cnt;\
	}\
\
	return MAL_SUCCEED;\
}

MOSestimate_DEF(bte)
MOSestimate_DEF(sht)
MOSestimate_DEF(int)
MOSestimate_DEF(lng)
#ifdef HAVE_HGE
MOSestimate_DEF(hge)
#endif

#define MOSpostEstimate_DEF(TPE)\
MOSpostEstimate_SIGNATURE(prefix, TPE)\
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
MOScompress_SIGNATURE(prefix, TPE)\
{\
	ALIGN_BLOCK_HEADER(task,  prefix, TPE);\
\
	MosaicBlk blk = task->blk;\
	MOSsetTag(blk,MOSAIC_PREFIX);\
	MOSsetCnt(blk, 0);\
	PrefixTpe(TPE)* src = (PrefixTpe(TPE)*) getSrc(TPE, task);\
	BUN i = 0;\
	BUN limit = estimate->cnt;\
	MOSBlockHeaderTpe(prefix, TPE)* parameters = (MOSBlockHeaderTpe(prefix, TPE)*) (task)->blk;\
	determinePrefixParameters(*parameters, src, limit, TPE);\
	BitVector base = MOScodevectorPrefix(task, TPE);\
	task->dst = (char*) base;\
	PrefixTpe(TPE) suffix_mask = getSuffixMask(parameters->suffix_bits, TPE);\
	for(i = 0; i < MOSgetCnt(task->blk); i++, src++) {\
		/*TODO: assert that prefix's actually does not cause an overflow. */\
		PrefixTpe(TPE) suffix = *src & suffix_mask;\
		setBitVector(base, i, parameters->suffix_bits, (BitVectorChunk) /*TODO: fix this once we have increased capacity of bitvector*/ suffix);\
	}\
	task->dst += BitVectorSize(i, parameters->suffix_bits);\
}

MOScompress_DEF(bte)
MOScompress_DEF(sht)
MOScompress_DEF(int)
MOScompress_DEF(lng)
#ifdef HAVE_HGE
MOScompress_DEF(hge)
#endif

#define MOSdecompress_DEF(TPE) \
MOSdecompress_SIGNATURE(prefix, TPE)\
{\
	MOSBlockHeaderTpe(prefix, TPE)* parameters = (MOSBlockHeaderTpe(prefix, TPE)*) (task)->blk;\
	BUN lim = MOSgetCnt(task->blk);\
    PrefixTpe(TPE) prefix = parameters->prefix;\
	BitVector base = (BitVector) MOScodevectorPrefix(task, TPE);\
	BUN i;\
	for(i = 0; i < lim; i++){\
		PrefixTpe(TPE) suffix = getBitVector(base, i, parameters->suffix_bits);\
		/*TODO: assert that suffix's actually does not cause an overflow. */\
		PrefixTpe(TPE) val = prefix | suffix;\
		((PrefixTpe(TPE)*)task->src)[i] = val;\
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

#define scan_loop_prefix(TPE, CI_NEXT, TEST) {\
	MOSBlockHeaderTpe(prefix, TPE)* parameters = (MOSBlockHeaderTpe(prefix, TPE)*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task, TPE);\
	PrefixTpe(TPE) prefix = parameters->prefix;\
	bte suffix_bits = parameters->suffix_bits;\
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
    MOSBlockHeaderTpe(prefix, TPE)* parameters = (MOSBlockHeaderTpe(prefix, TPE)*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task, TPE);\
	PrefixTpe(TPE) prefix = parameters->prefix;\
	bte suffix_bits = parameters->suffix_bits;\
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
    MOSBlockHeaderTpe(prefix, TPE)* parameters = (MOSBlockHeaderTpe(prefix, TPE)*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task, TPE);\
	PrefixTpe(TPE) prefix = parameters->prefix;\
	bte suffix_bits = parameters->suffix_bits;\
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
