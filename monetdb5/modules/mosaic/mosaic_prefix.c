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
MOSlayout_prefix(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
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

#define NAME prefix
#define METHOD_SPECIFIC_INCLUDE MAKE_TEMPLATE_INCLUDE_FILE(NAME)

#define COMPRESSION_DEFINITION
#define TPE bte
#include METHOD_SPECIFIC_INCLUDE
#undef TPE
#define TPE sht
#include METHOD_SPECIFIC_INCLUDE
#undef TPE
#define TPE int
#include METHOD_SPECIFIC_INCLUDE
#undef TPE
#define TPE lng
#include METHOD_SPECIFIC_INCLUDE
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include METHOD_SPECIFIC_INCLUDE
#undef TPE
#endif
#undef COMPRESSION_DEFINITION

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

#define TPE bte
#include "mosaic_projection_template.h"
#undef TPE
#define TPE sht
#include "mosaic_projection_template.h"
#undef TPE
#define TPE int
#include "mosaic_projection_template.h"
#undef TPE
#define TPE lng
#include "mosaic_projection_template.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_projection_template.h"
#undef TPE
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

#undef METHOD_SPECIFIC_INCLUDE
#undef NAME
