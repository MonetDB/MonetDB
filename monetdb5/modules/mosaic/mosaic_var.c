/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB b.V.
 */


/*
 * 2014-2016 author Martin Kersten
 * Global info encoding
 * Index value zero is not used to easy detection of filler values
 * The info index size is derived from the number of entries covered.
 * It leads to a compact n-bit representation.
 * Floating points are not expected to be replicated 
 * A limit of 256 elements is currently assumed.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_var.h"
#include "mosaic_private.h"
#include "mosaic_dictionary.h"

bool MOStypes_var(BAT* b) {
	switch (b->ttype){
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
MOSadvance_var(MOStask task)
{
	int *dst = (int*)  MOScodevectorDict(task);
	BUN cnt = MOSgetCnt(task->blk);
	long bytes;

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->stop = task->stop;
	bytes =  (long) (cnt * task->hdr->bits_var)/8 + (((cnt * task->hdr->bits_var) %8) != 0);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, BitVectorChunk)); 
}

typedef struct {
	MosaicBlkRec base;
	unsigned int bits;
	unsigned int dictsize;
	unsigned char offset; // padding for type alignment of the actual info entries.
} MOSDictHdr_t;

#define MOSgetDictFreq(DICTIONARY, KEY) ((BUN*)(((char*) DICTIONARY) + wordaligned(sizeof(DICTIONARY), BUN))[KEY])

typedef struct _GlobalVarInfo {
	BAT* dict;
	EstimationParameters parameters;
} GlobalVarInfo;

#define GET_BASE(INFO, TPE)				((TPE*) Tloc((INFO)->dict, 0))
#define GET_COUNT(INFO)					(BATcount((INFO)->dict))
#define GET_CAP(INFO)					(BATcapacity((INFO)->dict))
#define GET_DELTA_COUNT(INFO)				((INFO)->parameters.delta_count)
#define GET_BITS(INFO)					((INFO)->parameters.bits)
#define GET_BITS_EXTENDED(INFO)			((INFO)->parameters.bits_extended)
#define EXTEND(INFO, new_capacity)		(BATextend((INFO)->dict, new_capacity) == GDK_SUCCEED)
#define CONDITIONAL_INSERT(INFO, VAL, TPE)	(true)

// task dependent macro's
#define GET_FINAL_DICT(TASK, TPE) (((TPE*) (TASK)->bsrc->tvmosaic->base) + (TASK)->hdr->pos_var)
#define GET_FINAL_BITS(TASK) ((TASK)->hdr->bits_var)
#define GET_FINAL_DICT_COUNT(TASK) ((TASK)->hdr->length_var);\

#define DictionaryClass(TPE) \
find_value_DEF(TPE)\
insert_into_dict_DEF(TPE)\
extend_delta_DEF(TPE, GlobalVarInfo)\
merge_delta_Into_dictionary_DEF(TPE, GlobalVarInfo)\
compress_dictionary_DEF(TPE)\
decompress_dictionary_DEF(TPE)\
join_dictionary_DEF(TPE)

DictionaryClass(bte)
DictionaryClass(sht)
DictionaryClass(int)
DictionaryClass(oid)
DictionaryClass(lng)
DictionaryClass(flt)
DictionaryClass(dbl)
#ifdef HAVE_HGE
DictionaryClass(hge)
#endif

#define GetSizeInBytes(INFO) (BATcount((INFO)->dict) * (INFO)->dict->twidth)

void
MOSlayout_var_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	unsigned int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];
	(void) boutput;

	BUN dictsize = GET_COUNT(task->var_info);

	for(i=0; i< dictsize; i++){
		snprintf(buf, BUFSIZ,"var[%u]",i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			// BUNappend(boutput, MOSgetDictFreq(dict_hdr, i), false) != GDK_SUCCEED ||
			BUNappend(bproperties, bufv, false) != GDK_SUCCEED)
		return;
	}
}

void
MOSlayout_var(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * task->hdr->bits_var)/8 + (((cnt * task->hdr->bits_var) %8) != 0);
	if( BUNappend(btech, "var blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSskip_var(MOStask task)
{
	MOSadvance_var(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

str
MOSprepareEstimate_var(MOStask task)
{
	str error;

	GlobalVarInfo** info = &task->var_info;
	BAT* source = task->bsrc;

	if ( (*info = GDKmalloc(sizeof(GlobalVarInfo))) == NULL ) {
		throw(MAL,"mosaic.var",MAL_MALLOC_FAIL);	
	}

	BAT* dict;
	if ((dict = COLnew(0, source->ttype, 0, TRANSIENT)) == NULL) {
		error = createException(MAL, "mosaic.var.COLnew", GDK_EXCEPTION);
		return error;
	}

	(*info)->dict = dict;

	return MAL_SUCCEED;
}

#define estimateDict(TASK, CURRENT, TPE)\
do {\
	/*TODO*/\
	GlobalVarInfo* info = TASK->var_info;\
	BUN limit = (BUN) ((TASK)->stop - (TASK)->start > MOSAICMAXCNT? MOSAICMAXCNT: (TASK)->stop - (TASK)->start);\
	TPE* val = getSrc(TPE, (TASK));\
	BUN delta_count;\
	BUN nr_compressed;\
\
	BUN old_keys_size	= ((CURRENT)->nr_var_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;\
	BUN old_dict_size	= GET_COUNT(info) * sizeof(TPE);\
	BUN old_headers_size	= (CURRENT)->nr_var_encoded_blocks * (MosaicBlkSize + sizeof(TPE));\
	BUN old_bytes		= old_keys_size + old_dict_size + old_headers_size;\
\
	if (extend_delta_##TPE(&nr_compressed, &delta_count, limit, info, val)) {\
		throw(MAL, "mosaic.var", MAL_MALLOC_FAIL);\
	}\
\
	(CURRENT)->is_applicable = true;\
	(CURRENT)->nr_var_encoded_elements += nr_compressed;\
	(CURRENT)->nr_var_encoded_blocks++;\
\
	BUN new_keys_size	= ((CURRENT) -> nr_var_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;\
	BUN new_dict_size	= (delta_count + GET_COUNT(info)) * sizeof(TPE);\
	BUN new_headers_size	= (CURRENT)->nr_var_encoded_blocks * (MosaicBlkSize + sizeof(TPE));\
	BUN new_bytes		= new_keys_size + new_dict_size + new_headers_size;\
\
	(CURRENT)->compression_strategy.tag = MOSAIC_VAR;\
	(CURRENT)->compression_strategy.cnt = (unsigned int) nr_compressed;\
\
	(CURRENT)->uncompressed_size	+= (BUN) ( nr_compressed * sizeof(TPE));\
	(CURRENT)->compressed_size		+= (BUN) (wordaligned( MosaicBlkSize, BitVector) + new_bytes - old_bytes);\
} while (0)

// calculate the expected reduction using INFO in terms of elements compressed
str
MOSestimate_var(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
{	
	(void) previous;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: estimateDict(task, current, bte); break; 
	case TYPE_sht: estimateDict(task, current, sht); break;
	case TYPE_int: estimateDict(task, current, int); break;
	case TYPE_oid: estimateDict(task, current, oid); break;
	case TYPE_lng: estimateDict(task, current, lng); break;
	case TYPE_flt: estimateDict(task, current, flt); break;
	case TYPE_dbl: estimateDict(task, current, dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateDict(task, current, hge); break;
#endif
	}
	return MAL_SUCCEED;
}

#define postEstimate(TASK, TPE) merge_delta_Into_dictionary_##TPE( (TASK)->var_info)

void
MOSpostEstimate_var(MOStask task) {
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: postEstimate(task, bte); break; 
	case TYPE_sht: postEstimate(task, sht); break;
	case TYPE_int: postEstimate(task, int); break;
	case TYPE_oid: postEstimate(task, oid); break;
	case TYPE_lng: postEstimate(task, lng); break;
	case TYPE_flt: postEstimate(task, flt); break;
	case TYPE_dbl: postEstimate(task, dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: postEstimate(task, hge); break;
#endif
	}
}

static str
_finalizeDictionary(BAT* b, GlobalVarInfo* info, BUN* pos_dict, BUN* length_dict, bte* bits_dict) {
	Heap* vmh = b->tvmosaic;
	BUN size_in_bytes = vmh->free + GetSizeInBytes(info);
	if (HEAPextend(vmh, size_in_bytes, true) != GDK_SUCCEED) {
		throw(MAL, "mosaic.mergeDictionary_var.HEAPextend", GDK_EXCEPTION);
	}
	char* dst = vmh->base + vmh->free;
	char* src = info->dict->theap.base;
	/* TODO: consider optimizing this by swapping heaps instead of copying them.*/
	memcpy(dst, src, size_in_bytes);

	vmh->free += (size_t) GetSizeInBytes(info);
	vmh->dirty = true;

	*pos_dict = 0;
	*length_dict = GET_COUNT(info);
	*bits_dict = calculateBits(*length_dict);

	BBPreclaim(info->dict);

	GDKfree(info);

	return MAL_SUCCEED;
}

str
finalizeDictionary_var(MOStask task) {
	return _finalizeDictionary(
		task->bsrc,
		task->var_info,
		&task->hdr->pos_var,
		&task->hdr->length_var,
		&task->hdr->bits_var);
}

void
MOScompress_var(MOStask task, MosaicBlkRec* estimate)
{
	MosaicBlk blk = task->blk;
	task->dst = MOScodevectorDict(task);
	MOSsetTag(blk,MOSAIC_VAR);\

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTcompress(task, bte); break;
	case TYPE_sht: DICTcompress(task, sht); break;
	case TYPE_int: DICTcompress(task, int); break;
	case TYPE_lng: DICTcompress(task, lng); break;
	case TYPE_oid: DICTcompress(task, oid); break;
	case TYPE_flt: DICTcompress(task, flt); break;
	case TYPE_dbl: DICTcompress(task, dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTcompress(task, hge); break;
#endif
	}
}

void
MOSdecompress_var(MOStask task)
{
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTdecompress(task, bte); break;
	case TYPE_sht: DICTdecompress(task, sht); break;
	case TYPE_int: DICTdecompress(task, int); break;
	case TYPE_lng: DICTdecompress(task, lng); break;
	case TYPE_oid: DICTdecompress(task, oid); break;
	case TYPE_flt: DICTdecompress(task, flt); break;
	case TYPE_dbl: DICTdecompress(task, dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTdecompress(task, hge); break;
#endif
	}
}

#define scan_loop_var(TPE, CANDITER_NEXT, TEST) \
    scan_loop_dictionary(TPE, CANDITER_NEXT, TEST)

MOSselect_DEF(var, bte)
MOSselect_DEF(var, sht)
MOSselect_DEF(var, int)
MOSselect_DEF(var, lng)
MOSselect_DEF(var, flt)
MOSselect_DEF(var, dbl)
#ifdef HAVE_HGE
MOSselect_DEF(var, hge)
#endif

str
MOSprojection_var( MOStask task)
{
	switch(ATOMbasetype(task->type)){
		case TYPE_bte: DICTprojection(bte); break;
		case TYPE_sht: DICTprojection(sht); break;
		case TYPE_int: DICTprojection(int); break;
		case TYPE_lng: DICTprojection(lng); break;
		case TYPE_oid: DICTprojection(oid); break;
		case TYPE_flt: DICTprojection(flt); break;
		case TYPE_dbl: DICTprojection(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: DICTprojection(hge); break;
#endif
	}
	MOSskip_var(task);
	return MAL_SUCCEED;
}

str
MOSjoin_var( MOStask task, bit nil_matches)
{
	// set the oid range covered and advance scan range
	switch(ATOMbasetype(task->type)) {
		case TYPE_bte: DICTjoin(bte); break;
		case TYPE_sht: DICTjoin(sht); break;
		case TYPE_int: DICTjoin(int); break;
		case TYPE_lng: DICTjoin(lng); break;
		case TYPE_oid: DICTjoin(oid); break;
		case TYPE_flt: DICTjoin(flt); break;
		case TYPE_dbl: DICTjoin(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: DICTjoin(hge); break;
#endif
	}
	MOSskip_var(task);
	return MAL_SUCCEED;
}
