/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB b.V.
 */


/*
 * authors Martin Kersten, Aris Koning
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
#include "mosaic_dict.h"
#include "mosaic_private.h"

bool MOStypes_dict(BAT* b) {
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

#define MOSadvance_DEF(TPE)\
MOSadvance_SIGNATURE(dict, TPE) advance_dictionary(dict, TPE)

MOSadvance_DEF(bte)
MOSadvance_DEF(sht)
MOSadvance_DEF(int)
MOSadvance_DEF(lng)
MOSadvance_DEF(flt)
MOSadvance_DEF(dbl)
#ifdef HAVE_HGE
MOSadvance_DEF(hge)
#endif

#define MOSgetDictFreq(DICTIONARY, KEY) ((BUN*)(((char*) DICTIONARY) + wordaligned(sizeof(DICTIONARY), BUN))[KEY])

typedef struct _GlobalVarInfo {
	BAT* dict;
	EstimationParameters parameters;
} GlobalVarInfo;

#define CONDITIONAL_INSERT_dict(INFO, VAL, TPE)	(true)

#define DictionaryClass(TPE) \
find_value_DEF(TPE)\
insert_into_dict_DEF(TPE)\
extend_delta_DEF(dict, TPE, GlobalVarInfo)\
merge_delta_Into_dictionary_DEF(TPE, GlobalVarInfo)\
compress_dictionary_DEF(TPE)\
decompress_dictionary_DEF(TPE)

DictionaryClass(bte)
DictionaryClass(sht)
DictionaryClass(int)
DictionaryClass(lng)
DictionaryClass(flt)
DictionaryClass(dbl)
#ifdef HAVE_HGE
DictionaryClass(hge)
#endif

#define GetSizeInBytes(INFO) (BATcount((INFO)->dict) * (INFO)->dict->twidth)

void
MOSlayout_dict_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	unsigned int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];
	(void) boutput;

	BUN dictsize = GET_COUNT(task->dict_info);

	for(i=0; i< dictsize; i++){
		snprintf(buf, BUFSIZ,"dict[%u]",i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			// BUNappend(boutput, MOSgetDictFreq(dict_hdr, i), false) != GDK_SUCCEED ||
			BUNappend(bproperties, bufv, false) != GDK_SUCCEED)
		return;
	}
}

void
MOSlayout_dict(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * task->hdr->bits_dict)/8 + (((cnt * task->hdr->bits_dict) %8) != 0);
	if( BUNappend(btech, "dict blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

str
MOSprepareEstimate_dict(MOStask task)
{
	str error;

	GlobalVarInfo** info = &task->dict_info;
	BAT* source = task->bsrc;

	if ( (*info = GDKmalloc(sizeof(GlobalVarInfo))) == NULL ) {
		throw(MAL,"mosaic.dict",MAL_MALLOC_FAIL);	
	}

	BAT* dict;
	if ((dict = COLnew(0, source->ttype, 0, TRANSIENT)) == NULL) {
		error = createException(MAL, "mosaic.dict.COLnew", GDK_EXCEPTION);
		return error;
	}

	(*info)->dict = dict;

	return MAL_SUCCEED;
}

#define MOSestimate_DEF(TPE) \
MOSestimate_SIGNATURE(dict, TPE)\
{\
	(void) previous;\
	GlobalVarInfo* info = task->dict_info;\
	if (task->start < *(current)->dict_limit) {\
		/*Dictionary estimation is expensive. So only allow it on disjoint regions.*/\
		current->is_applicable = false;\
		return MAL_SUCCEED;\
	}\
	BUN limit = (BUN) (task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start);\
\
	if (*current->max_compression_length != 0 &&  *current->max_compression_length < limit) {\
		limit = *current->max_compression_length;\
	}\
\
	*(current)->dict_limit = task->start + limit;\
\
	TPE* val = getSrc(TPE, task);\
	BUN delta_count;\
	BUN nr_compressed;\
\
	BUN old_keys_size	= (current->nr_dict_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;\
	BUN old_dict_size	= GET_COUNT(info) * sizeof(TPE);\
	BUN old_headers_size	= current->nr_dict_encoded_blocks * 2 * sizeof(MOSBlockHeaderTpe(dict, TPE));\
	BUN old_bytes		= old_keys_size + old_dict_size + old_headers_size;\
\
	if (extend_delta_##TPE(&nr_compressed, &delta_count, limit, info, val)) {\
		throw(MAL, "mosaic.dict", MAL_MALLOC_FAIL);\
	}\
\
	current->is_applicable = true;\
	current->nr_dict_encoded_elements += nr_compressed;\
	current->nr_dict_encoded_blocks++;\
\
	BUN new_keys_size	= (current -> nr_dict_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;\
	BUN new_dict_size	= (delta_count + GET_COUNT(info)) * sizeof(TPE);\
	BUN new_headers_size	= current->nr_dict_encoded_blocks * 2 * sizeof(MOSBlockHeaderTpe(dict, TPE));\
	BUN new_bytes		= new_keys_size + new_dict_size + new_headers_size;\
\
	current->compression_strategy.tag = MOSAIC_DICT;\
	current->compression_strategy.cnt = (unsigned int) nr_compressed;\
\
	current->uncompressed_size	+= (BUN) ( nr_compressed * sizeof(TPE));\
	current->compressed_size	+= (BUN) (wordaligned( MosaicBlkSize, BitVector) + new_bytes - old_bytes);\
\
	return MAL_SUCCEED;\
}

MOSestimate_DEF(bte)
MOSestimate_DEF(sht)
MOSestimate_DEF(int)
MOSestimate_DEF(lng)
MOSestimate_DEF(flt)
MOSestimate_DEF(dbl)
#ifdef HAVE_HGE
MOSestimate_DEF(hge)
#endif

#define MOSpostEstimate_DEF(TPE)\
MOSpostEstimate_SIGNATURE(dict, TPE)\
{\
	merge_delta_Into_dictionary_##TPE( task->dict_info);\
}

MOSpostEstimate_DEF(bte)
MOSpostEstimate_DEF(sht)
MOSpostEstimate_DEF(int)
MOSpostEstimate_DEF(lng)
MOSpostEstimate_DEF(flt)
MOSpostEstimate_DEF(dbl)
#ifdef HAVE_HGE
MOSpostEstimate_DEF(hge)
#endif

static str
_finalizeDictionary(BAT* b, GlobalVarInfo* info, BUN* pos_dict, BUN* length_dict, bte* bits_dict) {
	Heap* vmh = b->tvmosaic;
	BUN size_in_bytes = vmh->free + GetSizeInBytes(info);
	if (HEAPextend(vmh, size_in_bytes, true) != GDK_SUCCEED) {
		throw(MAL, "mosaic.mergeDictionary_dict.HEAPextend", GDK_EXCEPTION);
	}
	char* dst = vmh->base + vmh->free;
	char* src = info->dict->theap.base;
	/* TODO: consider optimizing this by swapping heaps instead of copying them.*/
	memcpy(dst, src, size_in_bytes);

	vmh->free += (size_t) GetSizeInBytes(info);
	vmh->dirty = true;

	*pos_dict = 0;
	*length_dict = GET_COUNT(info);
	calculateBits(*bits_dict, *length_dict);

	BBPreclaim(info->dict);

	GDKfree(info);

	return MAL_SUCCEED;
}

str
finalizeDictionary_dict(MOStask task) {
	return _finalizeDictionary(
		task->bsrc,
		task->dict_info,
		&task->hdr->pos_dict,
		&task->hdr->length_dict,
		&task->hdr->bits_dict);
}

// rather expensive simple value non-compressed store
#define MOScompress_DEF(TPE)\
MOScompress_SIGNATURE(dict, TPE)\
{\
	MOSsetTag(task->blk, MOSAIC_DICT);\
	DICTcompress(dict, TPE);\
}

MOScompress_DEF(bte)
MOScompress_DEF(sht)
MOScompress_DEF(int)
MOScompress_DEF(lng)
MOScompress_DEF(flt)
MOScompress_DEF(dbl)
#ifdef HAVE_HGE
MOScompress_DEF(hge)
#endif

#define MOSdecompress_DEF(TPE) \
MOSdecompress_SIGNATURE(dict, TPE)\
{\
	DICTdecompress(dict, TPE);\
}

MOSdecompress_DEF(bte)
MOSdecompress_DEF(sht)
MOSdecompress_DEF(int)
MOSdecompress_DEF(lng)
MOSdecompress_DEF(flt)
MOSdecompress_DEF(dbl)
#ifdef HAVE_HGE
MOSdecompress_DEF(hge)
#endif

#define scan_loop_dict(TPE, CI_NEXT, TEST) \
    scan_loop_dictionary(dict, TPE, CI_NEXT, TEST)

MOSselect_DEF(dict, bte)
MOSselect_DEF(dict, sht)
MOSselect_DEF(dict, int)
MOSselect_DEF(dict, lng)
MOSselect_DEF(dict, flt)
MOSselect_DEF(dict, dbl)
#ifdef HAVE_HGE
MOSselect_DEF(dict, hge)
#endif

#define projection_loop_dict(TPE, CI_NEXT) \
    projection_loop_dictionary(dict, TPE, CI_NEXT)

MOSprojection_DEF(dict, bte)
MOSprojection_DEF(dict, sht)
MOSprojection_DEF(dict, int)
MOSprojection_DEF(dict, lng)
MOSprojection_DEF(dict, flt)
MOSprojection_DEF(dict, dbl)
#ifdef HAVE_HGE
MOSprojection_DEF(dict, hge)
#endif

#define outer_loop_dict(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
    outer_loop_dictionary(HAS_NIL, NIL_MATCHES, dict, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT)

MOSjoin_COUI_DEF(dict, bte)
MOSjoin_COUI_DEF(dict, sht)
MOSjoin_COUI_DEF(dict, int)
MOSjoin_COUI_DEF(dict, lng)
MOSjoin_COUI_DEF(dict, flt)
MOSjoin_COUI_DEF(dict, dbl)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(dict, hge)
#endif
