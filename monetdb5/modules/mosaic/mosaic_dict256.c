/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */


/*
 * authors Martin Kersten, A. Koning
 * Global dictionary encoding
 * Index value zero is not used to easy detection of filler values
 * The dictionary index size is derived from the number of entries covered.
 * It leads to a compact n-bit representation.
 * Floating points are not expected to be replicated.
 * A limit of 256 elements is currently assumed.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_dict256.h"
#include "mosaic_private.h"
#include "group.h"

bool MOStypes_dict256(BAT* b) {
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

#define CAPPEDDICT 256

// Create a larger dict256 buffer then we allow for in the mosaic header first
// Store the most frequent ones in the compressed heap header directly based on estimated savings
// Improve by using binary search rather then linear scan
#define TMPDICT 16*CAPPEDDICT

typedef union{
	bte valbte[TMPDICT];
	sht valsht[TMPDICT];
	int valint[TMPDICT];
	lng vallng[TMPDICT];
	flt valflt[TMPDICT];
	dbl valdbl[TMPDICT];
#ifdef HAVE_HGE
	hge valhge[TMPDICT];
#endif
} _DictionaryData;

typedef struct _CappedParameters_t {
	MosaicBlkRec base;
} MosaicBlkHeader_dict256_t;

typedef struct _GlobalCappedInfo {
	BAT* dict;
	BAT* temp_dict;
	EstimationParameters parameters;
} GlobalCappedInfo;

#define PresentInTempDictFuncDef(TPE) \
static inline \
bool presentInTempDict##TPE(GlobalCappedInfo* info, TPE val) {\
	TPE* dict = (TPE*) Tloc(info->temp_dict, 0);\
	BUN dict_count = BATcount(info->temp_dict);\
	BUN key = find_value_##TPE(dict, dict_count, val);\
	return key < dict_count && ((dict[key] == val) || (IS_NIL(TPE, dict[key]) && IS_NIL(TPE, dict[key])) )  ;\
}

#define CONDITIONAL_INSERT_dict256(INFO, VAL, TPE) presentInTempDict##TPE((INFO), (VAL))

#define DictionaryClass(TPE) \
find_value_DEF(TPE)\
PresentInTempDictFuncDef(TPE)\
insert_into_dict_DEF(TPE)\
extend_delta_DEF(dict256, TPE, GlobalCappedInfo)\
merge_delta_Into_dictionary_DEF(TPE, GlobalCappedInfo)\
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

#define GetTypeWidth(INFO)			((INFO)->dict->twidth)
#define GetSizeInBytes(INFO)		(BATcount((INFO)->dict) * GetTypeWidth(INFO))

#define MOSadvance_DEF(TPE)\
MOSadvance_SIGNATURE(dict256, TPE) advance_dictionary(dict256, TPE)

MOSadvance_DEF(bte)
MOSadvance_DEF(sht)
MOSadvance_DEF(int)
MOSadvance_DEF(lng)
MOSadvance_DEF(flt)
MOSadvance_DEF(dbl)
#ifdef HAVE_HGE
MOSadvance_DEF(hge)
#endif

void
MOSlayout_dict256_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	unsigned int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];
	(void) boutput;

	BUN dictsize = GET_COUNT(task->dict256_info);

	for(i=0; i< dictsize; i++){
		snprintf(buf, BUFSIZ,"dict256[%u]",i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			// BUNappend(boutput, MOSgetDictFreq(dict_hdr, i), false) != GDK_SUCCEED ||
			BUNappend(bproperties, bufv, false) != GDK_SUCCEED)
		return;
	}
}

void
MOSlayout_dict256(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	BUN cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * GET_FINAL_BITS(task, dict256))/8 + (((cnt * GET_FINAL_BITS(task, dict256)) %8) != 0);
	if( BUNappend(btech, "dict256 blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

str
MOSprepareEstimate_dict256(MOStask task)
{
	str error;

	GlobalCappedInfo** info = &task->dict256_info;
	BAT* source = task->bsrc;

	if ( (*info = GDKmalloc(sizeof(GlobalCappedInfo))) == NULL ) {
		throw(MAL,"mosaic.dict256",MAL_MALLOC_FAIL);	
	}

	BAT *ngid, *next, *freq;

	BAT * source_view;
	if ((source_view = VIEWcreate(source->hseqbase, source)) == NULL) {
		throw(MAL, "mosaic.createGlobalDictInfo.VIEWcreate", GDK_EXCEPTION);
	}

	if (BATgroup(&ngid, &next, &freq, source_view, NULL, NULL, NULL, NULL) != GDK_SUCCEED) {
		BBPunfix(source_view->batCacheid);
		throw(MAL, "mosaic.createGlobalDictInfo.BATgroup", GDK_EXCEPTION);
	}
	BBPunfix(source_view->batCacheid);
	BBPunfix(ngid->batCacheid);

	BAT *cand_dict256_dict;
	if (BATfirstn(&cand_dict256_dict, NULL, freq, NULL, NULL, CAPPEDDICT, false, true, false) != GDK_SUCCEED) {
		BBPunfix(next->batCacheid);
		BBPunfix(freq->batCacheid);
		error = createException(MAL, "mosaic.dict256.BATfirstn_unique", GDK_EXCEPTION);
		return error;
	}
	BBPunfix(freq->batCacheid);

	BAT* dict;
	if ( (dict = BATproject(next, source)) == NULL) {
		BBPunfix(next->batCacheid);
		BBPunfix(cand_dict256_dict->batCacheid);
		throw(MAL, "mosaic.createGlobalDictInfo.BATproject", GDK_EXCEPTION);
	}
	BBPunfix(next->batCacheid);

	BAT *dict256_dict;
	if ((dict256_dict = BATproject(cand_dict256_dict, dict)) == NULL) {
		BBPunfix(cand_dict256_dict->batCacheid);
		BBPunfix(dict->batCacheid);
		error = createException(MAL, "mosaic.dict256.BATproject", GDK_EXCEPTION);
		return error;
	}
	BBPunfix(cand_dict256_dict->batCacheid);
	BBPunfix(dict->batCacheid);

	BAT* sorted_dict256_dict;
	if (BATsort(&sorted_dict256_dict, NULL, NULL, dict256_dict, NULL, NULL, false, false, false) != GDK_SUCCEED) {
		BBPunfix(dict256_dict->batCacheid);
		error = createException(MAL, "mosaic.dict256.BATfirstn_unique", GDK_EXCEPTION);
		return error;
	}
	BBPunfix(dict256_dict->batCacheid);

	BAT* final_dict256_dict;
	if ((final_dict256_dict = COLnew(0, sorted_dict256_dict->ttype, 0, TRANSIENT)) == NULL) {
		BBPunfix(sorted_dict256_dict->batCacheid);
		error = createException(MAL, "mosaic.dict256.COLnew", GDK_EXCEPTION);
		return error;
	}

	(*info)->temp_dict = sorted_dict256_dict;
	(*info)->dict = final_dict256_dict;

	return MAL_SUCCEED;
}

#define MOSestimate_DEF(TPE) \
MOSestimate_SIGNATURE(dict256, TPE)\
{\
	(void) previous;\
	GlobalCappedInfo* info = task->dict256_info;\
	if (task->start < *(current)->dict256_limit) {\
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
	*(current)->dict256_limit = task->start + limit;\
\
	TPE* val = getSrc(TPE, task);\
	BUN delta_count;\
	BUN nr_compressed;\
\
	BUN old_keys_size		= (current->nr_dict256_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;\
	BUN old_dict_size		= GET_COUNT(info) * sizeof(TPE);\
	BUN old_headers_size	= current->nr_dict256_encoded_blocks * 2 * sizeof(MOSBlockHeaderTpe(dict256, TPE));\
	BUN old_bytes			= old_keys_size + old_dict_size + old_headers_size;\
\
	if (extend_delta_##TPE(&nr_compressed, &delta_count, limit, info, val)) {\
		throw(MAL, "mosaic.dict256", MAL_MALLOC_FAIL);\
	}\
\
	current->is_applicable = nr_compressed > 0;\
	current->nr_dict256_encoded_elements += nr_compressed;\
	current->nr_dict256_encoded_blocks++;\
\
	BUN new_keys_size		= (current->nr_dict256_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;\
	BUN new_dict_size		= (delta_count + GET_COUNT(info)) * sizeof(TPE);\
	BUN new_headers_size	= current->nr_dict256_encoded_blocks * 2 * sizeof(MOSBlockHeaderTpe(dict256, TPE));\
	BUN new_bytes			= new_keys_size + new_dict_size + new_headers_size;\
\
	current->compression_strategy.tag = MOSAIC_DICT256;\
	current->compression_strategy.cnt = (unsigned int) nr_compressed;\
\
	current->uncompressed_size	+= (BUN) ( nr_compressed * sizeof(TPE));\
	current->compressed_size		+= (BUN) (wordaligned( MosaicBlkSize, BitVectorChunk) + new_bytes - old_bytes);\
\
	return MAL_SUCCEED;\
}

MOSestimate_DEF(bte)
MOSestimate_DEF(sht)
MOSestimate_SIGNATURE(dict256, int)
{
	(void) previous;
	GlobalCappedInfo* info = task->dict256_info;
	if (task->start < *(current)->dict256_limit) {
		/*Dictionary estimation is expensive. So only allow it on disjoint regions.*/
		current->is_applicable = false;
		return MAL_SUCCEED;
	}
	BUN limit = (BUN) (task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start);

	if (*current->max_compression_length != 0 &&  *current->max_compression_length < limit) {
		limit = *current->max_compression_length;
	}

	*(current)->dict256_limit = task->start + limit;

	int* val = getSrc(int, task);
	BUN delta_count;
	BUN nr_compressed;

	BUN old_keys_size		= (current->nr_dict256_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;
	BUN old_dict_size		= GET_COUNT(info) * sizeof(int);
	BUN old_headers_size	= current->nr_dict256_encoded_blocks * 2 * sizeof(MOSBlockHeaderTpe(dict256, int));
	BUN old_bytes			= old_keys_size + old_dict_size + old_headers_size;

	if (extend_delta_int(&nr_compressed, &delta_count, limit, info, val)) {
		throw(MAL, "mosaic.dict256", MAL_MALLOC_FAIL);
	}

	current->is_applicable = nr_compressed > 0;
	current->nr_dict256_encoded_elements += nr_compressed;
	current->nr_dict256_encoded_blocks++;

	BUN new_keys_size		= (current->nr_dict256_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;
	BUN new_dict_size		= (delta_count + GET_COUNT(info)) * sizeof(int);
	BUN new_headers_size	= current->nr_dict256_encoded_blocks * 2 * sizeof(MOSBlockHeaderTpe(dict256, int));
	BUN new_bytes			= new_keys_size + new_dict_size + new_headers_size;

	current->compression_strategy.tag = MOSAIC_DICT256;
	current->compression_strategy.cnt = (unsigned int) nr_compressed;

	current->uncompressed_size	+= (BUN) ( nr_compressed * sizeof(int));
	current->compressed_size		+= (BUN) (wordaligned( MosaicBlkSize, BitVectorChunk) + new_bytes - old_bytes);

	return MAL_SUCCEED;
}
MOSestimate_DEF(lng)
MOSestimate_DEF(flt)
MOSestimate_DEF(dbl)
#ifdef HAVE_HGE
MOSestimate_DEF(hge)
#endif

#define MOSpostEstimate_DEF(TPE)\
MOSpostEstimate_SIGNATURE(dict256, TPE)\
{\
	merge_delta_Into_dictionary_##TPE( task->dict256_info);\
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
_finalizeDictionary(BAT* b, GlobalCappedInfo* info, BUN* pos_dict, BUN* length_dict, bte* bits_dict) {
	Heap* vmh = b->tvmosaic;
	BUN size_in_bytes = vmh->free + GetSizeInBytes(info);
	if (HEAPextend(vmh, size_in_bytes, true) != GDK_SUCCEED) {
		throw(MAL, "mosaic.mergeDictionary_dict256.HEAPextend", GDK_EXCEPTION);
	}
	char* dst = vmh->base + vmh->free;
	char* src = info->dict->theap.base;
	/* TODO: consider optimizing this by swapping heaps instead of copying them.*/
	memcpy(dst, src, GetSizeInBytes(info));

	assert(vmh->free % GetTypeWidth(info) == 0);
	*pos_dict = (vmh->free / GetTypeWidth(info));

	vmh->free += (size_t) GetSizeInBytes(info);
	vmh->dirty = true;

	*length_dict = GET_COUNT(info);
	calculateBits(*bits_dict, *length_dict);

	BBPreclaim(info->dict);
	BBPreclaim(info->temp_dict);

	GDKfree(info);

	return MAL_SUCCEED;
}

str
finalizeDictionary_dict256(MOStask task) {
	return _finalizeDictionary(
		task->bsrc,
		task->dict256_info,
		&task->hdr->pos_dict256,
		&task->hdr->length_dict256,
		&GET_FINAL_BITS(task, dict256));
}

// rather expensive simple value non-compressed store
#define MOScompress_DEF(TPE)\
MOScompress_SIGNATURE(dict256, TPE)\
{\
	MOSsetTag(task->blk, MOSAIC_DICT256);\
	DICTcompress(dict256, TPE);\
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
MOSdecompress_SIGNATURE(dict256, TPE)\
{\
	DICTdecompress(dict256, TPE);\
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

#define scan_loop_dict256(TPE, CI_NEXT, TEST) \
    scan_loop_dictionary(dict256, TPE, CI_NEXT, TEST)

MOSselect_DEF(dict256, bte)
MOSselect_DEF(dict256, sht)
MOSselect_DEF(dict256, int)
MOSselect_DEF(dict256, lng)
MOSselect_DEF(dict256, flt)
MOSselect_DEF(dict256, dbl)
#ifdef HAVE_HGE
MOSselect_DEF(dict256, hge)
#endif

#define projection_loop_dict256(TPE, CI_NEXT) \
    projection_loop_dictionary(dict256, TPE, CI_NEXT)

MOSprojection_DEF(dict256, bte)
MOSprojection_DEF(dict256, sht)
MOSprojection_DEF(dict256, int)
MOSprojection_DEF(dict256, lng)
MOSprojection_DEF(dict256, flt)
MOSprojection_DEF(dict256, dbl)
#ifdef HAVE_HGE
MOSprojection_DEF(dict256, hge)
#endif

#define outer_loop_dict256(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
    outer_loop_dictionary(HAS_NIL, NIL_MATCHES, dict256, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT)

MOSjoin_COUI_DEF(dict256, bte)
MOSjoin_COUI_DEF(dict256, sht)
MOSjoin_COUI_DEF(dict256, int)
MOSjoin_COUI_DEF(dict256, lng)
MOSjoin_COUI_DEF(dict256, flt)
MOSjoin_COUI_DEF(dict256, dbl)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(dict256, hge)
#endif
