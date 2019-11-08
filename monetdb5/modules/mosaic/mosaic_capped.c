/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */


/*
 * 2014-2016 author Martin Kersten
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
#include "mosaic_capped.h"
#include "mosaic_dictionary.h"
#include "mosaic_private.h"
#include "group.h"

bool MOStypes_capped(BAT* b) {
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

// Create a larger capped buffer then we allow for in the mosaic header first
// Store the most frequent ones in the compressed heap header directly based on estimated savings
// Improve by using binary search rather then linear scan
#define TMPDICT 16*CAPPEDDICT

typedef union{
	bte valbte[TMPDICT];
	sht valsht[TMPDICT];
	int valint[TMPDICT];
	lng vallng[TMPDICT];
	oid valoid[TMPDICT];
	flt valflt[TMPDICT];
	dbl valdbl[TMPDICT];
#ifdef HAVE_HGE
	hge valhge[TMPDICT];
#endif
} _DictionaryData;

typedef struct _CappedParameters_t {
	MosaicBlkRec base;
} MosaicBlkHeader_capped_t;

typedef struct _GlobalCappedInfo {
	BAT* dict;
	BAT* temp_dict;
	EstimationParameters parameters;
} GlobalCappedInfo;

#define GET_BASE(INFO, TPE)			((TPE*) Tloc((INFO)->dict, 0))
#define GET_COUNT(INFO)				(BATcount((INFO)->dict))
#define GET_CAP(INFO)				(BATcapacity((INFO)->dict))
#define GET_DELTA_COUNT(INFO)		((INFO)->parameters.delta_count)
#define GET_BITS(INFO)				((INFO)->parameters.bits)
#define GET_BITS_EXTENDED(INFO)		((INFO)->parameters.bits_extended)
#define EXTEND(INFO, new_capacity)	(BATextend((INFO)->dict, new_capacity) == GDK_SUCCEED)

// task dependent macro's
#define GET_FINAL_DICT(TASK, TPE) (((TPE*) (TASK)->bsrc->tvmosaic->base) + (TASK)->hdr->pos_capped)
#define GET_FINAL_BITS(TASK) ((TASK)->hdr->bits_capped)
#define GET_FINAL_DICT_COUNT(TASK) ((TASK)->hdr->length_capped);\

#define PresentInTempDictFuncDef(TPE) \
static inline \
bool presentInTempDict##TPE(GlobalCappedInfo* info, TPE val) {\
	TPE* dict = (TPE*) Tloc(info->temp_dict, 0);\
	BUN dict_count = BATcount(info->temp_dict);\
	BUN key = find_value_##TPE(dict, dict_count, val);\
	return key < dict_count && ((dict[key] == val) || (IS_NIL(TPE, dict[key]) && IS_NIL(TPE, dict[key])) )  ;\
}

#define CONDITIONAL_INSERT(INFO, VAL, TPE) presentInTempDict##TPE((INFO), (VAL))

#define DictionaryClass(TPE) \
find_value_DEF(TPE)\
PresentInTempDictFuncDef(TPE)\
insert_into_dict_DEF(TPE)\
extend_delta_DEF(TPE, GlobalCappedInfo)\
merge_delta_Into_dictionary_DEF(TPE, GlobalCappedInfo)\
compress_dictionary_DEF(TPE)\
decompress_dictionary_DEF(TPE)\
select_dictionary_DEF(TPE)\
thetaselect_dictionary_DEF(TPE)\
join_dictionary_DEF(TPE)

DictionaryClass(bte)
DictionaryClass(sht)
DictionaryClass(int)
DictionaryClass(lng)
DictionaryClass(oid)
DictionaryClass(flt)
DictionaryClass(dbl)
#ifdef HAVE_HGE
DictionaryClass(hge)
#endif

#define GetTypeWidth(INFO)			((INFO)->dict->twidth)
#define GetSizeInBytes(INFO)		(BATcount((INFO)->dict) * GetTypeWidth(INFO))

void
MOSadvance_capped(MOStask task)
{
	int *dst = (int*)  MOScodevectorDict(task);
	BUN cnt = MOSgetCnt(task->blk);
	BUN bytes;

	assert(cnt > 0);
	task->start += (oid) cnt;
	bytes =  (cnt * GET_FINAL_BITS(task))/8 + (((cnt * GET_FINAL_BITS(task)) %8) != 0);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, int));
}

void
MOSlayout_capped_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	unsigned int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];
	(void) boutput;

	BUN dictsize = GET_COUNT(task->capped_info);

	for(i=0; i< dictsize; i++){
		snprintf(buf, BUFSIZ,"capped[%u]",i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			// BUNappend(boutput, MOSgetDictFreq(dict_hdr, i), false) != GDK_SUCCEED ||
			BUNappend(bproperties, bufv, false) != GDK_SUCCEED)
		return;
	}
}

void
MOSlayout_capped(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	BUN cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * GET_FINAL_BITS(task))/8 + (((cnt * GET_FINAL_BITS(task)) %8) != 0);
	if( BUNappend(btech, "capped blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSskip_capped(MOStask task)
{
	MOSadvance_capped(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

str
MOSprepareEstimate_capped(MOStask task)
{
	str error;

	GlobalCappedInfo** info = &task->capped_info;
	BAT* source = task->bsrc;

	if ( (*info = GDKmalloc(sizeof(GlobalCappedInfo))) == NULL ) {
		throw(MAL,"mosaic.capped",MAL_MALLOC_FAIL);	
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

	BAT *cand_capped_dict;
	if (BATfirstn(&cand_capped_dict, NULL, freq, NULL, NULL, CAPPEDDICT, true, true, false) != GDK_SUCCEED) {
		BBPunfix(next->batCacheid);
		BBPunfix(freq->batCacheid);
		error = createException(MAL, "mosaic.capped.BATfirstn_unique", GDK_EXCEPTION);
		return error;
	}
	BBPunfix(freq->batCacheid);

	BAT* dict;
	if ( (dict = BATproject(next, source)) == NULL) {
		BBPunfix(next->batCacheid);
		BBPunfix(cand_capped_dict->batCacheid);
		throw(MAL, "mosaic.createGlobalDictInfo.BATproject", GDK_EXCEPTION);
	}
	BBPunfix(next->batCacheid);

	BAT *capped_dict;
	if ((capped_dict = BATproject(cand_capped_dict, dict)) == NULL) {
		BBPunfix(cand_capped_dict->batCacheid);
		BBPunfix(dict->batCacheid);
		error = createException(MAL, "mosaic.capped.BATproject", GDK_EXCEPTION);
		return error;
	}
	BBPunfix(cand_capped_dict->batCacheid);
	BBPunfix(dict->batCacheid);

	BAT* sorted_capped_dict;
	if (BATsort(&sorted_capped_dict, NULL, NULL, capped_dict, NULL, NULL, false, false, false) != GDK_SUCCEED) {
		BBPunfix(capped_dict->batCacheid);
		error = createException(MAL, "mosaic.capped.BATfirstn_unique", GDK_EXCEPTION);
		return error;
	}
	BBPunfix(capped_dict->batCacheid);

	BAT* final_capped_dict;
	if ((final_capped_dict = COLnew(0, sorted_capped_dict->ttype, 0, TRANSIENT)) == NULL) {
		BBPunfix(sorted_capped_dict->batCacheid);
		error = createException(MAL, "mosaic.capped.COLnew", GDK_EXCEPTION);
		return error;
	}

	(*info)->temp_dict = sorted_capped_dict;
	(*info)->dict = final_capped_dict;

	return MAL_SUCCEED;
}

#define estimate_dict(TASK, CURRENT, TPE)\
do {\
	/*TODO*/\
	GlobalCappedInfo* info = TASK->capped_info;\
	BUN limit = (BUN) ((TASK)->stop - (TASK)->start > MOSAICMAXCNT? MOSAICMAXCNT: (TASK)->stop - (TASK)->start);\
	TPE* val = getSrc(TPE, (TASK));\
	BUN delta_count;\
	BUN nr_compressed;\
\
	BUN old_keys_size	= ((CURRENT)->nr_capped_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;\
	BUN old_dict_size	= GET_COUNT(info) * sizeof(TPE);\
	BUN old_headers_size	= (CURRENT)->nr_capped_encoded_blocks * (MosaicBlkSize + sizeof(TPE));\
	BUN old_bytes		= old_keys_size + old_dict_size + old_headers_size;\
\
	if (extend_delta_##TPE(&nr_compressed, &delta_count, limit, info, val)) {\
		throw(MAL, "mosaic.capped", MAL_MALLOC_FAIL);\
	}\
\
	(CURRENT)->is_applicable = nr_compressed > 0;\
	(CURRENT)->nr_capped_encoded_elements += nr_compressed;\
	(CURRENT)->nr_capped_encoded_blocks++;\
\
	BUN new_keys_size	= ((CURRENT)->nr_capped_encoded_elements * GET_BITS_EXTENDED(info)) / CHAR_BIT;\
	BUN new_dict_size	= (delta_count + GET_COUNT(info)) * sizeof(TPE);\
	BUN new_headers_size	= (CURRENT)->nr_capped_encoded_blocks * (MosaicBlkSize + sizeof(TPE));\
	BUN new_bytes		= new_keys_size + new_dict_size + new_headers_size;\
\
	(CURRENT)->compression_strategy.tag = MOSAIC_CAPPED;\
	(CURRENT)->compression_strategy.cnt = (unsigned int) nr_compressed;\
\
	(CURRENT)->uncompressed_size	+= (BUN) ( nr_compressed * sizeof(TPE));\
	(CURRENT)->compressed_size		+= (BUN) (wordaligned( MosaicBlkSize, BitVector) + new_bytes - old_bytes);\
} while (0)

// calculate the expected reduction using DICT in terms of elements compressed
str
MOSestimate_capped(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
{
	(void) previous;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: estimate_dict(task, current, bte); break;
	case TYPE_sht: estimate_dict(task, current, sht); break;
	case TYPE_int: estimate_dict(task, current, int); break;
	case TYPE_oid: estimate_dict(task, current, oid); break;
	case TYPE_lng: estimate_dict(task, current, lng); break;
	case TYPE_flt: estimate_dict(task, current, flt); break;
	case TYPE_dbl: estimate_dict(task, current, dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimate_dict(task, current, hge); break;
#endif
	}
	return MAL_SUCCEED;
}

#define postEstimate(TASK, TPE) merge_delta_Into_dictionary_##TPE( (TASK)->capped_info)

void
MOSpostEstimate_capped(MOStask task) {
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: postEstimate(task, bte); break; 
	case TYPE_sht: postEstimate(task, sht); break;
	case TYPE_int: postEstimate(task, int); break;
	case TYPE_lng: postEstimate(task, lng); break;
	case TYPE_oid: postEstimate(task, oid); break;
	case TYPE_flt: postEstimate(task, flt); break;
	case TYPE_dbl: postEstimate(task, dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: postEstimate(task, hge); break;
#endif
	}
}

static str
_finalizeDictionary(BAT* b, GlobalCappedInfo* info, BUN* pos_dict, BUN* length_dict, bte* bits_dict) {
	Heap* vmh = b->tvmosaic;
	BUN size_in_bytes = vmh->free + GetSizeInBytes(info);
	if (HEAPextend(vmh, size_in_bytes, true) != GDK_SUCCEED) {
		throw(MAL, "mosaic.mergeDictionary_capped.HEAPextend", GDK_EXCEPTION);
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
	*bits_dict = calculateBits(*length_dict);

	BBPreclaim(info->dict);
	BBPreclaim(info->temp_dict);

	GDKfree(info);

	return MAL_SUCCEED;
}

str
finalizeDictionary_capped(MOStask task) {
	return _finalizeDictionary(
		task->bsrc,
		task->capped_info,
		&task->hdr->pos_capped,
		&task->hdr->length_capped,
		&GET_FINAL_BITS(task));
}

void
MOScompress_capped(MOStask task, MosaicBlkRec* estimate)
{
	MosaicBlk blk = task->blk;

	task->dst = MOScodevectorDict(task);

	MOSsetTag(blk,MOSAIC_CAPPED);
	MOSsetCnt(blk,0);

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
MOSdecompress_capped(MOStask task)
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

str
MOSselect_capped(MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTselect(bte); break;
	case TYPE_sht: DICTselect(sht); break;
	case TYPE_int: DICTselect(int); break;
	case TYPE_lng: DICTselect(lng); break;
	case TYPE_oid: DICTselect(oid); break;
	case TYPE_flt: DICTselect(flt); break;
	case TYPE_dbl: DICTselect(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTselect(hge); break;
#endif
	}
	MOSskip_capped(task);
	return MAL_SUCCEED;
}

str
MOSthetaselect_capped( MOStask task, void *val, str oper)
{
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTthetaselect(bte); break;
	case TYPE_sht: DICTthetaselect(sht); break;
	case TYPE_int: DICTthetaselect(int); break;
	case TYPE_lng: DICTthetaselect(lng); break;
	case TYPE_oid: DICTthetaselect(oid); break;
	case TYPE_flt: DICTthetaselect(flt); break;
	case TYPE_dbl: DICTthetaselect(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTthetaselect(hge); break;
#endif
	}
	MOSskip_capped(task);
	return MAL_SUCCEED;
}

str
MOSprojection_capped( MOStask task)
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
	MOSskip_capped(task);
	return MAL_SUCCEED;
}

str
MOSjoin_capped( MOStask task, bit nil_matches)
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
	MOSskip_capped(task);
	return MAL_SUCCEED;
}
