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
	bytes =  (long) (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, int)); 
}

typedef struct {
	MosaicBlkRec base;
	unsigned int bits;
	unsigned int dictsize;
	unsigned char offset; // padding for type alignment of the actual info entries.
} MOSDictHdr_t;

#define MOSgetDictFreq(DICTIONARY, KEY) ((ulng*)(((char*) DICTIONARY) + wordaligned(sizeof(DICTIONARY), ulng))[KEY])

typedef struct _GlobalVarInfo {
	BAT* dict;
	EstimationParameters parameters;
} GlobalVarInfo;

#define GetBase(INFO, TPE)				((TPE*) Tloc((INFO)->dict, 0))
#define GetCount(INFO)					(BATcount((INFO)->dict))
#define GetSizeInBytes(INFO)			(BATcount((INFO)->dict) * (INFO)->dict->twidth)
#define GetCap(INFO)					(BATcapacity((INFO)->dict))
#define GetDeltaCount(INFO)				((INFO)->parameters.delta_count)
#define GetBits(INFO)					((INFO)->parameters.bits)
#define GetBitsExtended(INFO)			((INFO)->parameters.bits_extended)
#define Extend(INFO, new_capacity)		(BATextend((INFO)->dict, new_capacity) == GDK_SUCCEED)
#define GetValue(info, key, TPE) 		((GetBase(info, TPE))[key])
#define InsertCondition(INFO, VAL, TPE)	(true)

#define DerivedDictionaryClass(TPE) DictionaryClass(TPE, GlobalVarInfo, GetBase, GetCount, GetDeltaCount, GetBits, GetBitsExtended, GetCap, GetValue, Extend, InsertCondition)

DerivedDictionaryClass(bte)
DerivedDictionaryClass(sht)
DerivedDictionaryClass(int)
DerivedDictionaryClass(oid)
DerivedDictionaryClass(lng)
DerivedDictionaryClass(flt)
DerivedDictionaryClass(dbl)
#ifdef HAVE_HGE
DerivedDictionaryClass(hge)
#endif

void
MOSlayout_var_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	unsigned int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];
	(void) boutput;

	BUN dictsize = GetCount(task->var_info);

	for(i=0; i< dictsize; i++){
		snprintf(buf, BUFSIZ,"var[%d]",i);
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
	output =  MosaicBlkSize + (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
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
	BUN limit = (TASK)->stop - (TASK)->start > MOSAICMAXCNT? MOSAICMAXCNT: (TASK)->stop - (TASK)->start;\
	TPE* val = ((TPE*)task->src);\
	BUN delta_count;\
	BUN nr_compressed;\
\
	size_t old_keys_size	= ((CURRENT)->nr_var_encoded_elements * GetBitsExtended(info)) / CHAR_BIT;\
	size_t old_dict_size	= GetCount(info) * sizeof(TPE);\
	size_t old_headers_size	= (CURRENT)->nr_var_encoded_blocks * (MosaicBlkSize + sizeof(TPE));\
	size_t old_bytes		= old_keys_size + old_dict_size + old_headers_size;\
\
	if (estimateDict_##TPE(&nr_compressed, &delta_count, limit, info, val)) {\
		throw(MAL, "mosaic.var", MAL_MALLOC_FAIL);\
	}\
\
	(CURRENT)->is_applicable = true;\
	(CURRENT)->nr_var_encoded_elements += nr_compressed;\
	(CURRENT)->nr_var_encoded_blocks++;\
\
	size_t new_keys_size	= ((CURRENT) -> nr_var_encoded_elements * GetBitsExtended(info)) / CHAR_BIT;\
	size_t new_dict_size	= (delta_count + GetCount(info)) * sizeof(TPE);\
	size_t new_headers_size	= (CURRENT)->nr_var_encoded_blocks * (MosaicBlkSize + sizeof(TPE));\
	size_t new_bytes		= new_keys_size + new_dict_size + new_headers_size;\
\
	(CURRENT)->compression_strategy.tag = MOSAIC_VAR;\
	(CURRENT)->compression_strategy.cnt = nr_compressed;\
\
	(CURRENT)->uncompressed_size	+= (BUN) ( nr_compressed * sizeof(TPE));\
	(CURRENT)->compressed_size		+= (wordaligned( MosaicBlkSize, BitVector) + new_bytes - old_bytes);\
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

#define postEstimate(TASK, TPE) _mergeDeltaIntoDictionary_##TPE( (TASK)->var_info)

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
_finalizeDictionary(BAT* b, GlobalVarInfo* info, ulng* pos_dict, ulng* length_dict, bte* bits_dict) {
	Heap* vmh = b->tvmosaic;
	BUN size_in_bytes = vmh->free + GetSizeInBytes(info);
	if (HEAPextend(vmh, size_in_bytes, true) != GDK_SUCCEED) {
		throw(MAL, "mosaic.mergeDictionary_var.HEAPextend", GDK_EXCEPTION);
	}
	char* dst = vmh->base + vmh->free;
	char* src = info->dict->theap.base;
	/* TODO: consider optimizing this by swapping heaps instead of copying them.*/
	memcpy(dst, src, size_in_bytes);

	vmh->free += info->dict->theap.free;
	vmh->dirty = true;

	*pos_dict = 0;
	*length_dict = GetCount(info);
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

#define GetFinalVarDict(TASK, TPE) (((TPE*) (TASK)->bsrc->tvmosaic->base) + (TASK)->hdr->pos_var)

#define DICTcompress(TASK, TPE) {\
	TPE *val = getSrc(TPE, (TASK));\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT:task->stop - task->start;\
	(TASK)->dst = MOScodevectorDict(TASK);\
	BitVector base = (BitVector) ((TASK)->dst);\
	BUN i;\
	TPE* dict = GetFinalVarDict(TASK, TPE);\
	BUN dict_size = (BUN) task->hdr->length_var;\
	bte bits = task->hdr->bits_var;\
	_compress_dictionary_##TPE(dict, dict_size, &i, val, limit, base, bits);\
	MOSsetCnt((TASK)->blk, i);\
}

void
MOScompress_var(MOStask task)
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

// the inverse operator, extend the src

#define DICTdecompress(TASK, TPE)\
{	BUN lim = MOSgetCnt((TASK)->blk);\
	BitVector base = (BitVector) MOScodevectorDict(TASK);\
	bte bits	= (TASK)->hdr->bits_var;\
	TPE* dict = GetFinalVarDict(TASK, TPE);\
	TPE* dest = (TPE*) (TASK)->src;\
	_decompress_dictionary_##TPE(dict, bits, base, lim, &dest);\
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

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

/* TODO: the select_operator for dictionaries doesn't use
 * the ordered property of the actual global info.
 * Which is a shame because it could in this select operator
 * safe on the info look ups by using the info itself
 * as a reverse index for the ranges of your select predicate.
 * Or if we want to stick to this set up, then we should use a
 * hash based info.
*/

#define select_var(TASK, TPE)\
do {\
	BitVector base = (BitVector) MOScodevectorDict(TASK);\
	bte bits	= (TASK)->hdr->bits_var;\
	TPE* dict = GetFinalVarDict(TASK, TPE);\
	if( !*anti){\
		if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) low) ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,bits); \
				cmp  =  ((*hi && dict[j] <= * (TPE*)hgh ) || (!*hi && dict[j] < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for(i=0; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,bits); \
				cmp  =  ((*li && dict[j] >= * (TPE*)low ) || (!*li && dict[j] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,bits); \
				cmp  =  ((*hi && dict[j] <= * (TPE*)hgh ) || (!*hi && dict[j] < *(TPE*)hgh )) &&\
						((*li && dict[j] >= * (TPE*)low ) || (!*li && dict[j] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		}\
	} else {\
		if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
			/* nothing is matching */\
		} else\
		if( is_nil(TPE, *(TPE*) low) ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,bits); \
				cmp  =  ((*hi && dict[j] <= * (TPE*)hgh ) || (!*hi && dict[j] < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,bits); \
				cmp  =  ((*li && dict[j] >= * (TPE*)low ) || (!*li && dict[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,bits); \
				cmp  =  ((*hi && dict[j] <= * (TPE*)hgh ) || (!*hi && dict[j] < *(TPE*)hgh )) &&\
						((*li && dict[j] >= * (TPE*)low ) || (!*li && dict[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		}\
	}\
} while(0)

str
MOSselect_var(MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN i, first,last;
	int cmp;
	bte j;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_var(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_var(task, bte); break;
	case TYPE_sht: select_var(task, sht); break;
	case TYPE_int: select_var(task, int); break;
	case TYPE_lng: select_var(task, lng); break;
	case TYPE_oid: select_var(task, oid); break;
	case TYPE_flt: select_var(task, flt); break;
	case TYPE_dbl: select_var(task, dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_var(task, hge); break;
#endif
	}
	MOSskip_var(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_var_str(TPE)\
	throw(MAL,"mosaic.var","TBD");

#define thetaselect_var(TPE)\
{ 	TPE low,hgh;\
	BitVector base = (BitVector) MOScodevectorDict(task);\
	bte bits	= task->hdr->bits_var;\
	TPE* dict = GetFinalVarDict(task, TPE);\
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
		hgh= low= *(TPE*) val;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) val;\
	} \
	for( ; first < last; first++){\
		MOSskipit();\
		j= getBitVector(base,first,bits); \
		if( (is_nil(TPE, low) || dict[j] >= low) && (dict[j] <= hgh || is_nil(TPE, hgh)) ){\
			if ( !anti) {\
				*o++ = (oid) first;\
			}\
		} else\
			if( anti){\
				*o++ = (oid) first;\
			}\
	}\
} 

str
MOSthetaselect_var( MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	bte j;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_var(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_var(bte); break;
	case TYPE_sht: thetaselect_var(sht); break;
	case TYPE_int: thetaselect_var(int); break;
	case TYPE_lng: thetaselect_var(lng); break;
	case TYPE_oid: thetaselect_var(oid); break;
	case TYPE_flt: thetaselect_var(flt); break;
	case TYPE_dbl: thetaselect_var(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_var(hge); break;
#endif
	}
	MOSskip_var(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_var(TPE)\
{	TPE *v;\
	BitVector base = (BitVector) MOScodevectorDict(task);\
	bte bits	= task->hdr->bits_var;\
	TPE* dict = GetFinalVarDict(task, TPE);\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++){\
		MOSskipit();\
		j= getBitVector(base,i,bits); \
		*v++ = dict[j];\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_var( MOStask task)
{
	BUN i,first,last;
	unsigned short j;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_var(bte); break;
		case TYPE_sht: projection_var(sht); break;
		case TYPE_int: projection_var(int); break;
		case TYPE_lng: projection_var(lng); break;
		case TYPE_oid: projection_var(oid); break;
		case TYPE_flt: projection_var(flt); break;
		case TYPE_dbl: projection_var(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_var(hge); break;
#endif
	}
	MOSskip_var(task);
	return MAL_SUCCEED;
}

#define join_var_str(TPE)\
	throw(MAL,"mosaic.var","TBD");

#define join_var(TPE)\
{	TPE  *w;\
	BitVector base = (BitVector) MOScodevectorDict(task);\
	bte bits	= task->hdr->bits_var;\
	TPE* dict = GetFinalVarDict(task, TPE);\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->stop; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			j= getBitVector(base,i,bits); \
			if ( *w == dict[j]){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED)\
				throw(MAL,"mosaic.var",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

str
MOSjoin_var( MOStask task)
{
	BUN i,n,limit;
	oid o, oo;
	int j;

	// set the oid range covered and advance scan range
	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_var(bte); break;
		case TYPE_sht: join_var(sht); break;
		case TYPE_int: join_var(int); break;
		case TYPE_lng: join_var(lng); break;
		case TYPE_oid: join_var(oid); break;
		case TYPE_flt: join_var(flt); break;
		case TYPE_dbl: join_var(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_var(hge); break;
#endif
	}
	MOSskip_var(task);
	return MAL_SUCCEED;
}
