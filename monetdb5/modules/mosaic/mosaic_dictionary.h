/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 */
#ifndef _MOSAIC_DICTIONARY_ 
#define _MOSAIC_DICTIONARY_ 

#include "gdk.h"
#include "gdk_bitvector.h"
#include "mal_exception.h"

 /*TODO: assuming (for now) that bats have nils during compression*/
static const bool nil = true;

#define calculateBits(RES, COUNT)\
{\
	unsigned char bits = 0;\
	while ((COUNT) >> bits) {\
		bits++;\
	}\
	(RES) = bits;\
}

typedef struct _EstimationParameters {
	BUN count;
	unsigned char bits;
	BUN delta_count;
	unsigned char bits_extended; // number of bits required to index the info after the delta would have been merged.
} EstimationParameters;

typedef struct _GlobalDictionaryInfo {
	BAT* dict;
	BAT* temp_bitvector;
	BAT* temp_dict;
	EstimationParameters parameters;
} GlobalDictionaryInfo;


#define GET_BASE(INFO, TPE)			((TPE*) Tloc((INFO)->dict, 0))
#define GET_COUNT(INFO)				(BATcount((INFO)->dict))
#define GET_CAP(INFO)				(BATcapacity((INFO)->dict))
#define GET_DELTA_COUNT(INFO)		((INFO)->parameters.delta_count)
#define GET_BITS(INFO)				((INFO)->parameters.bits)
#define GET_BITS_EXTENDED(INFO)		((INFO)->parameters.bits_extended)
#define EXTEND(INFO, new_capacity)	(BATextend((INFO)->dict, new_capacity) == GDK_SUCCEED)

// task dependent macro's
#define GET_FINAL_DICT(task, NAME, TPE) (((TPE*) (task)->bsrc->tvmosaic->base) + (task)->hdr->pos_##NAME)
#define GET_FINAL_BITS(task, NAME) ((task)->hdr->bits_##NAME)
#define GET_FINAL_DICT_COUNT(task, NAME) ((task)->hdr->length_##NAME);\

#define prepare_estimate_DEF(NAME, CUT_OFF_SIZE) \
str \
MOSprepareEstimate_##NAME(MOStask* task)\
{\
	str error;\
\
	GlobalDictionaryInfo** info = &task->NAME##_info;\
	BAT* source = task->bsrc;\
\
	if ( (*info = GDKmalloc(sizeof(GlobalDictionaryInfo))) == NULL ) {\
		throw(MAL,"mosaic." #NAME ,MAL_MALLOC_FAIL);	\
	}\
\
	BAT *ngid, *next, *freq;\
\
	BAT* source_copy;\
	/* Work around to prevent re-entering batIdxLock in BATsort when it decides to create an ordered/hash indices.\
	 * This is some what expensive since it requires a full copy of the original bat.\
	 * TODO: build vmosaic heap in a occ way.
	 * TODO: there can be a single function that builds both the capped and the global dictionary
	 */\
	if ( (source_copy = COLcopy(source, source->ttype, true /*writable = true*/, TRANSIENT)) == NULL) {\
		error = createException(MAL, "mosaic." #NAME ".COLcopy", GDK_EXCEPTION);\
		return error;\
	}\
\
	if (BATgroup(&ngid, &next, &freq, source_copy, NULL, NULL, NULL, NULL) != GDK_SUCCEED) {\
		BBPunfix(source_copy->batCacheid);\
		throw(MAL, "mosaic.createGlobalDictInfo.BATgroup", GDK_EXCEPTION);\
	}\
	BBPunfix(ngid->batCacheid);\
\
	BAT* unsorted_dict;\
	if (CUT_OFF_SIZE) {\
		BAT *cand_capped_dict;\
		if (BATfirstn(&cand_capped_dict, NULL, freq, NULL, NULL, CUT_OFF_SIZE, false, true, false) != GDK_SUCCEED) {\
			BBPunfix(next->batCacheid);\
			BBPunfix(freq->batCacheid);\
			BBPunfix(source_copy->batCacheid);\
			error = createException(MAL, "mosaic." #NAME ".BATfirstn_unique", GDK_EXCEPTION);\
			return error;\
		}\
\
		BAT* projection_chain[4] = {NULL};\
		projection_chain[0] = cand_capped_dict;\
		projection_chain[1] = next;\
		projection_chain[2] = source_copy;\
		if ((unsorted_dict = BATprojectchain(projection_chain)) == NULL) {\
			BBPunfix(cand_capped_dict->batCacheid);\
			BBPunfix(next->batCacheid);\
			BBPunfix(source_copy->batCacheid);\
			error = createException(MAL, "mosaic." #NAME ".BATprojectchain", GDK_EXCEPTION);\
			return error;\
		}\
	}\
	else {\
		if ((unsorted_dict = BATproject(next, source_copy)) == NULL) {\
			BBPunfix(next->batCacheid);\
			BBPunfix(source_copy->batCacheid);\
			error = createException(MAL, "mosaic." #NAME ".BATproject", GDK_EXCEPTION);\
			return error;\
		}\
	}\
	BBPunfix(freq->batCacheid);\
	BBPunfix(next->batCacheid);\
	BBPunfix(source_copy->batCacheid);\
\
	BAT* sorted_dict;\
	if (BATsort(&sorted_dict, NULL, NULL, unsorted_dict, NULL, NULL, false, false, false) != GDK_SUCCEED) {\
		BBPunfix(unsorted_dict->batCacheid);\
		error = createException(MAL, "mosaic." #NAME ".BATfirstn_unique", GDK_EXCEPTION);\
		return error;\
	}\
	BBPunfix(unsorted_dict->batCacheid);\
\
	BAT* final_dict;\
	if ((final_dict = COLnew(0, sorted_dict->ttype, 0, TRANSIENT)) == NULL) {\
		BBPunfix(sorted_dict->batCacheid);\
		error = createException(MAL, "mosaic." #NAME ".COLnew", GDK_EXCEPTION);\
		return error;\
	}\
\
	BAT* bit_vector;\
	if ((bit_vector = COLnew(0, TYPE_int, BATcount(sorted_dict) / (sizeof(int) * CHAR_BIT), TRANSIENT)) == NULL) {\
		BBPunfix(final_dict->batCacheid);\
		BBPunfix(sorted_dict->batCacheid);\
		error = createException(MAL, "mosaic." #NAME ".COLnew", GDK_EXCEPTION);\
		return error;\
	}\
\
	if (BAThash(sorted_dict) != GDK_SUCCEED) {\
		BBPunfix(sorted_dict->batCacheid);\
		BBPunfix(bit_vector->batCacheid);\
		BBPunfix(final_dict->batCacheid);\
		throw(MAL, "mosaic.createGlobalDictInfo.BAThash", GDK_EXCEPTION);\
\
	}\
\
	(*info)->temp_bitvector = bit_vector;\
	(*info)->temp_dict = sorted_dict;\
	(*info)->dict = final_dict;\
\
	return MAL_SUCCEED;\
}\

#define find_value_DEF(TPE) \
static inline \
BUN find_value_##TPE(TPE* dict, BUN dict_count, TPE val) {\
	BUN m, f= 0, l = dict_count, offset = 0;\
	/* This function assumes that the implementation of a dictionary*/\
	/* is that of a sorted array with nils starting first.*/\
	if (IS_NIL(TPE, val)) return 0;\
	if (dict_count > 0 && IS_NIL(TPE, dict[0])) {\
		/*If the dictionary starts with a nil,*/\
		/*the actual sorted dictionary starts an array entry later.*/\
		dict++;\
		offset++;\
		l--;\
	}\
	while( l-f > 0 ) {\
		m = f + (l-f)/2;\
		if ( val < dict[m]) l=m-1; else f= m;\
		if ( val > dict[m]) f=m+1; else l= m;\
	}\
	return f + offset;\
}
#define insert_into_dict_DEF(TPE) \
static inline \
void insert_into_dict_##TPE(TPE* dict, BUN* dict_count, BUN key, TPE val)\
{\
	TPE w = val;\
\
	if (IS_NIL(TPE, w)) {\
		assert(key == 0);\
		TPE v = dict[key];\
		dict[key] = w;\
\
		if (*dict_count > 0) {\
			w = v;\
		}\
	}\
	\
	if (IS_NIL(TPE, dict[key]) && !IS_NIL(TPE, w)) {\
		assert(key == 0);\
		key++;\
	}\
	for( ; key < *dict_count; key++) {\
		if (dict[key] > w ){\
			TPE v = dict[key];\
			dict[key] = w;\
			w = v;\
		}\
	}\
	(*dict_count)++;\
	dict[key] = w;\
}
#define extend_delta_DEF(NAME, TPE) \
static str \
extend_delta_##TPE(BUN* nr_compressed, BUN* delta_count, BUN limit, GlobalDictionaryInfo* info, TPE* val) {\
	BUN buffer_size = 256;\
	TPE* dict		= (TPE*) GET_BASE(info, TPE);\
	BUN dict_count	= GET_COUNT(info);\
	TPE* delta		= dict + dict_count;\
	*delta_count	= 0;\
	for((*nr_compressed) = 0; (*nr_compressed)< limit; (*nr_compressed)++, val++) {\
		BUN pos = find_value_##TPE(dict, dict_count, *val);\
		if (pos == dict_count || !ARE_EQUAL(dict[pos], *val, nil, TPE)) {\
			/*This value is not in the base dictionary. See if we can add it to the delta dictionary.*/;\
			if (CONDITIONAL_INSERT_##NAME(info, *val, TPE)) {\
				BUN key = find_value_##TPE(delta, (*delta_count), *val);\
				if (key < *delta_count && ARE_EQUAL(delta[key], *val, nil, TPE)) {\
					/*This delta value is already in the dictionary hence we can skip it.*/\
					continue;\
				}\
				if (dict_count + *delta_count + 1 == GET_CAP(info)) {\
					if( !EXTEND(info, dict_count + *delta_count + (buffer_size <<=1)) ) throw(MAL, "mosaic.dictionary", MAL_MALLOC_FAIL);\
					dict = GET_BASE(info, TPE);\
					delta = dict + dict_count;\
				}\
				insert_into_dict_##TPE(delta, delta_count, key, *val);\
			}\
			else break;\
		}\
	}\
	GET_DELTA_COUNT(info) = (*delta_count);\
	BUN new_count = dict_count + GET_DELTA_COUNT(info);\
	calculateBits(GET_BITS_EXTENDED(info), new_count);\
	return MAL_SUCCEED;\
}
#define merge_delta_Into_dictionary_DEF(TPE) \
static \
void merge_delta_Into_dictionary_##TPE(GlobalDictionaryInfo* info) {\
	TPE* delta			= (TPE*) GET_BASE(info, TPE) + GET_COUNT(info);\
	if (GET_COUNT(info) == 0) {\
		/* The rest of the algorithm expects a non-empty dictionary.
		 * So we move the first element of the delta into a currently empty dictionary.*/\
		/*TODO: since the delta is a dictionary in its own right, we can just swap it to the dictionary */\
		delta++;\
		GET_COUNT(info)++;\
		GET_DELTA_COUNT(info)--;\
	}\
	BUN delta_count	= GET_DELTA_COUNT(info);\
	TPE* dict		= (TPE*) GET_BASE(info, TPE);\
	BUN* dict_count	= &GET_COUNT(info);\
\
	for (BUN i = 0; i < delta_count; i++) {\
		BUN key = find_value_##TPE(dict, *dict_count, delta[i]);\
		assert(key == *dict_count || !ARE_EQUAL(dict[key], delta[i], nil, TPE));\
		insert_into_dict_##TPE(dict, dict_count, key, delta[i]);\
	}\
	GET_BITS(info) = GET_BITS_EXTENDED(info);\
}
#define compress_dictionary_DEF(TPE) \
static void \
compress_dictionary_##TPE(TPE* dict, BUN dict_size, BUN* i, TPE* val, BUN limit,  BitVector base, bte bits) {\
	for((*i) = 0; (*i) < limit; (*i)++, val++) {\
		BUN key = find_value_##TPE(dict, dict_size, *val);\
		assert(key < dict_size);\
		setBitVector(base, (*i), bits, (BitVectorChunk) key);\
	}\
}
#define decompress_dictionary_DEF(TPE) \
static void \
decompress_dictionary_##TPE(TPE* dict, bte bits, BitVector base, BUN limit, TPE** dest) {\
	for(BUN i = 0; i < limit; i++){\
		BUN key = getBitVector(base,i,(int) bits);\
		(*dest)[i] = dict[key];\
	}\
	*dest += limit;\
}

#define MosaicBlkHeader_DEF_dictionary(NAME, TPE)\
typedef struct {\
	MosaicBlkRec rec;\
	char padding;\
	BitVectorChunk bitvector; /*First chunk of bitvector to force correct alignment.*/\
} MOSBlockHeader_##NAME##_##TPE;

#define DICTBlockHeaderTpe(NAME, TPE) MOSBlockHeader_##NAME##_##TPE

// MOStask object dependent macro's

#define MOScodevectorDict(task, NAME, TPE) ((BitVector) &((DICTBlockHeaderTpe(NAME, TPE)*) (task)->blk)->bitvector)

#define advance_dictionary(NAME, TPE)\
{\
	BUN cnt = MOSgetCnt(task->blk);\
\
	assert(cnt > 0);\
	task->start += (oid) cnt;\
\
	char* blk = (char*)task->blk;\
	blk += sizeof(MOSBlockHeaderTpe(NAME, TPE));\
	blk += BitVectorSize(cnt, GET_FINAL_BITS(task, NAME));\
	blk += GET_PADDING(task->blk, NAME, TPE);\
\
	task->blk = (MosaicBlk) blk;\
}

// insert a series of values into the compressor block using dictionary
#define DICTcompress(NAME, TPE)\
{\
	ALIGN_BLOCK_HEADER(task,  NAME, TPE);\
\
	TPE *val = getSrc(TPE, (task));\
	BUN cnt = estimate->cnt;\
	BitVector base = MOScodevectorDict(task, NAME, TPE);\
	BUN i;\
	TPE* dict = GET_FINAL_DICT(task, NAME, TPE);\
	BUN dict_size = GET_FINAL_DICT_COUNT(task, NAME);\
	bte bits = GET_FINAL_BITS(task, NAME);\
	compress_dictionary_##TPE(dict, dict_size, &i, val, cnt, base, bits);\
	MOSsetCnt(task->blk, i);\
}

// the inverse operator, extend the src
#define DICTdecompress(NAME, TPE)\
{	BUN cnt = MOSgetCnt((task)->blk);\
	BitVector base = MOScodevectorDict(task, NAME, TPE);\
	bte bits = GET_FINAL_BITS(task, NAME);\
	TPE* dict = GET_FINAL_DICT(task, NAME, TPE);\
	TPE* dest = (TPE*) (task)->src;\
	decompress_dictionary_##TPE(dict, bits, base, cnt, &dest);\
	(task)->src = (char*) dest;\
}

#define scan_loop_dictionary(NAME, TPE, CANDITER_NEXT, TEST) {\
    TPE* dict = GET_FINAL_DICT(task, NAME, TPE);\
	BitVector base = MOScodevectorDict(task, NAME, TPE);\
    bte bits = GET_FINAL_BITS(task, NAME);\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        BitVectorChunk j = getBitVector(base,i,bits); \
        v = dict[j];\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

#define projection_loop_dictionary(NAME, TPE, CANDITER_NEXT)\
{\
	TPE* dict = GET_FINAL_DICT(task, NAME, TPE);\
	BitVector base = MOScodevectorDict(task, NAME, TPE);\
    bte bits = GET_FINAL_BITS(task, NAME);\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (o - first);\
        BitVectorChunk j = getBitVector(base,i,bits); \
		*bt++ = dict[j];\
		task->cnt++;\
	}\
}

#define outer_loop_dictionary(HAS_NIL, NIL_MATCHES, NAME, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
	bte bits		= GET_FINAL_BITS(task, NAME);\
	TPE* dict		= GET_FINAL_DICT(task, NAME, TPE);\
	BitVector base	= MOScodevectorDict(task, NAME, TPE);\
    for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (lo - first);\
		BitVectorChunk j= getBitVector(base,i,bits);\
        TPE lval = dict[j];\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, lval))) {continue;};\
		}\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
}

#define join_inner_loop_dictionary(NAME, TPE, HAS_NIL, RIGHT_CI_NEXT)\
{\
	bte bits		= GET_FINAL_BITS(task, NAME);\
	TPE* dict		= GET_FINAL_DICT(task, NAME, TPE);\
	BitVector base	= MOScodevectorDict(task, NAME, TPE);\
    for (oid ro = canditer_peekprev(task->ci); !is_oid_nil(ro) && ro < last; ro = RIGHT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (ro - first);\
		BitVectorChunk j= getBitVector(base,i,bits);\
        TPE rval = dict[j];\
        IF_EQUAL_APPEND_RESULT(HAS_NIL, TPE);\
	}\
}

#endif /* _MOSAIC_DICTIONARY_  */
