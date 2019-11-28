#ifndef _MOSAIC_DICTIONARY_ 
#define _MOSAIC_DICTIONARY_ 

#include "gdk.h"
#include "gdk_bitvector.h"
#include "mal_exception.h"

 /*TODO: assuming (for now) that bats have nils during compression*/
static const bool nil = true;

static unsigned char
calculateBits(BUN count) {
	unsigned char bits = 0;
	while (count >> bits) {
		bits++;
	}
	return bits;
}

typedef struct _EstimationParameters {
	BUN count;
	unsigned char bits;
	BUN delta_count;
	unsigned char bits_extended; // number of bits required to index the info after the delta would have been merged.
} EstimationParameters;

#define find_value_DEF(TPE) \
static inline \
BUN find_value_##TPE(TPE* dict, BUN dict_count, TPE val) {\
	BUN m, f= 0, l = dict_count, offset = 0;\
	/* This function assumes that the implementation of a dictionary*/\
	/* is that of a sorted array with nils starting first.*/\
	if (dict_count > 0 && IS_NIL(TPE, val) && IS_NIL(TPE, dict[0])) return 0;\
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
#define extend_delta_DEF(TPE, DICTIONARY_TYPE) \
static str \
extend_delta_##TPE(BUN* nr_compressed, BUN* delta_count, BUN limit, DICTIONARY_TYPE* info, TPE* val) {\
	BUN buffer_size = 256;\
	TPE* dict		= (TPE*) GET_BASE(info, TPE);\
	BUN dict_count	= GET_COUNT(info);\
	TPE* delta		= dict + dict_count;\
	*delta_count	= 0;\
	for((*nr_compressed) = 0; (*nr_compressed)< limit; (*nr_compressed)++, val++) {\
		BUN pos = find_value_##TPE(dict, dict_count, *val);\
		if (pos == dict_count || !ARE_EQUAL(delta[pos], *val, nil, TPE)) {\
			/*This value is not in the base dictionary. See if we can add it to the delta dictionary.*/;\
			if (CONDITIONAL_INSERT(info, *val, TPE)) {\
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
	GET_BITS_EXTENDED(info) = calculateBits(new_count);\
	return MAL_SUCCEED;\
}
#define merge_delta_Into_dictionary_DEF(TPE, DICTIONARY_TYPE) \
static \
void merge_delta_Into_dictionary_##TPE(DICTIONARY_TYPE* info) {\
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
		if (key < *dict_count && ARE_EQUAL(dict[key], delta[i], nil, TPE)) {\
			/*This delta value is already in the dictionary hence we can skip it.*/\
			continue;\
		}\
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

typedef struct {
	MosaicBlkRec base;
	/* offset to the location of the actual variable sized info.
	 * It should always be after the global Mosaic header.*/
} MOSBlkHdr_dictionary_t;

// MOStask object dependent macro's

#define MOScodevectorDict(Task) (((char*) (Task)->blk) + wordaligned(sizeof(MOSBlkHdr_dictionary_t), BitVectorChunk))

// insert a series of values into the compressor block using dictionary
#define DICTcompress(TASK, TPE) {\
	TPE *val = getSrc(TPE, (TASK));\
	BUN cnt = estimate->cnt;\
	(TASK)->dst = MOScodevectorDict(TASK);\
	BitVector base = (BitVector) ((TASK)->dst);\
	BUN i;\
	TPE* dict = GET_FINAL_DICT(TASK, TPE);\
	BUN dict_size = GET_FINAL_DICT_COUNT(TASK);\
	bte bits = GET_FINAL_BITS(task);\
	compress_dictionary_##TPE(dict, dict_size, &i, val, cnt, base, bits);\
	MOSsetCnt((TASK)->blk, i);\
}

// the inverse operator, extend the src
#define DICTdecompress(TASK, TPE)\
{	BUN cnt = MOSgetCnt((TASK)->blk);\
	BitVector base = (BitVector) MOScodevectorDict(TASK);\
	bte bits = GET_FINAL_BITS(task);\
	TPE* dict = GET_FINAL_DICT(TASK, TPE);\
	TPE* dest = (TPE*) (TASK)->src;\
	decompress_dictionary_##TPE(dict, bits, base, cnt, &dest);\
}

#define scan_loop_dictionary(TPE, CANDITER_NEXT, TEST) {\
    TPE* dict = GET_FINAL_DICT(task, TPE);\
	BitVector base = (BitVector) MOScodevectorDict(task);\
    bte bits = GET_FINAL_BITS(task);\
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (c - first);\
        BitVectorChunk j = getBitVector(base,i,bits); \
        v = dict[j];\
        /*TODO: change from control to data dependency.*/\
        if (TEST)\
            *o++ = c;\
    }\
}

#define projection_loop_dictionary(TPE, CANDITER_NEXT)\
{\
	TPE* dict = GET_FINAL_DICT(task, TPE);\
	BitVector base = (BitVector) MOScodevectorDict(task);\
    bte bits = GET_FINAL_BITS(task);\
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CANDITER_NEXT(task->ci)) {\
        BUN i = (BUN) (o - first);\
        BitVectorChunk j = getBitVector(base,i,bits); \
		*bt++ = dict[j];\
		task->cnt++;\
	}\
}

#define outer_loop_dictionary(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
	bte bits		= GET_FINAL_BITS(task);\
	TPE* dict		= GET_FINAL_DICT(task, TPE);\
	BitVector base	= (BitVector) MOScodevectorDict(task);\
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

#endif /* _MOSAIC_DICTIONARY_  */
