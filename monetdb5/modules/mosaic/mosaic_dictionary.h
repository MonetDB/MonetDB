#ifndef _MOSAIC_DICTIONARY_ 
#define _MOSAIC_DICTIONARY_ 

#include "mal_exception.h"

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

#define DictionaryClass(TPE, DICTIONARY_TYPE, GET_BASE, GET_COUNT, GET_DELTA_COUNT, GET_BITS, GET_BITS_EXTENDED, GET_CAP, GET_VALUE, EXTEND, CONDITIONAL_INSERT)\
static inline \
BUN _findValue_##TPE(TPE* dict, BUN dict_count, TPE val) {\
	int m, f= 0, l = dict_count;\
	while( l-f > 0 ) {\
		m = f + (l-f)/2;\
		if ( val < dict[m]) l=m-1; else f= m;\
		if ( val > dict[m]) f=m+1; else l= m;\
	}\
	return f;\
}\
static inline \
TPE _getValue_##TPE(DICTIONARY_TYPE* info, BUN key) {\
	return GET_VALUE(info, key, TPE);\
}\
static str \
estimateDict_##TPE(BUN* nr_compressed, BUN* delta_count, BUN limit, DICTIONARY_TYPE* info, TPE* val) {\
	size_t buffer_size = 256;\
	TPE* dict		= (TPE*) GET_BASE(info, TPE);\
	BUN dict_cnt	= GET_COUNT(info);\
	TPE* delta		= dict + dict_cnt;\
	*delta_count = 0;\
	for((*nr_compressed) = 0; (*nr_compressed)< limit; (*nr_compressed)++, val++) {\
		BUN pos = _findValue_##TPE(dict, dict_cnt, *val);\
		if (pos == dict_cnt || _getValue_##TPE(info, pos) != *val) {\
			/*This value is not in the base dictionary. See if we can add it to the delta dictionary.*/;\
			if (CONDITIONAL_INSERT(info, *val, TPE)) {\
				BUN key = _findValue_##TPE(delta, (*delta_count), *val);\
				if (key < *delta_count && delta[key] == *val) {\
					/*This delta value is already in the dictionary hence we can skip it.*/\
					continue;\
				}\
				if (*delta_count + 1 == GET_CAP(info) && !EXTEND(info, dict_cnt + (buffer_size <<=1))) {\
					throw(MAL, "mosaic.var", MAL_MALLOC_FAIL);\
				}\
				TPE w = *val;\
				for( ; key< *delta_count; key++) {\
					if (delta[key] > w){\
						TPE v = delta[key];\
						delta[key] = w;\
						w = v;\
					}\
				}\
				delta[key] = w;\
				(*delta_count)++;\
			}\
			else break;\
		}\
	}\
	GET_DELTA_COUNT(info) = (*delta_count);\
	BUN new_count = dict_cnt + GET_DELTA_COUNT(info);\
	GET_BITS_EXTENDED(info) = calculateBits(new_count);\
	return MAL_SUCCEED;\
}\
static \
void _mergeDeltaIntoDictionary_##TPE(DICTIONARY_TYPE* info) {\
	TPE* delta			= (TPE*) GET_BASE(info, TPE) + GET_COUNT(info);\
	if (GET_COUNT(info) == 0) {\
		/* The rest of the algorithm expects a non-empty dictionary.
		 * So we move the first element of the delta into a currently empty dictionary.*/\
		/*TODO: since the delta is a dictionary in its own right, we can just swap it to the dictionary */\
		++delta;\
		GET_COUNT(info)++;\
		GET_DELTA_COUNT(info)--;\
	}\
	BUN limit = GET_DELTA_COUNT(info);\
	for (BUN i = 0; i < limit; i++) {\
		BUN key = _findValue_##TPE(GET_BASE(info, TPE), GET_COUNT(info), delta[i]);\
		if (key < GetCount(info) && GET_VALUE(info, key, TPE) == delta[i]) {\
			/*This delta value is already in the dictionary hence we can skip it.*/\
			continue;\
		}\
		TPE w = delta[i];\
		for( ; key< GET_COUNT(info); key++) {\
			if (GET_VALUE(info, key, TPE) > w){\
				TPE v =GET_VALUE(info, key, TPE);\
				GET_VALUE(info, key, TPE)= w;\
				w = v;\
			}\
		}\
		GET_COUNT(info)++;\
		GET_VALUE(info, key, TPE)= w;\
	}\
	GET_BITS(info) = GET_BITS_EXTENDED(info);\
}\
static void \
_compress_dictionary_##TPE(TPE* dict, BUN dict_size, BUN* i, TPE* val, BUN limit,  BitVector base, bte bits) {\
	for((*i) = 0; (*i) < limit; (*i)++, val++) {\
		BUN key = _findValue_##TPE(dict, dict_size, *val);\
		assert(key < dict_size);\
		setBitVector(base, (*i), bits, (unsigned int) key);\
	}\
}\
static void \
_decompress_dictionary_##TPE(TPE* dict, bte bits, BitVector base, BUN limit, TPE** dest) {\
	for(BUN i = 0; i < limit; i++){\
		size_t key = getBitVector(base,i,(int) bits);\
		(*dest)[i] = dict[key];\
	}\
	*dest += limit;\
}

typedef struct {
	MosaicBlkRec base;
	/* offset to the location of the actual variable sized info.
	 * It should always be after the global Mosaic header.*/
} MOSBlkHdr_dictionary_t;

#define MOScodevectorDict(Task) (((char*) (Task)->blk)+ wordaligned(sizeof(MOSBlkHdr_dictionary_t), BitVector))

#endif /* _MOSAIC_DICTIONARY_  */
