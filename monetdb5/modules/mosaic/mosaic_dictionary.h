#ifndef _MOSAIC_DICTIONARY_ 
#define _MOSAIC_DICTIONARY_ 

#include "gdk.h"
#include "gdk_bitvector.h"
#include "mal_exception.h"

 /*TODO: assuming (for now) that bats have nils during compression*/
static const bool has_nil = true;

static unsigned char
calculateBits(BUN count) {
	unsigned char bits = 0;
	while (count >> bits) {
		bits++;
	}
	return bits;
}

#define IS_NIL(TPE, VAL) is_##TPE##_nil(VAL)

typedef struct _EstimationParameters {
	BUN count;
	unsigned char bits;
	BUN delta_count;
	unsigned char bits_extended; // number of bits required to index the info after the delta would have been merged.
} EstimationParameters;

#define ARE_EQUAL(v, w, HAS_NIL, TPE) ((v == w || (HAS_NIL && IS_NIL(TPE, v) && IS_NIL(TPE, w)) ) )

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
	size_t buffer_size = 256;\
	TPE* dict		= (TPE*) GET_BASE(info, TPE);\
	BUN dict_count	= GET_COUNT(info);\
	TPE* delta		= dict + dict_count;\
	*delta_count	= 0;\
	for((*nr_compressed) = 0; (*nr_compressed)< limit; (*nr_compressed)++, val++) {\
		BUN pos = find_value_##TPE(dict, dict_count, *val);\
		if (pos == dict_count || !ARE_EQUAL(delta[pos], *val, has_nil, TPE)) {\
			/*This value is not in the base dictionary. See if we can add it to the delta dictionary.*/;\
			if (CONDITIONAL_INSERT(info, *val, TPE)) {\
				BUN key = find_value_##TPE(delta, (*delta_count), *val);\
				if (key < *delta_count && ARE_EQUAL(delta[key], *val, has_nil, TPE)) {\
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
		if (key < *dict_count && ARE_EQUAL(dict[key], delta[i], has_nil, TPE)) {\
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
		setBitVector(base, (*i), bits, (unsigned int) key);\
	}\
}
#define decompress_dictionary_DEF(TPE) \
static void \
decompress_dictionary_##TPE(TPE* dict, bte bits, BitVector base, BUN limit, TPE** dest) {\
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


#define select_dictionary_general(HAS_NIL, ANTI, TPE) {\
	if		( IS_NIL(TPE, low) &&  IS_NIL(TPE, hgh) && li && hi && !(ANTI)) {\
		if(HAS_NIL) {\
			for(unsigned int i = 0; i < cnt; i++){\
				unsigned int j = getBitVector(base,i,bits);\
				if (IS_NIL(TPE, dict[j]))\
					*(*result)++ = (oid) (i + hseqbase);\
			}\
		}\
	}\
	else if	( IS_NIL(TPE, low) &&  IS_NIL(TPE, hgh) && li && hi && (ANTI)) {\
		if(HAS_NIL) {\
			for(unsigned int i = 0; i < cnt; i++){\
				unsigned int j = getBitVector(base,i,bits);\
				if (!IS_NIL(TPE, dict[j]))\
					*(*result)++ = (oid) (i + hseqbase);\
			}\
		}\
		else for(unsigned int i = 0; i < cnt; i++){ *(*result)++ = (oid) (i + hseqbase); }\
	}\
	else if	( IS_NIL(TPE, low) &&  IS_NIL(TPE, hgh) && !(li && hi) && !(ANTI)) {\
		if(HAS_NIL) {\
			for(unsigned int i = 0; i < cnt; i++){\
				unsigned int j = getBitVector(base,i,bits);\
				if (!IS_NIL(TPE, dict[j]))\
					*(*result)++ = (oid) (i + hseqbase);\
			}\
		}\
		else for(unsigned int i = 0; i < cnt; i++){ *(*result)++ = (oid) (i + hseqbase); }\
	}\
	else if	( IS_NIL(TPE, low) &&  IS_NIL(TPE, hgh) && !(li && hi) && (ANTI)) {\
			/*Empty (*result) set.*/\
	}\
	else if	( !IS_NIL(TPE, low) &&  !IS_NIL(TPE, hgh) && low == hgh && !(li && hi) && (ANTI)) {\
		if(HAS_NIL) {\
			for(unsigned int i = 0; i < cnt; i++){\
				unsigned int j = getBitVector(base,i,bits);\
				if (!IS_NIL(TPE, dict[j]))\
					*(*result)++ = (oid) (i + hseqbase);\
			}\
		}\
		else for(unsigned int i = 0; i < cnt; i++){ *(*result)++ = (oid) (i + hseqbase); }\
	}\
	else if	( !IS_NIL(TPE, low) &&  !IS_NIL(TPE, hgh) && low == hgh && !(li && hi) && !(ANTI)) {\
		/*Empty (*result) set.*/\
	}\
	else if	( !IS_NIL(TPE, low) &&  !IS_NIL(TPE, hgh) && low > hgh && !(ANTI)) {\
		/*Empty (*result) set.*/\
	}\
	else if	( !IS_NIL(TPE, low) &&  !IS_NIL(TPE, hgh) && low > hgh && (ANTI)) {\
		if(HAS_NIL) {\
			for(unsigned int i = 0; i < cnt; i++){\
				unsigned int j = getBitVector(base,i,bits);\
				if (!IS_NIL(TPE, dict[j]))\
					*(*result)++ = (oid) (i + hseqbase);\
			}\
		}\
		else for(unsigned int i = 0; i < cnt; i++){ *(*result)++ = (oid) (i + hseqbase); }\
	}\
	else {\
		/*normal cases.*/\
		if( !ANTI){\
			if( IS_NIL(TPE, low) ){\
				for(unsigned int i = 0; i < cnt; i++){\
					unsigned int j = getBitVector(base,i,bits); \
					if (HAS_NIL && IS_NIL(TPE, dict[j])) { continue;}\
					bool cmp  =  ((hi && dict[j] <= hgh ) || (!hi && dict[j] < hgh ));\
					if (cmp )\
						*(*result)++ = (oid) (i + hseqbase);\
				}\
			} else\
			if( IS_NIL(TPE, hgh) ){\
				for(unsigned int i = 0; i < cnt; i++){\
					unsigned int j = getBitVector(base,i,bits); \
					if (HAS_NIL && IS_NIL(TPE, dict[j])) { continue;}\
					bool cmp  =  ((li && dict[j] >= low ) || (!li && dict[j] > low ));\
					if (cmp )\
						*(*result)++ = (oid) (i + hseqbase);\
				}\
			} else{\
				for(unsigned int i = 0; i < cnt; i++){\
					unsigned int j = getBitVector(base,i,bits); \
					if (HAS_NIL && IS_NIL(TPE, dict[j])) { continue;}\
					bool cmp  =  ((hi && dict[j] <= hgh ) || (!hi && dict[j] < hgh )) &&\
							((li && dict[j] >= low ) || (!li && dict[j] > low ));\
					if (cmp )\
						*(*result)++ = (oid) (i + hseqbase);\
				}\
			}\
		} else {\
			if( IS_NIL(TPE, low) && IS_NIL(TPE, hgh)){\
				/* nothing is matching */\
			} else\
			if( IS_NIL(TPE, low) ){\
				for(unsigned int i = 0; i < cnt; i++){\
					unsigned int j = getBitVector(base,i,bits); \
					if (HAS_NIL && IS_NIL(TPE, dict[j])) { continue;}\
					bool cmp  =  ((hi && dict[j] <= hgh ) || (!hi && dict[j] < hgh ));\
					if ( !cmp )\
						*(*result)++ = (oid) (i + hseqbase);\
				}\
			} else\
			if( IS_NIL(TPE, hgh) ){\
				for(unsigned int i = 0; i < cnt; i++){\
					unsigned int j = getBitVector(base,i,bits); \
					if (HAS_NIL && IS_NIL(TPE, dict[j])) { continue;}\
					bool cmp  =  ((li && dict[j] >= low ) || (!li && dict[j] > low ));\
					if ( !cmp )\
						*(*result)++ = (oid) (i + hseqbase);\
				}\
			} else{\
				for(unsigned int i = 0; i < cnt; i++){\
					unsigned int j = getBitVector(base,i,bits); \
					if (HAS_NIL && IS_NIL(TPE, dict[j])) { continue;}\
					bool cmp  =  ((hi && dict[j] <= hgh ) || (!hi && dict[j] < hgh )) &&\
							((li && dict[j] >= low ) || (!li && dict[j] > low ));\
					if ( !cmp )\
						*(*result)++ = (oid) (i + hseqbase);\
				}\
			}\
		}\
	}\
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

/* TODO: the select_operator for dictionaries doesn't use
 * the ordered property of the actual global dictionary.
 * Which is a shame because it could in this select operator
 * safe on the dictionary look ups by using the dictionary itself
 * as a reverse index for the ranges of your select predicate.
 * Or if we want to stick to this set up, then we should use a
 * hash based dictionary.
*/
#define select_dictionary_DEF(TPE)\
static \
void select_dictionary_##TPE(\
	oid **result, BUN hseqbase, BUN cnt, TPE* dict, BitVector base, bte bits,\
	TPE low, TPE hgh, bool li, bool hi, bool nil, bool anti) {\
	if(	nil && anti){\
		select_dictionary_general(true, true, TPE);\
	}\
	if( !nil && anti){\
		select_dictionary_general(false, true, TPE);\
	}\
	if( nil && !anti){\
		select_dictionary_general(true, false, TPE);\
	}\
	if( !nil && !anti){\
		select_dictionary_general(false, false, TPE);\
	}\
}

#define thetaselect_dictionary_general(HAS_NIL, TPE)\
{\
	TPE low,hgh;\
	low= hgh = TPE##_nil;\
	bool anti = false;\
	if ( strcmp(oper,"<") == 0){\
		hgh= val;\
		hgh = PREVVALUE##TPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= val;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low = val;\
		low = NEXTVALUE##TPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low = val;\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		hgh= low= val;\
		anti = true;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= val;\
	} \
	for(unsigned int i = 0; i < cnt; i++){\
		unsigned int j = getBitVector(base, i, bits); \
		if (HAS_NIL && IS_NIL(TPE, dict[j])) { continue;}\
		if( (is_nil(TPE, low) || dict[j] >= low) && (dict[j] <= hgh || is_nil(TPE, hgh)) ){\
			if ( !anti) {\
				*(*result)++ = (oid) (i + hseqbase);\
			}\
		} else\
			if( anti){\
				*(*result)++ = (oid) (i + hseqbase);\
			}\
	}\
}

#define thetaselect_dictionary_DEF(TPE)\
static \
void thetaselect_dictionary_##TPE(\
	oid **result, BUN hseqbase, BUN cnt, TPE* dict, BitVector base, bte bits,\
	TPE val, str oper, bool nil) {\
	if( nil ){\
		thetaselect_dictionary_general(true, TPE);\
	}\
	else /*!nil*/{\
		thetaselect_dictionary_general(false, TPE);\
	}\
}

#define join_dictionary_general(NIL_MATCHES, TPE) {\
	BUN i, n;\
	oid hr, hl; /*The right and left head values respectively.*/\
	for(hr=0, n= rcnt; n-- > 0; hr++,tr++ ){\
		for(hl = (oid) hseqbase, i = 0; i < lcnt; i++,hl++){\
			unsigned int j= getBitVector(base,i,bits);\
			if (!NIL_MATCHES) {\
				if (IS_NIL(TPE, dict[j])) { continue;}\
			}\
			if ( *tr == dict[j]){\
				if(BUNappend(lres, &hl, false) != GDK_SUCCEED ||\
				BUNappend(rres, &hr, false) != GDK_SUCCEED)\
				throw(MAL,"mosaic.dictionary",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

#define join_dictionary_DEF(TPE)\
static \
str join_dictionary_##TPE(\
BAT* lres, BAT* rres,\
BUN hseqbase, BUN lcnt, TPE* dict, BitVector base, bte bits, TPE* tr, BUN rcnt,\
bool has_nil, bool nil_matches)\
{\
	if( has_nil && !nil_matches){\
		join_dictionary_general(false, TPE);\
	}\
	else /*!has_nil*/{\
		join_dictionary_general(true, TPE);\
	}\
	return MAL_SUCCEED;\
}

// MOStask object dependent macro's

#define MOScodevectorDict(Task) (((char*) (Task)->blk) + wordaligned(sizeof(MOSBlkHdr_dictionary_t), unsigned int))

// insert a series of values into the compressor block using dictionary
#define DICTcompress(TASK, TPE) {\
	TPE *val = getSrc(TPE, (TASK));\
	BUN limit = estimate->cnt;\
	(TASK)->dst = MOScodevectorDict(TASK);\
	BitVector base = (BitVector) ((TASK)->dst);\
	BUN i;\
	TPE* dict = GET_FINAL_DICT(TASK, TPE);\
	BUN dict_size = GET_FINAL_DICT_COUNT(TASK);\
	bte bits = GET_FINAL_BITS(task);\
	compress_dictionary_##TPE(dict, dict_size, &i, val, limit, base, bits);\
	MOSsetCnt((TASK)->blk, i);\
}

// the inverse operator, extend the src
#define DICTdecompress(TASK, TPE)\
{	BUN lim = MOSgetCnt((TASK)->blk);\
	BitVector base = (BitVector) MOScodevectorDict(TASK);\
	bte bits = GET_FINAL_BITS(task);\
	TPE* dict = GET_FINAL_DICT(TASK, TPE);\
	TPE* dest = (TPE*) (TASK)->src;\
	decompress_dictionary_##TPE(dict, bits, base, lim, &dest);\
}

#define DICTselect(TPE) {\
	oid* result = task->lb;\
	BUN hseqbase = task->start;\
	BUN cnt = MOSgetCnt(task->blk);\
	bool nil = !task->bsrc->tnonil;\
	TPE* dict = GET_FINAL_DICT(task, TPE);\
	BitVector base = (BitVector) MOScodevectorDict(task);\
	bte bits = GET_FINAL_BITS(task);\
	select_dictionary_##TPE(\
		&result, hseqbase, cnt, dict, base, bits,\
		*(TPE*) low, *(TPE*) hgh, *li, *hi, nil, *anti);\
	task->lb = result;\
}

#define DICTthetaselect(TPE) {\
	oid* result = task->lb;\
	BUN hseqbase = task->start;\
	BUN cnt = MOSgetCnt(task->blk);\
	bool nil = !task->bsrc->tnonil;\
	TPE* dict = GET_FINAL_DICT(task, TPE);\
	BitVector base = (BitVector) MOScodevectorDict(task);\
	bte bits = GET_FINAL_BITS(task);\
	thetaselect_dictionary_##TPE(\
		&result, hseqbase, cnt, dict, base, bits,\
		*(TPE*) val, oper, nil);\
	task->lb = result;\
}

#define DICTprojection(TPE) {	\
	BUN i,first,last;\
	unsigned short j;\
	/* set the oid range covered and advance scan range*/\
	first = task->start;\
	last = first + MOSgetCnt(task->blk);\
	TPE *v;\
	bte bits	= GET_FINAL_BITS(task);\
	TPE* dict	= GET_FINAL_DICT(task, TPE);\
	BitVector base		= (BitVector) MOScodevectorDict(task);\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++){\
		MOSskipit();\
		j= getBitVector(base,i,bits); \
		*v++ = dict[j];\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

#define DICTjoin(TPE) {\
	bte bits		= GET_FINAL_BITS(task);\
	TPE* dict		= GET_FINAL_DICT(task, TPE);\
	BitVector base	= (BitVector) MOScodevectorDict(task);\
	BUN lcnt = MOSgetCnt(task->blk);\
	BUN hseqbase = task->start;\
	BAT* lres = task->lbat;\
	BAT* rres = task->rbat;\
	TPE* tr = (TPE*) task->src;/*right tail value, i.e. the non-mosaic side. */\
	bool has_nil = !task->bsrc->tnonil;\
	BUN rcnt = task->stop;\
	str result = join_dictionary_##TPE(\
		lres, rres,\
	 	hseqbase, lcnt, dict, base, bits, /*left mosaic side*/\
		tr, rcnt, /*right (treated as) non-mosaic side*/\
		has_nil, nil_matches);\
	if (result != MAL_SUCCEED) return result;\
}

#endif /* _MOSAIC_DICTIONARY_  */
