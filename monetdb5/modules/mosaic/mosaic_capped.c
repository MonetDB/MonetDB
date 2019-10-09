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
#include "mosaic_private.h"

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

void
MOSadvance_capped(MOStask task)
{
	int *dst = (int*)  MOScodevector(task);
	BUN cnt = MOSgetCnt(task->blk);
	long bytes;

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->stop = task->stop;
	bytes =  (long) (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, int));
}

void
MOSlayout_capped_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];

	for(i=0; i< task->hdr->dictsize; i++){
		snprintf(buf, BUFSIZ,"capped[%d]",i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			BUNappend(boutput, &task->hdr->dictfreq[i], false) != GDK_SUCCEED ||
			BUNappend(bproperties, bufv, false) != GDK_SUCCEED)
		return;
	}
}


void
MOSlayout_capped(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
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

#define MOSfind(Res,DICT,VAL,F,L)\
{ int m,f= F, l=L; \
   while( l-f > 0 ) { \
	m = f + (l-f)/2;\
	if ( VAL < DICT[m] ) l=m-1; else f= m;\
	if ( VAL > DICT[m] ) f=m+1; else l= m;\
   }\
   Res= f;\
}

#define estimateDict(TPE)\
{\
	current->compression_strategy.tag = MOSAIC_CAPPED;\
	TPE *val = ((TPE*)task->src) + task->start;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,hdr->dict.val##TPE,*val,0,hdr->dictsize);\
		if( j == hdr->dictsize || hdr->dict.val##TPE[j] != *val )\
			break;\
	}\
	current->is_applicable = i > 0;\
	if ( current->is_applicable ) {\
		current->uncompressed_size += (BUN) (i * sizeof(TPE));\
		size_t bytes = (i * hdr->bits)/CHAR_BIT;\
		current->compressed_size += wordaligned( MosaicBlkSize, unsigned int) + bytes;\
	}\
	current->compression_strategy.cnt = i;\
}

// Create a larger capped buffer then we allow for in the mosaic header first
// Store the most frequent ones in the compressed heap header directly based on estimated savings
// Improve by using binary search rather then linear scan
#define TMPDICT 16*256

#define makeDict(TPE)\
{	TPE *val,v,w;\
	BUN limit = task->stop - task->start > TMPDICT ? TMPDICT:  task->stop - task->start;\
	BAT* bsample = BATsample_with_seed(task->bsrc, limit, (16-07-1985));\
	lng cw,cv; \
	for(i = 0; i< limit; i++){\
		oid sample = BUNtoid(bsample,i);\
		val = ((TPE*)task->src) + (sample - task->bsrc->hseqbase);\
		MOSfind(j,dict.val##TPE,*val,0,dictsize);\
		if(j == dictsize && dictsize == 0 ){\
			dict.val##TPE[j]= *val;\
			cnt[j] = 1;\
			dictsize++;\
		} else  \
		if( dictsize < TMPDICT && dict.val##TPE[j] != *val){\
			w= *val; cw= 1;\
			for( ; j< dictsize; j++)\
			if (dict.val##TPE[j] > w){\
				v =dict.val##TPE[j];\
				dict.val##TPE[j]= w;\
				w = v;\
				cv = cnt[j];\
				cnt[j]= cw;\
				cw = cv;\
			}\
			dictsize++;\
			dict.val##TPE[j]= w;\
			cnt[j] = 1;\
		} else if (dictsize < TMPDICT) cnt[j]++;\
	}\
	BBPunfix(bsample->batCacheid);\
	/* find the 256 most frequent values and save them in the mosaic header */ \
	if( dictsize <= 256){ \
		memcpy((char*)&task->hdr->dict,  (char*)&dict, dictsize * sizeof(TPE)); \
		memcpy((char*)task->hdr->dictfreq,  (char*)&cnt, dictsize * sizeof(lng)); \
		hdr->dictsize = dictsize; \
	} else { \
		/* brute force search of the top-k dictionary values with highest cnt.
		 * This is a variation of bubble sort to get the top-k so that
		 * we can maintain the previous value based dictionary ordering in our top k.
		 * */ \
		for(j=0; j< 256; j++){ \
			/* initialize max with the first non-kept index.*/\
			for(max = 0; max <dictsize && keep[max]; max++){}\
			for(k=0; k< dictsize; k++) \
			if( keep[k]==0){ \
				if( cnt[k]> cnt[max]) max = k; \
			} \
			keep[max]=1; \
		} \
		/* keep the top-k, in order */ \
		for( j=k=0; k<dictsize; k++) \
		if( keep[k]){ \
			task->hdr->dict.val##TPE[j] = dict.val##TPE[k]; \
			task->hdr->dictfreq[j] = cnt[k]; \
			j++; \
		} \
		hdr->dictsize = j; \
		assert(j<=256); \
	} \
}


/* Take a larger sample before fixing the dictionary */
void
MOScreatedictionary(MOStask task)
{
	unsigned int i;
	int j, k, max;
	MosaicHdr hdr = task->hdr;
    union{
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
    }dict;
	lng cnt[TMPDICT];
	bte keep[TMPDICT];
	int dictsize = 0;

	memset((char*) &dict, 0, TMPDICT * sizeof(lng));
	memset((char*) cnt, 0, sizeof(cnt));
	memset((char*) keep, 0, sizeof(keep));

	hdr->dictsize = dictsize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: makeDict(bte); break;
	case TYPE_sht: makeDict(sht); break;
	case TYPE_int: makeDict(int); break;
	case TYPE_lng: makeDict(lng); break;
	case TYPE_oid: makeDict(oid); break;
	case TYPE_flt: makeDict(flt); break;
	case TYPE_dbl: makeDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: makeDict(hge); break;
#endif
	}
	/* calculate the bit-width */
	hdr->bits = 1;
	hdr->mask =1;
	for( j=2 ; j < dictsize; j *=2){
		hdr->bits++;
		hdr->mask = (hdr->mask <<1) | 1;
	}
}

// calculate the expected reduction using DICT in terms of elements compressed
str
MOSestimate_capped(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
{
	unsigned int i = 0;
	int j;
	flt factor= 0.0;
	MosaicHdr hdr = task->hdr;
	(void) previous;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: estimateDict(bte); break;
	case TYPE_sht: estimateDict(sht); break;
	case TYPE_int: estimateDict(int); break;
	case TYPE_oid: estimateDict(oid); break;
	case TYPE_lng: estimateDict(lng); break;
	case TYPE_flt: estimateDict(flt); break;
	case TYPE_dbl: estimateDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateDict(hge); break;
#endif
	}
	task->factor[MOSAIC_CAPPED] = factor;
	task->range[MOSAIC_CAPPED] = task->start + i;
	return MAL_SUCCEED;
}

// insert a series of values into the compressor block using dictionary

#define DICTcompress(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start;\
	BitVector base = (BitVector) MOScodevector(task);\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,task->hdr->dict.val##TPE,*val,0,hdr->dictsize);\
		if(j == hdr->dictsize || task->hdr->dict.val##TPE[j] !=  *val) \
			break;\
		else {\
			hdr->checksum.sum##TPE += *val;\
			hdr->dictfreq[j]++;\
			MOSincCnt(blk,1);\
			setBitVector(base,i,hdr->bits,(unsigned int)j);\
		}\
	}\
	assert(i);\
}


void
MOScompress_capped(MOStask task)
{
	BUN i;
	int j;
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;

	task->dst = MOScodevector(task);

	MOSsetTag(blk,MOSAIC_CAPPED);
	MOSsetCnt(blk,0);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTcompress(bte); break;
	case TYPE_sht: DICTcompress(sht); break;
	case TYPE_int: DICTcompress(int); break;
	case TYPE_lng: DICTcompress(lng); break;
	case TYPE_oid: DICTcompress(oid); break;
	case TYPE_flt: DICTcompress(flt); break;
	case TYPE_dbl: DICTcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src

#define DICTdecompress(TPE)\
{	BUN lim = MOSgetCnt(blk);\
	base = (BitVector) MOScodevector(task);\
	for(i = 0; i < lim; i++){\
		j= getBitVector(base,i,(int) hdr->bits); \
		((TPE*)task->src)[i] = task->hdr->dict.val##TPE[j];\
		hdr->checksum2.sum##TPE += task->hdr->dict.val##TPE[j];\
	}\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_capped(MOStask task)
{
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;
	BUN i;
	int j;
	BitVector base;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTdecompress(bte); break;
	case TYPE_sht: DICTdecompress(sht); break;
	case TYPE_int: DICTdecompress(int); break;
	case TYPE_lng: DICTdecompress(lng); break;
	case TYPE_oid: DICTdecompress(oid); break;
	case TYPE_flt: DICTdecompress(flt); break;
	case TYPE_dbl: DICTdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTdecompress(hge); break;
#endif
	}
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

#define select_capped_str(TPE) \
	throw(MAL,"mosaic.capped","TBD");
#define select_capped(TPE) {\
	base = (BitVector) MOScodevector(task);\
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
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for(i=0; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*li && task->hdr->dict.val##TPE[j] >= * (TPE*)low ) || (!*li && task->hdr->dict.val##TPE[j] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh )) &&\
						((*li && task->hdr->dict.val##TPE[j] >= * (TPE*)low ) || (!*li && task->hdr->dict.val##TPE[j] > *(TPE*)low ));\
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
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*li && task->hdr->dict.val##TPE[j] >= * (TPE*)low ) || (!*li && task->hdr->dict.val##TPE[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh )) &&\
						((*li && task->hdr->dict.val##TPE[j] >= * (TPE*)low ) || (!*li && task->hdr->dict.val##TPE[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

str
MOSselect_capped(MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN i, first,last;
	MosaicHdr hdr = task->hdr;
	int cmp;
	bte j;
	BitVector base;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_capped(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_capped(bte); break;
	case TYPE_sht: select_capped(sht); break;
	case TYPE_int: select_capped(int); break;
	case TYPE_lng: select_capped(lng); break;
	case TYPE_oid: select_capped(oid); break;
	case TYPE_flt: select_capped(flt); break;
	case TYPE_dbl: select_capped(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_capped(hge); break;
#endif
	}
	MOSskip_capped(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_capped_str(TPE)\
	throw(MAL,"mosaic.capped","TBD");

#define thetaselect_capped(TPE)\
{ 	TPE low,hgh;\
	base = (BitVector) MOScodevector(task);\
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
		j= getBitVector(base,first,(int) hdr->bits); \
		if( (is_nil(TPE, low) || task->hdr->dict.val##TPE[j] >= low) && (task->hdr->dict.val##TPE[j] <= hgh || is_nil(TPE, hgh)) ){\
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
MOSthetaselect_capped( MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	MosaicHdr hdr = task->hdr;
	bte j;
	BitVector base;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_capped(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_capped(bte); break;
	case TYPE_sht: thetaselect_capped(sht); break;
	case TYPE_int: thetaselect_capped(int); break;
	case TYPE_lng: thetaselect_capped(lng); break;
	case TYPE_oid: thetaselect_capped(oid); break;
	case TYPE_flt: thetaselect_capped(flt); break;
	case TYPE_dbl: thetaselect_capped(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_capped(hge); break;
#endif
	}
	MOSskip_capped(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_capped_str(TPE)\
	throw(MAL,"mosaic.capped","TBD");
#define projection_capped(TPE)\
{	TPE *v;\
	base = (BitVector) MOScodevector(task);\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++){\
		MOSskipit();\
		j= getBitVector(base,i,(int) hdr->bits); \
		*v++ = task->hdr->dict.val##TPE[j];\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_capped( MOStask task)
{
	BUN i,first,last;
	MosaicHdr hdr = task->hdr;
	unsigned short j;
	BitVector base;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_capped(bte); break;
		case TYPE_sht: projection_capped(sht); break;
		case TYPE_int: projection_capped(int); break;
		case TYPE_lng: projection_capped(lng); break;
		case TYPE_oid: projection_capped(oid); break;
		case TYPE_flt: projection_capped(flt); break;
		case TYPE_dbl: projection_capped(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_capped(hge); break;
#endif
	}
	MOSskip_capped(task);
	return MAL_SUCCEED;
}

#define join_capped_str(TPE)\
	throw(MAL,"mosaic.capped","TBD");

#define join_capped(TPE)\
{	TPE  *w;\
	BitVector base = (BitVector) MOScodevector(task);\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->stop; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			j= getBitVector(base,i,(int) hdr->bits); \
			if ( *w == task->hdr->dict.val##TPE[j]){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED)\
				throw(MAL,"mosaic.capped",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

str
MOSjoin_capped( MOStask task)
{
	BUN i,n,limit;
	oid o, oo;
	MosaicHdr hdr = task->hdr;
	int j;

	// set the oid range covered and advance scan range
	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_capped(bte); break;
		case TYPE_sht: join_capped(sht); break;
		case TYPE_int: join_capped(int); break;
		case TYPE_lng: join_capped(lng); break;
		case TYPE_oid: join_capped(oid); break;
		case TYPE_flt: join_capped(flt); break;
		case TYPE_dbl: join_capped(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_capped(hge); break;
#endif
	}
	MOSskip_capped(task);
	return MAL_SUCCEED;
}
