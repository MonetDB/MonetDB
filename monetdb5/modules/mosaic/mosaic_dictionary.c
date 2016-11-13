/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 *                * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * 2014-2016 author Martin Kersten
 * Global dictionary encoding
 * Index value zero is not used to easy detection of filler
 * The dictionary index size is derived from the number of entries covered.
 * It leads to a compact n-bit representation.
 * Floating points are not expected to be replicated 
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_dictionary.h"

void
MOSadvance_dictionary(Client cntxt, MOStask task)
{
	int *dst = (int*)  MOScodevector(task);
	BUN cnt = MOSgetCnt(task->blk);
	long bytes;
	(void) cntxt;

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->stop = task->elm;
	bytes =  (long) (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, int)); 
}

/* Beware, the dump routines use the compressed part of the task */
static void
MOSdump_dictionaryInternal(char *buf, size_t len, MOStask task, int i)
{

	switch(ATOMbasetype(task->type)){
	case TYPE_bte:
		snprintf(buf,len,"%d", task->hdr->dict.valbte[i]); break;
	case TYPE_sht:
		snprintf(buf,len,"%hd", task->hdr->dict.valsht[i]); break;
	case TYPE_int:
		snprintf(buf,len,"%d", task->hdr->dict.valint[i]); break;
	case  TYPE_oid:
		snprintf(buf,len,OIDFMT,  task->hdr->dict.valoid[i]); break;
	case  TYPE_lng:
		snprintf(buf,len,LLFMT,  task->hdr->dict.vallng[i]); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		snprintf(buf,len,"%.40g",  (dbl) task->hdr->dict.valhge[i]); break;
#endif
	case TYPE_flt:
		snprintf(buf,len,"%f", task->hdr->dict.valflt[i]); break;
	case TYPE_dbl:
		snprintf(buf,len,"%g", task->hdr->dict.valdbl[i]); break;
	case TYPE_str:
		switch(task->bsrc->twidth){
			case 1: snprintf(buf,len,"%d", task->hdr->dict.valbte[i]); break;
			case 2: snprintf(buf,len,"%hd", task->hdr->dict.valsht[i]); break;
			case 4: snprintf(buf,len,"%d", task->hdr->dict.valint[i]); break;
			case 8: snprintf(buf,len,LLFMT,  task->hdr->dict.vallng[i]); 
		}
}
}

void
MOSdump_dictionary(Client cntxt, MOStask task)
{
	int i,len= BUFSIZ;
	char buf[BUFSIZ];

	mnstr_printf(cntxt->fdout,"#dictionary bits %d dictsize %d",task->hdr->bits, task->hdr->dictsize);
	for(i=0; i< task->hdr->dictsize; i++){
		MOSdump_dictionaryInternal(buf, BUFSIZ, task,i);
		mnstr_printf(cntxt->fdout,"[%d] %s ",i,buf);
	}
	mnstr_printf(cntxt->fdout,"\n");
	switch(ATOMbasetype(task->type)){
	case TYPE_bte:
		snprintf(buf,len,"%d %d", task->hdr->checksum.sumbte,task->hdr->checksum2.sumbte); break;
	case TYPE_sht:
		snprintf(buf,len,"%hd %hd", task->hdr->checksum.sumsht,task->hdr->checksum2.sumsht); break;
	case TYPE_int:
		snprintf(buf,len,"%d %d", task->hdr->checksum.sumint,task->hdr->checksum2.sumint); break;
	case  TYPE_oid:
		snprintf(buf,len,OIDFMT " " OIDFMT, task->hdr->checksum.sumoid,task->hdr->checksum2.sumoid); break;
	case  TYPE_lng:
		snprintf(buf,len,LLFMT " " LLFMT, task->hdr->checksum.sumlng,task->hdr->checksum2.sumlng); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		snprintf(buf,len,"%.40g %.40g", (dbl)task->hdr->checksum.sumhge,(dbl)task->hdr->checksum2.sumhge); break;
#endif
	case TYPE_flt:
		snprintf(buf,len,"%f %f", task->hdr->checksum.sumflt,task->hdr->checksum2.sumflt); break;
	case TYPE_dbl:
		snprintf(buf,len,"%g %g", task->hdr->checksum.sumdbl,task->hdr->checksum2.sumdbl); break;
	case TYPE_str:
		switch(task->bsrc->twidth){
			case 1: snprintf(buf,len,"%d", task->hdr->dict.valbte[i]); break;
			case 2: snprintf(buf,len,"%hd", task->hdr->dict.valsht[i]); break;
			case 4: snprintf(buf,len,"%d", task->hdr->dict.valint[i]); break;
			case 8: snprintf(buf,len,LLFMT,  task->hdr->dict.vallng[i]); 
		}
	}
	mnstr_printf(cntxt->fdout,"#checksums %s\n",buf);
}

void
MOSlayout_dictionary_hdr(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	int i;
	char buf[BUFSIZ];

	(void) cntxt;
	for(i=0; i< task->hdr->dictsize; i++){
		snprintf(buf, BUFSIZ,"dictionary[%d]",i);
		BUNappend(btech, buf, FALSE);
		BUNappend(bcount, &zero, FALSE);
		BUNappend(binput, &zero, FALSE);
		BUNappend(boutput, &task->hdr->dictfreq[i], FALSE);
		MOSdump_dictionaryInternal(buf, BUFSIZ, task,i);
		BUNappend(bproperties, buf, FALSE);
	}
}


void
MOSlayout_dictionary(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	(void) cntxt;
	BUNappend(btech, "dictionary", FALSE);
	BUNappend(bcount, &cnt, FALSE);
	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
	BUNappend(binput, &input, FALSE);
	BUNappend(boutput, &output, FALSE);
	BUNappend(bproperties, "", FALSE);
}

void
MOSskip_dictionary(Client cntxt, MOStask task)
{
	MOSadvance_dictionary(cntxt, task);
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
{	TPE *val = ((TPE*)task->src) + task->start;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	if( task->range[MOSAIC_DICT] > task->start){\
		i = task->range[MOSAIC_DICT] - task->start;\
		if ( i > MOSlimit() ) i = MOSlimit();\
		if( i * sizeof(TPE) <= wordaligned( MosaicBlkSize + (i*hdr->bits)/8,TPE))\
			return 0.0;\
		if( task->dst +  wordaligned(MosaicBlkSize + (i*hdr->bits)/8,sizeof(TPE)) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)\
			return 0.0;\
		if(i) factor = ((flt) i * sizeof(TPE))/ wordaligned(MosaicBlkSize + sizeof(int) + (i*hdr->bits)/8,TPE);\
		return factor;\
	}\
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,hdr->dict.val##TPE,*val,0,hdr->dictsize);\
		if( j == hdr->dictsize || hdr->dict.val##TPE[j] != *val )\
			break;\
	}\
	if( i * sizeof(TPE) <= wordaligned( MosaicBlkSize + (i * hdr->bits)/8 ,TPE))\
		return 0.0;\
	if(i) factor = (flt) ((int)i * sizeof(TPE)) / wordaligned( MosaicBlkSize + (i * hdr->bits)/8,TPE);\
}

// Create a larger dictionary buffer then we allow for in the mosaic header first
// Store the most frequent ones in the compressed heap header directly based on estimated savings
// Improve by using binary search rather then linear scan
#define TMPDICT 16*256

#define makeDict(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start,v,w;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	lng cw,cv;\
	for(i = 0; i< limit; i++, val++){\
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
			dict.val##TPE[j]= (TPE) *val;\
			cnt[j] = 1;\
		} else cnt[j]++;\
} }


/* Take a larger sample before fixing the dictionary */
void
MOScreatedictionary(Client cntxt, MOStask task)
{
	BUN i;
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

	(void) cntxt;
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
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: //makeDict(bte); break;
{	bte *val = ((bte*)task->src) + task->start,v,w;
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
	int cw,cv;
	for(i = 0; i< limit; i++, val++){
		MOSfind(j,dict.valbte,*val,0,dictsize);
		if(j == dictsize && dictsize == 0 ){
			dict.valbte[j]= *val;
			cnt[j] = 1;
			dictsize++;
		} else  
		if(dictsize < TMPDICT &&  dict.valbte[j] != *val){
			w= *val; cw= 1;
			for( ; j< dictsize; j++)
			if (dict.valbte[j] > w){
				v =dict.valbte[j];
				dict.valbte[j]= w;
				w = v;
				cv = cnt[j];
				cnt[j]= cw;
				cw = cv;
			}
			dictsize++;
			dict.valbte[j]= w;
			cnt[j] = cw;
		} else cnt[j]++;
} }
	break;
		case 2: makeDict(sht); break;
		case 4: makeDict(int); break;
		case 8: makeDict(lng); break;
		}
		break;
#ifdef _DEBUG_MOSAIC_
	default:
		mnstr_printf(cntxt->fdout,"#does not support dictionary type %d\n",ATOMbasetype(task->type));
#endif
	}
	/* find the 256 most frequent values and save them in the mosaic header */
	if( dictsize < 256){
#ifdef HAVE_HGE
		memcpy((char*)&task->hdr->dict,  (char*)&dict, dictsize * sizeof(hge));
#else
		memcpy((char*)&task->hdr->dict,  (char*)&dict, dictsize * sizeof(lng));
#endif
		memcpy((char*)task->hdr->dictfreq,  (char*)&cnt, dictsize * sizeof(lng));
		hdr->dictsize = dictsize;
	} else {
		/* brute force search of the top-k */
		for(j=0; j< 256; j++){
			for(max = 0; max <dictsize && keep[max]; max++){}
			for(k=0; k< dictsize; k++)
			if( keep[k]==0){
				if( cnt[k]> cnt[max]) max = k;
			}
			keep[max]=1;
		}
		/* keep the top-k, in order */
		for( j=k=0; k<dictsize; k++)
		if( keep[k]){
#ifdef HAVE_HGE
			task->hdr->dict.valhge[j] = dict.valhge[k];
#else
			task->hdr->dict.vallng[j] = dict.vallng[k];
#endif
			task->hdr->dictfreq[j] = cnt[k];
		}
		hdr->dictsize = j;
		assert(j<256);
	}
	/* calculate the bit-width */
	hdr->bits = 1;
	hdr->mask =1;
	for( j=2 ; j < dictsize; j *=2){
		hdr->bits++;
		hdr->mask = (hdr->mask <<1) | 1;
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_dictionary(cntxt, task);
#endif
}

// calculate the expected reduction using DICT in terms of elements compressed
flt
MOSestimate_dictionary(Client cntxt, MOStask task)
{	
	BUN i = 0;
	int j;
	flt factor= 0.0;
	MosaicHdr hdr = task->hdr;
	(void) cntxt;

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
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: //estimateDict(bte); break;
{	bte *val = ((bte*)task->src) + task->start;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	if( task->range[MOSAIC_DICT] > task->start){\
		i = task->range[MOSAIC_DICT] - task->start;\
		if ( i > MOSlimit() ) i = MOSlimit();\
		if( i * sizeof(bte) <= wordaligned( MosaicBlkSize + i,bte))\
			return 0.0;\
		if( task->dst +  wordaligned(MosaicBlkSize + i,sizeof(bte)) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)\
			return 0.0;\
		if(i) factor = (flt) ((int)i * sizeof(bte)) / wordaligned( MosaicBlkSize + (i*hdr->bits)/8,bte);
		return factor;\
	}\
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,hdr->dict.valbte,*val,0,hdr->dictsize);\
		if( j == hdr->dictsize || hdr->dict.valbte[j] != *val )\
			break;\
	}\
	if( i * sizeof(bte) <= wordaligned( MosaicBlkSize + (i * hdr->bits)/8,bte))\
		return 0.0;\
	if(i) factor = (flt) ((int) i * sizeof(bte)) / wordaligned( MosaicBlkSize + (i*hdr->bits)/8,bte);\
}
break;
		case 2: estimateDict(sht); break;
		case 4: estimateDict(int); break;
		case 8: estimateDict(lng); break;
		}
		break;
#ifdef _DEBUG_MOSAIC_
	default:
		mnstr_printf(cntxt->fdout,"#does not support dictionary type %d\n",ATOMbasetype(task->type));
#endif
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate dict "BUNFMT" elm %4.2f factor\n", i, factor);
#endif
	task->factor[MOSAIC_DICT] = factor;
	task->range[MOSAIC_DICT] = task->start + i;
	return factor; 
}

// insert a series of values into the compressor block using dictionary

#define DICTcompress(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start;\
	BitVector base = (BitVector) MOScodevector(task);\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
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
MOScompress_dictionary(Client cntxt, MOStask task)
{
	BUN i;
	int j;
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;

	task->dst = MOScodevector(task);

	(void) cntxt;
	MOSsetTag(blk,MOSAIC_DICT);
	MOSsetCnt(blk,0);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTcompress(sht); break;
	case TYPE_sht: DICTcompress(sht); break;
	case TYPE_int: DICTcompress(int); break;
	case TYPE_lng: DICTcompress(lng); break;
	case TYPE_oid: DICTcompress(oid); break;
	case TYPE_flt: DICTcompress(flt); break;
	case TYPE_dbl: DICTcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTcompress(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: DICTcompress(bte); break;
		case 2: DICTcompress(sht); break;
		case 4: DICTcompress(int); break;
		case 8: DICTcompress(lng); break;
		}
		break;
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
MOSdecompress_dictionary(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;
	BUN i;
	int j;
	BitVector base;
	(void) cntxt;

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
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: DICTdecompress(bte); break;
		case 2: DICTdecompress(sht); break;
		case 4: DICTdecompress(int); break;
		case 8: DICTdecompress(lng); break;
		}
		break;
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define subselect_dictionary_str(TPE) \
	throw(MAL,"mosaic.dictionary","TBD");
#define subselect_dictionary(TPE) {\
	base = (BitVector) MOScodevector(task);\
	if( !*anti){\
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
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
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			/* nothing is matching */\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
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
MOSsubselect_dictionary(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN i, first,last;
	MosaicHdr hdr = task->hdr;
	int cmp;
	bte j;
	BitVector base;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_dictionary(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bte: subselect_dictionary(bte); break;
	case TYPE_sht: subselect_dictionary(sht); break;
	case TYPE_int: subselect_dictionary(int); break;
	case TYPE_lng: subselect_dictionary(lng); break;
	case TYPE_oid: subselect_dictionary(oid); break;
	case TYPE_flt: subselect_dictionary(flt); break;
	case TYPE_dbl: subselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_dictionary(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: subselect_dictionary_str(bte); break;
		case 2: subselect_dictionary_str(sht); break;
		case 4: subselect_dictionary_str(int); break;
		case 8: subselect_dictionary_str(lng); break;
		}
		break;
	}
	MOSskip_dictionary(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_dictionary_str(TPE)\
	throw(MAL,"mosaic.dictionary","TBD");

#define thetasubselect_dictionary(TPE)\
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
		if( (low == TPE##_nil || task->hdr->dict.val##TPE[j] >= low) && (task->hdr->dict.val##TPE[j] <= hgh || hgh == TPE##_nil) ){\
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
MOSthetasubselect_dictionary(Client cntxt,  MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	MosaicHdr hdr = task->hdr;
	bte j;
	BitVector base;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_dictionary(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bte: thetasubselect_dictionary(bte); break;
	case TYPE_sht: thetasubselect_dictionary(sht); break;
	case TYPE_int: thetasubselect_dictionary(int); break;
	case TYPE_lng: thetasubselect_dictionary(lng); break;
	case TYPE_oid: thetasubselect_dictionary(oid); break;
	case TYPE_flt: thetasubselect_dictionary(flt); break;
	case TYPE_dbl: thetasubselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_dictionary(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: thetasubselect_dictionary_str(bte); break;
		case 2: thetasubselect_dictionary_str(sht); break;
		case 4: thetasubselect_dictionary_str(int); break;
		case 8: thetasubselect_dictionary_str(lng); break;
		}
		break;
	}
	MOSskip_dictionary(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_dictionary_str(TPE)\
	throw(MAL,"mosaic.dictionary","TBD");
#define projection_dictionary(TPE)\
{	TPE *v;\
	base = (BitVector) MOScodevector(task);\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++){\
		MOSskipit();\
		j= getBitVector(base,i,(int) hdr->bits); \
		*v++ = task->hdr->dict.val##TPE[j];\
		task->n--;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_dictionary(Client cntxt,  MOStask task)
{
	BUN i,first,last;
	MosaicHdr hdr = task->hdr;
	unsigned short j;
	BitVector base;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_dictionary(bte); break;
		case TYPE_sht: projection_dictionary(sht); break;
		case TYPE_lng: projection_dictionary(lng); break;
		case TYPE_oid: projection_dictionary(oid); break;
		case TYPE_flt: projection_dictionary(flt); break;
		case TYPE_dbl: projection_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_dictionary(hge); break;
#endif
		case TYPE_int:
		{	int *v;
			base  = (BitVector) (((char*) task->blk) + MosaicBlkSize);
			v= (int*) task->src;
			for(i=0 ; first < last; first++, i++){
				MOSskipit();
				j= getBitVector(base,i,(int) hdr->bits); \
				*v++ = task->hdr->dict.valint[j];
				task->n--;
				task->cnt++;
			}
			task->src = (char*) v;
		}
		break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: projection_dictionary_str(bte); break;
		case 2: projection_dictionary_str(sht); break;
		case 4: projection_dictionary_str(int); break;
		case 8: projection_dictionary_str(lng); break;
		}
		break;
	}
	MOSskip_dictionary(cntxt,task);
	return MAL_SUCCEED;
}

#define join_dictionary_str(TPE)\
	throw(MAL,"mosaic.dictionary","TBD");

#define join_dictionary(TPE)\
{	TPE  *w;\
	BitVector base = (BitVector) MOScodevector(task);\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->elm; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			j= getBitVector(base,i,(int) hdr->bits); \
			if ( *w == task->hdr->dict.val##TPE[j]){\
				BUNappend(task->lbat, &oo, FALSE);\
				BUNappend(task->rbat, &o, FALSE);\
			}\
		}\
	}\
}

str
MOSsubjoin_dictionary(Client cntxt,  MOStask task)
{
	BUN i,n,limit;
	oid o, oo;
	MosaicHdr hdr = task->hdr;
	int j;
	(void) cntxt;

	// set the oid range covered and advance scan range
	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_dictionary(bte); break;
		case TYPE_sht: join_dictionary(sht); break;
		case TYPE_int: join_dictionary(int); break;
		case TYPE_lng: join_dictionary(lng); break;
		case TYPE_oid: join_dictionary(oid); break;
		case TYPE_flt: join_dictionary(flt); break;
		case TYPE_dbl: join_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_dictionary(hge); break;
#endif
		case TYPE_str:
			switch(task->bsrc->twidth){
			case 1: join_dictionary_str(bte); break;
			case 2: join_dictionary_str(sht); break;
			case 4: join_dictionary_str(int); break;
			case 8: join_dictionary_str(lng); break;
			}
			break;
	}
	MOSskip_dictionary(cntxt,task);
	return MAL_SUCCEED;
}
