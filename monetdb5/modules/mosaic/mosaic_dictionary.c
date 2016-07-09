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
	int *dst = (int*)  (((char*) task->blk) + MosaicBlkSize);
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
	void *val = (void*)task->hdr->dict;

	switch(ATOMbasetype(task->type)){
	case TYPE_sht:
		snprintf(buf,len,"%hd", ((sht*) val)[i]); break;
	case TYPE_int:
		snprintf(buf,len,"%d", ((int*) val)[i]); break;
	case  TYPE_oid:
		snprintf(buf,len,OIDFMT,  ((oid*) val)[i]); break;
	case  TYPE_lng:
		snprintf(buf,len,LLFMT,  ((lng*) val)[i]); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		snprintf(buf,len,"%.40g",  (dbl) ((hge*) val)[i]); break;
#endif
	case TYPE_flt:
		snprintf(buf,len,"%f", ((flt*) val)[i]); break;
	case TYPE_dbl:
		snprintf(buf,len,"%g", ((dbl*) val)[i]); break;
	}
}

void
MOSdump_dictionary(Client cntxt, MOStask task)
{
	int i;
	char buf[BUFSIZ];

	mnstr_printf(cntxt->fdout,"#bits %d",task->hdr->bits);
	for(i=0; i< task->hdr->dictsize; i++){
		MOSdump_dictionaryInternal(buf, BUFSIZ, task,i);
		mnstr_printf(cntxt->fdout,"[%d] %s ",i,buf);
	}
	mnstr_printf(cntxt->fdout,"\n");
}

void
MOSlayout_dictionary_hdr(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	int i;
	char buf[BUFSIZ];

	(void) cntxt;
	for(i=0; i< task->hdr->dictsize; i++){
		MOSdump_dictionaryInternal(buf, BUFSIZ, task,i);
		BUNappend(btech, "dictionary_hdr", FALSE);
		BUNappend(bcount, &zero, FALSE);
		BUNappend(binput, &zero, FALSE);
		BUNappend(boutput, &task->hdr->dictfreq[i], FALSE);
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

#define MOSfind(X,VAL,F,L)\
{ int m,f= F, l=L; \
   while( l-f > 0 ) { \
	m = f + (l-f)/2;\
	if ( VAL < dict[m] ) l=m-1; else f= m;\
	if ( VAL > dict[m] ) f=m+1; else l= m;\
   }\
   X= f;\
}

#define estimateDict(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start;\
	TPE *dict= (TPE*)hdr->dict;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	if( task->range[MOSAIC_DICT] > task->start){\
		i = task->range[MOSAIC_DICT] - task->start;\
		if ( i > MOSlimit() ) i = MOSlimit();\
		if( i * sizeof(TPE) <= wordaligned( MosaicBlkSize + i,TPE))\
			return 0.0;\
		if( task->dst +  wordaligned(MosaicBlkSize + i,sizeof(TPE)) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)\
			return 0.0;\
		if(i) factor = ((flt) i * sizeof(TPE))/ wordaligned(MosaicBlkSize + sizeof(int) + i,TPE);\
		return factor;\
	}\
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,*val,0,hdr->dictsize);\
		if( j == hdr->dictsize || dict[j] != *val )\
			break;\
	}\
	if( i * sizeof(TPE) <= wordaligned( MosaicBlkSize + i,TPE))\
		return 0.0;\
	if(i) factor = (flt) ((int)i * sizeof(int)) / wordaligned( MosaicBlkSize + i,TPE);\
}

// store it in the compressed heap header directly
// filter out the most frequent ones
#define makeDict(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start;\
	TPE *dict = (TPE*)hdr->dict,v;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	for(i = 0; i< limit; i++, val++){\
		for(j= 0; j< hdr->dictsize; j++)\
			if( dict[j] == *val) break;\
		if ( j == hdr->dictsize){\
			if ( hdr->dictsize == 256){\
				int min = 0;\
				for(j=1;j<256;j++)\
					if( cnt[min] <cnt[j]) min = j;\
				j=min;\
				cnt[j]=0;\
				break;\
			}\
			dict[j] = *val;\
			cnt[j]++;\
			hdr->dictsize++;\
		} else\
			cnt[j]++;\
	}\
	for(k=0; k< hdr->dictsize; k++)\
		for(j=k+1; j< hdr->dictsize; j++)\
			if(dict[k] >dict[j]){\
				v= dict[k];\
				dict[k] = dict[j];\
				dict[j] = v;\
			}\
	hdr->bits = 1;\
	hdr->mask =1;\
	for( k=2 ; k < hdr->dictsize; k *=2){\
		hdr->bits++;\
		hdr->mask = (hdr->mask <<1) | 1;\
	}\
}


void
MOScreatedictionary(Client cntxt, MOStask task)
{
	BUN i;
	int j,k;
	MosaicHdr hdr = task->hdr;
	lng cnt[256];

	(void) cntxt;
	memset((char*) cnt, 0, sizeof(cnt));
	hdr->dictsize = 0;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: break; // no compression achievable
	case TYPE_sht: makeDict(sht); break;
	case TYPE_int: makeDict(int); break;
	case TYPE_lng: makeDict(lng); break;
	case TYPE_oid: makeDict(oid); break;
	case TYPE_flt: makeDict(flt); break;
	case TYPE_dbl: makeDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: makeDict(hge); break;
#endif
	default:
		mnstr_printf(cntxt->fdout,"#does not support dictionary type %d\n",ATOMbasetype(task->type));
	}
	MOSdump_dictionary(cntxt, task);
#ifdef _DEBUG_MOSAIC_
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
	case TYPE_bte: break; // no compression achievable
	case TYPE_sht: estimateDict(sht); break;
	case TYPE_int: estimateDict(int); break;
	case TYPE_oid: estimateDict(oid); break;
	case TYPE_flt: estimateDict(flt); break;
	case TYPE_dbl: estimateDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateDict(hge); break;
#endif
	case TYPE_lng:
		{	lng *val = ((lng*)task->src) + task->start;
			lng *dict = (lng*)hdr->dict;
			// assume uniform compression statistics
			if( task->range[MOSAIC_DICT] > task->start){
				i = task->range[MOSAIC_DICT] - task->start;
				if ( i > MOSlimit() ) i = MOSlimit();
				if( i * sizeof(lng) <= wordaligned( MosaicBlkSize + i,lng))
					return 0.0;
				if( task->dst +  wordaligned(MosaicBlkSize + i,sizeof(lng)) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
					return 0.0;
				if(i) factor = ((flt) i * sizeof(lng))/ wordaligned(MosaicBlkSize + sizeof(lng) + i,lng);
				return factor;
			}

			for(i =task->start; i<task->stop; i++, val++){
				MOSfind(j,*val,0,hdr->dictsize);
				if( j == hdr->dictsize || dict[j] != *val)
					break;
			}
			i -= task->start;
			if ( i > MOSlimit() ) i = MOSlimit();
			if( i * sizeof(lng) < wordaligned( MosaicBlkSize + i,lng))
				return 0.0;
			if(i) factor = (flt) ((lng)i * sizeof(lng)) / wordaligned( MosaicBlkSize + i,lng);
		}
		break;
	default:
		mnstr_printf(cntxt->fdout,"#does not support dictionary type %d\n",ATOMbasetype(task->type));
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate dict "BUNFMT" elm %4.2f factor\n", i, factor);
#endif
	task->factor[MOSAIC_DICT] = factor;
	task->range[MOSAIC_DICT] = task->start + i;
	return factor; 
}

// insert a series of values into the compressor block using dictionary
#define dictcompress(I,Bits,Value) setBitVector(base,I,Bits,Value);

#define DICTcompress(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start;\
	TPE *dict = (TPE*)hdr->dict;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	for(i =0; i<limit; i++, val++){\
		hdr->checksum.sum##TPE += *val;\
		MOSfind(j,*val,0,hdr->dictsize);\
		if(j == hdr->dictsize || dict[j] != *val) \
			break;\
		else {\
			hdr->dictfreq[j]++;\
			MOSincCnt(blk,1);\
			dictcompress(i,hdr->bits,(unsigned int)j);\
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
	BitVector base;

	task->dst = ((char*) task->blk)+ MosaicBlkSize;
	base  = (BitVector) task->dst; 

	(void) cntxt;
	MOSsetTag(blk,MOSAIC_DICT);
	MOSsetCnt(blk,0);

	switch(ATOMbasetype(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
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
#define dictdecompress(I) j= getBitVector(base,I,(int) hdr->bits); 

#define DICTdecompress(TPE)\
{	TPE *dict =(TPE*)((char*)hdr->dict);\
	BUN lim = MOSgetCnt(blk);\
	base = (BitVector)(((char*)blk) +  MosaicBlkSize);\
	for(i = 0; i < lim; i++){\
		dictdecompress(i);\
		((TPE*)task->src)[i] = dict[j];\
		hdr->checksum2.sum##TPE += dict[j];\
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
	//case TYPE_bte: CASE_bit: no compression achievable
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

#define subselect_dictionary(TPE) {\
 	TPE *dict= (TPE*) hdr->dict;\
	base = (BitVector) (((char*) task->blk) + MosaicBlkSize);\
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
				dictdecompress(i); \
				cmp  =  ((*hi && dict[j] <= * (TPE*)hgh ) || (!*hi && dict[j] < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for(i=0; first < last; first++, i++){\
				MOSskipit();\
				dictdecompress(i); \
				cmp  =  ((*li && dict[j] >= * (TPE*)low ) || (!*li && dict[j] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				dictdecompress(i); \
				cmp  =  ((*hi && dict[j] <= * (TPE*)hgh ) || (!*hi && dict[j] < *(TPE*)hgh )) &&\
						((*li && dict[j] >= * (TPE*)low ) || (!*li && dict[j] > *(TPE*)low ));\
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
				dictdecompress(i); \
				cmp  =  ((*hi && dict[j] <= * (TPE*)hgh ) || (!*hi && dict[j] < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				dictdecompress(i); \
				cmp  =  ((*li && dict[j] >= * (TPE*)low ) || (!*li && dict[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				dictdecompress(i); \
				cmp  =  ((*hi && dict[j] <= * (TPE*)hgh ) || (!*hi && dict[j] < *(TPE*)hgh )) &&\
						((*li && dict[j] >= * (TPE*)low ) || (!*li && dict[j] > *(TPE*)low ));\
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

	switch(task->type){
	case TYPE_sht: subselect_dictionary(sht); break;
	case TYPE_int: subselect_dictionary(int); break;
	case TYPE_lng: subselect_dictionary(lng); break;
	case TYPE_oid: subselect_dictionary(oid); break;
	case TYPE_flt: subselect_dictionary(flt); break;
	case TYPE_dbl: subselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_dictionary(hge); break;
#endif
	default:
		if( task->type == TYPE_date)
			subselect_dictionary(date);
		if( task->type == TYPE_daytime)
			subselect_dictionary(daytime);
		if( task->type == TYPE_timestamp)
		{ 	lng *dict= (lng*) hdr->dict;
			int lownil = timestamp_isnil(*(timestamp*)low);
			int hghnil = timestamp_isnil(*(timestamp*)hgh);
			base = (BitVector) (((char*) task->blk) + MosaicBlkSize);

			if( !*anti){
				if( lownil && hghnil){
					for( ; first < last; first++){
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( lownil){
					for( i=0 ; first < last; first++, i++){
						MOSskipit();
						dictdecompress(i);
						cmp  =  ((*hi && dict[j] <= * (lng*)hgh ) || (!*hi && dict[j] < *(lng*)hgh ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else
				if( hghnil){
					for(i=0 ; first < last; first++, i++){
						MOSskipit();
						dictdecompress(i);
						cmp  =  ((*li && dict[j] >= * (lng*)low ) || (!*li && dict[j] > *(lng*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else{
					for(i=0 ; first < last; first++, i++){
						MOSskipit();
						dictdecompress(i);
						cmp  =  ((*hi && dict[j] <= * (lng*)hgh ) || (!*hi && dict[j] < *(lng*)hgh )) &&
								((*li && dict[j] >= * (lng*)low ) || (!*li && dict[j] > *(lng*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				}
			} else {
				if( lownil && hghnil){
					/* nothing is matching */
				} else
				if( lownil){
					for(i=0 ; first < last; first++, i++){
						MOSskipit();
						dictdecompress(i);
						cmp  =  ((*hi && dict[i] <= * (lng*)hgh ) || (!*hi && dict[i] < *(lng*)hgh ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else
				if( hghnil){
					for(i=0 ; first < last; first++, i++){
						MOSskipit();
						dictdecompress(i);
						cmp  =  ((*li && dict[i] >= * (lng*)low ) || (!*li && dict[i] > *(lng*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else{
					for( i=0 ; first < last; first++, i++){
						MOSskipit();
						dictdecompress(i);
						cmp  =  ((*hi && dict[j] <= * (lng*)hgh ) || (!*hi && dict[j] < *(lng*)hgh )) &&
								((*li && dict[j] >= * (lng*)low ) || (!*li && dict[j] > *(lng*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				}
			}
		}
	}
	MOSskip_dictionary(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_dictionary(TPE)\
{ 	TPE low,hgh;\
	TPE *dict= (TPE*) hdr->dict;\
	base = (BitVector) (((char*) task->blk) + MosaicBlkSize);\
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
		dictdecompress(first); \
		if( (low == TPE##_nil || dict[j] >= low) && (dict[j] <= hgh || hgh == TPE##_nil) ){\
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
	BUN i,first,last;
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

	switch(task->type){
	case TYPE_sht: thetasubselect_dictionary(sht); break;
	case TYPE_int: thetasubselect_dictionary(int); break;
	case TYPE_lng: thetasubselect_dictionary(lng); break;
	case TYPE_oid: thetasubselect_dictionary(oid); break;
	case TYPE_flt: thetasubselect_dictionary(flt); break;
	case TYPE_dbl: thetasubselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_dictionary(hge); break;
#endif
	default:
		if( task->type == TYPE_timestamp){
		{ 	lng *dict= (lng*) hdr->dict;
			lng low,hgh;
			base = (BitVector) (((char*) task->blk) + MosaicBlkSize);

			low= hgh = int_nil;
			if ( strcmp(oper,"<") == 0){
				hgh= *(lng*) val;
				hgh = PREVVALUElng(hgh);
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *(lng*) val;
			} else
			if ( strcmp(oper,">") == 0){
				low = *(lng*) val;
				low = NEXTVALUElng(low);
			} else
			if ( strcmp(oper,">=") == 0){
				low = *(lng*) val;
			} else
			if ( strcmp(oper,"!=") == 0){
				hgh= low= *(lng*) val;
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(lng*) val;
			} 
			for(i=0 ; first < last; first++,i++){
				MOSskipit();
				dictdecompress(i);
				if( (low == int_nil || dict[j] >= low) && (dict[j] <= hgh || hgh == int_nil) ){
					if ( !anti) {
						*o++ = (oid) first;
					}
				} else
				if( anti){
					*o++ = (oid) first;
				}
			}
		} 
		}
	}
	MOSskip_dictionary(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_dictionary(TPE)\
{	TPE *v;\
	TPE *dict= (TPE*) hdr->dict;\
	base = (BitVector) (((char*) task->blk) + MosaicBlkSize);\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++){\
		MOSskipit();\
		dictdecompress(i);\
		*v++ = dict[j];\
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
			int *dict= (int*) hdr->dict;
			base  = (BitVector) (((char*) task->blk) + MosaicBlkSize);
			v= (int*) task->src;
			for(i=0 ; first < last; first++, i++){
				MOSskipit();
				dictdecompress(i);
				*v++ = dict[j];
				task->n--;
				task->cnt++;
			}
			task->src = (char*) v;
		}
	}
	MOSskip_dictionary(cntxt,task);
	return MAL_SUCCEED;
}

#define join_dictionary(TPE)\
{	TPE  *w;\
	TPE *dict= (TPE*) hdr->dict;\
	base = (BitVector) (((char*) task->blk) + MosaicBlkSize);\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->elm; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			dictdecompress(i);\
			if ( *w == dict[j]){\
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
	BitVector base;
	(void) cntxt;

	// set the oid range covered and advance scan range
	switch(ATOMbasetype(task->type)){
		case TYPE_sht: join_dictionary(sht); break;
		case TYPE_int: join_dictionary(int); break;
		case TYPE_lng: join_dictionary(lng); break;
		case TYPE_oid: join_dictionary(oid); break;
		case TYPE_flt: join_dictionary(flt); break;
		case TYPE_dbl: join_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_dictionary(hge); break;
#endif
	}
	MOSskip_dictionary(cntxt,task);
	return MAL_SUCCEED;
}
