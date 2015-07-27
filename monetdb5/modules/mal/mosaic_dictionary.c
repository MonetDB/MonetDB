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
 * (c)2014 author Martin Kersten
 * Global dictionary encoding
 * Index value zero is not used to easy detection of filler
 * The dictionary index size is derived from the number of entries covered.
 * It leads to a compact n-bit representation.
 * Floating points are not expected to be replicated 
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_dictionary.h"

void
MOSadvance_dictionary(Client cntxt, MOStask task)
{
	int *dst = (int*)  (((char*) task->blk) + MosaicBlkSize);
	long cnt = MOSgetCnt(task->blk);
	long bytes;
	(void) cntxt;

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->stop = task->elm;
	bytes =  (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, lng)); 
}

/* Beware, the dump routines use the compressed part of the task */
void
MOSdump_dictionary(Client cntxt, MOStask task)
{
	MosaicHdr hdr= task->hdr;
	int i;
	void *val = (void*)hdr->dict;

	mnstr_printf(cntxt->fdout,"# bits %d",hdr->bits);
	switch(ATOMstorage(task->type)){
	case TYPE_sht:
		for(i=0; i< hdr->dictsize; i++)
		mnstr_printf(cntxt->fdout,"sht [%d] %hd ",i, ((sht*) val)[i]); break;
	case TYPE_int:
		for(i=0; i< hdr->dictsize; i++)
		mnstr_printf(cntxt->fdout,"int [%d] %d ",i, ((int*) val)[i]); break;
	case  TYPE_oid:
		for(i=0; i< hdr->dictsize; i++)
		mnstr_printf(cntxt->fdout,"oid [%d] "OIDFMT, i, ((oid*) val)[i]); break;
	case  TYPE_lng:
		for(i=0; i< hdr->dictsize; i++)
		mnstr_printf(cntxt->fdout,"lng [%d] "LLFMT, i, ((lng*) val)[i]); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		for(i=0; i< hdr->dictsize; i++)
		mnstr_printf(cntxt->fdout,"hge [%d] %.40g ", i, (dbl) ((hge*) val)[i]); break;
#endif
	case  TYPE_wrd:
		for(i=0; i< hdr->dictsize; i++)
		mnstr_printf(cntxt->fdout,"wrd [%d] "SZFMT, i, ((wrd*) val)[i]); break;
	case TYPE_flt:
		for(i=0; i< hdr->dictsize; i++)
		mnstr_printf(cntxt->fdout,"flt [%d] %f ",i, ((flt*) val)[i]); break;
	case TYPE_dbl:
		for(i=0; i< hdr->dictsize; i++)
		mnstr_printf(cntxt->fdout,"dbl [%d] %g ",i, ((dbl*) val)[i]); break;
	}
	mnstr_printf(cntxt->fdout,"\n");
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
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,*val,0,hdr->dictsize);\
		if( j == hdr->dictsize || dict[j] != *val )\
			break;\
	}\
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
	for(i=0; i< (BUN) hdr->dictsize; i++)\
		for(j=i+1; j< hdr->dictsize; j++)\
			if(dict[i] >dict[j]){\
				v= dict[i];\
				dict[i] = dict[j];\
				dict[j] = v;\
			}\
	hdr->bits = 1;\
	hdr->mask =1;\
	for( i=2 ; i < (BUN) hdr->dictsize; i *=2){\
		hdr->bits++;\
		hdr->mask = (hdr->mask <<1) | 1;\
	}\
}


void
MOScreatedictionary(Client cntxt, MOStask task)
{	BUN i;
	int j;
	MosaicHdr hdr = task->hdr;
	lng cnt[256];

	(void) cntxt;
	for(j=0;j<256;j++)
		cnt[j]=0;
	hdr->dictsize = 0;
	switch(ATOMstorage(task->type)){
	case TYPE_sht: makeDict(sht); break;
	case TYPE_lng: makeDict(lng); break;
	case TYPE_oid: makeDict(oid); break;
	case TYPE_wrd: makeDict(wrd); break;
	case TYPE_flt: makeDict(flt); break;
	case TYPE_dbl: makeDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: makeDict(hge); break;
#endif
	case TYPE_int:
		{	int *val = ((int*)task->src) + task->start;
			int *dict = (int*)hdr->dict,v;
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;

			for(i =0; i< limit; i++, val++){
				for(j= 0; j< hdr->dictsize; j++)
					if( dict[j] == *val) break;
				if ( j == hdr->dictsize){
					if ( hdr->dictsize == 256){
						int min = 0;
						// select low frequent candidate
						for(j=1;j<256;j++)
							if( cnt[min] <cnt[j]) min = j;
						j=min;
						cnt[j]=0;
						break;
					}
					dict[j] = *val;
					cnt[j]++;
					hdr->dictsize++;
				} else
					cnt[j]++;
			}
			// sort it
			for(i=0; i< (BUN) hdr->dictsize; i++)
				for(j=i+1; j< hdr->dictsize; j++)
					if(dict[i] >dict[j]){
						v= dict[i];
						dict[i] = dict[j];
						dict[j] = v;
					}
			hdr->bits = 1;
			hdr->mask =1;
			for( i=2 ; i < (BUN) hdr->dictsize; i *=2){
				hdr->bits++;
				hdr->mask = (hdr->mask <<1) | 1;
			}
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_dictionary(cntxt, task);
#endif
}

// calculate the expected reduction using DICT in terms of elements compressed
flt
MOSestimate_dictionary(Client cntxt, MOStask task)
{	BUN i = -1;
	int j;
	flt factor= 1.0;
	MosaicHdr hdr = task->hdr;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: estimateDict(sht); break;
	case TYPE_lng: estimateDict(lng); break;
	case TYPE_oid: estimateDict(oid); break;
	case TYPE_wrd: estimateDict(wrd); break;
	case TYPE_flt: estimateDict(flt); break;
	case TYPE_dbl: estimateDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateDict(hge); break;
#endif
	case TYPE_int:
		{	int *val = ((int*)task->src) + task->start;
			int *dict = (int*)hdr->dict;
			for(i =task->start; i<task->stop; i++, val++){
				MOSfind(j,*val,0,hdr->dictsize);
				if( j == hdr->dictsize || dict[j] != *val)
					break;
			}
			i -= task->start;
			if ( i > MOSlimit() ) i = MOSlimit();
			if(i) factor = (flt) ((int)i * sizeof(int)) / wordaligned( MosaicBlkSize + i,lng);
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate dict "BUNFMT" elm %4.2f factor\n", i, factor);
#endif
	return factor; 
}

// insert a series of values into the compressor block using dictionary
#define dictcompress(Vector,I,Bits,Value)\
{int cid,lshift,rshift;\
	cid = (I * Bits)/64;\
	lshift= 63 -((I * Bits) % 64) ;\
	if ( lshift >= Bits){\
		Vector[cid]= Vector[cid] | (((unsigned long) Value) << (lshift- Bits));\
	}else{ \
		rshift= 63 -  ((I+1) * Bits) % 64;\
		Vector[cid]= Vector[cid] | (((unsigned long) Value) >> (Bits-lshift));\
		Vector[cid+1]= 0 | (((unsigned long) Value)  << rshift);\
}}

#define DICTcompress(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start;\
	TPE *dict = (TPE*)hdr->dict;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	task->dst = ((char*) task->blk)+ MosaicBlkSize;\
	base  = (unsigned long*) task->dst; \
	base[0]=0;\
	for(i =0; i<limit; i++, val++){\
		hdr->checksum.sum##TPE += *val;\
		MOSfind(j,*val,0,hdr->dictsize);\
		if(j == hdr->dictsize || dict[j] != *val) \
			break;\
		else {\
			MOSincCnt(blk,1);\
			dictcompress(base,i,hdr->bits,j);\
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
	int cid, lshift, rshift;
	unsigned long *base;

	(void) cntxt;
	MOSsetTag(blk,MOSAIC_DICT);
	MOSsetCnt(blk,0);

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: DICTcompress(sht); break;
	case TYPE_int: DICTcompress(int); break;
	case TYPE_oid: DICTcompress(oid); break;
	case TYPE_wrd: DICTcompress(wrd); break;
	case TYPE_flt: DICTcompress(flt); break;
	case TYPE_dbl: DICTcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTcompress(hge); break;
#endif
	case TYPE_lng:
		{	lng *val = ((lng*)task->src) + task->start;
			lng *dict = (lng*)hdr->dict;
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;

			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			base  = (unsigned long*) task->dst; // start of bit vector
			base[0]=0;
			for(i =0; i<limit; i++, val++){
				hdr->checksum.sumlng += *val;
				MOSfind(j,*val,0,hdr->dictsize);
				//mnstr_printf(cntxt->fdout,"compress ["BUNFMT"] val %d index %d bits %d\n",i, *val,j,hdr->bits);
				if( j == hdr->dictsize || dict[j] != *val )
					break;
				else {
					MOSincCnt(blk,1);
					cid = i * hdr->bits/64;
					lshift= 63 -((i * hdr->bits) % 64) ;
					if ( lshift >= hdr->bits){
						base[cid]= base[cid] | (((unsigned long)j) << (lshift-hdr->bits));
						//mnstr_printf(cntxt->fdout,"[%d] shift %d rbits %d \n",cid, lshift, hdr->bits);
					}else{ 
						rshift= 63 -  ((i+1) * hdr->bits) % 64;
						base[cid]= base[cid] | (((unsigned long)j) >> (hdr->bits-lshift));
						base[cid+1]= 0 | (((unsigned long)j)  << rshift);
						//mnstr_printf(cntxt->fdout,"[%d] shift %d %d val %o %o\n", cid, lshift, rshift,
							//(j >> (hdr->bits-lshift)),  (j <<rshift));
					}
				} 
			}
			assert(i);
		}
	}
}

// the inverse operator, extend the src
#define dictdecompress(I)\
cid = (I * hdr->bits)/64;\
lshift= 63 -((I * hdr->bits) % 64) ;\
if ( lshift >= hdr->bits){\
	j = (base[cid]>> (lshift-hdr->bits)) & ((unsigned long)hdr->mask);\
  }else{ \
	rshift= 63 -  ((I+1) * hdr->bits) % 64;\
	m1 = (base[cid] & ( ((unsigned long)hdr->mask) >> (hdr->bits-lshift)));\
	m2 = (base[cid+1] >>rshift) & 0377;\
	j= ((m1 <<(hdr->bits-lshift)) | m2) & 0377;\
  }

#define DICTdecompress(TPE)\
{	TPE *dict =(TPE*)((char*)hdr->dict);\
	BUN lim = MOSgetCnt(blk);\
	base = (unsigned long*)(((char*)blk) +  MosaicBlkSize);\
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
	unsigned int j,m1,m2;
	int cid, lshift, rshift;
	unsigned long *base;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: DICTdecompress(sht); break;
	case TYPE_lng: DICTdecompress(lng); break;
	case TYPE_oid: DICTdecompress(oid); break;
	case TYPE_wrd: DICTdecompress(wrd); break;
	case TYPE_flt: DICTdecompress(flt); break;
	case TYPE_dbl: DICTdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTdecompress(hge); break;
#endif
	case TYPE_int:
		{	int *dict =(int*)((char*)hdr->dict);
			BUN lim = MOSgetCnt(blk);
			base  = (unsigned long*)(((char*)blk) + MosaicBlkSize);

			for(i = 0; i < lim; i++){
				cid = (i * hdr->bits)/64;
				lshift= 63 -((i * hdr->bits) % 64) ;
				if ( lshift >= hdr->bits){
					j = (base[cid]>> (lshift-hdr->bits)) & ((unsigned long)hdr->mask);
					//mnstr_printf(cntxt->fdout,"[%d] lshift %d m %d\n", cid,  lshift,m1);
				  }else{ 
					rshift= 63 -  ((i+1) * hdr->bits) % 64;
					m1 = (base[cid] & ( ((unsigned long)hdr->mask) >> (hdr->bits-lshift)));
					m2 = (base[cid+1] >>rshift);
					j= ((m1 <<(hdr->bits-lshift)) | m2) & 0377;\
					//mnstr_printf(cntxt->fdout,"[%d] shift %d %d cid %lo %lo val %o %o\n", cid, lshift, rshift,base[cid],base[cid+1], m1,  m2);
				  }
				((int*)task->src)[i] = dict[j];
				hdr->checksum2.sumint += dict[j];
			}
			task->src += i * sizeof(int);
		}
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define subselect_dictionary(TPE) {\
 	TPE *dict= (TPE*) hdr->dict;\
	base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);\
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
	bte j,m1,m2;
	int cid, lshift, rshift;
	unsigned long *base;
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
	case TYPE_lng: subselect_dictionary(lng); break;
	case TYPE_oid: subselect_dictionary(oid); break;
	case TYPE_wrd: subselect_dictionary(wrd); break;
	case TYPE_flt: subselect_dictionary(flt); break;
	case TYPE_dbl: subselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_dictionary(hge); break;
#endif
	case TYPE_int:
	// Expanded MOSselect_dictionary for debugging
	{ 	int *dict= (int*) hdr->dict;
		base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);

		if( !*anti){
			if( *(int*) low == int_nil && *(int*) hgh == int_nil){
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else
			if( *(int*) low == int_nil ){
				for(i=0 ; first < last; first++,i++){
					MOSskipit();
					dictdecompress(i);
					cmp  =  ((*hi && dict[j] <= * (int*)hgh ) || (!*hi && dict[j] < *(int*)hgh ));
					if (cmp )
						*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				for(i=0 ; first < last; first++, i++){
					MOSskipit();
					dictdecompress(i);
					cmp  =  ((*li && dict[j] >= * (int*)low ) || (!*li && dict[j] > *(int*)low ));
					if (cmp )
						*o++ = (oid) first;
				}
			} else{
				for(i=0 ; first < last; first++, i++){
					MOSskipit();
					dictdecompress(i);
					cmp  =  ((*hi && dict[j] <= * (int*)hgh ) || (!*hi && dict[j] < *(int*)hgh )) &&
							((*li && dict[j] >= * (int*)low ) || (!*li && dict[j] > *(int*)low ));
					if (cmp )
						*o++ = (oid) first;
				}
			}
		} else {
			if( *(int*) low == int_nil && *(int*) hgh == int_nil){
				/* nothing is matching */
			} else
			if( *(int*) low == int_nil ){
				for(i=0 ; first < last; first++, i++){
					MOSskipit();
					dictdecompress(i);
					cmp  =  ((*hi && dict[j] <= * (int*)hgh ) || (!*hi && dict[j] < *(int*)hgh ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				for(i=0 ; first < last; first++, i++){
					MOSskipit();
					dictdecompress(i);
					cmp  =  ((*li && dict[j] >= * (int*)low ) || (!*li && dict[j] > *(int*)low ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			} else{
				for( i=0 ; first < last; first++, i++){
					MOSskipit();
					dictdecompress(i);
					cmp  =  ((*hi && dict[j] <= * (int*)hgh ) || (!*hi && dict[j] < *(int*)hgh )) &&
							((*li && dict[j] >= * (int*)low ) || (!*li && dict[j] > *(int*)low ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			}
		}
	}
	break;
	default:
		if( task->type == TYPE_date)
			subselect_dictionary(date);
		if( task->type == TYPE_daytime)
			subselect_dictionary(daytime);
		if( task->type == TYPE_timestamp)
		{ 	lng *dict= (lng*) hdr->dict;
			int lownil = timestamp_isnil(*(timestamp*)low);
			int hghnil = timestamp_isnil(*(timestamp*)hgh);
			base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);

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
	base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);\
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
	bte j,m1,m2;
	int cid, lshift, rshift;
	unsigned long *base;
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
	case TYPE_lng: thetasubselect_dictionary(lng); break;
	case TYPE_oid: thetasubselect_dictionary(oid); break;
	case TYPE_wrd: thetasubselect_dictionary(wrd); break;
	case TYPE_flt: thetasubselect_dictionary(flt); break;
	case TYPE_dbl: thetasubselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_dictionary(hge); break;
#endif
	case TYPE_int:
		{ 	int low,hgh;
			int *dict= (int*) hdr->dict;
			base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);
			low= hgh = int_nil;
			if ( strcmp(oper,"<") == 0){
				hgh= *(int*) val;
				hgh = PREVVALUEint(hgh);
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *(int*) val;
			} else
			if ( strcmp(oper,">") == 0){
				low = *(int*) val;
				low = NEXTVALUEint(low);
			} else
			if ( strcmp(oper,">=") == 0){
				low = *(int*) val;
			} else
			if ( strcmp(oper,"!=") == 0){
				hgh= low= *(int*) val;
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(int*) val;
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
		break;
	default:
		if( task->type == TYPE_timestamp){
		{ 	lng *dict= (lng*) hdr->dict;
			lng low,hgh;
			base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);

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

#define leftfetchjoin_dictionary(TPE)\
{	TPE *v;\
	TPE *dict= (TPE*) hdr->dict;\
	base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);\
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
MOSleftfetchjoin_dictionary(Client cntxt,  MOStask task)
{
	BUN i,first,last;
	MosaicHdr hdr = task->hdr;
	bte j,m1,m2;
	int cid, lshift, rshift;
	unsigned long *base;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMstorage(task->type)){
		case TYPE_sht: leftfetchjoin_dictionary(sht); break;
		case TYPE_lng: leftfetchjoin_dictionary(lng); break;
		case TYPE_oid: leftfetchjoin_dictionary(oid); break;
		case TYPE_wrd: leftfetchjoin_dictionary(wrd); break;
		case TYPE_flt: leftfetchjoin_dictionary(flt); break;
		case TYPE_dbl: leftfetchjoin_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: leftfetchjoin_dictionary(hge); break;
#endif
		case TYPE_int:
		{	int *v;
			int *dict= (int*) hdr->dict;
			base  = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);
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
	base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);\
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
MOSjoin_dictionary(Client cntxt,  MOStask task)
{
	BUN i,n,limit;
	oid o, oo;
	MosaicHdr hdr = task->hdr;
	bte j,m1,m2;
	int cid, lshift, rshift;
	unsigned long *base;
	(void) cntxt;

	// set the oid range covered and advance scan range
	switch(ATOMstorage(task->type)){
		case TYPE_sht: join_dictionary(sht); break;
		case TYPE_lng: join_dictionary(lng); break;
		case TYPE_oid: join_dictionary(oid); break;
		case TYPE_wrd: join_dictionary(wrd); break;
		case TYPE_flt: join_dictionary(flt); break;
		case TYPE_dbl: join_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_dictionary(hge); break;
#endif
		case TYPE_int:
		{	int  *w;
			int *dict= (int*) hdr->dict;
			base = (unsigned long*) (((char*) task->blk) + MosaicBlkSize);
			w = (int*) task->src;
			limit= MOSgetCnt(task->blk);
			for( o=0, n= task->elm; n-- > 0; o++,w++ ){
				for(oo = task->start,i=0; i < limit; i++,oo++){
					//dictdecompress(i);
					cid = (i * hdr->bits)/64;
					lshift= 63 -((i * hdr->bits) % 64) ;
					if ( lshift >= hdr->bits){
						j = (base[cid]>> (lshift-hdr->bits)) & ((unsigned long)hdr->mask);
					  }else{ 
						rshift= 63 -  ((i+1) * hdr->bits) % 64;
						m1 = (base[cid] & ( ((unsigned long)hdr->mask) >> (hdr->bits-lshift)));
						m2 = (base[cid+1] >>rshift ) & 0377;
						j= ((m1 <<(hdr->bits-lshift)) | m2) & 0377;
					  }
					if ( *w == dict[j]){
						BUNappend(task->lbat, &oo, FALSE);
						BUNappend(task->rbat, &o, FALSE);
					}
				}
			}

		}
	}
	MOSskip_dictionary(cntxt,task);
	return MAL_SUCCEED;
}
