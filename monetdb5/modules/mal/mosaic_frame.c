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
 * Frame of reference compression with dictionary
 * A chunk is beheaded by a reference value F from the column. The elements V in the
 * chunk are replaced by an index into a global dictionary of V-F offsets.
 *
 * The dictionary is limited to 256 entries and all indices are at most one byte.
 * The maximal achievable compression ratio depends on the size of the dictionary
 *
 * This scheme is particularly geared at evolving time series, e.g. stock markets.
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_frame.h"

// we use longs as the basis for bit vectors
#define chunk_size(Task,Cnt) wordaligned(MosaicBlkSize + (Cnt * Task->hdr->framebits)/8 + (((Cnt * Task->hdr->framebits) %8) != 0), lng);

void
MOSadvance_frame(Client cntxt, MOStask task)
{
	int *dst = (int*)  (((char*) task->blk) + MosaicBlkSize);
	long cnt = MOSgetCnt(task->blk);
	long bytes;
	(void) cntxt;

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->stop = task->elm;
	bytes =  (cnt * task->hdr->framebits)/8 + (((cnt * task->hdr->framebits) %8) != 0) + sizeof(unsigned long);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, lng)); 
}

/* Beware, the dump routines use the compressed part of the task */
static void
MOSdump_frameInternal(char *buf, size_t len, MOStask task, int i)
{
	void * val = (void*) task->hdr->frame;
	switch(ATOMstorage(task->type)){
	case TYPE_sht:
		snprintf(buf,len,"sht [%d] %hd ",i, ((sht*) val)[i]); break;
	case TYPE_int:
		snprintf(buf,len,"int [%d] %d ",i, ((int*) val)[i]); break;
	case  TYPE_oid:
		snprintf(buf,len,"oid [%d] "OIDFMT, i, ((oid*) val)[i]); break;
	case  TYPE_lng:
		snprintf(buf,len,"lng [%d] "LLFMT, i, ((lng*) val)[i]); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		snprintf(buf,len,"hge [%d] %.40g ", i, (dbl) ((hge*) val)[i]); break;
#endif
	case  TYPE_wrd:
		snprintf(buf,len,"wrd [%d] "SZFMT, i, ((wrd*) val)[i]); break;
	case TYPE_flt:
		snprintf(buf,len,"flt [%d] %f ",i, ((flt*) val)[i]); break;
	case TYPE_dbl:
		snprintf(buf,len,"dbl [%d] %g ",i, ((dbl*) val)[i]); break;
	}
}

void
MOSdump_frame(Client cntxt, MOStask task)
{
	int i;
	char buf[BUFSIZ];

	mnstr_printf(cntxt->fdout,"# framebits %d",task->hdr->framebits);
	for(i=0; i< task->hdr->framesize; i++){
		MOSdump_frameInternal(buf, BUFSIZ, task,i);
		mnstr_printf(cntxt->fdout,"%s",buf);
	}
	mnstr_printf(cntxt->fdout,"\n");
}

void
MOSlayout_frame_hdr(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng cnt=0,i;
	char buf[BUFSIZ];

	(void) cntxt;
	for(i=0; i< task->hdr->framesize; i++){
		MOSdump_frameInternal(buf, BUFSIZ, task,i);
		BUNappend(btech, "frame_hdr", FALSE);
		BUNappend(bcount, &i, FALSE);
		BUNappend(binput, &cnt, FALSE);
		BUNappend(boutput, &cnt, FALSE);
		BUNappend(bproperties, buf, FALSE);
	}
}

void
MOSlayout_frame(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	(void) cntxt;
	BUNappend(btech, "frame", FALSE);
	BUNappend(bcount, &cnt, FALSE);
	input = cnt * ATOMsize(task->type);
	output = chunk_size(task,cnt);
	BUNappend(binput, &input, FALSE);
	BUNappend(boutput, &output, FALSE);
	BUNappend(bproperties, "", FALSE);
}

void
MOSskip_frame(Client cntxt, MOStask task)
{
	MOSadvance_frame(cntxt, task);
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

#define estimateFrame(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start, frame = *val, delta;\
	TPE *dict= (TPE*)hdr->frame;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	for(i =0; i<limit; i++, val++){\
		delta = *val - frame;\
		MOSfind(j,delta,0,hdr->framesize);\
		if( j == hdr->framesize || dict[j] != delta )\
			break;\
	}\
	if(i) factor = (flt) ((int)i * sizeof(TYPE)) / chunk_size(task,i);\
}

// store it in the compressed heap header directly
// filter out the most frequent ones
#define makeFrame(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start, frame = *val, delta;\
	TPE *dict = (TPE*)hdr->frame,v;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	for(i =0; i< limit; i++, val++){\
		delta = *val - frame;\
		for(j= 0; j< hdr->framesize; j++)\
			if( dict[j] == delta) break;\
		if ( j == hdr->framesize){\
			if ( hdr->framesize == 256){\
				int min = 0;\
				for(j=1;j<256;j++)\
					if( cnt[min] <cnt[j]) min = j;\
				j=min;\
				cnt[j]=0;\
				break;\
			}\
			dict[j] = delta;\
			cnt[j]++;\
			hdr->framesize++;\
		} else\
			cnt[j]++;\
	}\
	for(i=0; i< (BUN) hdr->framesize; i++)\
		for(j=i+1; j< hdr->framesize; j++)\
			if(dict[i] >dict[j]){\
				v= dict[i];\
				dict[i] = dict[j];\
				dict[j] = v;\
			}\
	hdr->framebits = 1;\
	hdr->mask =1;\
	for( i=2 ; i < (BUN) hdr->framesize; i *=2){\
		hdr->framebits++;\
		hdr->mask = (hdr->mask <<1) | 1;\
	}\
}


void
MOScreateframeDictionary(Client cntxt, MOStask task)
{	BUN i;
	int j;
	MosaicHdr hdr = task->hdr;
	lng cnt[256];

	(void) cntxt;
	for(j=0;j<256;j++)
		cnt[j]=0;
	hdr->framesize = 0;
	switch(ATOMstorage(task->type)){
	case TYPE_sht: makeFrame(sht); break;
	case TYPE_lng: makeFrame(lng); break;
	case TYPE_oid: makeFrame(oid); break;
	case TYPE_wrd: makeFrame(wrd); break;
	case TYPE_flt: makeFrame(flt); break;
	case TYPE_dbl: makeFrame(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: makeFrame(hge); break;
#endif
	case TYPE_int:
		{	int *val = ((int*)task->src) + task->start, frame = *val, delta;
			int *dict = (int*)hdr->frame,v;
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;

			for(i =0; i< limit; i++, val++){
				delta = *val - frame;
				for(j= 0; j< hdr->framesize; j++)
					if( dict[j] == delta) break;
				if ( j == hdr->framesize){
					if ( hdr->framesize == 256){
						int min = 0;
						// select low frequent candidate
						for(j=1;j<256;j++)
							if( cnt[min] <cnt[j]) min = j;
						j=min;
						cnt[j]=0;
						break;
					}
					dict[j] = delta;
					cnt[j]++;
					hdr->framesize++;
				} else
					cnt[j]++;
			}
			//assert(hdr->framesize);
			// sort it
			for(i=0; i< (BUN) hdr->framesize; i++)
				for(j=i+1; j< hdr->framesize; j++)
					if(dict[i] >dict[j]){
						v= dict[i];
						dict[i] = dict[j];
						dict[j] = v;
					}
			hdr->framebits = 1;
			hdr->mask =1;
			for( i=2 ; i < (BUN) hdr->framesize; i *=2){
				hdr->framebits++;
				hdr->mask = (hdr->mask <<1) | 1;
			}
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_frame(cntxt, task);
#endif
}

// calculate the expected reduction using dictionary in terms of elements compressed
flt
MOSestimate_frame(Client cntxt, MOStask task)
{	BUN i = -1;
	int j;
	flt factor= 1.0;
	MosaicHdr hdr = task->hdr;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: estimateFrame(sht); break;
	case TYPE_lng: estimateFrame(lng); break;
	case TYPE_oid: estimateFrame(oid); break;
	case TYPE_wrd: estimateFrame(wrd); break;
	case TYPE_flt: estimateFrame(flt); break;
	case TYPE_dbl: estimateFrame(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateFrame(hge); break;
#endif
	case TYPE_int:
		{	int *val = ((int*)task->src) + task->start, frame = *val, delta;
			int *dict = (int*)hdr->frame;
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
			for(i =0; i<limit; i++, val++){
				delta= *val - frame;
				MOSfind(j,delta,0,hdr->framesize);
				if( j == hdr->framesize || dict[j] != delta)
					break;
			}
			if ( i > MOSlimit() ) i = MOSlimit();
			//if(i) factor = (flt) ((int)i * sizeof(int)) / wordaligned( MosaicBlkSize + i,lng);
			if(i) factor = (flt) ((int)i * sizeof(int)) / chunk_size(task,i);\
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate dict "BUNFMT" elm %4.2f factor\n", i, factor);
#endif
	return factor; 
}

// insert a series of values into the compressor block using frame
#define framecompress(Vector,I,Bits,Value)\
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

#define FRAMEcompress(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start, frame = *val, delta;\
	TPE *dict = (TPE*)hdr->frame;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	task->dst = ((char*) task->blk)+ MosaicBlkSize;\
    *(TPE*) task->dst = frame;\
	base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);\
	base[0]=0;\
	for(i =0; i<limit; i++, val++){\
		hdr->checksum.sum##TPE += *val;\
		delta = *val - frame;\
		MOSfind(j,delta,0,hdr->framesize);\
		if(j == hdr->framesize || dict[j] != delta) \
			break;\
		else {\
			MOSincCnt(blk,1);\
			framecompress(base,i,hdr->framebits,j);\
		}\
	}\
	assert(i);\
}

void
MOScompress_frame(Client cntxt, MOStask task)
{
	BUN i;
	int j;
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;
	int cid, lshift, rshift;
	unsigned long *base;

	(void) cntxt;
	MOSsetTag(blk,MOSAIC_FRAME);
	MOSsetCnt(blk,0);

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: FRAMEcompress(sht); break;
	case TYPE_lng: FRAMEcompress(lng); break;
	case TYPE_oid: FRAMEcompress(oid); break;
	case TYPE_wrd: FRAMEcompress(wrd); break;
	case TYPE_flt: FRAMEcompress(flt); break;
	case TYPE_dbl: FRAMEcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: FRAMEcompress(hge); break;
#endif
	case TYPE_int:
		{	int *val = ((int*)task->src) + task->start, frame = *val, delta;
			int *dict = (int*)hdr->frame;
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;

			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			*(int*) task->dst = frame;
			task->dst += sizeof(unsigned long);
			base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);
			base[0]=0;
			for(i =0; i<limit; i++, val++){
				hdr->checksum.sumint += *val;
				delta = *val - frame;
				MOSfind(j,delta,0,hdr->framesize);
				//mnstr_printf(cntxt->fdout,"compress ["BUNFMT"] val %d index %d framebits %d\n",i, *val,j,hdr->framebits);
				if( j == hdr->framesize || dict[j] != delta )
					break;
				else {
					MOSincCnt(blk,1);
					cid = i * hdr->framebits/64;
					lshift= 63 -((i * hdr->framebits) % 64) ;
					if ( lshift >= hdr->framebits){
						base[cid]= base[cid] | (((unsigned long)j) << (lshift-hdr->framebits));
						//mnstr_printf(cntxt->fdout,"[%d] shift %d rbits %d \n",cid, lshift, hdr->framebits);
					}else{ 
						rshift= 63 -  ((i+1) * hdr->framebits) % 64;
						base[cid]= base[cid] | (((unsigned long)j) >> (hdr->framebits-lshift));
						base[cid+1]= 0 | (((unsigned long)j)  << rshift);
						//mnstr_printf(cntxt->fdout,"[%d] shift %d %d val %o %o\n", cid, lshift, rshift,
							//(j >> (hdr->framebits-lshift)),  (j <<rshift));
					}
				} 
			}
			assert(i);
		}
	}
}

// the inverse operator, extend the src
#define framedecompress(I)\
cid = (I * hdr->framebits)/64;\
lshift= 63 -((I * hdr->framebits) % 64) ;\
if ( lshift >= hdr->framebits){\
	j = (base[cid]>> (lshift-hdr->framebits)) & ((unsigned long)hdr->mask);\
  }else{ \
	rshift= 63 -  ((I+1) * hdr->framebits) % 64;\
	m1 = (base[cid] & ( ((unsigned long)hdr->mask) >> (hdr->framebits-lshift)));\
	m2 = base[cid+1] >>rshift;\
	j= ((m1 <<(hdr->framebits-lshift)) | m2) & 0377;\
  }

#define FRAMEdecompress(TPE)\
{	TPE *dict =(TPE*)((char*)hdr->frame), frame;\
	BUN lim = MOSgetCnt(blk);\
	frame = *(TPE*)(((char*)blk) +  MosaicBlkSize);\
	base = (unsigned long*) (((char*) blk) +  2 * MosaicBlkSize);\
	for(i = 0; i < lim; i++){\
		framedecompress(i);\
		((TPE*)task->src)[i] = frame + dict[j];\
		hdr->checksum2.sum##TPE += dict[j];\
	}\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_frame(Client cntxt, MOStask task)
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
	case TYPE_sht: FRAMEdecompress(sht); break;
	case TYPE_lng: FRAMEdecompress(lng); break;
	case TYPE_oid: FRAMEdecompress(oid); break;
	case TYPE_wrd: FRAMEdecompress(wrd); break;
	case TYPE_flt: FRAMEdecompress(flt); break;
	case TYPE_dbl: FRAMEdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: FRAMEdecompress(hge); break;
#endif
	case TYPE_int:
		{	int *dict =(int*)((char*)hdr->frame), frame;
			BUN lim = MOSgetCnt(blk);
			frame = *(int*)(((char*)blk) +  MosaicBlkSize);
			base = (unsigned long*) (((char*) blk) +  2 * MosaicBlkSize);

			for(i = 0; i < lim; i++){
				cid = (i * hdr->framebits)/64;
				lshift= 63 -((i * hdr->framebits) % 64) ;
				if ( lshift >= hdr->framebits){
					j = (base[cid]>> (lshift-hdr->framebits)) & ((unsigned long)hdr->mask);
					//mnstr_printf(cntxt->fdout,"[%d] lshift %d m %d\n", cid,  lshift,m1);
				  }else{ 
					rshift= 63 -  ((i+1) * hdr->framebits) % 64;
					m1 = (base[cid] & ( ((unsigned long)hdr->mask) >> (hdr->framebits-lshift)));
					m2 = (base[cid+1] >>rshift);
					j= ((m1 <<(hdr->framebits-lshift)) | m2) & 0377;\
					//mnstr_printf(cntxt->fdout,"[%d] shift %d %d cid %lo %lo val %o %o\n", cid, lshift, rshift,base[cid],base[cid+1], m1,  m2);
				  }
				hdr->checksum2.sumint += dict[j];
				((int*)task->src)[i] = frame + dict[j];
			}
			task->src += i * sizeof(int);
		}
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define subselect_frame(TPE) {\
 	TPE *dict= (TPE*) hdr->frame, frame;\
	frame = *(TPE*)(((char*)task->blk) +  MosaicBlkSize);\
	base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);\
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
				framedecompress(i); \
				cmp  =  ((*hi && frame + dict[j] <= * (TPE*)hgh ) || (!*hi && frame + dict[j] < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for(i=0; first < last; first++, i++){\
				MOSskipit();\
				framedecompress(i); \
				cmp  =  ((*li && frame + dict[j] >= * (TPE*)low ) || (!*li && frame + dict[j] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				framedecompress(i); \
				cmp  =  ((*hi && frame + dict[j] <= * (TPE*)hgh ) || (!*hi && frame + dict[j] < *(TPE*)hgh )) &&\
						((*li && frame + dict[j] >= * (TPE*)low ) || (!*li && frame + dict[j] > *(TPE*)low ));\
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
				framedecompress(i); \
				cmp  =  ((*hi && frame + dict[j] <= * (TPE*)hgh ) || (!*hi && frame + dict[j] < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				framedecompress(i); \
				cmp  =  ((*li && frame + dict[j] >= * (TPE*)low ) || (!*li && frame + dict[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				framedecompress(i); \
				cmp  =  ((*hi && frame + dict[j] <= * (TPE*)hgh ) || (!*hi && frame + dict[j] < *(TPE*)hgh )) &&\
						((*li && frame + dict[j] >= * (TPE*)low ) || (!*li && frame + dict[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

str
MOSsubselect_frame(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
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
		MOSskip_frame(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_sht: subselect_frame(sht); break;
	case TYPE_lng: subselect_frame(lng); break;
	case TYPE_oid: subselect_frame(oid); break;
	case TYPE_wrd: subselect_frame(wrd); break;
	case TYPE_flt: subselect_frame(flt); break;
	case TYPE_dbl: subselect_frame(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_frame(hge); break;
#endif
	case TYPE_int:
	// Expanded MOSselect_frame for debugging
 	{ int *dict= (int*) hdr->frame, frame;
		frame = *(int*)(((char*)task->blk) +  MosaicBlkSize);
		base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);

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
					framedecompress(i);
					cmp  =  ((*hi && frame + dict[j] <= * (int*)hgh ) || (!*hi && frame + dict[j] < *(int*)hgh ));
					if (cmp )
						*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				for(i=0 ; first < last; first++, i++){
					MOSskipit();
					framedecompress(i);
					cmp  =  ((*li && frame + dict[j] >= * (int*)low ) || (!*li && frame + dict[j] > *(int*)low ));
					if (cmp )
						*o++ = (oid) first;
				}
			} else{
				for(i=0 ; first < last; first++, i++){
					MOSskipit();
					framedecompress(i);
					cmp  =  ((*hi && frame + dict[j] <= * (int*)hgh ) || (!*hi && frame + dict[j] < *(int*)hgh )) &&
							((*li && frame + dict[j] >= * (int*)low ) || (!*li && frame + dict[j] > *(int*)low ));
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
					framedecompress(i);
					cmp  =  ((*hi && frame + dict[j] <= * (int*)hgh ) || (!*hi && frame + dict[j] < *(int*)hgh ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				for(i=0 ; first < last; first++, i++){
					MOSskipit();
					framedecompress(i);
					cmp  =  ((*li && frame + dict[j] >= * (int*)low ) || (!*li && frame + dict[j] > *(int*)low ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			} else{
				for( i=0 ; first < last; first++, i++){
					MOSskipit();
					framedecompress(i);
					cmp  =  ((*hi && frame + dict[j] <= * (int*)hgh ) || (!*hi && frame + dict[j] < *(int*)hgh )) &&
							((*li && frame + dict[j] >= * (int*)low ) || (!*li && frame + dict[j] > *(int*)low ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			}
		}
	}
	break;
	default:
		if( task->type == TYPE_date)
			subselect_frame(date);
		if( task->type == TYPE_daytime)
			subselect_frame(daytime);
		if( task->type == TYPE_timestamp)
		{ lng *dict= (lng*) hdr->frame, frame;
			int lownil = timestamp_isnil(*(timestamp*)low);
			int hghnil = timestamp_isnil(*(timestamp*)hgh);
			frame = *(lng*)(((char*)task->blk) +  MosaicBlkSize);
			base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);

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
						framedecompress(i);
						cmp  =  ((*hi && frame + dict[j] <= * (lng*)hgh ) || (!*hi && frame + dict[j] < *(lng*)hgh ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else
				if( hghnil){
					for(i=0 ; first < last; first++, i++){
						MOSskipit();
						framedecompress(i);
						cmp  =  ((*li && frame + dict[j] >= * (lng*)low ) || (!*li && frame + dict[j] > *(lng*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else{
					for(i=0 ; first < last; first++, i++){
						MOSskipit();
						framedecompress(i);
						cmp  =  ((*hi && frame + dict[j] <= * (lng*)hgh ) || (!*hi && frame + dict[j] < *(lng*)hgh )) &&
								((*li && frame + dict[j] >= * (lng*)low ) || (!*li && frame + dict[j] > *(lng*)low ));
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
						framedecompress(i);
						cmp  =  ((*hi && frame + dict[i] <= * (lng*)hgh ) || (!*hi && frame + dict[i] < *(lng*)hgh ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else
				if( hghnil){
					for(i=0 ; first < last; first++, i++){
						MOSskipit();
						framedecompress(i);
						cmp  =  ((*li && frame + dict[i] >= * (lng*)low ) || (!*li && frame + dict[i] > *(lng*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else{
					for( i=0 ; first < last; first++, i++){
						MOSskipit();
						framedecompress(i);
						cmp  =  ((*hi && frame + dict[j] <= * (lng*)hgh ) || (!*hi && frame + dict[j] < *(lng*)hgh )) &&
								((*li && frame + dict[j] >= * (lng*)low ) || (!*li && frame + dict[j] > *(lng*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				}
			}
		}
	}
	MOSskip_frame(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_frame(TPE)\
{ 	TPE low,hgh;\
 	TPE *dict= (TPE*) hdr->frame, frame;\
	frame = *(TPE*)(((char*)task->blk) +  MosaicBlkSize);\
	base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);\
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
		framedecompress(first); \
		if( (low == TPE##_nil || frame + dict[j] >= low) && (frame + dict[j] <= hgh || hgh == TPE##_nil) ){\
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
MOSthetasubselect_frame(Client cntxt,  MOStask task, void *val, str oper)
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
		MOSskip_frame(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_sht: thetasubselect_frame(sht); break;
	case TYPE_lng: thetasubselect_frame(lng); break;
	case TYPE_oid: thetasubselect_frame(oid); break;
	case TYPE_wrd: thetasubselect_frame(wrd); break;
	case TYPE_flt: thetasubselect_frame(flt); break;
	case TYPE_dbl: thetasubselect_frame(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_frame(hge); break;
#endif
	case TYPE_int:
		{ 	int low,hgh;
			int *dict= (int*) hdr->frame, frame;
			frame = *(int*)(((char*)task->blk) +  MosaicBlkSize);
			base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);
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
				framedecompress(i);
				if( (low == int_nil || frame + dict[j] >= low) && (frame + dict[j] <= hgh || hgh == int_nil) ){
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
		if( task->type == TYPE_timestamp)
		{ 	lng low,hgh;
			lng *dict= (lng*) hdr->frame, frame;
			frame = *(lng*)(((char*)task->blk) +  MosaicBlkSize);
			base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);

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
				framedecompress(i);
				if( (low == int_nil || frame + dict[j] >= low) && (frame + dict[j] <= hgh || hgh == int_nil) ){
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
	MOSskip_frame(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_frame(TPE)\
{	TPE *v;\
	TPE *dict= (TPE*) hdr->frame, frame;\
	frame = *(TPE*)(((char*)task->blk) +  MosaicBlkSize);\
	base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++){\
		MOSskipit();\
		framedecompress(i);\
		*v++ = frame + dict[j];\
		task->n--;\
	}\
	task->src = (char*) v;\
}

str
MOSleftfetchjoin_frame(Client cntxt,  MOStask task)
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
		case TYPE_sht: leftfetchjoin_frame(sht); break;
		case TYPE_lng: leftfetchjoin_frame(lng); break;
		case TYPE_oid: leftfetchjoin_frame(oid); break;
		case TYPE_wrd: leftfetchjoin_frame(wrd); break;
		case TYPE_flt: leftfetchjoin_frame(flt); break;
		case TYPE_dbl: leftfetchjoin_frame(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: leftfetchjoin_frame(hge); break;
#endif
		case TYPE_int:
		{	int *v;
			int *dict= (int*) hdr->frame, frame;
			frame = *(int*)(((char*)task->blk) +  MosaicBlkSize);
			base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);
			v= (int*) task->src;
			for(i=0 ; first < last; first++, i++){
				MOSskipit();
				framedecompress(i);
				*v++ = frame + dict[j];
				task->n--;
			}
			task->src = (char*) v;
		}
	}
	MOSskip_frame(cntxt,task);
	return MAL_SUCCEED;
}

#define join_frame(TPE)\
{	TPE *w;\
	TPE *dict= (TPE*) hdr->frame, frame;\
	frame = *(TPE*)(((char*)task->blk) +  MosaicBlkSize);\
	base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->elm; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			framedecompress(i);\
			if ( *w == frame + dict[j]){\
				BUNappend(task->lbat, &oo, FALSE);\
				BUNappend(task->rbat, &o, FALSE);\
			}\
		}\
	}\
}

str
MOSjoin_frame(Client cntxt,  MOStask task)
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
		case TYPE_sht: join_frame(sht); break;
		case TYPE_lng: join_frame(lng); break;
		case TYPE_oid: join_frame(oid); break;
		case TYPE_wrd: join_frame(wrd); break;
		case TYPE_flt: join_frame(flt); break;
		case TYPE_dbl: join_frame(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_frame(hge); break;
#endif
		case TYPE_int:
		{	int  *w;
			int *dict= (int*) hdr->frame, frame;
			frame = *(int*)(((char*)task->blk) +  MosaicBlkSize);
			base = (unsigned long*) (((char*) task->blk) +  2 * MosaicBlkSize);
			w = (int*) task->src;
			limit= MOSgetCnt(task->blk);
			for( o=0, n= task->elm; n-- > 0; o++,w++ ){
				for(oo = task->start,i=0; i < limit; i++,oo++){
					framedecompress(i);
					if ( *w == frame + dict[j]){
						BUNappend(task->lbat, &oo, FALSE);
						BUNappend(task->rbat, &o, FALSE);
					}
				}
			}

		}
	}
	MOSskip_frame(cntxt,task);
	return MAL_SUCCEED;
}
