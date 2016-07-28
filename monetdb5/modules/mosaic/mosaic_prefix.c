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
 * Bit_prefix compression
 * Factor out the leading bits from a series of values.
 * The prefix size is determined by the first two non-identical values.
 * Prefix compression does not require type knowledge
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_prefix.h"
#include "gdk_bitvector.h"

/* Beware, the dump routines use the compressed part of the task */
void
MOSdump_prefix(Client cntxt, MOStask task)
{
	MosaicBlk blk= task->blk;
	void *val = (void*)(((char*) blk) + MosaicBlkSize);

	mnstr_printf(cntxt->fdout,"#rle "BUNFMT" ", MOSgetCnt(blk));
	switch(task->type){
	case TYPE_bte:
		mnstr_printf(cntxt->fdout,"bte %hhd", *(bte*) val); break;
	case TYPE_sht:
		mnstr_printf(cntxt->fdout,"sht %hd", *(sht*) val); break;
	case TYPE_int:
		mnstr_printf(cntxt->fdout,"int %d", *(int*) val); break;
	case  TYPE_oid:
		mnstr_printf(cntxt->fdout,"oid "OIDFMT, *(oid*) val); break;
	case  TYPE_lng:
		mnstr_printf(cntxt->fdout,"lng "LLFMT, *(lng*) val); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		mnstr_printf(cntxt->fdout,"hge %.40g", (dbl) *(hge*) val); break;
#endif
	case TYPE_flt:
		mnstr_printf(cntxt->fdout,"flt  %f", *(flt*) val); break;
	case TYPE_dbl:
		mnstr_printf(cntxt->fdout,"flt  %f", *(dbl*) val); break;
	case TYPE_str:
		mnstr_printf(cntxt->fdout,"str TBD"); break;
	default:
		if( task->type == TYPE_date)
			mnstr_printf(cntxt->fdout,"date %d ", *(int*) val); 
		if( task->type == TYPE_daytime)
			mnstr_printf(cntxt->fdout,"daytime %d ", *(int*) val);
		if( task->type == TYPE_timestamp)
			mnstr_printf(cntxt->fdout,"int "LLFMT, *(lng*) val); 
	}
	mnstr_printf(cntxt->fdout,"\n");
}

void
MOSlayout_prefix(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;
	int bits, bytes;
	int size = ATOMsize(task->type);

	(void) cntxt;
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	BUNappend(btech, "prefix", FALSE);
	BUNappend(bcount, &cnt, FALSE);
	input = cnt * ATOMsize(task->type);
	switch(size){
	case 1:
		{	bte *dst = (bte*)  (((char*) task->blk) + MosaicBlkSize);
			bte mask = *dst++;
			bte val = *dst++;
			bits = val & (~mask);
			// be aware that we use longs as bit vectors
			bytes = sizeof(ulng) * ((MOSgetCnt(task->blk) * bits)/64 + (((MOSgetCnt(task->blk) * bits) %64) != 0));
			output =  wordaligned(bytes,bte); 
		}
		break;
	case 2:
		{	sht *dst = (sht*)  (((char*) task->blk) + MosaicBlkSize);
			sht mask = *dst++;
			sht val = *dst++;
			bits = val & (~mask);
			bytes = sizeof(ulng) * ((MOSgetCnt(task->blk) * bits)/64 + (((MOSgetCnt(task->blk) * bits) %64) != 0));
			output = wordaligned(bytes,sht); 
		}
		break;
	case 4:
		{	int *dst = (int*)  (((char*) task->blk) + MosaicBlkSize);
			int mask = *dst++;
			int val = *dst++;
			bits = val & (~mask);
			bytes = sizeof(ulng) * ((MOSgetCnt(task->blk) * bits)/64 + (((MOSgetCnt(task->blk) * bits) %64) != 0));
			output = wordaligned(bytes, int); 
		}
		break;
	case 8:
		{	lng *dst = (lng*)  (((char*) task->blk) + MosaicBlkSize);
			lng mask = *dst++;
			lng val = *dst++;
			bits = val & (~mask);
			bytes = sizeof(ulng) * ((MOSgetCnt(task->blk) * bits)/64 + (((MOSgetCnt(task->blk) * bits) %64) != 0));
			output = wordaligned(bytes, lng); 
		}
	}
	BUNappend(binput, &input, FALSE);
	BUNappend(boutput, &output, FALSE);
	BUNappend(bproperties, "", FALSE);
}

void
MOSadvance_prefix(Client cntxt, MOStask task)
{
	int bits, bytes;
	int size;
	(void) cntxt;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	task->start += MOSgetCnt(task->blk);
	task->stop = task->elm;
	switch(size){
	case 1:
		{	bte *dst = (bte*)  (((char*) task->blk) + MosaicBlkSize);
			bte mask = *dst++;
			bte val = *dst++;
			bits = val & (~mask);
			// be aware that we use longs as bit vectors
			bytes = sizeof(ulng) * ((MOSgetCnt(task->blk) * bits)/64 + (((MOSgetCnt(task->blk) * bits) %64) != 0));
			task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes,lng)); 
			//mnstr_printf(cntxt->fdout,"advance mask width %d bytes %d %d \n",bits,bytes,(int)wordaligned(bytes,int));
		}
		break;
	case 2:
		{	sht *dst = (sht*)  (((char*) task->blk) + MosaicBlkSize);
			sht mask = *dst++;
			sht val = *dst++;
			bits = val & (~mask);
			bytes = sizeof(ulng) * ((MOSgetCnt(task->blk) * bits)/64 + (((MOSgetCnt(task->blk) * bits) %64) != 0));
			task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes,lng)); 
			//mnstr_printf(cntxt->fdout,"advance mask width %d bytes %d %d \n",bits,bytes,(int)wordaligned(bytes,int));
		}
		break;
	case 4:
		{	int *dst = (int*)  (((char*) task->blk) + MosaicBlkSize);
			int mask = *dst++;
			int val = *dst++;
			bits = val & (~mask);
			bytes = sizeof(ulng) * ((MOSgetCnt(task->blk) * bits)/64 + (((MOSgetCnt(task->blk) * bits) %64) != 0));
			task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, lng)); 
			//mnstr_printf(cntxt->fdout,"advance mask width %d bytes %d %d \n",bits,bytes,(int)wordaligned(bytes,int));
		}
		break;
	case 8:
		{	lng *dst = (lng*)  (((char*) task->blk) + MosaicBlkSize);
			lng mask = *dst++;
			lng val = *dst++;
			bits = val & (~mask);
			bytes = sizeof(ulng) * ((MOSgetCnt(task->blk) * bits)/64 + (((MOSgetCnt(task->blk) * bits) %64) != 0));
			task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, lng)); 
			//mnstr_printf(cntxt->fdout,"advance mask width %d bytes %d %d \n",bits,bytes,(int)wordaligned(bytes,int));
		}
	}
}

void
MOSskip_prefix(Client cntxt, MOStask task)
{
	MOSadvance_prefix(cntxt, task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

// Find common prefix
#define Prefix(Prefix,Mask,X,Y,N) \
{ int k, m = 1; \
  for(k=0; k<N; k++, X>>=1, Y>>=1){\
	if( X == Y) break;\
	m= (m<<1)|1; \
  }\
  Prefix = N-k;\
  Mask = ~(m >>1);\
} 


// calculate the expected reduction 
flt
MOSestimate_prefix(Client cntxt, MOStask task)
{	BUN i = 0;
	flt factor = 0.0;
	int prefix = 0,bits, size;
	lng store;
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
	(void) cntxt;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	if( task->elm >= 2)
	switch(size){
	case 1:
		{	bte *v = ((bte*) task->src) + task->start, *w= v+1, val= *v,val2= *w, mask;
			// search first non-identical value
			for(i = 0;i < limit-1; i++, w++)
			if( *v != *w ){
				val2 = *w;
				break;
			}
			if ( i == limit -1)
				break;
			Prefix(bits, mask, val, val2, 8);
			if( prefix == 0)
				break;

			if( task->range[MOSAIC_PREFIX] > task->start +1 /* need at least two*/){
				bits = (task->range[MOSAIC_PREFIX] - task->start) * (8-prefix);
				store = bits/8 + ((bits % 8) >0);
				store = wordaligned( MosaicBlkSize + 2 * sizeof(bte) +  store,bte);
				if( store >= (flt)i * sizeof(bte))
					return 0.0;
				return task->factor[MOSAIC_PREFIX] = ( (flt)i * sizeof(bte))/ store;
			}
			
			val = *v & mask;
			for(w=v, i = 0; i < limit ; w++, i++){
				if ( val != (*w & mask) )
					break;
			}
			bits = i * (8-prefix);
			store = bits/8 + ((bits % 8) >0);
			store = wordaligned( MosaicBlkSize + 2 * sizeof(bte) +  store,bte);
			if( store >= (flt)i * sizeof(bte))
				return 0.0;
			if( task->dst +  store >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(bte))/ store;
		}
		break;
	case 2:
		{	sht *v = ((sht*) task->src) + task->start, *w= v+1, val= *v,val2= *w, mask;
			// search first non-identical value
			for(i = 0;i < limit-1;i++, w++)
			if( *v != *w ){
				val2 = *w;
				break;
			}
			if ( i == limit-1)
				break;
			Prefix(prefix, mask, val, val2, 16);
			if( prefix == 0)
				break;

			if( task->range[MOSAIC_PREFIX] > task->start + 1){
				bits = (task->range[MOSAIC_PREFIX] - task->start) * (16-prefix);
				store = bits/8 + ((bits % 8) >0);
				store = wordaligned( MosaicBlkSize + 2 * sizeof(sht) +  store,sht);
				if( store >= (flt)i * sizeof(sht))
					return 0.0;
				return task->factor[MOSAIC_PREFIX] = ( (flt)i * sizeof(sht))/ store;
			}
			
			val = *v & mask;
			for(w=v,i = 0; i < limit ; w++, i++){
				if ( val != (*w & mask) )
					break;
			}
			bits = i * (16-prefix);
			store = bits/8 + ((bits % 8) >0);
			store = wordaligned( MosaicBlkSize + 2 * sizeof(sht) +  store,lng);
			if( store >= (flt)i * sizeof(sht))
				return 0.0;
			if( task->dst +  store >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(sht))/ store;
		}
		break;
	case 4:
		{	int *v = ((int*) task->src) + task->start, *w= v+1, val= *v,val2= *w, mask;
			// search first non-identical value
			for(i = 0;i < limit-1 ;i++, w++)
			if( *v != *w ){
				val2 = *w;
				break;
			}
			if ( i == limit-1)
				break;
			Prefix(bits, mask, val, val2, 32);
			if( prefix == 0)
				break;

			if( task->range[MOSAIC_PREFIX] > task->start + 1){
				bits = (task->range[MOSAIC_PREFIX] - task->start) * (32-prefix);
				store = bits/8 + ((bits % 8) >0);
				store = wordaligned( MosaicBlkSize + 2 * sizeof(int) +  store,lng);
				if( store > (flt)i * sizeof(int))
					return 0.0;
				return task->factor[MOSAIC_PREFIX] = ( (flt)i * sizeof(int))/ store;
			}
			
			val = *v & mask;
			for(w=v,i = 0; i < limit; w++, i++){
				if ( val != (*w & mask) )
					break;
			}
			bits = i * (32-prefix);
			store = bits/8 + ((bits % 8) >0);
			store = wordaligned( MosaicBlkSize + 2 * sizeof(int) +  store,lng);
			if( store >= (flt)i * sizeof(int))
				return 0.0;
			if( task->dst +  store >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(int))/ store;
		}
		break;
	case 8:
		{	lng *v = ((lng*) task->src) + task->start, *w= v+1, val= *v,val2= *w, mask;
			// search first non-identical value
			for(i = 0;i < limit-1 ;i++, w++)
			if( *v != *w ){
				val2 = *w;
				break;
			}
			if ( i == limit-1 )
				break;
			Prefix(prefix, mask, val, val2, 32); // at most 32bits for bitvector
			if( prefix == 0)
				break;

			if( task->range[MOSAIC_PREFIX] > task->start + 1){
				bits = (task->range[MOSAIC_PREFIX] - task->start) * (32-prefix);
				store = bits/8 + ((bits % 8) >0);
				store = wordaligned( MosaicBlkSize + 2 * sizeof(lng) +  store,lng);
				if( store >= (flt)i * sizeof(lng))
					return 0.0;
				return task->factor[MOSAIC_PREFIX] = ( (flt)i * sizeof(lng))/ store;
			}
			
			val = *v & mask;
			for(w=v, i = 0; i < limit ; w++, i++){
				if ( val != (*w & mask) )
					break;
			}
			bits = i * (32-prefix);
			store = bits/8 + ((bits % 8) >0);
			store = wordaligned(MosaicBlkSize + 2 * sizeof(lng) + store,lng);
			if( store >= (flt)i * sizeof(lng))
				return 0.0;
			if( task->dst +  store >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(lng))/ store;
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate prefix %d "BUNFMT" elm %4.3f factor\n",prefix,i,factor);
#endif
	task->factor[MOSAIC_PREFIX] = factor;
	task->range[MOSAIC_PREFIX] = task->start + i;
	return factor;
}

#define compress(Vector,I, Bits, Value) setBitVector(Vector,I,Bits,Value);

void
MOScompress_prefix(Client cntxt, MOStask task)
{
	BUN i, j =0 ;
	int size;
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = task->blk;

	(void) cntxt;
	MOSsetTag(blk, MOSAIC_PREFIX);

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	if( task->elm >=2 )
	switch(size){
	case 1:
		{	bte *v = ((bte*) task->src) + task->start, *w= v+1, val = *v, val2 = *w, mask;
			bte *dst = (bte*)  (((char*) blk) + MosaicBlkSize);
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
			BitVector base;
			int prefix,residu; 
			// search first non-identical value
			for(i = 0;i < limit;i++, w++)
			if( *v != *w ){
				val2 = *w;
				break;
			}
			w = v+1;
			Prefix(prefix, mask, val, val2, 8);
			residu = 8-prefix;
			*dst++ = mask;
			val = *v & mask;	//reference value
			*dst = val;
			*dst = *dst | residu; // bits outside mask
			dst++;
			base  = (BitVector) dst; // start of bit vector
			
			if( i < limit)
			for(j=0, w = v, i = 0; i < limit; w++, i++, j++){
				if ( val  != (*w & mask) )
					break;
				compress(base, j, residu, (int)( *w & (~mask))); // residu
				hdr->checksum.sumbte += val;
			}
			MOSsetCnt(blk,j);
		}
		break;
	case 2:
		{	sht *v = ((sht*) task->src) + task->start, *w= v+1, val = *v, val2 = *w, mask;
			sht *dst = (sht*)  (((char*) blk) + MosaicBlkSize);
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
			BitVector base;
			int prefix, residu; 

			// search first non-identical value
			for(i = 0;i < limit;i++, w++)
			if( *v != *w ){
				val2 = *w;
				break;
			}
			w = v+1;
			Prefix(prefix, mask, val, val2, 16);
			residu = 16-prefix;
			*dst++ = mask;
			val = *v & mask;	//reference value
			*dst = val;
			*dst = *dst | residu; // bits outside mask
			dst++;
			base  = (BitVector) dst; // start of bit vector
			
			if( i < limit)
			for(j=0, w = v, i = 0; i < limit; w++, i++, j++){
				if ( val  != (*w & mask) )
					break;
				compress(base,j,residu, (int)( *w & (~mask))); // residu
				hdr->checksum.sumsht += val;
			}
			MOSsetCnt(blk,j);
		}
		break;
	case 4:
		{	int *v = ((int*) task->src) + task->start, *w= v+1, val = *v, val2 = *w, mask;
			int *dst = (int*)  (((char*) blk) + MosaicBlkSize);
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
			BitVector base;
			int prefix, residu; 

			// search first non-identical value
			for(i = 0;i < limit;i++, w++)
			if( *v != *w ){
				val2 = *w;
				break;
			}
			w = v+1;
			Prefix(prefix, mask, val, val2, 32);
			residu = 32-prefix;
			*dst++ = mask;
			val = *v & mask;	//reference value
			*dst = val;
			*dst = *dst | residu; // bits outside mask
			dst++;
			base  = (BitVector) dst; // start of bit vector
			
			//mnstr_printf(cntxt->fdout,"compress %o %o val %d bits %d, %d mask %o\n",*v,*w,val,bits, residu,mask);
			if( i < limit)
			for(j=0, w = v, i = 0; i < limit; w++, i++, j++){
				if ( val  != (*w & mask) )
					break;
				compress(base,j,residu, (int) (*w & (~mask))); // residu
				hdr->checksum.sumint += val;
			}
			MOSsetCnt(blk,j);
		}
		break;
	case 8:
		{	lng *v = ((lng*) task->src) + task->start, *w= v+1, val = *v, val2 = *w, mask;
			lng *dst = (lng*)  (((char*) blk) + MosaicBlkSize);
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
			BitVector base;
			int prefix, residu; 

			// search first non-identical value
			for(i = 0;i < limit;i++, w++)
			if( *v != *w ){
				val2 = *w;
				break;
			}
			w = v+1;
			Prefix(prefix, mask, val, val2, 32);
			residu = 64-prefix;
			*dst++ = mask;
			val = *v & mask;	//reference value
			*dst = val;
			*dst = *dst | residu; // bits outside mask
			dst++;
			base  = (BitVector) dst; // start of bit vector
			
			//mnstr_printf(cntxt->fdout,"compress %o %o val %d bits %d, %d mask %o\n",*v,*w,val,bits, residu,mask);
			if( i < limit)
			for(j=0, w = v, i = 0; i < limit; w++, i++,j++){
				if ( val  != (*w & mask) )
					break;
				compress(base,j,residu, (int)(*w & (~mask))); // residu
				hdr->checksum.sumlng += val;
			}
			MOSsetCnt(blk,j);
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_prefix(cntxt, task);
#endif
}

#define decompress(Vector,I,Bits) getBitVector(Vector,I,Bits);

void
MOSdecompress_prefix(Client cntxt, MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	int size;
	(void) cntxt;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	switch(size){
	case 1:
		{	bte *dst =  (bte*)  (((char*) blk) + MosaicBlkSize);
			bte mask = *dst++, val  =  *dst++, v;
			bte *w = ((bte*) task->src) + task->start;
			bte m;
			BUN lim= MOSgetCnt(blk);
			BitVector base;
			int residu;

			m = ~mask;
			residu = val & m;
			val = val & mask;
			base = (BitVector) dst;
			//mnstr_printf(cntxt->fdout,"decompress residu %d mask %o val %d\n",residu,m,val);
			for(i = 0; i < lim; i++){
				v = decompress(base,i,residu);
				hdr->checksum.sumsht += v;
				*w++ = v;
			}
		}
		break;
	case 2:
		{	sht *dst =  (sht*)  (((char*) blk) + MosaicBlkSize);
			sht mask = *dst++, val  =  *dst++, v;
			sht *w = ((sht*) task->src) + task->start;
			sht m;
			BUN lim= MOSgetCnt(blk);
			BitVector base;
			int residu;

			m = ~mask;
			residu = val & m;
			val = val & mask;
			base = (BitVector) dst;
			//mnstr_printf(cntxt->fdout,"decompress residu %d mask %o val %d\n",residu,m,val);
			for(i = 0; i < lim; i++){
				v =decompress(base,i,residu);
				hdr->checksum.sumsht += v;
				*w++ = v;
			}
		}
		break;
	case 4:
		{	unsigned int *dst =  (unsigned int*)  (((char*) blk) + MosaicBlkSize);
			unsigned int mask = *dst++, val  =  *dst++, v;
			unsigned int *w = ((unsigned int*) task->src) + task->start;
			BUN lim= MOSgetCnt(blk);
			BitVector base;
			int m;
			int residu;

			m = ~mask;
			residu = val & m;
			val = val & mask;
			base = (BitVector) dst;
			//mnstr_printf(cntxt->fdout,"decompress residu %d mask %o val %d\n",residu,m,val);
			for(i = 0; i < lim; i++){
				v = decompress(base,i,residu);
				hdr->checksum.sumint += v;
				*w++ = v;
			}
		}
		break;
	case 8:
		{	lng *dst =  (lng*)  (((char*) blk) + MosaicBlkSize);
			lng mask = *dst++, val  =  *dst++, v;
			lng *w = ((lng*) task->src) + task->start;
			lng m;
			BUN lim= MOSgetCnt(blk);
			BitVector base;
			int residu;

			m = ~mask;
			residu = (int) val & m;
			val = val & mask;
			base = (BitVector) dst;
			//mnstr_printf(cntxt->fdout,"decompress residu %d mask %o val %d\n",residu,m,val);
			for(i = 0; i < lim; i++){
				v= decompress(base,i,residu);
				hdr->checksum.sumlng += v;
				*w++ = v;
			}
		}
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define  subselect_prefix(TPE, TPE2) \
{	TPE2 *dst =  (TPE2*)  (((char*) blk) + MosaicBlkSize);\
	TPE2 mask = *dst++;\
	TPE2  val = *dst++,v;\
	TPE2 m;\
	BitVector base;\
	int residu;\
	TPE value;\
	m = ~mask;\
	residu = (int)(val & m);\
	val = val & mask;\
	base = (BitVector) dst;\
	if( !*anti){\
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v = decompress(base,i,residu);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*hi && value <= * (TPE*)hgh ) || (!*hi && value < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v = decompress(base,i,residu);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*li && value >= * (TPE*)low ) || (!*li && value > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v = decompress(base,i,residu);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*hi && value <= * (TPE*)hgh ) || (!*hi && value < *(TPE*)hgh )) &&\
						((*li && value >= * (TPE*)low ) || (!*li && value > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		}\
	} else {\
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			/* nothing is matching */\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v =decompress(base,i,residu);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*hi && value <= * (TPE*)hgh ) || (!*hi && value < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for( ; first < last; first++, val++,i++){\
				MOSskipit();\
				v = decompress(base,i,residu);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*li && value >= * (TPE*)low ) || (!*li && value > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, val++,i++){\
				MOSskipit();\
				v = decompress(base,i,residu);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*hi && value <= * (TPE*)hgh ) || (!*hi && value < *(TPE*)hgh )) &&\
						((*li && value >= * (TPE*)low ) || (!*li && value > *(TPE*)low ));\
				if (!cmp)\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

str
MOSsubselect_prefix(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	int cmp;
	BUN i = 0,first,last;
	MosaicBlk blk= task->blk;

	// set the oid range covered
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	(void) cntxt;
	if (task->cl && *task->cl > last){
		MOSadvance_prefix(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bit: subselect_prefix(bit,unsigned int); break;
	case TYPE_bte: subselect_prefix(bte,unsigned int); break;
	case TYPE_sht: subselect_prefix(sht,unsigned int); break;
	case TYPE_int: subselect_prefix(int,unsigned int); break;
	case TYPE_lng: subselect_prefix(lng,ulng); break;
	case TYPE_oid: subselect_prefix(oid,ulng); break;
	case TYPE_flt: subselect_prefix(flt,unsigned int); break;
	case TYPE_dbl: subselect_prefix(dbl,ulng); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_prefix(hge,unsigned long long); break;
#endif
	default:
		if( task->type == TYPE_date)
			subselect_prefix(date,int); 
		if( task->type == TYPE_daytime)
			subselect_prefix(daytime,int); 
		if( task->type == TYPE_timestamp)
			subselect_prefix(lng,long long); 
	}
	MOSadvance_prefix(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_prefix(TPE, TPE2, BITS)\
{ 	TPE low,hgh;\
    TPE2 *dst =  (TPE2*)  (((char*) blk) + MosaicBlkSize);\
    TPE2 mask = *dst++;\
    TPE2  val = *dst++,v;\
    TPE2 m;\
    BitVector base;\
    int residu;\
    TPE value;\
	m = ~mask;\
	residu = (int)( val & m);\
	val = val & mask;\
	base = (BitVector) dst;\
	low= hgh = TPE##_nil;\
	if ( strcmp(oper,"<") == 0){\
		hgh= *(TPE*) input;\
		hgh = PREVVALUE##TPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *(TPE*) input;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low = *(TPE*) input;\
		low = NEXTVALUE##TPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low = *(TPE*) input;\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		low = hgh = *(TPE*) input;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) input;\
	} \
	if ( !anti)\
		for( ; first < last; first++,i++){\
			MOSskipit();\
			v =decompress(base,i,residu);\
			value =  (TPE) ((TPE2)val |(TPE2) v);\
			if( (low == TPE##_nil || value >= low) && (value <= hgh || hgh == TPE##_nil) )\
			*o++ = (oid) first;\
		}\
	else\
		for( ; first < last; first++,i++){\
			MOSskipit();\
			v = decompress(base,i,residu);\
			value =  (TPE) ((TPE2)val |(TPE2) v);\
			if( !( (low == TPE##_nil || value >= low) && (value <= hgh || hgh == TPE##_nil) ))\
				*o++ = (oid) first;\
		}\
}

str
MOSthetasubselect_prefix(Client cntxt,  MOStask task, void *input, str oper)
{
	oid *o;
	int anti=0;
	BUN i=0,first,last;
	MosaicBlk blk = task->blk;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_prefix(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bit: thetasubselect_prefix(bit, unsigned int, 8); break;
	case TYPE_bte: thetasubselect_prefix(bte, unsigned int, 8); break;
	case TYPE_sht: thetasubselect_prefix(sht, unsigned int, 16); break;
	case TYPE_int: thetasubselect_prefix(int, unsigned int, 16); break;
	case TYPE_lng: thetasubselect_prefix(lng, ulng, 16); break;
	case TYPE_oid: thetasubselect_prefix(oid, ulng, 64); break;
	case TYPE_flt: thetasubselect_prefix(flt, unsigned int, 32); break;
	case TYPE_dbl: thetasubselect_prefix(dbl, ulng, 64); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_prefix(hge, unsigned long long,128); break;
#endif
	}
	MOSskip_prefix(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_prefix(TPE, TPE2)\
{	TPE *r;\
    TPE2 *dst =  (TPE2*)  (((char*) blk) + MosaicBlkSize);\
    TPE2 mask = *dst++;\
    TPE2  val = *dst++,v;\
    TPE2 m;\
    BitVector base;\
    int residu;\
    TPE value;\
	m = ~mask;\
	residu = (int) val & m;\
	val = val & mask;\
	base = (BitVector) dst;\
	r= (TPE*) task->src;\
	for(; first < last; first++,i++){\
		MOSskipit();\
		v = decompress(base,i,residu);\
		value =  (TPE) ((TPE2)val |(TPE2) v);\
		*r++ = value;\
		task->n--;\
		task->cnt++;\
	}\
	task->src = (char*) r;\
}

str
MOSprojection_prefix(Client cntxt,  MOStask task)
{
	BUN i=0, first,last;
	MosaicBlk blk= task->blk;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMstorage(task->type)){
		case TYPE_bit: projection_prefix(bit, unsigned char); break;
		case TYPE_bte: projection_prefix(bte, unsigned char); break;
		case TYPE_sht: projection_prefix(sht, unsigned short); break;
		case TYPE_int: projection_prefix(int, unsigned short); break;
		case TYPE_lng: projection_prefix(lng, ulng); break;
		case TYPE_oid: projection_prefix(oid, ulng); break;
		case TYPE_flt: //projection_prefix(flt, unsigned int); break;
{	flt *r;
    unsigned int *dst =  (unsigned int*)  (((char*) blk) + MosaicBlkSize);
    unsigned int mask = *dst++;
    unsigned int val  =  *dst++,v;
    unsigned int m;
    BitVector base;
    int residu;
    flt value;
	m = ~mask;
	residu = (int) val & m;
	val = val & mask;
	base = (BitVector) dst;
	r= (flt*) task->src;
	for(; first < last; first++,i++){
		MOSskipit();
		v = decompress(base,i,residu);
		value =  (flt) ((unsigned int)val |(unsigned int) v);
		*r++ = value;
		task->n--;
		task->cnt++;
	}
	task->src = (char*) r;
}
break;
		case TYPE_dbl: //projection_prefix(dbl, ulng); break;
/*
{	dbl *r;
    ulng *dst =  (ulng*)  (((char*) blk) + MosaicBlkSize);
    ulng mask = *dst++;
    ulng  val  =  *dst++,v;
    ulng m;
    BitVector base;
    int residu;
    dbl value;
	m = ~mask;
	residu = (int) val & m;
	val = val & mask;
	base = (BitVector) dst;
	r= (dbl*) task->src;
	for(; first < last; first++,i++){
		MOSskipit();
		v = decompress(base,i,residu);
		value =  (dbl) ((ulng)val |(ulng) v);
		*r++ = value;
		task->n--;
		task->cnt++;
	}
	task->src = (char*) r;
}
*/
#ifdef HAVE_HGE
		case TYPE_hge: projection_prefix(hge, unsigned long long); break;
#endif
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: projection_prefix(bte, unsigned char); break;
		case 2: projection_prefix(sht, unsigned short); break;
		case 4: projection_prefix(int, unsigned int); break;
		case 8: projection_prefix(lng, ulng); break;
		}
		break;
	}
	MOSskip_prefix(cntxt,task);
	return MAL_SUCCEED;
}

#define join_prefix(TPE,TPE2)\
{   TPE *w;\
	TPE2 *dst =  (TPE2*)  (((char*) blk) + MosaicBlkSize);\
	TPE2 mask = *dst++;\
	TPE2  val = *dst++,v;\
	TPE2 m;\
	BitVector base;\
	int residu;\
	TPE value;\
	m = ~mask;\
	residu = (int)(val & m);\
	val = val & mask;\
	base = (BitVector) dst;\
	w = (TPE*) task->src;\
	for(n = task->elm, o = 0; n -- > 0; w++,o++){\
		for(i=0, oo= (oid) first; oo < (oid) last; v++, oo++,i++){\
			v = decompress(base,i,residu);\
			value =  (TPE) ((TPE2)val |(TPE2) v);\
			if ( *w == value){\
				BUNappend(task->lbat, &oo, FALSE);\
				BUNappend(task->rbat, &o, FALSE);\
			}\
		}\
	}\
}

str
MOSsubjoin_prefix(Client cntxt,  MOStask task)
{
	BUN i= 0,n,first,last;
	MosaicBlk blk= task->blk;
	oid o, oo;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMstorage(task->type)){
		case TYPE_bit: join_prefix(bit,unsigned char); break;
		case TYPE_bte: join_prefix(bte,unsigned char); break;
		case TYPE_sht: join_prefix(sht,unsigned short); break;
		case TYPE_int: join_prefix(int,unsigned short); break;
		case TYPE_lng: //join_prefix(lng,ulng); break;
{   lng *w;
	ulng *dst =  (ulng*)  (((char*) blk) + MosaicBlkSize);
	ulng mask = *dst++;
	ulng  val = *dst++,v;
	ulng m;
	BitVector base;
	int residu;
	lng value;
	m = ~mask;
	residu = (int)(val & m);
	val = val & mask;
	base = (BitVector) dst;
	w = (lng*) task->src;
	for(n = task->elm, o = 0; n -- > 0; w++,o++){
		for(i=0, oo= (oid) first; oo < (oid) last; v++, oo++,i++){
			v = decompress(base,i,residu);
			value =  (lng) ((ulng)val |(ulng) v);
			if ( *w == value){
				BUNappend(task->lbat, &oo, FALSE);
				BUNappend(task->rbat, &o, FALSE);
			}
		}
	}
}
	break;
		case TYPE_oid: join_prefix(oid,BUN); break;
		case TYPE_flt: join_prefix(flt,unsigned int); break;
		case TYPE_dbl: join_prefix(dbl,unsigned int); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_prefix(hge,unsigned long long); break;
#endif
		case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: join_prefix(bte, unsigned char); break;
		case 2: join_prefix(sht, unsigned short); break;
		case 4: join_prefix(int, unsigned int); break;
		case 8: join_prefix(lng, unsigned int); break;
		}
	}
	MOSskip_prefix(cntxt,task);
	return MAL_SUCCEED;
}
