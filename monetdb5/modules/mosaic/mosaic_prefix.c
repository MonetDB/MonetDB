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
 * Factor out leading bits from a series of values.
 * The prefix size is determined by looking ahead in a small block.
 * To use the bitvector, we limit the extracted tail to at most 32bits
 * The administration are 2 TPE values (mask,reference value)
 * The size of the residu is stored in the reference value lower bits
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

	mnstr_printf(cntxt->fdout,"#prefix "BUNFMT" ", MOSgetCnt(blk));

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
	int bits =0, bytes=0;
	int size = ATOMsize(task->type);
	char buf[32];

	(void) cntxt;
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	BUNappend(btech, "prefix blk", FALSE);
	BUNappend(bcount, &cnt, FALSE);
	input = cnt * ATOMsize(task->type);
	switch(size){
	case 1:
		{	unsigned char *dst = (unsigned char*)  MOScodevector(task);
			unsigned char mask = *dst++;
			unsigned char val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned char),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) * sizeof(int), int);
		}
		break;
	case 2:
		{	unsigned short *dst = (unsigned short*)  MOScodevector(task);
			unsigned short mask = *dst++;
			unsigned short val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned short),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) * sizeof(int), int);
		}
		break;
	case 4:
		{	unsigned int *dst = (unsigned int*)  MOScodevector(task);
			unsigned int mask = *dst++;
			unsigned int val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned int),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) * sizeof(int), int);
		}
		break;
	case 8:
		{	ulng *dst = (ulng*)  MOScodevector(task);
			ulng mask = *dst++;
			ulng val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(ulng),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) * sizeof(int), int);
		}
	}
	output = wordaligned(bytes, int); 
	BUNappend(binput, &input, FALSE);
	BUNappend(boutput, &output, FALSE);
	snprintf(buf,32,"%d bits",bits);
	BUNappend(bproperties, buf, FALSE);
}

void
MOSadvance_prefix(Client cntxt, MOStask task)
{
	int bits = 0;
	size_t bytes= 0;
	int size;
	(void) cntxt;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	task->start += MOSgetCnt(task->blk);
	task->stop = task->elm;
	switch(size){
	case 1:
		{	unsigned char *dst = (unsigned char*)  MOScodevector(task);
			unsigned char mask = *dst++;
			unsigned char val = *dst++;
			bits = (int)(val & (~mask));
			bytes = wordaligned(2 * sizeof(unsigned char),int);
			bytes += wordaligned(getBitVectorSize(MOSgetCnt(task->blk),bits) * sizeof(int), int);
			task->blk = (MosaicBlk) (((char*) dst)  + bytes); 
		}
		break;
	case 2:
		{	unsigned short *dst = (unsigned short*)  MOScodevector(task);
			unsigned short mask = *dst++;
			unsigned short val = *dst++;
			bits = (int)(val & (~mask));
			bytes = wordaligned(2 * sizeof(unsigned short),int);
			bytes += wordaligned(getBitVectorSize(MOSgetCnt(task->blk),bits) * sizeof(int), int);
			task->blk = (MosaicBlk) (((char*) dst)  + bytes); 
		}
		break;
	case 4:
		{	unsigned int *dst = (unsigned int*)  MOScodevector(task);
			unsigned int mask = *dst++;
			unsigned int val = *dst++;
			bits = (int)(val & (~mask));
			bytes = wordaligned(2 * sizeof(unsigned int),int);
			bytes += wordaligned(getBitVectorSize(MOSgetCnt(task->blk),bits) * sizeof(int), int);
			task->blk = (MosaicBlk) (((char*) dst)  + bytes); 
		}
		break;
	case 8:
		{	ulng *dst = (ulng*)  MOScodevector(task);
			ulng mask = *dst++;
			ulng val = *dst++;
			bits = (int)(val & (~mask));
			bytes = wordaligned(2 * sizeof(ulng),int);
			bytes += wordaligned(getBitVectorSize(MOSgetCnt(task->blk),bits) * sizeof(int), int);
			task->blk = (MosaicBlk) (((char*) dst)  + bytes); 
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#advance mask width %d bytes %d %d \n",bits,(int)bytes,(int)wordaligned(bytes,int));
#endif
}

void
MOSskip_prefix(Client cntxt, MOStask task)
{
	MOSadvance_prefix(cntxt, task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

// logarithmic search for common prefix in a given block
// use static prefix mask attempts

static void
findPrefixBit(Client cntxt, unsigned char *v, int limit, int *bits, unsigned char *prefixmask)
{
	int i, step = 8, width = 0;
	unsigned char prefix, mask;
	*bits = 0;
	*prefixmask = 0;
	do{
		step /=2;
		mask = 1 ;
		for( i=0; i < 8 - 1 - (width +step); i++)
			mask = (mask <<1) | 1;
		mask = ~mask;
		prefix = v[0] & mask;
		for(i=0; i< limit; i++)
			if( (v[i] & mask) != prefix)
				break;
#ifdef _DEBUG_PREFIX_
		mnstr_printf(cntxt->fdout,"#findprefix width %d step %d mask %o  %d\n", width, step, mask, limit-i);
#endif
		if( i ==  limit){
			width += step;
			*bits = width;
			*prefixmask = mask;
		}
	} while (step > 1);
#ifdef _DEBUG_PREFIX_
	mnstr_printf(cntxt->fdout,"#findprefix final %d \n", *bits);
#else
	(void) cntxt;
#endif
}

static void
findPrefixSht(Client cntxt, unsigned short *v, int limit, int *bits, unsigned short *prefixmask)
{
	int i, step = 16, width = 0;
	unsigned short prefix, mask;
	*bits = 0;
	*prefixmask = 0;
#ifdef _DEBUG_PREFIX_
	mnstr_printf(cntxt->fdout,"#findprefix start %u %d \n", *v, *bits);
#endif
	do{
		step /=2;
		mask = 1 ;
		for( i=0; i < 16-1 - (width +step); i++)
			mask = (mask <<1) | 1;
		mask = ~mask;
		prefix = v[0] & mask;
		for(i=0; i< limit; i++)
			if( (v[i] & mask) != prefix)
				break;
#ifdef _DEBUG_PREFIX_
		mnstr_printf(cntxt->fdout,"#findprefix width %d step %d mask %o  %d\n", width, step, mask, limit-i);
#endif
		if( i ==  limit){
			width += step;
			*bits = width;
			*prefixmask = mask;
		}
	} while (step > 1);
#ifdef _DEBUG_PREFIX_
	mnstr_printf(cntxt->fdout,"#findprefix final %d \n", *bits);
#else
	(void) cntxt;
#endif
}

static void
findPrefixInt(Client cntxt, unsigned int *v, int limit, int *bits, unsigned int *prefixmask)
{
	int i, step = 32, width = 0;
	unsigned int prefix, mask;
	*bits = 0;
	*prefixmask = 0;
	do{
		step /=2;
		mask = 1 ;
		for( i=0; i < 32-1 - (width +step); i++)
			mask = (mask <<1) | 1;
		mask = ~mask;
		prefix = v[0] & mask;
		for(i=0; i< limit; i++)
			if( (v[i] & mask) != prefix)
				break;
#ifdef _DEBUG_PREFIX_
		mnstr_printf(cntxt->fdout,"#findprefix width %d step %d mask %o  %d\n", width, step, mask, limit-i);
#endif
		if( i ==  limit){
			width += step;
			*bits = width;
			*prefixmask = mask;
		}
	} while (step > 1);
#ifdef _DEBUG_PREFIX_
	mnstr_printf(cntxt->fdout,"#findprefix final %d \n", *bits);
#else
	(void) cntxt;
#endif
}

static void
findPrefixLng(Client cntxt, ulng *v, int limit, int *bits, ulng *prefixmask)
{
	int i, step = 64, width = 0;
	ulng prefix, mask;
	*bits = 0;
	*prefixmask = 0;
	do{
		step /=2;
		mask = 1 ;
		for( i=0; i < 64 - 1 - (width +step); i++)
			mask = (mask <<1) | 1;
		mask = ~mask;
		prefix = v[0] & mask;
		for(i=0; i< limit; i++)
			if( (v[i] & mask) != prefix)
				break;
#ifdef _DEBUG_PREFIX_
		mnstr_printf(cntxt->fdout,"#findprefix width %d step %d mask "LLFMT"  %d\n", width, step, mask, limit-i);
#endif
		if( i ==  limit){
			width += step;
			*bits = width;
			*prefixmask = mask;
		}
	} while (step > 1 && *bits < 32);
	// we only use at most 32 bits as prefix due to bitvector implementation
#ifdef _DEBUG_PREFIX_
	mnstr_printf(cntxt->fdout,"#findprefix final %d \n", *bits);
#else
	(void) cntxt;
#endif
}

#define Prefix(Prefix,Mask,X,Y,N) \
{ int k, m = 1; \
  for(k=0; k<N; k+=1, X>>=1, Y>>=1){\
	if( X == Y) break;\
	m= (m<<1)|1; \
  }\
  Prefix = N-k;\
  Mask = ~(m >> 1);\
} 


#define LOOKAHEAD  (limit <10? limit:10)
// calculate the expected reduction 
flt
MOSestimate_prefix(Client cntxt, MOStask task)
{	BUN i = 0;
	flt factor = 0.0;
	int prefixbits = 0,size;
	lng bits,store;
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
	(void) cntxt;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	if( task->elm >= 2)
	switch(size){
	case 1:
		{	unsigned char *v = ((unsigned char*) task->src) + task->start, *w= v+1, val= *v, mask;
			findPrefixBit(cntxt, v, LOOKAHEAD, &prefixbits, &mask);
			if( prefixbits == 0)
				break;

#ifdef _DEBUG_PREFIX_
            mnstr_printf(cntxt->fdout,"#prefix  estimate size 1 %o %o val %d bits %d mask %o\n",
                *v,*w, val, prefixbits, mask);
#endif
			if( task->range[MOSAIC_PREFIX] > task->start +1 /* need at least two*/){
				bits = (task->range[MOSAIC_PREFIX] - task->start) * (8-prefixbits);
				store = wordaligned(2 * sizeof(unsigned char),int);
				store += wordaligned(bits/8 + ((bits % 8) >0),int);
				store = wordaligned( MosaicBlkSize + store,int);

				if( store >= (flt)i * sizeof(bte))
					return 0.0;
				return task->factor[MOSAIC_PREFIX] = ( (flt)i * sizeof(bte))/ store;
			}
			
			// calculate the number of values covered by this prefix
			val = *v & mask;
			for(w=v, i = 0; i < limit ; w++, i++){
				if ( val != (*w & mask) )
					break;
			}
			bits = i * (8-prefixbits);
			store = wordaligned(2 * sizeof(unsigned char),int);
			store += wordaligned(bits/8 + ((bits % 8) >0),int);
			store = wordaligned( MosaicBlkSize + store,int);
			if( store >= (flt)i * sizeof(bte))
				return 0.0;
			if( task->dst +  store >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(bte))/ store;
		}
		break;
	case 2:
		{	unsigned short *v = ((unsigned short*) task->src) + task->start, *w= v+1, val= *v, mask;
			findPrefixSht(cntxt, v, LOOKAHEAD, &prefixbits, &mask);
			if( prefixbits == 0)
				break;
#ifdef _DEBUG_PREFIX_
            mnstr_printf(cntxt->fdout,"#prefix  estimate size 2 %u %o elm "BUNFMT" val %d bits %d mask %o\n",
                *v,*w, i,val, prefixbits, mask);
#endif

			if( task->range[MOSAIC_PREFIX] > task->start + 1){
				bits = (task->range[MOSAIC_PREFIX] - task->start) * (16-prefixbits);
				store = wordaligned(2 * sizeof(unsigned short),int);
				store += wordaligned(bits/8 + ((bits % 8) >0),int);
				store = wordaligned( MosaicBlkSize + store,int);
				if( store >= (flt)i * sizeof(sht))
					return 0.0;
				return task->factor[MOSAIC_PREFIX] = ( (flt)i * sizeof(sht))/ store;
			}
			
			// calculate the number of values covered by this prefix
			val = *v & mask;
			for(w=v,i = 0; i < limit ; w++, i++){
				if ( val != (*w & mask) )
					break;
			}
			bits = i * (16-prefixbits);
			store = wordaligned(2 * sizeof(unsigned short),int);
			store += wordaligned(bits/8 + ((bits % 8) >0),int);
			store = wordaligned( MosaicBlkSize + store,int);
			if( store >= (flt)i * sizeof(sht))
				return 0.0;
			if( task->dst +  store >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(sht))/ store;
		}
		break;
	case 4:
		{	unsigned int *v = ((unsigned int*) task->src) + task->start, *w= v+1, val= *v, mask;
			findPrefixInt(cntxt, v, LOOKAHEAD, &prefixbits,&mask);
			if( prefixbits == 0)
				break;

#ifdef _DEBUG_PREFIX_
            mnstr_printf(cntxt->fdout,"#prefix estimate size 4 %o elm "BUNFMT" val %d bits %d mask %o\n",
                *v, i, val, prefixbits, mask);
#endif
			if( task->range[MOSAIC_PREFIX] > task->start + 1){
				bits = (task->range[MOSAIC_PREFIX] - task->start) * (32-prefixbits);
				store = wordaligned(2 * sizeof(unsigned int),int);
				store += wordaligned(bits/8 + ((bits % 8) >0),int);
				store = wordaligned( MosaicBlkSize + store,int);
				if( store > (flt)i * sizeof(int))
					return 0.0;
				return task->factor[MOSAIC_PREFIX] = ( (flt)i * sizeof(int))/ store;
			}
			
			// calculate the number of values covered by this prefix
			val = *v & mask;
			for(w=v,i = 0; i < limit; w++, i++){
				if ( val != (*w & mask) )
					break;
			}
			bits = i * (32-prefixbits);
			// calculate the bitvector size
			store = wordaligned(2 * sizeof(unsigned int),int);
			store += wordaligned(bits/8 + ((bits % 8) >0),int);
			store = wordaligned( MosaicBlkSize + store,int);
			if( store >= (flt)i * sizeof(int))
				return 0.0;
			if( task->dst +  store >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(int))/ store;
		}
		break;
	case 8:
		{	ulng *v = ((ulng*) task->src) + task->start, *w= v+1, val= *v,mask;
			findPrefixLng(cntxt, v, LOOKAHEAD, &prefixbits,&mask);
			if( prefixbits == 0 )
				break;

			if( task->range[MOSAIC_PREFIX] > task->start + 1){
				bits = (task->range[MOSAIC_PREFIX] - task->start) * (64-prefixbits);
				store = wordaligned(2 * sizeof(ulng),int);
				store += wordaligned(bits/8 + ((bits % 8) >0),int);
				store = wordaligned( MosaicBlkSize + store,int);
				if( store >= (flt)i * sizeof(lng))
					return 0.0;
				return task->factor[MOSAIC_PREFIX] = ( (flt)i * sizeof(lng))/ store;
			}
			
			val = *v & mask;
			for(w=v, i = 0; i < limit ; w++, i++){
				if ( val != (*w & mask) )
					break;
			}
			bits = (int)(i * (32-prefixbits));
			store = wordaligned(2 * sizeof(ulng),int);
			store += wordaligned(bits/8 + ((bits % 8) >0),int);
			store = wordaligned( MosaicBlkSize + store,int);
			if( store >= (flt)i * sizeof(lng))
				return 0.0;
			if( task->dst +  store >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(lng))/ store;
		}
	}
#ifdef _DEBUG_PREFIX_
	mnstr_printf(cntxt->fdout,"#estimate prefixbits %d "BUNFMT" elm %4.3f factor\n",prefixbits,i,factor);
#endif
	task->factor[MOSAIC_PREFIX] = factor;
	task->range[MOSAIC_PREFIX] = task->start + i;
	return factor;
}

#define compress(Vector,I, Bits, Value) setBitVector(Vector,I,Bits,Value);

void
MOScompress_prefix(Client cntxt, MOStask task)
{
	BUN limit,  j =0 ;
	int prefixbits,bits;
	lng size; 
	BitVector base;
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = task->blk;

	(void) cntxt;
	MOSsetTag(blk, MOSAIC_PREFIX);

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
	if( task->elm >=2 )
	switch(size){
	case 1:
		{	unsigned char *v = ((unsigned char*) task->src) + task->start, *wlimit= v + limit, val1 = *v, mask;
			unsigned char *dst = (unsigned char*) MOScodevector(task);
			findPrefixBit(cntxt, v, LOOKAHEAD, &prefixbits,&mask);
			bits = 8-prefixbits;
			base = (BitVector)( ((char*)dst) + wordaligned(2 * sizeof(unsigned char),int));
			*dst++ = mask;
			val1 = *v & mask;	//reference value
			*dst++ = val1 | bits; // bits outside mask
			
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout,"#prefix 1 compress %o val %d bits (%d, %d) mask %o\n",
				*v, val1, prefixbits, bits,mask);
#endif
			for(j=0  ; v < wlimit; v++, j++){
				if ( val1  != (*v & mask) )
					break;
				compress(base, j, bits, (int) (*v & (~mask))); 
#ifdef _DEBUG_PREFIX_
				mnstr_printf(cntxt->fdout,"#compress %d store %d\n", *v,  (int) (*v & (~mask)));
#endif
				hdr->checksum.sumbte += *v;
			}
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout," blk "BUNFMT"\n",j);
#endif
			MOSsetCnt(blk,j);
		}
		break;
	case 2:
		{	unsigned short *v = ((unsigned short*) task->src) + task->start, *wlimit= v + limit, val1, mask;
			unsigned short *dst = (unsigned short*) MOScodevector(task);

			findPrefixSht(cntxt, v, LOOKAHEAD, &prefixbits,&mask);
			bits = 16-prefixbits;
			base = (BitVector)( ((char*)dst) + wordaligned(2 * sizeof(unsigned short),int));
			*dst++ = mask;
			val1 = *v & mask;	//reference value
			*dst++ = val1 | bits; // bits outside mask
			
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout,"#compress[2] "BUNFMT" bits %d mask %o address "LLFMT"\n",MOSgetCnt(blk), bits,mask,(lng) MOScodevector(task));
#endif
			for(j=0  ; v < wlimit; v++, j++){
				if ( val1  != (*v & mask) )
					break;
				compress(base, j, bits, (int) (*v & (~mask))); 
#ifdef _DEBUG_PREFIX_
				mnstr_printf(cntxt->fdout,"#compress %d store %d\n", *v,  (int) (*v & (~mask)));
#endif
				hdr->checksum.sumsht += *v;
			}
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout," blk "BUNFMT"\n",j);
#endif
			MOSsetCnt(blk,j);
		}
		break;
	case 4:
		{	unsigned int *v = ((unsigned int*) task->src) + task->start, *wlimit=  v + limit, val1, mask;
			unsigned int *dst = (unsigned int*)  MOScodevector(task);

			findPrefixInt(cntxt, v, LOOKAHEAD, &prefixbits,&mask);
			bits = 32-prefixbits;
			base = (BitVector)(((char*)dst) + wordaligned(2 * sizeof(unsigned int),int));
			*dst++ = mask;
			val1 = *v & mask ;		//reference value 
			*dst++ = val1 | bits;	// and keep bits
			
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout,"#compress[4] "BUNFMT" bits %d mask %o address "LLFMT"\n",MOSgetCnt(blk), bits,mask,(lng) MOScodevector(task));
#endif
			for(j=0  ; v < wlimit; v++, j++){
				if ( val1  != (*v & mask) )
					break;
				compress(base, j, bits, (int) (*v & (~mask))); 
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout,"#compress %d store %d\n", *v,  (int) (*v & (~mask)));
#endif
				hdr->checksum.sumint += *v;
			}
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout," blk "BUNFMT"\n",j);
#endif
			MOSsetCnt(blk,j);
		}
		break;
	case 8:
		{	ulng *v = ((ulng*) task->src) + task->start, *wlimit = v + limit,  val1, mask;
			ulng *dst = (ulng*)  MOScodevector(task);
			
			findPrefixLng(cntxt, v, LOOKAHEAD, &prefixbits,&mask);
			bits = 64-prefixbits;
			base = (BitVector)(((char*)dst) + wordaligned(2 * sizeof(ulng),int));
			if (bits <= 32){
				*dst++ = mask;
				val1 = *v & mask;	//reference value
				*dst++ = val1 | (ulng) bits; // bits outside mask
				
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout,"#compress[4] bits %d mask "LLFMT" address "LLFMT"\n",bits,mask,(lng) MOScodevector(task));
#endif
				for(j=0 ; v < wlimit ; v++, j++){
					if ( val1  != (*v & mask) )
						break;
					compress(base,j,bits, (int)(*v & (~mask))); // bits
					hdr->checksum.sumlng += *v;
				}
			}
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout," blk "BUNFMT"\n",j);
#endif
			MOSsetCnt(blk,j);
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_prefix(cntxt, task);
#endif
}

#define decompress(Vector,I,Bits) getBitVector(Vector,I,Bits)

void
MOSdecompress_prefix(Client cntxt, MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i,lim;
	int bits,size;
	BitVector base;
	(void) cntxt;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	lim= MOSgetCnt(blk);
#ifdef _DEBUG_PREFIX_
	mnstr_printf(cntxt->fdout,"#decompress prefix blk "BUNFMT"\n",lim);
#endif
	switch(size){
	case 1:
		{	unsigned char *dst =  (unsigned char*)  MOScodevector(task);
			unsigned char mask = *dst++, val  =  *dst++, v;

			base = (BitVector)((char*)MOScodevector(task) + wordaligned(2 * sizeof(unsigned char),int));
			bits =(int) (val & (~mask));
			val = val & mask;
			//mnstr_printf(cntxt->fdout,"decompress bits %d mask %o val %d\n",bits,m,val);
			for(i = 0; i < lim; i++){
				v = val | decompress(base,i,bits);
				hdr->checksum2.sumsht += v;
				((unsigned char*) task->src)[i] = v;
			}
			task->src += i * sizeof(unsigned char);
		}
		break;
	case 2:
		{	unsigned short *dst =  (unsigned short*)   MOScodevector(task);
			unsigned short mask = *dst++, val  =  *dst++, v;

			base = (BitVector)((char*)MOScodevector(task) + wordaligned(2 * sizeof(unsigned short),int));
			bits = (int) (val & (~mask));
			val = val & mask;
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout,"#decompress[2] bits %d mask %o address "LLFMT"\n",bits,mask,(lng) MOScodevector(task));
#endif
			for(i = 0; i < lim; i++){
				v = val | decompress(base,i,bits);
#ifdef _DEBUG_PREFIX_
				mnstr_printf(cntxt->fdout,"#decompress elm "BUNFMT" v %d comp %d  val %d\n",
					i, val, decompress(base,i,bits), v);
#endif
				hdr->checksum2.sumsht += v;
				((unsigned short*) task->src)[i] = v;
			}
			task->src += i * sizeof(unsigned short);
		}
		break;
	case 4:
		{	unsigned int *dst =  (unsigned int*)  MOScodevector(task);
			unsigned int mask = *dst++, val  =  *dst++, v;

			base = (BitVector)(MOScodevector(task) + wordaligned(2 * sizeof(unsigned int),int));
			bits = (int)(val & (~mask));
			val = val & mask;
#ifdef _DEBUG_PREFIX_
			mnstr_printf(cntxt->fdout,"#decompress[4] bits %d mask %o address "LLFMT"\n",bits,mask,(lng) MOScodevector(task));
#endif
			for(i = 0; i < lim; i++){
				v = val | decompress(base,i,bits);
#ifdef _DEBUG_PREFIX_
				mnstr_printf(cntxt->fdout,"#decompress elm "BUNFMT" v %d comp %d  val %d\n",
					i, val, decompress(base,i,bits), v);
#endif
				hdr->checksum2.sumint += v;
				((unsigned int*) task->src)[i] = v;
			}
			task->src += i * sizeof(unsigned int);
		}
		break;
	case 8:
		{	ulng *dst =  (ulng*)  MOScodevector(task);
			ulng mask = *dst++, val  =  *dst++, v;

			base = (BitVector)((char*)MOScodevector(task) + wordaligned(2 * sizeof(ulng),int));
			bits = (int)(val & (~mask));
			val = val & mask;
			//mnstr_printf(cntxt->fdout,"decompress bits %d mask %o val %d\n",bits,m,val);
			for(i = 0; i < lim; i++){
				v= val | decompress(base,i,bits);
				hdr->checksum2.sumlng += v;
				((ulng*) task->src)[i] = v;
			}
			task->src += i * sizeof(ulng);
		}
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define  select_prefix(TPE, TPE2) \
{	TPE2 *dst =  (TPE2*)  MOScodevector(task);\
	TPE2 mask = *dst++, val = *dst++,v;\
	TPE value;\
	base = (BitVector)(MOScodevector(task) + wordaligned(2 * sizeof(TPE),int));\
	bits = (int) (val & (~mask));\
	val = val & mask;\
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
				v = val | decompress(base,i,bits);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*hi && value <= * (TPE*)hgh ) || (!*hi && value < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v = val | decompress(base,i,bits);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*li && value >= * (TPE*)low ) || (!*li && value > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v = val | decompress(base,i,bits);\
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
				v = val | decompress(base,i,bits);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*hi && value <= * (TPE*)hgh ) || (!*hi && value < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for( ; first < last; first++, val++,i++){\
				MOSskipit();\
				v = val | decompress(base,i,bits);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*li && value >= * (TPE*)low ) || (!*li && value > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, val++,i++){\
				MOSskipit();\
				v = val | decompress(base,i,bits);\
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
MOSselect_prefix(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	int bits,cmp;
	BUN i = 0,first,last;
	BitVector base;
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
	case TYPE_bte: select_prefix(bte,unsigned char); break;
	case TYPE_sht: select_prefix(sht,unsigned short); break;
	case TYPE_int: select_prefix(int,unsigned int); break;
	case TYPE_lng: select_prefix(lng,ulng); break;
	case TYPE_oid: select_prefix(oid,ulng); break;
	case TYPE_flt: select_prefix(flt,unsigned int); break;
	case TYPE_dbl: select_prefix(dbl,ulng); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_prefix(hge,unsigned long long); break;
#endif
	default:
		if( task->type == TYPE_date)
			select_prefix(date,unsigned int); 
		if( task->type == TYPE_daytime)
			select_prefix(daytime,unsigned int); 
		if( task->type == TYPE_timestamp)
			select_prefix(lng,ulng); 
	}
	MOSadvance_prefix(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_prefix(TPE, TPE2)\
{ 	TPE low,hgh;\
    TPE2 *dst =  (TPE2*)  (((char*) blk) + MosaicBlkSize);\
    TPE2 mask = *dst++, val = *dst++,v;\
    TPE value;\
	base = (BitVector)(MOScodevector(task) + wordaligned(2 * sizeof(TPE),int));\
	bits = (int)( val & (~mask));\
	val = val & mask;\
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
			v = val | decompress(base,i,bits);\
			value =  (TPE) ((TPE2)val |(TPE2) v);\
			if( (low == TPE##_nil || value >= low) && (value <= hgh || hgh == TPE##_nil) )\
			*o++ = (oid) first;\
		}\
	else\
		for( ; first < last; first++,i++){\
			MOSskipit();\
			v = val | decompress(base,i,bits);\
			value =  (TPE) ((TPE2)val |(TPE2) v);\
			if( !( (low == TPE##_nil || value >= low) && (value <= hgh || hgh == TPE##_nil) ))\
				*o++ = (oid) first;\
		}\
}

str
MOSthetaselect_prefix(Client cntxt,  MOStask task, void *input, str oper)
{
	oid *o;
	int bits, anti=0;
	BUN i=0,first,last;
	MosaicBlk blk = task->blk;
    BitVector base;
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
	case TYPE_bte: thetaselect_prefix(bte, unsigned char); break;
	case TYPE_sht: thetaselect_prefix(sht, unsigned short); break;
	case TYPE_int: thetaselect_prefix(int, unsigned int); break;
	case TYPE_lng: thetaselect_prefix(lng, ulng); break;
	case TYPE_oid: thetaselect_prefix(oid, ulng); break;
	case TYPE_flt: thetaselect_prefix(flt, unsigned int); break;
	case TYPE_dbl: thetaselect_prefix(dbl, ulng); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_prefix(hge, unsigned long long); break;
#endif
	}
	MOSskip_prefix(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_prefix(TPE, TPE2)\
{	TPE *r;\
    TPE2 *dst =  (TPE2*)  MOScodevector(task);\
    TPE2 mask = *dst++, val = *dst++,v;\
    TPE value;\
	base = (BitVector)(MOScodevector(task) + wordaligned(2 * sizeof(TPE),int));\
	bits = (int) (val & (~mask));\
	val = val & mask;\
	r= (TPE*) task->src;\
	for(; first < last; first++,i++){\
		MOSskipit();\
		v = val | decompress(base,i,bits);\
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
	int bits; 
	BUN i=0, first,last;
    BitVector base;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMstorage(task->type)){
		case TYPE_bte: projection_prefix(bte, unsigned char); break;
		case TYPE_sht: projection_prefix(sht, unsigned short); break;
		case TYPE_int: projection_prefix(int, unsigned int); break;
		case TYPE_lng: projection_prefix(lng, ulng); break;
		case TYPE_oid: projection_prefix(oid, ulng); break;
		case TYPE_flt: //projection_prefix(flt, unsigned int); break;
{	flt *r;
    unsigned int *dst =  (unsigned int*)  MOScodevector(task);
    unsigned int mask = *dst++, val  =  *dst++,v;
    flt value;
	bits = (int) val & (~mask);
	val = val & mask;
	base = (BitVector) dst;
	r= (flt*) task->src;
	for(; first < last; first++,i++){
		MOSskipit();
		v = val | decompress(base,i,bits);
		value =  (flt) ((unsigned int)val |(unsigned int) v);
		*r++ = value;
		task->n--;
		task->cnt++;
	}
	task->src = (char*) r;
}
break;
		case TYPE_dbl: projection_prefix(dbl, ulng); break;
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
	TPE2 *dst =  (TPE2*)  MOScodevector(task);\
	TPE2 mask = *dst++, val = *dst++,v;\
	TPE value;\
	base = (BitVector)(MOScodevector(task) + wordaligned(2 * sizeof(TPE),int));\
	bits = (int) (val & (~mask));\
	val = val & mask;\
	w = (TPE*) task->src;\
	for(n = task->elm, o = 0; n -- > 0; w++,o++){\
		for(i=0, oo= (oid) first; oo < (oid) last; v++, oo++,i++){\
			v = val | decompress(base,i,bits);\
			value =  (TPE) ((TPE2)val |(TPE2) v);\
			if ( *w == value){\
				BUNappend(task->lbat, &oo, FALSE);\
				BUNappend(task->rbat, &o, FALSE);\
			}\
		}\
	}\
}

str
MOSjoin_prefix(Client cntxt,  MOStask task)
{
	int bits;
	BUN i= 0,n,first,last;
	oid o, oo;
	BitVector base;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMstorage(task->type)){
		case TYPE_bte: join_prefix(bte,unsigned char); break;
		case TYPE_sht: join_prefix(sht,unsigned short); break;
		case TYPE_int: join_prefix(int,unsigned int); break;
		case TYPE_lng: join_prefix(lng,ulng); break;
		case TYPE_oid: join_prefix(oid,BUN); break;
		case TYPE_flt: join_prefix(flt,unsigned int); break;
		case TYPE_dbl: join_prefix(dbl,ulng); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_prefix(hge,unsigned long long); break;
#endif
		case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: join_prefix(bte, unsigned char); break;
		case 2: join_prefix(sht, unsigned short); break;
		case 4: join_prefix(int, unsigned int); break;
		case 8: join_prefix(lng, ulng); break;
		}
	}
	MOSskip_prefix(cntxt,task);
	return MAL_SUCCEED;
}
