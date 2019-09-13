/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "mosaic_private.h"
#include "gdk_bitvector.h"

bool MOStypes_prefix(BAT* b) {
	switch(ATOMbasetype(getBatType(b->ttype))){
	case TYPE_bte: return true;
	case TYPE_bit: return true;
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
	case TYPE_flt: return true;
	case TYPE_dbl: return true;
#ifdef HAVE_HGE
	case TYPE_hge: return true;
#endif
	case  TYPE_str:
		switch(b->twidth){
		case 1: return true;
		case 2: return true;
		case 4: return true;
		case 8: return true;
		}
		break;
	}

	return false;
}

void
MOSlayout_prefix(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	BUN cnt = MOSgetCnt(blk), input=0, output= 0, bytes = 0;
	int bits =0;
	int size = ATOMsize(task->type);
	char buf[32];

	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	input = cnt * ATOMsize(task->type);
	switch(size){
	case 1:
		{	unsigned char *dst = (unsigned char*)  MOScodevector(task);
			unsigned char mask = *dst++;
			unsigned char val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned char),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits), int);
		}
		break;
	case 2:
		{	unsigned short *dst = (unsigned short*)  MOScodevector(task);
			unsigned short mask = *dst++;
			unsigned short val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned short),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , int);
		}
		break;
	case 4:
		{	unsigned int *dst = (unsigned int*)  MOScodevector(task);
			unsigned int mask = *dst++;
			unsigned int val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(unsigned int),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , int);
		}
		break;
	case 8:
		{	ulng *dst = (ulng*)  MOScodevector(task);
			ulng mask = *dst++;
			ulng val = *dst;
			bits = (int)(val & (~mask));
			bytes = wordaligned(MosaicBlkSize + 2 * sizeof(ulng),int);
			bytes += wordaligned(getBitVectorSize(cnt,bits) , int);
		}
	}
	output = wordaligned(bytes, int); 
	snprintf(buf,32,"%d bits",bits);
	if( BUNappend(btech, "prefix blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, buf, false) != GDK_SUCCEED)
		return;
}

void
MOSadvance_prefix(MOStask task)
{
	int bits = 0;
	size_t bytes= 0;
	int size;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	task->start += MOSgetCnt(task->blk);
	task->stop = task->stop;
	switch(size){
	case 1:
		{	unsigned char *dst = (unsigned char*)  MOScodevector(task);
			unsigned char mask = *dst++;
			unsigned char val = *dst++;
			bits = (int)(val & (~mask));
			bytes = wordaligned(2 * sizeof(unsigned char),int);
			bytes += wordaligned(getBitVectorSize(MOSgetCnt(task->blk),bits), int);
			task->blk = (MosaicBlk) (((char*) dst)  + bytes); 
		}
		break;
	case 2:
		{	unsigned short *dst = (unsigned short*)  MOScodevector(task);
			unsigned short mask = *dst++;
			unsigned short val = *dst++;
			bits = (int)(val & (~mask));
			bytes = wordaligned(2 * sizeof(unsigned short),int);
			bytes += wordaligned(getBitVectorSize(MOSgetCnt(task->blk),bits), int);
			task->blk = (MosaicBlk) (((char*) dst)  + bytes); 
		}
		break;
	case 4:
		{	unsigned int *dst = (unsigned int*)  MOScodevector(task);
			unsigned int mask = *dst++;
			unsigned int val = *dst++;
			bits = (int)(val & (~mask));
			bytes = wordaligned(2 * sizeof(unsigned int),int);
			bytes += wordaligned(getBitVectorSize(MOSgetCnt(task->blk),bits), int);
			task->blk = (MosaicBlk) (((char*) dst)  + bytes); 
		}
		break;
	case 8:
		{	ulng *dst = (ulng*)  MOScodevector(task);
			ulng mask = *dst++;
			ulng val = *dst++;
			bits = (int)(val & (~mask));
			bytes = wordaligned(2 * sizeof(ulng),int);
			bytes += wordaligned(getBitVectorSize(MOSgetCnt(task->blk),bits), int);
			task->blk = (MosaicBlk) (((char*) dst)  + bytes); 
		}
	}
}

void
MOSskip_prefix(MOStask task)
{
	MOSadvance_prefix(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

// logarithmic search for common prefix in a given block
// use static prefix mask attempts

static void
findPrefixBit(unsigned char *v, int limit, int *bits, unsigned char *prefixmask)
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
		if( i ==  limit){
			width += step;
			*bits = width;
			*prefixmask = mask;
		}
	} while (step > 1);
}

static void
findPrefixSht(unsigned short *v, int limit, int *bits, unsigned short *prefixmask)
{
	int i, step = 16, width = 0;
	unsigned short prefix, mask;
	*bits = 0;
	*prefixmask = 0;
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
		if( i ==  limit){
			width += step;
			*bits = width;
			*prefixmask = mask;
		}
	} while (step > 1);
}

static void
findPrefixInt(unsigned int *v, int limit, int *bits, unsigned int *prefixmask)
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
		if( i ==  limit){
			width += step;
			*bits = width;
			*prefixmask = mask;
		}
	} while (step > 1);
}

static void
findPrefixLng(ulng *v, int limit, int *bits, ulng *prefixmask)
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
		if( i ==  limit){
			width += step;
			*bits = width;
			*prefixmask = mask;
		}
	} while (step > 1 && *bits < 32);
	// we only use at most 32 bits as prefix due to bitvector implementation
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


#define LOOKAHEAD  (int)(limit <10? limit:10)
// calculate the expected reduction 
flt
MOSestimate_prefix(MOStask task)
{	BUN i = 0;
	flt factor = 0.0;
	int prefixbits = 0,size;
	lng bits,store;
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	if( task->stop >= 2)
	switch(size){
	case 1:
		{	unsigned char *v = ((unsigned char*) task->src) + task->start, val= *v, mask;
			findPrefixBit( v, LOOKAHEAD, &prefixbits, &mask);
			if( prefixbits == 0)
				break;

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
			for( i = 0; i < limit ; v++, i++){
				if ( val != (*v & mask) )
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
		{	unsigned short *v = ((unsigned short*) task->src) + task->start, val= *v, mask;
			findPrefixSht( v, LOOKAHEAD, &prefixbits, &mask);
			if( prefixbits == 0)
				break;

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
			for(i = 0; i < limit ; v++, i++){
				if ( val != (*v & mask) )
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
		{	unsigned int *v = ((unsigned int*) task->src) + task->start, val= *v, mask;
			findPrefixInt( v, LOOKAHEAD, &prefixbits,&mask);
			if( prefixbits == 0)
				break;

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
			for(i = 0; i < limit; v++, i++){
				if ( val != (*v & mask) )
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
		{	ulng *v = ((ulng*) task->src) + task->start, val= *v, mask;
			findPrefixLng( v, LOOKAHEAD, &prefixbits, &mask);
			if( prefixbits < 32 ) // residu should fit bitvector cell
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
			for(i = 0; i < limit ; v++, i++){
				if ( val != (*v & mask) )
					break;
			}
			bits = (int)(i * (64-prefixbits));
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
	task->factor[MOSAIC_PREFIX] = factor;
	task->range[MOSAIC_PREFIX] = task->start + i;
	return factor;
}

#define compress(Vector,I, Bits, Value) setBitVector(Vector,I,Bits,Value);

void
MOScompress_prefix(MOStask task)
{
	BUN limit,  j =0 ;
	int prefixbits;
	lng size;
	BitVector base;
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = task->blk;

	MOSsetTag(blk, MOSAIC_PREFIX);

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;
	if( task->stop >=2 )
	switch(size){
	case 1:
		{	unsigned char *v = ((unsigned char*) task->src) + task->start, *wlimit= v + limit, val1 = *v, mask, bits;
			unsigned char *dst = (unsigned char*) MOScodevector(task);
			findPrefixBit( v, LOOKAHEAD, &prefixbits,&mask);
			bits = 8-prefixbits;
			base = (BitVector)( ((char*)dst) + wordaligned(2 * sizeof(unsigned char),int));
			*dst++ = mask;
			val1 = *v & mask;	//reference value
			*dst++ = val1 | bits; // bits outside mask
			
			for(j=0  ; v < wlimit; v++, j++){
				if ( val1  != (*v & mask) )
					break;
				compress(base, j, (int) bits, (int) (*v & (~mask))); 
				hdr->checksum.sumbte += *v;
			}
			MOSsetCnt(blk,j);
		}
		break;
	case 2:
		{	unsigned short *v = ((unsigned short*) task->src) + task->start, *wlimit= v + limit, val1, mask, bits;
			unsigned short *dst = (unsigned short*) MOScodevector(task);

			findPrefixSht( v, LOOKAHEAD, &prefixbits,&mask);
			bits = 16-prefixbits;
			base = (BitVector)( ((char*)dst) + wordaligned(2 * sizeof(unsigned short),int));
			*dst++ = mask;
			val1 = *v & mask;	//reference value
			*dst++ = val1 | bits; // bits outside mask
			
			for(j=0  ; v < wlimit; v++, j++){
				if ( val1  != (*v & mask) )
					break;
				compress(base, j, (int) bits, (int) (*v & (~mask))); 
				hdr->checksum.sumsht += *v;
			}
			MOSsetCnt(blk,j);
		}
		break;
	case 4:
		{	unsigned int *v = ((unsigned int*) task->src) + task->start, *wlimit=  v + limit, val1, mask, bits;
			unsigned int *dst = (unsigned int*)  MOScodevector(task);

			findPrefixInt( v, LOOKAHEAD, &prefixbits,&mask);
			bits = 32-prefixbits;
			base = (BitVector)(((char*)dst) + wordaligned(2 * sizeof(unsigned int),int));
			*dst++ = mask;
			val1 = *v & mask ;		//reference value 
			*dst++ = val1 | bits;	// and keep bits
			
			for(j=0  ; v < wlimit; v++, j++){
				if ( val1  != (*v & mask) )
					break;
				compress(base, j, bits, (int) (*v & (~mask))); 
				hdr->checksum.sumint += *v;
			}
			MOSsetCnt(blk,j);
		}
		break;
	case 8:
		{	ulng *v = ((ulng*) task->src) + task->start, *wlimit = v + limit,  val1, mask, bits;
			ulng *dst = (ulng*)  MOScodevector(task);
			
			findPrefixLng( v, LOOKAHEAD, &prefixbits,&mask);
			bits = 64-prefixbits;
			base = (BitVector)(((char*)dst) + wordaligned(2 * sizeof(ulng),int));
			if (bits <= 32){
				*dst++ = mask;
				val1 = *v & mask;	//reference value
				*dst++ = val1 | (ulng) bits; // bits outside mask
				
				for(j=0 ; v < wlimit ; v++, j++){
					if ( val1  != (*v & mask) )
						break;
					compress(base,j, (int) bits, (int)(*v  & (ulng)UINT_MAX & (~mask))); // at most 32 bits
					hdr->checksum.sumlng += *v;
				}
			}
			MOSsetCnt(blk,j);
		}
	}
}

#define decompress(Vector,I,Bits) getBitVector(Vector,I,Bits)

void
MOSdecompress_prefix(MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i,lim;
	int bits,size;
	BitVector base;

	size = ATOMsize(task->type);
	if( ATOMstorage(task->type == TYPE_str))
			size =task->bsrc->twidth;
	lim= MOSgetCnt(blk);
	switch(size){
	case 1:
		{	unsigned char *dst =  (unsigned char*)  MOScodevector(task);
			unsigned char mask = *dst++, val  =  *dst++, v;

			base = (BitVector)((char*)MOScodevector(task) + wordaligned(2 * sizeof(unsigned char),int));
			bits =(int) (val & (~mask));
			val = val & mask;
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
			for(i = 0; i < lim; i++){
				v = val | decompress(base,i,bits);
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
			for(i = 0; i < lim; i++){
				v = val | decompress(base,i,bits);
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
		if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) low) ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v = val | decompress(base,i,bits);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*hi && value <= * (TPE*)hgh ) || (!*hi && value < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
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
		if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
			/* nothing is matching */\
		} else\
		if( is_nil(TPE, *(TPE*) low) ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v = val | decompress(base,i,bits);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*hi && value <= * (TPE*)hgh ) || (!*hi && value < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for( ; first < last; first++,i++){\
				MOSskipit();\
				v = val | decompress(base,i,bits);\
				value =  (TPE) ((TPE2)val |(TPE2) v);\
				cmp  =  ((*li && value >= * (TPE*)low ) || (!*li && value > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++,i++){\
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
MOSselect_prefix( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	int bits,cmp;
	BUN i = 0,first,last;
	BitVector base;
	// set the oid range covered
	first = task->start;
	last = first + MOSgetCnt(task->blk);

		if (task->cl && *task->cl > last){
		MOSadvance_prefix(task);
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
	MOSadvance_prefix(task);
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
			if( (is_nil(TPE, low) || value >= low) && (value <= hgh || is_nil(TPE, hgh)) )\
			*o++ = (oid) first;\
		}\
	else\
		for( ; first < last; first++,i++){\
			MOSskipit();\
			v = val | decompress(base,i,bits);\
			value =  (TPE) ((TPE2)val |(TPE2) v);\
			if( !( (is_nil(TPE, low) || value >= low) && (value <= hgh || is_nil(TPE, hgh)) ))\
				*o++ = (oid) first;\
		}\
}

str
MOSthetaselect_prefix( MOStask task, void *input, str oper)
{
	oid *o;
	int bits, anti=0;
	BUN i=0,first,last;
	MosaicBlk blk = task->blk;
    BitVector base;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_prefix(task);
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
	MOSskip_prefix(task);
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
		task->cnt++;\
	}\
	task->src = (char*) r;\
}

str
MOSprojection_prefix( MOStask task)
{
	int bits; 
	BUN i=0, first,last;
    BitVector base;

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
	MOSskip_prefix(task);
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
	for(n = task->stop, o = 0; n -- > 0; w++,o++){\
		for(i=0, oo= (oid) first; oo < (oid) last; v++, oo++,i++){\
			v = val | decompress(base,i,bits);\
			value =  (TPE) ((TPE2)val |(TPE2) v);\
			if ( *w == value){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED )\
				throw(MAL,"mosaic.prefix",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

str
MOSjoin_prefix( MOStask task)
{
	int bits;
	BUN i= 0,n,first,last;
	oid o, oo;
	BitVector base;

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
	MOSskip_prefix(task);
	return MAL_SUCCEED;
}
