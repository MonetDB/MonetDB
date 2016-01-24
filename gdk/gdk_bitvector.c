/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* author: M Kersten
 * This collection of routines manages put/get of bit patterns into a vector.
 * The width of the individual elements is limited to sizeof(int)
 * The meta-information is not stored within the vector.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"

#define BITS (sizeof( unsigned int) * 8)
static unsigned int masks[BITS+1];

void initBitMasks(void)
{
	unsigned int i,v=1;
	for( i=0; i<BITS; i++){
		masks[i+1] = v;
		v = (v << 1) | 1;
	}
}

BitVector
newBitVector(BUN cnt, int width)  
{ 
	BitVector m;
	if( (unsigned) width > BITS)
		return 0;
	
	m = (BitVector) malloc( (cnt * width)/ 8 + (((cnt * width) % 8) > 0)* 8);
	memset((char*) m, 0,  (cnt * width)/ 8 + (((cnt * width) % 8) > 0)* 8);
	return m;
}

// set the bits of cell idx to the lower number of bits of the value
void
setBitVector(BitVector vector, const BUN i, const int bits, const int value)
{
	BUN cid;
	unsigned int m1,  m2, shift;

	cid = (i * bits) / BITS;
	shift  = (i * bits) % BITS;
    if ( (shift + bits) <= BITS){
        vector[cid]= (vector[cid]  & ~( masks[bits] << shift)) | ((value & masks[bits]) << (shift));
		//printf("#setBitVector i "BUNFMT" bits %d value %d shift %d vector[cid]=%o\n",i,bits,value,shift, vector[cid]);
    }else{ 
		m1 = BITS - shift;
		m2 = bits - m1;
        vector[cid]= (vector[cid]  & ~( masks[m1] << shift)) | ( (value & masks[m1]) << shift);
        vector[cid+1]= 0 |  ((value >> m1) & masks[m2]) ;
		//printf("#setBitVector i "BUNFMT" bits %d value %d shift=%d m1=%d m2=%d vector[cid]=%o vector[cid+1]=%o\n",i,bits,value,shift,m1,m2, vector[cid],vector[cid+1]);
	}
}

// clear a cell
void
clrBitVector(BitVector vector, BUN i, int bits)
{
	setBitVector(vector,i,bits, 0);
}

// get the bits of cell i 
int
getBitVector(BitVector vector, BUN i, int bits)
{
	BUN cid;
	unsigned int value = 0, m1,m2= 0, shift;
	
	cid = (i * bits) / BITS;
	shift  = (i * bits) % BITS;
	if ( (shift + bits) <= BITS){
		value = (vector[cid] >> shift) & masks[bits];
		//printf("#getBitVector i "BUNFMT" bits %d value %d shift %d vector[cid]=%o\n",i,bits,value,shift, vector[cid]);
	} else{ 
		m1 = BITS-shift;
		m2 = bits - m1;
		value = ((vector[cid] >> shift) & masks[m1]) | ((vector[cid+1] & masks[m2])<< m1);
		//printf("#getBitVector i "BUNFMT" bits %d value %d m1=%d m2=%d vector[cid]=%o vector[cid+1]=%o\n",i,bits,value,m1,m2, vector[cid],vector[cid+1]);
	  }
	return value;
}

int
tstBitVector(BitVector m, BUN idx, int width)
{
	return getBitVector(m,idx,width) > 0;
}

