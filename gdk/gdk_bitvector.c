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
{   int size;
	BitVector m;
	if( (unsigned) width > BITS)
		return 0;
	
	size = (cnt * width)/8 + ((cnt * width)%8) >0;
	size = (size / sizeof(int)) * sizeof(int) + ((size % sizeof(int)) > 0) * sizeof(int);

	m = (BitVector) GDKzalloc( size);
	initBitMasks();
	return m;
}

// set the bits of cell idx to the lower number of bits of the value
void
setBitVector(BitVector vector, const BUN i, const int bits, const int value)
{
	BUN cid;
	unsigned int m1,  m2;

	cid = (i * bits) / BITS;
	m1  = ((i * bits) % BITS)/bits;
    //mnstr_printf(GDKout,"#setBitVector %ld i "BUNFMT" bits %d value %d cid %d ",(long)vector,i,bits, value, vector[cid]);
    if ( m1 * bits <= BITS)
        vector[cid]= (vector[cid]  & ~( masks[bits] << (m1 * bits))) | ((value & masks[bits]) << (m1 * bits));
    else{ 
		m1 = (m1 * bits) % BITS;
		m2 = bits - m1;
        vector[cid]= (vector[cid]  & ~( masks[bits] << (m1 * bits))) | ( (value & masks[bits]) << (BITS-m1));
        vector[cid+1]= 0 | ( (value & masks[bits])  >> m2);
	}
    //mnstr_printf(GDKout,"-> %o\n",vector[cid]);
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
	unsigned int value = 0, m1,m2;
	
	cid = (i * bits) / BITS;
	m1  = ((i * bits) % BITS)/bits;
	if ( m1 * bits <= BITS)
		value = (vector[cid] >> (m1 * bits)) & masks[bits];
	else{ 
		m1 = (m1 * bits) % BITS;
		m2 = bits - m1;
		value  = (((vector[cid] >> (BITS - m1)) & masks[bits - m1]) << m2) | (vector[cid+1] & masks[m2]);
	  }
    //mnstr_printf(GDKout,"#getBitVector %ld i "BUNFMT" bits %d value %d cid %o\n",(long)vector,i,bits,value,vector[cid]);
	return value;
}

int
tstBitVector(BitVector m, BUN idx, int width)
{
	return getBitVector(m,idx,width) > 0;
}


/* Unit testing
static void
printVector(BitVector v, BUN cnt, int width)
{
	int i;
	for ( i = 0; i< cnt; i++)
		printf("[%d] %d\n",i, getBitVector(v,i,width));
}

int main(int argc, char **argv)
{
	int cnt, width,i,j,k;
	BitVector vector;

	if( argc != 3){
		printf("use:%s <cnt> <width>\n",argv[0]);
		exit(-1);
	}
	cnt = atoi(argv[1]);
	width= atoi(argv[2]);
	printf("testing bitvectors %d %d %d\n",cnt,width, BITS);
	initBitMasks();
	vector = newBitVector(cnt,width);

	printVector(vector,cnt,width);
	for(i = 0; i< cnt; i++)
		setBitVector(vector,i,width, i);
	printVector(vector,cnt,width);
	for(i = 0; i < cnt; i++){
		j = rand() % width;
		setBitVector(vector,i,width, j );
		if( j != (k = getBitVector(vector,i,width)) )
			printf("mismatch[%d] %d %d\n",i,j,k);
	}
}
*/
