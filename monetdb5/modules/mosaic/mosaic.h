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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/* (c) M Kersten
 * The multi-purpose compressor infrastructure
 */
#ifndef _MOSLIST_H
#define _MOSLIST_H
#include "mal_interpreter.h"
#include "mal_exception.h"
#include "mal_function.h"

#include "mtime.h"
#include "math.h"
#include "opt_prelude.h"
#include "algebra.h"

//#define _DEBUG_MOSAIC_
//#define _DEBUG_PREFIX_

/* do not invest in compressing BATs smaller than this */
#define MOSAIC_THRESHOLD 1

/* The compressor kinds currently hardwired */
#define MOSAIC_METHODS	9
#define MOSAIC_RAW     0		// no compression at all
#define MOSAIC_RLE      1		// use run-length encoding
#define MOSAIC_DICT     2		// local dictionary encoding
#define MOSAIC_DELTA	3		// use delta encoding
#define MOSAIC_LINEAR 	4		// use an encoding for a linear sequence
#define MOSAIC_FRAME	5		// delta dictionary for frame of reference value
#define MOSAIC_PREFIX	6		// prefix/postfix bitwise compression
#define MOSAIC_CALENDAR	7		// compression of temporal elements
#define MOSAIC_EOL		8		// marker for the last block

//Compression should have a significant reduction to apply.
#define COMPRESS_THRESHOLD 50   //percent
#define MOSAICINDEX 8  //> 2 elements

/*
 * The header is reserved for meta information, e.g. oid indices.
 * The block header encodes the information needed for the chunk decompressor
 */
typedef Heap *mosaic;	// compressed data is stored on a heap.

typedef struct MOSAICHEADER{
	int version;
	union{
		bte sumbte;
		bit sumbit;
		sht sumsht;
		int sumint;
		oid sumoid;
		lng sumlng;
#ifdef HAVE_HGE
		hge sumhge;
#endif
		flt sumflt;
		dbl sumdbl;
	} checksum, checksum2;	// for validity checks
	int top;
	// skip list for direct OID-based access
	oid oidbase[MOSAICINDEX];	// to speedup localization
	BUN offset[MOSAICINDEX];
	// both dictionary and framebased compression require a global dictionary of frequent values
	// Their size is purposely topped 
	bte mask, bits, framebits;	// global compression type properties
	int dictsize;		// used by dictionary compression, it is a small table
	int framesize;		// used by frame compression, it is a small table
	union{
		bte valbte[256];
		sht valsht[256];
		int valint[256];
		lng vallng[256];
		oid valoid[256];
		flt valflt[256];
		dbl valdbl[256];
#ifdef HAVE_HGE
		hge valhge[256];
#endif
	}dict;
	lng dictfreq[256];// keep track on their use
	union{
		bte valbte[256];
		sht valsht[256];
		int valint[256];
		lng vallng[256];
		oid valoid[256];
		flt valflt[256];
		dbl valdbl[256];
#ifdef HAVE_HGE
		hge valhge[256];
#endif
	}frame;
	// collect compression statistics for the particular task
	flt ratio;	//compresion ratio
	lng blks[MOSAIC_METHODS];	
	lng elms[MOSAIC_METHODS];	
	lng framefreq[256];
} * MosaicHdr;

// bit stuffed header block, currently 4 bytes wide and chunks should be 4-byte aligned
#define MOSAICMAXCNT (1<<23)

typedef struct MOSAICBLK{
	unsigned int tag:8, cnt:24;
} MosaicBlkRec, *MosaicBlk;

#define MOSgetTag(Blk) (Blk->tag)
#define MOSsetTag(Blk,Tag)  (Blk)->tag = Tag
#define MOSsetCnt(Blk,I) (assert(I < MOSAICMAXCNT), (Blk)->cnt = (unsigned int)(I))
#define MOSgetCnt(Blk) (BUN)((Blk)->cnt)
#define MOSincCnt(Blk,I) (assert((Blk)->cnt +I < MOSAICMAXCNT), (Blk)->cnt+= (unsigned int)(I))

/* The start of the encoding withing a Mosaic block */
#define MOScodevector(Task) (((char*) (Task)->blk)+ MosaicBlkSize)

/* Memory word alignement is type and platform dependent.
 * We use an encoding that fits the column type requirements
 */
#define wordaligned(SZ,TYPE) \
	 ((SZ) +  ((SZ) % sizeof(TYPE)? sizeof(TYPE) - ((SZ)%sizeof(TYPE)) : 0))

// alignment is focused on mosaichdr size
#define MosaicHdrSize  wordaligned(sizeof(struct MOSAICHEADER),sizeof(struct MOSAICBLK))
#define MosaicBlkSize  sizeof(MosaicBlk *)


/* The (de) compression task descriptor */
typedef struct MOSTASK{
	int type;		// one of the permissible compression types
	int filter[MOSAIC_METHODS];// algorithmic (de)compression mix

	/* collect the range being applied for the last compression call */
	BUN range[MOSAIC_METHODS]; // end of compression range
	float factor[MOSAIC_METHODS];// compression factor of last range

	MosaicHdr hdr;	// header block with index/synopsis information
	MosaicBlk blk;	// current block header in scan
	char 	 *dst;		// write pointer into current compressed blocks
	oid 	start;		// oid of first element in current blk
	oid		stop;		// last oid of range to be scanned
	flt		ratio;		// compression ratio encountered


	BAT *bsrc;		// target column to extended with compressed heap
	BUN	elm;		// number of elements left to be compress
	char *src;		// read pointer into source
	//BAT *index;		// collection of unique elements
	//BAT *freq;		// frequency of these elements

	lng timer;		// compression time

	oid *lb, *rb;	// Collected oids from operations
	oid *cl;		// candidate admin
	lng	n;			// element count in candidate list
	BUN cnt;		// elements in result set

	BAT *lbat, *rbat; // for the joins, where we dont know their size upfront

} *MOStask;

/* Run through a column to produce a compressed version */

// skip until you hit a candidate
#define MOSskipit()\
if ( task->n && task->cl ){\
	while(task->n > 0 && *task->cl < (oid) first)\
		{task->cl++; task->n--;}\
	if (task->n <= 0 || *task->cl > (oid) first )\
		continue;\
	if ( *task->cl == (oid) first ){\
		task->cl++; \
	}\
} else if (task->cl) continue;

mal_export char *MOSfiltername[];
mal_export BUN MOSblocklimit;
mal_export str MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSdecompressInternal(Client cntxt, bat *bid);
mal_export str MOSdecompressStorage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOScompressInternal(Client cntxt, bat *bid, MOStask task, int debug);
mal_export str MOSanalyse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSthetaselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSprojection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSlayout(Client cntxt, BAT *b, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export str MOSsliceInternal(Client cntxt, bat *slices, BUN size, BAT *b);
mal_export int MOSanalyseInternal(Client cntxt, int threshold, MOStask task, bat bid);
mal_export void MOSanalyseReport(Client cntxt, BAT *b, BAT *btech, BAT *output, BAT *factor, BAT *compress, BAT *decompress, str compressions);
mal_export str MOSoptimizer(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSslice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void MOSblk(MosaicBlk blk);
mal_export BUN MOSlimit(void);

#endif /* _MOSLIST_H */
