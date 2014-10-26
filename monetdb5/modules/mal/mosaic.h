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

#define MOSAIC_VERSION 20140808

/* do not invest in compressing BATs smaller than this */
#define MIN_INPUT_COUNT 1

/* The compressor kinds currently hardwired */
#define MOSAIC_METHODS	8
#define MOSAIC_NONE     0		// no compression at all
#define MOSAIC_RLE      1		// use run-length encoding
#define MOSAIC_DICT     2		// local dictionary encoding
#define MOSAIC_DELTA	3		// use delta encoding
#define MOSAIC_LINEAR 	4		// use an encoding for a linear sequence
#define MOSAIC_FRAME	5		// delta dictionary for frame of reference value
#define MOSAIC_PREFIX	6		// prefix/postfix bitwise compression
#define MOSAIC_EOL		7		// marker for the last block

//Compression should have a significant reduction to apply.
#define COMPRESS_THRESHOLD 50   //percent

/*
 * The header is reserved for meta information, e.g. oid indices.
 * The block header encodes the information needed for the chunk decompressor
 */
#define MOSAICINDEX 4  //> 2 elements
typedef struct MOSAICHEADER{
	int version;
	// collect compression statistics for the particular task
	lng blks[MOSAIC_METHODS];	
	lng elms[MOSAIC_METHODS];	
	flt factor;
	int top;
	oid oidbase[MOSAICINDEX];
	BUN offset[MOSAICINDEX];
	bte mask, bits, framebits;
	int dictsize;
	int framesize;
#ifdef HAVE_HGE
	hge dict[256];
	hge frame[256];
#else
	lng dict[256];
	lng frame[256];
#endif
} * MosaicHdr;

// bit stuffed header block, currently 4 bytes wide
#define MOSshift 24
typedef int *MosaicBlk;

#define MOSgetTag(Blk) (*(Blk)>>MOSshift)
#define MOSsetTag(Blk,Tag)  *Blk = (Tag) <<MOSshift
#define MOSsetCnt(Blk,I) *Blk = I & ~(0377<<MOSshift)
#define MOSgetCnt(Blk) (BUN)(*(Blk) & ~(0377<<MOSshift))
#define MOSincCnt(Blk,I) *(Blk)= *(Blk)+I

/* Memory word alignement is type and platform dependent.
 * We use an encoding that fits the column type requirements
 */
#define wordaligned(SZ,TYPE) \
	 ((SZ) +  ((SZ) % sizeof(TYPE)? sizeof(TYPE) - ((SZ)%sizeof(TYPE)) : 0))

#define MosaicHdrSize  wordaligned(sizeof(struct MOSAICHEADER),lng)
#define MosaicBlkSize  wordaligned(sizeof(lng),lng)


typedef struct MOSTASK{
	int type;		// one of the permissible compression types
	int filter[MOSAIC_METHODS];// algorithmic mix

	MosaicHdr hdr;	// header block with index/synopsis information
	MosaicBlk blk;	// current block header in scan
	oid start;		// oid of first element in current blk
	oid stop;		// last oid of range to be scanned
	flt factor;

	char *dst;		// write pointer into current compressed blocks

	BAT *b;			// source column
	BUN	elm;		// (sample) elements left to compress
	char *src;		// read pointer into source
	BAT *index;		// collection of unique elements
	BAT *freq;		// frequency of these elements

	lng  xsize,size;// original and compressed size
	lng timer;		// compression time
	void *min, *max;// space for zones indices

	oid *lb, *rb;	// Collected oids from operations
	oid *cl;		// candidate admin
	lng	n;			// element count in candidate list

	BAT *lbat, *rbat; // for the joins, where we dont know their size upfront

} *MOStask;

/* Run through a column to produce a compressed version */

#ifdef _MSC_VER
#define nextafter   _nextafter
float nextafterf(float x, float y);
#endif

#define PREVVALUEbit(x) ((x) - 1)
#define PREVVALUEbte(x) ((x) - 1)
#define PREVVALUEsht(x) ((x) - 1)
#define PREVVALUEint(x) ((x) - 1)
#define PREVVALUElng(x) ((x) - 1)
#define PREVVALUEwrd(x) ((x) - 1)
#define PREVVALUEoid(x) ((x) - 1)
#ifdef HAVE_HGE
#define PREVVALUEhge(x) ((x) - 1)
#endif
#define PREVVALUEflt(x) nextafterf((x), -GDK_flt_max)
#define PREVVALUEdbl(x) nextafter((x), -GDK_dbl_max)
#define PREVVALUEdate(x) ((x) - 1)
#define PREVVALUEdaytime(x) ((x) - 1)

#define NEXTVALUEbit(x) ((x) + 1)
#define NEXTVALUEbte(x) ((x) + 1)
#define NEXTVALUEsht(x) ((x) + 1)
#define NEXTVALUEint(x) ((x) + 1)
#define NEXTVALUElng(x) ((x) + 1)
#define NEXTVALUEwrd(x) ((x) + 1)
#define NEXTVALUEoid(x) ((x) + 1)
#ifdef HAVE_HGE
#define NEXTVALUEhge(x) ((x) + 1)
#endif
#define NEXTVALUEflt(x) nextafterf((x), GDK_flt_max)
#define NEXTVALUEdbl(x) nextafter((x), GDK_dbl_max)
#define NEXTVALUEdate(x) ((x) + 1)
#define NEXTVALUEdaytime(x) ((x) + 1)

// skip until you hit a candidate
#define MOSskipit()\
if ( task->cl && task->n){\
	while( *task->cl < (oid) first)\
		{task->cl++; task->n--;}\
	if ( *task->cl > (oid) first  || task->n ==0)\
		continue;\
	if ( *task->cl == (oid) first ){\
		task->cl++; task->n--;\
	}\
} else if (task->cl) continue;

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mosaic_export extern __declspec(dllimport)
#else
#define mosaic_export extern __declspec(dllexport)
#endif
#else
#define mosaic_export extern
#endif

mosaic_export char *MOSfiltername[];
mosaic_export BUN MOSblocklimit;
mosaic_export str MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOScompressInternal(Client cntxt, int *ret, int *bid, MOStask task,int inplace,int flg);
mosaic_export str MOSdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOSdecompressStorage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOSdecompressInternal(Client cntxt, int *ret, int *bid,int inplace);
mosaic_export str MOSanalyse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOSsubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOSthetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOSleftfetchjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOSdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export str MOSoptimize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mosaic_export void MOSblk(MosaicBlk blk);
mosaic_export BUN MOSlimit(void);

#endif /* _MOSLIST_H */
