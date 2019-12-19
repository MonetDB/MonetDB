/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/* authors M Kersten, Aris. Koning
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

#include <limits.h>

//#define _DEBUG_MOSAIC_
//#define _DEBUG_PREFIX_

/* do not invest in compressing BATs smaller than this */
#define MOSAIC_THRESHOLD 1

/* The compressor kinds currently hardwired */
#define MOSAIC_METHODS	9
#define MOSAIC_RAW	0		// no compression at all
#define MOSAIC_RLE      1		// use run-length encoding
#define MOSAIC_CAPPED   2		// capped global dictionary encoding
#define MOSAIC_VAR      3		// variable global dictionary encoding
#define MOSAIC_DELTA	4		// use delta encoding
#define MOSAIC_LINEAR 	5		// use an encoding for a linear sequence
#define MOSAIC_FRAME	6		// delta dictionary for frame of reference value
#define MOSAIC_PREFIX	7		// prefix/postfix bitwise compression
#define MOSAIC_EOL	8		// marker for the last block

#define METHOD_NOT_AVAILABLE -1

/*
 * The header is reserved for meta information, e.g. oid indices.
 * The block header encodes the information needed for the chunk decompressor
 */
typedef Heap *mosaic;	// compressed data is stored on a heap.


#define IS_NIL(TPE, VAL) is_##TPE##_nil(VAL)
#define ARE_EQUAL(v, w, HAS_NIL, TPE) ((v == w || (HAS_NIL && IS_NIL(TPE, v) && IS_NIL(TPE, w)) ) )

/* For compression techniques based on value differences, we need the storage type */
#define Deltabte uint8_t
#define Deltasht uint16_t
#define Deltaint uint32_t
#define Deltalng uint64_t
#define Deltaoid uint64_t
#ifdef HAVE_HGE
#define Deltahge uhge
#endif

#define DeltaTpe(TPE) Delta##TPE

/* Use standard unsigned integer operations 
 * to avoid undefined behavior due to overflow's
 */
#define GET_DELTA(TPE, x, y)  ((DeltaTpe(TPE)) x - (DeltaTpe(TPE)) y)
#define ADD_DELTA(TPE, x, d)  (TPE) ((DeltaTpe(TPE)) x + (DeltaTpe(TPE)) d)

// Storage types for safe Integer Promotion for the bitwise operations in getSuffixMask
#define IPbte uint32_t
#define IPsht uint32_t
#define IPint uint32_t
#define IPlng uint64_t
#define IPoid uint64_t
#define IPflt uint64_t
#define IPdbl uint64_t
#ifdef HAVE_HGE
#define IPhge uhge
#endif

#define IPTpe(TPE) IP##TPE

typedef struct MOSAICHEADER{
	int version;		// to recognize the underlying implementation used.
	int top; 			// TODO: rename to e.g. nblocks because it is the number of blocks
	flt ratio;			// Compresion ratio achieved
	/* Collect compression statistics for the particular task
	 * A value of METHOD_NOT_AVAILABLE in blks or elms indicates that the corresponding method wasn't considered as candidate.
	 */
	lng blks[MOSAIC_METHODS]; // number of blks per method.
	lng elms[MOSAIC_METHODS]; // number of compressed values in all blocks for this method.
	/* The var(iable sized) and capped dictionary compression techniques are the only
	 * compression techniques that have global properties which are stored in mosaic global header.
	 */
	bte bits_var;
	BUN pos_var;
	BUN length_var;
	bte bits_capped;
	BUN pos_capped;
	BUN length_capped;
} * MosaicHdr;

/* Each compressed block comes with a small header.
 * It contains the compression type and the number of elements it covers
 */

#define CNT_BITS 24
#define MOSAICMAXCNT ((1 << CNT_BITS) - 1)

typedef struct MOSAICBLK {
	unsigned int tag:((sizeof(unsigned int) * CHAR_BIT) - CNT_BITS), cnt:CNT_BITS;
} MosaicBlkRec, *MosaicBlk;

typedef struct {
	MosaicBlkRec rec;
	char padding;
} MosaicBlkHdrGeneric;

#define MOSgetTag(Blk) (Blk->tag)
#define MOSsetTag(Blk,Tag)  (Blk)->tag = Tag
#define MOSsetCnt(Blk,I) (assert(I <= MOSAICMAXCNT), (Blk)->cnt = (unsigned int)(I))
#define MOSgetCnt(Blk) (BUN)((Blk)->cnt)
#define MOSincCnt(Blk,I) (assert((Blk)->cnt +I <= MOSAICMAXCNT), (Blk)->cnt += (unsigned int)(I))

/* The start of the encoding withing a Mosaic block */
#define MOScodevector(Task) (((char*) (Task)->blk)+ MosaicBlkSize)

/* Memory word alignement is type and platform dependent.
 * We use an encoding that fits the column type requirements
 */
#define wordaligned(SZ,TYPE) \
	 ((SZ) +  ((SZ) % sizeof(TYPE)? sizeof(TYPE) - ((SZ)%sizeof(TYPE)) : 0))

// alignment is focused on mosaichdr size
#define MosaicHdrSize  wordaligned(sizeof(struct MOSAICHEADER),sizeof(struct MOSAICBLK))
#define MosaicBlkSize  sizeof(MosaicBlkRec)

#define getSrc(TPE, TASK) (((TPE*)TASK->src) + TASK->start)

typedef struct _GlobalCappedInfo GlobalCappedInfo;
typedef struct _GlobalVarInfo GlobalVarInfo;

/* The (de) compression task descriptor */
typedef struct MOSTASK{
	int type;		// one of the permissible compression types
	int filter[MOSAIC_METHODS];// algorithmic (de)compression mix

	MosaicHdr hdr;	// header block with index/synopsis information
	MosaicBlk blk;	// current block header in scan
	GlobalVarInfo* var_info;
	GlobalCappedInfo* capped_info;
	char 	 *dst;		// write pointer into current compressed blocks
	oid 	start;		// oid of first element in current blk
	oid		stop;		// last oid of range to be scanned

	BAT *bsrc;		// target column to extended with compressed heap
	char *src;		// read pointer into source
	char* padding; // padding at the end of a block necessary to correctly align the next block during compression.

	lng timer;		// compression time

	oid *lb, *rb;	// Collected oids from operations
	oid *cl;		// candidate admin
	struct canditer* ci; // candidate list iterator.
	lng	n;			// element count in candidate list
	BUN cnt;		// elements in result set

	BAT *lbat, *rbat; // for the joins, where we dont know their size upfront

} *MOStask;

/* The compressor is built around a two phase process
 * where in the first phase we collect the structure of
 * the final mosaic file
 */
typedef struct _MosaicEstimation {
	BUN compressed_size;
	BUN uncompressed_size;
	MosaicBlkRec compression_strategy;
	bool is_applicable;
	bool must_be_merged_with_previous;
	BUN nr_var_encoded_elements;
	BUN nr_var_encoded_blocks;
	BUN nr_capped_encoded_elements;
	BUN nr_capped_encoded_blocks;
	BUN* max_compression_length;
} MosaicEstimation;

#define GET_PADDING(blk, NAME, TPE) (((MOSBlockHeaderTpe(NAME, TPE)*) (blk))->base.padding)

#define ALIGN_BLOCK_HEADER(task, NAME, TPE)\
{\
	if (task->padding) {\
		ALIGNMENT_HELPER_TPE(NAME, TPE) dummy;\
		uintptr_t alignment = (BUN) ((uintptr_t) (void*) &dummy.b - (uintptr_t) (void*) &dummy.a);\
		uintptr_t SZ = (uintptr_t) (char*)((task)->blk);\
		BUN padding = (BUN) ( ( (SZ) % alignment ) ? ( alignment - ((SZ)%alignment) ) : 0 );\
		assert (padding < CHAR_MAX);\
		(task)->blk = (MosaicBlk) ((char*)((task)->blk) + padding);\
		*task->padding = (char) padding;\
		/*printf("padding: %ld\n", padding);*/\
	}\
	task->padding = &GET_PADDING(task->blk, NAME, TPE);\
	*task->padding = 0;\
}

#define ASSERT_ALIGNMENT_BLOCK_HEADER(ptr, NAME, TPE)\
{\
	ALIGNMENT_HELPER_TPE(NAME, TPE) dummy;\
	uintptr_t alignment = ((uintptr_t) (void*) &dummy.b - (uintptr_t) (void*) &dummy.a);\
	assert((uintptr_t) (void*) (ptr) % alignment == 0);\
	(void) alignment;\
}

mal_export char *MOSfiltername[];
mal_export bool MOSisTypeAllowed(int compression, BAT* b);
mal_export str MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSdecompress(bat* ret, const bat* bid);
mal_export str MOScompressInternal(BAT* bsrc, const char* compressions);
mal_export str MOSselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSthetaselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSprojection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSlayout(BAT *b, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export str MOSsliceInternal(bat *slices, BUN size, BAT *b);
mal_export str MOSAnalysis(BAT *b, BAT *btech, BAT *output, BAT *factor, BAT *compress, BAT *decompress, str compressions);
mal_export str MOSslice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void MOSblk(MosaicBlk blk);
mal_export BUN MOSlimit(void);

#endif /* _MOSLIST_H */
