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

#include "gdk.h"
#include "gdk_tracer.h"
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

#define MOSAIC_RAW		raw			// no compression at all
#define MOSAIC_RLE      runlength	// use run-length encoding
#define MOSAIC_DICT256	dict256		// dict256 global dictionary encoding
#define MOSAIC_DICT     dict		// variable global dictionary encoding
#define MOSAIC_DELTA	delta		// use delta encoding
#define MOSAIC_LINEAR 	linear		// use an encoding for a linear sequence
#define MOSAIC_FRAME	frame		// delta dictionary for frame of reference value
#define MOSAIC_PREFIX	prefix		// prefix/postfix bitwise compression

typedef enum {
	MOSAIC_RAW = 0,
	MOSAIC_RLE,
	MOSAIC_DICT256,
	MOSAIC_DICT,
	MOSAIC_DELTA,
	MOSAIC_LINEAR,
	MOSAIC_FRAME,
	MOSAIC_PREFIX,
	nr_methods // Must be the last.
} EMethod;

#define MOSAIC_METHODS nr_methods

typedef struct {
	const uint16_t	bit;
	const char*		name;
} Method;

/* The header is reserved for meta information, e.g. oid indices.
 * The block header encodes the information needed for the chunk decompressor
 */
typedef Heap *mosaic;	// compressed data is stored on a heap.

/* For compression MOSmethods based on value differences, we need the storage type */
#define Deltabte uint8_t
#define Deltasht uint16_t
#define Deltaint uint32_t
#define Deltalng uint64_t
#define Deltaoid uint64_t
#ifdef HAVE_HGE
#define Deltahge uhge
#endif

#define DeltaTpe(TPE) CONCAT2(Delta, TPE)

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

#define IPTpe(TPE) CONCAT2(IP, TPE)

#define METHOD_NOT_AVAILABLE -1
typedef struct MOSAICHEADER{
	int version;		// to recognize the underlying implementation used.
	int top; 			// TODO: rename to e.g. nblocks because it is the number of blocks
	flt ratio;			// Compresion ratio achieved
	/* Collect compression statistics for the particular task
	 * A value of METHOD_NOT_AVAILABLE in blks or elms indicates that the corresponding method wasn't considered as candidate.
	 */
	lng blks[MOSAIC_METHODS]; // number of blks per method.
	lng elms[MOSAIC_METHODS]; // number of compressed values in all blocks for this method.
	/* The variable sized 'dict' and capped 'dict256' dictionary compression methods are the only
	 * compression methods that have global properties which are stored in mosaic global header.
	 */
	bte bits_dict;
	BUN pos_dict;
	BUN length_dict;
	bte bits_dict256;
	BUN pos_dict256;
	BUN length_dict256;
} * MosaicHdr;

/* Each compressed block comes with a small header.
 * It contains the compression type and the number of elements it covers
 */

#define CNT_BITS 24
//#define MOSAICMAXCNT 10000
#define MOSAICMAXCNT ((1 << CNT_BITS) - 1)

typedef struct MOSAICBLK {
	unsigned int tag:((sizeof(unsigned int) * CHAR_BIT) - CNT_BITS), cnt:CNT_BITS;
} MosaicBlkRec, *MosaicBlk;


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

typedef struct _GlobalDictionaryInfo GlobalDictionaryInfo;

/* The (de) compression task descriptor */
typedef struct MOSTASK{
	int type;		// one of the permissible compression types
	sht mask;		// In this bit field each set bit corresponds to a specific applied compression technique.

	MosaicHdr hdr;	// header block with index/synopsis information
	MosaicBlk blk;	// current block header in scan
	GlobalDictionaryInfo* dict_info;
	GlobalDictionaryInfo* dict256_info;
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

} MOStask;

/* The compressor is built around a two phase process
 * where in the first phase we collect the structure of
 * the final mosaic file
 */
typedef struct _MosaicEstimation {
	BUN previous_compressed_size;
	BUN compressed_size;
	BUN uncompressed_size;
	MosaicBlkRec compression_strategy;
	bool is_applicable;
	bool must_be_merged_with_previous;
	BUN nr_dict_encoded_elements;
	BUN nr_dict_encoded_blocks;
	BUN* dict_limit;
	BUN* dict256_limit;
	BUN nr_dict256_encoded_elements;
	BUN nr_dict256_encoded_blocks;
	BUN* max_compression_length;
} MosaicEstimation;



typedef struct _MosaicLayout {
	BAT *bsn;
	BAT *tech;
	BAT *count;
	BAT *input;
	BAT *output;
	BAT *properties;
} MosaicLayout;

#define GET_PADDING(blk, METHOD, TPE) (((MOSBlockHeaderTpe(METHOD, TPE)*) (blk))->padding)

#define ALIGN_BLOCK_HEADER(task, METHOD, TPE)\
{\
	if (task->padding) {\
		ALIGNMENT_HELPER_TPE(METHOD, TPE) dummy;\
		uintptr_t alignment = (BUN) ((uintptr_t) (void*) &dummy.b - (uintptr_t) (void*) &dummy.a);\
		uintptr_t SZ = (uintptr_t) (char*)((task)->blk);\
		BUN padding = (BUN) ( ( (SZ) % alignment ) ? ( alignment - ((SZ)%alignment) ) : 0 );\
		assert (padding < CHAR_MAX);\
		(task)->blk = (MosaicBlk) ((char*)((task)->blk) + padding);\
		*task->padding = (char) padding;\
		/*printf("padding: %ld\n", padding);*/\
	}\
	task->padding = &GET_PADDING(task->blk, METHOD, TPE);\
	*task->padding = 0;\
}

#define ASSERT_ALIGNMENT_BLOCK_HEADER(ptr, METHOD, TPE)\
{\
	ALIGNMENT_HELPER_TPE(METHOD, TPE) dummy;\
	uintptr_t alignment = ((uintptr_t) (void*) &dummy.b - (uintptr_t) (void*) &dummy.a);\
	assert((uintptr_t) (void*) (ptr) % alignment == 0);\
	(void) alignment;\
}

#include "json.h"

#define LAYOUT_INSERT(SET_VALUES) \
{\
	lng bsn = 0;\
\
	const char* tech = str_nil;\
	lng count = lng_nil;\
	lng input = lng_nil;\
	lng output = lng_nil;\
	const char*  properties = str_nil;\
\
	SET_VALUES;\
\
	if (!strNil(properties)) {\
		char buffer[LAYOUT_BUFFER_SIZE];\
		char* pbuffer = &buffer[0];\
		str msg = JSONstr2json(&pbuffer, (char**) &properties);\
		assert (msg == MAL_SUCCEED);\
		if (msg != MAL_SUCCEED) return msg;\
	}\
\
	if (\
		(layout->bsn		&& BUNappend(layout->bsn		, &bsn,			false) != GDK_SUCCEED) ||\
		(layout->tech		&& BUNappend(layout->tech		, tech,			false) != GDK_SUCCEED) ||\
		(layout->count		&& BUNappend(layout->count		, &count,		false) != GDK_SUCCEED) ||\
		(layout->input		&& BUNappend(layout->input		, &input,		false) != GDK_SUCCEED) ||\
		(layout->properties && BUNappend(layout->properties	, properties,	false) != GDK_SUCCEED) ||\
		(layout->output		&& BUNappend(layout->output		, &output,		false) != GDK_SUCCEED)\
	) {\
		throw(MAL, __func__, MAL_MALLOC_FAIL);\
	}\
}

mal_export const Method MOSmethods[];
mal_export bit MOSisTypeAllowed(char compression, BAT* b);
mal_export str MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSdecompress(bat* ret, const bat* bid);
mal_export str MOScompressInternal(BAT* bsrc, const char* compressions);
mal_export str MOSselect1	(bat *ret, const bat *bid					, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
mal_export str MOSselect2	(bat *ret, const bat *bid, const bat *cid	, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
mal_export str MOSselect1nil(bat *ret, const bat *bid					, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti, const bit *unknown);
mal_export str MOSselect2nil(bat *ret, const bat *bid, const bat *cid	, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti, const bit *unknown);
mal_export str MOSthetaselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSprojection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MOSlayout(BAT *b, BAT *bbsn, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export str MOSAnalysis(BAT *b, BAT *btech, BAT *blayout, BAT *output, BAT *factor, BAT *compress, BAT *decompress, str compressions);

void MOSupdateHeader(MOStask* task);
void MOSinitHeader(MOStask* task);
void MOSinitializeScan(MOStask* task, BAT* b);

#endif /* _MOSLIST_H */
