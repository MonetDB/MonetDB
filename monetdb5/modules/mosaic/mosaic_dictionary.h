/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 */
#ifndef _MOSAIC_DICTIONARY_ 
#define _MOSAIC_DICTIONARY_ 

#include "gdk.h"
#include "gdk_bitvector.h"
#include "mal_exception.h"

 /*TODO: assuming (for now) that bats have nils during compression*/
static const bool nil = true;

#define calculateBits(RES, COUNT)\
{\
	unsigned char bits = 0;\
	while ((COUNT) >> bits) {\
		bits++;\
	}\
	(RES) = bits;\
}

typedef struct _EstimationParameters {
	BUN count;
	unsigned char bits;
	BUN delta_count;
	unsigned char bits_extended; // number of bits required to index the info after the delta would have been merged.
} EstimationParameters;

typedef struct _GlobalDictionaryInfo {
	BAT* dict;
	/* admin: the admin column is aligned with the full dictionary column dict.
	 * Each entry in the admin column represents a tuple consisting of
	 * the frequency of the corresponding dictionary value in the current estimation run
	 * and a bit which represents the final choice to include the corresponding dictionary value in the final dictionary. */
	BAT* admin;
	BAT* selection_vector;
	BAT* increments;
	BUN previous_start;
	BUN previous_limit;
	BUN count;
	EstimationParameters parameters;
} GlobalDictionaryInfo;


#define GET_COUNT(INFO)				((INFO)->count)
#define GET_DELTA_COUNT(INFO)		((INFO)->parameters.delta_count)
#define GET_BITS(INFO)				((INFO)->parameters.bits)
#define GET_BITS_EXTENDED(INFO)		((INFO)->parameters.bits_extended)
#define GetTypeWidth(INFO)			((INFO)->dict->twidth)
#define GetSizeInBytes(INFO)		(GET_COUNT(INFO) * GetTypeWidth(INFO))

// task dependent macro's
#define GET_FINAL_DICT(task, METHOD, TPE) (((TPE*) (task)->bsrc->tvmosaic->base) + (task)->hdr->CONCAT2(pos_, METHOD))
#define GET_FINAL_BITS(task, METHOD) ((task)->hdr->CONCAT2(bits_, METHOD))
#define GET_FINAL_DICT_COUNT(task, METHOD) ((task)->hdr->CONCAT2(length_, METHOD))

#define MosaicBlkHeader_DEF_dictionary(METHOD, TPE)\
typedef struct {\
	MosaicBlkRec rec;\
	char padding;\
	BitVectorChunk bitvector; /*First chunk of bitvector to force correct alignment.*/\
} MOSBlockHeader_##METHOD##_##TPE;

#define DICTBlockHeaderTpe(METHOD, TPE) MOSBlockHeader_##METHOD##_##TPE

// MOStask object dependent macro's

#define MOScodevectorDict(task, METHOD, TPE) ((BitVector) &((DICTBlockHeaderTpe(METHOD, TPE)*) (task)->blk)->bitvector)

#define outer_loop_dictionary(HAS_NIL, NIL_MATCHES, METHOD, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
	bte bits		= GET_FINAL_BITS(task, METHOD);\
	TPE* dict		= GET_FINAL_DICT(task, METHOD, TPE);\
	BitVector base	= MOScodevectorDict(task, METHOD, TPE);\
    for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (lo - first);\
		BitVectorChunk j= getBitVector(base,i,bits);\
        TPE lval = dict[j];\
		if (HAS_NIL && !NIL_MATCHES) {\
			if (IS_NIL(TPE, lval)) {continue;};\
		}\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
}

#endif /* _MOSAIC_DICTIONARY_  */
