/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */


/*
 * authors Martin Kersten, A. Koning
 * Global dictionary encoding
 * Index value zero is not used to easy detection of filler values
 * The dictionary index size is derived from the number of entries covered.
 * It leads to a compact n-bit representation.
 * Floating points are not expected to be replicated.
 * A limit of 256 elements is currently assumed.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_dict256.h"
#include "mosaic_private.h"
#include "group.h"

bool MOStypes_dict256(BAT* b) {
	switch (b->ttype){
	case TYPE_bit: return true; // Will be mapped to bte
	case TYPE_bte: return true;
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
	case TYPE_flt: return true;
	case TYPE_dbl: return true;
#ifdef HAVE_HGE
	case TYPE_hge: return true;
#endif
	default:
		if (b->ttype == TYPE_date) {return true;} // Will be mapped to int
		if (b->ttype == TYPE_daytime) {return true;} // Will be mapped to lng
		if (b->ttype == TYPE_timestamp) {return true;} // Will be mapped to lng
	}

	return false;
}

#define CAPPEDDICT 256

// Create a larger dict256 buffer then we allow for in the mosaic header first
// Store the most frequent ones in the compressed heap header directly based on estimated savings
// Improve by using binary search rather then linear scan
#define TMPDICT 16*CAPPEDDICT

typedef union{
	bte valbte[TMPDICT];
	sht valsht[TMPDICT];
	int valint[TMPDICT];
	lng vallng[TMPDICT];
	flt valflt[TMPDICT];
	dbl valdbl[TMPDICT];
#ifdef HAVE_HGE
	hge valhge[TMPDICT];
#endif
} _DictionaryData;

typedef struct _CappedParameters_t {
	MosaicBlkRec base;
} MosaicBlkHeader_dict256_t;

void
MOSlayout_dict256_hdr(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	unsigned int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];
	(void) boutput;

	BUN dictsize = GET_COUNT(task->dict256_info);

	for(i=0; i< dictsize; i++){
		snprintf(buf, BUFSIZ,"dict256[%u]",i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			// BUNappend(boutput, MOSgetDictFreq(dict_hdr, i), false) != GDK_SUCCEED ||
			BUNappend(bproperties, bufv, false) != GDK_SUCCEED)
		return;
	}
}

void
MOSlayout_dict256(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	BUN cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * GET_FINAL_BITS(task, dict256))/8 + (((cnt * GET_FINAL_BITS(task, dict256)) %8) != 0);
	if( BUNappend(btech, "dict256 blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

// TODO: factor this out and create compile time safety guards for NAME and TPE macro.
#define NAME dict256
#define MOS_CUT_OFF_SIZE CAPPEDDICT
#include "mosaic_dictionary_prepare_context_impl.h"
#define TPE bte
#include "mosaic_dictionary_impl.h"
#undef TPE
#define TPE sht
#include "mosaic_dictionary_impl.h"
#undef TPE
#define TPE int
#include "mosaic_dictionary_impl.h"
#undef TPE
#define TPE lng
#include "mosaic_dictionary_impl.h"
#undef TPE
#define TPE flt
#include "mosaic_dictionary_impl.h"
#undef TPE
#define TPE dbl
#include "mosaic_dictionary_impl.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_dictionary_impl.h"
#undef TPE
#endif
#undef MOS_CUT_OFF_SIZE
#undef NAME

#define scan_loop_dict256(TPE, CI_NEXT, TEST) \
    scan_loop_dictionary(dict256, TPE, CI_NEXT, TEST)

MOSselect_DEF(dict256, bte)
MOSselect_DEF(dict256, sht)
MOSselect_DEF(dict256, int)
MOSselect_DEF(dict256, lng)
MOSselect_DEF(dict256, flt)
MOSselect_DEF(dict256, dbl)
#ifdef HAVE_HGE
MOSselect_DEF(dict256, hge)
#endif

#define projection_loop_dict256(TPE, CI_NEXT) \
    projection_loop_dictionary(dict256, TPE, CI_NEXT)

MOSprojection_DEF(dict256, bte)
MOSprojection_DEF(dict256, sht)
MOSprojection_DEF(dict256, int)
MOSprojection_DEF(dict256, lng)
MOSprojection_DEF(dict256, flt)
MOSprojection_DEF(dict256, dbl)
#ifdef HAVE_HGE
MOSprojection_DEF(dict256, hge)
#endif

#define outer_loop_dict256(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
    outer_loop_dictionary(HAS_NIL, NIL_MATCHES, dict256, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT)

MOSjoin_COUI_DEF(dict256, bte)
MOSjoin_COUI_DEF(dict256, sht)
MOSjoin_COUI_DEF(dict256, int)
MOSjoin_COUI_DEF(dict256, lng)
MOSjoin_COUI_DEF(dict256, flt)
MOSjoin_COUI_DEF(dict256, dbl)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(dict256, hge)
#endif
