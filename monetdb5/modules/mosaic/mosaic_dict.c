/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB b.V.
 */


/*
 * authors Martin Kersten, Aris Koning
 * Global info encoding
 * Index value zero is not used to easy detection of filler values
 * The info index size is derived from the number of entries covered.
 * It leads to a compact n-bit representation.
 * Floating points are not expected to be replicated 
 * A limit of 256 elements is currently assumed.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_dict.h"
#include "mosaic_private.h"

bool MOStypes_dict(BAT* b) {
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

#define MOSadvance_DEF(TPE)\
MOSadvance_SIGNATURE(dict, TPE) advance_dictionary(dict, TPE)

MOSadvance_DEF(bte)
MOSadvance_DEF(sht)
MOSadvance_DEF(int)
MOSadvance_DEF(lng)
MOSadvance_DEF(flt)
MOSadvance_DEF(dbl)
#ifdef HAVE_HGE
MOSadvance_DEF(hge)
#endif

#define MOSgetDictFreq(DICTIONARY, KEY) ((BUN*)(((char*) DICTIONARY) + wordaligned(sizeof(DICTIONARY), BUN))[KEY])

#define CONDITIONAL_INSERT_dict(INFO, VAL, TPE)	(true)

#define DictionaryClass(TPE) \
find_value_DEF(TPE)\
merge_delta_Into_dictionary_DEF(TPE)\
decompress_dictionary_DEF(TPE)

DictionaryClass(bte)
DictionaryClass(sht)
DictionaryClass(int)
DictionaryClass(lng)
DictionaryClass(flt)
DictionaryClass(dbl)
#ifdef HAVE_HGE
DictionaryClass(hge)
#endif

void
MOSlayout_dict_hdr(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	unsigned int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];
	(void) boutput;

	BUN dictsize = GET_COUNT(task->dict_info);

	for(i=0; i< dictsize; i++){
		snprintf(buf, BUFSIZ,"dict[%u]",i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			// BUNappend(boutput, MOSgetDictFreq(dict_hdr, i), false) != GDK_SUCCEED ||
			BUNappend(bproperties, bufv, false) != GDK_SUCCEED)
		return;
	}
}

void
MOSlayout_dict(MOStask* task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * task->hdr->bits_dict)/8 + (((cnt * task->hdr->bits_dict) %8) != 0);
	if( BUNappend(btech, "dict blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

#define NAME dict
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
#undef NAME

#define MOSpostEstimate_DEF(TPE)\
MOSpostEstimate_SIGNATURE(dict, TPE)\
{\
	merge_delta_Into_dictionary_##TPE( task->dict_info);\
}

MOSpostEstimate_DEF(bte)
MOSpostEstimate_DEF(sht)
MOSpostEstimate_DEF(int)
MOSpostEstimate_DEF(lng)
MOSpostEstimate_DEF(flt)
MOSpostEstimate_DEF(dbl)
#ifdef HAVE_HGE
MOSpostEstimate_DEF(hge)
#endif

#define MOSdecompress_DEF(TPE) \
MOSdecompress_SIGNATURE(dict, TPE)\
{\
	DICTdecompress(dict, TPE);\
}

MOSdecompress_DEF(bte)
MOSdecompress_DEF(sht)
MOSdecompress_DEF(int)
MOSdecompress_DEF(lng)
MOSdecompress_DEF(flt)
MOSdecompress_DEF(dbl)
#ifdef HAVE_HGE
MOSdecompress_DEF(hge)
#endif

#define scan_loop_dict(TPE, CI_NEXT, TEST) \
    scan_loop_dictionary(dict, TPE, CI_NEXT, TEST)

MOSselect_DEF(dict, bte)
MOSselect_DEF(dict, sht)
MOSselect_DEF(dict, int)
MOSselect_DEF(dict, lng)
MOSselect_DEF(dict, flt)
MOSselect_DEF(dict, dbl)
#ifdef HAVE_HGE
MOSselect_DEF(dict, hge)
#endif

#define projection_loop_dict(TPE, CI_NEXT) \
    projection_loop_dictionary(dict, TPE, CI_NEXT)

MOSprojection_DEF(dict, bte)
MOSprojection_DEF(dict, sht)
MOSprojection_DEF(dict, int)
MOSprojection_DEF(dict, lng)
MOSprojection_DEF(dict, flt)
MOSprojection_DEF(dict, dbl)
#ifdef HAVE_HGE
MOSprojection_DEF(dict, hge)
#endif

#define outer_loop_dict(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
    outer_loop_dictionary(HAS_NIL, NIL_MATCHES, dict, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT)

MOSjoin_COUI_DEF(dict, bte)
MOSjoin_COUI_DEF(dict, sht)
MOSjoin_COUI_DEF(dict, int)
MOSjoin_COUI_DEF(dict, lng)
MOSjoin_COUI_DEF(dict, flt)
MOSjoin_COUI_DEF(dict, dbl)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(dict, hge)
#endif
