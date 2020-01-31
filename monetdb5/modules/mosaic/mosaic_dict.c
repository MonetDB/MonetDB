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

#define MOSgetDictFreq(DICTIONARY, KEY) ((BUN*)(((char*) DICTIONARY) + wordaligned(sizeof(DICTIONARY), BUN))[KEY])

#define CONDITIONAL_INSERT_dict(INFO, VAL, TPE)	(true)

#define METHOD dict
#define METHOD_TEMPLATES_INCLUDE "mosaic_dictionary_templates.h"
#define PREPARATION_DEFINITION
#include METHOD_TEMPLATES_INCLUDE
#undef PREPARATION_DEFINITION

#define COMPRESSION_DEFINITION
#define TPE bte
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE sht
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE int
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE lng
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE flt
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE dbl
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#endif
#undef COMPRESSION_DEFINITION

#define LAYOUT_DEFINITION
#define TPE bte
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE sht
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE int
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE lng
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE flt
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#define TPE dbl
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#endif
#undef LAYOUT_DEFINITION

#define TPE bte
#include "mosaic_select_template.h"
#include "mosaic_projection_template.h"
#undef TPE
#define TPE sht
#include "mosaic_select_template.h"
#include "mosaic_projection_template.h"
#undef TPE
#define TPE int
#include "mosaic_select_template.h"
#include "mosaic_projection_template.h"
#undef TPE
#define TPE lng
#include "mosaic_select_template.h"
#include "mosaic_projection_template.h"
#undef TPE
#define TPE flt
#include "mosaic_select_template.h"
#include "mosaic_projection_template.h"
#undef TPE
#define TPE dbl
#include "mosaic_select_template.h"
#include "mosaic_projection_template.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_select_template.h"
#include "mosaic_projection_template.h"
#undef TPE
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

#undef METHOD_TEMPLATES_INCLUDE
#undef METHOD
