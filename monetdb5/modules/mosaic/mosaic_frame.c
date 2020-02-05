/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 * Frame of reference compression with dictionary
 * A codevector chunk is beheaded by a reference value F from the column. The elements V in the
 * chunk are replaced by an index into a global dictionary of V-F offsets.
 *
 * The dictionary is limited to 256 entries and all indices are at most one byte.
 * The maximal achievable compression ratio depends on the size of the dictionary
 *
 * This scheme is particularly geared at evolving time series, e.g. stock markets.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_frame.h"
#include "mosaic_private.h"

#include <stdint.h>

bool MOStypes_frame(BAT* b) {
	switch (b->ttype){
	case TYPE_bit: return true; // Will be mapped to bte
	case TYPE_bte: return true;
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
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

#define METHOD frame
#define METHOD_TEMPLATES_INCLUDE MAKE_TEMPLATES_INCLUDE_FILE(METHOD)

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
#ifdef HAVE_HGE
#define TPE hge
#include METHOD_TEMPLATES_INCLUDE
#undef TPE
#endif
#undef COMPRESSION_DEFINITION

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
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_select_template.h"
#include "mosaic_projection_template.h"
#undef TPE
#endif

#define outer_loop_frame(HAS_NIL, NIL_MATCHES, TPE, LEFT_CI_NEXT, RIGHT_CI_NEXT) \
{\
    MOSBlockHeaderTpe(frame, TPE)* parameters = (MOSBlockHeaderTpe(frame, TPE)*) ((task))->blk;\
	const TPE min =  parameters->min;\
	const BitVector base = (BitVector) MOScodevectorFrame(task, TPE);\
	const bte bits = parameters->bits;\
    for (oid lo = canditer_peekprev(task->ci); !is_oid_nil(lo) && lo < last; lo = LEFT_CI_NEXT(task->ci)) {\
        BUN i = (BUN) (lo - first);\
		TPE lval = ADD_DELTA(TPE, min, getBitVector(base, i, bits));\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, lval))) {continue;};\
		}\
		INNER_LOOP_UNCOMPRESSED(HAS_NIL, TPE, RIGHT_CI_NEXT);\
	}\
}

MOSjoin_COUI_DEF(frame, bte)
MOSjoin_COUI_DEF(frame, sht)
MOSjoin_COUI_DEF(frame, int)
MOSjoin_COUI_DEF(frame, lng)
#ifdef HAVE_HGE
MOSjoin_COUI_DEF(frame, hge)
#endif

#undef METHOD_TEMPLATES_INCLUDE
#undef METHOD
