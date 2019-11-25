/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (c)2014 author Martin Kersten
 */

#ifndef _MOSAIC_PREFIX_
#define _MOSAIC_PREFIX_

/* #define _DEBUG_PREFIX_*/

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_select.h"

bool MOStypes_prefix(BAT* b);
mal_export void MOSlayout_prefix(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_prefix(MOStask task);
mal_export void MOSskip_prefix(MOStask task);
mal_export str  MOSestimate_prefix(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_prefix(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_prefix(MOStask task);
mal_export str MOSprojection_prefix( MOStask task);
mal_export str MOSjoin_prefix( MOStask task, bit nil_matches);

MOSselect_SIGNATURE(prefix, bte);
MOSselect_SIGNATURE(prefix, sht);
MOSselect_SIGNATURE(prefix, int);
MOSselect_SIGNATURE(prefix, lng);
#ifdef HAVE_HGE
MOSselect_SIGNATURE(prefix, hge);
#endif

#define select_prefix_bte do_select(prefix, bte)
#define select_prefix_sht do_select(prefix, sht)
#define select_prefix_int do_select(prefix, int)
#define select_prefix_lng do_select(prefix, lng)
#define select_prefix_flt assert(0);
#define select_prefix_dbl assert(0);
#ifdef HAVE_HGE
#define select_prefix_hge do_select(prefix, hge)
#endif

#define SELECT_PREFIX(TPE) select_prefix_##TPE
#endif /* _MOSAIC_PREFIX_ */
