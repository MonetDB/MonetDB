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

#ifndef _MOSAIC_LINEAR_
#define _MOSAIC_LINEAR_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_select.h"

bool MOStypes_linear(BAT* b);
mal_export void MOSlayout_linear(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_linear(MOStask task);
mal_export void MOSskip_linear(MOStask task);
mal_export str  MOSestimate_linear(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_linear(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_linear(MOStask task);
mal_export str MOSprojection_linear( MOStask task);
mal_export str MOSjoin_linear( MOStask task, bit nil_matches);

MOSselect_SIGNATURE(linear, bte);
MOSselect_SIGNATURE(linear, sht);
MOSselect_SIGNATURE(linear, int);
MOSselect_SIGNATURE(linear, lng);
#ifdef HAVE_HGE
MOSselect_SIGNATURE(linear, hge);
#endif

#define select_linear_bte do_select(linear, bte)
#define select_linear_sht do_select(linear, sht)
#define select_linear_int do_select(linear, int)
#define select_linear_lng do_select(linear, lng)
#define select_linear_flt assert(0);
#define select_linear_dbl assert(0);
#ifdef HAVE_HGE
#define select_linear_hge do_select(linear, hge)
#endif

#define SELECT_LINEAR(TPE) select_linear_##TPE

#endif /* _MOSAIC_LINEAR_ */
