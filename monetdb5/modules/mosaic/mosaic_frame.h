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

#ifndef _MOSAIC_FRAME_
#define _MOSAIC_FRAME_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_select.h"

bool MOStypes_frame(BAT* b);
mal_export void MOScreateframeDictionary(MOStask task);
mal_export void MOSlayout_frame_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSlayout_frame(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_frame(MOStask task);
mal_export void MOSskip_frame(MOStask task);
mal_export str  MOSestimate_frame(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_frame(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_frame(MOStask task);
mal_export str MOSprojection_frame( MOStask task);
mal_export str MOSjoin_frame( MOStask task, bit nil_matches);

MOSselect_SIGNATURE(frame, bte);
MOSselect_SIGNATURE(frame, sht);
MOSselect_SIGNATURE(frame, int);
MOSselect_SIGNATURE(frame, lng);
#ifdef HAVE_HGE
MOSselect_SIGNATURE(frame, hge);
#endif

#define select_frame_bte do_select(frame, bte)
#define select_frame_sht do_select(frame, sht)
#define select_frame_int do_select(frame, int)
#define select_frame_lng do_select(frame, lng)
#define select_frame_flt assert(0);
#define select_frame_dbl assert(0);
#ifdef HAVE_HGE
#define select_frame_hge do_select(frame, hge)
#endif

#define SELECT_FRAME(TPE) select_frame_##TPE

#endif /* _MOSAIC_FRAME_ */
