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

#ifndef _MOSAIC_RLE_
#define _MOSAIC_RLE_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_select.h"

bool MOStypes_runlength(BAT* b);
mal_export void MOSlayout_runlength(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_runlength(MOStask task);
mal_export void MOSskip_runlength(MOStask task);
mal_export str  MOSestimate_runlength(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_runlength(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_runlength(MOStask task);
mal_export str MOSprojection_runlength( MOStask task);
mal_export str MOSjoin_runlength( MOStask task, bit nil_matches);

MOSselect_SIGNATURE(runlength, bte);
MOSselect_SIGNATURE(runlength, sht);
MOSselect_SIGNATURE(runlength, int);
MOSselect_SIGNATURE(runlength, lng);
MOSselect_SIGNATURE(runlength, flt);
MOSselect_SIGNATURE(runlength, dbl);
#ifdef HAVE_HGE
MOSselect_SIGNATURE(runlength, hge);
#endif

#define SELECT_RUNLENGTH(TPE) do_select(runlength, TPE)

#endif /* _MOSAIC_RLE_ */
