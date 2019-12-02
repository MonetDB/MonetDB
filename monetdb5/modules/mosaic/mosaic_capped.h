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

#ifndef _MOSAIC_CAPPED_
#define _MOSAIC_CAPPED_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"
#include "mosaic_dictionary.h"

#define DICTTHRESHOLD 4192
#define DICTSIZE 256

bool MOStypes_capped(BAT* b);
mal_export str MOScreateCappedInfo(MOStask task);
mal_export void MOSlayout_capped(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSlayout_capped_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_capped(MOStask task);
mal_export void MOSskip_capped(MOStask task);
mal_export str MOSprepareEstimate_capped(MOStask task);
mal_export str  MOSestimate_capped(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOSpostEstimate_capped(MOStask task);
mal_export str finalizeDictionary_capped(MOStask task);
mal_export void MOScompress_capped(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_capped(MOStask task);

ALGEBRA_INTERFACES_ALL_TYPES(capped);

#define DO_OPERATION_ON_capped(OPERATION, TPE, ...) DO_OPERATION_ON_ALL_TYPES(OPERATION, capped, TPE, __VA_ARGS__)

#define join_inner_loop_capped(TPE, HAS_NIL, RIGHT_CI_NEXT) join_inner_loop_dictionary(capped, TPE, HAS_NIL, RIGHT_CI_NEXT)

#endif /* _MOSAIC_CAPPED_ */
