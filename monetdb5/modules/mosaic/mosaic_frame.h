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
#include "mosaic_utility.h"

bool MOStypes_frame(BAT* b);
mal_export void MOScreateframeDictionary(MOStask task);
mal_export void MOSlayout_frame_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSlayout_frame(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_frame(MOStask task);
mal_export void MOSskip_frame(MOStask task);
mal_export str  MOSestimate_frame(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_frame(MOStask task, MosaicBlkRec* estimate);
mal_export void MOSdecompress_frame(MOStask task);

ALGEBRA_INTERFACES_INTEGERS_ONLY(frame);
#define DO_OPERATION_ON_frame(OPERATION, TPE) DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, frame, TPE)

#endif /* _MOSAIC_FRAME_ */
