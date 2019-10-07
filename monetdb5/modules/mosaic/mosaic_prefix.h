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

bool MOStypes_prefix(BAT* b);
mal_export void MOSlayout_prefix(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_prefix(MOStask task);
mal_export void MOSskip_prefix(MOStask task);
mal_export str  MOSestimate_prefix(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous);
mal_export void MOScompress_prefix(MOStask task);
mal_export void MOSdecompress_prefix(MOStask task);
mal_export str MOSselect_prefix( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti);
mal_export str MOSthetaselect_prefix( MOStask task, void *val, str oper);
mal_export str MOSprojection_prefix( MOStask task);
mal_export str MOSjoin_prefix( MOStask task);
#endif /* _MOSAIC_PREFIX_ */
