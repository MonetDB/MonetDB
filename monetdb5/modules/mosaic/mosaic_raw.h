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

#ifndef _MOSAIC_RAW_
#define _MOSAIC_RAW_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"

bool MOStypes_raw(BAT* b);
mal_export void MOSlayout_raw(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_raw(MOStask task);
mal_export void MOSskip_raw(MOStask task);
mal_export void MOScompress_raw(MOStask task);
mal_export void MOSdecompress_raw(MOStask task);
mal_export str MOSselect_raw( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti);
mal_export str MOSthetaselect_raw( MOStask task, void *val, str oper);
mal_export str MOSprojection_raw( MOStask task);
mal_export str MOSjoin_raw( MOStask task);
#endif /* _MOSAIC_RAW_ */
