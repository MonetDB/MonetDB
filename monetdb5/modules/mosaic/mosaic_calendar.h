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

#ifndef _MOSAIC_TEMPORAL_
#define _MOSAIC_TEMPORAL_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"

#define TEMPORALTHRESHOLD 4192
#define TEMPORALSIZE 256 

bool MOStypes_calendar(BAT* b);
mal_export void MOScreatecalendar(MOStask task);
mal_export void MOSlayout_calendar(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSlayout_calendar_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_calendar(MOStask task);
mal_export void MOSskip_calendar(MOStask task);
mal_export flt  MOSestimate_calendar(MOStask task);
mal_export void MOScompress_calendar(MOStask task);
mal_export void MOSdecompress_calendar(MOStask task);
mal_export str MOSselect_calendar( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti);
mal_export str MOSthetaselect_calendar( MOStask task, void *val, str oper);
mal_export str MOSprojection_calendar( MOStask task);
mal_export str MOSjoin_calendar( MOStask task);
#endif /* _MOSAIC_TEMPORAL_ */
