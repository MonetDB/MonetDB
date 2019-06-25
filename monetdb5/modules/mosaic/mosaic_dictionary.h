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

#ifndef _MOSAIC_DICT_
#define _MOSAIC_DICT_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"

#define DICTTHRESHOLD 4192
#define DICTSIZE 256 

bool MOStypes_dictionary(BAT* b);
mal_export void MOScreatedictionary(MOStask task);
mal_export void MOSlayout_dictionary(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSlayout_dictionary_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_dictionary(MOStask task);
mal_export void MOSskip_dictionary(MOStask task);
mal_export flt  MOSestimate_dictionary(MOStask task);
mal_export void MOScompress_dictionary(MOStask task);
mal_export void MOSdecompress_dictionary(MOStask task);
mal_export str MOSselect_dictionary( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti);
mal_export str MOSthetaselect_dictionary( MOStask task, void *val, str oper);
mal_export str MOSprojection_dictionary( MOStask task);
mal_export str MOSjoin_dictionary( MOStask task);
#endif /* _MOSAIC_DICT_ */
