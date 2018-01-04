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

mal_export void MOSdump_runlength(Client cntxt, MOStask task);
mal_export void MOSlayout_runlength(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);
mal_export void MOSadvance_runlength(Client cntxt, MOStask task);
mal_export void MOSskip_runlength(Client cntxt, MOStask task);
mal_export flt  MOSestimate_runlength(Client cntxt, MOStask task);
mal_export void MOScompress_runlength(Client cntxt, MOStask task);
mal_export void MOSdecompress_runlength(Client cntxt, MOStask task);
mal_export str MOSselect_runlength(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti);
mal_export str MOSthetaselect_runlength(Client cntxt,  MOStask task, void *val, str oper);
mal_export str MOSprojection_runlength(Client cntxt,  MOStask task);
mal_export str MOSjoin_runlength(Client cntxt,  MOStask task);
#endif /* _MOSAIC_RLE_ */
