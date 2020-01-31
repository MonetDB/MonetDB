/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 */

#ifndef _MOSAIC_HDR_
#define _MOSAIC_HDR_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"

void MOSupdateHeader(MOStask* task);
void MOSinitHeader(MOStask* task);
void MOSinitializeScan(MOStask* task, BAT* b);
str  MOSlayout_hdr(MOStask* task, MosaicLayout* layout);

#endif /* _MOSAIC_HDR_ */
