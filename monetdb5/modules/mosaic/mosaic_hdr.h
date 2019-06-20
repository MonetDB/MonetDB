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

#ifndef _MOSAIC_HDR_
#define _MOSAIC_HDR_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"

mal_export void MOSupdateHeader(Client cntxt, MOStask task);
mal_export void MOSinitHeader(MOStask task);
mal_export void MOSinitializeScan(Client cntxt, MOStask task, int startblk, int stopblk);
#endif /* _MOSAIC_HDR_ */
