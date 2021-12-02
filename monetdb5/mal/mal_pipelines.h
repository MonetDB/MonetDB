/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _MAL_SHARD_H
#define _MAL_SHARD_H

#include "mal.h"
#include "mal_client.h"

mal_export str runMALpipelines(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, int maxparts, MalStkPtr stk);

#endif /*  _MAL_SHARD_H*/
