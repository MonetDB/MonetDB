/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _MAL_RECYCLE_
#define _MAL_RECYCLE_

#include "mal.h"
#include "mal_exception.h"
#include "mal_instruction.h"
#include "mal_runtime.h"
#include "mal_client.h"

#define _DEBUG_RECYCLE_
#define _DEBUG_CACHE_
#define _DEBUG_RESET_

/*
 * We need some hard limits to not run out of datastructure spaces.
 */
#define HARDLIMIT_STMT 1000 /*5000*/

#define NO_RECYCLING -1
#define RECYCLING 1
/*
 * To avoid a polution of the recycle cache, we do not store any
 * intruction for which there is no function/command/pattern implementation.
 */
#define RECYCLEinterest(p) ( p->recycle == RECYCLING && getFunctionId(p) != NULL)

mal_export int recycleCacheLimit;
mal_export MalBlkPtr recycleBlk;

mal_export void RECYCLEinit(void);
mal_export lng  RECYCLEentry(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof);
mal_export void RECYCLEexit(Client cntxt,MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof);
mal_export void RECYCLEdrop(Client cntxt);

mal_export str RECYCLEcolumn(Client cntxt, str sch,str tbl, str col);
mal_export str RECYCLEresetBAT(Client cntxt, int bid);

mal_export void RECYCLEdump(stream *s);
#endif
