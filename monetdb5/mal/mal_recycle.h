/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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
#define HARDLIMIT_STMT 250 /*5000*/

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
