/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * In most cases we pass a BAT identifier, which should be unified
 * with a BAT descriptor. Upon failure we can simply abort the function.
 */
#ifndef _BBP_H_
#define _BBP_H_
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_module.h"
#include "mal_session.h"
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_profiler.h"
#include "bat5.h"

#ifdef _MSC_VER
#define getcwd _getcwd
#endif

mal_export str CMDbbpbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDbbpDiskSpace(lng *ret);
mal_export str CMDgetPageSize(int *ret);
mal_export str CMDbbpNames(bat *ret);
mal_export str CMDbbpName(str *ret, bat *bid);
mal_export str CMDbbpCount(bat *ret);
mal_export str CMDbbpLocation(bat *ret);
mal_export str CMDbbpHeat(bat *ret);
mal_export str CMDbbpDirty(bat *ret);
mal_export str CMDbbpStatus(bat *ret);
mal_export str CMDbbpKind(bat *ret);
mal_export str CMDbbpRefCount(bat *ret);
mal_export str CMDbbpLRefCount(bat *ret);
mal_export str CMDbbpgetIndex(int *res, bat *bid);
mal_export str CMDgetBATrefcnt(int *res, bat *bid);
mal_export str CMDgetBATlrefcnt(int *res, bat *bid);
mal_export str CMDbbp(bat *ID, bat *NS, bat *TT, bat *CNT, bat *REFCNT, bat *LREFCNT, bat *LOCATION, bat *HEAT, bat *DIRTY, bat *STATUS, bat *KIND);
mal_export str CMDsetName(str *rname, const bat *b, str *name);
#endif /* _BBP_H_*/
