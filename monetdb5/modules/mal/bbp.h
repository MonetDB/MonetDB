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

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define bbp_export extern __declspec(dllimport)
#else
#define bbp_export extern __declspec(dllexport)
#endif
#else
#define bbp_export extern
#endif

#ifdef _MSC_VER
#define getcwd _getcwd
#endif

bbp_export str CMDbbpbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bbp_export str CMDbbpDiskSpace(lng *ret);
bbp_export str CMDgetPageSize(int *ret);
bbp_export str CMDbbpNames(bat *ret);
bbp_export str CMDbbpName(str *ret, bat *bid);
bbp_export str CMDbbpCount(bat *ret);
bbp_export str CMDbbpLocation(bat *ret);
bbp_export str CMDbbpHeat(bat *ret);
bbp_export str CMDbbpDirty(bat *ret);
bbp_export str CMDbbpStatus(bat *ret);
bbp_export str CMDbbpKind(bat *ret);
bbp_export str CMDbbpRefCount(bat *ret);
bbp_export str CMDbbpLRefCount(bat *ret);
bbp_export str CMDbbpgetIndex(int *res, bat *bid);
bbp_export str CMDgetBATrefcnt(int *res, bat *bid);
bbp_export str CMDgetBATlrefcnt(int *res, bat *bid);
bbp_export str CMDbbp(bat *ID, bat *NS, bat *TT, bat *CNT, bat *REFCNT, bat *LREFCNT, bat *LOCATION, bat *HEAT, bat *DIRTY, bat *STATUS, bat *KIND);
bbp_export str CMDsetName(str *rname, const bat *b, str *name);
#endif /* _BBP_H_*/
