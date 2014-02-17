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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
bbp_export str CMDbbpDiskReads(lng *ret);
bbp_export str CMDbbpDiskWrites(lng *ret);
bbp_export str CMDgetPageSize(int *ret);
bbp_export str CMDbbpNames(int *ret);
bbp_export str CMDbbpName(str *ret, int *bid);
bbp_export str CMDbbpRNames(int *ret);
bbp_export str CMDbbpCount(int *ret);
bbp_export str CMDbbpLocation(int *ret);
bbp_export str CMDbbpHeat(int *ret);
bbp_export str CMDbbpDirty(int *ret);
bbp_export str CMDbbpStatus(int *ret);
bbp_export str CMDbbpKind(int *ret);
bbp_export str CMDbbpRefCount(int *ret);
bbp_export str CMDbbpLRefCount(int *ret);
bbp_export str CMDbbpgetIndex(int *res, int *bid);
bbp_export str CMDgetBATrefcnt(int *res, int *bid);
bbp_export str CMDgetBATlrefcnt(int *res, int *bid);
bbp_export str CMDbbpcompress(int *ret, int *bid, str *fnme);
bbp_export str CMDbbpdecompress(int *ret, int *bid, str *fnme);
bbp_export str CMDbbptruncate(int *ret, int *bid, str *fnme);
bbp_export str CMDbbpexpand(int *ret, int *bid, str *fnme);
bbp_export str CMDbbp(bat *ID, bat *NS, bat *HT, bat *TT, bat *CNT, bat *REFCNT, bat *LREFCNT, bat *LOCATION, bat *HEAT, bat *DIRTY, bat *STATUS, bat *KIND);
#endif /* _BBP_H_*/
