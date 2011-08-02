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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _RECYCLE_
#define _RECYCLE_

#include "mal.h"
#include "mal_instruction.h"
#include "bat5.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define recycle_export extern __declspec(dllimport)
#else
#define recycle_export extern __declspec(dllexport)
#endif
#else
#define recycle_export extern
#endif

recycle_export str RECYCLEdumpWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
recycle_export str RECYCLEstart(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
recycle_export str RECYCLEstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
recycle_export str RECYCLEsetAdmission(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
recycle_export str RECYCLEsetReuse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
recycle_export str RECYCLEresetCMD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
recycle_export str RECYCLEsetCache(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
recycle_export str RECYCLEgetAdmission(int *p);
recycle_export str RECYCLEgetReuse(int *p);
recycle_export str RECYCLEgetCache(int *p);
recycle_export str RECYCLEshutdownWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
recycle_export str RECYCLEmonitor(int *d, int *p);
recycle_export str RECYCLElog(int *d, str *nm);
recycle_export str RECYCLEprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
recycle_export str RECYCLEepilogue(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
recycle_export str RECYCLEdumpQPatWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
