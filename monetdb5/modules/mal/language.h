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
 * @+ Dummy code
 */
#ifndef _LANGUAGE_H
#define _LANGUAGE_H
#include "mal.h"
#include "mal_module.h"
#include "mal_session.h"
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_dataflow.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define language_export extern __declspec(dllimport)
#else
#define language_export extern __declspec(dllexport)
#endif
#else
#define language_export extern
#endif

language_export str CMDraise(str *ret, str *msg);
language_export str MALassertBit(int *ret, bit *val, str *msg);
language_export str MALassertStr(int *ret, str *val, str *msg);
language_export str MALassertOid(int *ret, oid *val, str *msg);
language_export str MALassertSht(int *ret, sht *val, str *msg);
language_export str MALassertInt(int *ret, int *val, str *msg);
language_export str MALassertLng(int *ret, lng *val, str *msg);
language_export str MALstartDataflow( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str MALpass( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str MALgarbagesink( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDregisterFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDcallString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDcallFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDcallBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDevalFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDincludeFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDdebug(int *ret, int *flg);
language_export str MALassertTriple(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
#endif /* _LANGUAGE_H */
