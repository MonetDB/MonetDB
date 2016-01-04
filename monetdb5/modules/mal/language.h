/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
language_export str MALassertBit(void *ret, bit *val, str *msg);
language_export str MALassertStr(void *ret, str *val, str *msg);
language_export str MALassertOid(void *ret, oid *val, str *msg);
language_export str MALassertSht(void *ret, sht *val, str *msg);
language_export str MALassertInt(void *ret, int *val, str *msg);
language_export str MALassertLng(void *ret, lng *val, str *msg);
#ifdef HAVE_HGE
language_export str MALassertHge(void *ret, hge *val, str *msg);
#endif
language_export str MALstartDataflow( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str MALpass( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str MALgarbagesink( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDregisterFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDcallString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDcallFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDcallBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str CMDevalFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
language_export str MALassertTriple(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
#endif /* _LANGUAGE_H */
