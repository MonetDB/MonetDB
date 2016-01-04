/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _INSPECT_H
#define _INSPECT_H

#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_interpreter.h"
#include "mal_listing.h"
#include "mal_namespace.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define inspect_export extern __declspec(dllimport)
#else
#define inspect_export extern __declspec(dllexport)
#endif
#else
#define inspect_export extern
#endif

inspect_export str INSPECTgetkind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetAllSignatures(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetAllModules(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetAllFunctions(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetAllAddresses(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetDefinition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetSignature(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetAddress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetComment(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetSource(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTgetFunctionSize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
inspect_export str INSPECTgetSize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
inspect_export str INSPECTgetEnvironment(bat *ret, bat *ret2);
inspect_export str INSPECTgetEnvironmentKey(str *ret, str *key);
inspect_export str INSPECTatom_names(bat *ret);
inspect_export str INSPECTatom_sup_names(bat *ret);
inspect_export str INSPECTatom_sizes(bat *ret);
inspect_export str INSPECTshowFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
inspect_export str INSPECTshowFunction3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
inspect_export str INSPECTtypeName(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
inspect_export str INSPECTequalType(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _INSPECT_H */
