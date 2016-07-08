/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _MDB_H
#define _MDB_H
#include "gdk.h"
#include "mutils.h"
#include <stdarg.h>
#include <time.h>
#include <sys/types.h>
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#include "mal_resolve.h"
#include "mal_linker.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mdb_export extern __declspec(dllimport)
#else
#define mdb_export extern __declspec(dllexport)
#endif
#else
#define mdb_export extern
#endif

mdb_export str MDBstart(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBstartFactory(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBinspect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str CMDmodules(bat *bid);
mdb_export str MDBsetTrace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBsetVarTrace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBgetDebug(int *ret);
mdb_export str MDBsetDebug(int *ret, int *flg);
mdb_export str MDBsetDebugStr(int *ret, str *nme);
mdb_export str MDBsetCatch(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBgetExceptionVariable(str *ret, str *msg);
mdb_export str MDBgetExceptionReason(str *ret, str *msg);
mdb_export str MDBgetExceptionContext(str *ret, str *msg);
mdb_export str MDBlist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBlistMapi(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBshowFlowGraph(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBlist3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBlistDetail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBlist3Detail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBvar(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBvar3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mdb_export str MDBStkDepth(Client cntxt, MalBlkPtr mb, MalStkPtr s, InstrPtr p);
mdb_export str MDBgetStackFrameN(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
mdb_export str MDBgetStackFrame(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
mdb_export str MDBStkTrace(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
mdb_export str MDBgetDefinition(Client cntxt, MalBlkPtr m, MalStkPtr stk, InstrPtr p);
mdb_export str MDBgrapTrappedProcess(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mdb_export str MDBtrapFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mdb_export str MDBdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

mdb_export str MDBdummy(int *ret);
#endif /* _MDB_H */
