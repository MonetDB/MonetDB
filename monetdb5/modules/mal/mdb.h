/*
 *The contents of this file are subject to the MonetDB Public License
 *Version 1.1 (the "License"); you may not use this file except in
 *compliance with the License. You may obtain a copy of the License at
 *http://www.monetdb.org/Legal/MonetDBLicense
 *
 *Software distributed under the License is distributed on an "AS IS"
 *basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 *License for the specific language governing rights and limitations
 *under the License.
 *
 *The Original Code is the MonetDB Database System.
 *
 *The Initial Developer of the Original Code is CWI.
 *Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 *Copyright August 2008-2014 MonetDB B.V.
 *All Rights Reserved.
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
mdb_export str CMDmodules(int *bid);
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
mdb_export str MDBlifespan(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
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
