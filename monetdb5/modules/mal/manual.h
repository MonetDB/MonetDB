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

#ifndef _MANUAL_H
#define _MANUAL_H
#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define manual_export extern __declspec(dllimport)
#else
#define manual_export extern __declspec(dllexport)
#endif
#else
#define manual_export extern
#endif

manual_export str MANUALcreateSection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreate1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreate0(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALsearch(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALhelp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreateIndex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreateSummary(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcompletion(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _MANUAL_H */
