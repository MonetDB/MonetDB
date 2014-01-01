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

#ifndef _CONST_H
#define _CONST_H
#include "clients.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_authorize.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define const_export extern __declspec(dllimport)
#else
#define const_export extern __declspec(dllexport)
#endif
#else
#define const_export extern
#endif

const_export str CSTprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTopen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTclose(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTdestroy(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTdeposit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTtake(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTreleaseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTdiscard(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTtoString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSTnewIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export str CSThasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
const_export  str CSTepilogue(int *ret);
#endif /* _CONST_H */
