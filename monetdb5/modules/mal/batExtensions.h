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

#ifndef _BAT_EXTENSIONS_
#define _BAT_EXTENSIONS_

#include "mal_client.h"
#include "mal_interpreter.h"
#include "bat5.h"
#include "algebra.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define be_export extern __declspec(dllimport)
#else
#define be_export extern __declspec(dllexport)
#endif
#else
#define be_export extern
#endif

be_export str CMDBATclone(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
be_export str CMDBATnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
be_export str CMDBATnewDerived(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
be_export str CMDBATderivedByName(int *ret, str *nme);
be_export str CMDBATnewint(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
be_export str CMDbatpartition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
be_export str CMDbatpartition2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _BAT_EXTENSIONS_ */
