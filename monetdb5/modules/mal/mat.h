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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAT_H
#define _MAT_H

#include <stdarg.h>
#include "mal_resolve.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mat_export extern __declspec(dllimport)
#else
#define mat_export extern __declspec(dllexport)
#endif
#else
#define mat_export extern
#endif

mat_export str MATpack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATmergepack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATpack2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATpack3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATpackIncrement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATpackValues(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATpackSlice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATnewIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MAThasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATdummy(int *ret, str *grp);
mat_export str MATinfo(int *ret, str *grp, str *elm);
mat_export str MATprint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATproject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mat_export str MATsortReverseTail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mat_export str MATsortTail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


#endif /* _MAT_H */
