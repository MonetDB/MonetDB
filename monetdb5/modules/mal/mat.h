/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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
mat_export str MATpackIncrement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATpackValues(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATpackSlice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATnewIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MAThasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mat_export str MATinfo(bat *ret, str *grp, str *elm);
mat_export str MATproject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mat_export str MATsortReverse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mat_export str MATsort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


#endif /* _MAT_H */
