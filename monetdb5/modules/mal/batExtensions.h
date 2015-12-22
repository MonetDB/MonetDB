/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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

be_export str CMDBATnewColumn(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
be_export str CMDBATnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
be_export str CMDBATnew_persistent(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
be_export str CMDBATnewDerived(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
be_export str CMDBATsingle(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
be_export str CMDBATpartition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
be_export str CMDBATpartition2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
be_export str CMDBATimprints(void *ret, bat *bid);
be_export str CMDBATimprintsize(lng *ret, bat *bid);

#endif /* _BAT_EXTENSIONS_ */
