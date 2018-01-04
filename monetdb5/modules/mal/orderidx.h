/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _OIDX_H
#define _OIDX_H

#include "mal.h"
#include "mal_builder.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define orderidx_export extern __declspec(dllimport)
#else
#define orderidx_export extern __declspec(dllexport)
#endif
#else
#define orderidx_export extern
#endif

//#define _DEBUG_OIDX_
orderidx_export str OIDXcreate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
orderidx_export str OIDXcreateImplementation(Client cntxt, int tpe, BAT *b, int pieces);
orderidx_export str OIDXdropImplementation(Client cntxt, BAT *b);
orderidx_export str OIDXorderidx(bat *ret, const bat *bid, const bit *stable);
orderidx_export str OIDXmerge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
orderidx_export str OIDXhasorderidx(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
orderidx_export str OIDXgetorderidx(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _OIDX_H */
