/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _ARNG_H
#define _ARNG_H

#include "mal.h"
#include "mal_builder.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define arrange_export extern __declspec(dllimport)
#else
#define arrange_export extern __declspec(dllexport)
#endif
#else
#define arrange_export extern
#endif

#define _DEBUG_ARNG_
arrange_export str ARNGcreate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
arrange_export str ARNGmerge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _ARNG_H */
