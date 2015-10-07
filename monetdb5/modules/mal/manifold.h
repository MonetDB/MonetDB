/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Kersten
 * Default multiplex operator implementation
 */
#ifndef _MANIFOLD_LIB_
#define _MANIFOLD_LIB_
#include <string.h>

#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

/* #define _DEBUG_MANIFOLD_*/

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define manifoldplex_export extern __declspec(dllimport)
#else
#define manifoldplex_export extern __declspec(dllexport)
#endif
#else
#define manifoldplex_export extern
#endif

manifoldplex_export MALfcn MANIFOLDtypecheck(Client cntxt, MalBlkPtr mb, InstrPtr pci);
manifoldplex_export str MANIFOLDevaluate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manifoldplex_export str MANIFOLDremapMultiplex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif /* _MANIFOLD_LIB_ */
