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

/*
 * M. Kersten
 * Default multiplex operator implementation
 */
#ifndef _MANIFOLD_LIB_
#define _MANIFOLD_LIB_
#include "monetdb_config.h"
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

manifoldplex_export MALfcn MANIFOLDtypecheck(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manifoldplex_export str MANIFOLDevaluate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manifoldplex_export str MANIFOLDremapMultiplex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif /* _MANIFOLD_LIB_ */
