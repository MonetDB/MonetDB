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

/*
 * @+ Implementation code
 */
#ifndef _FACTORIES_H
#define _FACTORIES_H

#include "mal.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define factories_export extern __declspec(dllimport)
#else
#define factories_export extern __declspec(dllexport)
#endif
#else
#define factories_export extern
#endif

factories_export str FCTgetPlants(int *ret, int *ret2);
factories_export str FCTgetCaller(int *ret);
factories_export str FCTgetOwners(int *ret);
factories_export str FCTgetArrival(int *ret);
factories_export str FCTgetDeparture(int *ret);
factories_export str FCTsetLocation(int *ret, str *loc);
factories_export str FCTgetLocations(int *ret);
factories_export str FCTshutdown(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _FACTORIES_H */
