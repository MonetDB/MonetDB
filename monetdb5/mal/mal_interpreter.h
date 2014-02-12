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
 * Author M. Kersten
 * The MAL Interpreter
 */

#ifndef _MAL_INTERPRET_H
#define _MAL_INTERPRET_H

#include "mal_client.h"
#include "mal_factory.h"
#include "mal_profiler.h"
#include "mal_recycle.h"

/*
 * Activation of a thread requires construction of the argument list
 * to be passed by a handle.
 */

/* #define DEBUG_FLOW */

mal_export void showErrors(Client cntxt);
mal_export MalStkPtr prepareMALstack(MalBlkPtr mb, int size);
mal_export void initMALstack(MalBlkPtr mb, MalStkPtr stk);
mal_export str runMAL(Client c, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr env);
mal_export str runMALsequence(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, MalStkPtr stk, MalStkPtr env, InstrPtr pcicaller);
mal_export str reenterMAL(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, MalStkPtr stk);
mal_export str callMAL(Client cntxt, MalBlkPtr mb, MalStkPtr *glb, ValPtr argv[], char debug);
mal_export void garbageElement(Client cntxt, ValPtr v);
mal_export void garbageCollector(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int flag);
mal_export void releaseBAT(MalBlkPtr mb, MalStkPtr stk, int bid);
mal_export str malCommandCall(MalStkPtr stk, InstrPtr pci);
mal_export int isNotUsedIn(InstrPtr p, int start, int a);
mal_export str safeguardStack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str catchKernelException(Client cntxt, str ret);

mal_export ptr getArgReference(MalStkPtr stk, InstrPtr pci, int k);

#define FREE_EXCEPTION(p) do { if (p && p != M5OutOfMemory) GDKfree(p); } while (0)
#endif /*  _MAL_INTERPRET_H*/
