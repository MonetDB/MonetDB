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
 * @-
 * @+ Dummy code
 */
#ifndef __BOX_H
#define __BOX_H

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define box_export extern __declspec(dllimport)
#else
#define box_export extern __declspec(dllexport)
#endif
#else
#define box_export extern
#endif

box_export str BOXopen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXclose(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXdestroy(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXdeposit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXtake(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXreleaseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXdiscard(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXtoString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
box_export str BOXgetBoxNames(int *bid);
box_export str BOXiterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* __BOX_H */
