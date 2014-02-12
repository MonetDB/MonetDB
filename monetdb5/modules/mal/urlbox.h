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

#ifndef _URL_BOX_H
#define _URL_BOX_H
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define urlbox_export extern __declspec(dllimport)
#else
#define urlbox_export extern __declspec(dllexport)
#endif
#else
#define urlbox_export extern
#endif

urlbox_export str URLBOXprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXopen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXclose(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXdestroy(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXdepositFile(int *r, str *fnme);
urlbox_export str URLBOXdeposit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXtake(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXreleaseOid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXreleaseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXdiscard(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXdiscardOid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXdiscardAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXtoString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXnewIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXhasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
urlbox_export str URLBOXgetLevel(int *r, int *level);
urlbox_export str URLBOXgetNames(int *r);
urlbox_export str URLBOXgetCount(int *r);
urlbox_export str URLBOXgetCardinality(int *r);
urlbox_export str URLBOXgetSize(int *r);
#endif /* _URL_BOX_H */
