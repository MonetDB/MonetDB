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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/
#ifndef _STATISTICS_DEF
#define _STATISTICS_DEF

/* #define DEBUG_STATISTICS */

#include <mal.h>
#include <mal_client.h>
#include <mal_interpreter.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define s_export extern __declspec(dllimport)
#else
#define s_export extern __declspec(dllexport)
#endif
#else
#define s_export extern
#endif

s_export str STATforceUpdateAll(int *ret);
s_export str STATdrop(str nme);
s_export str STATforceUpdateAll(int *ret);
s_export str STATenroll(int *ret, str *nme);
s_export str STATenrollHistogram(int *ret, str *nme);
s_export str STATupdateAll(int *ret, int forced);
s_export str STATupdate(int *ret);
s_export str STATforceUpdate(int *ret, str *nme);
s_export str STATdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATprelude(int *ret);
s_export str STATepilogue(int *ret);
s_export str STATopen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATclose(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATdestroy(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATdepositStr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATdeposit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATtake(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATreleaseStr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATreleaseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATdiscard(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATdiscard2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATtoString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATnewIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STAThasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATgetHotset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATgetObjects(int *bid);
s_export str STATgetHistogram (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATgetCount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATgetSize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATgetMin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
s_export str STATgetMax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _STATISTICS_DEF */
