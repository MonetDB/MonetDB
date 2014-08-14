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

/* (c) M Kersten
 * The multi-purpose compressor infrastructure
 */
#ifndef _MOSLIST_H
#define _MOSLIST_H
#include "mal_interpreter.h"
#include "mal_exception.h"
#include "mal_function.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mos_export extern __declspec(dllimport)
#else
#define mos_export extern __declspec(dllexport)
#endif
#else
#define mos_export extern
#endif

mos_export str MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mos_export str MOScompressInternal(Client cntxt, int *ret, int *bid, str properties);
mos_export str MOSdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mos_export str MOSdecompressInternal(Client cntxt, int *ret, int *bid);
mos_export str MOSanalyse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mos_export str MOSsubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mos_export str MOSthetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mos_export str MOSleftfetchjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mos_export str MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mos_export str MOSdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _MOSLIST_H */
