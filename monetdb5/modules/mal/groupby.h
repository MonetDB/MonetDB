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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @-
 * @include prelude.mx
 * @+ Implementation code
 */
#ifndef _GROUPBY_H
#define _GROUPBY_H

#define _DEBUG_GROUPBY_

#include "mal.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define group_by extern __declspec(dllimport)
#else
#define group_by extern __declspec(dllexport)
#endif
#else
#define group_by extern
#endif

group_by str GROUPid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
group_by str GROUPcount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
group_by str GROUPmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
group_by str GROUPmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
group_by str GROUPavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _GROUPBY_H */
