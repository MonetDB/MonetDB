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
#ifndef _OPT_CENTIPEDE_
#define _OPT_CENTIPEDE_
#include "opt_prelude.h"
#include "opt_support.h"

#define MAXSITES 4		/* to be refined */

opt_export str OPTvector(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
opt_export str OPTvectorOid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
opt_export str OPTcentipedeMaterialize(int *result, int *bid, ptr low, ptr high);
opt_export str OPTpeers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
opt_export int OPTcentipedeImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define LOCAL_EXECUTION	/* or REMOTE_EXECUTION */

#define OPTDEBUGcentipede  if ( optDebug & ((lng)1 <<DEBUG_OPT_CENTIPEDE) )
#endif
