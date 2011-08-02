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

#ifndef _RUN_OCTOPUS
#define _RUN_OCTOPUS
#include "mal.h"
#include "mal_instruction.h"
#include "mal_client.h"

	/*#define DEBUG_RUN_OCTOPUS 	to trace processing */

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define octopus_export extern __declspec(dllimport)
#else
#define octopus_export extern __declspec(dllexport)
#endif
#else
#define octopus_export extern
#endif

octopus_export str OCTOPUSrun(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
octopus_export str OCTOPUSdiscoverRegister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
octopus_export str OCTOPUSregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
octopus_export str OCTOPUSbidding(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
octopus_export str OCTOPUSmakeSchedule(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
octopus_export str OCTOPUSconnect(str *c, str *dbname);
octopus_export str OCTOPUSgetVersion(int *res);
#endif /* MAL_RUN_OCTOPUS */

