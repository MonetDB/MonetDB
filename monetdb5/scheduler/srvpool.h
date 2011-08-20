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

/* Author(s) M.Ivanova, M.Kersten
 * The srvpool abstracts away specifics from octopus and centipede
 * to reach a re-useable framework for ochestrating multiple mservers
 * an execute distributed queries.
*/
#ifndef _RUN_SRVPOOL
#define _RUN_SRVPOOL
#include "mal.h"
#include "mal_instruction.h"
#include "mal_client.h"

#define DEBUG_RUN_SRVPOOL 	/* to trace processing */

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define octopus_export extern __declspec(dllimport)
#else
#define mpool_export extern __declspec(dllexport)
#endif
#else
#define mpool_export extern
#endif

mpool_export str SRVPOOLscheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mpool_export str SRVPOOLexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mpool_export str SRVPOOLregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mpool_export str SRVPOOLserver(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mpool_export str SRVPOOLreset(int *ret);
mpool_export str SRVPOOLconnect(str *c, str *dbname);
mpool_export str SRVsetServers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* MAL_RUN_SRVPOOL */

