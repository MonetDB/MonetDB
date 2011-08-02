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
 * Performance experimentation on Eir on Dec 17, 2010.
 * The code base uses the SQLstatementIntern to populate both the catalog and the mseed files.
 * Experiment INSERTLOAD concerns loading the mseedN file using a sequence of inserts. This involves
 * the complete code path from generating SQL statement, parsing, optimization and execution.
 * No query caching is enabled. The first record from
 * ftp://www.orfeus-eu.org/pub/data/POND/1990/002/MN_BNI__BHN.1990.002.20.21.32.mseed
 * with 3768 events was loaded in 10.770 msec
 * (Total would be around 260 sec, but there are leaks)
 *
 * Experiment COPYLOAD produced a copy file, separately loaded into MonetDB using
 * copy 400000 records into mseed2 from '/tmp/data/' delimiters ',','\n';
 * this track leads to a load time of 326ms for the first record.
 * The first file with 26 records and 97592 tuples was loaded this way within 8710 msec.
 * This approach still uses the SQL logger. The expected speedup for LOCKED copy into would
 * be something like a factor 3.
 *
 * Experiment ATTACHLOAD produced the single binary BAT images followed by
 * copy into mseed2 from ('/tmp/time','/tmp/data');
 * Attach procedure for complete file ran in about 232 ms, which excludes
 * a forced saving of the state on disk.
 *
 * Experiment MANUALLOAD generates a file with insert statements, which is executed manually
 * against mclient. The total time for this first batch is 34 sec.
 * This shows the gains from cached plans. (compared to experiment 0)
 *
 * The database explosion factor is 10.
 * -rw-r--r-- 1 mk ins 721464 2010-12-18 10:01 4156.tail
 * -rw-r--r-- 1 mk ins 360732 2010-12-18 10:01 4272.tail
 *
 *
 * The current ORFEUS repository is ca 3.2M files (ca 350GB). Excluding ftp transfer cost,
 * the following optimistic load times are foreseen:
 * Experiment  Loadtime
 * INSERTLOAD	28 yrs
 * COPYLOAD	322 days (107 days with LOCKED)
 * ATTACHLOAD   10 days (37days at 1sec)
 * MANUALLOAD	3.2 yrs
 *
 * Caveat. The way SQL works, leads to clearing the cache whenever we change the catalog.
 * Using private mseed files, leads to this situation. However, clearing the cache is
 * not reselient fo the context. This means that we may shoot in the foot, removing
 * the plan we are just interpreting.
 */
#ifndef _MSEED_H
#define _MSEED_H

#include "clients.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "mtime.h"
#include "libmseed.h"

#ifdef WIN32
#ifndef LIBMSEED
#define vault_export extern __declspec(dllimport)
#else
#define vault_export extern __declspec(dllexport)
#endif
#else
#define vault_export extern
#endif

vault_export str MseedImport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
vault_export str MseedLoadSQL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
vault_export str MseedLoad(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define _MSEED_DEBUG_

#endif /* _MSEED_H */
