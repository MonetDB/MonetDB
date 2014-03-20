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
 * (author) R Cijvat
 * The code in this file represents the heart of the bam loader functionality. It uses the functionality provided by
 * the BAM wrapper and the DB Interface, which can be found in respectively bam_wrapper.[h|c] and bam_db_interface.[h|c]. 
 */

#ifndef _BAM_LOADER_H
#define _BAM_LOADER_H

#include "sql_scenario.h"
#include "sql_mvc.h"

#ifdef WIN32
#ifndef LIBBAM
#define bam_export extern __declspec(dllimport)
#else
#define bam_export extern __declspec(dllexport)
#endif
#else
#define bam_export extern
#endif

bam_export str bam_loader_repos(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bam_export str bam_loader_files(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bam_export str bam_loader_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bam_export str bam_drop_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
/* TODO Consider drop on file_location/repos bases */


#endif
