/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * The code in this file represents the heart of the bam loader
 * functionality. It uses the functionality provided by the BAM
 * wrapper and the DB Interface, which can be found in respectively
 * bam_wrapper.[h|c] and bam_db_interface.[h|c].
 */

#ifndef _BAM_LOADER_H
#define _BAM_LOADER_H

#include "sql_execute.h"
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

bam_export str bam_loader_repos(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
				InstrPtr pci);
bam_export str bam_loader_files(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
				InstrPtr pci);
bam_export str bam_loader_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
				   InstrPtr pci);
bam_export str bam_drop_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
				 InstrPtr pci);
/* TODO Consider drop on file_location/repos bases */


#endif
