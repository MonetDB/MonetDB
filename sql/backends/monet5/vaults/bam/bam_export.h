/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * The code in this file defines export functionality, that enables
 * users to write data from the bam.export table to a SAM or BAM file.
 * After exporting data, the exporting functions will automatically
 * empty the bam.export table.
 */

#ifndef _BAM_EXPORT_H
#define _BAM_EXPORT_H

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


bam_export str sam_exportf(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
				InstrPtr pci);
bam_export str bam_exportf(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
				InstrPtr pci);



#endif
