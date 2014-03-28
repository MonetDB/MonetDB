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
 * The code in this file defines export functionality, that enables
 * users to write data from the bam.export table to a SAM or BAM file.
 * After exporting data, the exporting functions will automatically
 * empty the bam.export table.
 */
 
#include "monetdb_config.h"
#include "bam_export.h"

str 
sam_export(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    /* arg 1: path to desired output file */
	str output_path = *(str *) getArgReference(stk, pci, pci->retc);
	
    (void)output_path;
    (void)cntxt;
    (void)mb;
    (void)stk;
    (void)pci;
    throw(MAL, "sam_export", "Not implemented yet...");
}



str 
bam_export(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    /* arg 1: path to desired output file */
	str output_path = *(str *) getArgReference(stk, pci, pci->retc);

    (void)output_path;
    (void)cntxt;
    (void)mb;
    (void)stk;
    (void)pci;
    throw(MAL, "bam_export", "Not implemented yet...");
}

