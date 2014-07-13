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
 * The functions in this file represent the BAM SQL library, that make
 * querying BAM data easier.
 */

#ifndef _BAM_LIB_H
#define _BAM_LIB_H

#include "mal_exception.h"

#ifdef WIN32
#ifndef LIBBAM
#define bam_export extern __declspec(dllimport)
#else
#define bam_export extern __declspec(dllexport)
#endif
#else
#define bam_export extern
#endif

bam_export str bam_flag(bit * ret, sht * flag, str * name);
bam_export str reverse_seq(str * ret, str * seq);
bam_export str reverse_qual(str * ret, str * qual);
bam_export str seq_length(int *ret, str * cigar);

bam_export str bam_flag_bat(bat * ret, bat * bid, str * name);
bam_export str reverse_seq_bat(bat * ret, bat * bid);
bam_export str reverse_qual_bat(bat * ret, bat * bid);
bam_export str seq_length_bat(bat * ret, bat * bid);

#endif
