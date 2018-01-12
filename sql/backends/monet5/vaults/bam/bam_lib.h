/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
bam_export str seq_char(str * ret, int * ref_pos, str * alg_seq, int * alg_pos, str * alg_cigar);

bam_export str bam_flag_bat(bat * ret, bat * bid, str * name);
bam_export str reverse_seq_bat(bat * ret, bat * bid);
bam_export str reverse_qual_bat(bat * ret, bat * bid);
bam_export str seq_length_bat(bat * ret, bat * bid);
bam_export str seq_char_bat(bat * ret, int * ref_pos, bat * alg_seq, bat * alg_pos, bat * alg_cigar);

#endif
