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

bam_export str bam_flag(bit *ret, sht *flag, str *name);
bam_export str reverse_seq(str *ret, str *seq);
bam_export str reverse_qual(str *ret, str *qual);
bam_export str seq_length(int *ret, str *cigar);

bam_export str bam_flag_bat(bat *ret, bat *bid, str *name);
bam_export str reverse_seq_bat(bat *ret, bat *bid);
bam_export str reverse_qual_bat(bat *ret, bat *bid);
bam_export str seq_length_bat(bat *ret, bat *bid);

#endif
