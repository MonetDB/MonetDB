SET SCHEMA bam;

# BAM_FLAG

# Should all give 'true'
SELECT bam_flag(1, 'mult_segm');
SELECT bam_flag(2, 'prop_alig');
SELECT bam_flag(4, 'segm_unma');
SELECT bam_flag(8, 'next_unma');
SELECT bam_flag(16, 'segm_reve');
SELECT bam_flag(32, 'next_reve');
SELECT bam_flag(64, 'firs_segm');
SELECT bam_flag(128, 'last_segm');
SELECT bam_flag(256, 'seco_alig');
SELECT bam_flag(512, 'qual_cont');
SELECT bam_flag(1024, 'opti_dupl');
SELECT bam_flag(2048, 'supp_alig');

# Should all give 'false'
SELECT bam_flag(2, 'mult_segm');
SELECT bam_flag(4, 'prop_alig');
SELECT bam_flag(8, 'segm_unma');
SELECT bam_flag(16, 'next_unma');
SELECT bam_flag(4, 'segm_reve');
SELECT bam_flag(2, 'next_reve');
SELECT bam_flag(4, 'firs_segm');
SELECT bam_flag(8, 'last_segm');
SELECT bam_flag(16, 'seco_alig');
SELECT bam_flag(32, 'qual_cont');
SELECT bam_flag(64, 'opti_dupl');
SELECT bam_flag(128, 'supp_alig');

# Should all give 'true'
SELECT bam_flag(2047, 'mult_segm');
SELECT bam_flag(2047, 'prop_alig');
SELECT bam_flag(2047, 'segm_unma');
SELECT bam_flag(2047, 'next_unma');
SELECT bam_flag(2047, 'segm_reve');
SELECT bam_flag(2047, 'next_reve');
SELECT bam_flag(2047, 'firs_segm');
SELECT bam_flag(2047, 'last_segm');
SELECT bam_flag(2047, 'seco_alig');
SELECT bam_flag(2047, 'qual_cont');
SELECT bam_flag(2047, 'opti_dupl');
SELECT bam_flag(4095, 'supp_alig');

# Should fail
SELECT bam_flag(111, 'Fail-hard');

# Bulk
SELECT flag,
	bam_flag(flag, 'mult_segm') AS mult_segm,
	bam_flag(flag, 'prop_alig') AS prop_alig,
	bam_flag(flag, 'segm_unma') AS segm_unma,
	bam_flag(flag, 'next_unma') AS next_unma,
	bam_flag(flag, 'segm_reve') AS segm_reve,
	bam_flag(flag, 'next_reve') AS next_reve,
	bam_flag(flag, 'firs_segm') AS firs_segm,
	bam_flag(flag, 'last_segm') AS last_segm,
	bam_flag(flag, 'seco_alig') AS seco_alig,
	bam_flag(flag, 'qual_cont') AS qual_cont,
	bam_flag(flag, 'opti_dupl') AS opti_dupl,
	bam_flag(flag, 'supp_alig') AS supp_alig
FROM bam.alignments_1;




# REVERSE_SEQ
SELECT reverse_seq('A');
SELECT reverse_seq('T');
SELECT reverse_seq('C');
SELECT reverse_seq('G');
SELECT reverse_seq('R');
SELECT reverse_seq('Y');
SELECT reverse_seq('S');
SELECT reverse_seq('W');
SELECT reverse_seq('K');
SELECT reverse_seq('M');
SELECT reverse_seq('H');
SELECT reverse_seq('D');
SELECT reverse_seq('V');
SELECT reverse_seq('B');
SELECT reverse_seq('N');
SELECT reverse_seq('ATCGRYSWKMHDVBN');
SELECT reverse_seq('invalidchars');

# Bulk
SELECT seq, reverse_seq(seq) AS reverse_seq
FROM bam.alignments_1;


# REVERSE_QUAL
SELECT reverse_qual('Should be reversed');

# Bulk
SELECT qual, reverse_qual(qual) AS reverse_qual
FROM bam.alignments_1;




# SEQ_LENGTH
SELECT seq_length('18M');
SELECT seq_length('3I5D6N');
SELECT seq_length('3=1X1=1X43=1X16=1X33=');

# Bulk
SELECT cigar, seq_length(cigar) AS seq_length
FROM bam.alignments_1;



# SEQ_CHAR

# Some simple cases
SELECT seq_char(5000, 'ACTGAG', 0, '6M');
SELECT seq_char(5000, 'ACTGAG', 4995, '6M');
SELECT seq_char(5000, 'ACTGAG', 4994, '6M');
SELECT seq_char(5000, 'ACTGAG', 5000, '6M');
SELECT seq_char(5000, 'ACTGAG', 5001, '6M');

# Cases inspired by http://genome.sph.umich.edu/wiki/SAM#What_is_a_CIGAR.3F
SELECT seq_char(7, 'ACTAGAATGGCT', 5, '3M1I3M1D5M');
SELECT seq_char(8, 'ACTAGAATGGCT', 5, '3M1I3M1D5M');
SELECT seq_char(11, 'ACTAGAATGGCT', 5, '3M1I3M1D5M');
SELECT seq_char(16, 'ACTAGAATGGCT', 5, '3M1I3M1D5M');
SELECT seq_char(17, 'ACTAGAATGGCT', 5, '3M1I3M1D5M');

# Bulk
SELECT 17922987 AS ref_pos, seq, pos, cigar, seq_char(17922987, seq, pos, cigar) AS seq_char
FROM bam.alignments_1
WHERE seq_char(17922987, seq, pos, cigar) IS NOT NULL;
