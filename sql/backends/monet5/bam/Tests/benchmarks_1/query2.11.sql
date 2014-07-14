SELECT qname, l_flag, l_rname, l_pos, l_mapq, l_cigar, l_rnext, l_pnext, l_tlen, l_seq, l_qual,
              r_flag, r_rname, r_pos, r_mapq, r_cigar, r_rnext, r_pnext, r_tlen, r_seq, r_qual
FROM bam.paired_secondary_alignments_i
ORDER BY qname;
