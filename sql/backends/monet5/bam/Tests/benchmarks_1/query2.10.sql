SELECT qname, l_flag, l_rname, l_pos, l_mapq, l_cigar, l_rnext, l_pnext, l_tlen, l_seq, l_qual,
              r_flag, r_rname, r_pos, r_mapq, r_cigar, r_rnext, r_pnext, r_tlen, r_seq, r_qual
FROM bam.paired_primary_alignments_i
WHERE l_rname = rname_2_10
  AND r_rname = rname_2_10
  AND CASE WHEN l_pos < r_pos
           THEN (pos_2_10 >= l_pos + seq_length(l_cigar) AND pos_2_10 < r_pos)
           ELSE (pos_2_10 >= r_pos + seq_length(r_cigar) AND pos_2_10 < l_pos)
      END
ORDER BY l_pos;
