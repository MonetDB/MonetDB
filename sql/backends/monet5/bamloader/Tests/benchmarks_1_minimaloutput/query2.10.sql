SELECT l_pos, l_virtual_offset, r_pos, r_virtual_offset
FROM bam.paired_primary_alignments_i
WHERE l_rname = rname_2_10
  AND r_rname = rname_2_10
  AND CASE WHEN l_pos < r_pos
           THEN (pos_2_10 >= l_pos + seq_length(l_cigar) AND pos_2_10 < r_pos)
           ELSE (pos_2_10 >= r_pos + seq_length(r_cigar) AND pos_2_10 < l_pos)
      END
ORDER BY l_pos