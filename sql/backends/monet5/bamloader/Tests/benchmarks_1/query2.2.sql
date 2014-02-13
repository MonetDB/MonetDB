SELECT     
    CASE WHEN l_pos < r_pos 
         THEN r_pos - (l_pos + seq_length(l_cigar))
         ELSE l_pos - (r_pos + seq_length(r_cigar))
    END AS distance, 
    COUNT(*) AS nr_alignments
FROM bam.paired_primary_alignments_i
WHERE l_rname = r_rname
GROUP BY distance
ORDER BY nr_alignments DESC;
