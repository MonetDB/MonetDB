SELECT qname,
    CASE WHEN bam_flag(l_flag, 'segm_reve') THEN reverse_seq(l_seq)   ELSE l_seq  END AS l_seq,
    CASE WHEN bam_flag(l_flag, 'segm_reve') THEN reverse_qual(l_qual) ELSE l_qual END AS l_qual,
    CASE WHEN bam_flag(r_flag, 'segm_reve') THEN reverse_seq(r_seq)   ELSE r_seq  END AS r_seq,
    CASE WHEN bam_flag(r_flag, 'segm_reve') THEN reverse_qual(r_qual) ELSE r_qual END AS r_qual
FROM bam.paired_primary_alignments_i
WHERE l_mapq < mapq_2_1
  AND r_mapq < mapq_2_1
ORDER BY qname;