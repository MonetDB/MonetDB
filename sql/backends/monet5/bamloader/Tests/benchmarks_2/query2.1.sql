WITH alig AS (
    SELECT virtual_offset, qname, flag
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
      AND mapq < mapq_2_1
), alig_proj AS (
    SELECT qname, flag,
        CASE WHEN bam_flag(flag, 'segm_reve') THEN reverse_seq(seq)   ELSE seq  END AS seq,
        CASE WHEN bam_flag(flag, 'segm_reve') THEN reverse_qual(qual) ELSE qual END AS qual
    FROM alig 
        JOIN bam.extern_alignments_i AS extern
        ON alig.virtual_offset = extern.virtual_offset
    WHERE qname IN (
        SELECT qname
        FROM alig
        GROUP BY qname
        HAVING COUNT(*) = 2
           AND SUM(bam_flag(flag, 'firs_segm')) = 1
           AND SUM(bam_flag(flag, 'last_segm')) = 1
        )
)
SELECT l.qname AS qname, l.seq AS l_seq, l.qual AS qual1, r.seq AS r_seq, r.qual AS qual2
FROM (
    SELECT *
    FROM alig_proj
    WHERE bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT *
    FROM alig_proj
    WHERE bam_flag(flag, 'last_segm') = True
) AS r 
    ON l.qname = r.qname
ORDER BY qname;