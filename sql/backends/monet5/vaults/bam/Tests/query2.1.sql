WITH alig AS (
    SELECT qname, flag, seq, qual
    FROM bam.alignments_1
    WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
      AND bam.bam_flag(flag, 'seco_alig') = False
      AND mapq < 100
), alig_proj AS (
    SELECT qname, flag,
        CASE WHEN bam.bam_flag(flag, 'segm_reve') THEN bam.reverse_seq(seq)   ELSE seq  END AS seq,
        CASE WHEN bam.bam_flag(flag, 'segm_reve') THEN bam.reverse_qual(qual) ELSE qual END AS qual
    FROM alig
    WHERE qname IN (
        SELECT qname
        FROM alig
        GROUP BY qname
        HAVING COUNT(*) = 2
           AND SUM(bam.bam_flag(flag, 'firs_segm')) = 1
           AND SUM(bam.bam_flag(flag, 'last_segm')) = 1
        )
)
SELECT l.qname AS qname, l.seq AS l_seq, l.qual AS qual1, r.seq AS r_seq, r.qual AS qual2
FROM (
    SELECT *
    FROM alig_proj
    WHERE bam.bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT *
    FROM alig_proj
    WHERE bam.bam_flag(flag, 'last_segm') = True
) AS r 
    ON l.qname = r.qname
ORDER BY qname;

SELECT qname,
    CASE WHEN bam.bam_flag(l_flag, 'segm_reve') THEN bam.reverse_seq(l_seq)   ELSE l_seq  END AS l_seq,
    CASE WHEN bam.bam_flag(l_flag, 'segm_reve') THEN bam.reverse_qual(l_qual) ELSE l_qual END AS l_qual,
    CASE WHEN bam.bam_flag(r_flag, 'segm_reve') THEN bam.reverse_seq(r_seq)   ELSE r_seq  END AS r_seq,
    CASE WHEN bam.bam_flag(r_flag, 'segm_reve') THEN bam.reverse_qual(r_qual) ELSE r_qual END AS r_qual
FROM bam.paired_primary_alignments_3
WHERE l_mapq < 100
  AND r_mapq < 100
ORDER BY qname;
