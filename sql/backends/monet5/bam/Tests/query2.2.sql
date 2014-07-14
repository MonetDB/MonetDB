WITH alig AS (
    SELECT qname, flag, rname, pos, cigar
    FROM bam.alignments_1
    WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
      AND bam.bam_flag(flag, 'seco_alig') = False
      AND qname IN (
        SELECT qname
        FROM bam.alignments_1
        WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
          AND bam.bam_flag(flag, 'seco_alig') = False
        GROUP BY qname
        HAVING COUNT(*) = 2
           AND SUM(bam.bam_flag(flag, 'firs_segm')) = 1
           AND SUM(bam.bam_flag(flag, 'last_segm')) = 1
     )
)
SELECT 
    CASE WHEN l.pos < r.pos 
         THEN r.pos - (l.pos + bam.seq_length(l.cigar))
         ELSE l.pos - (r.pos + bam.seq_length(r.cigar))
    END AS distance, 
    COUNT(*) AS nr_alignments
FROM (
    SELECT qname, rname, pos, cigar
    FROM alig
    WHERE bam.bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT qname, rname, pos, cigar
    FROM alig
    WHERE bam.bam_flag(flag, 'last_segm') = True
) AS r 
    ON l.qname = r.qname
   AND l.rname = r.rname
GROUP BY distance
ORDER BY nr_alignments DESC;

SELECT     
    CASE WHEN l_pos < r_pos 
         THEN r_pos - (l_pos + bam.seq_length(l_cigar))
         ELSE l_pos - (r_pos + bam.seq_length(r_cigar))
    END AS distance, 
    COUNT(*) AS nr_alignments
FROM bam.paired_primary_alignments_3
WHERE l_rname = r_rname
GROUP BY distance
ORDER BY nr_alignments DESC;
