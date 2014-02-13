WITH alig AS (
    SELECT qname, flag, rname, pos, cigar
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
      AND qname IN (
        SELECT qname
        FROM bam.intern_alignments_i
        WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
          AND bam_flag(flag, 'seco_alig') = False
        GROUP BY qname
        HAVING COUNT(*) = 2
           AND SUM(bam_flag(flag, 'firs_segm')) = 1
           AND SUM(bam_flag(flag, 'last_segm')) = 1
     )
)
SELECT 
    CASE WHEN l.pos < r.pos 
         THEN r.pos - (l.pos + seq_length(l.cigar))
         ELSE l.pos - (r.pos + seq_length(r.cigar))
    END AS distance, 
    COUNT(*) AS nr_alignments
FROM (
    SELECT qname, rname, pos, cigar
    FROM alig
    WHERE bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT qname, rname, pos, cigar
    FROM alig
    WHERE bam_flag(flag, 'last_segm') = True
) AS r 
    ON l.qname = r.qname
   AND l.rname = r.rname
GROUP BY distance
ORDER BY nr_alignments DESC;