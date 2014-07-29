WITH alig AS (
    SELECT *
    FROM bam.alignments_1
    WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
      AND rname = 'chr22'
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
SELECT l.qname AS qname, l.flag AS l_flag, l.rname AS l_rname, l.pos AS l_pos, l.mapq AS l_mapq, l.cigar AS l_cigar, 
    l.rnext AS l_rnext, l.pnext AS l_pnext, l.tlen AS l_tlen, l.seq AS l_seq, l.qual AS qual1, 
                          r.flag AS r_flag, r.rname AS r_rname, r.pos AS r_pos, r.mapq AS r_mapq, r.cigar AS r_cigar, 
    r.rnext AS r_rnext, r.pnext AS r_pnext, r.tlen AS r_tlen, r.seq AS r_seq, r.qual AS qual2
FROM (
    SELECT *
    FROM alig
    WHERE bam.bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT *
    FROM alig
    WHERE bam.bam_flag(flag, 'last_segm') = True
) AS r ON l.qname = r.qname
       AND CASE WHEN l.pos < r.pos
                THEN (80000000 >= l.pos + bam.seq_length(l.cigar) AND 80000000 < r.pos)
                ELSE (80000000 >= r.pos + bam.seq_length(r.cigar) AND 80000000 < l.pos)
           END
ORDER BY l_pos;

SELECT qname, l_flag, l_rname, l_pos, l_mapq, l_cigar, l_rnext, l_pnext, l_tlen, l_seq, l_qual,
              r_flag, r_rname, r_pos, r_mapq, r_cigar, r_rnext, r_pnext, r_tlen, r_seq, r_qual
FROM bam.paired_primary_alignments_3
WHERE l_rname = 'chr22'
  AND r_rname = 'chr22'
  AND CASE WHEN l_pos < r_pos
           THEN (80000000 >= l_pos + bam.seq_length(l_cigar) AND 80000000 < r_pos)
           ELSE (80000000 >= r_pos + bam.seq_length(r_cigar) AND 80000000 < l_pos)
      END
ORDER BY l_pos;
