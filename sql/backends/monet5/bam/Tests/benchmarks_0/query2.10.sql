WITH alig AS (
    SELECT *
    FROM bam.alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND rname = rname_2_10
      AND bam_flag(flag, 'seco_alig') = False
      AND qname IN (
        SELECT qname
        FROM bam.alignments_i
        WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
          AND bam_flag(flag, 'seco_alig') = False
        GROUP BY qname
        HAVING COUNT(*) = 2
           AND SUM(bam_flag(flag, 'firs_segm')) = 1
           AND SUM(bam_flag(flag, 'last_segm')) = 1
     )
)
SELECT l.qname AS qname, l.flag AS l_flag, l.rname AS l_rname, l.pos AS l_pos, l.mapq AS l_mapq, l.cigar AS l_cigar, 
    l.rnext AS l_rnext, l.pnext AS l_pnext, l.tlen AS l_tlen, l.seq AS l_seq, l.qual AS qual1, 
                          r.flag AS r_flag, r.rname AS r_rname, r.pos AS r_pos, r.mapq AS r_mapq, r.cigar AS r_cigar, 
    r.rnext AS r_rnext, r.pnext AS r_pnext, r.tlen AS r_tlen, r.seq AS r_seq, r.qual AS qual2
FROM (
    SELECT *
    FROM alig
    WHERE bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT *
    FROM alig
    WHERE bam_flag(flag, 'last_segm') = True
) AS r ON l.qname = r.qname
       AND CASE WHEN l.pos < r.pos
                THEN (pos_2_10 >= l.pos + seq_length(l.cigar) AND pos_2_10 < r.pos)
                ELSE (pos_2_10 >= r.pos + seq_length(r.cigar) AND pos_2_10 < l.pos)
           END
ORDER BY l_pos;
