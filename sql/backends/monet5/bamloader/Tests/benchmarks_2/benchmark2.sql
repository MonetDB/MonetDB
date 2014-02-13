--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.1 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

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
SELECT l.qname AS qname, l.seq AS l_seq, l.qual AS l_qual, r.seq AS r_seq, r.qual AS r_qual
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











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.2 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

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











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.3 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i 
WHERE qname IN (
    SELECT qname
    FROM bam.intern_alignments_i
    GROUP BY qname
    HAVING SUM(bam_flag(flag, 'firs_segm')) = 0
        OR SUM(bam_flag(flag, 'last_segm')) = 0
)
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.4 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE qname IN (
    SELECT qname
    FROM (
        SELECT qname, bam_flag(flag, 'seco_alig') AS seco_alig, bam_flag(flag, 'segm_unma') AS segm_unma, 
            bam_flag(flag, 'firs_segm') AS firs_segm
        FROM bam.intern_alignments_i
        WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
    ) AS qnames
    GROUP BY qname, firs_segm
    HAVING SUM(segm_unma) < COUNT(*)
       AND (COUNT(*) - SUM(seco_alig)) <> 1
)
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.5 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH qnames1 AS (
    SELECT DISTINCT qname
    FROM bam.intern_alignments_i
), qnames2 AS (
    SELECT DISTINCT qname
    FROM bam.intern_alignments_j
)
SELECT count_a_insct_b, count_qnames1 - count_a_insct_b AS count_a_minus_b, count_qnames2 - count_a_insct_b AS count_b_minus_a
FROM (
    SELECT COUNT(*) AS count_a_insct_b
    FROM (
        SELECT *
        FROM qnames1
        INTERSECT
        SELECT *
        FROM qnames2
    ) AS a_insct_b
) AS insct_sub, (
    SELECT COUNT(*) AS count_qnames1
    FROM qnames1
) AS qnames1_sub, (
    SELECT COUNT(*) AS count_qnames2
    FROM qnames2
) AS qnames2_sub;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.6 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH qnames_insct AS (
    SELECT distinct qname
    FROM bam.intern_alignments_i
    INTERSECT
    SELECT distinct qname
    FROM bam.intern_alignments_j
)
SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
UNION
SELECT 'f2', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_j
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.7 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE qname IN (
    SELECT DISTINCT qname
    FROM bam.intern_alignments_i
    EXCEPT
    SELECT DISTINCT qname
    FROM bam.intern_alignments_j
)
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.8 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT i_f1.qname AS qname, i_f1.flag AS f1_flag, i_f1.rname AS f1_rname, i_f1.pos AS f1_pos, i_f1.mapq AS f1_mapq, i_f1.cigar AS f1_cigar, 
    i_f1.rnext AS f1_rnext, i_f1.pnext AS f1_pnext, i_f1.tlen AS f1_tlen, e_f1.seq AS f1_seq, e_f1.qual AS f1_qual, 
                          i_f2.flag AS f2_flag, i_f2.rname AS f2_rname, i_f2.pos AS f2_pos, i_f2.mapq AS f2_mapq, i_f2.cigar AS f2_cigar, 
    i_f2.rnext AS f2_rnext, i_f2.pnext AS f2_pnext, i_f2.tlen AS f2_tlen, e_f2.seq AS f2_seq, e_f2.qual AS f2_qual
FROM (
(
    SELECT *
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS i_f1 JOIN (
    SELECT *
    FROM bam.intern_alignments_j
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS i_f2 
    ON  i_f1.qname = i_f2.qname
    AND bam_flag(i_f1.flag, 'firs_segm') = bam_flag(i_f2.flag, 'firs_segm')
    AND (i_f1.rname <> i_f2.rname OR i_f1.pos <> i_f2.pos)
) JOIN bam.extern_alignments_i AS e_f1
    ON i_f1.virtual_offset = e_f1.virtual_offset
  JOIN bam.extern_alignments_j AS e_f2
    ON i_f2.virtual_offset = e_f2.virtual_offset
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.9 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE rname = rname_2_9
  AND pos_2_9 >= pos
  AND pos_2_9 < pos + seq_length(cigar)
ORDER BY pos;




	





--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.10 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT *
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND rname = rname_2_10
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
SELECT l.qname AS qname, l.flag AS l_flag, l.rname AS l_rname, l.pos AS l_pos, l.mapq AS l_mapq, l.cigar AS l_cigar, 
    l.rnext AS l_rnext, l.pnext AS l_pnext, l.tlen AS l_tlen, e_l.seq AS l_seq, e_l.qual AS l_qual, 
                          r.flag AS r_flag, r.rname AS r_rname, r.pos AS r_pos, r.mapq AS r_mapq, r.cigar AS r_cigar, 
    r.rnext AS r_rnext, r.pnext AS r_pnext, r.tlen AS r_tlen, e_r.seq AS r_seq, e_r.qual AS r_qual
FROM (
(
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
) JOIN bam.extern_alignments_i AS e_l
    ON l.virtual_offset = e_l.virtual_offset
  JOIN bam.extern_alignments_i AS e_r
    ON r.virtual_offset = e_r.virtual_offset
ORDER BY l_pos;











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.11 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT *
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext <> '*'
      AND pnext > 0
)
SELECT l.qname AS qname, l.flag AS l_flag, l.rname AS l_rname, l.pos AS l_pos, l.mapq AS l_mapq, l.cigar AS l_cigar, 
    l.rnext AS l_rnext, l.pnext AS l_pnext, l.tlen AS l_tlen, e_l.seq AS l_seq, e_l.qual AS l_qual, 
                         r.flag AS r_flag, r.rname AS r_rname, r.pos AS r_pos, r.mapq AS r_mapq, r.cigar AS r_cigar, 
    r.rnext AS r_rnext, r.pnext AS r_pnext, r.tlen AS r_tlen, e_r.seq AS r_seq, e_r.qual AS r_qual
FROM (
(
    SELECT *
    FROM alig
    WHERE bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT *
    FROM alig
    WHERE bam_flag(flag, 'last_segm') = True
) AS r
    ON l.qname = r.qname
   AND ((l.rnext = '=' AND l.rname = r.rname) OR l.rnext = r.rname)
   AND l.pnext = r.pos
   AND ((r.rnext = '=' AND l.rname = r.rname) OR r.rnext = l.rname)
   AND r.pnext = l.pos
) JOIN bam.extern_alignments_i AS e_l
    ON l.virtual_offset = e_l.virtual_offset
  JOIN bam.extern_alignments_i AS e_r
    ON r.virtual_offset = e_r.virtual_offset
ORDER BY l.qname;











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.12 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT *
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext = '*'
      AND pnext = 0
)
SELECT qname, rname, l_flag, l_pos, l_mapq, l_cigar, l_rnext, l_pnext, l_tlen, e_l.seq AS l_seq, e_l.qual AS l_qual,
                     r_flag, r_pos, r_mapq, r_cigar, r_rnext, r_pnext, r_tlen, e_r.seq AS r_seq, e_r.qual AS r_qual
FROM (
    SELECT l.qname AS qname, l.rname AS rname, l.flag AS l_flag, l.pos AS l_pos, l.mapq AS l_mapq, l.cigar AS l_cigar, 
        l.rnext AS l_rnext, l.pnext AS l_pnext, l.tlen AS l_tlen, 
                                                 r.flag AS r_flag, r.pos AS r_pos, r.mapq AS r_mapq, r.cigar AS r_cigar, 
        r.rnext AS r_rnext, r.pnext AS r_pnext, r.tlen AS r_tlen
        CASE WHEN l.pos < r.pos 
             THEN r.pos - (l.pos + seq_length(l.cigar))
             ELSE l.pos - (r.pos + seq_length(r.cigar))
        END AS distance
    FROM (
        SELECT *
        FROM alig
        WHERE bam_flag(flag, 'firs_segm') = True
    ) AS l JOIN (
        SELECT *
        FROM alig
        WHERE bam_flag(flag, 'last_segm') = True
    ) AS r 
        ON  l.qname = r.qname
        AND l.rname = r.rname
    WHERE distance > 0
      AND distance < distance_2_12
) JOIN bam.extern_alignments_i AS e_l
    ON l.virtual_offset = e_l.virtual_offset
  JOIN bam.extern_alignments_i AS e_r
    ON r.virtual_offset = e_r.virtual_offset
ORDER BY rname;
