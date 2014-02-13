--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.3 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, virtual_offset
FROM bam.intern_alignments_i 
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

SELECT qname, virtual_offset
FROM bam.intern_alignments_i
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
------------------------------------------------------------- Query 2.6 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH qnames_insct AS (
    SELECT distinct qname
    FROM bam.intern_alignments_i
    INTERSECT
    SELECT distinct qname
    FROM bam.intern_alignments_j
)
SELECT 'f1', qname, virtual_offset
FROM bam.intern_alignments_i
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
UNION
SELECT 'f2', qname, virtual_offset
FROM bam.intern_alignments_j
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.7 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT 'f1', qname, virtual_offset
FROM bam.intern_alignments_i
WHERE qname IN (
    SELECT distinct qname
    FROM bam.intern_alignments_i
    EXCEPT
    SELECT distinct qname
    FROM bam.intern_alignments_j
)
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.8 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT f1.qname AS qname, f1.virtual_offset AS f1_virtual_offset, f2.virtual_offset AS f2_virtual_offset
FROM (
    SELECT qname, flag, rname, pos, virtual_offset
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS f1 JOIN (
    SELECT qname, flag, rname, pos, virtual_offset
    FROM bam.intern_alignments_j
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS f2 
    ON  f1.qname = f2.qname
    AND bam_flag(f1.flag, 'firs_segm') = bam_flag(f2.flag, 'firs_segm')
    AND (f1.rname <> f2.rname OR f1.pos <> f2.pos)
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.9 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT pos, virtual_offset
FROM bam.intern_alignments_i
WHERE rname = rname_2_9
  AND pos_2_9 >= pos
  AND pos_2_9 < pos + seq_length(cigar)
ORDER BY pos;



	





--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.10 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT qname, flag, pos, cigar, virtual_offset
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
SELECT l.pos AS l_pos, l.virtual_offset AS l_virtual_offset, r.pos AS r_pos, r.virtual_offset AS r_virtual_offset
FROM (
    SELECT qname, pos, cigar, virtual_offset
    FROM alig
    WHERE bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT qname, pos, cigar, virtual_offset
    FROM alig
    WHERE bam_flag(flag, 'last_segm') = True
) AS r ON l.qname = r.qname
       AND CASE WHEN l.pos < r.pos
                THEN (pos_2_10 >= l.pos + seq_length(l.cigar) AND pos_2_10 < r.pos)
                ELSE (pos_2_10 >= r.pos + seq_length(r.cigar) AND pos_2_10 < l.pos)
           END
ORDER BY l_pos;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.11 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT qname, flag, rname, pos, rnext, pnext, virtual_offset
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext <> '*'
      AND pnext > 0
)
SELECT l.qname AS qname, l.virtual_offset AS l_virtual_offset, r.virtual_offset AS r_virtual_offset
FROM (
    SELECT qname, rname, pos, rnext, pnext, virtual_offset
    FROM alig
    WHERE bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT qname, rname, pos, rnext, pnext, virtual_offset
    FROM alig
    WHERE bam_flag(flag, 'last_segm') = True
) AS r
    ON l.qname = r.qname
   AND ((l.rnext = '=' AND l.rname = r.rname) OR l.rnext = r.rname)
   AND l.pnext = r.pos
   AND ((r.rnext = '=' AND l.rname = r.rname) OR r.rnext = l.rname)
   AND r.pnext = l.pos
ORDER BY l.qname;











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.12 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT qname, flag, rname, pos, cigar, virtual_offset
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext = '*'
      AND pnext = 0
)
SELECT rname, distance, l_virtual_offset, r_virtual_offset
FROM (
    SELECT l.rname AS rname, 
        CASE WHEN l.pos < r.pos 
             THEN r.pos - (l.pos + seq_length(l.cigar))
             ELSE l.pos - (r.pos + seq_length(r.cigar))
        END AS distance,
        l.virtual_offset AS l_virtual_offset, r.virtual_offset AS r_virtual_offset
    FROM (
        SELECT qname, rname, pos, cigar, virtual_offset
        FROM alig
        WHERE bam_flag(flag, 'firs_segm') = True
    ) AS l JOIN (
        SELECT qname, rname, pos, cigar, virtual_offset
        FROM alig
        WHERE bam_flag(flag, 'last_segm') = True
    ) AS r 
        ON  l.qname = r.qname
        AND l.rname = r.rname
) AS alig_joined
WHERE distance > 0
  AND distance < distance_2_12
ORDER BY rname;
