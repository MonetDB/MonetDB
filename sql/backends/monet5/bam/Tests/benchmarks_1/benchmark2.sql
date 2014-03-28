--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.1 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------


SELECT qname,
    CASE WHEN bam_flag(l_flag, 'segm_reve') THEN reverse_seq(l_seq)   ELSE l_seq  END AS l_seq,
    CASE WHEN bam_flag(l_flag, 'segm_reve') THEN reverse_qual(l_qual) ELSE l_qual END AS l_qual,
    CASE WHEN bam_flag(r_flag, 'segm_reve') THEN reverse_seq(r_seq)   ELSE r_seq  END AS r_seq,
    CASE WHEN bam_flag(r_flag, 'segm_reve') THEN reverse_qual(r_qual) ELSE r_qual END AS r_qual
FROM bam.paired_primary_alignments_i
WHERE l_mapq < mapq_2_1
  AND r_mapq < mapq_2_1
ORDER BY qname;











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.2 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

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











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.3 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE qname IN (
    SELECT qname
    FROM bam.unpaired_all_alignments_i
    GROUP BY qname
    HAVING SUM(bam_flag(flag, 'firs_segm')) = 0
        OR SUM(bam_flag(flag, 'last_segm')) = 0
)
ORDER BY qname;










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.4 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE qname IN (
    SELECT qname
    FROM (
        SELECT qname, bam_flag(flag, 'seco_alig') AS seco_alig, bam_flag(flag, 'segm_unma') AS segm_unma, 
            bam_flag(flag, 'firs_segm') AS firs_segm
        FROM bam.unpaired_all_alignments_i
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
    FROM bam.unpaired_all_alignments_i
), qnames2 AS (
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_j
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

-- Description:
-- Collects some set statistics from two BAM files, based on the qnames that are in these files:
-- * Intersection: The number of qnames that appear in both files
-- * Set minus: The number of qnames that appear in file 1 but not in file 2
-- * Set minus: The number of qnames that appear in file 2 but not in file 1










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.6 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH qnames_insct AS (
    SELECT distinct qname
    FROM bam.unpaired_all_alignments_i
    INTERSECT
    SELECT distinct qname
    FROM bam.unpaired_all_alignments_j
)
SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
UNION
SELECT 'f2', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_j
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
ORDER BY qname;









--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.7 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE qname IN (
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_i
    EXCEPT
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_j
)
ORDER BY qname;









--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.8 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT f1.qname AS qname, f1.flag AS f1_flag, f1.rname AS f1_rname, f1.pos AS f1_pos, f1.mapq AS f1_mapq, f1.cigar AS f1_cigar, 
    f1.rnext AS f1_rnext, f1.pnext AS f1_pnext, f1.tlen AS f1_tlen, f1.seq AS f1_seq, f1.qual AS f1_qual, 
                          f2.flag AS f2_flag, f2.rname AS f2_rname, f2.pos AS f2_pos, f2.mapq AS f2_mapq, f2.cigar AS f2_cigar, 
    f2.rnext AS f2_rnext, f2.pnext AS f2_pnext, f2.tlen AS f2_tlen, f2.seq AS f2_seq, f2.qual AS f2_qual
FROM (
   SELECT *
   FROM bam.unpaired_primary_alignments_i
) AS f1 JOIN (
    SELECT *
   FROM bam.unpaired_primary_alignments_j
) AS f2 
    ON  f1.qname = f2.qname
    AND bam_flag(f1.flag, 'firs_segm') = bam_flag(f2.flag, 'firs_segm')
    AND (f1.rname <> f2.rname OR f1.pos <> f2.pos)
ORDER BY qname;











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.9 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE rname = rname_2_9
  AND pos_2_9 >= pos
  AND pos_2_9 < pos + seq_length(cigar)
ORDER BY pos;




	





--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.10 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, l_flag, l_rname, l_pos, l_mapq, l_cigar, l_rnext, l_pnext, l_tlen, l_seq, l_qual,
              r_flag, r_rname, r_pos, r_mapq, r_cigar, r_rnext, r_pnext, r_tlen, r_seq, r_qual
FROM bam.paired_primary_alignments_i
WHERE l_rname = rname_2_10
  AND r_rname = rname_2_10
  AND CASE WHEN l_pos < r_pos
           THEN (pos_2_10 >= l_pos + seq_length(l_cigar) AND pos_2_10 < r_pos)
           ELSE (pos_2_10 >= r_pos + seq_length(r_cigar) AND pos_2_10 < l_pos)
      END
ORDER BY l_pos










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.11 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, l_flag, l_rname, l_pos, l_mapq, l_cigar, l_rnext, l_pnext, l_tlen, l_seq, l_qual,
              r_flag, r_rname, r_pos, r_mapq, r_cigar, r_rnext, r_pnext, r_tlen, r_seq, r_qual
FROM bam.paired_secondary_alignments_i
ORDER BY qname;

-- Description:
-- Optimized version of query 11a, by doing some filtering in the WITH clause.
-- Tests will have to prove which of the two queries runs faster.










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.12 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT *
    FROM bam.unpaired_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext = '*'
      AND pnext = 0
)
SELECT *
FROM (
    SELECT l.qname AS qname, l.rname AS rname, l.flag AS l_flag, l.pos AS l_pos, l.mapq AS l_mapq, l.cigar AS l_cigar, 
        l.rnext AS l_rnext, l.pnext AS l_pnext, l.tlen AS l_tlen, l.seq AS l_seq, l.qual AS l_qual, 
                                                 r.flag AS r_flag, r.pos AS r_pos, r.mapq AS r_mapq, r.cigar AS r_cigar, 
        r.rnext AS r_rnext, r.pnext AS r_pnext, r.tlen AS r_tlen, r.seq AS r_seq, r.qual AS r_qual, 
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
) AS alig_joined
WHERE distance > 0
  AND distance < distance_2_12
ORDER BY rname;
