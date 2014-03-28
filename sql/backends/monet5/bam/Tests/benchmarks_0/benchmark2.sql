--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.1 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT qname, flag, seq, qual
    FROM bam.alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
      AND mapq < mapq_2_1
), alig_proj AS (
    SELECT qname, flag,
        CASE WHEN bam_flag(flag, 'segm_reve') THEN reverse_seq(seq)   ELSE seq  END AS seq,
        CASE WHEN bam_flag(flag, 'segm_reve') THEN reverse_qual(qual) ELSE qual END AS qual
    FROM alig
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

-- Description:
-- This query selects fields required by the FASTQ file format (qname, seq/seq-reverse, qual/qual-reverse).
-- It performs a join resulting in a wide table starting with a qname, followed by the seq/seq-reverse
-- and the qual/qual-reverse for both reads of this qname. I.e., every tuple in the result contains a read pair. 
-- The outer query joins two subresults together. Both subresults only contain primary alignments. 
-- Also, alignments that have not stored their seq or qual value are filtered out in the
-- subresults and alignments with a mapping quality >= 100 are also filtered out.
-- The subresults contain alignments with their 'firs_segm' and 'last_segm' flag set respectively. Furthermore,
-- since we don't want alignments showing up in the result more than once, we have to make sure that for every
-- qname, exactly one left and exactly one right alignment remains in the subresults. Therefore, qnames that
-- do not fulfill the conditions in the WITH clause are excluded from the subresults. These qnames would be 
-- invalid and are not considered in this usecase. 
-- The WITH clause first filters alignments that are not primary and not unmapped and also alignments that have
-- their 'firs_segm' flag equal to their 'last_segm' flag. It then only selects those qnames for which 
-- sum(nr_primary) + sum(nr_unmapped) = 2. Furthermore, from these 2 alignments, one has to be flagged as 
-- 'firs_segm' and one has to be flagged as'last_segm'. This makes sure that only qnames will result that
-- have exactly one matching alignment for both the left side and the right side of the result.
-- The result is ordered by qname.
-- A custom renderer has been developed that can be used with mclient to output actual FASTQ files as a result
-- of this query.











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.2 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT qname, flag, rname, pos, cigar
    FROM bam.alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
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

-- Description:
-- This query calculates a histogram that displays the number of read pairs with a certain distance.
-- The outer query joins two subresults together. These subresults contain all primary alignments for
-- the first segment and the last segment respectively. Note that only alignments are considered that have
-- exactly one flag of the flags 'firs_segm' and 'last_segm' set.
-- The IN clause in both subqueries ensures that only qnames will end up in the result that have exactly
-- two primary alignments; one for the left and one for the right read. 
-- Furtermore, only alignments are joined that are in the same chromosome, since the distance between two
-- alignments is not defined if they are not in the same chromosome.
-- The distance of each record in this joined table can now be calculated. A CASE statement is used since
-- there is no guarantee on whether or not the alignments flagged as first_segm (last_segm) are the left (right)
-- alignments. 
-- Furthermore, grouping is done on this distance to create the histogram.
-- The result is ordered by the nr of alignments in a descending fashion, such that the most occurring
-- distances will be presented first.











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.3 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i 
WHERE qname IN (
    SELECT qname
    FROM bam.alignments_i
    GROUP BY qname
    HAVING SUM(bam_flag(flag, 'firs_segm')) = 0
        OR SUM(bam_flag(flag, 'last_segm')) = 0
)
ORDER BY qname;

-- Description:
-- This query checks a consistency aspect of the BAM file. It returns all alignments
-- for qnames that have either no alignment flagged as first segment or no alignment flagged as last segment.
-- The inner query groups by qname and selects the inconsistent ones. The outer query then selects all relevant attributes
-- from all alignments for these qnames.
-- To provide the user with a convenient overview of the inconsistencies, the result is ordered by qname.










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
        FROM bam.alignments_i
        WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
    ) AS qnames
    GROUP BY qname, firs_segm
    HAVING SUM(segm_unma) < COUNT(*)
       AND (COUNT(*) - SUM(seco_alig)) <> 1
)
ORDER BY qname;



-- Description:
-- This query also checks a consistency aspect of the BAM file. Every qname consists of two reads.
-- For both reads it must hold that all its alignments must either be unmapped or there must be exactly one primary
-- alignment.
-- An important aspect of the innermost query is that it only considers alignments that have 'firs_segm' <> 'last_segm'.
-- The reason for this is that when these flags are equal, the alignment becomes irrelevant for this consistency check.
-- Due to the aforementioned property of the remaining alignments, grouping on either 'firs_segm' or 'last_segm' would
-- result in exactly the same groups. Therefore, grouping is only done on 'firs_segm' (and qname of course).
-- The subquery then selects a group if not all of its alignments are unmapped and the number of primary alignments
-- <> 1. Note that this could approve of alignments with both the 'segm_unma' flag set to True and the 'seco_alig' set
-- to False. This is not covered by this consistency check.
-- The outer query again selects all relevant attributes of all alignments in the inconsistent qnames. 
-- The result is again ordered by qname for convenient displaying.










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.5 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH qnames1 AS (
    SELECT DISTINCT qname
    FROM bam.alignments_i
), qnames2 AS (
    SELECT DISTINCT qname
    FROM bam.alignments_j
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
    FROM bam.alignments_i
    INTERSECT
    SELECT distinct qname
    FROM bam.alignments_j
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

-- Description:
-- Does a set intersection on two BAM files, based on and ordered by qname.










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.7 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE qname IN (
    SELECT distinct qname
    FROM bam.alignments_i
    EXCEPT
    SELECT distinct qname
    FROM bam.alignments_j
)
ORDER BY qname;

-- Description:
-- Does a set minus on two BAM files, based on and ordered by qname. Note that in contrary to the
-- previous set operation no alignments of the second file are selected, since the result of the inner
-- query will by definition never contain qnames that exist in the second file










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.8 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT f1.qname AS qname, f1.flag AS f1_flag, f1.rname AS f1_rname, f1.pos AS f1_pos, f1.mapq AS f1_mapq, f1.cigar AS f1_cigar, 
    f1.rnext AS f1_rnext, f1.pnext AS f1_pnext, f1.tlen AS f1_tlen, f1.seq AS f1_seq, f1.qual AS f1_qual, 
                          f2.flag AS f2_flag, f2.rname AS f2_rname, f2.pos AS f2_pos, f2.mapq AS f2_mapq, f2.cigar AS f2_cigar, 
    f2.rnext AS f2_rnext, f2.pnext AS f2_pnext, f2.tlen AS f2_tlen, f2.seq AS f2_seq, f2.qual AS f2_qual
FROM (
    SELECT *
    FROM bam.alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS f1 JOIN (
    SELECT *
    FROM bam.alignments_j
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS f2 
    ON  f1.qname = f2.qname
    AND bam_flag(f1.flag, 'firs_segm') = bam_flag(f2.flag, 'firs_segm')
    AND (f1.rname <> f2.rname OR f1.pos <> f2.pos)
ORDER BY qname;

-- Description:
-- Joins primary alignments from two files together if these alignments have the same qname but
-- are mapped to different positions. 
-- The first subquery selects all primary alignments from the first file that have either flag 'firs_segm' or 'last_segm' set.
-- The second subquery selects all primary alignments from the second file that have either flag 'firs_segm' or 'last_segm' set
-- The outer query then joins two alignments from these two subresults together under the following conditions:
-- * The qnames of the alignments are equal.
-- * The alignments are both either left or right alignments. Note that it is enough to check equality of firs_segm. If these are
--   equal (unequal) this implies that last_segm is also equal (unequal), due to the filtering on firs_segm <> last_segm.
-- * The positions of the alignments aren't equal (determined by both rname and pos).
-- The final result is ordered by qname.










--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.9 --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE rname = rname_2_9
  AND pos_2_9 >= pos
  AND pos_2_9 < pos + seq_length(cigar)
ORDER BY pos;

-- Description:
-- Selects all alignments that overlap position 17922988 in chromosome "chr13"
-- The result is ordered by position




	





--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.10 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

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

-- Description:
-- Selects all primary alignment pairs whose internal segment overlap position 17922988 in chr13. To accomplish this, 
-- primary alignments flagged as 'firs_segm' and 'last_segm' respectively will be joined together. Note that, as in
-- query 1 and 2, inconsistencies are filtered out (including the fact that exactly 2 alignments must be marked
-- as primary).
-- All alignments that will then be joined by the join operator if they fulfill the following requirements:
-- * The qnames match
-- * The end pos of the left alignment is before the given position
-- * The begin pos of the right alignment is after the given position
-- Note that the last two requirements are calculated depending on which alignment of the two is the left one.
-- Note also that this query excludes alignments that are mapped to different chromosomes, since distance between
-- two alignments is not defined if they are in different chromosomes.
-- The result is ordered by the position of the alignment with the 'firs_segm' flag set.











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.11 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT *
    FROM bam.alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext <> '*'
      AND pnext > 0
)
SELECT l.qname AS qname, l.flag AS l_flag, l.rname AS l_rname, l.pos AS l_pos, l.mapq AS l_mapq, l.cigar AS l_cigar, 
    l.rnext AS l_rnext, l.pnext AS l_pnext, l.tlen AS l_tlen, l.seq AS l_seq, l.qual AS l_qual, 
                         r.flag AS r_flag, r.rname AS r_rname, r.pos AS r_pos, r.mapq AS r_mapq, r.cigar AS r_cigar, 
    r.rnext AS r_rnext, r.pnext AS r_pnext, r.tlen AS r_tlen, r.seq AS r_seq, r.qual AS r_qual
FROM (
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
ORDER BY l.qname;

-- Description:
-- Uses information in the rnext and pnext fields to pair secondary alignments.
-- The outer query joins two subresults. These subresults contain all secondary alignments where all of
-- rname, pos, rnext and pnext contain a valid value for the alignments with the flags 'firs_segm' and 
-- 'last_segm' set respectively. Furthermore, only alignments are considered that have either their 
-- 'firs_segm' or their 'last_segm' flag set (not both). The join conditions are:
-- * qnames must of course be equal
-- * the rname and pos of l must be pointed to by the rnext and pnext of r and vica versa. If the rnames 
--   of the two reads are equal however, an '=' is used in the rnext field. Hence the OR check which 
--   first checks equality of the rnames and second checks matching rnext and rname.
-- * The rnext/rname pnext/pos checks are performed twice, once for both alignments. By doing this, we
--   make sure that l points to r and vica versa. This therefore filters out inconsistent mates, that
--   could exist in the data.
-- The result is ordered by qname











--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------- Query 2.12 -------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

WITH alig AS (
    SELECT *
    FROM bam.alignments_i
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

-- Description:
-- Searches for potential secondary alignment pairs. The subquery joins two subresults together. Both subresults must
-- have exactly one of their 'firs_segm' and 'last_segm' flags set to true. Furthermore, the subresults include only
-- secondary alignments where rnext and pnext aren't set. The subresults return the results for alignments with the
-- 'firs_segm' flag or the 'last_segm' flag set respectively.
-- The subresults are then joined on qname and rname. Joining on rname is necessary, since a distance can not be 
-- calculated for two alignments from different chromosomes. The subquery then also calculates the distance
-- between the joined alignments (which is feasible since the join ensures that only alignments in the same
-- chromosome are joined). The outer query then uses this distance to do some additional filtering. Negative 
-- distances are still possible, since the starting position of the left alignment plus its length can exceed
-- the starting position of the right alignment. Furthermore, only alignments with distance < 100000 are considered
-- to be good candidates.
-- The result is ordered by chromosome
