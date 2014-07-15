WITH qnames1 AS (
    SELECT DISTINCT qname
    FROM bam.alignments_1
), qnames2 AS (
    SELECT DISTINCT qname
    FROM bam.alignments_2
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

WITH qnames1 AS (
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_3
), qnames2 AS (
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_4
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
