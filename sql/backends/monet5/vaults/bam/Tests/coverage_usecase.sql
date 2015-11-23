# Creation of base table containing positional data
CREATE TABLE base (
    refpos INT, seq_char CHAR, cnt INT
);

INSERT INTO base (
    SELECT s.value AS refpos, 
           bam.seq_char(s.value, al.seq, al.pos, al.cigar) AS seq_char,
           COUNT(*) AS cnt
    FROM 
        generate_series(CAST(0 AS INT), CAST(18960 AS INT)) as s
        JOIN (
            SELECT pos, seq, cigar
            FROM bam.alignments_1
            WHERE pos > 0
        ) AS al
        ON s.value BETWEEN al.pos AND al.pos + bam.seq_length(al.cigar)
    GROUP BY refpos, seq_char
);

# Coverage, taken from the base table
CREATE VIEW coverage AS (
    SELECT refpos, SUM(cnt) AS cnt
    FROM base
    WHERE seq_char IS NOT NULL
    GROUP BY refpos
);

SELECT *
FROM coverage
ORDER BY cnt DESC;


# Coverage groups
SELECT refpos - refpos % 1000 AS grp_start,
       refpos - refpos % 1000 + 1000 AS grp_end,
       AVG(cnt) AS average
FROM coverage
GROUP BY grp_start, grp_end
ORDER BY average DESC;


# Diversity from the use case for the btw paper is not calculated, since we do not have a reference string for
# our small test files


# Clean up
DROP VIEW coverage;
DROP TABLE base;
