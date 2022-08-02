CREATE FUNCTION seq_char(val integer, seq string, pos integer, cigar string)
RETURNS INTEGER
BEGIN
	RETURN 1;
END;

CREATE TABLE "alignments_2" (
	"virtual_offset" BIGINT        NOT NULL,
	"qname"          CHARACTER LARGE OBJECT NOT NULL,
	"flag"           SMALLINT      NOT NULL,
	"rname"          CHARACTER LARGE OBJECT NOT NULL,
	"pos"            INTEGER       NOT NULL,
	"epos"            INTEGER       NOT NULL,
	"mapq"           SMALLINT      NOT NULL,
	"cigar"          CHARACTER LARGE OBJECT NOT NULL,
	"rnext"          CHARACTER LARGE OBJECT NOT NULL,
	"pnext"          INTEGER       NOT NULL,
	"tlen"           INTEGER       NOT NULL,
	"seq"            CHARACTER LARGE OBJECT NOT NULL,
	"qual"           CHARACTER LARGE OBJECT NOT NULL,
	CONSTRAINT "alignments_2_pkey_virtual_offset" PRIMARY KEY ("virtual_offset")
);
ALTER TABLE alignments_2 SET READ ONLY;
SELECT s.value AS refpos, COUNT(*) AS cnt
	FROM 
	    generate_series(128, 18960) AS s
	JOIN (
	    SELECT epos, pos, seq, cigar FROM alignments_2 WHERE pos > 0 ) AS al 
	ON (
		s.value >= al.pos AND s.value <= al."epos"
		AND seq_char(s.value, al.seq, al.pos, al.cigar) IS NOT NULL
	)
GROUP BY refpos
ORDER BY cnt DESC
LIMIT 10;

DROP TABLE alignments_2;
DROP FUNCTION seq_char;
