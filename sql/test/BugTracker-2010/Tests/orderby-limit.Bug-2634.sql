CREATE TABLE "tbl1" (
        "id"          int           NOT NULL,
        "id1"         int           NOT NULL,
        "id2"         int           NOT NULL
)
;

CREATE TABLE "tbl2" (
        "id"          int           NOT NULL,
        "title"       varchar(10)
)
;

INSERT INTO tbl1 VALUES (1,1,1)
;
INSERT INTO tbl1 VALUES (2,2,2)
;
INSERT INTO tbl2 VALUES (1, 'one')
;
INSERT INTO tbl2 VALUES (2, 'two')
;
INSERT INTO tbl2 VALUES (3, 'three')
;

--
-- ERROR = !MALException:group.refine:Operation failed
--         !ERROR: CTrefine: both BATs must have the same cardinality and their heads must form a 1-1 match.
--
SELECT
    tbl1.id,
    tbl1.id1,
    tbl1.id2
FROM
    tbl1 INNER JOIN tbl2 
        ON (tbl1.id1 = tbl2.id)
ORDER BY
    tbl2.title ASC,
    tbl1.id1 ASC
LIMIT 1
;

DROP TABLE "tbl2";
DROP TABLE "tbl1";
