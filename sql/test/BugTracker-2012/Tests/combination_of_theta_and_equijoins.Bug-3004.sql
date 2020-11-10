
CREATE TABLE e (
    "a" CLOB,
    "r" BIGINT
);
CREATE TABLE s (
    "a" CLOB,
    "v" BIGINT
);

INSERT INTO e VALUES ('Simon', '1');
INSERT INTO s VALUES ('Simon', '0');

SELECT *
FROM 
        (
        SELECT 
            "a" AS "ea",
            "r" AS "er"
        FROM 
            "e"
        ) AS "e"
    ,
        (
        SELECT 
            "a" AS "sa",
            "v" AS "sv"
        FROM 
            "s"
        ) AS "s"
WHERE "sv" <= "er"
  AND "ea" = "sa"
;

SELECT *
FROM 
        (
        SELECT 
            "a" AS "ea",
            "r" AS "er"
        FROM 
            "e"
        ) AS "e"
    ,
        (
        SELECT 
            "a" AS "sa",
            "v" AS "sv"
        FROM 
            "s"
        ) AS "s"
WHERE "ea" = "sa"
  AND "sv" <= "er"          
;

SELECT *
FROM 
        (
        SELECT 
            "a" AS "ea",
            "r" AS "er"
        FROM 
            "e"
        ) AS "e",
        (
        SELECT 
            "a" AS "sa",
            "v" AS "sv"
        FROM 
            "s"
        ) AS "s"
WHERE ("sv" <= "er") = true
  AND ("ea" = "sa") = true
;

DROP TABLE "e";
DROP TABLE "s";
