start transaction;
CREATE TABLE ontime (
        "Year"       SMALLINT,
        "Month"      TINYINT,
        "DayofMonth" TINYINT,
        "Carrier"    CHAR(2),
        "CRSDepTime" DECIMAL(8,2),
        "ArrDelay"   DECIMAL(8,2)
);
CREATE TABLE tmp (
        "Hour" TINYINT, 
        "PredictedArrDelay" DECIMAL(8,2) DEFAULT 0.0
);
INSERT INTO tmp ("Hour")
VALUES
    (0), (1), (2), (3), (4), (5), 
    (6), (7), (8), (9), (10), (11), 
    (12), (13), (14), (15), (16), (17), 
    (18), (19), (20), (21), (22), (23);

INSERT INTO ontime VALUES (2001, 9, 2, 'AA', 900.00, -6.00);
ALTER TABLE "ontime" SET READ ONLY;
ANALYZE sys.ontime;
WITH t1 AS (
    SELECT "Carrier", CAST (FLOOR("CRSDepTime"%2400/100) AS INT) AS "Hour", 
           CAST(AVG("ArrDelay") AS DECIMAL(8,2)) AS "PredictedArrDelay"
    FROM ontime
    WHERE "Year" = 2007 AND "Month" = 10 AND "DayofMonth" = 24
    GROUP BY "Carrier", "Hour"
),
t2 AS (
    SELECT t."Carrier", tmp.*
    FROM tmp, (SELECT DISTINCT "Carrier" FROM t1) AS t
)
SELECT "Carrier", "Hour", CAST(SUM("PredictedArrDelay") AS DECIMAL(18,2))
FROM (SELECT * FROM t1 UNION SELECT * FROM t2) AS t
GROUP BY "Carrier", "Hour"
ORDER BY "Carrier", "Hour";
rollback;
