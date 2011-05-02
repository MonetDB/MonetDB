-- can't drop table after crash
START TRANSACTION;

CREATE TABLE dbg (a INT, b INT);
INSERT INTO dbg (a,b) VALUES (10,10);

-- no alias around the SUM(b), works
SELECT a as d, SUM(b), (2 * (SUM(b) / (SELECT 2))) as f FROM dbg GROUP BY d;

-- alias e, crashes :/
SELECT a as d, SUM(b) as e, (2 * (SUM(b) / (SELECT 2))) as f FROM dbg GROUP
BY d;

ROLLBACK;
