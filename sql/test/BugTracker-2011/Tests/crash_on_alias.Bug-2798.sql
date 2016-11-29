-- can't drop table after crash
START TRANSACTION;

CREATE TABLE dbg (a INT, b INT);
INSERT INTO dbg (a,b) VALUES (10,10);

-- no alias around the SUM(b), works
plan SELECT a as d, SUM(b), (2 * (SUM(b) / (SELECT 2))) as f FROM dbg GROUP BY d;
set optimizer = 'sequential_pipe';
--explain SELECT a as d, SUM(b), (2 * (SUM(b) / (SELECT 2))) as f FROM dbg GROUP BY d;
set optimizer = 'default_pipe';
SELECT a as d, SUM(b), (2 * (SUM(b) / (SELECT 2))) as f FROM dbg GROUP BY d;

ROLLBACK;

START TRANSACTION;

CREATE TABLE dbg (a INT, b INT);
INSERT INTO dbg (a,b) VALUES (10,10);

-- alias e, crashes :/
plan SELECT a as d, SUM(b) as e, (2 * (SUM(b) / (SELECT 2))) as f FROM dbg GROUP BY d;
set optimizer = 'sequential_pipe';
--explain SELECT a as d, SUM(b) as e, (2 * (SUM(b) / (SELECT 2))) as f FROM dbg GROUP BY d;
set optimizer = 'default_pipe';
SELECT a as d, SUM(b) as e, (2 * (SUM(b) / (SELECT 2))) as f FROM dbg GROUP
BY d;

ROLLBACK;
