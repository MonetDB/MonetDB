-- can't drop table after crash
START TRANSACTION;

CREATE TABLE dbg (a INT, b INT);
INSERT INTO dbg (a,b) VALUES (10,10);

-- no alias for the MIN(b), works
plan
SELECT a as d, MIN(b), (2 * (MIN(b) / (SELECT 2))) as f FROM dbg GROUP BY d;
set optimizer = 'sequential_pipe';
--explain SELECT a as d, MIN(b), (2 * (MIN(b) / (SELECT 2))) as f FROM dbg GROUP BY d;
set optimizer = 'default_pipe';
SELECT a as d, MIN(b), (2 * (MIN(b) / (SELECT 2))) as f FROM dbg GROUP BY d;

ROLLBACK;


START TRANSACTION;

CREATE TABLE dbg (a INT, b INT);
INSERT INTO dbg (a,b) VALUES (10,10);

-- with alias e, it crashes :/
plan
SELECT a as d, MIN(b) as e, (2 * (MIN(b) / (SELECT 2))) as f FROM dbg GROUP BY d;
set optimizer = 'sequential_pipe';
--explain SELECT a as d, MIN(b) as e, (2 * (MIN(b) / (SELECT 2))) as f FROM dbg GROUP BY d;
set optimizer = 'default_pipe';
SELECT a as d, MIN(b) as e, (2 * (MIN(b) / (SELECT 2))) as f FROM dbg GROUP BY d;

ROLLBACK;
