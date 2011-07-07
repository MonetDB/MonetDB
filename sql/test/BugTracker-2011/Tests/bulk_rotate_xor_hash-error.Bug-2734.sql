CREATE TABLE t_bug2734 (a INT);
INSERT INTO t_bug2734 VALUES (1);
SELECT * FROM (SELECT a, 1 AS n FROM t_bug2734 UNION SELECT a, 2 AS n FROM t_bug2734) AS abc INNER JOIN (SELECT a, 1 AS n FROM t_bug2734 UNION SELECT a, 2 AS n FROM t_bug2734) AS cba ON abc.a = cba.a AND abc.n = cba.n;
DROP TABLE t_bug2734;

CREATE TABLE foo2734(row int not null, col int not null, val int not null, primary key (row, col));
INSERT INTO foo2734(row, col, val) VALUES (1,1,1), (1,2,2), (2,1,3), (2,2,4);

WITH
-- binding due to rownum operator
t0000 (iter4_nat, iter5_nat) AS
  (SELECT a0000.iter4_nat, ROW_NUMBER () OVER () AS iter5_nat
     FROM (VALUES (1)) AS a0000(iter4_nat)),

-- binding due to rownum operator
t0001 (iter4_nat, iter5_nat, item1_int, item2_int, item3_int,
  iter23_nat) AS
  (SELECT a0001.iter4_nat, a0001.iter5_nat, a0002.col AS item1_int,
          a0002.row AS item2_int, a0002.val AS item3_int,
          ROW_NUMBER () OVER
          (ORDER BY a0001.iter5_nat ASC, a0002.row ASC, a0002.col ASC) AS
          iter23_nat
     FROM t0000 AS a0001,
          foo2734 AS a0002),

-- binding due to aggregate
t0002 (iter11_nat, pos12_nat) AS
  (SELECT a0003.iter5_nat AS iter11_nat, MIN (a0003.iter23_nat) AS pos12_nat
     FROM t0001 AS a0003
    GROUP BY a0003.iter5_nat, a0003.item2_int),

-- binding due to rownum operator
t0003 (iter11_nat, pos12_nat, iter4_nat, iter5_nat, item1_int,
  item2_int, item3_int, iter23_nat, pos25_bool, pos26_nat) AS
  (SELECT a0004.iter11_nat, a0004.pos12_nat, a0005.iter4_nat, a0005.iter5_nat,
          a0005.item1_int, a0005.item2_int, a0005.item3_int, a0005.iter23_nat,
          CASE WHEN a0005.iter23_nat = a0004.pos12_nat THEN 1 ELSE 0 END AS
          pos25_bool,
          ROW_NUMBER () OVER
          (PARTITION BY a0004.iter11_nat ORDER BY a0005.iter23_nat ASC) AS
          pos26_nat
     FROM t0002 AS a0004,
          t0001 AS a0005
    WHERE a0004.iter11_nat = a0005.iter5_nat
      AND a0005.iter23_nat = a0004.pos12_nat)

SELECT 1 AS iter20_nat, a0006.item2_int AS item8_int
   FROM t0003 AS a0006
  WHERE a0006.pos26_nat = 2
  ORDER BY a0006.iter11_nat ASC, a0006.pos26_nat ASC;

DROP TABLE foo2734;
