SELECT COUNT (*) > 0 as res
  FROM (SELECT id, COUNT (*) AS cnt
          FROM tables
         GROUP BY id) as i
 WHERE NOT (1 < i.cnt);

-- 1 < i.cnt => i.cnt > 1
SELECT COUNT (*) > 0 as res
  FROM (SELECT id, COUNT (*) AS cnt
          FROM tables
         GROUP BY id) as i
 WHERE NOT (i.cnt > 1);

-- COUNT (*) => 1
SELECT COUNT (*) > 0 as res
  FROM (SELECT id, 1 AS cnt
          FROM tables
         GROUP BY id) as i
 WHERE NOT (1 < i.cnt);

-- NOT (1 < i.cnt) => 1 >= i.cnt
SELECT COUNT (*) > 0 as res
  FROM (SELECT id, COUNT (*) AS cnt
          FROM tables
         GROUP BY id) as i
 WHERE 1 >= i.cnt;

-- NOT (1 < i.cnt) => i.cnt <= 1
SELECT COUNT (*) > 0 as res
  FROM (SELECT id, COUNT (*) AS cnt
          FROM tables
         GROUP BY id) as i
 WHERE i.cnt <= 1;

-- inverted result
SELECT COUNT (*) = 0 as res
  FROM (SELECT id, COUNT (*) AS cnt
          FROM tables
         GROUP BY id) as i
 WHERE 1 < i.cnt;
