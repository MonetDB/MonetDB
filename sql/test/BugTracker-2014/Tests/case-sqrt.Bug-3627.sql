START TRANSACTION;

CREATE TABLE t1 (id SERIAL, colint INT);

INSERT INTO t1 (colint) VALUES (-2),(-1),(0),(1),(2);

SELECT t0.id
      ,t0.colint
      ,SQRT(t0.colint_sq) 
  FROM (SELECT id
              ,colint
              ,CASE WHEN colint < 0
                    THEN -1 * colint
                    ELSE colint
               END AS colint_sq
          FROM t1
       ) t0
;

SELECT t0.id
      ,t0.colint
      ,t0.sqrt_colint
  FROM (SELECT id
              ,colint
              ,CASE WHEN colint < 0
                    THEN SQRT(-1 * colint)
                    ELSE SQRT(colint)
               END AS sqrt_colint
          FROM t1
       ) t0
;

ROLLBACK;
