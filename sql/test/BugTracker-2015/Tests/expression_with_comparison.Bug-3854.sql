CREATE TABLE test (id INTEGER, foo INTEGER);
INSERT INTO test VALUES (1, 1);
SELECT t.id, t.foo FROM test t
      WHERE FALSE
      AND (TRUE OR TRUE)
      AND ((FALSE AND (TRUE OR FALSE))
        OR (15 > t.foo)
      );
DROP TABLE test;
