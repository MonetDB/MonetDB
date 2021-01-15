
DROP TABLE IF EXISTS foo CASCADE;
CREATE TABLE foo(i INT, j INT DEFAULT 42);

CREATE PROCEDURE append_twice() BEGIN INSERT INTO foo(i) VALUES (0), (1), (2); INSERT INTO foo(i) VALUES (10), (11), (12); END;

-- if optimizer.parappend does not keep the appends separate, data may get lost
CALL append_twice();

SELECT * FROM foo;
