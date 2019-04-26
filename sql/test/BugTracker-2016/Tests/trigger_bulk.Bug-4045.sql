CREATE TABLE t111 (id INT, name VARCHAR(1024));

INSERT INTO t111 VALUES(10, 'monetdb');
INSERT INTO t111 VALUES(20, 'monet');

-- trigger which triggers itself recursively until reaching max nesting depth or out of stack space
CREATE TRIGGER 
   test5
BEFORE INSERT ON t111
FOR EACH ROW
  INSERT INTO t111 VALUES(4, 'update_when_statement_true');

INSERT INTO t111 SELECT * FROM t111;

SELECT * FROM t111;

INSERT INTO t111 VALUES(30,'single');
SELECT * FROM t111;

DROP TABLE t111;
-- fails with: unable to drop table t111 (there are database objects which depend on it)

-- cleanup table and dependent trigger
DROP TABLE t111 CASCADE;
