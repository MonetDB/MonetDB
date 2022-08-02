DROP TABLE IF EXISTS t;
CREATE TABLE t (i  INT, s VARCHAR(32));
INSERT INTO t VALUES (-450, 'foo'), (29, 'bar'), (-250, 'foobar');

SELECT i FROM t WHERE t.i NOT IN (-450 , 29) AND (t."s" <> 'xyz' OR t."s" IS NULL );
DROP TABLE t;
