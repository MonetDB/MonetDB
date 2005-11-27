START TRANSACTION;
CREATE TABLE t1 (
  id INT,
  text1 VARCHAR(32),
  text2 VARCHAR(32) NOT NULL,
  text3 VARCHAR(32) DEFAULT 'foo',
  text4 VARCHAR(32) NOT NULL DEFAULT 'foo'
);

INSERT INTO t1 VALUES(1, 'test1', 'test2', 'test3', 'test4');
INSERT INTO t1 (id, text2) VALUES(2, 'test2');
INSERT INTO t1 (text2) VALUES('test2');

commit;

-- next ones should fail
INSERT INTO t1 (id) VALUES(1);
INSERT INTO t1 (text1) VALUES('test1');
INSERT INTO t1 (id, text3) VALUES(1, 'test3');
INSERT INTO t1 (id, text1, text3, text4) VALUES(1, 'test1', 'test3', 'test4');
INSERT INTO t1 (id, text1, text2, text3, text4) VALUES(1, 'test1', 'test2', 'test3', NULL);

select * from t1;

drop table t1;
