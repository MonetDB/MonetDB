CREATE TABLE t1(id int, name varchar(1024), age int, PRIMARY KEY(id));

CREATE VIEW v1 as select id, age from t1 where name like 'monet%';

ALTER TABLE v1 DROP COLUMN age;

CREATE TRIGGER trigger_test AFTER INSERT ON v1
        INSERT INTO t2 values(1,23);

CREATE INDEX id_age_index ON v1(id,age);

DROP view v1;

DROP table t1;
