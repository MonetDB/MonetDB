create table t1 (id int, name varchar(1024), age int, PRIMARY KEY(id));

create table t2 (id int, age int, PRIMARY KEY (ID), FOREIGN KEY(id) REFERENCES t1(id));

create trigger test1 after insert on t1
	insert into t1 values(0, 'after', 23);

create view v1 as select * from t1;

CREATE FUNCTION f1(num int)
RETURNS int
BEGIN 
	IF num >0
		THEN RETURN 1;
		ELSE RETURN 0;
	END IF;
END;

CREATE FUNCTION f2()
RETURNS TABLE(id int)
BEGIN
	DECLARE TABLE f1_t1(id int);
	DECLARE x int;
	
	SET x = 3;	

	IF f1(x) > 0
		THEN RETURN f1_t1;
	END IF;

	INSERT INTO f1_t1 VALUES(1);

	IF f1(x) < 0
		THEN RETURN f1_t1;
	END IF;

	RETURN TABLE (SELECT t1.id FROM v1, t1 WHERE v1.age > 10 AND t1.name LIKE 'monet');
END;

CREATE INDEX id_index ON t1(id);

DROP TABLE t1 CASCADE;

select * from tables where name = 't1';
select * from tables where name = 't2';
select * from tables where name = 'v1';
select * from triggers where name = 'test1';
select * from functions where name = 'f2';
select * from idxs where name = 'id_index';

select * from dependencies;

