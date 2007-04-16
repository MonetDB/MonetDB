create table t1 (id int, name varchar(1024), age int);

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

CREATE VIEW v2 as select * from v1;

drop view v1 cascade;


select name from tables where name = 'v1';
select name from tables where name = 'v2';
select name from functions where name = 'f2';

--just for debug
--select * from dependencies;


drop function f1;
drop table t1;

--just for debug
--select * from dependencies;

