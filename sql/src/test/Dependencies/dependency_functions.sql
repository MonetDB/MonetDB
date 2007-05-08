CREATE FUNCTION f1(num int)
RETURNS int
BEGIN 
	IF num >0
		THEN RETURN 1;
		ELSE RETURN 0;
	END IF;
END;

CREATE FUNCTION f1()
RETURNS int
BEGIN 
	RETURN 0;
END;

CREATE FUNCTION f2(x int)
RETURNS int
BEGIN
	IF f1(x) > 0
		THEN RETURN 1;
	END IF;

	IF f1() < 0
		THEN RETURN 2;
	END IF;

END;

CREATE FUNCTION f3()
RETURNS int
BEGIN
	IF f1() < 0
		THEN RETURN 1;
	END IF;
END;

--Function f1 has a dependency on function f2
--SELECT f1.id, f1.func, f2.id, 'DEP_FUNC' from functions as f1, functions as f2, dependencies as dep where f1.id = dep.id AND f2.id = dep.depend_id AND dep.depend_type = 7 order by f2.name, f1.name;
SELECT f1.name, f2.name, 'DEP_FUNC' from functions as f1, functions as f2, dependencies as dep where f1.id = dep.id AND f2.id = dep.depend_id AND dep.depend_type = 7 order by f2.name, f1.name;



DROP FUNCTION f2;

DROP FUNCTION f3;

DROP ALL FUNCTION f1;

SELECT f1.name, f2.name, 'DEP_FUNC' from functions as f1, functions as f2, dependencies as dep where f1.id = dep.id AND f2.id = dep.depend_id AND dep.depend_type = 7 order by f2.name, f1.name;

create table t1(id int, name varchar(1024), age int);

create function f1()
returns int
BEGIN
        return 1;
END;

create view v1 as select * from t1 where id = f1();

DROP function f1;
DROP function f1 cascade;
DROP table t1;

