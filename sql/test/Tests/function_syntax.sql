create function f1()
RETURNS int
BEGIN
	return 0;
END;

create function f1(id int)
RETURNS int
BEGIN
	return 0;
END;


create function f2(id int)
RETURNS int
BEGIN
	return 0;
END;

create function f3(id int, name varchar(1024))
RETURNS int
BEGIN
	return 0;
END;

select name from functions where name = 'f1' or name = 'f2' or name = 'f3';

DROP FUNCTION f1;

select name from functions where name = 'f1' or name = 'f2' or name = 'f3';

DROP FUNCTION f1();

select name from functions where name = 'f1' or name = 'f2' or name = 'f3';

DROP FUNCTION f2 (int, varchar(1024));

select name from functions where name = 'f1' or name = 'f2' or name = 'f3';

DROP FUNCTION f2 (int);

select name from functions where name = 'f1' or name = 'f2' or name = 'f3';

DROP FUNCTION f3 (int, varchar(1024));

select name from functions where name = 'f1' or name = 'f2' or name = 'f3';

create function f1()
RETURNS int
BEGIN
	return f1(3);
END;

DROP ALL FUNCTION f1;

DROP ALL FUNCTION f1 CASCADE;

select name from functions where name = 'f1' or name = 'f2' or name = 'f3';
