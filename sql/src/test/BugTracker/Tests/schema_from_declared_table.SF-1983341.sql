create table t1 (id int, age int);

create function f1()
RETURNS table (idd int, aage int)
BEGIN
DECLARE TABLE cover(
id int, htmidEnd int
);
INSERT into cover
SELECT id, age
FROM t1;
RETURN TABLE (
SELECT id , htmidEnd
FROM cover H
WHERE 1 > id);
END;

select * from f1() n;
select * from f1() n;

drop function f1;
drop table t1;
