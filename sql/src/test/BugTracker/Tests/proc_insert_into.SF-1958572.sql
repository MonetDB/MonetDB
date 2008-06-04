
create sequence "ff" as integer start with 1;
CREATE TABLE t1(timeid INT DEFAULT NEXT VALUE FOR "ff" PRIMARY KEY, a INT, b INT, c INT, d INT, e INT, f VARCHAR(20), g VARCHAR(20), h INT, i VARCHAR(10), j VARCHAR(10), k VARCHAR(10), l INT, m INT, n VARCHAR(10), o VARCHAR(10), p VARCHAR(10), q VARCHAR(15), r INT);

insert into t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) values(1, 1, 1, 1, 1, 'first row', 'first row',1, 'first row', 'first row', 'first row',1,1, 'first row', 'first row', 'first row', 'first row', 1);

select * from t1;

insert into t1(a, b, c, d, e, f, g, h) select a, b, c, d, e, f, g, h from t1 where timeid = 1;

select * from t1;

create procedure tt()
begin
	insert into t1(a, b, c, d, e, f, g, h) select a, b, c, d, e, f, g, h from t1 where timeid = 1;
end;

call tt();
call tt();
call tt();
call tt();

select * from t1;

select count(*) from t1;
select timeid from t1;
select * from t1;

drop procedure tt;
drop table t1;
drop sequence ff;
