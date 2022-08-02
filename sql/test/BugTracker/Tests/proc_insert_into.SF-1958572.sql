
create sequence "ff" as integer start with 1;
CREATE TABLE proc_insert_into_t1(timeid INT DEFAULT NEXT VALUE FOR "ff" PRIMARY KEY, a INT, b INT, c INT, d INT, e INT, f VARCHAR(20), g VARCHAR(20), h INT, i VARCHAR(10), j VARCHAR(10), k VARCHAR(10), l INT, m INT, n VARCHAR(10), o VARCHAR(10), p VARCHAR(10), q VARCHAR(15), r INT);

insert into proc_insert_into_t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) values(1, 1, 1, 1, 1, 'first row', 'first row',1, 'first row', 'first row', 'first row',1,1, 'first row', 'first row', 'first row', 'first row', 1);

select * from proc_insert_into_t1;

insert into proc_insert_into_t1(a, b, c, d, e, f, g, h) select a, b, c, d, e, f, g, h from proc_insert_into_t1 where timeid = 1;

select * from proc_insert_into_t1;

create procedure proc_insert_into_tt()
begin
	insert into proc_insert_into_t1(a, b, c, d, e, f, g, h) select a, b, c, d, e, f, g, h from proc_insert_into_t1 where timeid = 1;
end;

call proc_insert_into_tt();
call proc_insert_into_tt();
call proc_insert_into_tt();
call proc_insert_into_tt();

select * from proc_insert_into_t1;

select count(*) from proc_insert_into_t1;
select timeid from proc_insert_into_t1;
select * from proc_insert_into_t1;

drop procedure proc_insert_into_tt;
drop table proc_insert_into_t1;
drop sequence ff;
