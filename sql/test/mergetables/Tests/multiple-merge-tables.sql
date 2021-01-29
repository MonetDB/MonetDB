create table t (i int);
insert into t values (42), (666);
create merge table mt1 (like t);
create merge table mt2 (like t);

create temp table mycount(cc BIGINT) ON COMMIT PRESERVE ROWS;
insert into mycount SELECT (SELECT COUNT(*) FROM sys.dependencies) + (SELECT COUNT(*) FROM sys.objects);

alter table mt1 add table t;
select i from mt1;
select i from mt2; -- error, no tables associated

alter table mt2 add table t;
select i from mt1;
select i from mt2;

alter table mt1 drop table t;
select i from mt1; -- error, no tables associated
select i from mt2;

alter table mt2 drop table t;
select i from mt1; -- error, no tables associated
select i from mt2; -- error, no tables associated

SELECT CAST((SELECT COUNT(*) FROM sys.dependencies) + (SELECT COUNT(*) FROM sys.objects) - (SELECT cc FROM mycount) AS BIGINT);
	-- 0 it shouldn't have increased

drop table mycount;
drop table mt1;
drop table mt2;
drop table t;
