start transaction;
create table nutki(id int,val int);
insert into nutki values(1,1);
insert into nutki values(1,2);
insert into nutki values(3,4);
insert into nutki values(3,3);
commit;

select id, cast(sum(val) as bigint) AS valsum from nutki group by id having sum(val)>1;
select id, cast(sum(val) as bigint) AS valsum from nutki group by id having val>1;
-- Error: SELECT: cannot use non GROUP BY column 'val' in query results without an aggregate function
select id, cast(sum(val) as bigint) AS valsum from nutki group by id having val>2;
-- Error: SELECT: cannot use non GROUP BY column 'val' in query results without an aggregate function
select id, cast(sum(val) as bigint) AS valsum from nutki group by id having valsum>3;
-- Error: SELECT: cannot use non GROUP BY column 'valsum' in query results without an aggregate function

drop table nutki;
