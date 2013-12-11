create table testjsonisnull(js json);
insert into testjsonisnull values (NULL);
select * from testjsonisnull where js is null;

drop table testjsonisnull;
