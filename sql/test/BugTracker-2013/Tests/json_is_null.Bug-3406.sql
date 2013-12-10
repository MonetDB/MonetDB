create table testjsonisnull(js json);

select * from testjsonisnull where js is null;

drop table testjsonisnull;
