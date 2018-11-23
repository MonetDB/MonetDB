create table nosql(j json);
insert into  nosql values
	('{}'),
	('{"f1":1}'),
	('{"f1":1,"f2":2}'),
	('{"f1":1,"f2":2,"f1":3}'),
	('{"f1":{"f12":3},"f2":[2,3,4]}'),
	('[1,"f2", 2]');
select * from nosql;

create table nosql_string as (select cast (j as string) as j from nosql);
select * from nosql_string;
select cast (j as json) as j from nosql_string;

-- consider them as initial strings
create table tmpsql(j string);
insert into  tmpsql values
	('{}'),
	('{"f1":1}'),
	('{"f1":1,"f2":2}'),
	('{"f1":1,"f2":2,"f1":3}'),
	('{"f1":{"f12":3},"f2":[2,3,4]}'),
	('[1,"f2", 2]');
select * from tmpsql;

create table tmpsql_json as (select cast (j as json) as j from tmpsql);
select * from tmpsql_json;
select cast (j as string) as j from tmpsql_json;

drop table tmpsql;
drop table nosql;
drop table tmpsql_json;
drop table nosql_string;
