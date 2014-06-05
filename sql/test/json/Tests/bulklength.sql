create table nosql(j json);
insert into  nosql values
	('{}'),
	('{"f1":1}'),
	('{"f1":1,"f2":2}'),
	('{"f1":1,"f2":2,"f1":3}'),
	('{"f1":{"f12":3},"f2":[2,3,4]}');
insert into nosql values('[1,"f2", 2]');
select * from nosql;

select json.length(j) from nosql;

drop table nosql;
