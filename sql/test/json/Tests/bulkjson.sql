create table nosql(j json);
insert into  nosql values
	('{}'),
	('{"f1":1}'),
	('{"f1":1,"f2":2}'),
	('{"f1":1,"f2":2,"f1":3}'),
	('{"f1":{"f12":3},"f2":[2,3,4]}');
select * from nosql;

select json.filter(j,'f1') from nosql;
select json.filter(j,'f2') from nosql;
select json.filter(j,'..f12') from nosql;

delete from nosql;
select * from nosql;
insert into nosql values('[1,"f2", 2]');
select * from nosql;

select json.filter(j,0) from nosql;
select json.filter(j,1) from nosql;
select json.filter(j,2) from nosql;
select json.filter(j,3) from nosql;

drop table nosql;
