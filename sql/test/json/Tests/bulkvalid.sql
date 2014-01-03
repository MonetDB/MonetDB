create table nosql(j json);
insert into  nosql values
	('{}'),
	('{"f1":1}'),
	('{"f1":1,"f2":2}'),
	('{"f1":1,"f2":2,"f1":3}'),
	('{"f1":{"f12":3},"f2":[2,3,4]}'),
	('[1,"f2", 2]');
select * from nosql;

explain select json.isvalid(j) from nosql;
select json.isvalid(j) from nosql;

explain select json.isvalidobject(j) from nosql;
select json.isvalidobject(j) from nosql;

explain select json.isvalidarray(j) from nosql;
select json.isvalidarray(j) from nosql;

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

explain select json.isvalid(j) from nosql;
select json.isvalid(j) from nosql;

explain select json.isvalidobject(j) from nosql;
select json.isvalidobject(j) from nosql;

explain select json.isvalidarray(j) from nosql;
select json.isvalidarray(j) from nosql;

drop table tmpsql;
drop table nosql;
