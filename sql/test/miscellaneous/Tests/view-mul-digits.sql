START TRANSACTION;

create temp table mytable(b bigint);
insert into mytable values (1), (2), (3);

create view sys.myview as select cast(2 * "b" as bigint) as mycol from mytable;

select t.name, c.name, c.type, c.type_digits, c.type_scale
from sys.tables t join sys.columns c on c.table_id = t.id where t.name = 'myview';

ROLLBACK;

create schema myschema;
create table myschema.mygroyp(code varchar(10),amount int);
insert into myschema.mygroyp(code,amount)values('a',1),('a',2),('b',3),('b',4);
select code, min(amount) as eind from myschema.mygroyp group by code;

create view myschema.mygroypview as select code, min(amount) as eind from myschema.mygroyp group by code;
select * from myschema.mygroypview;
select code, eind from myschema.mygroypview;
select code from myschema.mygroypview;
select eind from myschema.mygroypview;
select code, min(eind) from myschema.mygroypview group by code;

select code, min(eind) from myschema.mygroypview;
	-- error, cannot use non GROUP BY column 'code' in query results without an aggregate function
create view myschema.ups as select code, min(amount) as eind from myschema.mygroyp;
	-- error, cannot use non GROUP BY column 'code' in query results without an aggregate function
create view myschema.ups as select code, amount as eind from myschema.mygroyp group by code;
	-- error, cannot use non GROUP BY column 'amount' in query results without an aggregate function

drop schema myschema cascade;
