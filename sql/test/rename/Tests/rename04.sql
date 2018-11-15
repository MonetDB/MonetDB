create table "trychangeme" (something int);
insert into "trychangeme" values (1);

start transaction;

insert into "trychangeme" values (2);
select "something" from "trychangeme";
alter table "trychangeme" rename to "anothername";
select "something" from "anothername";
select "name" from sys.tables where "name" in ('trychangeme', 'anothername');
insert into "anothername" values (3);
select "something" from "anothername";

savepoint sp1;

insert into "anothername" values (4);
select "something" from "anothername";
alter table "anothername" rename column "something" to "somethingelse";
select "somethingelse" from "anothername";
select "name" from sys.columns where "table_id" in (select "id" from sys.tables where "name" = 'anothername');

rollback to savepoint sp1;

insert into "anothername" values (5);
select "something" from "anothername";
select "name" from sys.columns where "table_id" in (select "id" from sys.tables where "name" = 'anothername');

rollback;

select "name" from sys.tables where "name" in ('trychangeme', 'anothername');
insert into "trychangeme" values (6);
select "something" from "trychangeme";

drop table "trychangeme";
select "name" from sys.tables where "name" in ('trychangeme', 'anothername');
