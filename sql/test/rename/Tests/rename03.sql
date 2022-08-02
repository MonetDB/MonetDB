create table "rename" (oneval int, twoval clob);
insert into "rename" values (1, 'one'), (2, 'two');
select oneval from "rename";
select "name" from sys.tables where "name" = 'rename';
select "name" from sys.tables where "name" = 'ichanged';
select "oneval", "twoval" from "rename";

alter table "rename" rename to "ichanged";
insert into "rename" values (0, 'ups'); --error
insert into "ichanged" values (3, 'three'), (4, 'four');
select "name" from sys.tables where "name" = 'rename';
select "name" from sys.tables where "name" = 'ichanged';
select "oneval", "twoval" from "rename"; --error
select "oneval", "twoval" from "ichanged";
select * from "rename"; --error
select * from "ichanged";

select "name" from sys.columns where "table_id" in (select "id" from sys.tables where "name" = 'rename');
select "name" from sys.columns where "table_id" in (select "id" from sys.tables where "name" = 'ichanged');

alter table "rename" rename column "oneval" to "threeval"; --error
alter table "ichanged" rename column "oneval" to "threeval";

insert into "rename" values (NULL, NULL), (5, 'five'); --error
insert into "ichanged" values (NULL, NULL), (5, 'five');
insert into "ichanged" values (NULL, NULL), ('five', 5); --error

select "name" from sys.columns where "table_id" in (select "id" from sys.tables where "name" = 'rename');
select "name" from sys.columns where "table_id" in (select "id" from sys.tables where "name" = 'ichanged');

select "oneval", "twoval" from "ichanged"; --error
select "threeval", "twoval" from "ichanged";
select * from "ichanged";

drop table "rename"; --error
drop table "ichanged";
select "name" from sys.tables where "name" in ('rename', 'ichanged');
