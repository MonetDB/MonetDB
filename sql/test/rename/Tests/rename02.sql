create schema testing;

start transaction;
alter schema "testing" rename to "altered";
select "name" from sys.schemas where "name" = 'testing';
select "name" from sys.schemas where "name" = 'altered';
create table "altered"."anothertable" (oneval int);
insert into "altered"."anothertable" values (1);
rollback;

select "name" from sys.schemas where "name" = 'testing';
select "name" from sys.schemas where "name" = 'altered';
select "name" from sys.tables where name = 'anothertable';

drop schema "testing";
