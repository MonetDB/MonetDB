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
select "name" from sys.tables where "name" = 'anothertable';

drop schema "testing";
select "name" from sys.schemas where "name" in ('testing', 'altered');
select "name" from sys.tables where "name" = 'anothertable';

create schema testing;
start transaction;
alter schema "testing" rename to "altered";
this query is wrong; --error
rollback;

drop schema "testing";
select "name" from sys.schemas where "name" in ('testing', 'altered');

create schema testing;
start transaction;
alter schema "testing" rename to "altered";
commit;

select "name" from sys.schemas where "name" in ('testing', 'altered');
drop schema "testing"; --error
drop schema "altered";
select "name" from sys.schemas where "name" in ('testing', 'altered');
