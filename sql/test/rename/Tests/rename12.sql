create schema "SA";
create schema "SB";

create table "SA"."t1" (a uuid);
insert into "SA"."t1" values (null), ('be887b3d-08f7-c308-7285-354a1857cbd9');

start transaction;
select "a" from "SA"."t1";
alter table "SA"."t1" set schema "SB";
select "a" from "SB"."t1";
drop table "SB"."t1";
rollback;

select "a" from "SA"."t1";
select "a" from "SB"."t1"; --error

start transaction;
select "a" from "SA"."t1";
alter table "SA"."t1" rename column "a" to "b"; 
select "b" from "SA"."t1";
commit;

select "b" from "SA"."t1";
select "a" from "SA"."t1"; --error

start transaction;
select "b" from "SA"."t1";
alter table "SA"."t1" rename column "b" to "c"; 
select "c" from "SA"."t1";
select "b" from "SA"."t1"; --error
rollback;

select "b" from "SA"."t1";
select "c" from "SA"."t1"; --error

start transaction;
select "b" from "SA"."t1";
alter table "SA"."t1" set schema "SB";
select "b" from "SB"."t1";
commit;

select "b" from "SB"."t1";
select "b" from "SA"."t1"; --error

drop schema "SA" cascade;
drop schema "SB" cascade;
