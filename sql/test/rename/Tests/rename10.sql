create schema "SA";
create schema "SB";

create table "SA"."t1" (a int);
insert into "SA"."t1" values (1);

start transaction;
alter table "SA"."t1" set schema "SB";
drop table "SB"."t1";
rollback;

select "a" from "SA"."t1";
select "a" from "SB"."t1"; --error

drop schema "SA" cascade;
drop schema "SB" cascade;
