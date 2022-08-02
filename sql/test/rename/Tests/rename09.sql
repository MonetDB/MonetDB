create schema "SA";
create schema "SB";
create schema "SC";

create table "SA"."t1" (a int);
insert into "SA"."t1" values (1);
select "a" from "SA"."t1";

start transaction; --Attempt to change schema twice within the same transaction
select "a" from "SA"."t1";
alter table "SA"."t1" set schema "SB";
select "a" from "SB"."t1";
alter table "SB"."t1" set schema "SC";
select "a" from "SC"."t1";
select "a" from "SB"."t1"; --error
rollback;

select "a" from "SA"."t1";
select "a" from "SB"."t1"; --error
select "a" from "SC"."t1"; --error

alter table "SA"."t1" set schema "SB";
select "a" from "SA"."t1"; --error
select "a" from "SB"."t1";
select "a" from "SC"."t1"; --error

alter table "SB"."t1" set schema "SC";
select "a" from "SA"."t1"; --error
select "a" from "SB"."t1"; --error
select "a" from "SC"."t1";

alter table "SC"."t1" set schema "SA";

start transaction; --Attempt to change schema and back to the original one
select "a" from "SA"."t1";
alter table "SA"."t1" set schema "SB";
select "a" from "SB"."t1";
alter table "SB"."t1" set schema "SA";
select "a" from "SA"."t1";
select "a" from "SB"."t1"; --error
rollback;

select "a" from "SA"."t1";
select "a" from "SB"."t1"; --error

drop schema "SA" cascade;
drop schema "SB" cascade;
drop schema "SC" cascade;
