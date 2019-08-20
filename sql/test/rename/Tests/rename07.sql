create schema "oldtables";
create schema "newtables";

create table "oldtables"."atable" (a int);
insert into "oldtables"."atable" values (1);

select "a" from "oldtables"."atable" where false;
select "a" from "newtables"."atable"; --error

select "ss"."name" from "sys"."tables" "tt" inner join "sys"."schemas" "ss" on "tt"."schema_id" = "ss"."id" where "tt"."name" = 'atable';
alter table "oldtables"."atable" set schema "newtables";
select "ss"."name" from "sys"."tables" "tt" inner join "sys"."schemas" "ss" on "tt"."schema_id" = "ss"."id" where "tt"."name" = 'atable';

select "a" from "oldtables"."atable"; --error
select "a" from "newtables"."atable" where false;

start transaction;
select "ss"."name" from "sys"."tables" "tt" inner join "sys"."schemas" "ss" on "tt"."schema_id" = "ss"."id" where "tt"."name" = 'atable';
alter table "newtables"."atable" set schema "oldtables";
select "ss"."name" from "sys"."tables" "tt" inner join "sys"."schemas" "ss" on "tt"."schema_id" = "ss"."id" where "tt"."name" = 'atable';
select "a" from "oldtables"."atable" where false;
rollback;

select "ss"."name" from "sys"."tables" "tt" inner join "sys"."schemas" "ss" on "tt"."schema_id" = "ss"."id" where "tt"."name" = 'atable';
select "a" from "newtables"."atable" where false;

drop schema "oldtables" cascade;
drop schema "newtables" cascade;
