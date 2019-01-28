create schema "oldtables";
create schema "newtables";
create table "oldtables"."t1" ("a" int primary key, "b" int default '2');

select "kk"."type" from "sys"."keys" "kk" inner join "sys"."tables" "tt" on "kk"."table_id" = "tt"."id" where "tt"."name" = 't1';
select "ii"."type" from "sys"."idxs" "ii" inner join "sys"."tables" "tt" on "ii"."table_id" = "tt"."id" where "tt"."name" = 't1';
insert into "oldtables"."t1" values (3, default);
select "a", "b" from "oldtables"."t1";

alter table "oldtables"."t1" set schema "newtables";

select "kk"."type" from "sys"."keys" "kk" inner join "sys"."tables" "tt" on "kk"."table_id" = "tt"."id" where "tt"."name" = 't1';
select "ii"."type" from "sys"."idxs" "ii" inner join "sys"."tables" "tt" on "ii"."table_id" = "tt"."id" where "tt"."name" = 't1';
insert into "newtables"."t1" values (4, default);
select "a", "b" from "newtables"."t1";

create trigger "newtables"."tr1" after insert on "newtables"."t1" insert into "newtables"."t1" values (5, default);
alter table "newtables"."t1" set schema "oldtables"; --error, dependency on trigger

drop schema "oldtables" cascade;
drop schema "newtables" cascade;

select "kk"."type" from "sys"."keys" "kk" inner join "sys"."tables" "tt" on "kk"."table_id" = "tt"."id" where "tt"."name" = 't1';
select "ii"."type" from "sys"."idxs" "ii" inner join "sys"."tables" "tt" on "ii"."table_id" = "tt"."id" where "tt"."name" = 't1';
