create schema "oldtables";
create schema "newtables";

create table "oldtables"."atable" (a int);
insert into "oldtables"."atable" values (1);

select "a" from "oldtables"."atable";
select "a" from "newtables"."atable"; --error

alter table "oldtables"."atable" set schema "newtables";

select "a" from "oldtables"."atable"; --error
select "a" from "newtables"."atable";

drop table "newtables"."atable";
drop schema "oldtables";
drop schema "newtables";
