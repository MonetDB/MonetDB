create schema changeme;
create table "changeme"."testme" (oneval int);
insert into "changeme"."testme" values (1), (2), (NULL);

select "name", "system" from sys.schemas where "name" = 'changeme';
select "name", "system" from sys.schemas where "name" = 'changed';
select oneval from "changeme"."testme";
select oneval from "changed"."testme"; --error

alter schema "changeme" rename to "changed";

select "name", "system" from sys.schemas where "name" = 'changeme';
select "name", "system" from sys.schemas where "name" = 'changed';
select oneval from "changeme"."testme"; --error
select oneval from "changed"."testme";

drop table "changeme"."testme"; --error
drop table "changed"."testme";

drop schema "changeme"; --error
drop schema "changed";
select "name", "system" from sys.schemas where "name" in ('changeme', 'changed');
