create schema changeme;

select "name", "system" from sys.schemas where "name" = 'changeme';
select "name", "system" from sys.schemas where "name" = 'changed';
alter schema "changeme" rename to "changed";
select "name", "system" from sys.schemas where "name" = 'changeme';
select "name", "system" from sys.schemas where "name" = 'changed';

create table "changed"."testme" (oneval int);
insert into "changed"."testme" values (1), (2), (NULL);

alter schema "changeme" rename to "another"; --error, does not exist
alter schema "changed" rename to "another"; --error, dependencies on it

select oneval from "changeme"."testme"; --error, does not exist
select oneval from "changed"."testme";
drop table "changeme"."testme"; --error, does not exist
drop table "changed"."testme";

drop schema "changeme"; --error, does not exist
drop schema "changed";
select "name", "system" from sys.schemas where "name" in ('changeme', 'changed');
