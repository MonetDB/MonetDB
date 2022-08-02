create schema "nschema";
set schema "nschema";
alter schema "nschema" rename to "nother"; --should be possible to rename current schema, if it's not a system one

create table "nother"."ntable" (a int);
insert into "nother"."ntable" values (1);
select "a" from "ntable";
select "a" from "nother"."ntable";

alter schema "sys" rename to "nsys"; --error

create function onefunc() returns int
begin
	return select count(*) from "ntable";
end;

select onefunc();
alter table "ntable" rename to "ttable"; --error because of dependencies
insert into "ntable" values (1);
select onefunc();

drop function "onefunc";
drop table "ntable";
set schema "sys";
drop schema "nother";
