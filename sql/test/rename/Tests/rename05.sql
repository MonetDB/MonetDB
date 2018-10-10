create schema "nschema";
create table "nschema"."ntable" (a int);
insert into "nschema"."ntable" values (1);

set schema "nschema";
alter schema "nschema" rename to "nother"; --should be possible to rename current schema, if it's not a system one
select "a" from "ntable";
select "a" from "nother"."ntable";

alter schema "sys" rename to "nsys"; --error

create function onefunc() returns int
begin
	return select count(*) from "ntable";
end;

select onefunc();
alter table "ntable" rename to "ttable";
insert into "ttable" values (1);
select onefunc(); --error

drop function "onefunc";
drop table "ttable";
set schema "sys";
drop schema "nother";
