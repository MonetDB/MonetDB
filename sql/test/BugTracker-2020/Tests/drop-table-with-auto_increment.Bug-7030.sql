create local temp table "depend_count" ("a" bigint) on commit preserve rows;
insert into "depend_count" values ((select count(*) from dependencies, sequences));

create schema s1;
create table s1.t (i int not null auto_increment);
drop table s1.t;
drop schema s1;

select cast(count(*) - (select "a" from "depend_count") as bigint) from dependencies, sequences;
	-- the number of dependencies and sequences shouldn't increase
drop table "depend_count";
