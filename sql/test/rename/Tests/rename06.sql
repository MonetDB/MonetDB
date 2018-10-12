alter schema if exists "thisschemashouldnotexist" rename to "somethingelse";
select "name" from sys.schemas where "name" in ('thisschemashouldnotexist', 'somethingelse'); --should be empty

create schema "renameme";
alter schema if exists "renameme" rename to "somethingelse";
select "name" from sys.schemas where "name" in ('renameme', 'somethingelse');
drop schema "somethingelse";

select "name" from sys.tables where "name" = 'thistableshouldnotexist'; --should be empty
alter table if exists "thistableshouldnotexist" rename to "somethingelse";
alter table if exists "thistableshouldnotexist" rename column "some" to "other";
alter table if exists "thistableshouldnotexist" drop column "a";
alter table "thistableshouldnotexist" add column "a" int; --error

create table "other_table"(a int);
alter table if exists "other_table" SET READ ONLY;
insert into "other_table" values (1); --error
alter table "other_table" SET READ WRITE;
insert into "other_table" values (2);
select "a" from "other_table";

alter table if exists "other_table" rename to "other_stuff";
alter table if exists "other_stuff" rename column "a" to "b";
alter table if exists "other_stuff" rename column "c" to "d"; --error, the "if exists" clauses verifies until the table
select "b" from "other_stuff";

alter table "other_stuff" add column "b" int; --error
alter table if exists "other_stuff" add column "c" int;
select "c" from "other_stuff";

drop table "other_stuff";
