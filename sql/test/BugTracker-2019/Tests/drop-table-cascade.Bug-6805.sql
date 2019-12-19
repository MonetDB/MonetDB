create schema "configuration";
create table "configuration".testcascade (testkolom varchar(50), testkolom2 varchar(50));
create table "configuration".testcascade2 (testkolom varchar(50));
insert into "configuration".testcascade (testkolom, testkolom2) values('derect','jip'),('hans','job'),('gruber','jet');
create view sys.testcascade_view as select testcascade.testkolom from "configuration".testcascade;
drop table "configuration".testcascade; --error, dependency exists
drop table "configuration".testcascade cascade;

select * from sys.testcascade_view; --error, no longer exists
select * from "configuration".testcascade; --error, no longer exists

drop schema "configuration" cascade;

select * from sys.testcascade_view; --error, no longer exists
select * from "configuration".testcascade; --error, no longer exists
