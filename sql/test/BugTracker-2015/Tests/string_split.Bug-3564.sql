
select split_part('joeuser@mydatabase','@',0) AS "an error";
select split_part('joeuser@mydatabase','@',1) AS "joeuser";
select split_part('joeuser@mydatabase','@',2) AS "mydatabase";
select split_part('joeuser@mydatabase','@',3) AS "empty string";
select split_part('','@',3) AS "error";

start transaction;

create table somestrings(a string);
insert into somestrings values(''),(' '),('joeuser@mydatabase'), ('a@'), ('@b'), ('a@@@b'), ('@@b');
select * from somestrings;
select split_part(a,'@',1), split_part(a,'@',2) from somestrings;
drop table somestrings;

rollback;
