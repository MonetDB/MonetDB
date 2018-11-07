create table updateme (a int, b int);
create table other (a int, b int);
insert into updateme values (1,1), (2,2), (3,3);

update updateme as other set a=3 where b=2;
update updateme as other set a=2 where other.b=3;
update updateme as other set a=3 where updateme.b=2; --error
update updateme as other set a=4 from other where other.a=1; --error
select a, b from updateme;

delete from updateme as other where other.a=3;
delete from updateme where other.b=2; --error
delete from updateme as other where updateme.b=2; --error
select a, b from updateme;

drop table updateme;
drop table other;
