create table bug (a int, b int);
insert into bug values (1,1);
insert into bug values (1,2);
insert into bug values (1,3);
create view bug_2 as select * from bug;
create view bug_3 as select * from bug_2 except select * from bug where bug.b=2;

select * from bug_3;
select * from bug_3 limit 1;

drop view bug_3;
drop view bug_2;
drop table bug;
