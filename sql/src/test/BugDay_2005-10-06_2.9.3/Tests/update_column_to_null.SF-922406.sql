create table up ( att int );
insert into up values (1);
commit;
select * from up;

update up set att = null;
select * from up;
