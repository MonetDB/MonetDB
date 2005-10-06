create table up ( att int );
insert into up values (1);
commit;

update up set att = null;
