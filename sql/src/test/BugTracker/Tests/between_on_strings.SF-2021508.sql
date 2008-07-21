create table between_on_strings1 (id int, v char(10));
create table between_on_strings2 (id int, v char(10));
insert into between_on_strings1 values (1,'s1');
insert into between_on_strings1 values (2,'s2');
insert into between_on_strings1 values (3,'s3');
insert into between_on_strings1 values (4,'s4');
insert into between_on_strings1 values (5,'s5');
insert into between_on_strings1 values (6,'s6');
insert into between_on_strings2 values (1,'s1');
insert into between_on_strings2 values (2,'s2');
insert into between_on_strings2 values (3,'s3');
insert into between_on_strings2 values (4,'s4');
insert into between_on_strings2 values (5,'s5');
insert into between_on_strings2 values (6,'s6');
select between_on_strings1.id from between_on_strings1, between_on_strings2 where between_on_strings1.v between between_on_strings2.v and between_on_strings2.v||'c' ;

drop table between_on_strings1;
drop table between_on_strings2;
