-- http://bugs.monetdb.org/show_bug.cgi?id=2569

create table tmp_a (node integer, k varchar(255), v varchar(1024));   
create table tmp_b (node integer, k varchar(255), v varchar(1024));

insert into tmp_a values (1,'a','a');
insert into tmp_b values (1,'a','a');
insert into tmp_b values (2,'b','a');

insert into tmp_a select node,k,v from tmp_b except select node,k,v from tmp_a;


drop table tmp_a;
drop table tmp_b;
