
create table x (id int);
insert into x (id) values (1);
insert into x (id) values (2);
insert into x (id) values (3);


create table y (n int, s varchar(10));
insert into y (n,s) values (1, 'one');
insert into y (n,s) values (2, 'two');
insert into y (n,s) values (3, 'three');


create view yv as select s as a1, n as a2 from y;

create view j as select yv.a2 as a1, yv.a1 as a2 from x,yv where x.id = yv.a2;

select * from j where a2='y-three';

drop table x cascade;
drop table y cascade;
