
create table A (id int);
create table B (id int);
insert into  A values(1);
insert into  B values(1);

select * from B left join B on B.id = B.id;
select * from B left join B as b2 on B.id = b2.id;

select * from A, B left join B on B.id = A.id;

select * from A, B left join B as b2 on B.id = A.id;

drop table A;
drop table B;
