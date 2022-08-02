-- set debug=4096;
create table tst( a0 int unique, a1 int);
insert into tst values(1,1);
insert into tst values(2,2);
insert into tst values(3,3);
insert into tst values(4,4);
select * from tst;
select * from tst where a1>=0 and a1 <=3;

create table tst2( b0 int unique, b1 int);
insert into tst2 values(1,1);
insert into tst2 values(2,2);
insert into tst2 values(3,3);
insert into tst2 values(4,4);
select * from tst2;
select * from tst2 where b1>=0 and b1 <=3;

drop table tst;
drop table tst2;
