set debug=4096;
create table tst( a0 int unique, a1 int);
insert into tst values(1,1);
insert into tst values(2,2);
select * from tst;
select * from tst where a1>=0 and a1 <=3;
