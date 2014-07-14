create table Rc(i int, j int);
create table Sc(a int, b int);
create table Tc(x int, y int, z int);

insert into Rc values (1,1), (2,3), (2,4), (2,5), (3,3);
insert into Sc values (1,1), (1,3);
insert into Tc values (1,1,1), (2,2,3), (2,2,4), (2,2,5), (3,3,3);

select * from Rc;
select * from Sc;
select * from Tc;

set optimizer='centipede_pipe';

select i,count(*) from Rc group by i;
select j,count(*) from Rc group by j;
select a,count(*) from Sc group by a;
select b,count(*) from Sc group by b;

select i,sum(j) from Rc group by i;
select j,sum(i) from Rc group by j;
select a,sum(b) from Sc group by a;
select b,sum(a) from Sc group by b;

select i,avg(j) from Rc group by i;
select j,avg(i) from Rc group by j;
select a,avg(b) from Sc group by a;
select b,avg(a) from Sc group by b;

select i,min(j) from Rc group by i;
select j,min(i) from Rc group by j;
select a,min(b) from Sc group by a;
select b,min(a) from Sc group by b;

select i,max(j) from Rc group by i;
select j,max(i) from Rc group by j;
select a,max(b) from Sc group by a;
select b,max(a) from Sc group by b;

select x,y,count(*) from Tc group by x,y;
select x,y,max(z) from Tc group by x,y;
select x,y,sum(z) from Tc group by x,y;

drop table Rc;
drop table Sc;
drop table Tc;
